#!/usr/bin/env python3
"""Audit option trading-day evidence against retained FinMind day-object receipts.

All-positive-product dates are extracted from receipt-pinned TAIFEX annual
archives using the shared source reader. Natural-day FinMind objects are
checked independently. A verified object is not proof of contract, second,
price, historical point-in-time or execution completeness.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sys

import polars as pl
import pyarrow as pa
import pyarrow.csv as arrow_csv

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from scripts.audit_taifex_option_source_universe import _csv_column_names, _session
from stockagent.data.tw_index_futures import iter_taifex_daily_csv_streams


def official_product_days(source_dir: Path, manifest_path: Path, start_year: int, end_year: int):
    if start_year > end_year:
        raise ValueError("invalid year interval")
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    pinned = {Path(x["path"]).name: x["sha256"] for x in manifest["receipts"] if "/annual/" in x["path"]}
    observations = defaultdict(lambda: defaultdict(set))
    sources = []
    for year in range(start_year, end_year + 1):
        path = source_dir / f"{year}_opt.zip"
        expected = pinned.get(path.name)
        if not expected or not path.is_file():
            raise ValueError(f"missing pinned annual source {path}")
        members = 0
        for stream, member, digest in iter_taifex_daily_csv_streams(path):
            if digest != expected:
                raise ValueError(f"annual source SHA mismatch: {path}")
            members += 1
            names, _ = _csv_column_names(stream.buffer, member)
            cols = ["交易日期", "契約", "成交量", "交易時段"]
            reader = arrow_csv.open_csv(
                stream.buffer,
                read_options=arrow_csv.ReadOptions(encoding="cp950", block_size=2 * 1024 * 1024,
                                                   column_names=names, skip_rows=1),
                convert_options=arrow_csv.ConvertOptions(include_columns=cols, include_missing_columns=True,
                                                         column_types={x: pa.string() for x in cols}),
            )
            try:
                for batch in reader:
                    frame = pl.from_arrow(batch).select(
                        pl.col("交易日期").str.strip_chars().str.strptime(pl.Date, "%Y/%m/%d", strict=False).alias("date"),
                        pl.col("契約").str.strip_chars().str.to_uppercase().alias("product"),
                        pl.col("成交量").str.strip_chars().str.replace_all(",", "").cast(pl.Int64, strict=False).alias("volume"),
                        pl.col("交易時段").fill_null("").str.strip_chars().alias("session"),
                    )
                    if frame.filter(pl.col("date").is_null() | pl.col("product").is_null() | (pl.col("product") == "")).height:
                        raise ValueError(f"invalid date or product in {member}")
                    positive = frame.filter(pl.col("volume") > 0).select("date", "product", "session").unique()
                    for day, product, session in positive.iter_rows():
                        if day.year != year:
                            raise ValueError(f"annual archive date outside {year}: {day}")
                        observations[day][_session(session)].add(product)
            finally:
                reader.close()
        if not members or sha256_file(path) != expected:
            raise ValueError(f"annual source changed or has no members: {path}")
        sources.append({"path": str(path.resolve()), "sha256": expected, "members": members})
        print(f"audited official option product dates: {year}", file=sys.stderr, flush=True)
    if manifest_path.read_bytes() != manifest_bytes:
        raise ValueError("option annual manifest changed during audit")
    return observations, sources, hashlib.sha256(manifest_bytes).hexdigest()


def receipt_status(day: date, root: Path, *, verify_payload_sha: bool) -> dict:
    path = root / "receipts/TaiwanOptionTick/all" / f"{day.isoformat()}.json"
    if not path.is_file():
        return {"status": "missing_receipt"}
    try:
        payload = path.read_bytes()
        receipt = json.loads(payload)
        if (receipt.get("dataset") != "TaiwanOptionTick" or receipt.get("partition") != day.isoformat()
                or receipt.get("data_id") != ""):
            return {"status": "receipt_identity_mismatch"}
        state = receipt.get("status", "unknown")
        result = {"status": state, "receipt_sha256": hashlib.sha256(payload).hexdigest(),
                  "receipt_path": str(path.resolve()), "rows": receipt.get("rows")}
        if state == "complete":
            source = (root / receipt["parquet_path"]).resolve()
            if not source.is_relative_to(root.resolve()) or not source.is_file():
                return {**result, "status": "complete_payload_missing_or_outside_root"}
            if source.stat().st_size != receipt.get("parquet_size_bytes"):
                return {**result, "status": "complete_payload_size_mismatch"}
            result.update(parquet_path=str(source), parquet_sha256=receipt.get("sha256"))
            if verify_payload_sha:
                if sha256_file(source) != receipt.get("sha256"):
                    return {**result, "status": "complete_payload_sha_mismatch"}
                result["status"] = "complete_payload_sha_verified"
            else:
                result["status"] = "complete_payload_exists_not_hashed"
        if path.read_bytes() != payload:
            raise ValueError(f"receipt changed during audit: {path}")
        return result
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        return {"status": "invalid_receipt", "error_type": type(exc).__name__}


def audit(source_dir: Path, manifest_path: Path, finmind_root: Path, start_year: int, end_year: int, *, verify_payload_sha: bool):
    days, sources, manifest_digest = official_product_days(source_dir, manifest_path, start_year, end_year)
    rows = []
    for day, by_session in sorted(days.items()):
        # Unreported is kept separate: annual daily records cannot prove which
        # physical calendar day or specific intraday clock had a trade.
        candidate = bool(by_session["day"] or by_session["unreported"])
        receipt = receipt_status(day, finmind_root, verify_payload_sha=verify_payload_sha)
        rows.append({
            "date": day.isoformat(),
            "day_positive_products": "|".join(sorted(by_session["day"])),
            "unreported_session_positive_products": "|".join(sorted(by_session["unreported"])),
            "night_positive_products": "|".join(sorted(by_session["night"])),
            "unknown_session_positive_products": "|".join(sorted(by_session["unknown"])),
            "day_or_unreported_positive_candidate": candidate,
            "finmind_status": receipt["status"],
            "finmind_rows": receipt.get("rows"),
            "finmind_receipt_path": receipt.get("receipt_path", ""),
            "finmind_receipt_sha256": receipt.get("receipt_sha256", ""),
            "finmind_parquet_path": receipt.get("parquet_path", ""),
            "finmind_parquet_sha256": receipt.get("parquet_sha256", ""),
        })
    by_year = []
    for year in range(start_year, end_year + 1):
        subset = [x for x in rows if x["date"].startswith(str(year))]
        candidates = [x for x in subset if x["day_or_unreported_positive_candidate"]]
        by_year.append({"year": year, "official_any_session_positive_days": len(subset),
                        "official_day_or_unreported_positive_days": len(candidates),
                        "day_or_unreported_finmind_receipt_states": dict(sorted(Counter(x["finmind_status"] for x in candidates).items()))})
    return rows, {
        "contract": "taifex_option_annual_positive_days_finmind_receipts_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_manifest": str(manifest_path.resolve()), "source_manifest_sha256": manifest_digest,
        "annual_sources": sources, "finmind_root": str(finmind_root.resolve()),
        "finmind_payload_sha_verified": verify_payload_sha,
        "audit_sha256": sha256_file(Path(__file__)),
        "annual_reader_sha256": sha256_file(Path("stockagent/data/tw_index_futures.py")),
        "header_reader_sha256": sha256_file(Path("scripts/audit_taifex_option_source_universe.py")),
        "by_year": by_year,
        "limitations": [
            "The official set includes only dates with at least one positive reported option daily volume, not all listings.",
            "Day-session and unreported-session evidence are separate; night trade-date attribution is not a natural-day tick requirement.",
            "A native receipt and even a byte-verified parquet do not establish per-product or per-contract source completeness, clock eligibility, or fillability.",
            "The retained vendor objects are retrospective and their historical point-in-time publication is not verified.",
        ],
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-dir", type=Path, default=Path("data_tw_index_options_daily/raw/annual"))
    p.add_argument("--manifest", type=Path, default=Path("data_tw_index_options_daily/manifest.json"))
    p.add_argument("--finmind-root", type=Path, default=Path("data_finmind/complement"))
    p.add_argument("--start-year", type=int, default=2011)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument("--verify-payload-sha", action="store_true")
    p.add_argument("--output-dir", type=Path, required=True)
    a = p.parse_args()
    rows, summary = audit(a.source_dir, a.manifest, a.finmind_root, a.start_year, a.end_year,
                          verify_payload_sha=a.verify_payload_sha)
    if not rows:
        raise ValueError("no official positive option days")
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    body = text.getvalue().encode("utf-8")
    csv_path = a.output_dir / "official_option_days_finmind_receipts.csv"
    atomic_write_bytes(csv_path, body)
    summary.update(day_inventory=str(csv_path.resolve()), day_inventory_sha256=hashlib.sha256(body).hexdigest())
    atomic_write_json(a.output_dir / "official_option_days_finmind_receipts.summary.json", summary)
    print(json.dumps({"output": str(csv_path), "by_year": summary["by_year"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
