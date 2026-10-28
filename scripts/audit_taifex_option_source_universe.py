#!/usr/bin/env python3
"""Inventory product-day observations in retained official option archives.

This is a source audit, not an execution tape or a point-in-time listing
catalog.  The shared TAIFEX reader supplies the archive members and source
hash; Arrow reads each member in bounded batches without extracting a copy.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

import polars as pl
import pyarrow as pa
import pyarrow.csv as arrow_csv

from stockagent.data.tw_index_futures import (
    TAIFEX_DAY_SESSION_ALIASES,
    iter_taifex_daily_csv_streams,
)

CONTRACT = "taifex_option_archive_product_day_inventory_v1"
INDEX_CODES = frozenset(
    # TAIFEX's historical index-option catalog also lists MSO and GTO.
    {"TXO", "TX1", "TX2", "TX3", "TX4", "TX5", "TXU", "TXV", "TXX", "TXY", "TXZ", "TEO", "TFO", "XIO", "MSO", "GTO"}
)
COMMODITY_CODES = frozenset({"TGO"})
CURRENCY_CODES = frozenset({"RHO", "RTO"})
NIGHT_ALIASES = frozenset({"盤後", "盤後交易時段", "after_hours", "night", "night_session"})
COLUMNS = ("交易日期", "契約", "成交量", "交易時段")


def provisional_class(product: str, product_name: str) -> str:
    """Classify named families conservatively; no currency/listing inference."""
    if product in INDEX_CODES:
        return "index_option"
    if product in COMMODITY_CODES:
        return "commodity_option"
    if product in CURRENCY_CODES:
        return "currency_option"
    if "選擇權" not in product_name:
        return "unclassified"
    if "ETF" in product_name.upper():
        return "etf_option_candidate"
    return "security_option_candidate"


def _session(value: object) -> str:
    label = str(value or "").strip()
    if label in TAIFEX_DAY_SESSION_ALIASES:
        return "day"
    if label in NIGHT_ALIASES:
        return "night"
    return "unreported" if not label else "unknown"


def _csv_column_names(binary, member: str) -> tuple[list[str], bool]:
    """Reconcile official files with an extra trailing empty CSV column.

    Some annual members include an unlabelled trailing empty field on every
    row; one has the extra delimiter only in its header. Inspect the first row,
    change only empty trailing header names, and let Arrow reject any later row
    with a different width rather than silently skipping it.
    """
    header_line, first_line = binary.readline(), binary.readline()
    if not header_line or not first_line:
        raise ValueError(f"missing CSV header or data: {member}")
    header = next(csv.reader([header_line.decode("cp950")]))
    first = next(csv.reader([first_line.decode("cp950")]))
    original = len(header)
    if len(first) > len(header):
        if any(value.strip() for value in first[len(header):]):
            raise ValueError(f"unlabelled nonempty CSV columns: {member}")
        header.extend([""] * (len(first) - len(header)))
    elif len(first) < len(header):
        if any(value.strip() for value in header[len(first):]):
            raise ValueError(f"missing named CSV columns: {member}")
        header = header[:len(first)]
    names = [name if name else f"__unlabelled_{i}" for i, name in enumerate(header)]
    if len(set(names)) != len(names):
        raise ValueError(f"duplicate CSV header names: {member}")
    binary.seek(0)
    return names, original != len(header)


def _atomic_text(path: Path, write) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", dir=path.parent, delete=False) as f:
            name = f.name
            write(f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def audit(source_dir: Path, manifest_path: Path, catalog_path: Path, start_year: int, end_year: int) -> tuple[list[dict], dict]:
    if start_year > end_year:
        raise ValueError("start-year is after end-year")
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    receipts = {Path(row["path"]).name: row["sha256"] for row in manifest["receipts"] if "/annual/" in row["path"]}
    catalog_bytes = catalog_path.read_bytes()
    with catalog_path.open(encoding="utf-8", newline="") as f:
        names = {row["code"].strip(): row["product_name"].strip() for row in csv.DictReader(f)}

    stats: dict[str, dict] = {}
    inputs = []
    for year in range(start_year, end_year + 1):
        source = source_dir / f"{year}_opt.zip"
        expected = receipts.get(source.name)
        if not expected or not source.is_file():
            raise ValueError(f"missing source or pinned annual receipt: {source}")
        members = 0
        adjusted_headers = []
        for text_stream, member, sha256 in iter_taifex_daily_csv_streams(source):
            if sha256 != expected:
                raise ValueError(f"annual source SHA mismatch: {source}")
            members += 1
            column_names, adjusted = _csv_column_names(text_stream.buffer, member)
            if adjusted:
                adjusted_headers.append(member)
            reader = arrow_csv.open_csv(
                text_stream.buffer,
                read_options=arrow_csv.ReadOptions(encoding="cp950", block_size=2 * 1024 * 1024, column_names=column_names, skip_rows=1),
                convert_options=arrow_csv.ConvertOptions(
                    include_columns=list(COLUMNS),
                    include_missing_columns=True,
                    column_types={name: pa.string() for name in COLUMNS},
                ),
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
                        raise ValueError(f"invalid product/date in {member}")
                    grouped = frame.group_by("product", "date", "session").agg(
                        pl.len().alias("rows"),
                        (pl.col("volume") > 0).fill_null(False).sum().alias("positive_rows"),
                        (pl.col("volume").is_null() | (pl.col("volume") < 0)).sum().alias("invalid_volume_rows"),
                    )
                    for product, day, session, count, positive, invalid in grouped.iter_rows():
                        row = stats.setdefault(product, {"source_rows": 0, "positive_volume_rows": 0, "invalid_volume_rows": 0, "observed_days": set(), "positive_days": set(), "positive_day_sessions": set(), "positive_night_sessions": set(), "positive_unreported_sessions": set(), "positive_unknown_sessions": set()})
                        row["source_rows"] += count
                        row["positive_volume_rows"] += positive
                        row["invalid_volume_rows"] += invalid
                        row["observed_days"].add(day)
                        if positive:
                            row["positive_days"].add(day)
                            row[f"positive_{_session(session)}_sessions"].add(day)
            finally:
                reader.close()
        if not members:
            raise ValueError(f"archive has no CSV members: {source}")
        inputs.append({"path": str(source.resolve()), "sha256": expected, "csv_members": members, "empty_trailing_header_adjustments": adjusted_headers})
        print(f"audited annual archive {year}: {members} CSV members", file=sys.stderr, flush=True)

    if manifest_path.read_bytes() != manifest_bytes or catalog_path.read_bytes() != catalog_bytes:
        raise ValueError("source manifest or catalog changed during audit")
    for item in inputs:
        if _file_sha256(Path(item["path"])) != item["sha256"]:
            raise ValueError(f"annual source changed during audit: {item['path']}")

    rows = []
    for product, item in sorted(stats.items()):
        observed, positive = item["observed_days"], item["positive_days"]
        name = names.get(product, "")
        rows.append({
            "product": product, "catalog_name": name,
            "provisional_class": provisional_class(product, name),
            "currency": "unverified",
            "first_observed_date": min(observed).isoformat(), "last_observed_date": max(observed).isoformat(),
            "observed_days": len(observed),
            "first_positive_volume_date": min(positive).isoformat() if positive else "",
            "last_positive_volume_date": max(positive).isoformat() if positive else "",
            "positive_volume_days": len(positive),
            "positive_day_session_dates": len(item["positive_day_sessions"]),
            "positive_night_session_dates": len(item["positive_night_sessions"]),
            "positive_unreported_session_dates": len(item["positive_unreported_sessions"]),
            "positive_unknown_session_dates": len(item["positive_unknown_sessions"]),
            "source_rows": item["source_rows"], "positive_volume_rows": item["positive_volume_rows"],
            "invalid_volume_rows": item["invalid_volume_rows"],
        })
    summary = {
        "contract": CONTRACT,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_scope": f"official annual archives {start_year}-{end_year}",
        "audit_script": str(Path(__file__).resolve()),
        "audit_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "shared_reader_path": str((Path(__file__).resolve().parents[1] / "stockagent/data/tw_index_futures.py")),
        "shared_reader_sha256": hashlib.sha256((Path(__file__).resolve().parents[1] / "stockagent/data/tw_index_futures.py").read_bytes()).hexdigest(),
        "source_manifest": str(manifest_path.resolve()),
        "source_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "product_name_catalog": str(catalog_path.resolve()),
        "product_name_catalog_sha256": hashlib.sha256(catalog_bytes).hexdigest(),
        "annual_inputs": inputs,
        "distinct_observed_products": len(rows),
        "products_with_positive_volume": sum(r["positive_volume_days"] > 0 for r in rows),
        "provisional_classes": {k: sum(r["provisional_class"] == k for r in rows) for k in sorted({r["provisional_class"] for r in rows})},
        "boundaries": [
            "Source rows may overlap between archive members; unique dates are deduplicated per product and session.",
            "A positive daily volume is evidence of some trading, not a quote, order fill, or execution at either decision clock.",
            "Session-unreported rows are kept separate; classification and currency require dated authoritative verification.",
            "Observed dates are not complete lifecycle, historical membership, contract terms, or training readiness.",
        ],
    }
    return rows, summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path("data_tw_index_options_daily/raw/annual"))
    parser.add_argument("--manifest", type=Path, default=Path("data_tw_index_options_daily/manifest.json"))
    parser.add_argument("--catalog", type=Path, default=Path("data_tw_futures/taifex_contract_codes.csv"))
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    rows, summary = audit(args.source_dir, args.manifest, args.catalog, args.start_year, args.end_year)
    if not rows:
        raise ValueError("no option product observations")
    csv_path = args.output_dir / "official_annual_option_product_days.csv"
    def write_csv(f):
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    _atomic_text(csv_path, write_csv)
    summary["product_inventory"] = str(csv_path.resolve())
    summary["product_inventory_sha256"] = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    _atomic_text(args.output_dir / "official_annual_option_product_days.summary.json", lambda f: json.dump(summary, f, ensure_ascii=False, indent=2))
    print(json.dumps({"contract": CONTRACT, "products": len(rows), "products_with_positive_volume": summary["products_with_positive_volume"], "output": str(csv_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
