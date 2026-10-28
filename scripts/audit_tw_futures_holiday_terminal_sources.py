#!/usr/bin/env python3
"""Read back retained official final-settlement receipts on named dates.

The stock/ETF source is queried by delivery month, not settlement month.  All
retained monthly receipts must therefore be scanned, including later delivery
months.  This audit does not establish broker processing or daily valuations.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file


def audit(manifest_path: Path, normalized_path: Path, targets: list[date]) -> dict:
    """Check every declared raw receipt and the normalized dated rows."""
    manifest_digest = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "complete" or manifest.get("schema_version") != 2:
        raise ValueError("source manifest is not a complete all-family release")
    first = date.fromisoformat(manifest["requested_start_date"])
    last = date.fromisoformat(manifest["requested_end_date"])
    if not targets or len(set(targets)) != len(targets) or any(not first <= d <= last for d in targets):
        raise ValueError("target dates must be unique and inside the retained source horizon")
    output = manifest.get("outputs", {}).get("futures_final_settlement_history", {})
    normalized_digest = sha256_file(normalized_path)
    if (normalized_digest != output.get("sha256")
            or normalized_path.stat().st_size != output.get("bytes")):
        raise ValueError("normalized official final-settlement source changed")
    frame = pl.read_parquet(normalized_path)
    if frame.height != output.get("rows"):
        raise ValueError("normalized official final-settlement row count changed")
    kinds = {"index_futures", "stock_etf_futures", "commodity_futures", "interest_rate_futures"}
    receipts = manifest.get("receipts", [])
    if not receipts or {item.get("kind") for item in receipts} != kinds:
        raise ValueError("official final-settlement source family inventory changed")
    counts: Counter = Counter()
    raw_matches: list[dict] = []
    seen = set()
    patterns = {d: re.compile(rb"(?<!\d)" + str(d.year).encode() + rb"\s*/\s*0?"
                              + str(d.month).encode() + rb"\s*/\s*0?"
                              + str(d.day).encode() + rb"(?!\d)") for d in targets}
    for receipt in receipts:
        key = (receipt["kind"], receipt["period"])
        if key in seen:
            raise ValueError("official source period is duplicated")
        seen.add(key)
        path = Path(receipt["path"])
        raw = path.read_bytes()
        if (len(raw) != receipt["bytes"]
                or hashlib.sha256(raw).hexdigest() != receipt["sha256"]):
            raise ValueError(f"official raw source changed: {path}")
        counts[receipt["kind"]] += 1
        for target, pattern in patterns.items():
            if pattern.search(raw):
                raw_matches.append({"date": target.isoformat(), "kind": receipt["kind"],
                                    "period": receipt["period"], "path": str(path),
                                    "sha256": receipt["sha256"]})
    for year in {d.year for d in targets}:
        for kind in kinds - {"stock_etf_futures"}:
            if (kind, str(year)) not in seen:
                raise ValueError(f"missing retained official {kind} year {year}")
    # Stock pages are delivery-month queries; all declared receipts have been
    # read, but missing or out-of-horizon contracts still cannot be inferred.
    for target in targets:
        if ("stock_etf_futures", target.strftime("%Y-%m")) not in seen:
            raise ValueError(f"missing retained stock/ETF delivery-month page {target:%Y-%m}")
    normalized_matches = (frame.filter(pl.col("settlement_date").is_in(targets))
                          .select("settlement_date", "product", "contract", "source_kind",
                                  "reported_date_role", "settlement_method")
                          .with_columns(pl.col("settlement_date").cast(pl.String)).to_dicts())
    if sha256_file(manifest_path) != manifest_digest or sha256_file(normalized_path) != normalized_digest:
        raise ValueError("source changed during readback")
    return {
        "contract": "official_holiday_final_settlement_source_readback_v1",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "target_dates": [d.isoformat() for d in targets],
        "source_requested_horizon": [first.isoformat(), last.isoformat()],
        "manifest_path": str(manifest_path), "manifest_sha256": manifest_digest,
        "normalized_path": str(normalized_path), "normalized_sha256": normalized_digest,
        "normalized_rows": frame.height, "receipt_counts": dict(sorted(counts.items())),
        "all_declared_receipt_bytes_and_sha256_verified": True,
        "raw_date_matches": raw_matches, "normalized_date_matches": normalized_matches,
        "scope_limits": [
            "raw text matches are candidates, not automatic contract admission",
            "stock/ETF pages are delivery-month queries; only declared retained pages were checked",
            "interest-rate pages report last trading day, not necessarily final settlement day",
            "absence in retained pages is not proof of broker account processing or valuation",
            "neither historical availability nor full exchange product coverage is established",
        ],
        "financial_or_training_adopted": False,
        "audit_script_sha256": sha256_file(Path(__file__)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--normalized", type=Path, required=True)
    parser.add_argument("--date", action="append", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("existing source-audit evidence is immutable")
    report = audit(args.manifest, args.normalized, args.date)
    atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in (
        "target_dates", "receipt_counts", "raw_date_matches", "normalized_date_matches",
        "financial_or_training_adopted")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
