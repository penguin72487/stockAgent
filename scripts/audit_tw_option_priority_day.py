#!/usr/bin/env python3
"""Compare one receipted TAIFEX option day with the existing FinMind object.

The result is source triage, not an executable quote, a PIT guarantee, or an
admission to an options account. No provider is called by this script.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_contract_presence import KEYS, official_year
from scripts.audit_taifex_option_market_daily_contracts import audit_day


def audit(day: date, official: Path, root: Path, output: Path) -> dict:
    official, root, output = official.resolve(), root.resolve(), output.resolve()
    if output.exists() or not output.is_relative_to((ROOT / "artifacts/markets").resolve()):
        raise ValueError("select a new output within artifacts/markets")
    official_receipt = official.with_suffix(".receipt.json")
    vendor_receipt = root / "receipts/TaiwanOptionDaily/all" / f"{day}.json"
    source = json.loads(official_receipt.read_bytes())
    vendor_bytes = vendor_receipt.read_bytes()
    vendor = json.loads(vendor_bytes)
    if (source.get("contract") != "taifex_option_all_products_daily_raw_v1"
            or source.get("range_start") != day.isoformat()
            or source.get("range_end") != day.isoformat()
            or source.get("point_in_time_snapshot") is not False
            or Path(source.get("path", "")).resolve() != official
            or source.get("bytes") != official.stat().st_size
            or source.get("sha256") != sha256_file(official)):
        raise ValueError("official source or native receipt changed")
    if (vendor.get("dataset") != "TaiwanOptionDaily" or vendor.get("data_id") != ""
            or vendor.get("partition") != day.isoformat() or vendor.get("status") != "complete"):
        raise ValueError("FinMind source receipt identity mismatch")
    data = root / vendor["parquet_path"]
    paths = [official, official_receipt, vendor_receipt, data, Path(__file__).resolve(),
             ROOT / "scripts/audit_taifex_option_contract_presence.py",
             ROOT / "scripts/audit_taifex_option_market_daily_contracts.py",
             ROOT / "downloader/finmind_option_contract_keys.py"]
    pins = {str(p): sha256_file(p) for p in paths}
    with official.open(encoding="cp950", newline="") as stream:
        rows = list(csv.DictReader(stream))
    identities = [(r["契約"], r["到期月份(週別)"], r["履約價"], r["買賣權"], r["交易時段"]) for r in rows]
    if (not rows or any(r["交易日期"] != day.strftime("%Y/%m/%d") for r in rows)
            or len(identities) != len(set(identities))):
        raise ValueError("official source has ambiguous session identities or day")
    official_days, stats = official_year(official, source["sha256"], day.year)
    if set(official_days) != {day.isoformat()}:
        raise ValueError("official supported rows do not match requested day")
    selected = official_days[day.isoformat()]
    row = {"date": day.isoformat(), "source_status": "daily_readback_verified",
           "receipt_sha256": hashlib.sha256(vendor_bytes).hexdigest(),
           "source_kind": vendor["kind"], "parquet_sha256": vendor["sha256"]}
    result, missing = audit_day(row, root, selected)
    if result["source_status"] != "daily_contract_readback_verified":
        raise ValueError(f"FinMind independent readback failed: {result}")
    vendor_frame = pl.scan_parquet(data).select("option_id", "volume").collect()
    official_products = set(selected["product"].unique().to_list())
    positive_products = set(vendor_frame.filter(pl.col("volume") > 0)["option_id"].to_list())
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("source changed during the readback")
    report = {
        "contract": "tw_option_priority_day_source_reconciliation_v1",
        "observed_at_utc": datetime.now(UTC).isoformat(), "market_day": day.isoformat(),
        "input_sha256": pins, "official_raw_rows": len(rows),
        "official_raw_products": len({r["契約"] for r in rows}),
        "official_supported_positive_contract_keys": selected.select(KEYS).unique().height,
        "official_supported_positive_products": sorted(official_products),
        "official_parser_stats": stats, "finmind_comparison": result,
        "finmind_missing_positive_products": sorted(official_products - positive_products),
        "missing_positive_contracts": missing, "provider_calls": 0,
        "financial_or_training_admitted": False,
        "limitations": [
            "Daily observations are not decision-clock prices or broker fills.",
            "Contract comparison uses float32 strike precision and normalized series.",
            "Natural-day vendor sessions and exchange sessions can differ.",
            "Later official capture and earlier FinMind capture can differ; cause is not established.",
            "Neither source readback proves historical point-in-time availability.",
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    parser.add_argument("--official", required=True, type=Path)
    parser.add_argument("--finmind-root", default=ROOT / "data_finmind/sponsor", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = audit(args.day, args.official, args.finmind_root, args.output)
    print(json.dumps({"output": str(args.output), "comparison": result["finmind_comparison"],
                      "missing_products": result["finmind_missing_positive_products"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
