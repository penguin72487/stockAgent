"""Check historical emerging-stock bars admitted by the minute training masks.

Price-grid validity does not prove a continuous-market execution model applies.
Keep source partitions unchanged and produce a separate admission receipt.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_listing_admission import (
    VERIFIED_EMERGING_TO_TPEX_LISTINGS,
    regular_market_admission_contract,
    regular_market_admission_mask,
)


def audit(root: Path, dataset: Path | None = None) -> dict:
    dataset = dataset or (root / "data_tw_minute/research_dataset")
    manifest_path = dataset / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows: dict[str, dict] = defaultdict(lambda: {
        "rows": 0,
        "feature_valid": 0,
        "label_valid_1m": 0,
        "feature_and_label_valid": 0,
        "examples": [],
    })
    checked = 0
    for entry in manifest.get("partitions", []):
        date_text = str(entry["trade_date"])
        eligible = {
            symbol: listing
            for symbol, listing in VERIFIED_EMERGING_TO_TPEX_LISTINGS.items()
            if date_text < listing.isoformat()
        }
        if not eligible:
            continue
        path = dataset / str(entry["output"])
        if not path.is_file() or sha256_file(path) != entry.get("output_sha256"):
            raise RuntimeError(f"minute partition hash mismatch: {path}")
        checked += 1
        frame = pl.read_parquet(
            path,
            columns=["symbol", "market", "ts", "feature_valid", "label_valid_1m"],
        ).filter(
            pl.col("market") == "tpex"
        ).with_columns(pl.col("symbol").cast(pl.String))
        for symbol in eligible:
            selected = frame.filter(pl.col("symbol") == symbol)
            record = rows[symbol]
            record["rows"] += selected.height
            record["feature_valid"] += int(selected["feature_valid"].sum() or 0)
            record["label_valid_1m"] += int(selected["label_valid_1m"].sum() or 0)
            overlapping = selected.filter(
                pl.col("feature_valid") & pl.col("label_valid_1m")
            )
            record["feature_and_label_valid"] += overlapping.height
            record["examples"].extend(
                {"date": date_text, "ts": str(ts)}
                for ts in overlapping["ts"].to_list()
            )
    admitted = sum(item["feature_and_label_valid"] for item in rows.values())
    effective_admitted = sum(
        bool(regular_market_admission_mask([symbol], example["date"])[0])
        for symbol, item in rows.items()
        for example in item["examples"]
    )
    return {
        "status": "training_admission_excludes_source_rows" if admitted and not effective_admitted else (
            "needs_training_eligibility_fix" if effective_admitted else "no_overlap_detected"
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_root": str(dataset.resolve()),
        "manifest_sha256": sha256_file(manifest_path),
        "source_partitions_hashed": checked,
        "official_listing_dates": {
            symbol: day.isoformat()
            for symbol, day in VERIFIED_EMERGING_TO_TPEX_LISTINGS.items()
        },
        "by_symbol": dict(rows),
        "emerging_feature_and_label_valid": admitted,
        "effective_emerging_feature_and_label_valid": effective_admitted,
        "training_admission_contract": regular_market_admission_contract(),
        "meaning": "Source masks retain the historical observations. Training admission excludes pre-listing rows from model features, labels and fitted normalization moments; raw bytes remain unchanged.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--dataset", type=Path, help="minute research dataset root (schema 4 or 5)")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/data_quality/tw_price_precision/emerging_admission.json")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    result = audit(args.root, args.dataset)
    atomic_write_json(args.output, result)
    print(json.dumps({
        "status": result["status"],
        "source_rows": result["emerging_feature_and_label_valid"],
        "effective_rows": result["effective_emerging_feature_and_label_valid"],
        "output": str(args.output),
    }))
    return 2 if args.strict and result["effective_emerging_feature_and_label_valid"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
