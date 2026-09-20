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
from stockagent.data.tw_exchange_price_classification import (
    VERIFIED_EMERGING_TO_TPEX_LISTINGS,
)


def audit(root: Path) -> dict:
    dataset = root / "data_tw_minute/research_dataset"
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
    return {
        "status": "needs_training_eligibility_fix" if admitted else "no_overlap_detected",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "manifest_sha256": sha256_file(manifest_path),
        "source_partitions_hashed": checked,
        "official_listing_dates": {
            symbol: day.isoformat()
            for symbol, day in VERIFIED_EMERGING_TO_TPEX_LISTINGS.items()
        },
        "by_symbol": dict(rows),
        "emerging_feature_and_label_valid": admitted,
        "meaning": "This is an admission audit, not a price-grid failure. Source bytes stay unchanged; training eligibility and normalizer need separately versioned remediation.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/data_quality/tw_price_precision/emerging_admission.json")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    result = audit(args.root)
    atomic_write_json(args.output, result)
    print(json.dumps({"status": result["status"], "rows": result["emerging_feature_and_label_valid"], "output": str(args.output)}))
    return 2 if args.strict and result["emerging_feature_and_label_valid"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
