#!/usr/bin/env python3
"""Profile every public-feature column without constructing a model panel.

Non-null counts and date bounds describe observed values, not PIT eligibility or
per-symbol completeness.  The latter are separately proved by the training
audit and original-release receipts.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_tw_public_data_layer import audit_public_feature_table
from stockagent.data.tw_public_features import DEFAULT_MARKET_SYMBOL, FEATURE_COLUMNS


def _signature(path: Path) -> tuple[int, int, int]:
    stat = path.stat()
    return stat.st_ino, stat.st_size, stat.st_mtime_ns


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--feature-path", default="data_tw_public/features/tw_public_stock_daily.parquet"
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--source-schema", action="store_true",
        help="Inventory every twpub_* column in the selected research parquet instead of the canonical strict ABI.",
    )
    args = parser.parse_args()
    path = Path(args.feature_path).resolve()
    output_dir = Path(args.output_dir)
    before = _signature(path)
    feature_columns = (
        [name for name in pq.read_schema(path).names if name.startswith("twpub_")]
        if args.source_schema else list(FEATURE_COLUMNS)
    )
    stats, annual, findings = audit_public_feature_table(
        path, feature_columns, DEFAULT_MARKET_SYMBOL
    )
    if before != _signature(path):
        raise RuntimeError("feature table changed during inventory; retry on a stable release")
    rows = [{"feature": name, **stats.get(name, {})} for name in feature_columns]
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "feature_inventory.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["feature", "count", "symbols", "first", "last", "min", "max", "nonfinite"]
        )
        writer.writeheader()
        writer.writerows(rows)
    with (output_dir / "annual_feature_inventory.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["year", "feature", "raw_non_null", "stock_rows", "market_rows"]
        )
        writer.writeheader()
        writer.writerows(annual)
    summary = {
        "feature_path": str(path),
        "file_signature": {"inode": before[0], "size": before[1], "mtime_ns": before[2]},
        "feature_count": len(feature_columns),
        "with_values": sum(bool(row.get("count")) for row in rows),
        "all_null": [row["feature"] for row in rows if not row.get("count")],
        "findings": [finding.__dict__ for finding in findings],
        "warning": "Observed history is not proof of original vintage or strict training eligibility.",
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"feature_count": summary["feature_count"], "with_values": summary["with_values"], "all_null": summary["all_null"], "findings": len(findings)}, ensure_ascii=False))
    if findings:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
