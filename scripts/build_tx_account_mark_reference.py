#!/usr/bin/env python3
"""Stage dated TX settlement marks for a future source-bound margin benchmark.

The result is a research-only valuation input, not a price at which an order
can be executed, a complete account replay, or a training-admission receipt.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import sha256_file  # noqa: E402
from scripts import audit_tx_daily_margin_marks as mark_audit  # noqa: E402


def build(reference: pl.DataFrame, daily: pl.DataFrame) -> pl.DataFrame:
    review = mark_audit.audit(reference, daily)
    if review["candidate_market_sessions"] == 0 or review["invalid_or_missing_own_contract_marks"]:
        raise ValueError("TX account marks require a reviewed observed own-contract mark every candidate day")
    tx = daily.filter((pl.col("product") == "TX") & ~pl.col("accounting_only_date"))
    result = (reference.select("date", "contract", "front_month_roll", "observed_front_close",
                               "cash_settlement_event", "final_settlement_price")
              .join(tx.select("date", "contract", "official_settlement"),
                    on=["date", "contract"], how="left", validate="1:1")
              .with_columns(pl.col("official_settlement").cast(pl.Float64, strict=False)
                            .alias("official_daily_settlement"))
              .with_columns(pl.when(pl.col("cash_settlement_event"))
                            .then(pl.col("final_settlement_price"))
                            .otherwise(pl.col("official_daily_settlement"))
                            .alias("account_settlement_mark"))
              .drop("official_settlement").sort("date"))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--daily", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("preserve earlier data; use a new dataset directory")
    reference = pl.read_parquet(args.reference)
    daily = (pl.scan_parquet(args.daily).filter(pl.col("product") == "TX")
             .select("date", "product", "contract", "accounting_only_date", "official_settlement",
                     "settlement", "daily_mark", "observed_close", "official_row_observed",
                     "source_row_observed", "cash_settlement", "final_settlement_price").collect())
    result = build(reference, daily)
    args.output.mkdir(parents=True)
    table = args.output / "daily_account_marks.parquet"
    result.write_parquet(table)
    manifest = {"contract": "tx_daily_account_settlement_mark_research_v1",
                "observed_at_utc": datetime.now(UTC).isoformat(),
                "start_date": str(result["date"][0]), "end_date": str(result["date"][-1]),
                "market_sessions": result.height,
                "final_settlement_sessions": result.filter(pl.col("cash_settlement_event")).height,
                "daily_settlement_sessions": result.filter(~pl.col("cash_settlement_event")).height,
                "data_sha256": sha256_file(table),
                "inputs_sha256": {str(p): sha256_file(p) for p in
                                  (args.reference, args.daily, Path(__file__), Path(mark_audit.__file__))},
                "research_only": True, "historical_point_in_time": False,
                "one_contract_benchmark_admitted": False, "training_admitted": False,
                "limitations": ["Marks are not trade prices; entry and roll observations are separate.",
                                "Account-only dates, broker funding, fee/tax and intraday margin are not modeled.",
                                "Dated source availability and own-contract continuity require further verification."]}
    (args.output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
