#!/usr/bin/env python3
"""Compare retained TX daily settlement, close and final-settlement marks.

This diagnostic does not establish source PIT, fill prices, broker treatment,
or admit a benchmark for training.
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


def audit(reference: pl.DataFrame, daily: pl.DataFrame) -> dict:
    """Compare each candidate's own-month retained official settlement/close."""
    ref = reference.select("date", "contract", "observed_front_close", "cash_settlement_event",
                           pl.col("final_settlement_price").alias("reference_final_settlement"))
    tx = daily.filter((pl.col("product") == "TX") & ~pl.col("accounting_only_date"))
    for frame, keys in ((ref, ["date"]), (tx, ["date", "contract"])):
        if frame.select(pl.struct(keys).is_duplicated().any()).item():
            raise ValueError("ambiguous TX daily mark identity")
    rows = ref.join(tx.select("date", "contract", "official_settlement", "settlement", "daily_mark",
                              "observed_close", "official_row_observed", "source_row_observed",
                              "cash_settlement", "final_settlement_price"),
                    on=["date", "contract"], how="left", validate="1:1")
    settlement = pl.col("official_settlement").cast(pl.Float64, strict=False)
    base_valid = ((pl.col("official_row_observed") == True) & (pl.col("source_row_observed") == True)
             & pl.col("observed_close").is_finite() & (pl.col("observed_close") > 0)
             & (pl.col("observed_close") == pl.col("observed_front_close"))).fill_null(False)
    expiry = pl.col("cash_settlement_event")
    regular_valid = ~expiry & settlement.is_finite() & (settlement > 0)
    expiry_valid = (expiry & (pl.col("cash_settlement") == True)
                    & pl.col("final_settlement_price").is_finite()
                    & (pl.col("final_settlement_price") > 0)
                    & (pl.col("final_settlement_price") == pl.col("reference_final_settlement")))
    valid = (base_valid & (regular_valid | expiry_valid)).fill_null(False)
    rows = rows.with_columns(settlement.alias("official_settlement_numeric"), valid.alias("valid"))
    mismatch = (pl.col("observed_front_close") != pl.col("official_settlement_numeric"))
    nonexpiry = ~pl.col("cash_settlement_event")
    valid_rows = rows.filter(pl.col("valid"))
    mismatched = valid_rows.filter(nonexpiry & mismatch)
    examples = mismatched.select("date", "contract", "observed_front_close", "official_settlement_numeric",
                                 "settlement", "daily_mark").head(5).to_dicts()
    diff = (pl.col("official_settlement_numeric") - pl.col("observed_front_close")).abs().max()
    return {"contract": "tx_daily_margin_mark_diagnostic_v2", "candidate_market_sessions": rows.height,
            "valid_own_contract_marks": valid_rows.height,
            "invalid_or_missing_own_contract_marks": rows.height - valid_rows.height,
            "valid_nonexpiry_official_daily_settlements": valid_rows.filter(nonexpiry).height,
            "valid_expiry_final_settlements": valid_rows.filter(pl.col("cash_settlement_event")).height,
            "nonexpiry_observed_close_differs_from_official_settlement": mismatched.height,
            "nonexpiry_max_absolute_points_difference": mismatched.select(diff).item() if mismatched.height else 0.0,
            "nonexpiry_difference_examples": examples,
            "one_contract_benchmark_admitted": False, "training_admitted": False,
            "limitations": ["Daily settlement marks are not executable trades.",
                            "Account processing, source publication times and intraday margin remain unverified."]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--daily", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("preserve earlier audit; choose a new path")
    reference = pl.read_parquet(args.reference)
    daily = (pl.scan_parquet(args.daily).filter(pl.col("product") == "TX")
             .select("date", "product", "contract", "accounting_only_date", "official_settlement",
                     "settlement", "daily_mark", "observed_close", "official_row_observed",
                     "source_row_observed", "cash_settlement", "final_settlement_price").collect())
    result = audit(reference, daily)
    result["observed_at_utc"] = datetime.now(UTC).isoformat()
    result["inputs_sha256"] = {str(p): sha256_file(p) for p in (args.reference, args.daily, Path(__file__))}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
