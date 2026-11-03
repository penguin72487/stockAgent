#!/usr/bin/env python3
"""Materialize review-only prior-market context candidates with source hashes.

The resulting rows are not a trading release and cannot authorize training.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import inspect
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_portfolio_daily import FUTURES_MODEL_FEATURE_COLUMNS
from stockagent.data.tw_futures_prior_context import (
    PRIOR_CONTEXT_CANDIDATE_CONTRACT, recover_prior_market_context_candidates,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frame", required=True, type=Path)
    parser.add_argument("--release", required=True, type=Path)
    parser.add_argument("--panel-dates", required=True, type=Path)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    if args.start > args.end:
        raise ValueError("invalid requested decision interval")
    inputs = {str(p.resolve()): sha256_file(p) for p in (args.frame, args.release, args.panel_dates)}
    release_columns = ["date", "symbol", "physical_contract", "accounting_only_date",
                       "same_contract_as_previous_session"]
    original_columns = ["date", "symbol", "physical_contract", "accounting_only_date",
                        "official_row_observed", "valuation_research_applied", "official_settlement",
                        "settlement", "underlying_symbol", *FUTURES_MODEL_FEATURE_COLUMNS]
    release = pl.read_parquet(args.release, columns=release_columns)
    original = pl.read_parquet(args.frame, columns=original_columns)
    panel_dates = np.load(args.panel_dates, allow_pickle=False)
    candidates = recover_prior_market_context_candidates(panel_dates, release, original).filter(
        pl.col("decision_date").is_between(args.start, args.end))
    if any(sha256_file(p) != digest for p, digest in inputs.items()):
        raise ValueError("prior context input changed during collection")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    output = args.output_dir / "prior_market_context_candidates.parquet"
    atomic_write_parquet(output, candidates)
    source_code = Path(inspect.getfile(recover_prior_market_context_candidates)).resolve()
    payload = {
        "contract": PRIOR_CONTEXT_CANDIDATE_CONTRACT,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision_start": args.start.isoformat(), "decision_end": args.end.isoformat(),
        "inputs_sha256": inputs,
        "implementation_sha256": sha256_file(source_code),
        "builder_sha256": sha256_file(Path(__file__)),
        "candidate_rows": candidates.height,
        "unique_physical_contracts": candidates["physical_contract"].n_unique(),
        "unique_fixed_slots": candidates["symbol"].n_unique(),
        "outputs": {"prior_market_context_candidates.parquet": sha256_file(output)},
        "research_only": True,
        "point_in_time_listing_verified": False,
        "financial_sources_admitted": False,
        "training_admitted": False,
        "account_events_created": False,
        "limitations": [
            "Feature-source candidates only: no account row, trade, fill or training permission is created.",
            "Historical listing and publication clocks and retrospective parent-release selection require separate review.",
            "An observed prior official settlement and matching physical generation do not prove current order capacity.",
            "The source carries original missing feature values; it does not construct prices or fill missing observations.",
        ],
    }
    atomic_write_json(args.output_dir / "manifest.json", payload)
    print(f"candidate_rows={candidates.height} unique_physical_contracts={payload['unique_physical_contracts']}")
    print(f"training_admitted=false output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
