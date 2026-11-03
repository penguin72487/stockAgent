#!/usr/bin/env python3
"""Registerable FinLab candidate partitions for a source-bound TX benchmark.

The plan requests real contract-days. A query registration, full-day price or
minute candle does not itself prove a broker fill, historical availability,
clearing time or training readiness.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import re
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import sha256_file  # noqa: E402


def candidate_items(settlement: pl.DataFrame, raw: pl.DataFrame) -> tuple[list[dict], dict]:
    """Select the next physical month on each expiry and each year's first day."""
    necessary = {"date", "contract", "front_month_roll", "cash_settlement_event"}
    if not necessary <= set(settlement.columns):
        raise ValueError("candidate reference lacks expiry, identity or roll fields")
    settlement = settlement.sort("date")
    rows = settlement.select(sorted(necessary)).to_dicts()
    if not rows:
        raise ValueError("candidate reference is empty")
    observed: set[tuple[object, str]] = set()
    seen: set[tuple[object, str]] = set()
    for row in raw.filter(pl.col("product") == "TX").select(
        "date", "contract", "source_row_observed", "close"
    ).to_dicts():
        key = (row["date"], row["contract"])
        if key in seen:
            raise ValueError("duplicate observed TX contract-day")
        seen.add(key)
        if row["source_row_observed"] is True and row["close"] is not None and row["close"] > 0:
            observed.add(key)
    items: set[tuple[str, str]] = set()
    years: set[int] = set()
    roll_count = 0
    for i, row in enumerate(rows):
        day, contract = row["date"], row["contract"]
        if day is None or not isinstance(contract, str) or not re.fullmatch(r"\d{6}", contract):
            raise ValueError("candidate has invalid TX contract identity")
        if not isinstance(row["front_month_roll"], bool) or not isinstance(row["cash_settlement_event"], bool):
            raise ValueError("candidate requires explicit boolean roll and settlement fields")
        if i and day <= rows[i - 1]["date"]:
            raise ValueError("candidate session dates must be unique and increasing")
        if i and bool(row["front_month_roll"]) != (contract != rows[i - 1]["contract"]):
            raise ValueError("candidate roll flags disagree with physical contracts")
        if day.year not in years:
            years.add(day.year)
            if (day, contract) not in observed:
                raise ValueError("candidate yearly entry lacks an observed contract-day")
            items.add((contract, day.isoformat()))
        if row["cash_settlement_event"]:
            if i + 1 == len(rows):
                raise ValueError("last candidate expiry lacks a next session")
            next_row = rows[i + 1]
            next_contract = next_row["contract"]
            if (not bool(next_row["front_month_roll"]) or next_contract == contract
                    or (day, next_contract) not in observed):
                raise ValueError("candidate expiry lacks an observed next-month contract-day")
            items.add((next_contract, day.isoformat()))
            roll_count += 1
    actual_rolls = sum(bool(row["front_month_roll"]) for row in rows[1:])
    if roll_count != actual_rolls:
        raise ValueError("candidate roll lacks a same-day cash-settlement event")
    ordered = [{"dataset": f"tw_futures_minute:TX_{month}", "partition": day}
               for month, day in sorted(items, key=lambda item: (item[1], item[0]))]
    return ordered, {"settlement_reopen_candidate_days": roll_count,
                     "yearly_first_entry_candidate_days": len(years),
                     "unique_contract_days": len(ordered), "years": sorted(years)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settlement-reference", required=True, type=Path)
    parser.add_argument("--raw-reference", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise FileExistsError("preserve existing plan; choose a new output directory")
    items, count = candidate_items(pl.read_parquet(args.settlement_reference),
                                   pl.read_parquet(args.raw_reference))
    now = datetime.now(UTC)
    plan = {"contract": "provider_priority_query_v1", "provider": "finlab",
            "reason": "TX one-contract benchmark: independently inspect the next physical month on each old-month expiry and each year's first market session; validate source clocks, dated rules, final settlement, margin and executable price separately.",
            "created_at_utc": now.isoformat(), "expires_at_utc": (now + timedelta(days=6)).isoformat(),
            "items": items}
    proof = {"contract": "tx_settlement_reopen_priority_plan_v1", **count,
             "inputs_sha256": {str(path): sha256_file(path) for path in
                               (args.settlement_reference, args.raw_reference, Path(__file__))},
             "provider_calls": 0, "historical_pit_verified": False,
             "benchmark_admitted": False, "training_admitted": False,
             "note": "Candidate partitions are not obtained minutes or broker fills; clearing times and reserved margins require dated source review."}
    args.output_dir.mkdir(parents=True)
    (args.output_dir / "plan.json").write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n")
    (args.output_dir / "evidence.json").write_text(json.dumps(proof, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(proof, ensure_ascii=False))


if __name__ == "__main__":
    main()
