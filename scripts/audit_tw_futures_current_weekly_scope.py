#!/usr/bin/env python3
"""Extend the existing source-pinned monthly audit with weekly deliveries.

The exchange's dated report is an observation, not a listing, fill or
training-admission certificate. Never combine different snapshot generations.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json
from scripts.audit_tw_futures_current_scope import (
    DEFAULT_INVENTORY, DEFAULT_SNAPSHOT, ROOT, compare_snapshot, stable_digest,
)
from scripts.audit_tw_futures_forward_scope import REVIEW


def compare_weekly(snapshot: pl.DataFrame, inventory: pl.DataFrame, market_day: date) -> dict:
    monthly = compare_snapshot(snapshot, inventory, market_day)
    rows = snapshot.filter(
        (pl.col("Date") == market_day.strftime("%Y%m%d"))
        & (pl.col("TradingSession") == "一般")
        & (pl.col("_downloaded_at_utc") == monthly["snapshot_downloaded_at_utc"])
        & pl.col("ContractMonth(Week)").str.contains(r"^\d{6}W[1-5]$")
    )
    if rows.height:
        if (rows["Contract"].null_count() or rows["_payload_sha256"].null_count()
                or rows.select("Contract", "ContractMonth(Week)").is_duplicated().any()
                or set(rows["_payload_sha256"]) != {monthly["snapshot_raw_payload_sha256"]}):
            raise ValueError("weekly contracts have ambiguous identity or a different raw snapshot")
    rows = rows.with_columns(
        pl.col("Volume").str.replace_all(",", "").cast(pl.Int64, strict=False).alias("volume_value")
    )
    if rows.filter(pl.col("volume_value") < 0).height:
        raise ValueError("weekly contract volume is negative")
    inventory_by_code = {item["product"]: item for item in inventory.iter_rows(named=True)}
    weekly = []
    for row in rows.sort("Contract", "ContractMonth(Week)").iter_rows(named=True):
        code = row["Contract"]
        known = inventory_by_code.get(code)
        classification, currency, _ = REVIEW.get(code, ("unreviewed", None, None))
        weekly.append({"product": code, "delivery": row["ContractMonth(Week)"],
                       "volume": row["volume_value"],
                       "settlement_currency": known["settlement_currency"] if known else currency,
                       "scope": "historical_inventory" if known else classification})
    monthly_codes = {item["product"] for item in monthly["products"]}
    weekly_codes = {item["product"] for item in weekly}
    return {
        "market_day": market_day.isoformat(),
        "snapshot_downloaded_at_utc": monthly["snapshot_downloaded_at_utc"],
        "snapshot_raw_payload_sha256": monthly["snapshot_raw_payload_sha256"],
        "reported_monthly_contract_rows": monthly["reported_monthly_contract_rows"],
        "reported_weekly_contract_rows": len(weekly),
        "reported_outright_contract_rows": monthly["reported_monthly_contract_rows"] + len(weekly),
        "monthly_products": len(monthly_codes), "weekly_products": len(weekly_codes),
        "combined_products": len(monthly_codes | weekly_codes),
        "weekly_only_products": sorted(weekly_codes - monthly_codes),
        "weekly_contracts": weekly,
    }


def audit(snapshot: Path, inventory: Path, market_day: date) -> dict:
    paths = [snapshot, inventory, Path(__file__),
             ROOT / "scripts/audit_tw_futures_current_scope.py",
             ROOT / "scripts/audit_tw_futures_forward_scope.py"]
    pins = {str(path): stable_digest(path) for path in paths}
    result = compare_weekly(pl.read_parquet(snapshot), pl.read_csv(inventory, infer_schema_length=0), market_day)
    if any(stable_digest(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("source changed during current weekly scope audit")
    return {
        "contract": "taifex_current_futures_monthly_weekly_source_audit_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), **result,
        "input_sha256": pins, "financial_or_training_adopted": False,
        "limitations": [
            "One general-session daily snapshot is not the complete exchange listing or account permission.",
            "Inventory currency describes settlement; quote currency requires separate proof.",
            "Volume is a daily market observation, not a decision-window execution or broker fill.",
            "A current source snapshot is not a point-in-time historical admission certificate.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-day", type=date.fromisoformat, required=True)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--inventory", type=Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to((ROOT / "artifacts/markets").resolve()) or output.exists() or output.is_symlink():
        parser.error("output must be a new path under artifacts/markets")
    result = audit(args.snapshot.resolve(), args.inventory.resolve(), args.market_day)
    atomic_write_json(output, result)
    print(f"{output}: {result['reported_monthly_contract_rows']} monthly, "
          f"{result['reported_weekly_contract_rows']} weekly")


if __name__ == "__main__":
    main()
