#!/usr/bin/env python3
"""Compare one retained TAIFEX daily snapshot with the reviewed futures scope.

This is a source-identity and coverage audit, not an instrument, execution,
financial-accounting or training admission.  The official market date is
``Date``; ``date``/``_as_of_date`` describe the later capture instead.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file
from scripts.audit_tw_futures_forward_scope import REVIEW


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SNAPSHOT = ROOT / "data_tw_public/taifex_daily_futures.parquet"
DEFAULT_INVENTORY = (ROOT / "artifacts/markets/tw_futures_universe765_recovery_20261027_v1"
                     / "source_inventory.csv")
CONTRACT = "taifex_current_futures_scope_source_audit_v1"


def stable_digest(path: Path) -> str:
    before = path.stat()
    result = sha256_file(path)
    after = path.stat()
    signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if signature(before) != signature(after):
        raise ValueError(f"source changed while hashing: {path}")
    return result


def compare_snapshot(snapshot: pl.DataFrame, inventory: pl.DataFrame, market_day: date) -> dict:
    required = {"Date", "Contract", "ContractMonth(Week)", "TradingSession", "Volume",
                "_downloaded_at_utc", "_payload_sha256"}
    if required - set(snapshot.columns):
        raise ValueError("snapshot lacks required market or provenance columns")
    if {"product", "product_name", "asset_class", "settlement_currency"} - set(inventory.columns):
        raise ValueError("inventory lacks required identity or currency columns")
    if (inventory["product"].null_count() or inventory["product"].n_unique() != len(inventory)
            or inventory.filter(pl.col("settlement_currency").is_null()).height):
        raise ValueError("inventory identity or currency is ambiguous")
    day_rows = snapshot.filter(
        (pl.col("Date") == market_day.strftime("%Y%m%d"))
        & (pl.col("TradingSession") == "一般")
        & pl.col("ContractMonth(Week)").str.contains(r"^\d{6}$")
    )
    if not day_rows.height or day_rows["_downloaded_at_utc"].null_count():
        raise ValueError("requested market day has no provenance-complete general-session rows")
    capture = day_rows["_downloaded_at_utc"].max()
    day_rows = day_rows.filter(pl.col("_downloaded_at_utc") == capture)
    digests = day_rows["_payload_sha256"].unique().to_list()
    if len(digests) != 1 or not isinstance(digests[0], str) or len(digests[0]) != 64:
        raise ValueError("snapshot must have exactly one retained raw payload identity")
    if (day_rows["Contract"].null_count() or day_rows.select("Contract", "ContractMonth(Week)")
            .is_duplicated().any()):
        raise ValueError("snapshot contains ambiguous contract/month keys")
    day_rows = day_rows.with_columns(
        pl.col("Volume").str.replace_all(",", "").cast(pl.Int64, strict=False).alias("volume_value")
    )
    if day_rows.filter(pl.col("volume_value") < 0).height:
        raise ValueError("snapshot contains negative volumes")
    lookup = {r["product"]: r for r in inventory.iter_rows(named=True)}
    products = []
    for group in day_rows.partition_by("Contract", maintain_order=True):
        code = group["Contract"][0]
        known = lookup.get(code)
        classification, reviewed_currency, reference = REVIEW.get(code, ("unreviewed", None, None))
        if known is not None:
            currency = known["settlement_currency"]
            scope = "historical_inventory"
            asset_class = known["asset_class"]
            name = known["product_name"]
        else:
            currency = reviewed_currency
            scope = classification
            asset_class = None
            name = None
        volumes = group["volume_value"].to_list()
        products.append({
            "product": code, "product_name": name, "asset_class": asset_class,
            "settlement_currency": currency, "scope": scope,
            "review_reference": reference if known is None else None,
            "monthly_contract_rows": len(group),
            "positive_volume_contracts": sum(v is not None and v > 0 for v in volumes),
            "explicit_zero_volume_contracts": sum(v == 0 for v in volumes),
            "missing_or_invalid_volume_contracts": sum(v is None for v in volumes),
            "months": sorted(group["ContractMonth(Week)"].to_list()),
        })
    products.sort(key=lambda x: x["product"])
    twd = [p for p in products if p["settlement_currency"] == "TWD"]
    return {
        "market_day": market_day.isoformat(), "snapshot_downloaded_at_utc": capture,
        "snapshot_raw_payload_sha256": digests[0],
        "scope": "general-session outright monthly contract rows in retained daily report",
        "catalogue_products": len(inventory), "reported_monthly_contract_rows": len(day_rows),
        "reported_products": len(products), "reported_twd_settlement_products": len(twd),
        "twd_settlement_products_with_positive_daily_volume": sum(p["positive_volume_contracts"] > 0 for p in twd),
        "unreviewed_codes": [p["product"] for p in products if p["scope"] == "unreviewed"],
        "outside_catalogue": [p["product"] for p in products if p["scope"] != "historical_inventory"],
        "products": products,
    }


def audit(snapshot_path: Path, inventory_path: Path, market_day: date) -> dict:
    paths = [snapshot_path, inventory_path, Path(__file__), ROOT / "scripts/audit_tw_futures_forward_scope.py"]
    pins = {str(path): stable_digest(path) for path in paths}
    snapshot = pl.read_parquet(snapshot_path)
    inventory = pl.read_csv(inventory_path, infer_schema_length=0)
    result = compare_snapshot(snapshot, inventory, market_day)
    if any(stable_digest(path) != pins[str(path)] for path in paths):
        raise ValueError("source changed during audit")
    return {
        "contract": CONTRACT, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        **result, "input_sha256": pins,
        "limitations": [
            "A reported monthly product is not a complete listing or trading-permission inventory.",
            "The historical inventory records settlement currency, not separately verified quote currency.",
            "Daily volume is not a decision-window price, liquidity guarantee or broker fill.",
            "Capture after the session does not prove information was available at the decision clock.",
            "Adjusted physical contracts, margin, lifecycle and transfer require separate evidence.",
            "This audit neither publishes data nor admits products to training.",
        ],
        "financial_or_training_adopted": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-day", required=True, type=date.fromisoformat)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--inventory", type=Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    target = args.output.resolve()
    if not target.is_relative_to((ROOT / "artifacts/markets").resolve()):
        parser.error("output must be in artifacts/markets")
    result = audit(args.snapshot.resolve(), args.inventory.resolve(), args.market_day)
    with target.open("x", encoding="utf-8") as out:
        json.dump(result, out, ensure_ascii=False, indent=2)
        out.write("\n")
    print(f"{target}: {result['reported_products']} products; "
          f"{len(result['unreviewed_codes'])} unreviewed")


if __name__ == "__main__":
    main()
