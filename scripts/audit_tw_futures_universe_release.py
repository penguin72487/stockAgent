#!/usr/bin/env python3
"""Read a versioned all-TWD release by product, date and execution window.

An observed print is source evidence, not an order fill or a claim that all
products were simultaneously available. This tool never modifies a release.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, sha256_file

KEYS = ["date", "physical_contract"]


def inventory_scope(catalog: pl.DataFrame, requested: list[str]) -> tuple[list[str], list[str]]:
    """Return omitted and extra product identities without guessing listing dates."""
    if (catalog.is_empty() or catalog["product"].null_count() or not requested
            or catalog.filter(pl.col("product").str.strip_chars() == "").height
            or any(not isinstance(code, str) or not code.strip() for code in requested)
            or catalog["product"].is_duplicated().any() or len(requested) != len(set(requested))):
        raise ValueError("unique nonempty product identities required")
    source, selected = set(catalog["product"].to_list()), set(requested)
    return sorted(source - selected), sorted(selected - source)


def validate_clock_join(facts: pl.DataFrame, entries: pl.DataFrame) -> pl.DataFrame:
    """Validate the physical generation and slot, including zero-fill days."""
    if facts.select(KEYS).is_duplicated().any() or entries.select(KEYS).is_duplicated().any():
        raise ValueError("duplicated physical contract/day")
    if entries.height != facts.height:
        raise ValueError("clock and financial physical/slot axes differ")
    joined = facts.join(entries, on=KEYS, how="left", validate="1:1")
    if (joined.height != facts.height or joined["status"].null_count() or joined["clock_symbol"].null_count()
            or joined["symbol"].null_count()
            or joined.filter(pl.col("symbol") != pl.col("clock_symbol")).height):
        raise ValueError("clock and financial physical/slot axes differ")
    return joined


def audit(inventory: Path, inventory_summary: Path, frozen: Path, output: Path,
          start: date, end: date) -> dict:
    if start > end or output.exists() or output.is_symlink():
        raise ValueError("ordered dates and a new evidence output are required")
    summary = json.loads(inventory_summary.read_text())
    review = json.loads(frozen.read_text())
    if review.get("contract") != "tw_futures_v18_source_pins_readback_v1":
        raise ValueError("expected the reviewed v18 release")
    expected_inventory = summary.get("formal_outputs", {}).get(str(inventory.relative_to(ROOT)))
    if expected_inventory != sha256_file(inventory):
        raise ValueError("historical product inventory differs from its receipt")
    pins = review["source_pins_verified"]
    daily_paths = [Path(p) for p in pins if p.endswith("/continuous_daily.parquet")]
    clock_paths = [Path(p) for p in pins if p.endswith("/entries.parquet")]
    if len(daily_paths) != 1 or len(clock_paths) != 4:
        raise ValueError("one daily source and four clocks required")
    daily_path = daily_paths[0]
    daily_manifest = daily_path.with_name("manifest.json")
    watched = [inventory, inventory_summary, frozen, daily_path, daily_manifest,
               *clock_paths, Path(__file__)]
    hashes = {str(path.resolve()): sha256_file(path) for path in watched}
    for path in [daily_path, *clock_paths]:
        if hashes[str(path.resolve())] != pins[str(path)]:
            raise ValueError("frozen release hash differs")
    manifest = json.loads(daily_manifest.read_text())
    if manifest.get("outputs", {}).get("continuous_daily", {}).get("sha256") != pins[str(daily_path)]:
        raise ValueError("daily release manifest does not bind the frozen source")
    catalog = pl.read_csv(inventory, columns=["product", "product_name", "asset_class",
        "source_start", "source_end", "source_status"], infer_schema=False)
    omitted, added = inventory_scope(catalog, manifest["requested_products"])
    facts = (pl.scan_parquet(daily_path).filter(pl.col("date").is_between(start, end)).select(
        *KEYS, "product", "symbol", "asset_class", "volume", "executable").collect())
    if facts.select(KEYS).is_duplicated().any():
        raise ValueError("duplicate financial physical contract/day")
    daily = facts.group_by("product").agg(
        pl.col("asset_class").drop_nulls().unique().alias("release_asset_classes"),
        pl.col("date").min().alias("first_release_date"), pl.col("date").max().alias("last_release_date"),
        ((pl.col("volume") > 0) & pl.col("executable")).fill_null(False).sum().alias("positive_daily_rows"))
    per_product = catalog.join(daily, on="product", how="left", validate="1:1")
    window_checks = {}
    for path in clock_paths:
        clock = path.parent.name + ("_300m" if "entry_window_300m" in path.parts else "_1m")
        if clock in window_checks or clock not in {"0845_1m", "0900_1m", "0845_300m", "0900_300m"}:
            raise ValueError("ambiguous decision-window identity")
        entries = (pl.scan_parquet(path).filter(pl.col("date").is_between(start, end))
                   .select(*KEYS, pl.col("symbol").alias("clock_symbol"), "status").collect())
        joined = validate_clock_join(facts, entries)
        good = joined.filter((pl.col("status") == "observed_minute") & pl.col("executable"))
        counts = good.group_by("product").agg(pl.len().alias(clock))
        per_product = per_product.join(counts, on="product", how="left", validate="1:1")
        window_checks[clock] = dict(
            positive_observation_contract_days=good.height, observed_products=good["product"].n_unique(),
            quarantined_contract_days=joined.filter(pl.col("status") == "unverified_execution_quarantined").height)
    class_counts = (per_product.group_by("asset_class").agg(
        pl.len().alias("catalog_products"),
        (pl.col("positive_daily_rows").fill_null(0) > 0).sum().alias("products_with_positive_daily"),
        *[(pl.col(c).fill_null(0) > 0).sum().alias(c + "_products") for c in sorted(window_checks)])
        .sort("asset_class").to_dicts())
    rows = per_product.sort("product").to_dicts()
    for row in rows:
        for key, value in row.items():
            if isinstance(value, date):
                row[key] = value.isoformat()
    if any(sha256_file(Path(path)) != digest for path, digest in hashes.items()):
        raise ValueError("source changed during scope readback")
    report = dict(contract="tw_futures_universe_release_scope_v1", observed_at_utc=datetime.now(UTC).isoformat(),
        input_sha256=hashes, window=[str(start), str(end)], catalog_products=catalog.height,
        requested_release_products=len(manifest["requested_products"]), omitted_catalog_products=omitted,
        added_release_products=added, products_with_positive_daily=sum(
            (r["positive_daily_rows"] or 0) > 0 for r in rows), categories=class_counts,
        window_checks=window_checks, products=rows, source_point_in_time_verified=False,
        product_union_is_not_simultaneous_availability=True, actual_orders_or_fills=False,
        financial_or_training_complete=False)
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--inventory-summary", type=Path, required=True)
    parser.add_argument("--frozen", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.inventory.resolve(), args.inventory_summary.resolve(), args.frozen.resolve(),
                   args.output.resolve(), args.start, args.end)
    print(json.dumps({k: result[k] for k in ("catalog_products", "requested_release_products",
          "omitted_catalog_products", "products_with_positive_daily", "categories", "window_checks")},
          ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
