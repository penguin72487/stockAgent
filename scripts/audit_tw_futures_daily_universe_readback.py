#!/usr/bin/env python3
"""Read back a frozen futures *daily* universe without claiming clock admission.

This audit is deliberately limited to the supplied files and interval.  In
particular, the daily ``executable`` flag is not a decision-time observation,
an account permission, or proof that the whole product history is complete.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def audit(daily_path: Path, inventory_path: Path, start: date, end: date,
          expected_daily_sha256: str) -> dict:
    if start > end:
        raise ValueError("audit interval is reversed")
    before = sha256(daily_path)
    if before != expected_daily_sha256:
        raise ValueError("daily source does not match its expected SHA-256")
    inventory_digest = sha256(inventory_path)
    with inventory_path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"product", "product_name", "asset_class", "settlement_currency",
                    "source_start", "source_end", "raw_positive_volume_rows", "source_status"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError("inventory is missing required identity or coverage columns")
        inventory = {}
        for row in reader:
            code = row["product"]
            if not code or code in inventory:
                raise ValueError("inventory product identities must be nonempty and unique")
            inventory[code] = row

    frame = pl.scan_parquet(daily_path)
    schema = frame.collect_schema()
    if (any(name not in schema for name in ("date", "product", "asset_class", "executable"))
            or schema["date"] != pl.Date or schema["product"] != pl.String
            or schema["asset_class"] != pl.String or schema["executable"] != pl.Boolean):
        raise ValueError("daily source lacks the expected typed audit columns")
    frame = frame.filter(pl.col("date").is_between(start, end, closed="both"))
    aggregate = frame.group_by("product").agg(
        pl.len().alias("rows"),
        pl.col("asset_class").first().alias("asset_class"),
        pl.col("asset_class").n_unique().alias("class_count"),
        pl.col("executable").null_count().alias("unknown_execution_rows"),
        pl.col("executable").sum().alias("daily_executable_rows"),
        pl.min("date").alias("first"),
        pl.max("date").alias("last"),
    ).sort("product").collect()
    if not aggregate.height:
        raise ValueError("daily source has no records in the requested interval")
    if (aggregate["product"].null_count() or aggregate["asset_class"].null_count()
            or aggregate.filter((pl.col("product") == "") | (pl.col("asset_class") == "")
                                | (pl.col("class_count") != 1)
                                | (pl.col("unknown_execution_rows") != 0)).height):
        raise ValueError("daily source has ambiguous product, class or executable status")
    products = {row["product"]: row for row in aggregate.to_dicts()}
    classes = {}
    for row in products.values():
        entry = classes.setdefault(row["asset_class"],
                                   {"products_with_rows": 0, "products_with_daily_executable_rows": 0,
                                    "daily_executable_rows": 0})
        entry["products_with_rows"] += 1
        entry["products_with_daily_executable_rows"] += int(row["daily_executable_rows"] > 0)
        entry["daily_executable_rows"] += row["daily_executable_rows"]

    def inventory_fields(row: dict) -> dict:
        return {key: row[key] for key in ("product", "product_name", "asset_class",
                "settlement_currency", "source_start", "source_end",
                "raw_positive_volume_rows", "source_status")}

    missing = [inventory_fields(inventory[code]) for code in sorted(set(inventory) - set(products))]
    no_execution = []
    for code, row in products.items():
        if row["daily_executable_rows"] == 0:
            no_execution.append({"product": code, "asset_class": row["asset_class"],
                                 "product_name": inventory.get(code, {}).get("product_name"),
                                 "rows": row["rows"], "first": row["first"].isoformat(),
                                 "last": row["last"].isoformat()})
    if sha256(daily_path) != before or sha256(inventory_path) != inventory_digest:
        raise ValueError("an input changed during the readback")
    return {
        "contract": "tw_futures_daily_universe_readback_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "interval": {"start": start.isoformat(), "end": end.isoformat()},
        "inputs": {"daily_path": str(daily_path), "daily_sha256": before,
                   "inventory_path": str(inventory_path), "inventory_sha256": inventory_digest,
                   "implementation_sha256": sha256(Path(__file__))},
        "inventory_products": len(inventory),
        "daily_rows": sum(row["rows"] for row in products.values()),
        "daily_products_with_rows": len(products),
        "daily_products_with_executable_rows": sum(row["daily_executable_rows"] > 0 for row in products.values()),
        "daily_executable_rows": sum(row["daily_executable_rows"] for row in products.values()),
        "by_asset_class": dict(sorted(classes.items())),
        "inventory_products_absent_from_daily": missing,
        "daily_products_absent_from_inventory": sorted(set(products) - set(inventory)),
        "products_without_daily_executable_rows": no_execution,
        "scope": ["source-bound daily flags, not executable prices at 08:45 or 09:00",
                  "historical product union, not simultaneously tradeable products",
                  "settlement currency in the supplied inventory is not quote-currency verification",
                  "not historical point-in-time, account-permission or training admission evidence"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--expected-daily-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.daily, args.inventory, args.start, args.end, args.expected_daily_sha256)
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
