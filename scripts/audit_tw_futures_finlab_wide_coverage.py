#!/usr/bin/env python3
"""Audit source-backed FinLab daily futures candidates against TAIFEX scope.

Daily wide-table cells are potential evidence only. This does not identify
decision-clock executions, prove PIT availability, or admit products to a
financial or training release.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file
from downloader.provider_priority import inspect


ROOT = Path(__file__).resolve().parents[1]
KEYS = ("futures_price:收盤價", "futures_price:成交量", "futures_price:到期月份(週別)")


def compare_wide(close: pl.DataFrame, volume: pl.DataFrame, month: pl.DataFrame,
                 inventory: pl.DataFrame, official: dict, *, first: str, last: str) -> dict:
    if first > last or official.get("market_day") != last:
        raise ValueError("the official market day must equal the audit end")
    if "product" not in inventory.columns or inventory["product"].null_count() or inventory["product"].is_duplicated().any():
        raise ValueError("historical inventory product identities must be unique")
    products = official.get("products")
    if not isinstance(products, list) or any(not isinstance(row, dict) or not row.get("product") for row in products):
        raise ValueError("official product inventory is missing")
    official_twd = {row["product"]: row for row in products if row.get("settlement_currency") == "TWD"}
    if len(official_twd) != sum(row.get("settlement_currency") == "TWD" for row in products):
        raise ValueError("official TWD product identities must be unique")
    historical = set(inventory["product"].to_list())
    candidates = sorted(historical | set(official_twd))

    prepared = []
    axis_sets = []
    for frame in (close, volume, month):
        if "source_index" not in frame.columns:
            raise ValueError("FinLab daily source has no source index")
        frame = frame.with_columns(pl.col("source_index").cast(pl.String).str.slice(0, 10).alias("_day"))
        frame = frame.filter(pl.col("_day").is_between(pl.lit(first), pl.lit(last)))
        if frame["_day"].null_count() or frame["_day"].is_duplicated().any():
            raise ValueError("FinLab daily source has ambiguous market-day rows")
        axis_sets.append(set(frame["_day"].to_list()))
        prepared.append(frame)
    common_days = sorted(set.intersection(*axis_sets))
    if not common_days or last not in common_days:
        raise ValueError("the three FinLab fields lack the requested common market day")
    columns = [code + "一般" for code in candidates
               if all(code + "一般" in frame.columns for frame in prepared)]
    if not columns:
        raise ValueError("the daily fields have no shared general-session product columns")
    aligned = [f.filter(pl.col("_day").is_in(common_days)).sort("_day") for f in prepared]
    prices = aligned[0].select(pl.col(c).cast(pl.Float64, strict=False) for c in columns).to_numpy()
    volumes = aligned[1].select(pl.col(c).cast(pl.Float64, strict=False) for c in columns).to_numpy()
    month_values = aligned[2].select(pl.col(c).cast(pl.String).fill_null("") for c in columns).to_numpy()
    valid_month = aligned[2].select(
        pl.col(c).cast(pl.String).str.contains(r"^\d{6}$").fill_null(False) for c in columns
    ).to_numpy()
    valid = np.isfinite(prices) & (prices > 0) & np.isfinite(volumes) & (volumes > 0) & valid_month
    details = []
    for index, column in enumerate(columns):
        code = column[:-2]
        rows = np.flatnonzero(valid[:, index])
        today_valid = bool(valid[-1, index])
        official_row = official_twd.get(code)
        official_months = official_row.get("months", []) if official_row else []
        details.append({
            "product": code, "historical_inventory": code in historical,
            "historical_positive_candidate_days": int(rows.size),
            "first_positive_candidate_day": common_days[int(rows[0])] if rows.size else None,
            "last_positive_candidate_day": common_days[int(rows[-1])] if rows.size else None,
            "latest_positive_candidate": today_valid,
            "latest_month": str(month_values[-1, index]) if today_valid else None,
            "latest_month_in_official_snapshot": (str(month_values[-1, index]) in official_months)
                if today_valid and official_row else None,
        })
    by_code = {row["product"]: row for row in details}
    official_positive = {code for code, row in official_twd.items() if row.get("positive_volume_contracts", 0) > 0}
    matched = {code for code in official_positive if code in by_code
               and by_code[code]["latest_positive_candidate"]
               and by_code[code]["latest_month_in_official_snapshot"]}
    return {
        "first_requested_date": first, "last_requested_date": last,
        "historical_inventory_products": len(historical),
        "common_source_days": len(common_days),
        "field_days_missing_from_common_axis": [len(days - set(common_days)) for days in axis_sets],
        "historical_products_with_any_positive_daily_candidate": sum(
            row["historical_inventory"] and row["historical_positive_candidate_days"] > 0 for row in details),
        "historical_products_without_positive_daily_candidate": sorted(
            code for code in historical if code not in by_code or not by_code[code]["historical_positive_candidate_days"]),
        "official_twd_products": len(official_twd),
        "official_twd_products_with_positive_volume": len(official_positive),
        "official_positive_with_finlab_positive_candidate_and_reported_month": len(matched),
        "official_positive_without_that_finlab_candidate": sorted(official_positive - matched),
        "products": details,
    }


def audit(root: Path, request_id: str, inventory_path: Path, official_path: Path,
          *, first: str, last: str) -> dict:
    status = inspect(root, request_id)
    rows = {row["key"]: row for row in status["items"]}
    paths = [inventory_path, official_path, Path(__file__), root / "priority_queries" / (request_id + ".json")]
    pins = {str(path): sha256_file(path) for path in paths}
    frames = []
    sources = []
    for key in KEYS:
        row = rows.get(key, {})
        if row.get("state") != "upstream_checked":
            raise ValueError(f"fresh native source receipt is unavailable: {key}")
        receipt_path = Path(row["receipt_path"])
        receipt_hash = sha256_file(receipt_path)
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("dataset") != key or sha256_file(receipt_path) != receipt_hash:
            raise ValueError("FinLab receipt dataset identity mismatch")
        data_path = (root / receipt["parquet_path"]).resolve()
        if not data_path.is_relative_to(root.resolve()) or sha256_file(data_path) != receipt.get("sha256"):
            raise ValueError("FinLab source object identity mismatch")
        paths.extend((receipt_path, data_path))
        pins[str(receipt_path)] = receipt_hash
        pins[str(data_path)] = receipt["sha256"]
        sources.append({"dataset": key, "receipt_path": str(receipt_path),
                        "source_checked_at_utc": receipt.get("source_checked_at_utc"),
                        "source_sha256": receipt["sha256"]})
        frames.append(pl.read_parquet(data_path))
    result = compare_wide(*frames, pl.read_csv(inventory_path, infer_schema_length=0),
                          json.loads(official_path.read_text(encoding="utf-8")), first=first, last=last)
    if any(sha256_file(path) != pins[str(path)] for path in paths):
        raise ValueError("an audit input changed during readback")
    return {"contract": "tw_futures_finlab_wide_candidate_audit_v1",
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "request_id": request_id, "sources": sources, **result, "input_sha256": pins,
            "limitations": ["Daily source fields are product-level candidates, not proof of executable decision-clock prices.",
                "The monthly match uses the official report's listed months, not a validated per-contract price or fill.",
                "Point-in-time availability, full account rules and quote currency remain separately unverified.",
                "Non-observed products or days are unknown, not evidence of zero trades."],
            "financial_or_training_adopted": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--official", type=Path, required=True)
    parser.add_argument("--first", default="2011-01-01")
    parser.add_argument("--last", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    target = args.output.resolve()
    if not target.is_relative_to((ROOT / "artifacts/markets").resolve()):
        parser.error("output must be in artifacts/markets")
    result = audit(args.root.resolve(), args.request_id, args.inventory.resolve(),
                   args.official.resolve(), first=args.first, last=args.last)
    with target.open("x", encoding="utf-8") as out:
        json.dump(result, out, ensure_ascii=False, indent=2)
        out.write("\n")
    print(json.dumps({key: result[key] for key in ("historical_products_with_any_positive_daily_candidate",
        "official_twd_products_with_positive_volume", "official_positive_with_finlab_positive_candidate_and_reported_month")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
