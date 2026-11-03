#!/usr/bin/env python3
"""Inspect omitted first-observation windows against retained raw ticks.

This is a source diagnostic, not a first-day listing, clock or trading policy.
Raw ticks can show prints but cannot by themselves prove point-in-time listing,
complete coverage, available execution capacity, or broker fills.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.build_taifex_option_finmind_reference_window import capacity_check


KEYS = ["date", "physical_contract"]
TARGET_COLUMNS = [*KEYS, "product", "contract", "asset_class", "series_type",
                  "first_observed_date", "outright_volume"]


def inspect_windows(raw: pl.DataFrame, targets: pl.DataFrame, day: date) -> pl.DataFrame:
    """Count observed one-minute prints; do not assume completeness or fills."""
    mapping = targets.select("product", "contract", "physical_contract")
    if (targets.select(KEYS).is_duplicated().any()
            or mapping.select("product", "contract").is_duplicated().any()
            or targets.filter(pl.col("date") != day).height):
        raise ValueError("ambiguous or incorrectly dated targets")
    selected = (raw.select("date", "futures_id", "contract_date", "price", "volume")
        .rename({"futures_id": "product", "contract_date": "contract"})
        .join(mapping, on=["product", "contract"], how="inner", validate="m:1")
        .with_columns(pl.col("date").str.to_datetime(time_unit="us", strict=True).alias("_timestamp")))
    if selected.filter(pl.col("_timestamp").is_null() | (pl.col("_timestamp").dt.date() != day)).height:
        raise ValueError("selected raw tick is not from its receipt calendar date")
    selected = selected.with_columns((pl.col("_timestamp").dt.hour().cast(pl.Int32) * 60
                                       + pl.col("_timestamp").dt.minute().cast(pl.Int32)).alias("_minute"))
    window = selected.filter(pl.col("_minute").is_in([526, 541]))
    invalid = (pl.col("price").is_null() | ~pl.col("price").is_finite() | (pl.col("price") <= 0)
        | pl.col("volume").is_null() | (pl.col("volume") <= 0)
        | (pl.col("volume") != pl.col("volume").floor()) | ((pl.col("volume") % 2) != 0)).fill_null(True)
    stats = (window.with_columns(invalid.alias("_invalid"))
        .group_by("physical_contract", "_minute")
        .agg(pl.len().alias("raw_print_rows"), pl.col("_invalid").sum().alias("invalid_raw_rows"),
             pl.col("_timestamp").min().alias("first_raw_timestamp")))
    out = targets
    for minute, label in [(526, "0845"), (541, "0900")]:
        part = (stats.filter(pl.col("_minute") == minute).drop("_minute")
                .rename({"raw_print_rows": f"{label}_raw_print_rows",
                         "invalid_raw_rows": f"{label}_invalid_raw_rows",
                         "first_raw_timestamp": f"{label}_first_raw_timestamp"}))
        out = out.join(part, on="physical_contract", how="left", validate="1:1").with_columns(
            pl.col(f"{label}_raw_print_rows").fill_null(0),
            pl.col(f"{label}_invalid_raw_rows").fill_null(0))
    return out


def build(workspace: Path, daily_dir: Path, checkpoint: Path, checkpoint_sha: str,
          output: Path, report: Path) -> dict:
    workspace, output, report = workspace.resolve(), output.resolve(), report.resolve()
    if (not output.is_relative_to(workspace) or not output.relative_to(workspace).parts[0].startswith("data_")
            or output.exists() or output.is_symlink() or report.exists() or report.is_symlink()
            or not report.is_relative_to(workspace / "artifacts/markets")):
        raise ValueError("requires new data_* output and artifacts/markets report")
    manifest_path = checkpoint / "manifest.json"
    if sha256_file(manifest_path) != checkpoint_sha:
        raise ValueError("bulk source inventory does not match the reviewed checkpoint")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("contract") != "verified_bulk_window_checkpoint_v1":
        raise ValueError("unsupported source checkpoint")
    objects = manifest["identity"]["objects"]
    inventory = {item["date"]: item for item in objects}
    if len(inventory) != len(objects):
        raise ValueError("source inventory has duplicate market dates")
    warm_path, daily_meta = daily_dir / "first_observation_warmup.parquet", daily_dir / "manifest.json"
    warm_sha, meta_sha = sha256_file(warm_path), sha256_file(daily_meta)
    warm = (pl.scan_parquet(warm_path)
        .filter((pl.col("date") >= date(2011, 1, 3)) & (pl.col("date") <= date(2026, 9, 24))
                & (pl.col("date") == pl.col("first_observed_date")) & (pl.col("outright_volume") > 0))
        .select(TARGET_COLUMNS).collect().sort(KEYS))
    if (not warm.height or warm.select(KEYS).is_duplicated().any()
            or warm.select("date", "product", "contract").is_duplicated().any()):
        raise ValueError("no targets or ambiguous first observation identities")
    initial_capacity = capacity_check(output.parent)
    pieces, source_counts = [], {"raw_receipt_matched_days": 0, "missing_or_unsupported_days": 0}
    dates = warm["date"].unique().sort().to_list()
    for idx, day in enumerate(dates, 1):
        targets = warm.filter(pl.col("date") == day)
        item = inventory.get(day.isoformat())
        if item is None or item.get("source_kind"):
            source_counts["missing_or_unsupported_days"] += 1
            pieces.append(targets.with_columns(pl.lit("missing_or_unsupported_source").alias("source_status")))
            continue
        path, receipt = Path(item["path"]), Path(item["receipt_path"])
        before = path.stat()
        if (before.st_size != item["bytes"] or sha256_file(path) != item["sha256"]
                or sha256_file(receipt) != item["receipt_sha256"]
                or pq.ParquetFile(path).metadata.num_rows != item["rows"]):
            raise ValueError(f"retained raw or receipt identity mismatch: {day}")
        raw = pl.read_parquet(path, columns=["date", "futures_id", "contract_date", "price", "volume"])
        found = inspect_windows(raw, targets, day).with_columns(
            pl.lit("raw_receipt_matched").alias("source_status"),
            pl.lit(item["sha256"]).alias("source_sha256"),
            pl.lit(item["receipt_sha256"]).alias("receipt_sha256"))
        after = path.stat()
        if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                or sha256_file(path) != item["sha256"] or sha256_file(receipt) != item["receipt_sha256"]):
            raise ValueError(f"raw or receipt changed during readback: {day}")
        source_counts["raw_receipt_matched_days"] += 1
        pieces.append(found)
        if idx % 100 == 0 or idx == len(dates):
            print(json.dumps({"stage": "first_day_window_raw_audit", "dates_checked": idx,
                              "dates_total": len(dates)}), flush=True)
    result = pl.concat(pieces, how="diagonal_relaxed").sort(KEYS)
    if result.height != warm.height or result.select(KEYS).is_duplicated().any():
        raise ValueError("first-day source results lost or duplicated target rows")
    matched = result.filter(pl.col("source_status") == "raw_receipt_matched")
    expressions = [pl.len().alias("first_observation_contract_days"),
                   pl.col("product").n_unique().alias("historical_product_codes")]
    for label in ("0845", "0900"):
        expressions.extend([
            ((pl.col(f"{label}_raw_print_rows") > 0) & (pl.col(f"{label}_invalid_raw_rows") == 0))
                .sum().alias(f"{label}_positive_raw_window"),
            (pl.col(f"{label}_invalid_raw_rows") > 0).sum().alias(f"{label}_invalid_raw_window")])
    by_class = matched.group_by("asset_class", "series_type").agg(*expressions).sort("asset_class", "series_type")
    if (sha256_file(warm_path) != warm_sha or sha256_file(daily_meta) != meta_sha
            or sha256_file(manifest_path) != checkpoint_sha):
        raise ValueError("parent input changed during audit")
    publication_capacity = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / "observations.parquet"
    atomic_write_parquet(payload, result)
    doc = {"contract": "tw_futures_omitted_first_day_windows_v1", "created_at_utc": datetime.now(UTC).isoformat(),
        "target_contract_days": warm.height, "source_dates": len(dates), "source_counts": source_counts,
        "by_asset_class_and_series": by_class.to_dicts(), "data_path": str(payload), "data_sha256": sha256_file(payload),
        "input_sha256": {str(warm_path): warm_sha, str(daily_meta): meta_sha, str(manifest_path): checkpoint_sha},
        "implementation_sha256": sha256_file(Path(__file__)), "training_ready": False,
        "initial_capacity": initial_capacity, "publication_capacity": publication_capacity,
        "limitations": ["The warmup file is hashed here but is not independently hashed in the daily release manifest.",
            "Daily positive volume and first-observed dates are retrospective, not listing-at-decision evidence.",
            "Raw ticks lack historical receipt-time proof and complete-book or exchange-grid reconciliation here.",
            "Positive source-window prints do not prove executable capacity, first-day eligibility or fills.",
            "No training or execution source is modified or admitted."]}
    atomic_write_json(output / "manifest.json", doc)
    atomic_write_json(report, doc)
    return doc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--daily-dir", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--checkpoint-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = build(args.workspace, args.daily_dir, args.checkpoint, args.checkpoint_sha, args.output, args.report)
    print(json.dumps({"target_contract_days": result["target_contract_days"],
                      "source_counts": result["source_counts"],
                      "by_asset_class_and_series": result["by_asset_class_and_series"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
