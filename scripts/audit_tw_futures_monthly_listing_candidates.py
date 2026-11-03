#!/usr/bin/env python3
"""Compare omitted stock-futures first observations with an old exchange manual.

This is a retrospective source diagnostic, never an eligibility or fill mask.
The retained manual's historical effective interval and individual product
listings are not established by its PDF creation metadata.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.build_taifex_option_finmind_reference_window import capacity_check


MANUAL_SHA256 = "deb0ddaca528c6bf3e395614e668872666a465db987f24ce751581e75bd1b68d"
MANUAL_URL = "https://www.taifex.com.tw/file/taifex/CHINESE/10/stf.pdf"


def _month(year: int, month: int, offset: int) -> tuple[int, int]:
    index = year * 12 + month - 1 + offset
    return divmod(index, 12)[0], divmod(index, 12)[1] + 1


def manual_five_months(year: int, month: int) -> tuple[str, ...]:
    """Two consecutive months and the next three distinct quarterly months."""
    if not 1 <= month <= 12 or year < 1:
        raise ValueError("valid calendar month required")
    selected = [_month(year, month, offset) for offset in (0, 1)]
    offset = 0
    while len(selected) < 5:
        candidate = _month(year, month, offset)
        if candidate[1] in (3, 6, 9, 12) and candidate not in selected:
            selected.append(candidate)
        offset += 1
    return tuple(f"{y:04d}{m:02d}" for y, m in sorted(selected))


def classify(contract: str, observed_day: date, observed_source_days: set[date]) -> dict:
    """Only match the literal ordinary schedule; exceptions remain unresolved."""
    previous_day = observed_day - timedelta(days=1)
    result = {"previous_nominal_expiry": previous_day, "manual_new_month": None}
    if not isinstance(contract, str) or not re.fullmatch(r"\d{6}", contract):
        return {**result, "state": "unsupported_contract_label"}
    year, month = int(contract[:4]), int(contract[4:])
    if not 1 <= month <= 12 or year < 1:
        return {**result, "state": "unsupported_contract_label"}
    if not (previous_day.weekday() == 2 and 15 <= previous_day.day <= 21):
        return {**result, "state": "not_next_day_after_nominal_third_wednesday"}
    before = set(manual_five_months(previous_day.year, previous_day.month))
    next_year, next_month = _month(previous_day.year, previous_day.month, 1)
    after = set(manual_five_months(next_year, next_month))
    new = after - before
    if len(new) != 1:
        raise ValueError("manual month transition must add exactly one month")
    result["manual_new_month"] = next(iter(new))
    if previous_day not in observed_source_days or observed_day not in observed_source_days:
        return {**result, "state": "nominal_session_source_not_observed"}
    if contract != result["manual_new_month"]:
        return {**result, "state": "different_from_manual_new_month"}
    return {**result, "state": "matches_2009_manual_schedule_candidate"}


def build(workspace: Path, warm_path: Path, daily_path: Path, daily_manifest: Path,
          observations_manifest: Path, checkpoint: Path, manual: Path,
          manual_receipt: Path, output: Path, report: Path) -> dict:
    workspace, output, report = workspace.resolve(), output.resolve(), report.resolve()
    if (not output.is_relative_to(workspace) or not output.relative_to(workspace).parts[0].startswith("data_")
            or output.exists() or output.is_symlink() or report.exists() or report.is_symlink()
            or not report.is_relative_to(workspace / "artifacts/markets")):
        raise ValueError("requires new data_* output and artifacts/markets report")
    paths = (warm_path, daily_path, daily_manifest, observations_manifest, checkpoint, manual, manual_receipt)
    pins = {str(path): sha256_file(path) for path in paths}
    manual_info = json.loads(manual_receipt.read_text())
    if (pins[str(manual)] != MANUAL_SHA256 or manual_info.get("sha256") != MANUAL_SHA256
            or manual_info.get("url") != MANUAL_URL or manual_info.get("historical_published_at") is not None):
        raise ValueError("retained official manual or its receipt changed")
    daily_meta = json.loads(daily_manifest.read_text())
    if daily_meta["outputs"]["continuous_daily"]["sha256"] != pins[str(daily_path)]:
        raise ValueError("daily source differs from release manifest")
    observations_meta = json.loads(observations_manifest.read_text())
    if (observations_meta.get("contract") != "tw_futures_omitted_first_day_windows_v1"
            or observations_meta.get("training_ready") is not False):
        raise ValueError("unsupported first-observation research artifact")
    observations_path = Path(observations_meta["data_path"])
    pins[str(observations_path)] = sha256_file(observations_path)
    if pins[str(observations_path)] != observations_meta["data_sha256"]:
        raise ValueError("first-observation data hash differs")
    for path in (warm_path, checkpoint):
        if observations_meta["input_sha256"].get(str(path)) != pins[str(path)]:
            raise ValueError("input differs from first-observation audit")
    objects = json.loads(checkpoint.read_text())["identity"]["objects"]
    days = {date.fromisoformat(obj["date"]) for obj in objects if obj.get("rows", 0) > 0}
    warm = (pl.scan_parquet(warm_path).filter((pl.col("asset_class") == "stock_future")
        & (pl.col("series_type") == "monthly") & (pl.col("date") == pl.col("first_observed_date"))
        & (pl.col("outright_volume") > 0) & (pl.col("date") >= date(2011, 1, 3))
        & (pl.col("date") <= date(2026, 9, 24)))
        .select("date", "physical_contract", "product", "contract", "official_expiry",
                "corporate_boundary", "corporate_identity_candidate_only").collect())
    observations = (pl.scan_parquet(observations_path)
        .filter((pl.col("asset_class") == "stock_future") & (pl.col("series_type") == "monthly"))
        .select("date", "physical_contract", "source_status", "0845_raw_print_rows",
                "0845_invalid_raw_rows", "0900_raw_print_rows", "0900_invalid_raw_rows").collect())
    key = ["date", "physical_contract"]
    if (not warm.height or warm.select(key).is_duplicated().any()
            or observations.select(key).is_duplicated().any()):
        raise ValueError("missing or duplicate stock-future first observations")
    joined = warm.join(observations, on=key, how="left", validate="1:1")
    if joined.height != observations.height or joined["source_status"].null_count():
        raise ValueError("stock observations do not match warmup target axis")
    records = []
    for row in joined.iter_rows(named=True):
        result = classify(row["contract"], row["date"], days)
        prev = result["previous_nominal_expiry"]
        records.append({**row, **result, "previous_month_contract": f"{prev.year:04d}{prev.month:02d}"})
    frame = pl.DataFrame(records).sort(key)
    prior = (pl.scan_parquet(daily_path).filter((pl.col("asset_class") == "stock_future")
        & (pl.col("series_type") == "monthly") & pl.col("official_row_observed").fill_null(False))
        .select(pl.col("date").alias("previous_nominal_expiry"), "product",
                pl.col("contract").alias("previous_month_contract"), "official_expiry")
        .group_by("previous_nominal_expiry", "product", "previous_month_contract")
        .agg(pl.len().alias("previous_observed_rows"),
             (pl.col("official_expiry") == pl.col("previous_nominal_expiry")).any()
             .alias("previous_expiry_matches_nominal")).collect())
    frame = frame.join(prior, on=["previous_nominal_expiry", "product", "previous_month_contract"],
                       how="left", validate="m:1")
    states = frame.group_by("state").agg(pl.len().alias("contract_days")).sort("state").to_dicts()
    matches = frame.filter(pl.col("state") == "matches_2009_manual_schedule_candidate")
    counts = {"matching_contract_days": matches.height, "historical_product_codes": matches["product"].n_unique(),
              "prior_month_official_row_observed": matches.filter(pl.col("previous_observed_rows") > 0).height,
              "prior_month_recorded_expiry_matches": matches.filter(pl.col("previous_expiry_matches_nominal")).height,
              "records_with_any_recorded_corporate_boundary": matches.filter(pl.col("corporate_boundary").is_not_null()).height}
    for label in ("0845", "0900"):
        counts[f"{label}_positive_raw_window"] = matches.filter(
            (pl.col("source_status") == "raw_receipt_matched") & (pl.col(f"{label}_raw_print_rows") > 0)
            & (pl.col(f"{label}_invalid_raw_rows") == 0)).height
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("a candidate source changed during processing")
    capacity = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / "candidates.parquet"
    atomic_write_parquet(payload, frame)
    doc = {"contract": "taifex_stock_future_2009_manual_schedule_candidates_v1",
           "created_at_utc": datetime.now(UTC).isoformat(), "candidate_rows": frame.height,
           "by_state": states, "manual_schedule_matches": counts, "data_path": str(payload),
           "data_sha256": sha256_file(payload), "inputs_sha256": pins,
           "implementation_sha256": sha256_file(Path(__file__)), "capacity": capacity,
           "manual_url": MANUAL_URL, "manual_historical_published_at": None,
           "training_ready": False,
           "limitations": ["2009 PDF metadata and an old manual do not establish its historical effective interval or later amendments.",
               "The ordinary monthly comparison excludes holiday and shifted sessions; source-day presence is not a complete exchange calendar.",
               "First observations, recorded later expiry and same-product prior records are retrospective; physical generations remain candidates.",
               "Product listing, permissions, contract adjustments, known-at time, capacity, broker fills, and carry accounting are not established.",
               "No eligibility mask, financial release, checkpoint or training input is changed."]}
    atomic_write_json(output / "manifest.json", doc)
    atomic_write_json(report, doc)
    return doc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("workspace", "warm-path", "daily-path", "daily-manifest", "observations-manifest",
                 "checkpoint", "manual", "manual-receipt", "output", "report"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    result = build(args.workspace, args.warm_path, args.daily_path, args.daily_manifest,
                   args.observations_manifest, args.checkpoint, args.manual, args.manual_receipt,
                   args.output, args.report)
    print(json.dumps({"candidate_rows": result["candidate_rows"], "by_state": result["by_state"],
                      "manual_schedule_matches": result["manual_schedule_matches"]}))


if __name__ == "__main__":
    main()
