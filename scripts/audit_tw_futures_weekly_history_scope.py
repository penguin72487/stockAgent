#!/usr/bin/env python3
"""Audit weekly contract observations and first-day omissions, without admission."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


KEYS = ["date", "physical_contract"]
DAILY_COLUMNS = [*KEYS, "product", "contract", "series_type", "first_observed_date",
                 "source_row_observed", "executable", "outright_volume", "accounting_only_date"]
ENTRY_COLUMNS = [*KEYS, "status", "entry_volume"]


def _validate_weekly(frame: pl.DataFrame) -> None:
    if frame.select(KEYS).is_duplicated().any():
        raise ValueError("duplicate weekly physical contract-day")
    if any(frame[c].null_count() for c in [*KEYS, "product", "contract", "first_observed_date"]):
        raise ValueError("missing weekly identity or first observation")
    if frame.filter(~pl.col("contract").str.contains(r"^\d{6}W[1-5]$")).height:
        raise ValueError("weekly label and contract format disagree")
    if frame.filter(pl.col("date") < pl.col("first_observed_date")).height:
        raise ValueError("physical row precedes its first recorded observation")


def summarize(daily: pl.DataFrame, warmup: pl.DataFrame, entries: dict[str, pl.DataFrame]) -> dict:
    _validate_weekly(daily)
    _validate_weekly(warmup)
    if not daily.height:
        raise ValueError("weekly release contains no rows")
    if daily.select(KEYS).join(warmup.select(KEYS), on=KEYS, how="semi").height:
        raise ValueError("released and warmup weekly rows overlap")
    first = warmup.filter(pl.col("date") == pl.col("first_observed_date"))
    if first.select("physical_contract").is_duplicated().any():
        raise ValueError("multiple first observations for a physical contract")
    firsts = (daily.group_by("physical_contract", "first_observed_date")
              .agg(pl.col("date").min().alias("first_released_date"))
              .with_columns((pl.col("first_released_date") - pl.col("first_observed_date"))
                            .dt.total_days().alias("delay_calendar_days")))
    if firsts.select("physical_contract").is_duplicated().any():
        raise ValueError("first-observation identity changes within a contract")
    overlap = first.join(firsts.select("physical_contract"), on="physical_contract", how="semi")
    first_summary = {
        "warmup_rows": warmup.height,
        "first_observation_rows": first.height,
        "first_observation_also_has_later_release": overlap.height,
        "source_observed_on_first_day": first.filter(pl.col("source_row_observed").fill_null(False)).height,
        "positive_outright_volume_on_first_day": first.filter(pl.col("outright_volume").fill_null(0) > 0).height,
        "daily_source_executable_on_first_day": first.filter(pl.col("executable").fill_null(False)).height,
        "release_first_row_delay_calendar_days": firsts.group_by("delay_calendar_days").len()
            .sort("delay_calendar_days").to_dicts(),
    }
    clocks = []
    for clock, frame in entries.items():
        if clock not in {"08:45", "09:00"} or frame.select(KEYS).is_duplicated().any():
            raise ValueError("unsupported clock or duplicate entry identity")
        joined = daily.join(frame.select(ENTRY_COLUMNS), on=KEYS, how="left", validate="1:1")
        counts = (joined.with_columns(pl.col("status").fill_null("missing_from_clock"))
                  .group_by("status").agg(pl.len().alias("contract_days"),
                    ((pl.col("entry_volume").is_finite().fill_null(False))
                     & (pl.col("entry_volume").fill_null(0) > 0)).sum().alias("positive_window_prints"))
                  .sort("status").to_dicts())
        clocks.append({"clock": clock, "status_counts": counts})
    return {
        "released_contract_days": daily.height,
        "physical_contracts": daily["physical_contract"].n_unique(),
        "historical_products": sorted(daily["product"].unique().to_list()),
        "first_released_date": daily["date"].min().isoformat(),
        "last_released_date": daily["date"].max().isoformat(),
        "accounting_only_rows": daily.filter(pl.col("accounting_only_date").fill_null(False)).height,
        "first_observation": first_summary,
        "clocks": clocks,
    }


def build(daily_dir: Path, early_dir: Path, late_dir: Path, report: Path) -> dict:
    if report.exists() or report.is_symlink():
        raise FileExistsError("preserve previous evidence; select a new report")
    daily_path, warm_path, daily_meta = (daily_dir / "continuous_daily.parquet",
                                       daily_dir / "first_observation_warmup.parquet", daily_dir / "manifest.json")
    doc = json.loads(daily_meta.read_text())
    daily_sha = sha256_file(daily_path)
    if doc["outputs"]["continuous_daily"]["sha256"] != daily_sha:
        raise ValueError("daily release hash differs from its manifest")
    pins = {daily_path: daily_sha, warm_path: sha256_file(warm_path), daily_meta: sha256_file(daily_meta)}
    daily = pl.scan_parquet(daily_path).filter(pl.col("series_type") == "weekly").select(DAILY_COLUMNS).collect()
    warmup = pl.scan_parquet(warm_path).filter(pl.col("series_type") == "weekly").select(DAILY_COLUMNS).collect()
    frames, pair = {}, None
    for clock, directory in [("08:45", early_dir), ("09:00", late_dir)]:
        path, manifest_path = directory / "entries.parquet", directory / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        digest = sha256_file(path)
        if (manifest["entries_sha256"] != digest or manifest["source_daily_sha256"] != daily_sha
                or manifest["clock"]["decision_time"] != clock):
            raise ValueError("clock manifest, input release or data hash disagree")
        if pair is not None and manifest["pair_identity"] != pair:
            raise ValueError("decision clocks do not use the same source pair")
        pair = manifest["pair_identity"]
        pins.update({path: digest, manifest_path: sha256_file(manifest_path)})
        frames[clock] = (pl.scan_parquet(path).select(ENTRY_COLUMNS).join(
            daily.lazy().select(KEYS), on=KEYS, how="semi").collect())
    result = summarize(daily, warmup, frames)
    if any(sha256_file(path) != digest for path, digest in pins.items()):
        raise ValueError("source changed during audit")
    output = {"contract": "tw_futures_weekly_history_scope_v1", "created_at_utc": datetime.now(UTC).isoformat(),
        "summary": result, "source_sha256": {str(path): digest for path, digest in pins.items()},
        "implementation_sha256": sha256_file(Path(__file__)), "training_ready": False,
        "limitations": [
            "The warmup file is hashed for this audit but is not separately hashed by the daily release manifest.",
            "First source observation and full-day volume are retrospective; neither proves listing at decision time.",
            "A window print is not model eligibility, an executable order, or a broker fill.",
            "No first-day trading policy, source identity or accounting release is admitted by this audit.",
        ]}
    atomic_write_json(report, output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily-dir", type=Path, required=True)
    parser.add_argument("--early-dir", type=Path, required=True)
    parser.add_argument("--late-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.daily_dir, args.early_dir, args.late_dir, args.report)["summary"], ensure_ascii=False))


if __name__ == "__main__":
    main()
