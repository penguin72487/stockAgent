#!/usr/bin/env python3
"""Audit later observations for quarantined physical-contract sessions.

This is an after-the-fact research diagnostic. Later observations must never
become decision-time features or automatically establish a trading session.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file


DAILY_COLUMNS = (
    "date", "physical_contract", "product", "contract", "asset_class",
    "official_expiry", "source_row_observed", "official_row_observed",
    "official_source_sha256", "outright_volume", "accounting_only_date",
    "corporate_identity_candidate_only", "corporate_boundary", "lifetime_status",
)
IDENTITY = ["physical_contract", "product", "contract"]


def match_later_observations(gaps: pl.DataFrame, daily: pl.DataFrame) -> pl.DataFrame:
    """Match the first later positive, source-observed day under exact identity."""
    required = {"date", "physical_contract", "product", "delivery_month"}
    if required - set(gaps.columns) or set(DAILY_COLUMNS) - set(daily.columns):
        raise ValueError("required quarantine or daily columns are missing")
    if (gaps.select("date", "physical_contract").is_duplicated().any()
            or daily.select("date", "physical_contract").is_duplicated().any()):
        raise ValueError("duplicate contract-day identity")
    wanted = gaps.select("date", "physical_contract", pl.col("product").alias("q_product"),
                         pl.col("delivery_month").alias("q_contract"))
    current = wanted.join(daily.select(DAILY_COLUMNS), on=["date", "physical_contract"],
                          how="left", validate="1:1")
    if (current.filter(pl.col("product").is_null() | pl.col("contract").is_null()
                       | pl.col("q_product").is_null() | pl.col("q_contract").is_null()
                       | (pl.col("product") != pl.col("q_product"))
                       | (pl.col("contract") != pl.col("q_contract"))).height):
        raise ValueError("quarantined identity does not match the pinned daily source")

    later = (daily.filter(pl.col("source_row_observed").fill_null(False)
               & pl.col("official_row_observed").fill_null(False)
               & ~pl.col("accounting_only_date").fill_null(True)
               & (pl.col("outright_volume") > 0)
               & (pl.col("official_source_sha256").str.len_chars() == 64))
             .select(*IDENTITY, pl.col("date").alias("later_date"),
                     pl.col("official_source_sha256").alias("later_source_sha256")))
    # Adding a day enforces strictly later observations, including when the
    # quarantined date itself has positive official daily volume.
    current = current.with_columns((pl.col("date") + pl.duration(days=1)).alias("after_date"))
    result = (current.sort(*IDENTITY, "after_date").join_asof(
        later.sort(*IDENTITY, "later_date"), left_on="after_date", right_on="later_date",
        by=IDENTITY, strategy="forward", check_sortedness=False))
    if result.filter(pl.col("later_date").is_not_null() & (pl.col("later_date") <= pl.col("date"))).height:
        raise ValueError("later observation is not strictly later")
    return result.with_columns(
        pl.col("later_date").is_not_null().alias("later_observed"),
        (pl.col("later_date").is_not_null() & (pl.col("corporate_boundary").is_null()
            | (pl.col("later_date") < pl.col("corporate_boundary")))).alias("later_before_boundary"),
        (pl.col("later_date").is_not_null() & pl.col("official_expiry").is_not_null()
            & (pl.col("later_date") > pl.col("official_expiry"))).alias("later_after_recorded_expiry"),
        pl.col("official_expiry").is_null().alias("expiry_missing"),
        pl.col("corporate_identity_candidate_only").fill_null(True).alias("identity_candidate_only"),
    ).drop("after_date", "q_product", "q_contract")


def audit(quarantine: Path, daily_path: Path, *, quarantine_sha: str) -> dict:
    if sha256_file(quarantine) != quarantine_sha:
        raise ValueError("quarantine source differs from its pin")
    doc = json.loads(quarantine.read_text())
    if doc.get("contract") != "tw_futures_unknown_entry_quarantine_v1":
        raise ValueError("unsupported quarantine source")
    daily_sha = doc["source_daily_sha256"]
    if sha256_file(daily_path) != daily_sha:
        raise ValueError("daily source differs from the quarantine pin")
    gaps = pl.DataFrame(doc["contract_days"]).with_columns(pl.col("date").str.to_date())
    daily = pl.scan_parquet(daily_path).select(DAILY_COLUMNS).collect()
    result = match_later_observations(gaps, daily)
    if sha256_file(quarantine) != quarantine_sha or sha256_file(daily_path) != daily_sha:
        raise ValueError("source changed during the audit")
    group_keys = ["asset_class", "expiry_missing", "later_observed", "later_before_boundary",
                  "identity_candidate_only", "later_after_recorded_expiry"]
    grouped = result.group_by(group_keys).len().sort(group_keys)
    return {
        "contract": "tw_futures_quarantine_later_observation_audit_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_sha256": {"quarantine": quarantine_sha, "daily": daily_sha,
                         "implementation": sha256_file(Path(__file__))},
        "contract_days": result.height,
        "later_observed": result["later_observed"].sum(),
        "later_before_boundary": result["later_before_boundary"].sum(),
        "later_after_recorded_expiry": result["later_after_recorded_expiry"].sum(),
        "identity_candidate_only": result["identity_candidate_only"].sum(),
        "counts_by_status": grouped.to_dicts(),
        "examples": result.select("date", "physical_contract", "asset_class", "official_expiry",
            "corporate_boundary", "later_date", "later_source_sha256", "identity_candidate_only")
            .sort("date", "physical_contract").head(15).with_columns(
                pl.col("date", "official_expiry", "corporate_boundary", "later_date").cast(pl.String)
            ).to_dicts(),
        "training_ready": False,
        "limitations": [
            "Later observations are retrospective research evidence, never a decision-time feature.",
            "The physical generation is source candidate identity, not a verified legal continuity claim.",
            "A later trade cannot establish the earlier day's trading hours, receive time, fill or account valuation.",
            "No quarantine, source release, action eligibility or training admission is changed.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarantine", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--quarantine-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    workspace = Path.cwd().resolve()
    output = args.output.resolve()
    if not output.is_relative_to(workspace / "artifacts/markets") or output.exists() or output.is_symlink():
        raise ValueError("choose a new report under artifacts/markets")
    result = audit(args.quarantine, args.daily, quarantine_sha=args.quarantine_sha)
    atomic_write_json(output, result)
    print(json.dumps({k: result[k] for k in ("contract_days", "later_observed", "later_before_boundary",
          "later_after_recorded_expiry", "identity_candidate_only", "counts_by_status")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
