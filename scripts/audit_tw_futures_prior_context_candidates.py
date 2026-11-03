#!/usr/bin/env python3
"""Independently audit review-only prior features against source and clocks."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_portfolio_daily import FUTURES_MODEL_FEATURE_COLUMNS


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-dir", required=True, type=Path)
    parser.add_argument("--frame", required=True, type=Path)
    parser.add_argument("--release", required=True, type=Path)
    parser.add_argument("--panel-dates", required=True, type=Path)
    parser.add_argument("--clock-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest_path = args.candidate_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    candidate_path = args.candidate_dir / "prior_market_context_candidates.parquet"
    if (manifest.get("contract") != "tw_futures_prior_market_context_candidate_v1"
            or manifest.get("training_admitted") is not False
            or manifest.get("outputs", {}).get(candidate_path.name) != sha256_file(candidate_path)):
        raise ValueError("prior-context candidate manifest or hash differs")
    for source in (args.frame, args.release, args.panel_dates):
        if manifest["inputs_sha256"].get(str(source.resolve())) != sha256_file(source):
            raise ValueError("prior-context source hash differs from candidate manifest")
    side = pl.read_parquet(candidate_path)
    keys = ["decision_date", "symbol"]
    if (side.height != manifest["candidate_rows"] or side.select(keys).is_duplicated().any()
            or side.filter(pl.col("context_date") >= pl.col("decision_date")).height):
        raise ValueError("prior-context candidate chronology or identity differs")
    dates = np.load(args.panel_dates, allow_pickle=False).astype("datetime64[D]")
    if dates.ndim != 1 or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError("panel date axis is invalid")
    calendar = pl.DataFrame({"decision_date": dates[1:], "context_date": dates[:-1]}).with_columns(
        pl.all().cast(pl.Date))
    if side.select("decision_date", "context_date").join(calendar, on=["decision_date", "context_date"],
                                                          how="anti").height:
        raise ValueError("prior-context source is not the immediately previous decision session")
    fields = ["underlying_symbol", "official_settlement", "settlement", *FUTURES_MODEL_FEATURE_COLUMNS]
    original = (pl.scan_parquet(args.frame).select(
        pl.col("date").alias("context_date"), "symbol", "physical_contract", *fields,
        "official_row_observed", "valuation_research_applied", "accounting_only_date").collect())
    original = original.rename({field: f"{field}_raw" for field in fields})
    matched = side.join(original, on=["context_date", "symbol", "physical_contract"],
                        how="left", validate="m:1")
    mismatch = matched.filter(
        pl.any_horizontal(~pl.col(field).eq_missing(pl.col(f"{field}_raw")) for field in fields)
        | ~pl.col("official_row_observed").fill_null(False)
        | pl.col("valuation_research_applied").fill_null(True)
        | pl.col("accounting_only_date").fill_null(True))
    if mismatch.height:
        raise ValueError(f"prior-context values or source flags differ on {mismatch.height} rows")
    release = (pl.scan_parquet(args.release)
               .filter(~pl.col("accounting_only_date"))
               .select("date", "symbol", "physical_contract", "same_contract_as_previous_session")
               .collect())
    current = release.select(pl.col("date").alias("decision_date"), "symbol", "physical_contract",
                             "same_contract_as_previous_session")
    checked = side.join(current, on=["decision_date", "symbol", "physical_contract"],
                        how="left", validate="m:1")
    if checked.filter(~pl.col("same_contract_as_previous_session").fill_null(False)).height:
        raise ValueError("prior context lacks the matching current physical identity")
    prior_release = release.select(pl.col("date").alias("context_date"), "symbol").unique()
    if side.join(prior_release, on=["context_date", "symbol"], how="semi").height:
        raise ValueError("prior context would overwrite an existing release feature row")
    del original, matched, checked, release, current, prior_release
    parent_release_sha = manifest["inputs_sha256"][str(args.release.resolve())]
    clocks = []
    for clock in ("0845", "0900"):
        for window in (1, 300):
            directory = (args.clock_root / clock if window == 1
                         else args.clock_root / "entry_window_300m" / clock)
            source, clock_manifest_path = directory / "entries.parquet", directory / "manifest.json"
            cm = json.loads(clock_manifest_path.read_text())
            source_sha = sha256_file(source)
            if (cm.get("entries_sha256") != source_sha or cm.get("source_daily_sha256") != parent_release_sha
                    or cm.get("clock", {}).get("window_minutes", 1) != window):
                raise ValueError("clock input does not match its manifest and daily release")
            src = (pl.scan_parquet(source).select("date", "symbol", "physical_contract", "status", "entry_volume")
                   .collect())
            joined = side.select(pl.col("decision_date").alias("date"), "symbol", "physical_contract").join(
                src, on=["date", "symbol", "physical_contract"], how="left", validate="m:1")
            observed = joined.filter(pl.col("status") == "observed_minute")
            at_least_one = observed.filter(pl.col("entry_volume") >= 1)
            at_least_two = observed.filter(pl.col("entry_volume") >= 2)
            def product_count(frame):
                return frame.select(pl.col("physical_contract").str.split(":").list.first().n_unique()).item()
            clocks.append({"decision_clock": clock, "window_minutes": window,
                "clock_manifest_sha256": sha256_file(clock_manifest_path), "entries_sha256": source_sha,
                "observed_window_rows": observed.height,
                "observed_window_volume_at_least_one": at_least_one.height,
                "observed_window_volume_at_least_two": at_least_two.height,
                "products_with_observed_volume_at_least_two": product_count(at_least_two)})
            if sha256_file(source) != source_sha:
                raise ValueError("clock source changed during independent readback")
    for source in (args.frame, args.release, args.panel_dates):
        if manifest["inputs_sha256"][str(source.resolve())] != sha256_file(source):
            raise ValueError("prior-context source changed during independent readback")
    payload = {"contract": "tw_futures_prior_market_context_independent_readback_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "candidate_manifest_sha256": sha256_file(manifest_path),
        "candidate_data_sha256": sha256_file(candidate_path),
        "auditor_sha256": sha256_file(Path(__file__)),
        "source_value_and_identity_rows_verified": side.height, "source_mismatches": 0,
        "existing_release_prior_overwrites": 0, "clock_window_upper_bounds": clocks,
        "training_admitted": False,
        "limitations": ["Counts are window-volume observations, not model eligibility, account capital, price-direction permissions or actual fills.",
            "100% participation cannot establish that the model could obtain the observed market volume.",
            "The source release is retrospectively selected; historical listing and publication times are not admitted."]}
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
