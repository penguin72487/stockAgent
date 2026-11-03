#!/usr/bin/env python3
"""Inspect all independent holiday gates against a pinned futures daily source.

This is a diagnostic, not a holiday-source or training-admission authority.
It does not change the canonical fail-closed attachment or create valuations.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left
from datetime import datetime, timezone
import json
import math
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data import tw_futures_account_attachment as attachment


def _gate(call) -> dict:
    try:
        call()
    except ValueError as error:
        return {"state": "rejected", "reason": str(error)}
    return {"state": "passed"}


def _prior_market_settlement_comparison(frame: pl.LazyFrame, candidates: pl.LazyFrame) -> dict | None:
    """Compare the same physical contract to the nearest recorded market day.

    This diagnostic never infers the applicable holiday rule or price
    availability, and it does not certify that the research calendar is a
    complete exchange calendar. Unmatched generations and source gaps remain
    unknown instead of being filled from a different contract or future date.
    """
    needed = {"date", "product", "contract", "physical_instance", "asset_class",
              "accounting_only_date", "cash_settlement", "valuation_price",
              "official_settlement", "official_source_sha256"}
    if not needed <= set(frame.collect_schema().names()):
        return None
    account = candidates.filter(pl.col("cash_settlement") == False).select(  # noqa: E712
        "date", "product", "contract", "physical_instance", "asset_class", "valuation_price").collect()
    if account.select("date", "physical_instance").is_duplicated().any():
        raise ValueError("holiday candidate contains duplicate physical contract dates")
    account_days = account["date"].unique().sort().to_list()
    market_days = (frame.filter(pl.col("accounting_only_date") == False)  # noqa: E712
                   .select("date").unique().sort("date").collect()["date"].to_list())
    prior_dates = []
    for day in account_days:
        pos = bisect_left(market_days, day)
        prior_dates.append({"date": day, "prior_market_date": market_days[pos - 1] if pos else None})
    mapping = pl.DataFrame(prior_dates, schema={"date": pl.Date, "prior_market_date": pl.Date})
    preceding = [row["prior_market_date"] for row in prior_dates if row["prior_market_date"] is not None]
    prior = (frame.filter((pl.col("accounting_only_date") == False) & pl.col("date").is_in(preceding))  # noqa: E712
             .select(pl.col("date").alias("prior_market_date"), "physical_instance",
                     "official_settlement", "official_source_sha256", pl.lit(True).alias("_prior_present"))
             .collect())
    if prior.select("prior_market_date", "physical_instance").is_duplicated().any():
        raise ValueError("research market date contains duplicate physical contracts")
    joined = (account.join(mapping, on="date", how="left", validate="m:1")
              .join(prior, on=["prior_market_date", "physical_instance"], how="left", validate="m:1")
              .with_columns(pl.col("official_settlement").cast(pl.String).str.replace_all(",", "")
                            .cast(pl.Float64, strict=False).alias("prior_official_price")))
    valid_price = pl.col("prior_official_price").is_finite() & (pl.col("prior_official_price") > 0)
    valid_source = pl.col("official_source_sha256").cast(pl.String).str.contains(r"^[0-9a-f]{64}$")
    status = (pl.when(pl.col("valuation_price").is_null() | ~pl.col("valuation_price").is_finite())
              .then(pl.lit("unknown_candidate_value"))
              .when(pl.col("prior_market_date").is_null()).then(pl.lit("no_prior_research_market_date"))
              .when(pl.col("_prior_present").is_null()).then(pl.lit("same_physical_prior_row_missing"))
              .when(~valid_source.fill_null(False)).then(pl.lit("prior_source_hash_missing"))
              .when(~valid_price.fill_null(False)).then(pl.lit("prior_official_settlement_unknown"))
              .when(pl.col("valuation_price") == pl.col("prior_official_price")).then(pl.lit("equal"))
              .when((pl.col("valuation_price") - pl.col("prior_official_price")).abs()
                    <= (4 * np.finfo(np.float64).eps) * pl.max_horizontal(
                        pl.col("valuation_price").abs(), pl.col("prior_official_price").abs(), pl.lit(1.0)))
              .then(pl.lit("equal_within_fp64_roundoff"))
              .otherwise(pl.lit("different")))
    joined = joined.with_columns(status.alias("comparison"))
    groups = (joined.group_by("date", "prior_market_date", "asset_class", "comparison")
              .agg(pl.len().alias("candidate_rows"), pl.col("product").n_unique().alias("product_codes"))
              .sort("date", "asset_class", "comparison").to_dicts())
    gold = (joined.filter((pl.col("product") == "TGF")
                          & pl.col("date").cast(pl.String).is_in(["2018-02-13", "2018-02-14"]))
            .select("date", "prior_market_date", "contract", "physical_instance", "valuation_price",
                    "official_settlement", "official_source_sha256", "comparison")
            .sort("date", "contract").to_dicts())
    exceptions = (joined.filter(pl.col("comparison") != "equal")
                  .select("date", "prior_market_date", "product", "contract", "physical_instance",
                          "valuation_price", "official_settlement", "official_source_sha256", "comparison")
                  .sort("date", "product", "contract").to_dicts())
    for row in (*groups, *gold, *exceptions):
        for key in ("date", "prior_market_date"):
            row[key] = str(row[key]) if row[key] is not None else None
    return {"scope": "nonterminal candidate rows compared to nearest preceding date in the research calendar",
            "roundoff_rule": "absolute difference <= 4 * float64 epsilon * max(abs(values), 1); diagnostic only",
            "rows_by_date_asset_and_comparison": groups, "non_equal_details": exceptions, "tgf_2018_detail": gold,
            "historical_broker_mark_verified": False, "training_admitted": False}


def audit(daily_path: Path, start: str, end: str, *, research_dir: Path | None = None,
          night_readback: Path | None = None) -> dict:
    daily_path = daily_path.resolve(strict=True)
    manifest_path = daily_path.with_name("manifest.json")
    attachment_path = Path(attachment.__file__).resolve(strict=True)
    paths = [daily_path, manifest_path, attachment_path, Path(__file__).resolve(strict=True)]
    before = {str(path): sha256_file(path) for path in paths}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (manifest.get("status") != "complete"
            or manifest.get("outputs", {}).get("continuous_daily", {}).get("sha256")
            != before[str(daily_path)]):
        raise ValueError("daily file and source manifest do not match")
    try:
        dates = np.asarray([start, end], dtype="datetime64[D]")
    except ValueError as error:
        raise ValueError("audit needs valid start and end dates") from error
    if (np.isnat(dates).any() or dates[0] > dates[1]
            or str(dates[0]) != start or str(dates[1]) != end):
        raise ValueError("audit needs an ordered finite date range")

    frame = pl.scan_parquet(daily_path)
    required = {"date", "product", "accounting_only_date", "cash_settlement", "executable"}
    if not required <= set(frame.collect_schema().names()):
        raise ValueError("daily file does not contain the holiday audit fields")
    scoped = frame.filter(
        pl.col("date").cast(pl.String).is_between(pl.lit(start), pl.lit(end), closed="both")
    )
    days = scoped.select("date", "accounting_only_date").unique().sort("date").collect()
    if not days.height or days["date"].is_duplicated().any() or days["accounting_only_date"].null_count():
        raise ValueError("daily source has missing or ambiguous account-date classification")
    grouped = (scoped.filter(pl.col("accounting_only_date"))
               .group_by("date").agg(
                   pl.len().alias("candidate_contract_rows"),
                   (pl.col("cash_settlement") == True).sum().alias("cash_settlement_rows"),  # noqa: E712
                   (pl.col("cash_settlement") == False).sum().alias("nonterminal_rows"),  # noqa: E712
                   pl.col("cash_settlement").null_count().alias("unknown_settlement_rows"),
                   (pl.col("executable") == True).sum().alias("executable_rows"),  # noqa: E712
                   pl.col("executable").null_count().alias("unknown_execution_rows"),
               ).sort("date").collect())
    records = [{**row, "date": str(row["date"])} for row in grouped.to_dicts()]
    expected = [day for day in attachment._KNOWN_BROKER_HOLIDAY_ACCOUNT_DATES
                if start <= day <= end]
    by_day = {str(row["date"]): bool(row["accounting_only_date"]) for row in days.to_dicts()}
    missing = [day for day in expected if day not in by_day]
    conflicting = [day for day in expected if day in by_day and not by_day[day]]
    early = []
    for previous, following in (("2011-01-28", "2011-02-08"),
                                ("2012-01-18", "2012-01-30")):
        if start <= previous and following <= end:
            early.append({"preceding_market_date": previous, "following_market_date": following,
                          **_gate(lambda: attachment._reject_unreviewed_early_holiday_calendars(
                              np.asarray([previous, following], dtype="datetime64[D]")))})
    gates = {
        "early_holidays": early,
        "known_account_dates": _gate(lambda: attachment._reject_missing_holiday_account_dates(days, dates)),
        "nonterminal_valuations": _gate(lambda: attachment._reject_unreviewed_holiday_valuations(daily_path, dates)),
    }
    research = None
    if research_dir is not None:
        root = research_dir.resolve(strict=True)
        stage_path, checkpoint_path, frame_path = (root / "accounting_stage.json",
                                                    root / "accounting_checkpoint.json",
                                                    root / "frame.parquet")
        for path in (stage_path, checkpoint_path, frame_path):
            before[str(path)] = sha256_file(path)
            paths.append(path)
        stage = json.loads(stage_path.read_text(encoding="utf-8"))
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if (stage.get("status") != "holiday_account_dates_research_checkpoint_not_published"
                or stage.get("financial_release_accepted") is not False
                or stage.get("holiday_valuation_complete") is not False
                or stage.get("training_admitted") is not False
                or stage.get("checkpoint_sha256") != before[str(checkpoint_path)]
                or checkpoint.get("outputs", {}).get("frame.parquet") != before[str(frame_path)]):
            raise ValueError("research checkpoint, status or frame identity does not match")
        full_research_frame = pl.scan_parquet(frame_path)
        candidate = full_research_frame
        if not {"date", "accounting_only_date"} <= set(candidate.collect_schema().names()):
            raise ValueError("research frame lacks account dates")
        candidate = candidate.filter(pl.col("accounting_only_date")
                    & pl.col("date").cast(pl.String).is_between(pl.lit(start), pl.lit(end), closed="both"))
        candidate_days = candidate.select("date").unique().sort("date").collect()
        values = [str(day) for day in candidate_days["date"].to_list()]
        valuation_scope_fields = {"product", "asset_class", "cash_settlement", "valuation_price",
                                  "executable", "valuation_research_applied"}
        valuation_scope = None
        if valuation_scope_fields <= set(candidate.collect_schema().names()):
            groups = (candidate.group_by("asset_class", "cash_settlement").agg(
                pl.len().alias("candidate_rows"),
                pl.col("product").n_unique().alias("candidate_product_codes"),
                pl.col("valuation_price").null_count().alias("null_valuation_rows"),
                ((~pl.col("valuation_price").is_finite()).fill_null(False)).sum()
                    .alias("nonfinite_nonnull_valuation_rows"),
                (pl.col("executable") == True).sum().alias("executable_rows"),  # noqa: E712
                pl.col("executable").null_count().alias("unknown_execution_rows"),
                (pl.col("valuation_research_applied") == True).sum().alias("research_applied_rows"),  # noqa: E712
                pl.col("valuation_research_applied").null_count().alias("unknown_research_applied_rows"),
            ).sort("asset_class", "cash_settlement").collect())
            valuation_scope = groups.to_dicts()
        previous_mark_scope = None
        if {"cash_settlement", "valuation_price", "previous_settlement"} <= set(candidate.collect_schema().names()):
            previous = candidate.filter(pl.col("cash_settlement") == False)  # noqa: E712
            previous_mark_scope = (previous.group_by(pl.col("date").dt.year().alias("year")).agg(
                pl.len().alias("candidate_nonterminal_rows"),
                (pl.col("valuation_price") == pl.col("previous_settlement")).fill_null(False).sum()
                    .alias("equal_previous_settlement_rows"),
                (pl.col("valuation_price") != pl.col("previous_settlement")).fill_null(False).sum()
                    .alias("different_previous_settlement_rows"),
                (pl.col("valuation_price").is_null() | pl.col("previous_settlement").is_null()).sum()
                    .alias("unknown_comparison_rows"),
            ).sort("year").collect().to_dicts())
        research = {
            "status": stage["status"],
            "candidate_account_only_dates": values,
            "known_dates_not_in_research_frame": [day for day in expected if day not in values],
            "v22_missing_dates_present_in_research_frame": [day for day in missing if day in values],
            "candidate_valuation_scope_by_asset_and_settlement": valuation_scope,
            "candidate_nonterminal_previous_settlement_comparison_by_year": previous_mark_scope,
            "same_physical_prior_market_settlement_comparison": _prior_market_settlement_comparison(
                full_research_frame, candidate),
            "holiday_valuation_complete": False,
            "financial_release_accepted": False,
            "training_admitted": False,
        }
    night_comparison = None
    if night_readback is not None:
        if research_dir is None:
            raise ValueError("night comparison requires the pinned research checkpoint")
        night_path = night_readback.resolve(strict=True)
        before[str(night_path)] = sha256_file(night_path)
        paths.append(night_path)
        observed = json.loads(night_path.read_text(encoding="utf-8"))
        original_rows = observed.get("physical_rows")
        if (observed.get("contract") != "holiday_night_session_corrected_readback_v3"
                or observed.get("remote_financial_parent_sha256") != before[str(daily_path)]
                or not isinstance(original_rows, list)
                or observed.get("physical_parent_count") != len(original_rows)):
            raise ValueError("night readback contract, parent or row count differs")
        needed = {"date", "physical_instance", "valuation_price", "cash_settlement"}
        if not needed <= set(candidate.collect_schema().names()):
            raise ValueError("research frame lacks the physical night-comparison identity")
        physical = candidate.select(*sorted(needed)).collect()
        if physical.select("date", "physical_instance").is_duplicated().any():
            raise ValueError("research frame has ambiguous physical contract dates")
        lookup = {(str(row["date"]), row["physical_instance"]): row for row in physical.to_dicts()}
        comparisons = []
        for row in original_rows:
            if row.get("source_status") != "official_and_unique_finmind_match":
                continue
            parent, observation = row["parent"], row["observation"]
            official = observation["official"]["close"]
            tick = observation["finmind_last_tick"]
            if (parent["product"] != observation["product"]
                    or parent["contract"] != observation["contract"]
                    or observation.get("exact_close_match") is not True
                    or tick.get("unique_price_at_last_timestamp") is not True
                    or tick.get("prices") != [official]
                    or not isinstance(official, (int, float)) or not math.isfinite(official)):
                raise ValueError("night readback does not support its exact-match row")
            for day in row["account_dates"]:
                if not start <= day <= end:
                    continue
                key = (day, parent["physical_instance"])
                matched = lookup.get(key)
                price = matched["valuation_price"] if matched else None
                status = ("absent_research_row" if matched is None else
                          "unknown_research_valuation" if price is None or not math.isfinite(price) else
                          "equal" if price == official else "different")
                comparisons.append({"date": day, "physical_instance": key[1],
                                    "research_valuation": price, "observed_night_close": official,
                                    "research_cash_settlement": matched["cash_settlement"] if matched else None,
                                    "comparison": status})
        keys = [(row["date"], row["physical_instance"]) for row in comparisons]
        if len(set(keys)) != len(keys):
            raise ValueError("night readback contains duplicate physical contract dates")
        night_comparison = {
            "compared_source": "existing_cross_source_readback_not_new_source_calls",
            "rows": sorted(comparisons, key=lambda row: (row["date"], row["physical_instance"])),
            "historical_broker_mark_verified": False,
            "training_admitted": False,
        }
    if any(sha256_file(path) != before[str(path)] for path in paths):
        raise ValueError("daily, manifest or attachment changed during the audit")
    return {
        "contract": "tw_futures_holiday_gate_coverage_diagnostic_v2",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "start_date": start, "end_date": end,
        "inputs_sha256": before,
        "known_account_dates_expected": expected,
        "known_account_dates_missing": missing,
        "known_account_dates_conflicting": conflicting,
        "account_only_dates": records,
        "canonical_gates": gates,
        "existing_research_checkpoint": research,
        "night_readback_comparison": night_comparison,
        "scope": "existing source rows and independent canonical guard results; not held positions",
        "historical_source_or_broker_review_complete": False,
        "training_admitted": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", required=True, type=Path)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--research-dir", type=Path)
    parser.add_argument("--night-readback", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("refusing to overwrite an existing diagnostic receipt")
    result = audit(args.daily, args.start, args.end, research_dir=args.research_dir,
                   night_readback=args.night_readback)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "inputs_sha256": result["inputs_sha256"],
                      "account_only_dates": len(result["account_only_dates"]),
                      "known_dates_missing": len(result["known_account_dates_missing"]),
                      "canonical_gates": result["canonical_gates"],
                      "training_admitted": False}, ensure_ascii=False))


if __name__ == "__main__":
    main()
