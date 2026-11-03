#!/usr/bin/env python3
"""Audit a factorized feature history against two observed futures clocks.

This checks identities and calendar alignment only. It neither changes the
training panel nor admits execution, account-calendar or point-in-time data.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file
import stockagent.data.factorized_panel as factorized_panel
from stockagent.data.factorized_panel import (
    SUPPORTED_CONTRACTS,
    _factorized_date_slice_offset,
    factorized_member_proofs,
    verify_factorized_members,
)


def _dates(values: list[str], label: str) -> list[str]:
    if not isinstance(values, list) or not values:
        raise ValueError(f"{label} dates are empty")
    try:
        normalized = [date.fromisoformat(value).isoformat() for value in values]
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{label} dates are invalid") from exc
    if normalized != values or normalized != sorted(set(normalized)):
        raise ValueError(f"{label} dates must be unique, increasing ISO dates")
    return normalized


def _feature_axis(manifest: dict, label: str) -> tuple[list[str], list[str], list[str]]:
    if (manifest.get("status") != "complete"
            or manifest.get("contract") not in SUPPORTED_CONTRACTS
            or manifest.get("feature_lag") != 1
            or manifest.get("research_only") is not True
            or manifest.get("historical_point_in_time") is not False):
        raise ValueError(f"{label} feature manifest has an unexpected contract")
    dates = _dates(manifest.get("dates"), label)
    symbols = manifest.get("symbols")
    if (not isinstance(symbols, list) or not symbols
            or any(not isinstance(item, str) or not item for item in symbols)
            or len(symbols) != len(set(symbols))):
        raise ValueError(f"{label} feature symbols are invalid")
    names = []
    for field in ("base_feature_names", "individual_channels", "common_channels"):
        column = manifest.get(field)
        if not isinstance(column, list) or any(not isinstance(item, str) or not item for item in column):
            raise ValueError(f"{label} feature names are invalid")
        names.extend(column)
    if not names or len(names) != len(set(names)):
        raise ValueError(f"{label} feature names are duplicated or empty")
    return dates, symbols, names


def analyze_alignment(new: dict, old: dict, decision_dates: list[str], *,
                      lookback: int, feature_lag: int) -> dict:
    """Verify row ownership and return the first decision's actual causal window."""
    new_dates, new_symbols, new_names = _feature_axis(new, "new")
    old_dates, old_symbols, old_names = _feature_axis(old, "old")
    decisions = _dates(decision_dates, "decision")
    if new_symbols != old_symbols:
        raise ValueError("old and new feature symbol identities or order differ")
    old_offset = _factorized_date_slice_offset(new_dates, old_dates)
    offset = _factorized_date_slice_offset(new_dates, decisions)
    if lookback < 1 or feature_lag < 1:
        raise ValueError("lookback and the execution feature lag must be positive")
    end = offset + 1 - feature_lag
    start = end - lookback
    if start < 0 or end > offset or start >= end:
        raise ValueError("first decision lacks the required strictly prior feature history")
    first_window = new_dates[start:end]
    if len(first_window) != lookback or first_window[-1] >= decisions[0]:
        raise ValueError("feature window is not strictly before the first decision")
    return {
        "feature_dates": len(new_dates), "old_feature_dates": len(old_dates),
        "feature_symbols": len(new_symbols), "decision_dates": len(decisions),
        "feature_precontext_sessions": offset,
        "old_feature_offset_in_new": old_offset,
        "first_decision": decisions[0], "last_decision": decisions[-1],
        "lookback": lookback, "execution_feature_lag_sessions": feature_lag,
        "first_decision_feature_window": first_window,
        "old_feature_count": len(old_names), "new_feature_count": len(new_names),
        "removed_feature_channels": sorted(set(old_names) - set(new_names)),
        "added_feature_channels": sorted(set(new_names) - set(old_names)),
        "same_ordered_feature_schema": old_names == new_names,
    }


def _account_calendar(path: Path, start: str, end: str, expected_sha256: str) -> tuple[pl.DataFrame, str]:
    """Read the financial source's explicit market/account-only classification."""
    digest = sha256_file(path)
    if digest != expected_sha256:
        raise ValueError("clock financial source SHA-256 differs from the daily source")
    days = (pl.scan_parquet(path)
            .filter(pl.col("date").cast(pl.String).is_between(pl.lit(start), pl.lit(end), closed="both"))
            .select(pl.col("date").cast(pl.String), "accounting_only_date")
            .unique().sort("date").collect())
    if (not days.height or days["date"].is_duplicated().any()
            or days["accounting_only_date"].null_count()):
        raise ValueError("financial source has an ambiguous account calendar")
    _dates(days["date"].to_list(), "financial source")
    if sha256_file(path) != digest:
        raise ValueError("financial source changed during the audit")
    return days, digest


def _classify_clock_dates(frame: pl.DataFrame, account: pl.DataFrame) -> tuple[list[str], list[dict]]:
    """Exclude only explicit account nodes with no executable clock rows."""
    if (frame["date"].is_duplicated().any() or account["date"].is_duplicated().any()
            or account["accounting_only_date"].null_count()):
        raise ValueError("clock or account date classifications are ambiguous")
    joined = frame.join(account, on="date", how="full", coalesce=True, validate="1:1").sort("date")
    if joined["accounting_only_date"].null_count() or joined["rows"].null_count():
        raise ValueError("clock and financial source contain different dates")
    only = joined.filter(pl.col("accounting_only_date"))
    if only.filter((pl.col("bad_volume") > 0) | (pl.col("bad_status") > 0)
                   | (pl.col("present_quotes") > 0)).height:
        raise ValueError("account-only clock date unexpectedly contains executable data")
    decisions = joined.filter(~pl.col("accounting_only_date"))["date"].to_list()
    _dates(decisions, "decision")
    return decisions, only.select("date", "rows").to_dicts()


def _clock(path: Path, expected_time: str) -> tuple[dict, pl.DataFrame, dict[str, str]]:
    manifest_path = path / "manifest.json"
    entry_path = path / "entries.parquet"
    pins = {str(manifest_path): sha256_file(manifest_path),
            str(entry_path): sha256_file(entry_path)}
    manifest = json.loads(manifest_path.read_text())
    clock = manifest.get("clock", {})
    if (manifest.get("status") != "complete"
            or clock.get("decision_time") != expected_time
            or clock.get("feature_clock") != "completed_prior_session_zero_current_cash_open_gap"
            or manifest.get("entries_sha256") != pins[str(entry_path)]):
        raise ValueError(f"{expected_time} clock source or prior-session policy is invalid")
    frame = (pl.scan_parquet(entry_path).group_by("date").agg(
        pl.len().alias("rows"),
        ((pl.col("entry_volume").is_null()) | (~pl.col("entry_volume").is_finite())
         | (pl.col("entry_volume") != 0)).sum().alias("bad_volume"),
        (pl.col("status").is_null() | (pl.col("status") != "daily_not_executable"))
            .sum().alias("bad_status"),
        pl.col("entry_quote").is_not_null().sum().alias("present_quotes"),
    ).with_columns(pl.col("date").cast(pl.String)).sort("date").collect())
    days = _dates(frame["date"].to_list(), expected_time)
    if days[0] != manifest.get("start_date") or days[-1] != manifest.get("end_date"):
        raise ValueError(f"{expected_time} clock date horizon differs from its entries")
    if any(sha256_file(Path(name)) != digest for name, digest in pins.items()):
        raise ValueError(f"{expected_time} source changed during the audit")
    return manifest, frame, pins


def audit(new_path: Path, old_path: Path, clock_0845: Path, clock_0900: Path, daily_path: Path,
          *, lookback: int, feature_lag: int) -> dict:
    audit_code = Path(__file__).resolve()
    canonical_verifier = Path(factorized_panel.__file__).resolve()
    pins = {str(path): sha256_file(path)
            for path in (new_path, old_path, audit_code, canonical_verifier)}
    new = json.loads(new_path.read_text())
    old = json.loads(old_path.read_text())
    # Use the same immutable-member verifier as the canonical panel loader.
    verify_factorized_members(new_path.parent, new)
    first, first_frame, first_pins = _clock(clock_0845, "08:45")
    second, second_frame, second_pins = _clock(clock_0900, "09:00")
    if (first.get("source_daily_sha256") != second.get("source_daily_sha256")
            or first.get("source_rules_sha256") != second.get("source_rules_sha256")):
        raise ValueError("08:45 and 09:00 are not bound to the same financial sources")
    if first.get("start_date") != second.get("start_date") or first.get("end_date") != second.get("end_date"):
        raise ValueError("08:45 and 09:00 clock horizons differ")
    account, daily_sha = _account_calendar(daily_path, first["start_date"], first["end_date"],
                                           first.get("source_daily_sha256"))
    first_dates, first_account = _classify_clock_dates(first_frame, account)
    second_dates, second_account = _classify_clock_dates(second_frame, account)
    if first_dates != second_dates or first_account != second_account:
        raise ValueError("08:45 and 09:00 decision or account calendars differ")
    result = analyze_alignment(new, old, first_dates, lookback=lookback, feature_lag=feature_lag)
    pins.update(first_pins)
    pins.update(second_pins)
    pins[str(daily_path)] = daily_sha
    if any(sha256_file(Path(name)) != digest for name, digest in pins.items()):
        raise ValueError("an input source changed during the audit")
    return {
        "contract": "tw_futures_feature_precontext_alignment_audit_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), **result,
        "new_feature_members_sha256_verified": len(factorized_member_proofs(new)),
        "clock_feature_policy": first["clock"]["feature_clock"],
        "explicit_account_only_clock_dates": first_account,
        "input_sha256": pins,
        "limitations": [
            "Calendar alignment does not prove historical point-in-time feature availability.",
            "A verified feature window is not an account day, fill, price or position.",
            "Feature-only history is not yet connected to the canonical training executor.",
            "Financial source, holiday accounting and training admission require separate evidence.",
        ],
        "financial_or_training_adopted": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--new-manifest", type=Path, required=True)
    parser.add_argument("--old-manifest", type=Path, required=True)
    parser.add_argument("--clock-0845", type=Path, required=True)
    parser.add_argument("--clock-0900", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--lookback", type=int, required=True)
    parser.add_argument("--feature-lag", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    target = args.output.resolve()
    if not target.is_relative_to((Path.cwd() / "artifacts/markets").resolve()):
        parser.error("output must be in artifacts/markets under the workspace")
    report = audit(args.new_manifest.resolve(), args.old_manifest.resolve(),
                   args.clock_0845.resolve(), args.clock_0900.resolve(),
                   args.daily.resolve(),
                   lookback=args.lookback, feature_lag=args.feature_lag)
    with target.open("x", encoding="utf-8") as out:
        json.dump(report, out, ensure_ascii=False, indent=2)
        out.write("\n")
    print(json.dumps({key: report[key] for key in (
        "first_decision", "last_decision", "decision_dates", "feature_precontext_sessions",
        "same_ordered_feature_schema", "financial_or_training_adopted")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
