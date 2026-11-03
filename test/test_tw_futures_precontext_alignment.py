from __future__ import annotations

from copy import deepcopy

import pytest
import polars as pl

from downloader.artifact_io import sha256_file
from scripts.audit_tw_futures_precontext_alignment import (
    _account_calendar, _classify_clock_dates, analyze_alignment,
)


def manifest(dates, *, symbols=None, channels=None):
    return {
        "contract": "tw_factorized_nullable_panel_values_lag1_v2",
        "status": "complete", "feature_lag": 1, "research_only": True,
        "historical_point_in_time": False,
        "dates": dates, "symbols": symbols or ["A", "B"],
        "base_feature_names": ["base"], "individual_channels": channels or ["x"],
        "common_channels": ["common"],
    }


def inputs():
    days = ["2010-12-28", "2010-12-29", "2010-12-30", "2010-12-31",
            "2011-01-03", "2011-01-04", "2011-01-05"]
    return manifest(days), manifest(days[4:], channels=["old"]), days[4:]


def test_first_decision_uses_only_the_real_preceding_feature_sessions():
    new, old, days = inputs()
    result = analyze_alignment(new, old, days, lookback=3, feature_lag=1)
    assert result["feature_precontext_sessions"] == 4
    assert result["first_decision_feature_window"] == [
        "2010-12-29", "2010-12-30", "2010-12-31"]
    assert result["decision_dates"] == 3
    assert result["added_feature_channels"] == ["x"]
    assert result["removed_feature_channels"] == ["old"]
    assert result["same_ordered_feature_schema"] is False


@pytest.mark.parametrize("mutation", [
    "missing_decision", "duplicate_decision", "reordered_decision", "missing_history",
    "misordered_symbols", "duplicate_channels", "wrong_manifest_lag",
])
def test_misaligned_or_ambiguous_sources_fail_closed(mutation):
    new, old, days = inputs()
    if mutation == "missing_decision":
        days = [days[0], days[2]]
    elif mutation == "duplicate_decision":
        days = [days[0], days[0], days[2]]
    elif mutation == "reordered_decision":
        days = days[::-1]
    elif mutation == "missing_history":
        new["dates"] = new["dates"][3:]
    elif mutation == "misordered_symbols":
        old["symbols"] = ["B", "A"]
    elif mutation == "duplicate_channels":
        new["common_channels"] = ["base"]
    elif mutation == "wrong_manifest_lag":
        new["feature_lag"] = 0
    with pytest.raises(ValueError):
        analyze_alignment(new, old, days, lookback=3, feature_lag=1)


def test_noncanonical_dates_and_same_day_features_are_rejected():
    new, old, days = inputs()
    with pytest.raises(ValueError):
        analyze_alignment(deepcopy(new), deepcopy(old), days, lookback=3, feature_lag=0)
    days[0] = "2011-1-03"
    with pytest.raises(ValueError):
        analyze_alignment(new, old, days, lookback=3, feature_lag=1)


def test_lag_two_moves_the_entire_window_back_one_observed_session():
    new, old, days = inputs()
    result = analyze_alignment(new, old, days, lookback=3, feature_lag=2)
    assert result["first_decision_feature_window"] == [
        "2010-12-28", "2010-12-29", "2010-12-30"]


def test_account_only_node_is_not_a_feature_or_decision_day():
    frame = pl.DataFrame({"date": ["2011-01-03", "2011-01-04", "2011-01-05"],
                          "rows": [2, 3, 2], "bad_volume": [1, 0, 1],
                          "bad_status": [1, 0, 1], "present_quotes": [1, 0, 1]})
    account = pl.DataFrame({"date": frame["date"],
                            "accounting_only_date": [False, True, False]})
    decisions, account_nodes = _classify_clock_dates(frame, account)
    assert decisions == ["2011-01-03", "2011-01-05"]
    assert account_nodes == [{"date": "2011-01-04", "rows": 3}]


@pytest.mark.parametrize("problem", ["missing", "ambiguous", "volume", "status", "quote"])
def test_account_nodes_are_never_silently_promoted_to_market_days(problem):
    frame = pl.DataFrame({"date": ["2011-01-03", "2011-01-04"],
                          "rows": [1, 1], "bad_volume": [0, 0],
                          "bad_status": [0, 0], "present_quotes": [0, 0]})
    account = pl.DataFrame({"date": frame["date"], "accounting_only_date": [False, True]})
    if problem == "missing":
        account = account.head(1)
    elif problem == "ambiguous":
        account = pl.concat([account, account.tail(1)])
    else:
        column = {"volume": "bad_volume", "status": "bad_status", "quote": "present_quotes"}[problem]
        frame = frame.with_columns(pl.when(pl.col("date") == "2011-01-04")
                                   .then(1).otherwise(pl.col(column)).alias(column))
    with pytest.raises(ValueError):
        _classify_clock_dates(frame, account)


def test_financial_calendar_readback_filters_dates_and_pins_bytes(tmp_path):
    path = tmp_path / "daily.parquet"
    pl.DataFrame({"date": ["2010-12-31", "2011-01-03", "2011-01-03", "2011-01-04"],
                  "accounting_only_date": [False, False, False, True]}).write_parquet(path)
    digest = sha256_file(path)
    days, actual = _account_calendar(path, "2011-01-03", "2011-01-04", digest)
    assert actual == digest
    assert days.to_dicts() == [{"date": "2011-01-03", "accounting_only_date": False},
                               {"date": "2011-01-04", "accounting_only_date": True}]
    with pytest.raises(ValueError, match="SHA-256"):
        _account_calendar(path, "2011-01-03", "2011-01-04", "0" * 64)
