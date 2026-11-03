from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest

from scripts.audit_tw_futures_core_attachment import entry_capacity_gates, summarize_masks, summarize_products


def test_reports_overlap_separately_from_either_mask_and_by_year():
    calendar = SimpleNamespace(
        dates=np.array(["2019-12-31", "2020-01-01", "2020-01-02"], dtype="datetime64[D]"),
        account_only_mask=np.array([False, True, False]),
    )
    candidate = np.array([[True, True, False], [False, False, False], [False, True, False]])
    executable = np.array([[True, False, True], [False, False, False], [False, False, False]])
    capacity = np.array([[False, False, False], [False, False, False], [False, False, False]])
    result = summarize_masks(calendar, candidate, executable, capacity, capacity)
    assert result["candidate_and_executable_positions"] == 1
    assert result["candidate_without_executable_positions"] == 2
    assert result["executable_without_candidate_positions"] == 1
    assert result["joint_without_one_contract_capacity_positions"] == 1
    assert result["market_sessions_without_candidate_and_executable"] == 1
    assert result["joint_positions_per_market_session"] == {"minimum": 0, "median": 0.5, "maximum": 1}
    assert [(row["year"], row["candidate_and_executable_positions"])
            for row in result["per_calendar_year"]] == [(2019, 1), (2020, 0)]


def test_rejects_candidates_on_account_only_days_and_misaligned_axes():
    calendar = SimpleNamespace(dates=np.array(["2020-01-01"], dtype="datetime64[D]"),
                               account_only_mask=np.array([True]))
    with pytest.raises(ValueError, match="account-only"):
        summarize_masks(calendar, np.array([[True]]), np.array([[False]]), np.array([[False]]), np.array([[False]]))
    with pytest.raises(ValueError, match="share a Boolean"):
        summarize_masks(calendar, np.array([[False]]), np.array([[0]]), np.array([[False]]), np.array([[False]]))


def test_maps_reused_slots_to_dated_products_and_rejects_ambiguous_identity():
    calendar = SimpleNamespace(dates=np.array(["2020-01-02", "2020-01-03"], dtype="datetime64[D]"))
    candidate = np.array([[True, False], [True, True]])
    executable = np.array([[True, True], [True, False]])
    source = pl.DataFrame({
        "date": ["2020-01-02", "2020-01-02", "2020-01-03", "2020-01-03"],
        "symbol": ["TAIFEX_SLOT_0001", "TAIFEX_SLOT_0002", "TAIFEX_SLOT_0001", "TAIFEX_SLOT_0002"],
        "product": ["A", "B", "C", "B"],
        "asset_class": ["stock_future", "index_future", "stock_future", "index_future"],
        "daily_executable": [True, True, True, True],
        "status": ["observed_minute", "observed_minute", "observed_minute", "verified_no_window_trade"],
    }).with_columns(pl.col("date").str.to_date())
    direction_gate = np.array([[True, False], [False, False]])
    result = summarize_products(calendar, candidate, executable, direction_gate, source)
    assert result["products_with_candidate_and_executable"] == 2
    assert result["products_with_necessary_entry_gates"] == 1
    assert [(row["asset_class"], row["products_with_candidate_and_executable"])
            for row in result["by_asset_class"]] == [("index_future", 0), ("stock_future", 2)]
    missing = result["products_with_daily_executable_but_no_candidate_and_clock_executable"]
    assert len(missing) == 1 and missing[0]["product"] == "B"
    assert missing[0]["clock_statuses_on_daily_executable_dates"] == ["observed_minute", "verified_no_window_trade"]
    with pytest.raises(ValueError, match="duplicate"):
        summarize_products(calendar, candidate, executable, direction_gate, pl.concat([source, source.head(1)]))


def test_one_contract_gate_requires_capacity_and_a_permitted_direction():
    candidate = np.ones((1, 5), dtype=bool)
    executable = np.array([[True, True, True, True, False]])
    tape = np.zeros((1, 5, 29), dtype=float)
    tape[0, :, 8] = [0, 1, 1, np.nan, 5]
    tape[0, :, 18] = [1, 1, 0, 1, 1]
    tape[0, :, 19] = [0, 0, 0, 1, 1]
    capacity, direction = entry_capacity_gates(candidate, executable, tape)
    assert capacity.tolist() == [[False, True, True, False, False]]
    assert direction.tolist() == [[False, True, False, False, False]]
    with pytest.raises(ValueError, match="matching margin execution tape"):
        entry_capacity_gates(candidate, executable, tape[..., :10])
