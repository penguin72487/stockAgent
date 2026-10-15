from __future__ import annotations

from datetime import datetime, timezone
import random

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from stockagent.live import taifex_history_column_cache as cache
from stockagent.live import taifex_history_selection as selection
from stockagent.live import taifex_volatility_dashboard as dashboard
from test_taifex_volatility_dashboard import _fixture


def frame(rows):
    return pl.DataFrame(rows, schema={
        "strategy_id": pl.String, "__bucket": pl.Int64,
        "decision_ts_ns": pl.Int64, "__selection_return": pl.Float64,
        "valuation_available": pl.Boolean, "valuation_carried_forward": pl.Boolean,
        "valuation_age_seconds": pl.Float64, "raw_value": pl.Float64, "row_id": pl.Int64,
    })


@pytest.mark.parametrize("seed", range(12))
def test_grouped_keeps_exact_window_rows_order_duplicates_nulls_and_ties(seed):
    rng = random.Random(seed)
    rows = [{
        "strategy_id": rng.choice(["TX", "MXF", "", None]),
        "__bucket": rng.randrange(12), "decision_ts_ns": rng.choice([None, *range(20)]),
        "__selection_return": rng.choice([None, -0.0, 0.0, -2., 3., float("inf"),
                                             -float("inf"), float("nan")]),
        "valuation_available": rng.choice([True, False, None]),
        "valuation_carried_forward": rng.choice([True, False, None]),
        "valuation_age_seconds": rng.choice([None, 0., 120.]),
        "raw_value": rng.choice([-0.0, 1e-120, -1.2345678912345678]), "row_id": i,
    } for i in range(512)]
    data = frame(rows)
    before = data.clone()
    expected = selection.select_window_rows(data.lazy()).collect(engine="streaming")
    actual = selection.select_grouped_rows(data.lazy()).collect(engine="streaming")
    assert_frame_equal(actual, expected, check_exact=True)
    assert actual["raw_value"].to_numpy().tobytes() == expected["raw_value"].to_numpy().tobytes()
    assert_frame_equal(data, before, check_exact=True)


@pytest.mark.parametrize("rows", [[], [{"strategy_id": "TX", "__bucket": 1,
    "decision_ts_ns": 1, "__selection_return": None, "valuation_available": False,
    "valuation_carried_forward": False, "valuation_age_seconds": 0.,
    "raw_value": -0., "row_id": 1}]])
def test_empty_and_entirely_unavailable_group_is_not_filled(rows):
    data = frame(rows)
    assert_frame_equal(selection.select_grouped_rows(data.lazy()).collect(),
                       selection.select_window_rows(data.lazy()).collect(), check_exact=True)


@pytest.mark.parametrize("range_key", tuple(dashboard.HISTORY_RANGE_SECONDS))
def test_full_fixture_curves_keep_every_financial_value_and_clock(tmp_path, monkeypatch, range_key):
    state, _ = _fixture(tmp_path)
    root = tmp_path / "column-cache"
    root.mkdir()
    monkeypatch.setenv(cache.CACHE_ENV, str(root))
    monkeypatch.setenv(cache.ENABLE_ENV, "1")
    monkeypatch.setattr(cache, "MIN_SOURCE_BYTES", 1)
    now = datetime(2026, 8, 17, 0, 2, tzinfo=timezone.utc)
    expected = dashboard.build_dashboard_history_snapshot(state_dir=state, now=now, range_key=range_key)
    monkeypatch.setattr(selection, "select_window_rows", selection.select_grouped_rows)
    actual = dashboard.build_dashboard_history_snapshot(state_dir=state, now=now, range_key=range_key)
    assert actual == expected


def test_unverified_raw_reader_never_uses_two_pass_selector(tmp_path, monkeypatch):
    state, _ = _fixture(tmp_path)
    monkeypatch.setenv(cache.ENABLE_ENV, "0")
    def forbidden(*args, **kwargs):
        raise AssertionError("a mutable raw source cannot support two-pass selection")
    monkeypatch.setattr(selection, "select_grouped_rows", forbidden)
    result = dashboard.build_dashboard_history_snapshot(state_dir=state,
        now=datetime(2026, 8, 17, 0, 2, tzinfo=timezone.utc), range_key="1d")
    assert result["history"]


