from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_public_release_schedule import RULES, align_observation_frame, align_observations, prepare_observation_keys


def test_state_age_updates_and_invalid_barrier_preserve_order():
    keys = pl.DataFrame({"date": [date(2026, 9, d) for d in (4, 1, 2, 3, 7)], "symbol": ["2330"] * 5})
    observed = pl.DataFrame({"date": [date(2026, 9, d) for d in (2, 4)], "symbol": ["2330"] * 2, "x": [0., None]})
    result = align_observation_frame(keys, observed, "x", RULES["revenue"])
    assert result["x"].to_list() == [None, None, 0., 0., None]
    assert result["x__available"].to_list() == [False, False, True, True, False]
    assert result["x__age_days"].to_list() == [None, None, 0., 1., None]
    assert result["x__updated"].to_list() == [True, False, True, False, False]
    assert result["x"].equals(align_observations(keys, observed, "x", RULES["revenue"]))
    assert result.equals(align_observation_frame(prepare_observation_keys(keys), observed, "x", RULES["revenue"]))


def test_daily_flow_never_becomes_a_carried_state():
    keys = pl.DataFrame({"date": [date(2026, 9, 2), date(2026, 9, 3)], "symbol": ["2330"] * 2})
    observed = keys.head(1).with_columns(pl.lit(0.).alias("x"))
    result = align_observation_frame(keys, observed, "x", RULES["daily"])
    assert result["x"].to_list() == [0., None]
    assert result["x__updated"].to_list() == [True, False]


def test_market_state_broadcast_is_not_stock_report_coverage():
    keys = pl.DataFrame({"date": [date(2026, 9, 2)] * 2, "symbol": ["2330", "0050"],
                         "lifecycle_start": [date(2000, 1, 1), date(2026, 9, 2)]})
    observed = pl.DataFrame({"date": [date(2026, 9, 1)], "symbol": ["__MARKET__"], "x": [3.]})
    result = align_observation_frame(keys, observed, "x", RULES["business"])
    assert result["x"].to_list() == [3., 3.]
    assert result["x__age_days"].to_list() == [1., 1.]


def test_security_reincarnation_stops_state_carry():
    keys = pl.DataFrame({"date": [date(2026, 9, 3)], "symbol": ["2330"], "lifecycle_start": [date(2026, 9, 2)]})
    observed = pl.DataFrame({"date": [date(2026, 9, 1)], "symbol": ["2330"], "x": [3.]})
    assert align_observation_frame(keys, observed, "x", RULES["revenue"])["x"].to_list() == [None]


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_report_is_a_barrier_not_a_carried_previous_value(bad):
    keys = pl.DataFrame({"date": [date(2026, 9, 3)], "symbol": ["2330"]})
    observed = pl.DataFrame({"date": [date(2026, 9, 1), date(2026, 9, 2)], "symbol": ["2330"] * 2, "x": [3., bad]})
    assert align_observation_frame(keys, observed, "x", RULES["revenue"])["x"].to_list() == [None]


def test_unknown_observations_and_empty_history_stay_unavailable():
    keys = pl.DataFrame({"date": [date(2026, 9, 3)], "symbol": ["2330"]})
    observed = pl.DataFrame(schema={"date": pl.Date, "symbol": pl.String, "x": pl.Float64})
    result = align_observation_frame(keys, observed, "x", RULES["revenue"])
    assert result["x__available"].to_list() == [False]
    assert result["x__age_days"].to_list() == [None]


def test_duplicate_target_grain_is_not_silently_multiplied():
    keys = pl.DataFrame({"date": [date(2026, 9, 3)] * 2, "symbol": ["2330"] * 2})
    with pytest.raises(ValueError, match="duplicate target"):
        align_observation_frame(keys, keys.head(1).with_columns(pl.lit(1.).alias("x")), "x", RULES["daily"])
