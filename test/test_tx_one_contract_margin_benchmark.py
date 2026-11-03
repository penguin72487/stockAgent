"""Cash-flow and physical-contract checks for the one-TX research primitive."""

from datetime import date

import numpy as np
import polars as pl
import pytest

from stockagent.data.tw_futures_benchmark import (
    load_tx_front_rolling_benchmark,
    replay_tx_one_contract_margin_benchmark,
)


def inputs():
    reference = {
        "dates": np.array(["2026-01-02", "2026-01-05", "2026-01-06"], dtype="datetime64[D]"),
        "source_covered": np.array([True, True, True]),
        "contract_months": np.array(["202601", "202602", "202602"]),
        "front_month_roll_mask": np.array([False, True, False]),
        # The large 105 -> 200 calendar spread must never be treated as PnL.
        "prior_same_contract_close": np.array([100.0, 200.0, 195.0]),
        "front_month_close": np.array([105.0, 195.0, 195.0]),
    }
    args = {
        "expected_trading_dates": reference["dates"].copy(),
        "initial_capital_twd": 100.0,
        "opening_margin_twd": np.array([100.0, 115.0, 120.0]),
        "closing_margin_twd": np.array([100.0, 120.0, 120.0]),
        "opening_transaction_cost_twd": np.array([10.0, 5.0, 0.0]),
        "multiplier_twd_per_point": 2.0,
    }
    return reference, args


def test_one_contract_roll_separates_pnl_costs_and_external_deposits():
    reference, args = inputs()
    result = replay_tx_one_contract_margin_benchmark(reference, **args)
    np.testing.assert_allclose(result["gross_pnl_twd"], [10, -10, 0])
    np.testing.assert_allclose(result["opening_external_deposit_twd"], [10, 10, 0])
    np.testing.assert_allclose(result["closing_external_deposit_twd"], [0, 15, 0])
    np.testing.assert_allclose(result["equity_twd"], [110, 120, 120])
    np.testing.assert_allclose(np.exp(result["benchmark_log_returns"]), [1, 105 / 120, 1])
    net_pnl = np.cumsum(result["gross_pnl_twd"] - result["opening_transaction_cost_twd"])
    np.testing.assert_allclose(
        result["equity_twd"], 100 + result["cumulative_external_deposit_twd"] + net_pnl
    )


def test_replay_consumes_the_existing_physical_contract_reference(tmp_path):
    path = tmp_path / "tx.parquet"
    pl.DataFrame({
        "date": [date(2026, 1, 2), date(2026, 1, 2), date(2026, 1, 5),
                 date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 6)],
        "product": ["TX"] * 6,
        "contract": ["202601", "202602"] * 3,
        "tenor_rank": [1, 2, 1, 2, 2, 1],
        "close": [100.0, 200.0, 105.0, 200.0, 106.0, 195.0],
        "source_row_observed": [True] * 6,
    }).write_parquet(path)
    dates = np.array(["2026-01-05", "2026-01-06"], dtype="datetime64[D]")
    reference = load_tx_front_rolling_benchmark(path, dates)
    _, args = inputs()
    args["expected_trading_dates"] = dates.copy()
    for key in ("opening_margin_twd", "closing_margin_twd", "opening_transaction_cost_twd"):
        args[key] = args[key][:2]
    result = replay_tx_one_contract_margin_benchmark(reference, **args)
    np.testing.assert_allclose(result["gross_pnl_twd"], [10, -10])
    assert result["front_month_roll_mask"].tolist() == [False, True]


@pytest.mark.parametrize("field,value", [
    ("source_covered", np.array([True, False, True])),
    ("prior_same_contract_close", np.array([100.0, np.nan, 195.0])),
    ("front_month_roll_mask", np.array([False, False, False])),
    ("contract_months", np.array(["202601", "", "202602"])),
])
def test_missing_marks_or_inconsistent_contracts_are_rejected(field, value):
    reference, args = inputs()
    reference[field] = value
    with pytest.raises(ValueError):
        replay_tx_one_contract_margin_benchmark(reference, **args)


def test_non_roll_cost_or_unknown_margin_is_not_silently_accepted():
    reference, args = inputs()
    args["opening_transaction_cost_twd"][-1] = 1
    with pytest.raises(ValueError, match="entry or roll"):
        replay_tx_one_contract_margin_benchmark(reference, **args)
    args["opening_transaction_cost_twd"][-1] = 0
    args["closing_margin_twd"][1] = np.nan
    with pytest.raises(ValueError, match="closing margin"):
        replay_tx_one_contract_margin_benchmark(reference, **args)


def test_a_missing_market_session_cannot_be_connected_with_a_straight_line():
    reference, args = inputs()
    args["expected_trading_dates"] = np.array(
        ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"], dtype="datetime64[D]"
    )
    with pytest.raises(ValueError, match="complete supplied market calendar"):
        replay_tx_one_contract_margin_benchmark(reference, **args)


def test_nonpositive_equity_is_not_repaired_with_an_unobserved_intraday_deposit():
    reference, args = inputs()
    reference["front_month_close"][1] = 1
    with pytest.raises(ValueError, match="insolvent"):
        replay_tx_one_contract_margin_benchmark(reference, **args)
