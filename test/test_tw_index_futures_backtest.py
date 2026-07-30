from __future__ import annotations

import numpy as np
import pytest
import torch

from stockagent.backtest.tw_index_futures import (
    FuturesCostSchedule,
    run_tw_index_futures_day_continuous,
    run_tw_index_futures_day_integer,
    select_tw_index_futures_contract_basket,
)
from stockagent.data.tw_index_futures import TaiwanIndexFuturesDaySession


def _market() -> TaiwanIndexFuturesDaySession:
    opens = np.asarray(
        [[10000.0, 10000.0, 10000.0], [10000.0, 10000.0, 10000.0]]
    )
    closes = np.asarray(
        [[10100.0, 10100.0, 10100.0], [9900.0, 9900.0, 9900.0]]
    )
    return TaiwanIndexFuturesDaySession(
        dates=np.asarray(["2025-01-02", "2025-01-03"], dtype="datetime64[D]"),
        products=("TX", "MTX", "TMF"),
        contract_months=np.full((2, 3), "202501", dtype="U16"),
        open_prices=opens,
        high_prices=np.maximum(opens, closes),
        low_prices=np.minimum(opens, closes),
        close_prices=closes,
        volumes=np.full((2, 3), 1000, dtype=np.int64),
        log_returns=np.log(closes / opens),
        tradable_mask=np.ones((2, 3), dtype=bool),
        multipliers=np.asarray([200, 50, 10], dtype=np.int64),
    )


def test_continuous_contract_is_daily_flat_and_masks_missing_session() -> None:
    result = run_tw_index_futures_day_continuous(
        torch.tensor([0.5, -1.0, 0.8]),
        torch.log(torch.tensor([1.01, 0.99, 1.50])),
        torch.tensor([True, True, False]),
        round_trip_cost_rate=0.001,
    )
    expected = torch.tensor(
        [0.5 * 0.01 - 0.5 * 0.001, -1.0 * -0.01 - 0.001, 0.0]
    )
    torch.testing.assert_close(result.strategy_returns, expected)
    torch.testing.assert_close(result.turnovers, torch.tensor([1.0, 2.0, 0.0]))


def test_basket_uses_same_underlying_products_for_integer_sizing() -> None:
    basket = select_tw_index_futures_contract_basket(
        1_000_000.0,
        np.asarray([10000.0, 10000.0, 10000.0]),
        np.ones(3, dtype=bool),
        cost_schedule=FuturesCostSchedule(tax_rate=0.0),
        max_notional=1_000_000.0,
    )
    # One TX is TWD 2m, so the exact TWD 1m basket is two MTX.
    np.testing.assert_array_equal(basket.quantities, [0, 2, 0])
    assert basket.actual_notional == pytest.approx(1_000_000.0)


def test_integer_backtest_applies_direction_tax_and_both_side_fees() -> None:
    schedule = FuturesCostSchedule(
        tax_rate=0.00002,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(0.0, 0.0, 0.0),
        slippage_points_per_side=(0.0, 0.0, 0.0),
    )
    result = run_tw_index_futures_day_integer(
        np.asarray([1.0, -1.0]),
        _market(),
        initial_capital=1_000_000.0,
        cost_schedule=schedule,
    )

    np.testing.assert_array_equal(result.contract_quantities[0], [0, 2, 0])
    assert result.gross_pnl_twd[0] == pytest.approx(10_000.0)
    assert result.fees_twd[0] == pytest.approx(50.0)
    assert result.tax_twd[0] == pytest.approx(40.2)
    assert result.net_pnl_twd[0] == pytest.approx(9_909.8)
    # Day two is short and therefore also profits when the index falls.
    assert result.gross_pnl_twd[1] > 0.0
    assert result.equity[-1] > 1_000_000.0
    assert result.alive.all()

