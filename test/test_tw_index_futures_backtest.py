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
from stockagent.backtest.simulator import (
    run_backtest_integer_shares,
    run_backtest_torch,
)
from stockagent.data.tw_index_futures import TaiwanIndexFuturesDaySession


def _market() -> TaiwanIndexFuturesDaySession:
    opens = np.asarray(
        [[10000.0, 10000.0, 10000.0], [10000.0, 10000.0, 10000.0]]
    )
    closes = np.asarray(
        [[10100.0, 10100.0, 10100.0], [9900.0, 9900.0, 9900.0]]
    )
    rolling = np.log(closes / opens)
    return TaiwanIndexFuturesDaySession(
        dates=np.asarray(["2025-01-02", "2025-01-03"], dtype="datetime64[D]"),
        products=("TX", "MTX", "TMF"),
        contract_months=np.full((2, 3), "202501", dtype="U16"),
        open_prices=opens,
        high_prices=np.maximum(opens, closes),
        low_prices=np.minimum(opens, closes),
        close_prices=closes,
        volumes=np.full((2, 3), 1000, dtype=np.int64),
        log_returns=rolling,
        tradable_mask=np.ones((2, 3), dtype=bool),
        multipliers=np.asarray([200, 50, 10], dtype=np.int64),
        rolling_buy_hold_log_returns=rolling,
        rolling_buy_hold_tradable_mask=np.ones((2, 3), dtype=bool),
        front_month_roll_mask=np.zeros((2, 3), dtype=bool),
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


def test_integer_backtest_applies_sell_only_tax_and_both_side_fees() -> None:
    schedule = FuturesCostSchedule(
        tax_rate=0.0002,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(40.0, 11.5, 8.0),
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
    # Two MTX, TWD 24 per contract per side, two sides.
    assert result.fees_twd[0] == pytest.approx(96.0)
    # Long opens with a buy and closes with a sale: only close is taxed.
    assert result.tax_twd[0] == pytest.approx(2 * 50 * 10_100 * 0.0002)
    assert result.net_pnl_twd[0] == pytest.approx(10_000.0 - 96.0 - 202.0)
    # Day two is short and therefore also profits when the index falls.
    assert result.gross_pnl_twd[1] > 0.0
    # Short opens with a sale and closes with a buy: only open is taxed.
    assert result.tax_twd[1] == pytest.approx(2 * 50 * 10_000 * 0.0002)
    assert result.equity[-1] > 1_000_000.0
    assert result.alive.all()


def test_basket_estimate_charges_one_sale_tax_and_two_fixed_fees() -> None:
    schedule = FuturesCostSchedule(
        tax_rate=0.0002,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(40.0, 11.5, 8.0),
    )
    basket = select_tw_index_futures_contract_basket(
        1_000_000.0,
        np.asarray([10_000.0, 10_000.0, 10_000.0]),
        np.ones(3, dtype=bool),
        cost_schedule=schedule,
        max_notional=1_000_000.0,
    )
    np.testing.assert_array_equal(basket.quantities, [0, 2, 0])
    # 2 contracts * 2 sides * TWD 24 + one TWD 1m sale * 0.0002.
    assert basket.estimated_round_trip_cost == pytest.approx(96.0 + 200.0)


def test_one_hundred_million_capital_executes_small_model_exposure() -> None:
    result = run_tw_index_futures_day_integer(
        np.asarray([0.03, 0.03]),
        _market(),
        initial_capital=100_000_000.0,
        cost_schedule=FuturesCostSchedule(tax_rate=0.0),
    )

    assert np.all(np.abs(result.contract_quantities).sum(axis=1) > 0)
    assert np.all(result.executed_exposure > 0.0)
    assert np.any(result.strategy_returns != 0.0)


def test_missing_unused_product_does_not_ruin_integer_account() -> None:
    market = _market()
    market.open_prices[:, 2] = np.nan
    market.high_prices[:, 2] = np.nan
    market.low_prices[:, 2] = np.nan
    market.close_prices[:, 2] = np.nan
    market.log_returns[:, 2] = np.nan
    market.tradable_mask[:, 2] = False

    result = run_tw_index_futures_day_integer(
        np.asarray([1.0, -1.0]),
        market,
        initial_capital=1_000_000.0,
        cost_schedule=FuturesCostSchedule(tax_rate=0.0),
    )

    np.testing.assert_array_equal(result.contract_quantities[:, 2], 0)
    assert np.isfinite(result.strategy_returns).all()
    assert np.any(result.strategy_returns != 0.0)
    assert result.alive.all()


def test_nonfinite_price_on_selected_product_fails_closed() -> None:
    market = _market()
    market.close_prices[0, 1] = np.nan

    with pytest.raises(
        ValueError,
        match="selected futures contracts require finite positive open/close prices",
    ):
        run_tw_index_futures_day_integer(
            np.asarray([1.0, -1.0]),
            market,
            initial_capital=1_000_000.0,
            cost_schedule=FuturesCostSchedule(tax_rate=0.0),
        )


def test_canonical_tensor_adapter_collapses_only_to_one_futures_exposure() -> None:
    pseudo_weights = torch.tensor([[0.2, 0.3], [-0.4, -0.1]], requires_grad=True)
    repeated_returns = torch.log(
        torch.tensor([[1.01, 1.01], [0.99, 0.99]])
    )
    mask = torch.ones_like(pseudo_weights, dtype=torch.bool)
    result = run_backtest_torch(
        pseudo_weights,
        repeated_returns,
        mask,
        repeated_returns[:, 0],
        0.0005,
        0.0005,
        long_only=False,
        portfolio_activation="pre_normalized",
        execution_mode="tw_index_futures_day",
    )

    expected_simple = torch.tensor([0.5 * 0.01 - 0.0005, 0.5 * 0.01 - 0.0005])
    torch.testing.assert_close(torch.expm1(result.strategy_returns), expected_simple)
    torch.testing.assert_close(result.turnovers, torch.ones(2))
    (-result.strategy_returns.mean()).backward()
    assert pseudo_weights.grad is not None
    assert torch.isfinite(pseudo_weights.grad).all()


def test_canonical_tensor_adapter_keeps_ruin_absorbing_within_chunk() -> None:
    pseudo_weights = torch.ones((2, 1))
    repeated_returns = torch.log(torch.tensor([[0.01], [2.0]]))
    mask = torch.ones_like(pseudo_weights, dtype=torch.bool)
    result = run_backtest_torch(
        pseudo_weights,
        repeated_returns,
        mask,
        repeated_returns[:, 0],
        0.05,
        0.05,
        long_only=False,
        portfolio_activation="pre_normalized",
        execution_mode="tw_index_futures_day",
    )

    assert result.strategy_returns[0] < 0.0
    assert result.strategy_returns[1].item() == 0.0
    torch.testing.assert_close(result.turnovers, torch.tensor([2.0, 0.0]))
    assert not bool(result.final_alive)


def test_canonical_integer_adapter_uses_all_in_user_fees() -> None:
    pseudo_weights = np.asarray([[0.5, 0.5], [-0.5, -0.5]])
    returns = np.repeat(_market().log_returns[:, :1], 2, axis=1)
    mask = np.ones_like(pseudo_weights, dtype=bool)
    schedule = FuturesCostSchedule(
        tax_rate=0.0,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(40.0, 11.5, 8.0),
    )
    result, records = run_backtest_integer_shares(
        pseudo_weights,
        returns,
        mask,
        np.asarray(_market().log_returns[:, 0], dtype=np.float32),
        initial_capital=1_000_000.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        dates=_market().dates,
        execution_mode="tw_index_futures_day",
        futures_market=_market(),
        futures_cost_schedule=schedule,
    )

    # TWD 1m exposure is two MTX; two sides * two contracts * TWD 24.
    assert len(records) == 4
    assert result.execution_mode == "tw_index_futures_day"
    first_day_net = np.expm1(result.strategy_returns[0]) * 1_000_000.0
    assert first_day_net == pytest.approx(10_000.0 - 96.0, rel=1e-5)
