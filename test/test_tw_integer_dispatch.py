from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import pytest
import torch

from stockagent.backtest.simulator import (
    _tw_integer_holdings_records,
    run_backtest_integer_shares,
)
from stockagent.backtest.tw_integer_execution import run_tw_cash_integer
from stockagent.training.trainer import (
    _ExecutionRuntime,
    _integer_execution_runtime_kwargs,
    _save_holdings_table,
)


def _base_inputs(time: int, symbols: int) -> dict[str, np.ndarray]:
    return {
        "future_returns": np.zeros((time, symbols), dtype=np.float64),
        "tradable_mask": np.ones((time, symbols), dtype=np.bool_),
        "benchmark_returns": np.zeros(time, dtype=np.float64),
    }


def _no_corporate_actions(values: np.ndarray) -> np.ndarray:
    return np.zeros_like(values, dtype=np.bool_)


def _open_masks(values: np.ndarray) -> dict[str, np.ndarray]:
    return {
        "day_trade_can_buy_open_mask": np.ones_like(values, dtype=np.bool_),
        "day_trade_can_sell_open_mask": np.ones_like(values, dtype=np.bool_),
    }


def test_integer_dispatch_naive_default_is_exactly_backward_compatible() -> None:
    weights = np.array([[0.6, 0.4], [0.0, 1.0]], dtype=np.float64)
    common = _base_inputs(2, 2)
    kwargs = dict(
        **common,
        close_prices=np.array([[10.0, 20.0], [11.0, 19.0]]),
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        initial_capital=10_000.0,
        collect_holdings=False,
    )

    default_result, default_records = run_backtest_integer_shares(weights, **kwargs)
    explicit_result, explicit_records = run_backtest_integer_shares(
        weights, **kwargs, execution_mode="naive"
    )

    np.testing.assert_array_equal(
        default_result.strategy_returns, explicit_result.strategy_returns
    )
    np.testing.assert_array_equal(default_result.turnovers, explicit_result.turnovers)
    np.testing.assert_array_equal(
        default_result.weights_history, explicit_result.weights_history
    )
    assert default_records == explicit_records == []


def test_integer_runtime_capacity_switch_makes_zero_inventory_nonbinding() -> None:
    target = np.array([[-1.0]], dtype=np.float64)
    runtime = _ExecutionRuntime(
        mode="tw_cash",
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        lot_sizes=np.ones(1, dtype=np.int64),
        settlement_lag_sessions=2,
        integer_buy_fee_rates=np.zeros(1),
        integer_sell_fee_rates=np.zeros(1),
        short_lot_sizes=np.full(1, 1000, dtype=np.int64),
        short_capacity_limit_enabled=False,
    )
    execution_kwargs = _integer_execution_runtime_kwargs(
        runtime,
        open_prices=None,
        day_trade_eligible_mask=None,
        day_trade_can_buy_open_mask=None,
        day_trade_can_sell_open_mask=None,
        unresolved_corporate_action_mask=np.zeros_like(target, dtype=np.bool_),
        state_advance_mask=np.ones(1, dtype=np.bool_),
        short_capacity_shares=np.zeros_like(target, dtype=np.int64),
        short_margin_rate=np.full_like(target, 0.9),
    )

    result, _ = run_backtest_integer_shares(
        target,
        future_returns=np.zeros_like(target),
        tradable_mask=np.ones_like(target, dtype=np.bool_),
        benchmark_returns=np.zeros(1),
        close_prices=np.full_like(target, 100.0),
        can_buy_mask=np.ones_like(target, dtype=np.bool_),
        can_sell_mask=np.ones_like(target, dtype=np.bool_),
        can_short_open_mask=np.ones_like(target, dtype=np.bool_),
        force_short_cover_mask=np.zeros_like(target, dtype=np.bool_),
        initial_capital=100_000.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        collect_holdings=False,
        **execution_kwargs,
    )

    assert result.shares_history[0, 0] == -1000


def test_tw_cash_dispatch_exposes_exact_t_plus_two_state_and_chunks_exactly() -> None:
    weights = np.array([[0.5], [0.0], [0.0], [0.0]], dtype=np.float64)
    closes = np.array([[10.0], [12.0], [12.0], [12.0]], dtype=np.float64)
    common = _base_inputs(4, 1)
    kwargs = dict(
        **common,
        close_prices=closes,
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.array([0.001]),
        sell_fee_rates=np.array([0.003]),
        lot_sizes=np.array([1]),
        unresolved_corporate_action_mask=_no_corporate_actions(weights),
        initial_capital=10_000.0,
        collect_holdings=False,
    )

    full, _ = run_backtest_integer_shares(weights, **kwargs)
    first, _ = run_backtest_integer_shares(
        weights[:2],
        future_returns=common["future_returns"][:2],
        tradable_mask=common["tradable_mask"][:2],
        benchmark_returns=common["benchmark_returns"][:2],
        close_prices=closes[:2],
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.array([0.001]),
        sell_fee_rates=np.array([0.003]),
        lot_sizes=np.array([1]),
        unresolved_corporate_action_mask=_no_corporate_actions(weights[:2]),
        initial_capital=10_000.0,
        collect_holdings=False,
    )
    second, _ = run_backtest_integer_shares(
        weights[2:],
        future_returns=common["future_returns"][2:],
        tradable_mask=common["tradable_mask"][2:],
        benchmark_returns=common["benchmark_returns"][2:],
        close_prices=closes[2:],
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.array([0.001]),
        sell_fee_rates=np.array([0.003]),
        lot_sizes=np.array([1]),
        unresolved_corporate_action_mask=_no_corporate_actions(weights[2:]),
        initial_capital=10_000.0,
        initial_integer_state=first.final_integer_state,
        collect_holdings=False,
    )

    np.testing.assert_array_equal(full.shares_history[:, 0], [500, 0, 0, 0])
    np.testing.assert_allclose(full.cash_history, [10_000.0, 10_000.0, 4_995.0, 10_977.0])
    assert full.settlement_ledger_unit == "currency"
    np.testing.assert_allclose(
        full.payables_history,
        [[0.0, 5_005.0], [5_005.0, 0.0], [0.0, 0.0], [0.0, 0.0]],
    )
    np.testing.assert_allclose(
        full.receivables_history,
        [[0.0, 0.0], [0.0, 5_982.0], [5_982.0, 0.0], [0.0, 0.0]],
    )
    for name in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "settlement_default",
        "shares_history",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate([getattr(first, name), getattr(second, name)], axis=0),
            rtol=0.0,
            atol=1e-9,
        )
    assert full.final_integer_state is not None
    assert second.final_integer_state is not None
    assert full.final_integer_state.settled_cash == pytest.approx(
        second.final_integer_state.settled_cash
    )
    np.testing.assert_allclose(
        full.final_integer_state.payable_queue,
        second.final_integer_state.payable_queue,
    )


def test_tw_cash_dispatch_realizes_canonical_forward_return_on_same_row() -> None:
    result, records = run_backtest_integer_shares(
        np.array([[1.0]], dtype=np.float64),
        future_returns=np.array([[np.log(1.10)]], dtype=np.float64),
        tradable_mask=np.ones((1, 1), dtype=np.bool_),
        benchmark_returns=np.zeros(1, dtype=np.float64),
        close_prices=np.array([[100.0]], dtype=np.float64),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        lot_sizes=np.ones(1, dtype=np.int64),
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        initial_capital=100_000.0,
        symbols=["2330"],
        collect_holdings=True,
    )

    assert result.strategy_returns[0] == pytest.approx(np.log(1.10))
    assert result.shares_history[0, 0] == 1_000
    assert result.final_integer_state is not None
    assert result.final_integer_state.last_nav == pytest.approx(110_000.0)
    assert sum(record.holding_ratio for record in records) == pytest.approx(1.0)
    stock_record = next(record for record in records if not record.is_cash)
    assert stock_record.market_value == pytest.approx(100_000.0)
    assert stock_record.holding_ratio == pytest.approx(1.0)


def test_tw_integer_symbol_indices_reorder_equal_length_fee_and_lot_vectors() -> None:
    common = dict(
        future_returns=np.zeros((1, 2)),
        tradable_mask=np.ones((1, 2), dtype=np.bool_),
        benchmark_returns=np.zeros(1),
        close_prices=np.full((1, 2), 100.0),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        unresolved_corporate_action_mask=np.zeros((1, 2), dtype=np.bool_),
        initial_capital=10_000.0,
        collect_holdings=False,
    )
    mapped, _ = run_backtest_integer_shares(
        np.array([[0.5, 0.5]]),
        **common,
        buy_fee_rates=np.array([0.0, 0.1]),
        sell_fee_rates=np.array([0.0, 0.1]),
        lot_sizes=np.array([1, 10]),
        symbol_indices=np.array([1, 0]),
    )
    explicit, _ = run_backtest_integer_shares(
        np.array([[0.5, 0.5]]),
        **common,
        buy_fee_rates=np.array([0.1, 0.0]),
        sell_fee_rates=np.array([0.1, 0.0]),
        lot_sizes=np.array([10, 1]),
    )

    np.testing.assert_array_equal(mapped.shares_history, explicit.shares_history)
    np.testing.assert_array_equal(mapped.strategy_returns, explicit.strategy_returns)


def test_tw_integer_equal_length_full_universe_symbols_require_explicit_projection() -> None:
    result, records = run_backtest_integer_shares(
        np.array([[1.0, 0.0]]),
        future_returns=np.zeros((1, 2)),
        tradable_mask=np.ones((1, 2), dtype=np.bool_),
        benchmark_returns=np.zeros(1),
        close_prices=np.full((1, 2), 100.0),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(2),
        sell_fee_rates=np.zeros(2),
        lot_sizes=np.ones(2, dtype=np.int64),
        unresolved_corporate_action_mask=np.zeros((1, 2), dtype=np.bool_),
        symbol_indices=np.array([1, 0]),
        symbols=["A", "B"],
        symbols_are_full_universe=True,
        initial_capital=10_000.0,
    )

    assert result.shares_history[0, 0] == 100
    assert {record.symbol for record in records if not record.is_cash} == {"B"}


def test_tw_integer_dispatch_forwards_broker_fee_profile() -> None:
    result, _ = run_backtest_integer_shares(
        np.array([[1.0]]),
        future_returns=np.zeros((1, 1)),
        tradable_mask=np.ones((1, 1), dtype=np.bool_),
        benchmark_returns=np.zeros(1),
        close_prices=np.array([[10.0]]),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.array([0.000855]),
        sell_fee_rates=np.array([0.003855]),
        minimum_commission=20.0,
        commission_rounding="floor",
        tax_rounding="floor",
        lot_sizes=np.ones(1, dtype=np.int64),
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        initial_capital=100.0,
        collect_holdings=False,
    )

    assert result.shares_history[0, 0] == 8
    assert result.final_integer_state is not None
    assert result.final_integer_state.payable_queue[-1] == pytest.approx(100.0)


def test_tw_cash_force_exit_bypasses_side_volume_and_turnover_limits() -> None:
    weights = np.array([[1.0], [1.0]], dtype=np.float64)
    common = _base_inputs(2, 1)
    result, records = run_backtest_integer_shares(
        weights,
        **common,
        close_prices=np.full((2, 1), 10.0),
        execution_mode="tw_cash",
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([0.0]),
        lot_sizes=np.array([1]),
        unresolved_corporate_action_mask=_no_corporate_actions(weights),
        can_buy_mask=np.ones((2, 1), dtype=np.bool_),
        can_sell_mask=np.array([[True], [False]], dtype=np.bool_),
        force_exit_mask=np.array([[False], [True]], dtype=np.bool_),
        cash_close_volume_reference=np.zeros((2, 1)),
        max_volume_participation=1.0,
        max_turnover_ratio=0.01,
        initial_capital=10_000.0,
        collect_holdings=False,
    )

    # The first voluntary buy is volume-capped to zero.  Seed a position via a
    # separate first chunk, then prove terminal liquidation bypasses all caps.
    seeded, _ = run_backtest_integer_shares(
        weights[:1],
        future_returns=common["future_returns"][:1],
        tradable_mask=common["tradable_mask"][:1],
        benchmark_returns=common["benchmark_returns"][:1],
        close_prices=np.array([[10.0]]),
        execution_mode="tw_cash",
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([0.0]),
        lot_sizes=np.array([1]),
        unresolved_corporate_action_mask=_no_corporate_actions(weights[:1]),
        initial_capital=10_000.0,
        collect_holdings=False,
    )
    exited, _ = run_backtest_integer_shares(
        weights[:1],
        future_returns=common["future_returns"][:1],
        tradable_mask=np.zeros((1, 1), dtype=np.bool_),
        benchmark_returns=common["benchmark_returns"][:1],
        close_prices=np.array([[10.0]]),
        execution_mode="tw_cash",
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([0.0]),
        lot_sizes=np.array([1]),
        unresolved_corporate_action_mask=_no_corporate_actions(weights[:1]),
        can_buy_mask=np.zeros((1, 1), dtype=np.bool_),
        can_sell_mask=np.zeros((1, 1), dtype=np.bool_),
        force_exit_mask=np.ones((1, 1), dtype=np.bool_),
        cash_close_volume_reference=np.zeros((1, 1)),
        max_volume_participation=1.0,
        max_turnover_ratio=0.01,
        initial_integer_state=seeded.final_integer_state,
        collect_holdings=False,
    )
    assert result.shares_history[0, 0] == 0
    assert seeded.shares_history[0, 0] == 1_000
    assert exited.shares_history[0, 0] == 0
    assert exited.turnovers[0] == pytest.approx(1.0)


def test_tw_cash_blocked_sale_cannot_break_integer_gross_budget() -> None:
    seeded, _ = run_backtest_integer_shares(
        np.array([[0.5, 0.0]]),
        future_returns=np.zeros((1, 2)),
        tradable_mask=np.ones((1, 2), dtype=np.bool_),
        benchmark_returns=np.zeros(1),
        close_prices=np.array([[10.0, 10.0]]),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(2),
        sell_fee_rates=np.zeros(2),
        lot_sizes=np.ones(2, dtype=np.int64),
        unresolved_corporate_action_mask=np.zeros((1, 2), dtype=np.bool_),
        gross_leverage=0.5,
        initial_capital=10_000.0,
        collect_holdings=False,
    )
    rotated, _ = run_backtest_integer_shares(
        np.array([[0.0, 0.5]]),
        future_returns=np.zeros((1, 2)),
        tradable_mask=np.ones((1, 2), dtype=np.bool_),
        benchmark_returns=np.zeros(1),
        close_prices=np.array([[10.0, 10.0]]),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(2),
        sell_fee_rates=np.zeros(2),
        lot_sizes=np.ones(2, dtype=np.int64),
        unresolved_corporate_action_mask=np.zeros((1, 2), dtype=np.bool_),
        cash_close_volume_reference=np.array([[0.0, np.nan]]),
        max_volume_participation=1.0,
        gross_leverage=0.5,
        initial_integer_state=seeded.final_integer_state,
        collect_holdings=False,
    )

    np.testing.assert_array_equal(rotated.shares_history[0], [500, 0])
    assert np.abs(rotated.weights_history[0]).sum() <= 0.5 + 1e-12


def test_tw_cash_volume_cap_requires_explicit_causal_reference() -> None:
    common = _base_inputs(1, 1)
    with pytest.raises(ValueError, match="explicit causal"):
        run_backtest_integer_shares(
            np.array([[1.0]]),
            **common,
            close_prices=np.array([[10.0]]),
            execution_mode="tw_cash",
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
            # This same-session total includes the closing auction and must not
            # silently become a fill-sizing input.
            daily_volumes=np.array([[1.0e9]]),
            max_volume_participation=0.1,
            initial_capital=1_000.0,
            collect_holdings=False,
        )


def test_tw_cash_explicit_close_volume_is_invariant_to_same_row_volume() -> None:
    common = _base_inputs(1, 1)
    kwargs = dict(
        close_prices=np.array([[10.0]]),
        execution_mode="tw_cash",
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        cash_close_volume_reference=np.array([[100.0]]),
        max_volume_participation=0.1,
        initial_capital=1_000.0,
        collect_holdings=False,
    )
    low_same_row, _ = run_backtest_integer_shares(
        np.array([[1.0]]),
        **common,
        **kwargs,
        daily_volumes=np.array([[0.0]]),
    )
    high_same_row, _ = run_backtest_integer_shares(
        np.array([[1.0]]),
        **common,
        **kwargs,
        daily_volumes=np.array([[1.0e12]]),
    )

    np.testing.assert_array_equal(
        low_same_row.shares_history, high_same_row.shares_history
    )
    assert low_same_row.shares_history[0, 0] == 10


def test_tw_integer_dispatch_selects_full_universe_fee_lot_and_symbol_vectors() -> None:
    common = _base_inputs(1, 1)
    result, records = run_backtest_integer_shares(
        np.array([[0.53]]),
        **common,
        close_prices=np.array([[10.0]]),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.array([0.0, 0.10]),
        sell_fee_rates=np.array([0.0, 0.10]),
        lot_sizes=np.array([1, 10]),
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        symbol_indices=np.array([1]),
        symbols=["1101", "0050"],
        initial_capital=1_000.0,
    )

    assert result.shares_history[0, 0] == 50
    assert result.final_integer_state is not None
    assert result.final_integer_state.last_nav == pytest.approx(950.0)
    assert {record.symbol for record in records} == {"CASH", "0050"}


def test_tw_day_trade_dispatch_uses_point_in_time_eligibility_sides_and_lots() -> None:
    common = _base_inputs(1, 2)
    result, records = run_backtest_integer_shares(
        np.array([[-0.5, 0.5]]),
        **common,
        close_prices=np.array([[99.0, 101.0]]),
        open_prices=np.array([[100.0, 100.0]]),
        execution_mode="tw_day_trade",
        long_only=False,
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.array([0.001, 0.001]),
        sell_fee_rates=np.array([0.0025, 0.0015]),
        lot_sizes=np.array([1_000, 500]),
        day_trade_eligible_mask=np.ones((1, 2), dtype=np.bool_),
        can_buy_mask=np.ones((1, 2), dtype=np.bool_),
        can_sell_mask=np.ones((1, 2), dtype=np.bool_),
        can_short_open_mask=np.array([[True, False]], dtype=np.bool_),
        **_open_masks(np.array([[-0.5, 0.5]])),
        initial_capital=300_000.0,
    )

    np.testing.assert_array_equal(result.shares_history[0], [-1_000, 1_500])
    assert result.final_integer_state is not None
    np.testing.assert_array_equal(result.final_integer_state.holdings, [0, 0])
    # The account is flat at the close, but holdings.csv must still audit the
    # exact signed opening executions rather than emitting cash-only rows.
    by_symbol = {record.symbol: record for record in records}
    assert set(by_symbol) == {"CASH", "SYM_0000", "SYM_0001"}
    assert by_symbol["SYM_0000"].record_type == "day_trade_open"
    assert by_symbol["SYM_0000"].side == "SELL"
    assert by_symbol["SYM_0000"].shares == -1_000
    assert by_symbol["SYM_0000"].price == pytest.approx(100.0)
    assert by_symbol["SYM_0000"].market_value == pytest.approx(-100_000.0)
    assert by_symbol["SYM_0001"].record_type == "day_trade_open"
    assert by_symbol["SYM_0001"].side == "BUY"
    assert by_symbol["SYM_0001"].shares == 1_500
    assert by_symbol["SYM_0001"].price == pytest.approx(100.0)
    assert by_symbol["SYM_0001"].market_value == pytest.approx(150_000.0)


def test_tw_day_trade_holdings_csv_labels_opening_trades(tmp_path: Path) -> None:
    values = np.array([[1.0]], dtype=np.float64)
    _, records = run_backtest_integer_shares(
        values,
        **_base_inputs(1, 1),
        close_prices=np.array([[105.0]], dtype=np.float64),
        open_prices=np.array([[100.0]], dtype=np.float64),
        execution_mode="tw_day_trade",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        lot_sizes=np.ones(1, dtype=np.int64),
        day_trade_eligible_mask=np.ones((1, 1), dtype=np.bool_),
        **_open_masks(values),
        initial_capital=1_000.0,
    )
    path = tmp_path / "holdings.csv"

    _save_holdings_table(path, records)

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    trade = next(row for row in rows if row["symbol"] == "SYM_0000")
    assert trade["record_type"] == "day_trade_open"
    assert trade["side"] == "BUY"
    assert int(trade["shares"]) == 10
    assert float(trade["price"]) == pytest.approx(100.0)


def test_tw_day_trade_volume_then_turnover_cap_rounds_to_whole_lots() -> None:
    common = _base_inputs(1, 1)
    result, records = run_backtest_integer_shares(
        np.array([[1.0]]),
        **common,
        close_prices=np.array([[100.0]]),
        open_prices=np.array([[100.0]]),
        execution_mode="tw_day_trade",
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([0.0]),
        lot_sizes=np.array([1_000]),
        day_trade_eligible_mask=np.ones((1, 1), dtype=np.bool_),
        **_open_masks(np.array([[1.0]])),
        # The volume budget covers both legs of the forced round trip, so
        # 2,500 reported shares permit one 1,000-share lot (2,000 traded
        # shares) before the turnover cap is applied.
        day_trade_entry_volume_reference=np.array([[2_500.0]]),
        max_volume_participation=1.0,
        max_turnover_ratio=0.2,
        initial_capital=1_000_000.0,
        collect_holdings=False,
    )

    assert result.shares_history[0, 0] == 1_000
    assert result.turnovers[0] == pytest.approx(0.2)


def test_tw_day_trade_volume_cap_counts_both_roundtrip_legs() -> None:
    common = _base_inputs(1, 1)
    result, _ = run_backtest_integer_shares(
        np.array([[1.0]]),
        **common,
        close_prices=np.array([[100.0]]),
        open_prices=np.array([[100.0]]),
        execution_mode="tw_day_trade",
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([0.0]),
        lot_sizes=np.array([1_000]),
        day_trade_eligible_mask=np.ones((1, 1), dtype=np.bool_),
        **_open_masks(np.array([[1.0]])),
        day_trade_entry_volume_reference=np.array([[1_500.0]]),
        max_volume_participation=1.0,
        initial_capital=1_000_000.0,
        collect_holdings=False,
    )

    assert result.shares_history[0, 0] == 0
    assert result.turnovers[0] == pytest.approx(0.0)


def test_tw_day_trade_volume_cap_requires_explicit_causal_reference() -> None:
    common = _base_inputs(1, 1)
    with pytest.raises(ValueError, match="explicit causal"):
        run_backtest_integer_shares(
            np.array([[1.0]]),
            **common,
            close_prices=np.array([[100.0]]),
            open_prices=np.array([[100.0]]),
            execution_mode="tw_day_trade",
            buy_fee_rates=np.array([0.0]),
            sell_fee_rates=np.array([0.0]),
            lot_sizes=np.array([1_000]),
            day_trade_eligible_mask=np.ones((1, 1), dtype=np.bool_),
            **_open_masks(np.array([[1.0]])),
            # Same-session volume is valid for reporting, but cannot size an
            # order filled at that session's open.
            daily_volumes=np.array([[1_000_000.0]]),
            max_volume_participation=1.0,
            initial_capital=1_000_000.0,
            collect_holdings=False,
        )


def test_tw_day_trade_explicit_entry_volume_is_invariant_to_same_row_volume() -> None:
    common = _base_inputs(1, 1)
    kwargs = dict(
        close_prices=np.array([[100.0]]),
        open_prices=np.array([[100.0]]),
        execution_mode="tw_day_trade",
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([0.0]),
        lot_sizes=np.array([1_000]),
        day_trade_eligible_mask=np.ones((1, 1), dtype=np.bool_),
        day_trade_entry_volume_reference=np.array([[2_500.0]]),
        max_volume_participation=1.0,
        initial_capital=1_000_000.0,
        collect_holdings=False,
        **_open_masks(np.array([[1.0]])),
    )
    low_same_row, _ = run_backtest_integer_shares(
        np.array([[1.0]]),
        **common,
        **kwargs,
        daily_volumes=np.array([[0.0]]),
    )
    high_same_row, _ = run_backtest_integer_shares(
        np.array([[1.0]]),
        **common,
        **kwargs,
        daily_volumes=np.array([[1.0e12]]),
    )

    np.testing.assert_array_equal(
        low_same_row.shares_history, high_same_row.shares_history
    )
    assert low_same_row.shares_history[0, 0] == 1_000


@pytest.mark.parametrize(
    ("missing", "message"),
    [
        ("fees", "explicit per-symbol"),
        ("open", "open_prices"),
        ("eligibility", "point-in-time"),
    ],
)
def test_tw_day_trade_dispatch_fails_closed_on_missing_contract_data(
    missing: str,
    message: str,
) -> None:
    common = _base_inputs(1, 1)
    kwargs = dict(
        **common,
        close_prices=np.array([[100.0]]),
        open_prices=np.array([[100.0]]),
        execution_mode="tw_day_trade",
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([0.0]),
        day_trade_eligible_mask=np.ones((1, 1), dtype=np.bool_),
        **_open_masks(np.array([[1.0]])),
        collect_holdings=False,
    )
    if missing == "fees":
        kwargs["buy_fee_rates"] = None
    elif missing == "open":
        kwargs["open_prices"] = None
    else:
        kwargs["day_trade_eligible_mask"] = None

    with pytest.raises(ValueError, match=message):
        run_backtest_integer_shares(np.array([[1.0]]), **kwargs)


def test_tw_day_trade_lifecycle_masks_block_only_applicable_new_entries() -> None:
    common = _base_inputs(1, 1)
    base = dict(
        **common,
        close_prices=np.array([[100.0]]),
        open_prices=np.array([[100.0]]),
        execution_mode="tw_day_trade",
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([0.0]),
        lot_sizes=np.array([1_000]),
        day_trade_eligible_mask=np.ones((1, 1), dtype=np.bool_),
        can_short_open_mask=np.ones((1, 1), dtype=np.bool_),
        **_open_masks(np.array([[1.0]])),
        initial_capital=1_000_000.0,
        collect_holdings=False,
    )

    terminal, _ = run_backtest_integer_shares(
        np.array([[1.0]]),
        **base,
        force_exit_mask=np.ones((1, 1), dtype=np.bool_),
    )
    covered_short, _ = run_backtest_integer_shares(
        np.array([[-1.0]]),
        **base,
        long_only=False,
        force_short_cover_mask=np.ones((1, 1), dtype=np.bool_),
    )
    permitted_long, _ = run_backtest_integer_shares(
        np.array([[1.0]]),
        **base,
        force_short_cover_mask=np.ones((1, 1), dtype=np.bool_),
    )

    assert terminal.shares_history[0, 0] == 0
    assert covered_short.shares_history[0, 0] == 0
    assert permitted_long.shares_history[0, 0] == 10_000


def test_tw_cash_full_panel_ignores_only_inactive_unheld_invalid_prices() -> None:
    """Staggered listings may leave structurally irrelevant NaNs in a full panel."""

    weights = np.array(
        [[1.0, 0.0], [1.0, 0.0], [0.0, 1.0], [0.0, 1.0]],
        dtype=np.float64,
    )
    close_prices = np.array(
        [[10.0, np.nan], [11.0, -1.0], [np.nan, 20.0], [0.0, 22.0]],
        dtype=np.float64,
    )
    tradable = np.array(
        [[True, False], [True, False], [False, True], [False, True]],
        dtype=np.bool_,
    )
    force_exit = np.zeros_like(tradable)
    force_exit[1, 0] = True  # Last valid quote exits before the post-listing NaNs.
    future_returns = np.zeros_like(weights)
    future_returns[0, 0] = np.log(1.1)
    future_returns[2, 1] = np.log(1.1)

    result, records = run_backtest_integer_shares(
        weights,
        future_returns=future_returns,
        tradable_mask=tradable,
        force_exit_mask=force_exit,
        benchmark_returns=np.zeros(4, dtype=np.float64),
        close_prices=close_prices,
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(2),
        sell_fee_rates=np.zeros(2),
        lot_sizes=np.ones(2, dtype=np.int64),
        unresolved_corporate_action_mask=np.zeros_like(weights, dtype=np.bool_),
        initial_capital=1_000.0,
        symbols=["EARLY", "LATE"],
        dates=np.array(
            ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
            dtype="datetime64[D]",
        ),
        collect_holdings=True,
    )

    np.testing.assert_array_equal(
        result.shares_history,
        np.array([[100, 0], [0, 0], [0, 55], [0, 55]], dtype=np.int64),
    )
    np.testing.assert_allclose(
        result.strategy_returns,
        [np.log(1.1), 0.0, np.log(1.1), 0.0],
        rtol=1e-12,
        atol=1e-12,
    )
    assert result.final_integer_state is not None
    assert result.final_integer_state.alive
    assert [
        (record.date, record.symbol, record.shares)
        for record in records
        if not record.is_cash
    ] == [
        ("2024-01-02", "EARLY", 100),
        ("2024-01-04", "LATE", 55),
        ("2024-01-05", "LATE", 55),
    ]


@pytest.mark.parametrize("invalid_price", [np.nan, 0.0, -1.0, np.inf])
def test_tw_cash_full_panel_ignores_invalid_quote_for_unheld_noop(
    invalid_price: float,
) -> None:
    result, _ = run_backtest_integer_shares(
        # A causal outer-universe target can remain positive even though the
        # current session's buy side is unavailable.  No order is sendable, so
        # the absent quote is neither an execution nor valuation input.
        np.array([[1.0]], dtype=np.float64),
        future_returns=np.array([[np.nan]], dtype=np.float64),
        tradable_mask=np.ones((1, 1), dtype=np.bool_),
        can_buy_mask=np.zeros((1, 1), dtype=np.bool_),
        can_sell_mask=np.zeros((1, 1), dtype=np.bool_),
        benchmark_returns=np.zeros(1, dtype=np.float64),
        close_prices=np.array([[invalid_price]], dtype=np.float64),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        lot_sizes=np.ones(1, dtype=np.int64),
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        collect_holdings=False,
    )

    np.testing.assert_array_equal(result.shares_history, [[0]])
    np.testing.assert_array_equal(result.turnovers, [0.0])
    np.testing.assert_array_equal(result.strategy_returns, [0.0])


def test_tw_cash_full_panel_requires_quote_for_sendable_entry() -> None:
    with pytest.raises(ValueError, match=r"close_prices.*required prices.*time index 0"):
        run_backtest_integer_shares(
            np.array([[1.0]], dtype=np.float64),
            future_returns=np.zeros((1, 1), dtype=np.float64),
            tradable_mask=np.ones((1, 1), dtype=np.bool_),
            can_buy_mask=np.ones((1, 1), dtype=np.bool_),
            can_sell_mask=np.ones((1, 1), dtype=np.bool_),
            benchmark_returns=np.zeros(1, dtype=np.float64),
            close_prices=np.array([[np.nan]], dtype=np.float64),
            execution_mode="tw_cash",
            portfolio_activation="pre_normalized",
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            lot_sizes=np.ones(1, dtype=np.int64),
            unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
            collect_holdings=False,
        )


def test_tw_cash_full_panel_fails_closed_if_holding_reaches_missing_delisting_quote() -> None:
    force_exit = np.array([[False], [True]], dtype=np.bool_)
    with pytest.raises(ValueError, match=r"close_prices.*required prices.*time index 1"):
        run_backtest_integer_shares(
            np.array([[1.0], [0.0]], dtype=np.float64),
            future_returns=np.zeros((2, 1), dtype=np.float64),
            tradable_mask=np.array([[True], [False]], dtype=np.bool_),
            force_exit_mask=force_exit,
            benchmark_returns=np.zeros(2, dtype=np.float64),
            close_prices=np.array([[10.0], [np.nan]], dtype=np.float64),
            execution_mode="tw_cash",
            portfolio_activation="pre_normalized",
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            lot_sizes=np.ones(1, dtype=np.int64),
            unresolved_corporate_action_mask=np.zeros((2, 1), dtype=np.bool_),
            initial_capital=1_000.0,
            collect_holdings=False,
        )


def test_tw_cash_holdings_report_uses_oracle_mark_when_raw_quote_is_missing() -> None:
    integer_result = run_tw_cash_integer(
        np.ones((3, 1), dtype=np.float64),
        np.array([[10.0], [np.nan], [12.0]], dtype=np.float64),
        future_log_returns=np.array([[0.0], [np.log(1.2)], [0.0]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        tradable_mask=np.ones((3, 1), dtype=np.bool_),
        can_buy_mask=np.array([[True], [False], [True]], dtype=np.bool_),
        can_sell_mask=np.array([[True], [False], [True]], dtype=np.bool_),
        unresolved_corporate_action_mask=np.zeros((3, 1), dtype=np.bool_),
        initial_cash=1_000.0,
    )

    records = _tw_integer_holdings_records(
        integer_result,
        close_prices=np.array([[10.0], [np.nan], [12.0]], dtype=np.float64),
        symbols=["HELD"],
        dates=np.array(
            ["2024-01-02", "2024-01-03", "2024-01-04"],
            dtype="datetime64[D]",
        ),
    )

    stock_records = [record for record in records if not record.is_cash]
    assert [record.price for record in stock_records] == pytest.approx(
        [10.0, 10.0, 12.0]
    )
    assert [record.market_value for record in stock_records] == pytest.approx(
        [1_000.0, 1_000.0, 1_200.0]
    )


def test_tw_cash_holdings_report_keeps_dividend_claim_at_its_ledger_time() -> None:
    integer_result = run_tw_cash_integer(
        np.ones((2, 1), dtype=np.float64),
        np.array([[10.0], [9.0]], dtype=np.float64),
        future_log_returns=np.array([[np.log(0.9)], [0.0]], dtype=np.float64),
        cash_dividend_yield=np.array([[0.1], [0.0]], dtype=np.float64),
        cash_dividend_payment_delay_sessions=np.array(
            [[3], [0]], dtype=np.int64
        ),
        claim_queue_sessions=3,
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        tradable_mask=np.ones((2, 1), dtype=np.bool_),
        can_buy_mask=np.ones((2, 1), dtype=np.bool_),
        can_sell_mask=np.ones((2, 1), dtype=np.bool_),
        unresolved_corporate_action_mask=np.zeros((2, 1), dtype=np.bool_),
        initial_cash=1_000.0,
    )

    records = _tw_integer_holdings_records(
        integer_result,
        close_prices=np.array([[10.0], [9.0]], dtype=np.float64),
        symbols=["DIVIDEND"],
        dates=np.array(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
    )

    first_day = [record for record in records if record.date == "2024-01-02"]
    by_symbol = {record.symbol: record for record in first_day}
    assert by_symbol["CASH"].market_value == pytest.approx(0.0)
    assert by_symbol["DIVIDEND"].market_value == pytest.approx(1_000.0)
    assert integer_result.execution_receivable_history[0] == pytest.approx(0.0)
    assert integer_result.receivable_history[0] == pytest.approx(100.0)


def test_tw_day_trade_full_panel_ignores_inactive_invalid_price_legs() -> None:
    tradable = np.array([[True, False], [False, True]], dtype=np.bool_)
    result, _ = run_backtest_integer_shares(
        np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float64),
        future_returns=np.zeros((2, 2), dtype=np.float64),
        tradable_mask=tradable,
        benchmark_returns=np.zeros(2, dtype=np.float64),
        open_prices=np.array([[100.0, np.nan], [0.0, 50.0]], dtype=np.float64),
        close_prices=np.array([[101.0, np.nan], [-1.0, 49.0]], dtype=np.float64),
        execution_mode="tw_day_trade",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(2),
        sell_fee_rates=np.zeros(2),
        lot_sizes=np.full(2, 1_000, dtype=np.int64),
        day_trade_eligible_mask=tradable.copy(),
        day_trade_can_buy_open_mask=tradable.copy(),
        day_trade_can_sell_open_mask=tradable.copy(),
        initial_capital=100_000.0,
        collect_holdings=False,
    )

    np.testing.assert_array_equal(
        result.shares_history,
        np.array([[1_000, 0], [0, 2_000]], dtype=np.int64),
    )
    np.testing.assert_allclose(
        result.strategy_returns,
        [np.log(101_000.0 / 100_000.0), np.log(99_000.0 / 101_000.0)],
    )


def test_tw_cash_public_final_weights_use_end_valuation_not_execution_history() -> None:
    weights = np.array([[0.5]], dtype=np.float64)
    result, _ = run_backtest_integer_shares(
        weights,
        future_returns=np.array([[np.log(2.0)]], dtype=np.float64),
        tradable_mask=np.ones_like(weights, dtype=np.bool_),
        benchmark_returns=np.zeros(1),
        close_prices=np.array([[10.0]]),
        execution_mode="tw_cash",
        portfolio_activation="pre_normalized",
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        lot_sizes=np.ones(1, dtype=np.int64),
        unresolved_corporate_action_mask=np.zeros_like(weights, dtype=np.bool_),
        initial_capital=1_000.0,
        collect_holdings=False,
    )

    assert result.weights_history[0, 0] == pytest.approx(0.5)
    assert result.final_weights is not None
    assert result.final_weights[0] == pytest.approx(2.0 / 3.0)
