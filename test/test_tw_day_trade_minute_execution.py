from __future__ import annotations

import numpy as np
import torch

from stockagent.backtest.simulator import (
    BacktestResult,
    run_backtest_integer_shares,
    run_backtest_torch,
)
from stockagent.backtest.tw_day_trade_minute import (
    MARGIN_FINANCING_ONE_DAY_RATE,
    MARGIN_SHORT_HANDLING_FEE_RATE,
    MARGIN_SHORT_ONE_DAY_BORROW_RATE,
    run_tw_day_trade_minute_execution,
)
from stockagent.data.tw_day_trade_execution import (
    DAY_TRADE_EXECUTION_FIELD_COUNT,
    DayTradeExecutionField as F,
)
from stockagent.training.loss import risk_aware_loss


def _tape(days: int = 1, symbols: int = 1) -> torch.Tensor:
    tape = torch.full((days, symbols, DAY_TRADE_EXECUTION_FIELD_COUNT), float("nan"))
    tape[:, :, F.OFFICIAL_OPEN] = 100.0
    tape[:, :, F.ENTRY_VWAP_0901] = 101.0
    tape[:, :, F.ENTRY_VOLUME_0901] = 20_000.0
    tape[:, :, F.LIMIT_PRICE_1320] = 105.0
    tape[:, :, F.HIGH_1321] = 106.0
    tape[:, :, F.LOW_1321] = 104.0
    tape[:, :, F.VOLUME_1321] = 4_000.0
    tape[:, :, F.HIGH_1322] = 105.0
    tape[:, :, F.LOW_1322] = 105.0
    tape[:, :, F.VOLUME_1322] = 100_000.0
    tape[:, :, F.HIGH_1323] = 105.0
    tape[:, :, F.LOW_1323] = 105.0
    tape[:, :, F.VOLUME_1323] = 100_000.0
    tape[:, :, F.MARKET_VWAP_1324] = 103.0
    tape[:, :, F.MARKET_VOLUME_1324] = 4_000.0
    tape[:, :, F.AUCTION_PRICE_1330] = 102.0
    tape[:, :, F.AUCTION_VOLUME_1330] = 4_000.0
    return tape


def _run(
    weights: torch.Tensor,
    tape: torch.Tensor,
    *,
    buy_fee_rates: torch.Tensor | None = None,
    sell_fee_rates: torch.Tensor | None = None,
    normal_sell_fee_rates: torch.Tensor | None = None,
):
    mask = torch.ones_like(weights, dtype=torch.bool)
    zeros = torch.zeros(weights.size(1))
    buy_fees = zeros if buy_fee_rates is None else buy_fee_rates
    sell_fees = zeros if sell_fee_rates is None else sell_fee_rates
    normal_sell_fees = (
        sell_fees if normal_sell_fee_rates is None else normal_sell_fee_rates
    )
    return run_tw_day_trade_minute_execution(
        weights,
        tape,
        mask,
        mask,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        normal_sell_fees,
        initial_capital_twd=1_000_000.0,
    )


def test_daily_target_executes_first_minute_then_limit_market_and_auction() -> None:
    # 10% / official open 100 = exactly 1,000 requested shares.  The first
    # later bar penetrates the 105 sell limit and has 2,000-share capacity, so
    # the complete position exits there; equality-only later bars are inert.
    returns, turnover, history, equity = _run(torch.tensor([[0.10]]), _tape())
    assert torch.allclose(history, torch.tensor([[0.101]]))
    assert torch.allclose(returns.exp() - 1.0, torch.tensor([0.004]), atol=1e-7)
    assert torch.allclose(
        turnover, torch.tensor([(101_000.0 + 105_000.0) / 1_000_000.0])
    )
    assert torch.allclose(equity, torch.tensor([1.004]))


def test_touch_without_penetration_does_not_claim_queue_fill() -> None:
    tape = _tape()
    tape[:, :, F.HIGH_1321] = 105.0
    returns, _, _, _ = _run(torch.tensor([[0.10]]), tape)
    # 13:24 has enough capacity, so the exact forward PnL is 1,000*(103-101).
    assert torch.allclose(returns.exp() - 1.0, torch.tensor([0.002]), atol=1e-7)


def test_residual_long_and_short_use_unlimited_margin_costs_without_default() -> None:
    tape = _tape(symbols=2)
    for field in (
        F.VOLUME_1321,
        F.VOLUME_1322,
        F.VOLUME_1323,
        F.MARKET_VOLUME_1324,
        F.AUCTION_VOLUME_1330,
    ):
        tape[:, :, field] = 0.0
    buy_fees = torch.full((2,), 0.000855)
    day_trade_sell_fees = torch.full((2,), 0.002355)
    normal_sell_fees = torch.full((2,), 0.003855)
    returns, turnover, _, _ = _run(
        torch.tensor([[0.10, -0.10]]),
        tape,
        buy_fee_rates=buy_fees,
        sell_fee_rates=day_trade_sell_fees,
        normal_sell_fee_rates=normal_sell_fees,
    )
    long_pnl = (
        1_000.0 * (102.0 - 101.0)
        - 1_000.0 * 101.0 * 0.000855
        - 1_000.0 * 102.0 * 0.003855
        - 1_000.0 * 102.0 * MARGIN_FINANCING_ONE_DAY_RATE
    )
    short_pnl = (
        1_000.0 * (101.0 - 102.0)
        - 1_000.0 * 101.0 * 0.002355
        - 1_000.0 * 102.0 * 0.000855
        - 1_000.0 * 101.0 * (0.003855 - 0.002355)
        - 1_000.0 * 101.0 * MARGIN_SHORT_HANDLING_FEE_RATE
        - 1_000.0 * 102.0 * MARGIN_SHORT_ONE_DAY_BORROW_RATE
    )
    expected = (long_pnl + short_pnl) / 1_000_000.0
    assert torch.allclose(returns.exp() - 1.0, torch.tensor([expected]), atol=1e-7)
    assert torch.allclose(
        turnover,
        torch.tensor([(2.0 * 101_000.0 + 2.0 * 102_000.0) / 1_000_000.0]),
    )


def test_residual_margin_conversion_does_not_carry_position_to_next_day() -> None:
    tape = _tape(days=2)
    for field in (
        F.VOLUME_1321,
        F.VOLUME_1322,
        F.VOLUME_1323,
        F.MARKET_VOLUME_1324,
        F.AUCTION_VOLUME_1330,
    ):
        tape[:, :, field] = 0.0
    returns, turnover, history, _ = _run(torch.tensor([[0.10], [0.10]]), tape)
    assert torch.isfinite(returns).all()
    assert torch.all(history > 0.0)
    assert torch.all(turnover > 0.0)


def test_canonical_result_has_no_settlement_or_cross_day_position_state() -> None:
    weights = torch.tensor([[0.10], [0.10]])
    mask = torch.ones_like(weights, dtype=torch.bool)
    tape = _tape(days=2)
    for field in (
        F.VOLUME_1321,
        F.VOLUME_1322,
        F.VOLUME_1323,
        F.MARKET_VOLUME_1324,
        F.AUCTION_VOLUME_1330,
    ):
        tape[:, :, field] = 0.0
    zeros = torch.zeros(1)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(2),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        execution_mode="tw_day_trade",
        buy_fee_rates=zeros,
        sell_fee_rates=zeros,
        normal_sell_fee_rates=zeros,
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        overnight_returns=tape,
        day_trade_execution_initial_capital=1_000_000.0,
    )
    assert torch.equal(result.final_weights, torch.zeros_like(result.final_weights))
    assert result.settlement_default is not None
    assert not bool(result.settlement_default.any())
    assert result.cash_history is not None
    assert torch.equal(result.cash_history, torch.ones_like(result.cash_history))


def test_t_profit_settles_after_t_plus_2_close_and_sizes_only_t_plus_3() -> None:
    tape = _tape(days=4)
    # T earns exactly one initial-capital unit from one 1,000-share lot.
    tape[0, :, F.LIMIT_PRICE_1320] = 1_101.0
    tape[0, :, F.HIGH_1321] = 1_102.0
    # Later sessions close flat at the entry price.  Their entries reveal
    # whether the pending T gain was incorrectly used for sizing.
    tape[1:, :, F.LIMIT_PRICE_1320] = 101.0
    tape[1:, :, F.HIGH_1321] = 102.0
    result = _run(torch.full((4, 1), 0.10), tape)

    # T economic PnL is recognized immediately by the loss/NAV.
    assert torch.allclose(
        result.strategy_returns.exp() - 1.0,
        torch.tensor([1.0, 0.0, 0.0, 0.0]),
        atol=1.0e-7,
    )
    # It remains a receivable through T+1. T+2 orders still use cash=1.0;
    # only after those orders does the close settlement raise cash to 2.0.
    assert torch.allclose(
        result.cash_history,
        torch.tensor([1.0, 1.0, 2.0, 2.0]),
        atol=1.0e-7,
    )
    assert torch.allclose(
        result.receivables_history,
        torch.tensor([[0.0, 1.0], [1.0, 0.0], [0.0, 0.0], [0.0, 0.0]]),
        atol=1.0e-7,
    )
    # Relative to economic NAV=2, T+1/T+2 execute one lot (5.05%), while
    # T+3 can finally use the settled cash and executes two lots (10.10%).
    assert torch.allclose(
        result.weights_history[:, 0],
        torch.tensor([0.1010, 0.0505, 0.0505, 0.1010]),
        atol=1.0e-7,
    )


def test_canonical_minute_result_carries_only_net_t_plus_2_claim() -> None:
    weights = torch.tensor([[0.10], [0.0], [0.0]])
    mask = torch.ones_like(weights, dtype=torch.bool)
    tape = _tape(days=3)
    zeros = torch.zeros(1)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(3),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        execution_mode="tw_day_trade",
        buy_fee_rates=zeros,
        sell_fee_rates=zeros,
        normal_sell_fee_rates=zeros,
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        overnight_returns=tape,
        day_trade_execution_initial_capital=1_000_000.0,
        settlement_lag_sessions=2,
    )
    assert result.receivables_history is not None
    assert result.payables_history is not None
    assert result.cash_history is not None
    assert torch.allclose(
        result.receivables_history,
        torch.tensor([[0.0, 0.004], [0.004, 0.0], [0.0, 0.0]]),
        atol=1.0e-7,
    )
    assert torch.equal(
        result.payables_history, torch.zeros_like(result.payables_history)
    )
    assert torch.allclose(
        result.cash_history, torch.tensor([1.0, 1.0, 1.004]), atol=1.0e-7
    )
    assert torch.equal(result.final_weights, torch.zeros_like(result.final_weights))


def test_log_utility_loss_carries_the_same_t_plus_2_net_claim_state() -> None:
    weights = torch.tensor([[0.10], [0.0], [0.0]], requires_grad=True)
    mask = torch.ones_like(weights, dtype=torch.bool)
    aux: dict[str, torch.Tensor] = {}
    zeros = torch.zeros(1)
    loss = risk_aware_loss(
        weights,
        torch.zeros_like(weights),
        mask,
        benchmark_returns=torch.zeros(3),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        long_only=False,
        portfolio_activation="pre_normalized",
        execution_mode="tw_day_trade",
        buy_fee_rates=zeros,
        sell_fee_rates=zeros,
        normal_sell_fee_rates=zeros,
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        settlement_lag_sessions=2,
        objective="log_utility",
        gamma_turnover=0.0,
        rank_ic_weight=0.0,
        return_rank_ic_weight=0.0,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        concentration_weight=0.0,
        overnight_log_returns=_tape(days=3),
        day_trade_execution_initial_capital=1_000_000.0,
        aux_outputs=aux,
    )
    loss.backward()
    assert torch.isfinite(loss)
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert torch.allclose(aux["_final_cash"], torch.tensor(1.004), atol=1.0e-7)
    assert torch.equal(aux["_final_payables"], torch.zeros_like(aux["_final_payables"]))
    assert torch.equal(
        aux["_final_receivables"], torch.zeros_like(aux["_final_receivables"])
    )


def test_exact_forward_lot_rounding_still_supplies_training_gradient() -> None:
    weights = torch.tensor([[0.151]], requires_grad=True)
    returns, _, _, _ = _run(weights, _tape())
    loss = -returns.mean()
    loss.backward()
    # Forward request is exactly one board lot even though the raw request is
    # 1,510 shares; backward remains useful via the documented STE.
    assert torch.allclose(returns.exp() - 1.0, torch.tensor([0.004]), atol=1e-7)
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0.0


def test_two_sided_request_keeps_side_with_an_executable_lot() -> None:
    tape = _tape(symbols=2)
    tape[:, 1, F.ENTRY_VOLUME_0901] = 0.0
    returns, turnover, history, _ = _run(torch.tensor([[0.10, -0.10]]), tape)
    assert history[0, 0] > 0.0
    assert history[0, 1] == 0.0
    assert turnover[0] > 0.0
    assert returns[0] != 0.0


def test_independent_side_execution_keeps_nonzero_gradient() -> None:
    tape = _tape(symbols=2)
    tape[:, 1, F.ENTRY_VOLUME_0901] = 0.0
    weights = torch.tensor([[0.10, -0.10]], requires_grad=True)
    returns, _, history, _ = _run(weights, tape)
    assert history[0, 0] > 0.0
    assert history[0, 1] == 0.0
    (-returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0.0


def test_missing_exit_prices_fail_closed_without_nan_gradient() -> None:
    tape = _tape()
    for field in (
        F.LIMIT_PRICE_1320,
        F.MARKET_VWAP_1324,
        F.AUCTION_PRICE_1330,
    ):
        tape[:, :, field] = float("nan")
    weights = torch.tensor([[0.10]], requires_grad=True)
    returns, _, _, _ = _run(weights, tape)
    assert torch.isfinite(returns).all()
    (-returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_four_day_block_and_padded_tail_preserve_daily_recurrence() -> None:
    weights = torch.full((6, 1), 0.10, requires_grad=True)
    returns, _, history, equity = _run(weights, _tape(days=6))
    assert returns.shape == (6,)
    assert history.shape == (6, 1)
    expected_simple = 0.004 / (1.0 + torch.arange(6, dtype=torch.float32) * 0.004)
    assert torch.allclose(returns.exp() - 1.0, expected_simple, atol=1.0e-7)
    expected_equity = 1.0 + torch.arange(1, 7, dtype=torch.float32) * 0.004
    assert torch.allclose(equity, expected_equity, atol=1.0e-6)
    (-returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_exact_minute_result_is_not_replaced_by_open_close_audit() -> None:
    exact = BacktestResult(
        strategy_returns=np.asarray([0.01, -0.02], dtype=np.float64),
        benchmark_returns=np.zeros(2, dtype=np.float64),
        turnovers=np.asarray([0.2, 0.3], dtype=np.float64),
        weights_history=np.zeros((2, 1), dtype=np.float64),
        execution_mode="tw_day_trade",
    )
    result, holdings = run_backtest_integer_shares(
        weights=np.zeros((2, 1), dtype=np.float64),
        future_returns=np.zeros((2, 1), dtype=np.float64),
        tradable_mask=np.ones((2, 1), dtype=bool),
        benchmark_returns=np.zeros(2, dtype=np.float64),
        execution_mode="tw_day_trade",
        precomputed_exact_backtest=exact,
    )
    assert result is exact
    assert holdings == []
