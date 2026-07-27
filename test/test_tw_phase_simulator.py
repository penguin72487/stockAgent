from __future__ import annotations

import math

import numpy as np
import pytest
import torch

from stockagent.backtest.simulator import (
    run_backtest_integer_shares,
    run_backtest_torch,
)
from stockagent.training.loss import risk_aware_loss


def _common(rows: int) -> dict[str, torch.Tensor]:
    daily = torch.ones((rows, 1), dtype=torch.bool)
    return {
        "tradable_mask": daily,
        "benchmark_returns": torch.zeros(rows, dtype=torch.float32),
        "can_buy_mask": daily.clone(),
        "can_sell_mask": daily.clone(),
        "day_trade_can_buy_open_mask": daily.clone(),
        "day_trade_can_sell_open_mask": daily.clone(),
        "unresolved_corporate_action_mask": torch.zeros_like(daily),
        "buy_fee_rates": torch.zeros(1, dtype=torch.float32),
        "sell_fee_rates": torch.zeros(1, dtype=torch.float32),
    }


def test_tw_cash_dispatch_keeps_open_and_close_fill_masks_independent() -> None:
    common = _common(1)
    common["day_trade_can_buy_open_mask"][0, 0] = False
    actions = torch.tensor([[[0.4], [0.4]]], dtype=torch.float32)

    result = run_backtest_torch(
        actions,
        torch.zeros((1, 1), dtype=torch.float32),
        common.pop("tradable_mask"),
        common.pop("benchmark_returns"),
        0.0,
        0.0,
        execution_mode="tw_cash",
        overnight_returns=torch.zeros((1, 1), dtype=torch.float32),
        portfolio_activation="pre_normalized",
        **common,
    )

    assert result.executed_long_buy_weights is not None
    torch.testing.assert_close(
        result.executed_long_buy_weights[0, :, 0],
        torch.tensor([0.0, 0.4]),
    )
    torch.testing.assert_close(result.final_weights, torch.tensor([0.4]))


def test_tw_cash_dispatch_uses_distinct_open_and_close_short_masks() -> None:
    common = _common(1)
    actions = torch.tensor([[[-0.4], [-0.4]]], dtype=torch.float32)
    close_short = torch.ones((1, 1), dtype=torch.bool)
    open_short = torch.zeros((1, 1), dtype=torch.bool)

    result = run_backtest_torch(
        actions,
        torch.zeros((1, 1), dtype=torch.float32),
        common.pop("tradable_mask"),
        common.pop("benchmark_returns"),
        0.0,
        0.0,
        execution_mode="tw_cash",
        overnight_returns=torch.zeros((1, 1), dtype=torch.float32),
        portfolio_activation="pre_normalized",
        long_only=False,
        can_short_open_mask=close_short,
        can_short_open_open_mask=open_short,
        short_capacity_weights=torch.ones((1, 1), dtype=torch.float32),
        short_margin_rate=torch.full((1, 1), 0.9),
        **common,
    )

    assert result.executed_short_open_weights is not None
    torch.testing.assert_close(
        result.executed_short_open_weights[0, :, 0],
        torch.tensor([0.0, 0.4]),
    )
    assert result.final_weights is not None
    assert result.final_weights[0] < 0.0


@pytest.mark.parametrize(
    ("execution_mode", "phases"),
    [("tw_cash", 2), ("tw_overnight", 3)],
)
def test_phase_integer_prepare_masks_before_shared_gross_budget(
    execution_mode: str,
    phases: int,
) -> None:
    """Invalid logits cannot dilute exact orders relative to tensor execution."""

    rows, symbols = 1, 2
    tradable = np.array([[True, False]], dtype=np.bool_)
    side_mask = np.ones((rows, symbols), dtype=np.bool_)
    unresolved = np.zeros((rows, symbols), dtype=np.bool_)
    actions = np.zeros((rows, phases, symbols), dtype=np.float64)
    if execution_mode == "tw_cash":
        actions[:, :, 0] = 1.0
        actions[:, :, 1] = 100.0
    else:
        actions[:, 1:, 0] = 1.0
        actions[:, 1:, 1] = 100.0

    exact, _holdings = run_backtest_integer_shares(
        actions,
        np.zeros((rows, symbols), dtype=np.float64),
        tradable,
        np.zeros(rows, dtype=np.float64),
        can_buy_mask=side_mask,
        can_sell_mask=side_mask,
        close_prices=np.ones((rows, symbols), dtype=np.float64),
        open_prices=np.ones((rows, symbols), dtype=np.float64),
        day_trade_can_buy_open_mask=side_mask,
        day_trade_can_sell_open_mask=side_mask,
        unresolved_corporate_action_mask=unresolved,
        buy_fee_rates=np.zeros(symbols, dtype=np.float64),
        sell_fee_rates=np.zeros(symbols, dtype=np.float64),
        lot_sizes=np.ones(symbols, dtype=np.int64),
        symbols=["A", "B"],
        collect_holdings=False,
        execution_mode=execution_mode,
        portfolio_activation="identity",
        initial_capital=1_000_000.0,
    )
    tensor = run_backtest_torch(
        torch.as_tensor(actions, dtype=torch.float32),
        torch.zeros((rows, symbols), dtype=torch.float32),
        torch.as_tensor(tradable),
        torch.zeros(rows, dtype=torch.float32),
        0.0,
        0.0,
        execution_mode=execution_mode,
        overnight_returns=torch.zeros((rows, symbols), dtype=torch.float32),
        portfolio_activation="identity",
        can_buy_mask=torch.as_tensor(side_mask),
        can_sell_mask=torch.as_tensor(side_mask),
        day_trade_can_buy_open_mask=torch.as_tensor(side_mask),
        day_trade_can_sell_open_mask=torch.as_tensor(side_mask),
        unresolved_corporate_action_mask=torch.as_tensor(unresolved),
        buy_fee_rates=torch.zeros(symbols, dtype=torch.float32),
        sell_fee_rates=torch.zeros(symbols, dtype=torch.float32),
    )

    assert tensor.final_weights is not None
    np.testing.assert_allclose(
        exact.final_weights,
        tensor.final_weights.detach().cpu().numpy(),
        rtol=0.0,
        atol=0.0,
    )
    np.testing.assert_allclose(
        exact.turnovers,
        tensor.turnovers.detach().cpu().numpy(),
        rtol=0.0,
        atol=0.0,
    )
    np.testing.assert_array_equal(exact.final_weights, [1.0, 0.0])


def test_overnight_canonical_loss_backpropagates_through_all_three_heads() -> None:
    common = _common(2)
    actions = torch.tensor(
        [
            [[0.5], [0.0], [0.25]],
            [[0.5], [0.20], [0.10]],
        ],
        dtype=torch.float32,
        requires_grad=True,
    )
    intraday = torch.tensor(
        [[0.0], [math.log(1.10)]],
        dtype=torch.float32,
    )
    overnight = torch.tensor(
        [[0.0], [math.log(1.05)]],
        dtype=torch.float32,
    )

    loss = risk_aware_loss(
        actions,
        intraday,
        common.pop("tradable_mask"),
        common.pop("benchmark_returns"),
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        objective="log_utility",
        execution_mode="tw_overnight",
        overnight_log_returns=overnight,
        portfolio_activation="pre_normalized",
        **common,
    )
    loss.backward()

    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    for phase in range(3):
        assert actions.grad[:, phase].abs().sum() > 0.0
