from __future__ import annotations

import math

import numpy as np
import pytest
import torch

import stockagent.backtest.tw_continuous as tw_continuous_module
from stockagent.backtest.simulator import run_backtest, run_backtest_torch
from stockagent.backtest.tw_continuous import (
    run_tw_cash_continuous,
    run_tw_day_trade_continuous,
)
from stockagent.backtest.tw_integer_execution import (
    run_tw_cash_integer,
    run_tw_day_trade_integer,
)
from stockagent.training.loss import risk_aware_loss


def _mask_like(values: torch.Tensor) -> torch.Tensor:
    return torch.ones_like(values, dtype=torch.bool)


def _real_execution_masks(
    values: torch.Tensor,
    execution_mode: str,
) -> dict[str, torch.Tensor]:
    if execution_mode == "tw_cash":
        return {
            "unresolved_corporate_action_mask": torch.zeros_like(
                values, dtype=torch.bool
            )
        }
    return {
        "day_trade_can_buy_open_mask": torch.ones_like(values, dtype=torch.bool),
        "day_trade_can_sell_open_mask": torch.ones_like(values, dtype=torch.bool),
    }


def _assert_tw_cash_margin_identity(result) -> None:
    """Check every normalized row of the cash-plus-margin double-entry ledger."""

    assert result.cash_history is not None
    assert result.payables_history is not None
    assert result.receivables_history is not None
    assert result.short_sale_collateral_history is not None
    assert result.short_margin_collateral_history is not None
    ledger_equity = (
        result.cash_history
        + result.weights_history.sum(dim=1)
        + result.short_sale_collateral_history.sum(dim=1)
        + result.short_margin_collateral_history.sum(dim=1)
        + result.receivables_history.sum(dim=1)
        - result.payables_history.sum(dim=1)
    )
    torch.testing.assert_close(ledger_equity, torch.ones_like(ledger_equity))


def test_naive_dispatch_is_exactly_backward_compatible() -> None:
    weights = torch.tensor([[0.8, 0.2], [0.1, 0.9], [0.4, 0.6]], dtype=torch.float32)
    returns = torch.tensor([[0.01, -0.02], [0.03, 0.01], [-0.01, 0.02]], dtype=torch.float32)
    mask = _mask_like(weights)
    benchmark = torch.zeros(3)

    legacy = run_backtest_torch(weights, returns, mask, benchmark, 0.001, 0.002)
    explicit = run_backtest_torch(
        weights,
        returns,
        mask,
        benchmark,
        0.001,
        0.002,
        execution_mode="naive",
    )

    assert torch.equal(legacy.strategy_returns, explicit.strategy_returns)
    assert torch.equal(legacy.turnovers, explicit.turnovers)
    assert torch.equal(legacy.weights_history, explicit.weights_history)
    assert explicit.execution_mode == "naive"
    assert explicit.final_cash is None


def test_tw_cash_manual_ledger_and_t_plus_two_payment() -> None:
    # Buy 0.5 NAV with a 10% commission and then freeze the holding.  The trade
    # creates a 0.55 payable, so trade-date NAV is 0.95.  It is paid exactly on
    # the second later session, never on the intervening session.
    weights = torch.tensor([[0.5], [0.5 / 0.95], [0.5 / 0.95]], dtype=torch.float64)
    returns = torch.zeros_like(weights)
    tradable = torch.tensor([[True], [False], [False]])
    benchmark = torch.zeros(3, dtype=torch.float64)
    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        0.0,
        0.0,
        gross_leverage=0.5,
        execution_mode="tw_cash",
        buy_fee_rates=torch.tensor([0.10], dtype=torch.float64),
        sell_fee_rates=torch.tensor([0.0], dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
    )

    assert result.strategy_returns[0].item() == pytest.approx(math.log(0.95))
    torch.testing.assert_close(
        result.strategy_returns[1:], torch.zeros_like(result.strategy_returns[1:])
    )
    assert result.payables_history is not None
    assert result.cash_history is not None
    assert result.payables_history[0, 1].item() == pytest.approx(0.55 / 0.95)
    assert result.payables_history[1, 0].item() == pytest.approx(0.55 / 0.95)
    assert result.payables_history[2].sum().item() == pytest.approx(0.0)
    assert result.cash_history[1].item() == pytest.approx(1.0 / 0.95)
    assert result.cash_history[2].item() == pytest.approx((1.0 - 0.55) / 0.95)
    assert result.final_alive is not None and bool(result.final_alive)


def test_tw_cash_margin_short_open_locks_proceeds_and_posts_t_plus_two_margin() -> None:
    # Open a 0.5-NAV credit short.  The 1.0% sell cost plus 0.5% short handling
    # fee leaves 0.4925 in restricted sale collateral.  The 90% initial margin
    # is both restricted collateral and a T+2 payable, so neither amount is
    # free buying power.  Only the 0.0075 sale costs reduce trade-date equity.
    target = torch.tensor([[-0.5]], dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    result = run_tw_cash_continuous(
        target,
        torch.zeros_like(target),
        mask,
        mask,
        mask,
        torch.tensor([0.02], dtype=torch.float64),
        torch.tensor([0.01], dtype=torch.float64),
        can_short_open_mask=mask,
        short_margin_rate=0.90,
        short_capacity_weights=torch.full_like(target, 0.5),
        short_handling_fee_rate=0.005,
        unresolved_corporate_action_mask=torch.zeros_like(mask),
    )

    nav = 1.0 - 0.5 + 0.5 * (1.0 - 0.01 - 0.005)
    assert nav == pytest.approx(0.9925)
    assert result.strategy_returns[0].item() == pytest.approx(math.log(nav))
    assert result.weights_history[0, 0].item() == pytest.approx(-0.5 / nav)
    assert result.cash_history[0].item() == pytest.approx(1.0 / nav)
    assert result.short_sale_collateral_history[0, 0].item() == pytest.approx(
        0.4925 / nav
    )
    assert result.short_margin_collateral_history[0, 0].item() == pytest.approx(
        0.45 / nav
    )
    assert result.payables_history[0, 1].item() == pytest.approx(0.45 / nav)
    assert result.receivables_history[0].sum().item() == pytest.approx(0.0)
    _assert_tw_cash_margin_identity(result)


def test_tw_cash_margin_short_cover_releases_collateral_as_t_plus_two_net_claim() -> None:
    # With zero prices/costs, opening a 0.5 short posts 0.45 margin on T+2.
    # Covering releases sale collateral 0.50 plus margin 0.45, pays 0.50 for
    # the shares, and therefore creates one net 0.45 T+2 receivable.
    target = torch.tensor(
        [[-0.5], [-0.5], [-0.5], [0.0], [0.0], [0.0]],
        dtype=torch.float64,
    )
    mask = torch.ones_like(target, dtype=torch.bool)
    result = run_tw_cash_continuous(
        target,
        torch.zeros_like(target),
        mask,
        mask,
        mask,
        torch.zeros(1, dtype=torch.float64),
        torch.zeros(1, dtype=torch.float64),
        can_short_open_mask=mask,
        short_margin_rate=0.90,
        short_capacity_weights=torch.full_like(target, 0.5),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
    )

    assert result.payables_history[0, 1].item() == pytest.approx(0.45)
    assert result.payables_history[1, 0].item() == pytest.approx(0.45)
    assert result.cash_history[2].item() == pytest.approx(0.55)
    assert result.short_sale_collateral_history[2, 0].item() == pytest.approx(0.50)
    assert result.short_margin_collateral_history[2, 0].item() == pytest.approx(0.45)
    assert result.weights_history[3, 0].item() == pytest.approx(0.0)
    assert result.short_sale_collateral_history[3, 0].item() == pytest.approx(0.0)
    assert result.short_margin_collateral_history[3, 0].item() == pytest.approx(0.0)
    assert result.receivables_history[3, 1].item() == pytest.approx(0.45)
    assert result.receivables_history[4, 0].item() == pytest.approx(0.45)
    assert result.cash_history[5].item() == pytest.approx(1.0)
    _assert_tw_cash_margin_identity(result)


def test_tw_cash_partial_cover_retains_whole_account_maintenance_collateral() -> None:
    # The first short carries 1.00 collateral and the second carries 0.50.
    # Covering the first would ordinarily release its full 1.00, but the
    # remaining 0.50 liability needs 1.30 * 0.50 = 0.65 collateral.  Therefore
    # only 0.85 is released and the retained 0.65 is reassigned to the remaining
    # per-symbol short pools without adding another recurrent state.
    target = torch.tensor([[0.0, -0.5]], dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    result = run_tw_cash_continuous(
        target,
        torch.zeros_like(target),
        mask,
        mask,
        mask,
        torch.zeros(2, dtype=torch.float64),
        torch.zeros(2, dtype=torch.float64),
        short_maintenance_ratio=1.30,
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        initial_weights=torch.tensor([-0.5, -0.5], dtype=torch.float64),
        initial_cash=torch.tensor(0.5, dtype=torch.float64),
        initial_short_sale_collateral=torch.tensor(
            [0.8, 0.2], dtype=torch.float64
        ),
        initial_short_margin_collateral=torch.tensor(
            [0.2, 0.3], dtype=torch.float64
        ),
    )

    torch.testing.assert_close(
        result.short_sale_collateral_history[0],
        torch.tensor([0.0, 0.32], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.short_margin_collateral_history[0],
        torch.tensor([0.0, 0.33], dtype=torch.float64),
    )
    assert result.receivables_history[0, 1].item() == pytest.approx(0.35)
    assert (
        result.short_sale_collateral_history[0].sum()
        + result.short_margin_collateral_history[0].sum()
    ).item() == pytest.approx(1.30 * 0.5)
    _assert_tw_cash_margin_identity(result)


def test_tw_cash_undermaintenance_partial_cover_releases_no_collateral() -> None:
    # A pre-existing maintenance deficit is not an instruction to fabricate a
    # same-day margin-call cure.  The cover may execute, but Article 56 permits
    # no collateral release until the remaining account is above its floor.
    target = torch.tensor([[0.0, -0.5]], dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    result = run_tw_cash_continuous(
        target,
        torch.zeros_like(target),
        mask,
        mask,
        mask,
        torch.zeros(2, dtype=torch.float64),
        torch.zeros(2, dtype=torch.float64),
        short_maintenance_ratio=1.30,
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        initial_weights=torch.tensor([-0.5, -0.5], dtype=torch.float64),
        initial_cash=torch.tensor(1.5, dtype=torch.float64),
        initial_short_sale_collateral=torch.tensor(
            [0.2, 0.1], dtype=torch.float64
        ),
        initial_short_margin_collateral=torch.tensor(
            [0.1, 0.1], dtype=torch.float64
        ),
    )

    torch.testing.assert_close(
        result.short_sale_collateral_history[0],
        torch.tensor([0.0, 0.3], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.short_margin_collateral_history[0],
        torch.tensor([0.0, 0.2], dtype=torch.float64),
    )
    assert result.receivables_history[0].sum().item() == pytest.approx(0.0)
    assert result.payables_history[0].sum().item() == pytest.approx(0.5)
    assert result.final_alive.item()
    assert not result.settlement_default.any().item()
    _assert_tw_cash_margin_identity(result)


def test_tw_cash_full_cover_releases_all_restricted_short_collateral() -> None:
    target = torch.zeros((1, 2), dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    result = run_tw_cash_continuous(
        target,
        target,
        mask,
        mask,
        mask,
        torch.zeros(2, dtype=torch.float64),
        torch.zeros(2, dtype=torch.float64),
        short_maintenance_ratio=1.30,
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        initial_weights=torch.tensor([-0.5, -0.5], dtype=torch.float64),
        initial_cash=torch.tensor(0.5, dtype=torch.float64),
        initial_short_sale_collateral=torch.tensor(
            [0.8, 0.2], dtype=torch.float64
        ),
        initial_short_margin_collateral=torch.tensor(
            [0.2, 0.3], dtype=torch.float64
        ),
    )

    torch.testing.assert_close(
        result.short_sale_collateral_history,
        torch.zeros_like(result.short_sale_collateral_history),
    )
    torch.testing.assert_close(
        result.short_margin_collateral_history,
        torch.zeros_like(result.short_margin_collateral_history),
    )
    assert result.receivables_history[0, 1].item() == pytest.approx(0.5)
    _assert_tw_cash_margin_identity(result)


def test_tw_cash_forced_short_cover_becomes_absorbing_default_if_buy_is_blocked() -> None:
    target = torch.full((2, 1), -0.5, dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    can_buy = mask.clone()
    can_buy[1] = False
    force_cover = torch.tensor([[False], [True]])
    result = run_tw_cash_continuous(
        target,
        torch.zeros_like(target),
        mask,
        can_buy,
        mask,
        torch.zeros(1, dtype=torch.float64),
        torch.zeros(1, dtype=torch.float64),
        can_short_open_mask=mask,
        force_short_cover_mask=force_cover,
        short_margin_rate=0.90,
        short_capacity_weights=torch.full_like(target, 0.5),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
    )

    assert result.settlement_default.tolist() == [False, True]
    assert result.strategy_returns[1].item() == pytest.approx(math.log(1.0e-6))
    assert not result.final_alive.item()
    torch.testing.assert_close(result.final_weights, torch.zeros_like(result.final_weights))


def test_tw_cash_margin_short_recurrent_chunk_state_matches_full_run() -> None:
    # Split while the opening margin payable is still pending.  Exact replay
    # therefore depends on carrying both settlement queues and both restricted
    # collateral vectors across the boundary.
    target = torch.tensor(
        [[-0.5], [-0.5], [-0.5], [0.0], [0.0], [0.0]],
        dtype=torch.float64,
    )
    returns = torch.zeros_like(target)
    mask = torch.ones_like(target, dtype=torch.bool)
    actions = torch.zeros_like(mask)
    fees = torch.zeros(1, dtype=torch.float64)

    def execute(
        start: int,
        end: int,
        **initial_state,
    ):
        return run_tw_cash_continuous(
            target[start:end],
            returns[start:end],
            mask[start:end],
            mask[start:end],
            mask[start:end],
            fees,
            fees,
            can_short_open_mask=mask[start:end],
            short_margin_rate=0.90,
            short_capacity_weights=torch.full_like(target[start:end], 0.5),
            unresolved_corporate_action_mask=actions[start:end],
            **initial_state,
        )

    full = execute(0, len(target))
    prefix = execute(0, 2)
    suffix = execute(
        2,
        len(target),
        initial_weights=prefix.final_weights,
        initial_cash=prefix.final_cash,
        initial_payables=prefix.final_payables,
        initial_receivables=prefix.final_receivables,
        initial_alive=prefix.final_alive,
        initial_equity_scale=prefix.final_equity_scale,
        initial_short_sale_collateral=prefix.final_short_sale_collateral,
        initial_short_margin_collateral=prefix.final_short_margin_collateral,
    )

    for field in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "short_sale_collateral_history",
        "short_margin_collateral_history",
        "settlement_default",
        "equity_scale_history",
    ):
        replayed = torch.cat((getattr(prefix, field), getattr(suffix, field)), dim=0)
        torch.testing.assert_close(replayed, getattr(full, field), rtol=0.0, atol=0.0)
    for field in (
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_alive",
        "final_equity_scale",
        "final_short_sale_collateral",
        "final_short_margin_collateral",
    ):
        torch.testing.assert_close(
            getattr(suffix, field),
            getattr(full, field),
            rtol=0.0,
            atol=0.0,
        )
    _assert_tw_cash_margin_identity(full)


def test_tw_day_trade_manual_round_trip_and_flat_close() -> None:
    weights = torch.tensor([[1.0], [0.0], [0.0]], dtype=torch.float64)
    intraday = torch.tensor([[math.log(1.10)], [0.0], [0.0]], dtype=torch.float64)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        intraday,
        mask,
        torch.zeros(3, dtype=torch.float64),
        0.0,
        0.0,
        long_only=True,
        execution_mode="tw_day_trade",
        buy_fee_rates=torch.tensor([0.01], dtype=torch.float64),
        sell_fee_rates=torch.tensor([0.02], dtype=torch.float64),
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
    )

    # Buy claim=1.01, sell claim=1.10*(1-.02)=1.078, net T+2 receivable=.068.
    assert result.strategy_returns[0].item() == pytest.approx(math.log(1.068), abs=1.0e-6)
    assert result.receivables_history is not None
    assert result.receivables_history[0, 1].item() == pytest.approx(0.068 / 1.068, abs=1.0e-6)
    assert result.receivables_history[1, 0].item() == pytest.approx(0.068 / 1.068, abs=1.0e-6)
    assert result.receivables_history[2].sum().item() == pytest.approx(0.0)
    assert result.final_weights is not None
    torch.testing.assert_close(result.final_weights, torch.zeros_like(result.final_weights))


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
def test_simulator_dispatch_exposes_initial_and_final_equity_scale(
    execution_mode: str,
) -> None:
    weights = torch.tensor([[0.4], [0.6]], dtype=torch.float64)
    returns = torch.zeros_like(weights)
    mask = _mask_like(weights)
    mode_kwargs: dict[str, torch.Tensor] = {
        "buy_fee_rates": torch.zeros(1, dtype=torch.float64),
        "sell_fee_rates": torch.zeros(1, dtype=torch.float64),
    }
    if execution_mode == "tw_cash":
        mode_kwargs["unresolved_corporate_action_mask"] = torch.zeros_like(
            mask
        )
    else:
        mode_kwargs["day_trade_eligible_mask"] = mask
        mode_kwargs["day_trade_can_buy_open_mask"] = mask
        mode_kwargs["day_trade_can_sell_open_mask"] = mask

    torch_result = run_backtest_torch(
        weights,
        returns,
        mask,
        torch.zeros(2, dtype=torch.float64),
        0.0,
        0.0,
        execution_mode=execution_mode,
        initial_equity_scale=torch.tensor(0.25, dtype=torch.float64),
        **mode_kwargs,
    )
    assert torch_result.equity_scale_history is not None
    assert torch_result.final_equity_scale is not None
    assert torch_result.equity_scale_history.dtype == torch.float32
    assert torch_result.final_equity_scale.dtype == torch.float32
    torch.testing.assert_close(
        torch_result.equity_scale_history,
        torch.full((2,), 0.25, dtype=torch.float32),
    )
    torch.testing.assert_close(
        torch_result.final_equity_scale,
        torch.tensor(0.25, dtype=torch.float32),
    )

    numpy_result = run_backtest(
        weights.numpy(),
        returns.numpy(),
        mask.numpy(),
        np.zeros(2, dtype=np.float64),
        0.0,
        0.0,
        execution_mode=execution_mode,
        initial_equity_scale=np.asarray(0.25, dtype=np.float64),
        **{key: value.numpy() for key, value in mode_kwargs.items()},
    )
    assert numpy_result.equity_scale_history is not None
    assert numpy_result.final_equity_scale is not None
    np.testing.assert_allclose(numpy_result.equity_scale_history, 0.25)
    np.testing.assert_allclose(numpy_result.final_equity_scale, 0.25)


@pytest.mark.parametrize(
    ("replacement_weight", "expected_receivable"),
    [(1.0, 0.0), (0.5, 0.5)],
)
def test_tw_cash_same_trade_date_uses_one_net_settlement_claim(
    replacement_weight: float,
    expected_receivable: float,
) -> None:
    weights = torch.tensor([[0.0, replacement_weight]], dtype=torch.float64)
    mask = _mask_like(weights)
    result = run_tw_cash_continuous(
        weights,
        torch.zeros_like(weights),
        mask,
        mask,
        mask,
        torch.zeros(2, dtype=torch.float64),
        torch.zeros(2, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        initial_weights=torch.tensor([1.0, 0.0], dtype=torch.float64),
        initial_cash=torch.tensor(0.0, dtype=torch.float64),
    )

    # TWSE nets all buys and sells from one trade date.  The sale can fund the
    # replacement purchase; the account never carries simultaneous gross
    # payable and receivable claims for that date.
    assert result.final_payables.sum().item() == pytest.approx(0.0)
    assert result.final_receivables[-1].item() == pytest.approx(expected_receivable)
    assert not (
        result.final_payables[-1].item() > 0.0
        and result.final_receivables[-1].item() > 0.0
    )


def test_tw_cash_negative_forced_sale_proceeds_become_a_net_payable() -> None:
    weights = torch.zeros((1, 1), dtype=torch.float64)
    mask = _mask_like(weights)
    result = run_tw_cash_continuous(
        weights,
        torch.zeros_like(weights),
        mask,
        mask,
        mask,
        torch.zeros(1, dtype=torch.float64),
        torch.tensor([1.5], dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        force_exit_mask=mask,
        initial_weights=torch.tensor([0.1], dtype=torch.float64),
        initial_cash=torch.tensor(0.9, dtype=torch.float64),
    )

    torch.testing.assert_close(result.weights_history, torch.zeros_like(weights))
    assert result.final_payables[-1].item() == pytest.approx(0.05 / 0.85)
    assert result.final_receivables.sum().item() == pytest.approx(0.0)
    assert result.strategy_returns[0].item() == pytest.approx(math.log(0.85))


@pytest.mark.parametrize("bad_return", [float("nan"), float("inf"), float("-inf"), 1.0e4])
def test_tw_cash_defaults_on_nonfinite_valuation_only_for_executed_holdings(
    bad_return: float,
) -> None:
    mask = torch.ones((1, 1), dtype=torch.bool)
    common = dict(
        tradable_mask=mask,
        can_buy_mask=mask,
        can_sell_mask=mask,
        buy_fee_rates=torch.zeros(1, dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
    )
    inactive = run_tw_cash_continuous(
        torch.zeros((1, 1), dtype=torch.float64),
        torch.tensor([[bad_return]], dtype=torch.float64),
        **common,
    )
    assert inactive.strategy_returns[0].item() == pytest.approx(0.0)

    active = run_tw_cash_continuous(
        torch.full((1, 1), 0.5, dtype=torch.float64),
        torch.tensor([[bad_return]], dtype=torch.float64),
        **common,
    )
    assert active.settlement_default[0].item()
    assert active.strategy_returns[0].item() == pytest.approx(math.log(1.0e-6))
    assert not active.final_alive.item()
    torch.testing.assert_close(active.final_weights, torch.zeros_like(active.final_weights))


@pytest.mark.parametrize("bad_return", [float("nan"), float("inf"), float("-inf"), 1.0e4])
def test_tw_day_trade_cancels_entry_when_round_trip_valuation_is_nonfinite(
    bad_return: float,
) -> None:
    mask = torch.ones((1, 1), dtype=torch.bool)
    common = dict(
        intraday_log_returns=torch.tensor([[bad_return]], dtype=torch.float64),
        tradable_mask=mask,
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        day_trade_eligible_mask=mask,
        buy_fee_rates=torch.zeros(1, dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
    )
    inactive = run_tw_day_trade_continuous(
        torch.zeros((1, 1), dtype=torch.float64),
        **common,
    )
    assert inactive.strategy_returns[0].item() == pytest.approx(0.0)

    active = run_tw_day_trade_continuous(
        torch.ones((1, 1), dtype=torch.float64),
        **common,
    )
    assert active.strategy_returns[0].item() == pytest.approx(0.0)
    assert active.turnovers[0].item() == pytest.approx(0.0)
    assert not active.settlement_default[0].item()


def test_tw_day_trade_open_sizing_is_invariant_to_the_later_close() -> None:
    mask = torch.ones((1, 1), dtype=torch.bool)

    def execute(log_return: float):
        return run_tw_day_trade_continuous(
            torch.ones((1, 1), dtype=torch.float64),
            torch.tensor([[log_return]], dtype=torch.float64),
            mask,
            mask,
            mask,
            mask,
            mask,
            torch.zeros(1, dtype=torch.float64),
            torch.zeros(1, dtype=torch.float64),
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
            max_turnover_ratio=0.5,
        )

    unchanged = execute(0.0)
    doubled = execute(math.log(2.0))
    assert unchanged.weights_history[0, 0].item() == pytest.approx(0.25)
    torch.testing.assert_close(unchanged.weights_history, doubled.weights_history)
    assert unchanged.turnovers[0].item() == pytest.approx(0.5)
    assert doubled.turnovers[0].item() == pytest.approx(0.75)


@pytest.mark.parametrize(
    ("target", "intraday_return", "can_buy_close", "can_sell_close"),
    [
        # Buy at an inside-band open, then sell after the stock reaches its
        # upper limit.  A close-side buy block must not cancel the long trade.
        (1.0, math.log(1.10), False, True),
        # Sell first at an inside-band open, then buy back after the stock
        # reaches its lower limit.  A close-side sell block must not cancel the
        # short trade.
        (-1.0, math.log(0.90), True, False),
    ],
)
def test_tw_day_trade_executes_when_limit_is_reached_after_the_open(
    target: float,
    intraday_return: float,
    can_buy_close: bool,
    can_sell_close: bool,
) -> None:
    shape = (1, 1)
    open_mask = torch.ones(shape, dtype=torch.bool)
    result = run_tw_day_trade_continuous(
        torch.tensor([[target]], dtype=torch.float64),
        torch.tensor([[intraday_return]], dtype=torch.float64),
        torch.ones(shape, dtype=torch.bool),
        torch.full(shape, can_buy_close, dtype=torch.bool),
        torch.full(shape, can_sell_close, dtype=torch.bool),
        open_mask,
        open_mask,
        torch.zeros(1, dtype=torch.float64),
        torch.zeros(1, dtype=torch.float64),
        day_trade_can_buy_open_mask=open_mask,
        day_trade_can_sell_open_mask=open_mask,
    )

    assert result.strategy_returns[0].item() == pytest.approx(math.log(1.10))
    assert result.turnovers[0].item() > 0.0
    assert result.weights_history[0, 0].item() != pytest.approx(0.0)


def test_tw_continuous_rejects_non_scalar_initial_state_fields_cleanly() -> None:
    values = torch.zeros((1, 1), dtype=torch.float64)
    mask = torch.ones_like(values, dtype=torch.bool)
    with pytest.raises(ValueError, match="initial_cash must be a scalar"):
        run_tw_cash_continuous(
            values,
            values,
            mask,
            mask,
            mask,
            torch.zeros(1, dtype=torch.float64),
            torch.zeros(1, dtype=torch.float64),
            unresolved_corporate_action_mask=torch.zeros_like(mask),
            initial_cash=torch.ones(1, dtype=torch.float64),
        )


def test_tw_day_trade_requires_point_in_time_eligibility() -> None:
    values = torch.zeros((2, 1))
    mask = _mask_like(values)
    with pytest.raises(ValueError, match="point-in-time"):
        run_backtest_torch(
            values,
            values,
            mask,
            torch.zeros(2),
            0.0,
            0.0,
            execution_mode="tw_day_trade",
            buy_fee_rates=torch.zeros(1),
            sell_fee_rates=torch.zeros(1),
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
        )


def test_tw_cash_margin_short_requires_point_in_time_borrow_evidence() -> None:
    values = -torch.ones((2, 1))
    mask = _mask_like(values)
    common = dict(
        future_returns=torch.zeros_like(values),
        tradable_mask=mask,
        benchmark_returns=torch.zeros(2),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=0.5,
        execution_mode="tw_cash",
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        short_margin_rate=0.90,
        unresolved_corporate_action_mask=torch.zeros_like(values, dtype=torch.bool),
    )

    with pytest.raises(ValueError, match="missing can_short_open_mask"):
        run_backtest_torch(
            values,
            **common,
        )

    unavailable = run_backtest_torch(
        values,
        **common,
        can_short_open_mask=torch.zeros_like(mask),
        short_capacity_weights=torch.ones_like(values),
    )
    zero_capacity = run_backtest_torch(
        values,
        **common,
        can_short_open_mask=mask,
        short_capacity_weights=torch.zeros_like(values),
    )
    available = run_backtest_torch(
        values,
        **common,
        can_short_open_mask=mask,
        short_capacity_weights=torch.ones_like(values),
    )

    torch.testing.assert_close(
        unavailable.weights_history,
        torch.zeros_like(unavailable.weights_history),
    )
    torch.testing.assert_close(
        zero_capacity.weights_history,
        torch.zeros_like(zero_capacity.weights_history),
    )
    assert available.weights_history[0, 0].item() == pytest.approx(-0.5)
    _assert_tw_cash_margin_identity(available)


def test_tw_cash_short_open_requires_explicit_margin_and_capacity_contracts() -> None:
    target = torch.tensor([[-0.5]], dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    common = dict(
        target_weights=target,
        asset_log_returns=torch.zeros_like(target),
        tradable_mask=mask,
        can_buy_mask=mask,
        can_sell_mask=mask,
        buy_fee_rates=torch.zeros(1, dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        can_short_open_mask=mask,
        unresolved_corporate_action_mask=torch.zeros_like(mask),
    )

    with pytest.raises(ValueError, match="explicit point-in-time short_margin_rate"):
        run_tw_cash_continuous(
            **common,
            short_capacity_weights=torch.full_like(target, 0.5),
        )
    with pytest.raises(ValueError, match="point-in-time short_capacity_weights"):
        run_tw_cash_continuous(
            **common,
            short_margin_rate=0.90,
        )


@pytest.mark.parametrize(
    ("sale_collateral", "margin_collateral"),
    [
        (torch.tensor([0.5], dtype=torch.float64), None),
        (None, torch.tensor([0.45], dtype=torch.float64)),
        (
            torch.tensor([0.5], dtype=torch.float64),
            torch.tensor([0.0], dtype=torch.float64),
        ),
        (
            torch.tensor([0.0], dtype=torch.float64),
            torch.tensor([0.45], dtype=torch.float64),
        ),
    ],
)
def test_tw_cash_carried_short_requires_both_nonzero_collateral_pools(
    sale_collateral: torch.Tensor | None,
    margin_collateral: torch.Tensor | None,
) -> None:
    target = torch.zeros((1, 1), dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    with pytest.raises(ValueError, match="supplied together|nonzero per-symbol"):
        run_tw_cash_continuous(
            target,
            target,
            mask,
            mask,
            mask,
            torch.zeros(1, dtype=torch.float64),
            torch.zeros(1, dtype=torch.float64),
            unresolved_corporate_action_mask=torch.zeros_like(mask),
            initial_weights=torch.tensor([-0.5], dtype=torch.float64),
            initial_cash=torch.tensor(0.55, dtype=torch.float64),
            initial_short_sale_collateral=sale_collateral,
            initial_short_margin_collateral=margin_collateral,
        )


def test_tw_cash_float32_carried_short_roundoff_dust_is_not_a_naked_short() -> None:
    # The compiled FP32 recurrence can leave a sub-ULP signed residue after a
    # full cover.  It remains part of the normalized ledger, but is not an
    # economically open short requiring fabricated collateral.
    target = torch.zeros((1, 1), dtype=torch.float32)
    mask = torch.ones_like(target, dtype=torch.bool)
    result = run_tw_cash_continuous(
        target,
        target,
        mask,
        mask,
        mask,
        torch.zeros(1, dtype=torch.float32),
        torch.zeros(1, dtype=torch.float32),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        initial_weights=torch.tensor([-1.02039709e-12], dtype=torch.float32),
        initial_cash=torch.tensor(1.0, dtype=torch.float32),
        initial_short_sale_collateral=torch.zeros(1, dtype=torch.float32),
        initial_short_margin_collateral=torch.zeros(1, dtype=torch.float32),
    )

    assert result.final_alive is not None and bool(result.final_alive)
    assert result.final_weights[0].item() == pytest.approx(0.0, abs=2.0e-12)
    _assert_tw_cash_margin_identity(result)


def test_tw_cash_float32_distributed_small_shorts_use_account_materiality() -> None:
    # Each carried short is below the FP32 per-symbol threshold, while their
    # aggregate is material.  Complete collateral makes this a valid account,
    # not an orphaned-collateral state.
    initial_weights = torch.tensor([-1.0e-6, -1.0e-6, -1.0e-6], dtype=torch.float32)
    sale = torch.full((3,), 1.0e-6, dtype=torch.float32)
    margin = torch.full((3,), 0.9e-6, dtype=torch.float32)
    target = torch.zeros((1, 3), dtype=torch.float32)
    mask = torch.ones_like(target, dtype=torch.bool)
    initial_cash = 1.0 - initial_weights.sum() - sale.sum() - margin.sum()

    result = run_tw_cash_continuous(
        target,
        target,
        mask,
        mask,
        mask,
        torch.zeros(3, dtype=torch.float32),
        torch.zeros(3, dtype=torch.float32),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_short_sale_collateral=sale,
        initial_short_margin_collateral=margin,
    )

    assert result.final_alive is not None and bool(result.final_alive)
    _assert_tw_cash_margin_identity(result)


def test_tw_cash_float32_collateralized_short_does_not_magnify_naked_dust() -> None:
    initial_weights = torch.tensor([-0.25, -1.02039709e-12], dtype=torch.float32)
    sale = torch.tensor([0.25, 0.0], dtype=torch.float32)
    margin = torch.tensor([0.225, 0.0], dtype=torch.float32)
    target = torch.zeros((1, 2), dtype=torch.float32)
    mask = torch.ones_like(target, dtype=torch.bool)
    initial_cash = 1.0 - initial_weights.sum() - sale.sum() - margin.sum()

    result = run_tw_cash_continuous(
        target,
        target,
        mask,
        mask,
        mask,
        torch.zeros(2, dtype=torch.float32),
        torch.zeros(2, dtype=torch.float32),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_short_sale_collateral=sale,
        initial_short_margin_collateral=margin,
    )

    assert result.final_alive is not None and bool(result.final_alive)
    _assert_tw_cash_margin_identity(result)


def test_tw_cash_float32_distributed_material_short_still_requires_collateral() -> None:
    initial_weights = torch.tensor([-1.0e-6, -1.0e-6, -1.0e-6], dtype=torch.float32)
    target = torch.zeros((1, 3), dtype=torch.float32)
    mask = torch.ones_like(target, dtype=torch.bool)

    with pytest.raises(ValueError, match="nonzero per-symbol"):
        run_tw_cash_continuous(
            target,
            target,
            mask,
            mask,
            mask,
            torch.zeros(3, dtype=torch.float32),
            torch.zeros(3, dtype=torch.float32),
            unresolved_corporate_action_mask=torch.zeros_like(mask),
            initial_weights=initial_weights,
            initial_cash=1.0 - initial_weights.sum(),
            initial_short_sale_collateral=torch.zeros(3, dtype=torch.float32),
            initial_short_margin_collateral=torch.zeros(3, dtype=torch.float32),
        )


def test_tw_cash_carried_account_allows_and_reassigns_orphaned_short_collateral() -> None:
    target = torch.tensor([[0.0, -0.5]], dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    result = run_tw_cash_continuous(
        target,
        torch.zeros_like(target),
        mask,
        mask,
        mask,
        torch.zeros(2, dtype=torch.float64),
        torch.zeros(2, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        initial_weights=torch.tensor([0.0, -0.5], dtype=torch.float64),
        initial_cash=torch.tensor(0.55, dtype=torch.float64),
        initial_short_sale_collateral=torch.tensor(
            [0.1, 0.4], dtype=torch.float64
        ),
        initial_short_margin_collateral=torch.tensor(
            [0.1, 0.35], dtype=torch.float64
        ),
    )

    torch.testing.assert_close(
        result.short_sale_collateral_history[0],
        torch.tensor([0.0, 0.5], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.short_margin_collateral_history[0],
        torch.tensor([0.0, 0.45], dtype=torch.float64),
    )
    _assert_tw_cash_margin_identity(result)


@pytest.mark.parametrize("ratio", [0.0, 0.99, float("nan"), float("inf")])
def test_tw_cash_rejects_invalid_short_maintenance_ratio(ratio: float) -> None:
    target = torch.zeros((1, 1), dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    with pytest.raises(ValueError, match=r"finite scalar >= 1\.0"):
        run_tw_cash_continuous(
            target,
            target,
            mask,
            mask,
            mask,
            torch.zeros(1, dtype=torch.float64),
            torch.zeros(1, dtype=torch.float64),
            short_maintenance_ratio=ratio,
            unresolved_corporate_action_mask=torch.zeros_like(mask),
        )


def test_tw_cash_official_action_transition_liquidates_then_allows_reentry() -> None:
    weights = torch.tensor([[0.5], [0.5], [0.5]], dtype=torch.float64)
    returns = torch.tensor([[0.0], [math.log(3.0)], [0.0]], dtype=torch.float64)
    mask = _mask_like(weights)
    actions = torch.tensor([[False], [True], [False]])

    result = run_tw_cash_continuous(
        weights,
        returns,
        mask,
        mask,
        mask,
        torch.zeros(1, dtype=torch.float64),
        torch.tensor([0.10], dtype=torch.float64),
        unresolved_corporate_action_mask=actions,
    )

    # The official ex-date is the session after row 1.  The execution-only
    # transition therefore liquidates at row 1's close, charges the sell fee,
    # and carries no position through the deliberately huge raw-price jump.
    assert result.weights_history[0, 0].item() == pytest.approx(0.5)
    assert result.weights_history[1, 0].item() == pytest.approx(0.0)
    assert result.turnovers[1].item() == pytest.approx(0.5)
    assert result.strategy_returns[1].item() == pytest.approx(math.log(0.95))
    # The event is a one-transition avoidance rule, not a permanent blacklist.
    assert result.weights_history[2, 0].item() > 0.0


def test_tw_cash_blocked_official_action_exit_becomes_absorbing_default() -> None:
    weights = torch.tensor([[0.5], [0.5]], dtype=torch.float64)
    mask = _mask_like(weights)
    can_sell = torch.tensor([[True], [False]])
    actions = torch.tensor([[False], [True]])

    result = run_tw_cash_continuous(
        weights,
        torch.zeros_like(weights),
        mask,
        mask,
        can_sell,
        torch.zeros(1, dtype=torch.float64),
        torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=actions,
    )

    assert result.settlement_default.tolist() == [False, True]
    assert result.strategy_returns[1].item() == pytest.approx(math.log(1.0e-6))
    assert not result.final_alive.item()


def test_tw_cash_prior_alive_selection_mask_does_not_block_current_exit() -> None:
    """Selection causality must not erase an independently executable close."""

    weights = torch.tensor([[0.5], [0.5]], dtype=torch.float32)
    selection_mask = torch.tensor([[True], [False]])
    side_mask = torch.ones_like(selection_mask)
    actions = torch.tensor([[False], [True]])

    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        selection_mask,
        torch.zeros(2),
        0.0,
        0.0,
        execution_mode="tw_cash",
        long_only=True,
        can_buy_mask=side_mask,
        can_sell_mask=side_mask,
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        unresolved_corporate_action_mask=actions,
    )

    assert result.weights_history[0, 0] > 0.0
    torch.testing.assert_close(result.weights_history[1], torch.zeros(1))
    assert result.turnovers[1] > 0.0


def test_tw_cash_official_action_transition_blocks_new_entry_without_holding() -> None:
    weights = torch.tensor([[0.5]], dtype=torch.float64)
    mask = _mask_like(weights)
    result = run_tw_cash_continuous(
        weights,
        torch.zeros_like(weights),
        mask,
        mask,
        mask,
        torch.zeros(1, dtype=torch.float64),
        torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.ones_like(weights, dtype=torch.bool),
    )

    torch.testing.assert_close(result.weights_history, torch.zeros_like(weights))
    torch.testing.assert_close(result.turnovers, torch.zeros(1, dtype=torch.float64))


def test_tw_cash_exact_cash_dividend_survives_sale_until_announced_payment() -> None:
    # Row 0 closes immediately before the ex-date.  The raw price loses 10%,
    # while the holder earns a 10%-of-prior-close cash claim.  Selling the base
    # shares on row 1 must not delete that already-earned receivable, and it
    # must become settled cash only after the announced three-session delay.
    target = torch.tensor([[1.0], [0.0], [0.0], [0.0]], dtype=torch.float64)
    raw_returns = torch.tensor(
        [[math.log(0.9)], [0.0], [0.0], [0.0]], dtype=torch.float64
    )
    mask = torch.ones_like(target, dtype=torch.bool)
    dividend_yield = torch.tensor([[0.1], [0.0], [0.0], [0.0]], dtype=torch.float64)
    payment_delay = torch.tensor([[3], [0], [0], [0]], dtype=torch.int64)

    result = run_tw_cash_continuous(
        target,
        raw_returns,
        mask,
        mask,
        mask,
        torch.zeros(1, dtype=torch.float64),
        torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        cash_dividend_yield=dividend_yield,
        cash_dividend_payment_delay_sessions=payment_delay,
        claim_queue_sessions=4,
    )

    assert result.strategy_returns[0].item() == pytest.approx(0.0, abs=1.0e-12)
    assert result.weights_history[1, 0].item() == pytest.approx(0.0)
    # The claim moves through the queue independently of the sold security.
    assert result.receivables_history[0].sum().item() == pytest.approx(0.1)
    # Row 1 also schedules the 0.9 sale proceeds; both claims happen to mature
    # together, so the queue carries 1.0 and the final cash proves the 0.1
    # dividend was not erased by selling the base shares.
    assert result.receivables_history[1].sum().item() == pytest.approx(1.0)
    assert result.receivables_history[2].sum().item() == pytest.approx(1.0)
    assert result.cash_history[2].item() == pytest.approx(0.0)
    assert result.cash_history[3].item() == pytest.approx(1.0)


def test_tw_cash_exact_cash_dividend_rejects_unrepresentable_payment_delay() -> None:
    target = torch.zeros((1, 1), dtype=torch.float64)
    mask = torch.ones_like(target, dtype=torch.bool)
    with pytest.raises(ValueError, match="payment delay exceeds"):
        run_tw_cash_continuous(
            target,
            target,
            mask,
            mask,
            mask,
            torch.zeros(1, dtype=torch.float64),
            torch.zeros(1, dtype=torch.float64),
            unresolved_corporate_action_mask=torch.zeros_like(mask),
            cash_dividend_yield=torch.ones_like(target),
            cash_dividend_payment_delay_sessions=torch.full_like(
                target, 5, dtype=torch.int64
            ),
            claim_queue_sessions=4,
        )


def test_tw_day_trade_blocked_close_prevents_round_trip_entry() -> None:
    weights = torch.tensor([[1.0]], dtype=torch.float64)
    open_mask = _mask_like(weights)
    close_sell_mask = torch.zeros_like(open_mask)

    result = run_tw_day_trade_continuous(
        weights,
        torch.zeros_like(weights),
        open_mask,
        open_mask,
        close_sell_mask,
        open_mask,
        open_mask,
        torch.zeros(1, dtype=torch.float64),
        torch.zeros(1, dtype=torch.float64),
        day_trade_can_buy_open_mask=open_mask,
        day_trade_can_sell_open_mask=open_mask,
    )

    assert result.strategy_returns[0].item() == pytest.approx(0.0)
    assert result.turnovers[0].item() == pytest.approx(0.0)
    assert not result.settlement_default[0].item()


@pytest.mark.parametrize("mode", ["tw_cash", "tw_day_trade"])
def test_tw_state_is_exact_across_chunks(mode: str) -> None:
    weights = torch.tensor(
        [[0.4, 0.2], [0.1, 0.6], [0.5, 0.0], [0.2, 0.4], [0.0, 0.7]],
        dtype=torch.float64,
    )
    returns = torch.tensor(
        [[0.02, -0.01], [0.01, 0.03], [-0.02, 0.01], [0.04, -0.01], [0.0, 0.02]],
        dtype=torch.float64,
    )
    mask = _mask_like(weights)
    fees_buy = torch.tensor([0.001, 0.002], dtype=torch.float64)
    fees_sell = torch.tensor([0.004, 0.003], dtype=torch.float64)
    common = dict(
        buy_fee_rates=fees_buy,
        sell_fee_rates=fees_sell,
        execution_mode=mode,
        day_trade_eligible_mask=mask if mode == "tw_day_trade" else None,
    )
    full = run_backtest_torch(
        weights,
        returns,
        mask,
        torch.zeros(5, dtype=torch.float64),
        0.0,
        0.0,
        **common,
        **_real_execution_masks(weights, mode),
    )
    first = run_backtest_torch(
        weights[:2],
        returns[:2],
        mask[:2],
        torch.zeros(2, dtype=torch.float64),
        0.0,
        0.0,
        **{**common, "day_trade_eligible_mask": mask[:2] if mode == "tw_day_trade" else None},
        **_real_execution_masks(weights[:2], mode),
    )
    second = run_backtest_torch(
        weights[2:],
        returns[2:],
        mask[2:],
        torch.zeros(3, dtype=torch.float64),
        0.0,
        0.0,
        initial_weights=first.final_weights,
        initial_cash=first.final_cash,
        initial_payables=first.final_payables,
        initial_receivables=first.final_receivables,
        initial_alive=first.final_alive,
        **{**common, "day_trade_eligible_mask": mask[2:] if mode == "tw_day_trade" else None},
        **_real_execution_masks(weights[2:], mode),
    )

    torch.testing.assert_close(
        torch.cat((first.strategy_returns, second.strategy_returns)),
        full.strategy_returns,
        rtol=1e-12,
        atol=1e-12,
    )
    torch.testing.assert_close(second.final_weights, full.final_weights, rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(second.final_cash, full.final_cash, rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(second.final_payables, full.final_payables, rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(second.final_receivables, full.final_receivables, rtol=1e-12, atol=1e-12)
    assert bool(second.final_alive) == bool(full.final_alive)


def test_padding_does_not_advance_settlement_or_apply_return() -> None:
    weights = torch.tensor([[0.5], [0.9], [0.5]], dtype=torch.float64)
    returns = torch.tensor([[0.0], [math.log(2.0)], [0.0]], dtype=torch.float64)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        returns,
        mask,
        torch.zeros(3, dtype=torch.float64),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.tensor([0.10], dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        state_advance_mask=torch.tensor([True, False, True]),
    )
    assert result.strategy_returns[1].item() == pytest.approx(0.0)
    assert result.payables_history is not None
    torch.testing.assert_close(result.payables_history[1], result.payables_history[0])
    # Only one real session elapsed after the trade, so the claim is due next,
    # not already paid on the padded row.
    assert result.payables_history[2, 0].item() > 0.0


def test_settlement_default_records_ruin_exactly_once() -> None:
    weights = torch.zeros((3, 1), dtype=torch.float64)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        weights,
        mask,
        torch.zeros(3, dtype=torch.float64),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.zeros(1, dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        initial_cash=torch.tensor(0.0, dtype=torch.float64),
        initial_payables=torch.tensor([1.0, 0.0], dtype=torch.float64),
        # The account is solvent on a NAV basis but cannot use a same-session
        # receipt to satisfy the opening payment deadline.
        initial_receivables=torch.tensor([2.0, 0.0], dtype=torch.float64),
    )

    assert result.strategy_returns[0].item() < -10.0
    torch.testing.assert_close(
        result.strategy_returns[1:], torch.zeros_like(result.strategy_returns[1:])
    )
    assert result.settlement_default is not None
    assert result.settlement_default.tolist() == [True, False, False]
    assert result.final_alive is not None and not bool(result.final_alive)


def test_cash_purchase_can_use_receipts_arriving_before_its_due_session() -> None:
    # Sell on t0 and repurchase on t1.  The t0 receipt arrives on t2; the t1
    # payment is not due until t3, so the earlier receivable is valid funding.
    weights = torch.tensor([[0.0], [1.0]], dtype=torch.float64)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(2, dtype=torch.float64),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.zeros(1, dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        initial_weights=torch.ones(1, dtype=torch.float64),
        initial_cash=torch.tensor(0.0, dtype=torch.float64),
    )

    assert result.weights_history[1, 0].item() == pytest.approx(1.0)
    assert result.payables_history is not None
    assert result.receivables_history is not None
    assert result.receivables_history[1, 0].item() == pytest.approx(1.0)
    assert result.payables_history[1, 1].item() == pytest.approx(1.0)


def test_cash_purchase_cannot_skip_an_unfunded_intermediate_payable() -> None:
    weights = torch.ones((1, 1), dtype=torch.float64)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(1, dtype=torch.float64),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.zeros(1, dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        settlement_lag_sessions=3,
        initial_cash=torch.tensor(0.0, dtype=torch.float64),
        initial_payables=torch.tensor([0.0, 1.0, 0.0], dtype=torch.float64),
        initial_receivables=torch.tensor([0.0, 0.0, 2.0], dtype=torch.float64),
    )

    # After today's queue shift, the 1.0 payable is due before the 2.0 receipt.
    # No later-dated purchase may be admitted on the strength of that receipt.
    assert result.weights_history[0, 0].item() == pytest.approx(0.0)


def test_partial_cash_state_infers_residual_cash_and_rejects_invalid_cash() -> None:
    weights = torch.ones((1, 1), dtype=torch.float32)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(1),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        initial_weights=torch.ones(1),
    )
    assert result.final_cash is not None
    assert result.final_cash.item() == pytest.approx(0.0)
    assert result.final_weights is not None
    assert result.final_weights[0].item() == pytest.approx(1.0)

    with pytest.raises(ValueError, match="initial_cash"):
        run_backtest_torch(
            weights,
            torch.zeros_like(weights),
            mask,
            torch.zeros(1),
            0.0,
            0.0,
            execution_mode="tw_cash",
            buy_fee_rates=torch.zeros(1),
            sell_fee_rates=torch.zeros(1),
            unresolved_corporate_action_mask=torch.zeros_like(
                weights, dtype=torch.bool
            ),
            initial_cash=torch.tensor(float("nan")),
        )


@pytest.mark.parametrize("mode", ["tw_cash", "tw_day_trade"])
def test_explicit_recurrent_state_must_conserve_normalized_nav(mode: str) -> None:
    weights = torch.ones((1, 1), dtype=torch.float32)
    mask = _mask_like(weights)
    kwargs: dict[str, object] = {
        "execution_mode": mode,
        "buy_fee_rates": torch.zeros(1),
        "sell_fee_rates": torch.zeros(1),
        "initial_cash": torch.tensor(0.8 if mode == "tw_cash" else 2.0),
    }
    if mode == "tw_cash":
        kwargs["initial_weights"] = torch.tensor([0.8])
    else:
        kwargs["day_trade_eligible_mask"] = mask
    kwargs.update(_real_execution_masks(weights, mode))

    with pytest.raises(ValueError, match="normalized accounting identity"):
        run_backtest_torch(
            weights,
            torch.zeros_like(weights),
            mask,
            torch.zeros(1),
            0.0,
            0.0,
            **kwargs,
        )


def test_tw_cash_fullgraph_capture_supports_official_action_avoidance() -> None:
    def execute(
        weights: torch.Tensor,
        returns: torch.Tensor,
        mask: torch.Tensor,
        fees: torch.Tensor,
        actions: torch.Tensor,
    ) -> torch.Tensor:
        return run_tw_cash_continuous(
            weights,
            returns,
            mask,
            mask,
            mask,
            fees,
            fees,
            unresolved_corporate_action_mask=actions,
            return_weights_history=False,
        ).strategy_returns

    compiled = torch.compile(execute, backend="eager", fullgraph=True)
    values = torch.tensor([[0.2], [0.3]], dtype=torch.float32)
    mask = torch.ones_like(values, dtype=torch.bool)
    actions = torch.tensor([[False], [True]])
    output = compiled(
        values,
        torch.zeros_like(values),
        mask,
        torch.zeros(1),
        actions,
    )
    assert torch.isfinite(output).all()


def test_tw_finance_state_stays_fp32_for_bf16_model_outputs() -> None:
    weights = torch.tensor([[0.4], [0.6], [0.2]], dtype=torch.bfloat16)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(3, dtype=torch.bfloat16),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.tensor([0.001]),
        sell_fee_rates=torch.tensor([0.004]),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
    )

    assert result.strategy_returns.dtype == torch.float32
    assert result.turnovers.dtype == torch.float32
    assert result.weights_history.dtype == torch.float32
    assert result.final_weights is not None and result.final_weights.dtype == torch.float32
    assert result.final_cash is not None and result.final_cash.dtype == torch.float32


def test_cash_blocked_sale_cannot_fund_gross_budget_break() -> None:
    weights = torch.tensor([[0.0, 0.5]], dtype=torch.float32)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(1),
        0.0,
        0.0,
        gross_leverage=0.5,
        execution_mode="tw_cash",
        buy_fee_rates=torch.zeros(2),
        sell_fee_rates=torch.zeros(2),
        initial_weights=torch.tensor([0.5, 0.0]),
        initial_cash=torch.tensor(0.5),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        volume_limit_weights=torch.tensor([[0.0, float("inf")]]),
    )

    torch.testing.assert_close(
        result.weights_history[0], torch.tensor([0.5, 0.0])
    )
    assert result.weights_history[0].sum().item() <= 0.5


def test_torch_symbol_indices_reorder_equal_length_fee_vectors() -> None:
    weights = torch.tensor([[1.0, 0.0]])
    mask = _mask_like(weights)
    common = dict(
        future_returns=torch.zeros_like(weights),
        tradable_mask=mask,
        benchmark_returns=torch.zeros(1),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        execution_mode="tw_cash",
        sell_fee_rates=torch.zeros(2),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
    )
    mapped = run_backtest_torch(
        weights,
        **common,
        buy_fee_rates=torch.tensor([0.0, 0.1]),
        symbol_indices=torch.tensor([1, 0]),
    )
    explicit = run_backtest_torch(
        weights,
        **common,
        buy_fee_rates=torch.tensor([0.1, 0.0]),
    )

    torch.testing.assert_close(mapped.strategy_returns, explicit.strategy_returns)
    torch.testing.assert_close(mapped.weights_history, explicit.weights_history)


def test_day_trade_volume_cap_counts_both_round_trip_legs() -> None:
    weights = torch.ones((1, 1), dtype=torch.float32)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(1),
        0.0,
        0.0,
        execution_mode="tw_day_trade",
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        volume_limit_weights=torch.tensor([[0.5]]),
    )

    assert result.weights_history[0, 0].item() == pytest.approx(0.25)
    assert result.turnovers[0].item() == pytest.approx(0.5)


def test_cash_volume_cap_tracks_compounded_nav_across_chunks_and_exact_oracle() -> None:
    """A fixed share cap must not grow merely because the account became richer."""

    targets = torch.tensor([[1.0, 0.0], [0.0, 1.0]], dtype=torch.float64)
    returns = torch.tensor(
        [[math.log(2.0), 0.0], [0.0, 0.0]], dtype=torch.float64
    )
    mask = torch.ones_like(targets, dtype=torch.bool)
    actions = torch.zeros_like(targets, dtype=torch.bool)
    fees = torch.zeros(2, dtype=torch.float64)
    # Causal capped notional / reference equity.  On row 1 the second symbol's
    # cap is 200 / 1000 = .2, but current NAV is 2000, so its current-NAV weight
    # cap is .2 / 2 = .1 (20 shares at TWD 10), not .2.
    reference_caps = torch.tensor(
        [[1.0, 0.0], [2.0, 0.2]], dtype=torch.float64
    )

    def execute(
        target: torch.Tensor,
        asset_returns: torch.Tensor,
        caps: torch.Tensor,
        **state: torch.Tensor,
    ):
        return run_tw_cash_continuous(
            target,
            asset_returns,
            mask[: target.size(0)],
            mask[: target.size(0)],
            mask[: target.size(0)],
            fees,
            fees,
            unresolved_corporate_action_mask=actions[: target.size(0)],
            volume_limit_weights=caps,
            **state,
        )

    full = execute(targets, returns, reference_caps)
    torch.testing.assert_close(
        full.equity_scale_history, torch.tensor([2.0, 2.0], dtype=torch.float64)
    )
    torch.testing.assert_close(
        full.weights_history[1], torch.tensor([0.0, 0.1], dtype=torch.float64)
    )

    first = execute(targets[:1], returns[:1], reference_caps[:1])
    second = execute(
        targets[1:],
        returns[1:],
        reference_caps[1:],
        initial_weights=first.final_weights,
        initial_cash=first.final_cash,
        initial_payables=first.final_payables,
        initial_receivables=first.final_receivables,
        initial_alive=first.final_alive,
        initial_equity_scale=first.final_equity_scale,
    )
    torch.testing.assert_close(second.weights_history, full.weights_history[1:])
    torch.testing.assert_close(second.final_weights, full.final_weights)
    torch.testing.assert_close(second.final_cash, full.final_cash)
    torch.testing.assert_close(second.final_payables, full.final_payables)
    torch.testing.assert_close(second.final_receivables, full.final_receivables)
    torch.testing.assert_close(second.final_equity_scale, full.final_equity_scale)

    exact = run_tw_cash_integer(
        targets.numpy(),
        np.array([[10.0, 10.0], [20.0, 10.0]]),
        future_log_returns=returns.numpy(),
        unresolved_corporate_action_mask=actions.numpy(),
        buy_fee_rates=np.zeros(2),
        sell_fee_rates=np.zeros(2),
        lot_sizes=np.ones(2, dtype=np.int64),
        tradable_mask=mask.numpy(),
        can_buy_mask=mask.numpy(),
        can_sell_mask=mask.numpy(),
        daily_volumes=np.array([[100.0, 0.0], [100.0, 20.0]]),
        max_volume_participation=1.0,
        initial_cash=1_000.0,
    )
    np.testing.assert_allclose(
        full.strategy_returns.numpy(), exact.strategy_returns, rtol=1e-12, atol=1e-12
    )
    np.testing.assert_allclose(
        full.weights_history.numpy(), exact.weights, rtol=1e-12, atol=1e-12
    )
    np.testing.assert_allclose(
        full.final_weights.numpy(), exact.final_weights, rtol=1e-12, atol=1e-12
    )
    assert full.final_equity_scale.item() == pytest.approx(
        exact.final_state.last_nav / 1_000.0
    )
    np.testing.assert_allclose(
        full.final_payables.numpy(),
        exact.final_state.payable_queue / exact.final_state.last_nav,
        rtol=1e-12,
        atol=1e-12,
    )
    np.testing.assert_allclose(
        full.final_receivables.numpy(),
        exact.final_state.receivable_queue / exact.final_state.last_nav,
        rtol=1e-12,
        atol=1e-12,
    )


def test_cash_weights_history_uses_exact_post_fee_execution_nav() -> None:
    targets = torch.tensor([[0.5]], dtype=torch.float64)
    returns = torch.zeros_like(targets)
    mask = torch.ones_like(targets, dtype=torch.bool)
    continuous = run_tw_cash_continuous(
        targets,
        returns,
        mask,
        mask,
        mask,
        torch.tensor([0.01], dtype=torch.float64),
        torch.tensor([0.01], dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
    )
    exact = run_tw_cash_integer(
        targets.numpy(),
        np.array([[2.0]]),
        future_log_returns=returns.numpy(),
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        buy_fee_rates=np.array([0.01]),
        sell_fee_rates=np.array([0.01]),
        lot_sizes=np.ones(1, dtype=np.int64),
        tradable_mask=mask.numpy(),
        can_buy_mask=mask.numpy(),
        can_sell_mask=mask.numpy(),
        initial_cash=1_000.0,
    )

    # TWD500 shares and TWD5 commission leave execution NAV TWD995.
    assert continuous.weights_history[0, 0].item() == pytest.approx(500.0 / 995.0)
    np.testing.assert_allclose(
        continuous.weights_history.numpy(), exact.weights, rtol=1e-12, atol=1e-12
    )
    np.testing.assert_allclose(
        continuous.strategy_returns.numpy(),
        exact.strategy_returns,
        rtol=1e-12,
        atol=1e-12,
    )


def test_day_trade_volume_cap_tracks_compounded_nav_across_chunks_and_exact_oracle() -> None:
    targets = torch.ones((2, 1), dtype=torch.float64)
    returns = torch.tensor([[math.log(2.0)], [0.0]], dtype=torch.float64)
    mask = torch.ones_like(targets, dtype=torch.bool)
    fees = torch.zeros(1, dtype=torch.float64)
    # Row 1: 40 causal shares / 1000 reference equity * TWD10 = .4.
    # A round trip consumes two legs and NAV has doubled, hence .4 / 2 / 2=.1.
    reference_caps = torch.tensor([[2.0], [0.4]], dtype=torch.float64)

    def execute(
        target: torch.Tensor,
        intraday_returns: torch.Tensor,
        caps: torch.Tensor,
        **state: torch.Tensor,
    ):
        local_mask = mask[: target.size(0)]
        return run_tw_day_trade_continuous(
            target,
            intraday_returns,
            local_mask,
            local_mask,
            local_mask,
            local_mask,
            local_mask,
            fees,
            fees,
            day_trade_can_buy_open_mask=local_mask,
            day_trade_can_sell_open_mask=local_mask,
            volume_limit_weights=caps,
            **state,
        )

    full = execute(targets, returns, reference_caps)
    torch.testing.assert_close(
        full.equity_scale_history, torch.tensor([2.0, 2.0], dtype=torch.float64)
    )
    assert full.weights_history[1, 0].item() == pytest.approx(0.1)

    first = execute(targets[:1], returns[:1], reference_caps[:1])
    second = execute(
        targets[1:],
        returns[1:],
        reference_caps[1:],
        initial_cash=first.final_cash,
        initial_payables=first.final_payables,
        initial_receivables=first.final_receivables,
        initial_alive=first.final_alive,
        initial_equity_scale=first.final_equity_scale,
    )
    torch.testing.assert_close(second.weights_history, full.weights_history[1:])
    torch.testing.assert_close(second.final_cash, full.final_cash)
    torch.testing.assert_close(second.final_payables, full.final_payables)
    torch.testing.assert_close(second.final_receivables, full.final_receivables)
    torch.testing.assert_close(second.final_equity_scale, full.final_equity_scale)

    exact = run_tw_day_trade_integer(
        targets.numpy(),
        np.array([[10.0], [10.0]]),
        np.array([[20.0], [10.0]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        eligibility_mask=mask.numpy(),
        lot_sizes=np.ones(1, dtype=np.int64),
        tradable_mask=mask.numpy(),
        can_buy_mask=mask.numpy(),
        can_sell_mask=mask.numpy(),
        day_trade_can_buy_open_mask=mask.numpy(),
        day_trade_can_sell_open_mask=mask.numpy(),
        daily_volumes=np.array([[200.0], [40.0]]),
        max_volume_participation=1.0,
        initial_cash=1_000.0,
    )
    np.testing.assert_allclose(
        full.strategy_returns.numpy(), exact.strategy_returns, rtol=1e-12, atol=1e-12
    )
    np.testing.assert_allclose(
        full.weights_history.numpy(), exact.weights, rtol=1e-12, atol=1e-12
    )
    assert full.final_equity_scale.item() == pytest.approx(
        exact.final_state.last_nav / 1_000.0
    )
    np.testing.assert_allclose(
        full.final_receivables.numpy(),
        exact.final_state.receivable_queue / exact.final_state.last_nav,
        rtol=1e-12,
        atol=1e-12,
    )


def test_day_trade_lifecycle_masks_match_integer_entry_contract() -> None:
    mask = torch.ones((1, 1), dtype=torch.bool)
    common = dict(
        future_returns=torch.zeros((1, 1)),
        tradable_mask=mask,
        benchmark_returns=torch.zeros(1),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        execution_mode="tw_day_trade",
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        can_short_open_mask=mask,
    )
    terminal = run_backtest_torch(
        torch.ones((1, 1)),
        **common,
        force_exit_mask=mask,
    )
    covered_short = run_backtest_torch(
        -torch.ones((1, 1)),
        **common,
        long_only=False,
        force_short_cover_mask=mask,
    )
    permitted_long = run_backtest_torch(
        torch.ones((1, 1)),
        **common,
        force_short_cover_mask=mask,
    )

    assert terminal.weights_history[0, 0].item() == pytest.approx(0.0)
    assert covered_short.weights_history[0, 0].item() == pytest.approx(0.0)
    assert permitted_long.weights_history[0, 0].item() == pytest.approx(1.0)


def test_many_padding_rows_are_bitwise_state_identity() -> None:
    generator = torch.Generator().manual_seed(19)
    rows, symbols = 63, 7
    weights = torch.rand((rows, symbols), generator=generator)
    mask = torch.ones_like(weights, dtype=torch.bool)
    initial_weights = torch.rand((symbols,), generator=generator)
    initial_weights = initial_weights / initial_weights.sum() * 0.4
    initial_payables = torch.tensor([0.01357924, 0.02468013])
    initial_receivables = torch.tensor([0.03753186, 0.04864297])
    initial_cash = (
        1.0
        - initial_weights.sum()
        - initial_receivables.sum()
        + initial_payables.sum()
    )
    result = run_backtest_torch(
        weights,
        torch.randn((rows, symbols), generator=generator) * 0.02,
        mask,
        torch.zeros(rows),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.zeros(symbols),
        sell_fee_rates=torch.zeros(symbols),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
        state_advance_mask=torch.zeros(rows, dtype=torch.bool),
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
    )

    assert torch.equal(result.final_weights, initial_weights)
    assert torch.equal(result.final_cash, initial_cash)
    assert torch.equal(result.final_payables, initial_payables)
    assert torch.equal(result.final_receivables, initial_receivables)
    assert torch.equal(result.strategy_returns, torch.zeros(rows))


def test_admitted_zero_return_cash_trades_do_not_false_default() -> None:
    for seed in range(20):
        generator = torch.Generator().manual_seed(seed)
        weights = torch.rand((24, 13), generator=generator)
        mask = torch.ones_like(weights, dtype=torch.bool)
        result = run_backtest_torch(
            weights,
            torch.zeros_like(weights),
            mask,
            torch.zeros(24),
            0.0,
            0.0,
            execution_mode="tw_cash",
            buy_fee_rates=torch.full((13,), 0.000855),
            sell_fee_rates=torch.full((13,), 0.003855),
            unresolved_corporate_action_mask=torch.zeros_like(
                weights, dtype=torch.bool
            ),
        )
        assert result.settlement_default is not None
        assert not bool(result.settlement_default.any()), f"false default at seed={seed}"


def test_tw_continuous_path_is_differentiable() -> None:
    logits = torch.tensor([[0.2, -0.1], [0.1, 0.3], [-0.2, 0.4]], requires_grad=True)
    returns = torch.tensor([[0.01, -0.02], [0.02, 0.01], [-0.01, 0.03]])
    mask = _mask_like(logits)
    result = run_backtest_torch(
        logits,
        returns,
        mask,
        torch.zeros(3),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.tensor([0.001, 0.001]),
        sell_fee_rates=torch.tensor([0.004, 0.002]),
        unresolved_corporate_action_mask=torch.zeros_like(logits, dtype=torch.bool),
    )
    loss = -result.strategy_returns.sum()
    loss.backward()
    assert logits.grad is not None
    assert torch.isfinite(logits.grad).all()
    assert logits.grad.abs().sum().item() > 0.0


def test_tw_cash_compiled_tuple_carries_margin_state_and_separates_cache_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the compiled recurrent ABI on CPU without claiming CPU speed."""

    monkeypatch.setattr(torch, "compile", lambda function, **_kwargs: function)
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)

    target = torch.tensor([[-0.4, 0.2], [0.0, 0.3]], dtype=torch.float64)
    returns = torch.zeros_like(target)
    mask = torch.ones_like(target, dtype=torch.bool)
    no_force = torch.zeros_like(mask)
    rates = torch.tensor([[0.90, 0.90], [0.95, 0.95]], dtype=torch.float64)
    capacities = torch.full_like(target, 0.5)
    handling = torch.full_like(target, 0.001)
    fees = torch.zeros(2, dtype=torch.float64)
    zero_rows = torch.zeros_like(target)
    zero_state = torch.zeros(2, dtype=torch.float64)
    initial_cash = torch.ones((), dtype=torch.float64)
    zero_scalar = torch.zeros((), dtype=torch.float64)
    zero_month_id = torch.zeros((), dtype=torch.int64)
    initial_alive = torch.tensor(True)
    initial_scale = torch.ones((), dtype=torch.float64)

    short_key, short_runner = tw_continuous_module._tw_cash_compiled_chunk_runner(
        target_weights=target,
        chunk_rows=2,
        settlement_lag_sessions=2,
        gross_budget=1.0,
        max_turnover_ratio=0.0,
        short_maintenance_ratio=1.30,
        has_volume_limit=False,
        has_short_contract=True,
        has_short_open_mask=True,
        has_force_short_cover_mask=False,
        has_short_capacity=True,
        return_weights_history=True,
        requires_state_grad=False,
        min_symbols=2,
        max_symbols=2,
    )
    values = short_runner(
        target,
        returns,
        mask,
        mask,
        mask,
        mask,
        no_force,
        rates,
        capacities,
        handling,
        zero_rows,
        zero_rows,
        fees,
        fees,
        fees,
        zero_rows,
        no_force,
        no_force,
        torch.ones(2, dtype=torch.bool),
        torch.zeros(2, dtype=torch.int64),
        torch.zeros(2, dtype=torch.bool),
        zero_state,
        initial_cash,
        zero_state,
        zero_state,
        zero_state,
        zero_state,
        zero_scalar,
        zero_scalar,
        zero_month_id,
        initial_alive,
        initial_scale,
    )
    result = tw_continuous_module._tw_cash_result_from_tuple(values)
    reference = run_tw_cash_continuous(
        target,
        returns,
        mask,
        mask,
        mask,
        fees,
        fees,
        can_short_open_mask=mask,
        force_short_cover_mask=no_force,
        short_margin_rate=rates,
        short_capacity_weights=capacities,
        short_handling_fee_rate=handling,
        unresolved_corporate_action_mask=no_force,
    )
    for actual, expected in zip(
        tw_continuous_module._tw_cash_result_tuple(result),
        tw_continuous_module._tw_cash_result_tuple(reference),
        strict=True,
    ):
        torch.testing.assert_close(actual, expected, rtol=0.0, atol=0.0)

    long_key, _ = tw_continuous_module._tw_cash_compiled_chunk_runner(
        target_weights=target,
        chunk_rows=2,
        settlement_lag_sessions=2,
        gross_budget=1.0,
        max_turnover_ratio=0.0,
        short_maintenance_ratio=1.30,
        has_volume_limit=False,
        has_short_contract=False,
        has_short_open_mask=False,
        has_force_short_cover_mask=False,
        has_short_capacity=False,
        return_weights_history=True,
        requires_state_grad=False,
        min_symbols=2,
        max_symbols=2,
    )
    ratio_key, _ = tw_continuous_module._tw_cash_compiled_chunk_runner(
        target_weights=target,
        chunk_rows=2,
        settlement_lag_sessions=2,
        gross_budget=1.0,
        max_turnover_ratio=0.0,
        short_maintenance_ratio=1.40,
        has_volume_limit=False,
        has_short_contract=True,
        has_short_open_mask=True,
        has_force_short_cover_mask=False,
        has_short_capacity=True,
        return_weights_history=True,
        requires_state_grad=False,
        min_symbols=2,
        max_symbols=2,
    )
    assert short_key != long_key
    assert short_key != ratio_key
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compile_constructors"] == 3
    assert result.final_short_sale_collateral is not None
    assert result.final_short_margin_collateral is not None

    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_tw_cash_compiled_chunks_preserve_cross_chunk_gradient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_compile = torch.compile
    graph_count = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal graph_count
        graph_count += 1
        return graph_module.forward

    def compile_with_eager_backend(fn, **kwargs):
        kwargs.pop("options", None)
        return real_compile(fn, backend=counting_backend, **kwargs)

    weights_eager = torch.tensor(
        [[0.35, 0.20], [0.15, 0.55], [0.50, 0.10], [0.20, 0.60]],
        device="cuda",
        requires_grad=True,
    )
    returns = torch.tensor(
        [[0.02, -0.01], [0.03, 0.01], [-0.02, 0.04], [0.01, 0.03]],
        device="cuda",
    )
    mask = torch.ones_like(weights_eager, dtype=torch.bool)
    actions = torch.zeros_like(mask)
    buy_fees = torch.tensor([0.000855, 0.000855], device="cuda")
    sell_fees = torch.tensor([0.003855, 0.001855], device="cuda")
    initial_weights = torch.tensor(
        [0.10, 0.20], device="cuda", requires_grad=True
    )
    initial_cash = torch.tensor(0.70, device="cuda", requires_grad=True)
    initial_payables = torch.zeros(2, device="cuda", requires_grad=True)
    initial_receivables = torch.zeros(2, device="cuda", requires_grad=True)
    initial_equity_scale = torch.ones((), device="cuda", requires_grad=True)
    initial_state = {
        "initial_weights": initial_weights,
        "initial_cash": initial_cash,
        "initial_payables": initial_payables,
        "initial_receivables": initial_receivables,
        "initial_equity_scale": initial_equity_scale,
    }

    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    reference = run_tw_cash_continuous(
        weights_eager,
        returns,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        unresolved_corporate_action_mask=actions,
        **initial_state,
    )
    reference_objective = reference.strategy_returns[2:].sum()
    reference_objective.backward()
    reference_grad = weights_eager.grad.detach().clone()

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_eager_backend)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", "2")

    weights_chunked = weights_eager.detach().clone().requires_grad_(True)
    chunked = run_tw_cash_continuous(
        weights_chunked,
        returns,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        unresolved_corporate_action_mask=actions,
        **initial_state,
    )
    chunked.strategy_returns[2:].sum().backward()

    torch.testing.assert_close(
        chunked.strategy_returns,
        reference.strategy_returns,
        rtol=2e-6,
        atol=2e-7,
    )
    torch.testing.assert_close(
        chunked.final_weights,
        reference.final_weights,
        rtol=2e-6,
        atol=2e-7,
    )
    torch.testing.assert_close(
        weights_chunked.grad,
        reference_grad,
        rtol=2e-5,
        atol=2e-6,
    )
    assert reference_grad[:2].abs().sum().item() > 0.0
    assert graph_count == 1
    for state_tensor in initial_state.values():
        assert state_tensor.grad is None
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compiled_chunk_calls"] == 2
    assert stats["eager_fallback_calls"] == 0

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_tw_day_trade_compiled_chunks_match_eager_and_preserve_gradient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_compile = torch.compile
    graph_count = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal graph_count
        graph_count += 1
        return graph_module.forward

    def compile_with_eager_backend(fn, **kwargs):
        kwargs.pop("options", None)
        return real_compile(fn, backend=counting_backend, **kwargs)

    weights_eager = torch.tensor(
        [
            [0.35, -0.20],
            [0.15, 0.30],
            [-0.40, 0.10],
            [0.20, -0.30],
            [0.25, 0.15],
        ],
        device="cuda",
        requires_grad=True,
    )
    returns_eager = torch.tensor(
        [
            [0.02, -0.01],
            [0.03, 0.01],
            [-0.02, 0.04],
            [0.01, 0.03],
            [-0.01, 0.02],
        ],
        device="cuda",
        requires_grad=True,
    )
    mask = torch.ones_like(weights_eager, dtype=torch.bool)
    fees = torch.tensor([0.000855, 0.001855], device="cuda")
    volume_limits = torch.tensor(
        [
            [10.0, 10.0],
            [10.0, 10.0],
            [0.20, 0.20],
            [0.20, 0.20],
            [0.20, 0.20],
        ],
        device="cuda",
    )
    initial_cash = torch.ones((), device="cuda", requires_grad=True)
    initial_payables = torch.zeros(2, device="cuda", requires_grad=True)
    initial_receivables = torch.zeros(2, device="cuda", requires_grad=True)
    initial_equity_scale = torch.ones((), device="cuda", requires_grad=True)
    initial_state = {
        "initial_cash": initial_cash,
        "initial_payables": initial_payables,
        "initial_receivables": initial_receivables,
        "initial_equity_scale": initial_equity_scale,
    }
    common = {
        "tradable_mask": mask,
        "can_buy_mask": mask,
        "can_sell_mask": mask,
        "can_short_open_mask": mask,
        "day_trade_eligible_mask": mask,
        "buy_fee_rates": fees,
        "sell_fee_rates": fees,
        "day_trade_can_buy_open_mask": mask,
        "day_trade_can_sell_open_mask": mask,
        "volume_limit_weights": volume_limits,
        **initial_state,
    }

    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_GRADIENT_HORIZON_ROWS", "4")
    reference = run_tw_day_trade_continuous(
        weights_eager,
        returns_eager,
        **common,
    )
    reference_objective = (
        reference.strategy_returns[2:].sum()
        + reference.final_equity_scale * 0.3
    )
    reference_objective.backward()
    reference_weight_grad = weights_eager.grad.detach().clone()
    reference_return_grad = returns_eager.grad.detach().clone()

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_eager_backend)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", "2")

    weights_chunked = weights_eager.detach().clone().requires_grad_(True)
    returns_chunked = returns_eager.detach().clone().requires_grad_(True)
    chunked = run_tw_day_trade_continuous(
        weights_chunked,
        returns_chunked,
        **common,
    )
    chunked_objective = (
        chunked.strategy_returns[2:].sum()
        + chunked.final_equity_scale * 0.3
    )
    chunked_objective.backward()

    for field in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "settlement_default",
        "equity_scale_history",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_alive",
        "final_equity_scale",
    ):
        torch.testing.assert_close(
            getattr(chunked, field),
            getattr(reference, field),
            rtol=2e-6,
            atol=2e-7,
        )
    torch.testing.assert_close(
        weights_chunked.grad,
        reference_weight_grad,
        rtol=2e-5,
        atol=2e-6,
    )
    torch.testing.assert_close(
        returns_chunked.grad,
        reference_return_grad,
        rtol=2e-5,
        atol=2e-6,
    )
    assert reference_weight_grad[:2].abs().sum().item() > 0.0
    assert graph_count == 1
    for state_tensor in initial_state.values():
        assert state_tensor.grad is None
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compiled_chunk_calls"] == 2
    assert stats["tw_day_trade_compiled_chunk_calls"] == 2
    assert stats["eager_fallback_calls"] == 0

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_tw_cash_exact_dividend_compiled_chunks_match_eager_and_queue_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_compile = torch.compile
    graph_count = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal graph_count
        graph_count += 1
        return graph_module.forward

    def compile_with_eager_backend(fn, **kwargs):
        kwargs.pop("options", None)
        return real_compile(fn, backend=counting_backend, **kwargs)

    base = torch.tensor(
        [[0.60, 0.20], [0.00, 0.50], [0.30, 0.30], [0.10, 0.60]],
        device="cuda",
    )
    returns = torch.tensor(
        [[-0.10, 0.01], [0.02, -0.01], [0.01, 0.03], [-0.02, 0.01]],
        device="cuda",
    )
    mask = torch.ones_like(base, dtype=torch.bool)
    actions = torch.zeros_like(mask)
    dividend_yield = torch.zeros_like(base)
    dividend_delay = torch.zeros_like(base, dtype=torch.int64)
    dividend_yield[0, 0] = 0.10
    dividend_delay[0, 0] = 4
    buy_fees = torch.tensor([0.000855, 0.000855], device="cuda")
    sell_fees = torch.tensor([0.003855, 0.001855], device="cuda")
    common = dict(
        unresolved_corporate_action_mask=actions,
        cash_dividend_yield=dividend_yield,
        cash_dividend_payment_delay_sessions=dividend_delay,
        claim_queue_sessions=6,
    )

    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    eager_weights = base.detach().clone().requires_grad_(True)
    eager = run_tw_cash_continuous(
        eager_weights,
        returns,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        **common,
    )
    eager.strategy_returns.sum().backward()
    eager_grad = eager_weights.grad.detach().clone()

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_eager_backend)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", "2")

    compiled_weights = base.detach().clone().requires_grad_(True)
    compiled = run_tw_cash_continuous(
        compiled_weights,
        returns,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        **common,
    )
    compiled.strategy_returns.sum().backward()

    for field in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
    ):
        torch.testing.assert_close(
            getattr(compiled, field),
            getattr(eager, field),
            rtol=2e-6,
            atol=2e-7,
        )
    torch.testing.assert_close(
        compiled_weights.grad,
        eager_grad,
        rtol=2e-5,
        atol=2e-6,
    )
    # Selling on row 1 cannot erase the row-0 entitlement, and the six-slot
    # queue is recurrent state crossing both compiled chunks.
    assert compiled.final_receivables[0].item() > 0.0
    assert graph_count == 1
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compiled_chunk_calls"] == 2
    assert stats["eager_fallback_calls"] == 0

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_tw_cash_margin_short_compiled_chunks_match_eager_and_backward(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_compile = torch.compile
    graph_count = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal graph_count
        graph_count += 1
        return graph_module.forward

    def compile_with_eager_backend(fn, **kwargs):
        kwargs.pop("options", None)
        return real_compile(fn, backend=counting_backend, **kwargs)

    base = torch.tensor(
        [
            [-0.25, -0.25],
            [0.00, -0.25],
            [0.00, -0.20],
            [0.00, 0.00],
            [-0.20, 0.00],
        ],
        device="cuda",
    )
    returns = torch.tensor(
        [
            [0.000, 0.000],
            [0.000, 0.000],
            [0.015, 0.000],
            [0.005, -0.010],
            [-0.010, 0.020],
        ],
        device="cuda",
    )
    mask = torch.ones_like(base, dtype=torch.bool)
    force_cover = torch.zeros_like(mask)
    force_cover[3, 1] = True
    actions = torch.zeros_like(mask)
    rates = torch.tensor([1.10, 0.90], device="cuda").expand_as(base)
    capacities = torch.full_like(base, 0.50)
    handling = torch.full_like(base, 0.0002)
    volume_limits = torch.full_like(base, 0.80)
    buy_fees = torch.tensor([0.000855, 0.000855], device="cuda")
    sell_fees = torch.tensor([0.003855, 0.001855], device="cuda")
    common = dict(
        can_short_open_mask=mask,
        force_short_cover_mask=force_cover,
        short_margin_rate=rates,
        short_capacity_weights=capacities,
        short_handling_fee_rate=handling,
        short_maintenance_ratio=1.95,
        volume_limit_weights=volume_limits,
        unresolved_corporate_action_mask=actions,
    )

    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    eager_weights = base.detach().clone().requires_grad_(True)
    eager = run_tw_cash_continuous(
        eager_weights,
        returns,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        **common,
    )
    eager.strategy_returns[-1].backward()
    eager_grad = eager_weights.grad.detach().clone()
    retained_ratio = (
        eager.short_sale_collateral_history[1].sum()
        + eager.short_margin_collateral_history[1].sum()
    ) / (-eager.weights_history[1].clamp_max(0.0).sum())
    assert retained_ratio.item() == pytest.approx(1.95, rel=2e-5)

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_eager_backend)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", "2")

    compiled_weights = base.detach().clone().requires_grad_(True)
    compiled = run_tw_cash_continuous(
        compiled_weights,
        returns,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        **common,
    )
    compiled.strategy_returns[-1].backward()

    for field in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "equity_scale_history",
        "short_sale_collateral_history",
        "short_margin_collateral_history",
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_equity_scale",
        "final_short_sale_collateral",
        "final_short_margin_collateral",
    ):
        torch.testing.assert_close(
            getattr(compiled, field),
            getattr(eager, field),
            rtol=2e-6,
            atol=2e-7,
        )
    assert torch.equal(compiled.settlement_default, eager.settlement_default)
    assert torch.equal(compiled.final_alive, eager.final_alive)
    torch.testing.assert_close(
        compiled_weights.grad,
        eager_grad,
        rtol=2e-5,
        atol=2e-6,
    )
    # The last objective is in the eager ragged tail.  Its gradient reaching
    # earlier rows proves both compiled chunks retained the collateral graph.
    assert eager_grad[:4].abs().sum().item() > 0.0
    assert graph_count == 1
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compiled_chunk_calls"] == 2
    assert stats["eager_fallback_calls"] == 0

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_tw_cash_compiled_active_signature_matches_eager_with_ragged_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Match the real loss signature, including a claim crossing a chunk edge."""

    real_compile = torch.compile

    def compile_with_eager_backend(fn, **kwargs):
        kwargs.pop("options", None)
        return real_compile(fn, backend="eager", **kwargs)

    weights_eager = torch.tensor(
        [
            [0.45, 0.10],
            [0.10, 0.55],
            [0.50, 0.20],
            [0.15, 0.60],
            [0.55, 0.05],
        ],
        device="cuda",
        requires_grad=True,
    )
    returns = torch.tensor(
        [
            [0.02, -0.01],
            [0.03, 0.01],
            [-0.02, 0.04],
            [0.01, 0.03],
            [-0.01, 0.02],
        ],
        device="cuda",
    )
    mask = torch.ones_like(weights_eager, dtype=torch.bool)
    actions = torch.zeros_like(mask)
    volume_limits = torch.full_like(weights_eager, 0.25)
    buy_fees = torch.tensor([0.000855, 0.000855], device="cuda")
    sell_fees = torch.tensor([0.003855, 0.001855], device="cuda")
    common_kwargs = {
        "unresolved_corporate_action_mask": actions,
        "volume_limit_weights": volume_limits,
        "return_weights_history": False,
    }

    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    reference = run_tw_cash_continuous(
        weights_eager,
        returns,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        **common_kwargs,
    )
    reference.strategy_returns[-1].backward()
    reference_grad = weights_eager.grad.detach().clone()

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_eager_backend)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", "2")

    weights_chunked = weights_eager.detach().clone().requires_grad_(True)
    chunked = run_tw_cash_continuous(
        weights_chunked,
        returns,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        **common_kwargs,
    )
    chunked.strategy_returns[-1].backward()

    for name in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "equity_scale_history",
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_equity_scale",
    ):
        torch.testing.assert_close(
            getattr(chunked, name),
            getattr(reference, name),
            rtol=2e-6,
            atol=2e-7,
        )
    assert torch.equal(chunked.settlement_default, reference.settlement_default)
    assert torch.equal(chunked.final_alive, reference.final_alive)
    torch.testing.assert_close(
        weights_chunked.grad,
        reference_grad,
        rtol=2e-5,
        atol=2e-6,
    )
    # The objective is only on the eager tail, yet it must backpropagate
    # through both earlier compiled chunks and their T+2 carried claims.
    assert reference_grad[:2].abs().sum().item() > 0.0
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compiled_chunk_calls"] == 2
    assert stats["eager_fallback_calls"] == 0

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_tw_cash_single_symbol_uses_static_compiled_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_compile = torch.compile

    def compile_with_eager_backend(fn, **kwargs):
        kwargs.pop("options", None)
        return real_compile(fn, backend="eager", **kwargs)

    weights = torch.full((4, 1), 0.25, device="cuda", requires_grad=True)
    returns = torch.full((4, 1), 0.01, device="cuda")
    mask = torch.ones_like(weights, dtype=torch.bool)
    actions = torch.zeros_like(mask)

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_eager_backend)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    # A broader configured universe must not make Dynamo treat S=1 as dynamic.
    monkeypatch.setenv("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", "8")

    result = run_tw_cash_continuous(
        weights,
        returns,
        mask,
        mask,
        mask,
        torch.zeros(1, device="cuda"),
        torch.zeros(1, device="cuda"),
        unresolved_corporate_action_mask=actions,
    )
    result.strategy_returns.sum().backward()

    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compiled_chunk_calls"] == 2
    assert stats["eager_fallback_calls"] == 0

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_tw_cash_compiled_chunk_reuses_one_graph_across_symbol_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_compile = torch.compile
    graph_count = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal graph_count
        graph_count += 1
        return graph_module.forward

    def compile_with_counting_backend(fn, **kwargs):
        kwargs.pop("options", None)
        return real_compile(fn, backend=counting_backend, **kwargs)

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_counting_backend)
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", "4")

    for symbols in (2, 3):
        base = torch.full((4, symbols), 0.5 / symbols, device="cuda")
        returns = torch.linspace(
            -0.01,
            0.02,
            steps=4 * symbols,
            device="cuda",
        ).reshape(4, symbols)
        mask = torch.ones_like(base, dtype=torch.bool)
        actions = torch.zeros_like(mask)
        fees = torch.full((symbols,), 0.000855, device="cuda")

        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
        eager_weights = base.detach().clone().requires_grad_(True)
        eager = run_tw_cash_continuous(
            eager_weights,
            returns,
            mask,
            mask,
            mask,
            fees,
            fees,
            unresolved_corporate_action_mask=actions,
        )
        eager.strategy_returns.sum().backward()

        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
        compiled_weights = base.detach().clone().requires_grad_(True)
        compiled = run_tw_cash_continuous(
            compiled_weights,
            returns,
            mask,
            mask,
            mask,
            fees,
            fees,
            unresolved_corporate_action_mask=actions,
        )
        compiled.strategy_returns.sum().backward()

        torch.testing.assert_close(
            compiled.strategy_returns,
            eager.strategy_returns,
            rtol=2e-6,
            atol=2e-7,
        )
        torch.testing.assert_close(
            compiled_weights.grad,
            eager_weights.grad,
            rtol=2e-5,
            atol=2e-6,
        )

    assert graph_count == 1
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compile_constructors"] == 1
    assert stats["compiled_chunk_calls"] == 4
    assert stats["eager_fallback_calls"] == 0

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_tw_cash_margin_short_exact_dividend_chunk_reuses_one_graph_across_symbol_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_compile = torch.compile
    graph_count = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal graph_count
        graph_count += 1
        return graph_module.forward

    def compile_with_counting_backend(fn, **kwargs):
        kwargs.pop("options", None)
        return real_compile(fn, backend=counting_backend, **kwargs)

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()
    tw_continuous_module.get_tw_continuous_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_counting_backend)
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", "4")

    for symbols in (2, 3):
        base = torch.full(
            (4, symbols),
            0.5 / symbols,
            device="cuda",
        )
        base[:, ::2] *= -1.0
        returns = torch.linspace(
            -0.01,
            0.02,
            steps=4 * symbols,
            device="cuda",
        ).reshape(4, symbols)
        mask = torch.ones_like(base, dtype=torch.bool)
        force_cover = torch.zeros_like(mask)
        force_cover[2, 0] = True
        actions = torch.zeros_like(mask)
        capacities = torch.full_like(base, 0.5)
        dividend_yield = torch.zeros_like(base)
        dividend_delay = torch.zeros_like(base, dtype=torch.int64)
        dividend_yield[0, -1] = 0.02
        dividend_delay[0, -1] = 4
        fees = torch.full((symbols,), 0.000855, device="cuda")
        common = dict(
            can_short_open_mask=mask,
            force_short_cover_mask=force_cover,
            short_margin_rate=0.90,
            short_capacity_weights=capacities,
            short_handling_fee_rate=0.0002,
            unresolved_corporate_action_mask=actions,
            cash_dividend_yield=dividend_yield,
            cash_dividend_payment_delay_sessions=dividend_delay,
            claim_queue_sessions=6,
        )

        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
        eager_weights = base.detach().clone().requires_grad_(True)
        eager = run_tw_cash_continuous(
            eager_weights,
            returns,
            mask,
            mask,
            mask,
            fees,
            fees,
            **common,
        )
        eager.strategy_returns.sum().backward()

        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
        compiled_weights = base.detach().clone().requires_grad_(True)
        compiled = run_tw_cash_continuous(
            compiled_weights,
            returns,
            mask,
            mask,
            mask,
            fees,
            fees,
            **common,
        )
        compiled.strategy_returns.sum().backward()

        for field in (
            "strategy_returns",
            "receivables_history",
            "short_sale_collateral_history",
            "short_margin_collateral_history",
            "final_receivables",
            "final_short_sale_collateral",
            "final_short_margin_collateral",
        ):
            torch.testing.assert_close(
                getattr(compiled, field),
                getattr(eager, field),
                rtol=2e-6,
                atol=2e-7,
            )
        torch.testing.assert_close(
            compiled_weights.grad,
            eager_weights.grad,
            rtol=2e-5,
            atol=2e-6,
        )

    assert graph_count == 1
    stats = tw_continuous_module.get_tw_continuous_compile_stats()
    assert stats["compile_constructors"] == 1
    assert stats["compiled_chunk_calls"] == 4
    assert stats["eager_fallback_calls"] == 0

    torch._dynamo.reset()
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()


@pytest.mark.parametrize("rows", [2, 4])
def test_zero_cost_unchanged_cash_target_has_zero_gradient(rows: int) -> None:
    """The settlement ledger must not create a gradient from a zero PnL path."""

    weights = torch.full((rows, 1), 0.2, dtype=torch.float64, requires_grad=True)
    mask = _mask_like(weights)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(rows, dtype=torch.float64),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.zeros(1, dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(weights, dtype=torch.bool),
    )

    objective = result.strategy_returns.sum()
    objective.backward()
    torch.testing.assert_close(objective, torch.zeros_like(objective), rtol=0.0, atol=0.0)
    assert weights.grad is not None
    torch.testing.assert_close(weights.grad, torch.zeros_like(weights), rtol=0.0, atol=0.0)


def test_fixed_reference_cap_has_finite_gradient_at_zero_equity_scale() -> None:
    """An inactive zero-NAV branch must not form Inf in DivBackward."""

    requested = torch.tensor([0.0, 0.4], dtype=torch.float64, requires_grad=True)
    equity_scale = torch.zeros((), dtype=torch.float64, requires_grad=True)
    capped = tw_continuous_module._cap_current_nav_request_from_reference_notional(
        requested,
        torch.tensor([0.25, 0.25], dtype=torch.float64),
        equity_scale,
    )

    torch.testing.assert_close(capped, requested)
    capped.sum().backward()
    assert requested.grad is not None and torch.isfinite(requested.grad).all()
    assert equity_scale.grad is not None and torch.isfinite(equity_scale.grad)
    torch.testing.assert_close(requested.grad, torch.ones_like(requested))
    torch.testing.assert_close(equity_scale.grad, torch.zeros_like(equity_scale))


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
def test_dead_taiwan_account_volume_cap_backward_is_finite(
    execution_mode: str,
) -> None:
    """Absorbing ruin remains a differentiable no-op for later target rows."""

    weights = torch.tensor([[0.4]], dtype=torch.float64, requires_grad=True)
    mask = torch.ones_like(weights, dtype=torch.bool)
    common = dict(
        target_weights=weights,
        asset_log_returns=torch.zeros_like(weights),
        tradable_mask=mask,
        can_buy_mask=mask,
        can_sell_mask=mask,
        buy_fee_rates=torch.zeros(1, dtype=torch.float64),
        sell_fee_rates=torch.zeros(1, dtype=torch.float64),
        volume_limit_weights=torch.tensor([[0.5]], dtype=torch.float64),
        initial_cash=torch.zeros((), dtype=torch.float64),
        initial_payables=torch.zeros(2, dtype=torch.float64),
        initial_receivables=torch.zeros(2, dtype=torch.float64),
        initial_alive=torch.tensor(False),
        initial_equity_scale=torch.zeros((), dtype=torch.float64),
    )
    if execution_mode == "tw_cash":
        result = run_tw_cash_continuous(
            unresolved_corporate_action_mask=torch.zeros_like(mask),
            initial_weights=torch.zeros(1, dtype=torch.float64),
            initial_short_sale_collateral=torch.zeros(1, dtype=torch.float64),
            initial_short_margin_collateral=torch.zeros(1, dtype=torch.float64),
            **common,
        )
    else:
        day_common = dict(common)
        day_common["intraday_log_returns"] = day_common.pop("asset_log_returns")
        result = run_tw_day_trade_continuous(
            can_short_open_mask=mask,
            day_trade_eligible_mask=mask,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
            **day_common,
        )

    result.strategy_returns.sum().backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    torch.testing.assert_close(weights.grad, torch.zeros_like(weights))


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
def test_settlement_gradient_horizon_preserves_exact_forward_ledger(
    monkeypatch: pytest.MonkeyPatch,
    execution_mode: str,
) -> None:
    """Truncated ledger BPTT changes graph connectivity, never account values."""

    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    weights = torch.tensor(
        [[0.2, -0.1], [0.4, 0.1], [-0.2, 0.3], [0.1, 0.5]],
        dtype=torch.float64,
    )
    returns = torch.tensor(
        [[0.01, -0.02], [0.02, 0.01], [-0.01, 0.03], [0.04, -0.02]],
        dtype=torch.float64,
    )
    mask = torch.ones_like(weights, dtype=torch.bool)
    fees = torch.tensor([0.000855, 0.001], dtype=torch.float64)

    def execute() -> object:
        if execution_mode == "tw_cash":
            return run_tw_cash_continuous(
                weights,
                returns,
                mask,
                mask,
                mask,
                fees,
                fees,
                can_short_open_mask=mask,
                force_short_cover_mask=mask,
                short_margin_rate=0.9,
                short_capacity_weights=torch.ones_like(weights),
                unresolved_corporate_action_mask=torch.zeros_like(mask),
            )
        return run_tw_day_trade_continuous(
            weights,
            returns,
            mask,
            mask,
            mask,
            mask,
            mask,
            fees,
            fees,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
        )

    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_GRADIENT_HORIZON_ROWS", "0")
    full_horizon = execute()
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_GRADIENT_HORIZON_ROWS", "2")
    truncated = execute()

    for field in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "settlement_default",
        "equity_scale_history",
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_alive",
        "final_equity_scale",
    ):
        assert torch.equal(getattr(full_horizon, field), getattr(truncated, field))


def test_settlement_gradient_horizon_cuts_only_prior_state_gradient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_GRADIENT_HORIZON_ROWS", "2")
    weights = torch.tensor(
        [[0.2, 0.3], [0.5, 0.1], [0.1, 0.6], [0.4, 0.2]],
        dtype=torch.float64,
        requires_grad=True,
    )
    returns = torch.tensor(
        [[0.01, -0.02], [0.02, 0.01], [-0.01, 0.03], [0.04, -0.02]],
        dtype=torch.float64,
    )
    mask = torch.ones_like(weights, dtype=torch.bool)
    result = run_tw_cash_continuous(
        weights,
        returns,
        mask,
        mask,
        mask,
        torch.tensor([0.000855, 0.000855], dtype=torch.float64),
        torch.tensor([0.003855, 0.003855], dtype=torch.float64),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
    )

    result.strategy_returns[2:].sum().backward()
    assert weights.grad is not None and torch.isfinite(weights.grad).all()
    torch.testing.assert_close(weights.grad[:2], torch.zeros_like(weights.grad[:2]))
    assert float(weights.grad[2:].abs().sum()) > 0.0


def test_cash_and_day_trade_ledgers_pass_double_precision_gradcheck() -> None:
    mask = torch.ones((2, 2), dtype=torch.bool)
    fees_buy = torch.tensor([0.000855, 0.000855], dtype=torch.float64)
    fees_sell = torch.tensor([0.003855, 0.001855], dtype=torch.float64)
    cash_returns = torch.tensor(
        [[0.01, -0.02], [0.015, 0.005]], dtype=torch.float64
    )
    day_returns = torch.tensor(
        [[0.012, -0.008], [-0.009, 0.011]], dtype=torch.float64
    )

    def cash_objective(weights: torch.Tensor) -> torch.Tensor:
        return run_tw_cash_continuous(
            weights,
            cash_returns,
            mask,
            mask,
            mask,
            fees_buy,
            fees_sell,
            unresolved_corporate_action_mask=torch.zeros_like(
                weights, dtype=torch.bool
            ),
        ).strategy_returns.sum()

    def day_objective(weights: torch.Tensor) -> torch.Tensor:
        return run_tw_day_trade_continuous(
            weights,
            day_returns,
            mask,
            mask,
            mask,
            mask,
            mask,
            fees_buy,
            fees_sell,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
        ).strategy_returns.sum()

    cash_weights = torch.tensor(
        [[0.18, 0.22], [0.25, 0.15]], dtype=torch.float64, requires_grad=True
    )
    day_weights = torch.tensor(
        [[0.18, -0.22], [-0.25, 0.15]], dtype=torch.float64, requires_grad=True
    )
    assert torch.autograd.gradcheck(cash_objective, (cash_weights,))
    assert torch.autograd.gradcheck(day_objective, (day_weights,))


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
def test_log_utility_loss_updates_real_settlement_state(
    execution_mode: str,
) -> None:
    weights = torch.tensor(
        [[0.4, 0.3], [0.2, 0.5], [0.3, 0.4]], requires_grad=True
    )
    if execution_mode == "tw_day_trade":
        weights = torch.tensor(
            [[0.4, -0.3], [-0.2, 0.5], [0.3, -0.4]], requires_grad=True
        )
    mask = _mask_like(weights)
    aux = {
        "initial_weights": torch.zeros(2),
        "initial_alive": torch.tensor(True),
        "initial_cash": torch.tensor(1.0),
        "initial_payables": torch.zeros(2),
        "initial_receivables": torch.zeros(2),
    }
    loss = risk_aware_loss(
        weights,
        torch.tensor([[0.01, -0.02], [0.02, 0.01], [-0.01, 0.03]]),
        mask,
        benchmark_returns=torch.zeros(3),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        long_only=execution_mode == "tw_cash",
        execution_mode=execution_mode,
        buy_fee_rates=torch.full((2,), 0.000855),
        sell_fee_rates=torch.full((2,), 0.003855),
        day_trade_eligible_mask=(mask if execution_mode == "tw_day_trade" else None),
        **_real_execution_masks(weights, execution_mode),
        objective="log_utility",
        concentration_weight=0.0,
        net_exposure_weight=0.0,
        aux_outputs=aux,
    )
    loss.backward()

    assert torch.isfinite(loss)
    assert weights.grad is not None and torch.isfinite(weights.grad).all()
    for key in ("_final_cash", "_final_payables", "_final_receivables", "_final_alive"):
        assert key in aux


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
@pytest.mark.parametrize(
    "objective",
    ["log_utility", "portfolio_autoencoder", "factor_generalization"],
)
def test_all_canonical_loss_wrappers_carry_equity_scale(
    execution_mode: str,
    objective: str,
) -> None:
    weights = torch.tensor(
        [[0.4, 0.3], [0.2, 0.5]], dtype=torch.float32, requires_grad=True
    )
    mask = _mask_like(weights)
    aux = {
        "initial_weights": torch.zeros(2),
        "initial_alive": torch.tensor(True),
        "initial_cash": torch.tensor(1.0),
        "initial_payables": torch.zeros(2),
        "initial_receivables": torch.zeros(2),
        "initial_equity_scale": torch.tensor(0.25),
    }
    loss = risk_aware_loss(
        weights,
        torch.zeros_like(weights),
        mask,
        benchmark_returns=torch.zeros(2),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        long_only=True,
        execution_mode=execution_mode,
        buy_fee_rates=torch.zeros(2),
        sell_fee_rates=torch.zeros(2),
        day_trade_eligible_mask=(mask if execution_mode == "tw_day_trade" else None),
        **_real_execution_masks(weights, execution_mode),
        objective=objective,
        concentration_weight=0.0,
        net_exposure_weight=0.0,
        aux_outputs=aux,
    )
    loss.backward()

    assert torch.isfinite(loss)
    assert weights.grad is not None and torch.isfinite(weights.grad).all()
    assert "_final_equity_scale" in aux
    torch.testing.assert_close(aux["_final_equity_scale"], torch.tensor(0.25))


def test_compiled_canonical_loss_returns_carried_equity_scale() -> None:
    if not hasattr(torch, "compile"):
        pytest.skip("torch.compile is unavailable")

    def execute(
        weights: torch.Tensor,
        initial_equity_scale: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        mask = torch.ones_like(weights, dtype=torch.bool)
        aux = {
            "initial_weights": torch.zeros_like(weights[0]),
            "initial_alive": torch.tensor(True),
            "initial_cash": torch.tensor(1.0),
            "initial_payables": torch.zeros(2),
            "initial_receivables": torch.zeros(2),
            "initial_equity_scale": initial_equity_scale,
        }
        loss = risk_aware_loss(
            weights,
            torch.zeros_like(weights),
            mask,
            benchmark_returns=torch.zeros(weights.size(0)),
            can_buy_mask=mask,
            can_sell_mask=mask,
            long_only=True,
            execution_mode="tw_cash",
            buy_fee_rates=torch.zeros(weights.size(1)),
            sell_fee_rates=torch.zeros(weights.size(1)),
            unresolved_corporate_action_mask=torch.zeros_like(mask),
            objective="log_utility",
            concentration_weight=0.0,
            net_exposure_weight=0.0,
            aux_outputs=aux,
        )
        return loss, aux["_final_equity_scale"]

    compiled = torch.compile(execute, backend="eager", fullgraph=True)
    weights = torch.tensor([[0.4, 0.3], [0.2, 0.5]], requires_grad=True)
    loss, final_equity_scale = compiled(weights, torch.tensor(0.25))
    loss.backward()

    assert torch.isfinite(loss)
    torch.testing.assert_close(final_equity_scale, torch.tensor(0.25))
    assert weights.grad is not None and torch.isfinite(weights.grad).all()


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
@pytest.mark.parametrize(
    "objective",
    ["log_utility", "portfolio_autoencoder", "factor_generalization"],
)
@pytest.mark.parametrize("bad_return", [float("nan"), float("inf"), float("-inf"), 1.0e4])
def test_taiwan_loss_handles_active_invalid_return_without_device_assertion(
    execution_mode: str,
    objective: str,
    bad_return: float,
) -> None:
    if objective == "factor_generalization":
        weights = torch.tensor([[1.0, -1.0]], requires_grad=True)
        returns = torch.tensor([[bad_return, 0.0]])
    else:
        weights = torch.tensor([[1.0]], requires_grad=True)
        returns = torch.tensor([[bad_return]])
    mask = torch.ones_like(weights, dtype=torch.bool)
    kwargs = dict(
        benchmark_returns=torch.zeros(1),
        can_buy_mask=mask,
        can_sell_mask=mask,
        long_only=True,
        execution_mode=execution_mode,
        buy_fee_rates=torch.zeros(weights.size(1)),
        sell_fee_rates=torch.zeros(weights.size(1)),
        objective=objective,
        concentration_weight=0.0,
        net_exposure_weight=0.0,
    )
    if execution_mode == "tw_cash":
        kwargs["unresolved_corporate_action_mask"] = torch.zeros_like(mask)
    else:
        kwargs.update(
            day_trade_eligible_mask=mask,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
        )

    loss = risk_aware_loss(weights, returns, mask, **kwargs)
    assert torch.isfinite(loss)
    loss.backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
def test_taiwan_loss_ignores_invalid_return_only_when_no_order_or_holding(
    execution_mode: str,
) -> None:
    weights = torch.zeros((1, 1), requires_grad=True)
    mask = torch.ones_like(weights, dtype=torch.bool)
    kwargs = dict(
        benchmark_returns=torch.zeros(1),
        can_buy_mask=mask,
        can_sell_mask=mask,
        long_only=True,
        execution_mode=execution_mode,
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        objective="log_utility",
        concentration_weight=0.0,
        net_exposure_weight=0.0,
    )
    if execution_mode == "tw_cash":
        kwargs["unresolved_corporate_action_mask"] = torch.zeros_like(mask)
    else:
        kwargs.update(
            day_trade_eligible_mask=mask,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
        )

    loss = risk_aware_loss(
        weights,
        torch.full_like(weights, float("nan")),
        mask,
        **kwargs,
    )

    assert torch.isfinite(loss)


def test_numpy_tw_dispatch_matches_torch() -> None:
    weights = np.asarray([[0.4, 0.6], [0.2, 0.8], [0.0, 1.0]], dtype=np.float32)
    returns = np.asarray([[0.01, 0.02], [-0.01, 0.03], [0.0, -0.02]], dtype=np.float32)
    mask = np.ones_like(weights, dtype=bool)
    buy = np.asarray([0.001, 0.001], dtype=np.float32)
    sell = np.asarray([0.004, 0.002], dtype=np.float32)
    numpy_result = run_backtest(
        weights,
        returns,
        mask,
        np.zeros(3, dtype=np.float32),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=buy,
        sell_fee_rates=sell,
        unresolved_corporate_action_mask=np.zeros_like(mask, dtype=bool),
    )
    torch_result = run_backtest_torch(
        torch.from_numpy(weights),
        torch.from_numpy(returns),
        torch.from_numpy(mask),
        torch.zeros(3),
        0.0,
        0.0,
        execution_mode="tw_cash",
        buy_fee_rates=torch.from_numpy(buy),
        sell_fee_rates=torch.from_numpy(sell),
        unresolved_corporate_action_mask=torch.zeros_like(
            torch.from_numpy(mask), dtype=torch.bool
        ),
    ).to_numpy()
    np.testing.assert_array_equal(numpy_result.strategy_returns, torch_result.strategy_returns)
    np.testing.assert_array_equal(numpy_result.turnovers, torch_result.turnovers)
    np.testing.assert_array_equal(numpy_result.weights_history, torch_result.weights_history)
