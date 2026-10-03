"""A known delisting announcement requests trades; it cannot invent fills."""

import os

import pytest
import torch

from stockagent.backtest.crypto_perpetual import (
    CryptoPerpetualDataError, run_crypto_perpetual_torch,
)
from stockagent.backtest.simulator import run_backtest_torch


def _run(*, sign=1, rows=1, capital_scale=1.0, initial=0.5, cap=0.1,
         permitted=True, announced=True, fee=0.0, effective=0.0, price=0.0,
         canonical=False, **kwargs):
    target = torch.full((rows, 1), float(-sign))  # An opposing model cannot reverse the exit.
    policy = torch.zeros_like(target, dtype=torch.bool)
    yes = torch.ones_like(policy)
    exit_mask = torch.full_like(policy, announced)
    buy = yes if sign > 0 or permitted else ~yes
    sell = yes if sign < 0 or permitted else ~yes
    common = dict(buy_fee_rate=fee, sell_fee_rate=fee, long_only=False,
                  initial_weights=torch.tensor([sign * initial]),
                  initial_equity_scale=torch.tensor(capital_scale, dtype=torch.float64),
                  volume_limit_weights=torch.full_like(target, cap))
    common.update(kwargs)
    effective_log = torch.log1p(torch.full_like(target, effective))
    price_log = torch.log1p(torch.full_like(target, price))
    if canonical:
        return run_backtest_torch(
            target, effective_log, policy, torch.zeros(rows),
            can_buy_mask=buy, can_sell_mask=sell, can_short_open_mask=yes,
            force_exit_mask=exit_mask, overnight_returns=price_log,
            execution_mode="crypto_perpetual", portfolio_activation="pre_normalized",
            **common,
        )
    return run_crypto_perpetual_torch(
        target, effective_log, price_log, policy, buy, sell, yes, exit_mask,
        maximum_gross=1.0, **common,
    )


@pytest.mark.parametrize("sign", [-1, 1])
@pytest.mark.parametrize("canonical", [False, True])
@pytest.mark.parametrize("permitted", [False, True])
def test_announced_exit_uses_real_side_permission_outside_policy_universe(sign, canonical, permitted):
    result = _run(sign=sign, canonical=canonical, permitted=permitted)
    expected_fill = 0.1 if permitted else 0.0
    assert result.turnovers.item() == pytest.approx(expected_fill)
    assert result.final_weights.item() == pytest.approx(sign * (0.5 - expected_fill))


@pytest.mark.parametrize("sign", [-1, 1])
def test_persistent_announcement_retries_capacity_limited_residual_without_reentry(sign):
    result = _run(sign=sign, rows=4, cap=0.2)
    torch.testing.assert_close(result.executed_weights[:, 0],
                               torch.tensor([sign * 0.3, sign * 0.1, 0.0, 0.0]))
    torch.testing.assert_close(result.turnovers, torch.tensor([0.2, 0.2, 0.1, 0.0]))
    assert result.final_weights.item() == 0.0


@pytest.mark.parametrize("sign", [-1, 1])
@pytest.mark.parametrize("cap,turnover_cap,scale,expected", [
    (0.0, 0.0, 1.0, 0.0),
    (0.2, 0.05, 1.0, 0.05),
    (0.2, 0.0, 2.0, 0.1),
])
def test_announced_exit_respects_zero_capacity_turnover_and_live_nav(sign, cap, turnover_cap, scale, expected):
    result = _run(sign=sign, cap=cap, capital_scale=scale, max_turnover_ratio=turnover_cap)
    assert result.turnovers.item() == pytest.approx(expected)
    assert result.executed_weights.item() == pytest.approx(sign * (0.5 - expected))


@pytest.mark.parametrize("sign", [-1, 1])
def test_announced_residual_still_receives_price_pnl_funding_and_real_fill_fees(sign):
    result = _run(sign=sign, fee=0.00055, effective=0.08, price=0.10)
    expected_net = sign * 0.4 * 0.08 - 0.1 * 0.00055
    assert result.strategy_simple_returns.item() == pytest.approx(expected_net)
    assert result.final_weights.item() == pytest.approx(sign * 0.4 * 1.10 / (1 + expected_net))
    assert result.final_equity_scale.item() == pytest.approx(1 + expected_net)


@pytest.mark.parametrize("sign", [-1, 1])
def test_announced_exit_bypasses_only_the_proximal_dead_zone(sign):
    result = _run(sign=sign, initial=0.0002, fee=0.00055,
                  stateful_proximal_allocator=True)
    assert result.executed_weights.item() == 0.0
    assert result.turnovers.item() == pytest.approx(0.0002)
    blocked = _run(sign=sign, initial=0.0002, fee=0.00055, permitted=False,
                   stateful_proximal_allocator=True)
    assert blocked.turnovers.item() == 0.0
    assert blocked.executed_weights.item() == pytest.approx(sign * 0.0002)


@pytest.mark.parametrize("canonical", [False, True])
def test_unannounced_nonpolicy_holding_stays_frozen_despite_side_permissions(canonical):
    result = _run(announced=False, canonical=canonical)
    assert result.turnovers.item() == 0.0
    assert result.final_weights.item() == 0.5


@pytest.mark.parametrize("cap,permitted", [(0.1, True), (1.0, False)])
def test_announcement_does_not_erase_missing_valuation_for_unfilled_inventory(cap, permitted):
    with pytest.raises(CryptoPerpetualDataError, match="held valuation unavailable"):
        _run(cap=cap, permitted=permitted, effective=float("nan"), price=float("nan"))
    closed = _run(cap=1.0, effective=float("nan"), price=float("nan"))
    assert closed.final_weights.item() == 0.0
    assert closed.final_alive.item()


def test_announced_padding_row_is_identity_even_with_missing_labels():
    result = _run(effective=float("nan"), price=float("nan"),
                  state_advance_mask=torch.tensor([False]))
    assert result.turnovers.item() == 0.0
    assert result.strategy_simple_returns.item() == 0.0
    assert result.final_weights.item() == 0.5
    assert result.final_equity_scale.item() == 1.0


@pytest.mark.skipif(os.environ.get("STOCKAGENT_TEST_CRYPTO_CUDA") != "1" or not torch.cuda.is_available(),
                    reason="explicit bounded CUDA ledger check only")
@pytest.mark.filterwarnings("ignore:The .grad attribute of a Tensor that is not a leaf Tensor:UserWarning")
def test_compiled_announced_exit_matches_eager_values_and_gradients(monkeypatch):
    results, gradients = [], []
    for compiled in ("0", "1"):
        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", compiled)
        action = torch.tensor([[.3, -.2]] * 8, device="cuda", requires_grad=True)
        announcement = torch.zeros_like(action, dtype=torch.bool)
        announcement[3:, 0] = True
        policy = ~announcement
        yes = torch.ones_like(policy)
        price = torch.full_like(action, .01)
        result = run_crypto_perpetual_torch(
            action, torch.log1p(price - .001), torch.log1p(price),
            policy, yes, yes, yes, announcement,
            buy_fee_rate=.00055, sell_fee_rate=.00055, long_only=False,
            maximum_gross=1.0, volume_limit_weights=torch.full_like(action, .08),
            stateful_proximal_allocator=True,
        )
        result.final_equity_scale.log().backward()
        results.append(result)
        gradients.append(action.grad)
    for field in ("strategy_simple_returns", "turnovers", "executed_weights", "equity_scale_history", "final_weights"):
        torch.testing.assert_close(getattr(results[0], field), getattr(results[1], field), atol=2e-7, rtol=2e-5)
    assert torch.isfinite(gradients[0]).all() and torch.isfinite(gradients[1]).all()
    torch.testing.assert_close(*gradients, atol=2e-7, rtol=2e-5)
