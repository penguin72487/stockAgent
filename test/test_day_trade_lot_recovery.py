from dataclasses import replace
from functools import partial

import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import compact_day_trade_carry_session
from stockagent.training.day_trade_lot_recovery import (
    build_sub_lot_recovery_labels,
    one_lot_net_pnl,
    sub_lot_recovery_delta,
)
from stockagent.training.loss import risk_aware_loss
from test_tw_day_trade_carry import session, v, run


def rates():
    return dict(buy=v(.001425), sell=v(.002925), normal=v(.004425), rebate=v(.00114))


@pytest.mark.parametrize("side", [-1, 1])
@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("ordinary_exit", [False, True])
def test_one_lot_labels_match_authoritative_account(side, compact, ordinary_exit):
    s = replace(session(exits=ordinary_exit), terminal_liquidation_price=v(1010.))
    if compact:
        s = compact_day_trade_carry_session(s)
    pnl, valid = one_lot_net_pnl(s, side=side, eligible=torch.ones(1, dtype=torch.bool), **rates())
    exact = run(v(side * .1).reshape(1, 1), [s], event_compression=compact)
    assert valid.all()
    torch.testing.assert_close(pnl.sum(), exact.final_state.last_nav - 10_000_000., rtol=0, atol=1e-8)


@pytest.mark.parametrize("weight", [0., .001, .099, -.001, -.099, .1, -.1])
def test_sub_lot_gradient_learns_profitable_lot_and_preserves_exact_forward(weight):
    s = replace(session(), terminal_liquidation_price=v(1010.))
    w = v(weight).reshape(1, 1).requires_grad_()
    result = run(w, [s])
    exact = result.strategy_returns.clone()
    shadow = sub_lot_recovery_delta(w, [s], result, can_long=torch.ones_like(w, dtype=torch.bool),
                                   can_short=torch.ones_like(w, dtype=torch.bool), initial_nav=10_000_000., **rates())
    torch.testing.assert_close(shadow, torch.zeros_like(shadow), rtol=0, atol=0)
    torch.testing.assert_close(result.strategy_returns + shadow, exact, rtol=0, atol=0)
    shadow.sum().backward()
    if abs(weight) < .1:
        assert w.grad.item() > 0  # buy or reduce a losing short at an up close
    else:
        assert w.grad.item() == 0  # filled actions keep their existing gradient


@pytest.mark.parametrize("weight", [0., .001, .099, -.001, -.099, .1, -.1])
def test_precomputed_sub_lot_labels_preserve_forward_and_gradient(weight):
    s = replace(session(), terminal_liquidation_price=v(1010.))
    allowed = torch.ones((1, 1), dtype=torch.bool)
    labels = build_sub_lot_recovery_labels((s,), **rates())

    def evaluate(precomputed):
        w = v(weight).reshape(1, 1).requires_grad_()
        result = run(w, [s])
        shadow = sub_lot_recovery_delta(
            w,
            [s],
            result,
            can_long=allowed,
            can_short=allowed,
            initial_nav=10_000_000.,
            labels=precomputed,
            **rates(),
        )
        shadow.sum().backward()
        return shadow.detach(), w.grad.detach()

    eager_value, eager_grad = evaluate(None)
    cached_value, cached_grad = evaluate(labels)
    torch.testing.assert_close(cached_value, eager_value, rtol=0, atol=0)
    torch.testing.assert_close(cached_grad, eager_grad, rtol=0, atol=0)


def test_precomputed_sub_lot_labels_still_obey_live_direction_permissions():
    s = replace(session(), terminal_liquidation_price=v(1010.))
    labels = build_sub_lot_recovery_labels((s,), **rates())
    w = v(.01).reshape(1, 1).requires_grad_()
    result = run(w, [s])
    blocked = torch.zeros_like(w, dtype=torch.bool)
    shadow = sub_lot_recovery_delta(
        w,
        [s],
        result,
        can_long=blocked,
        can_short=blocked,
        initial_nav=10_000_000.,
        labels=labels,
        **rates(),
    )
    shadow.sum().backward()
    assert w.grad.item() == 0


@pytest.mark.parametrize("blocked", ["eligibility", "capacity", "halted", "source_gap", "cash", "dead", "marks"])
def test_recovery_does_not_invent_executable_opportunities(blocked):
    s = replace(session(), terminal_liquidation_price=v(1010.))
    if blocked == "capacity": s = replace(s, entry_volume=v(1999))
    if blocked == "halted": s = replace(s, halted=v(1))
    if blocked == "source_gap": s = replace(s, source_gap_mask=v(1))
    if blocked == "marks": s = replace(s, marks=torch.full_like(s.marks, float("nan")))
    w = v(.01).reshape(1, 1).requires_grad_()
    result = run(w, [s])
    allowed = torch.full_like(w, blocked != "eligibility", dtype=torch.bool)
    nav = 100_000. if blocked == "cash" else 0. if blocked == "dead" else 10_000_000.
    shadow = sub_lot_recovery_delta(w, [s], result, can_long=allowed, can_short=allowed,
                                   initial_nav=nav, **rates())
    shadow.sum().backward()
    assert w.grad.item() == 0


def test_profitable_close_does_not_hide_intraday_counterfactual_default():
    s = replace(session(), terminal_liquidation_price=v(900.), marks=torch.full((1, 270), 21000., dtype=torch.float64))
    w = v(-.01).reshape(1, 1).requires_grad_()
    result = run(w, [s])
    shadow = sub_lot_recovery_delta(w, [s], result,
        can_long=torch.ones_like(w, dtype=torch.bool), can_short=torch.ones_like(w, dtype=torch.bool),
        initial_nav=10_000_000., **rates())
    shadow.sum().backward()
    assert w.grad.item() == 0


def test_loss_integration_and_eval_are_exact():
    s = replace(session(), terminal_liquidation_price=v(1010.))
    w = v(.01).reshape(1, 1).requires_grad_()
    mask = torch.ones_like(w, dtype=torch.bool)
    loss = partial(risk_aware_loss, objective="log_utility", long_only=False,
        gamma_sharpe=1., gamma_turnover=0., execution_mode="tw_day_trade",
        portfolio_activation="pre_normalized", day_trade_unlimited_margin_conversion=True,
        day_trade_execution_initial_capital=10_000_000., day_trade_carry_sessions=(s,),
        day_trade_eligible_mask=mask, day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask, can_short_open_mask=mask,
        normal_sell_fee_rates=v(.004425), buy_fee_rates=v(.001425), sell_fee_rates=v(.002925),
        commission_rebate_rates=v(.00114), benchmark_returns=v(0.))
    control = loss(w, torch.zeros_like(w), mask)
    recovered = loss(w, torch.zeros_like(w), mask, day_trade_sub_lot_recovery=True)
    torch.testing.assert_close(recovered, control, rtol=0, atol=0)
    recovered.backward()
    assert w.grad.item() < 0
    with torch.no_grad():
        torch.testing.assert_close(loss(w, torch.zeros_like(w), mask, day_trade_sub_lot_recovery=True), control, rtol=0, atol=0)
