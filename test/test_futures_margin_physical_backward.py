"""The learning account must retain physical ownership and mandatory exits."""
from dataclasses import fields

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch as run
from stockagent.data import tw_futures_margin as m
from test_tw_futures_margin import tape


def paired_tape(rows=1):
    x = tape(rows, 2)
    x[..., 9] = 0
    x[..., 10] = torch.tensor([0., 1.])
    x[:, 1, 3:5] = 250
    x[:, 1, m.PREVIOUS_MARK] = x[:, 1, m.TERMINAL_MARK] = 250
    x[:, 1, [m.INITIAL, m.PREVIOUS_INITIAL, m.END_INITIAL]] = 25
    x[:, 1, [m.MAINTENANCE, m.PREVIOUS_MAINTENANCE, m.END_MAINTENANCE]] = 18.75
    return x


@pytest.mark.parametrize('sign', [-1., 1.])
@pytest.mark.parametrize('recover', [False, True])
def test_other_denomination_cannot_close_or_reprice_locked_inventory(sign, recover):
    x = paired_tape()
    x[:, 0, 8] = 0  # Standard inventory is locked; mini remains tradable.
    x[:, 0, 4] += sign * 10
    x[:, 1, 4] += sign * 100  # Tempting but unheld/unrequested mini return.
    w = torch.zeros(1, 2, requires_grad=True)
    r = run(w, x, initial_capital=1000., initial_quantities=torch.tensor([sign * 5, 0.]),
            recoverable_backward=recover)
    r.strategy_returns.sum().backward()
    assert r.final_weights.tolist() == [sign * 5, 0]
    assert r.final_equity_scale.item() == pytest.approx(1.05)
    torch.testing.assert_close(w.grad, torch.zeros_like(w), rtol=0, atol=0)


@pytest.mark.parametrize('sign', [-1., 1.])
def test_gradient_prices_selected_physical_denomination(sign):
    x = paired_tape()
    x[:, 0, 4] += sign * 20
    x[:, 1, 4] -= sign * 5
    w = torch.tensor([[sign * .5, 0.]], requires_grad=True)
    r = run(w, x, initial_capital=1000.)
    q = r.contract_quantities_history[0].float()
    share = q.abs() * x[0, :, m.INITIAL]
    share /= share.sum()
    slope = (share * (x[0, :, 4] - x[0, :, 3]) / x[0, :, m.INITIAL]).sum()
    expected = slope / r.final_equity_scale.detach()
    r.strategy_returns.sum().backward()
    torch.testing.assert_close(w.grad, expected.expand_as(w), rtol=1e-5, atol=1e-7)


@pytest.mark.parametrize('sign', [-1., 1.])
@pytest.mark.parametrize('intraday_move', [0., 1000.])
def test_unfilled_margin_call_keeps_actual_inventory_and_its_marked_pnl(sign, intraday_move):
    x = tape(2)
    x[0, :, 4] = 1000 - sign * 140
    x[1, :, 3] = x[0, :, 4] + sign * 10
    x[1, :, m.PREVIOUS_MARK] = x[0, :, 4]
    x[1, :, 4] = x[1, :, 3] + sign * intraday_move
    x[1, :, m.CAN_SELL if sign > 0 else m.CAN_BUY] = 0
    w = torch.tensor([[sign * .5], [sign * .9]], requires_grad=True)
    r = run(w, x, initial_capital=1000., recoverable_backward=True)
    r.strategy_returns.sum().backward()
    # Gradient/account v12 retains unfilled physical risk. Failed liquidation
    # alone is not insolvency and cannot suppress the retained book's PnL.
    assert r.default_reason_history.tolist() == [0, 0]
    assert r.final_alive
    assert r.contract_quantities_history[:, 0].tolist() == [int(sign * 5)] * 2
    assert r.final_equity_scale == pytest.approx((350 + 5 * intraday_move) / 1000)
    if intraday_move == 0:
        assert w.grad[0].item() * sign < 0
    else:
        assert w.grad[0].item() * sign > 0
    assert w.grad[1].item() == 0
    assert torch.isfinite(w.grad).all()


@pytest.mark.parametrize('recover', [False, True])
def test_expiry_at_zero_has_a_physical_first_contract_gradient(recover):
    x = tape()
    x[..., 2] = x[..., m.CASH_SETTLEMENT] = 1
    x[..., m.TERMINAL_MARK] = 1100
    w = torch.zeros(1, 1, requires_grad=True)
    r = run(w, x, initial_capital=1000., recoverable_backward=recover)
    r.strategy_returns.sum().backward()
    assert r.final_equity_scale.item() == 1.
    assert w.grad.item() == pytest.approx(1., rel=1e-4)


def test_failed_position_limit_close_has_reduction_gradient_without_price_loss():
    x = tape(2)
    x[1, :, m.POSITION_LIMIT] = 3
    x[1, :, 8] = 1  # Four remain: still above the limit after every allowed close.
    w = torch.tensor([[.5], [.9]], requires_grad=True)
    r = run(w, x, initial_capital=1000., recoverable_backward=True)
    r.strategy_returns.sum().backward()
    assert r.default_reason_history.tolist() == [0, 4]
    assert w.grad[0].item() < 0
    assert w.grad[1].item() == 0


@pytest.mark.parametrize('recover', [False, True])
def test_inert_padding_preserves_every_forward_field_and_gradient(recover):
    x = tape(3)
    x[:, 0, 4] = torch.tensor([1005., 995., 1020.])
    x[1:, 0, m.PREVIOUS_MARK] = x[:-1, 0, 4]
    w = torch.tensor([[.4], [.3], [.5]], requires_grad=True)
    a = run(w, x, initial_capital=1000., recoverable_backward=recover)
    a.strategy_returns.sum().backward()
    padded_w = torch.cat((w.detach(), torch.zeros_like(w))).requires_grad_()
    b = run(padded_w, torch.cat((x, torch.zeros_like(x))), initial_capital=1000.,
            state_advance_mask=torch.tensor([1, 1, 1, 0, 0, 0], dtype=torch.bool),
            recoverable_backward=recover)
    b.strategy_returns.sum().backward()
    for f in fields(a):
        if f.name.startswith('_'):
            continue
        before, after = getattr(a, f.name), getattr(b, f.name)
        if isinstance(before, torch.Tensor):
            if before.shape != after.shape:
                after = after[:3]
            torch.testing.assert_close(before, after, rtol=0, atol=0)
    torch.testing.assert_close(w.grad, padded_w.grad[:3], rtol=0, atol=0)
    assert padded_w.grad[3:].count_nonzero() == 0
