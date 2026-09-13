"""Exact-account, denomination, and chronological-gradient regression cases."""
from dataclasses import fields
from pathlib import Path

import pytest
import torch

from stockagent.backtest.tw_stock_futures_day_trade import (
    _scheduled_cash_bracket,
    run_tw_stock_futures_day_trade_integer_torch,
)
from test_tw_stock_futures_minute_gradients import profitable_tape


def execute(weights, tape, *, recovery=True, **kwargs):
    return run_tw_stock_futures_day_trade_integer_torch(
        weights, tape, initial_capital=1e6, scheduled_events=True,
        recoverable_backward=recovery, use_compile=False, **kwargs,
    )


def test_flat_policy_has_a_directional_gradient_from_real_baskets():
    w = torch.zeros(1, 1, requires_grad=True)
    r = execute(w, profitable_tape())
    r.strategy_returns.sum().backward()
    assert r.strategy_returns.item() == 0
    assert not r.contract_quantities_history.any()
    # The improving one-sided slope includes both sides' fees and rounded tax.
    assert w.grad.item() == pytest.approx(19912 / 200088, rel=1e-6)


@pytest.mark.parametrize("no_exit", [False, True])
def test_flat_cash_is_not_pushed_into_two_losing_or_trapped_baskets(no_exit):
    tape = profitable_tape()
    if no_exit:
        tape[..., 13:] = 0
    else:
        # The tiny price move cannot cover either round trip's fixed costs.
        bars = tape[..., 3:].reshape(1, 1, 2, 12, 5)
        bars[0, 0, 0, 6, :4] = 100.01
    w = torch.zeros(1, 1, requires_grad=True)
    execute(w, tape).strategy_returns.sum().backward()
    assert w.grad.item() == 0


def test_flat_policy_learns_the_profitable_short_after_costs():
    tape = profitable_tape()
    bars = tape[..., 3:].reshape(1, 1, 2, 12, 5)
    bars[0, 0, 0, 6, :4] = 90
    w = torch.zeros(1, 1, requires_grad=True)
    execute(w, tape).strategy_returns.sum().backward()
    assert w.grad.item() == pytest.approx(-19912 / 200088, rel=1e-6)


def test_cash_brackets_do_not_use_future_exit_prices_or_capacity():
    tape = profitable_tape()[0]
    altered = tape.clone()
    altered[..., 13:] = 999
    w = torch.tensor([.25])
    a = _scheduled_cash_bracket(w, tape, torch.tensor(1e6))
    b = _scheduled_cash_bracket(w, altered, torch.tensor(1e6))
    for x, y in zip(a, b):
        torch.testing.assert_close(x, y, rtol=0, atol=0)


def test_learning_can_leave_flat_cash_and_improve_actual_integer_return():
    weight = torch.nn.Parameter(torch.zeros(1, 1))
    optimizer = torch.optim.SGD([weight], lr=.1)
    tape = profitable_tape()
    for _ in range(25):
        optimizer.zero_grad()
        result = execute(weight, tape)
        (-result.strategy_returns.sum()).backward()
        optimizer.step()
    with torch.no_grad():
        evaluation = execute(weight, tape, recovery=False)
    assert evaluation.strategy_returns.item() > 0
    assert evaluation.contract_quantities_history.sum().item() == 1
    assert evaluation.final_alive


@pytest.mark.parametrize("direction", [-1, 1])
def test_unavailable_standard_does_not_dilute_the_mini_gradient(direction):
    tape = profitable_tape()
    tape[:, :, 1] = tape[:, :, 0]
    tape[:, :, 1, 0] = 100
    tape[:, :, 0, 7] = 0  # Standard has no entry capacity.
    w = torch.tensor([[direction * .0001]], requires_grad=True)
    r = execute(w, tape)
    r.strategy_returns.sum().backward()
    assert r.strategy_returns.item() == 0
    assert w.grad.item() == pytest.approx((1000 - direction * 80) / 10080, rel=1e-6)


def test_recovery_keeps_exact_dead_tail_but_learns_its_executable_opportunities():
    tape = profitable_tape().repeat(3, 1, 1, 1)
    tape[0, ..., 13:] = 0
    w = torch.tensor([[.25], [.0001], [-.0001]], requires_grad=True)
    old = execute(w, tape, recovery=False)
    new = execute(w, tape)
    for f in fields(old):
        torch.testing.assert_close(getattr(new, f.name), getattr(old, f.name), rtol=0, atol=0)
    new.strategy_returns.sum().backward()
    assert not new.final_alive
    assert new.default_history.tolist() == [True, False, False]
    assert not new.contract_quantities_history[1:].any()
    assert w.grad[0].item() < 0  # Discourage the trapped long.
    assert w.grad[1].item() == pytest.approx(19912 / 200088, rel=1e-6)
    assert w.grad[2].item() == pytest.approx(20088 / 200088, rel=1e-6)


def test_dead_state_across_batch_boundary_has_same_values_and_gradients():
    tape = profitable_tape().repeat(4, 1, 1, 1)
    tape[0, ..., 13:] = 0
    initial = torch.tensor([[.25], [.0001], [-.0001], [.0002]])
    whole_w = initial.clone().requires_grad_()
    whole = execute(whole_w, tape)
    whole.strategy_returns.sum().backward()
    chunk_w = initial.clone().requires_grad_()
    first = execute(chunk_w[:2], tape[:2])
    second = execute(chunk_w[2:], tape[2:], initial_alive=first.final_alive,
                     initial_equity_scale=first.final_equity_scale.detach())
    torch.cat([first.strategy_returns, second.strategy_returns]).sum().backward()
    torch.testing.assert_close(whole_w.grad, chunk_w.grad, rtol=0, atol=0)
    torch.testing.assert_close(whole.strategy_returns, torch.cat([first.strategy_returns, second.strategy_returns]))


def test_nonadvancing_rows_and_unfillable_entries_have_no_shadow_gradient():
    tape = profitable_tape().repeat(3, 1, 1, 1)
    tape[2, ..., 7] = 0
    w = torch.tensor([[.0001], [.0001], [.0001]], requires_grad=True)
    result = execute(w, tape, state_advance_mask=torch.tensor([True, False, True]))
    result.strategy_returns.sum().backward()
    assert w.grad[0].item() > 0
    assert w.grad[1:].count_nonzero() == 0


@pytest.mark.parametrize("direction", [-1, 1])
def test_basket_secant_accounts_for_capacity_and_partial_exit(direction):
    tape = profitable_tape()
    bars = tape[..., 3:].reshape(1, 1, 2, 12, 5)
    bars[0, 0, 0, 6, 4] = 1
    adverse = 80. if direction == 1 else 150.
    bars[0, 0, 0, 7] = torch.tensor([adverse, adverse, adverse, adverse, 1.])
    w = torch.tensor([[direction * .25]], requires_grad=True)
    # The second whole contract is the marginal basket change. Its entire fill
    # occurs at the second market price, not a half-gradient at the first cap.
    first_profit = direction * 20000 - 88
    second_tax = int(adverse * 2000 * .00002 + .5)
    second_profit = direction * (adverse - 100) * 2000 - 84 - second_tax
    slope = direction * second_profit / 200088 / (1 + first_profit / 1e6)
    result = execute(w, tape)
    result.strategy_returns.sum().backward()
    assert w.grad.item() == pytest.approx(slope, rel=1e-6)
    assert direction * w.grad.item() < 0  # The next contract exits at an adverse price.


def test_inference_has_no_recovery_and_new_config_keeps_1330_contract():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _trading_checkpoint_contract
    config = load_config("configs/markets/tw_stock_futures_day_trade_0845_gradient_v3.yaml")
    contract = _trading_checkpoint_contract(config)["taiwan_stock_futures_day_trade"]
    assert config.training.epochs == 1000
    assert config.training.futures_portfolio_optimizer_step_per_trajectory
    assert contract["day_trade_margin_discount_eligible"] is False
    assert contract["deadline_basis"] == "user_required_1330_flat_strategy_constraint"
    assert "1330" in contract["execution_clock"]
    tape = profitable_tape().repeat(2, 1, 1, 1)
    tape[0, ..., 13:] = 0
    with torch.no_grad():
        result = execute(torch.tensor([[.25], [.25]]), tape)
    assert not result.final_alive and not result.contract_quantities_history[1:].any()


@pytest.mark.parametrize("setting, message", [
    ("futures_portfolio_training_surrogate_only: true", "exact integer forward"),
    ("futures_portfolio_optimizer_step_per_trajectory: false", "full-trajectory"),
])
def test_recovery_config_rejects_incompatible_account_and_cadence(tmp_path, setting, message):
    from stockagent.config import load_config
    base = Path("configs/markets/tw_stock_futures_day_trade_0845_gradient_v3.yaml").resolve()
    path = tmp_path / "invalid.yaml"
    path.write_text(f"base_config: {base}\ntraining:\n  {setting}\n")
    with pytest.raises(ValueError, match=message):
        load_config(path)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
@pytest.mark.filterwarnings("ignore:The .grad attribute of a Tensor that is not a leaf Tensor is being accessed:UserWarning")
def test_compiled_recovery_matches_eager_across_default_and_zero_actions():
    tape = profitable_tape().repeat(3, 4, 1, 1).cuda()
    tape[0, ..., 13:] = 0
    values, gradients = [], []
    for compiled in (False, True):
        w = torch.tensor([[.25, 0, -.0001, .0001]] * 3, device="cuda", requires_grad=True)
        r = run_tw_stock_futures_day_trade_integer_torch(
            w, tape, initial_capital=1e6, scheduled_events=True,
            recoverable_backward=True, use_compile=compiled,
        )
        r.strategy_returns.sum().backward()
        assert torch.isfinite(w.grad).all()
        assert (w.grad[1:] != 0).all()
        values.append(r)
        gradients.append(w.grad)
    for f in fields(values[0]):
        torch.testing.assert_close(getattr(values[0], f.name), getattr(values[1], f.name))
    torch.testing.assert_close(gradients[0], gradients[1], rtol=1e-5, atol=1e-7)
