"""Analytic gradient checks below the first whole-contract threshold."""
import math

import pytest
import torch

from stockagent.backtest.tw_stock_futures_day_trade import run_tw_stock_futures_day_trade_integer_torch
from stockagent.data.tw_stock_futures_minute import TAPE_FIELDS


def profitable_tape(*, exit_event=6, empty_capacity=False):
    tape = torch.zeros(1, 1, 2, TAPE_FIELDS)
    tape[0, 0, 0, :3] = torch.tensor([2000., 40., 0.00002])
    bars = tape[..., 3:].reshape(1, 1, 2, 12, 5)
    bars[0, 0, 0, 0] = torch.tensor([100., 100., 100., 100., 100.])
    bars[0, 0, 0, 1] = torch.tensor([105., 105., 105., 105., 100.])
    if empty_capacity:
        # Real prices with no executable contracts are also a no-op.
        bars[0, 0, 0, 2:] = torch.tensor([50., 50., 50., 50., 0.])
    bars[0, 0, 0, exit_event] = torch.tensor([110., 110., 110., 110., 100.])
    return tape


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("exit_event", [6, 11])
@pytest.mark.parametrize("empty_capacity", [False, True])
def test_subcontract_gradient_ignores_unfilled_minutes(direction, exit_event, empty_capacity):
    w = torch.tensor([[direction * 1e-4]], requires_grad=True)
    result = run_tw_stock_futures_day_trade_integer_torch(
        w, profitable_tape(exit_event=exit_event, empty_capacity=empty_capacity),
        initial_capital=1e6, scheduled_events=True, use_compile=False,
    )
    # Forward is a true cash account: this request cannot afford one contract.
    assert not result.contract_quantities_history.any()
    assert not result.residual_contract_quantities_history.any()
    assert result.strategy_returns.item() == 0
    assert result.final_alive
    result.strategy_returns.sum().backward()
    # d/dw [w * PnL_per_contract/reserve - |w| * fees_per_contract/reserve]
    # entry tax=4, exit tax=4, two commissions=80, reserve=200000+88.
    expected = (20000 - direction * 88) / 200088
    assert w.grad.item() == pytest.approx(expected, rel=1e-6)


@pytest.mark.parametrize("direction", [1, -1])
def test_entirely_unfillable_exit_preserves_residual_failure_gradient(direction):
    x = profitable_tape()
    x[..., 3 + 2 * 5:] = 0
    w = torch.tensor([[direction * .25]], requires_grad=True)
    result = run_tw_stock_futures_day_trade_integer_torch(
        w, x, initial_capital=1e6, scheduled_events=True, use_compile=False,
    )
    assert result.residual_contract_quantities_history[0, 0, 0].item() == direction
    assert not result.final_alive
    assert result.strategy_returns.item() == pytest.approx(math.log(1e-7))
    result.strategy_returns.sum().backward()
    # Missing exits must retain the entire residual sensitivity, not halve it
    # at each empty min(remaining, 0) event.
    assert w.grad.item() == pytest.approx(-direction * 200000 / 200088, rel=1e-6)


def test_no_entry_capacity_has_no_gradient_or_transaction():
    x = profitable_tape()
    x[..., 7] = 0
    w = torch.tensor([[1e-4]], requires_grad=True)
    result = run_tw_stock_futures_day_trade_integer_torch(
        w, x, initial_capital=1e6, scheduled_events=True, use_compile=False,
    )
    result.strategy_returns.sum().backward()
    assert result.strategy_returns.item() == 0 and w.grad.item() == 0


# Dynamo probes non-leaf .grad internally; pytest promotes its benign warning
# to an exception. Keep other warnings strict, including actual compile errors.
@pytest.mark.skipif(not torch.cuda.is_available(), reason="compiled minute executor requires CUDA")
@pytest.mark.filterwarnings("ignore:The .grad attribute of a Tensor that is not a leaf Tensor is being accessed:UserWarning")
def test_compiled_shadow_matches_eager_for_cash_and_whole_contracts():
    tape = profitable_tape(exit_event=11).expand(1, 4, 2, TAPE_FIELDS).contiguous().cuda()
    outputs, gradients = [], []
    for compiled in (False, True):
        w = torch.tensor([[1e-4, -1e-4, .25, -.25]], device="cuda", requires_grad=True)
        result = run_tw_stock_futures_day_trade_integer_torch(
            w, tape, initial_capital=1e6, scheduled_events=True, use_compile=compiled,
        )
        result.strategy_returns.sum().backward()
        assert torch.isfinite(w.grad).all() and (w.grad != 0).all()
        outputs.append(result)
        gradients.append(w.grad)
    for name in ("strategy_returns", "contract_quantities_history",
                 "residual_contract_quantities_history", "default_history", "final_equity_scale"):
        torch.testing.assert_close(getattr(outputs[0], name), getattr(outputs[1], name))
    torch.testing.assert_close(gradients[0], gradients[1], rtol=1e-5, atol=1e-7)
