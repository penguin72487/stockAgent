"""Whole-contract STE must not differentiate through hard execution ceilings."""
import os

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch as run
from stockagent.data import tw_futures_margin as m
from test_tw_futures_margin import tape


def rising_tape(rows, sign=1., constraint='position'):
    x = tape(rows)
    opening = 10000 + sign * torch.arange(rows) * 10
    x[:, 0, 3] = x[:, 0, m.PREVIOUS_MARK] = opening
    x[:, 0, 4] = x[:, 0, m.TERMINAL_MARK] = opening + sign * 10
    if constraint == 'position':
        x[..., m.POSITION_LIMIT] = 5
    else:
        x[..., 8] = 1
    return x


@pytest.mark.parametrize('constraint', ['position', 'capacity'])
@pytest.mark.parametrize('sign', [-1., 1.])
@pytest.mark.parametrize('rows', [8, 32, 128])
def test_saturated_action_has_no_fictitious_compounding_gradient(constraint, sign, rows):
    x = rising_tape(rows, sign, constraint)
    w = torch.full((rows, 1), sign * .9, requires_grad=True)
    r = run(w, x, initial_capital=1000., recoverable_backward=True)
    r.strategy_returns.sum().backward()
    assert r.final_alive
    lo = run(torch.full_like(w, sign * .89), x, initial_capital=1000.)
    hi = run(torch.full_like(w, sign * .91), x, initial_capital=1000.)
    torch.testing.assert_close(lo.strategy_returns, hi.strategy_returns, rtol=0, atol=0)
    # This zero is the continuous feasible action's local derivative, not a
    # claim that all piecewise-constant integer derivatives should be used.
    assert abs(w.grad.sum().item()) < 1e-4
    assert w.grad.abs().max().item() < 1e-5


@pytest.mark.parametrize('weight', [-.02, 0., .02])
def test_sub_contract_actions_keep_a_nonzero_first_contract_gradient(weight):
    x = tape()
    x[..., 4] += 10
    x[..., m.POSITION_LIMIT] = 5
    w = torch.tensor([[weight]], requires_grad=True)
    r = run(w, x, initial_capital=1000., recoverable_backward=False)
    assert r.contract_quantities_history.item() == 0
    r.strategy_returns.sum().backward()
    assert w.grad.item() == pytest.approx(.1, rel=1e-5)


def test_binding_group_limit_preserves_relative_allocation_learning():
    x = tape(slots=2)
    x[..., m.POSITION_GROUP] = 0
    x[..., m.POSITION_LIMIT] = 5
    x[0, :, 4] += torch.tensor([10., -10.])
    w = torch.tensor([[.4, .4]], requires_grad=True)
    r = run(w, x, initial_capital=1000., recoverable_backward=False)
    r.strategy_returns.sum().backward()
    # Continuous projection: q = 5*w/sum(w); its budget-radial derivative is
    # zero, but rotating the allocation toward the profitable contract works.
    torch.testing.assert_close(w.grad, torch.tensor([[.0625, -.0625]]), rtol=1e-5, atol=1e-7)


@pytest.mark.skipif(os.environ.get('STOCKAGENT_TEST_CUDA_GRAPH') != '1'
                   or not torch.cuda.is_available(), reason='explicit CUDA acceptance required')
@pytest.mark.parametrize('constraint', ['position', 'capacity'])
def test_saturation_cuda_graph_preserves_forward_and_feasible_gradient(monkeypatch, constraint):
    from test_futures_cuda_graph import assert_result_equal
    from stockagent.backtest.futures_cuda_graph import clear_futures_cuda_graph_cache
    clear_futures_cuda_graph_cache()
    x = rising_tape(32, constraint=constraint).cuda()
    results, gradients = [], []
    for graph in (False, True):
        monkeypatch.setenv('STOCKAGENT_BACKTEST_COMPILE', '0')
        monkeypatch.setenv('STOCKAGENT_FUTURES_CUDA_GRAPH', str(int(graph)))
        w = torch.full((32, 1), .9, device='cuda', requires_grad=True)
        result = run(w, x, initial_capital=1000., recoverable_backward=True)
        result.strategy_returns.sum().backward()
        results.append(result)
        gradients.append(w.grad.clone())
    assert_result_equal(*results)
    torch.testing.assert_close(*gradients, rtol=0, atol=0)
    assert gradients[0].abs().max() < 1e-5
