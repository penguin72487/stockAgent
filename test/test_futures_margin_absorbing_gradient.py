"""A stopped account cannot earn a policy gradient from future market returns."""
import os

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import (
    run_tw_futures_portfolio_integer_torch as run,
)
from stockagent.data import tw_futures_margin as m
from test_tw_futures_margin import tape


def failed_margin_close(rows, sign=1., future_move=100.):
    x = tape(rows)
    x[0, :, 4] = 1000 - sign * 140
    x[1, :, 3:5] = 1000 - sign * 130
    x[1, :, m.PREVIOUS_MARK] = x[0, :, 4]
    x[1, :, m.CAN_SELL if sign > 0 else m.CAN_BUY] = 0
    x[2:, :, 4] += sign * future_move
    return x


@pytest.mark.parametrize('sign', [-1., 1.])
@pytest.mark.parametrize('future_move', [-100., 100.])
def test_post_default_market_cannot_reverse_pre_default_learning(sign, future_move):
    # A shared action parameter loses 700/1000, breaches maintenance, then
    # cannot close. Appending 126 untradeable-in-time days cannot alter its
    # reward or gradient. Re-capitalizing a backward-only shadow used to let
    # those future profits reverse the direction learned from the failure.
    x = failed_margin_close(128, sign, future_move)
    prefix = torch.full((2, 1), sign * .5, requires_grad=True)
    full = torch.full((128, 1), sign * .5, requires_grad=True)
    a = run(prefix, x[:2], initial_capital=1000., recoverable_backward=True)
    b = run(full, x, initial_capital=1000., recoverable_backward=True)
    a.strategy_returns.sum().backward()
    b.strategy_returns.sum().backward()
    assert b.default_reason_history[:2].tolist() == [0, 4]
    assert not b.final_alive
    torch.testing.assert_close(a.strategy_returns, b.strategy_returns[:2], rtol=0, atol=0)
    assert b.strategy_returns[2:].count_nonzero() == 0
    torch.testing.assert_close(prefix.grad, full.grad[:2], rtol=0, atol=0)
    torch.testing.assert_close(full.grad[2:], torch.zeros_like(full.grad[2:]), rtol=0, atol=0)
    assert full.grad.sum().item() * sign < 0


@pytest.mark.parametrize('recover', [False, True])
def test_absorbing_gradient_survives_public_batch_boundary(recover):
    x = failed_margin_close(4)
    prefix = run(torch.full((2, 1), .5), x[:2], initial_capital=1000.)
    assert not prefix.final_alive
    weights = torch.full((2, 1), .5, requires_grad=True)
    tail = run(weights, x[2:], initial_capital=1000.,
               initial_quantities=prefix.final_weights,
               initial_equity_scale=prefix.final_equity_scale,
               initial_alive=prefix.final_alive,
               recoverable_backward=recover)
    # This also tests denominator stability when starting with zero NAV.
    (tail.strategy_returns.sum() + tail.turnovers.sum()
     + tail.weights_history.sum()).backward()
    assert torch.isfinite(weights.grad).all()
    torch.testing.assert_close(weights.grad, torch.zeros_like(weights), rtol=0, atol=0)
    assert not tail.final_alive
    assert tail.final_equity_scale == 0


@pytest.mark.skipif(
    os.environ.get('STOCKAGENT_TEST_CUDA_GRAPH') != '1' or not torch.cuda.is_available(),
    reason='explicit CUDA graph acceptance run required',
)
def test_cuda_graph_replay_retains_absorbing_default(monkeypatch):
    from stockagent.backtest.futures_cuda_graph import clear_futures_cuda_graph_cache
    clear_futures_cuda_graph_cache()
    monkeypatch.setenv('STOCKAGENT_BACKTEST_COMPILE', '0')
    monkeypatch.setenv('STOCKAGENT_FUTURES_FUNDING_COMPILE', '1')
    for initially_alive in (True, False):
        outputs, gradients = [], []
        x = failed_margin_close(8).cuda()
        for graph in (False, True):
            monkeypatch.setenv('STOCKAGENT_FUTURES_CUDA_GRAPH', str(int(graph)))
            w = torch.full((8, 1), .5, device='cuda', requires_grad=True)
            r = run(w, x, initial_capital=1000., recoverable_backward=True,
                    initial_alive=torch.tensor(initially_alive, device='cuda'),
                    initial_equity_scale=torch.tensor(float(initially_alive), device='cuda'))
            r.strategy_returns.sum().backward()
            outputs.append(r.strategy_returns.detach().clone())
            gradients.append(w.grad.clone())
        torch.testing.assert_close(*outputs, rtol=0, atol=0)
        torch.testing.assert_close(*gradients, rtol=0, atol=0)
        after = gradients[1][2 if initially_alive else 0:]
        torch.testing.assert_close(after, torch.zeros_like(after), rtol=0, atol=0)
    clear_futures_cuda_graph_cache()
