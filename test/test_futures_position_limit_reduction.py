"""A position-limit reduction is not a margin call to flatten the account."""
import os

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch as run
from stockagent.data import tw_futures_margin as m
from test_tw_futures_margin import tape


@pytest.mark.parametrize('sign', [-1., 1.])
@pytest.mark.parametrize('capacity,remaining,alive', [(0, 5, False), (1, 4, False), (2, 3, True), (100, 3, True)])
def test_reduce_only_to_limit_and_default_only_if_still_over(sign, capacity, remaining, alive):
    x = tape()
    x[..., m.POSITION_LIMIT] = 3
    x[..., 8] = capacity
    r = run(torch.tensor([[sign * .5]]), x, initial_capital=1000.,
            initial_quantities=torch.tensor([sign * 5]))
    assert r.residual_contract_quantities_history.item() == sign * remaining
    assert bool(r.final_alive) is alive
    assert r.default_reason_history.item() == (0 if alive else 4)
    assert r.turnovers.item() == pytest.approx(5 - remaining)
    if alive:
        assert r.final_equity_scale == 1
        assert r.margin_audit_history[0, 10] == 0


def test_closed_group_and_unrelated_positions_are_not_liquidated():
    x = tape(slots=3)
    x[..., m.POSITION_GROUP] = torch.tensor([0., 0., 1.])
    x[..., m.POSITION_UNIT] = torch.tensor([1., .25, 1.])
    x[..., m.POSITION_LIMIT] = torch.tensor([3., 3., 20.])
    x[:, 0, 8] = 0  # Two locked standard contracts. Only minis can reduce.
    r = run(torch.tensor([[.2, 0., .1]]), x, initial_capital=2000.,
            initial_quantities=torch.tensor([2., 8., 2.]))
    assert r.final_alive
    assert r.final_weights.tolist() == [2., 0., 2.]
    assert r.margin_audit_history[0, 10] == 0


def test_mandatory_reduction_uses_available_denomination_and_integer_rounding():
    x = tape(slots=2)
    x[..., m.POSITION_GROUP] = 0
    x[..., m.POSITION_UNIT] = torch.tensor([1., .25])
    x[..., m.POSITION_LIMIT] = 3
    x[:, 0, 8] = 0
    r = run(torch.tensor([[.1, .4]]), x, initial_capital=2000.,
            initial_quantities=torch.tensor([2., 8.]))
    assert r.final_alive
    assert r.final_weights.tolist() == [2., 4.]
    assert (r.final_weights.abs() * x[0, :, m.POSITION_UNIT]).sum() == 3


def test_forbidden_close_still_fails_closed():
    x = tape()
    x[..., m.POSITION_LIMIT] = 3
    x[..., m.CAN_SELL] = 0
    r = run(torch.tensor([[.5]]), x, initial_capital=1000., initial_quantities=torch.tensor([5.]))
    assert not r.final_alive
    assert r.default_reason_history.item() == 4
    assert r.residual_contract_quantities_history.item() == 5


def test_corrected_accounting_rejects_old_artifact_and_optimizer(monkeypatch, tmp_path):
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest, _validate_checkpoint_manifest
    from test_tw_stock_context_futures_portfolio import _stock_panel
    c = load_config('configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_absorbing_gradient_v12.yaml')
    panel = _stock_panel()
    current = build_checkpoint_manifest(panel, c, include_data_content=False)
    with monkeypatch.context() as patch:
        patch.setattr(m, 'MARGIN_ACCOUNTING_CONTRACT_VERSION', 1)
        previous = build_checkpoint_manifest(panel, c, include_data_content=False)
    for scope in ('resume', 'artifact'):
        with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
            _validate_checkpoint_manifest({'experiment_manifest': previous}, current,
                checkpoint_path=tmp_path/'v11.pt', scope=scope)
    # Model-only transfer is still supported, but is not historical replay.
    _validate_checkpoint_manifest({'experiment_manifest': previous}, current,
        checkpoint_path=tmp_path/'v11.pt', scope='model')


@pytest.mark.skipif(
    os.environ.get('STOCKAGENT_TEST_CUDA_GRAPH') != '1' or not torch.cuda.is_available(),
    reason='explicit CUDA graph acceptance run required',
)
def test_cuda_graph_replays_changed_limits_without_a_false_default(monkeypatch):
    from stockagent.backtest.futures_cuda_graph import clear_futures_cuda_graph_cache
    clear_futures_cuda_graph_cache()
    monkeypatch.setenv('STOCKAGENT_BACKTEST_COMPILE', '0')
    monkeypatch.setenv('STOCKAGENT_FUTURES_FUNDING_COMPILE', '1')
    for limit, alive in ((3, True), (1, False), (3, True)):
        outputs, gradients = [], []
        x = tape(2).cuda()
        x[1, :, m.POSITION_LIMIT] = limit
        x[1, :, 8] = 2
        for graph in (False, True):
            monkeypatch.setenv('STOCKAGENT_FUTURES_CUDA_GRAPH', str(int(graph)))
            w = torch.full((2, 1), .5, device='cuda', requires_grad=True)
            r = run(w, x, initial_capital=1000., recoverable_backward=True)
            r.strategy_returns.sum().backward()
            outputs.append(r.strategy_returns.detach().clone())
            gradients.append(w.grad.clone())
            assert bool(r.final_alive) is alive
            assert r.residual_contract_quantities_history[-1].item() == 3
        torch.testing.assert_close(*outputs, rtol=0, atol=0)
        torch.testing.assert_close(*gradients, rtol=0, atol=0)
    clear_futures_cuda_graph_cache()
