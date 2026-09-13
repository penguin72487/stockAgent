"""Feasible inward sensitivity at an exhausted entry capacity boundary."""
from dataclasses import fields
from pathlib import Path

import pytest
import torch

from stockagent.backtest.tw_stock_futures_day_trade import run_tw_stock_futures_day_trade_integer_torch
from test_tw_stock_futures_minute_gradients import profitable_tape


def execute(w, tape, *, enabled=True, compiled=False, **kwargs):
    return run_tw_stock_futures_day_trade_integer_torch(
        w, tape, initial_capital=1e6, scheduled_events=True,
        recoverable_backward=True, saturation_recovery=enabled,
        use_compile=compiled, **kwargs,
    )


@pytest.mark.parametrize('direction', [-1., 1.])
@pytest.mark.parametrize('budget', [.200088, .25, .9])
def test_trapped_last_entry_contract_has_removal_gradient(direction, budget):
    tape=profitable_tape()
    tape[...,7]=1
    tape[...,13:]=0
    old_w=torch.tensor([[direction*budget]],requires_grad=True)
    w=old_w.detach().clone().requires_grad_()
    old=execute(old_w,tape,enabled=False); result=execute(w,tape)
    for field in fields(old):
        torch.testing.assert_close(getattr(old,field.name),getattr(result,field.name),rtol=0,atol=0)
    old.strategy_returns.sum().backward(); result.strategy_returns.sum().backward()
    assert old_w.grad.item()==0
    assert direction*w.grad.item()<0
    assert w.grad.item()==pytest.approx(-direction*200044/200088,rel=1e-6)
    assert not result.final_alive


@pytest.mark.parametrize('direction', [-1., 1.])
@pytest.mark.parametrize('profitable', [False,True])
def test_capacity_boundary_only_allows_inward_improvement(direction,profitable):
    tape=profitable_tape(); tape[...,7]=1
    bars=tape[...,3:].reshape(1,1,2,12,5)
    bars[0,0,0,6,:4]=100+(10 if profitable else -10)*direction
    w=torch.tensor([[direction*.9]],requires_grad=True)
    r=execute(w,tape); r.strategy_returns.sum().backward()
    assert r.final_alive
    if profitable: assert w.grad.item()==0
    else: assert direction*w.grad.item()<0


def test_partial_exit_last_contract_gradient_uses_marginal_residual():
    tape=profitable_tape(); tape[...,7]=2
    bars=tape[...,3:].reshape(1,1,2,12,5); bars[0,0,0,6,4]=1
    w=torch.tensor([[.5]],requires_grad=True)
    r=execute(w,tape); r.strategy_returns.sum().backward()
    # First contract exits profitably; the second has only its entry cost and
    # a full residual. An average payoff could incorrectly hide this failure.
    expected=(-44/(1+19868/1e6)-200000)/200088
    assert w.grad.item()==pytest.approx(expected,rel=1e-6)
    assert r.residual_contract_quantities_history.sum()==1


def test_optimizer_can_escape_a_saturated_execution_failure():
    tape=profitable_tape(); tape[...,7]=1; tape[...,13:]=0
    w=torch.nn.Parameter(torch.tensor([[.25]])); opt=torch.optim.SGD([w],lr=.1)
    opt.zero_grad(); (-execute(w,tape).strategy_returns.sum()).backward(); opt.step()
    with torch.no_grad(): result=execute(w,tape)
    assert result.final_alive and not result.contract_quantities_history.any()


def test_new_backward_does_not_change_a_cash_or_padded_row():
    tape=profitable_tape().repeat(2,1,1,1); tape[...,7]=1; tape[...,13:]=0
    w=torch.tensor([[0.],[.25]],requires_grad=True)
    r=execute(w,tape,state_advance_mask=torch.tensor([True,False]))
    r.strategy_returns.sum().backward()
    assert not w.grad.any()


def test_common_risk_loss_passes_the_boundary_setting_to_the_integer_executor():
    from stockagent.training.loss import risk_aware_loss
    tape=profitable_tape(); tape[...,7]=1; tape[...,13:]=0
    values=[]; gradients=[]
    for enabled in [False,True]:
        w=torch.tensor([[.25]],requires_grad=True)
        loss=risk_aware_loss(w,torch.zeros_like(w),torch.ones_like(w,dtype=torch.bool),
            benchmark_returns=torch.zeros(1),long_only=False,portfolio_activation='pre_normalized',
            execution_mode='tw_stock_futures_day_trade_0845_minute',objective='log_utility',
            gamma_turnover=0.,direction_weight=0.,volatility_regime_weight=0.,concentration_weight=0.,
            overnight_log_returns=tape,futures_portfolio_recoverable_backward=True,
            futures_minute_saturation_recovery=enabled)
        loss.backward(); values.append(loss.detach()); gradients.append(w.grad.item())
    torch.testing.assert_close(*values,rtol=0,atol=0)
    assert gradients[0]==0 and gradients[1]>0


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires CUDA')
@pytest.mark.filterwarnings('ignore:The .grad attribute of a Tensor that is not a leaf Tensor is being accessed:UserWarning')
def test_saturated_eager_compiled_values_and_gradients_match():
    tape=profitable_tape().repeat(2,4,1,1).cuda(); tape[...,7]=1; tape[0,...,13:]=0
    outputs=[]; gradients=[]
    for compiled in [False,True]:
        w=torch.tensor([[.25,-.25,0.,.0001]]*2,device='cuda',requires_grad=True)
        result=execute(w,tape,compiled=compiled)
        result.strategy_returns.sum().backward()
        outputs.append(result); gradients.append(w.grad)
    for field in fields(outputs[0]):
        torch.testing.assert_close(getattr(outputs[0],field.name),getattr(outputs[1],field.name))
    torch.testing.assert_close(*gradients,rtol=1e-5,atol=1e-7)


def test_v6_changes_only_backward_contract_and_preserves_v5(tmp_path):
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _trading_checkpoint_contract
    old=load_config('configs/markets/tw_stock_futures_day_trade_0845_capacity_ceil_v5.yaml')
    new=load_config('configs/markets/tw_stock_futures_day_trade_0845_gradient_v6.yaml')
    assert old.training.epochs==new.training.epochs==10000
    assert not old.training.futures_minute_saturation_recovery
    assert new.training.futures_minute_saturation_recovery
    a=_trading_checkpoint_contract(old); b=_trading_checkpoint_contract(new)
    assert a!=b
    b['taiwan_stock_futures_day_trade']['gradient_contract']=a['taiwan_stock_futures_day_trade']['gradient_contract']
    del b['taiwan_stock_futures_day_trade']['gradient_capacity_boundary']
    assert a==b
    from test_checkpoint_manifest import _panel
    from stockagent.training.checkpoint_contract import _checkpoint_manifest, _validate_checkpoint_manifest
    old_manifest=_checkpoint_manifest(_panel(),old)
    new_manifest=_checkpoint_manifest(_panel(),new)
    assert old_manifest['fingerprints']['model']==new_manifest['fingerprints']['model']
    with pytest.raises(RuntimeError,match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest':old_manifest},new_manifest,
            checkpoint_path=tmp_path/'v5.pt',scope='resume')
    base=Path('configs/markets/tw_stock_futures_day_trade_0845_gradient_v6.yaml').resolve()
    path=tmp_path/'bad.yaml'
    path.write_text(f'base_config: {base}\ntraining:\n  futures_portfolio_recoverable_backward: false\n')
    with pytest.raises(ValueError,match='minute recoverable backward'): load_config(path)
