"""Finite basket utility search keeps the exact failure value and optima."""
import math
from dataclasses import fields
import pytest
import torch
from stockagent.backtest.tw_stock_futures_day_trade import run_tw_stock_futures_day_trade_integer_torch
from test_tw_stock_futures_minute_gradients import profitable_tape


def execute(w,tape,*,objective='execution_utility',compiled=False,**kwargs):
    return run_tw_stock_futures_day_trade_integer_torch(w,tape,initial_capital=1e6,
        scheduled_events=True,recoverable_backward=True,saturation_recovery=True,
        recovery_objective=objective,use_compile=compiled,**kwargs)


@pytest.mark.parametrize('direction',[-1.,1.])
@pytest.mark.parametrize('cap',[1,2,20])
def test_actual_failure_cost_survives_saturation_and_adjacent_failed_baskets(direction,cap):
    tape=profitable_tape(); tape[...,7]=cap; tape[...,13:]=0
    w=torch.tensor([[direction*.5]],requires_grad=True)
    result=execute(w,tape); result.strategy_returns.sum().backward()
    q=int(result.contract_quantities_history.abs().sum())
    assert not result.final_alive
    assert w.grad.item()==pytest.approx(direction*math.log(1e-7)/(q*200088/1e6),rel=1e-6)


def test_profitable_one_contract_optimum_is_not_pushed_to_cash_by_bad_second_contract():
    tape=profitable_tape(); tape[...,7]=2
    bars=tape[...,3:].reshape(1,1,2,12,5); bars[0,0,0,6,4]=1
    gradients=[]
    for objective in ['residual_notional','execution_utility']:
        w=torch.tensor([[.25]],requires_grad=True)
        r=execute(w,tape,objective=objective); r.strategy_returns.sum().backward()
        assert r.final_alive and r.strategy_returns.item()>0
        gradients.append(w.grad.item())
    assert gradients[0]<0 and gradients[1]==0


@pytest.mark.parametrize('start',[-.001,0.,.001])
def test_cash_can_find_profitable_direction_without_cancelling_costs(start):
    tape=profitable_tape(); tape[...,7]=1
    w=torch.tensor([[start]],requires_grad=True)
    execute(w,tape).strategy_returns.sum().backward()
    assert w.grad.item()>0
    bars=tape[...,3:].reshape(1,1,2,12,5); bars[0,0,0,6,:4]=100.01
    w=torch.tensor([[start]],requires_grad=True)
    execute(w,tape).strategy_returns.sum().backward()
    assert w.grad.item()==0


def test_learning_reaches_and_retains_an_actual_one_contract_optimum():
    tape=profitable_tape(); tape[...,7]=2
    bars=tape[...,3:].reshape(1,1,2,12,5); bars[0,0,0,6,4]=1
    w=torch.nn.Parameter(torch.zeros(1,1)); opt=torch.optim.SGD([w],lr=.1)
    for _ in range(80):
        opt.zero_grad(); (-execute(w,tape).strategy_returns.sum()).backward(); opt.step()
    with torch.no_grad(): result=execute(w,tape)
    assert result.final_alive and result.contract_quantities_history.sum()==1
    assert result.strategy_returns.item()>0


def test_multiple_failed_coordinates_each_receive_a_restoration_direction():
    tape=profitable_tape().repeat(2,3,1,1); tape[0,...,13:]=0
    w=torch.tensor([[.25,-.25,.25],[.001,.001,.001]],requires_grad=True)
    new=execute(w,tape)
    old=execute(w.detach(),tape,objective='residual_notional')
    for f in fields(old): torch.testing.assert_close(getattr(old,f.name),getattr(new,f.name),rtol=0,atol=0)
    new.strategy_returns.sum().backward()
    assert (w[0]*w.grad[0]<0).all()
    assert (w.grad[1]>0).all()


def test_random_standard_mini_accounts_keep_all_forward_histories():
    generator=torch.Generator().manual_seed(93)
    tape=profitable_tape().repeat(16,7,1,1)
    tape[:,:,1]=tape[:,:,0]; tape[:,:,1,0]=100.
    tape[...,7]=torch.randint(0,5,(16,7,2),generator=generator).float()
    bars=tape[...,3:].reshape(16,7,2,12,5)
    bars[...,6,4]=torch.randint(0,5,(16,7,2),generator=generator).float()
    prices=80+40*torch.rand((16,7,2),generator=generator)
    bars[...,6,:4]=prices[...,None]
    w=torch.randn((16,7),generator=generator)
    w=(.9*w/w.abs().sum(-1,keepdim=True)).requires_grad_()
    advance=torch.ones(16,dtype=torch.bool); advance[::5]=False
    old=execute(w.detach(),tape,objective='residual_notional',state_advance_mask=advance)
    new=execute(w,tape,state_advance_mask=advance)
    for f in fields(old): torch.testing.assert_close(getattr(old,f.name),getattr(new,f.name),rtol=0,atol=0)
    new.strategy_returns.sum().backward()
    assert torch.isfinite(w.grad).all() and not w.grad[~advance].any()
    with torch.no_grad(): inference=execute(w,tape,state_advance_mask=advance)
    for f in fields(old): torch.testing.assert_close(getattr(old,f.name),getattr(inference,f.name),rtol=0,atol=0)


def test_utility_option_reaches_common_loss_and_binds_resume_contract(tmp_path):
    from stockagent.training.loss import risk_aware_loss
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _checkpoint_manifest, _validate_checkpoint_manifest
    from test_checkpoint_manifest import _panel
    tape=profitable_tape(); tape[...,7]=1; tape[...,13:]=0
    w=torch.tensor([[.25]],requires_grad=True)
    loss=risk_aware_loss(w,torch.zeros_like(w),torch.ones_like(w,dtype=torch.bool),
        benchmark_returns=torch.zeros(1),long_only=False,portfolio_activation='pre_normalized',
        execution_mode='tw_stock_futures_day_trade_0845_minute',objective='log_utility',
        gamma_turnover=0.,direction_weight=0.,volatility_regime_weight=0.,concentration_weight=0.,
        overnight_log_returns=tape,futures_portfolio_recoverable_backward=True,
        futures_minute_recovery_objective='execution_utility')
    loss.backward()
    assert w.grad.item()==pytest.approx(-252*math.log(1e-7)/.200088,rel=1e-6)
    old=load_config('configs/markets/tw_stock_futures_day_trade_0845_capacity_ceil_v5.yaml')
    new=load_config('configs/markets/tw_stock_futures_day_trade_0845_gradient_v7.yaml')
    assert new.training.epochs==10000
    a,b=(_checkpoint_manifest(_panel(),c) for c in (old,new))
    assert a['fingerprints']['data']==b['fingerprints']['data']
    assert a['fingerprints']['model']==b['fingerprints']['model']
    with pytest.raises(RuntimeError,match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest':a},b,checkpoint_path=tmp_path/'v5.pt',scope='resume')


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires CUDA')
@pytest.mark.filterwarnings('ignore:The .grad attribute of a Tensor that is not a leaf Tensor is being accessed:UserWarning')
def test_compiled_execution_utility_matches_eager():
    tape=profitable_tape().repeat(2,4,1,1).cuda(); tape[...,7]=1; tape[0,...,13:]=0
    out=[]; grad=[]
    for compiled in [False,True]:
        w=torch.tensor([[.25,-.25,0.,.001]]*2,device='cuda',requires_grad=True)
        result=execute(w,tape,compiled=compiled); result.strategy_returns.sum().backward()
        assert torch.isfinite(w.grad).all()
        out.append(result);grad.append(w.grad)
    for f in fields(out[0]): torch.testing.assert_close(getattr(out[0],f.name),getattr(out[1],f.name))
    torch.testing.assert_close(*grad,rtol=1e-5,atol=1e-6)
