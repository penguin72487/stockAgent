import numpy as np
import pytest
import torch

from stockagent.data.tw_stock_futures_minute import TAPE_FIELDS
from stockagent.data.tw_stock_futures_carry import CARRY_TAPE_FIELDS
from stockagent.backtest.tw_stock_futures_carry import run_tw_stock_futures_carry_torch as run


def tape(days=3, symbols=1, lanes=2):
    x=torch.zeros(days,symbols,lanes,CARRY_TAPE_FIELDS)
    x[:,:,0,0]=100; x[:,:,0,1]=40
    x[:,:,0,3:8]=torch.tensor([100.,100.,100.,100.,10.])
    x[:,:,0,TAPE_FIELDS:]=torch.tensor([1.,1.,1.,10000.,0.,0.,1.,0.,0.])
    return x


def bar(x, day, price, capacity, event=0, slot=0):
    x[day,:,slot,3+event*5:8+event*5]=torch.tensor([price,price,price,price,capacity])


def test_carry_same_target_charges_no_repeat_entry_fee_and_marks_gap():
    x=tape(2); bar(x,1,110,10); x[1,:,0,TAPE_FIELDS+3]=11000
    out=run(torch.tensor([[.11],[.12]]),x,initial_capital=100000.,use_compile=False)
    assert out.final_alive
    assert out.contract_quantities_history[:,0,0].tolist()==[1,1]
    assert out.residual_contract_quantities_history[:,0,0].tolist()==[1,1]
    assert out.equity_scale_history.tolist()==pytest.approx([.9996,1.0096],abs=1e-6)
    assert out.turnovers.tolist()==pytest.approx([.1,0.])


def test_next_day_cash_closes_only_existing_contract_and_accounts_gap():
    x=tape(2);bar(x,1,110,10);x[1,:,0,TAPE_FIELDS+3]=11000
    out=run(torch.tensor([[.11],[0.]]),x,initial_capital=100000.,use_compile=False)
    assert out.final_alive and not out.final_carry_state[...,0].any()
    assert out.equity_scale_history[-1]==pytest.approx(1.0092,abs=1e-6)
    assert out.turnovers[1]==pytest.approx(11000/99960)


def test_partial_reversal_consumes_one_shared_minute_capacity():
    x=tape(2);bar(x,1,100,1)
    out=run(torch.tensor([[.21],[-.21]]),x,initial_capital=100000.,use_compile=False)
    assert out.contract_quantities_history[:,0,0].tolist()==[2,1]
    assert out.residual_contract_quantities_history[:,0,0].tolist()==[2,1]
    assert out.turnovers[1]==pytest.approx(10000/99920)


def test_expiry_settles_official_value_not_daily_close():
    x=tape(2);x[1,0,0,TAPE_FIELDS+4]=12000;x[1,0,0,TAPE_FIELDS+5]=1
    out=run(torch.tensor([[.11],[.11]]),x,initial_capital=100000.,use_compile=False)
    assert out.final_alive and not out.final_carry_state[...,0].any()
    assert out.final_equity_scale==pytest.approx(1.0192,abs=1e-6)


@pytest.mark.parametrize('field,value,reason',[(TAPE_FIELDS,2.,1),(TAPE_FIELDS+6,0.,2),(TAPE_FIELDS+3,0.,3)])
def test_invalid_held_evidence_fails_without_erasing_inventory(field,value,reason):
    from stockagent.backtest.futures_data_validity import FuturesCarryDataError
    x=tape(2);x[1,0,0,field]=value
    with pytest.raises(FuturesCarryDataError) as error:
        run(torch.tensor([[.11],[.11]]),x,initial_capital=100000.,use_compile=False)
    assert error.value.evidence['row']==1
    out=run(torch.tensor([[.11],[.11]]),x,initial_capital=100000.,use_compile=False,diagnostic_only=True)
    assert torch.isnan(out.strategy_returns[-1])
    assert not out.final_alive
    assert out.default_reason_history[-1]==reason
    assert out.final_carry_state[0,0,0]==1
    assert out.final_equity_scale==pytest.approx(.9996,abs=1e-6)


def test_flat_exit_and_batch_continuation_match_full_path():
    x=tape(3);bar(x,1,110,10);x[1,:,0,TAPE_FIELDS+3]=11000
    bar(x,2,115,10);bar(x,2,120,1,event=6);x[2,:,0,TAPE_FIELDS+3]=12000
    w=torch.tensor([[.11],[.12],[.13]],requires_grad=True)
    full=run(w,x,initial_capital=100000.,use_compile=False)
    first=run(w[:1],x[:1],initial_capital=100000.,use_compile=False)
    rest=run(w[1:],x[1:],initial_capital=100000.,use_compile=False,
        initial_carry_state=first.final_carry_state, initial_equity_scale=first.final_equity_scale)
    torch.testing.assert_close(full.strategy_returns,torch.cat([first.strategy_returns,rest.strategy_returns]),rtol=0,atol=0)
    assert full.final_alive and not full.final_carry_state[...,0].any()
    full.strategy_returns.sum().backward()
    assert torch.isfinite(w.grad).all() and w.grad.abs().sum()>0


def test_old_contract_and_new_front_are_not_netted_across_physical_ids():
    x=tape(2,lanes=3)
    x[1,0,0,TAPE_FIELDS+2]=0;bar(x,1,100,0)
    x[1,0,2]=x[0,0,0]; x[1,0,2,TAPE_FIELDS]=2
    out=run(torch.tensor([[.11],[.11]]),x,initial_capital=100000.,use_compile=False)
    assert out.final_alive
    assert out.final_carry_state[0,:,0].tolist()==[1,0,1]


def test_padding_does_not_advance_inventory_or_mtm():
    x=tape(2);bar(x,1,200,10);x[1,0,0,TAPE_FIELDS+3]=20000
    out=run(torch.tensor([[.11],[.9]]),x,initial_capital=100000.,use_compile=False,
        state_advance_mask=torch.tensor([True,False]))
    assert out.final_alive and out.strategy_returns[-1]==0
    assert out.final_carry_state[0,0,1]==10000
    assert out.final_equity_scale==pytest.approx(.9996,abs=1e-6)


def test_common_loss_passes_carry_state_to_next_batch():
    from stockagent.training.loss import risk_aware_loss
    x=tape(2);bar(x,1,110,10);x[1,:,0,TAPE_FIELDS+3]=11000
    w=torch.tensor([[.11],[.12]],requires_grad=True)
    aux={}
    kwargs=dict(execution_mode='tw_stock_futures_day_trade_0845_minute',objective='log_utility',
        buy_fee_rate=0.,sell_fee_rate=0.,long_only=False,portfolio_activation='pre_normalized',
        day_trade_execution_initial_capital=100000.,gamma_sharpe=0.,gamma_excess=0.,gamma_cvar=0.,
        gamma_drawdown=0.,gamma_turnover=0.,gamma_underperformance=0.,
        rank_ic_weight=0.,return_rank_ic_weight=0.,direction_weight=0.,volatility_regime_weight=0.,concentration_weight=0.)
    risk_aware_loss(w[:1],torch.zeros(1,1),torch.ones(1,1,dtype=torch.bool),
        overnight_log_returns=x[:1],aux_outputs=aux,**kwargs)
    next_aux={'initial_futures_carry_state':aux['_final_futures_carry_state'],
              'initial_equity_scale':aux['_final_equity_scale']}
    loss=risk_aware_loss(w[1:],torch.zeros(1,1),torch.ones(1,1,dtype=torch.bool),
        overnight_log_returns=x[1:],aux_outputs=next_aux,**kwargs)
    assert next_aux['_final_equity_scale']==pytest.approx(1.0096,abs=1e-6)
    loss.backward();assert torch.isfinite(w.grad).all()


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires CUDA')
@pytest.mark.filterwarnings('ignore:The .grad attribute of a Tensor that is not a leaf Tensor is being accessed:UserWarning')
def test_compiled_carry_forward_and_gradient_match():
    x=tape(3,symbols=4,lanes=3).cuda()
    bar(x,1,110,1); x[1,:,:,TAPE_FIELDS+3]=11000
    bar(x,2,115,2);bar(x,2,120,1,event=6)
    outputs=[];grads=[]
    for compiled in (False,True):
        w=torch.tensor([[.11,-.11,0.,.21],[.12,-.21,.11,0.],[.13,.11,-.11,.21]],device='cuda',requires_grad=True)
        result=run(w,x,initial_capital=100000.,use_compile=compiled)
        result.strategy_returns.sum().backward()
        assert torch.isfinite(w.grad).all()
        outputs.append(result);grads.append(w.grad)
    from dataclasses import fields
    for field in fields(outputs[0]):
        torch.testing.assert_close(getattr(outputs[0],field.name),getattr(outputs[1],field.name),rtol=1e-5,atol=1e-6)
    torch.testing.assert_close(*grads,rtol=1e-5,atol=1e-6)


def test_new_carry_contract_rejects_flat_checkpoint():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _checkpoint_manifest, _validate_checkpoint_manifest
    from test_checkpoint_manifest import _panel
    a,b=[_checkpoint_manifest(_panel(),load_config(f'configs/markets/{name}.yaml')) for name in
         ['tw_stock_futures_day_trade_0845_capacity_ceil_v5','tw_stock_futures_day_trade_0845_carry_v9']]
    assert a['fingerprints']['model']==b['fingerprints']['model']
    with pytest.raises(RuntimeError,match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest':a},b,checkpoint_path=__file__,scope='resume')


def test_random_cash_conservation_against_scalar_integer_oracle():
    rng=np.random.default_rng(821)
    n=64;x=tape(n);w=rng.uniform(-.85,.85,n).astype(np.float32)
    equity=100000.;quantity=0;mark=0.;expected=[];qexpected=[];rexpected=[]
    for i in range(n):
        opening=float(rng.integers(70,130))*100
        terminal=float(rng.integers(70,130))*100
        closing=float(rng.integers(70,130))*100
        cap=int(rng.integers(1,5));exit_cap=int(rng.integers(0,5))
        bar(x,i,opening/100,cap);bar(x,i,closing/100,exit_cap,event=6)
        x[i,0,0,TAPE_FIELDS+3]=terminal
        eopen=equity+quantity*(opening-mark)
        target=int(np.sign(w[i]))*min(int(abs(float(w[i]))*eopen//(opening+80)),abs(quantity)+cap)
        delta=int(np.clip(target-quantity,-cap,cap))
        reduce=-int(np.sign(quantity))*min(abs(quantity),max(0,-int(np.sign(quantity))*delta))
        after=quantity+reduce;addition=delta-reduce
        available=max(0,eopen-abs(reduce)*40-abs(after)*(opening+40))
        if abs(addition)*(opening+80)>available:
            addition=int(np.sign(addition))*int(available//(opening+80))
        delta=reduce+addition;position=quantity+delta
        exited=min(abs(position),exit_cap);remain=position-int(np.sign(position))*exited
        equity=eopen-abs(delta)*40+exited*(int(np.sign(position))*(closing-opening)-40)+remain*(terminal-opening)
        quantity=remain;mark=terminal
        expected.append(equity/100000.);qexpected.append(position);rexpected.append(remain)
    out=run(torch.from_numpy(w[:,None]),x,initial_capital=100000.,use_compile=False)
    assert out.final_alive
    assert out.contract_quantities_history[:,0,0].tolist()==qexpected
    assert out.residual_contract_quantities_history[:,0,0].tolist()==rexpected
    np.testing.assert_allclose(out.equity_scale_history.numpy(),expected,rtol=1e-6,atol=1e-6)


def test_data_lanes_extend_to_official_expiry_and_never_reuse_old_month():
    import polars as pl
    from datetime import date
    from stockagent.data.tw_stock_futures_carry import build_carry_tape
    dates=np.array(['2026-01-19','2026-01-20','2026-01-21'],dtype='datetime64[D]')
    source=pl.DataFrame({'date':[date(2026,1,19),date(2026,1,20),date(2026,1,21)],
        'physical_contract':['ABC:202601','ABC:202602','ABC:202602'],
        'underlying_symbol':['1234']*3,'contract_multiplier':[100.]*3,
        'valuation_settlement':[100.,200.,210.], 'source_row_observed':[True]*3})
    selected=source.select('date','physical_contract','underlying_symbol')
    cov=source.select('date','physical_contract').with_columns(pl.lit('minute_verified').alias('status'))
    minutes=source.select('date','physical_contract').with_columns(
        pl.lit(526).alias('minute'),*[pl.lit(100.).alias(k) for k in ['vwap','high','low','close','volume']])
    settlements=pl.DataFrame({'date':[date(2026,1,21)],'physical_contract':['ABC:202601'],'final_settlement_price':[105.]})
    x,meta=build_carry_tape(source,selected,minutes,cov,dates,('1234',),settlements,
        fee=40.,participation=.5,capacity_rounding='ceil')
    old=meta['physical_ids']['ABC:202601'];new=meta['physical_ids']['ABC:202602']
    oldlane=int(np.flatnonzero(x[0,0,:,TAPE_FIELDS]==old)[0])
    newlane=int(np.flatnonzero(x[1,0,:,TAPE_FIELDS]==new)[0])
    assert oldlane!=newlane
    assert x[1,0,oldlane,TAPE_FIELDS]==old
    assert x[1,0,oldlane,TAPE_FIELDS+6]==0  # explicitly unknown, not fabricated capacity
    assert x[2,0,oldlane,TAPE_FIELDS+4]==10500
    assert x[2,0,oldlane,TAPE_FIELDS+5]==1
    assert meta['valuation_is_official_daily_settlement_guaranteed'] is False
    # An observed official mark fills only valuation; it cannot make an
    # unknown-minute contract tradable or replace the legal final settlement.
    marks = pl.DataFrame({'date':[date(2026,1,20),date(2026,1,21)],
        'physical_contract':['ABC:202601']*2, 'official_daily_settlement':[101.,103.]})
    repaired,meta = build_carry_tape(source,selected,minutes,cov,dates,('1234',),settlements,
        fee=40.,participation=.5,capacity_rounding='ceil',daily_settlements=marks)
    assert repaired[1,0,oldlane,TAPE_FIELDS+3] == 10100
    assert repaired[2,0,oldlane,TAPE_FIELDS+4] == 10500
    np.testing.assert_array_equal(repaired[...,:TAPE_FIELDS], x[...,:TAPE_FIELDS])
    np.testing.assert_array_equal(repaired[...,TAPE_FIELDS+6], x[...,TAPE_FIELDS+6])
    assert meta['official_daily_settlement_contract_days'] == 2
    # Reported final contract value owns settlement, including embedded rights.
    rights_settlements = settlements.with_columns(pl.lit(10583.).alias('final_settlement_value'))
    with_rights, _ = build_carry_tape(source,selected,minutes,cov,dates,('1234',),rights_settlements,
        fee=40.,participation=.5,capacity_rounding='ceil',daily_settlements=marks)
    assert with_rights[2,0,oldlane,TAPE_FIELDS+4] == 10583
    np.testing.assert_array_equal(with_rights[...,:TAPE_FIELDS], repaired[...,:TAPE_FIELDS])


def test_artifact_roundtrip_prefix_and_status_preserve_open_inventory(tmp_path):
    from stockagent.backtest.simulator import run_backtest_torch
    from stockagent.training.trainer import _save_backtest_artifact, _load_backtest_artifact, _prefix_backtest_result, _slice_backtest_rows
    from stockagent.evaluation.futures_execution_status import futures_minute_execution_status
    w=torch.tensor([[.11],[.11],[.11]])
    out=run_backtest_torch(w,torch.zeros_like(w),torch.ones_like(w,dtype=torch.bool),torch.zeros(3),0.,0.,
        long_only=False,portfolio_activation='pre_normalized',execution_mode='tw_stock_futures_day_trade_0845_minute',
        overnight_returns=tape(3),day_trade_execution_initial_capital=100000.).to_numpy()
    dates=np.array(['2026-01-19','2026-01-20','2026-01-21'],dtype='datetime64[D]')
    p=tmp_path/'carry.npz';_save_backtest_artifact(p,out,dates)
    loaded,_=_load_backtest_artifact(p)
    np.testing.assert_array_equal(loaded.final_futures_carry_state,out.final_futures_carry_state)
    for result in (_prefix_backtest_result(loaded,2),_slice_backtest_rows(loaded,0,2)):
        assert result.final_weights[0]==pytest.approx(10000/99960)
        assert result.final_futures_carry_state[0,0,0]==1
        _save_backtest_artifact(tmp_path/'prefix.npz',result,dates[:2])
    status=futures_minute_execution_status(loaded,dates,['1234'])
    assert status['status']=='valid_marked_open_positions'
    assert status['traded_days']==1 and status['overnight_position_days']==3
    assert status['entry_contracts'] is None


def test_failed_carry_reason_survives_artifact_roundtrip_and_slices(tmp_path):
    from stockagent.backtest.simulator import run_backtest_torch
    from stockagent.training.trainer import _save_backtest_artifact, _load_backtest_artifact, _prefix_backtest_result, _slice_backtest_rows
    from stockagent.evaluation.futures_execution_status import futures_minute_execution_status
    x=tape(3);bar(x,1,300,0);x[1,0,0,TAPE_FIELDS+3]=30000
    w=torch.full((3,1),-.51)
    out=run_backtest_torch(w,torch.zeros_like(w),torch.ones_like(w,dtype=torch.bool),torch.zeros(3),0.,0.,
        long_only=False,portfolio_activation='pre_normalized',execution_mode='tw_stock_futures_day_trade_0845_minute',
        overnight_returns=x,day_trade_execution_initial_capital=100000.).to_numpy()
    dates=np.array(['2026-01-19','2026-01-20','2026-01-21'],dtype='datetime64[D]')
    p=tmp_path/'failed.npz';_save_backtest_artifact(p,out,dates)
    loaded,_=_load_backtest_artifact(p)
    for result,ds in [(loaded,dates),(_prefix_backtest_result(loaded,2),dates[:2]),
                      (_slice_backtest_rows(loaded,0,2),dates[:2])]:
        assert result.default_reason_history[1]==4
        status=futures_minute_execution_status(result,ds,['1234'])
        assert status['first_failure_reason']=='nonpositive_equity'
        assert result.final_futures_carry_state[0,0,0]==-5


def test_insolvent_carry_records_loss_and_keeps_unclosed_inventory():
    x=tape(2);bar(x,1,300,0);x[1,0,0,TAPE_FIELDS+3]=30000
    out=run(torch.full((2,1),-.51),x,initial_capital=100000.,use_compile=False)
    assert out.contract_quantities_history[0,0,0]==-5
    assert not out.final_alive and out.default_reason_history[-1]==4
    assert out.final_equity_scale==0
    assert out.final_carry_state[0,0,0]==-5
    assert out.final_carry_state[0,0,1]==30000


def test_official_carry_evidence_accepts_only_proven_zero_and_rechecks_raw_sha(tmp_path):
    import json
    from datetime import date
    import polars as pl
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_stock_futures_carry import load_carry_no_trade_evidence
    raw=tmp_path/'raw.csv';raw.write_text('unit-test official fixture')
    path=tmp_path/'official_evidence.parquet'
    pl.DataFrame({'date':[date(2021,5,19)]*3,'physical_contract':['A:202105','B:202105','C:202105'],
        'outright_volume':[0,1,None],'official_reason':['absent_from_complete_day','outright_volume','missing_official_day'],
        'official_day_sources':[json.dumps([sha256_file(raw)])]*2+['[]']}).write_parquet(path)
    receipt={'source':'taifex_complete_daily_and_spread_legs_v1','sha256':sha256_file(path),
        'sources':[{'path':str(raw),'sha256':sha256_file(raw)}]}
    path.with_name('official_evidence_manifest.json').write_text(json.dumps(receipt))
    assert load_carry_no_trade_evidence(path)['physical_contract'].to_list()==['A:202105']
    raw.write_text('changed')
    with pytest.raises(ValueError,match='raw archive SHA'):
        load_carry_no_trade_evidence(path)


def test_carry_zero_evidence_cannot_overwrite_dispute_or_contradict_minutes():
    from datetime import date
    import polars as pl
    from stockagent.data.tw_stock_futures_carry import add_carry_no_trade_evidence
    day=date(2021,5,19)
    coverage=pl.DataFrame({'date':[day],'physical_contract':['A:202105'],'status':['source_empty_unresolved']})
    proof=pl.DataFrame({'date':[day]*2,'physical_contract':['A:202105','B:202105'],
        'status':['official_no_outright_trades']*2})
    minutes=pl.DataFrame({'date':[day],'physical_contract':['B:202105'],'volume':[1.]})
    with pytest.raises(ValueError,match='contradicts'):
        add_carry_no_trade_evidence(coverage,minutes,proof)
    result=add_carry_no_trade_evidence(coverage,minutes.head(0),proof).sort('physical_contract')
    assert result['status'].to_list()==['source_empty_unresolved','official_no_outright_trades']
