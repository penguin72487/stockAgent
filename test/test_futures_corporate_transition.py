import copy
import numpy as np
import pytest
import torch

from test_tw_stock_futures_carry import tape, bar
from stockagent.backtest.tw_stock_futures_carry import run_tw_stock_futures_carry_torch as run
from stockagent.backtest.futures_data_validity import FuturesCarryDataError
from stockagent.data.tw_stock_futures_minute import TAPE_FIELDS
from stockagent.data.tw_stock_futures_carry import CARRY_TAPE_FIELDS


def transition_tape():
    x=torch.nn.functional.pad(tape(3,lanes=3),(0,1))
    # No event-day trades; the old position must survive in the adjusted lane.
    x[1:,:,0,3:TAPE_FIELDS]=0
    x[1,0,0,TAPE_FIELDS+8]=1
    x[1:,0,2,:CARRY_TAPE_FIELDS]=x[0,0,0,:CARRY_TAPE_FIELDS]
    x[1:,0,2,3:TAPE_FIELDS]=0
    x[1:,0,2,TAPE_FIELDS]=2
    x[1:,0,2,TAPE_FIELDS+2]=0  # Adjusted contract never becomes a new target.
    x[1,0,2,CARRY_TAPE_FIELDS]=1
    return x


@pytest.mark.parametrize('direction',[1.,-1.])
def test_transfer_preserves_quantity_mark_and_cash_without_fill_or_fee(direction):
    x=transition_tape()
    out=run(torch.tensor([[direction*.11],[0.],[0.]]),x,initial_capital=100000.,use_compile=False)
    assert out.final_alive
    assert out.residual_contract_quantities_history[:,0].tolist()==[[int(direction),0,0],[0,0,int(direction)],[0,0,int(direction)]]
    torch.testing.assert_close(out.equity_scale_history,torch.full((3,),.9996),rtol=0,atol=1e-7)
    assert out.turnovers[1:].tolist()==[0.,0.]
    assert out.carry_state_history[1,0,2,:3].tolist()==[direction,10000.,2.]


def test_adjusted_and_new_standard_share_no_capacity_or_position_netting():
    x=transition_tape()
    bar(x,1,100,10,slot=0)  # New PLF can trade; old PL1 has no minute prints.
    out=run(torch.tensor([[.11],[-.11],[0.]]),x,initial_capital=100000.,use_compile=False)
    assert out.final_alive
    assert out.residual_contract_quantities_history[1,0].tolist()==[-1,0,1]
    assert out.turnovers[1]==pytest.approx(10000/99960)


def test_adjusted_expiry_uses_full_contract_value_and_charges_only_settlement():
    x=transition_tape();x[2,0,2,TAPE_FIELDS+4]=10283;x[2,0,2,TAPE_FIELDS+5]=1
    out=run(torch.tensor([[.11],[0.],[0.]]),x,initial_capital=100000.,use_compile=False)
    assert out.final_alive and not out.final_carry_state[...,0].any()
    assert out.final_equity_scale==pytest.approx((100000-40+283-40)/100000,abs=1e-7)


@pytest.mark.parametrize('damage',['missing_source','missing_mark','wrong_origin','two_destinations','occupied_destination'])
def test_transfer_does_not_suppress_unknown_data_or_merge_positions(damage):
    x=transition_tape()
    state=None
    if damage=='missing_source':x[1,0,2,TAPE_FIELDS+6]=0
    elif damage=='missing_mark':x[1,0,2,TAPE_FIELDS+3]=float('nan')
    elif damage=='wrong_origin':x[1,0,2,CARRY_TAPE_FIELDS]=999
    elif damage=='two_destinations':
        x[1,0,1]=x[1,0,2];x[1,0,1,TAPE_FIELDS]=3
    else:
        state=torch.zeros(1,3,4);state[0,2]=torch.tensor([1.,10000.,2.,.1])
        x[0,0,2]=x[1,0,2];x[0,0,2,CARRY_TAPE_FIELDS]=0
    with pytest.raises(FuturesCarryDataError):
        run(torch.tensor([[.11],[0.],[0.]]),x,initial_capital=100000.,initial_carry_state=state,use_compile=False)


def test_chunk_boundary_transfer_and_gradients_match_whole_trajectory():
    x=transition_tape();x[2,0,2,TAPE_FIELDS+3]=11000
    w=torch.tensor([[.11],[0.],[0.]],requires_grad=True)
    full=run(w,x,initial_capital=100000.,use_compile=False)
    before=run(w[:1],x[:1],initial_capital=100000.,use_compile=False)
    after=run(w[1:],x[1:],initial_capital=100000.,initial_carry_state=before.final_carry_state,
              initial_equity_scale=before.final_equity_scale,use_compile=False)
    torch.testing.assert_close(full.strategy_returns,torch.cat((before.strategy_returns,after.strategy_returns)),rtol=0,atol=0)
    full.strategy_returns.sum().backward()
    assert torch.isfinite(w.grad).all() and w.grad[0].abs().sum()>0


@pytest.mark.skipif(not torch.cuda.is_available(),reason='CUDA required')
@pytest.mark.filterwarnings('ignore:The .grad attribute of a Tensor that is not a leaf Tensor is being accessed:UserWarning')
def test_compiled_transfer_matches_eager_values_and_gradients():
    x=transition_tape().cuda();x[2,0,2,TAPE_FIELDS+3]=11000
    w=torch.tensor([[.11],[0.],[0.]],device='cuda',requires_grad=True)
    eager=run(w,x,initial_capital=100000.,use_compile=False)
    grad=torch.autograd.grad(eager.strategy_returns.sum(),w)[0]
    compiled=run(w,x,initial_capital=100000.,use_compile=True)
    torch.testing.assert_close(eager.strategy_returns,compiled.strategy_returns)
    torch.testing.assert_close(eager.final_carry_state,compiled.final_carry_state)
    torch.testing.assert_close(grad,torch.autograd.grad(compiled.strategy_returns.sum(),w)[0])


def test_checkpoint_scope_rejects_old_optimizer_but_preserves_v9_default():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _trading_checkpoint_contract, _configuration_fingerprint_snapshot
    from stockagent.training.trainer import _mode_artifact_contract_for_config
    old=load_config('configs/markets/tw_stock_futures_day_trade_0845_carry_v9.yaml')
    new=load_config('configs/markets/tw_stock_futures_day_trade_0845_carry_v10.yaml')
    assert 'tw_stock_futures_day_trade_corporate_transition_path' not in _configuration_fingerprint_snapshot(old)['trading']
    a=_trading_checkpoint_contract(old)['taiwan_stock_futures_day_trade']
    b=_trading_checkpoint_contract(new)['taiwan_stock_futures_day_trade']
    assert a['data_contract_version']==4 and b['data_contract_version']==5
    assert len(b['execution_tensor_channels'])==len(a['execution_tensor_channels'])+1
    assert _mode_artifact_contract_for_config(new)['mode_details']['execution_contract_version']==5


def test_future_adjusted_price_cannot_change_earlier_holdings_or_returns():
    x=transition_tape()
    changed=x.clone();changed[2,0,2,TAPE_FIELDS+3]=25000
    w=torch.tensor([[.11],[0.],[0.]])
    a=run(w,x,initial_capital=100000.,use_compile=False)
    b=run(w,changed,initial_capital=100000.,use_compile=False)
    torch.testing.assert_close(a.strategy_returns[:2],b.strategy_returns[:2],rtol=0,atol=0)
    torch.testing.assert_close(a.carry_state_history[:2],b.carry_state_history[:2],rtol=0,atol=0)


def test_real_bundle_checks_required_hashes_and_exact_raw_reconstruction(tmp_path):
    from pathlib import Path
    import hashlib
    import json
    import shutil
    import polars as pl
    from stockagent.data.tw_stock_futures_transition import load_transition_bundle
    source=Path('artifacts/data_preparation/futures_corporate_transitions_v2_20260910')
    if not source.exists():pytest.skip('receipt-backed remote acceptance fixture unavailable')
    manifest=tmp_path/'bundle/manifest.json'
    shutil.copytree(source,manifest.parent)
    bundle=load_transition_bundle(manifest)
    assert bundle['coverage'].height==111 and bundle['minutes'].height==9
    original=json.loads(manifest.read_text())
    damaged=copy.deepcopy(original);damaged['files'].pop('minutes.parquet')
    manifest.write_text(json.dumps(damaged))
    with pytest.raises(ValueError,match='omits required source hashes'):load_transition_bundle(manifest)
    bars=manifest.parent/'minutes.parquet'
    pl.read_parquet(bars).with_columns((pl.col('volume')+1).alias('volume')).write_parquet(bars)
    original['files']['minutes.parquet']['sha256']=hashlib.sha256(bars.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(original))
    with pytest.raises(ValueError,match='differs from exact source KBars'):load_transition_bundle(manifest)
