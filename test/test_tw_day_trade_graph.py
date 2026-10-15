from dataclasses import fields, replace

import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import (
    DayTradeCarrySession, DayTradeCarryState, run_day_trade_carry_sessions)
from stockagent.backtest.tw_day_trade_graph import _Program


def _inputs(rows=3, device='cpu', tail=0):
    s=2;d=torch.device(device)
    v=lambda x: torch.full((s,),x,dtype=torch.float64,device=d)
    sessions=[]
    for day in range(rows):
        marks=v(1000+day*10)[:,None].expand(s,270).clone()
        marks[:,-1]=1010+day*10
        prices=torch.full((s,270,2),float('nan'),device=d,dtype=torch.float64)
        capacity=torch.zeros_like(prices)
        prices[:,264,:]=1020+day*10;capacity[:,264,:]=1000
        sessions.append(DayTradeCarrySession(739839+day,v(1000+day*10),v(1000+day*10),
            v(1000+day*10),v(6000),v(900),v(1200),v(0),prices,capacity,marks,
            action_mask=v(0),share_ratio=v(1),cash_per_old_share=v(0),payment_day=v(739950),
            stock_delivery_day=v(0),source_gap_mask=v(0),unresolved_action_gap_mask=v(0),
            daily_proxy_mask=v(0),terminal_liquidation_price=v(1010+day*10)))
    w=torch.tensor([[.21,-.24]]*rows,device=d,dtype=torch.float32,requires_grad=True)
    fees=(v(.001425),v(.002925),v(.004425),v(.00114))
    initial=DayTradeCarryState.empty(s,10_000_000,d)
    p=_Program(sessions[0],rows,s,10_000_000.)
    inv=initial.inventory
    values=(w,torch.ones_like(w,dtype=torch.bool),*fees,inv.realized_net_pnl,inv.carry_cost,
        inv.action_cursor,inv.decision_day,inv.observed_day,inv.failed,initial.last_nav,initial.alive,
        torch.tensor([x.day for x in sessions],device=d,dtype=torch.float64),
        torch.arange(rows,device=d)<rows-tail,
        *(torch.stack(tuple(getattr(x,n) for x in sessions)) for n in p.names))
    common=dict(can_enter=values[1],buy_fee_rate=fees[0],day_sell_fee_rate=fees[1],
        normal_sell_fee_rate=fees[2],rebate_rate=fees[3],initial_capital=10_000_000.,
        event_compression=False,require_minute_nav_grad=False)
    return w,tuple(sessions),p,values,common


@pytest.mark.parametrize('tail',[0,1,2])
@pytest.mark.parametrize('compressed',[False,True])
def test_functional_flat_block_matches_canonical_financial_forward_and_vjp(tail,compressed):
    w,sessions,p,values,common=_inputs(tail=tail)
    p.event_compression=compressed
    common['event_compression']=compressed
    count=len(sessions)-tail
    actual=p(values)
    reference=run_day_trade_carry_sessions(w[:count],sessions[:count],
        **{**common,'can_enter':common['can_enter'][:count]})
    for a,name in zip(actual[:6],('strategy_returns','turnovers','weights_history',
                                    'shares_history','minute_nav','settlement_default')):
        torch.testing.assert_close(a[:count],getattr(reference,name),rtol=1e-10,atol=1e-8)
    assert bool(actual[-1])
    torch.testing.assert_close(actual[6][:count],reference.final_state.inventory.cohorts,rtol=1e-10,atol=1e-8)
    torch.testing.assert_close(actual[7][:count],reference.final_state.inventory.claims,rtol=1e-10,atol=1e-8)
    torch.testing.assert_close(actual[14],reference.final_state.last_nav,rtol=1e-12,atol=1e-8)
    for i,name in zip(range(8,14),('realized_net_pnl','carry_cost','action_cursor',
                                  'decision_day','observed_day','failed')):
        torch.testing.assert_close(actual[i],getattr(reference.final_state.inventory,name),rtol=1e-12,atol=1e-8)
    expected_grad=torch.autograd.grad(reference.strategy_returns.sum(),w,retain_graph=True)[0]
    actual_grad=torch.autograd.grad(actual[0][:count].sum(),w)[0]
    torch.testing.assert_close(actual_grad,expected_grad,rtol=1e-6,atol=1e-9)
    assert torch.isfinite(actual_grad).all() and actual_grad.abs().sum()>0
    if tail:assert torch.count_nonzero(actual_grad[count:])==0


def test_program_releases_input_source_storage_after_recording_schema():
    import gc
    import weakref
    _,sessions,program,values,_=_inputs()
    source=weakref.ref(sessions[0].marks)
    del sessions
    gc.collect()
    assert source() is None
    assert not any(isinstance(value,torch.Tensor) for value in program.session_defaults.values())
    assert bool(program(values)[-1])


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires CUDA graph runtime')
@pytest.mark.parametrize('adjoint',['recompute','saved'])
@pytest.mark.parametrize('compressed',[False,True])
def test_graph_reuse_different_calendar_tail_and_pending_backward(monkeypatch,adjoint,compressed):
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH','1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_CUDAGRAPHS','0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS','4')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT',adjoint)
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_TAPE_SLOTS','1')
    w,sessions,p,values,common=_inputs(device='cuda')
    common['event_compression']=compressed
    reference=run_day_trade_carry_sessions(w,sessions,**common,_allow_flat_terminal=False)
    actual=run_day_trade_carry_sessions(w,sessions,**common)
    # Another live forward uses the same engine before the first backward.
    w2=w.detach().clone().requires_grad_()
    future=tuple(replace(s,day=s.day+20) for s in sessions[:2])
    second=run_day_trade_carry_sessions(w2[:2],future,**{**common,'can_enter':common['can_enter'][:2]})
    torch.testing.assert_close(actual.strategy_returns,reference.strategy_returns,rtol=1e-8,atol=1e-8)
    grad_ref=torch.autograd.grad(reference.strategy_returns.sum(),w)[0]
    actual.strategy_returns.sum().backward(retain_graph=True)
    torch.testing.assert_close(w.grad,grad_ref,rtol=1e-5,atol=1e-8)
    w.grad.zero_()
    actual.strategy_returns.sum().backward()
    torch.testing.assert_close(w.grad,grad_ref,rtol=1e-5,atol=1e-8)
    second.strategy_returns.sum().backward()
    assert w2.grad[:2].abs().sum()>0 and torch.isfinite(w2.grad).all()


def test_graph_adjoint_checkpoint_identity_is_explicit_and_disabled_is_compatible(monkeypatch):
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import (
        _configuration_fingerprint_snapshot,_training_checkpoint_contract)
    import os
    from pathlib import Path
    cfg=load_config(Path(os.environ.get('STOCKAGENT_TEST_CONFIG_ROOT','configs'))/'experiment_baseline.yaml')
    cfg.trading.execution_mode='tw_day_trade'
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP','0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH','0')
    disabled=_configuration_fingerprint_snapshot(cfg)
    assert 'physical_fifo_explicit_graph_contract' not in disabled['training']
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH','1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT','recompute')
    a=_configuration_fingerprint_snapshot(cfg);ta=_training_checkpoint_contract(cfg)
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT','saved')
    b=_configuration_fingerprint_snapshot(cfg);tb=_training_checkpoint_contract(cfg)
    assert a!=b and ta!=tb
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP','1')
    c=_configuration_fingerprint_snapshot(cfg);tc=_training_checkpoint_contract(cfg)
    assert c!=b and tc!=tb
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP','0')
    assert _configuration_fingerprint_snapshot(cfg)==b


def test_graph_unsupported_fee_or_source_jacobian_keeps_canonical_owner(monkeypatch):
    from stockagent.backtest.tw_day_trade_graph import run_flat_graph_sessions
    w,sessions,_,_,common=_inputs()
    # These return before CUDA graph construction even on a CPU test runner.
    learned_fee=common['buy_fee_rate'].detach().clone().requires_grad_()
    assert run_flat_graph_sessions(w,sessions,**{**common,'buy_fee_rate':learned_fee}) is None
    assert run_flat_graph_sessions(w,sessions,**{**common,'buy_fee_rate':learned_fee.detach()[0]}) is None
    learned_source=replace(sessions[0],marks=sessions[0].marks.clone().requires_grad_())
    assert run_flat_graph_sessions(w,(learned_source,*sessions[1:]),**common) is None


def _packed_source_fixture():
    from types import SimpleNamespace
    from stockagent.training.day_trade_carry_bridge import (
        PackedDayTradeCarrySession,PreparedDayTradeCarrySource)
    _,sessions,_,_,_=_inputs()
    packed=[]
    for s in sessions:
        # Exercise source bits that ordinary numeric equality would hide.
        s.marks[0,3]=-0.
        s.marks.view(torch.int64)[0,4]=0x7ff8000000001234
        values={f.name:getattr(s,f.name) for f in fields(PackedDayTradeCarrySession)
                if hasattr(s,f.name)}
        flat=torch.nonzero(torch.isfinite(s.exit_prices.flatten()),as_tuple=False).flatten()
        values.update(exit_flat=flat,exit_price=s.exit_prices.flatten()[flat],
                      exit_capacity=s.exit_capacity.flatten()[flat])
        packed.append(PackedDayTradeCarrySession(**values))
    calls=[]
    def load(row):
        calls.append(row)
        return packed[row]
    source=PreparedDayTradeCarrySource((),('2300','2301'),'prepared-cache-fixture-v1',
        session_days=tuple(s.day for s in sessions),session_loader=lambda row:sessions[row],
        packed_session_loader=load)
    class Split(SimpleNamespace):
        def __len__(self):return len(self._valid_indices_cpu)
    split=Split(features=torch.zeros((3,2,1)),_valid_indices_cpu=torch.arange(3),
                sample_mask=None,symbol_indices=None,execution_mode='tw_day_trade')
    return source,split,calls


def test_prepared_source_cache_scope_bits_eviction_and_mutation(monkeypatch):
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_BYTES',str(1<<20))
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_PINNED','0')
    source,split,calls=_packed_source_fixture()
    first=source.prepare_batch(split,0,2,event_compression=True)
    assert source.prepare_batch(split,0,2,event_compression=True) is first
    assert sorted(calls)==[0,1]
    # Public scope validation precedes a hit; no stale cached row can admit it.
    split._valid_indices_cpu[1]=0
    with pytest.raises(ValueError,match='skips, repeats'):
        source.prepare_batch(split,0,2,event_compression=True)
    split._valid_indices_cpu[1]=1
    other=source.prepare_batch(split,0,2,event_compression=False)
    for key in first.tensors:
        assert torch.equal(first.tensors[key].view(torch.int64),other.tensors[key].view(torch.int64))
    assert replace(source).prepared_batch_cache_info()['entries']==0
    first.tensors['marks'].add_(1)
    with pytest.raises(RuntimeError,match='immutable prepared physical cache was mutated'):
        source.prepare_batch(split,0,2,event_compression=True)
    # One-row capacity must evict, rather than growing with the calendar.
    fresh,split,_=_packed_source_fixture()
    one=fresh.prepare_batch(split,0,1,event_compression=True)
    size=sum(t.untyped_storage().nbytes() for t in one.tensors.values())
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_BYTES',str(size+1))
    fresh.prepare_batch(split,1,2,event_compression=True)
    info=fresh.prepared_batch_cache_info()
    assert info['resident_bytes']<=size+1 and info['evictions']==1
    inference_source,split,_=_packed_source_fixture()
    with torch.inference_mode():
        batch=inference_source.prepare_batch(split,0,1,event_compression=True)
    assert not any(torch.is_inference(t) for t in batch.tensors.values())
    assert inference_source.prepare_batch(split,0,1,event_compression=True) is batch


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires pinned CUDA transport')
def test_graph_reuse_pinned_source_cache_preserves_all_fp64_bits(monkeypatch):
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_BYTES',str(1<<20))
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_PINNED','1')
    source,split,calls=_packed_source_fixture()
    with torch.inference_mode():
        staged=source.prepare_batch(split,0,3,event_compression=True)
    assert not any(torch.is_inference(t) for t in staged.tensors.values())
    assert all(t.is_pinned() for t in staged.tensors.values())
    actual=staged.sessions(torch.device('cuda'),transport_block_rows=1)
    expected=staged.sessions(torch.device('cpu'),transport_block_rows=0)
    torch.cuda.synchronize()
    for a,b in zip(actual,expected):
        for f in fields(a):
            av,bv=getattr(a,f.name),getattr(b,f.name)
            if isinstance(av,torch.Tensor):
                assert torch.equal(av.cpu().contiguous().view(torch.int64),bv.contiguous().view(torch.int64))
    assert source.prepare_batch(split,0,3,event_compression=True) is staged
    assert source.prepared_batch_cache_info()['pinned_bytes']>0


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires CUDA side-stream source prefetch')
@pytest.mark.parametrize('device_index',[0,1])
def test_device_prefetch_preserves_source_bits_slices_and_cpu_cache(monkeypatch,device_index):
    from stockagent.training.day_trade_carry_bridge import prefetch_physical_carry_batches
    if device_index>=torch.cuda.device_count():
        pytest.skip('second device unavailable')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_BYTES',str(1<<20))
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_PINNED','1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH','1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH_ROWS','1')
    source,split,_=_packed_source_fixture()
    device=torch.device('cuda',device_index)
    batches=list(prefetch_physical_carry_batches(source,split,[0,1],2,
        batch_spans=((0,2),(2,3)),device=device,event_compression=True))
    for index,batch in batches:
        assert batch._device==device and batch._device_ready is not None
        expected=batch.sessions(torch.device('cpu'))
        actual=batch.sessions(device)
        for a,b in zip(actual,expected,strict=True):
            for f in fields(a):
                av,bv=getattr(a,f.name),getattr(b,f.name)
                if isinstance(av,torch.Tensor):
                    assert av.device==device
                    assert torch.equal(av.cpu().contiguous().view(torch.int64),
                                       bv.contiguous().view(torch.int64))
        sliced=batch.row_slice(0,1)
        assert sliced.sessions(device)[0] is actual[0]
        start,end=((0,2),(2,3))[index]
        cached=source.prepare_batch(split,start,end,event_compression=True)
        assert not cached._device_sessions and cached._device_ready is None
        assert all(value.device.type=='cpu' for value in cached.tensors.values())


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires CUDA graph runtime')
@pytest.mark.parametrize('compressed',[False,True])
@pytest.mark.parametrize('compiled',['0','1'])
def test_graph_reuse_inference_first_then_train(monkeypatch,compressed,compiled):
    from stockagent.backtest.tw_day_trade_graph import get_explicit_carry_graph_stats
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH','1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_CUDAGRAPHS','0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS','8')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT','saved')
    monkeypatch.setenv('STOCKAGENT_BACKTEST_COMPILE',compiled)
    w,sessions,_,_,common=_inputs(device='cuda')
    common['event_compression']=compressed
    before=get_explicit_carry_graph_stats()['accepted_blocks']
    with torch.inference_mode():
        actual=run_day_trade_carry_sessions(w.detach(),sessions,
            **{**common,'require_minute_nav_grad':True})
        inference_reference=run_day_trade_carry_sessions(w.detach(),sessions,
            **{**common,'require_minute_nav_grad':True},_allow_flat_terminal=False)
    assert get_explicit_carry_graph_stats()['accepted_blocks']==before+1
    for name in ('strategy_returns','turnovers','weights_history','shares_history',
                 'minute_nav','settlement_default'):
        torch.testing.assert_close(getattr(actual,name),getattr(inference_reference,name),
                                   rtol=1e-8,atol=1e-8)
    reference=run_day_trade_carry_sessions(w,sessions,**common,_allow_flat_terminal=False)
    train=run_day_trade_carry_sessions(w,sessions,**common)
    torch.testing.assert_close(actual.strategy_returns,reference.strategy_returns)
    torch.testing.assert_close(actual.minute_nav,reference.minute_nav)
    expected=torch.autograd.grad(reference.strategy_returns.sum(),w)[0]
    train.strategy_returns.sum().backward()
    torch.testing.assert_close(w.grad,expected,rtol=1e-5,atol=1e-8)


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires CUDA graph runtime')
def test_graph_reuse_invalid_terminal_replays_atomic_canonical_failure(monkeypatch):
    from stockagent.backtest.tw_day_trade_graph import get_explicit_carry_graph_stats
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH','1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_CUDAGRAPHS','0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS','4')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT','saved')
    w,sessions,_,_,common=_inputs(rows=1,device='cuda')
    sessions=(replace(sessions[0],terminal_liquidation_price=torch.full((2,),float('nan'),device='cuda',dtype=torch.float64),
                      exit_capacity=torch.zeros_like(sessions[0].exit_capacity)),)
    common['event_compression']=True
    reference=run_day_trade_carry_sessions(w,sessions,**common,_allow_flat_terminal=False)
    before=get_explicit_carry_graph_stats()['certificate_fallbacks']
    actual=run_day_trade_carry_sessions(w,sessions,**common)
    assert get_explicit_carry_graph_stats()['certificate_fallbacks']==before+1
    assert actual.final_state.inventory.failed==reference.final_state.inventory.failed==1
    assert torch.isnan(actual.strategy_returns).all()
    for name in ('strategy_returns','shares_history','minute_nav'):
        torch.testing.assert_close(getattr(actual,name),getattr(reference,name),equal_nan=True)
