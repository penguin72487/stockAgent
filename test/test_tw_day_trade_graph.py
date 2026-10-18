from dataclasses import fields, replace

import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import (
    DayTradeCarrySession, DayTradeCarryState, run_day_trade_carry_sessions)
from stockagent.backtest.tw_day_trade_graph import _Program


@pytest.mark.parametrize('layout', ['consecutive', 'strided', 'separate', 'reordered', 'repeated'])
@pytest.mark.parametrize('tail', [0, 2])
@pytest.mark.parametrize('device', ['cpu', 'cuda'])
def test_session_field_transport_preserves_every_value_and_inactive_tail(layout, tail, device):
    if device == 'cuda' and not torch.cuda.is_available():
        pytest.skip('requires CUDA')
    from stockagent.backtest.tw_day_trade_graph import _copy_session_field_
    original = torch.arange(5 * 3 * 7, dtype=torch.float64, device=device).reshape(5, 3, 7)
    original[2, 1, 4] = float('nan')
    source = original[:, :, :5] if layout == 'strided' else original
    values = tuple(source[i] for i in range(3))
    if layout == 'separate':
        values = tuple(x.clone() for x in values)
    elif layout == 'reordered':
        values = (values[2], values[0], values[1])
    elif layout == 'repeated':
        values = (values[1],) * 3
    actual = torch.empty((3 + tail, *source.shape[1:]), dtype=source.dtype, device=device)
    _copy_session_field_(actual, values)
    expected = torch.stack((*values, *((values[-1],) * tail)))
    torch.testing.assert_close(actual, expected, rtol=0, atol=0, equal_nan=True)


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
@pytest.mark.parametrize('packed_source',[False,True])
def test_graph_reuse_different_calendar_tail_and_pending_backward(monkeypatch,adjoint,compressed,packed_source):
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH','1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_CUDAGRAPHS','0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS','4')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT',adjoint)
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_TAPE_SLOTS','1')
    w,sessions,p,values,common=_inputs(device='cuda')
    if packed_source:
        from stockagent.training.day_trade_carry_bridge import _sessions_from_field_batch
        tensors={name:torch.stack(tuple(getattr(s,name) for s in sessions)) for name in p.names}
        sessions=_sessions_from_field_batch(tuple(s.day for s in sessions),tensors)
    common['event_compression']=compressed
    reference=run_day_trade_carry_sessions(w,sessions,**common,_allow_flat_terminal=False)
    actual=run_day_trade_carry_sessions(w,sessions,**common)
    # Another live forward uses the same engine before the first backward.
    w2=w.detach().clone().requires_grad_()
    future=tuple(replace(s,day=s.day+20) for s in sessions[:2])
    second=run_day_trade_carry_sessions(w2[:2],future,**{**common,'can_enter':common['can_enter'][:2]})
    torch.testing.assert_close(actual.strategy_returns,reference.strategy_returns,rtol=1e-8,atol=1e-8)
    from stockagent.backtest.tw_day_trade_graph import detached_carry_state
    carried=detached_carry_state(actual.final_state)
    assert carried.inventory.cohorts.shape[0] == carried.inventory.claims.shape[0] == 0
    # Only the carried, detached copy is compacted; public history and its
    # pending VJP remain intact through the following independent replay.
    torch.testing.assert_close(actual.final_state.inventory.cohorts,
                               reference.final_state.inventory.cohorts,rtol=1e-8,atol=1e-8)
    torch.testing.assert_close(carried.last_nav,actual.final_state.last_nav,rtol=0,atol=0)
    grad_ref=torch.autograd.grad(reference.strategy_returns.sum(),w)[0]
    actual.strategy_returns.sum().backward(retain_graph=True)
    torch.testing.assert_close(w.grad,grad_ref,rtol=1e-5,atol=1e-8)
    w.grad.zero_()
    actual.strategy_returns.sum().backward()
    torch.testing.assert_close(w.grad,grad_ref,rtol=1e-5,atol=1e-8)
    second.strategy_returns.sum().backward()
    assert w2.grad[:2].abs().sum()>0 and torch.isfinite(w2.grad).all()


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA graph runtime')
def test_graph_prefix_backend_change_cannot_replay_a_previous_captured_program(monkeypatch):
    from stockagent.backtest.tw_day_trade_graph import get_explicit_carry_graph_stats
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_CUDAGRAPHS', '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS', '2')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT', 'saved')
    w, sessions, _, _, common = _inputs(rows=1, device='cuda')
    oracle = run_day_trade_carry_sessions(w, sessions, **common, _allow_flat_terminal=False)
    expected, = torch.autograd.grad(oracle.strategy_returns.sum(), w)
    for backend in ['native', 'triton']:
        monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', backend)
        before = get_explicit_carry_graph_stats()['constructors']
        result = run_day_trade_carry_sessions(w, sessions, **common)
        assert get_explicit_carry_graph_stats()['constructors'] == before + 1
        torch.testing.assert_close(result.strategy_returns, oracle.strategy_returns, rtol=1e-8, atol=1e-8)
        actual, = torch.autograd.grad(result.strategy_returns.sum(), w)
        torch.testing.assert_close(actual, expected, rtol=1e-5, atol=1e-8)


def test_certified_detach_preserves_mutated_rows_and_never_compacts_an_uncertified_state():
    from stockagent.backtest.tw_day_trade_graph import detached_carry_state, _inventory_row_token
    from stockagent.backtest.tw_day_trade_inventory import CohortField
    state=DayTradeCarryState.empty(2,10_000_000,torch.device('cpu'))
    state=replace(state,inventory=replace(state.inventory,
        cohorts=torch.zeros((3,2,len(CohortField)),dtype=torch.float64),
        claims=torch.zeros((3,2,3),dtype=torch.float64)))
    assert detached_carry_state(state).inventory.cohorts.shape[0] == 3
    object.__setattr__(state,'_graph_empty_inventory_rows',(_inventory_row_token(state),None))
    detached=detached_carry_state(state)
    assert detached.inventory.cohorts.shape[0] == detached.inventory.claims.shape[0] == 0
    state.inventory.cohorts[0,0,CohortField.SHARES]=1000
    fallback=detached_carry_state(state)
    assert fallback.inventory.cohorts.shape[0] == 3
    assert fallback.inventory.shares[0] == 1000
    assert not fallback.inventory.cohorts.requires_grad
    torch.testing.assert_close(fallback.last_nav,state.last_nav,rtol=0,atol=0)


@pytest.mark.skipif(not torch.cuda.is_available(),reason='requires CUDA graph runtime')
def test_deferred_graph_keeps_complete_account_and_vjp_before_the_update_gate(monkeypatch):
    from stockagent.backtest.tw_day_trade_graph import DeferredLedgerValidation, detached_carry_state
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH','1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_CUDAGRAPHS','0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS','4')
    w,sessions,_,_,common=_inputs(device='cuda')
    reference=run_day_trade_carry_sessions(w,sessions,**common,_allow_flat_terminal=False)
    with DeferredLedgerValidation(w.device) as gate:
        actual=run_day_trade_carry_sessions(w,sessions,**common)
        carried=detached_carry_state(actual.final_state)
        assert carried.inventory.cohorts.shape[0] == 0
        assert len(gate.predicates) >= 2
        gate.finish()
    torch.testing.assert_close(actual.strategy_returns,reference.strategy_returns,rtol=1e-8,atol=1e-8)
    expected=torch.autograd.grad(reference.strategy_returns.sum(),w)[0]
    observed=torch.autograd.grad(actual.strategy_returns.sum(),w)[0]
    torch.testing.assert_close(observed,expected,rtol=1e-5,atol=1e-8)


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


def test_sparse_prepared_source_cache_separates_layout_slots_and_rejects_mutation(monkeypatch):
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_BYTES', str(1 << 20))
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_PINNED', '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENTS', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS', '16')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_COMPACT_TRANSPORT', '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0')
    source, split, _ = _packed_source_fixture()
    with torch.inference_mode():
        sparse = source.prepare_batch(split, 0, 2, event_compression=True)
    assert sparse.tensors['exit_prices'].shape == (2, 16)
    assert not any(torch.is_inference(t) for t in sparse.tensors.values())
    assert source.prepare_batch(split, 0, 2, event_compression=True) is sparse
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENTS', '0')
    packed = source.prepare_batch(split, 0, 2, event_compression=True)
    assert packed is not sparse and packed.packed_transport
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENTS', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS', '32')
    larger = source.prepare_batch(split, 0, 2, event_compression=True)
    assert larger is not sparse and larger.tensors['exit_prices'].shape == (2, 32)
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_COMPACT_TRANSPORT', '1')
    compact = source.prepare_batch(split, 0, 2, event_compression=True)
    assert compact is not larger and compact.sparse_transport_slots == 32
    assert compact.tensors['exit_prices'].shape == (2, 4)
    for expected, actual in zip(larger.sessions(torch.device('cpu')), compact.sessions(torch.device('cpu'))):
        for field in fields(expected):
            left, right = getattr(expected, field.name), getattr(actual, field.name)
            if isinstance(left, torch.Tensor):
                assert torch.equal(left.contiguous().view(torch.uint8), right.contiguous().view(torch.uint8))
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '1')
    bucket = source.prepare_batch(split, 0, 2, event_compression=True)
    assert bucket is not compact and bucket.sparse_transport_slots == 0
    assert bucket.tensors['exit_prices'].shape == (2, 4)
    assert bucket.sessions(torch.device('cpu'))[0].exit_prices.shape == (4,)
    assert source.prepare_batch(split, 0, 2, event_compression=True) is bucket
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_COMPACT_TRANSPORT', '0')
    larger.tensors['exit_capacity'].add_(1)
    with pytest.raises(RuntimeError, match='immutable prepared physical cache was mutated'):
        source.prepare_batch(split, 0, 2, event_compression=True)


def test_sparse_graph_schedule_versions_configuration_and_optimizer_contracts(monkeypatch):
    from stockagent.config import load_config
    from stockagent.backtest.tw_day_trade_inventory import INVENTORY_VOLUME_ADJOINT_ABI
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot, _training_checkpoint_contract
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENTS', '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0')
    cfg = load_config('configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml')
    dense_config, dense_optimizer = _configuration_fingerprint_snapshot(cfg), _training_checkpoint_contract(cfg)
    assert dense_config['training']['physical_fifo_volume_adjoint_contract'] == INVENTORY_VOLUME_ADJOINT_ABI
    assert dense_optimizer['physical_fifo_volume_adjoint_contract'] == INVENTORY_VOLUME_ADJOINT_ABI
    assert INVENTORY_VOLUME_ADJOINT_ABI == 'cumulative_closed_telescoping_volume_adjoint_v1'
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENTS', '1')
    sparse_config, sparse_optimizer = _configuration_fingerprint_snapshot(cfg), _training_checkpoint_contract(cfg)
    assert sparse_config != dense_config and sparse_optimizer != dense_optimizer
    left = sparse_config['training']['physical_fifo_explicit_graph_contract']
    right = sparse_optimizer['physical_fifo_explicit_graph_contract']
    assert left == right
    assert left['sparse_session_step'] == 'canonical_csr_liquidity_full_pending_adjoint_v1'
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_COMPACT_TRANSPORT', '1')
    packed_config, packed_optimizer = _configuration_fingerprint_snapshot(cfg), _training_checkpoint_contract(cfg)
    assert packed_config != sparse_config and packed_optimizer != sparse_optimizer
    assert packed_config['training']['physical_fifo_explicit_graph_contract'] == packed_optimizer['physical_fifo_explicit_graph_contract']
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '1')
    bucket_config, bucket_optimizer = _configuration_fingerprint_snapshot(cfg), _training_checkpoint_contract(cfg)
    assert bucket_config != packed_config and bucket_optimizer != packed_optimizer
    assert bucket_config['training']['physical_fifo_explicit_graph_contract'] == bucket_optimizer['physical_fifo_explicit_graph_contract']
    assert bucket_optimizer['physical_fifo_explicit_graph_contract']['sparse_session_transport'] == 'power_of_two_packet_device_capacity_buckets_v1'


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires sparse CUDA ledger graphs')
@pytest.mark.parametrize('adjoint', ['recompute', 'saved'])
@pytest.mark.parametrize('copy_backend', ['native', 'triton'])
@pytest.mark.parametrize('transport_mode', ['padded', 'compact', 'bucketed'])
def test_sparse_graph_preserves_dense_financial_endpoint_and_pending_vjp(monkeypatch, adjoint, copy_backend, transport_mode):
    from stockagent.backtest.tw_day_trade_carry import compact_day_trade_carry_session
    from stockagent.backtest.tw_day_trade_graph import get_explicit_carry_graph_stats
    from stockagent.training.day_trade_carry_bridge import PreparedDayTradeCarryBatch
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_CUDAGRAPHS', '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS', '4')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT', adjoint)
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_TAPE_SLOTS', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_PACKED_COPY', copy_backend)
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENTS', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS', '16')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_COMPACT_TRANSPORT', '0' if transport_mode == 'padded' else '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '1' if transport_mode == 'bucketed' else '0')
    w, dense, _, _, common = _inputs(device='cuda')
    _, cpu, _, _, _ = _inputs()
    staged = PreparedDayTradeCarryBatch.from_sessions(
        tuple(compact_day_trade_carry_session(s) for s in cpu), len(cpu),
        event_compression=True, sparse_events=True)
    sparse = staged.sessions(torch.device('cuda'))
    common['event_compression'] = True
    reference = run_day_trade_carry_sessions(w, dense, **common, _allow_flat_terminal=False)
    before = get_explicit_carry_graph_stats()
    actual = run_day_trade_carry_sessions(w, sparse, **common)
    w2 = w.detach().clone().requires_grad_()
    future_cpu = tuple(replace(s, day=s.day + 20,
        exit_prices=s.exit_prices.clone(), exit_capacity=s.exit_capacity.clone()) for s in cpu[:2])
    for session in future_cpu:
        session.exit_prices[:, 89, :] = session.exit_prices[:, 264, :]
        session.exit_capacity[:, 89, :] = session.exit_capacity[:, 264, :]
    future = PreparedDayTradeCarryBatch.from_sessions(future_cpu, 2,
        event_compression=True, sparse_events=True).sessions(torch.device('cuda'))
    if transport_mode == 'bucketed':
        assert future[0].exit_prices.numel() > sparse[0].exit_prices.numel()
    future_dense = tuple(replace(s, **{f.name: getattr(s, f.name).cuda()
        for f in fields(s) if isinstance(getattr(s, f.name), torch.Tensor)}) for s in future_cpu)
    reference2 = run_day_trade_carry_sessions(w2[:2], future_dense,
        **{**common, 'can_enter': common['can_enter'][:2]}, _allow_flat_terminal=False)
    second = run_day_trade_carry_sessions(w2[:2], future,
        **{**common, 'can_enter': common['can_enter'][:2]})
    for name in ['strategy_returns', 'turnovers', 'weights_history', 'shares_history', 'settlement_default']:
        torch.testing.assert_close(getattr(actual, name), getattr(reference, name), rtol=1e-10, atol=1e-9)
    torch.testing.assert_close(actual.minute_nav[:, -1], reference.minute_nav[:, -1], rtol=1e-12, atol=1e-7)
    for field in fields(actual.final_state.inventory):
        torch.testing.assert_close(getattr(actual.final_state.inventory, field.name),
                                   getattr(reference.final_state.inventory, field.name), rtol=1e-12, atol=1e-7)
    expected = torch.autograd.grad(reference.strategy_returns.sum(), w)[0]
    actual.strategy_returns.sum().backward(retain_graph=True)
    torch.testing.assert_close(w.grad, expected, rtol=1e-6, atol=1e-9)
    w.grad.zero_()
    actual.strategy_returns.sum().backward()
    torch.testing.assert_close(w.grad, expected, rtol=1e-6, atol=1e-9)
    second.strategy_returns.sum().backward()
    expected2, = torch.autograd.grad(reference2.strategy_returns.sum(), w2)
    torch.testing.assert_close(second.strategy_returns, reference2.strategy_returns, rtol=1e-10, atol=1e-9)
    torch.testing.assert_close(w2.grad, expected2, rtol=1e-6, atol=1e-9)
    assert torch.isfinite(w2.grad).all() and w2.grad[:2].abs().sum() > 0
    after = get_explicit_carry_graph_stats()
    assert after['forward_replays'] >= before['forward_replays'] + 2
    assert after['backward_replays'] >= before['backward_replays'] + 3
    assert after['sparse_forward_replays'] >= before['sparse_forward_replays'] + 2
    assert after['sparse_backward_replays'] >= before['sparse_backward_replays'] + 3
    if transport_mode == 'bucketed':
        assert after['sparse_bucketed_forward_replays'] >= before['sparse_bucketed_forward_replays'] + 2
    assert after['certificate_fallbacks'] == before['certificate_fallbacks']
    if copy_backend == 'triton':
        assert after['packed_field_copy_replays'] > before['packed_field_copy_replays']


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
