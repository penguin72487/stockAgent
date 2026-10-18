"""Fixed-buffer CUDA recurrence and recomputed VJP of the canonical ledger.

There is no second fill/accounting implementation here. The captured program
calls execute_carry_session. Only proved inert rows are excluded from further
execution; complete diagnostic histories are rebuilt at the public boundary.
"""
from __future__ import annotations

from collections import OrderedDict
from contextvars import ContextVar
from dataclasses import fields, replace
import os
import threading
import warnings

import torch
from torch import Tensor

EXPLICIT_CARRY_GRAPH_ABI = "canonical_terminal_flat_cumulative_fill_transport_v6"
_VALIDATION_GATE = ContextVar('annual_ledger_validation_gate', default=None)
_CURRENT_VALIDATION_GATE = object()


class DeferredLedgerValidationError(RuntimeError):
    """Discard all speculative work before an optimizer update; retry canonically."""


class DeferredLedgerValidation:
    """Collect device predicates, then agree across ranks before any update."""

    def __init__(self, device):
        self.device, self.predicates, self.labels = torch.device(device), [], []
        self.stream = (torch.cuda.current_stream(self.device)
                       if self.device.type == 'cuda' else None)
        self.events = []
        self.records_lock = threading.Lock()
        self.finished = False

    def __enter__(self):
        if _VALIDATION_GATE.get() is not None:
            raise RuntimeError('ledger validation gates cannot be nested')
        self.token = _VALIDATION_GATE.set(self)
        return self

    def __exit__(self, *error):
        _VALIDATION_GATE.reset(self.token)
        if not error[0] and not self.finished:
            raise RuntimeError('deferred ledger validation was never observed before update')

    def record(self, predicate, label):
        if self.finished:
            raise RuntimeError('cannot append ledger checks after the update gate')
        value = predicate.detach().to(device=self.device, dtype=torch.bool)
        if value.numel() != 1:
            value = value.all()
        # Snapshot on the PRODUCER stream before a later graph replay can
        # overwrite its predicate buffer. The final gate waits on this event;
        # merely moving the clone to another stream would introduce a race.
        snapshot = value.reshape(()).clone()
        event = None
        if self.stream is not None and self.stream != torch.cuda.current_stream(self.device):
            event = torch.cuda.Event()
            event.record(torch.cuda.current_stream(self.device))
        with self.records_lock:
            self.predicates.append(snapshot)
            self.labels.append(label)
            if event is not None:
                self.events.append(event)

    def finish(self):
        if self.stream is not None and self.stream != torch.cuda.current_stream(self.device):
            torch.cuda.current_stream(self.device).wait_stream(self.stream)
        for event in self.events:
            torch.cuda.current_stream(self.device).wait_event(event)
        values = (torch.stack(self.predicates) if self.predicates
                  else torch.ones(1, device=self.device, dtype=torch.bool))
        failed = (~values.all()).to(torch.int32)
        if torch.distributed.is_initialized():
            torch.distributed.all_reduce(failed, op=torch.distributed.ReduceOp.MAX)
        self.finished = True
        if bool(failed):
            labels = [label for label, passed in zip(self.labels, values.cpu().tolist()) if not passed]
            raise DeferredLedgerValidationError(f'ledger pass rejected before update; local={labels}; peer failure also requires canonical retry')


def current_ledger_validation_gate():
    return _VALIDATION_GATE.get()


def ledger_predicate(predicate, label, *, gate=_CURRENT_VALIDATION_GATE):
    if gate is _CURRENT_VALIDATION_GATE:
        gate = _VALIDATION_GATE.get()
    if gate is None:
        return bool(predicate)
    gate.record(predicate, label)
    return True


def ledger_validation_is_deferred():
    return _VALIDATION_GATE.get() is not None


def finish_ledger_validation():
    gate = _VALIDATION_GATE.get()
    if gate is None:
        return 0
    gate.finish()
    return len(gate.predicates)
_ENGINES = OrderedDict()
_LOCK = threading.RLock()
_STATS = dict(constructors=0, forward_replays=0, backward_replays=0,
              accepted_blocks=0, certificate_fallbacks=0, tape_constructors=0,
              saved_tape_backwards=0, recomputed_backwards=0,
              packed_field_copy_replays=0, packed_field_copy_fallbacks=0,
              sparse_forward_replays=0, sparse_backward_replays=0,
              sparse_bucketed_forward_replays=0, sparse_forward_event_cells=0,
              parallel_adjoint_replays=0, parallel_adjoint_fallbacks=0)
_STATS.update(parallel_flat_forward_replays=0, parallel_flat_forward_fallbacks=0)
_STATS.update(recorded_return_candidate_replays=0, recorded_return_candidate_fallbacks=0,
              parallel_flat_deferred_certificates=0, canonical_fallback_prewarms=0)
# These outputs carry an action/NAV Jacobian. Minute histories intentionally
# retain the selected require_minute_nav_grad=False contract.
_DIFFERENTIABLE = (0, 1, 8, 9, 14)


def get_explicit_carry_graph_stats():
    return dict(_STATS)


def explicit_carry_graph_contract():
    adjoint=os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT','recompute')
    if adjoint not in ('recompute','saved'):
        raise ValueError('ledger graph adjoint must be recompute or saved')
    result = dict(version=EXPLICIT_CARRY_GRAPH_ABI,adjoint=adjoint)
    from stockagent.backtest.tw_inventory_scan import POSITIVE_PREFIX_ABI
    result['positive_prefix'] = POSITIVE_PREFIX_ABI
    parallel = os.environ.get('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '0')
    prewarm = os.environ.get('STOCKAGENT_DAY_TRADE_PREWARM_CANONICAL_FALLBACK', '0')
    if prewarm not in ('0', '1'):
        raise ValueError('canonical fallback prewarm must be 0 or 1')
    recorded = os.environ.get('STOCKAGENT_DAY_TRADE_RECORDED_RETURN_CANDIDATE', '0')
    deferred_parallel = os.environ.get('STOCKAGENT_DAY_TRADE_DEFER_PARALLEL_FORWARD_CERTIFICATE', '0')
    if recorded not in ('0', '1') or deferred_parallel not in ('0', '1'):
        raise ValueError('recorded candidates and parallel certificate deferral must be 0 or 1')
    if parallel not in ('0', '1', '2'):
        raise ValueError('parallel day adjoint must be 0, 1 or 2')
    if parallel != '0' and prewarm == '1':
        result['canonical_fallback_warmup'] = 'forward_and_vjp_before_parallel_replay_v1'
    if parallel == '1':
        result['parallel_state_adjoint'] = 'canonical_serial_forward_parallel_day_vjp_affine_prefix_v1'
    elif parallel == '2':
        rows = int(os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS', '16'))
        if rows <= 0 or rows > 64 or rows & (rows-1):
            raise ValueError('parallel graph rows must be a power of two up to 64')
        result['parallel_state_adjoint'] = 'canonical_parallel_flat_funding_certificate_affine_v2'
        result['parallel_forward_iterations'] = 4
        result['parallel_forward_candidate'] = 'canonical_pnl_only_day_maps_v1'
        result['parallel_funding_certificate'] = 'unfunded_target_and_funded_quantities_interval_v1'
        result['parallel_financial_interval'] = 'solvency_and_unclamped_log_return_interval_v1'
        result['parallel_map_block_rows'] = rows
        result['parallel_financial_tolerance_twd'] = 1e-6
        result['parallel_funding_interval_twd'] = rows*1e-6
        if recorded == '1':
            result['parallel_replay_candidate'] = 'recorded_daily_log_returns_full_funding_certificate_v1'
        if deferred_parallel == '1':
            result['parallel_forward_validation'] = 'whole_pass_explicit_gate_before_update_v1'
    if os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP', '0') == '1':
        result['step'] = 'canonical_whole_session_compiled_v1'
    if os.environ.get('STOCKAGENT_DAY_TRADE_DEFER_VALIDATION', '0') == '1':
        result['validation'] = 'whole_pass_explicit_stream_owned_gate_rank_agreement_canonical_retry_v2'
    from stockagent.backtest.tw_inventory_scan import inventory_prefix_contract
    prefix = inventory_prefix_contract()
    if prefix is not None:
        result['prefix_sum'] = prefix
    from stockagent.backtest.tw_day_trade_graph_transport import packed_session_copy_contract
    transport = packed_session_copy_contract()
    if transport is not None:
        result['packed_session_copy'] = transport
    if os.environ.get('STOCKAGENT_DAY_TRADE_SPARSE_EVENTS', '0').lower() not in ('0', 'false', 'off', 'no'):
        result['sparse_session_step'] = 'canonical_csr_liquidity_full_pending_adjoint_v1'
        if os.environ.get('STOCKAGENT_DAY_TRADE_SPARSE_COMPACT_TRANSPORT', '0') == '1':
            result['sparse_session_transport'] = 'packed_events_fixed_device_capacity_v1'
            if os.environ.get('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0') == '1':
                result['sparse_session_transport'] = 'power_of_two_packet_device_capacity_buckets_v1'
                result['sparse_engine_cache'] = 'eight_shape_banks_pending_tapes_keep_evicted_owners_v1'
        elif os.environ.get('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0') == '1':
            raise ValueError('sparse capacity buckets require compact sparse transport')
    return result


def _copy_session_field_(destination, values, *, combined=None):
    """Copy consecutive row views once, retaining exact fallback and tail data."""
    first = values[0]
    count = len(values)
    if combined is not None:
        # Native cat is preferable for padded/strided planes. The source
        # owner proves row identity once per block, not per day and field.
        if combined.is_contiguous():
            destination[:count].copy_(combined)
            if count < destination.size(0):
                destination[count:].copy_(values[-1])
        else:
            padded = (*values, *((values[-1],) * (destination.size(0) - count)))
            torch.stack(padded, out=destination)
        return
    offset = first.storage_offset()
    step = values[1].storage_offset() - offset if count > 1 else 0
    storage = first.untyped_storage().data_ptr()
    consecutive = step >= 0 and all(
        value.device == first.device and value.dtype == first.dtype
        and value.shape == first.shape and value.stride() == first.stride()
        and value.untyped_storage().data_ptr() == storage
        and value.storage_offset() == offset + index * step
        for index, value in enumerate(values))
    if consecutive:
        combined = first.as_strided((count, *first.shape), (step, *first.stride()), offset)
        destination[:count].copy_(combined)
        if count < destination.size(0):
            destination[count:].copy_(values[-1])
    else:
        padded = (*values, *((values[-1],) * (destination.size(0) - count)))
        torch.stack(padded, out=destination)


def _inventory_row_token(state):
    values = (state.inventory.cohorts, state.inventory.claims)
    if any(torch.is_inference(value) for value in values):
        return None
    return tuple((id(value), value._version) for value in values)


def detached_carry_state(state, *, device=None):
    """Drop certified inert rows only at an explicitly detached BPTT boundary.

    The public graph result retains the complete canonical inventory history.
    Its successful financial certificate already proves these rows carry no
    shares, entry cost, locked shares or claim amounts. Re-scanning and sorting
    the same rows before the next block adds synchronization but no information.
    Tensor mutation invalidates the certificate and uses ordinary detachment.
    """
    token = _inventory_row_token(state)
    certificate = getattr(state, '_graph_empty_inventory_rows', None)
    if (token is not None and certificate is not None and certificate[0] == token
            and (certificate[1] is None or certificate[1] is _VALIDATION_GATE.get())):
        state = replace(state, inventory=replace(state.inventory,
            cohorts=state.inventory.cohorts[:0], claims=state.inventory.claims[:0]))
    return state.detached(device=device)


class _Program:
    @torch.inference_mode(False)
    def __init__(self, template, rows, symbols, capital, event_compression=False):
        from stockagent.backtest.tw_day_trade_inventory import DayTradeInventoryState
        self.rows, self.capital = rows, capital
        self.event_compression=bool(event_compression)
        self.names = tuple(f.name for f in fields(template)
                           if isinstance(getattr(template, f.name), Tensor))
        # Engines outlive individual batches. Retain schema/optional policy
        # only: keeping one row view here pins its entire field-major GPU tape.
        self.session_type = type(template)
        self.session_defaults = {f.name: getattr(template, f.name) for f in fields(template)
            if f.name != 'day' and f.name not in self.names}
        self.empty = DayTradeInventoryState.empty(symbols, device=template.official_open.device)
        self.step = self._step
        if (os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP', '0') == '1'
                and os.environ.get('STOCKAGENT_BACKTEST_COMPILE', '1').lower() not in ('0', 'false', 'off')):
            from stockagent.backtest.tw_day_trade_carry import (
                _carry_compile_options, _compile_isolated_code_object)
            import types
            function = _compile_isolated_code_object(type(self)._step,
                name=f'canonical_graph_step_{id(self)}')
            self.step = torch.compile(types.MethodType(function, self),
                fullgraph=True, dynamic=False, options=_carry_compile_options())

    def _step(self, state, session, w, enter, buy, sell, normal, rebate, active):
        from stockagent.backtest.tw_day_trade_carry import DayTradeCarryState, execute_carry_session
        from stockagent.backtest.tw_day_trade_inventory import CohortField
        from stockagent.backtest.simulator import _portfolio_simple_returns_to_log_torch
        after, curve, notional = execute_carry_session(state, session,
            weights=w, can_enter=enter, buy_fee_rate=buy,
            day_sell_fee_rate=sell, normal_sell_fee_rate=normal,
            rebate_rate=rebate, initial_capital=self.capital,
            event_compression=self.event_compression, require_minute_nav_grad=False)
        c, claims = after.inventory.cohorts, after.inventory.claims
        proof = ((c[..., CohortField.SHARES] == 0).all()
            & (c[..., CohortField.ENTRY_COST] == 0).all()
            & (c[..., CohortField.LOCKED_SHARES] == 0).all()
            & (claims[..., 0] == 0).all() & (after.inventory.failed == 0)
            & after.alive & torch.isfinite(curve).all() & (curve > 0).all())
        ratio = after.last_nav / state.last_nav.clamp_min(1e-30)
        ret = torch.where(torch.isfinite(ratio), torch.where(state.alive,
            _portfolio_simple_returns_to_log_torch(ratio - 1), 0), float('nan'))
        shares = after.inventory.shares
        price = torch.where(torch.isfinite(session.terminal_liquidation_price)
            & (session.terminal_liquidation_price > 0),
            session.terminal_liquidation_price, session.marks[:, -1])
        exposure = torch.where(after.alive, shares * torch.nan_to_num(price, nan=0)
                               / after.last_nav.clamp_min(1e-30), 0)
        claim = claims[-1] if claims.shape[0] else self.empty.action_cursor[:, None].expand(-1, 3)
        history = (ret, notional / state.last_nav.clamp_min(1e-30), exposure,
                   shares, curve, ~after.alive, c[-1], claim)
        inv = replace(self.empty, **{name: torch.where(active,
            getattr(after.inventory, name), getattr(state.inventory, name))
            for name in ('realized_net_pnl', 'carry_cost', 'action_cursor',
                         'decision_day', 'observed_day', 'failed')})
        next_state = DayTradeCarryState(inv, torch.where(active, after.last_nav, state.last_nav),
            torch.where(active, after.alive, state.alive), self.capital)
        return next_state, history, (~active | proof)

    def __call__(self, values, *, record_states=False):
        from stockagent.backtest.tw_day_trade_carry import DayTradeCarryState
        w, enter, buy, sell, normal, rebate = values[:6]
        pnl, cost, cursor, decision, observed, failed, nav, alive, days, active = values[6:16]
        state = DayTradeCarryState(replace(self.empty, realized_net_pnl=pnl, carry_cost=cost,
            action_cursor=cursor, decision_day=decision, observed_day=observed, failed=failed),
            nav, alive, self.capital)
        history = [[] for _ in range(8)]
        trace = [[] for _ in range(8)] if record_states else None
        proof = torch.ones((), dtype=torch.bool, device=w.device)
        for row in range(self.rows):
            if record_states:
                for target, value in zip(trace, (state.inventory.realized_net_pnl,
                        state.inventory.carry_cost, state.inventory.action_cursor,
                        state.inventory.decision_day, state.inventory.observed_day,
                        state.inventory.failed, state.last_nav, state.alive)):
                    target.append(value)
            session = self.session_type(day=days[row], **self.session_defaults, **{
                name: tensor[row] for name, tensor in zip(self.names, values[16:])})
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=UserWarning,
                    message=r'The \.grad attribute of a Tensor that is not a leaf Tensor.*')
                state, row_history, row_proof = self.step(state, session,
                    w[row], enter[row], buy, sell, normal, rebate, active[row])
            proof = proof & row_proof
            for target, value in zip(history, row_history):
                target.append(value)
        h = [torch.stack(v) for v in history]
        # Settled zero-amount claims retain their canonical payment flags.
        claim = h[7]
        h[7] = torch.stack((claim[..., 0], claim[..., 1],
            torch.where(claim[..., 1] <= state.inventory.observed_day, 1, claim[..., 2])), dim=-1)
        result = (*h, state.inventory.realized_net_pnl, state.inventory.carry_cost,
            state.inventory.action_cursor, state.inventory.decision_day,
            state.inventory.observed_day, state.inventory.failed,
            state.last_nav, state.alive, proof)
        return (result, tuple(torch.stack(v) for v in trace)) if record_states else result


class _Engine:
    """One forward pool and one recompute+VJP pool, independent of dates."""
    @torch.inference_mode(False)
    def __init__(self, program, example, *, capture=True):
        self.program = program
        self.inputs = tuple(v.detach().clone().contiguous() for v in example)
        self.grad_inputs = (0, 6, 7, 12)
        for i in self.grad_inputs:
            self.inputs[i].requires_grad_(True)
        self.device = example[0].device
        self.stream = torch.cuda.Stream(device=self.device)
        self.stream.wait_stream(torch.cuda.current_stream(self.device))
        if capture:
            with torch.cuda.stream(self.stream), torch.no_grad():
                for _ in range(2):
                    self.outputs = program(self.inputs)
            self.stream.synchronize()
            self.forward = torch.cuda.CUDAGraph()
            with torch.cuda.graph(self.forward, stream=self.stream), torch.no_grad():
                self.outputs = program(self.inputs)
            self.stream.synchronize()
        self.backward = None
        self.vjp = None
        self.tapes = []
        self.calendars = OrderedDict()
        self.last_stream = torch.cuda.current_stream(self.device)
        _STATS['constructors'] += 1

    def enter_stream(self):
        current = torch.cuda.current_stream(self.device)
        if current != self.last_stream:
            current.wait_stream(self.last_stream)
            self.last_stream = current

    def copy(self, w, state, sessions, enter, fees):
        inv = state.inventory
        with torch.no_grad():
            count = len(sessions)
            # Shape-stable tail: repeated source rows are inactive. They never
            # advance money, calendar, ownership or real-row output coverage.
            self.inputs[0][:count].copy_(w)
            self.inputs[1][:count].copy_(enter)
            if count < self.program.rows:
                self.inputs[0][count:].zero_();self.inputs[1][count:].zero_()
            for destination, value in zip(self.inputs[2:6], fees):
                destination.copy_(value)
            for destination, value in zip(self.inputs[6:14],
                (inv.realized_net_pnl, inv.carry_cost, inv.action_cursor,
                 inv.decision_day, inv.observed_day, inv.failed, state.last_nav, state.alive)):
                destination.copy_(value)
            calendar = [s.day for s in sessions]
            # Even inactive work must have a finite primal/adjoint: repeating
            # the last committed decision day produces a failed candidate and
            # 0*NaN can leak through its masked VJP. Synthetic inactive ordinals
            # are internal scheduling only; no row or calendar is published.
            calendar += [calendar[-1] + i + 1 for i in range(self.program.rows - count)]
            calendar_key = tuple(calendar)
            cached_calendar = self.calendars.get(calendar_key)
            if cached_calendar is None:
                cached_calendar = torch.tensor(calendar, dtype=torch.float64, device=self.device)
                self.calendars[calendar_key] = cached_calendar
                while len(self.calendars) > 256:
                    self.calendars.popitem(last=False)
            else:
                self.calendars.move_to_end(calendar_key)
            self.inputs[14].copy_(cached_calendar)
            self.inputs[15][:count].fill_(True);self.inputs[15][count:].fill_(False)
            packed = getattr(sessions[0], '_transport_fields', None)
            row = getattr(sessions[0], '_transport_row', -1)
            if packed is not None and not all(
                    getattr(session, '_transport_fields', None) is packed
                    and getattr(session, '_transport_row', None) == row + index
                    for index, session in enumerate(sessions)):
                packed = None
            from stockagent.backtest.tw_day_trade_graph_transport import (
                packed_session_copy_contract, copy_packed_session_fields_)
            if packed is not None and packed_session_copy_contract() is not None:
                copied = copy_packed_session_fields_(self.inputs[16:],
                    tuple(packed[name] for name in self.program.names),
                    source_row=row, rows=count,
                    cache=self.__dict__.setdefault('_packed_field_copy_tables', OrderedDict()))
                if copied:
                    _STATS['packed_field_copy_replays'] += 1
                    return
                _STATS['packed_field_copy_fallbacks'] += 1
            for name, destination in zip(self.program.names, self.inputs[16:]):
                _copy_session_field_(destination, tuple(getattr(s, name) for s in sessions),
                    combined=None if packed is None else packed[name][row:row+count])

    @torch.inference_mode(False)
    def prepare_backward(self):
        if self.backward is not None:
            return
        self.stream.wait_stream(torch.cuda.current_stream(self.device))
        differentiable_inputs = tuple(self.inputs[i] for i in self.grad_inputs)
        def recompute():
            with torch.enable_grad():
                out = self.program(self.inputs)
                return torch.autograd.grad(tuple(out[i] for i in _DIFFERENTIABLE),
                    differentiable_inputs, grad_outputs=self.vjp, allow_unused=True)
        with torch.cuda.stream(self.stream):
            with torch.enable_grad():
                sample = self.program(self.inputs)
            self.vjp = tuple(torch.zeros_like(sample[i]) for i in _DIFFERENTIABLE)
            del sample
            for _ in range(2):
                self.grad_outputs = recompute()
        self.stream.synchronize()
        self.backward = torch.cuda.CUDAGraph()
        with torch.cuda.graph(self.backward, stream=self.stream):
            self.grad_outputs = recompute()
        self.stream.synchronize()
        torch.cuda.current_stream(self.device).wait_stream(self.stream)

    def acquire_tape(self):
        if explicit_carry_graph_contract()['adjoint']!='saved':
            return None
        for tape in self.tapes:
            if not tape.active:
                return _TapeLease(tape)
        limit=int(os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_TAPE_SLOTS','8'))
        if not 1<=limit<=16:
            raise ValueError('ledger graph tape slots must be between 1 and 16')
        if len(self.tapes)>=limit:
            return None
        tape=_SavedTape(self)
        self.tapes.append(tape)
        return _TapeLease(tape)


class _SavedTape:
    """A private forward tape; source buffers are restored before its VJP."""
    @torch.inference_mode(False)
    def __init__(self, engine):
        self.active=False
        self.vjp=tuple(torch.zeros_like(engine.outputs[i]) for i in _DIFFERENTIABLE)
        inputs=tuple(engine.inputs[i] for i in engine.grad_inputs)
        stream=engine.stream
        stream.wait_stream(torch.cuda.current_stream(engine.device))
        with torch.cuda.stream(stream):
            for _ in range(2):
                with torch.enable_grad():
                    out=engine.program(engine.inputs)
                    torch.autograd.grad(tuple(out[i] for i in _DIFFERENTIABLE),inputs,
                        grad_outputs=self.vjp,allow_unused=True)
                del out
        stream.synchronize()
        self.forward=torch.cuda.CUDAGraph()
        with torch.cuda.graph(self.forward,stream=stream),torch.enable_grad():
            self.outputs=engine.program(engine.inputs)
        # Fill saved tensors once before recording the separate backward.
        with torch.cuda.stream(stream):self.forward.replay()
        stream.synchronize()
        self.backward=torch.cuda.CUDAGraph()
        with torch.cuda.graph(self.backward,stream=stream),torch.enable_grad():
            self.grad_outputs=torch.autograd.grad(
                tuple(self.outputs[i] for i in _DIFFERENTIABLE),inputs,
                grad_outputs=self.vjp,allow_unused=True)
        stream.synchronize()
        torch.cuda.current_stream(engine.device).wait_stream(stream)
        _STATS['tape_constructors']+=1


class _TapeLease:
    def __init__(self,tape):
        self.tape=tape;self.released=False;tape.active=True

    def release(self):
        if not self.released:
            self.tape.active=False;self.released=True

    def __del__(self):
        self.release()


class _RecomputedLedger(torch.autograd.Function):
    @staticmethod
    def forward(ctx, engine, sessions, can_enter, fees, state, w, pnl, cost, nav):
        ctx.engine, ctx.sessions, ctx.can_enter, ctx.fees, ctx.state = engine, sessions, can_enter, fees, state
        ctx.save_for_backward(w, pnl, cost, nav)
        engine.copy(w, state, sessions, can_enter, fees)
        # In inference no backward owns a tape. Autograd Functions always run
        # forward under no_grad, so use the caller's input contract here.
        ctx.lease=engine.acquire_tape() if w.requires_grad else None
        tape=ctx.lease.tape if ctx.lease is not None else engine
        tape.forward.replay()
        _STATS['forward_replays'] += 1
        if sessions[0].uses_sparse_events:
            _STATS['sparse_forward_replays'] += 1
            _STATS['sparse_forward_event_cells'] += sessions[0].exit_prices.numel()*len(sessions)
            if os.environ.get('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0') == '1':
                _STATS['sparse_bucketed_forward_replays'] += 1
        result = tuple(v.clone() for v in tape.outputs)
        ctx.mark_non_differentiable(*(v for i, v in enumerate(result) if i not in _DIFFERENTIABLE))
        ctx.set_materialize_grads(False)
        return result

    @staticmethod
    def backward(ctx, *grad_outputs):
        engine = ctx.engine
        w, pnl, cost, nav = ctx.saved_tensors
        with _LOCK:
            engine.enter_stream()
            engine.copy(w, ctx.state, ctx.sessions, ctx.can_enter, ctx.fees)
            if ctx.lease is not None and not ctx.lease.released:
                tape=ctx.lease.tape
                _STATS['saved_tape_backwards']+=1
            else:
                engine.prepare_backward()
                tape=engine
                _STATS['recomputed_backwards']+=1
            with torch.no_grad():
                for destination, i in zip(tape.vjp, _DIFFERENTIABLE):
                    g = grad_outputs[i]
                    if g is None:destination.zero_()
                    else:destination.copy_(g)
            tape.backward.replay()
            _STATS['backward_replays'] += 1
            if ctx.sessions[0].uses_sparse_events:
                _STATS['sparse_backward_replays'] += 1
            gradients = tuple(None if g is None else g.clone() for g in tape.grad_outputs)
            if ctx.lease is not None:ctx.lease.release()
        count = len(ctx.sessions)
        return (None, None, None, None, None, gradients[0][:count], *gradients[1:])


def run_flat_graph_sessions(weights, sessions, *, can_enter, buy_fee_rate,
        day_sell_fee_rate, normal_sell_fee_rate, rebate_rate, initial_capital,
        initial_state=None, event_compression=False, require_minute_nav_grad=False,
        replay_log_returns=None):
    from stockagent.backtest.tw_day_trade_carry import (
        DayTradeCarryResult, DayTradeCarryState, _compact_detached_carry_state)
    # Inference still returns the complete minute curve. A caller requesting
    # its Jacobian must use the canonical differentiable minute executor.
    if require_minute_nav_grad and torch.is_grad_enabled():
        return None
    if os.environ.get('STOCKAGENT_DAY_TRADE_CUDAGRAPHS', '0') not in ('0', 'false', 'off'):
        raise ValueError('Explicit ledger graphs require inner Inductor graph trees disabled')
    if weights.shape != can_enter.shape or len(sessions) != weights.shape[0]:
        raise ValueError('ledger graph actions/permissions/source rows must agree')
    if any(a.day >= b.day for a, b in zip(sessions, sessions[1:])):
        raise ValueError('ledger graph sessions must be chronological')
    if any(s.uses_sparse_events for s in sessions) and (
            not event_compression or not all(s.uses_sparse_events for s in sessions)):
        return None
    # This custom adjoint owns actions and recurrent money only. Preserve the
    # canonical Jacobian for callers learning fees or source observations, and
    # do not reuse a graph with a different fee buffer shape/dtype/device.
    fees = (buy_fee_rate, day_sell_fee_rate, normal_sell_fee_rate, rebate_rate)
    if any(not isinstance(f, Tensor) or f.shape != weights.shape[1:]
           or f.dtype != torch.float64 or f.device != weights.device
           or f.requires_grad for f in fees):
        return None
    if any(value.requires_grad for session in sessions for field in fields(session)
           if isinstance(value := getattr(session, field.name), Tensor)):
        return None
    if (os.environ.get('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '0') == '2'
            and any(session.entry_path is not None for session in sessions)):
        return None
    state = initial_state or DayTradeCarryState.empty(weights.shape[1], initial_capital, weights.device)
    if sessions[0].day <= state.last_session_day:
        raise ValueError('ledger graph cannot repeat a committed day')
    valid = state.validate(symbols=weights.shape[1], device=weights.device, initial_capital=initial_capital)
    state = _compact_detached_carry_state(state)
    if state.inventory.cohorts.numel() or state.inventory.claims.numel():
        return None
    if not ledger_predicate(valid, 'graph_continuing_account_state'):
        return None
    for session in sessions:
        session.validate_shape(weights.shape[1], weights.device)
    rows = int(os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS', '16'))
    if rows <= 0 or rows > 64 or rows & (rows-1):
        raise ValueError('ledger graph rows must be a power of two up to 64')
    if sessions[-1].day + rows > 3652059:
        return None
    histories = [[] for _ in range(8)]
    proofs = []
    contract = explicit_carry_graph_contract()
    parallel = contract.get('parallel_state_adjoint')
    for begin in range(0, len(sessions), rows):
        block = sessions[begin:begin+rows];count=len(block)
        template = block[0]
        presence = tuple((f.name, getattr(template,f.name) is not None) for f in fields(template) if f.name != 'day')
        if any(tuple((f.name,getattr(s,f.name) is not None) for f in fields(s) if f.name!='day') != presence for s in block):
            return None
        key = (os.environ.get('STOCKAGENT_BACKTEST_COMPILE','1'),
               os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP','0'),
               parallel, contract.get('parallel_replay_candidate'),
               contract.get('canonical_fallback_warmup'),
               contract.get('prefix_sum'),
               contract.get('packed_session_copy', {}).get('version'),
               weights.device, weights.dtype, can_enter.dtype, weights.shape[1], rows,
               float(initial_capital), bool(event_compression), presence,
               tuple((name, tuple(getattr(template,name).shape), getattr(template,name).dtype)
                     for name, present in presence if present))
        with _LOCK:
            engine = _ENGINES.get(key)
            if engine is None:
                program = _Program(template, rows, weights.shape[1], initial_capital, event_compression)
                inv = state.inventory
                example = (weights.new_zeros((rows,weights.shape[1])), can_enter.new_zeros((rows,weights.shape[1])),
                    *fees, inv.realized_net_pnl, inv.carry_cost, inv.action_cursor,
                    inv.decision_day, inv.observed_day, inv.failed, state.last_nav, state.alive,
                    torch.tensor([s.day for s in block]+[block[-1].day+i+1 for i in range(rows-count)], device=weights.device,dtype=torch.float64),
                    torch.arange(rows,device=weights.device)<count,
                    *(torch.stack(tuple(getattr(s,name) for s in (*block,*((block[-1],)*(rows-count))))) for name in program.names))
                if parallel:
                    from stockagent.backtest.tw_day_trade_parallel import (
                        ParallelDayAdjointEngine, ParallelFlatForwardEngine)
                    engine_type = (ParallelFlatForwardEngine if
                        os.environ.get('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '0') == '2'
                        else ParallelDayAdjointEngine)
                    engine = engine_type(program, example)
                else:
                    engine = _Engine(program, example)
                _ENGINES[key] = engine
            else:
                _ENGINES.move_to_end(key)
            # Sparse packet capacities span a few powers of two. Keeping
            # only two banks would recapture common shapes every epoch.
            # Pending autograd contexts retain any evicted engine and tapes.
            limit = (8 if template.uses_sparse_events and
                os.environ.get('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0') == '1' else 2)
            while len(_ENGINES)>limit:_ENGINES.popitem(last=False)
            engine.enter_stream()
            if parallel:
                from stockagent.backtest.tw_day_trade_parallel import (
                    ParallelDayLedger, ParallelFlatLedger)
                operation = (ParallelFlatLedger if
                    os.environ.get('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '0') == '2'
                    else ParallelDayLedger)
            else:
                operation = _RecomputedLedger
            if os.environ.get('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '0') == '2':
                engine.set_return_candidate(
                    replay_log_returns[begin:begin+count]
                    if replay_log_returns is not None and contract.get('parallel_replay_candidate') else None)
            result = operation.apply(engine, block, can_enter[begin:begin+count], fees, state,
                weights[begin:begin+count], state.inventory.realized_net_pnl,
                state.inventory.carry_cost, state.last_nav)
        for destination,value in zip(histories,result[:8]):destination.append(value[:count])
        proofs.append(result[-1])
        state = DayTradeCarryState(replace(state.inventory, realized_net_pnl=result[8],carry_cost=result[9],
            action_cursor=result[10],decision_day=result[11],observed_day=result[12],failed=result[13]),
            result[14],result[15],initial_capital,block[-1].day)
    if not ledger_predicate(torch.stack(proofs).all(), 'graph_terminal_flat_financial_certificate'):
        _STATS['certificate_fallbacks'] += 1
        return None
    h = [torch.cat(v) for v in histories]
    claims = h[7]
    h[7] = torch.stack((claims[..., 0], claims[..., 1],
        torch.where(claims[..., 1] <= state.inventory.observed_day, 1, claims[..., 2])), dim=-1)
    state = replace(state, inventory=replace(state.inventory,cohorts=h[6],claims=h[7]))
    token = _inventory_row_token(state)
    if token is not None:
        # Assigned only after every active session's canonical financial proof
        # was observed above. The full result tensors remain unchanged.
        object.__setattr__(state, '_graph_empty_inventory_rows', (token, _VALIDATION_GATE.get()))
    _STATS['accepted_blocks'] += 1
    return DayTradeCarryResult(*h[:6],state)
