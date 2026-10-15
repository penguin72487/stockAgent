"""Fixed-buffer CUDA recurrence and recomputed VJP of the canonical ledger.

There is no second fill/accounting implementation here. The captured program
calls execute_carry_session. Only proved inert rows are excluded from further
execution; complete diagnostic histories are rebuilt at the public boundary.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import fields, replace
import os
import threading
import warnings

import torch
from torch import Tensor

EXPLICIT_CARRY_GRAPH_ABI = "canonical_terminal_flat_buffered_bounded_adjoint_v2"
_ENGINES = OrderedDict()
_LOCK = threading.RLock()
_STATS = dict(constructors=0, forward_replays=0, backward_replays=0,
              accepted_blocks=0, certificate_fallbacks=0, tape_constructors=0,
              saved_tape_backwards=0, recomputed_backwards=0)
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
    if os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP', '0') == '1':
        result['step'] = 'canonical_whole_session_compiled_v1'
    return result


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

    def __call__(self, values):
        from stockagent.backtest.tw_day_trade_carry import DayTradeCarryState
        w, enter, buy, sell, normal, rebate = values[:6]
        pnl, cost, cursor, decision, observed, failed, nav, alive, days, active = values[6:16]
        state = DayTradeCarryState(replace(self.empty, realized_net_pnl=pnl, carry_cost=cost,
            action_cursor=cursor, decision_day=decision, observed_day=observed, failed=failed),
            nav, alive, self.capital)
        history = [[] for _ in range(8)]
        proof = torch.ones((), dtype=torch.bool, device=w.device)
        for row in range(self.rows):
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
        return (*h, state.inventory.realized_net_pnl, state.inventory.carry_cost,
            state.inventory.action_cursor, state.inventory.decision_day,
            state.inventory.observed_day, state.inventory.failed,
            state.last_nav, state.alive, proof)


class _Engine:
    """One forward pool and one recompute+VJP pool, independent of dates."""
    @torch.inference_mode(False)
    def __init__(self, program, example):
        self.program = program
        self.inputs = tuple(v.detach().clone().contiguous() for v in example)
        self.grad_inputs = (0, 6, 7, 12)
        for i in self.grad_inputs:
            self.inputs[i].requires_grad_(True)
        self.device = example[0].device
        self.stream = torch.cuda.Stream(device=self.device)
        self.stream.wait_stream(torch.cuda.current_stream(self.device))
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
            self.inputs[14].copy_(torch.tensor(calendar, dtype=torch.float64, device='cpu'))
            self.inputs[15][:count].fill_(True);self.inputs[15][count:].fill_(False)
            padded = (*sessions, *((sessions[-1],) * (self.program.rows - count)))
            for name, destination in zip(self.program.names, self.inputs[16:]):
                torch.stack(tuple(getattr(s, name) for s in padded), out=destination)

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
            gradients = tuple(None if g is None else g.clone() for g in tape.grad_outputs)
            if ctx.lease is not None:ctx.lease.release()
        count = len(ctx.sessions)
        return (None, None, None, None, None, gradients[0][:count], *gradients[1:])


def run_flat_graph_sessions(weights, sessions, *, can_enter, buy_fee_rate,
        day_sell_fee_rate, normal_sell_fee_rate, rebate_rate, initial_capital,
        initial_state=None, event_compression=False, require_minute_nav_grad=False):
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
    state = initial_state or DayTradeCarryState.empty(weights.shape[1], initial_capital, weights.device)
    if sessions[0].day <= state.last_session_day:
        raise ValueError('ledger graph cannot repeat a committed day')
    valid = state.validate(symbols=weights.shape[1], device=weights.device, initial_capital=initial_capital)
    state = _compact_detached_carry_state(state)
    if state.inventory.cohorts.numel() or state.inventory.claims.numel():
        return None
    if not bool(valid.detach().cpu()):
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
    for begin in range(0, len(sessions), rows):
        block = sessions[begin:begin+rows];count=len(block)
        template = block[0]
        presence = tuple((f.name, getattr(template,f.name) is not None) for f in fields(template) if f.name != 'day')
        if any(tuple((f.name,getattr(s,f.name) is not None) for f in fields(s) if f.name!='day') != presence for s in block):
            return None
        key = (os.environ.get('STOCKAGENT_BACKTEST_COMPILE','1'),
               os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP','0'),
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
                engine = _Engine(program, example)
                _ENGINES[key] = engine
                # Saved autograd contexts own engines evicted from this lookup.
                while len(_ENGINES)>2:_ENGINES.popitem(last=False)
            else:
                _ENGINES.move_to_end(key)
            engine.enter_stream()
            result = _RecomputedLedger.apply(engine, block, can_enter[begin:begin+count], fees, state,
                weights[begin:begin+count], state.inventory.realized_net_pnl,
                state.inventory.carry_cost, state.last_nav)
        for destination,value in zip(histories,result[:8]):destination.append(value[:count])
        proofs.append(result[-1])
        state = DayTradeCarryState(replace(state.inventory, realized_net_pnl=result[8],carry_cost=result[9],
            action_cursor=result[10],decision_day=result[11],observed_day=result[12],failed=result[13]),
            result[14],result[15],initial_capital,block[-1].day)
    if not bool(torch.stack(proofs).all().detach().cpu()):
        _STATS['certificate_fallbacks'] += 1
        return None
    h = [torch.cat(v) for v in histories]
    claims = h[7]
    h[7] = torch.stack((claims[..., 0], claims[..., 1],
        torch.where(claims[..., 1] <= state.inventory.observed_day, 1, claims[..., 2])), dim=-1)
    state = replace(state, inventory=replace(state.inventory,cohorts=h[6],claims=h[7]))
    _STATS['accepted_blocks'] += 1
    return DayTradeCarryResult(*h[:6],state)
