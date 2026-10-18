"""Parallel canonical day maps with certified financial states and affine VJPs.

The serial mode records exact entering money/calendar states. The parallel
forward mode proposes balances, certifies the monetary recurrence and opening
quantities over its full error interval, then falls back to the serial graph
when certification fails. Both use the original day executor and an FP64
affine prefix to transport return and terminal-state adjoints. The ordinary
canonical VJP handles unsupported states and turnover penalties.
"""
from __future__ import annotations

from dataclasses import replace
from collections import OrderedDict
import hashlib
import os
import types
import warnings

import torch

from stockagent.backtest.tw_day_trade_graph import (
    _DIFFERENTIABLE, _Engine, _LOCK, _STATS, ledger_predicate,
    current_ledger_validation_gate)
from stockagent.backtest.tw_inventory_scan import (
    inventory_column_stack, inventory_prefix_sum, inventory_searchsorted,
    inventory_positive_prefix)

_METHOD_GENERATIONS = OrderedDict()


def _compiled_day_method(owner, function, label):
    """Isolate frame guards per owner while keeping fresh-process names stable.

    Sharing one code object across capacity banks exceeds Dynamo's recompile
    limit. Names based on geometry plus its local generation avoid that clash
    and avoid baking a process address into the persistent compile signature.
    This bounded metadata table retains no source or device tensors.
    """
    from stockagent.backtest.tw_day_trade_carry import (
        _carry_compile_options, _compile_isolated_code_object)
    from stockagent.backtest.tw_inventory_scan import inventory_prefix_contract
    options = _carry_compile_options()
    signature = (label, 'physical_inventory_parallel_batch_boundaries_v1',
        owner.program.capital, owner.program.event_compression,
        inventory_prefix_contract(), tuple(sorted(options.items())),
        tuple((tuple(v.shape), tuple(v.stride()), str(v.dtype), str(v.device)) for v in owner.inputs))
    digest = hashlib.sha256(repr(signature).encode()).hexdigest()[:20]
    generation = _METHOD_GENERATIONS.get(digest, 0)
    _METHOD_GENERATIONS[digest] = generation+1
    _METHOD_GENERATIONS.move_to_end(digest)
    while len(_METHOD_GENERATIONS) > 128:
        _METHOD_GENERATIONS.popitem(last=False)
    isolated = _compile_isolated_code_object(function, name=f'{label}_{digest}_{generation}')
    return torch.compile(types.MethodType(isolated, owner), fullgraph=True,
                         dynamic=False, options=options)


def _batch_axis(value, dim, count):
    return (value.movedim(dim, 0) if dim is not None
            else value.unsqueeze(0).expand(count, *value.shape))


@torch.library.register_vmap(inventory_column_stack)
def _stack_batch(info, in_dims, columns):
    return inventory_column_stack([_batch_axis(v, d, info.batch_size)
        for v, d in zip(columns, in_dims[0])]), 0


@torch.library.register_vmap(inventory_prefix_sum)
def _prefix_batch(info, in_dims, value, dim):
    logical_axis = dim % (value.ndim-(in_dims[0] is not None))
    batched = _batch_axis(value, in_dims[0], info.batch_size)
    # Keep the bounded short-axis kernel available after adding the day axis.
    if logical_axis+1 == batched.ndim-1 and batched.ndim > 2:
        return inventory_prefix_sum(batched.reshape(-1, batched.size(-1)), 1).reshape(batched.shape), 0
    return inventory_prefix_sum(batched, logical_axis+1), 0


@torch.library.register_vmap(inventory_positive_prefix)
def _positive_prefix_batch(info, in_dims, value, dim=0):
    logical_axis = dim % (value.ndim-(in_dims[0] is not None))
    batched = _batch_axis(value, in_dims[0], info.batch_size)
    return inventory_positive_prefix(batched, logical_axis+1), 0


@torch.library.custom_op('stockagent::inventory_parallel_day_searchsorted_v1', mutates_args=())
def _batched_searchsorted(boundaries: torch.Tensor, values: torch.Tensor, right: bool) -> torch.Tensor:
    # The physical [day,event] boundary needs a flattened query per day.
    # Keep adaptation behind the same opaque native lookup boundary.
    if boundaries.ndim == 2:
        return inventory_searchsorted(boundaries, values.reshape(boundaries.size(0), -1), right).reshape(values.shape)
    return inventory_searchsorted(boundaries, values, right)


@_batched_searchsorted.register_fake
def _batched_searchsorted_fake(boundaries, values, right):
    return values.new_empty(values.shape, dtype=torch.int64)


@torch.library.register_vmap(inventory_searchsorted)
def _search_batch(info, in_dims, boundaries, values, right):
    boundaries = _batch_axis(boundaries, in_dims[0], info.batch_size).contiguous()
    values = _batch_axis(values, in_dims[1], info.batch_size).contiguous()
    return _batched_searchsorted(boundaries, values, right), 0


def affine_day_adjoint(beta, terminal_pnl, terminal_cost, terminal_nav,
                       state_jacobian, action_jacobian, after_nav, initial_nav):
    """VJP of log returns and independent final PNL/cost/NAV state outputs.

    In a certified flat block C remains constant and NAV=capital+P-C.
    Translating incoming P and C together leaves all trading decisions and
    returns unchanged; it translates final P and C equally. Initial last-NAV
    remains an independent input, as required by chunk-to-chunk BPTT.
    """
    following = torch.cat((beta[1:], torch.zeros_like(beta[:1])))
    local = (beta-following)/after_nav
    local = torch.cat((local[:-1], local[-1:]+terminal_pnl+terminal_nav))
    product = state_jacobian.cumprod(0)
    adjoint = (local*product).flip((0,)).cumsum(0).flip((0,))/product
    pnl = state_jacobian[0]*adjoint[0]
    gradients = ((adjoint[:, None]*action_jacobian.double()).to(action_jacobian.dtype),
        pnl, terminal_pnl+terminal_cost-pnl, -beta[0]/initial_nav)
    valid = (torch.isfinite(product).all() & (product != 0).all()
             & torch.isfinite(gradients[0]).all())
    return gradients, valid


def _canonical_fallback_engine(owner):
    if owner.serial_fallback is None:
        owner.serial_fallback = _Engine(owner.program, owner.inputs)
    return owner.serial_fallback


def _prewarm_canonical_fallback(owner):
    # Funding/adjoint rejection depends on the learned actions, not just shape.
    # Compile and capture the unchanged executor before the first replay so a
    # newly visited lot boundary cannot introduce compilation into steady epochs.
    # Keep the private buffers: pending parallel tapes must never be overwritten.
    if os.environ.get('STOCKAGENT_DAY_TRADE_PREWARM_CANONICAL_FALLBACK', '0') == '1':
        _canonical_fallback_engine(owner).prepare_backward()
        _STATS['canonical_fallback_prewarms'] += 1


class ParallelDayAdjointEngine(_Engine):
    @torch.inference_mode(False)
    def __init__(self, program, example):
        super().__init__(program, example, capture=False)
        self.serial_fallback = None
        with torch.cuda.stream(self.stream), torch.no_grad():
            for _ in range(2):
                self.outputs, self.trace = program(self.inputs, record_states=True)
        self.stream.synchronize()
        self.forward = torch.cuda.CUDAGraph()
        with torch.cuda.graph(self.forward, stream=self.stream), torch.no_grad():
            self.outputs, self.trace = program(self.inputs, record_states=True)
        self.stream.synchronize()
        _prewarm_canonical_fallback(self)

    def _one_day(self, weight, enter, day, source, trace, active, fees):
        from stockagent.backtest.tw_day_trade_carry import DayTradeCarryState
        p = self.program
        session = p.session_type(day=day, **p.session_defaults,
                                **dict(zip(p.names, source)))
        state = DayTradeCarryState(replace(p.empty, **dict(zip(
            ('realized_net_pnl', 'carry_cost', 'action_cursor', 'decision_day',
             'observed_day', 'failed'), trace[:6]))), trace[6], trace[7], p.capital)
        after, history, proof = p._step(state, session, weight, enter, *fees, active)
        return after.inventory.realized_net_pnl, after.inventory.carry_cost, after.last_nav, history[1], proof

    def _day_maps(self, inputs, trace):
        return torch.vmap(self._one_day, in_dims=(0, 0, 0, 0, 0, 0, None))(
            inputs[0], inputs[1], inputs[14], inputs[16:], trace, inputs[15], inputs[2:6])

    @torch.inference_mode(False)
    def prepare_backward(self):
        if self.backward is not None:
            return
        self.day_maps = _compiled_day_method(self, type(self)._day_maps, 'parallel_day_vjp')
        self.stream.wait_stream(torch.cuda.current_stream(self.device))
        # These leaf owners are first used and warmed on the capture stream.
        with torch.cuda.stream(self.stream):
            self.trace_inputs = tuple(v.detach().clone() for v in self.trace)
            self.trace_inputs[0].requires_grad_(True)
            self.vjp = tuple(torch.zeros_like(self.outputs[i]) for i in _DIFFERENTIABLE)
            self.expected = (torch.cat((self.trace[0][1:], self.outputs[8][None])),
                torch.cat((self.trace[1][1:], self.outputs[9][None])),
                torch.cat((self.trace[6][1:], self.outputs[14][None])), self.outputs[1])
            self.expected_inputs = tuple(v.clone() for v in self.expected)

            def run():
                with torch.enable_grad():
                    after, cost, nav, turnover, proof = self.day_maps(self.inputs, self.trace_inputs)
                    a, jacobian = torch.autograd.grad(after.sum(),
                        (self.trace_inputs[0], self.inputs[0]))
                gradients, valid = affine_day_adjoint(self.vjp[0], self.vjp[2],
                    self.vjp[3], self.vjp[4], a, jacobian,
                    self.expected_inputs[2], self.inputs[12])
                valid = (valid & proof.all() & (cost == self.trace_inputs[1]).all()
                    & ((after-self.expected_inputs[0]).abs() <= 1e-6).all()
                    & ((cost-self.expected_inputs[1]).abs() <= 1e-6).all()
                    & ((nav-self.expected_inputs[2]).abs() <= 1e-6).all()
                    & ((turnover-self.expected_inputs[3]).abs() <= 1e-12).all()
                    & (self.expected_inputs[2] > 1e-6).all()
                    & (self.inputs[12] > 1e-6)
                    & (self.trace_inputs[7]).all())
                # Avoid the log-return clamp branch: that exceptional state
                # requires the unmodified canonical VJP, including ruin.
                ratios = self.expected_inputs[2]/self.trace_inputs[6].clamp_min(1e-30)
                return gradients, valid & (ratios > 1e-6).all()

            for _ in range(2):
                self.grad_outputs, self.adjoint_valid = run()
        self.stream.synchronize()
        self.backward = torch.cuda.CUDAGraph()
        with torch.cuda.graph(self.backward, stream=self.stream):
            self.grad_outputs, self.adjoint_valid = run()
        self.stream.synchronize()
        torch.cuda.current_stream(self.device).wait_stream(self.stream)

    def canonical_vjp(self, w, state, sessions, enter, fees, gradients):
        engine = _canonical_fallback_engine(self)
        engine.enter_stream()
        engine.copy(w, state, sessions, enter, fees)
        engine.prepare_backward()
        with torch.no_grad():
            for destination, index in zip(engine.vjp, _DIFFERENTIABLE):
                value = gradients[index]
                destination.zero_() if value is None else destination.copy_(value)
        engine.backward.replay()
        _STATS['parallel_adjoint_fallbacks'] += 1
        return tuple(None if g is None else g.clone() for g in engine.grad_outputs)


class ParallelDayLedger(torch.autograd.Function):
    @staticmethod
    def forward(ctx, engine, sessions, can_enter, fees, state, w, pnl, cost, nav):
        ctx.engine, ctx.sessions, ctx.enter, ctx.fees, ctx.state = engine, sessions, can_enter, fees, state
        ctx.validation_gate = current_ledger_validation_gate()
        engine.copy(w, state, sessions, can_enter, fees)
        engine.forward.replay()
        ctx.save_for_backward(w, pnl, cost, nav,
            *(v.clone() for v in engine.trace),
            *(v.clone() for v in (torch.cat((engine.trace[0][1:], engine.outputs[8][None])),
                torch.cat((engine.trace[1][1:], engine.outputs[9][None])),
                torch.cat((engine.trace[6][1:], engine.outputs[14][None])), engine.outputs[1])))
        result = tuple(v.clone() for v in engine.outputs)
        ctx.mark_non_differentiable(*(v for i, v in enumerate(result) if i not in _DIFFERENTIABLE))
        ctx.set_materialize_grads(False)
        _STATS['forward_replays'] += 1
        if sessions[0].uses_sparse_events:
            _STATS['sparse_forward_replays'] += 1
            _STATS['sparse_forward_event_cells'] += sessions[0].exit_prices.numel()*len(sessions)
            from os import environ
            if environ.get('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0') == '1':
                _STATS['sparse_bucketed_forward_replays'] += 1
        return result

    @staticmethod
    def backward(ctx, *grad_outputs):
        engine = ctx.engine
        w, pnl, cost, nav, *saved = ctx.saved_tensors
        with _LOCK:
            engine.enter_stream()
            engine.copy(w, ctx.state, ctx.sessions, ctx.enter, ctx.fees)
            if grad_outputs[1] is not None:
                gradients = engine.canonical_vjp(w, ctx.state, ctx.sessions,
                    ctx.enter, ctx.fees, grad_outputs)
            else:
                engine.prepare_backward()
                with torch.no_grad():
                    for destination, value in zip((*engine.trace_inputs, *engine.expected_inputs), saved):
                        destination.copy_(value)
                    for destination, index in zip(engine.vjp, _DIFFERENTIABLE):
                        value = grad_outputs[index]
                        destination.zero_() if value is None else destination.copy_(value)
                engine.backward.replay()
                if ledger_predicate(engine.adjoint_valid, 'parallel_day_state_adjoint_certificate',
                                    gate=ctx.validation_gate):
                    gradients = tuple(g.clone() for g in engine.grad_outputs)
                    _STATS['parallel_adjoint_replays'] += 1
                else:
                    gradients = engine.canonical_vjp(w, ctx.state, ctx.sessions,
                        ctx.enter, ctx.fees, grad_outputs)
            _STATS['backward_replays'] += 1
            if ctx.sessions[0].uses_sparse_events:
                _STATS['sparse_backward_replays'] += 1
        count = len(ctx.sessions)
        return (None, None, None, None, None, gradients[0][:count], *gradients[1:])


class ParallelFlatForwardEngine(_Engine):
    """Batched fixed-point day maps, exact funding certificate, bounded VJP.

    The four prefix iterations are a proposal. Acceptance checks both the
    monetary recurrence residual and every stock's canonical opening shares
    and entry cost throughout the accumulated balance-error interval. Discrete fills
    therefore remain self-consistent. Rejected proposals execute the original
    serial graph, rather than accepting approximate whole-lot decisions.
    """
    @torch.inference_mode(False)
    def __init__(self, program, example):
        super().__init__(program, example, capture=False)
        self.serial_fallback = None
        self.forward_graphs = {}
        self.return_candidate = self.inputs[6].new_zeros(program.rows)
        self.has_return_candidate = False
        self.maps = _compiled_day_method(self, type(self)._maps, 'parallel_flat_day')
        self.balance_maps = _compiled_day_method(self, type(self)._balance_maps, 'parallel_flat_balance')
        self.opening_maps = _compiled_day_method(self, type(self)._opening_maps, 'parallel_flat_opening')
        _prewarm_canonical_fallback(self)

    @torch.no_grad()
    def set_return_candidate(self, returns):
        self.has_return_candidate = returns is not None
        if returns is not None:
            count = returns.numel()
            if returns.ndim != 1 or count > self.program.rows:
                raise ValueError('recorded return candidate exceeds the financial graph block')
            self.return_candidate[:count].copy_(returns.detach())
            self.return_candidate[count:].zero_()

    def _trace(self, pnl):
        values = self.inputs
        n = self.program.rows
        # With no incoming shares/claims and a proved flat close each day,
        # no old holder is entitled to an action: the canonical cursor stays
        # unchanged. The decision/observation clocks still advance normally.
        return (pnl, values[7].expand(n), values[8].expand(n, -1),
            torch.cat((values[9][None], values[14][:-1])),
            torch.cat((values[10][None], values[14][:-1])),
            values[11].expand(n),
            torch.cat((values[12][None], (self.program.capital+pnl-values[7])[1:])),
            values[13].expand(n))

    def _state_session(self, day, source, trace):
        from stockagent.backtest.tw_day_trade_carry import DayTradeCarryState
        p = self.program
        session = p.session_type(day=day, **p.session_defaults,
                                 **dict(zip(p.names, source)))
        state = DayTradeCarryState(replace(p.empty, **dict(zip(
            ('realized_net_pnl', 'carry_cost', 'action_cursor', 'decision_day',
             'observed_day', 'failed'), trace[:6]))), trace[6], trace[7], p.capital)
        return state, session

    def _one_day(self, weight, enter, day, source, trace, active, fees):
        state, session = self._state_session(day, source, trace)
        after, history, proof = self.program._step(state, session, weight, enter, *fees, active)
        inv = after.inventory
        return (*history, inv.realized_net_pnl, inv.carry_cost, inv.action_cursor,
            inv.decision_day, inv.observed_day, inv.failed, after.last_nav, after.alive, proof)

    def _opening(self, weight, enter, day, source, trace, fees):
        from stockagent.backtest.tw_day_trade_carry import _prepare_session_funding
        from stockagent.backtest.tw_day_trade_inventory import (
            CohortField, rebalance_inventory_at_open)
        state, session = self._state_session(day, source, trace)
        allowed, funded, _, target, _, _, _ = _prepare_session_funding(state, session,
            weights=weight, can_enter=enter, initial_capital=self.program.capital)
        opened = rebalance_inventory_at_open(funded, weights=target,
            official_open=session.official_open, opening_marks=session.opening_marks,
            entry_price=session.entry_price, entry_volume_shares=session.entry_volume,
            lower_limit=session.lower_limit, upper_limit=session.upper_limit,
            can_enter=allowed, buy_fee_rate=fees[0], day_sell_fee_rate=fees[1],
            normal_sell_fee_rate=fees[2], rebate_rate=fees[3],
            initial_capital=self.program.capital, day=session.day, halted=session.halted,
            daily_proxy_mask=session.daily_proxy_mask, state_already_advanced=True)
        return (opened.state.shares, opened.state.cohorts[-1, :, CohortField.ENTRY_COST],
            opened.target_shares, opened.sizing_nav, opened.state.failed)

    def _maps(self, values, trace):
        return torch.vmap(self._one_day, in_dims=(0, 0, 0, 0, 0, 0, None))(
            values[0], values[1], values[14], values[16:], trace, values[15], values[2:6])

    def _one_balance(self, weight, enter, day, source, trace, active, fees):
        state, session = self._state_session(day, source, trace)
        after, _, _ = self.program._step(state, session, weight, enter, *fees, active)
        return after.inventory.realized_net_pnl

    def _balance_maps(self, values, trace):
        # A proposal consumes only F(P). Keeping the full history/proof tuple
        # as compiled outputs prevents dead-output elimination in each of the
        # four iterations. Final acceptance still uses the complete day map.
        return torch.vmap(self._one_balance, in_dims=(0, 0, 0, 0, 0, 0, None))(
            values[0], values[1], values[14], values[16:], trace, values[15], values[2:6])

    def _opening_maps(self, values, trace):
        return torch.vmap(self._opening, in_dims=(0, 0, 0, 0, 0, None))(
            values[0], values[1], values[14], values[16:], trace, values[2:6])

    def _proposal(self, need_grad):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=UserWarning,
                message=r'The \.grad attribute of a Tensor that is not a leaf Tensor.*')
            return self._proposal_impl(need_grad)

    def _proposal_impl(self, need_grad):
        from stockagent.backtest.simulator import _portfolio_simple_returns_to_log_torch
        values = self.inputs
        with torch.no_grad():
            if getattr(self, 'has_return_candidate', False):
                prefix = self.return_candidate.cumsum(0)
                prior = torch.cat((prefix.new_zeros(1), prefix[:-1]))
                pnl = values[12]*prior.exp()-self.program.capital+values[7]
                pnl = torch.cat((values[6][None], pnl[1:]))
            else:
                pnl = values[6].expand(self.program.rows)
                for _ in range(4):
                    after_pnl = self.balance_maps(values, self._trace(pnl))
                    delta = after_pnl-pnl
                    pnl = values[6]+delta.cumsum(0)-delta
            before = pnl.detach().requires_grad_(need_grad)
        with torch.set_grad_enabled(need_grad):
            out = self.maps(values, self._trace(before))
            if need_grad:
                a, jacobian = torch.autograd.grad(out[8].sum(), (before, values[0]))
            else:
                a = torch.ones_like(before)
                jacobian = torch.zeros_like(values[0])
        with torch.no_grad():
            implied = torch.cat((values[6][None], out[8][:-1]))
            # Prove stability on both sides of the accepted monetary error
            # budget, including its accumulation across this entire block.
            # An exact lot/cash boundary must fall back, even when two nearby
            # guesses happened to land on the same side of it in this replay.
            budget = self.program.rows*1e-6
            lower = torch.minimum(before.detach(), implied)-budget
            upper = torch.maximum(before.detach(), implied)+budget
            left = self.opening_maps(values, self._trace(lower))
            right = self.opening_maps(values, self._trace(upper))
            residual = (implied-before.detach()).abs().amax()
            valid = (out[16].all() & (out[9] == values[7]).all()
                & (out[4] > budget).all()
                & (residual <= 1e-6) & (left[0] == right[0]).all()
                & (left[1] == right[1]).all()
                # Fix the pre-scaling target vector across this interval.
                # Gross cost then remains constant and funded quantities
                # are monotone in the shared cash budget. Equal endpoint
                # fills certify the intervening whole-lot branch as well.
                & (left[2] == right[2]).all()
                & (left[3] > 0).all() & (right[3] > 0).all()
                & (left[4] == 0).all() & (right[4] == 0).all())
            after_nav = out[14].detach()
            prior_nav = torch.cat((values[12][None], after_nav[:-1]))
            ratio = after_nav/prior_nav.clamp_min(1e-30)
            valid = (valid & torch.isfinite(ratio).all()
                & ((after_nav-budget) > 1e-6*(prior_nav+budget)).all()
                & torch.isfinite(a).all() & (a != 0).all()
                & torch.isfinite(jacobian).all() & (values[12] > 1e-6))
            returns = torch.where(values[15], _portfolio_simple_returns_to_log_torch(ratio-1), 0.)
            index = (values[15].long().sum()-1).reshape(1)
            final = [v.index_select(0, index).squeeze(0) for v in out[8:16]]
            claim = out[7]
            claim = torch.stack((claim[..., 0], claim[..., 1],
                torch.where(claim[..., 1] <= final[4], 1, claim[..., 2])), -1)
            outputs = (returns, *out[1:7], claim, *final, valid)
            return outputs, a, jacobian, after_nav, residual

    @torch.inference_mode(False)
    def prepare_forward(self, need_grad):
        key = (need_grad, self.has_return_candidate)
        if key in self.forward_graphs:
            return self.forward_graphs[key]
        self.stream.wait_stream(torch.cuda.current_stream(self.device))
        with torch.cuda.stream(self.stream):
            for _ in range(2):
                sample = self._proposal(need_grad)
            del sample
        self.stream.synchronize()
        graph = torch.cuda.CUDAGraph()
        with torch.cuda.graph(graph, stream=self.stream):
            recorded = self._proposal(need_grad)
        self.stream.synchronize()
        torch.cuda.current_stream(self.device).wait_stream(self.stream)
        self.forward_graphs[key] = (graph, recorded)
        return graph, recorded

    @torch.inference_mode(False)
    def prepare_backward(self):
        if self.backward is not None:
            return
        n, symbols = self.inputs[0].shape
        with torch.cuda.stream(self.stream):
            self.saved_a = torch.ones(n, device=self.device, dtype=torch.float64)
            self.saved_jacobian = torch.zeros_like(self.inputs[0])
            self.saved_nav = torch.ones_like(self.saved_a)
            self.saved_initial_nav = self.inputs[12].detach().clone()
            self.vjp = (torch.zeros_like(self.saved_a), *(torch.zeros_like(self.saved_initial_nav) for _ in range(3)))
            def run():
                return affine_day_adjoint(*self.vjp, self.saved_a, self.saved_jacobian,
                                          self.saved_nav, self.saved_initial_nav)
            for _ in range(2):
                self.grad_outputs, self.adjoint_valid = run()
        self.stream.synchronize()
        self.backward = torch.cuda.CUDAGraph()
        with torch.cuda.graph(self.backward, stream=self.stream):
            self.grad_outputs, self.adjoint_valid = run()
        self.stream.synchronize()
        torch.cuda.current_stream(self.device).wait_stream(self.stream)

    canonical_vjp = ParallelDayAdjointEngine.canonical_vjp


class ParallelFlatLedger(torch.autograd.Function):
    @staticmethod
    def forward(ctx, engine, sessions, can_enter, fees, state, w, pnl, cost, nav):
        ctx.engine, ctx.sessions, ctx.enter, ctx.fees, ctx.state = engine, sessions, can_enter, fees, state
        ctx.validation_gate = current_ledger_validation_gate()
        engine.copy(w, state, sessions, can_enter, fees)
        graph, saved = engine.prepare_forward(w.requires_grad)
        graph.replay()
        outputs, a, jacobian, after_nav, residual = saved
        # Immediate mode uses canonical fallback. An explicitly enabled whole-
        # pass gate may keep this result speculative until BOTH ranks accept
        # every forward/backward certificate before the sole optimizer update.
        # Rejection discards all gradients/tape, restores RNG and retries with
        # immediate canonical validation. Inference has no such gate.
        if (ctx.validation_gate is not None and os.environ.get(
                'STOCKAGENT_DAY_TRADE_DEFER_PARALLEL_FORWARD_CERTIFICATE', '0') == '1'):
            ctx.fast = ledger_predicate(outputs[-1], 'parallel_flat_forward_funding_certificate',
                                        gate=ctx.validation_gate)
            _STATS['parallel_flat_deferred_certificates'] += 1
        else:
            ctx.fast = bool(outputs[-1])
        if engine.has_return_candidate:
            _STATS['recorded_return_candidate_replays'] += 1
        if not ctx.fast:
            serial = _canonical_fallback_engine(engine)
            serial.enter_stream()
            serial.copy(w, state, sessions, can_enter, fees)
            serial.forward.replay()
            outputs = serial.outputs
            _STATS['parallel_flat_forward_fallbacks'] += 1
            if engine.has_return_candidate:
                _STATS['recorded_return_candidate_fallbacks'] += 1
        else:
            _STATS['parallel_flat_forward_replays'] += 1
        ctx.save_for_backward(w, pnl, cost, nav, a.clone(), jacobian.clone(), after_nav.clone())
        result = tuple(v.clone() for v in outputs)
        ctx.mark_non_differentiable(*(v for i, v in enumerate(result) if i not in _DIFFERENTIABLE))
        ctx.set_materialize_grads(False)
        _STATS['forward_replays'] += 1
        if sessions[0].uses_sparse_events:
            _STATS['sparse_forward_replays'] += 1
            _STATS['sparse_forward_event_cells'] += sessions[0].exit_prices.numel()*len(sessions)
            from os import environ
            if environ.get('STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS', '0') == '1':
                _STATS['sparse_bucketed_forward_replays'] += 1
        return result

    @staticmethod
    def backward(ctx, *grad_outputs):
        engine = ctx.engine
        w, pnl, cost, nav, a, jacobian, after_nav = ctx.saved_tensors
        with _LOCK:
            engine.enter_stream()
            if not ctx.fast or grad_outputs[1] is not None:
                gradients = engine.canonical_vjp(w, ctx.state, ctx.sessions, ctx.enter, ctx.fees, grad_outputs)
            else:
                engine.prepare_backward()
                with torch.no_grad():
                    for destination, value in zip((engine.saved_a, engine.saved_jacobian,
                            engine.saved_nav, engine.saved_initial_nav), (a, jacobian, after_nav, nav)):
                        destination.copy_(value)
                    for destination, index in zip(engine.vjp, (0, 8, 9, 14)):
                        value = grad_outputs[index]
                        destination.zero_() if value is None else destination.copy_(value)
                engine.backward.replay()
                if ledger_predicate(engine.adjoint_valid, 'parallel_flat_affine_adjoint_certificate',
                                    gate=ctx.validation_gate):
                    gradients = tuple(g.clone() for g in engine.grad_outputs)
                    _STATS['parallel_adjoint_replays'] += 1
                else:
                    gradients = engine.canonical_vjp(w, ctx.state, ctx.sessions, ctx.enter, ctx.fees, grad_outputs)
            _STATS['backward_replays'] += 1
            if ctx.sessions[0].uses_sparse_events:
                _STATS['sparse_backward_replays'] += 1
        count = len(ctx.sessions)
        return (None, None, None, None, None, gradients[0][:count], *gradients[1:])
