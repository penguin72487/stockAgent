"""Whole-contract residual carry with target-difference orders and daily MTM."""
from __future__ import annotations

import math
from functools import lru_cache
import os

import torch

from stockagent.data.tw_stock_futures_minute import TAPE_FIELDS, EVENT_MINUTES, BAR_FIELDS
from stockagent.data.tw_stock_futures_carry import CARRY_TAPE_FIELDS, CARRY_STATE_FIELDS, CARRY_SUPPORTED_TAPE_FIELDS
from stockagent.data.tw_stock_futures_carry import CARRY_QUARANTINE_OFFSET
from stockagent.backtest.futures_data_validity import CARRY_DATA_REASONS, FuturesCarryDataError
from stockagent.backtest.tw_stock_futures_day_trade import (
    StockFuturesDayTradeTensorResult, _integer_candidate_basket, _scheduled_exit_fills,
    _scheduled_cash_bracket, _scheduled_symbol_payoffs,
)


def _transfer_corporate_positions(state, execution):
    """Move old signed inventory and its prior mark; a legal transfer is no fill."""
    origins = execution[..., CARRY_TAPE_FIELDS]
    targets = execution[..., TAPE_FIELDS]
    active = state[..., 0].detach() != 0
    matches = ((origins[:, :, None] > 0) & (origins[:, :, None] == state[:, None, :, 2])
               & active[:, None, :])
    incoming = matches.any(-1)
    outgoing = matches.any(-2)
    invalid = ((matches.sum(-1) > 1).any() | (matches.sum(-2) > 1).any()
               | (incoming & active).any() | (incoming & (targets == origins)).any())
    indices = matches.to(torch.int64).argmax(-1)
    donor = state.gather(1, indices[..., None].expand(-1,-1,CARRY_STATE_FIELDS))
    moved = torch.stack((donor[...,0],donor[...,1],targets,donor[...,3]),-1)
    cleared = torch.where(outgoing[...,None],torch.zeros_like(state),state)
    return torch.where(incoming[...,None],moved,cleared), invalid


def _carry_day(requested, execution, state, equity):
    """One canonical exact forward, with an explicit quantity STE backward.

    Capital sizing is detached, as in the existing integer futures account.
    Quantity sensitivities propagate through the within-chunk carry recurrence.
    No gradient penalizes a solvent residual merely for surviving 13:30.
    """
    invalid_transfer = torch.zeros((),device=state.device,dtype=torch.bool)
    if execution.shape[-1] != CARRY_TAPE_FIELDS:
        state, invalid_transfer = _transfer_corporate_positions(state, execution)
    previous, previous_mark, previous_id = state[..., :3].unbind(-1)
    multiplier, fee, tax = execution[..., :3].unbind(-1)
    physical_id, tier, selected, settlement, final, expires, verified, cash_adjustment, unsupported_event = execution[..., TAPE_FIELDS:CARRY_TAPE_FIELDS].unbind(-1)
    quarantined = torch.zeros_like(verified, dtype=torch.bool)
    quarantine_mark = torch.zeros_like(settlement)
    if execution.shape[-1] > CARRY_QUARANTINE_OFFSET:
        quarantined = execution[..., CARRY_QUARANTINE_OFFSET] > 0
        quarantine_mark = execution[..., CARRY_QUARANTINE_OFFSET + 1]
    bars = execution[..., 3:TAPE_FIELDS].reshape(*previous.shape, len(EVENT_MINUTES), len(BAR_FIELDS))
    entry = bars[..., 0, 0]
    capacity = bars[..., 0, 4]
    metadata = (physical_id > 0) & (multiplier > 0) & (fee >= 0) & (tax >= 0)
    execution_allowed = metadata & (verified > 0) & ~quarantined
    tradable = execution_allowed & torch.isfinite(entry) & (entry > 0) & (capacity > 0)
    capacity = torch.where(tradable, capacity, torch.zeros_like(capacity))
    active = previous.detach() != 0
    # Pure cash ex-dividends credit/debit only positions held before the event.
    # Adjust an unquoted opening reference by the same cash amount; the event
    # alone must not create spendable profit before new-order sizing.
    reference_mark = torch.where(active, previous_mark - cash_adjustment, previous_mark)
    entry_notional = torch.where(tradable, entry * multiplier, reference_mark)
    equity_adjustment = (previous * cash_adjustment).sum()
    bad_corporate_action = invalid_transfer | (active & (unsupported_event > 0)).any()
    bad_identity = (active & (previous_id != physical_id)).any()
    bad_source = (active & (((verified <= 0) & ~quarantined) | ~metadata)).any()
    # No current or future daily price enters the opening affordability test.
    gap = (previous * torch.where(active, entry_notional - previous_mark, 0.)).sum()
    opening_equity = equity + equity_adjustment + gap
    entry_cost = fee + torch.floor(entry_notional * tax + .5)
    reserve = entry_notional + 2 * entry_cost
    eligible = (selected > 0) & tradable
    # Select exactly the same causal standard/mini basket as the flat executor.
    indices = torch.stack([torch.where(eligible & (tier == i), 1., 0.).argmax(-1) for i in (0, 1)], -1)
    gather = lambda x: x.gather(1, indices)
    candidate_valid = gather(eligible) & (gather(tier) == torch.arange(2, device=tier.device))
    candidate_reserve = gather(reserve)
    candidate_notional = gather(entry_notional)
    max_target = gather(previous.detach().abs() + capacity)
    cash = requested.abs() * opening_equity.detach().clamp_min(0.)
    basket = _integer_candidate_basket(cash, candidate_notional, candidate_reserve, max_target, candidate_valid)
    exact_target = torch.zeros_like(previous).scatter_add(
        1, indices, basket.to(previous.dtype) * requested.detach().sign()[:, None])
    cash_boundary_correction = requested.new_zeros(())
    shadow_request = requested
    if torch.is_grad_enabled():
        # At an empty account's first-contract boundary, abs(exact_delta)==0
        # used to erase the entry-fee derivative. Compare both actual integer
        # round trips, using the existing allocator and scheduled fill engine.
        # This bounded secant applies only when BOTH alternatives end flat;
        # overnight alternatives retain recurrent credit assignment below.
        with torch.no_grad():
            candidate_tape = execution[..., :TAPE_FIELDS].gather(
                1, indices[..., None].expand(-1, -1, TAPE_FIELDS))
            candidate_tape = torch.where(candidate_valid[..., None], candidate_tape, 0.)
            capital = opening_equity.detach().clamp_min(1e-12)
            _, first_cash = _scheduled_cash_bracket(torch.zeros_like(requested), candidate_tape, capital)
            first_request = first_cash / capital
            long_net, long_residual = _scheduled_symbol_payoffs(first_request, candidate_tape, capital)
            short_net, short_residual = _scheduled_symbol_payoffs(-first_request, candidate_tape, capital)
            free_cash = (capital - (exact_target.abs() * reserve).sum()).clamp_min(0.)
            boundary = ((previous == 0).all() & (exact_target == 0).all(-1)
                        & (first_cash > 0) & (first_cash <= free_cash)
                        & (long_residual == 0) & (short_residual == 0))
            long_slope = long_net * capital / first_request.clamp_min(1e-12)
            short_slope = -short_net * capital / first_request.clamp_min(1e-12)
            at_zero = torch.where((long_slope > 0) & (long_slope >= -short_slope), long_slope,
                                 torch.where(short_slope < 0, short_slope, 0.))
            slope = torch.where(requested > 0, long_slope,
                                torch.where(requested < 0, short_slope, at_zero))
        shadow_request = torch.where(boundary, requested.detach(), requested)
        cash_boundary_correction = ((requested - requested.detach()) * torch.where(boundary, slope, 0.)).sum()
    denom = torch.where(candidate_valid, candidate_reserve, 0.).sum(-1).clamp_min(1e-12)
    soft_target = torch.where(eligible, shadow_request[:, None] * opening_equity.detach().clamp_min(0.) / denom[:, None], 0.)
    target = soft_target + (exact_target - soft_target).detach()
    delta = torch.where(capacity > 0, torch.maximum(torch.minimum(target - previous, capacity), -capacity), 0.)
    close = -previous.detach().sign() * torch.minimum(previous.abs(), (-previous.detach().sign() * delta).clamp_min(0.))
    after_close = previous + close
    addition = delta - close
    # Filled reductions release collateral first. New orders share only the
    # remaining cash; floor rounding cannot invent fractions or extra capital.
    close_cost = (close.abs() * entry_cost).sum()
    committed = (after_close.abs() * (entry_notional + entry_cost)).sum()
    available = (opening_equity - close_cost - committed).detach().clamp_min(0.)
    add_cost = (addition.detach().abs() * reserve).sum()
    scale = torch.minimum(available / add_cost.clamp_min(1e-12), torch.ones_like(available))
    funded_add = addition * scale
    exact_add = funded_add.detach().sign() * torch.floor(funded_add.detach().abs())
    addition = funded_add + (exact_add - funded_add).detach()
    delta = close + addition
    position = previous + delta
    position = position + (torch.round(position.detach()) - position).detach()
    cost = (delta.abs() * entry_cost).sum()
    pnl = equity_adjustment + gap - cost
    turnover = (delta.detach().abs() * entry_notional).sum()
    direction = torch.where(position.detach() != 0, position.detach().sign(),
                            torch.where(requested[:, None] < 0, -1., 1.))
    remaining, pnl, turnover = _scheduled_exit_fills(
        position * direction, direction, bars, execution_allowed,
        entry_notional / multiplier.clamp_min(1.), multiplier, fee, tax, pnl, turnover,
    )
    residual = remaining * direction
    residual = residual + (torch.round(residual.detach()) - residual).detach()
    mark = torch.where(expires > 0, final, settlement)
    # Quarantine authorizes no executions, not a claim of zero market volume.
    # Only its independently verified same-day official mark can value carry.
    mark = torch.where(quarantined, quarantine_mark, mark)
    needs_mark = residual.detach() != 0
    bad_mark = (needs_mark & (~torch.isfinite(mark) | (mark <= 0)
                             | (quarantined & (expires > 0)))).any()
    safe_mark = torch.where(torch.isfinite(mark) & (mark > 0), mark, entry_notional)
    # The canonical daily mark may carry the last known valuation on an
    # unobserved day; it is not guaranteed to be today's clearing price and
    # never creates a fill. Real final cash
    # settlement alone extinguishes the physical contract on its expiry date.
    pnl = pnl + (residual * (safe_mark - entry_notional)).sum()
    settling = torch.where(expires > 0, residual, 0.)
    settlement_cost = settling.abs() * (fee + torch.floor(safe_mark * tax + .5))
    pnl = pnl - settlement_cost.sum()
    turnover = turnover + (settling.detach().abs() * safe_mark).sum()
    ending = torch.where(expires > 0, torch.zeros_like(residual), residual)
    next_equity = equity + pnl + cash_boundary_correction
    reason = torch.where(bad_corporate_action, 6, torch.where(bad_identity, 1, torch.where(bad_source, 2, torch.where(bad_mark, 3,
             torch.where(~torch.isfinite(next_equity) | ~torch.isfinite(opening_equity), 5,
             torch.where((next_equity <= 0) | (opening_equity <= 0), 4, 0))))))
    ending_lane_weight = torch.where(next_equity.detach() > 0,
                                    ending.detach() * safe_mark / next_equity.detach().clamp_min(1e-12), 0.)
    next_state = torch.stack((ending, safe_mark, physical_id, ending_lane_weight), -1)
    executed_weight = (position.detach() * entry_notional).sum(-1) / opening_equity.detach().clamp_min(1e-12)
    final_weight = ending_lane_weight.sum(-1)
    return (next_equity, next_state, reason, turnover / equity.detach().clamp_min(1e-12),
            executed_weight, final_weight, torch.round(position.detach()).to(torch.int64),
            torch.round(ending.detach()).to(torch.int64))


@lru_cache(maxsize=1)
def _compiled_carry_day():
    return torch.compile(_carry_day, fullgraph=True, dynamic=True, mode="default")


def run_tw_stock_futures_carry_torch(
    target_weights, candidate_execution, *, initial_capital,
    initial_carry_state=None, initial_equity_scale=None, initial_alive=None,
    state_advance_mask=None, return_weights_history=True, use_compile=None,
    diagnostic_only=False,
):
    """Reject unknown data by default, including in canonical loss and evaluation.

    ``diagnostic_only`` is for standalone source audits. Invalid returns are NaN,
    never the old finite ruin marker; callers must not optimize or report them.
    Genuine economic insolvency (reason 4) retains absorbing ruin semantics.
    """
    if (target_weights.ndim != 2 or not target_weights.numel()
            or candidate_execution.ndim != 4 or candidate_execution.shape[:2] != target_weights.shape
            or candidate_execution.shape[-1] not in CARRY_SUPPORTED_TAPE_FIELDS):
        raise ValueError("carry executor requires weights [T,S] and physical tape [T,S,K,F]")
    if not math.isfinite(initial_capital) or initial_capital <= 0:
        raise ValueError("carry initial capital must be finite and positive")
    weights = torch.nan_to_num(target_weights.float(), nan=0., posinf=0., neginf=0.)
    execution = candidate_execution.to(device=weights.device, dtype=torch.float32)
    rows, symbols, lanes = execution.shape[:3]
    state = weights.new_zeros((symbols, lanes, CARRY_STATE_FIELDS)) if initial_carry_state is None else initial_carry_state.to(weights)
    if state.shape != (symbols, lanes, CARRY_STATE_FIELDS):
        raise ValueError("carry state shape differs from physical lane axis")
    equity = weights.new_tensor(initial_capital) * (1. if initial_equity_scale is None else initial_equity_scale.to(weights))
    alive = torch.ones((), device=weights.device, dtype=torch.bool) if initial_alive is None else initial_alive.to(device=weights.device, dtype=torch.bool)
    advance = torch.ones(rows, device=weights.device, dtype=torch.bool) if state_advance_mask is None else state_advance_mask.to(device=weights.device, dtype=torch.bool)
    if advance.shape != (rows,): raise ValueError("carry advance mask must have shape [T]")
    compile_requested = os.environ.get("STOCKAGENT_BACKTEST_COMPILE", "0") == "1" if use_compile is None else use_compile
    day = _compiled_carry_day() if compile_requested and weights.is_cuda else _carry_day
    logs=[]; turns=[]; wh=[]; qh=[]; rh=[]; eh=[]; dh=[]; reasons=[]; sh=[]
    data_invalid = torch.zeros((), device=weights.device, dtype=torch.bool)
    final_weight = (state[..., 0].detach() * state[..., 1]).sum(-1) / equity.detach().clamp_min(1e-12)
    for i in range(rows):
        enabled = advance[i] & alive
        result = day(torch.where(enabled, weights[i], 0.), execution[i], state, equity)
        next_equity, next_state, reason, turnover, weight, ending_weight, quantities, residuals = result
        failed = enabled & (reason != 0)
        net = (next_equity - equity) / equity.detach().clamp_min(1e-12)
        log = torch.where(reason == 0, torch.log1p(net.clamp_min(-1 + 1e-7)), weights.new_tensor(math.log(1e-7)))
        data_failed = failed & (reason != 4)
        data_invalid = data_invalid | data_failed
        logs.append(torch.where(data_invalid, float('nan'), torch.where(enabled, log, 0.)))
        turns.append(torch.where(enabled & ~data_failed, turnover, 0.))
        # Preserve the last valid inventory on an invalid-source state. It is
        # never cleared to cash just to make the report appear solvent.
        update = enabled & ~data_failed
        state = torch.where(update, next_state, state)
        next_equity = torch.nan_to_num(next_equity, nan=0., posinf=0., neginf=0.).clamp_min(0.)
        equity = torch.where(update, next_equity, equity)
        final_weight = torch.where(update, ending_weight, final_weight)
        wh.append(torch.where(enabled, weight, 0.)); qh.append(torch.where(enabled, quantities, 0))
        rh.append(torch.round(state[..., 0].detach()).to(torch.int64)); eh.append(equity / initial_capital)
        if return_weights_history: sh.append(state.detach())
        dh.append(failed); reasons.append(torch.where(enabled, reason, 0))
        alive = alive & ~failed
    if not diagnostic_only and bool(data_invalid.detach()):
        reason_values = torch.stack(reasons).detach().cpu().tolist()
        row = next(i for i, value in enumerate(reason_values) if value in CARRY_DATA_REASONS)
        # State is preserved at the first unknown row. Include both old inventory
        # and attempted opening quantities, since an opening can lack an EOD mark.
        held = state.detach().cpu()
        opened = qh[row].detach().cpu()
        row_tape = execution[row].detach().cpu()
        positions = []
        for s, k in torch.nonzero((held[..., 0] != 0) | (opened != 0)).tolist():
            positions.append({
                'symbol_index': s, 'lane': k,
                'held_physical_id': int(held[s, k, 2]),
                'tape_physical_id': int(row_tape[s, k, TAPE_FIELDS]),
                'held_quantity': int(held[s, k, 0]),
                'attempted_opening_quantity': int(opened[s, k]),
                'minute_source_verified': bool(row_tape[s, k, TAPE_FIELDS + 6] > 0),
                'unsupported_corporate_transition': bool(row_tape[s, k, TAPE_FIELDS + 8] > 0),
                'mark_available': bool(torch.isfinite(row_tape[s, k, TAPE_FIELDS + 3])
                                       & (row_tape[s, k, TAPE_FIELDS + 3] > 0)),
            })
        raise FuturesCarryDataError({
            'row': row, 'reason': CARRY_DATA_REASONS[reason_values[row]],
            'last_valid_equity': float(equity.detach()), 'positions': positions,
        })
    return StockFuturesDayTradeTensorResult(
        strategy_returns=torch.stack(logs), turnovers=torch.stack(turns),
        weights_history=torch.stack(wh) if return_weights_history else weights.new_empty((0, symbols)),
        final_weights=final_weight, final_alive=alive, equity_scale_history=torch.stack(eh),
        final_equity_scale=equity / initial_capital,
        contract_quantities_history=torch.stack(qh) if return_weights_history else None,
        residual_contract_quantities_history=torch.stack(rh) if return_weights_history else None,
        default_history=torch.stack(dh), default_reason_history=torch.stack(reasons), final_carry_state=state,
        carry_state_history=torch.stack(sh) if sh else None,
    )
