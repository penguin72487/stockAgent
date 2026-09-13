"""Differentiable daily-flat ledger for front-month single-stock futures."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from functools import lru_cache, partial

import torch

from stockagent.data.tw_stock_futures_minute import BAR_FIELDS, EVENT_MINUTES, TAPE_FIELDS, HYBRID_TAPE_FIELDS


@dataclass(slots=True)
class StockFuturesDayTradeTensorResult:
    strategy_returns: torch.Tensor
    turnovers: torch.Tensor
    weights_history: torch.Tensor
    final_weights: torch.Tensor
    final_alive: torch.Tensor
    equity_scale_history: torch.Tensor | None = None
    final_equity_scale: torch.Tensor | None = None
    contract_quantities_history: torch.Tensor | None = None
    residual_contract_quantities_history: torch.Tensor | None = None
    default_history: torch.Tensor | None = None
    final_carry_state: torch.Tensor | None = None
    carry_state_history: torch.Tensor | None = None
    default_reason_history: torch.Tensor | None = None


def run_tw_stock_futures_day_trade_continuous_torch(
    target_weights: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    executable_mask: torch.Tensor,
    *,
    round_trip_cost_rate_per_open_notional: torch.Tensor,
    volume_limit_weights: torch.Tensor | None = None,
    state_advance_mask: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    return_weights_history: bool = True,
) -> StockFuturesDayTradeTensorResult:
    """Execute independent OPEN-to-CLOSE futures round trips.

    Portfolio targets are opening-NAV notional weights.  The caller has
    already normalized them over the causally known futures universe.  Failed
    current-session executions are set to zero independently per symbol and
    are never reallocated to the remaining names.

    The cost tensor contains both fixed commissions and statutorily rounded
    transaction tax for the entry and exit contract, divided by opening
    contract notional.  It is therefore multiplied by ``abs(weight)`` exactly
    once here.
    """

    if target_weights.ndim != 2 or target_weights.numel() == 0:
        raise ValueError(
            "stock-futures day-trade target_weights must have non-empty shape [T,S]"
        )
    expected = tuple(target_weights.shape)
    if tuple(intraday_log_returns.shape) != expected:
        raise ValueError("intraday_log_returns must match target_weights [T,S]")
    if tuple(executable_mask.shape) != expected:
        raise ValueError("executable_mask must match target_weights [T,S]")
    if tuple(round_trip_cost_rate_per_open_notional.shape) != expected:
        raise ValueError(
            "round_trip_cost_rate_per_open_notional must match target_weights [T,S]"
        )

    weights = target_weights.to(dtype=torch.float32)
    device = weights.device
    returns = intraday_log_returns.to(device=device, dtype=torch.float32)
    costs = round_trip_cost_rate_per_open_notional.to(
        device=device, dtype=torch.float32
    )
    executable = executable_mask.to(device=device, dtype=torch.bool)
    advance = (
        torch.ones((expected[0],), device=device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=device, dtype=torch.bool)
    )
    if tuple(advance.shape) != (expected[0],):
        raise ValueError("state_advance_mask must have shape [T]")
    initial = (
        torch.ones((), device=device, dtype=torch.bool)
        if initial_alive is None
        else initial_alive.detach().clone().to(device=device, dtype=torch.bool).reshape(())
    )

    proposed = torch.where(
        executable & advance[:, None],
        torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0),
        torch.zeros_like(weights),
    )
    if volume_limit_weights is not None:
        limits = volume_limit_weights.to(device=device, dtype=torch.float32)
        if tuple(limits.shape) != expected:
            raise ValueError("volume_limit_weights must match target_weights [T,S]")
        clean_limits = torch.where(
            torch.isfinite(limits) & (limits >= 0.0),
            limits,
            torch.zeros_like(limits),
        )
        proposed = torch.sign(proposed) * torch.minimum(
            proposed.abs(), clean_limits
        )
    active = proposed.abs() > 1.0e-8
    valid_inputs = (
        (~active)
        | (
            torch.isfinite(returns)
            & torch.isfinite(costs)
            & (costs >= 0.0)
        )
    ).all(dim=1)
    clean_simple_returns = torch.where(
        torch.isfinite(returns),
        torch.expm1(returns),
        torch.zeros_like(returns),
    )
    clean_costs = torch.where(
        torch.isfinite(costs) & (costs >= 0.0),
        costs,
        torch.zeros_like(costs),
    )
    proposed_net_simple = (
        proposed * clean_simple_returns - proposed.abs() * clean_costs
    ).sum(dim=1)
    row_survives = (
        (~advance)
        | (
            valid_inputs
            & torch.isfinite(proposed_net_simple)
            & (proposed_net_simple > -1.0)
        )
    )
    prior_survival = torch.cat(
        (
            torch.ones((1,), device=device, dtype=torch.bool),
            torch.cumprod(row_survives[:-1].to(dtype=torch.int64), dim=0).to(
                dtype=torch.bool
            ),
        ),
        dim=0,
    )
    alive_before = initial & prior_survival
    executed = torch.where(alive_before[:, None], proposed, torch.zeros_like(proposed))
    safe_net = torch.where(
        row_survives,
        proposed_net_simple,
        torch.full_like(proposed_net_simple, -1.0 + 1.0e-7),
    )
    strategy_returns = torch.where(
        alive_before & advance,
        torch.log1p(safe_net),
        torch.zeros_like(safe_net),
    )
    turnovers = torch.where(
        alive_before & advance,
        2.0 * executed.abs().sum(dim=1),
        torch.zeros_like(safe_net),
    )
    final_alive = initial & row_survives.all()
    history = (
        executed
        if return_weights_history
        else executed.new_empty((0, expected[1]))
    )
    return StockFuturesDayTradeTensorResult(
        strategy_returns=strategy_returns,
        turnovers=turnovers,
        weights_history=history,
        final_weights=torch.zeros(
            (expected[1],), device=device, dtype=torch.float32
        ),
        final_alive=final_alive,
    )


def _integer_candidate_basket(
    target_cash: torch.Tensor,
    open_notionals: torch.Tensor,
    reserved_cash: torch.Tensor,
    maximum_contracts: torch.Tensor,
    valid: torch.Tensor,
) -> torch.Tensor:
    """Pack standard+mini candidates without exceeding a target cash sleeve.

    Slot 0 is the standard multiplier and slot 1 is the mini multiplier.  Three
    feasible baskets are compared: standard-first with mini residual fill, one
    fewer standard with mini refill, and mini-only.  The selected basket
    maximizes executed notional, then (through stable candidate order) prefers
    the more fee-efficient standard contract.  Every forward quantity is a
    non-negative integer and is independently capacity bounded.
    """

    if target_cash.ndim != 1:
        raise ValueError("target_cash must have shape [S]")
    expected = (int(target_cash.size(0)), 2)
    for name, value in (
        ("open_notionals", open_notionals),
        ("reserved_cash", reserved_cash),
        ("maximum_contracts", maximum_contracts),
        ("valid", valid),
    ):
        if tuple(value.shape) != expected:
            raise ValueError(f"{name} must have shape [S,2]")
    safe_cash = torch.where(
        valid,
        reserved_cash,
        torch.full_like(reserved_cash, float("inf")),
    )
    caps = torch.where(
        valid,
        torch.floor(maximum_contracts.clamp_min(0.0)),
        torch.zeros_like(maximum_contracts),
    )
    eps = torch.finfo(target_cash.dtype).eps * target_cash.abs().clamp_min(1.0) * 8.0
    standard_max = torch.minimum(
        torch.floor((target_cash + eps) / safe_cash[:, 0]),
        caps[:, 0],
    ).clamp_min(0.0)

    standard_candidates = torch.stack(
        (
            standard_max,
            (standard_max - 1.0).clamp_min(0.0),
            torch.zeros_like(standard_max),
        ),
        dim=1,
    )
    used_standard_cash = standard_candidates * torch.where(
        valid[:, 0], reserved_cash[:, 0], torch.zeros_like(reserved_cash[:, 0])
    )[:, None]
    residual = (target_cash[:, None] - used_standard_cash).clamp_min(0.0)
    mini_counts = torch.minimum(
        torch.floor((residual + eps[:, None]) / safe_cash[:, 1, None]),
        caps[:, 1, None],
    ).clamp_min(0.0)
    baskets = torch.stack((standard_candidates, mini_counts), dim=-1)
    basket_cash = (baskets * torch.where(valid, reserved_cash, torch.zeros_like(reserved_cash))[:, None, :]).sum(dim=-1)
    feasible = basket_cash <= target_cash[:, None] + eps[:, None]
    basket_notional = (
        baskets
        * torch.where(valid, open_notionals, torch.zeros_like(open_notionals))[
            :, None, :
        ]
    ).sum(dim=-1)
    basket_notional = torch.where(
        feasible,
        basket_notional,
        torch.full_like(basket_notional, -1.0),
    )
    best = basket_notional.argmax(dim=1)
    return baskets.gather(
        1,
        best[:, None, None].expand(-1, 1, 2),
    )[:, 0, :].to(dtype=torch.int64)


def _scheduled_exit_fills(remaining, sign, bars, valid, entry, multiplier, fee, tax,
                          pnl, turnover):
    """Shared physical-minute exit orders for flat and residual-carry ledgers."""
    limit = bars[:, :, 1, 3]
    for event in range(2, len(EVENT_MINUTES)):
        price, high, low, _, capacity = bars[:, :, event, :].unbind(-1)
        observed = (valid & torch.isfinite(price) & (price > 0)
                    & torch.isfinite(capacity) & (capacity >= 0))
        if event <= 5:
            observed = observed & torch.isfinite(limit) & (limit > 0) & torch.where(
                sign >= 0, high > limit, low < limit,
            )
            price = limit
        capacity = torch.where(observed, capacity.clamp_min(0), torch.zeros_like(capacity))
        filled = torch.where(
            observed & (capacity > 0), torch.minimum(remaining, capacity),
            torch.zeros_like(remaining),
        )
        safe_price = torch.where(observed, price, torch.zeros_like(price))
        exit_notional = safe_price * multiplier
        exit_tax = torch.floor(exit_notional * tax + 0.5)
        pnl = pnl + (filled * (sign * (safe_price - entry) * multiplier - fee - exit_tax)).sum()
        turnover = turnover + (filled.detach() * exit_notional).sum()
        remaining = remaining - filled
    return remaining, pnl, turnover


def _scheduled_futures_day(
    requested: torch.Tensor, execution: torch.Tensor, equity: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """One fixed daily order and its causal limit/market exit sequence.

    Only completed entry evidence constrains entry. Future exit capacity never
    shrinks an opening order. A residual is returned explicitly, never sold at
    a daily CLOSE, converted to stock margin, or silently erased.
    """
    multiplier, fee, tax = execution[:, :, 0], execution[:, :, 1], execution[:, :, 2]
    bars = execution[:, :, 3:TAPE_FIELDS].reshape(-1, 2, len(EVENT_MINUTES), len(BAR_FIELDS))
    entry = bars[:, :, 0, 0]
    caps = bars[:, :, 0, 4]
    daily = torch.zeros_like(entry, dtype=torch.bool)
    daily_close = torch.zeros_like(entry)
    if execution.size(-1) == HYBRID_TAPE_FIELDS:
        daily = execution[:, :, TAPE_FIELDS] == 1
        entry = torch.where(daily, execution[:, :, TAPE_FIELDS + 1], entry)
        daily_close = execution[:, :, TAPE_FIELDS + 2]
        caps = torch.where(daily, execution[:, :, TAPE_FIELDS + 3], caps)
    valid = (torch.isfinite(execution[:, :, :3]).all(-1)
             & (multiplier > 0) & (fee >= 0) & (tax >= 0)
             & torch.isfinite(entry) & (entry > 0)
             & torch.isfinite(caps) & (caps >= 0)
             & (~daily | (torch.isfinite(daily_close) & (daily_close > 0))))
    clean = lambda x: torch.where(valid, torch.nan_to_num(x), torch.zeros_like(x))
    notional = clean(entry * multiplier)
    entry_tax = torch.floor(clean(notional * tax) + 0.5)
    reserve = notional + 2 * (clean(fee) + entry_tax)
    target_cash = requested.abs() * equity.detach().clamp_min(0)
    counts_int = _integer_candidate_basket(target_cash, notional, reserve, caps, valid)
    # Fractional shadow of the same order, with exact integer forward fills.
    soft = target_cash[:, None] / torch.where(valid, reserve, torch.zeros_like(reserve)).sum(-1).clamp_min(1e-12)[:, None]
    soft = torch.where(valid, torch.minimum(soft, caps.clamp_min(0)), torch.zeros_like(soft))
    counts = soft + (counts_int.to(soft.dtype) - soft).detach()
    remaining = counts
    sign = torch.sign(requested)[:, None]
    pnl = -(counts * (clean(fee) + entry_tax)).sum()
    turnover = (counts.detach() * notional).sum()
    # The explicit early-history daily CLOSE has no invented 13:30 timestamp.
    daily_filled = torch.where(daily, counts, torch.zeros_like(counts))
    close_price = torch.where(daily & valid, daily_close, torch.zeros_like(daily_close))
    close_notional = close_price * clean(multiplier)
    close_tax = torch.floor(close_notional * clean(tax) + 0.5)
    pnl = pnl + (daily_filled * (sign * (close_price - clean(entry)) * clean(multiplier)
                                - clean(fee) - close_tax)).sum()
    turnover = turnover + (daily_filled.detach() * close_notional).sum()
    remaining = remaining - daily_filled
    remaining, pnl, turnover = _scheduled_exit_fills(
        remaining, sign, bars, valid, clean(entry), clean(multiplier),
        clean(fee), clean(tax), pnl, turnover,
    )
    exact_abs = (counts_int.to(notional.dtype) * notional).sum(-1)
    executed_weight = torch.sign(requested) * exact_abs / equity.detach().clamp_min(1e-12)
    signed = counts_int * sign.to(torch.int64)
    residual = torch.round(remaining.detach()).to(torch.int64) * sign.to(torch.int64)
    failed_exposure = (remaining * notional).sum() / equity.detach().clamp_min(1e-12)
    return pnl / equity.detach().clamp_min(1e-12), executed_weight, signed, residual, turnover / equity.detach().clamp_min(1e-12), failed_exposure


@lru_cache(maxsize=1)
def _compiled_scheduled_futures_day():
    """Fuse one session, keeping the chronological equity ledger outside it.

    The eager function remains the only fill/accounting implementation. Dynamic
    symbol width supports expanding folds without unrolling a full year or
    consuming a new fixed-shape graph for every universe. CUDA graphs are off:
    autograd retains several successive daily outputs until batch backward.
    Compile errors propagate; this path never silently switches executors.
    """
    return torch.compile(
        _scheduled_futures_day, fullgraph=True, dynamic=True,
        options={"triton.cudagraphs": False},
    )


def _scheduled_cash_bracket(requested, execution, equity):
    """Cash coordinates of the current basket and the next allocator breakpoint.

    Enumerate the same three standard/mini branches as the integer allocator,
    plus the next standard-contract boundary. Only entry facts enter this
    bracket; future exits are evaluated afterwards by the canonical executor.
    """
    multiplier, fee, tax = execution[:, :, :3].unbind(-1)
    bars = execution[:, :, 3:TAPE_FIELDS].reshape(-1, 2, len(EVENT_MINUTES), len(BAR_FIELDS))
    entry, caps = bars[:, :, 0, 0], bars[:, :, 0, 4]
    if execution.size(-1) == HYBRID_TAPE_FIELDS:
        daily = execution[:, :, TAPE_FIELDS] == 1
        entry = torch.where(daily, execution[:, :, TAPE_FIELDS + 1], entry)
        caps = torch.where(daily, execution[:, :, TAPE_FIELDS + 3], caps)
    valid = (torch.isfinite(execution[:, :, :3]).all(-1)
             & (multiplier > 0) & (fee >= 0) & (tax >= 0)
             & torch.isfinite(entry) & (entry > 0)
             & torch.isfinite(caps) & (caps >= 0))
    if execution.size(-1) == HYBRID_TAPE_FIELDS:
        close = execution[:, :, TAPE_FIELDS + 2]
        valid = valid & (~daily | (torch.isfinite(close) & (close > 0)))
    notional = torch.where(valid, torch.nan_to_num(entry * multiplier), 0.)
    reserve = notional + 2 * (
        torch.where(valid, fee, 0.)
        + torch.floor(torch.where(valid, notional * tax, 0.) + .5)
    )
    cash = requested.abs() * equity
    counts = _integer_candidate_basket(cash, notional, reserve, caps, valid)
    lower = (counts * reserve).sum(-1)
    budget = torch.maximum(cash, lower)
    cap = torch.where(valid, torch.floor(caps.clamp_min(0)), 0.)
    safe = torch.where(valid & (cap > 0), reserve, float("inf"))
    standard = torch.minimum(torch.floor(budget / safe[:, 0]), cap[:, 0])
    candidates = torch.stack((standard, (standard - 1).clamp_min(0),
                              torch.zeros_like(standard), standard + 1), -1)
    standard_cash = candidates * reserve[:, 0, None]
    mini = torch.where(standard_cash > budget[:, None], 0.,
                       torch.floor((budget[:, None] - standard_cash) / safe[:, 1, None]) + 1).clamp_min(0)
    candidate_cash = standard_cash + mini * reserve[:, 1, None]
    feasible = ((candidates <= cap[:, 0, None]) & (mini <= cap[:, 1, None])
                & (candidate_cash > budget[:, None]))
    upper = torch.where(feasible, candidate_cash, float("inf")).amin(-1)
    upper = torch.where(torch.isfinite(upper), upper, lower)
    return lower, upper


def _scheduled_symbol_payoffs(requested, execution, equity, *, return_failure=False):
    """Vectorize the authoritative whole-contract executor, without a copy."""
    def one(weight, tape):
        result = _scheduled_futures_day(weight[None], tape[None], equity)
        return result[0], (result[3] != 0).any() if return_failure else result[5]
    return torch.vmap(one)(requested, execution)


def _scheduled_recovery_gradient(requested, execution, equity, *, saturation_recovery=False):
    """Zero forward value; slope of adjacent executable basket outcomes.

    This is an explicit piecewise-linear training surrogate, not the derivative
    of a step function. Profit and residual constraint slopes use the same
    integer fees, tax and minute fills as evaluation. At zero use an improving
    one-sided basket slope, or zero when cash already beats both directions.
    A symmetric long/short difference would cancel fixed fees at zero and can
    encourage a trade even when both whole-contract alternatives lose money.
    The residual L1 exposure coefficient is one, as in the existing failure
    shadow. It is a constraint penalty, not an invented liquidation price.
    """
    with torch.no_grad():
        request = requested.detach()
        capital = equity.detach()
        lower, upper = _scheduled_cash_bracket(request, execution, capital)
        width = (upper - lower) / capital
        direction = torch.where(request >= 0, 1., -1.)
        high = direction * upper / capital
        low_net, low_residual = _scheduled_symbol_payoffs(request, execution, capital)
        high_net, high_residual = _scheduled_symbol_payoffs(high, execution, capital)
        opposite_net, opposite_residual = _scheduled_symbol_payoffs(-high, execution, capital)
        net_slope = direction * (high_net - low_net) / width.clamp_min(1e-12)
        residual_slope = direction * (high_residual - low_residual) / width.clamp_min(1e-12)
        slope = net_slope / (1 + low_net.sum()).clamp_min(1e-7) - residual_slope
        # At zero the right slope is ``slope``. The left slope includes the
        # opposite sign of d|weight|/dweight. Select a locally improving side;
        # a cash optimum must not be pushed into paying fixed costs.
        left = (-(opposite_net - low_net) / (1 + low_net.sum()).clamp_min(1e-7)
                + opposite_residual - low_residual) / width.clamp_min(1e-12)
        zero_slope = torch.where(
            (slope > 0) & (slope >= -left), slope,
            torch.where(left < 0, left, 0.),
        )
        slope = torch.where(request == 0, zero_slope, slope)
        slope = torch.where(width > 0, slope, 0.)
        if saturation_recovery:
            # No larger entry basket does not imply that the current basket
            # cannot be reduced. Probe below its actual cash cost, outside the
            # allocator's eight-epsilon affordability tolerance. Reuse that
            # allocator to obtain the previous basket (including denomination
            # substitutions); do not subtract one arbitrary candidate slot.
            decrement = torch.finfo(lower.dtype).eps * lower.abs().clamp_min(1.) * 32.
            previous_request = direction * (lower - decrement).clamp_min(0.) / capital
            previous_cash, _ = _scheduled_cash_bracket(previous_request, execution, capital)
            previous_net, previous_residual = _scheduled_symbol_payoffs(
                previous_request, execution, capital,
            )
            backward_width = (lower - previous_cash) / capital
            backward_slope = direction * (
                (low_net - previous_net) / (1 + low_net.sum()).clamp_min(1e-7)
                - (low_residual - previous_residual)
            ) / backward_width.clamp_min(1e-12)
            # At the upper capacity boundary the only feasible improvement is
            # inward. A profitable saturated basket stays stationary; a losing
            # or trapped marginal contract must retain its removal gradient.
            inward_slope = direction * torch.minimum(
                direction * backward_slope, torch.zeros_like(backward_slope),
            )
            slope = torch.where(
                (width == 0) & (backward_width > 0), inward_slope, slope,
            )
    return ((requested - requested.detach()) * slope).sum()


def _scheduled_execution_utility_gradient(requested, execution, equity):
    """Feasible finite improvement directions from exact basket utilities.

    This is a training-only search direction, not the derivative of integer
    execution. Cash is a feasible restoration point for a failed basket; the
    utility gap uses the same log(1e-7) failure value as the forward ledger.
    At a valid locally optimal basket neither neighbour should push it away.
    Other symbols are held fixed in each coordinate utility comparison (their
    residual flags are deliberately excluded, to restore multiple failures).
    """
    with torch.no_grad():
        request, capital = requested.detach(), equity.detach()
        direction = torch.where(request >= 0, 1., -1.)
        lower, upper = _scheduled_cash_bracket(request, execution, capital)
        decrement = torch.finfo(lower.dtype).eps * lower.abs().clamp_min(1.) * 32.
        previous = direction * (lower - decrement).clamp_min(0.) / capital
        previous_cash, _ = _scheduled_cash_bracket(previous, execution, capital)
        _, first_cash = _scheduled_cash_bracket(torch.zeros_like(request), execution, capital)
        first_opposite = -direction * first_cash / capital
        outcomes = partial(_scheduled_symbol_payoffs, return_failure=True)
        current_net, current_failed = outcomes(request, execution, capital)
        up_net, up_failed = outcomes(direction * upper / capital, execution, capital)
        down_net, down_failed = outcomes(previous, execution, capital)
        opposite_net, opposite_failed = outcomes(first_opposite, execution, capital)
        other_net = current_net.sum() - current_net

        def utility(net, failed):
            total = other_net + net
            feasible = ~failed & torch.isfinite(total) & (total > -1.0)
            return torch.where(
                feasible, torch.log1p(total.clamp_min(-1.0 + 1e-7)),
                torch.full_like(total, math.log(1e-7)),
            )

        current_u = utility(current_net, current_failed)
        cash_u = utility(torch.zeros_like(current_net), torch.zeros_like(current_failed))
        up_u, down_u = utility(up_net, up_failed), utility(down_net, down_failed)
        opposite_u = utility(opposite_net, opposite_failed)
        up_width = (upper - lower) / capital
        down_width = (lower - previous_cash) / capital
        cash_width = lower / capital
        opposite_width = request.abs() + first_cash / capital
        # The next basket, previous basket, cash, and opposite first basket
        # are labels only. No future price/capacity enters the model or masks.
        gains = torch.stack(
            (up_u - current_u, down_u - current_u, cash_u - current_u,
             opposite_u - current_u), -1,
        )
        widths = torch.stack((up_width, down_width, cash_width, opposite_width), -1)
        valid = torch.stack(
            (up_width > 0, down_width > 0, cash_width > 0,
             (lower == 0) & (first_cash > 0)), -1,
        )
        # On a failed current basket, restore toward cash. A constant failure
        # value at two adjacent trapped baskets must not erase this direction.
        failed = (current_failed | ~torch.isfinite(other_net + current_net)
                  | (other_net + current_net <= -1.0))
        empty = torch.zeros_like(failed)
        restore = torch.stack((empty, empty, cash_width > 0, empty), -1)
        valid = torch.where(failed[:, None], restore, valid)
        gains = torch.where(valid, gains, torch.zeros_like(gains)).clamp_min(0.0)
        best = gains.argmax(-1, keepdim=True)
        best_gain = gains.gather(-1, best).squeeze(-1)
        best_width = widths.gather(-1, best).squeeze(-1).clamp_min(1e-12)
        signs = torch.stack((direction, -direction, -direction, -direction), -1)
        slope = signs.gather(-1, best).squeeze(-1) * best_gain / best_width
    return ((requested - requested.detach()) * slope).sum()


@lru_cache(maxsize=3)
def _compiled_scheduled_recovery_gradient(
    saturation_recovery=False, recovery_objective="residual_notional",
):
    gradient = (
        _scheduled_execution_utility_gradient if recovery_objective == "execution_utility"
        else partial(_scheduled_recovery_gradient, saturation_recovery=saturation_recovery)
    )
    return torch.compile(
        gradient,
        fullgraph=True, dynamic=True,
        options={"triton.cudagraphs": False},
    )


def run_tw_stock_futures_day_trade_integer_torch(
    target_weights: torch.Tensor,
    candidate_execution: torch.Tensor,
    *,
    initial_capital: float,
    state_advance_mask: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    return_weights_history: bool = True,
    scheduled_events: bool = False,
    use_compile: bool | None = None,
    recoverable_backward: bool = False,
    saturation_recovery: bool = False,
    recovery_objective: str = "residual_notional",
) -> StockFuturesDayTradeTensorResult:
    """Run an exact-forward, fully collateralized whole-contract day ledger.

    Candidate channels are ``[long net return, short net return, opening
    notional, causally reserved cash, maximum contracts]``.  Each stock owns
    its model-requested absolute cash sleeve; unfilled residual stays cash and
    cannot be reassigned to another stock.  Long and short reserve the same
    full absolute opening notional.  A straight-through surrogate supplies
    gradients while every forward PnL, cost, quantity, and equity update uses
    the integer basket.
    """

    if target_weights.ndim != 2 or target_weights.numel() == 0:
        raise ValueError(
            "integer stock-futures target_weights must have non-empty shape [T,S]"
        )
    if candidate_execution.ndim != 4 or tuple(candidate_execution.shape[:2]) != tuple(
        target_weights.shape
    ) or tuple(candidate_execution.shape[2:]) not in (
        {(2, TAPE_FIELDS), (2, HYBRID_TAPE_FIELDS)} if scheduled_events else {(2, 5)}
    ):
        raise ValueError(
            "candidate_execution has incompatible standard/mini channels"
        )
    capital = float(initial_capital)
    if not capital > 0.0 or not math.isfinite(capital):
        raise ValueError("initial_capital must be finite and positive")
    if recoverable_backward and not scheduled_events:
        raise ValueError("recoverable stock-futures backward requires scheduled minute events")
    if saturation_recovery and not scheduled_events:
        raise ValueError("saturation recovery requires scheduled minute events")
    if recovery_objective not in {"residual_notional", "execution_utility"}:
        raise ValueError("unknown minute recovery objective")
    if recovery_objective != "residual_notional" and not scheduled_events:
        raise ValueError("execution utility recovery requires scheduled minute events")

    weights = torch.nan_to_num(
        target_weights.to(dtype=torch.float32),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    execution = candidate_execution.to(device=weights.device, dtype=torch.float32)
    rows, symbols = tuple(weights.shape)
    advance = (
        torch.ones((rows,), device=weights.device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=weights.device, dtype=torch.bool)
    )
    if tuple(advance.shape) != (rows,):
        raise ValueError("state_advance_mask must have shape [T]")
    alive = (
        torch.ones((), device=weights.device, dtype=torch.bool)
        if initial_alive is None
        else initial_alive.detach().clone().to(device=weights.device, dtype=torch.bool).reshape(())
    )
    starting_scale = (
        weights.new_ones(())
        if initial_equity_scale is None
        else initial_equity_scale.to(device=weights.device, dtype=weights.dtype).reshape(())
    )
    equity = weights.new_tensor(capital) * starting_scale

    compile_requested = (
        os.environ.get("STOCKAGENT_BACKTEST_COMPILE", "0").lower() in {"1", "true", "yes", "on"}
        if use_compile is None else bool(use_compile)
    )
    scheduled_day = _scheduled_futures_day
    recovery_gradient = partial(_scheduled_recovery_gradient, saturation_recovery=saturation_recovery)
    if recovery_objective == "execution_utility":
        recovery_gradient = _scheduled_execution_utility_gradient
    training_recovery = bool(recoverable_backward and torch.is_grad_enabled() and weights.requires_grad)
    if (scheduled_events and compile_requested and weights.device.type == "cuda"
            and not torch.compiler.is_compiling()):
        scheduled_day = _compiled_scheduled_futures_day()
        if training_recovery:
            recovery_gradient = _compiled_scheduled_recovery_gradient(saturation_recovery, recovery_objective)

    return_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    weight_rows: list[torch.Tensor] = []
    quantity_rows: list[torch.Tensor] = []
    equity_scale_rows: list[torch.Tensor] = []
    residual_rows: list[torch.Tensor] = []
    default_rows: list[torch.Tensor] = []
    for row in range(rows):
        requested = torch.where(
            advance[row] & alive,
            weights[row],
            torch.zeros_like(weights[row]),
        )
        if scheduled_events:
            net_simple, exact_signed_weight, signed_counts, residual, turnover, failed_exposure = scheduled_day(
                requested, execution[row], equity,
            )
            next_equity = equity * (1.0 + net_simple)
            failed = (residual != 0).any() | ~torch.isfinite(next_equity) | (next_equity <= 0)
            row_alive = ~failed
            # Separate the absorbing failure penalty from an invented sale.
            # Its bounded shadow discourages trapped exposure even when the
            # exact forward account is rejected, avoiding a constant dead loss.
            failure_log = torch.full_like(net_simple, math.log(1e-7)) - (failed_exposure - failed_exposure.detach())
            log_return = torch.where(row_alive, torch.log1p(net_simple.clamp_min(-1 + 1e-7)), failure_log)
            reported_log = torch.where(advance[row] & alive, log_return, torch.zeros_like(log_return))
            if training_recovery:
                # A failed exact account stays failed. Counterfactual daily
                # gradients can still teach all remaining dates to be feasible.
                # Reset only the backward reference cash, never exact state.
                reference_equity = torch.where(alive & (equity > 0), equity.detach(), weights.new_tensor(capital))
                shadow_delta = recovery_gradient(weights[row], execution[row], reference_equity)
                reported_log = reported_log.detach() + torch.where(advance[row], shadow_delta, torch.zeros_like(shadow_delta))
            return_rows.append(reported_log)
            default_rows.append(advance[row] & alive & failed)
            residual_rows.append(residual)
            equity = torch.where(advance[row] & alive, torch.where(row_alive, next_equity, torch.zeros_like(next_equity)), equity)
            alive = alive & ((~advance[row]) | row_alive)
            weight_rows.append(requested + (exact_signed_weight - requested).detach())
            quantity_rows.append(signed_counts)
            turnover_rows.append(turnover)
            equity_scale_rows.append(equity / capital)
            continue
        long_net = execution[row, :, :, 0]
        short_net = execution[row, :, :, 1]
        notionals = execution[row, :, :, 2]
        cash_required = execution[row, :, :, 3]
        maximum_contracts = execution[row, :, :, 4]
        valid = (
            torch.isfinite(execution[row]).all(dim=-1)
            & (notionals > 0.0)
            & (cash_required >= notionals)
            & (maximum_contracts >= 0.0)
        )
        target_cash = requested.abs() * equity.detach().clamp_min(0.0)
        counts_abs = _integer_candidate_basket(
            target_cash,
            notionals,
            cash_required,
            maximum_contracts,
            valid,
        )
        signed_counts = counts_abs * torch.sign(requested).to(dtype=torch.int64)[:, None]
        counts_f = counts_abs.to(dtype=weights.dtype)
        safe_notionals = torch.where(valid, notionals, torch.zeros_like(notionals))
        exact_abs_notional = (counts_f * safe_notionals).sum(dim=-1)
        exact_signed_weight = (
            torch.sign(requested)
            * exact_abs_notional
            / equity.detach().clamp_min(1.0e-12)
        )
        exact_abs_weight = exact_abs_notional / equity.detach().clamp_min(1.0e-12)

        directional = 0.5 * (long_net - short_net)
        cost_rate = -0.5 * (long_net + short_net)
        candidate_proxy = torch.where(valid, safe_notionals, torch.zeros_like(safe_notionals))
        proxy_denom = candidate_proxy.sum(dim=-1).clamp_min(1.0e-12)
        proxy_directional = (
            torch.where(valid, directional, torch.zeros_like(directional))
            * candidate_proxy
        ).sum(dim=-1) / proxy_denom
        proxy_cost = (
            torch.where(valid, cost_rate, torch.zeros_like(cost_rate))
            * candidate_proxy
        ).sum(dim=-1) / proxy_denom
        surrogate_net = (
            requested * proxy_directional - requested.abs() * proxy_cost
        ).sum()
        long_pnl = counts_f * safe_notionals * torch.where(
            valid, long_net, torch.zeros_like(long_net)
        )
        short_pnl = counts_f * safe_notionals * torch.where(
            valid, short_net, torch.zeros_like(short_net)
        )
        exact_net_pnl = torch.where(
            requested[:, None] >= 0.0,
            long_pnl,
            short_pnl,
        ).sum()
        exact_net_simple = exact_net_pnl / equity.detach().clamp_min(1.0e-12)
        net_simple = surrogate_net + (exact_net_simple - surrogate_net).detach()
        next_equity = equity * (1.0 + net_simple)
        row_alive = torch.isfinite(next_equity) & (next_equity > 0.0)
        safe_simple = torch.where(
            row_alive,
            net_simple,
            torch.full_like(net_simple, -1.0 + 1.0e-7),
        )
        log_return = torch.where(
            advance[row] & alive,
            torch.log1p(safe_simple),
            torch.zeros_like(safe_simple),
        )
        equity = torch.where(row_alive, next_equity, torch.zeros_like(next_equity))
        alive = alive & row_alive
        executed_weight = requested + (exact_signed_weight - requested).detach()
        return_rows.append(log_return)
        turnover_rows.append(2.0 * exact_abs_weight.sum())
        weight_rows.append(executed_weight)
        quantity_rows.append(signed_counts)
        equity_scale_rows.append(equity / capital)

    strategy_returns = torch.stack(return_rows)
    turnovers = torch.stack(turnover_rows)
    equity_scales = torch.stack(equity_scale_rows)
    weights_history = (
        torch.stack(weight_rows)
        if return_weights_history
        else weights.new_empty((0, symbols))
    )
    quantities_history = (
        torch.stack(quantity_rows)
        if return_weights_history
        else torch.empty((0, symbols, 2), device=weights.device, dtype=torch.int64)
    )
    return StockFuturesDayTradeTensorResult(
        strategy_returns=strategy_returns,
        turnovers=turnovers,
        weights_history=weights_history,
        final_weights=torch.zeros((symbols,), device=weights.device, dtype=torch.float32),
        final_alive=alive,
        equity_scale_history=equity_scales,
        final_equity_scale=equity_scales[-1] if equity_scales.numel() else starting_scale,
        contract_quantities_history=quantities_history,
        residual_contract_quantities_history=(torch.stack(residual_rows) if residual_rows else None),
        default_history=(torch.stack(default_rows) if default_rows else None),
    )


__all__ = [
    "StockFuturesDayTradeTensorResult",
    "run_tw_stock_futures_day_trade_continuous_torch",
    "run_tw_stock_futures_day_trade_integer_torch",
]
