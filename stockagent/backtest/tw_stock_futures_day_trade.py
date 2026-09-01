"""Differentiable daily-flat ledger for front-month single-stock futures."""

from __future__ import annotations

import math
from dataclasses import dataclass

import torch


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


def run_tw_stock_futures_day_trade_integer_torch(
    target_weights: torch.Tensor,
    candidate_execution: torch.Tensor,
    *,
    initial_capital: float,
    state_advance_mask: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    return_weights_history: bool = True,
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
    ) or tuple(candidate_execution.shape[2:]) != (2, 5):
        raise ValueError(
            "candidate_execution must have shape [T,S,2,5] for standard/mini"
        )
    capital = float(initial_capital)
    if not capital > 0.0 or not math.isfinite(capital):
        raise ValueError("initial_capital must be finite and positive")

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

    return_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    weight_rows: list[torch.Tensor] = []
    quantity_rows: list[torch.Tensor] = []
    equity_scale_rows: list[torch.Tensor] = []
    for row in range(rows):
        requested = torch.where(
            advance[row] & alive,
            weights[row],
            torch.zeros_like(weights[row]),
        )
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
    )


__all__ = [
    "StockFuturesDayTradeTensorResult",
    "run_tw_stock_futures_day_trade_continuous_torch",
    "run_tw_stock_futures_day_trade_integer_torch",
]
