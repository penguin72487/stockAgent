"""Bounded CUDA compilation for the dual-session Taiwan eager oracle.

The canonical formulas remain in :mod:`stockagent.backtest.tw_dual_session`.
Compiling its Python loop as one 32-row FX graph makes Inductor scheduler work
grow prohibitively large.  This module therefore compiles one complete
exchange-session transition and reuses that fullgraph kernel for every row in
a fixed 32-row block.  The recurrent state and autograd graph remain connected
across calls; only a final non-aligned tail runs through the eager oracle.

CUDA graphs are deliberately disabled.  The stateful finance ABI includes
cash, T+2 queues, signed risky holdings, both restricted short-collateral
pools, alive state, and absolute equity scale.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import torch

from stockagent.backtest.tw_continuous import (
    _prepare_cash_short_state,
    _prepare_equity_scale,
    _resolve_short_maintenance_ratio,
)
from stockagent.backtest.tw_dual_session import (
    TaiwanDualSessionResult,
    _as_daily_nonnegative,
    _as_phase_mask,
    _as_phase_rate,
    _run_dual_session,
    _validate_actions,
)


COMPILED_BLOCK_ROWS = 32

_COMPILED_DAY_CACHE: dict[
    tuple[object, ...],
    Callable[..., tuple[torch.Tensor, ...]],
] = {}
_FAILED_COMPILE_KEYS: set[tuple[object, ...]] = set()
_COMPILE_STATS: dict[str, int] = {
    "compile_constructors": 0,
    "compiled_block_calls": 0,
    "compiled_day_calls": 0,
    "eager_tail_calls": 0,
    "eager_fallback_calls": 0,
}


@dataclass(slots=True)
class _PreparedInputs:
    mode: str
    actions: torch.Tensor
    overnight_log_returns: torch.Tensor
    intraday_log_returns: torch.Tensor
    tradable_mask: torch.Tensor
    can_buy_mask: torch.Tensor
    can_sell_mask: torch.Tensor
    buy_fee_rates: torch.Tensor
    sell_fee_rates: torch.Tensor
    can_short_open_mask: torch.Tensor
    force_short_cover_mask: torch.Tensor
    short_margin_rate: torch.Tensor
    short_capacity_weights: torch.Tensor
    short_handling_fee_rate: torch.Tensor
    unresolved_corporate_action_mask: torch.Tensor
    cash_dividend_yield: torch.Tensor
    cash_dividend_payment_delay_sessions: torch.Tensor
    force_exit_mask: torch.Tensor
    volume_limit_weights: torch.Tensor
    state_advance_mask: torch.Tensor
    initial_weights: torch.Tensor
    initial_cash: torch.Tensor
    initial_payables: torch.Tensor
    initial_receivables: torch.Tensor
    initial_short_sale_collateral: torch.Tensor
    initial_short_margin_collateral: torch.Tensor
    initial_alive: torch.Tensor
    initial_equity_scale: torch.Tensor
    settlement_lag_sessions: int
    claim_queue_sessions: int
    gross_budget: float
    max_turnover_ratio: float
    short_maintenance_ratio: float
    return_weights_history: bool
    has_short_contract: bool
    has_short_open_mask: bool
    has_force_short_cover_mask: bool
    has_short_capacity: bool
    has_volume_limit: bool
    has_cash_dividends: bool
    requires_state_grad: bool


def get_tw_dual_session_compile_stats(
    *,
    reset: bool = False,
) -> dict[str, int]:
    snapshot = dict(_COMPILE_STATS)
    if reset:
        for name in _COMPILE_STATS:
            _COMPILE_STATS[name] = 0
    return snapshot


def clear_tw_dual_session_compile_cache() -> None:
    """Clear only process-local callable/failure caches, not Inductor artifacts."""

    _COMPILED_DAY_CACHE.clear()
    _FAILED_COMPILE_KEYS.clear()


def _as_matrix(
    value: torch.Tensor,
    *,
    reference: torch.Tensor,
    name: str,
) -> torch.Tensor:
    result = torch.as_tensor(
        value,
        device=reference.device,
        dtype=reference.dtype,
    )
    if tuple(result.shape) != tuple(reference.shape):
        raise ValueError(
            f"{name} must have shape {tuple(reference.shape)}, "
            f"got {tuple(result.shape)}"
        )
    return result.contiguous()


def _resolved_claim_queue(
    *,
    settlement_lag_sessions: int,
    claim_queue_sessions: int | None,
) -> tuple[int, int]:
    if (
        isinstance(settlement_lag_sessions, bool)
        or int(settlement_lag_sessions) != settlement_lag_sessions
        or int(settlement_lag_sessions) <= 0
    ):
        raise ValueError("settlement_lag_sessions must be a positive integer")
    lag = int(settlement_lag_sessions)
    if claim_queue_sessions is None:
        return lag, lag
    if (
        isinstance(claim_queue_sessions, bool)
        or not isinstance(claim_queue_sessions, int)
        or int(claim_queue_sessions) < lag
    ):
        raise ValueError(
            "claim_queue_sessions must be an integer at least as large as "
            "settlement_lag_sessions"
        )
    return lag, int(claim_queue_sessions)


def _prepare_dividends(
    *,
    cash_dividend_yield: torch.Tensor | None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None,
    reference: torch.Tensor,
    claim_queue_sessions: int,
) -> tuple[torch.Tensor, torch.Tensor, bool]:
    has_dividends = cash_dividend_yield is not None
    if has_dividends != (cash_dividend_payment_delay_sessions is not None):
        raise ValueError(
            "cash_dividend_yield and cash_dividend_payment_delay_sessions "
            "must be supplied together"
        )
    if not has_dividends:
        return (
            torch.zeros_like(reference),
            torch.zeros_like(reference, dtype=torch.int64),
            False,
        )
    yields = torch.as_tensor(
        cash_dividend_yield,
        device=reference.device,
        dtype=reference.dtype,
    )
    raw_delays = torch.as_tensor(
        cash_dividend_payment_delay_sessions,
        device=reference.device,
    )
    if tuple(yields.shape) != tuple(reference.shape):
        raise ValueError("cash_dividend_yield must have shape [T,S]")
    if tuple(raw_delays.shape) != tuple(reference.shape):
        raise ValueError("cash_dividend_payment_delay_sessions must have shape [T,S]")
    if not bool(torch.isfinite(yields).all().item()) or bool(
        (yields < 0.0).any().item()
    ):
        raise ValueError("cash_dividend_yield must be finite and non-negative")
    delays = raw_delays.to(dtype=torch.int64)
    if raw_delays.is_floating_point() and not bool(
        (raw_delays == delays.to(dtype=raw_delays.dtype)).all().item()
    ):
        raise ValueError("cash_dividend_payment_delay_sessions must contain integers")
    events = yields > 0.0
    invalid = events & ((delays < 1) | (delays > int(claim_queue_sessions)))
    if bool(invalid.any().item()):
        raise ValueError(
            "cash-dividend payment delay exceeds the configured claim queue"
        )
    if bool(((~events) & (delays != 0)).any().item()):
        raise ValueError("cash-dividend payment delay must be zero when yield is zero")
    return yields.contiguous(), delays.contiguous(), True


def _prepare_inputs(
    *,
    mode: str,
    actions: torch.Tensor,
    overnight_log_returns: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor | float,
    sell_fee_rates: torch.Tensor | float,
    can_short_open_mask: torch.Tensor | None,
    force_short_cover_mask: torch.Tensor | None,
    short_margin_rate: torch.Tensor | float | None,
    short_capacity_weights: torch.Tensor | float | None,
    short_handling_fee_rate: torch.Tensor | float,
    short_maintenance_ratio: float,
    unresolved_corporate_action_mask: torch.Tensor | None,
    cash_dividend_yield: torch.Tensor | None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None,
    claim_queue_sessions: int | None,
    force_exit_mask: torch.Tensor | None,
    settlement_lag_sessions: int,
    gross_budget: float,
    max_turnover_ratio: float,
    volume_limit_weights: torch.Tensor | float | None,
    state_advance_mask: torch.Tensor | None,
    return_weights_history: bool,
    initial_weights: torch.Tensor | None,
    initial_cash: torch.Tensor | None,
    initial_payables: torch.Tensor | None,
    initial_receivables: torch.Tensor | None,
    initial_alive: torch.Tensor | None,
    initial_equity_scale: torch.Tensor | None,
    initial_short_sale_collateral: torch.Tensor | None,
    initial_short_margin_collateral: torch.Tensor | None,
) -> _PreparedInputs:
    phases = 2 if mode == "tw_cash" else 3
    action_values = _validate_actions(
        actions,
        phases=phases,
        name="actions",
    ).contiguous()
    rows, _, symbols = action_values.shape
    daily_reference = action_values[:, 0]
    phase_reference = action_values.new_zeros((rows, 2, symbols))
    gap = _as_matrix(
        overnight_log_returns,
        reference=daily_reference,
        name="overnight_log_returns",
    )
    intraday = _as_matrix(
        intraday_log_returns,
        reference=daily_reference,
        name="intraday_log_returns",
    )
    tradable = _as_phase_mask(
        tradable_mask,
        reference=phase_reference,
        name="tradable_mask",
        default=False,
    ).contiguous()
    buy_mask = _as_phase_mask(
        can_buy_mask,
        reference=phase_reference,
        name="can_buy_mask",
        default=False,
    ).contiguous()
    sell_mask = _as_phase_mask(
        can_sell_mask,
        reference=phase_reference,
        name="can_sell_mask",
        default=False,
    ).contiguous()
    short_open = (
        _as_phase_mask(
            can_short_open_mask,
            reference=phase_reference,
            name="can_short_open_mask",
            default=False,
        )
        & sell_mask
    ).contiguous()
    forced_cover = _as_phase_mask(
        force_short_cover_mask,
        reference=phase_reference,
        name="force_short_cover_mask",
        default=False,
    ).contiguous()
    unresolved = _as_phase_mask(
        unresolved_corporate_action_mask,
        reference=phase_reference,
        name="unresolved_corporate_action_mask",
        default=False,
    ).contiguous()
    terminal = _as_phase_mask(
        force_exit_mask,
        reference=phase_reference,
        name="force_exit_mask",
        default=False,
    ).contiguous()
    buy_fees = _as_phase_rate(
        buy_fee_rates,
        reference=phase_reference,
        name="buy_fee_rates",
    ).contiguous()
    sell_fees = _as_phase_rate(
        sell_fee_rates,
        reference=phase_reference,
        name="sell_fee_rates",
    ).contiguous()
    if bool((sell_fees >= 1.0).any().item()):
        raise ValueError("sell_fee_rates must be less than 1")

    has_short_open_mask = can_short_open_mask is not None
    has_force_short_cover_mask = force_short_cover_mask is not None
    has_short_capacity = short_capacity_weights is not None
    has_explicit_short_handling = (
        bool(short_handling_fee_rate.numel())
        if isinstance(short_handling_fee_rate, torch.Tensor)
        else float(short_handling_fee_rate) != 0.0
    )
    has_short_contract = (
        short_margin_rate is not None
        or has_explicit_short_handling
        or any(
            value is not None
            for value in (
                can_short_open_mask,
                force_short_cover_mask,
                short_capacity_weights,
                initial_short_sale_collateral,
                initial_short_margin_collateral,
            )
        )
    )
    if can_short_open_mask is not None:
        if short_margin_rate is None:
            raise ValueError(
                "can_short_open_mask requires an explicit short_margin_rate"
            )
        if short_capacity_weights is None:
            raise ValueError(
                "can_short_open_mask requires explicit short_capacity_weights"
            )
    margin_rates = _as_phase_rate(
        0.90 if short_margin_rate is None else short_margin_rate,
        reference=phase_reference,
        name="short_margin_rate",
    ).contiguous()
    handling_rates = _as_phase_rate(
        short_handling_fee_rate,
        reference=phase_reference,
        name="short_handling_fee_rate",
    ).contiguous()
    if bool((sell_fees + handling_rates >= 1.0).any().item()):
        raise ValueError(
            "sell fees plus short handling fees must leave positive sale collateral"
        )
    short_cap = _as_daily_nonnegative(
        short_capacity_weights,
        reference=daily_reference,
        name="short_capacity_weights",
        allow_positive_inf=True,
    )
    if short_cap is None:
        short_cap = torch.zeros_like(daily_reference)
    else:
        short_cap = short_cap.contiguous()
    volume_cap = _as_daily_nonnegative(
        volume_limit_weights,
        reference=daily_reference,
        name="volume_limit_weights",
    )
    has_volume_limit = volume_cap is not None
    if volume_cap is None:
        volume_cap = torch.zeros_like(daily_reference)
    else:
        volume_cap = volume_cap.contiguous()

    lag, claim_queue = _resolved_claim_queue(
        settlement_lag_sessions=settlement_lag_sessions,
        claim_queue_sessions=claim_queue_sessions,
    )
    dividend_yields, dividend_delays, has_dividends = _prepare_dividends(
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(cash_dividend_payment_delay_sessions),
        reference=daily_reference,
        claim_queue_sessions=claim_queue,
    )
    if state_advance_mask is None:
        advance = torch.ones(
            (rows,),
            device=action_values.device,
            dtype=torch.bool,
        )
    else:
        advance = torch.as_tensor(
            state_advance_mask,
            device=action_values.device,
            dtype=torch.bool,
        )
        if tuple(advance.shape) != (rows,):
            raise ValueError("state_advance_mask must have shape [T]")
        advance = advance.contiguous()

    if mode == "tw_overnight":
        exit_fraction = action_values[:, 0]
        if bool(((exit_fraction < 0.0) | (exit_fraction > 1.0)).any().item()):
            raise ValueError(
                "tw_overnight actions[:,0,:] is an already-resolved "
                "exit-at-open fraction and must be within [0,1]"
            )
        entry_open = action_values[:, 1]
        entry_close = action_values[:, 2]
        reverse = (
            (entry_open.abs() > 1.0e-10)
            & (entry_close.abs() > 1.0e-10)
            & ((entry_open > 0.0) != (entry_close > 0.0))
        )
        if bool(reverse.any().item()):
            raise ValueError(
                "tw_overnight open-entry and close-entry allocations may not "
                "reverse the same symbol within one session"
            )

    (
        risky,
        cash,
        payables,
        receivables,
        short_sale_collateral,
        short_margin_collateral,
        alive,
        _,
    ) = _prepare_cash_short_state(
        daily_reference,
        settlement_lag_sessions=lag,
        claim_queue_sessions=claim_queue,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_alive=initial_alive,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        state_advance_mask=advance,
        detach_initial_state=True,
    )
    equity_scale = _prepare_equity_scale(
        daily_reference,
        initial_equity_scale=initial_equity_scale,
        alive=alive,
        detach_initial_state=True,
    )
    floating_inputs = (
        action_values,
        gap,
        intraday,
        buy_fees,
        sell_fees,
        margin_rates,
        short_cap,
        handling_rates,
        dividend_yields,
        volume_cap,
    )
    requires_state_grad = bool(
        torch.is_grad_enabled()
        and any(value.requires_grad for value in floating_inputs)
    )
    if requires_state_grad:
        risky = risky.detach().clone().requires_grad_(True)
        cash = cash.detach().clone().requires_grad_(True)
        payables = payables.detach().clone().requires_grad_(True)
        receivables = receivables.detach().clone().requires_grad_(True)
        short_sale_collateral = (
            short_sale_collateral.detach().clone().requires_grad_(True)
        )
        short_margin_collateral = (
            short_margin_collateral.detach().clone().requires_grad_(True)
        )
        equity_scale = equity_scale.detach().clone().requires_grad_(True)

    return _PreparedInputs(
        mode=mode,
        actions=action_values,
        overnight_log_returns=gap,
        intraday_log_returns=intraday,
        tradable_mask=tradable,
        can_buy_mask=buy_mask,
        can_sell_mask=sell_mask,
        buy_fee_rates=buy_fees,
        sell_fee_rates=sell_fees,
        can_short_open_mask=short_open,
        force_short_cover_mask=forced_cover,
        short_margin_rate=margin_rates,
        short_capacity_weights=short_cap,
        short_handling_fee_rate=handling_rates,
        unresolved_corporate_action_mask=unresolved,
        cash_dividend_yield=dividend_yields,
        cash_dividend_payment_delay_sessions=dividend_delays,
        force_exit_mask=terminal,
        volume_limit_weights=volume_cap,
        state_advance_mask=advance,
        initial_weights=risky,
        initial_cash=cash,
        initial_payables=payables,
        initial_receivables=receivables,
        initial_short_sale_collateral=short_sale_collateral,
        initial_short_margin_collateral=short_margin_collateral,
        initial_alive=alive,
        initial_equity_scale=equity_scale,
        settlement_lag_sessions=lag,
        claim_queue_sessions=claim_queue,
        gross_budget=float(gross_budget),
        max_turnover_ratio=float(max_turnover_ratio),
        short_maintenance_ratio=_resolve_short_maintenance_ratio(
            short_maintenance_ratio
        ),
        return_weights_history=bool(return_weights_history),
        has_short_contract=has_short_contract,
        has_short_open_mask=has_short_open_mask,
        has_force_short_cover_mask=has_force_short_cover_mask,
        has_short_capacity=has_short_capacity,
        has_volume_limit=has_volume_limit,
        has_cash_dividends=has_dividends,
        requires_state_grad=requires_state_grad,
    )


def _compile_key(prepared: _PreparedInputs) -> tuple[object, ...]:
    device_index = (
        prepared.actions.device.index
        if prepared.actions.device.index is not None
        else torch.cuda.current_device()
    )
    requires_grad_bitmap = tuple(
        value.requires_grad
        for value in (
            prepared.actions,
            prepared.overnight_log_returns,
            prepared.intraday_log_returns,
            prepared.buy_fee_rates,
            prepared.sell_fee_rates,
            prepared.short_margin_rate,
            prepared.short_capacity_weights,
            prepared.short_handling_fee_rate,
            prepared.cash_dividend_yield,
            prepared.volume_limit_weights,
            prepared.initial_weights,
            prepared.initial_cash,
            prepared.initial_payables,
            prepared.initial_receivables,
            prepared.initial_short_sale_collateral,
            prepared.initial_short_margin_collateral,
            prepared.initial_equity_scale,
        )
    )
    return (
        prepared.mode,
        int(device_index),
        str(prepared.actions.dtype),
        int(prepared.actions.size(2)),
        COMPILED_BLOCK_ROWS,
        prepared.settlement_lag_sessions,
        prepared.claim_queue_sessions,
        prepared.gross_budget,
        prepared.max_turnover_ratio,
        prepared.short_maintenance_ratio,
        prepared.return_weights_history,
        prepared.has_short_contract,
        prepared.has_short_open_mask,
        prepared.has_force_short_cover_mask,
        prepared.has_short_capacity,
        prepared.has_volume_limit,
        prepared.has_cash_dividends,
        bool(torch.is_grad_enabled()),
        requires_grad_bitmap,
        prepared.requires_state_grad,
        bool(torch.is_inference_mode_enabled()),
    )


def _compiled_day_runner(
    prepared: _PreparedInputs,
) -> tuple[tuple[object, ...], Callable[..., tuple[torch.Tensor, ...]]]:
    key = _compile_key(prepared)
    cached = _COMPILED_DAY_CACHE.get(key)
    if cached is not None:
        return key, cached

    mode = prepared.mode
    has_short_contract = prepared.has_short_contract
    has_short_open_mask = prepared.has_short_open_mask
    has_force_short_cover_mask = prepared.has_force_short_cover_mask
    has_short_capacity = prepared.has_short_capacity
    has_volume_limit = prepared.has_volume_limit
    has_cash_dividends = prepared.has_cash_dividends
    return_history = prepared.return_weights_history
    lag = prepared.settlement_lag_sessions
    claim_queue = prepared.claim_queue_sessions
    gross_budget = prepared.gross_budget
    max_turnover = prepared.max_turnover_ratio
    maintenance = prepared.short_maintenance_ratio

    def run_day(
        day_actions: torch.Tensor,
        day_overnight_log_returns: torch.Tensor,
        day_intraday_log_returns: torch.Tensor,
        day_tradable_mask: torch.Tensor,
        day_can_buy_mask: torch.Tensor,
        day_can_sell_mask: torch.Tensor,
        day_buy_fee_rates: torch.Tensor,
        day_sell_fee_rates: torch.Tensor,
        day_can_short_open_mask: torch.Tensor,
        day_force_short_cover_mask: torch.Tensor,
        day_short_margin_rate: torch.Tensor,
        day_short_capacity_weights: torch.Tensor,
        day_short_handling_fee_rate: torch.Tensor,
        day_unresolved_corporate_action_mask: torch.Tensor,
        day_cash_dividend_yield: torch.Tensor,
        day_cash_dividend_payment_delay_sessions: torch.Tensor,
        day_force_exit_mask: torch.Tensor,
        day_volume_limit_weights: torch.Tensor,
        day_state_advance_mask: torch.Tensor,
        initial_weights: torch.Tensor,
        initial_cash: torch.Tensor,
        initial_payables: torch.Tensor,
        initial_receivables: torch.Tensor,
        initial_short_sale_collateral: torch.Tensor,
        initial_short_margin_collateral: torch.Tensor,
        initial_alive: torch.Tensor,
        initial_equity_scale: torch.Tensor,
    ) -> tuple[torch.Tensor, ...]:
        result = _run_dual_session(
            mode=mode,
            actions=day_actions,
            overnight_log_returns=day_overnight_log_returns,
            intraday_log_returns=day_intraday_log_returns,
            tradable_mask=day_tradable_mask,
            can_buy_mask=day_can_buy_mask,
            can_sell_mask=day_can_sell_mask,
            buy_fee_rates=day_buy_fee_rates,
            sell_fee_rates=day_sell_fee_rates,
            can_short_open_mask=(
                day_can_short_open_mask if has_short_open_mask else None
            ),
            force_short_cover_mask=(
                day_force_short_cover_mask if has_force_short_cover_mask else None
            ),
            short_margin_rate=(day_short_margin_rate if has_short_contract else None),
            short_capacity_weights=(
                day_short_capacity_weights if has_short_capacity else None
            ),
            short_handling_fee_rate=(
                day_short_handling_fee_rate if has_short_contract else 0.0
            ),
            short_maintenance_ratio=maintenance,
            unresolved_corporate_action_mask=(day_unresolved_corporate_action_mask),
            cash_dividend_yield=(
                day_cash_dividend_yield if has_cash_dividends else None
            ),
            cash_dividend_payment_delay_sessions=(
                day_cash_dividend_payment_delay_sessions if has_cash_dividends else None
            ),
            claim_queue_sessions=claim_queue,
            force_exit_mask=day_force_exit_mask,
            settlement_lag_sessions=lag,
            gross_budget=gross_budget,
            max_turnover_ratio=max_turnover,
            volume_limit_weights=(
                day_volume_limit_weights if has_volume_limit else None
            ),
            state_advance_mask=day_state_advance_mask,
            return_weights_history=return_history,
            initial_weights=initial_weights,
            initial_cash=initial_cash,
            initial_payables=initial_payables,
            initial_receivables=initial_receivables,
            initial_alive=initial_alive,
            initial_equity_scale=initial_equity_scale,
            initial_short_sale_collateral=initial_short_sale_collateral,
            initial_short_margin_collateral=initial_short_margin_collateral,
            detach_initial_state=False,
        )
        empty_symbol = result.final_weights.new_empty((0,))
        empty_phase = result.final_weights.new_empty((0, 2))
        weights = result.weights_history[0] if return_history else empty_symbol
        open_weights = (
            result.open_weights_history[0] if return_history else empty_symbol
        )
        close_weights = (
            result.close_weights_history[0] if return_history else empty_symbol
        )
        buys = result.executed_buy_weights[0] if return_history else empty_phase
        sells = result.executed_sell_weights[0] if return_history else empty_phase
        long_buys = (
            result.executed_long_buy_weights[0] if return_history else empty_phase
        )
        long_sells = (
            result.executed_long_sell_weights[0] if return_history else empty_phase
        )
        short_opens = (
            result.executed_short_open_weights[0] if return_history else empty_phase
        )
        short_covers = (
            result.executed_short_cover_weights[0] if return_history else empty_phase
        )
        due = (
            result.due_weights_history[0]
            if mode == "tw_overnight" and return_history
            else empty_symbol
        )
        event_turnover = result.event_turnovers[0] if return_history else empty_phase
        return (
            result.strategy_returns[0],
            result.turnovers[0],
            event_turnover,
            weights,
            open_weights,
            close_weights,
            buys,
            sells,
            long_buys,
            long_sells,
            short_opens,
            short_covers,
            due,
            result.settlement_default[0],
            result.final_weights,
            result.final_cash,
            result.final_payables,
            result.final_receivables,
            result.final_short_sale_collateral,
            result.final_short_margin_collateral,
            result.final_alive,
            result.final_equity_scale,
        )

    compiled = torch.compile(
        run_day,
        fullgraph=True,
        dynamic=False,
        options={"triton.cudagraphs": False},
    )
    _COMPILED_DAY_CACHE[key] = compiled
    _COMPILE_STATS["compile_constructors"] += 1
    return key, compiled


def _day_inputs(
    prepared: _PreparedInputs,
    index: int,
) -> tuple[torch.Tensor, ...]:
    sl = slice(index, index + 1)
    return (
        prepared.actions[sl],
        prepared.overnight_log_returns[sl],
        prepared.intraday_log_returns[sl],
        prepared.tradable_mask[sl],
        prepared.can_buy_mask[sl],
        prepared.can_sell_mask[sl],
        prepared.buy_fee_rates[sl],
        prepared.sell_fee_rates[sl],
        prepared.can_short_open_mask[sl],
        prepared.force_short_cover_mask[sl],
        prepared.short_margin_rate[sl],
        prepared.short_capacity_weights[sl],
        prepared.short_handling_fee_rate[sl],
        prepared.unresolved_corporate_action_mask[sl],
        prepared.cash_dividend_yield[sl],
        prepared.cash_dividend_payment_delay_sessions[sl],
        prepared.force_exit_mask[sl],
        prepared.volume_limit_weights[sl],
        prepared.state_advance_mask[sl],
    )


def _state_tuple(prepared: _PreparedInputs) -> tuple[torch.Tensor, ...]:
    return (
        prepared.initial_weights,
        prepared.initial_cash,
        prepared.initial_payables,
        prepared.initial_receivables,
        prepared.initial_short_sale_collateral,
        prepared.initial_short_margin_collateral,
        prepared.initial_alive,
        prepared.initial_equity_scale,
    )


def _next_state(values: tuple[torch.Tensor, ...]) -> tuple[torch.Tensor, ...]:
    return (
        values[14],
        values[15],
        values[16],
        values[17],
        values[18],
        values[19],
        values[20],
        values[21],
    )


def _prefix_result(
    *,
    prepared: _PreparedInputs,
    rows: list[tuple[torch.Tensor, ...]],
) -> TaiwanDualSessionResult:
    symbols = int(prepared.actions.size(2))
    empty_weights = prepared.actions.new_empty((0, symbols))
    empty_phase = prepared.actions.new_empty((0, 2, symbols))
    record = prepared.return_weights_history
    final = rows[-1]
    return TaiwanDualSessionResult(
        mode=prepared.mode,
        strategy_returns=torch.stack([row[0] for row in rows]),
        turnovers=torch.stack([row[1] for row in rows]),
        event_turnovers=(
            torch.stack([row[2] for row in rows])
            if record
            else prepared.actions.new_empty((0, 2))
        ),
        weights_history=(
            torch.stack([row[3] for row in rows]) if record else empty_weights
        ),
        open_weights_history=(
            torch.stack([row[4] for row in rows]) if record else empty_weights
        ),
        close_weights_history=(
            torch.stack([row[5] for row in rows]) if record else empty_weights
        ),
        executed_buy_weights=(
            torch.stack([row[6] for row in rows]) if record else empty_phase
        ),
        executed_sell_weights=(
            torch.stack([row[7] for row in rows]) if record else empty_phase
        ),
        executed_long_buy_weights=(
            torch.stack([row[8] for row in rows]) if record else empty_phase
        ),
        executed_long_sell_weights=(
            torch.stack([row[9] for row in rows]) if record else empty_phase
        ),
        executed_short_open_weights=(
            torch.stack([row[10] for row in rows]) if record else empty_phase
        ),
        executed_short_cover_weights=(
            torch.stack([row[11] for row in rows]) if record else empty_phase
        ),
        due_weights_history=(
            torch.stack([row[12] for row in rows])
            if prepared.mode == "tw_overnight" and record
            else None
        ),
        settlement_default=torch.stack([row[13] for row in rows]),
        cash_history=torch.stack([row[15] for row in rows]),
        payables_history=torch.stack([row[16] for row in rows]),
        receivables_history=torch.stack([row[17] for row in rows]),
        short_sale_collateral_history=torch.stack([row[18] for row in rows]),
        short_margin_collateral_history=torch.stack([row[19] for row in rows]),
        equity_scale_history=torch.stack([row[21] for row in rows]),
        final_weights=final[14],
        final_cash=final[15],
        final_payables=final[16],
        final_receivables=final[17],
        final_short_sale_collateral=final[18],
        final_short_margin_collateral=final[19],
        final_alive=final[20],
        final_equity_scale=final[21],
        final_due_weights=(final[14] if prepared.mode == "tw_overnight" else None),
    )


def _concat_results(
    results: list[TaiwanDualSessionResult],
) -> TaiwanDualSessionResult:
    final = results[-1]
    mode = final.mode
    record = all(int(result.weights_history.size(0)) > 0 for result in results)
    symbols = int(final.final_weights.numel())
    empty_weights = final.final_weights.new_empty((0, symbols))
    empty_phase = final.final_weights.new_empty((0, 2, symbols))

    def cat(name: str) -> torch.Tensor:
        return torch.cat(
            [getattr(result, name) for result in results],
            dim=0,
        )

    return TaiwanDualSessionResult(
        mode=mode,
        strategy_returns=cat("strategy_returns"),
        turnovers=cat("turnovers"),
        event_turnovers=cat("event_turnovers"),
        weights_history=cat("weights_history") if record else empty_weights,
        open_weights_history=(cat("open_weights_history") if record else empty_weights),
        close_weights_history=(
            cat("close_weights_history") if record else empty_weights
        ),
        executed_buy_weights=(cat("executed_buy_weights") if record else empty_phase),
        executed_sell_weights=(cat("executed_sell_weights") if record else empty_phase),
        executed_long_buy_weights=(
            cat("executed_long_buy_weights") if record else empty_phase
        ),
        executed_long_sell_weights=(
            cat("executed_long_sell_weights") if record else empty_phase
        ),
        executed_short_open_weights=(
            cat("executed_short_open_weights") if record else empty_phase
        ),
        executed_short_cover_weights=(
            cat("executed_short_cover_weights") if record else empty_phase
        ),
        cash_history=cat("cash_history"),
        payables_history=cat("payables_history"),
        receivables_history=cat("receivables_history"),
        settlement_default=cat("settlement_default"),
        equity_scale_history=cat("equity_scale_history"),
        short_sale_collateral_history=cat("short_sale_collateral_history"),
        short_margin_collateral_history=cat("short_margin_collateral_history"),
        final_weights=final.final_weights,
        final_cash=final.final_cash,
        final_payables=final.final_payables,
        final_receivables=final.final_receivables,
        final_alive=final.final_alive,
        final_equity_scale=final.final_equity_scale,
        final_short_sale_collateral=final.final_short_sale_collateral,
        final_short_margin_collateral=final.final_short_margin_collateral,
        due_weights_history=(
            torch.cat(
                [
                    result.due_weights_history
                    for result in results
                    if result.due_weights_history is not None
                ],
                dim=0,
            )
            if mode == "tw_overnight" and record
            else None
        ),
        final_due_weights=(final.final_due_weights if mode == "tw_overnight" else None),
    )


def _eager_tail(
    prepared: _PreparedInputs,
    *,
    start: int,
    state: tuple[torch.Tensor, ...],
) -> TaiwanDualSessionResult:
    sl = slice(start, int(prepared.actions.size(0)))
    return _run_dual_session(
        mode=prepared.mode,
        actions=prepared.actions[sl],
        overnight_log_returns=prepared.overnight_log_returns[sl],
        intraday_log_returns=prepared.intraday_log_returns[sl],
        tradable_mask=prepared.tradable_mask[sl],
        can_buy_mask=prepared.can_buy_mask[sl],
        can_sell_mask=prepared.can_sell_mask[sl],
        buy_fee_rates=prepared.buy_fee_rates[sl],
        sell_fee_rates=prepared.sell_fee_rates[sl],
        can_short_open_mask=(
            prepared.can_short_open_mask[sl] if prepared.has_short_open_mask else None
        ),
        force_short_cover_mask=(
            prepared.force_short_cover_mask[sl]
            if prepared.has_force_short_cover_mask
            else None
        ),
        short_margin_rate=(
            prepared.short_margin_rate[sl] if prepared.has_short_contract else None
        ),
        short_capacity_weights=(
            prepared.short_capacity_weights[sl] if prepared.has_short_capacity else None
        ),
        short_handling_fee_rate=(
            prepared.short_handling_fee_rate[sl] if prepared.has_short_contract else 0.0
        ),
        short_maintenance_ratio=prepared.short_maintenance_ratio,
        unresolved_corporate_action_mask=(
            prepared.unresolved_corporate_action_mask[sl]
        ),
        cash_dividend_yield=(
            prepared.cash_dividend_yield[sl] if prepared.has_cash_dividends else None
        ),
        cash_dividend_payment_delay_sessions=(
            prepared.cash_dividend_payment_delay_sessions[sl]
            if prepared.has_cash_dividends
            else None
        ),
        claim_queue_sessions=prepared.claim_queue_sessions,
        force_exit_mask=prepared.force_exit_mask[sl],
        settlement_lag_sessions=prepared.settlement_lag_sessions,
        gross_budget=prepared.gross_budget,
        max_turnover_ratio=prepared.max_turnover_ratio,
        volume_limit_weights=(
            prepared.volume_limit_weights[sl] if prepared.has_volume_limit else None
        ),
        state_advance_mask=prepared.state_advance_mask[sl],
        return_weights_history=prepared.return_weights_history,
        initial_weights=state[0],
        initial_cash=state[1],
        initial_payables=state[2],
        initial_receivables=state[3],
        initial_short_sale_collateral=state[4],
        initial_short_margin_collateral=state[5],
        initial_alive=state[6],
        initial_equity_scale=state[7],
        detach_initial_state=False,
    )


def _run_prepared_compiled(
    prepared: _PreparedInputs,
    *,
    strict_compile: bool,
) -> TaiwanDualSessionResult:
    total_rows = int(prepared.actions.size(0))
    full_stop = total_rows - total_rows % COMPILED_BLOCK_ROWS
    if (
        prepared.actions.device.type != "cuda"
        or full_stop == 0
        or not hasattr(torch, "compile")
        or torch.compiler.is_compiling()
    ):
        return _eager_tail(
            prepared,
            start=0,
            state=_state_tuple(prepared),
        )

    try:
        key, compiled_day = _compiled_day_runner(prepared)
    except Exception as exc:
        if strict_compile:
            raise RuntimeError(
                "failed to construct compiled dual-session day runner"
            ) from exc
        _COMPILE_STATS["eager_fallback_calls"] += 1
        return _eager_tail(
            prepared,
            start=0,
            state=_state_tuple(prepared),
        )
    if key in _FAILED_COMPILE_KEYS:
        if strict_compile:
            raise RuntimeError("compiled dual-session contract previously failed")
        _COMPILE_STATS["eager_fallback_calls"] += 1
        return _eager_tail(
            prepared,
            start=0,
            state=_state_tuple(prepared),
        )

    state = _state_tuple(prepared)
    rows: list[tuple[torch.Tensor, ...]] = []
    try:
        for block_start in range(0, full_stop, COMPILED_BLOCK_ROWS):
            for index in range(
                block_start,
                block_start + COMPILED_BLOCK_ROWS,
            ):
                values = compiled_day(
                    *_day_inputs(prepared, index),
                    *state,
                )
                rows.append(values)
                state = _next_state(values)
                _COMPILE_STATS["compiled_day_calls"] += 1
            _COMPILE_STATS["compiled_block_calls"] += 1
    except Exception as exc:
        _FAILED_COMPILE_KEYS.add(key)
        if strict_compile:
            raise RuntimeError("compiled dual-session day runner failed") from exc
        _COMPILE_STATS["eager_fallback_calls"] += 1
        return _eager_tail(
            prepared,
            start=0,
            state=_state_tuple(prepared),
        )

    results = [_prefix_result(prepared=prepared, rows=rows)]
    if full_stop < total_rows:
        _COMPILE_STATS["eager_tail_calls"] += 1
        results.append(
            _eager_tail(
                prepared,
                start=full_stop,
                state=state,
            )
        )
    return _concat_results(results) if len(results) > 1 else results[0]


def _run_compiled_public(
    *,
    mode: str,
    actions: torch.Tensor,
    overnight_log_returns: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor | float,
    sell_fee_rates: torch.Tensor | float,
    can_short_open_mask: torch.Tensor | None,
    force_short_cover_mask: torch.Tensor | None,
    short_margin_rate: torch.Tensor | float | None,
    short_capacity_weights: torch.Tensor | float | None,
    short_handling_fee_rate: torch.Tensor | float,
    short_maintenance_ratio: float,
    unresolved_corporate_action_mask: torch.Tensor | None,
    cash_dividend_yield: torch.Tensor | None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None,
    claim_queue_sessions: int | None,
    force_exit_mask: torch.Tensor | None,
    settlement_lag_sessions: int,
    gross_budget: float,
    max_turnover_ratio: float,
    volume_limit_weights: torch.Tensor | float | None,
    state_advance_mask: torch.Tensor | None,
    return_weights_history: bool,
    initial_weights: torch.Tensor | None,
    initial_cash: torch.Tensor | None,
    initial_payables: torch.Tensor | None,
    initial_receivables: torch.Tensor | None,
    initial_alive: torch.Tensor | None,
    initial_equity_scale: torch.Tensor | None,
    initial_short_sale_collateral: torch.Tensor | None,
    initial_short_margin_collateral: torch.Tensor | None,
    strict_compile: bool,
) -> TaiwanDualSessionResult:
    prepared = _prepare_inputs(
        mode=mode,
        actions=actions,
        overnight_log_returns=overnight_log_returns,
        intraday_log_returns=intraday_log_returns,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        short_margin_rate=short_margin_rate,
        short_capacity_weights=short_capacity_weights,
        short_handling_fee_rate=short_handling_fee_rate,
        short_maintenance_ratio=short_maintenance_ratio,
        unresolved_corporate_action_mask=(unresolved_corporate_action_mask),
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(cash_dividend_payment_delay_sessions),
        claim_queue_sessions=claim_queue_sessions,
        force_exit_mask=force_exit_mask,
        settlement_lag_sessions=settlement_lag_sessions,
        gross_budget=gross_budget,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        state_advance_mask=state_advance_mask,
        return_weights_history=return_weights_history,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_alive=initial_alive,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
    )
    return _run_prepared_compiled(
        prepared,
        strict_compile=strict_compile,
    )


def run_tw_cash_dual_session_compiled(
    actions: torch.Tensor,
    overnight_log_returns: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor | float,
    sell_fee_rates: torch.Tensor | float,
    *,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    short_margin_rate: torch.Tensor | float | None = None,
    short_capacity_weights: torch.Tensor | float | None = None,
    short_handling_fee_rate: torch.Tensor | float = 0.0,
    short_maintenance_ratio: float = 1.30,
    unresolved_corporate_action_mask: torch.Tensor | None = None,
    cash_dividend_yield: torch.Tensor | None = None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None = None,
    claim_queue_sessions: int | None = None,
    force_exit_mask: torch.Tensor | None = None,
    settlement_lag_sessions: int = 2,
    gross_budget: float = 1.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | float | None = None,
    state_advance_mask: torch.Tensor | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_short_sale_collateral: torch.Tensor | None = None,
    initial_short_margin_collateral: torch.Tensor | None = None,
    strict_compile: bool = False,
) -> TaiwanDualSessionResult:
    return _run_compiled_public(
        mode="tw_cash",
        actions=actions,
        overnight_log_returns=overnight_log_returns,
        intraday_log_returns=intraday_log_returns,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        short_margin_rate=short_margin_rate,
        short_capacity_weights=short_capacity_weights,
        short_handling_fee_rate=short_handling_fee_rate,
        short_maintenance_ratio=short_maintenance_ratio,
        unresolved_corporate_action_mask=(unresolved_corporate_action_mask),
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(cash_dividend_payment_delay_sessions),
        claim_queue_sessions=claim_queue_sessions,
        force_exit_mask=force_exit_mask,
        settlement_lag_sessions=settlement_lag_sessions,
        gross_budget=gross_budget,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        state_advance_mask=state_advance_mask,
        return_weights_history=return_weights_history,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_alive=initial_alive,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        strict_compile=strict_compile,
    )


def run_tw_overnight_dual_session_compiled(
    actions: torch.Tensor,
    overnight_log_returns: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor | float,
    sell_fee_rates: torch.Tensor | float,
    *,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    short_margin_rate: torch.Tensor | float | None = None,
    short_capacity_weights: torch.Tensor | float | None = None,
    short_handling_fee_rate: torch.Tensor | float = 0.0,
    short_maintenance_ratio: float = 1.30,
    unresolved_corporate_action_mask: torch.Tensor | None = None,
    cash_dividend_yield: torch.Tensor | None = None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None = None,
    claim_queue_sessions: int | None = None,
    force_exit_mask: torch.Tensor | None = None,
    settlement_lag_sessions: int = 2,
    gross_budget: float = 1.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | float | None = None,
    state_advance_mask: torch.Tensor | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_short_sale_collateral: torch.Tensor | None = None,
    initial_short_margin_collateral: torch.Tensor | None = None,
    strict_compile: bool = False,
) -> TaiwanDualSessionResult:
    return _run_compiled_public(
        mode="tw_overnight",
        actions=actions,
        overnight_log_returns=overnight_log_returns,
        intraday_log_returns=intraday_log_returns,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        short_margin_rate=short_margin_rate,
        short_capacity_weights=short_capacity_weights,
        short_handling_fee_rate=short_handling_fee_rate,
        short_maintenance_ratio=short_maintenance_ratio,
        unresolved_corporate_action_mask=(unresolved_corporate_action_mask),
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(cash_dividend_payment_delay_sessions),
        claim_queue_sessions=claim_queue_sessions,
        force_exit_mask=force_exit_mask,
        settlement_lag_sessions=settlement_lag_sessions,
        gross_budget=gross_budget,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        state_advance_mask=state_advance_mask,
        return_weights_history=return_weights_history,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_alive=initial_alive,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        strict_compile=strict_compile,
    )


__all__ = [
    "COMPILED_BLOCK_ROWS",
    "clear_tw_dual_session_compile_cache",
    "get_tw_dual_session_compile_stats",
    "run_tw_cash_dual_session_compiled",
    "run_tw_overnight_dual_session_compiled",
]
