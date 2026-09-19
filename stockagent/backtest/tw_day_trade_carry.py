"""Session composition of the physical FIFO ledger, not a second trainer.

The caller must resolve receipt-verified execution facts outside model inputs.
This module owns chronological session state and daily/minute return accounting;
the existing inventory kernels own fills, costs, corporate claims and FIFO.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, replace
from datetime import date
import math
import os
import threading
import types
from typing import Callable
import warnings

import torch
from torch import Tensor

from stockagent.backtest.tw_day_trade_inventory import (
    CohortField, DayTradeInventoryState, InventoryPathReduction,
    InventoryReduction, _require, accrue_inventory_interest,
    apply_inventory_action, convert_inventory_to_margin, inventory_nav,
    inventory_intraday_nav_lower_bound_from_extrema, inventory_path_nav,
    rebalance_inventory_at_open, reduce_inventory_fifo_sparse_liquidity,
    reduce_inventory_fifo_path, release_inventory_stock_deliveries,
    settle_inventory_claims,
    validate_inventory_state,
)

CARRY_SESSION_ABI = "tw_day_trade_physical_fifo_sessions_v5_pending_stock"


class DayTradeCarryEventCompressionFallback(RuntimeError):
    """The sufficient-statistic proof was inconclusive; replay dense minutes."""

_COMPILED_PATHS: dict[tuple[object, ...], Callable[..., tuple[Tensor, ...]]] = {}
_COMPILED_SESSIONS: dict[tuple[object, ...], Callable[..., tuple[Tensor, ...]]] = {}
_COMPILED_PATH_LOCK = threading.Lock()
_CARRY_COMPILE_STATS = {
    "compile_constructors": 0,
    "session_compile_constructors": 0,
    "compiled_path_calls": 0,
    "compiled_session_calls": 0,
    "eager_path_calls": 0,
    "compile_failures": 0,
    "eager_fallback_calls": 0,
    "padded_session_calls": 0,
    "compacted_cohort_rows": 0,
    "compacted_claim_rows": 0,
    "event_compression_fallback_batches": 0,
    "sparse_event_session_calls": 0,
    "sparse_event_cells": 0,
    "dense_event_cells_avoided": 0,
}


def _compile_isolated_code_object(
    function: Callable[..., tuple[Tensor, ...]], *, name: str
) -> Callable[..., tuple[Tensor, ...]]:
    """Give each static exact-ledger ABI its own Dynamo guard cache.

    Dynamo indexes guards by Python code object even when callers deliberately
    create separate ``torch.compile`` functions.  The exact ledger has a small,
    bounded set of power-of-two cohort ABIs plus train/eval grad modes; sharing
    one nested-function code object therefore hits Dynamo's unrelated global
    eight-guard limit.  Cloning only code identity leaves bytecode, closure,
    tensor algebra and autograd unchanged while keeping one guard per ABI.
    """
    code = function.__code__.replace(co_name=name, co_qualname=name)
    isolated = types.FunctionType(
        code,
        function.__globals__,
        name=name,
        argdefs=function.__defaults__,
        closure=function.__closure__,
    )
    isolated.__kwdefaults__ = function.__kwdefaults__
    isolated.__annotations__ = function.__annotations__
    return isolated


def _env_truthy(name: str, default: str = "1") -> bool:
    return str(os.environ.get(name, default)).strip().lower() not in {
        "0", "false", "off", "no",
    }


def get_day_trade_carry_compile_stats() -> dict[str, int]:
    return {key: int(value) for key, value in _CARRY_COMPILE_STATS.items()}


def reset_day_trade_carry_compile_stats(*, clear_cache: bool = False) -> None:
    """Reset telemetry, optionally dropping Python compiled-callable caches.

    Training uses the default so a metrics reset never pays another compile.
    Tests and cold-start benchmarks can request ``clear_cache=True`` when they
    must also observe exactly one constructor independent of test order.
    """
    with _COMPILED_PATH_LOCK:
        for key in _CARRY_COMPILE_STATS:
            _CARRY_COMPILE_STATS[key] = 0
        if clear_cache:
            _COMPILED_PATHS.clear()
            _COMPILED_SESSIONS.clear()


def invalidate_day_trade_carry_compiled_caches() -> tuple[int, int]:
    """Drop only in-process compiled callables after an oracle mismatch.

    A failed compiled trajectory is never accepted. The caller first proves
    the same batch with the eager authoritative oracle, then invalidates these
    process-local wrappers before one bounded recompilation. Telemetry remains
    intact so the epoch receipt still exposes that recovery happened.
    """
    with _COMPILED_PATH_LOCK:
        counts = (len(_COMPILED_PATHS), len(_COMPILED_SESSIONS))
        _COMPILED_PATHS.clear()
        _COMPILED_SESSIONS.clear()
    return counts


def _strict_no_fallback_enabled() -> bool:
    return _env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0")


def _carry_compile_options() -> dict[str, object]:
    """Bound pathological mega-fusion while keeping the entire core compiled."""
    options: dict[str, object] = {"triton.cudagraphs": False}
    raw_fusion = os.environ.get("STOCKAGENT_DAY_TRADE_MAX_FUSION_SIZE", "8")
    try:
        max_fusion_size = int(raw_fusion)
    except ValueError as exc:
        raise ValueError(
            "STOCKAGENT_DAY_TRADE_MAX_FUSION_SIZE must be a positive integer"
        ) from exc
    if max_fusion_size <= 0:
        raise ValueError(
            "STOCKAGENT_DAY_TRADE_MAX_FUSION_SIZE must be a positive integer"
        )
    options["max_fusion_size"] = max_fusion_size
    if _env_truthy("STOCKAGENT_DAY_TRADE_DISABLE_COALESCE_TILING", "1"):
        options["triton.coalesce_tiling_analysis"] = False
    return options


def _carry_path_compile_enabled(reference: Tensor) -> bool:
    return (
        reference.device.type == "cuda"
        and hasattr(torch, "compile")
        and _env_truthy("STOCKAGENT_BACKTEST_COMPILE", "1")
        and _env_truthy("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", "1")
        and not torch.compiler.is_compiling()
    )


def _carry_full_session_compile_enabled(reference: Tensor) -> bool:
    return _carry_path_compile_enabled(reference) and _env_truthy(
        "STOCKAGENT_DAY_TRADE_FULL_SESSION_COMPILE", "1"
    )


def _compiled_inventory_path(
    state: DayTradeInventoryState,
    *,
    prices: Tensor,
    capacity_shares: Tensor,
    marks: Tensor,
    initial_capital: float,
) -> tuple[InventoryPathReduction, Tensor]:
    """Compile only the date/claim-independent FIFO tensor core.

    Corporate claims and carry cost enter minute NAV through one exact scalar.
    Keeping their variable row axis outside the signature prevents a new graph
    for every action day.  The returned delta is applied to the authoritative
    state, so its public/checkpoint representation is unchanged.
    """
    device_index = (
        state.cohorts.device.index
        if state.cohorts.device.index is not None
        else torch.cuda.current_device()
    )
    training_graph = bool(torch.is_grad_enabled() and state.cohorts.requires_grad)
    key: tuple[object, ...] = (
        int(device_index),
        str(state.cohorts.dtype),
        int(state.cohorts.shape[0]),
        int(state.cohorts.shape[1]),
        float(initial_capital),
        training_graph,
        (("triton.cudagraphs", False),),
    )
    with _COMPILED_PATH_LOCK:
        compiled = _COMPILED_PATHS.get(key)
        if compiled is None:
            symbols = int(state.cohorts.shape[1])

            def tensor_core(
                cohorts: Tensor,
                realized_net_pnl: Tensor,
                carry_cost: Tensor,
                corporate_action_net: Tensor,
                failed: Tensor,
                path_prices: Tensor,
                path_capacity: Tensor,
                path_marks: Tensor,
            ) -> tuple[Tensor, ...]:
                zero = realized_net_pnl.new_zeros(())
                effective_realized = (
                    realized_net_pnl + corporate_action_net - carry_cost
                )
                slim = DayTradeInventoryState(
                    cohorts=cohorts,
                    realized_net_pnl=effective_realized,
                    carry_cost=zero,
                    claims=cohorts.new_empty((0, symbols, 3)),
                    action_cursor=cohorts.new_zeros((symbols,)),
                    decision_day=zero,
                    observed_day=zero,
                    failed=failed,
                )
                path = reduce_inventory_fifo_path(
                    slim,
                    prices=path_prices,
                    capacity_shares=path_capacity,
                )
                minute_nav = inventory_path_nav(
                    slim,
                    prices=path_prices,
                    minute_filled_shares=path.minute_filled_shares,
                    marks=path_marks,
                    initial_capital=initial_capital,
                )
                reduction = path.reduction
                return (
                    reduction.state.cohorts,
                    reduction.state.realized_net_pnl - effective_realized,
                    reduction.filled_shares,
                    reduction.gross_pnl,
                    reduction.entry_fee_allocated,
                    reduction.exit_fee,
                    reduction.net_pnl,
                    reduction.state.failed,
                    path.minute_filled_shares,
                    minute_nav,
                )

            tensor_core = _compile_isolated_code_object(
                tensor_core,
                name=f"day_trade_inventory_path_core_{abs(hash(key))}",
            )
            compiled = torch.compile(
                tensor_core,
                fullgraph=True,
                dynamic=False,
                options={"triton.cudagraphs": False},
            )
            _COMPILED_PATHS[key] = compiled
            _CARRY_COMPILE_STATS["compile_constructors"] += 1

    try:
        # PyTorch 2.11's Dynamo input probe reads ``.grad`` on non-leaf
        # recurrent tensors. Under the repository's warnings-as-errors policy
        # that harmless probe becomes InternalTorchDynamoError before tracing.
        # Suppress only that framework warning; accounting/runtime warnings
        # and every compile exception still fail normally.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"The \.grad attribute of a Tensor that is not a leaf Tensor.*",
                category=UserWarning,
            )
            values = compiled(
                state.cohorts,
                state.realized_net_pnl,
                state.carry_cost,
                state.corporate_action_net,
                state.failed,
                prices,
                capacity_shares,
                marks,
            )
    except Exception:
        with _COMPILED_PATH_LOCK:
            _COMPILED_PATHS.pop(key, None)
        _CARRY_COMPILE_STATS["compile_failures"] += 1
        if _strict_no_fallback_enabled():
            raise
        _CARRY_COMPILE_STATS["eager_fallback_calls"] += 1
        path = reduce_inventory_fifo_path(
            state, prices=prices, capacity_shares=capacity_shares
        )
        return path, inventory_path_nav(
            state,
            prices=prices,
            minute_filled_shares=path.minute_filled_shares,
            marks=marks,
            initial_capital=initial_capital,
        )

    _CARRY_COMPILE_STATS["compiled_path_calls"] += 1
    updated = replace(
        state,
        cohorts=values[0],
        realized_net_pnl=state.realized_net_pnl + values[1],
        failed=values[7],
    )
    reduction = InventoryReduction(
        updated,
        values[2],
        values[3],
        values[4],
        values[5],
        values[6],
    )
    return InventoryPathReduction(reduction, values[8]), values[9]


def _restore_session_calendar(
    normalized: Tensor, before: Tensor, *, day: int
) -> Tensor:
    """Restore authoritative ordinals after the fixed-calendar compiled core."""
    if normalized.shape[0] != before.shape[0] + 1:
        raise RuntimeError("compiled carry session did not append exactly one cohort")
    acquired = torch.cat(
        (
            before[..., CohortField.ACQUIRED_DAY],
            before.new_full((1, before.shape[1]), day),
        ),
        dim=0,
    )
    accrued = torch.cat(
        (
            before[..., CohortField.ACCRUED_THROUGH],
            before.new_full((1, before.shape[1]), day),
        ),
        dim=0,
    )
    locked_until = torch.cat(
        (
            before[..., CohortField.LOCKED_UNTIL_DAY],
            before.new_zeros((1, before.shape[1])),
        ),
        dim=0,
    )
    return torch.stack(
        [
            acquired if index == CohortField.ACQUIRED_DAY
            else accrued if index == CohortField.ACCRUED_THROUGH
            else locked_until if index == CohortField.LOCKED_UNTIL_DAY
            else normalized[..., index]
            for index in range(len(CohortField))
        ],
        dim=-1,
    )


def _compiled_session_execution(
    funded: DayTradeInventoryState,
    session: DayTradeCarrySession,
    *,
    weights: Tensor,
    can_enter: Tensor,
    buy_fee_rate: Tensor,
    day_sell_fee_rate: Tensor,
    normal_sell_fee_rate: Tensor,
    rebate_rate: Tensor,
    initial_capital: float,
    event_compression: bool,
) -> tuple[DayTradeInventoryState, DayTradeInventoryState, Tensor, Tensor]:
    """Compile the complete opening/FIFO/mark/conversion tensor trajectory.

    Calendar and variable claim rows are authoritative state, but they are not
    degrees of freedom of the same-session numerical kernel.  Normalize only
    those two representation axes, compile one fixed power-of-two cohort shape,
    then restore the original claims and exact Gregorian ordinals.  The claim
    compression preserves both quantities used by the kernel exactly:
    ``corporate_action_net`` and unpaid positive receivables.
    """
    device_index = (
        funded.cohorts.device.index
        if funded.cohorts.device.index is not None
        else torch.cuda.current_device()
    )
    training_graph = bool(
        torch.is_grad_enabled()
        and (funded.cohorts.requires_grad or weights.requires_grad)
    )
    sparse_events = session.uses_sparse_events
    key: tuple[object, ...] = (
        int(device_index),
        str(funded.cohorts.dtype),
        int(funded.cohorts.shape[0]),
        int(funded.cohorts.shape[1]),
        float(initial_capital),
        training_graph,
        session.daily_proxy_mask is not None,
        session.terminal_liquidation_price is not None,
        bool(event_compression),
        bool(sparse_events),
        int(
            session.exit_prices.shape[-1]
            if sparse_events
            else session.exit_prices.shape[1]
        ),
        tuple(sorted(_carry_compile_options().items())),
    )
    with _COMPILED_PATH_LOCK:
        compiled = _COMPILED_SESSIONS.get(key)
        if compiled is None:
            symbols = int(funded.cohorts.shape[1])
            normalized_day = 2

            def tensor_core(
                cohorts: Tensor,
                realized_net_pnl: Tensor,
                carry_cost: Tensor,
                corporate_action_net: Tensor,
                corporate_action_receivable: Tensor,
                failed: Tensor,
                target_weights: Tensor,
                official_open: Tensor,
                opening_marks: Tensor,
                entry_price: Tensor,
                entry_volume: Tensor,
                lower_limit: Tensor,
                upper_limit: Tensor,
                halted: Tensor,
                exit_prices: Tensor,
                exit_capacity: Tensor,
                marks: Tensor,
                terminal_liquidation_price: Tensor,
                exit_symbol_indices: Tensor,
                exit_sides: Tensor,
                symbol_event_starts: Tensor,
                symbol_event_ends: Tensor,
                minimum_marks: Tensor,
                maximum_marks: Tensor,
                mark_path_valid: Tensor,
                minimum_exit_prices: Tensor,
                maximum_exit_prices: Tensor,
                eligible: Tensor,
                buy_rate: Tensor,
                day_sell_rate: Tensor,
                normal_sell_rate: Tensor,
                rebate: Tensor,
                daily_proxy: Tensor,
            ) -> tuple[Tensor, ...]:
                active = cohorts[..., CohortField.SHARES] != 0
                one = cohorts.new_ones(cohorts.shape[:2])
                zero_dates = cohorts.new_zeros(cohorts.shape[:2])
                normalized_dates = torch.where(active, one, zero_dates)
                normalized_delivery = torch.where(
                    cohorts[..., CohortField.LOCKED_SHARES] != 0,
                    cohorts.new_full(cohorts.shape[:2], normalized_day + 1),
                    zero_dates,
                )
                normalized_cohorts = torch.stack(
                    [
                        normalized_dates
                        if index in {
                            CohortField.ACQUIRED_DAY,
                            CohortField.ACCRUED_THROUGH,
                        }
                        else normalized_delivery
                        if index == CohortField.LOCKED_UNTIL_DAY
                        else cohorts[..., index]
                        for index in range(len(CohortField))
                    ],
                    dim=-1,
                )
                # One fixed claim row is sufficient after folding all paid and
                # payable net cash into realized PnL.  The remaining row owns
                # exactly the aggregate unpaid positive receivable used by the
                # opening funding constraint.
                claim_amount = torch.cat(
                    (
                        corporate_action_receivable.reshape(1),
                        cohorts.new_zeros((symbols - 1,)),
                    )
                )
                claim = torch.stack(
                    (
                        claim_amount,
                        torch.where(
                            claim_amount != 0,
                            claim_amount.new_full(claim_amount.shape, normalized_day + 1),
                            torch.zeros_like(claim_amount),
                        ),
                        torch.zeros_like(claim_amount),
                    ),
                    dim=-1,
                ).unsqueeze(0)
                slim_realized = (
                    realized_net_pnl
                    + corporate_action_net
                    - corporate_action_receivable
                )
                slim = DayTradeInventoryState(
                    cohorts=normalized_cohorts,
                    realized_net_pnl=slim_realized,
                    carry_cost=carry_cost,
                    claims=claim,
                    action_cursor=cohorts.new_ones((symbols,)),
                    decision_day=cohorts.new_ones(()),
                    observed_day=cohorts.new_full((), normalized_day),
                    failed=failed,
                )
                opening = rebalance_inventory_at_open(
                    slim,
                    weights=target_weights,
                    official_open=official_open,
                    opening_marks=opening_marks,
                    entry_price=entry_price,
                    entry_volume_shares=entry_volume,
                    lower_limit=lower_limit,
                    upper_limit=upper_limit,
                    can_enter=eligible,
                    buy_fee_rate=buy_rate,
                    day_sell_fee_rate=day_sell_rate,
                    normal_sell_fee_rate=normal_sell_rate,
                    rebate_rate=rebate,
                    initial_capital=initial_capital,
                    day=normalized_day,
                    halted=halted,
                    daily_proxy_mask=daily_proxy,
                    state_already_advanced=True,
                )
                short = (opening.state.shares < 0)[:, None]
                if sparse_events:
                    prices = exit_prices
                    capacity = exit_capacity
                else:
                    prices = torch.where(
                        short, exit_prices[..., 1], exit_prices[..., 0]
                    )
                    capacity = torch.where(
                        short, exit_capacity[..., 1], exit_capacity[..., 0]
                    )
                if event_compression:
                    if sparse_events:
                        # Explicit opt-in research path.  Its endpoint integral
                        # changes FP64 summation order and is therefore not the
                        # default loss implementation.
                        liquidity = reduce_inventory_fifo_sparse_liquidity(
                            opening.state,
                            prices=prices,
                            capacity_shares=capacity,
                            event_symbol_indices=exit_symbol_indices,
                            event_sides=exit_sides,
                            symbol_event_starts=symbol_event_starts,
                            symbol_event_ends=symbol_event_ends,
                        )
                        selected_minimum_exit = torch.where(
                            short.squeeze(-1),
                            minimum_exit_prices[:, 1],
                            minimum_exit_prices[:, 0],
                        )
                        selected_maximum_exit = torch.where(
                            short.squeeze(-1),
                            maximum_exit_prices[:, 1],
                            maximum_exit_prices[:, 0],
                        )
                        lower_bound = inventory_intraday_nav_lower_bound_from_extrema(
                            opening.state,
                            minimum_marks=minimum_marks,
                            maximum_marks=maximum_marks,
                            mark_path_valid=mark_path_valid,
                            minimum_prices=selected_minimum_exit,
                            maximum_prices=selected_maximum_exit,
                            initial_capital=initial_capital,
                        )
                        lower_bound = torch.where(
                            liquidity.reduction.state.failed == 0,
                            lower_bound,
                            torch.full_like(lower_bound, float("nan")),
                        )
                        path_state = liquidity.reduction.state
                        # Two slots preserve the public endpoint convention
                        # after execute_carry_session replaces the last slot.
                        minute_nav = lower_bound.expand(2)
                        exit_notional = liquidity.executed_notional.sum()
                    else:
                        # Authoritative fast path: run the identical first
                        # minute FIFO replay and exact 270-point NAV once.  The
                        # batch-level certificate below proves no insolvency;
                        # only an inconclusive batch pays for the masked replay.
                        path = reduce_inventory_fifo_path(
                            opening.state,
                            prices=prices,
                            capacity_shares=capacity,
                        )
                        minute_nav = inventory_path_nav(
                            opening.state,
                            prices=prices,
                            minute_filled_shares=path.minute_filled_shares,
                            marks=marks,
                            initial_capital=initial_capital,
                        )
                        # The dense minute recurrence above is unchanged.  Its
                        # training/eval caller needs only the exact minimum and
                        # close to certify that the omitted insolvency replay is
                        # an identity.  Compact before crossing the compiled
                        # ABI so 270 differentiable scalars are not retained per
                        # session.  Preserve any non-finite cell explicitly:
                        # amin alone would hide a positive infinity.
                        # execute_carry_session replaces the final raw minute
                        # mark with the post-conversion closing NAV.  Retain
                        # the exact minimum of only the preceding 269 values,
                        # plus the raw final value needed by the validity/alive
                        # predicate; the caller then performs that same close
                        # replacement before its final two-point compaction.
                        minute_minimum = minute_nav[:-1].amin()
                        minute_minimum = torch.where(
                            torch.isfinite(minute_nav).all(),
                            minute_minimum,
                            torch.full_like(minute_minimum, float("nan")),
                        )
                        minute_nav = torch.stack(
                            (minute_minimum, minute_nav[-1])
                        )
                        path_state = path.reduction.state
                        exit_notional = (
                            path.minute_filled_shares
                            * torch.nan_to_num(prices, nan=0)
                        ).sum()
                else:
                    if sparse_events:
                        raise RuntimeError(
                            "sparse carry source cannot produce a formal minute history"
                        )
                    first = reduce_inventory_fifo_path(
                        opening.state, prices=prices, capacity_shares=capacity
                    )
                    first_marks = inventory_path_nav(
                        opening.state,
                        prices=prices,
                        minute_filled_shares=first.minute_filled_shares,
                        marks=marks,
                        initial_capital=initial_capital,
                    )
                    # Fills at the first insolvent mark happened; no later fill
                    # is permitted.  The complete artifact path intentionally
                    # retains this second authoritative replay.
                    before_alive = torch.cat(
                        (
                            torch.ones_like(first_marks[:1], dtype=torch.bool),
                            (first_marks[:-1] > 0).cumprod(0).bool(),
                        )
                    )
                    path = reduce_inventory_fifo_path(
                        opening.state,
                        prices=prices,
                        capacity_shares=capacity * before_alive,
                    )
                    minute_nav = inventory_path_nav(
                        opening.state,
                        prices=prices,
                        minute_filled_shares=path.minute_filled_shares,
                        marks=marks,
                        initial_capital=initial_capital,
                    )
                    path_state = path.reduction.state
                    exit_notional = (
                        path.minute_filled_shares
                        * torch.nan_to_num(prices, nan=0)
                    ).sum()
                if session.terminal_liquidation_price is not None:
                    path_state, terminal_notional = (
                        _liquidate_terminal_inventory_without_capacity(
                            path_state,
                            terminal_price=terminal_liquidation_price,
                        )
                    )
                    exit_notional = exit_notional + terminal_notional
                converted = convert_inventory_to_margin(
                    path_state, day=normalized_day
                )
                notional = (
                    (
                        opening.reduction.filled_shares
                        + opening.addition_shares.abs()
                    )
                    * torch.nan_to_num(entry_price, nan=0)
                ).sum()
                notional = notional + exit_notional
                return (
                    path_state.cohorts,
                    path_state.realized_net_pnl - slim_realized,
                    path_state.failed,
                    converted.cohorts,
                    converted.realized_net_pnl - slim_realized,
                    converted.carry_cost - carry_cost,
                    converted.failed,
                    minute_nav,
                    notional,
                )

            tensor_core = _compile_isolated_code_object(
                tensor_core,
                name=f"day_trade_carry_session_core_{abs(hash(key))}",
            )
            compiled = torch.compile(
                tensor_core,
                fullgraph=True,
                dynamic=False,
                options=_carry_compile_options(),
            )
            _COMPILED_SESSIONS[key] = compiled
            _CARRY_COMPILE_STATS["session_compile_constructors"] += 1

    proxy = (
        torch.zeros_like(session.official_open)
        if session.daily_proxy_mask is None
        else session.daily_proxy_mask
    )
    empty_index = torch.empty(
        (0,), device=session.official_open.device, dtype=torch.int64
    )
    empty_vector = session.official_open.new_empty((0,))
    sparse_values = (
        session.exit_symbol_indices if sparse_events else empty_index,
        session.exit_sides if sparse_events else empty_index,
        session.symbol_event_starts if sparse_events else empty_index,
        session.symbol_event_ends if sparse_events else empty_index,
        session.minimum_marks if sparse_events else empty_vector,
        session.maximum_marks if sparse_events else empty_vector,
        session.mark_path_valid if sparse_events else empty_vector,
        session.minimum_exit_prices if sparse_events else empty_vector,
        session.maximum_exit_prices if sparse_events else empty_vector,
    )
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"The \.grad attribute of a Tensor that is not a leaf Tensor.*",
                category=UserWarning,
            )
            values = compiled(
                funded.cohorts,
                funded.realized_net_pnl,
                funded.carry_cost,
                funded.corporate_action_net,
                funded.corporate_action_receivable,
                funded.failed,
                weights,
                session.official_open,
                session.opening_marks,
                session.entry_price,
                session.entry_volume,
                session.lower_limit,
                session.upper_limit,
                session.halted,
                session.exit_prices,
                session.exit_capacity,
                session.marks,
                (
                    torch.zeros_like(session.official_open)
                    if session.terminal_liquidation_price is None
                    else session.terminal_liquidation_price
                ),
                *sparse_values,
                can_enter,
                buy_fee_rate,
                day_sell_fee_rate,
                normal_sell_fee_rate,
                rebate_rate,
                proxy,
            )
    except Exception:
        with _COMPILED_PATH_LOCK:
            _COMPILED_SESSIONS.pop(key, None)
        _CARRY_COMPILE_STATS["compile_failures"] += 1
        raise

    _CARRY_COMPILE_STATS["compiled_session_calls"] += 1
    common = {
        "claims": funded.claims,
        "action_cursor": funded.action_cursor,
        "decision_day": funded.decision_day.new_tensor(session.day),
        "observed_day": funded.observed_day.new_tensor(session.day),
    }
    path_state = DayTradeInventoryState(
        cohorts=_restore_session_calendar(values[0], funded.cohorts, day=session.day),
        realized_net_pnl=funded.realized_net_pnl + values[1],
        carry_cost=funded.carry_cost,
        failed=values[2],
        **common,
    )
    converted = DayTradeInventoryState(
        cohorts=_restore_session_calendar(values[3], funded.cohorts, day=session.day),
        realized_net_pnl=funded.realized_net_pnl + values[4],
        carry_cost=funded.carry_cost + values[5],
        failed=values[6],
        **common,
    )
    return path_state, converted, values[7], values[8]


def _run_inventory_path(
    state: DayTradeInventoryState,
    *,
    prices: Tensor,
    capacity_shares: Tensor,
    marks: Tensor,
    initial_capital: float,
) -> tuple[InventoryPathReduction, Tensor]:
    if _carry_path_compile_enabled(state.cohorts):
        return _compiled_inventory_path(
            state,
            prices=prices,
            capacity_shares=capacity_shares,
            marks=marks,
            initial_capital=initial_capital,
        )
    # Telemetry is an eager orchestration concern.  Mutating a Python counter
    # inside an enclosing ``torch.compile(fullgraph=True)`` region turns its
    # value into a guard and forces one recompilation per FIFO call.
    if not torch.compiler.is_compiling():
        _CARRY_COMPILE_STATS["eager_path_calls"] += 1
    path = reduce_inventory_fifo_path(
        state, prices=prices, capacity_shares=capacity_shares
    )
    return path, inventory_path_nav(
        state,
        prices=prices,
        minute_filled_shares=path.minute_filled_shares,
        marks=marks,
        initial_capital=initial_capital,
    )


def _liquidate_terminal_inventory_without_capacity(
    state: DayTradeInventoryState,
    *,
    terminal_price: Tensor,
) -> tuple[DayTradeInventoryState, Tensor]:
    """Close every remaining deliverable share at one terminal price.

    The capacity supplied to the FIFO reducer is exactly the account's own
    remaining deliverable quantity.  It is therefore not a market-volume
    estimate and cannot constrain the liquidation.  Locked corporate-action
    shares remain unavailable until their receipt-backed delivery day.  A
    required terminal close with no finite positive price fails the complete
    session atomically instead of fabricating a fill.
    """
    price = terminal_price.to(device=state.cohorts.device, dtype=torch.float64)
    if price.shape != (state.cohorts.shape[1],):
        raise ValueError("terminal liquidation price differs from the universe")
    absolute = state.cohorts[..., CohortField.SHARES].abs()
    locked = state.cohorts[..., CohortField.LOCKED_SHARES].abs()
    deliverable = (absolute - locked).clamp_min(0).sum(dim=0)
    price_ok = torch.isfinite(price) & (price > 0)
    valid = _require(
        (deliverable == 0) | price_ok,
        "terminal liquidation requires a finite positive official close",
    )
    reduction = reduce_inventory_fifo_path(
        state,
        prices=price[:, None],
        capacity_shares=deliverable[:, None],
    )
    liquidated = replace(
        reduction.reduction.state,
        failed=torch.maximum(
            reduction.reduction.state.failed,
            (~valid).to(dtype=state.failed.dtype),
        ),
    )
    filled = reduction.minute_filled_shares[:, 0]
    notional = (
        filled * torch.where(price_ok, price, torch.zeros_like(price))
    ).sum()
    return liquidated, torch.where(valid, notional, torch.zeros_like(notional))


def _compact_detached_carry_state(state: DayTradeCarryState) -> DayTradeCarryState:
    """Drop inert dense rows only at a truncated-BPTT boundary."""
    inventory = state.inventory
    if any(getattr(inventory, name).requires_grad for name in inventory.__dataclass_fields__):
        return state
    cohorts = inventory.cohorts
    old_cohorts = int(cohorts.shape[0])
    if old_cohorts:
        active = cohorts[..., CohortField.SHARES] != 0
        active_counts = active.sum(dim=0)
        retained = int(active_counts.max().detach())
        if retained < old_cohorts:
            order = torch.argsort((~active).to(torch.int8), dim=0, stable=True)
            packed = cohorts.gather(
                0, order.unsqueeze(-1).expand(-1, -1, cohorts.shape[-1])
            )[:retained]
            if retained:
                packed_active = packed[..., CohortField.SHARES] != 0
                packed = torch.where(
                    packed_active.unsqueeze(-1), packed, torch.zeros_like(packed)
                )
                observed = inventory.observed_day.expand_as(
                    packed[..., CohortField.ACQUIRED_DAY]
                )
                packed[..., CohortField.ACQUIRED_DAY] = torch.where(
                    packed_active,
                    packed[..., CohortField.ACQUIRED_DAY],
                    observed,
                )
                packed[..., CohortField.ACCRUED_THROUGH] = torch.where(
                    packed_active,
                    packed[..., CohortField.ACCRUED_THROUGH],
                    observed,
                )
            cohorts = packed
            _CARRY_COMPILE_STATS["compacted_cohort_rows"] += old_cohorts - retained

    claims = inventory.claims
    old_claims = int(claims.shape[0])
    if old_claims:
        keep = (claims[..., 0] != 0).any(dim=1)
        claims = claims[keep]
        _CARRY_COMPILE_STATS["compacted_claim_rows"] += old_claims - int(claims.shape[0])
    return replace(state, inventory=replace(inventory, cohorts=cohorts, claims=claims))


def _pad_carry_state_cohorts(
    state: DayTradeCarryState, *, target_rows: int
) -> tuple[DayTradeCarryState, int]:
    inventory = state.inventory
    logical_rows = int(inventory.cohorts.shape[0])
    if target_rows < logical_rows:
        raise ValueError("compiled carry cohort target is smaller than logical state")
    padding_rows = target_rows - logical_rows
    if padding_rows == 0:
        return state, logical_rows
    padding = inventory.cohorts.new_zeros(
        (padding_rows, inventory.cohorts.shape[1], inventory.cohorts.shape[2])
    )
    padding[..., CohortField.ACQUIRED_DAY] = inventory.observed_day
    padding[..., CohortField.ACCRUED_THROUGH] = inventory.observed_day
    padded = torch.cat((inventory.cohorts, padding), dim=0)
    _CARRY_COMPILE_STATS["padded_session_calls"] += 1
    return replace(state, inventory=replace(inventory, cohorts=padded)), logical_rows


def _strip_carry_session_padding(
    state: DayTradeCarryState, *, logical_rows_before_session: int
) -> DayTradeCarryState:
    cohorts = state.inventory.cohorts
    expected_minimum = logical_rows_before_session + 1
    if int(cohorts.shape[0]) < expected_minimum:
        raise RuntimeError("carry executor lost the newly appended acquisition cohort")
    compact = torch.cat(
        (cohorts[:logical_rows_before_session], cohorts[-1:]), dim=0
    )
    return replace(state, inventory=replace(state.inventory, cohorts=compact))


@dataclass(frozen=True)
class DayTradeCarrySession:
    """Executor-only facts: prices are not features or historical broker fills.

    Exit fields have shape [S,270,2] (long, short); marks [S,270]. All remaining
    vectors have shape [S]. ``opening_marks`` may contain an explicitly sourced
    carried valuation during a proven halt, never an invented executable open.
    Source adapters own completeness, dated legal prices and halt provenance.
    ``source_gap_mask`` is an explicit retrospective research exclusion, NOT
    market eligibility or a model feature. It suppresses only that symbol's
    new request, without renormalizing peers. It describes MISSING MINUTE
    SOURCES, not missing action terms. An affected physical position is a hard
    source failure. Already recognized, exactly dated cash claims need no stock
    quote and continue to settlement unchanged. Unknown corporate-action terms
    are checked only for prior physical inventory entitled to that event and
    travel through ``unresolved_action_gap_mask`` rather than masquerading as a
    missing-minute observation. ``stock_delivery_day`` keeps exact stock rights
    non-executable until the issuer's receipt-backed delivery/listing date.
    """
    day: int
    official_open: Tensor
    opening_marks: Tensor
    entry_price: Tensor
    entry_volume: Tensor
    lower_limit: Tensor
    upper_limit: Tensor
    halted: Tensor
    exit_prices: Tensor
    exit_capacity: Tensor
    marks: Tensor
    action_mask: Tensor | None = None
    share_ratio: Tensor | None = None
    cash_per_old_share: Tensor | None = None
    payment_day: Tensor | None = None
    stock_delivery_day: Tensor | None = None
    source_gap_mask: Tensor | None = None
    unresolved_action_gap_mask: Tensor | None = None
    daily_proxy_mask: Tensor | None = None
    exit_symbol_indices: Tensor | None = None
    exit_sides: Tensor | None = None
    symbol_event_starts: Tensor | None = None
    symbol_event_ends: Tensor | None = None
    minimum_marks: Tensor | None = None
    maximum_marks: Tensor | None = None
    mark_path_valid: Tensor | None = None
    minimum_exit_prices: Tensor | None = None
    maximum_exit_prices: Tensor | None = None
    # Explicit research assumption: the final liquidation uses this official
    # close and ignores market-volume capacity.  None preserves historical
    # residual-to-margin behavior and old artifacts exactly.
    terminal_liquidation_price: Tensor | None = None

    @property
    def uses_sparse_events(self) -> bool:
        sparse = (
            self.exit_symbol_indices,
            self.exit_sides,
            self.symbol_event_starts,
            self.symbol_event_ends,
            self.minimum_marks,
            self.maximum_marks,
            self.mark_path_valid,
            self.minimum_exit_prices,
            self.maximum_exit_prices,
        )
        if any(value is None for value in sparse) and not all(
            value is None for value in sparse
        ):
            raise ValueError("sparse carry session requires its complete event ABI")
        return all(value is not None for value in sparse)

    def validate_shape(self, symbols: int, device: torch.device) -> None:
        if not isinstance(self.day, int) or isinstance(self.day, bool) or not 0 < self.day <= 3652059:
            raise ValueError("carry session requires an exact Gregorian day ordinal")
        actions = (
            self.action_mask,
            self.share_ratio,
            self.cash_per_old_share,
            self.payment_day,
        )
        if any(x is None for x in actions) and not all(x is None for x in actions):
            raise ValueError("corporate action requires all exact physical/cash fields")
        sparse_events = self.uses_sparse_events
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == "day" or value is None:
                continue
            if sparse_events and field.name in {
                "exit_prices", "exit_capacity", "exit_symbol_indices", "exit_sides"
            }:
                shape = (self.exit_prices.shape[0],)
            elif field.name in {"symbol_event_starts", "symbol_event_ends"}:
                shape = (symbols,)
            elif field.name in {"minimum_exit_prices", "maximum_exit_prices"}:
                shape = (symbols, 2)
            elif sparse_events and field.name == "marks":
                shape = (symbols, 1)
            else:
                shape = ((symbols, 270, 2) if field.name in {"exit_prices", "exit_capacity"}
                         else (symbols, 270) if field.name == "marks" else (symbols,))
            if not isinstance(value, Tensor) or value.shape != shape or value.device != device:
                raise ValueError(f"carry session {field.name} differs from pinned universe/device")
            integer_identity = field.name in {
                "exit_symbol_indices", "exit_sides",
                "symbol_event_starts", "symbol_event_ends",
            }
            expected_dtype = torch.int64 if integer_identity else torch.float64
            if value.dtype != expected_dtype:
                raise ValueError(
                    f"carry session {field.name} requires {expected_dtype} dtype"
                )
        if sparse_events:
            events = int(self.exit_prices.shape[0])
            if events <= 0:
                raise ValueError("sparse carry session requires an inert or real event slot")
            if not torch.compiler.is_compiling() and device.type == "cpu":
                starts = self.symbol_event_starts
                ends = self.symbol_event_ends
                assert starts is not None and ends is not None
                if not bool(
                    ((starts >= 0) & (starts <= ends) & (ends <= events)).all()
                ):
                    raise ValueError("sparse carry session has invalid CSR intervals")
                if (
                    int(starts[0]) != 0
                    or not torch.equal(starts[1:], ends[:-1])
                ):
                    raise ValueError("sparse carry session CSR intervals are not contiguous")
                true_events = int(ends[-1])
                counts = ends - starts
                expected_symbols = torch.repeat_interleave(
                    torch.arange(symbols, dtype=torch.int64), counts
                )
                event_symbols = self.exit_symbol_indices
                event_sides = self.exit_sides
                assert event_symbols is not None and event_sides is not None
                if not torch.equal(event_symbols[:true_events], expected_symbols):
                    raise ValueError("sparse carry session events violate CSR symbol order")
                if not bool(((event_sides == 0) | (event_sides == 1)).all()):
                    raise ValueError("sparse carry session event side must be zero or one")
                if true_events < events and not bool(
                    (self.exit_capacity[true_events:] == 0).all()
                ):
                    raise ValueError("sparse carry session padding has executable liquidity")


def compact_day_trade_carry_session(
    session: DayTradeCarrySession,
) -> DayTradeCarrySession:
    """Losslessly replace the dense minute source by exact event sufficient statistics."""
    if session.uses_sparse_events:
        return session
    symbols, minutes, sides = session.exit_prices.shape
    if minutes != 270 or sides != 2 or session.marks.shape != (symbols, minutes):
        raise ValueError("only the authoritative dense 270-minute carry ABI can be compacted")
    prices = session.exit_prices
    capacity = session.exit_capacity
    retained = torch.isfinite(prices) | (capacity != 0) | ~torch.isfinite(capacity)
    flat_indices = torch.nonzero(retained.reshape(-1), as_tuple=False).flatten()
    event_symbols = torch.div(flat_indices, minutes * sides, rounding_mode="floor")
    event_sides = flat_indices.remainder(sides)
    flat_prices = prices.reshape(-1).index_select(0, flat_indices)
    flat_capacity = capacity.reshape(-1).index_select(0, flat_indices)
    symbol_axis = torch.arange(symbols, device=prices.device, dtype=torch.int64)
    starts = torch.searchsorted(event_symbols, symbol_axis, right=False)
    ends = torch.searchsorted(event_symbols, symbol_axis, right=True)

    price_ok = torch.isfinite(prices) & (prices > 0)
    positive_inf = torch.full_like(prices, float("inf"))
    negative_inf = torch.full_like(prices, float("-inf"))
    minimum_exit = torch.where(price_ok, prices, positive_inf).amin(dim=1)
    maximum_exit = torch.where(price_ok, prices, negative_inf).amax(dim=1)
    has_exit_price = price_ok.any(dim=1)
    minimum_exit = torch.where(has_exit_price, minimum_exit, 0)
    maximum_exit = torch.where(has_exit_price, maximum_exit, 0)

    mark_ok = torch.isfinite(session.marks) & (session.marks > 0)
    minimum_marks = torch.where(
        mark_ok, session.marks, torch.full_like(session.marks, float("inf"))
    ).amin(dim=1)
    maximum_marks = torch.where(
        mark_ok, session.marks, torch.full_like(session.marks, float("-inf"))
    ).amax(dim=1)
    mark_path_valid = mark_ok.all(dim=1).to(dtype=torch.float64)
    minimum_marks = torch.where(mark_path_valid.bool(), minimum_marks, 0)
    maximum_marks = torch.where(mark_path_valid.bool(), maximum_marks, 0)

    # Keep direct calls valid even for a session with no scheduler event.  CSR
    # intervals still end at zero, so this final slot is mathematically inert.
    if flat_indices.numel() == 0:
        flat_prices = prices.new_full((1,), float("nan"))
        flat_capacity = capacity.new_zeros((1,))
        event_symbols = torch.full(
            (1,), symbols - 1, device=prices.device, dtype=torch.int64
        )
        event_sides = torch.zeros((1,), device=prices.device, dtype=torch.int64)

    return replace(
        session,
        exit_prices=flat_prices,
        exit_capacity=flat_capacity,
        marks=session.marks[:, -1:],
        exit_symbol_indices=event_symbols,
        exit_sides=event_sides,
        symbol_event_starts=starts,
        symbol_event_ends=ends,
        minimum_marks=minimum_marks,
        maximum_marks=maximum_marks,
        mark_path_valid=mark_path_valid,
        minimum_exit_prices=minimum_exit,
        maximum_exit_prices=maximum_exit,
    )


@dataclass(frozen=True)
class DayTradeCarryState:
    inventory: DayTradeInventoryState
    last_nav: Tensor  # last committed close, not today's opening mark
    alive: Tensor  # financial default is independent of source failure
    initial_capital: float
    last_session_day: int = 0

    @classmethod
    def empty(cls, symbols: int, initial_capital: float, device: torch.device) -> DayTradeCarryState:
        if not math.isfinite(initial_capital) or initial_capital <= 0:
            raise ValueError("carry account initial capital must be positive finite")
        return cls(DayTradeInventoryState.empty(symbols, device=device),
                   torch.tensor(initial_capital, device=device, dtype=torch.float64),
                   torch.ones((), device=device, dtype=torch.bool), float(initial_capital))

    def detached(self, *, device: torch.device | str | None = None) -> DayTradeCarryState:
        target = self.last_nav.device if device is None else device
        inventory = DayTradeInventoryState(**{f.name: getattr(self.inventory, f.name).detach().to(target).clone()
                                             for f in fields(self.inventory)})
        return DayTradeCarryState(inventory, self.last_nav.detach().to(target).clone(),
            self.alive.detach().to(target).clone(), self.initial_capital, self.last_session_day)

    def validate(self, *, symbols: int, device: torch.device, initial_capital: float) -> Tensor:
        if (not math.isfinite(initial_capital) or initial_capital <= 0
                or self.initial_capital != initial_capital):
            raise ValueError("carry account initial capital differs from the continuing account")
        if (not isinstance(self.last_session_day, int) or isinstance(self.last_session_day, bool)
                or not 0 <= self.last_session_day <= 3652059):
            raise ValueError("carry account requires its last committed session ordinal")
        if (self.last_nav.shape != () or self.last_nav.dtype != torch.float64
                or self.last_nav.device != device or self.alive.shape != ()
                or self.alive.dtype != torch.bool or self.alive.device != device
                or self.inventory.cohorts.shape[1:] != (symbols, len(CohortField))):
            raise ValueError("carry account differs from the pinned universe/device/precision")
        for field in fields(self.inventory):
            value = getattr(self.inventory, field.name)
            if value.device != device or value.dtype != torch.float64:
                raise ValueError("carry inventory differs from the execution device/precision")
            expected = (self.inventory.cohorts.shape if field.name == "cohorts"
                else (self.inventory.claims.shape[0], symbols, 3) if field.name == "claims"
                else (symbols,) if field.name == "action_cursor" else ())
            if value.shape != expected:
                raise ValueError("carry inventory has invalid scalar/cursor/claim shape")
        return _require(torch.isfinite(self.last_nav)
            & torch.where(self.alive, self.last_nav > 0, self.last_nav == 0)
            & (self.inventory.failed == 0) & (self.inventory.observed_day <= self.last_session_day),
            "invalid continuing carry account valuation/state")

    def checkpoint_state(self, *, universe: tuple[str, ...], release_id: str) -> dict:
        valid = self.validate(symbols=len(universe), device=self.last_nav.device,
                              initial_capital=self.initial_capital)
        # Acceptance is outside the hot path and must observe CUDA predicates.
        if not bool(valid.detach().cpu()):
            raise ValueError("cannot checkpoint an invalid carry account")
        return {"abi": CARRY_SESSION_ABI, "initial_capital": self.initial_capital,
            "last_session_day": self.last_session_day,
            "last_nav": self.last_nav.detach().clone(), "alive": self.alive.detach().clone(),
            "inventory": self.inventory.checkpoint_state(universe=universe, release_id=release_id)}

    @classmethod
    def from_checkpoint_state(cls, payload: dict, *, universe: tuple[str, ...],
                              release_id: str, initial_capital: float,
                              device: torch.device | str = "cpu") -> DayTradeCarryState:
        if (set(payload) != {"abi", "initial_capital", "last_session_day", "last_nav", "alive", "inventory"}
                or payload["abi"] != CARRY_SESSION_ABI
                or payload["initial_capital"] != initial_capital):
            raise ValueError("incompatible physical carry checkpoint contract/capital")
        if (not isinstance(payload["last_nav"], Tensor) or not isinstance(payload["alive"], Tensor)
                or payload["last_nav"].dtype != torch.float64 or payload["alive"].dtype != torch.bool):
            raise ValueError("carry checkpoint lost valuation precision or boolean state")
        state = cls(DayTradeInventoryState.from_checkpoint_state(payload["inventory"],
            universe=universe, release_id=release_id, device="cpu"),
            payload["last_nav"].detach().cpu().clone(), payload["alive"].detach().cpu().clone(),
            initial_capital, payload["last_session_day"])
        state.validate(symbols=len(universe), device=torch.device("cpu"), initial_capital=initial_capital)
        validate_inventory_state(state.inventory, symbols=len(universe))
        return cls(DayTradeInventoryState(**{f.name: getattr(state.inventory, f.name).to(device)
            for f in fields(state.inventory)}), state.last_nav.to(device), state.alive.to(device),
            initial_capital, state.last_session_day)


@dataclass(frozen=True)
class DayTradeCarryResult:
    strategy_returns: Tensor
    turnovers: Tensor
    weights_history: Tensor
    shares_history: Tensor
    minute_nav: Tensor
    settlement_default: Tensor
    final_state: DayTradeCarryState


def _choose_inventory(condition: Tensor, new: DayTradeInventoryState,
                      old: DayTradeInventoryState) -> DayTradeInventoryState:
    """Freeze physical evidence on economic default; do not call it bad data."""
    values = {}
    for field in fields(old):
        before, after = getattr(old, field.name), getattr(new, field.name)
        if field.name in {"cohorts", "claims"} and before.shape[0] < after.shape[0]:
            padding = before.new_zeros((after.shape[0] - before.shape[0], *before.shape[1:]))
            if field.name == "cohorts" and before.shape[0]:
                # Inactive shape-padding still has to preserve FIFO chronology
                # when a financially defaulted (but valid) account is restored.
                padding[..., CohortField.ACQUIRED_DAY] = before[-1, :, CohortField.ACQUIRED_DAY]
            before = torch.cat((before, padding))
        values[field.name] = torch.where(condition, after, before)
    return DayTradeInventoryState(**values)


def execute_carry_session(
    state: DayTradeCarryState, session: DayTradeCarrySession, *, weights: Tensor,
    can_enter: Tensor, buy_fee_rate: Tensor, day_sell_fee_rate: Tensor,
    normal_sell_fee_rate: Tensor, rebate_rate: Tensor, initial_capital: float,
    event_compression: bool = False,
) -> tuple[DayTradeCarryState, Tensor, Tensor]:
    """09:00 target, 09:01 delta, scheduled exits, then physical residual carry.

    Returns committed state, gross traded notional, and either all 270
    net-liquidation marks or the exact ``[intraday lower bound, close NAV]``
    training summary. A default freezes physical evidence and prevents later
    trading; missing source evidence remains NaN/failed, not an economic
    default.
    """
    sparse_events = session.uses_sparse_events
    if sparse_events and not event_compression:
        raise ValueError(
            "sparse carry source is valid only for certified event compression"
        )
    inventory = release_inventory_stock_deliveries(
        state.inventory, day=session.day
    )
    source_valid = torch.ones((), dtype=torch.bool, device=weights.device)
    gap = torch.zeros_like(weights, dtype=torch.bool)
    if session.source_gap_mask is not None:
        raw_gap = session.source_gap_mask
        source_valid = _require(torch.isfinite(raw_gap) & ((raw_gap == 0) | (raw_gap == 1)),
                                "source gap mask must contain exact binary values")
        gap = raw_gap != 0
        # Check gross cohort quantities, not net shares (opposite cohorts must
        # not cancel evidence). A recognized cash claim has its own immutable
        # amount/payment date: missing stock quotes cannot invalidate it or
        # prevent payment. Missing/revised claim receipts remain adapter errors.
        exposed = (inventory.cohorts[..., CohortField.SHARES] != 0).any(dim=0)
        source_valid = source_valid & _require(~gap | ~exposed,
            "minute source gap intersects carried inventory; cannot mask away ownership")
        can_enter = torch.where(gap, 0, can_enter)
    unresolved_gap = torch.zeros_like(weights, dtype=torch.bool)
    if session.unresolved_action_gap_mask is not None:
        raw_unresolved = session.unresolved_action_gap_mask
        source_valid = source_valid & _require(
            torch.isfinite(raw_unresolved)
            & ((raw_unresolved == 0) | (raw_unresolved == 1)),
            "unresolved action gap mask must contain exact binary values",
        )
        unresolved_gap = raw_unresolved != 0
        exposed = (
            inventory.cohorts[..., CohortField.SHARES] != 0
        ).any(dim=0)
        source_valid = source_valid & _require(
            ~unresolved_gap | ~exposed,
            "unresolved corporate-action or terminal transition intersects "
            "carried inventory; cannot fabricate ownership terms",
        )
        can_enter = torch.where(unresolved_gap, 0, can_enter)
    # Defaulted accounts use an inert calculation branch; actual evidence is
    # restored below. This is tensor predication, not a refill of account cash.
    working = DayTradeInventoryState(**{
        f.name: torch.where(state.alive & source_valid, getattr(inventory, f.name), 0)
        for f in fields(inventory)
    })
    if session.action_mask is not None:
        # Match paper's held-interval gate. An ex-date catalogue row is not an
        # account entitlement: flat symbols and same-day purchases do not own
        # it. Never require every issuer's exact terms just to run healthy peers.
        source_valid = source_valid & _require(
            torch.isfinite(session.action_mask)
            & ((session.action_mask == 0) | (session.action_mask == 1)),
            "corporate action mask must contain exact binary values")
        entitled = ((working.cohorts[..., CohortField.SHARES] != 0)
                    & (working.cohorts[..., CohortField.ACQUIRED_DAY] < session.day)).any(dim=0)
        working = apply_inventory_action(working, event_day=session.day, as_of_day=session.day,
            event_mask=torch.where(entitled & source_valid, session.action_mask, 0), share_ratio=session.share_ratio,
            cash_per_old_share=session.cash_per_old_share,
            payment_day=session.payment_day,
            stock_delivery_day=session.stock_delivery_day)
    working = settle_inventory_claims(accrue_inventory_interest(working, day=session.day), day=session.day)
    opening_nav = inventory_nav(working, initial_capital=initial_capital,
                               marks=session.opening_marks)
    valid = (_require(torch.isfinite(opening_nav), "carry account opening valuation is invalid")
             & (inventory.failed == 0) & source_valid)
    trade_alive = state.alive & (opening_nav > 0) & valid & (working.failed == 0)
    funded_fields = {
        f.name: torch.where(trade_alive, getattr(working, f.name), 0)
        for f in fields(working)
    }
    # The inert branch owns no money or positions, but its calendar has already
    # advanced above. Preserve that fact so the opening kernel can skip the
    # formerly duplicated interest/claim pass even for an opening default.
    funded_fields["observed_day"] = working.observed_day
    funded = DayTradeInventoryState(**funded_fields)
    target_weights = torch.where(trade_alive, weights, 0)
    if _carry_full_session_compile_enabled(funded.cohorts):
        path_inventory, converted, marks, notional = _compiled_session_execution(
            funded,
            session,
            weights=target_weights,
            can_enter=can_enter,
            buy_fee_rate=buy_fee_rate,
            day_sell_fee_rate=day_sell_fee_rate,
            normal_sell_fee_rate=normal_sell_fee_rate,
            rebate_rate=rebate_rate,
            initial_capital=initial_capital,
            event_compression=event_compression,
        )
    else:
        opening = rebalance_inventory_at_open(funded, weights=target_weights,
            official_open=session.official_open, opening_marks=session.opening_marks,
            entry_price=session.entry_price, entry_volume_shares=session.entry_volume,
            lower_limit=session.lower_limit, upper_limit=session.upper_limit,
            can_enter=can_enter, buy_fee_rate=buy_fee_rate, day_sell_fee_rate=day_sell_fee_rate,
            normal_sell_fee_rate=normal_sell_fee_rate, rebate_rate=rebate_rate,
            initial_capital=initial_capital, day=session.day, halted=session.halted,
            daily_proxy_mask=session.daily_proxy_mask, state_already_advanced=True)
        # There are exactly two sides, so use a predicate, not arbitrary indices.
        # Preserve both NaN/no-order semantics and the selected side's gradient.
        short = (opening.state.shares < 0)[:, None]
        if sparse_events:
            prices = session.exit_prices
            capacity = session.exit_capacity
        else:
            prices = torch.where(
                short, session.exit_prices[..., 1], session.exit_prices[..., 0]
            )
            capacity = torch.where(
                short, session.exit_capacity[..., 1], session.exit_capacity[..., 0]
            )
        if event_compression:
            if sparse_events:
                liquidity = reduce_inventory_fifo_sparse_liquidity(
                    opening.state,
                    prices=prices,
                    capacity_shares=capacity,
                    event_symbol_indices=session.exit_symbol_indices,
                    event_sides=session.exit_sides,
                    symbol_event_starts=session.symbol_event_starts,
                    symbol_event_ends=session.symbol_event_ends,
                )
                assert session.minimum_exit_prices is not None
                assert session.maximum_exit_prices is not None
                assert session.minimum_marks is not None
                assert session.maximum_marks is not None
                assert session.mark_path_valid is not None
                selected_minimum_exit = torch.where(
                    short.squeeze(-1),
                    session.minimum_exit_prices[:, 1],
                    session.minimum_exit_prices[:, 0],
                )
                selected_maximum_exit = torch.where(
                    short.squeeze(-1),
                    session.maximum_exit_prices[:, 1],
                    session.maximum_exit_prices[:, 0],
                )
                lower_bound = inventory_intraday_nav_lower_bound_from_extrema(
                    opening.state,
                    minimum_marks=session.minimum_marks,
                    maximum_marks=session.maximum_marks,
                    mark_path_valid=session.mark_path_valid,
                    minimum_prices=selected_minimum_exit,
                    maximum_prices=selected_maximum_exit,
                    initial_capital=initial_capital,
                )
                lower_bound = torch.where(
                    liquidity.reduction.state.failed == 0,
                    lower_bound,
                    torch.full_like(lower_bound, float("nan")),
                )
                path_inventory = liquidity.reduction.state
                marks = lower_bound.expand(2)
                exit_notional = liquidity.executed_notional.sum()
            else:
                path, marks = _run_inventory_path(
                    opening.state,
                    prices=prices,
                    capacity_shares=capacity,
                    marks=session.marks,
                    initial_capital=initial_capital,
                )
                path_inventory = path.reduction.state
                exit_notional = (
                    path.minute_filled_shares * torch.nan_to_num(prices, nan=0)
                ).sum()
        else:
            path, marks = _run_inventory_path(
                opening.state,
                prices=prices,
                capacity_shares=capacity,
                marks=session.marks,
                initial_capital=initial_capital,
            )
            # A solvent first pass has ``before_alive == 1`` at every minute,
            # hence the replay inputs and every differentiable result match.
            first_pass_solvent = (
                not torch.compiler.is_compiling()
                and bool((marks > 0).all().detach())
            )
            if not first_pass_solvent:
                before_alive = torch.cat((torch.ones_like(marks[:1], dtype=torch.bool),
                                          (marks[:-1] > 0).cumprod(0).bool()))
                path, marks = _run_inventory_path(
                    opening.state,
                    prices=prices,
                    capacity_shares=capacity * before_alive,
                    marks=session.marks,
                    initial_capital=initial_capital,
                )
            path_inventory = path.reduction.state
            exit_notional = (
                path.minute_filled_shares * torch.nan_to_num(prices, nan=0)
            ).sum()
        if session.terminal_liquidation_price is not None:
            path_inventory, terminal_notional = (
                _liquidate_terminal_inventory_without_capacity(
                    path_inventory,
                    terminal_price=session.terminal_liquidation_price,
                )
            )
            exit_notional = exit_notional + terminal_notional
        converted = convert_inventory_to_margin(path_inventory, day=session.day)
        notional = ((opening.reduction.filled_shares + opening.addition_shares.abs())
                    * torch.nan_to_num(session.entry_price, nan=0)).sum()
        notional = notional + exit_notional
    valid = valid & _require(torch.isfinite(marks), "carry account minute valuation is invalid")
    intraday_alive = trade_alive & (marks > 0).all()
    final_inventory = _choose_inventory(intraday_alive, converted, path_inventory)
    closing_marks = session.marks[:, -1]
    if session.terminal_liquidation_price is not None:
        terminal_ok = (
            torch.isfinite(session.terminal_liquidation_price)
            & (session.terminal_liquidation_price > 0)
        )
        closing_marks = torch.where(
            terminal_ok,
            session.terminal_liquidation_price,
            closing_marks,
        )
    closing_nav = inventory_nav(final_inventory, initial_capital=initial_capital,
                                marks=closing_marks)
    valid = valid & _require(torch.isfinite(closing_nav), "carry account close valuation is invalid")
    final_alive = intraday_alive & (closing_nav > 0)
    raw_marks = torch.cat((marks[:-1], closing_nav.reshape(1)))
    mark_alive = trade_alive & (raw_marks > 0).cumprod(0).bool()
    committed_marks = torch.where(mark_alive, raw_marks, 0)
    committed_marks = torch.where(valid, committed_marks, float("nan"))
    if event_compression:
        exact_minimum = committed_marks.amin()
        exact_minimum = torch.where(
            torch.isfinite(committed_marks).all(),
            exact_minimum,
            torch.full_like(exact_minimum, float("nan")),
        )
        committed_marks = torch.stack(
            (exact_minimum, committed_marks[-1])
        )
    final_inventory = _choose_inventory(trade_alive, final_inventory, working)
    final_inventory = _choose_inventory(state.alive, final_inventory, inventory)
    # Source failure is atomic across the account, including malformed action
    # terms on CUDA. Do not leave successful peers half-advanced on retry.
    final_inventory = _choose_inventory(valid, final_inventory, inventory)
    final_inventory = replace(final_inventory,
        failed=torch.maximum(final_inventory.failed, (~valid).to(torch.float64)))
    return (DayTradeCarryState(final_inventory, committed_marks[-1],
            torch.where(valid, final_alive, state.alive), initial_capital, session.day),
            committed_marks, torch.where(trade_alive & valid, notional, 0))


def run_day_trade_carry_sessions(
    weights: Tensor, sessions: tuple[DayTradeCarrySession, ...], *, can_enter: Tensor,
    buy_fee_rate: Tensor, day_sell_fee_rate: Tensor, normal_sell_fee_rate: Tensor,
    rebate_rate: Tensor, initial_capital: float, initial_state: DayTradeCarryState | None = None,
    event_compression: bool = False,
) -> DayTradeCarryResult:
    """Chronological executor used by the canonical simulator/loss boundary."""
    if weights.ndim != 2 or weights.shape[0] != len(sessions) or not sessions:
        raise ValueError("carry sessions must match a nonempty [T,S] action trajectory")
    if can_enter.shape != weights.shape:
        raise ValueError("carry entry permissions must match the action trajectory")
    if any(a.day >= b.day for a, b in zip(sessions, sessions[1:])):
        raise ValueError("carry sessions must be strictly chronological")
    state = initial_state or DayTradeCarryState.empty(weights.shape[1], initial_capital, weights.device)
    if sessions[0].day <= state.last_session_day:
        raise ValueError("carry session precedes or repeats the last committed session")
    valid_state = state.validate(symbols=weights.shape[1], device=weights.device,
                                 initial_capital=initial_capital)
    state = replace(state, inventory=replace(state.inventory,
        failed=torch.maximum(state.inventory.failed, (~valid_state).to(torch.float64))))
    # Chunk boundaries are truncated-BPTT boundaries.  Once the incoming
    # inventory is detached, fully consumed FIFO/claim rows have no accounting
    # meaning and only retain storage.  Formal artifact replay deliberately
    # uses the eager path, so restricting this compaction to the compiled path
    # made its state grow by one inert cohort row per session.  The helper is a
    # no-op for any state that still participates in autograd and its stable
    # packing preserves every active FIFO row and claim in chronological order.
    state = _compact_detached_carry_state(state)
    compile_paths = _carry_path_compile_enabled(weights)
    if compile_paths:
        # Every session appends exactly one acquisition row. Temporarily pad
        # each input to K-1 so every FIFO core in this truncated-BPTT call sees
        # the same power-of-two K; remove only those inert middle rows after it.
        maximum_rows = int(state.inventory.cohorts.shape[0]) + len(sessions)
        path_cohort_rows = 1 << max(0, int(maximum_rows - 1).bit_length())
        pre_session_rows = path_cohort_rows - 1
    else:
        pre_session_rows = -1
    returns, turns, curves, holdings, exposure, defaults = [], [], [], [], [], []
    for i, session in enumerate(sessions):
        session.validate_shape(weights.shape[1], weights.device)
        # ``execute_carry_session`` deliberately uses the shared tensor
        # ``_require`` helper so CUDA/compiled training records an atomic
        # failure instead of poisoning every DDP rank with a device assert.
        # On the CPU no-grad artifact replay, however, that helper raises
        # before the richer post-session diagnostic below can name the bad
        # contract-day.  Diagnose the ownership/source invariant at this eager
        # boundary first.  This is validation only: it neither masks the
        # position nor changes any fill, NAV, or gradient semantics.
        if (
            not torch.compiler.is_compiling()
            and not torch.is_grad_enabled()
            and weights.device.type == "cpu"
            and session.source_gap_mask is not None
        ):
            exposed = (
                state.inventory.cohorts[..., CohortField.SHARES] != 0
            ).any(dim=0)
            gap_held = torch.nonzero(
                (session.source_gap_mask != 0) & exposed
            ).flatten()
            if gap_held.numel():
                indices = gap_held.detach().cpu().tolist()
                shares = state.inventory.shares.index_select(
                    0, gap_held
                ).detach().cpu().tolist()
                raise RuntimeError(
                    "physical source gap intersects carried inventory at first "
                    "bad contract-day; cannot mask away ownership: "
                    f"date={date.fromordinal(session.day).isoformat()} row={i} "
                    f"symbol_indices={indices[:32]} "
                    f"signed_shares={shares[:32]} count={len(indices)}"
                )
        if (
            not torch.compiler.is_compiling()
            and not torch.is_grad_enabled()
            and weights.device.type == "cpu"
            and session.unresolved_action_gap_mask is not None
        ):
            exposed = (
                state.inventory.cohorts[..., CohortField.SHARES] != 0
            ).any(dim=0)
            gap_held = torch.nonzero(
                (session.unresolved_action_gap_mask != 0) & exposed
            ).flatten()
            if gap_held.numel():
                indices = gap_held.detach().cpu().tolist()
                shares = state.inventory.shares.index_select(
                    0, gap_held
                ).detach().cpu().tolist()
                raise RuntimeError(
                    "unresolved corporate-action or terminal transition "
                    "intersects carried inventory at first bad contract-day; "
                    "cannot fabricate ownership terms: "
                    f"date={date.fromordinal(session.day).isoformat()} row={i} "
                    f"symbol_indices={indices[:32]} "
                    f"signed_shares={shares[:32]} count={len(indices)}"
                )
        if session.uses_sparse_events:
            sparse_cells = int(session.exit_prices.numel())
            _CARRY_COMPILE_STATS["sparse_event_session_calls"] += 1
            _CARRY_COMPILE_STATS["sparse_event_cells"] += sparse_cells
            _CARRY_COMPILE_STATS["dense_event_cells_avoided"] += max(
                0, int(weights.shape[1]) * 270 * 2 - sparse_cells
            )
        if compile_paths:
            state, logical_rows = _pad_carry_state_cohorts(
                state, target_rows=pre_session_rows
            )
        else:
            logical_rows = -1
        prior_nav, prior_alive, prior_inventory = (
            state.last_nav,
            state.alive,
            state.inventory,
        )
        state, curve, notional = execute_carry_session(state, session,
            weights=weights[i], can_enter=can_enter[i], buy_fee_rate=buy_fee_rate,
            day_sell_fee_rate=day_sell_fee_rate, normal_sell_fee_rate=normal_sell_fee_rate,
            rebate_rate=rebate_rate, initial_capital=initial_capital,
            event_compression=event_compression)
        if compile_paths:
            state = _strip_carry_session_padding(
                state, logical_rows_before_session=logical_rows
            )
        if (
            not torch.compiler.is_compiling()
            and not torch.is_grad_enabled()
            and not event_compression
            and bool(state.inventory.failed.detach().cpu())
        ):
            # CUDA kernels return a device validity predicate instead of using
            # a device assert, which would poison every DDP rank. At the eager
            # evaluation boundary, stop on the first bad contract-day and emit
            # enough evidence to repair that exact symbol/day without masking
            # the rest of the session.
            exposed = (
                prior_inventory.cohorts[..., CohortField.SHARES] != 0
            ).any(dim=0)
            gap = (
                torch.zeros_like(exposed)
                if session.source_gap_mask is None
                else session.source_gap_mask != 0
            )
            gap_held = torch.nonzero(gap & exposed).flatten().detach().cpu().tolist()
            unresolved = (
                torch.zeros_like(exposed)
                if session.unresolved_action_gap_mask is None
                else session.unresolved_action_gap_mask != 0
            )
            unresolved_held = torch.nonzero(
                unresolved & exposed
            ).flatten().detach().cpu().tolist()
            opening_missing = torch.nonzero(
                exposed
                & (~torch.isfinite(session.opening_marks) | (session.opening_marks <= 0))
            ).flatten().detach().cpu().tolist()
            path_missing = torch.nonzero(
                exposed
                & (
                    ~torch.isfinite(session.marks)
                    | (session.marks <= 0)
                ).any(dim=1)
            ).flatten().detach().cpu().tolist()
            close_missing = torch.nonzero(
                exposed
                & (
                    ~torch.isfinite(session.marks[:, -1])
                    | (session.marks[:, -1] <= 0)
                )
            ).flatten().detach().cpu().tolist()
            requested_missing_path = torch.nonzero(
                (weights[i] != 0)
                & can_enter[i].bool()
                & (
                    ~torch.isfinite(session.marks)
                    | (session.marks <= 0)
                ).any(dim=1)
            ).flatten().detach().cpu().tolist()
            action_indices = (
                []
                if session.action_mask is None
                else torch.nonzero(session.action_mask != 0)
                .flatten().detach().cpu().tolist()
            )
            raise FloatingPointError(
                "physical source integrity failed at first bad contract-day: "
                f"date={date.fromordinal(session.day).isoformat()} "
                f"row={i} gap_held_symbol_indices={gap_held[:32]} "
                f"gap_held_count={len(gap_held)} "
                f"unresolved_action_held_indices={unresolved_held[:32]} "
                f"unresolved_action_held_count={len(unresolved_held)} "
                f"opening_mark_missing_held_indices={opening_missing[:32]} "
                f"opening_mark_missing_held_count={len(opening_missing)} "
                f"minute_mark_missing_held_indices={path_missing[:32]} "
                f"minute_mark_missing_held_count={len(path_missing)} "
                f"close_mark_missing_held_indices={close_missing[:32]} "
                f"close_mark_missing_held_count={len(close_missing)} "
                f"requested_missing_path_indices={requested_missing_path[:32]} "
                f"requested_missing_path_count={len(requested_missing_path)} "
                f"action_indices={action_indices[:32]} "
                f"action_count={len(action_indices)}"
            )
        # Reuse the canonical return/ruin-floor math, while retaining invalid
        # source NaNs (the generic helper's legacy NaN sanitization is unsafe
        # for physical inventory acceptance).
        from stockagent.backtest.simulator import _portfolio_simple_returns_to_log_torch
        ratio = state.last_nav / prior_nav.clamp_min(1e-30)
        log_return = _portfolio_simple_returns_to_log_torch(ratio - 1)
        returns.append(torch.where(torch.isfinite(ratio),
            torch.where(prior_alive, log_return, 0), float("nan")))
        turns.append(notional / prior_nav.clamp_min(1e-30))
        curves.append(curve)
        holdings.append(state.inventory.shares)
        exposure.append(torch.where(state.alive,
            state.inventory.shares * torch.nan_to_num(
                (
                    torch.where(
                        torch.isfinite(session.terminal_liquidation_price)
                        & (session.terminal_liquidation_price > 0),
                        session.terminal_liquidation_price,
                        session.marks[:, -1],
                    )
                    if session.terminal_liquidation_price is not None
                    else session.marks[:, -1]
                ),
                nan=0,
            )
            / state.last_nav.clamp_min(1e-30), 0))
        defaults.append(~state.alive)
    result = DayTradeCarryResult(torch.stack(returns), torch.stack(turns), torch.stack(exposure),
        torch.stack(holdings), torch.stack(curves), torch.stack(defaults), state)
    if event_compression:
        # One synchronization per truncated-BPTT batch, never one per session.
        # Dense sessions carry their exact first-pass 270-point NAV; sparse
        # sessions carry a conservative bound.  Positivity proves the masked
        # default replay is an identity.  An inconclusive batch is recomputed
        # by the complete authoritative path, never accepted approximately.
        certified = torch.isfinite(result.minute_nav).all() & (
            result.minute_nav > 0
        ).all()
        if not bool(certified.detach().cpu()):
            del result, state, returns, turns, curves, holdings, exposure, defaults
            _CARRY_COMPILE_STATS["event_compression_fallback_batches"] += 1
            if sessions[0].uses_sparse_events:
                raise DayTradeCarryEventCompressionFallback(
                    "sparse event certificate was inconclusive; dense minute replay required"
                )
            fallback = run_day_trade_carry_sessions(
                weights,
                sessions,
                can_enter=can_enter,
                buy_fee_rate=buy_fee_rate,
                day_sell_fee_rate=day_sell_fee_rate,
                normal_sell_fee_rate=normal_sell_fee_rate,
                rebate_rate=rebate_rate,
                initial_capital=initial_capital,
                initial_state=initial_state,
                event_compression=False,
            )
            # The caller requested the compact ABI.  The fallback still
            # computes all 270 authoritative points, then retains their exact
            # worst and closing NAV so downstream buffers keep a fixed shape.
            compact_curve = torch.stack(
                (fallback.minute_nav.amin(dim=-1), fallback.minute_nav[:, -1]),
                dim=-1,
            )
            return replace(fallback, minute_nav=compact_curve)
        if result.minute_nav.shape[-1] != 2:
            result = replace(
                result,
                minute_nav=torch.stack(
                    (result.minute_nav.amin(dim=-1), result.minute_nav[:, -1]),
                    dim=-1,
                ),
            )
    return result
