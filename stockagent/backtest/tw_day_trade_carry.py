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
from torch.utils.checkpoint import checkpoint

from stockagent.backtest.tw_day_trade_inventory import (
    CohortField, DayTradeInventoryState, InventoryPathReduction,
    InventoryReduction, _require, accrue_inventory_interest,
    apply_inventory_action, convert_inventory_to_margin, inventory_nav,
    inventory_intraday_nav_lower_bound_from_extrema, inventory_path_nav,
    rebalance_inventory_at_open, reduce_inventory_fifo, reduce_inventory_fifo_sparse_liquidity,
    reduce_inventory_fifo_path, release_inventory_stock_deliveries,
    settle_inventory_claims,
    validate_inventory_state,
)

CARRY_SESSION_ABI = "tw_day_trade_physical_fifo_sessions_v5_pending_stock"


class DayTradeCarryEventCompressionFallback(RuntimeError):
    """The sufficient-statistic proof was inconclusive; replay dense minutes."""

_COMPILED_PATHS: dict[tuple[object, ...], Callable[..., tuple[Tensor, ...]]] = {}
_COMPILED_SESSIONS: dict[tuple[object, ...], Callable[..., tuple[Tensor, ...]]] = {}
_COMPILED_SWEEP_STEPS: dict[tuple[object, ...], Callable[..., tuple[Tensor, ...]]] = {}
_COMPILED_COMMITS: dict[tuple[object, ...], Callable[..., tuple[Tensor, ...]]] = {}
_COMPILED_PATH_LOCK = threading.Lock()
_CARRY_COMPILE_STATS = {
    "compile_constructors": 0,
    "session_compile_constructors": 0,
    "compiled_path_calls": 0,
    "compiled_session_calls": 0,
    "eager_path_calls": 0,
    "checkpointed_sweep_session_calls": 0,
    "eager_sweep_session_calls": 0,
    "compiled_sweep_step_calls": 0,
    "sweep_step_compile_constructors": 0,
    "certified_sweep_suffix_calls": 0,
    "certified_sweep_minutes_avoided": 0,
    "compile_failures": 0,
    "eager_fallback_calls": 0,
    "padded_session_calls": 0,
    "compacted_cohort_rows": 0,
    "compacted_claim_rows": 0,
    "event_compression_fallback_batches": 0,
    "sparse_event_session_calls": 0,
    "sparse_event_cells": 0,
    "dense_event_cells_avoided": 0,
    "checkpointed_trajectory_blocks": 0,
    "commit_compile_constructors": 0,
    "compiled_commit_calls": 0,
    "flat_terminal_batches": 0,
    "flat_terminal_cleared_sessions": 0,
    "flat_terminal_fallback_batches": 0,
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
            _COMPILED_SWEEP_STEPS.clear()
            _COMPILED_COMMITS.clear()


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
        _COMPILED_SWEEP_STEPS.clear()
        _COMPILED_COMMITS.clear()
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


def _inventory_path_nav_with_history_grad(
    state: DayTradeInventoryState, *, prices: Tensor, minute_filled_shares: Tensor,
    marks: Tensor, initial_capital: float, require_minute_nav_grad: bool,
) -> Tensor:
    """Keep the exact solvency scan, omit its unused adjoint for loss callers.

    The public simulator retains differentiable minute diagnostics by default.
    Financial state, fills and the post-conversion close remain differentiable;
    this policy concerns only the pre-conversion intraday valuation branch.
    """
    with torch.set_grad_enabled(torch.is_grad_enabled() and require_minute_nav_grad):
        return inventory_path_nav(
            state, prices=prices, minute_filled_shares=minute_filled_shares,
            marks=marks, initial_capital=initial_capital,
        )


def _compiled_inventory_path(
    state: DayTradeInventoryState,
    *,
    prices: Tensor,
    capacity_shares: Tensor,
    marks: Tensor,
    initial_capital: float,
    require_minute_nav_grad: bool = True,
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
    compile_options = _carry_compile_options()
    key: tuple[object, ...] = (
        int(device_index),
        str(state.cohorts.dtype),
        int(state.cohorts.shape[0]),
        int(state.cohorts.shape[1]),
        float(initial_capital),
        training_graph,
        require_minute_nav_grad,
        tuple(sorted(compile_options.items())),
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
                minute_nav = _inventory_path_nav_with_history_grad(
                    slim,
                    prices=path_prices,
                    minute_filled_shares=path.minute_filled_shares,
                    marks=path_marks,
                    initial_capital=initial_capital,
                    require_minute_nav_grad=require_minute_nav_grad,
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
                options=compile_options,
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
            state, prices=prices, capacity_shares=capacity_shares,
        )
        return path, _inventory_path_nav_with_history_grad(
            state,
            prices=prices,
            minute_filled_shares=path.minute_filled_shares,
            marks=marks,
            initial_capital=initial_capital,
            require_minute_nav_grad=require_minute_nav_grad,
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
    require_minute_nav_grad: bool = True,
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
        require_minute_nav_grad,
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
                        minute_nav = _inventory_path_nav_with_history_grad(
                            opening.state,
                            prices=prices,
                            minute_filled_shares=path.minute_filled_shares,
                            marks=marks,
                            initial_capital=initial_capital,
                            require_minute_nav_grad=require_minute_nav_grad,
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
                        opening.state, prices=prices, capacity_shares=capacity,
                    )
                    first_marks = _inventory_path_nav_with_history_grad(
                        opening.state,
                        prices=prices,
                        minute_filled_shares=first.minute_filled_shares,
                        marks=marks,
                        initial_capital=initial_capital,
                        require_minute_nav_grad=require_minute_nav_grad,
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
                    minute_nav = _inventory_path_nav_with_history_grad(
                        opening.state,
                        prices=prices,
                        minute_filled_shares=path.minute_filled_shares,
                        marks=marks,
                        initial_capital=initial_capital,
                        require_minute_nav_grad=require_minute_nav_grad,
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
                if not require_minute_nav_grad:
                    minute_nav = minute_nav.detach()
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
    require_minute_nav_grad: bool = True,
) -> tuple[InventoryPathReduction, Tensor]:
    if _carry_path_compile_enabled(state.cohorts):
        return _compiled_inventory_path(
            state,
            prices=prices,
            capacity_shares=capacity_shares,
            marks=marks,
            initial_capital=initial_capital,
            require_minute_nav_grad=require_minute_nav_grad,
        )
    # Telemetry is an eager orchestration concern.  Mutating a Python counter
    # inside an enclosing ``torch.compile(fullgraph=True)`` region turns its
    # value into a guard and forces one recompilation per FIFO call.
    if not torch.compiler.is_compiling():
        _CARRY_COMPILE_STATS["eager_path_calls"] += 1
    path = reduce_inventory_fifo_path(
        state, prices=prices, capacity_shares=capacity_shares,
    )
    return path, _inventory_path_nav_with_history_grad(
        state,
        prices=prices,
        minute_filled_shares=path.minute_filled_shares,
        marks=marks,
        initial_capital=initial_capital,
        require_minute_nav_grad=require_minute_nav_grad,
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
    # Opt-in daily frozen-order execution, including missing first-minute fills.
    # [S,270,(VWAP,volume_shares)] and fresh long/short stop observations.
    entry_path: Tensor | None = None
    stop_hits: Tensor | None = None

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
        if (self.entry_path is None) != (self.stop_hits is None):
            raise ValueError("minute entry continuation requires prices, volume and stop observations")
        if self.entry_path is not None and self.uses_sparse_events:
            raise ValueError("minute entry continuation requires dense carry")
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
                shape = ((symbols, 270, 2) if field.name in {"exit_prices", "exit_capacity", "entry_path", "stop_hits"}
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
    if session.entry_path is not None:
        raise ValueError("frozen target entry sweeps cannot discard the chronological minute tape")
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


def _pad_inventory_choice(name: str, before: Tensor, after: Tensor) -> Tensor:
    if name in {"cohorts", "claims"} and before.shape[0] < after.shape[0]:
        padding = before.new_zeros((after.shape[0] - before.shape[0], *before.shape[1:]))
        if name == "cohorts" and before.shape[0]:
            # Preserve the original frozen-default FIFO padding convention.
            padding[..., CohortField.ACQUIRED_DAY] = before[-1, :, CohortField.ACQUIRED_DAY]
        before = torch.cat((before, padding))
    return before


def _choose_inventory(condition: Tensor, new: DayTradeInventoryState,
                      old: DayTradeInventoryState) -> DayTradeInventoryState:
    """Freeze physical evidence on economic default; do not call it bad data."""
    values = {field.name: torch.where(condition, getattr(new, field.name),
        _pad_inventory_choice(field.name, getattr(old, field.name), getattr(new, field.name)))
        for field in fields(old)}
    return DayTradeInventoryState(**values)


_INVENTORY_FIELDS = tuple(f.name for f in fields(DayTradeInventoryState))


def _commit_carry_inventory(candidate: DayTradeInventoryState,
    working: DayTradeInventoryState, original: DayTradeInventoryState, *,
    trade_alive: Tensor, accept: Tensor) -> DayTradeInventoryState:
    """One fused selection instead of three full-ledger read/write passes.

    where(valid, where(alive, where(trade_alive, candidate, working), old), old)
    equals where(valid & alive, where(trade_alive, candidate, working), old).
    Calendar/claim padding is still the canonical eager operation. The kernel
    contains only selection, no FP64 reduction, scan, index or column stacking.
    """
    candidates = tuple(getattr(candidate, name) for name in _INVENTORY_FIELDS)
    working_values = tuple(_pad_inventory_choice(name, getattr(working, name), value)
                           for name, value in zip(_INVENTORY_FIELDS, candidates))
    originals = tuple(_pad_inventory_choice(name, getattr(original, name), value)
                       for name, value in zip(_INVENTORY_FIELDS, candidates))
    cohort_index = _INVENTORY_FIELDS.index("cohorts")

    def core(new, work, old, trading, accepting):
        return (torch.where(accepting, torch.where(trading, new, work), old),)

    # Claims have a variable row axis and are tiny relative to [K,S,12]. Keep
    # those eager; compiling them here would specialize once per action/date.
    inputs = (candidates[cohort_index], working_values[cohort_index],
              originals[cohort_index], trade_alive, accept)
    compiled = None
    if (_carry_path_compile_enabled(candidate.cohorts)
            and _env_truthy("STOCKAGENT_DAY_TRADE_CARRY_COMMIT_COMPILE", "1")):
        options = _carry_compile_options()
        key = (str(candidate.cohorts.device), torch.is_grad_enabled(),
               tuple((tuple(value.shape), tuple(value.stride()), str(value.dtype), value.requires_grad)
                     for value in inputs), tuple(sorted(options.items())))
        with _COMPILED_PATH_LOCK:
            compiled = _COMPILED_COMMITS.get(key)
            if compiled is None:
                compiled = torch.compile(_compile_isolated_code_object(core,
                    name=f"day_trade_carry_commit_{abs(hash(key))}"), fullgraph=True,
                    dynamic=False, options=options)
                _COMPILED_COMMITS[key] = compiled
                _CARRY_COMPILE_STATS["commit_compile_constructors"] += 1
    if compiled is None:
        cohorts, = core(*inputs)
    else:
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore",
                    message=r"The \.grad attribute of a Tensor that is not a leaf Tensor.*",
                    category=UserWarning)
                cohorts, = compiled(*inputs)
            _CARRY_COMPILE_STATS["compiled_commit_calls"] += 1
        except Exception:
            with _COMPILED_PATH_LOCK:
                _COMPILED_COMMITS.pop(key, None)
            _CARRY_COMPILE_STATS["compile_failures"] += 1
            if _strict_no_fallback_enabled():
                raise
            _CARRY_COMPILE_STATS["eager_fallback_calls"] += 1
            cohorts, = core(*inputs)
    values = tuple(cohorts if name == "cohorts" else torch.where(accept,
        torch.where(trade_alive, new, work), old)
        for name, new, work, old in zip(_INVENTORY_FIELDS, candidates, working_values, originals))
    return DayTradeInventoryState(**dict(zip(_INVENTORY_FIELDS, values)))


def _sweep_checkpoint_enabled(weights: Tensor) -> bool:
    return (weights.device.type == "cuda" and torch.is_grad_enabled()
            and _env_truthy("STOCKAGENT_DAY_TRADE_SWEEP_CHECKPOINT", "1"))


def _frozen_target_minute(
    inventory: DayTradeInventoryState, weights: Tensor, target: Tensor,
    sizing_nav: Tensor, stopped: Tensor, cancelled: Tensor, solvent: Tensor, *,
    minute_fields: tuple[Tensor, ...], entry_phase: bool, common: dict,
    initial_capital: float,
) -> tuple[Tensor, ...]:
    from stockagent.backtest.tw_day_trade_minute import _capacity

    short = inventory.shares < 0
    held = inventory.shares != 0
    new_quantity = inventory.cohorts[-1, :, CohortField.SHARES].abs()
    prices, capacities, entry, hits, mark = minute_fields
    price = torch.where(short, prices[:, 1], prices[:, 0])
    capacity = torch.where(short, capacities[:, 1], capacities[:, 0])
    quote, volume = entry.unbind(-1)
    if entry_phase:
        hit = torch.where(short, hits[:, 1], hits[:, 0]).bool()
        stopped = stopped | (held & hit)
        quote_ok = torch.isfinite(quote) & (quote > 0)
        observed_capacity = torch.where(quote_ok, _capacity(volume), 0)
        price = torch.where(stopped & held & quote_ok, quote, price)
        capacity = torch.where(stopped & held & quote_ok, observed_capacity, capacity)
    reduction = reduce_inventory_fifo(inventory,
        requested_shares=inventory.tradable_shares.abs(), price=price,
        capacity_shares=torch.where(solvent, capacity, 0))
    inventory = reduction.state
    notional = (reduction.filled_shares * torch.nan_to_num(price, nan=0)).sum()
    if entry_phase:
        # A stop on yesterday's opposite-side cohort must not cancel today's
        # still-unopened order. Paper owns exit latches per acquisition cohort.
        new_reduced = new_quantity > inventory.cohorts[-1, :, CohortField.SHARES].abs()
        cancelled = cancelled | ((new_quantity > 0) & stopped) | new_reduced
        stopped = stopped & (inventory.shares != 0)
        continuation_target = torch.where(cancelled, inventory.shares, target)
        remaining_capacity = (observed_capacity - reduction.filled_shares).clamp_min(0)
        continuation = rebalance_inventory_at_open(inventory, weights=weights, entry_price=quote,
            entry_volume_shares=torch.where(solvent, remaining_capacity * 2, 0),
            frozen_target_shares=continuation_target, frozen_sizing_nav=sizing_nav, **common)
        inventory = continuation.state
        # A completed order does not reopen after a later bracket exit. This
        # also covers a carried target already fully held before today's open.
        cancelled = cancelled | (inventory.shares == target)
        notional = notional + ((continuation.reduction.filled_shares + continuation.addition_shares.abs())
                               * torch.nan_to_num(quote, nan=0)).sum()
    nav = inventory_nav(inventory, initial_capital=initial_capital, marks=mark)
    solvent = solvent & torch.isfinite(nav) & (nav > 0)
    return (*(getattr(inventory, name) for name in _INVENTORY_FIELDS),
            stopped, cancelled, solvent, nav, notional)


def _flat_sweep_step(state_values, action_values, minute_fields, common, *, initial_capital, entry_phase):
    return _frozen_target_minute(DayTradeInventoryState(**dict(zip(_INVENTORY_FIELDS, state_values))),
        *action_values, minute_fields=minute_fields, common=common,
        initial_capital=initial_capital, entry_phase=entry_phase)


def _sweep_step_function(weights: Tensor, inventory: DayTradeInventoryState, *, initial_capital: float, entry_phase: bool):
    def core(state_values, action_values, minute_fields, common):
        return _flat_sweep_step(state_values, action_values, minute_fields, common,
                               initial_capital=initial_capital, entry_phase=entry_phase)
    if not (_carry_path_compile_enabled(weights) and _env_truthy("STOCKAGENT_DAY_TRADE_SWEEP_COMPILE", "1")):
        return core
    # Corporate events append a claim row daily, even when the account owns
    # no affected stock. That logical row count is not a new minute algorithm.
    # Pad ONLY this compiler boundary; the returned ledger keeps its exact
    # original rows/dates. Powers of two bound graph variants without changing
    # claim settlement, masking a balance, or inventing paid cash.
    claim_rows = max(32, 1 << max(0, int(inventory.claims.shape[0] - 1).bit_length()))
    key = (weights.device.index, weights.dtype, torch.is_grad_enabled(), initial_capital, entry_phase,
           tuple(((claim_rows, *inventory.claims.shape[1:]) if name == "claims"
                  else tuple(getattr(inventory, name).shape), getattr(inventory, name).requires_grad)
                 for name in _INVENTORY_FIELDS))
    with _COMPILED_PATH_LOCK:
        if key not in _COMPILED_SWEEP_STEPS:
            isolated = _compile_isolated_code_object(core, name=f"fifo_sweep_step_{len(_COMPILED_SWEEP_STEPS)}")
            _COMPILED_SWEEP_STEPS[key] = torch.compile(isolated, fullgraph=True, dynamic=False,
                                                     options=_carry_compile_options())
            _CARRY_COMPILE_STATS["sweep_step_compile_constructors"] += 1
    compiled = _COMPILED_SWEEP_STEPS[key]
    def checked(state_values, action_values, minute_fields, common):
        claim_index = _INVENTORY_FIELDS.index("claims")
        claims = state_values[claim_index]
        logical_rows = int(claims.shape[0])
        padded_values = list(state_values)
        if logical_rows < claim_rows:
            padded_values[claim_index] = torch.cat((claims, claims.new_zeros(
                (claim_rows - logical_rows, *claims.shape[1:]))), dim=0)
        try:
            # Same narrow PyTorch tracing warning filter as the established
            # full-session compiler. No financial/data warning is suppressed.
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning,
                    message=r"The \.grad attribute of a Tensor that is not a leaf Tensor.*")
                result = compiled(tuple(padded_values), action_values, minute_fields, common)
        except Exception:
            _CARRY_COMPILE_STATS["compile_failures"] += 1
            raise
        _CARRY_COMPILE_STATS["compiled_sweep_step_calls"] += 1
        result = list(result)
        result[claim_index] = result[claim_index][:logical_rows]
        return tuple(result)
    return checked


def _checkpointed_frozen_target_minutes(funded, session, *, weights, **kwargs):
    # Flatten state tensors: a dataclass passed as a non-Tensor checkpoint
    # argument would hold its intermediate cohorts alive and defeat the bound.
    def core(*values):
        state = DayTradeInventoryState(**dict(zip(_INVENTORY_FIELDS, values[:-1])))
        before, after, marks, notional = _run_frozen_target_minutes(
            state, session, weights=values[-1], **kwargs)
        return (*(getattr(before, name) for name in _INVENTORY_FIELDS),
                *(getattr(after, name) for name in _INVENTORY_FIELDS), marks, notional)

    values = checkpoint(core, *(getattr(funded, name) for name in _INVENTORY_FIELDS), weights,
                        use_reentrant=False, preserve_rng_state=False)
    n = len(_INVENTORY_FIELDS)
    return (DayTradeInventoryState(**dict(zip(_INVENTORY_FIELDS, values[:n]))),
            DayTradeInventoryState(**dict(zip(_INVENTORY_FIELDS, values[n:2*n]))),
            values[-2], values[-1])


def _certified_sweep_exit_suffix(inventory, session, *, target, stopped, cancelled,
                                 start: int, initial_capital: float):
    """Reuse dense FIFO integration only if no remaining order can execute.

    The certificate changes neither source nor order semantics. Every one of
    the 270 marks remains present. A potentially executable target difference
    or an inconclusive solvency proof returns to the chronological oracle.
    """
    if not _env_truthy("STOCKAGENT_DAY_TRADE_SWEEP_SUFFIX_FASTPATH", "1"):
        return None
    from stockagent.backtest.tw_day_trade_minute import _capacity

    quotes, volumes = session.entry_path.unbind(-1)
    possible = (torch.isfinite(quotes[:, start:259])
                & (quotes[:, start:259] > 0)
                & (volumes[:, start:259] >= 2000)).any(-1)
    pending = ~cancelled & (inventory.shares != target)
    if bool((pending & possible).any().detach().cpu()):
        return None
    short = (inventory.shares < 0)[:, None]
    prices = torch.where(short, session.exit_prices[..., 1], session.exit_prices[..., 0])
    capacity = torch.where(short, session.exit_capacity[..., 1], session.exit_capacity[..., 0])
    minutes = torch.arange(270, device=quotes.device)[None, :]
    remaining = minutes >= start
    hits = torch.where(short, session.stop_hits[..., 1], session.stop_hits[..., 0]).bool()
    latched = ((hits & remaining).to(torch.int64).cumsum(-1) > 0) | stopped[:, None]
    quote_ok = torch.isfinite(quotes) & (quotes > 0)
    override = latched & (minutes < 259) & quote_ok
    prices = torch.where(override, quotes, prices)
    capacity = torch.where(override, _capacity(volumes), capacity)
    # Keep the existing fixed 270-column kernel ABI. Pre-suffix slots are
    # zero-capacity padding, never fills or published reconstructed marks.
    prices = torch.where(remaining, prices, float('nan'))
    capacity = torch.where(remaining, capacity, 0)
    marks = torch.where(remaining, session.marks, session.marks[:, start-1:start])
    path, nav = _run_inventory_path(inventory, prices=prices,
        capacity_shares=capacity, marks=marks, initial_capital=initial_capital)
    nav = nav[start:]
    if not bool((torch.isfinite(nav) & (nav > 0)).all().detach().cpu()):
        return None
    _CARRY_COMPILE_STATS['certified_sweep_suffix_calls'] += 1
    _CARRY_COMPILE_STATS['certified_sweep_minutes_avoided'] += 270 - start
    notional = (path.minute_filled_shares * torch.nan_to_num(prices, nan=0)).sum()
    return path.reduction.state, nav, notional


def _run_frozen_target_minutes(
    funded: DayTradeInventoryState, session: DayTradeCarrySession, *,
    weights: Tensor, can_enter: Tensor, buy_fee_rate: Tensor,
    day_sell_fee_rate: Tensor, normal_sell_fee_rate: Tensor, rebate_rate: Tensor,
    initial_capital: float,
) -> tuple[DayTradeInventoryState, DayTradeInventoryState, Tensor, Tensor]:
    """Chronological oracle for a single frozen daily order and its remainders.

    Brackets have priority, then old-inventory reductions, then new entries.
    All three consume ONE symbol/minute budget. Only the first call sizes the
    target; a later price or NAV cannot change the original order. The source
    records fresh stop touches; a stop cannot latch before a position exists.
    """
    assert session.entry_path is not None and session.stop_hits is not None
    trace = _env_truthy("STOCKAGENT_DAY_TRADE_SWEEP_TRACE", "0")
    fee_args = dict(buy_fee_rate=buy_fee_rate, day_sell_fee_rate=day_sell_fee_rate,
                    normal_sell_fee_rate=normal_sell_fee_rate, rebate_rate=rebate_rate)
    common = dict(official_open=session.official_open, opening_marks=session.opening_marks,
                  lower_limit=session.lower_limit, upper_limit=session.upper_limit,
                  can_enter=can_enter, initial_capital=initial_capital, day=session.day,
                  halted=session.halted, daily_proxy_mask=session.daily_proxy_mask,
                  state_already_advanced=True, **fee_args)
    opening = rebalance_inventory_at_open(
        funded, weights=weights, entry_price=session.entry_price,
        entry_volume_shares=session.entry_volume, **common)
    inventory = opening.state
    target = opening.target_shares
    sizing_nav = opening.sizing_nav
    notional = ((opening.reduction.filled_shares + opening.addition_shares.abs())
                * torch.nan_to_num(session.entry_price, nan=0)).sum()
    # Approved daily proxies have NO later-minute observations. Keep their
    # two-event algebra instead of scanning 258 empty continuation slots.
    # This branch depends only on the source, never future model outcomes.
    if not torch.compiler.is_compiling() and not bool(
        torch.isfinite(session.entry_path[..., 0]).any().detach().cpu()
    ):
        short = (inventory.shares < 0)[:, None]
        prices = torch.where(short, session.exit_prices[..., 1], session.exit_prices[..., 0])
        capacity = torch.where(short, session.exit_capacity[..., 1], session.exit_capacity[..., 0])
        path, marks = _run_inventory_path(inventory, prices=prices,
                                          capacity_shares=capacity, marks=session.marks,
                                          initial_capital=initial_capital)
        notional = notional + (path.minute_filled_shares * torch.nan_to_num(prices, nan=0)).sum()
        inventory = path.reduction.state
        return inventory, convert_inventory_to_margin(inventory, day=session.day), marks, notional
    marks = [inventory_nav(inventory, initial_capital=initial_capital,
                           marks=session.marks[:, 0])]
    stopped = torch.zeros_like(weights, dtype=torch.bool)
    cancelled = inventory.shares == target
    solvent = torch.isfinite(marks[0]) & (marks[0] > 0)
    # The date is data, not a graph specialization. Keep the validated ordinal
    # as an FP64 scalar, preserving every calendar check without 2,000 graphs.
    step_common = common | {"day": inventory.observed_day}
    for minute in range(1, 270):
        if trace and minute in {1, 129, 259}:
            print(f"[fifo sweep] day={session.day} minute={minute+1} "
                  f"cohorts={inventory.cohorts.shape[0]} claims={inventory.claims.shape[0]} "
                  f"step_graphs={len(_COMPILED_SWEEP_STEPS)}", flush=True)
        if minute in {1, 3, 9, 33, 65, 129, 259}:
            suffix = _certified_sweep_exit_suffix(inventory, session, target=target,
                stopped=stopped, cancelled=cancelled, start=minute,
                initial_capital=initial_capital)
            if suffix is not None:
                inventory, suffix_marks, suffix_notional = suffix
                return (inventory, convert_inventory_to_margin(inventory, day=session.day),
                        torch.cat((torch.stack(marks), suffix_marks)), notional + suffix_notional)
        function = _sweep_step_function(weights, inventory, initial_capital=initial_capital, entry_phase=minute < 259)
        minute_fields = (session.exit_prices[:, minute], session.exit_capacity[:, minute],
                         session.entry_path[:, minute], session.stop_hits[:, minute], session.marks[:, minute])
        # Bind source slices and the callable by value for backward replay.
        def step(*values, function=function, minute_fields=minute_fields):
            n = len(_INVENTORY_FIELDS)
            return function(values[:n], values[n:], minute_fields, step_common)
        inputs = (*(getattr(inventory, name) for name in _INVENTORY_FIELDS),
                  weights, target, sizing_nav, stopped, cancelled, solvent)
        values = (checkpoint(step, *inputs, use_reentrant=False, preserve_rng_state=False)
                  if _sweep_checkpoint_enabled(weights) else step(*inputs))
        inventory = DayTradeInventoryState(**dict(zip(_INVENTORY_FIELDS, values[:len(_INVENTORY_FIELDS)])))
        stopped, cancelled, solvent, nav, traded = values[-5:]
        notional = notional + traded
        marks.append(nav)
    return inventory, convert_inventory_to_margin(inventory, day=session.day), torch.stack(marks), notional


def _failed_session_cpu_diagnostic(state, session, **kwargs) -> str:
    """Failure-only, one-session CPU oracle; never alter the accepted ledger.

    CUDA validity flags deliberately avoid poisoning DDP with device asserts.
    Replaying only the first rejected session exposes the actual failed
    invariant, instead of blaming source gaps when all source checks passed.
    """
    cpu_session = replace(session, **{
        field.name: value.detach().cpu()
        for field in fields(session)
        if isinstance(value := getattr(session, field.name), Tensor)
    })
    cpu_args = {key: value.detach().cpu() if isinstance(value, Tensor) else value
                for key, value in kwargs.items()}
    try:
        with torch.no_grad():
            checked, _, _ = execute_carry_session(
                state.detached(device="cpu"), cpu_session,
                event_compression=False, **cpu_args)
        if bool(checked.inventory.failed):
            return "cpu_oracle_returned_failed_inventory"
        return "cpu_oracle_passed_rejected_gpu_session"
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


def execute_carry_session(
    state: DayTradeCarryState, session: DayTradeCarrySession, *, weights: Tensor,
    can_enter: Tensor, buy_fee_rate: Tensor, day_sell_fee_rate: Tensor,
    normal_sell_fee_rate: Tensor, rebate_rate: Tensor, initial_capital: float,
    event_compression: bool = False,
    require_minute_nav_grad: bool = True,
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
    if session.entry_path is not None:
        rematerialize = _sweep_checkpoint_enabled(target_weights)
        _CARRY_COMPILE_STATS["checkpointed_sweep_session_calls" if rematerialize
                            else "eager_sweep_session_calls"] += 1
        sweep = _checkpointed_frozen_target_minutes if rematerialize else _run_frozen_target_minutes
        path_inventory, converted, marks, notional = sweep(
            funded, session, weights=target_weights, can_enter=can_enter,
            buy_fee_rate=buy_fee_rate, day_sell_fee_rate=day_sell_fee_rate,
            normal_sell_fee_rate=normal_sell_fee_rate, rebate_rate=rebate_rate,
            initial_capital=initial_capital)
        if session.terminal_liquidation_price is not None:
            # Only the terminal reducer is unbounded. Run it against the
            # pre-conversion FIFO state, so today's shorts never pay an
            # overnight conversion charge merely because earlier exits lacked
            # volume. The canonical final NAV below replaces only mark 270.
            path_inventory, terminal_notional = (
                _liquidate_terminal_inventory_without_capacity(
                    path_inventory, terminal_price=session.terminal_liquidation_price,
                )
            )
            converted = convert_inventory_to_margin(path_inventory, day=session.day)
            notional = notional + terminal_notional
    elif _carry_full_session_compile_enabled(funded.cohorts):
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
            require_minute_nav_grad=require_minute_nav_grad,
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
                    require_minute_nav_grad=require_minute_nav_grad,
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
                require_minute_nav_grad=require_minute_nav_grad,
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
                    require_minute_nav_grad=require_minute_nav_grad,
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
    if not require_minute_nav_grad:
        marks = marks.detach()
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
    # Source failure is atomic across the account, including malformed action
    # terms on CUDA. Do not leave successful peers half-advanced on retry.
    final_inventory = _commit_carry_inventory(final_inventory, working, inventory,
        trade_alive=trade_alive, accept=state.alive & valid)
    final_inventory = replace(final_inventory,
        failed=torch.maximum(final_inventory.failed, (~valid).to(torch.float64)))
    return (DayTradeCarryState(final_inventory, committed_marks[-1],
            torch.where(valid, final_alive, state.alive), initial_capital, session.day),
            committed_marks, torch.where(trade_alive & valid, notional, 0))


def _run_day_trade_carry_sessions_core(
    weights: Tensor, sessions: tuple[DayTradeCarrySession, ...], *, can_enter: Tensor,
    buy_fee_rate: Tensor, day_sell_fee_rate: Tensor, normal_sell_fee_rate: Tensor,
    rebate_rate: Tensor, initial_capital: float, initial_state: DayTradeCarryState | None = None,
    event_compression: bool = False,
    require_minute_nav_grad: bool = True,
    _fixed_cohort_rows: int | None = None,
    _compact_initial_state: bool = True,
    _flat_terminal_proofs: list[Tensor] | None = None,
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
    if _compact_initial_state:
        state = _compact_detached_carry_state(state)
    flat_terminal = _flat_terminal_proofs is not None
    if flat_terminal and (state.inventory.cohorts.numel() or state.inventory.claims.numel()):
        raise ValueError("flat-terminal candidate requires an empty incoming physical ledger")
    inert_history = []
    compile_paths = _carry_path_compile_enabled(weights)
    if compile_paths:
        # Every session appends exactly one acquisition row. Temporarily pad
        # each input to K-1 so every FIFO core in this truncated-BPTT call sees
        # the same power-of-two K; remove only those inert middle rows after it.
        maximum_rows = int(state.inventory.cohorts.shape[0]) + len(sessions)
        path_cohort_rows = (_fixed_cohort_rows if _fixed_cohort_rows is not None
                            else 1 << max(0, int(maximum_rows - 1).bit_length()))
        if path_cohort_rows < maximum_rows:
            raise ValueError("fixed checkpoint cohort axis cannot truncate inventory")
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
                state, target_rows=0 if flat_terminal else pre_session_rows
            )
        else:
            logical_rows = -1
        prior_nav, prior_alive, prior_inventory = (
            state.last_nav,
            state.alive,
            state.inventory,
        )
        prior_state = state
        state, curve, notional = execute_carry_session(state, session,
            weights=weights[i], can_enter=can_enter[i], buy_fee_rate=buy_fee_rate,
            day_sell_fee_rate=day_sell_fee_rate, normal_sell_fee_rate=normal_sell_fee_rate,
            rebate_rate=rebate_rate, initial_capital=initial_capital,
            event_compression=event_compression,
            require_minute_nav_grad=require_minute_nav_grad,
        )
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
            diagnosis = _failed_session_cpu_diagnostic(prior_state, session,
                weights=weights[i], can_enter=can_enter[i], buy_fee_rate=buy_fee_rate,
                day_sell_fee_rate=day_sell_fee_rate, normal_sell_fee_rate=normal_sell_fee_rate,
                rebate_rate=rebate_rate, initial_capital=initial_capital)
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
                f"action_count={len(action_indices)} invariant_diagnosis={diagnosis}"
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
        if flat_terminal:
            cohorts = state.inventory.cohorts
            # Terminal unlimited closes make these three quantities constant
            # zero with zero action Jacobian. Keep realized PnL/fees/claims/NAV
            # connected; this is not a new truncation or a daily capital reset.
            proof = ((cohorts[..., CohortField.SHARES] == 0).all()
                & (cohorts[..., CohortField.ENTRY_COST] == 0).all()
                & (cohorts[..., CohortField.LOCKED_SHARES] == 0).all()
                & (state.inventory.claims[..., 0] == 0).all()
                & (state.inventory.failed == 0) & state.alive)
            _flat_terminal_proofs.append(proof)
            inert_history.append(cohorts)
            state = replace(state, inventory=replace(state.inventory,
                cohorts=cohorts.new_empty((0, cohorts.shape[1], cohorts.shape[2]))))
            _CARRY_COMPILE_STATS["flat_terminal_cleared_sessions"] += 1
    if flat_terminal:
        # Retain the complete public/checkpoint representation, including
        # inactive acquisition dates. These rows are immutable after a proved
        # terminal close; only their needless re-execution was removed. The
        # user-approved compact FP64 sums may differ from zero-padded sums by
        # roundoff; never restore O(B**2*S) padding merely to match those bits.
        state = replace(state, inventory=replace(state.inventory,
            cohorts=torch.cat(inert_history, dim=0)))
    result = DayTradeCarryResult(torch.stack(returns), torch.stack(turns), torch.stack(exposure),
        torch.stack(holdings), torch.stack(curves), torch.stack(defaults), state)
    return result




_CARRY_RESULT_FIELDS = ("strategy_returns", "turnovers", "weights_history",
    "shares_history", "minute_nav", "settlement_default")


def _carry_checkpoint_block_rows(weights: Tensor) -> int:
    if not torch.is_grad_enabled() or not weights.requires_grad:
        return 0
    raw = os.environ.get("STOCKAGENT_DAY_TRADE_CARRY_CHECKPOINT_BLOCK_ROWS", "0")
    try:
        rows = int(raw)
    except ValueError as exc:
        raise ValueError("carry checkpoint block rows must be zero or a power of two") from exc
    if rows < 0 or (rows and rows & (rows - 1)):
        raise ValueError("carry checkpoint block rows must be zero or a power of two")
    return rows


def _run_checkpointed_carry_sessions(
    weights: Tensor, sessions: tuple[DayTradeCarrySession, ...], *, can_enter: Tensor,
    buy_fee_rate: Tensor, day_sell_fee_rate: Tensor, normal_sell_fee_rate: Tensor,
    rebate_rate: Tensor, initial_capital: float, initial_state: DayTradeCarryState | None = None,
    event_compression: bool = False,
    require_minute_nav_grad: bool = True,
    block_rows: int,
) -> DayTradeCarryResult:
    """Canonical FIFO recurrence with optional exact block rematerialization.

    The runtime-only block size is NOT a BPTT boundary: all input state/action
    tensors remain connected. Only the original public batch boundary detaches
    state. Keep the same whole-batch cohort axis and one solvency certificate,
    including complete fallback replay, so financial math/reduction order and
    default/claim ownership remain identical. Evaluation retains the ordinary
    path and all requested minute/artifact histories.
    """
    if weights.ndim != 2 or weights.shape[0] != len(sessions) or not sessions:
        raise ValueError("carry sessions must match a nonempty [T,S] action trajectory")
    if can_enter.shape != weights.shape:
        raise ValueError("carry entry permissions must match the action trajectory")
    if any(a.day >= b.day for a, b in zip(sessions, sessions[1:])):
        raise ValueError("carry sessions must be strictly chronological")
    state = initial_state or DayTradeCarryState.empty(weights.shape[1], initial_capital, weights.device)
    state.validate(symbols=weights.shape[1], device=weights.device, initial_capital=initial_capital)
    state = _compact_detached_carry_state(state)
    maximum_rows = int(state.inventory.cohorts.shape[0]) + len(sessions)
    fixed_rows = (1 << max(0, int(maximum_rows - 1).bit_length())
                  if _carry_path_compile_enabled(weights) else None)
    histories = {name: [] for name in _CARRY_RESULT_FIELDS}
    n = len(_INVENTORY_FIELDS)
    for begin in range(0, len(sessions), block_rows):
        end = min(begin + block_rows, len(sessions))
        # Bind every block's calendar/source metadata. Never capture the mutable
        # outer state or last loop slice for a delayed backward recomputation.
        def core(*values, block_sessions=sessions[begin:end],
                 block_permissions=can_enter[begin:end], prior_day=state.last_session_day):
            inventory = DayTradeInventoryState(**dict(zip(_INVENTORY_FIELDS, values[:n])))
            incoming = DayTradeCarryState(inventory, values[n], values[n+1], initial_capital, prior_day)
            result = _run_day_trade_carry_sessions_core(values[n+2], block_sessions,
                can_enter=block_permissions, buy_fee_rate=buy_fee_rate,
                day_sell_fee_rate=day_sell_fee_rate, normal_sell_fee_rate=normal_sell_fee_rate,
                rebate_rate=rebate_rate, initial_capital=initial_capital, initial_state=incoming,
                event_compression=event_compression, _fixed_cohort_rows=fixed_rows,
                require_minute_nav_grad=require_minute_nav_grad,
                _compact_initial_state=False)
            final = result.final_state
            return (*(getattr(final.inventory, name) for name in _INVENTORY_FIELDS),
                final.last_nav, final.alive,
                *(getattr(result, name) for name in _CARRY_RESULT_FIELDS))
        inputs = (*(getattr(state.inventory, name) for name in _INVENTORY_FIELDS),
                  state.last_nav, state.alive, weights[begin:end])
        values = checkpoint(core, *inputs, use_reentrant=False, preserve_rng_state=False)
        _CARRY_COMPILE_STATS["checkpointed_trajectory_blocks"] += 1
        state = DayTradeCarryState(
            DayTradeInventoryState(**dict(zip(_INVENTORY_FIELDS, values[:n]))),
            values[n], values[n+1], initial_capital, sessions[end-1].day)
        for name, value in zip(_CARRY_RESULT_FIELDS, values[n+2:]):
            histories[name].append(value)
    result = DayTradeCarryResult(*(torch.cat(histories[name], dim=0)
        for name in _CARRY_RESULT_FIELDS), state)
    return result


def run_day_trade_carry_sessions(
    weights: Tensor, sessions: tuple[DayTradeCarrySession, ...], *, can_enter: Tensor,
    buy_fee_rate: Tensor, day_sell_fee_rate: Tensor, normal_sell_fee_rate: Tensor,
    rebate_rate: Tensor, initial_capital: float, initial_state: DayTradeCarryState | None = None,
    event_compression: bool = False,
    require_minute_nav_grad: bool = True,
    _allow_flat_terminal: bool = True,
) -> DayTradeCarryResult:
    """Exact physical FIFO; block rematerialization never detaches state."""
    common = dict(can_enter=can_enter, buy_fee_rate=buy_fee_rate,
        day_sell_fee_rate=day_sell_fee_rate, normal_sell_fee_rate=normal_sell_fee_rate,
        rebate_rate=rebate_rate, initial_capital=initial_capital,
        initial_state=initial_state, event_compression=event_compression,
        require_minute_nav_grad=require_minute_nav_grad)
    block_rows = _carry_checkpoint_block_rows(weights)
    flat_candidate = (_allow_flat_terminal and _env_truthy("STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL", "0")
        and torch.is_grad_enabled() and weights.requires_grad and weights.ndim == 2
        and bool(sessions) and not require_minute_nav_grad
        and all(s.terminal_liquidation_price is not None and s.entry_path is None
                and not s.uses_sparse_events for s in sessions))
    if flat_candidate:
        incoming = initial_state or DayTradeCarryState.empty(weights.shape[1], initial_capital, weights.device)
        incoming = _compact_detached_carry_state(incoming)
        flat_candidate = not (incoming.inventory.cohorts.numel() or incoming.inventory.claims.numel())
    if flat_candidate:
        proofs: list[Tensor] = []
        result = _run_day_trade_carry_sessions_core(weights, sessions,
            **{**common, "initial_state": incoming}, _flat_terminal_proofs=proofs)
        certified = (torch.stack(proofs).all() & torch.isfinite(result.minute_nav).all()
                     & (result.minute_nav > 0).all())
        if bool(certified.detach().cpu()):
            _CARRY_COMPILE_STATS["flat_terminal_batches"] += 1
            return result
        # One batch certificate, not one synchronizing host check per day.
        # Replay the canonical full ledger on any uncertain/source/default
        # case, before accepting outputs or performing an optimizer update.
        del result
        _CARRY_COMPILE_STATS["flat_terminal_fallback_batches"] += 1
    if block_rows and len(sessions) > block_rows:
        result = _run_checkpointed_carry_sessions(weights, sessions, **common, block_rows=block_rows)
    else:
        result = _run_day_trade_carry_sessions_core(weights, sessions, **common)
    if not event_compression:
        return result
    # Exactly one certificate per public BPTT batch. Both execution paths have
    # returned: releasing a provisional result before fallback frees its graph,
    # not just one alias while another helper frame still owns every history.
    certified = torch.isfinite(result.minute_nav).all() & (result.minute_nav > 0).all()
    if not bool(certified.detach().cpu()):
        del result
        _CARRY_COMPILE_STATS["event_compression_fallback_batches"] += 1
        if sessions[0].uses_sparse_events:
            raise DayTradeCarryEventCompressionFallback(
                "sparse event certificate was inconclusive; dense minute replay required")
        fallback = run_day_trade_carry_sessions(weights, sessions,
            **{**common, "event_compression": False}, _allow_flat_terminal=False)
        return replace(fallback, minute_nav=torch.stack(
            (fallback.minute_nav.amin(dim=-1), fallback.minute_nav[:, -1]), dim=-1))
    if result.minute_nav.shape[-1] != 2:
        result = replace(result, minute_nav=torch.stack(
            (result.minute_nav.amin(dim=-1), result.minute_nav[:, -1]), dim=-1))
    return result
