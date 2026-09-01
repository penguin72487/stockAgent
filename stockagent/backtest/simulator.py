from __future__ import annotations

import logging
import math
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import torch

from stockagent.backtest.distributed_reduction import global_symbol_tensor_sum
from stockagent.backtest.crypto_perpetual import run_crypto_perpetual_torch
from stockagent.backtest.tw_continuous import (
    run_tw_cash_continuous,
    run_tw_day_trade_continuous,
)
from stockagent.backtest.tw_day_trade_minute import (
    run_tw_day_trade_minute_execution,
)
from stockagent.backtest.tw_commission_rebate import (
    commission_rebate_calendar,
    normalize_commission_rebate_timing,
)
from stockagent.backtest.tw_dual_session import (
    run_tw_cash_dual_session,
    run_tw_overnight_dual_session,
)
from stockagent.backtest.tw_dual_session_compiled import (
    run_tw_cash_dual_session_compiled,
    run_tw_overnight_dual_session_compiled,
)
from stockagent.backtest.tw_dual_session_integer import (
    TaiwanDualSessionIntegerBacktestResult,
    run_tw_cash_dual_session_integer,
    run_tw_overnight_dual_session_integer,
)
from stockagent.backtest.tw_execution import (
    TW_CARRYING_EXECUTION_MODES,
    TW_STOCK_FUTURES_DAY_TRADE_EXECUTION_MODES,
    TW_STOCK_FUTURES_INTEGER_DAY_TRADE_EXECUTION_MODES,
    normalize_execution_mode,
)
from stockagent.backtest.tw_futures_portfolio import (
    run_tw_futures_portfolio_continuous_numpy,
    run_tw_futures_portfolio_continuous_torch,
    run_tw_futures_portfolio_integer_surrogate_torch,
    run_tw_futures_portfolio_integer_torch,
)
from stockagent.backtest.tw_stock_futures_day_trade import (
    run_tw_stock_futures_day_trade_continuous_torch,
    run_tw_stock_futures_day_trade_integer_torch,
)
from stockagent.backtest.tw_integer_execution import (
    TaiwanIntegerBacktestResult,
    TaiwanIntegerState,
    run_tw_cash_integer,
    run_tw_day_trade_integer,
)
from stockagent.backtest.tw_index_futures import (
    FuturesCostSchedule,
    run_tw_index_futures_all_tenors_day_torch,
    run_tw_index_futures_day_continuous,
    run_tw_index_futures_day_integer,
)
from stockagent.backtest.tw_index_derivatives_day import (
    OptionDayCostSchedule,
    run_tw_index_derivatives_day_continuous,
    run_tw_index_derivatives_day_integer,
)
from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_ACTION_COUNT,
    TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
    TAIFEX_INDEX_FUTURES_PRODUCTS,
    TaiwanIndexFuturesDaySession,
)
from stockagent.data.tw_index_derivatives_day import (
    TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
    TaiwanIndexDerivativeDayCandidates,
)
from stockagent.data.tw_index_options_daily import (
    TaiwanIndexOptionChainDaySession,
)
from stockagent.models.normalization import (
    DEFAULT_PORTFOLIO_ACTIVATION,
    PORTFOLIO_L1_EPS,
    apply_portfolio_activation,
    masked_activation_l1_weights,
    normalize_portfolio_activation,
)
from stockagent.runtime_env import normalize_cuda_env
from torch.utils.checkpoint import checkpoint as checkpoint_fn

try:
    from stockagent.data import panel_numba as _panel_numba
except Exception:  # pragma: no cover - Numba is an acceleration dependency
    _panel_numba = None

INT64_MIN_FLOAT_SAFE = np.nextafter(float(np.iinfo(np.int64).min), 0.0)
INT64_MAX_FLOAT_SAFE = np.nextafter(float(np.iinfo(np.int64).max + 1), 0.0)
SCAN_CHUNK_CANDIDATES = (64, 128, 256, 512)
# v20 adds the optional crypto carrying-account proximal allocation contract:
# the previous executed portfolio and the actual asymmetric one-way fees derive
# the recurrent no-trade region before exchange permissions/capacity execute.
# v17 applies the same T+2-close net-claim ledger to the daily open-to-close
# day-trade executor.  Daily sizing now uses deployable cash rather than
# economic NAV, so T claims affect NAV immediately but first fund T+3 orders.
# v16 restores exact-minute day-trade T+2 semantics: securities offset on T,
# while only the signed net cash difference enters the receivable/payable
# queue and settles two trading sessions later. Economic NAV recognizes the
# claim on T; cash posts after T+2 orders, so new sizing first uses it on T+3.
# v15 adds the 2014+ daily no-settlement day-trade contract: blocked closing
# legs receive unlimited margin conversion with explicit costs, all sessions
# accounting-close flat, and no T+2/default recurrence.
# v19 removes portfolio-level long/short fill balancing.  After each symbol is
# independently constrained by permissions, capacity, turnover, and whole lots,
# its remaining executable quantity is retained without cross-symbol reduction.
# v20 makes unlimited day-trade conversion stateful and causal: a blocked close
# leaves signed recurrent inventory, the next OPEN trades only the target delta
# after the overnight mark, and unclosed long purchases create an explicit
# broker-financed principal liability rather than a fictitious all-cash T+2
# payable. Close masks and same-day full volume remain executor-only outcomes.
# v11 added the former direction-balancing behavior.  v10 separated gross
# commission collection from the economically earned
# broker rebate and added recurrent pending-rebate state.
CANONICAL_BACKTEST_CONTRACT_VERSION = 21

_SCAN_CHUNK_CACHE: dict[tuple, int] = {}
_SCAN_COMPILED_CACHE: dict[
    tuple,
    Callable[
        ...,
        tuple[
            torch.Tensor,
            torch.Tensor,
            torch.Tensor,
            torch.Tensor,
            torch.Tensor,
            torch.Tensor,
            torch.Tensor,
        ],
    ],
] = {}
_PREP_COMPILED_CACHE: dict[
    tuple, Callable[..., tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]]
] = {}
_FUTURES_PORTFOLIO_COMPILED_CACHE: dict[
    tuple, Callable[..., tuple[torch.Tensor, ...]]
] = {}
_SCAN_COMPILE_FAILED: set[tuple] = set()
_PREP_COMPILE_FAILED: set[tuple] = set()
_FUTURES_PORTFOLIO_COMPILE_FAILED: set[tuple] = set()
_SCAN_COMPILE_STATS: dict[str, int] = {
    "hits": 0,
    "misses": 0,
    "failures": 0,
    "disabled": 0,
}
_PREP_COMPILE_STATS: dict[str, int] = {
    "hits": 0,
    "misses": 0,
    "failures": 0,
    "disabled": 0,
}
_BACKTEST_RUNTIME_STATS: dict[str, float] = {
    "calls": 0.0,
    "total_s": 0.0,
    "total_cuda_s": 0.0,
    "prep_s": 0.0,
    "prep_cuda_s": 0.0,
    "prev_init_s": 0.0,
    "prev_init_cuda_s": 0.0,
    "runner_resolve_s": 0.0,
    "runner_call_s": 0.0,
    "runner_call_cuda_s": 0.0,
    "runtime_fallback_s": 0.0,
    "checkpoint_s": 0.0,
    "checkpoint_cuda_s": 0.0,
    "finalize_s": 0.0,
    "finalize_cuda_s": 0.0,
    "checkpoint_calls": 0.0,
    "compiled_prep_calls": 0.0,
    "eager_prep_calls": 0.0,
    "compiled_runner_calls": 0.0,
    "eager_runner_calls": 0.0,
    "stateful_calls": 0.0,
    "stateful_compiled_runner_calls": 0.0,
    "stateful_eager_runner_calls": 0.0,
    "nonstateful_compiled_runner_calls": 0.0,
    "nonstateful_eager_runner_calls": 0.0,
    "runtime_fallback_calls": 0.0,
    "return_weights_history_calls": 0.0,
}
_BACKTEST_PENDING_CUDA_EVENTS: list[tuple[str, torch.cuda.Event, torch.cuda.Event]] = []


def _round_half_up(values: np.ndarray | float, decimals: int = 2) -> np.ndarray:
    """Round with half-up semantics (0.5 always rounds away from zero)."""
    if _panel_numba is not None:
        return _panel_numba.round_half_up(values, decimals=decimals)
    arr = np.asarray(values, dtype=np.float64)
    factor = float(10**decimals)
    out = np.full(arr.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(arr)
    pos = valid & (arr >= 0.0)
    neg = valid & (arr < 0.0)
    out[pos] = np.floor(arr[pos] * factor + 0.5) / factor
    out[neg] = np.ceil(arr[neg] * factor - 0.5) / factor
    return out


def _is_tw_symbol(symbol: str) -> bool:
    symbol = str(symbol).strip()
    return bool(symbol) and symbol[0].isdigit()


def _clip_to_int64_storage_bounds(
    values: np.ndarray | float, *, non_negative: bool = False
) -> np.ndarray:
    """Clip numeric values to safe float bounds that can be cast to int64."""
    arr = np.nan_to_num(
        np.asarray(values, dtype=np.float64),
        nan=0.0,
        posinf=INT64_MAX_FLOAT_SAFE,
        neginf=0.0 if non_negative else INT64_MIN_FLOAT_SAFE,
    )
    lower = 0.0 if non_negative else INT64_MIN_FLOAT_SAFE
    return np.clip(arr, lower, INT64_MAX_FLOAT_SAFE)


def _floor_to_int64(
    values: np.ndarray | float, *, non_negative: bool = False
) -> np.ndarray:
    """Floor and cast to int64 after clipping strictly to int64 storage bounds."""
    clipped = _clip_to_int64_storage_bounds(values, non_negative=non_negative)
    return np.floor(clipped).astype(np.int64)


def _trunc_to_int64(values: np.ndarray | float) -> np.ndarray:
    """Truncate toward zero and cast to int64 within safe storage bounds."""
    clipped = _clip_to_int64_storage_bounds(values, non_negative=False)
    return np.trunc(clipped).astype(np.int64)


def _resolve_exposure_budget(gross_leverage: float) -> float:
    """Return the canonical unlevered absolute exposure budget in [0, 1]."""
    value = float(gross_leverage)
    if not math.isfinite(value):
        return 0.0
    return min(1.0, max(0.0, value))


_MIN_PORTFOLIO_SIMPLE_RETURN = -0.999999
_MIN_PORTFOLIO_WEALTH_FACTOR = 1.0 + _MIN_PORTFOLIO_SIMPLE_RETURN


def _asset_log_returns_to_simple_numpy(asset_log_returns: np.ndarray) -> np.ndarray:
    clean = np.nan_to_num(
        np.asarray(asset_log_returns, dtype=np.float32),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    return np.expm1(clean).astype(np.float32, copy=False)


def _portfolio_simple_returns_to_log_numpy(simple_returns: np.ndarray) -> np.ndarray:
    clean = np.nan_to_num(
        np.asarray(simple_returns, dtype=np.float32),
        nan=0.0,
        posinf=np.finfo(np.float32).max,
        neginf=_MIN_PORTFOLIO_SIMPLE_RETURN,
    )
    clipped = np.clip(clean, _MIN_PORTFOLIO_SIMPLE_RETURN, None)
    return np.log1p(clipped).astype(np.float32, copy=False)


def _asset_log_returns_to_simple_torch(
    asset_log_returns: torch.Tensor,
    *,
    device: torch.device | None = None,
    dtype: torch.dtype | None = None,
) -> torch.Tensor:
    clean = torch.nan_to_num(
        asset_log_returns.to(
            device=device if device is not None else asset_log_returns.device,
            dtype=dtype if dtype is not None else asset_log_returns.dtype,
        ),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    return torch.expm1(clean)


def _portfolio_simple_returns_to_log_torch(
    simple_returns: torch.Tensor,
) -> torch.Tensor:
    floor = torch.as_tensor(
        _MIN_PORTFOLIO_SIMPLE_RETURN,
        device=simple_returns.device,
        dtype=simple_returns.dtype,
    )
    clean = torch.nan_to_num(
        simple_returns,
        nan=0.0,
        posinf=torch.finfo(simple_returns.dtype).max,
        neginf=_MIN_PORTFOLIO_SIMPLE_RETURN,
    )
    return torch.log1p(clean.clamp_min(floor))


def _drift_weights_after_return_numpy(
    executed_weights: np.ndarray,
    asset_simple_returns: np.ndarray,
    strategy_net_simple_return: float | np.floating,
) -> tuple[np.ndarray, bool]:
    """Mark executed holdings to the next period's net-equity denominator.

    Fees are paid from cash/equity, so they reduce the denominator while the
    asset notionals remain ``w * (1 + r)``.  A non-finite or bankrupt wealth
    factor has no meaningful normalized holdings state; reset it to all cash
    (zero risky weights) rather than propagating infinities into later rows.
    """
    wealth_factor = 1.0 + float(strategy_net_simple_return)
    if (
        not math.isfinite(wealth_factor)
        or wealth_factor <= _MIN_PORTFOLIO_WEALTH_FACTOR
    ):
        return np.zeros_like(executed_weights, dtype=np.float32), False
    drifted = (
        np.asarray(executed_weights, dtype=np.float32)
        * (np.float32(1.0) + np.asarray(asset_simple_returns, dtype=np.float32))
        / np.float32(wealth_factor)
    )
    drifted = np.nan_to_num(
        drifted,
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    ).astype(np.float32, copy=False)
    return drifted, bool(np.isfinite(drifted).all())


def _drift_weights_after_return_torch(
    executed_weights: torch.Tensor,
    asset_simple_returns: torch.Tensor,
    strategy_net_simple_return: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Torch counterpart of :func:`_drift_weights_after_return_numpy`."""
    wealth_factor = strategy_net_simple_return + 1.0
    valid = torch.isfinite(wealth_factor) & (
        wealth_factor > float(_MIN_PORTFOLIO_WEALTH_FACTOR)
    )
    safe_denominator = torch.where(
        valid,
        wealth_factor,
        torch.ones_like(wealth_factor),
    )
    drifted = executed_weights * (1.0 + asset_simple_returns) / safe_denominator
    drifted = torch.nan_to_num(drifted, nan=0.0, posinf=0.0, neginf=0.0)
    drifted = torch.where(valid, drifted, torch.zeros_like(drifted))
    survived = valid & torch.isfinite(drifted).all()
    return drifted, survived


def _erf_numpy(values: np.ndarray) -> np.ndarray:
    try:
        from scipy import special as scipy_special  # type: ignore

        return scipy_special.erf(values)
    except Exception:
        return np.vectorize(math.erf, otypes=[values.dtype])(values)


def _apply_portfolio_activation_numpy(
    values: np.ndarray,
    dtype: np.dtype = np.dtype(np.float32),
    activation: str | None = None,
) -> np.ndarray:
    activation_name = normalize_portfolio_activation(activation)
    out = values.astype(dtype, copy=False)
    if activation_name in {"identity", "pre_normalized"}:
        return np.where(np.isfinite(out), out, dtype.type(0.0)).astype(
            dtype, copy=False
        )
    out = np.nan_to_num(out, nan=0.0, posinf=20.0, neginf=-20.0)
    out = np.clip(out, dtype.type(-20.0), dtype.type(20.0))
    if activation_name == "tanh":
        return np.tanh(out).astype(dtype, copy=False)
    if activation_name == "softsign":
        return out / (dtype.type(1.0) + np.abs(out))
    if activation_name == "isru":
        return out / np.sqrt(dtype.type(1.0) + np.square(out))
    if activation_name == "erf":
        return _erf_numpy(out * dtype.type(math.sqrt(math.pi) / 2.0)).astype(
            dtype, copy=False
        )
    if activation_name == "atan":
        return (
            dtype.type(2.0 / math.pi) * np.arctan(out * dtype.type(math.pi / 2.0))
        ).astype(dtype, copy=False)
    if activation_name == "gudermannian":
        return (
            dtype.type(2.0 / math.pi)
            * np.arctan(np.sinh(out * dtype.type(math.pi / 2.0)))
        ).astype(dtype, copy=False)
    raise AssertionError(f"Unhandled portfolio activation: {activation_name}")


def _normalize_target_weights_numpy(
    weights: np.ndarray,
    *,
    long_only: bool,
    gross_budget: float,
    portfolio_activation: str | None = None,
) -> np.ndarray:
    """Normalize target weights via bounded activation + L1 and apply gross budget."""
    activation_name = normalize_portfolio_activation(portfolio_activation)
    out = _apply_portfolio_activation_numpy(
        weights, np.dtype(np.float32), activation_name
    )
    if long_only:
        out = np.clip(out, 0.0, None)
    if activation_name == "pre_normalized":
        l1 = np.abs(out).sum(axis=1, keepdims=True).astype(np.float32)
        gross = np.float32(gross_budget)
        scale = np.ones_like(l1, dtype=np.float32)
        np.divide(
            gross,
            np.clip(l1, PORTFOLIO_L1_EPS, None),
            out=scale,
            where=l1 > gross,
        )
        return (out * scale).astype(np.float32, copy=False)
    l1 = np.abs(out).sum(axis=1, keepdims=True).astype(np.float32)
    out = np.where(
        l1 > 0.0,
        out / np.clip(l1, PORTFOLIO_L1_EPS, None),
        np.zeros_like(out),
    )
    out *= np.float32(gross_budget)
    return out.astype(np.float32, copy=False)


def _normalize_target_weights_row_numpy(
    weights_row: np.ndarray,
    *,
    long_only: bool,
    gross_budget: float,
    portfolio_activation: str | None = None,
) -> np.ndarray:
    """Single-row variant of bounded activation + L1 normalization for integer-share path."""
    activation_name = normalize_portfolio_activation(portfolio_activation)
    row = _apply_portfolio_activation_numpy(
        weights_row, np.dtype(np.float64), activation_name
    )
    if long_only:
        row = np.clip(row, 0.0, None)
    l1 = float(np.abs(row).sum(dtype=np.float64))
    if activation_name == "pre_normalized":
        gross = float(gross_budget)
        if l1 > gross and l1 > PORTFOLIO_L1_EPS:
            row = row * (gross / l1)
        return row
    if l1 > 0.0:
        row = row / max(l1, PORTFOLIO_L1_EPS)
    else:
        row = np.zeros_like(row, dtype=np.float64)
    row *= float(gross_budget)
    return row


def _normalize_target_weights_torch(
    weights: torch.Tensor,
    *,
    long_only: bool,
    gross_budget: float,
    portfolio_activation: str | None = None,
    symbol_sharded: bool = False,
) -> torch.Tensor:
    """Torch normalization via bounded activation + L1 and gross budget scaling."""
    activation_name = normalize_portfolio_activation(portfolio_activation)
    if activation_name == "pre_normalized":
        out = apply_portfolio_activation(weights, activation_name)
        if long_only:
            out = out.clamp_min(0.0)
        leverage = torch.as_tensor(
            gross_budget,
            device=out.device,
            dtype=out.dtype,
        )
        l1 = global_symbol_tensor_sum(
            out.abs().sum(dim=1, keepdim=True),
            symbol_sharded=symbol_sharded,
        )
        scale = torch.where(
            l1 > leverage,
            leverage / l1.clamp_min(PORTFOLIO_L1_EPS),
            torch.ones_like(l1),
        )
        return out * scale
    out = apply_portfolio_activation(weights, activation_name)
    if long_only:
        out = out.clamp_min(0.0)
    l1 = global_symbol_tensor_sum(
        out.abs().sum(dim=1, keepdim=True),
        symbol_sharded=symbol_sharded,
    )
    normalized = torch.where(
        l1 > 0.0,
        out / l1.clamp_min(PORTFOLIO_L1_EPS),
        torch.zeros_like(out),
    )
    leverage = torch.as_tensor(
        gross_budget,
        device=normalized.device,
        dtype=normalized.dtype,
    )
    return normalized * leverage


def _apply_min_trade_weight_numpy(
    weights: np.ndarray, min_trade_weight: float
) -> np.ndarray:
    threshold = float(min_trade_weight)
    if threshold <= 0.0:
        return weights
    gross_before = np.abs(weights).sum(axis=1, keepdims=True).astype(np.float32)
    out = np.where(
        np.abs(weights) >= np.float32(threshold), weights, np.float32(0.0)
    ).astype(np.float32, copy=False)
    gross_after = np.abs(out).sum(axis=1, keepdims=True).astype(np.float32)
    scale = np.divide(
        gross_before,
        np.clip(gross_after, 1e-12, None),
        out=np.zeros_like(gross_before, dtype=np.float32),
        where=gross_after > 1e-12,
    )
    return (out * scale).astype(np.float32, copy=False)


def _apply_min_trade_weight_row_numpy(
    weights_row: np.ndarray, min_trade_weight: float
) -> np.ndarray:
    threshold = float(min_trade_weight)
    if threshold <= 0.0:
        return weights_row
    gross_before = float(np.abs(weights_row).sum(dtype=np.float64))
    out = np.where(np.abs(weights_row) >= threshold, weights_row, 0.0).astype(
        weights_row.dtype, copy=False
    )
    gross_after = float(np.abs(out).sum(dtype=np.float64))
    if gross_before <= 1e-12 or gross_after <= 1e-12:
        return out
    return (out * (gross_before / gross_after)).astype(weights_row.dtype, copy=False)


def _apply_min_trade_weight_torch(
    weights: torch.Tensor,
    min_trade_weight: float,
    *,
    symbol_sharded: bool = False,
) -> torch.Tensor:
    threshold = float(min_trade_weight)
    if threshold <= 0.0:
        return weights
    gross_before = global_symbol_tensor_sum(
        weights.abs().sum(dim=1, keepdim=True),
        symbol_sharded=symbol_sharded,
    )
    out = torch.where(weights.abs() >= threshold, weights, torch.zeros_like(weights))
    gross_after = global_symbol_tensor_sum(
        out.abs().sum(dim=1, keepdim=True),
        symbol_sharded=symbol_sharded,
    )
    scale = torch.where(
        gross_after > 1e-12,
        gross_before / gross_after.clamp_min(1e-12),
        torch.zeros_like(gross_after),
    )
    return out * scale


def _apply_turnover_cap_numpy(
    prev_weights: np.ndarray,
    target_weights: np.ndarray,
    max_turnover_ratio: float,
) -> np.ndarray:
    if max_turnover_ratio <= 0.0:
        return target_weights

    deltas = target_weights - prev_weights
    turnovers = np.abs(deltas).sum(axis=1, keepdims=True).astype(np.float32)
    cap = np.float32(max_turnover_ratio)
    scale = np.ones_like(turnovers, dtype=np.float32)
    np.divide(cap, turnovers, out=scale, where=turnovers > cap)
    scale = np.clip(scale, 0.0, 1.0)
    return (prev_weights + deltas * scale).astype(np.float32)


def _apply_gross_exposure_cap_numpy(
    prev_weights: np.ndarray,
    proposed_weights: np.ndarray,
    gross_budget: float,
) -> np.ndarray:
    """Cap new exposure without manufacturing trades in frozen positions.

    ``proposed_weights`` has already passed the side, volume, and turnover
    constraints. Scaling the whole portfolio here would alter positions whose
    delta was deliberately set to zero by those constraints. Instead, execute
    every exposure-reducing part of the proposed rebalance, then scale only the
    exposure-increasing remainder into the available gross budget.
    """
    prev = np.asarray(prev_weights, dtype=np.float32)
    proposed = np.asarray(proposed_weights, dtype=np.float32)
    same_direction = (prev * proposed) > np.float32(0.0)
    reduced_base = np.where(
        same_direction,
        np.sign(prev) * np.minimum(np.abs(prev), np.abs(proposed)),
        np.float32(0.0),
    ).astype(np.float32, copy=False)
    additions = (proposed - reduced_base).astype(np.float32, copy=False)
    base_gross = float(np.abs(reduced_base).sum(dtype=np.float32))
    addition_gross = float(np.abs(additions).sum(dtype=np.float32))
    available = max(0.0, float(gross_budget) - base_gross)
    scale = min(1.0, available / max(addition_gross, 1e-12))
    return (reduced_base + additions * np.float32(scale)).astype(np.float32, copy=False)


def _apply_gross_exposure_cap_torch(
    prev_weights: torch.Tensor,
    proposed_weights: torch.Tensor,
    gross_budget: torch.Tensor,
) -> torch.Tensor:
    """Torch counterpart of :func:`_apply_gross_exposure_cap_numpy`."""
    same_direction = (prev_weights * proposed_weights) > 0.0
    reduced_base = torch.where(
        same_direction,
        torch.sign(prev_weights)
        * torch.minimum(prev_weights.abs(), proposed_weights.abs()),
        torch.zeros_like(prev_weights),
    )
    additions = proposed_weights - reduced_base
    available = (gross_budget - reduced_base.abs().sum()).clamp_min(0.0)
    addition_gross = additions.abs().sum()
    scale = torch.minimum(
        torch.ones_like(addition_gross),
        available / addition_gross.clamp_min(1e-12),
    )
    return reduced_base + additions * scale


def _apply_turnover_cap_torch(
    prev_weights: torch.Tensor,
    target_weights: torch.Tensor,
    max_turnover_ratio: float,
) -> torch.Tensor:
    if max_turnover_ratio <= 0.0:
        return target_weights

    deltas = target_weights - prev_weights
    turnovers = deltas.abs().sum(dim=1, keepdim=True)
    cap = torch.as_tensor(
        max_turnover_ratio, device=turnovers.device, dtype=turnovers.dtype
    )
    scale = torch.ones_like(turnovers)
    scale = torch.where(turnovers > cap, cap / turnovers.clamp_min(1e-12), scale)
    scale = scale.clamp_(0.0, 1.0)
    return prev_weights + deltas * scale


def _env_flag(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in {
        "0",
        "false",
        "off",
        "no",
    }


def _strict_no_fallback_enabled() -> bool:
    return _env_flag("STOCKAGENT_STRICT_NO_FALLBACK", "0")


def _require_side_masks_if_strict(
    can_buy_mask: object | None,
    can_sell_mask: object | None,
    *,
    context: str,
) -> None:
    if not _strict_no_fallback_enabled():
        return
    if can_buy_mask is None or can_sell_mask is None:
        raise ValueError(
            f"{context} requires can_buy_mask and can_sell_mask when strict_no_fallback=true; "
            "side masks are not inferred from tradable_mask."
        )


def _env_int(name: str, default: int = 0) -> int:
    raw = os.environ.get(name, str(default)).strip()
    try:
        return int(raw)
    except Exception:
        return default


def _runtime_cache_path(value: object) -> Path | None:
    raw = str(value or "").strip()
    if raw.lower() in {"", "0", "false", "off", "no", "none"}:
        return None
    return Path(os.path.expandvars(raw)).expanduser()


def _configure_compile_cache_paths() -> None:
    for env_name, default_path in (
        ("TORCHINDUCTOR_CACHE_DIR", "~/.cache/torchinductor"),
        ("TRITON_CACHE_DIR", "~/.cache/triton"),
        ("CUDA_CACHE_PATH", "~/.cache/nv_cuda"),
    ):
        path = _runtime_cache_path(os.environ.get(env_name, default_path))
        if path is None:
            continue
        try:
            path.mkdir(parents=True, exist_ok=True)
        except Exception:
            continue
        os.environ.setdefault(env_name, str(path))


def _torch_dynamo_is_compiling() -> bool:
    compiler = getattr(torch, "compiler", None)
    compiler_is_compiling = getattr(compiler, "is_compiling", None)
    if callable(compiler_is_compiling):
        try:
            return bool(compiler_is_compiling())
        except Exception:
            pass
    dynamo = getattr(torch, "_dynamo", None)
    if dynamo is None:
        return False
    is_compiling = getattr(dynamo, "is_compiling", None)
    if is_compiling is None:
        return False
    try:
        return bool(is_compiling())
    except Exception:
        return False


def _prepend_cuda_toolchain_paths() -> None:
    _configure_compile_cache_paths()
    normalize_cuda_env()
    env_bin = Path(sys.executable).resolve().parent
    env_root = env_bin.parent
    os.environ.setdefault("CONDA_PREFIX", str(env_root))
    ptxas_path = env_bin / "ptxas"
    if ptxas_path.exists():
        os.environ.setdefault("TRITON_PTXAS_PATH", str(ptxas_path))
        os.environ.setdefault("TRITON_PTXAS_BLACKWELL_PATH", str(ptxas_path))
    entries = [str(env_bin)]
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        entries.append(str(Path(conda_prefix) / "bin"))
    try:
        import site

        for site_dir in site.getsitepackages():
            entries.append(str(Path(site_dir) / "nvidia" / "cuda_nvcc" / "bin"))
    except Exception:
        pass
    existing = os.environ.get("PATH", "")
    existing_parts = [part for part in existing.split(os.pathsep) if part]
    prepend = [
        part
        for part in entries
        if part and Path(part).exists() and part not in existing_parts
    ]
    if prepend:
        os.environ["PATH"] = os.pathsep.join([*prepend, existing])
    os.environ.setdefault("CC", str(env_bin / "x86_64-conda-linux-gnu-gcc"))
    os.environ.setdefault("CXX", str(env_bin / "x86_64-conda-linux-gnu-g++"))


def _autotune_enabled() -> bool:
    return _env_flag("STOCKAGENT_BACKTEST_AUTOTUNE", "1")


def _compile_enabled() -> bool:
    if _torch_dynamo_is_compiling():
        return False
    enabled = _env_flag("STOCKAGENT_BACKTEST_COMPILE", "1")
    if enabled:
        _prepend_cuda_toolchain_paths()
        if not shutil.which("ptxas"):
            if _strict_no_fallback_enabled():
                raise RuntimeError(
                    "Backtest torch.compile was requested but ptxas was not found; "
                    "strict_no_fallback=true so eager backtest fallback is disabled."
                )
            return False
    return enabled


def _compile_stateful_enabled() -> bool:
    return _env_flag("STOCKAGENT_BACKTEST_COMPILE_STATEFUL", "1")


def _compile_dynamic_enabled() -> bool:
    return _env_flag("STOCKAGENT_BACKTEST_COMPILE_DYNAMIC", "0")


def _compile_prep_enabled() -> bool:
    return _env_flag("STOCKAGENT_BACKTEST_COMPILE_PREP", "1")


def _compile_verbose() -> bool:
    return _env_flag("STOCKAGENT_BACKTEST_VERBOSE", "0")


def _checkpoint_chunk_rows() -> int:
    return max(0, _env_int("STOCKAGENT_BACKTEST_CHECKPOINT_CHUNK_ROWS", 0))


def get_backtest_compile_stats(reset: bool = False) -> dict[str, int]:
    stats = dict(_SCAN_COMPILE_STATS)
    if reset:
        for key in _SCAN_COMPILE_STATS:
            _SCAN_COMPILE_STATS[key] = 0
    return stats


def get_backtest_prep_compile_stats(reset: bool = False) -> dict[str, int]:
    stats = dict(_PREP_COMPILE_STATS)
    if reset:
        for key in _PREP_COMPILE_STATS:
            _PREP_COMPILE_STATS[key] = 0
    return stats


def _add_backtest_runtime_stat(key: str, value: float = 1.0) -> None:
    if _torch_dynamo_is_compiling():
        return
    _BACKTEST_RUNTIME_STATS[key] = float(_BACKTEST_RUNTIME_STATS.get(key, 0.0)) + float(
        value
    )


def _runtime_stat_start() -> float | None:
    if _torch_dynamo_is_compiling():
        return None
    return time.perf_counter()


def _add_backtest_elapsed_stat(key: str, start: float | None) -> None:
    if start is None:
        return
    _add_backtest_runtime_stat(key, time.perf_counter() - start)


class _CudaRuntimeTimer:
    def __init__(self, key: str, tensor: torch.Tensor):
        self.key = key
        self.enabled = (
            tensor.device.type == "cuda"
            and torch.cuda.is_available()
            and not _torch_dynamo_is_compiling()
        )
        self.start: torch.cuda.Event | None = None
        self.end: torch.cuda.Event | None = None

    def __enter__(self) -> "_CudaRuntimeTimer":
        if self.enabled:
            self.start = torch.cuda.Event(enable_timing=True)
            self.end = torch.cuda.Event(enable_timing=True)
            self.start.record()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.enabled and self.start is not None and self.end is not None:
            self.end.record()
            _BACKTEST_PENDING_CUDA_EVENTS.append((self.key, self.start, self.end))


def _flush_backtest_cuda_event_stats() -> None:
    if not _BACKTEST_PENDING_CUDA_EVENTS:
        return
    pending = list(_BACKTEST_PENDING_CUDA_EVENTS)
    _BACKTEST_PENDING_CUDA_EVENTS.clear()
    for key, start, end in pending:
        try:
            end.synchronize()
            _add_backtest_runtime_stat(key, float(start.elapsed_time(end)) / 1000.0)
        except Exception:
            continue


def get_backtest_runtime_stats(reset: bool = False) -> dict[str, float]:
    _flush_backtest_cuda_event_stats()
    stats = dict(_BACKTEST_RUNTIME_STATS)
    if reset:
        for key in _BACKTEST_RUNTIME_STATS:
            _BACKTEST_RUNTIME_STATS[key] = 0.0
        _BACKTEST_PENDING_CUDA_EVENTS.clear()
    return stats


def _configure_inductor_cudagraphs() -> None:
    try:
        import torch._inductor.config as inductor_config  # type: ignore

        # Keep cudagraph enabled without forcing dynamic-shape partition warnings.
        inductor_config.triton.cudagraph_skip_dynamic_graphs = False
        inductor_config.triton.cudagraph_dynamic_shape_warn_limit = 0
        logging.getLogger("torch._inductor.utils").setLevel(logging.ERROR)
        logging.getLogger("torch._inductor.scheduler").setLevel(logging.ERROR)
    except Exception:
        pass


def _mark_static_shape(
    tensor: torch.Tensor | None, dims: list[int] | None = None
) -> None:
    if tensor is None:
        return
    mark_static = getattr(torch._dynamo, "mark_static", None)
    if mark_static is None:
        return
    try:
        if dims is None:
            dims = list(range(tensor.dim()))
        mark_static(tensor, dims)
    except Exception:
        return


def _supported_scan_dtype(dtype: torch.dtype) -> torch.dtype:
    if dtype in (torch.float16, torch.bfloat16, torch.float32, torch.float64):
        return dtype
    return torch.float32


def _scan_chunk_key(
    weights: torch.Tensor,
    long_only: bool,
    max_turnover_ratio: float,
) -> tuple:
    return (
        str(weights.device),
        int(weights.size(0)),
        int(weights.size(1)),
        str(weights.dtype),
        bool(long_only),
        float(max_turnover_ratio > 0.0),
    )


def _scan_compile_key(
    weights: torch.Tensor,
    long_only: bool,
    max_turnover_ratio: float,
    gross_budget: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
    scan_chunk_size: int,
    record_weights_history: bool,
    stateful_initial: bool = False,
) -> tuple:
    return (
        str(weights.device),
        int(weights.size(1)),
        str(weights.dtype),
        bool(long_only),
        float(max_turnover_ratio),
        float(gross_budget),
        float(buy_fee_rate),
        float(sell_fee_rate),
        int(scan_chunk_size),
        bool(record_weights_history),
        bool(stateful_initial),
        bool(_compile_dynamic_enabled()),
    )


def _scan_runner_factory(
    *,
    long_only: bool,
    max_turnover_ratio: float,
    gross_budget: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
    scan_chunk_size: int,
    record_weights_history: bool,
):
    def _runner(
        weights: torch.Tensor,
        future_returns: torch.Tensor,
        tradable_mask: torch.Tensor,
        can_buy_mask: torch.Tensor | None,
        can_sell_mask: torch.Tensor | None,
        prev_init: torch.Tensor,
        alive_init: torch.Tensor,
        can_short_open_mask: torch.Tensor,
        force_short_cover_mask: torch.Tensor,
        force_exit_mask: torch.Tensor,
        volume_limit_weights: torch.Tensor,
    ) -> tuple[
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
    ]:
        if long_only:
            return _vectorized_backtest_torch_scan_long_only(
                weights,
                future_returns,
                tradable_mask,
                can_buy_mask,
                can_sell_mask,
                force_exit_mask=force_exit_mask,
                prev_init=prev_init,
                alive_init=alive_init,
                max_turnover_ratio=max_turnover_ratio,
                buy_fee_rate=buy_fee_rate,
                sell_fee_rate=sell_fee_rate,
                volume_limit_weights=volume_limit_weights,
                gross_budget=gross_budget,
                scan_chunk_size=scan_chunk_size,
                record_weights_history=record_weights_history,
            )
        return _vectorized_backtest_torch_scan_long_short(
            weights,
            future_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            can_short_open_mask=can_short_open_mask,
            force_short_cover_mask=force_short_cover_mask,
            force_exit_mask=force_exit_mask,
            prev_init=prev_init,
            alive_init=alive_init,
            max_turnover_ratio=max_turnover_ratio,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            volume_limit_weights=volume_limit_weights,
            gross_budget=gross_budget,
            scan_chunk_size=scan_chunk_size,
            record_weights_history=record_weights_history,
        )

    return _runner


def _futures_portfolio_runner_factory(
    *,
    max_turnover_ratio: float,
    record_weights_history: bool,
) -> Callable[..., tuple[torch.Tensor, ...]]:
    """Return the canonical carry ledger with a compiler-friendly tensor ABI."""

    def _runner(
        target_weights: torch.Tensor,
        holding_log_returns: torch.Tensor,
        tradable_mask: torch.Tensor,
        must_liquidate_mask: torch.Tensor,
        fee_rate_per_open_notional: torch.Tensor,
        state_advance_mask: torch.Tensor,
        initial_weights: torch.Tensor,
        initial_alive: torch.Tensor,
    ) -> tuple[torch.Tensor, ...]:
        result = run_tw_futures_portfolio_continuous_torch(
            target_weights,
            holding_log_returns,
            tradable_mask,
            must_liquidate_mask,
            fee_rate_per_open_notional=fee_rate_per_open_notional,
            max_turnover_ratio=max_turnover_ratio,
            volume_limit_weights=None,
            state_advance_mask=state_advance_mask,
            return_weights_history=record_weights_history,
            initial_weights=initial_weights,
            initial_alive=initial_alive,
        )
        return (
            result.strategy_returns,
            result.turnovers,
            result.weights_history,
            result.final_weights,
            result.final_alive,
        )

    return _runner


def _resolve_futures_portfolio_runner(
    weights: torch.Tensor,
    *,
    max_turnover_ratio: float,
    record_weights_history: bool,
) -> Callable[..., tuple[torch.Tensor, ...]]:
    """Compile the recurrent all-futures eval ledger once per fixed ABI.

    The training loss already inlines this recurrence in its outer compiled
    graph. Epoch evaluation calls it outside that graph; without this resolver
    Python launches one kernel group per session and dominates the eval phase.
    """

    base_runner = _futures_portfolio_runner_factory(
        max_turnover_ratio=max_turnover_ratio,
        record_weights_history=record_weights_history,
    )
    if _torch_dynamo_is_compiling():
        return base_runner
    if (
        not _compile_stateful_enabled()
        or not _compile_enabled()
        or weights.device.type != "cuda"
        or not hasattr(torch, "compile")
    ):
        _SCAN_COMPILE_STATS["disabled"] += 1
        return base_runner

    key = (
        "tw_futures_portfolio_continuous",
        str(weights.device),
        tuple(int(v) for v in weights.shape),
        str(weights.dtype),
        float(max_turnover_ratio),
        bool(record_weights_history),
        bool(_compile_dynamic_enabled()),
    )
    if key in _FUTURES_PORTFOLIO_COMPILE_FAILED:
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "Futures portfolio ledger compile was previously marked failed; "
                "strict_no_fallback=true so eager fallback is disabled."
            )
        _SCAN_COMPILE_STATS["disabled"] += 1
        return base_runner

    cached = _FUTURES_PORTFOLIO_COMPILED_CACHE.get(key)
    if cached is not None:
        _SCAN_COMPILE_STATS["hits"] += 1
        return cached

    try:
        _SCAN_COMPILE_STATS["misses"] += 1
        _mark_static_shape(weights)
        compiled = torch.compile(
            base_runner,
            fullgraph=True,
            dynamic=_compile_dynamic_enabled(),
            options={"triton.cudagraphs": False},
        )
        _FUTURES_PORTFOLIO_COMPILED_CACHE[key] = compiled
        return compiled
    except Exception as exc:
        _FUTURES_PORTFOLIO_COMPILE_FAILED.add(key)
        _SCAN_COMPILE_STATS["failures"] += 1
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "Futures portfolio ledger torch.compile failed; "
                "strict_no_fallback=true so eager fallback is disabled."
            ) from exc
        print(
            "[backtest compile] futures portfolio compile failed, falling back "
            f"to eager: {type(exc).__name__}: {str(exc)[:300]}"
        )
        return base_runner


def _autotune_scan_chunk_size(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    volume_limit_weights: torch.Tensor,
    long_only: bool,
    max_turnover_ratio: float,
    gross_budget: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
) -> int:
    key = _scan_chunk_key(weights, long_only, max_turnover_ratio)
    cached = _SCAN_CHUNK_CACHE.get(key)
    if cached is not None:
        return cached
    if not _autotune_enabled():
        _SCAN_CHUNK_CACHE[key] = 256
        return 256

    # Keep warmup bounded to avoid large startup overhead.
    probe_rows = max(1, min(int(weights.size(0)), 1024))
    w_probe = weights[:probe_rows]
    r_probe = future_returns[:probe_rows]
    t_probe = tradable_mask[:probe_rows]
    buy_probe = can_buy_mask[:probe_rows] if can_buy_mask is not None else None
    sell_probe = can_sell_mask[:probe_rows] if can_sell_mask is not None else None
    volume_probe = volume_limit_weights[:probe_rows]
    prev_probe = torch.zeros_like(w_probe[0])
    alive_probe = torch.ones((), device=w_probe.device, dtype=torch.bool)
    short_probe = sell_probe if sell_probe is not None else t_probe
    force_probe = torch.zeros_like(t_probe, dtype=torch.bool)
    exit_probe = torch.zeros_like(t_probe, dtype=torch.bool)

    candidates = [c for c in SCAN_CHUNK_CANDIDATES if c <= probe_rows]
    if not candidates:
        candidates = [max(1, probe_rows)]

    def _measure(chunk_size: int) -> float:
        runner = _scan_runner_factory(
            long_only=long_only,
            max_turnover_ratio=max_turnover_ratio,
            gross_budget=gross_budget,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            scan_chunk_size=chunk_size,
            record_weights_history=True,
        )
        with torch.inference_mode():
            _ = runner(
                w_probe,
                r_probe,
                t_probe,
                buy_probe,
                sell_probe,
                prev_probe,
                alive_probe,
                short_probe,
                force_probe,
                exit_probe,
                volume_probe,
            )
            if weights.device.type == "cuda":
                torch.cuda.synchronize(weights.device)
            start = time.perf_counter()
            _ = runner(
                w_probe,
                r_probe,
                t_probe,
                buy_probe,
                sell_probe,
                prev_probe,
                alive_probe,
                short_probe,
                force_probe,
                exit_probe,
                volume_probe,
            )
            if weights.device.type == "cuda":
                torch.cuda.synchronize(weights.device)
            return time.perf_counter() - start

    best_chunk = candidates[0]
    best_time = float("inf")
    for chunk in candidates:
        elapsed = _measure(chunk)
        if elapsed < best_time:
            best_time = elapsed
            best_chunk = chunk

    _SCAN_CHUNK_CACHE[key] = best_chunk
    if _compile_verbose():
        print(
            "[backtest autotune] scan_chunk_size="
            f"{best_chunk} selected for shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, turnover_cap={max_turnover_ratio > 0.0})"
        )
    return best_chunk


try:
    _autotune_scan_chunk_size = torch._dynamo.disable(_autotune_scan_chunk_size)  # type: ignore[assignment]
except Exception:
    pass


def _resolve_scan_runner(
    weights: torch.Tensor,
    *,
    long_only: bool,
    max_turnover_ratio: float,
    gross_budget: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
    scan_chunk_size: int,
    record_weights_history: bool,
    stateful_initial: bool = False,
):
    base_runner = _scan_runner_factory(
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        gross_budget=gross_budget,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        scan_chunk_size=scan_chunk_size,
        record_weights_history=record_weights_history,
    )

    # If we're currently being traced by an outer torch.compile (e.g. the compiled
    # risk_aware_loss), skip creating a nested compiled runner.  The outer compiler
    # will inline the scan loop into its own unified CUDA graph, which avoids the
    # "tensor output of CUDAGraphs overwritten by subsequent run" error that arises
    # when two reduce-overhead / CUDA-graph compiled functions are nested at runtime.
    # Keep this check before _compile_enabled(), because _compile_enabled() probes
    # the local compiler toolchain via filesystem calls that Dynamo cannot trace.
    if _torch_dynamo_is_compiling():
        return base_runner

    if (
        not _compile_enabled()
        or weights.device.type != "cuda"
        or not hasattr(torch, "compile")
    ):
        return base_runner

    key = _scan_compile_key(
        weights,
        long_only,
        max_turnover_ratio,
        gross_budget,
        buy_fee_rate,
        sell_fee_rate,
        scan_chunk_size,
        record_weights_history,
        stateful_initial,
    )
    if key in _SCAN_COMPILE_FAILED:
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "Backtest scan runner compile was previously marked failed for this shape; "
                "strict_no_fallback=true so eager scan fallback is disabled."
            )
        _SCAN_COMPILE_STATS["disabled"] += 1
        if _compile_verbose():
            print(
                "[backtest compile] disabled after previous failure for "
                f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, record_weights={record_weights_history}, stateful={stateful_initial})"
            )
        return base_runner

    cached = _SCAN_COMPILED_CACHE.get(key)
    if cached is not None:
        _SCAN_COMPILE_STATS["hits"] += 1
        if _compile_verbose():
            print(
                "[backtest compile] cache hit for "
                f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, record_weights={record_weights_history}, stateful={stateful_initial})"
            )
        return cached

    try:
        _SCAN_COMPILE_STATS["misses"] += 1
        # Keep symbol axis static while allowing varying time/batch length.
        _mark_static_shape(weights, [1])
        compile_dynamic = _compile_dynamic_enabled()
        if _compile_verbose():
            print(
                "[backtest compile] compiling fixed-shape runner for "
                f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, record_weights={record_weights_history}, stateful={stateful_initial}, dynamic={compile_dynamic})"
            )
        _configure_inductor_cudagraphs()
        if stateful_initial:
            compiled = torch.compile(
                base_runner,
                fullgraph=True,
                dynamic=compile_dynamic,
                options={"triton.cudagraphs": False},
            )
        else:
            compiled = torch.compile(
                base_runner,
                mode="reduce-overhead",
                fullgraph=True,
                dynamic=compile_dynamic,
            )
        _SCAN_COMPILED_CACHE[key] = compiled
        return compiled
    except Exception as exc:
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "Backtest scan runner torch.compile failed; "
                "strict_no_fallback=true so eager scan fallback is disabled."
            ) from exc
        _SCAN_COMPILE_FAILED.add(key)
        _SCAN_COMPILE_STATS["failures"] += 1
        print(
            "[backtest compile] compile failed, falling back to eager for "
            f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, record_weights={record_weights_history}, stateful={stateful_initial}, dynamic={compile_dynamic})"
        )
        return base_runner


def _fallback_scan_runner_after_runtime_failure(
    *,
    error: Exception,
    weights: torch.Tensor,
    long_only: bool,
    max_turnover_ratio: float,
    gross_budget: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
    scan_chunk_size: int,
    record_weights_history: bool,
    stateful_initial: bool = False,
):
    key = _scan_compile_key(
        weights,
        long_only,
        max_turnover_ratio,
        gross_budget,
        buy_fee_rate,
        sell_fee_rate,
        scan_chunk_size,
        record_weights_history,
        stateful_initial,
    )
    _SCAN_COMPILE_FAILED.add(key)
    _SCAN_COMPILED_CACHE.pop(key, None)
    _SCAN_COMPILE_STATS["failures"] += 1
    if _strict_no_fallback_enabled():
        raise RuntimeError(
            "Compiled backtest scan runner failed at runtime; "
            "strict_no_fallback=true so eager scan fallback is disabled."
        ) from error
    print(
        "[backtest compile] runtime failed, falling back to eager for "
        f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, "
        f"dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, "
        f"record_weights={record_weights_history}, stateful={stateful_initial}): "
        f"{type(error).__name__}: {str(error)[:300]}"
    )
    return _scan_runner_factory(
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        gross_budget=gross_budget,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        scan_chunk_size=scan_chunk_size,
        record_weights_history=record_weights_history,
    )


@dataclass(slots=True)
class BacktestResult:
    """Container for a single backtest simulation run."""

    strategy_returns: np.ndarray  # [T] net portfolio log returns after costs
    benchmark_returns: np.ndarray  # [T] benchmark log returns
    turnovers: np.ndarray  # [T] total absolute weight change per day
    weights_history: np.ndarray  # [T, S] realised portfolio weights
    execution_mode: str = "naive"
    # ``currency`` for exact integer-share ledgers and ``nav_ratio`` for the
    # differentiable normalized training ledger.  Keeping this explicit
    # prevents numerically similar cash/claim arrays from being compared as if
    # they had the same units.
    settlement_ledger_unit: str | None = None
    cash_history: np.ndarray | None = None
    payables_history: np.ndarray | None = None
    receivables_history: np.ndarray | None = None
    settlement_default: np.ndarray | None = None
    equity_scale_history: np.ndarray | None = None
    final_cash: np.ndarray | None = None
    final_payables: np.ndarray | None = None
    final_receivables: np.ndarray | None = None
    commission_rebate_accrued_history: np.ndarray | None = None
    commission_rebate_paid_history: np.ndarray | None = None
    commission_rebate_current_history: np.ndarray | None = None
    commission_rebate_due_history: np.ndarray | None = None
    final_commission_rebate_current: np.ndarray | None = None
    final_commission_rebate_due: np.ndarray | None = None
    final_commission_rebate_month_id: np.ndarray | None = None
    final_equity_scale: np.ndarray | None = None
    short_sale_collateral_history: np.ndarray | None = None
    short_margin_collateral_history: np.ndarray | None = None
    final_short_sale_collateral: np.ndarray | None = None
    final_short_margin_collateral: np.ndarray | None = None
    long_margin_debt_history: np.ndarray | None = None
    final_long_margin_debt: np.ndarray | None = None
    final_alive: np.ndarray | None = None
    final_weights: np.ndarray | None = None
    shares_history: np.ndarray | None = None
    final_integer_state: TaiwanIntegerState | None = None
    # Original model requests before portfolio activation, normalization, side
    # gates, settlement, turnover, or participation constraints.  Integer-share
    # audit must replay these requests, not renormalize already-executed weights.
    requested_weights_history: np.ndarray | None = None
    # Dual-session audit fields.  Event axes are ordered [OPEN, CLOSE].
    open_weights_history: np.ndarray | None = None
    close_weights_history: np.ndarray | None = None
    event_turnovers: np.ndarray | None = None
    executed_buy_weights: np.ndarray | None = None
    executed_sell_weights: np.ndarray | None = None
    executed_long_buy_weights: np.ndarray | None = None
    executed_long_sell_weights: np.ndarray | None = None
    executed_short_open_weights: np.ndarray | None = None
    executed_short_cover_weights: np.ndarray | None = None
    due_weights_history: np.ndarray | None = None
    final_due_weights: np.ndarray | None = None


@dataclass(slots=True)
class BacktestResultTensor:
    """Torch tensor container for a single backtest simulation run."""

    strategy_returns: torch.Tensor  # [T] net portfolio log returns after costs
    benchmark_returns: torch.Tensor  # [T] benchmark log returns
    turnovers: torch.Tensor  # [T]
    weights_history: (
        torch.Tensor
    )  # [T, S], may be empty when caller disables history recording.
    final_weights: torch.Tensor | None = (
        None  # [S], realised weights after the final simulated day.
    )
    final_alive: torch.Tensor | None = None  # scalar bool; false is absorbing ruin.
    execution_mode: str = "naive"
    settlement_ledger_unit: str | None = None
    cash_history: torch.Tensor | None = None
    payables_history: torch.Tensor | None = None
    receivables_history: torch.Tensor | None = None
    settlement_default: torch.Tensor | None = None
    default_reason_history: torch.Tensor | None = None
    equity_scale_history: torch.Tensor | None = None
    final_cash: torch.Tensor | None = None
    final_payables: torch.Tensor | None = None
    final_receivables: torch.Tensor | None = None
    commission_rebate_accrued_history: torch.Tensor | None = None
    commission_rebate_paid_history: torch.Tensor | None = None
    commission_rebate_current_history: torch.Tensor | None = None
    commission_rebate_due_history: torch.Tensor | None = None
    final_commission_rebate_current: torch.Tensor | None = None
    final_commission_rebate_due: torch.Tensor | None = None
    final_commission_rebate_month_id: torch.Tensor | None = None
    final_equity_scale: torch.Tensor | None = None
    short_sale_collateral_history: torch.Tensor | None = None
    short_margin_collateral_history: torch.Tensor | None = None
    final_short_sale_collateral: torch.Tensor | None = None
    final_short_margin_collateral: torch.Tensor | None = None
    long_margin_debt_history: torch.Tensor | None = None
    final_long_margin_debt: torch.Tensor | None = None
    requested_weights_history: torch.Tensor | None = None
    # Dual-session audit fields.  Event axes are ordered [OPEN, CLOSE].
    open_weights_history: torch.Tensor | None = None
    close_weights_history: torch.Tensor | None = None
    event_turnovers: torch.Tensor | None = None
    executed_buy_weights: torch.Tensor | None = None
    executed_sell_weights: torch.Tensor | None = None
    executed_long_buy_weights: torch.Tensor | None = None
    executed_long_sell_weights: torch.Tensor | None = None
    executed_short_open_weights: torch.Tensor | None = None
    executed_short_cover_weights: torch.Tensor | None = None
    due_weights_history: torch.Tensor | None = None
    final_due_weights: torch.Tensor | None = None

    def to_numpy(self) -> BacktestResult:
        # NumPy has no native bfloat16 dtype. Cast at the torch boundary rather
        # than after .numpy(), because the latter raises before astype can run.
        def as_float32(tensor: torch.Tensor) -> np.ndarray:
            return tensor.detach().to(device="cpu", dtype=torch.float32).numpy()

        def optional_float32(tensor: torch.Tensor | None) -> np.ndarray | None:
            return None if tensor is None else as_float32(tensor)

        def optional_bool(tensor: torch.Tensor | None) -> np.ndarray | None:
            return (
                None
                if tensor is None
                else tensor.detach().to(device="cpu", dtype=torch.bool).numpy()
            )

        return BacktestResult(
            strategy_returns=as_float32(self.strategy_returns),
            benchmark_returns=as_float32(self.benchmark_returns),
            turnovers=as_float32(self.turnovers),
            weights_history=as_float32(self.weights_history),
            requested_weights_history=optional_float32(self.requested_weights_history),
            open_weights_history=optional_float32(self.open_weights_history),
            close_weights_history=optional_float32(self.close_weights_history),
            event_turnovers=optional_float32(self.event_turnovers),
            executed_buy_weights=optional_float32(self.executed_buy_weights),
            executed_sell_weights=optional_float32(self.executed_sell_weights),
            executed_long_buy_weights=optional_float32(self.executed_long_buy_weights),
            executed_long_sell_weights=optional_float32(
                self.executed_long_sell_weights
            ),
            executed_short_open_weights=optional_float32(
                self.executed_short_open_weights
            ),
            executed_short_cover_weights=optional_float32(
                self.executed_short_cover_weights
            ),
            due_weights_history=optional_float32(self.due_weights_history),
            final_due_weights=optional_float32(self.final_due_weights),
            execution_mode=self.execution_mode,
            settlement_ledger_unit=self.settlement_ledger_unit,
            cash_history=optional_float32(self.cash_history),
            payables_history=optional_float32(self.payables_history),
            receivables_history=optional_float32(self.receivables_history),
            settlement_default=optional_bool(self.settlement_default),
            equity_scale_history=optional_float32(self.equity_scale_history),
            final_weights=optional_float32(self.final_weights),
            final_cash=optional_float32(self.final_cash),
            final_payables=optional_float32(self.final_payables),
            final_receivables=optional_float32(self.final_receivables),
            commission_rebate_accrued_history=optional_float32(
                self.commission_rebate_accrued_history
            ),
            commission_rebate_paid_history=optional_float32(
                self.commission_rebate_paid_history
            ),
            commission_rebate_current_history=optional_float32(
                self.commission_rebate_current_history
            ),
            commission_rebate_due_history=optional_float32(
                self.commission_rebate_due_history
            ),
            final_commission_rebate_current=optional_float32(
                self.final_commission_rebate_current
            ),
            final_commission_rebate_due=optional_float32(
                self.final_commission_rebate_due
            ),
            final_commission_rebate_month_id=(
                None
                if self.final_commission_rebate_month_id is None
                else self.final_commission_rebate_month_id.detach()
                .to(device="cpu", dtype=torch.int64)
                .numpy()
            ),
            final_equity_scale=optional_float32(self.final_equity_scale),
            short_sale_collateral_history=optional_float32(
                self.short_sale_collateral_history
            ),
            short_margin_collateral_history=optional_float32(
                self.short_margin_collateral_history
            ),
            final_short_sale_collateral=optional_float32(
                self.final_short_sale_collateral
            ),
            final_short_margin_collateral=optional_float32(
                self.final_short_margin_collateral
            ),
            long_margin_debt_history=optional_float32(
                self.long_margin_debt_history
            ),
            final_long_margin_debt=optional_float32(
                self.final_long_margin_debt
            ),
            final_alive=optional_bool(self.final_alive),
        )


@dataclass(slots=True)
class HoldingsRecord:
    """Single position or execution record for one date/symbol."""

    date: str
    symbol: str
    shares: int
    price: float
    market_value: float
    holding_ratio: float
    is_cash: bool
    record_type: str = "position"
    side: str = ""


def holding_record_abs_sort_key(record: HoldingsRecord) -> tuple[float, str]:
    return (-abs(float(record.holding_ratio)), str(record.symbol))


def _vectorized_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    can_buy_mask: np.ndarray | None,
    can_sell_mask: np.ndarray | None,
    can_short_open_mask: np.ndarray | None,
    force_short_cover_mask: np.ndarray | None,
    force_exit_mask: np.ndarray | None,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    volume_limit_weights: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    target_weights = np.asarray(weights, dtype=np.float32).copy()
    gross_budget = _resolve_exposure_budget(gross_leverage)
    tradable = tradable_mask.astype(bool)
    _require_side_masks_if_strict(can_buy_mask, can_sell_mask, context="numpy backtest")
    # ``tradable`` is the outer execution gate.  Side masks may further
    # restrict an otherwise tradable asset, but they must never make an
    # ordinary halt/data hole executable.  ``force_exit_mask`` is the sole
    # exception and is settled separately below.
    buy_mask = (
        tradable if can_buy_mask is None else can_buy_mask.astype(bool) & tradable
    )
    sell_mask = (
        tradable if can_sell_mask is None else can_sell_mask.astype(bool) & tradable
    )
    short_open_mask = (
        sell_mask
        if can_short_open_mask is None
        else can_short_open_mask.astype(bool) & sell_mask & tradable
    )
    force_cover_mask = (
        np.zeros_like(tradable, dtype=bool)
        if force_short_cover_mask is None
        else force_short_cover_mask.astype(bool) & tradable
    )
    terminal_exit_mask = (
        np.zeros_like(tradable, dtype=bool)
        if force_exit_mask is None
        else np.asarray(force_exit_mask, dtype=bool)
    )

    target_weights = _normalize_target_weights_numpy(
        target_weights,
        long_only=long_only,
        gross_budget=gross_budget,
        portfolio_activation=portfolio_activation,
    )
    target_weights = _apply_min_trade_weight_numpy(target_weights, min_trade_weight)
    volume_limits = None
    if volume_limit_weights is not None:
        volume_limits = np.asarray(volume_limit_weights, dtype=np.float32)
        if volume_limits.shape != target_weights.shape:
            raise ValueError(
                "volume_limit_weights shape must match weights: "
                f"{volume_limits.shape} != {target_weights.shape}"
            )

    t_len, n_symbols = target_weights.shape
    asset_simple_returns = _asset_log_returns_to_simple_numpy(future_returns)
    weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)
    buy_turnovers = np.zeros((t_len,), dtype=np.float32)
    sell_turnovers = np.zeros((t_len,), dtype=np.float32)
    gross_simple_returns = np.zeros((t_len,), dtype=np.float32)

    prev = np.zeros((n_symbols,), dtype=np.float32)
    alive = True
    for t in range(t_len):
        if not alive:
            # Ruin is absorbing: later model targets cannot recreate capital.
            weights_history[t] = 0.0
            prev.fill(0.0)
            continue
        tradable_t = tradable[t]
        # Only an explicit lifecycle event may settle a position. Generic
        # non-tradability (data holes, zero volume, halts) freezes it instead.
        terminal_exit_t = terminal_exit_mask[t]
        forced_exit = np.where(terminal_exit_t, prev, 0.0).astype(
            np.float32, copy=False
        )
        forced_buy_turnover = np.clip(-forced_exit, 0.0, None).sum(dtype=np.float32)
        forced_sell_turnover = np.clip(forced_exit, 0.0, None).sum(dtype=np.float32)
        prev = np.where(terminal_exit_t, 0.0, prev).astype(np.float32, copy=False)
        target_t = target_weights[t].copy()
        target_t[~tradable_t] = 0.0
        target_t[terminal_exit_t] = 0.0
        forced_cover_now = force_cover_mask[t] & (prev < 0.0) & buy_mask[t]
        if not long_only:
            target_t = np.where(force_cover_mask[t] & (target_t < 0.0), 0.0, target_t)

        delta = target_t - prev
        buy_t = buy_mask[t]
        sell_t = sell_mask[t]
        delta[(delta > 0.0) & ~buy_t] = 0.0

        if long_only:
            delta[(delta < 0.0) & ~sell_t] = 0.0
            sell_deltas = np.clip(delta, None, 0.0)
            base_after_sells = prev + sell_deltas
            buy_deltas = np.clip(delta, 0.0, None)
            buy_sum = float(buy_deltas.sum(dtype=np.float32))
            buy_capacity = max(
                0.0,
                float(gross_budget) - float(base_after_sells.sum(dtype=np.float32)),
            )
            if buy_sum > buy_capacity and buy_sum > 0.0:
                buy_deltas *= np.float32(buy_capacity / buy_sum)
            delta = sell_deltas + buy_deltas
        else:
            constrained_target = prev + delta
            down = constrained_target < prev
            reduce_long = down & (prev > 0.0) & (constrained_target >= 0.0) & ~sell_t
            constrained_target[reduce_long] = prev[reduce_long]

            cross_from_long = down & (prev > 0.0) & (constrained_target < 0.0)
            blocked_cross_sell = cross_from_long & ~sell_t
            constrained_target[blocked_cross_sell] = prev[blocked_cross_sell]
            blocked_cross_short = cross_from_long & sell_t & ~short_open_mask[t]
            constrained_target[blocked_cross_short] = 0.0

            open_or_increase_short = (
                down & (prev <= 0.0) & (constrained_target < prev) & ~short_open_mask[t]
            )
            constrained_target[open_or_increase_short] = prev[open_or_increase_short]
            delta = constrained_target - prev

        next_weights = prev + delta
        if volume_limits is not None:
            volume_cap = volume_limits[t]
            abs_delta = np.abs(delta)
            cap_safe = np.where(
                np.isfinite(volume_cap) & (volume_cap >= 0.0),
                np.maximum(volume_cap, 0.0),
                abs_delta,
            )
            delta = np.sign(delta) * np.minimum(abs_delta, cap_safe)
            next_weights = prev + delta
        if max_turnover_ratio > 0.0:
            next_weights = _apply_turnover_cap_numpy(
                prev[None, :], next_weights[None, :], max_turnover_ratio
            )[0]
            delta = next_weights - prev

        if not long_only:
            # A regulatory/broker forced cover is mandatory liquidation, not a
            # discretionary rebalance.  Voluntary turnover and participation
            # caps must not leave a known-illegal short open on an executable
            # deadline session.
            next_weights[forced_cover_now] = np.maximum(
                next_weights[forced_cover_now],
                np.float32(0.0),
            )
            delta = next_weights - prev

        if not long_only:
            next_weights = _apply_gross_exposure_cap_numpy(
                prev,
                next_weights,
                gross_budget,
            )
            delta = next_weights - prev

        executed_weights = next_weights.astype(np.float32, copy=False)
        weights_history[t] = executed_weights
        buy_turnovers[t] = (
            np.clip(delta, 0.0, None).sum(dtype=np.float32) + forced_buy_turnover
        )
        sell_turnovers[t] = (
            np.clip(-delta, 0.0, None).sum(dtype=np.float32) + forced_sell_turnover
        )
        gross_simple_returns[t] = np.sum(
            executed_weights * asset_simple_returns[t],
            dtype=np.float32,
        )
        net_simple_t = (
            gross_simple_returns[t]
            - np.float32(buy_fee_rate) * buy_turnovers[t]
            - np.float32(sell_fee_rate) * sell_turnovers[t]
        )
        prev, alive = _drift_weights_after_return_numpy(
            executed_weights,
            asset_simple_returns[t],
            net_simple_t,
        )

    turnovers = (buy_turnovers + sell_turnovers).astype(np.float32)
    net_simple = (
        gross_simple_returns
        - buy_fee_rate * buy_turnovers
        - sell_fee_rate * sell_turnovers
    )
    strategy_returns = _portfolio_simple_returns_to_log_numpy(net_simple)
    return strategy_returns.astype(np.float32), turnovers, weights_history


def _vectorized_backtest_torch_scan_long_only(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    force_exit_mask: torch.Tensor | None = None,
    prev_init: torch.Tensor | None = None,
    alive_init: torch.Tensor | None = None,
    max_turnover_ratio: float = 0.0,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    volume_limit_weights: torch.Tensor | None = None,
    gross_budget: float = 1.0,
    scan_chunk_size: int = 256,
    record_weights_history: bool = True,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    future_returns_t = _asset_log_returns_to_simple_torch(
        future_returns, device=weights.device, dtype=weights.dtype
    )
    target_weights = weights
    tradable = tradable_mask
    _require_side_masks_if_strict(
        can_buy_mask, can_sell_mask, context="torch scan backtest"
    )
    buy_mask = (
        tradable
        if can_buy_mask is None
        else can_buy_mask.to(device=weights.device, dtype=torch.bool) & tradable
    )
    sell_mask = (
        tradable
        if can_sell_mask is None
        else can_sell_mask.to(device=weights.device, dtype=torch.bool) & tradable
    )
    terminal_exit_mask = (
        torch.zeros_like(tradable, dtype=torch.bool)
        if force_exit_mask is None
        else force_exit_mask.to(device=weights.device, dtype=torch.bool)
    )

    t_len, n_symbols = target_weights.shape
    dtype = target_weights.dtype
    device = target_weights.device
    prev = (
        prev_init.to(device=device, dtype=dtype)
        if prev_init is not None
        else torch.zeros_like(target_weights[0])
    )
    alive = (
        alive_init.to(device=device, dtype=torch.bool).reshape(())
        if alive_init is not None
        else torch.ones((), device=device, dtype=torch.bool)
    )
    weights_history = (
        torch.empty((t_len, n_symbols), device=device, dtype=dtype)
        if record_weights_history
        else torch.empty((0, n_symbols), device=device, dtype=dtype)
    )
    buy_turnovers = torch.empty((t_len,), device=device, dtype=dtype)
    sell_turnovers = torch.empty((t_len,), device=device, dtype=dtype)
    gross_returns = torch.empty((t_len,), device=device, dtype=dtype)
    cap = torch.as_tensor(max_turnover_ratio, device=device, dtype=dtype)
    gross_cap = torch.as_tensor(gross_budget, device=device, dtype=dtype)
    chunk_size = max(1, int(scan_chunk_size))

    for start in range(0, t_len, chunk_size):
        end = min(start + chunk_size, t_len)
        target_chunk = target_weights[start:end]
        tradable_chunk = tradable[start:end]
        buy_chunk = buy_mask[start:end]
        sell_chunk = sell_mask[start:end]
        terminal_chunk = terminal_exit_mask[start:end]
        volume_chunk = (
            None if volume_limit_weights is None else volume_limit_weights[start:end]
        )

        for offset in range(end - start):
            idx = start + offset
            prev = torch.where(alive, prev, torch.zeros_like(prev))
            tradable_t = tradable_chunk[offset]
            terminal_exit_t = terminal_chunk[offset]
            forced_exit = torch.where(terminal_exit_t, prev, torch.zeros_like(prev))
            forced_buy_turnover = (-forced_exit).clamp_min(0.0).sum()
            forced_sell_turnover = forced_exit.clamp_min(0.0).sum()
            prev = torch.where(terminal_exit_t, torch.zeros_like(prev), prev)
            target_t = torch.where(
                alive & tradable_t & ~terminal_exit_t,
                target_chunk[offset],
                torch.zeros_like(prev),
            )

            delta = target_t - prev
            buy_delta = delta.clamp_min(0.0) * buy_chunk[offset].to(dtype=dtype)
            sell_delta = delta.clamp_max(0.0) * sell_chunk[offset].to(dtype=dtype)
            base_after_sells = prev + sell_delta
            buy_sum = buy_delta.sum()
            buy_capacity = (gross_cap - base_after_sells.sum()).clamp_min(0.0)
            buy_scale = torch.minimum(
                torch.ones_like(buy_sum),
                buy_capacity / buy_sum.clamp_min(1e-12),
            )
            delta = sell_delta + buy_delta * buy_scale

            next_weights = prev + delta
            if volume_chunk is not None:
                volume_cap = volume_chunk[offset].to(device=device, dtype=dtype)
                abs_delta = delta.abs()
                cap_safe = torch.where(
                    torch.isfinite(volume_cap) & (volume_cap >= 0.0),
                    volume_cap.clamp_min(0.0),
                    abs_delta,
                )
                delta = torch.sign(delta) * torch.minimum(abs_delta, cap_safe)
                next_weights = prev + delta
            if max_turnover_ratio > 0.0:
                turnover = delta.abs().sum()
                turnover_scale = torch.minimum(
                    torch.ones_like(turnover),
                    cap / turnover.clamp_min(1e-12),
                )
                next_weights = prev + delta * turnover_scale
                delta = next_weights - prev

            if record_weights_history:
                weights_history[idx] = next_weights
            buy_turnovers[idx] = delta.clamp_min(0.0).sum() + forced_buy_turnover
            sell_turnovers[idx] = (-delta).clamp_min(0.0).sum() + forced_sell_turnover
            gross_return = (next_weights * future_returns_t[idx]).sum()
            gross_returns[idx] = gross_return
            net_simple_return = (
                gross_return
                - float(buy_fee_rate) * buy_turnovers[idx]
                - float(sell_fee_rate) * sell_turnovers[idx]
            )
            prev, survived = _drift_weights_after_return_torch(
                next_weights,
                future_returns_t[idx],
                net_simple_return,
            )
            alive = alive & survived
            prev = torch.where(alive, prev, torch.zeros_like(prev))

    turnovers = buy_turnovers + sell_turnovers
    return (
        turnovers,
        buy_turnovers,
        sell_turnovers,
        gross_returns,
        weights_history,
        prev,
        alive,
    )


def _vectorized_backtest_torch_scan_long_short(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    force_exit_mask: torch.Tensor | None = None,
    prev_init: torch.Tensor | None = None,
    alive_init: torch.Tensor | None = None,
    max_turnover_ratio: float = 0.0,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    volume_limit_weights: torch.Tensor | None = None,
    gross_budget: float = 1.0,
    scan_chunk_size: int = 256,
    record_weights_history: bool = True,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    future_returns_t = _asset_log_returns_to_simple_torch(
        future_returns, device=weights.device, dtype=weights.dtype
    )

    target_weights = weights
    tradable = tradable_mask
    _require_side_masks_if_strict(
        can_buy_mask, can_sell_mask, context="torch scan backtest"
    )
    buy_mask = (
        tradable
        if can_buy_mask is None
        else can_buy_mask.to(device=weights.device, dtype=torch.bool) & tradable
    )
    sell_mask = (
        tradable
        if can_sell_mask is None
        else can_sell_mask.to(device=weights.device, dtype=torch.bool) & tradable
    )
    short_open_mask = (
        sell_mask
        if can_short_open_mask is None
        else can_short_open_mask.to(device=weights.device, dtype=torch.bool)
        & sell_mask
        & tradable
    )
    force_cover_mask = (
        torch.zeros_like(tradable, dtype=torch.bool)
        if force_short_cover_mask is None
        else force_short_cover_mask.to(device=weights.device, dtype=torch.bool)
        & tradable
    )
    terminal_exit_mask = (
        torch.zeros_like(tradable, dtype=torch.bool)
        if force_exit_mask is None
        else force_exit_mask.to(device=weights.device, dtype=torch.bool)
    )

    t_len, n_symbols = target_weights.shape
    dtype = target_weights.dtype
    device = target_weights.device
    prev = (
        prev_init.to(device=device, dtype=dtype)
        if prev_init is not None
        else torch.zeros_like(target_weights[0])
    )
    alive = (
        alive_init.to(device=device, dtype=torch.bool).reshape(())
        if alive_init is not None
        else torch.ones((), device=device, dtype=torch.bool)
    )
    weights_history = (
        torch.empty((t_len, n_symbols), device=device, dtype=dtype)
        if record_weights_history
        else torch.empty((0, n_symbols), device=device, dtype=dtype)
    )
    buy_turnovers = torch.empty((t_len,), device=device, dtype=dtype)
    sell_turnovers = torch.empty((t_len,), device=device, dtype=dtype)
    gross_returns = torch.empty((t_len,), device=device, dtype=dtype)
    cap = torch.as_tensor(max_turnover_ratio, device=device, dtype=dtype)
    gross_cap = torch.as_tensor(gross_budget, device=device, dtype=dtype)
    chunk_size = max(1, int(scan_chunk_size))

    for start in range(0, t_len, chunk_size):
        end = min(start + chunk_size, t_len)
        target_chunk = target_weights[start:end]
        tradable_chunk = tradable[start:end]
        buy_chunk = buy_mask[start:end]
        sell_chunk = sell_mask[start:end]
        short_chunk = short_open_mask[start:end]
        force_chunk = force_cover_mask[start:end]
        terminal_chunk = terminal_exit_mask[start:end]
        volume_chunk = (
            None if volume_limit_weights is None else volume_limit_weights[start:end]
        )

        for offset in range(end - start):
            idx = start + offset
            prev = torch.where(alive, prev, torch.zeros_like(prev))
            tradable_t = tradable_chunk[offset]
            terminal_exit_t = terminal_chunk[offset]
            forced_exit = torch.where(terminal_exit_t, prev, torch.zeros_like(prev))
            forced_buy_turnover = (-forced_exit).clamp_min(0.0).sum()
            forced_sell_turnover = forced_exit.clamp_min(0.0).sum()
            prev = torch.where(terminal_exit_t, torch.zeros_like(prev), prev)
            target_t = torch.where(
                alive & tradable_t & ~terminal_exit_t,
                target_chunk[offset],
                torch.zeros_like(prev),
            )
            forced_cover_now = force_chunk[offset] & (prev < 0.0) & buy_chunk[offset]
            target_t = torch.where(
                force_chunk[offset] & (target_t < 0.0),
                torch.zeros_like(target_t),
                target_t,
            )

            delta = target_t - prev
            constrained_target = torch.where(
                (delta > 0.0) & ~buy_chunk[offset], prev, target_t
            )
            down = constrained_target < prev
            reduce_long = (
                down & (prev > 0.0) & (constrained_target >= 0.0) & ~sell_chunk[offset]
            )
            constrained_target = torch.where(reduce_long, prev, constrained_target)

            cross_from_long = down & (prev > 0.0) & (constrained_target < 0.0)
            constrained_target = torch.where(
                cross_from_long & ~sell_chunk[offset], prev, constrained_target
            )
            constrained_target = torch.where(
                cross_from_long & sell_chunk[offset] & ~short_chunk[offset],
                torch.zeros_like(constrained_target),
                constrained_target,
            )

            open_or_increase_short = (
                down
                & (prev <= 0.0)
                & (constrained_target < prev)
                & ~short_chunk[offset]
            )
            constrained_target = torch.where(
                open_or_increase_short, prev, constrained_target
            )
            delta = constrained_target - prev

            next_weights = prev + delta
            if volume_chunk is not None:
                volume_cap = volume_chunk[offset].to(device=device, dtype=dtype)
                abs_delta = delta.abs()
                cap_safe = torch.where(
                    torch.isfinite(volume_cap) & (volume_cap >= 0.0),
                    volume_cap.clamp_min(0.0),
                    abs_delta,
                )
                delta = torch.sign(delta) * torch.minimum(abs_delta, cap_safe)
                next_weights = prev + delta
            if max_turnover_ratio > 0.0:
                turnover = delta.abs().sum()
                turnover_scale = torch.minimum(
                    torch.ones_like(turnover),
                    cap / turnover.clamp_min(1e-12),
                )
                next_weights = prev + delta * turnover_scale
                delta = next_weights - prev

            # Forced covers are mandatory and therefore override voluntary
            # turnover/volume caps once the buy side is executable.
            next_weights = torch.where(
                forced_cover_now,
                next_weights.clamp_min(0.0),
                next_weights,
            )
            delta = next_weights - prev

            next_weights = _apply_gross_exposure_cap_torch(
                prev,
                next_weights,
                gross_cap,
            )
            delta = next_weights - prev

            if record_weights_history:
                weights_history[idx] = next_weights
            buy_turnovers[idx] = delta.clamp_min(0.0).sum() + forced_buy_turnover
            sell_turnovers[idx] = (-delta).clamp_min(0.0).sum() + forced_sell_turnover
            gross_return = (next_weights * future_returns_t[idx]).sum()
            gross_returns[idx] = gross_return
            net_simple_return = (
                gross_return
                - float(buy_fee_rate) * buy_turnovers[idx]
                - float(sell_fee_rate) * sell_turnovers[idx]
            )
            prev, survived = _drift_weights_after_return_torch(
                next_weights,
                future_returns_t[idx],
                net_simple_return,
            )
            alive = alive & survived
            prev = torch.where(alive, prev, torch.zeros_like(prev))

    turnovers = buy_turnovers + sell_turnovers
    return (
        turnovers,
        buy_turnovers,
        sell_turnovers,
        gross_returns,
        weights_history,
        prev,
        alive,
    )


def _prepare_runner_factory(
    *,
    long_only: bool,
    gross_budget: float,
    min_trade_weight: float,
    portfolio_activation: str,
    side_masks_require_tradable: bool,
    symbol_sharded: bool = False,
):
    activation_name = normalize_portfolio_activation(portfolio_activation)

    def _runner(
        weights: torch.Tensor,
        tradable_mask: torch.Tensor,
        can_buy_mask: torch.Tensor,
        can_sell_mask: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        compute_dtype = _supported_scan_dtype(weights.dtype)
        target_weights = weights.to(dtype=compute_dtype)
        tradable = tradable_mask.to(device=target_weights.device, dtype=torch.bool)
        raw_buy_mask = can_buy_mask.to(
            device=target_weights.device,
            dtype=torch.bool,
        )
        raw_sell_mask = can_sell_mask.to(
            device=target_weights.device,
            dtype=torch.bool,
        )
        if side_masks_require_tradable:
            buy_mask = raw_buy_mask & tradable
            sell_mask = raw_sell_mask & tradable
        else:
            # TW cash commits today's signal from the previous completed
            # session, so its selection mask is deliberately prior-alive.
            # Current-close execution masks are independent observables: a
            # held security that resumes today must still be sellable or
            # coverable even when it was absent from yesterday's universe.
            buy_mask = raw_buy_mask
            sell_mask = raw_sell_mask
        target_weights = _normalize_target_weights_torch(
            target_weights,
            long_only=long_only,
            gross_budget=gross_budget,
            portfolio_activation=activation_name,
            symbol_sharded=symbol_sharded,
        )
        target_weights = _apply_min_trade_weight_torch(
            target_weights,
            min_trade_weight,
            symbol_sharded=symbol_sharded,
        )
        return target_weights, tradable, buy_mask, sell_mask

    return _runner


def _prepare_compile_key(
    weights: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    long_only: bool,
    gross_budget: float,
    min_trade_weight: float,
    portfolio_activation: str,
    side_masks_require_tradable: bool,
    symbol_sharded: bool = False,
) -> tuple:
    return (
        str(weights.device),
        int(weights.size(0)),
        int(weights.size(1)),
        str(weights.dtype),
        str(tradable_mask.dtype),
        str(can_buy_mask.dtype),
        str(can_sell_mask.dtype),
        bool(long_only),
        float(gross_budget),
        float(min_trade_weight),
        normalize_portfolio_activation(portfolio_activation),
        bool(side_masks_require_tradable),
        bool(symbol_sharded),
        bool(_compile_dynamic_enabled()),
    )


def _resolve_prepare_runner(
    weights: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    *,
    long_only: bool,
    gross_budget: float,
    min_trade_weight: float,
    portfolio_activation: str,
    side_masks_require_tradable: bool,
    symbol_sharded: bool = False,
):
    base_runner = _prepare_runner_factory(
        long_only=long_only,
        gross_budget=gross_budget,
        min_trade_weight=min_trade_weight,
        portfolio_activation=portfolio_activation,
        side_masks_require_tradable=side_masks_require_tradable,
        symbol_sharded=symbol_sharded,
    )
    # When an outer torch.compile is tracing risk_aware_loss, do not create a
    # nested compiled prepare runner and do not mutate compile stats. Stats dict
    # mutation becomes a Dynamo guard and can force repeated loss recompiles.
    if _torch_dynamo_is_compiling():
        return base_runner
    if (
        not _compile_prep_enabled()
        or not _compile_enabled()
        or weights.device.type != "cuda"
        or not hasattr(torch, "compile")
    ):
        _PREP_COMPILE_STATS["disabled"] += 1
        _add_backtest_runtime_stat("eager_prep_calls")
        return base_runner

    key = _prepare_compile_key(
        weights,
        tradable_mask,
        can_buy_mask,
        can_sell_mask,
        long_only,
        gross_budget,
        min_trade_weight,
        portfolio_activation,
        side_masks_require_tradable,
        symbol_sharded,
    )
    if key in _PREP_COMPILE_FAILED:
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "Backtest prep runner compile was previously marked failed for this shape; "
                "strict_no_fallback=true so eager prep fallback is disabled."
            )
        _PREP_COMPILE_STATS["disabled"] += 1
        _add_backtest_runtime_stat("eager_prep_calls")
        return base_runner

    cached = _PREP_COMPILED_CACHE.get(key)
    if cached is not None:
        _PREP_COMPILE_STATS["hits"] += 1
        _add_backtest_runtime_stat("compiled_prep_calls")
        return cached

    try:
        _PREP_COMPILE_STATS["misses"] += 1
        _configure_inductor_cudagraphs()
        compile_dynamic = _compile_dynamic_enabled()
        compiled = torch.compile(
            base_runner,
            fullgraph=True,
            dynamic=compile_dynamic,
            options={"triton.cudagraphs": False},
        )
        _PREP_COMPILED_CACHE[key] = compiled
        _add_backtest_runtime_stat("compiled_prep_calls")
        return compiled
    except Exception as e:
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "Backtest prep runner torch.compile failed; "
                "strict_no_fallback=true so eager prep fallback is disabled."
            ) from e
        _PREP_COMPILE_FAILED.add(key)
        _PREP_COMPILE_STATS["failures"] += 1
        _add_backtest_runtime_stat("eager_prep_calls")
        if _compile_verbose():
            print(
                "[backtest prep compile] compile failed, falling back to eager for "
                f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, dynamic={compile_dynamic}): "
                f"{type(e).__name__}: {str(e)[:300]}"
            )
        return base_runner


def _prepare_scan_inputs(
    weights: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    long_only: bool,
    gross_leverage: float,
    min_trade_weight: float,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    side_masks_require_tradable: bool = True,
    symbol_sharded: bool = False,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    # Model forward stays under BF16/FP16 AMP, but portfolio state, fee-adjusted
    # returns, turnover, and finance reductions are numerically sensitive. Keep
    # the canonical backtest in FP32; .to() preserves the gradient path back to
    # lower-precision model outputs.
    weights = weights.to(dtype=torch.float32)
    gross_budget = _resolve_exposure_budget(gross_leverage)
    _require_side_masks_if_strict(
        can_buy_mask, can_sell_mask, context="backtest input preparation"
    )
    buy_input = tradable_mask if can_buy_mask is None else can_buy_mask
    sell_input = tradable_mask if can_sell_mask is None else can_sell_mask
    runner = _resolve_prepare_runner(
        weights,
        tradable_mask,
        buy_input,
        sell_input,
        long_only=long_only,
        gross_budget=gross_budget,
        min_trade_weight=min_trade_weight,
        portfolio_activation=portfolio_activation,
        side_masks_require_tradable=side_masks_require_tradable,
        symbol_sharded=symbol_sharded,
    )
    try:
        return runner(weights, tradable_mask, buy_input, sell_input)
    except Exception as e:
        if _torch_dynamo_is_compiling() or not (
            _compile_prep_enabled()
            and _compile_enabled()
            and weights.device.type == "cuda"
            and hasattr(torch, "compile")
        ):
            raise
        key = _prepare_compile_key(
            weights,
            tradable_mask,
            buy_input,
            sell_input,
            long_only,
            gross_budget,
            min_trade_weight,
            portfolio_activation,
            side_masks_require_tradable,
            symbol_sharded,
        )
        _PREP_COMPILE_FAILED.add(key)
        _PREP_COMPILED_CACHE.pop(key, None)
        _PREP_COMPILE_STATS["failures"] += 1
        _add_backtest_runtime_stat("eager_prep_calls")
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "Compiled backtest prep runner failed at runtime; "
                "strict_no_fallback=true so eager prep fallback is disabled."
            ) from e
        if _compile_verbose():
            print(
                "[backtest prep compile] runtime failed, falling back to eager for "
                f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}): "
                f"{type(e).__name__}: {str(e)[:300]}"
            )
        eager_runner = _prepare_runner_factory(
            long_only=long_only,
            gross_budget=gross_budget,
            min_trade_weight=min_trade_weight,
            portfolio_activation=portfolio_activation,
            side_masks_require_tradable=side_masks_require_tradable,
            symbol_sharded=symbol_sharded,
        )
        return eager_runner(weights, tradable_mask, buy_input, sell_input)


def _prepare_tw_phase_actions(
    actions: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    execution_mode: str,
    long_only: bool,
    gross_leverage: float,
    min_trade_weight: float,
    portfolio_activation: str,
    symbol_sharded: bool = False,
) -> torch.Tensor:
    """Resolve raw phase logits/requests into the canonical action ABI.

    The last axis is always symbols.  ``tw_cash`` owns an independent gross
    budget at open and close.  ``tw_overnight`` has one exit-fraction channel
    plus two entry channels that share a single daily gross budget.  This
    explicit boundary mirrors the model helper while keeping the canonical
    executor independent from any particular model class.
    """

    mode = normalize_execution_mode(execution_mode)
    if mode not in TW_CARRYING_EXECUTION_MODES:
        raise ValueError(
            "_prepare_tw_phase_actions supports tw_cash and tw_overnight only"
        )
    expected_channels = 2 if mode == "tw_cash" else 3
    if actions.dim() != 3 or int(actions.size(1)) != expected_channels:
        raise ValueError(
            f"{mode} actions must have shape [T,{expected_channels},S], "
            f"got {tuple(actions.shape)}"
        )
    expected_mask_shape = (int(actions.size(0)), int(actions.size(2)))
    if tuple(tradable_mask.shape) != expected_mask_shape:
        raise ValueError(
            "tradable_mask must have shape [T,S] matching phase actions: "
            f"{tuple(tradable_mask.shape)} != {expected_mask_shape}"
        )

    safe = torch.nan_to_num(
        actions.to(dtype=torch.float32),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    signal_mask = tradable_mask.to(device=safe.device, dtype=torch.bool)
    safe = safe.masked_fill(~signal_mask[:, None, :], 0.0)
    gross_budget = _resolve_exposure_budget(gross_leverage)

    if mode == "tw_cash":
        rows, phases, symbols = safe.shape
        flat = safe.reshape(int(rows) * int(phases), int(symbols))
        flat = _normalize_target_weights_torch(
            flat,
            long_only=long_only,
            gross_budget=gross_budget,
            portfolio_activation=portfolio_activation,
            symbol_sharded=symbol_sharded,
        )
        flat = _apply_min_trade_weight_torch(
            flat,
            min_trade_weight,
            symbol_sharded=symbol_sharded,
        )
        return flat.reshape(int(rows), int(phases), int(symbols)).masked_fill(
            ~signal_mask[:, None, :],
            0.0,
        )

    activation_name = normalize_portfolio_activation(portfolio_activation)
    raw_exit = safe[:, 0]
    exit_fraction = (
        raw_exit.clamp(0.0, 1.0)
        if activation_name == "pre_normalized"
        else torch.sigmoid(raw_exit)
    ).masked_fill(~signal_mask, 0.0)
    rows, _phases, symbols = safe.shape
    if activation_name == "pre_normalized":
        entry_logits = safe[:, 1:]
        opposite_same_day_entry = (
            (entry_logits[:, 0].abs() > 1.0e-10)
            & (entry_logits[:, 1].abs() > 1.0e-10)
            & ((entry_logits[:, 0] > 0.0) != (entry_logits[:, 1] > 0.0))
        )
        # Fail closed without a CUDA host synchronization in the per-batch
        # preparation hot path.  The direct ledger APIs still reject malformed
        # P3 requests; this model-output adapter turns an impossible
        # same-session reversal into no new position for that symbol.
        entry_logits = entry_logits.masked_fill(
            opposite_same_day_entry[:, None, :],
            0.0,
        )
    else:
        raw_open_entry = safe[:, 1]
        raw_close_entry = safe[:, 2]
        direction_logits = 0.5 * (raw_open_entry + raw_close_entry)
        close_entry_fraction = torch.sigmoid(raw_close_entry - raw_open_entry)
        entry_logits = torch.stack(
            (
                direction_logits * (1.0 - close_entry_fraction),
                direction_logits * close_entry_fraction,
            ),
            dim=1,
        )
    flat_entries = entry_logits.reshape(int(rows), 2 * int(symbols))
    flat_entries = _normalize_target_weights_torch(
        flat_entries,
        long_only=long_only,
        gross_budget=gross_budget,
        portfolio_activation=portfolio_activation,
        symbol_sharded=symbol_sharded,
    )
    flat_entries = _apply_min_trade_weight_torch(
        flat_entries,
        min_trade_weight,
        symbol_sharded=symbol_sharded,
    )
    entries = flat_entries.reshape(int(rows), 2, int(symbols)).masked_fill(
        ~signal_mask[:, None, :],
        0.0,
    )
    return torch.cat((exit_fraction[:, None, :], entries), dim=1)


def _vectorized_backtest_torch(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    can_short_open_mask: torch.Tensor | None,
    force_short_cover_mask: torch.Tensor | None,
    force_exit_mask: torch.Tensor | None,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    scan_chunk_size: int | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    volume_limit_weights: torch.Tensor | None = None,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    total_start = _runtime_stat_start()
    _add_backtest_runtime_stat("calls")
    if initial_weights is not None or initial_alive is not None:
        _add_backtest_runtime_stat("stateful_calls")
    if return_weights_history:
        _add_backtest_runtime_stat("return_weights_history_calls")

    with _CudaRuntimeTimer("total_cuda_s", weights):
        gross_budget = _resolve_exposure_budget(gross_leverage)
        # MTM drift or frozen holdings can leave prior gross exposure above the
        # configured budget.  Therefore even a cap >= 2 * gross_budget may bind
        # and must never be optimized away.
        effective_max_turnover_ratio = float(max_turnover_ratio)
        prep_start = _runtime_stat_start()
        with _CudaRuntimeTimer("prep_cuda_s", weights):
            prepped_weights, prepped_tradable, prepped_buy, prepped_sell = (
                _prepare_scan_inputs(
                    weights,
                    tradable_mask,
                    can_buy_mask,
                    can_sell_mask,
                    long_only,
                    gross_leverage,
                    min_trade_weight,
                    portfolio_activation,
                )
            )
            prepped_short_open = (
                prepped_sell
                if can_short_open_mask is None
                else can_short_open_mask.to(
                    device=prepped_weights.device, dtype=torch.bool
                )
                & prepped_sell
                & prepped_tradable
            )
            prepped_force_cover = (
                torch.zeros_like(prepped_tradable, dtype=torch.bool)
                if force_short_cover_mask is None
                else force_short_cover_mask.to(
                    device=prepped_weights.device, dtype=torch.bool
                )
                & prepped_tradable
            )
            prepped_force_exit = (
                torch.zeros_like(prepped_tradable, dtype=torch.bool)
                if force_exit_mask is None
                else force_exit_mask.to(
                    device=prepped_weights.device,
                    dtype=torch.bool,
                )
            )
            if volume_limit_weights is None:
                # A finite-cap tensor and an all-inf tensor share one compiled
                # signature; inf means the execution rule is unconstrained.
                prepped_volume_limit_weights = torch.full_like(
                    prepped_weights, float("inf")
                )
            else:
                prepped_volume_limit_weights = volume_limit_weights.to(
                    device=prepped_weights.device,
                    dtype=prepped_weights.dtype,
                )
                if tuple(prepped_volume_limit_weights.shape) != tuple(
                    prepped_weights.shape
                ):
                    raise ValueError(
                        "volume_limit_weights shape must match weights: "
                        f"{tuple(prepped_volume_limit_weights.shape)} != {tuple(prepped_weights.shape)}"
                    )
        _add_backtest_elapsed_stat("prep_s", prep_start)
        prev_init_start = _runtime_stat_start()
        with _CudaRuntimeTimer("prev_init_cuda_s", prepped_weights):
            prev_init = (
                torch.nan_to_num(
                    initial_weights.detach()
                    .clone(memory_format=torch.contiguous_format)
                    .to(
                        device=prepped_weights.device,
                        dtype=prepped_weights.dtype,
                    ),
                    nan=0.0,
                    posinf=0.0,
                    neginf=0.0,
                )
                if initial_weights is not None
                else torch.zeros_like(prepped_weights[0])
            )
            alive_init = (
                initial_alive.detach()
                .clone(memory_format=torch.contiguous_format)
                .to(device=prepped_weights.device, dtype=torch.bool)
                .reshape(())
                if initial_alive is not None
                else torch.ones((), device=prepped_weights.device, dtype=torch.bool)
            )
        _add_backtest_elapsed_stat("prev_init_s", prev_init_start)

    resolved_chunk = (
        int(scan_chunk_size)
        if scan_chunk_size is not None and int(scan_chunk_size) > 0
        else 256
        if _torch_dynamo_is_compiling()
        else _autotune_scan_chunk_size(
            prepped_weights,
            future_returns,
            prepped_tradable,
            prepped_buy,
            prepped_sell,
            prepped_volume_limit_weights,
            long_only,
            effective_max_turnover_ratio,
            gross_budget,
            buy_fee_rate,
            sell_fee_rate,
        )
    )
    checkpoint_rows = _checkpoint_chunk_rows()
    use_checkpoint = (
        checkpoint_rows > 0
        and torch.is_grad_enabled()
        and prepped_weights.requires_grad
    )

    if use_checkpoint:
        _add_backtest_runtime_stat("checkpoint_calls")
        resolve_start = _runtime_stat_start()
        # Checkpoint recomputation and cudagraph-captured compiled functions can
        # conflict in backward. Use eager runner for this path to keep training stable.
        runner = _scan_runner_factory(
            long_only=long_only,
            max_turnover_ratio=effective_max_turnover_ratio,
            gross_budget=gross_budget,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            scan_chunk_size=resolved_chunk,
            record_weights_history=return_weights_history,
        )
        _add_backtest_elapsed_stat("runner_resolve_s", resolve_start)
        _add_backtest_runtime_stat("eager_runner_calls")
    else:
        stateful_initial = initial_weights is not None or initial_alive is not None
        compile_possible = (
            _compile_enabled()
            and prepped_weights.device.type == "cuda"
            and hasattr(torch, "compile")
        )
        resolve_start = _runtime_stat_start()
        if stateful_initial:
            if _compile_stateful_enabled():
                # Stateful train/eval scans carry detached portfolio state between
                # batches/chunks. Compile this path separately with CUDA graphs off.
                runner = _resolve_scan_runner(
                    prepped_weights,
                    long_only=long_only,
                    max_turnover_ratio=effective_max_turnover_ratio,
                    gross_budget=gross_budget,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    scan_chunk_size=resolved_chunk,
                    record_weights_history=return_weights_history,
                    stateful_initial=True,
                )
                if compile_possible:
                    _add_backtest_runtime_stat("compiled_runner_calls")
                    _add_backtest_runtime_stat("stateful_compiled_runner_calls")
                else:
                    _add_backtest_runtime_stat("eager_runner_calls")
                    _add_backtest_runtime_stat("stateful_eager_runner_calls")
            else:
                runner = _scan_runner_factory(
                    long_only=long_only,
                    max_turnover_ratio=effective_max_turnover_ratio,
                    gross_budget=gross_budget,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    scan_chunk_size=resolved_chunk,
                    record_weights_history=return_weights_history,
                )
                _add_backtest_runtime_stat("eager_runner_calls")
                _add_backtest_runtime_stat("stateful_eager_runner_calls")
        else:
            runner = _resolve_scan_runner(
                prepped_weights,
                long_only=long_only,
                max_turnover_ratio=effective_max_turnover_ratio,
                gross_budget=gross_budget,
                buy_fee_rate=buy_fee_rate,
                sell_fee_rate=sell_fee_rate,
                scan_chunk_size=resolved_chunk,
                record_weights_history=return_weights_history,
                stateful_initial=False,
            )
            if compile_possible:
                _add_backtest_runtime_stat("compiled_runner_calls")
                _add_backtest_runtime_stat("nonstateful_compiled_runner_calls")
            else:
                _add_backtest_runtime_stat("eager_runner_calls")
                _add_backtest_runtime_stat("nonstateful_eager_runner_calls")
        _add_backtest_elapsed_stat("runner_resolve_s", resolve_start)

    if not use_checkpoint:
        try:
            runner_call_start = _runtime_stat_start()
            with _CudaRuntimeTimer("runner_call_cuda_s", prepped_weights):
                (
                    turnovers,
                    buy_turnovers,
                    sell_turnovers,
                    gross_returns,
                    weights_history,
                    final_weights,
                    final_alive,
                ) = runner(
                    prepped_weights,
                    future_returns,
                    prepped_tradable,
                    prepped_buy,
                    prepped_sell,
                    prev_init,
                    alive_init,
                    prepped_short_open,
                    prepped_force_cover,
                    prepped_force_exit,
                    prepped_volume_limit_weights,
                )
            _add_backtest_elapsed_stat("runner_call_s", runner_call_start)
        except Exception as e:
            if _torch_dynamo_is_compiling() or not (
                _compile_enabled()
                and prepped_weights.device.type == "cuda"
                and hasattr(torch, "compile")
            ):
                raise
            _add_backtest_runtime_stat("runtime_fallback_calls")
            fallback_start = _runtime_stat_start()
            runner = _fallback_scan_runner_after_runtime_failure(
                error=e,
                weights=prepped_weights,
                long_only=long_only,
                max_turnover_ratio=effective_max_turnover_ratio,
                gross_budget=gross_budget,
                buy_fee_rate=buy_fee_rate,
                sell_fee_rate=sell_fee_rate,
                scan_chunk_size=resolved_chunk,
                record_weights_history=return_weights_history,
                stateful_initial=stateful_initial,
            )
            _add_backtest_elapsed_stat("runtime_fallback_s", fallback_start)
            runner_call_start = _runtime_stat_start()
            with _CudaRuntimeTimer("runner_call_cuda_s", prepped_weights):
                (
                    turnovers,
                    buy_turnovers,
                    sell_turnovers,
                    gross_returns,
                    weights_history,
                    final_weights,
                    final_alive,
                ) = runner(
                    prepped_weights,
                    future_returns,
                    prepped_tradable,
                    prepped_buy,
                    prepped_sell,
                    prev_init,
                    alive_init,
                    prepped_short_open,
                    prepped_force_cover,
                    prepped_force_exit,
                    prepped_volume_limit_weights,
                )
            _add_backtest_elapsed_stat("runner_call_s", runner_call_start)
    else:
        checkpoint_start = _runtime_stat_start()
        with _CudaRuntimeTimer("checkpoint_cuda_s", prepped_weights):
            chunk_rows = max(1, int(checkpoint_rows))
            turnovers_chunks: list[torch.Tensor] = []
            buy_chunks: list[torch.Tensor] = []
            sell_chunks: list[torch.Tensor] = []
            gross_chunks: list[torch.Tensor] = []
            weights_chunks: list[torch.Tensor] = [] if return_weights_history else []
            prev = prev_init
            alive = alive_init

            for start in range(0, int(prepped_weights.size(0)), chunk_rows):
                end = min(start + chunk_rows, int(prepped_weights.size(0)))
                w_chunk = prepped_weights[start:end]
                r_chunk = future_returns[start:end]
                t_chunk = prepped_tradable[start:end]
                b_chunk = prepped_buy[start:end]
                s_chunk = prepped_sell[start:end]
                short_chunk = prepped_short_open[start:end]
                force_chunk = prepped_force_cover[start:end]
                exit_chunk = prepped_force_exit[start:end]
                volume_chunk = prepped_volume_limit_weights[start:end]

                chunk_out = checkpoint_fn(
                    runner,
                    w_chunk,
                    r_chunk,
                    t_chunk,
                    b_chunk,
                    s_chunk,
                    prev,
                    alive,
                    short_chunk,
                    force_chunk,
                    exit_chunk,
                    volume_chunk,
                    use_reentrant=False,
                    preserve_rng_state=False,
                )
                t_out, b_out, s_out, g_out, w_out, last_w, last_alive = chunk_out
                turnovers_chunks.append(t_out)
                buy_chunks.append(b_out)
                sell_chunks.append(s_out)
                gross_chunks.append(g_out)
                if return_weights_history:
                    weights_chunks.append(w_out)
                prev = last_w
                alive = last_alive

            turnovers = torch.cat(turnovers_chunks, dim=0)
            buy_turnovers = torch.cat(buy_chunks, dim=0)
            sell_turnovers = torch.cat(sell_chunks, dim=0)
            gross_returns = torch.cat(gross_chunks, dim=0)
            if return_weights_history:
                weights_history = torch.cat(weights_chunks, dim=0)
            else:
                weights_history = prepped_weights.new_empty(
                    (0, prepped_weights.size(1))
                )
            final_weights = prev
            final_alive = alive
        _add_backtest_elapsed_stat("checkpoint_s", checkpoint_start)

    finalize_start = _runtime_stat_start()
    with _CudaRuntimeTimer("finalize_cuda_s", prepped_weights):
        returns_dtype = prepped_weights.dtype
        net_simple_returns = (
            gross_returns
            - float(buy_fee_rate) * buy_turnovers
            - float(sell_fee_rate) * sell_turnovers
        )
        strategy_returns = _portfolio_simple_returns_to_log_torch(net_simple_returns)
        result = (
            strategy_returns.to(returns_dtype),
            turnovers.to(returns_dtype),
            weights_history.to(returns_dtype),
            final_weights.to(returns_dtype),
            final_alive.to(dtype=torch.bool),
        )
    _add_backtest_elapsed_stat("finalize_s", finalize_start)
    _add_backtest_elapsed_stat("total_s", total_start)
    return result


def run_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    crypto_stateful_proximal_allocator: bool = False,
    crypto_proximal_cost_multiplier: float = 1.0,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    can_buy_mask: np.ndarray | None = None,
    can_sell_mask: np.ndarray | None = None,
    can_short_open_mask: np.ndarray | None = None,
    force_short_cover_mask: np.ndarray | None = None,
    short_margin_rate: np.ndarray | float | None = None,
    short_capacity_weights: np.ndarray | None = None,
    short_maintenance_ratio: float = 1.30,
    short_handling_fee_rate: np.ndarray | float = 0.0,
    force_exit_mask: np.ndarray | None = None,
    volume_limit_weights: np.ndarray | None = None,
    execution_mode: str = "naive",
    buy_fee_rates: np.ndarray | None = None,
    sell_fee_rates: np.ndarray | None = None,
    commission_rebate_rates: np.ndarray | None = None,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: np.ndarray | None = None,
    commission_rebate_payment_eligible_mask: np.ndarray | None = None,
    settlement_lag_sessions: int = 2,
    state_advance_mask: np.ndarray | None = None,
    day_trade_eligible_mask: np.ndarray | None = None,
    day_trade_can_buy_open_mask: np.ndarray | None = None,
    day_trade_can_sell_open_mask: np.ndarray | None = None,
    unresolved_corporate_action_mask: np.ndarray | None = None,
    cash_dividend_yield: np.ndarray | None = None,
    cash_dividend_payment_delay_sessions: np.ndarray | None = None,
    claim_queue_sessions: int | None = None,
    initial_weights: np.ndarray | None = None,
    initial_cash: np.ndarray | float | None = None,
    initial_payables: np.ndarray | None = None,
    initial_receivables: np.ndarray | None = None,
    initial_commission_rebate_current: np.ndarray | float | None = None,
    initial_commission_rebate_due: np.ndarray | float | None = None,
    initial_commission_rebate_month_id: np.ndarray | int | None = None,
    initial_alive: np.ndarray | bool | None = None,
    symbol_indices: np.ndarray | None = None,
    initial_equity_scale: np.ndarray | float | None = None,
    initial_short_sale_collateral: np.ndarray | None = None,
    initial_short_margin_collateral: np.ndarray | None = None,
    initial_long_margin_debt: np.ndarray | None = None,
    overnight_returns: np.ndarray | None = None,
    can_short_open_open_mask: np.ndarray | None = None,
    day_trade_execution_initial_capital: float = 1_000_000.0,
) -> BacktestResult:
    """Simulate daily portfolio execution from model weights."""
    mode = normalize_execution_mode(execution_mode)
    if mode != "naive":

        def tensor(value: np.ndarray | float | bool | None) -> torch.Tensor | None:
            return None if value is None else torch.as_tensor(value)

        result = run_backtest_torch(
            torch.as_tensor(weights),
            torch.as_tensor(future_returns),
            torch.as_tensor(tradable_mask),
            torch.as_tensor(benchmark_returns),
            buy_fee_rate,
            sell_fee_rate,
            long_only=long_only,
            max_turnover_ratio=max_turnover_ratio,
            crypto_stateful_proximal_allocator=(
                crypto_stateful_proximal_allocator
            ),
            crypto_proximal_cost_multiplier=crypto_proximal_cost_multiplier,
            gross_leverage=gross_leverage,
            min_trade_weight=min_trade_weight,
            portfolio_activation=portfolio_activation,
            can_buy_mask=tensor(can_buy_mask),
            can_sell_mask=tensor(can_sell_mask),
            can_short_open_mask=tensor(can_short_open_mask),
            can_short_open_open_mask=tensor(can_short_open_open_mask),
            force_short_cover_mask=tensor(force_short_cover_mask),
            short_margin_rate=tensor(short_margin_rate),
            short_capacity_weights=tensor(short_capacity_weights),
            short_maintenance_ratio=short_maintenance_ratio,
            short_handling_fee_rate=tensor(short_handling_fee_rate),
            force_exit_mask=tensor(force_exit_mask),
            volume_limit_weights=tensor(volume_limit_weights),
            overnight_returns=tensor(overnight_returns),
            execution_mode=mode,
            buy_fee_rates=tensor(buy_fee_rates),
            sell_fee_rates=tensor(sell_fee_rates),
            commission_rebate_rates=tensor(commission_rebate_rates),
            commission_rebate_timing=commission_rebate_timing,
            session_month_ids=tensor(session_month_ids),
            commission_rebate_payment_eligible_mask=tensor(
                commission_rebate_payment_eligible_mask
            ),
            settlement_lag_sessions=settlement_lag_sessions,
            state_advance_mask=tensor(state_advance_mask),
            day_trade_eligible_mask=tensor(day_trade_eligible_mask),
            day_trade_can_buy_open_mask=tensor(day_trade_can_buy_open_mask),
            day_trade_can_sell_open_mask=tensor(day_trade_can_sell_open_mask),
            unresolved_corporate_action_mask=tensor(unresolved_corporate_action_mask),
            cash_dividend_yield=tensor(cash_dividend_yield),
            cash_dividend_payment_delay_sessions=tensor(
                cash_dividend_payment_delay_sessions
            ),
            claim_queue_sessions=claim_queue_sessions,
            initial_weights=tensor(initial_weights),
            initial_cash=tensor(initial_cash),
            initial_payables=tensor(initial_payables),
            initial_receivables=tensor(initial_receivables),
            initial_commission_rebate_current=tensor(initial_commission_rebate_current),
            initial_commission_rebate_due=tensor(initial_commission_rebate_due),
            initial_commission_rebate_month_id=tensor(
                initial_commission_rebate_month_id
            ),
            initial_alive=tensor(initial_alive),
            initial_equity_scale=tensor(initial_equity_scale),
            initial_short_sale_collateral=tensor(initial_short_sale_collateral),
            initial_short_margin_collateral=tensor(initial_short_margin_collateral),
            initial_long_margin_debt=tensor(initial_long_margin_debt),
            symbol_indices=tensor(symbol_indices),
            day_trade_execution_initial_capital=day_trade_execution_initial_capital,
        )
        return result.to_numpy()

    strategy_returns, turnovers, weights_history = _vectorized_backtest(
        weights,
        future_returns,
        tradable_mask,
        can_buy_mask,
        can_sell_mask,
        can_short_open_mask,
        force_short_cover_mask,
        force_exit_mask,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        gross_leverage=gross_leverage,
        min_trade_weight=min_trade_weight,
        portfolio_activation=portfolio_activation,
        volume_limit_weights=volume_limit_weights,
    )

    return BacktestResult(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns.astype(np.float32),
        turnovers=turnovers,
        weights_history=weights_history,
    )


def run_backtest_torch(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    benchmark_returns: torch.Tensor,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    crypto_stateful_proximal_allocator: bool = False,
    crypto_proximal_cost_multiplier: float = 1.0,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    can_buy_mask: torch.Tensor | None = None,
    can_sell_mask: torch.Tensor | None = None,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    short_margin_rate: torch.Tensor | float | None = None,
    short_capacity_weights: torch.Tensor | None = None,
    short_maintenance_ratio: float = 1.30,
    short_handling_fee_rate: torch.Tensor | float = 0.0,
    force_exit_mask: torch.Tensor | None = None,
    scan_chunk_size: int | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    volume_limit_weights: torch.Tensor | None = None,
    execution_mode: str = "naive",
    buy_fee_rates: torch.Tensor | None = None,
    sell_fee_rates: torch.Tensor | None = None,
    normal_sell_fee_rates: torch.Tensor | None = None,
    day_trade_unlimited_margin_conversion: bool = False,
    day_trade_margin_financing_ratio: float = 0.60,
    day_trade_margin_financing_annual_rate: float = 0.16,
    day_trade_margin_short_handling_fee_rate: float = 0.001,
    day_trade_margin_short_annual_borrow_rate: float = 0.20,
    commission_rebate_rates: torch.Tensor | None = None,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: torch.Tensor | None = None,
    commission_rebate_payment_eligible_mask: torch.Tensor | None = None,
    settlement_lag_sessions: int = 2,
    state_advance_mask: torch.Tensor | None = None,
    day_trade_eligible_mask: torch.Tensor | None = None,
    day_trade_can_buy_open_mask: torch.Tensor | None = None,
    day_trade_can_sell_open_mask: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_commission_rebate_current: torch.Tensor | None = None,
    initial_commission_rebate_due: torch.Tensor | None = None,
    initial_commission_rebate_month_id: torch.Tensor | None = None,
    symbol_indices: torch.Tensor | None = None,
    unresolved_corporate_action_mask: torch.Tensor | None = None,
    cash_dividend_yield: torch.Tensor | None = None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None = None,
    claim_queue_sessions: int | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_short_sale_collateral: torch.Tensor | None = None,
    initial_short_margin_collateral: torch.Tensor | None = None,
    initial_long_margin_debt: torch.Tensor | None = None,
    overnight_returns: torch.Tensor | None = None,
    can_short_open_open_mask: torch.Tensor | None = None,
    day_trade_execution_initial_capital: float = 1_000_000.0,
    symbol_sharded_ledger: bool = False,
    futures_portfolio_training_surrogate_only: bool = False,
    futures_portfolio_recoverable_backward: bool = False,
) -> BacktestResultTensor:
    """Simulate daily portfolio execution from model weights in torch."""
    mode = normalize_execution_mode(execution_mode)
    if mode == "crypto_perpetual":
        if overnight_returns is None:
            raise ValueError(
                "crypto_perpetual requires raw price log returns separately "
                "from funding-adjusted total returns"
            )
        prepped_weights, prepped_tradable, prepped_buy, prepped_sell = (
            _prepare_scan_inputs(
                weights,
                tradable_mask,
                can_buy_mask,
                can_sell_mask,
                long_only,
                gross_leverage,
                min_trade_weight,
                portfolio_activation,
            )
        )
        prepped_short = (
            prepped_sell
            if can_short_open_mask is None
            else can_short_open_mask.to(device=weights.device, dtype=torch.bool)
            & prepped_sell
            & prepped_tradable
        )
        crypto = run_crypto_perpetual_torch(
            prepped_weights,
            future_returns,
            overnight_returns,
            prepped_tradable,
            prepped_buy,
            prepped_sell,
            prepped_short,
            (
                torch.zeros_like(prepped_tradable, dtype=torch.bool)
                if force_exit_mask is None
                else force_exit_mask.to(device=weights.device, dtype=torch.bool)
            ),
            buy_fee_rate=float(buy_fee_rate),
            sell_fee_rate=float(sell_fee_rate),
            long_only=long_only,
            maximum_gross=_resolve_exposure_budget(gross_leverage),
            max_turnover_ratio=float(max_turnover_ratio),
            stateful_proximal_allocator=bool(
                crypto_stateful_proximal_allocator
            ),
            proximal_cost_multiplier=float(crypto_proximal_cost_multiplier),
            volume_limit_weights=volume_limit_weights,
            state_advance_mask=state_advance_mask,
            initial_weights=initial_weights,
            initial_alive=initial_alive,
            return_weights_history=return_weights_history,
        )
        return BacktestResultTensor(
            strategy_returns=_portfolio_simple_returns_to_log_torch(
                crypto.strategy_simple_returns
            ),
            benchmark_returns=benchmark_returns.to(
                device=weights.device, dtype=torch.float32
            ),
            turnovers=crypto.turnovers,
            weights_history=crypto.executed_weights,
            requested_weights_history=(
                weights.to(dtype=torch.float32) if return_weights_history else None
            ),
            final_weights=crypto.final_weights,
            final_alive=crypto.final_alive,
            execution_mode=mode,
            settlement_ledger_unit="notional_weight",
        )
    if mode == "tw_stock_context_futures_portfolio":
        if overnight_returns is None:
            raise ValueError(
                "tw_stock_context_futures_portfolio requires packed futures "
                "execution channels"
            )
        execution = overnight_returns.to(device=weights.device, dtype=torch.float32)
        if (
            execution.ndim != 3
            or tuple(execution.shape[:2]) != tuple(weights.shape)
            or int(execution.size(-1)) not in {4, 11}
        ):
            raise ValueError(
                "stock-context futures execution tensor must have shape "
                "[T,1936,4] or exact-integer [T,1936,11] matching model actions"
            )
        if float(buy_fee_rate) != 0.0 or float(sell_fee_rate) != 0.0:
            raise ValueError(
                "tw_stock_context_futures_portfolio fixed commission is "
                "mutually exclusive with proportional buy/sell fees"
            )
        gross_budget = _resolve_exposure_budget(gross_leverage)
        prepped_weights = _normalize_target_weights_torch(
            weights.to(dtype=torch.float32),
            long_only=long_only,
            gross_budget=gross_budget,
            portfolio_activation=portfolio_activation,
        )
        prepped_weights = _apply_min_trade_weight_torch(
            prepped_weights,
            float(min_trade_weight),
        )
        if int(execution.size(-1)) == 11:
            if float(max_turnover_ratio) != 0.0:
                raise ValueError(
                    "integer stock-context futures requires max_turnover_ratio=0"
                )
            if futures_portfolio_training_surrogate_only:
                result = run_tw_futures_portfolio_integer_surrogate_torch(
                    prepped_weights,
                    execution,
                    initial_capital=float(day_trade_execution_initial_capital),
                    state_advance_mask=state_advance_mask,
                    initial_weights=initial_weights,
                    initial_equity_scale=initial_equity_scale,
                    initial_alive=initial_alive,
                    return_weights_history=return_weights_history,
                )
            else:
                result = run_tw_futures_portfolio_integer_torch(
                    prepped_weights,
                    execution,
                    initial_capital=float(day_trade_execution_initial_capital),
                    state_advance_mask=state_advance_mask,
                    initial_quantities=initial_weights,
                    initial_equity_scale=initial_equity_scale,
                    initial_alive=initial_alive,
                    return_weights_history=return_weights_history,
                    recoverable_backward=futures_portfolio_recoverable_backward,
                )
            return BacktestResultTensor(
                strategy_returns=result.strategy_returns,
                benchmark_returns=benchmark_returns.to(
                    device=result.strategy_returns.device, dtype=torch.float32
                ),
                turnovers=result.turnovers,
                # This execution contract reports its recurrent ledger in
                # signed whole contracts.  Keep nominal weights only inside
                # the differentiable executor; persisting them under the
                # ``contract_quantity`` unit would make audit/replay silently
                # interpret fractions as contracts.
                weights_history=(
                    result.weights_history
                    if futures_portfolio_training_surrogate_only
                    else result.contract_quantities_history.to(dtype=torch.float32)
                    if result.contract_quantities_history is not None
                    else result.weights_history.new_empty(
                        (0, int(result.weights_history.size(-1)))
                    )
                ),
                requested_weights_history=(
                    prepped_weights if return_weights_history else None
                ),
                final_weights=result.final_weights,
                final_alive=result.final_alive,
                equity_scale_history=result.equity_scale_history,
                final_equity_scale=result.final_equity_scale,
                settlement_default=result.default_history,
                default_reason_history=result.default_reason_history,
                execution_mode=mode,
                settlement_ledger_unit=(
                    "notional_weight_training_surrogate"
                    if futures_portfolio_training_surrogate_only
                    else "contract_quantity"
                ),
            )
        holding_returns = execution[..., 0]
        executable = execution[..., 1] > 0.5
        must_liquidate = execution[..., 2] > 0.5
        fee_rates = execution[..., 3]
        state_advance = (
            torch.ones(
                (int(prepped_weights.size(0)),),
                device=prepped_weights.device,
                dtype=torch.bool,
            )
            if state_advance_mask is None
            else state_advance_mask.to(
                device=prepped_weights.device,
                dtype=torch.bool,
            )
        )
        prev_weights = (
            torch.zeros_like(prepped_weights[0])
            if initial_weights is None
            else initial_weights.to(
                device=prepped_weights.device,
                dtype=torch.float32,
            )
        )
        prev_alive = (
            torch.ones((), device=prepped_weights.device, dtype=torch.bool)
            if initial_alive is None
            else initial_alive.to(
                device=prepped_weights.device,
                dtype=torch.bool,
            ).reshape(())
        )
        runner = _resolve_futures_portfolio_runner(
            prepped_weights,
            max_turnover_ratio=float(max_turnover_ratio),
            record_weights_history=return_weights_history,
        )
        (
            strategy_returns,
            turnovers,
            weights_history,
            final_weights,
            final_alive,
        ) = runner(
            prepped_weights,
            holding_returns,
            executable,
            must_liquidate,
            fee_rates,
            state_advance,
            prev_weights,
            prev_alive,
        )
        return BacktestResultTensor(
            strategy_returns=strategy_returns,
            benchmark_returns=benchmark_returns.to(
                device=strategy_returns.device, dtype=torch.float32
            ),
            turnovers=turnovers,
            weights_history=weights_history,
            requested_weights_history=(weights if return_weights_history else None),
            final_weights=final_weights,
            final_alive=final_alive,
            execution_mode=mode,
            settlement_ledger_unit="notional_weight",
        )
    if mode == "tw_futures_portfolio_day":
        if force_exit_mask is None:
            raise ValueError(
                "tw_futures_portfolio_day requires must-liquidate rows in "
                "force_exit_mask"
            )
        if overnight_returns is None:
            raise ValueError(
                "tw_futures_portfolio_day requires aligned fixed commission "
                "rates in the executor auxiliary tensor"
            )
        if float(buy_fee_rate) != 0.0 or float(sell_fee_rate) != 0.0:
            raise ValueError(
                "tw_futures_portfolio_day fixed per-contract commission is "
                "mutually exclusive with proportional buy/sell fee rates"
            )
        prepped_weights, _, prepped_buy, prepped_sell = _prepare_scan_inputs(
            weights,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            long_only,
            gross_leverage,
            min_trade_weight,
            portfolio_activation,
        )
        result = run_tw_futures_portfolio_continuous_torch(
            prepped_weights,
            future_returns,
            prepped_buy & prepped_sell,
            force_exit_mask,
            fee_rate_per_open_notional=overnight_returns,
            max_turnover_ratio=float(max_turnover_ratio),
            volume_limit_weights=volume_limit_weights,
            state_advance_mask=state_advance_mask,
            return_weights_history=return_weights_history,
            initial_weights=initial_weights,
            initial_alive=initial_alive,
        )
        return BacktestResultTensor(
            strategy_returns=result.strategy_returns,
            benchmark_returns=benchmark_returns.to(
                device=result.strategy_returns.device, dtype=torch.float32
            ),
            turnovers=result.turnovers,
            weights_history=result.weights_history,
            requested_weights_history=(weights if return_weights_history else None),
            final_weights=result.final_weights,
            final_alive=result.final_alive,
            execution_mode=mode,
            settlement_ledger_unit="notional_weight",
        )
    if mode in TW_STOCK_FUTURES_INTEGER_DAY_TRADE_EXECUTION_MODES:
        if overnight_returns is None:
            raise ValueError(
                f"{mode} requires the standard/mini integer execution tensor"
            )
        if float(buy_fee_rate) != 0.0 or float(sell_fee_rate) != 0.0:
            raise ValueError(
                f"{mode} embeds exact per-contract commissions and tax"
            )
        if float(max_turnover_ratio) != 0.0:
            raise ValueError(f"{mode} requires max_turnover_ratio=0")
        policy_weights = torch.where(
            tradable_mask.to(device=weights.device, dtype=torch.bool),
            weights,
            torch.zeros_like(weights),
        )
        prepped_weights, _, _, _ = _prepare_scan_inputs(
            policy_weights,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            long_only,
            gross_leverage,
            min_trade_weight,
            portfolio_activation,
        )
        result = run_tw_stock_futures_day_trade_integer_torch(
            prepped_weights,
            overnight_returns,
            initial_capital=day_trade_execution_initial_capital,
            state_advance_mask=state_advance_mask,
            initial_equity_scale=initial_equity_scale,
            initial_alive=initial_alive,
            return_weights_history=return_weights_history,
        )
        return BacktestResultTensor(
            strategy_returns=result.strategy_returns,
            benchmark_returns=benchmark_returns.to(
                device=result.strategy_returns.device, dtype=torch.float32
            ),
            turnovers=result.turnovers,
            weights_history=result.weights_history,
            requested_weights_history=(weights if return_weights_history else None),
            final_weights=result.final_weights,
            final_alive=result.final_alive,
            equity_scale_history=result.equity_scale_history,
            final_equity_scale=result.final_equity_scale,
            execution_mode=mode,
            settlement_ledger_unit=None,
        )
    if mode in TW_STOCK_FUTURES_DAY_TRADE_EXECUTION_MODES:
        if overnight_returns is None:
            raise ValueError(
                f"{mode} requires aligned round-trip cost "
                "rates in the executor auxiliary tensor"
            )
        if float(buy_fee_rate) != 0.0 or float(sell_fee_rate) != 0.0:
            raise ValueError(
                f"{mode} embeds fixed commission and "
                "statutory transaction tax; proportional fee rates must be zero"
            )
        if float(max_turnover_ratio) != 0.0:
            raise ValueError(
                f"{mode} does not support a portfolio-wide "
                "turnover scaler"
            )
        # Enforce the causally known underlying-to-futures mapping before L1
        # normalization.  This keeps the full stock model output ABI while
        # making a name without a known nearby future contribute exactly zero
        # policy mass.  Current-session execution failure is applied later and
        # is deliberately not reallocated.
        policy_weights = torch.where(
            tradable_mask.to(device=weights.device, dtype=torch.bool),
            weights,
            torch.zeros_like(weights),
        )
        prepped_weights, _, prepped_buy, prepped_sell = _prepare_scan_inputs(
            policy_weights,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            long_only,
            gross_leverage,
            min_trade_weight,
            portfolio_activation,
        )
        result = run_tw_stock_futures_day_trade_continuous_torch(
            prepped_weights,
            future_returns,
            prepped_buy & prepped_sell,
            round_trip_cost_rate_per_open_notional=overnight_returns,
            volume_limit_weights=volume_limit_weights,
            state_advance_mask=state_advance_mask,
            initial_alive=initial_alive,
            return_weights_history=return_weights_history,
        )
        return BacktestResultTensor(
            strategy_returns=result.strategy_returns,
            benchmark_returns=benchmark_returns.to(
                device=result.strategy_returns.device, dtype=torch.float32
            ),
            turnovers=result.turnovers,
            weights_history=result.weights_history,
            requested_weights_history=(weights if return_weights_history else None),
            final_weights=result.final_weights,
            final_alive=result.final_alive,
            execution_mode=mode,
            settlement_ledger_unit="notional_weight",
        )
    if mode == "tw_index_derivatives_day":
        if (
            weights.dim() != 2
            or int(weights.size(1)) != TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4
        ):
            raise ValueError(
                "tw_index_derivatives_day expects direct actions [T,D_derivatives]"
            )
        rows = int(weights.size(0))
        symbols_count = int(future_returns.size(1))
        if tuple(future_returns.shape) != (rows, symbols_count):
            raise ValueError("future_returns must have shape [T,S]")
        if tuple(tradable_mask.shape) != (rows, symbols_count):
            raise ValueError("tradable_mask must have shape [T,S]")
        if overnight_returns is None or tuple(overnight_returns.shape) not in {
            (rows, TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4),
            (rows, TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4, 2),
        }:
            raise ValueError(
                "tw_index_derivatives_day requires derivative simple returns in "
                "overnight_returns [T,4102] or directional [T,4102,2]"
            )
        clean = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
        derivative_returns = overnight_returns.to(
            device=weights.device, dtype=torch.float32
        )
        if state_advance_mask is not None:
            advance = state_advance_mask.to(device=weights.device, dtype=torch.bool)
            derivative_returns = torch.where(
                advance.view(rows, *([1] * (derivative_returns.dim() - 1))),
                derivative_returns,
                torch.full_like(derivative_returns, float("nan")),
            )
        initial_alive_tensor = (
            torch.ones((), device=weights.device, dtype=torch.bool)
            if initial_alive is None
            else initial_alive.to(device=weights.device, dtype=torch.bool)
        )
        actions = torch.where(initial_alive_tensor, clean, torch.zeros_like(clean))
        continuous = run_tw_index_derivatives_day_continuous(
            actions,
            derivative_returns,
            futures_round_trip_cost_rate=float(buy_fee_rate) + float(sell_fee_rate),
            maximum_capital_fraction=min(_resolve_exposure_budget(gross_leverage), 1.0),
        )
        executed_attribution = torch.zeros(
            (rows, symbols_count), device=weights.device, dtype=torch.float32
        )
        weights_history = (
            executed_attribution
            if return_weights_history
            else executed_attribution.new_empty((0, symbols_count))
        )
        return BacktestResultTensor(
            strategy_returns=_portfolio_simple_returns_to_log_torch(
                continuous.strategy_returns
            ),
            benchmark_returns=benchmark_returns.to(
                device=weights.device, dtype=torch.float32
            ),
            turnovers=continuous.turnovers,
            weights_history=weights_history,
            requested_weights_history=(
                weights.float() if return_weights_history else None
            ),
            final_weights=torch.zeros(
                symbols_count, device=weights.device, dtype=torch.float32
            ),
            final_alive=initial_alive_tensor
            & (continuous.alive[-1] if rows else initial_alive_tensor),
            execution_mode=mode,
        )
    if mode == "tw_index_futures_day":
        if (
            weights.dim() == 2
            and int(weights.size(1)) == TAIFEX_INDEX_FUTURES_ACTION_COUNT
            and overnight_returns is not None
            and tuple(overnight_returns.shape)
            == (int(weights.size(0)), TAIFEX_INDEX_FUTURES_ACTION_COUNT, 3)
        ):
            rows = int(weights.size(0))
            symbols_count = int(future_returns.size(1))
            if tuple(future_returns.shape) != (rows, symbols_count):
                raise ValueError("future_returns must retain stock-panel shape [T,S]")
            if tuple(tradable_mask.shape) != (rows, symbols_count):
                raise ValueError("tradable_mask must retain stock-panel shape [T,S]")
            execution = overnight_returns.to(device=weights.device, dtype=torch.float32)
            if state_advance_mask is not None:
                advance = state_advance_mask.to(device=weights.device, dtype=torch.bool)
                execution = torch.where(
                    advance[:, None, None],
                    execution,
                    torch.full_like(execution, float("nan")),
                )
            direct = run_tw_index_futures_all_tenors_day_torch(
                weights,
                execution,
                initial_capital=float(day_trade_execution_initial_capital),
                max_abs_exposure=_resolve_exposure_budget(gross_leverage),
                initial_equity_scale=initial_equity_scale,
                initial_alive=initial_alive,
            )
            stock_history = torch.zeros(
                (rows, symbols_count),
                device=weights.device,
                dtype=torch.float32,
            )
            return BacktestResultTensor(
                strategy_returns=_portfolio_simple_returns_to_log_torch(
                    direct.strategy_returns
                ),
                benchmark_returns=benchmark_returns.to(
                    device=weights.device, dtype=torch.float32
                ),
                turnovers=direct.turnovers,
                weights_history=(
                    stock_history
                    if return_weights_history
                    else stock_history.new_empty((0, symbols_count))
                ),
                requested_weights_history=(
                    weights.float() if return_weights_history else None
                ),
                final_weights=torch.zeros(
                    symbols_count, device=weights.device, dtype=torch.float32
                ),
                final_alive=(
                    direct.final_alive
                    if direct.final_alive is not None
                    else torch.ones((), device=weights.device, dtype=torch.bool)
                ),
                equity_scale_history=direct.equity_scale_history,
                final_equity_scale=direct.final_equity_scale,
                execution_mode=mode,
            )
        if weights.dim() != 2:
            raise ValueError(
                "tw_index_futures_day expects canonical pseudo weights [T,S]"
            )
        if future_returns.shape != weights.shape:
            raise ValueError("future_returns must match futures pseudo weights [T,S]")
        if tradable_mask.shape != weights.shape:
            raise ValueError("tradable_mask must match futures pseudo weights [T,S]")
        selection_mask = tradable_mask.to(device=weights.device, dtype=torch.bool)
        clean_weights = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
        clean_weights = torch.where(
            selection_mask, clean_weights, torch.zeros_like(clean_weights)
        )
        requested_exposure = clean_weights.sum(dim=-1)
        if long_only:
            requested_exposure = requested_exposure.clamp_min(0.0)
        if float(min_trade_weight) > 0.0:
            requested_exposure = torch.where(
                requested_exposure.abs() >= float(min_trade_weight),
                requested_exposure,
                torch.zeros_like(requested_exposure),
            )

        raw_returns = future_returns.to(device=weights.device, dtype=torch.float32)
        valid_cells = selection_mask & torch.isfinite(raw_returns)
        valid_count = valid_cells.sum(dim=-1)
        reference_returns = torch.where(
            valid_count > 0,
            torch.where(valid_cells, raw_returns, torch.zeros_like(raw_returns)).sum(
                dim=-1
            )
            / valid_count.clamp_min(1).to(dtype=raw_returns.dtype),
            torch.zeros_like(requested_exposure, dtype=torch.float32),
        )
        futures_tradable = valid_count > 0
        if state_advance_mask is not None:
            futures_tradable = futures_tradable & state_advance_mask.to(
                device=weights.device, dtype=torch.bool
            )
        if initial_alive is not None:
            futures_tradable = futures_tradable & initial_alive.to(
                device=weights.device, dtype=torch.bool
            )
        exposure_limit = _resolve_exposure_budget(gross_leverage)
        continuous = run_tw_index_futures_day_continuous(
            requested_exposure,
            reference_returns,
            futures_tradable,
            round_trip_cost_rate=float(buy_fee_rate) + float(sell_fee_rate),
            max_abs_exposure=exposure_limit,
        )
        initial_alive_tensor = (
            torch.ones((), device=weights.device, dtype=torch.bool)
            if initial_alive is None
            else initial_alive.to(device=weights.device, dtype=torch.bool)
        )
        if int(weights.size(0)) > 0:
            survived_prior_rows = torch.cat(
                (
                    torch.ones((1,), device=weights.device, dtype=torch.bool),
                    torch.cumprod(
                        (continuous.strategy_returns[:-1] > -1.0).to(dtype=torch.int64),
                        dim=0,
                    ).to(dtype=torch.bool),
                ),
                dim=0,
            )
            alive_before_row = initial_alive_tensor & survived_prior_rows
            continuous = run_tw_index_futures_day_continuous(
                requested_exposure,
                reference_returns,
                futures_tradable & alive_before_row,
                round_trip_cost_rate=float(buy_fee_rate) + float(sell_fee_rate),
                max_abs_exposure=exposure_limit,
            )
        strategy_log_returns = _portfolio_simple_returns_to_log_torch(
            continuous.strategy_returns
        )
        # Avoid 0/0 without perturbing small but valid scalar exposures.
        scale = torch.where(
            continuous.requested_exposure != 0.0,
            continuous.executed_exposure
            / torch.where(
                continuous.requested_exposure != 0.0,
                continuous.requested_exposure,
                torch.ones_like(continuous.requested_exposure),
            ),
            torch.zeros_like(continuous.executed_exposure),
        )
        executed_pseudo_weights = clean_weights.to(
            dtype=torch.float32
        ) * scale.unsqueeze(-1)
        empty_or_history = (
            executed_pseudo_weights
            if return_weights_history
            else executed_pseudo_weights.new_empty((0, int(weights.size(-1))))
        )
        requested_history = (
            weights.to(dtype=torch.float32) if return_weights_history else None
        )
        survived = initial_alive_tensor & torch.all(continuous.strategy_returns > -1.0)
        return BacktestResultTensor(
            strategy_returns=strategy_log_returns,
            benchmark_returns=benchmark_returns.to(
                device=weights.device, dtype=torch.float32
            ),
            turnovers=continuous.turnovers,
            weights_history=empty_or_history,
            requested_weights_history=requested_history,
            final_weights=torch.zeros(
                int(weights.size(-1)), device=weights.device, dtype=torch.float32
            ),
            final_alive=survived,
            execution_mode=mode,
        )
    if mode != "naive":
        n_symbols = int(weights.size(-1))
        if buy_fee_rates is None or sell_fee_rates is None:
            raise ValueError(
                f"{mode} requires explicit per-symbol buy_fee_rates and sell_fee_rates"
            )
        if commission_rebate_rates is None:
            commission_rebate_rates = torch.zeros_like(buy_fee_rates)
        if symbol_indices is not None:
            active_symbol_indices = symbol_indices.to(
                device=buy_fee_rates.device,
                dtype=torch.long,
            )
            if tuple(active_symbol_indices.shape) != (n_symbols,):
                raise ValueError(
                    "symbol_indices must contain one index per active symbol"
                )
            buy_fee_rates = buy_fee_rates.index_select(0, active_symbol_indices)
            active_symbol_indices_sell = symbol_indices.to(
                device=sell_fee_rates.device,
                dtype=torch.long,
            )
            sell_fee_rates = sell_fee_rates.index_select(0, active_symbol_indices_sell)
            active_symbol_indices_rebate = symbol_indices.to(
                device=commission_rebate_rates.device,
                dtype=torch.long,
            )
            commission_rebate_rates = commission_rebate_rates.index_select(
                0,
                active_symbol_indices_rebate,
            )
        elif int(buy_fee_rates.numel()) != n_symbols:
            raise ValueError(
                "buy_fee_rates length differs from the active symbol dimension; "
                "symbol_indices is required for compact symbol batches"
            )
        elif int(sell_fee_rates.numel()) != n_symbols:
            raise ValueError(
                "sell_fee_rates length differs from the active symbol dimension; "
                "symbol_indices is required for compact symbol batches"
            )
        elif int(commission_rebate_rates.numel()) != n_symbols:
            raise ValueError(
                "commission_rebate_rates length differs from the active symbol "
                "dimension; symbol_indices is required for compact symbol batches"
            )
        if mode in TW_CARRYING_EXECUTION_MODES and not long_only:
            missing_short_contract = [
                name
                for name, value in (
                    ("can_short_open_mask", can_short_open_mask),
                    (
                        "can_short_open_open_mask",
                        can_short_open_open_mask
                        if weights.dim() == 3
                        else can_short_open_mask,
                    ),
                    ("short_capacity_weights", short_capacity_weights),
                    ("short_margin_rate", short_margin_rate),
                )
                if value is None
            ]
            if missing_short_contract:
                raise ValueError(
                    f"long/short {mode} requires explicit point-in-time margin-short "
                    "eligibility, demonstrated capacity, and initial-margin rates; "
                    "missing " + ", ".join(missing_short_contract)
                )
        if (
            mode in TW_CARRYING_EXECUTION_MODES
            and unresolved_corporate_action_mask is None
        ):
            raise ValueError(
                f"{mode} requires the receipt-verified official "
                "corporate-action avoidance mask"
            )
        if mode == "tw_day_trade" and day_trade_eligible_mask is None:
            raise ValueError(
                "tw_day_trade requires a point-in-time day_trade_eligible_mask"
            )
        if mode == "tw_day_trade" and (
            day_trade_can_buy_open_mask is None or day_trade_can_sell_open_mask is None
        ):
            raise ValueError(
                "tw_day_trade requires explicit point-in-time open-side buy/sell masks"
            )

        phase_execution = mode in TW_CARRYING_EXECUTION_MODES and weights.dim() == 3
        if mode == "tw_overnight" and not phase_execution:
            raise ValueError("tw_overnight requires actions with shape [T,3,S]")
        if phase_execution:
            if overnight_returns is None:
                raise ValueError(
                    f"{mode} phase execution requires prior-close-to-open "
                    "overnight_returns"
                )
            if can_buy_mask is None or can_sell_mask is None:
                raise ValueError(
                    f"{mode} phase execution requires explicit close-session "
                    "buy/sell masks"
                )
            if (
                day_trade_can_buy_open_mask is None
                or day_trade_can_sell_open_mask is None
            ):
                raise ValueError(
                    f"{mode} phase execution requires explicit point-in-time "
                    "open-session buy/sell masks"
                )
            prepped_weights = _prepare_tw_phase_actions(
                weights,
                tradable_mask,
                execution_mode=mode,
                long_only=long_only,
                gross_leverage=gross_leverage,
                min_trade_weight=min_trade_weight,
                portfolio_activation=portfolio_activation,
                symbol_sharded=symbol_sharded_ledger,
            )
            device = prepped_weights.device
            daily_tradable = tradable_mask.to(device=device, dtype=torch.bool)
            close_buy = can_buy_mask.to(device=device, dtype=torch.bool)
            close_sell = can_sell_mask.to(device=device, dtype=torch.bool)
            open_buy = day_trade_can_buy_open_mask.to(
                device=device,
                dtype=torch.bool,
            )
            open_sell = day_trade_can_sell_open_mask.to(
                device=device,
                dtype=torch.bool,
            )
            phase_tradable = daily_tradable[:, None, :].expand(-1, 2, -1)
            phase_buy = torch.stack((open_buy, close_buy), dim=1)
            phase_sell = torch.stack((open_sell, close_sell), dim=1)
            if long_only:
                phase_short_open = None
            else:
                assert can_short_open_mask is not None
                assert can_short_open_open_mask is not None
                phase_short_open = torch.stack(
                    (
                        can_short_open_open_mask.to(
                            device=device,
                            dtype=torch.bool,
                        ),
                        can_short_open_mask.to(
                            device=device,
                            dtype=torch.bool,
                        ),
                    ),
                    dim=1,
                )

            def repeated_phase_mask(
                value: torch.Tensor | None,
            ) -> torch.Tensor | None:
                if value is None:
                    return None
                daily = value.to(device=device, dtype=torch.bool)
                return daily[:, None, :].expand(-1, 2, -1)

            phase_force_exit = None
            if force_exit_mask is not None:
                close_exit = force_exit_mask.to(device=device, dtype=torch.bool)
                phase_force_exit = torch.stack(
                    (torch.zeros_like(close_exit), close_exit),
                    dim=1,
                )
            phase_margin_rate: torch.Tensor | float | None = short_margin_rate
            if (
                isinstance(short_margin_rate, torch.Tensor)
                and short_margin_rate.dim() == 2
            ):
                phase_margin_rate = short_margin_rate.to(
                    device=device,
                    dtype=prepped_weights.dtype,
                )[:, None, :].expand(-1, 2, -1)
            compile_phase_ledger = bool(
                prepped_weights.device.type == "cuda"
                and _compile_stateful_enabled()
                and _compile_enabled()
            )
            if compile_phase_ledger:
                phase_runner = (
                    run_tw_cash_dual_session_compiled
                    if mode == "tw_cash"
                    else run_tw_overnight_dual_session_compiled
                )
            else:
                phase_runner = (
                    run_tw_cash_dual_session
                    if mode == "tw_cash"
                    else run_tw_overnight_dual_session
                )
            phase_runner_extra: dict[str, bool] = {}
            if compile_phase_ledger:
                phase_runner_extra["strict_compile"] = _strict_no_fallback_enabled()
            phase_runner_extra["_symbol_sharded"] = symbol_sharded_ledger
            tw_result = phase_runner(
                prepped_weights,
                overnight_returns.to(
                    device=device,
                    dtype=torch.float32,
                ),
                future_returns.to(
                    device=device,
                    dtype=torch.float32,
                ),
                phase_tradable,
                phase_buy,
                phase_sell,
                buy_fee_rates,
                sell_fee_rates,
                commission_rebate_rates=commission_rebate_rates,
                commission_rebate_timing=commission_rebate_timing,
                session_month_ids=session_month_ids,
                commission_rebate_payment_eligible_mask=(
                    commission_rebate_payment_eligible_mask
                ),
                can_short_open_mask=phase_short_open,
                force_short_cover_mask=repeated_phase_mask(
                    None if long_only else force_short_cover_mask
                ),
                short_margin_rate=phase_margin_rate,
                short_capacity_weights=short_capacity_weights,
                short_maintenance_ratio=short_maintenance_ratio,
                short_handling_fee_rate=short_handling_fee_rate,
                unresolved_corporate_action_mask=repeated_phase_mask(
                    unresolved_corporate_action_mask
                ),
                cash_dividend_yield=cash_dividend_yield,
                cash_dividend_payment_delay_sessions=(
                    cash_dividend_payment_delay_sessions
                ),
                claim_queue_sessions=claim_queue_sessions,
                force_exit_mask=phase_force_exit,
                settlement_lag_sessions=settlement_lag_sessions,
                gross_budget=_resolve_exposure_budget(gross_leverage),
                max_turnover_ratio=max_turnover_ratio,
                volume_limit_weights=volume_limit_weights,
                state_advance_mask=state_advance_mask,
                return_weights_history=return_weights_history,
                initial_weights=initial_weights,
                initial_cash=initial_cash,
                initial_payables=initial_payables,
                initial_receivables=initial_receivables,
                initial_commission_rebate_current=(initial_commission_rebate_current),
                initial_commission_rebate_due=initial_commission_rebate_due,
                initial_commission_rebate_month_id=(initial_commission_rebate_month_id),
                initial_alive=initial_alive,
                initial_equity_scale=initial_equity_scale,
                initial_short_sale_collateral=initial_short_sale_collateral,
                initial_short_margin_collateral=initial_short_margin_collateral,
                **phase_runner_extra,
            )
        else:
            prepped_weights, prepped_tradable, prepped_buy, prepped_sell = (
                _prepare_scan_inputs(
                    weights,
                    tradable_mask,
                    can_buy_mask,
                    can_sell_mask,
                    long_only,
                    gross_leverage,
                    min_trade_weight,
                    portfolio_activation,
                    side_masks_require_tradable=(mode != "tw_cash"),
                    symbol_sharded=symbol_sharded_ledger,
                )
            )
        prepped_volume = (
            None
            if volume_limit_weights is None
            else volume_limit_weights.to(
                device=prepped_weights.device,
                dtype=prepped_weights.dtype,
            )
        )
        if phase_execution:
            pass
        elif mode == "tw_cash":
            prepped_short_open = (
                torch.zeros_like(prepped_tradable)
                if can_short_open_mask is None
                else can_short_open_mask.to(
                    device=prepped_weights.device,
                    dtype=torch.bool,
                )
                & prepped_sell
            )
            tw_result = run_tw_cash_continuous(
                prepped_weights,
                future_returns,
                prepped_tradable,
                prepped_buy,
                prepped_sell,
                buy_fee_rates,
                sell_fee_rates,
                commission_rebate_rates=commission_rebate_rates,
                commission_rebate_timing=commission_rebate_timing,
                session_month_ids=session_month_ids,
                commission_rebate_payment_eligible_mask=(
                    commission_rebate_payment_eligible_mask
                ),
                can_short_open_mask=(prepped_short_open if not long_only else None),
                force_short_cover_mask=(
                    None
                    if long_only or force_short_cover_mask is None
                    else force_short_cover_mask.to(
                        device=prepped_weights.device,
                        dtype=torch.bool,
                    )
                ),
                short_margin_rate=short_margin_rate,
                short_capacity_weights=short_capacity_weights,
                short_maintenance_ratio=short_maintenance_ratio,
                short_handling_fee_rate=short_handling_fee_rate,
                unresolved_corporate_action_mask=(
                    None
                    if unresolved_corporate_action_mask is None
                    else unresolved_corporate_action_mask.to(
                        device=prepped_weights.device,
                        dtype=torch.bool,
                    )
                ),
                cash_dividend_yield=cash_dividend_yield,
                cash_dividend_payment_delay_sessions=(
                    cash_dividend_payment_delay_sessions
                ),
                claim_queue_sessions=claim_queue_sessions,
                settlement_lag_sessions=settlement_lag_sessions,
                gross_budget=_resolve_exposure_budget(gross_leverage),
                max_turnover_ratio=max_turnover_ratio,
                volume_limit_weights=prepped_volume,
                force_exit_mask=force_exit_mask,
                state_advance_mask=state_advance_mask,
                return_weights_history=return_weights_history,
                initial_weights=initial_weights,
                initial_cash=initial_cash,
                initial_payables=initial_payables,
                initial_receivables=initial_receivables,
                initial_commission_rebate_current=(initial_commission_rebate_current),
                initial_commission_rebate_due=initial_commission_rebate_due,
                initial_commission_rebate_month_id=(initial_commission_rebate_month_id),
                initial_alive=initial_alive,
                initial_equity_scale=initial_equity_scale,
                initial_short_sale_collateral=initial_short_sale_collateral,
                initial_short_margin_collateral=initial_short_margin_collateral,
            )
        elif mode == "tw_day_trade":
            prepped_buy_open = day_trade_can_buy_open_mask.to(
                device=prepped_weights.device,
                dtype=torch.bool,
            )
            prepped_sell_open = day_trade_can_sell_open_mask.to(
                device=prepped_weights.device,
                dtype=torch.bool,
            )
            if can_short_open_mask is None:
                if not long_only:
                    raise ValueError(
                        "long/short tw_day_trade requires an explicit can_short_open_mask"
                    )
                prepped_short_open = torch.zeros_like(prepped_tradable)
            else:
                prepped_short_open = (
                    can_short_open_mask.to(
                        device=prepped_weights.device,
                        dtype=torch.bool,
                    )
                    & prepped_sell_open
                )
            if overnight_returns is not None and overnight_returns.dim() == 3:
                if day_trade_unlimited_margin_conversion:
                    raise ValueError(
                        "stateful carry tw_day_trade cannot use the legacy "
                        "minute tape executor because it discards per-symbol "
                        "residual holdings; use the causal daily OPEN/CLOSE "
                        "ledger until a stateful minute ledger is available"
                    )
                if normal_sell_fee_rates is None:
                    raise ValueError(
                        "exact-minute tw_day_trade requires explicit normal "
                        "sell fee rates for residual margin conversion"
                    )
                active_normal_sell_fees = normal_sell_fee_rates
                if symbol_indices is not None:
                    active_normal_sell_fees = normal_sell_fee_rates.index_select(
                        0,
                        symbol_indices.to(
                            device=normal_sell_fee_rates.device,
                            dtype=torch.long,
                        ),
                    )
                elif int(normal_sell_fee_rates.numel()) != n_symbols:
                    raise ValueError(
                        "normal_sell_fee_rates length differs from the active "
                        "symbol dimension; symbol_indices is required"
                    )
                minute_result = run_tw_day_trade_minute_execution(
                    prepped_weights,
                    overnight_returns,
                    prepped_tradable,
                    prepped_short_open,
                    day_trade_eligible_mask.to(
                        device=prepped_weights.device, dtype=torch.bool
                    ),
                    prepped_buy_open,
                    prepped_sell_open,
                    buy_fee_rates,
                    sell_fee_rates,
                    active_normal_sell_fees,
                    initial_capital_twd=day_trade_execution_initial_capital,
                    settlement_lag_sessions=settlement_lag_sessions,
                    state_advance_mask=state_advance_mask,
                    initial_cash=initial_cash,
                    initial_payables=initial_payables,
                    initial_receivables=initial_receivables,
                    initial_equity_scale=initial_equity_scale,
                )
                symbols = int(prepped_weights.size(1))
                rows = int(prepped_weights.size(0))
                zero_rows = torch.zeros(
                    rows,
                    device=minute_result.strategy_returns.device,
                    dtype=torch.float32,
                )
                return BacktestResultTensor(
                    strategy_returns=minute_result.strategy_returns,
                    benchmark_returns=benchmark_returns.to(
                        device=minute_result.strategy_returns.device,
                        dtype=minute_result.strategy_returns.dtype,
                    ),
                    turnovers=minute_result.turnovers,
                    weights_history=(
                        minute_result.weights_history
                        if return_weights_history
                        else minute_result.weights_history.new_empty((0, symbols))
                    ),
                    requested_weights_history=(
                        weights if return_weights_history else None
                    ),
                    final_weights=torch.zeros(
                        symbols,
                        device=minute_result.strategy_returns.device,
                        dtype=torch.float32,
                    ),
                    final_alive=minute_result.final_equity_scale > 0.0,
                    execution_mode=mode,
                    settlement_ledger_unit="nav_ratio",
                    cash_history=minute_result.cash_history,
                    payables_history=minute_result.payables_history,
                    receivables_history=minute_result.receivables_history,
                    settlement_default=torch.zeros(
                        rows,
                        device=minute_result.strategy_returns.device,
                        dtype=torch.bool,
                    ),
                    equity_scale_history=minute_result.equity_scale_history,
                    final_cash=minute_result.final_cash,
                    final_payables=minute_result.final_payables,
                    final_receivables=minute_result.final_receivables,
                    commission_rebate_accrued_history=zero_rows,
                    commission_rebate_paid_history=zero_rows.clone(),
                    commission_rebate_current_history=zero_rows.clone(),
                    commission_rebate_due_history=zero_rows.clone(),
                    final_commission_rebate_current=zero_rows.new_zeros(()),
                    final_commission_rebate_due=zero_rows.new_zeros(()),
                    final_commission_rebate_month_id=torch.zeros(
                        (),
                        device=minute_result.strategy_returns.device,
                        dtype=torch.long,
                    ),
                    final_equity_scale=minute_result.final_equity_scale,
                )
            if day_trade_unlimited_margin_conversion:
                if normal_sell_fee_rates is None:
                    raise ValueError(
                        "stateful carry tw_day_trade requires normal sell fee "
                        "rates for residual position accounting"
                    )
                active_normal_sell_fees = normal_sell_fee_rates
                if symbol_indices is not None:
                    active_normal_sell_fees = normal_sell_fee_rates.index_select(
                        0,
                        symbol_indices.to(
                            device=normal_sell_fee_rates.device,
                            dtype=torch.long,
                        ),
                    )
                elif int(normal_sell_fee_rates.numel()) != n_symbols:
                    raise ValueError(
                        "normal_sell_fee_rates length differs from the active "
                        "symbol dimension; symbol_indices is required"
                    )
                if overnight_returns is None or overnight_returns.dim() != 2:
                    raise ValueError(
                        "stateful carry tw_day_trade requires causal "
                        "prior-close-to-open returns [T,S]; the legacy minute "
                        "residual-conversion tape cannot represent recurrent holdings"
                    )
                if short_margin_rate is None or short_capacity_weights is None:
                    raise ValueError(
                        "long/short stateful carry tw_day_trade requires explicit "
                        "margin rates and an infinity-valued disabled-capacity tensor"
                    )
                if unresolved_corporate_action_mask is None:
                    raise ValueError(
                        "stateful carry tw_day_trade requires a receipt-verified "
                        "corporate-action avoidance mask"
                    )

                # OPEN is the learned post-event target. CLOSE always requests
                # zero. A blocked close leg therefore remains in the recurrent
                # ledger; after the next overnight mark the following OPEN
                # transition executes only target[t] - holdings[t-1].
                carry_actions = torch.stack(
                    (prepped_weights, torch.zeros_like(prepped_weights)),
                    dim=1,
                )
                phase_tradable = prepped_tradable[:, None, :].expand(-1, 2, -1)
                phase_buy = torch.stack((prepped_buy_open, prepped_buy), dim=1)
                phase_sell = torch.stack((prepped_sell_open, prepped_sell), dim=1)
                open_short = (
                    prepped_short_open
                    if can_short_open_open_mask is None
                    else can_short_open_open_mask.to(
                        device=prepped_weights.device,
                        dtype=torch.bool,
                    )
                    & prepped_sell_open
                )
                phase_short_open = torch.stack(
                    (open_short, torch.zeros_like(open_short)),
                    dim=1,
                )
                repeated_corporate_action = (
                    unresolved_corporate_action_mask.to(
                        device=prepped_weights.device,
                        dtype=torch.bool,
                    )[:, None, :].expand(-1, 2, -1)
                )
                phase_force_exit = None
                if force_exit_mask is not None:
                    close_force_exit = force_exit_mask.to(
                        device=prepped_weights.device,
                        dtype=torch.bool,
                    )
                    phase_force_exit = torch.stack(
                        (torch.zeros_like(close_force_exit), close_force_exit),
                        dim=1,
                    )
                phase_force_cover = None
                if force_short_cover_mask is not None:
                    force_cover = force_short_cover_mask.to(
                        device=prepped_weights.device,
                        dtype=torch.bool,
                    )
                    phase_force_cover = force_cover[:, None, :].expand(-1, 2, -1)
                phase_margin_rate: torch.Tensor | float = short_margin_rate
                if (
                    isinstance(short_margin_rate, torch.Tensor)
                    and short_margin_rate.dim() == 2
                ):
                    phase_margin_rate = short_margin_rate.to(
                        device=prepped_weights.device,
                        dtype=torch.float32,
                    )[:, None, :].expand(-1, 2, -1)
                compile_carry_ledger = bool(
                    prepped_weights.device.type == "cuda"
                    and _compile_stateful_enabled()
                    and _compile_enabled()
                )
                carry_runner = (
                    run_tw_cash_dual_session_compiled
                    if compile_carry_ledger
                    else run_tw_cash_dual_session
                )
                carry_extra: dict[str, bool] = {}
                if compile_carry_ledger:
                    carry_extra["strict_compile"] = _strict_no_fallback_enabled()
                carry_extra["_symbol_sharded"] = symbol_sharded_ledger
                tw_result = carry_runner(
                    carry_actions,
                    overnight_returns.to(
                        device=prepped_weights.device,
                        dtype=torch.float32,
                    ),
                    future_returns.to(
                        device=prepped_weights.device,
                        dtype=torch.float32,
                    ),
                    phase_tradable,
                    phase_buy,
                    phase_sell,
                    buy_fee_rates,
                    sell_fee_rates,
                    commission_rebate_rates=commission_rebate_rates,
                    commission_rebate_timing=commission_rebate_timing,
                    session_month_ids=session_month_ids,
                    commission_rebate_payment_eligible_mask=(
                        commission_rebate_payment_eligible_mask
                    ),
                    can_short_open_mask=phase_short_open,
                    force_short_cover_mask=phase_force_cover,
                    short_margin_rate=phase_margin_rate,
                    short_capacity_weights=short_capacity_weights,
                    short_handling_fee_rate=0.0,
                    day_trade_carry_normal_sell_fee_rates=(
                        active_normal_sell_fees
                    ),
                    day_trade_margin_financing_ratio=(
                        day_trade_margin_financing_ratio
                    ),
                    day_trade_margin_financing_annual_rate=(
                        day_trade_margin_financing_annual_rate
                    ),
                    day_trade_margin_short_handling_fee_rate=(
                        day_trade_margin_short_handling_fee_rate
                    ),
                    day_trade_margin_short_annual_borrow_rate=(
                        day_trade_margin_short_annual_borrow_rate
                    ),
                    short_maintenance_ratio=short_maintenance_ratio,
                    unresolved_corporate_action_mask=(
                        repeated_corporate_action
                    ),
                    cash_dividend_yield=cash_dividend_yield,
                    cash_dividend_payment_delay_sessions=(
                        cash_dividend_payment_delay_sessions
                    ),
                    claim_queue_sessions=claim_queue_sessions,
                    force_exit_mask=phase_force_exit,
                    settlement_lag_sessions=settlement_lag_sessions,
                    gross_budget=_resolve_exposure_budget(gross_leverage),
                    max_turnover_ratio=max_turnover_ratio,
                    volume_limit_weights=prepped_volume,
                    state_advance_mask=state_advance_mask,
                    return_weights_history=return_weights_history,
                    initial_weights=initial_weights,
                    initial_cash=initial_cash,
                    initial_payables=initial_payables,
                    initial_receivables=initial_receivables,
                    initial_commission_rebate_current=(
                        initial_commission_rebate_current
                    ),
                    initial_commission_rebate_due=initial_commission_rebate_due,
                    initial_commission_rebate_month_id=(
                        initial_commission_rebate_month_id
                    ),
                    initial_alive=initial_alive,
                    initial_equity_scale=initial_equity_scale,
                    initial_short_sale_collateral=(
                        initial_short_sale_collateral
                    ),
                    initial_short_margin_collateral=(
                        initial_short_margin_collateral
                    ),
                    initial_long_margin_debt=initial_long_margin_debt,
                    **carry_extra,
                )
                # Continue through the shared BacktestResultTensor adapter so
                # train/eval/chunk state propagation uses the same final state.
            else:
                tw_result = run_tw_day_trade_continuous(
                    prepped_weights,
                    future_returns,
                    prepped_tradable,
                    prepped_buy,
                    prepped_sell,
                    prepped_short_open,
                    day_trade_eligible_mask.to(
                        device=prepped_weights.device,
                        dtype=torch.bool,
                    ),
                    buy_fee_rates,
                    sell_fee_rates,
                    commission_rebate_rates=commission_rebate_rates,
                    commission_rebate_timing=commission_rebate_timing,
                    session_month_ids=session_month_ids,
                    commission_rebate_payment_eligible_mask=(
                        commission_rebate_payment_eligible_mask
                    ),
                    day_trade_can_buy_open_mask=prepped_buy_open,
                    day_trade_can_sell_open_mask=prepped_sell_open,
                    settlement_lag_sessions=settlement_lag_sessions,
                    max_turnover_ratio=max_turnover_ratio,
                    volume_limit_weights=prepped_volume,
                    force_short_cover_mask=force_short_cover_mask,
                    force_exit_mask=force_exit_mask,
                    state_advance_mask=state_advance_mask,
                    return_weights_history=return_weights_history,
                    initial_cash=initial_cash,
                    initial_payables=initial_payables,
                    initial_receivables=initial_receivables,
                    initial_commission_rebate_current=(initial_commission_rebate_current),
                    initial_commission_rebate_due=initial_commission_rebate_due,
                    initial_commission_rebate_month_id=(initial_commission_rebate_month_id),
                    initial_alive=initial_alive,
                    initial_equity_scale=initial_equity_scale,
                )
        else:
            raise AssertionError(f"unhandled execution mode {mode!r}")
        return BacktestResultTensor(
            # The model may run under BF16/FP16 AMP, but settlement state and
            # finance reductions remain FP32.  Do not quantize recurrent cash
            # claims or returns back to the model-output dtype.
            strategy_returns=tw_result.strategy_returns,
            benchmark_returns=benchmark_returns.to(
                device=tw_result.strategy_returns.device,
                dtype=tw_result.strategy_returns.dtype,
            ),
            turnovers=tw_result.turnovers,
            weights_history=tw_result.weights_history,
            requested_weights_history=(weights if return_weights_history else None),
            open_weights_history=getattr(
                tw_result,
                "open_weights_history",
                None,
            ),
            close_weights_history=getattr(
                tw_result,
                "close_weights_history",
                None,
            ),
            event_turnovers=getattr(tw_result, "event_turnovers", None),
            executed_buy_weights=getattr(
                tw_result,
                "executed_buy_weights",
                None,
            ),
            executed_sell_weights=getattr(
                tw_result,
                "executed_sell_weights",
                None,
            ),
            executed_long_buy_weights=getattr(
                tw_result,
                "executed_long_buy_weights",
                None,
            ),
            executed_long_sell_weights=getattr(
                tw_result,
                "executed_long_sell_weights",
                None,
            ),
            executed_short_open_weights=getattr(
                tw_result,
                "executed_short_open_weights",
                None,
            ),
            executed_short_cover_weights=getattr(
                tw_result,
                "executed_short_cover_weights",
                None,
            ),
            due_weights_history=getattr(
                tw_result,
                "due_weights_history",
                None,
            ),
            final_due_weights=getattr(tw_result, "final_due_weights", None),
            final_weights=tw_result.final_weights,
            final_alive=tw_result.final_alive,
            execution_mode=mode,
            settlement_ledger_unit="nav_ratio",
            cash_history=tw_result.cash_history,
            payables_history=tw_result.payables_history,
            receivables_history=tw_result.receivables_history,
            settlement_default=tw_result.settlement_default,
            default_reason_history=getattr(
                tw_result,
                "default_reason_history",
                None,
            ),
            equity_scale_history=tw_result.equity_scale_history,
            final_cash=tw_result.final_cash,
            final_payables=tw_result.final_payables,
            final_receivables=tw_result.final_receivables,
            commission_rebate_accrued_history=getattr(
                tw_result,
                "commission_rebate_accrued_history",
                None,
            ),
            commission_rebate_paid_history=getattr(
                tw_result,
                "commission_rebate_paid_history",
                None,
            ),
            commission_rebate_current_history=getattr(
                tw_result,
                "commission_rebate_current_history",
                None,
            ),
            commission_rebate_due_history=getattr(
                tw_result,
                "commission_rebate_due_history",
                None,
            ),
            final_commission_rebate_current=getattr(
                tw_result,
                "final_commission_rebate_current",
                None,
            ),
            final_commission_rebate_due=getattr(
                tw_result,
                "final_commission_rebate_due",
                None,
            ),
            final_commission_rebate_month_id=getattr(
                tw_result,
                "final_commission_rebate_month_id",
                None,
            ),
            final_equity_scale=tw_result.final_equity_scale,
            short_sale_collateral_history=tw_result.short_sale_collateral_history,
            short_margin_collateral_history=tw_result.short_margin_collateral_history,
            final_short_sale_collateral=tw_result.final_short_sale_collateral,
            final_short_margin_collateral=tw_result.final_short_margin_collateral,
            long_margin_debt_history=getattr(
                tw_result,
                "long_margin_debt_history",
                None,
            ),
            final_long_margin_debt=getattr(
                tw_result,
                "final_long_margin_debt",
                None,
            ),
        )

    strategy_returns, turnovers, weights_history, final_weights, final_alive = (
        _vectorized_backtest_torch(
            weights,
            future_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            can_short_open_mask,
            force_short_cover_mask,
            force_exit_mask,
            buy_fee_rate,
            sell_fee_rate,
            long_only=long_only,
            max_turnover_ratio=max_turnover_ratio,
            gross_leverage=gross_leverage,
            min_trade_weight=min_trade_weight,
            portfolio_activation=portfolio_activation,
            scan_chunk_size=scan_chunk_size,
            return_weights_history=return_weights_history,
            initial_weights=initial_weights,
            initial_alive=initial_alive,
            volume_limit_weights=volume_limit_weights,
        )
    )

    return BacktestResultTensor(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns.to(
            device=strategy_returns.device, dtype=strategy_returns.dtype
        ),
        turnovers=turnovers,
        weights_history=weights_history,
        requested_weights_history=(weights if return_weights_history else None),
        final_weights=final_weights,
        final_alive=final_alive,
        execution_mode=mode,
    )


def _select_integer_symbol_parameter(
    name: str,
    value: np.ndarray | float | int,
    *,
    n_symbols: int,
    symbol_indices: np.ndarray | None,
) -> np.ndarray | float | int:
    """Resolve a scalar/active/full-universe vector without implicit truncation."""

    raw = np.asarray(value)
    if raw.ndim == 0:
        return raw.item()
    if raw.ndim != 1:
        raise ValueError(f"{name} must be scalar or one-dimensional")
    if symbol_indices is None:
        if raw.shape == (n_symbols,):
            return raw
        raise ValueError(
            f"{name} length differs from the active symbol count; "
            "symbol_indices is required for a full-universe vector"
        )
    indices_raw = np.asarray(symbol_indices)
    if indices_raw.shape != (n_symbols,) or not np.issubdtype(
        indices_raw.dtype, np.integer
    ):
        raise ValueError(f"symbol_indices must be integer shape [{n_symbols}]")
    indices = indices_raw.astype(np.int64, copy=False)
    if np.any(indices < 0) or np.any(indices >= raw.size):
        raise ValueError(f"symbol_indices contains an out-of-range index for {name}")
    return raw[indices]


def _select_integer_time_symbol_parameter(
    name: str,
    value: np.ndarray | float | int,
    *,
    n_rows: int,
    n_symbols: int,
    symbol_indices: np.ndarray | None,
) -> np.ndarray | float | int:
    """Resolve scalar/[S]/[T,S] point-in-time execution parameters."""

    raw = np.asarray(value)
    if raw.ndim <= 1:
        return _select_integer_symbol_parameter(
            name,
            value,
            n_symbols=n_symbols,
            symbol_indices=symbol_indices,
        )
    if raw.ndim != 2 or int(raw.shape[0]) != int(n_rows):
        raise ValueError(f"{name} must be scalar, [S], or [T,S]")
    if raw.shape[1] == n_symbols:
        return raw
    if symbol_indices is None:
        raise ValueError(
            f"{name} symbol dimension differs from the active symbol count; "
            "symbol_indices is required for a full-universe matrix"
        )
    indices = np.asarray(symbol_indices)
    if indices.shape != (n_symbols,) or not np.issubdtype(indices.dtype, np.integer):
        raise ValueError(f"symbol_indices must be integer shape [{n_symbols}]")
    indices = indices.astype(np.int64, copy=False)
    if np.any(indices < 0) or np.any(indices >= raw.shape[1]):
        raise ValueError(f"symbol_indices contains an out-of-range index for {name}")
    return raw[:, indices]


def _tw_integer_public_result(
    result: TaiwanIntegerBacktestResult | TaiwanDualSessionIntegerBacktestResult,
    benchmark_returns: np.ndarray,
) -> BacktestResult:
    state = result.final_state
    return BacktestResult(
        strategy_returns=result.strategy_returns,
        benchmark_returns=np.asarray(benchmark_returns, dtype=np.float32),
        turnovers=result.turnover,
        weights_history=result.weights,
        execution_mode=state.mode,
        settlement_ledger_unit="currency",
        cash_history=result.settled_cash_history,
        # Preserve the complete due-session ledger.  The scalar
        # ``payable_history``/``receivable_history`` fields on the NumPy oracle
        # are convenient totals, but they discard which claim is due at T+1
        # versus T+2.  Public artifacts and settlement audits need the queue
        # history to be replayable and comparable with the continuous executor.
        payables_history=result.payable_queue_history,
        receivables_history=result.receivable_queue_history,
        settlement_default=result.default_mask,
        final_cash=np.asarray(state.settled_cash, dtype=np.float64),
        final_payables=state.payable_queue.copy(),
        final_receivables=state.receivable_queue.copy(),
        commission_rebate_accrued_history=getattr(
            result,
            "commission_rebate_accrued_history",
            None,
        ),
        commission_rebate_paid_history=getattr(
            result,
            "commission_rebate_paid_history",
            None,
        ),
        commission_rebate_current_history=getattr(
            result,
            "commission_rebate_current_history",
            None,
        ),
        commission_rebate_due_history=getattr(
            result,
            "commission_rebate_due_history",
            None,
        ),
        final_commission_rebate_current=np.asarray(
            state.commission_rebate_current,
            dtype=np.float64,
        ),
        final_commission_rebate_due=np.asarray(
            state.commission_rebate_due,
            dtype=np.float64,
        ),
        final_commission_rebate_month_id=np.asarray(
            state.commission_rebate_month_id,
            dtype=np.int64,
        ),
        short_sale_collateral_history=getattr(
            result, "short_sale_collateral_history", None
        ),
        short_margin_collateral_history=getattr(
            result, "short_margin_collateral_history", None
        ),
        final_short_sale_collateral=(
            None
            if state.short_sale_collateral is None
            else state.short_sale_collateral.copy()
        ),
        final_short_margin_collateral=(
            None
            if state.short_margin_collateral is None
            else state.short_margin_collateral.copy()
        ),
        final_alive=np.asarray(state.alive, dtype=np.bool_),
        final_weights=result.final_weights.copy(),
        shares_history=result.positions,
        final_integer_state=state,
        open_weights_history=(
            result.open_weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        close_weights_history=(
            result.weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        event_turnovers=(
            result.event_turnovers.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        executed_buy_weights=(
            result.executed_buy_weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        executed_sell_weights=(
            result.executed_sell_weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        executed_long_buy_weights=(
            result.executed_long_buy_weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        executed_long_sell_weights=(
            result.executed_long_sell_weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        executed_short_open_weights=(
            result.executed_short_open_weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        executed_short_cover_weights=(
            result.executed_short_cover_weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            else None
        ),
        due_weights_history=(
            result.weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            and result.mode == "tw_overnight"
            else None
        ),
        final_due_weights=(
            result.final_weights.copy()
            if isinstance(result, TaiwanDualSessionIntegerBacktestResult)
            and result.mode == "tw_overnight"
            else None
        ),
    )


def _tw_integer_holdings_records(
    result: TaiwanIntegerBacktestResult | TaiwanDualSessionIntegerBacktestResult,
    *,
    close_prices: np.ndarray,
    open_prices: np.ndarray | None = None,
    symbols: list[str],
    dates: np.ndarray | None,
) -> list[HoldingsRecord]:
    """Render cash holdings and exact per-symbol execution detail.

    Cash execution emits end-of-session positions.  A day-trade account is flat
    at every close, so its non-cash rows are explicitly labeled
    ``day_trade_open`` and record the signed quantity and opening execution
    price instead of pretending those quantities are overnight holdings.
    """

    t_len, n_symbols = result.positions.shape
    if len(symbols) != n_symbols:
        raise ValueError(f"symbols must contain exactly {n_symbols} entries")
    if np.asarray(close_prices).shape != (t_len, n_symbols):
        raise ValueError(f"close_prices must have shape [{t_len}, {n_symbols}]")
    mode = str(result.final_state.mode)
    open_values: np.ndarray | None = None
    if mode == "tw_day_trade":
        if open_prices is None:
            raise ValueError("tw_day_trade holdings records require open_prices")
        open_values = np.asarray(open_prices, dtype=np.float64)
        if open_values.shape != (t_len, n_symbols):
            raise ValueError(f"open_prices must have shape [{t_len}, {n_symbols}]")
    if dates is None:
        date_text = [f"t{idx:04d}" for idx in range(t_len)]
    else:
        date_values = np.asarray(dates)
        if date_values.shape != (t_len,):
            raise ValueError(f"dates must have shape [{t_len}]")
        date_text = [
            str(np.datetime_as_string(np.asarray(day, dtype="datetime64[D]"), unit="D"))
            for day in date_values
        ]

    records: list[HoldingsRecord] = []
    for t in range(t_len):
        # Use claims from the same execution/mark boundary as
        # ``execution_nav_history`` and ``weights``.  The end-of-row queue may
        # already include a newly earned dividend receivable and therefore
        # belongs to a later accounting instant.
        free_cash_and_claims = float(
            result.settled_cash_history[t]
            + result.execution_receivable_history[t]
            - result.execution_payable_history[t]
        )
        sale_collateral_history = getattr(result, "short_sale_collateral_history", None)
        margin_collateral_history = getattr(
            result, "short_margin_collateral_history", None
        )
        sale_collateral = (
            0.0
            if sale_collateral_history is None
            else float(np.sum(sale_collateral_history[t]))
        )
        margin_collateral = (
            0.0
            if margin_collateral_history is None
            else float(np.sum(margin_collateral_history[t]))
        )
        nonzero = np.flatnonzero(result.positions[t] != 0)
        if mode in TW_CARRYING_EXECUTION_MODES:
            # ``strategy_returns[t]`` already includes close[t] -> the next
            # causal valuation mark, whereas holdings are audited at close[t].
            # A halted holding has no raw execution quote on that row, so derive
            # its already-validated stale mark from the oracle's execution-time
            # NAV and weights.  This never creates an executable price.
            execution_nav = float(result.execution_nav_history[t])
            market_values = (
                np.asarray(result.weights[t, nonzero], dtype=np.float64) * execution_nav
            )
            reconstructed_prices = np.divide(
                market_values,
                result.positions[t, nonzero].astype(np.float64, copy=False),
            )
            invalid_marks = (
                ~np.isfinite(market_values)
                | (market_values == 0.0)
                | ~np.isfinite(reconstructed_prices)
                | (reconstructed_prices <= 0.0)
            )
            if np.any(invalid_marks):
                invalid_symbols = nonzero[invalid_marks]
                raise ValueError(
                    "integer cash result has invalid held-symbol valuation marks "
                    f"at time index {t}, symbol indices "
                    f"{invalid_symbols[:8].tolist()}"
                )
            expected_nav = (
                free_cash_and_claims
                + sale_collateral
                + margin_collateral
                + float(np.sum(market_values))
            )
            if not np.isclose(
                execution_nav,
                expected_nav,
                rtol=2e-12,
                atol=1e-7,
            ):
                raise RuntimeError(
                    "integer cash holdings report violates the execution-time "
                    "asset-liability identity"
                )
            cash_snapshot_value = free_cash_and_claims
        else:
            assert open_values is not None
            # These are opening executions, not closing holdings.  Use the
            # exact integer quantity and opening quote that produced the public
            # shares/weights histories.  Session-t close must not alter them.
            execution_nav = float(result.execution_nav_history[t])
            reconstructed_prices = open_values[t, nonzero]
            market_values = (
                result.positions[t, nonzero].astype(np.float64, copy=False)
                * reconstructed_prices
            )
            invalid_entries = (
                ~np.isfinite(reconstructed_prices)
                | (reconstructed_prices <= 0.0)
                | ~np.isfinite(market_values)
            )
            if np.any(invalid_entries):
                raise ValueError(
                    "integer day-trade result has invalid opening execution "
                    f"prices at time index {t}, symbol indices "
                    f"{nonzero[invalid_entries][:8].tolist()}"
                )
            expected_weights = np.divide(
                market_values,
                execution_nav,
                out=np.zeros_like(market_values),
                where=execution_nav > 0.0,
            )
            if not np.allclose(
                expected_weights,
                result.weights[t, nonzero],
                rtol=2e-12,
                atol=1e-12,
            ):
                raise RuntimeError(
                    "integer day-trade opening records disagree with execution weights"
                )
            # Synthetic cash at the opening snapshot makes signed stock values
            # plus cash reconcile exactly to opening NAV. Settlement cash and
            # T+2 claims remain in the dedicated settlement audit artifact.
            cash_snapshot_value = execution_nav - float(np.sum(market_values))
        denominator = execution_nav if execution_nav > 0.0 else 1.0
        day_rows = [
            HoldingsRecord(
                date=date_text[t],
                symbol="CASH",
                shares=int(_trunc_to_int64(cash_snapshot_value).item()),
                price=1.0,
                market_value=cash_snapshot_value,
                holding_ratio=(
                    cash_snapshot_value / denominator if execution_nav > 0.0 else 0.0
                ),
                is_cash=True,
                record_type="cash",
            )
        ]
        for label, value in (
            ("SHORT_SALE_COLLATERAL", sale_collateral),
            ("SHORT_MARGIN_COLLATERAL", margin_collateral),
        ):
            if value > 0.0:
                day_rows.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol=label,
                        shares=int(_trunc_to_int64(value).item()),
                        price=1.0,
                        market_value=value,
                        holding_ratio=value / denominator,
                        is_cash=True,
                        record_type="collateral",
                    )
                )
        if execution_nav > 0.0:
            for offset, idx in enumerate(nonzero.tolist()):
                market_value = float(market_values[offset])
                signed_shares = int(result.positions[t, idx])
                day_rows.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol=symbols[idx],
                        shares=signed_shares,
                        price=float(reconstructed_prices[offset]),
                        market_value=market_value,
                        holding_ratio=market_value / denominator,
                        is_cash=False,
                        record_type=(
                            "position"
                            if result.final_state.mode in TW_CARRYING_EXECUTION_MODES
                            else "day_trade_open"
                        ),
                        side=(
                            ""
                            if result.final_state.mode in TW_CARRYING_EXECUTION_MODES
                            else "BUY"
                            if signed_shares > 0
                            else "SELL"
                        ),
                    )
                )
        day_rows.sort(key=holding_record_abs_sort_key)
        records.extend(day_rows)
    return records


def run_backtest_integer_shares(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    can_buy_mask: np.ndarray | None = None,
    can_sell_mask: np.ndarray | None = None,
    can_short_open_mask: np.ndarray | None = None,
    force_short_cover_mask: np.ndarray | None = None,
    force_exit_mask: np.ndarray | None = None,
    *,
    initial_capital: float = 1_000_000.0,
    buy_fee_rate: float = 0.001425,
    sell_fee_rate: float = 0.004425,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    max_volume_participation: float = 0.0,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    close_prices: np.ndarray | None = None,
    daily_volumes: np.ndarray | None = None,
    cash_close_volume_reference: np.ndarray | None = None,
    day_trade_entry_volume_reference: np.ndarray | None = None,
    symbols: list[str] | None = None,
    dates: np.ndarray | None = None,
    collect_holdings: bool = True,
    precomputed_exact_backtest: BacktestResult | None = None,
    execution_mode: str = "naive",
    buy_fee_rates: np.ndarray | None = None,
    sell_fee_rates: np.ndarray | None = None,
    commission_rebate_rates: np.ndarray | float = 0.0,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: np.ndarray | None = None,
    commission_rebate_payment_eligible_mask: np.ndarray | None = None,
    minimum_commission: float = 0.0,
    commission_rounding: str = "none",
    tax_rounding: str = "none",
    lot_sizes: np.ndarray | int | None = None,
    short_lot_sizes: np.ndarray | int | None = None,
    short_margin_rate: np.ndarray | float | None = None,
    short_capacity_shares: np.ndarray | None = None,
    short_maintenance_ratio: float = 1.30,
    short_handling_fee_rate: np.ndarray | float = 0.0,
    open_prices: np.ndarray | None = None,
    can_short_open_open_mask: np.ndarray | None = None,
    day_trade_eligible_mask: np.ndarray | None = None,
    day_trade_can_buy_open_mask: np.ndarray | None = None,
    day_trade_can_sell_open_mask: np.ndarray | None = None,
    unresolved_corporate_action_mask: np.ndarray | None = None,
    cash_dividend_yield: np.ndarray | None = None,
    cash_dividend_payment_delay_sessions: np.ndarray | None = None,
    claim_queue_sessions: int | None = None,
    state_advance_mask: np.ndarray | None = None,
    settlement_lag_sessions: int = 2,
    initial_integer_state: TaiwanIntegerState | None = None,
    symbol_indices: np.ndarray | None = None,
    symbols_are_full_universe: bool = False,
    futures_market: TaiwanIndexFuturesDaySession | None = None,
    futures_cost_schedule: FuturesCostSchedule | None = None,
    futures_max_abs_exposure: float | None = None,
    options_chain_market: TaiwanIndexOptionChainDaySession | None = None,
    derivatives_day_candidates: TaiwanIndexDerivativeDayCandidates | None = None,
    option_day_cost_schedule: OptionDayCostSchedule | None = None,
    derivatives_maximum_capital_fraction: float = 0.98,
    futures_fee_rates: np.ndarray | None = None,
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    """Integer-share audit backtest for naive or Taiwan settlement execution.

    Naive-mode assumptions (kept backward compatible):
    - Initial capital is cash only.
    - Stock shares are integer lots: floor(target_value / current_price).
    - Buy and sell fees are charged separately by buy_fee_rate/sell_fee_rate.
    - Cash is a virtual asset with 0 daily return.

    Taiwan modes require explicit per-symbol fee vectors and exact prices.
    ``tw_cash`` accepts either the legacy close-only ``[T,S]`` request or the
    canonical dual-session ``[T,2,S]`` OPEN/CLOSE request. ``tw_overnight``
    requires ``[T,3,S]`` as exit-at-open fraction plus OPEN/CLOSE entries.
    Both dual-session modes use exact auction prices, one shared daily
    capacity budget, and one net settlement claim enqueued after CLOSE.
    ``tw_day_trade`` requires point-in-time eligibility, exact open and close,
    two executable legs, and finishes each session flat.  When participation
    limiting is enabled, ``cash_close_volume_reference`` or
    ``day_trade_entry_volume_reference`` must be an explicitly causal
    share-volume matrix (the built-in trainer supplies t-1 completed volume);
    same-row ``daily_volumes`` is never silently reused for a Taiwan fill.
    Cash execution uses
    the canonical close-to-next-session ``future_returns`` for mark-to-market;
    day-trade PnL is derived exactly from the supplied open/close prices.
    Broker minimum commission and whole-TWD rounding are exact-ledger-only
    controls, charged once per nonzero symbol-side aggregate order.  Gross
    commission is posted first; the commission-only discount difference is an
    NAV receivable until ``daily_close`` or the next-month-15th payment event,
    and cannot fund trades before payment.

    ``symbols`` is normally already aligned to the active weight columns.  Set
    ``symbols_are_full_universe=True`` when it instead needs the same explicit
    ``symbol_indices`` projection as full-universe fee and lot vectors.  The
    flag removes an otherwise unresolvable ambiguity when both lengths happen
    to equal the active symbol count but their orders differ.

    ``precomputed_exact_backtest`` is reserved for either an executor whose
    tensor forward already uses exact integer lots and the experiment's
    canonical event tape, or a declared continuous-notional product that has
    no separate integer research oracle. It prevents reporting from replacing
    either canonical result with a lower-fidelity Taiwan share audit.
    """
    mode = normalize_execution_mode(execution_mode)
    if precomputed_exact_backtest is not None:
        if mode not in {
            "tw_day_trade",
            "tw_futures_portfolio_day",
            "tw_stock_context_futures_portfolio",
            "tw_stock_futures_day_trade",
            "tw_stock_futures_day_trade_0900",
            "tw_stock_futures_day_trade_0900_integer",
            "crypto_perpetual",
        }:
            raise ValueError(
                "precomputed_exact_backtest is valid only for canonical "
                "tensor execution contracts"
            )
        if normalize_execution_mode(precomputed_exact_backtest.execution_mode) != mode:
            raise ValueError(
                "precomputed exact backtest execution mode does not match request"
            )
        expected_rows = int(np.asarray(weights).shape[0])
        if int(precomputed_exact_backtest.strategy_returns.shape[0]) != expected_rows:
            raise ValueError(
                "precomputed exact backtest row count does not match weights"
            )
        return precomputed_exact_backtest, []
    if mode == "tw_futures_portfolio_day":
        raw_weights = np.asarray(weights, dtype=np.float64)
        raw_returns = np.asarray(future_returns, dtype=np.float64)
        raw_tradable = np.asarray(tradable_mask)
        raw_benchmark = np.asarray(benchmark_returns, dtype=np.float32)
        _require_side_masks_if_strict(
            can_buy_mask,
            can_sell_mask,
            context="tw_futures_portfolio_day execution",
        )
        raw_can_buy = np.asarray(
            raw_tradable if can_buy_mask is None else can_buy_mask,
            dtype=bool,
        )
        raw_can_sell = np.asarray(
            raw_tradable if can_sell_mask is None else can_sell_mask,
            dtype=bool,
        )
        if force_exit_mask is None:
            raise ValueError(
                "tw_futures_portfolio_day requires must-liquidate rows in "
                "force_exit_mask"
            )
        if futures_fee_rates is None:
            raise ValueError("tw_futures_portfolio_day requires fixed fee rates [T,S]")
        if float(buy_fee_rate) != 0.0 or float(sell_fee_rate) != 0.0:
            raise ValueError(
                "fixed futures commission cannot be combined with proportional fees"
            )
        valid_shapes = (
            raw_weights.ndim == 2
            and raw_returns.shape == raw_weights.shape
            and raw_tradable.shape == raw_weights.shape
            and raw_can_buy.shape == raw_weights.shape
            and raw_can_sell.shape == raw_weights.shape
            and raw_benchmark.shape == (raw_weights.shape[0],)
        )
        if not valid_shapes:
            raise ValueError("tw_futures_portfolio_day arrays have invalid shapes")
        gross_budget = _resolve_exposure_budget(gross_leverage)
        target_weights = np.stack(
            [
                _apply_min_trade_weight_row_numpy(
                    _normalize_target_weights_row_numpy(
                        row,
                        long_only=long_only,
                        gross_budget=gross_budget,
                        portfolio_activation=portfolio_activation,
                    ),
                    min_trade_weight,
                )
                for row in np.nan_to_num(raw_weights)
            ],
            axis=0,
        )
        target_weights = np.where(raw_tradable, target_weights, 0.0)
        continuous = run_tw_futures_portfolio_continuous_numpy(
            target_weights,
            raw_returns,
            (raw_tradable.astype(bool, copy=False) & raw_can_buy & raw_can_sell),
            np.asarray(force_exit_mask, dtype=bool),
            fee_rate_per_open_notional=np.asarray(
                futures_fee_rates,
                dtype=np.float64,
            ),
            max_turnover_ratio=float(max_turnover_ratio),
        )
        return (
            BacktestResult(
                strategy_returns=continuous.strategy_returns,
                benchmark_returns=raw_benchmark,
                turnovers=continuous.turnovers,
                weights_history=continuous.weights_history,
                requested_weights_history=raw_weights.astype(np.float32),
                final_weights=continuous.final_weights,
                final_alive=continuous.final_alive,
                execution_mode=mode,
                settlement_ledger_unit="notional_weight",
            ),
            [],
        )
    if mode == "tw_index_derivatives_day":
        raw_weights = np.asarray(weights, dtype=np.float64)
        raw_returns = np.asarray(future_returns)
        raw_tradable = np.asarray(tradable_mask)
        raw_benchmark = np.asarray(benchmark_returns, dtype=np.float32)
        if (
            raw_weights.ndim != 2
            or raw_weights.shape[1] != TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4
        ):
            raise ValueError(
                "tw_index_derivatives_day weights must have direct shape [T,D_derivatives]"
            )
        rows = int(raw_weights.shape[0])
        symbols_count = int(raw_returns.shape[1])
        if raw_returns.shape != (rows, symbols_count):
            raise ValueError("future_returns must have shape [T,S]")
        if (
            raw_tradable.shape != (rows, symbols_count)
            or raw_tradable.dtype != np.bool_
        ):
            raise ValueError("tradable_mask must be boolean [T,S]")
        if raw_benchmark.shape != (rows,):
            raise ValueError("benchmark_returns must have shape [T]")
        if futures_market is None or derivatives_day_candidates is None:
            raise ValueError(
                "tw_index_derivatives_day requires futures and causal derivative candidates"
            )
        requested_dates = (
            np.asarray(futures_market.dates, dtype="datetime64[D]")
            if dates is None
            else np.asarray(dates, dtype="datetime64[D]")
        )
        if requested_dates.shape != (rows,):
            raise ValueError("dates must contain one row per derivative decision")
        market_dates = np.asarray(futures_market.dates, dtype="datetime64[D]")
        row_indices = np.searchsorted(market_dates, requested_dates)
        if bool(np.any(row_indices >= market_dates.size)) or not np.array_equal(
            market_dates[row_indices], requested_dates
        ):
            raise ValueError("derivative markets do not cover every requested date")
        selected_market = TaiwanIndexFuturesDaySession(
            dates=market_dates[row_indices],
            products=futures_market.products,
            contract_months=futures_market.contract_months[row_indices],
            open_prices=futures_market.open_prices[row_indices],
            high_prices=futures_market.high_prices[row_indices],
            low_prices=futures_market.low_prices[row_indices],
            close_prices=futures_market.close_prices[row_indices],
            volumes=futures_market.volumes[row_indices],
            log_returns=futures_market.log_returns[row_indices],
            tradable_mask=futures_market.tradable_mask[row_indices],
            multipliers=futures_market.multipliers,
            tenor_contract_months=(
                None
                if futures_market.tenor_contract_months is None
                else futures_market.tenor_contract_months[row_indices]
            ),
            tenor_open_prices=(
                None
                if futures_market.tenor_open_prices is None
                else futures_market.tenor_open_prices[row_indices]
            ),
            tenor_high_prices=(
                None
                if futures_market.tenor_high_prices is None
                else futures_market.tenor_high_prices[row_indices]
            ),
            tenor_low_prices=(
                None
                if futures_market.tenor_low_prices is None
                else futures_market.tenor_low_prices[row_indices]
            ),
            tenor_close_prices=(
                None
                if futures_market.tenor_close_prices is None
                else futures_market.tenor_close_prices[row_indices]
            ),
            tenor_volumes=(
                None
                if futures_market.tenor_volumes is None
                else futures_market.tenor_volumes[row_indices]
            ),
            tenor_log_returns=(
                None
                if futures_market.tenor_log_returns is None
                else futures_market.tenor_log_returns[row_indices]
            ),
            tenor_tradable_mask=(
                None
                if futures_market.tenor_tradable_mask is None
                else futures_market.tenor_tradable_mask[row_indices]
            ),
        )

        selected_candidates = derivatives_day_candidates.select_dates(requested_dates)
        actions = np.nan_to_num(raw_weights, nan=0.0, posinf=0.0, neginf=0.0)
        integer = run_tw_index_derivatives_day_integer(
            actions,
            selected_market,
            selected_candidates,
            initial_capital=initial_capital,
            maximum_capital_fraction=derivatives_maximum_capital_fraction,
            futures_cost_schedule=futures_cost_schedule,
            option_cost_schedule=option_day_cost_schedule,
        )
        result = BacktestResult(
            strategy_returns=_portfolio_simple_returns_to_log_numpy(
                integer.strategy_returns
            ),
            benchmark_returns=raw_benchmark,
            turnovers=integer.turnovers.astype(np.float32),
            weights_history=np.zeros((rows, symbols_count), dtype=np.float32),
            requested_weights_history=raw_weights.astype(np.float32),
            final_weights=np.zeros(symbols_count, dtype=np.float32),
            final_alive=np.asarray(integer.alive[-1] if rows else True, dtype=bool),
            execution_mode=mode,
        )
        records: list[HoldingsRecord] = []
        if collect_holdings:
            prior_equity = float(initial_capital)
            market_tenors = selected_market.require_tenor_panel()
            selected_options = selected_candidates.source_chain
            for row in range(rows):
                denominator = max(prior_equity, 1e-12)
                month_to_tenor = {
                    str(month): tenor
                    for tenor, month in enumerate(market_tenors[0][row])
                    if str(month)
                }
                for tenor_slot in range(TAIFEX_INDEX_FUTURES_TENOR_SLOTS):
                    month = str(integer.futures_contract_months[row, tenor_slot])
                    market_tenor = month_to_tenor.get(month)
                    if market_tenor is None:
                        continue
                    for product_index, product in enumerate(
                        TAIFEX_INDEX_FUTURES_PRODUCTS
                    ):
                        quantity = int(
                            integer.futures_contract_quantities[
                                row, tenor_slot, product_index
                            ]
                        )
                        for record_type, signed, price in (
                            (
                                "entry",
                                quantity,
                                market_tenors[1][row, market_tenor, product_index],
                            ),
                            (
                                "exit",
                                -quantity,
                                market_tenors[4][row, market_tenor, product_index],
                            ),
                        ):
                            if signed:
                                value = (
                                    signed
                                    * selected_market.multipliers[product_index]
                                    * float(price)
                                )
                                records.append(
                                    HoldingsRecord(
                                        date=str(requested_dates[row]),
                                        symbol=f"{product}_{month}",
                                        shares=signed,
                                        price=float(price),
                                        market_value=float(value),
                                        holding_ratio=float(value / denominator),
                                        is_cash=False,
                                        record_type=record_type,
                                        side="buy" if signed > 0 else "sell",
                                    )
                                )
                for slot_value in np.flatnonzero(
                    integer.option_contract_quantities[row]
                ):
                    slot = int(slot_value)
                    sparse_index = int(integer.option_sparse_indices[row, slot])
                    quantity = int(integer.option_contract_quantities[row, slot])
                    if sparse_index < 0 or quantity == 0:
                        continue
                    series = str(selected_options.option_series[sparse_index])
                    strike = float(selected_options.strikes[sparse_index])
                    right = str(selected_options.option_rights[sparse_index])
                    contract_symbol = f"TXO_{series}_{strike:g}_{right}"
                    for record_type, signed, price in (
                        ("entry", quantity, selected_options.open_prices[sparse_index]),
                        (
                            "exit",
                            -quantity,
                            selected_options.close_prices[sparse_index],
                        ),
                    ):
                        if signed:
                            value = signed * 50.0 * float(price)
                            records.append(
                                HoldingsRecord(
                                    date=str(requested_dates[row]),
                                    symbol=contract_symbol,
                                    shares=signed,
                                    price=float(price),
                                    market_value=float(value),
                                    holding_ratio=float(value / denominator),
                                    is_cash=False,
                                    record_type=record_type,
                                    side="buy" if signed > 0 else "sell",
                                )
                            )
                prior_equity = float(integer.equity[row])
        return result, records
    if mode == "tw_index_futures_day":
        raw_weights = np.asarray(weights, dtype=np.float64)
        raw_returns = np.asarray(future_returns)
        raw_tradable = np.asarray(tradable_mask)
        raw_benchmark = np.asarray(benchmark_returns, dtype=np.float32)
        if raw_weights.ndim != 2 or raw_weights.shape[0] <= 0:
            raise ValueError("tw_index_futures_day weights must have shape [T,D]")
        rows = int(raw_weights.shape[0])
        direct_tenor_actions = (
            int(raw_weights.shape[1]) == TAIFEX_INDEX_FUTURES_ACTION_COUNT
        )
        if raw_returns.ndim != 2 or int(raw_returns.shape[0]) != rows:
            raise ValueError("future_returns must retain stock-panel shape [T,S]")
        symbols_count = int(raw_returns.shape[1])
        if raw_tradable.shape != raw_returns.shape or raw_tradable.dtype != np.bool_:
            raise ValueError("tradable_mask must be boolean and match future_returns")
        if not direct_tenor_actions and raw_returns.shape != raw_weights.shape:
            raise ValueError("legacy futures pseudo weights must match [T,S]")
        if raw_benchmark.shape != (rows,):
            raise ValueError("benchmark_returns must have shape [T]")
        if futures_market is None:
            raise ValueError("tw_index_futures_day requires futures_market")

        market_dates = np.asarray(futures_market.dates, dtype="datetime64[D]")
        requested_dates = (
            market_dates if dates is None else np.asarray(dates, dtype="datetime64[D]")
        )
        if requested_dates.shape != (rows,):
            raise ValueError("dates must contain one row per futures decision")
        row_indices = np.searchsorted(market_dates, requested_dates)
        if bool(
            np.any(row_indices >= market_dates.size)
            or np.any(market_dates[row_indices] != requested_dates)
        ):
            raise ValueError("futures_market does not cover every requested date")
        selected_market = TaiwanIndexFuturesDaySession(
            dates=market_dates[row_indices],
            products=futures_market.products,
            contract_months=futures_market.contract_months[row_indices],
            open_prices=futures_market.open_prices[row_indices],
            high_prices=futures_market.high_prices[row_indices],
            low_prices=futures_market.low_prices[row_indices],
            close_prices=futures_market.close_prices[row_indices],
            volumes=futures_market.volumes[row_indices],
            log_returns=futures_market.log_returns[row_indices],
            tradable_mask=futures_market.tradable_mask[row_indices],
            multipliers=futures_market.multipliers,
            rolling_buy_hold_log_returns=(
                None
                if futures_market.rolling_buy_hold_log_returns is None
                else futures_market.rolling_buy_hold_log_returns[row_indices]
            ),
            rolling_buy_hold_tradable_mask=(
                None
                if futures_market.rolling_buy_hold_tradable_mask is None
                else futures_market.rolling_buy_hold_tradable_mask[row_indices]
            ),
            front_month_roll_mask=(
                None
                if futures_market.front_month_roll_mask is None
                else futures_market.front_month_roll_mask[row_indices]
            ),
            tenor_contract_months=(
                None
                if futures_market.tenor_contract_months is None
                else futures_market.tenor_contract_months[row_indices]
            ),
            tenor_open_prices=(
                None
                if futures_market.tenor_open_prices is None
                else futures_market.tenor_open_prices[row_indices]
            ),
            tenor_high_prices=(
                None
                if futures_market.tenor_high_prices is None
                else futures_market.tenor_high_prices[row_indices]
            ),
            tenor_low_prices=(
                None
                if futures_market.tenor_low_prices is None
                else futures_market.tenor_low_prices[row_indices]
            ),
            tenor_close_prices=(
                None
                if futures_market.tenor_close_prices is None
                else futures_market.tenor_close_prices[row_indices]
            ),
            tenor_volumes=(
                None
                if futures_market.tenor_volumes is None
                else futures_market.tenor_volumes[row_indices]
            ),
            tenor_log_returns=(
                None
                if futures_market.tenor_log_returns is None
                else futures_market.tenor_log_returns[row_indices]
            ),
            tenor_tradable_mask=(
                None
                if futures_market.tenor_tradable_mask is None
                else futures_market.tenor_tradable_mask[row_indices]
            ),
        )
        if direct_tenor_actions:
            integer = run_tw_index_futures_day_integer(
                raw_weights,
                selected_market,
                initial_capital=initial_capital,
                max_abs_exposure=min(
                    _resolve_exposure_budget(gross_leverage),
                    (
                        _resolve_exposure_budget(futures_max_abs_exposure)
                        if futures_max_abs_exposure is not None
                        else 1.0
                    ),
                ),
                cost_schedule=futures_cost_schedule,
            )
            result = BacktestResult(
                strategy_returns=_portfolio_simple_returns_to_log_numpy(
                    integer.strategy_returns
                ),
                benchmark_returns=raw_benchmark,
                turnovers=integer.turnovers.astype(np.float32, copy=False),
                weights_history=np.zeros((rows, symbols_count), dtype=np.float32),
                requested_weights_history=raw_weights.astype(np.float32, copy=False),
                equity_scale_history=(integer.equity / float(initial_capital)).astype(
                    np.float32, copy=False
                ),
                final_weights=np.zeros(symbols_count, dtype=np.float32),
                final_alive=np.asarray(
                    integer.alive[-1] if integer.alive.size else True,
                    dtype=bool,
                ),
                final_equity_scale=np.asarray(
                    (
                        integer.equity[-1] / float(initial_capital)
                        if integer.equity.size
                        else 1.0
                    ),
                    dtype=np.float32,
                ),
                execution_mode=mode,
            )
            records: list[HoldingsRecord] = []
            if collect_holdings:
                (
                    months,
                    opens,
                    _highs,
                    _lows,
                    closes,
                    _volumes,
                    _log_returns,
                    _tradable,
                ) = selected_market.flattened_tenor_panel()
                multipliers = np.repeat(
                    selected_market.multipliers.astype(np.float64),
                    TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
                )
                prior_equity = float(initial_capital)
                for row in range(rows):
                    denominator = max(prior_equity, 1e-12)
                    for slot, action_symbol in enumerate(integer.products):
                        quantity = int(integer.contract_quantities[row, slot])
                        if quantity == 0:
                            continue
                        concrete_symbol = (
                            f"{action_symbol.split('_', 1)[0]}_{months[row, slot]}"
                        )
                        for record_type, signed, price in (
                            ("entry", quantity, opens[row, slot]),
                            ("exit", -quantity, closes[row, slot]),
                        ):
                            value = signed * multipliers[slot] * float(price)
                            records.append(
                                HoldingsRecord(
                                    date=str(requested_dates[row]),
                                    symbol=concrete_symbol,
                                    shares=signed,
                                    price=float(price),
                                    market_value=float(value),
                                    holding_ratio=float(value / denominator),
                                    is_cash=False,
                                    record_type=record_type,
                                    side="buy" if signed > 0 else "sell",
                                )
                            )
                    prior_equity = float(integer.equity[row])
            return result, records
        clean_weights = np.nan_to_num(raw_weights, nan=0.0, posinf=0.0, neginf=0.0)
        clean_weights = np.where(raw_tradable, clean_weights, 0.0)
        requested_exposure = clean_weights.sum(axis=-1)
        if long_only:
            requested_exposure = np.maximum(requested_exposure, 0.0)
        if float(min_trade_weight) > 0.0:
            requested_exposure = np.where(
                np.abs(requested_exposure) >= float(min_trade_weight),
                requested_exposure,
                0.0,
            )
        integer = run_tw_index_futures_day_integer(
            requested_exposure,
            selected_market,
            initial_capital=initial_capital,
            max_abs_exposure=min(
                _resolve_exposure_budget(gross_leverage),
                (
                    _resolve_exposure_budget(futures_max_abs_exposure)
                    if futures_max_abs_exposure is not None
                    else 1.0
                ),
            ),
            cost_schedule=futures_cost_schedule,
        )
        denominator = np.where(requested_exposure != 0.0, requested_exposure, 1.0)
        scale = np.where(
            requested_exposure != 0.0,
            integer.executed_exposure / denominator,
            0.0,
        )
        executed_pseudo_weights = (clean_weights * scale[:, None]).astype(
            np.float32, copy=False
        )
        result = BacktestResult(
            strategy_returns=_portfolio_simple_returns_to_log_numpy(
                integer.strategy_returns
            ),
            benchmark_returns=raw_benchmark,
            turnovers=integer.turnovers.astype(np.float32, copy=False),
            weights_history=executed_pseudo_weights,
            requested_weights_history=raw_weights.astype(np.float32, copy=False),
            final_weights=np.zeros(symbols_count, dtype=np.float32),
            final_alive=np.asarray(
                integer.alive[-1] if integer.alive.size else True,
                dtype=bool,
            ),
            execution_mode=mode,
        )
        records: list[HoldingsRecord] = []
        if collect_holdings:
            multipliers = selected_market.multipliers.astype(np.float64)
            prior_equity = float(initial_capital)
            for row in range(rows):
                equity_denominator = max(prior_equity, 1e-12)
                for product_idx, product in enumerate(selected_market.products):
                    quantity = int(integer.contract_quantities[row, product_idx])
                    if quantity == 0:
                        continue
                    for record_type, price in (
                        ("entry", selected_market.open_prices[row, product_idx]),
                        ("exit", selected_market.close_prices[row, product_idx]),
                    ):
                        signed_quantity = (
                            quantity if record_type == "entry" else -quantity
                        )
                        market_value = (
                            signed_quantity * multipliers[product_idx] * float(price)
                        )
                        records.append(
                            HoldingsRecord(
                                date=str(requested_dates[row]),
                                symbol=str(product),
                                shares=signed_quantity,
                                price=float(price),
                                market_value=float(market_value),
                                holding_ratio=float(market_value / equity_denominator),
                                is_cash=False,
                                record_type=record_type,
                                side="buy" if signed_quantity > 0 else "sell",
                            )
                        )
                prior_equity = float(integer.equity[row])
        return result, records
    if mode != "naive":
        raw_weights = np.asarray(weights, dtype=np.float64)
        phase_execution = mode in TW_CARRYING_EXECUTION_MODES and raw_weights.ndim == 3
        if phase_execution:
            expected_phases = 2 if mode == "tw_cash" else 3
            if (
                raw_weights.shape[0] <= 0
                or raw_weights.shape[1] != expected_phases
                or raw_weights.shape[2] <= 0
            ):
                raise ValueError(
                    f"{mode} weights must have shape [T,{expected_phases},S]"
                )
            t_len_real = int(raw_weights.shape[0])
            n_symbols_real = int(raw_weights.shape[2])
        elif raw_weights.ndim == 2:
            if mode == "tw_overnight":
                raise ValueError("tw_overnight weights must have shape [T,3,S]")
            t_len_real, n_symbols_real = raw_weights.shape
        else:
            expected = (
                "[T,2,S]"
                if mode == "tw_cash"
                else "[T,3,S]"
                if mode == "tw_overnight"
                else "[T,S]"
            )
            raise ValueError(f"{mode} weights must have shape {expected}")
        daily_shape = (t_len_real, n_symbols_real)
        returns_real = np.asarray(future_returns)
        tradable_real = np.asarray(tradable_mask)
        benchmark_real = np.asarray(benchmark_returns)
        if returns_real.shape != daily_shape:
            raise ValueError("future_returns must have shape [T,S]")
        if tradable_real.shape != daily_shape or tradable_real.dtype != np.bool_:
            raise ValueError("tradable_mask must be boolean with shape [T,S]")
        if benchmark_real.shape != (t_len_real,):
            raise ValueError(f"benchmark_returns must have shape [{t_len_real}]")
        if close_prices is None:
            raise ValueError(f"{mode} requires exact close_prices")
        close_real = np.asarray(close_prices, dtype=np.float64)
        if close_real.shape != daily_shape:
            raise ValueError("close_prices must have shape [T,S]")
        if buy_fee_rates is None or sell_fee_rates is None:
            raise ValueError(
                f"{mode} requires explicit per-symbol buy_fee_rates and sell_fee_rates"
            )

        selected_buy_fees = _select_integer_symbol_parameter(
            "buy_fee_rates",
            buy_fee_rates,
            n_symbols=n_symbols_real,
            symbol_indices=symbol_indices,
        )
        selected_sell_fees = _select_integer_symbol_parameter(
            "sell_fee_rates",
            sell_fee_rates,
            n_symbols=n_symbols_real,
            symbol_indices=symbol_indices,
        )
        selected_commission_rebate_rates = _select_integer_time_symbol_parameter(
            "commission_rebate_rates",
            commission_rebate_rates,
            n_rows=t_len_real,
            n_symbols=n_symbols_real,
            symbol_indices=symbol_indices,
        )
        rebate_timing = normalize_commission_rebate_timing(commission_rebate_timing)
        rebate_month_ids = session_month_ids
        rebate_payment_eligible = commission_rebate_payment_eligible_mask
        if rebate_timing == "monthly_15th" and (
            rebate_month_ids is None or rebate_payment_eligible is None
        ):
            if dates is None:
                raise ValueError(
                    "monthly_15th integer commission rebates require dates or "
                    "explicit session-month/payment calendar arrays"
                )
            derived_month_ids, derived_payment_eligible = commission_rebate_calendar(
                dates
            )
            if rebate_month_ids is None:
                rebate_month_ids = derived_month_ids
            if rebate_payment_eligible is None:
                rebate_payment_eligible = derived_payment_eligible
        selected_lots = (
            None
            if lot_sizes is None
            else _select_integer_symbol_parameter(
                "lot_sizes",
                lot_sizes,
                n_symbols=n_symbols_real,
                symbol_indices=symbol_indices,
            )
        )
        selected_short_lots = (
            None
            if short_lot_sizes is None
            else _select_integer_symbol_parameter(
                "short_lot_sizes",
                short_lot_sizes,
                n_symbols=n_symbols_real,
                symbol_indices=symbol_indices,
            )
        )
        selected_short_margin_rate = (
            None
            if short_margin_rate is None
            else _select_integer_time_symbol_parameter(
                "short_margin_rate",
                short_margin_rate,
                n_rows=t_len_real,
                n_symbols=n_symbols_real,
                symbol_indices=symbol_indices,
            )
        )
        selected_short_capacity = (
            None
            if short_capacity_shares is None
            else _select_integer_time_symbol_parameter(
                "short_capacity_shares",
                short_capacity_shares,
                n_rows=t_len_real,
                n_symbols=n_symbols_real,
                symbol_indices=symbol_indices,
            )
        )
        selected_short_handling_fee = _select_integer_symbol_parameter(
            "short_handling_fee_rate",
            short_handling_fee_rate,
            n_symbols=n_symbols_real,
            symbol_indices=symbol_indices,
        )

        gross_budget = _resolve_exposure_budget(gross_leverage)
        clean_weights = np.nan_to_num(
            raw_weights,
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
        if phase_execution:
            # Match the tensor/model phase boundary: a symbol outside the
            # point-in-time selection mask must not consume OPEN/CLOSE gross
            # budget before it is zeroed.  Masking only after normalization
            # makes an arbitrary invalid-column logit dilute every valid order.
            clean_weights = np.where(
                tradable_real[:, None, :],
                clean_weights,
                0.0,
            )
            if mode == "tw_cash":
                flat_actions = clean_weights.reshape(
                    t_len_real * 2,
                    n_symbols_real,
                )
                normalized_flat = np.stack(
                    [
                        _apply_min_trade_weight_row_numpy(
                            _normalize_target_weights_row_numpy(
                                row,
                                long_only=long_only,
                                gross_budget=gross_budget,
                                portfolio_activation=portfolio_activation,
                            ),
                            min_trade_weight,
                        )
                        for row in flat_actions
                    ],
                    axis=0,
                )
                target_weights = normalized_flat.reshape(
                    t_len_real,
                    2,
                    n_symbols_real,
                )
            else:
                activation_name = normalize_portfolio_activation(portfolio_activation)
                raw_exit = clean_weights[:, 0]
                if activation_name == "pre_normalized":
                    exit_fraction = np.clip(raw_exit, 0.0, 1.0)
                else:
                    positive = raw_exit >= 0.0
                    exit_fraction = np.empty_like(raw_exit)
                    exit_fraction[positive] = 1.0 / (1.0 + np.exp(-raw_exit[positive]))
                    negative_exp = np.exp(raw_exit[~positive])
                    exit_fraction[~positive] = negative_exp / (1.0 + negative_exp)
                if activation_name == "pre_normalized":
                    entry_logits = clean_weights[:, 1:]
                else:
                    raw_open_entry = clean_weights[:, 1]
                    raw_close_entry = clean_weights[:, 2]
                    direction_logits = 0.5 * (raw_open_entry + raw_close_entry)
                    timing_delta = raw_close_entry - raw_open_entry
                    positive_timing = timing_delta >= 0.0
                    close_entry_fraction = np.empty_like(timing_delta)
                    close_entry_fraction[positive_timing] = 1.0 / (
                        1.0 + np.exp(-timing_delta[positive_timing])
                    )
                    negative_timing_exp = np.exp(timing_delta[~positive_timing])
                    close_entry_fraction[~positive_timing] = negative_timing_exp / (
                        1.0 + negative_timing_exp
                    )
                    entry_logits = np.stack(
                        (
                            direction_logits * (1.0 - close_entry_fraction),
                            direction_logits * close_entry_fraction,
                        ),
                        axis=1,
                    )
                flat_entries = entry_logits.reshape(
                    t_len_real,
                    2 * n_symbols_real,
                )
                normalized_entries = np.stack(
                    [
                        _apply_min_trade_weight_row_numpy(
                            _normalize_target_weights_row_numpy(
                                row,
                                long_only=long_only,
                                gross_budget=gross_budget,
                                portfolio_activation=portfolio_activation,
                            ),
                            min_trade_weight,
                        )
                        for row in flat_entries
                    ],
                    axis=0,
                ).reshape(t_len_real, 2, n_symbols_real)
                target_weights = np.concatenate(
                    (exit_fraction[:, None, :], normalized_entries),
                    axis=1,
                )
            target_weights = np.where(
                tradable_real[:, None, :],
                target_weights,
                0.0,
            )
        else:
            target_weights = np.stack(
                [
                    _apply_min_trade_weight_row_numpy(
                        _normalize_target_weights_row_numpy(
                            row,
                            long_only=long_only,
                            gross_budget=gross_budget,
                            portfolio_activation=portfolio_activation,
                        ),
                        min_trade_weight,
                    )
                    for row in clean_weights
                ],
                axis=0,
            )

        _require_side_masks_if_strict(
            can_buy_mask,
            can_sell_mask,
            context=f"{mode} integer-share backtest",
        )

        def reject_nonempty_unsupported_mask(
            name: str,
            value: np.ndarray | None,
        ) -> None:
            if value is None:
                return
            candidate = np.asarray(value)
            if candidate.shape != daily_shape or candidate.dtype != np.bool_:
                raise ValueError(f"{name} must be boolean with shape [T,S]")
            if np.any(candidate):
                raise ValueError(f"{mode} does not support active {name} entries")

        if symbols is None:
            if symbols_are_full_universe:
                raise ValueError(
                    "symbols_are_full_universe=true requires an explicit symbols list"
                )
            active_symbols = [f"SYM_{idx:04d}" for idx in range(n_symbols_real)]
        else:
            if not isinstance(symbols_are_full_universe, bool):
                raise ValueError("symbols_are_full_universe must be boolean")
            supplied_symbols = list(symbols)
            if symbols_are_full_universe:
                if symbol_indices is None:
                    raise ValueError(
                        "symbols_are_full_universe=true requires symbol_indices"
                    )
                indices_raw = np.asarray(symbol_indices)
                if indices_raw.shape != (n_symbols_real,) or not np.issubdtype(
                    indices_raw.dtype, np.integer
                ):
                    raise ValueError(
                        f"symbol_indices must be integer shape [{n_symbols_real}]"
                    )
                indices = indices_raw.astype(np.int64, copy=False)
                if np.any(indices < 0) or np.any(indices >= len(supplied_symbols)):
                    raise ValueError(
                        "symbol_indices contains an out-of-range symbol index"
                    )
                active_symbols = [supplied_symbols[index] for index in indices.tolist()]
            elif len(supplied_symbols) == n_symbols_real:
                active_symbols = supplied_symbols
            elif symbol_indices is not None:
                indices_raw = np.asarray(symbol_indices)
                if indices_raw.shape != (n_symbols_real,) or not np.issubdtype(
                    indices_raw.dtype, np.integer
                ):
                    raise ValueError(
                        f"symbol_indices must be integer shape [{n_symbols_real}]"
                    )
                indices = indices_raw.astype(np.int64, copy=False)
                if np.any(indices < 0) or np.any(indices >= len(supplied_symbols)):
                    raise ValueError(
                        "symbol_indices contains an out-of-range symbol index"
                    )
                active_symbols = [supplied_symbols[index] for index in indices.tolist()]
            else:
                raise ValueError(
                    f"symbols must contain exactly {n_symbols_real} entries, or "
                    "symbol_indices must map a full-universe list"
                )

        open_real: np.ndarray | None = None
        if phase_execution:
            if open_prices is None:
                raise ValueError(f"{mode} phase execution requires exact open_prices")
            open_real = np.asarray(open_prices, dtype=np.float64)
            if open_real.shape != daily_shape:
                raise ValueError("open_prices must have shape [T,S]")
            if can_buy_mask is None or can_sell_mask is None:
                raise ValueError(
                    f"{mode} phase execution requires explicit close-side "
                    "can_buy_mask and can_sell_mask"
                )
            if (
                day_trade_can_buy_open_mask is None
                or day_trade_can_sell_open_mask is None
            ):
                raise ValueError(
                    f"{mode} phase execution requires explicit open-side buy "
                    "and sell masks"
                )
            if unresolved_corporate_action_mask is None:
                raise ValueError(
                    f"{mode} requires the receipt-verified official "
                    "corporate-action avoidance mask"
                )

            def required_daily_bool(
                name: str,
                value: np.ndarray,
            ) -> np.ndarray:
                selected = np.asarray(value)
                if selected.shape != daily_shape or selected.dtype != np.bool_:
                    raise ValueError(f"{name} must be boolean with shape [T,S]")
                return selected

            close_buy = required_daily_bool("can_buy_mask", can_buy_mask)
            close_sell = required_daily_bool("can_sell_mask", can_sell_mask)
            open_buy = required_daily_bool(
                "day_trade_can_buy_open_mask",
                day_trade_can_buy_open_mask,
            )
            open_sell = required_daily_bool(
                "day_trade_can_sell_open_mask",
                day_trade_can_sell_open_mask,
            )
            unresolved_daily = required_daily_bool(
                "unresolved_corporate_action_mask",
                unresolved_corporate_action_mask,
            )
            phase_tradable = np.repeat(
                tradable_real[:, None, :],
                2,
                axis=1,
            )
            phase_buy = np.stack((open_buy, close_buy), axis=1)
            phase_sell = np.stack((open_sell, close_sell), axis=1)
            phase_unresolved = np.repeat(
                unresolved_daily[:, None, :],
                2,
                axis=1,
            )

            if long_only:
                phase_short_open = None
            else:
                missing_short_contract = [
                    name
                    for name, value in (
                        ("can_short_open_mask", can_short_open_mask),
                        (
                            "can_short_open_open_mask",
                            can_short_open_open_mask,
                        ),
                        ("short_capacity_shares", short_capacity_shares),
                        ("short_margin_rate", short_margin_rate),
                    )
                    if value is None
                ]
                if missing_short_contract:
                    raise ValueError(
                        f"long/short {mode} requires independent OPEN/CLOSE "
                        "short eligibility, demonstrated capacity, and "
                        "initial-margin rates; missing "
                        + ", ".join(missing_short_contract)
                    )
                close_short = required_daily_bool(
                    "can_short_open_mask",
                    can_short_open_mask,
                )
                open_short = required_daily_bool(
                    "can_short_open_open_mask",
                    can_short_open_open_mask,
                )
                phase_short_open = np.stack(
                    (open_short, close_short),
                    axis=1,
                )

            phase_force_cover = None
            if force_short_cover_mask is not None:
                force_cover_daily = required_daily_bool(
                    "force_short_cover_mask",
                    force_short_cover_mask,
                )
                phase_force_cover = np.repeat(
                    force_cover_daily[:, None, :],
                    2,
                    axis=1,
                )
            phase_force_exit = None
            if force_exit_mask is not None:
                force_exit_daily = required_daily_bool(
                    "force_exit_mask",
                    force_exit_mask,
                )
                phase_force_exit = np.stack(
                    (
                        np.zeros_like(force_exit_daily),
                        force_exit_daily,
                    ),
                    axis=1,
                )

            if max_volume_participation > 0.0:
                if cash_close_volume_reference is None:
                    raise ValueError(
                        f"{mode} with max_volume_participation > 0 requires "
                        "an explicit causal completed-session volume reference"
                    )
                cash_volume = np.asarray(
                    cash_close_volume_reference,
                    dtype=np.float64,
                )
                if cash_volume.shape != daily_shape:
                    raise ValueError(
                        "cash_close_volume_reference must have shape [T,S]"
                    )
            else:
                cash_volume = None

            phase_runner = (
                run_tw_cash_dual_session_integer
                if mode == "tw_cash"
                else run_tw_overnight_dual_session_integer
            )
            integer_result = phase_runner(
                target_weights,
                open_real,
                close_real,
                phase_tradable,
                phase_buy,
                phase_sell,
                selected_buy_fees,
                selected_sell_fees,
                commission_rebate_rates=selected_commission_rebate_rates,
                commission_rebate_timing=rebate_timing,
                session_month_ids=rebate_month_ids,
                commission_rebate_payment_eligible_mask=(rebate_payment_eligible),
                can_short_open_mask=phase_short_open,
                force_short_cover_mask=phase_force_cover,
                short_margin_rate=selected_short_margin_rate,
                short_capacity_shares=selected_short_capacity,
                short_lot_sizes=selected_short_lots,
                short_handling_fee_rate=selected_short_handling_fee,
                short_maintenance_ratio=short_maintenance_ratio,
                unresolved_corporate_action_mask=phase_unresolved,
                cash_dividend_yield=cash_dividend_yield,
                cash_dividend_payment_delay_sessions=(
                    cash_dividend_payment_delay_sessions
                ),
                claim_queue_sessions=claim_queue_sessions,
                force_exit_mask=phase_force_exit,
                minimum_commission=minimum_commission,
                commission_rounding=commission_rounding,
                tax_rounding=tax_rounding,
                lot_sizes=selected_lots,
                daily_volumes=cash_volume,
                max_volume_participation=max_volume_participation,
                max_turnover_ratio=max_turnover_ratio,
                gross_budget=gross_budget,
                state_advance_mask=state_advance_mask,
                settlement_lag_sessions=settlement_lag_sessions,
                initial_cash=initial_capital,
                initial_state=initial_integer_state,
            )
        elif mode == "tw_cash":
            if not long_only:
                missing_short_contract = [
                    name
                    for name, value in (
                        ("can_short_open_mask", can_short_open_mask),
                        ("short_capacity_shares", short_capacity_shares),
                        ("short_margin_rate", short_margin_rate),
                    )
                    if value is None
                ]
                if missing_short_contract:
                    raise ValueError(
                        "long/short tw_cash requires explicit point-in-time margin-short "
                        "eligibility, demonstrated capacity, and initial-margin rates; "
                        "missing " + ", ".join(missing_short_contract)
                    )
            if unresolved_corporate_action_mask is None:
                raise ValueError(
                    "tw_cash requires the receipt-verified official "
                    "corporate-action avoidance mask"
                )
            if max_volume_participation > 0.0:
                if cash_close_volume_reference is None:
                    raise ValueError(
                        "tw_cash with max_volume_participation > 0 requires an "
                        "explicit causal cash_close_volume_reference; "
                        "same-session daily_volumes includes the closing auction"
                    )
                cash_volume = np.asarray(
                    cash_close_volume_reference,
                    dtype=np.float64,
                )
                if cash_volume.shape != raw_weights.shape:
                    raise ValueError(
                        "cash_close_volume_reference shape must match weights"
                    )
            else:
                cash_volume = None
            integer_result = run_tw_cash_integer(
                target_weights,
                close_real,
                future_log_returns=returns_real,
                unresolved_corporate_action_mask=unresolved_corporate_action_mask,
                cash_dividend_yield=cash_dividend_yield,
                cash_dividend_payment_delay_sessions=(
                    cash_dividend_payment_delay_sessions
                ),
                claim_queue_sessions=claim_queue_sessions,
                buy_fee_rates=selected_buy_fees,
                sell_fee_rates=selected_sell_fees,
                commission_rebate_rates=selected_commission_rebate_rates,
                commission_rebate_timing=rebate_timing,
                session_month_ids=rebate_month_ids,
                commission_rebate_payment_eligible_mask=(rebate_payment_eligible),
                minimum_commission=minimum_commission,
                commission_rounding=commission_rounding,
                tax_rounding=tax_rounding,
                lot_sizes=selected_lots,
                short_lot_sizes=selected_short_lots,
                tradable_mask=tradable_real,
                can_buy_mask=can_buy_mask,
                can_sell_mask=can_sell_mask,
                can_short_open_mask=(None if long_only else can_short_open_mask),
                force_short_cover_mask=(None if long_only else force_short_cover_mask),
                short_margin_rate=selected_short_margin_rate,
                short_capacity_shares=selected_short_capacity,
                short_maintenance_ratio=short_maintenance_ratio,
                short_handling_fee_rate=selected_short_handling_fee,
                force_exit_mask=force_exit_mask,
                # The low-level oracle calls this matrix daily_volumes for API
                # compatibility, but the public boundary guarantees that it is
                # an explicitly causal completed-session reference.
                daily_volumes=cash_volume,
                max_volume_participation=max_volume_participation,
                max_turnover_ratio=max_turnover_ratio,
                gross_budget=gross_budget,
                state_advance_mask=state_advance_mask,
                settlement_lag_sessions=settlement_lag_sessions,
                initial_cash=initial_capital,
                initial_state=initial_integer_state,
            )
        else:
            if open_prices is None:
                raise ValueError("tw_day_trade requires exact open_prices")
            if day_trade_eligible_mask is None:
                raise ValueError(
                    "tw_day_trade requires a point-in-time day_trade_eligible_mask"
                )
            if (
                day_trade_can_buy_open_mask is None
                or day_trade_can_sell_open_mask is None
            ):
                raise ValueError(
                    "tw_day_trade requires explicit open-side buy and sell masks"
                )
            if not long_only and can_short_open_mask is None:
                raise ValueError(
                    "long/short tw_day_trade requires an explicit can_short_open_mask"
                )
            open_real = np.asarray(open_prices, dtype=np.float64)
            if open_real.shape != raw_weights.shape:
                raise ValueError("open_prices shape must match weights")
            if max_volume_participation > 0.0:
                if day_trade_entry_volume_reference is None:
                    raise ValueError(
                        "tw_day_trade with max_volume_participation > 0 requires "
                        "an explicit causal day_trade_entry_volume_reference; "
                        "same-session daily_volumes would leak the future"
                    )
                day_trade_volume = np.asarray(
                    day_trade_entry_volume_reference,
                    dtype=np.float64,
                )
                if day_trade_volume.shape != raw_weights.shape:
                    raise ValueError(
                        "day_trade_entry_volume_reference shape must match weights"
                    )
            else:
                day_trade_volume = None
            integer_result = run_tw_day_trade_integer(
                target_weights,
                open_real,
                close_real,
                buy_fee_rates=selected_buy_fees,
                sell_fee_rates=selected_sell_fees,
                commission_rebate_rates=selected_commission_rebate_rates,
                commission_rebate_timing=rebate_timing,
                session_month_ids=rebate_month_ids,
                commission_rebate_payment_eligible_mask=(rebate_payment_eligible),
                minimum_commission=minimum_commission,
                commission_rounding=commission_rounding,
                tax_rounding=tax_rounding,
                eligibility_mask=day_trade_eligible_mask,
                lot_sizes=selected_lots,
                tradable_mask=tradable_real,
                can_buy_mask=can_buy_mask,
                can_sell_mask=can_sell_mask,
                can_short_open_mask=can_short_open_mask,
                day_trade_can_buy_open_mask=day_trade_can_buy_open_mask,
                day_trade_can_sell_open_mask=day_trade_can_sell_open_mask,
                force_short_cover_mask=force_short_cover_mask,
                force_exit_mask=force_exit_mask,
                daily_volumes=day_trade_volume,
                max_volume_participation=max_volume_participation,
                max_turnover_ratio=max_turnover_ratio,
                state_advance_mask=state_advance_mask,
                settlement_lag_sessions=settlement_lag_sessions,
                initial_cash=initial_capital,
                initial_state=initial_integer_state,
            )

        public_result = _tw_integer_public_result(integer_result, benchmark_real)
        public_result.requested_weights_history = raw_weights.astype(
            np.float32, copy=True
        )
        records = (
            _tw_integer_holdings_records(
                integer_result,
                close_prices=close_real,
                open_prices=open_real if mode == "tw_day_trade" else None,
                symbols=active_symbols,
                dates=dates,
            )
            if collect_holdings
            else []
        )
        return public_result, records

    w = np.asarray(weights, dtype=np.float64)
    r = np.nan_to_num(
        np.asarray(future_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0
    )
    m = np.asarray(tradable_mask, dtype=bool)
    _require_side_masks_if_strict(
        can_buy_mask, can_sell_mask, context="integer-share backtest"
    )
    buy_m = m if can_buy_mask is None else np.asarray(can_buy_mask, dtype=bool) & m
    sell_m = m if can_sell_mask is None else np.asarray(can_sell_mask, dtype=bool) & m
    short_m = (
        sell_m
        if can_short_open_mask is None
        else np.asarray(can_short_open_mask, dtype=bool) & sell_m & m
    )
    force_m = (
        np.zeros_like(m, dtype=bool)
        if force_short_cover_mask is None
        else np.asarray(force_short_cover_mask, dtype=bool) & m
    )
    exit_m = (
        np.zeros_like(m, dtype=bool)
        if force_exit_mask is None
        else np.asarray(force_exit_mask, dtype=bool)
    )
    b = np.nan_to_num(
        np.asarray(benchmark_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0
    )

    t_len, n_symbols = w.shape
    if symbols is None:
        symbols = [f"SYM_{idx:04d}" for idx in range(n_symbols)]
    tw_symbol_mask = np.asarray([_is_tw_symbol(sym) for sym in symbols], dtype=bool)
    if collect_holdings:
        if dates is None:
            date_text = [f"t{idx:04d}" for idx in range(t_len)]
        else:
            date_text = [
                str(
                    np.datetime_as_string(
                        np.asarray(d, dtype="datetime64[D]"), unit="D"
                    )
                )
                for d in dates
            ]
    else:
        date_text = []

    strategy_returns = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    stock_weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)

    if close_prices is not None:
        price_matrix = np.nan_to_num(
            np.asarray(close_prices, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0
        )
        if price_matrix.shape != (t_len, n_symbols):
            raise ValueError(
                "close_prices shape must match (num_days, num_symbols): "
                f"expected {(t_len, n_symbols)}, got {price_matrix.shape}"
            )
        if np.any(tw_symbol_mask):
            price_matrix[:, tw_symbol_mask] = _round_half_up(
                price_matrix[:, tw_symbol_mask], decimals=2
            )
        current_prices = np.where(price_matrix[0] > 1e-12, price_matrix[0], 1.0)
    else:
        price_matrix = None
        current_prices = np.ones(n_symbols, dtype=np.float64)
    if daily_volumes is not None:
        volume_matrix = np.asarray(daily_volumes, dtype=np.float64)
        if volume_matrix.shape != (t_len, n_symbols):
            raise ValueError(
                "daily_volumes shape must match (num_days, num_symbols): "
                f"expected {(t_len, n_symbols)}, got {volume_matrix.shape}"
            )
    else:
        volume_matrix = None
    shares = np.zeros(n_symbols, dtype=np.int64)
    cash = float(initial_capital)
    cash_hold_mode = False
    portfolio_alive = True

    records: list[HoldingsRecord] = []
    gross_leverage = _resolve_exposure_budget(gross_leverage)

    for t in range(t_len):
        if not portfolio_alive:
            # Absorbing ruin: no later signal may recreate capital.
            strategy_returns[t] = 0.0
            turnovers[t] = 0.0
            stock_weights_history[t] = 0.0
            shares.fill(0)
            cash = 0.0
            if collect_holdings:
                records.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol="CASH",
                        shares=0,
                        price=1.0,
                        market_value=0.0,
                        holding_ratio=0.0,
                        is_cash=True,
                    )
                )
            continue
        if cash_hold_mode:
            strategy_returns[t] = 0.0
            turnovers[t] = 0.0
            stock_weights_history[t] = 0.0
            if collect_holdings:
                records.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol="CASH",
                        shares=int(_floor_to_int64(cash, non_negative=True).item()),
                        price=1.0,
                        market_value=float(cash),
                        holding_ratio=1.0 if cash > 0 else 0.0,
                        is_cash=True,
                    )
                )
            continue

        if price_matrix is not None:
            current_prices = np.where(
                price_matrix[t] > 1e-12, price_matrix[t], current_prices
            )

        day_mask = m[t]
        target_w = np.nan_to_num(w[t], nan=0.0, posinf=0.0, neginf=0.0)
        target_w = _normalize_target_weights_row_numpy(
            target_w,
            long_only=long_only,
            gross_budget=gross_leverage,
            portfolio_activation=portfolio_activation,
        )
        target_w = _apply_min_trade_weight_row_numpy(target_w, min_trade_weight)
        # Match the tensor path: normalize the model's complete target first,
        # then leave unavailable allocations as cash/frozen holdings.  Masking
        # before L1 normalization would silently redistribute a halted or
        # terminal asset's desired allocation into unrelated symbols.
        target_w[~day_mask] = 0.0
        target_w[exit_m[t]] = 0.0

        equity_before = float(cash + np.dot(shares.astype(np.float64), current_prices))
        equity_before = max(equity_before, 1e-12)

        # Terminal lifecycle settlement mirrors the canonical tensor path.
        # A permanently non-tradable symbol is closed once at its last marked
        # price, bypassing ordinary side/turnover/volume constraints. Temporary
        # suspensions keep tradable=True and therefore remain frozen below.
        terminal_mask = exit_m[t] & (shares != 0)
        terminal_sell_qty = np.where(terminal_mask & (shares > 0), shares, 0)
        terminal_buy_qty = np.where(terminal_mask & (shares < 0), -shares, 0)
        terminal_sell_notional = float(
            np.dot(terminal_sell_qty.astype(np.float64), current_prices)
        )
        terminal_buy_notional = float(
            np.dot(terminal_buy_qty.astype(np.float64), current_prices)
        )
        if bool(terminal_mask.any()):
            cash += (
                terminal_sell_notional
                - terminal_sell_notional * sell_fee_rate
                - terminal_buy_notional
                - terminal_buy_notional * buy_fee_rate
            )
            shares[terminal_mask] = 0

        desired_value = equity_before * target_w
        safe_prices = np.where(current_prices > 1e-12, current_prices, np.inf)
        raw_target_shares = desired_value / safe_prices
        desired_shares = (
            _floor_to_int64(raw_target_shares, non_negative=True)
            if long_only
            else _trunc_to_int64(raw_target_shares)
        )
        if not long_only:
            desired_shares[force_m[t] & (desired_shares < 0)] = 0

        # Terminal-settled symbols stay at zero. A temporary suspension remains
        # tradable and is frozen by the ordinary side masks below.
        desired_shares[~day_mask] = shares[~day_mask]

        can_buy_day = buy_m[t]
        can_sell_day = sell_m[t]
        can_short_day = short_m[t]
        delta = desired_shares - shares
        if long_only:
            desired_shares[(delta > 0) & ~can_buy_day] = shares[
                (delta > 0) & ~can_buy_day
            ]
            desired_shares[(delta < 0) & ~can_sell_day] = shares[
                (delta < 0) & ~can_sell_day
            ]
        else:
            desired_shares[(delta > 0) & ~can_buy_day] = shares[
                (delta > 0) & ~can_buy_day
            ]
            constrained_shares = desired_shares.copy()
            down = constrained_shares < shares
            reduce_long = (
                down & (shares > 0) & (constrained_shares >= 0) & ~can_sell_day
            )
            constrained_shares[reduce_long] = shares[reduce_long]

            cross_from_long = down & (shares > 0) & (constrained_shares < 0)
            blocked_cross_sell = cross_from_long & ~can_sell_day
            constrained_shares[blocked_cross_sell] = shares[blocked_cross_sell]
            blocked_cross_short = cross_from_long & can_sell_day & ~can_short_day
            constrained_shares[blocked_cross_short] = 0

            open_or_increase_short = (
                down & (shares <= 0) & (constrained_shares < shares) & ~can_short_day
            )
            constrained_shares[open_or_increase_short] = shares[open_or_increase_short]
            desired_shares = constrained_shares

        delta = desired_shares - shares
        # Match the canonical tensor constraint order: first apply each
        # symbol's participation cap, then apply the portfolio-wide turnover
        # cap to the remaining executable order.  The operations do not
        # commute when only some symbols are volume constrained.
        if max_volume_participation > 0.0 and volume_matrix is not None:
            volume_day = volume_matrix[t]
            valid_volume = np.isfinite(volume_day) & (volume_day >= 0.0)
            max_qty = np.full(delta.shape, np.iinfo(np.int64).max, dtype=np.int64)
            if np.any(valid_volume):
                capped = np.floor(
                    volume_day[valid_volume] * float(max_volume_participation)
                )
                capped = np.clip(capped, 0.0, float(np.iinfo(np.int64).max))
                max_qty[valid_volume] = capped.astype(np.int64, copy=False)
            abs_delta = np.abs(delta.astype(np.int64, copy=False))
            clipped_abs = np.minimum(abs_delta, max_qty)
            desired_shares = shares + (
                np.sign(delta.astype(np.float64)) * clipped_abs.astype(np.float64)
            ).astype(np.int64)
            delta = desired_shares - shares

        if max_turnover_ratio > 0.0:
            traded_notional_before_cap = float(
                np.dot(np.abs(delta).astype(np.float64), current_prices)
            )
            max_traded_notional = float(equity_before * max_turnover_ratio)
            if (
                traded_notional_before_cap > max_traded_notional + 1e-9
                and traded_notional_before_cap > 0.0
            ):
                scale = max(0.0, max_traded_notional / traded_notional_before_cap)
                scaled_delta = np.sign(delta.astype(np.float64)) * np.floor(
                    np.abs(delta.astype(np.float64)) * scale
                )
                desired_shares = shares + scaled_delta.astype(np.int64)
                delta = desired_shares - shares

        forced_cover_now = np.zeros(n_symbols, dtype=bool)
        if not long_only:
            forced_cover_now = force_m[t] & (shares < 0) & can_buy_day
            desired_shares[forced_cover_now] = np.maximum(
                desired_shares[forced_cover_now],
                0,
            )
            delta = desired_shares - shares

        # Risk-budget guardrail: enforce gross exposure for both long-only and
        # long/short portfolios. Preserve every executable reduction and every
        # frozen position; only scale exposure-expanding shares. Scaling the
        # whole portfolio would fabricate a sale when can_sell=False or a cover
        # when can_buy=False.
        gross_notional = float(
            np.dot(np.abs(desired_shares).astype(np.float64), current_prices)
        )
        gross_cap_notional = float(equity_before * gross_leverage)
        if gross_notional > gross_cap_notional + 1e-9 and gross_notional > 0.0:
            same_side = (
                shares.astype(np.float64) * desired_shares.astype(np.float64)
            ) > 0.0
            reduced_abs = np.where(
                same_side,
                np.minimum(np.abs(shares), np.abs(desired_shares)),
                0,
            )
            reduced_base = (
                np.sign(shares.astype(np.float64)) * reduced_abs.astype(np.float64)
            ).astype(np.int64)
            expansion = desired_shares - reduced_base
            base_notional = float(
                np.dot(np.abs(reduced_base).astype(np.float64), current_prices)
            )
            expansion_notional = float(
                np.dot(np.abs(expansion).astype(np.float64), current_prices)
            )
            expansion_budget = max(0.0, gross_cap_notional - base_notional)
            scale = (
                min(1.0, expansion_budget / expansion_notional)
                if expansion_notional > 0.0
                else 0.0
            )
            scaled_expansion = _trunc_to_int64(expansion.astype(np.float64) * scale)
            desired_shares = reduced_base + scaled_expansion
            delta = desired_shares - shares

        sell_qty = np.clip(-delta, 0, None)
        buy_qty = np.clip(delta, 0, None)

        sell_notional = float(np.dot(sell_qty.astype(np.float64), current_prices))
        buy_notional = float(np.dot(buy_qty.astype(np.float64), current_prices))

        available_cash = cash + sell_notional - sell_notional * sell_fee_rate
        max_affordable_buy = (
            available_cash / (1.0 + buy_fee_rate)
            if buy_fee_rate >= 0.0
            else available_cash
        )

        if buy_notional > max_affordable_buy + 1e-9 and buy_notional > 0.0:
            # Forced short covers are mandatory.  Execute them in full and
            # shrink only discretionary buys to the remaining cash capacity.
            # A temporarily negative cash balance is preferable to retaining
            # an explicitly illegal short while positive marked equity remains.
            mandatory_buy_qty = np.where(
                forced_cover_now,
                np.minimum(buy_qty, np.maximum(-shares, 0)),
                0,
            )
            discretionary_buy_qty = buy_qty - mandatory_buy_qty
            mandatory_buy_notional = float(
                np.dot(mandatory_buy_qty.astype(np.float64), current_prices)
            )
            mandatory_cost = mandatory_buy_notional * (1.0 + buy_fee_rate)
            discretionary_cash = max(0.0, available_cash - mandatory_cost)
            max_discretionary_notional = (
                discretionary_cash / (1.0 + buy_fee_rate)
                if buy_fee_rate >= 0.0
                else discretionary_cash
            )
            discretionary_notional = float(
                np.dot(discretionary_buy_qty.astype(np.float64), current_prices)
            )
            scale = (
                min(1.0, max_discretionary_notional / discretionary_notional)
                if discretionary_notional > 0.0
                else 0.0
            )
            scaled_discretionary_qty = _floor_to_int64(
                discretionary_buy_qty.astype(np.float64) * scale,
                non_negative=True,
            )
            buy_qty = mandatory_buy_qty + scaled_discretionary_qty
            desired_shares = shares - sell_qty + buy_qty
            delta = desired_shares - shares
            sell_qty = np.clip(-delta, 0, None)
            buy_qty = np.clip(delta, 0, None)
            sell_notional = float(np.dot(sell_qty.astype(np.float64), current_prices))
            buy_notional = float(np.dot(buy_qty.astype(np.float64), current_prices))

        # Guardrail: if this trade plan would make same-day post-trade equity
        # non-positive, skip rebalancing for the day to keep position/equity
        # ratios well-defined and bounded.
        tentative_cash = (
            cash
            + sell_notional
            - sell_notional * sell_fee_rate
            - buy_notional
            - buy_notional * buy_fee_rate
        )
        tentative_equity_after_trade = float(
            tentative_cash + np.dot(desired_shares.astype(np.float64), current_prices)
        )
        if (
            not np.isfinite(tentative_equity_after_trade)
        ) or tentative_equity_after_trade <= 1e-9:
            desired_shares = shares.copy()
            delta = desired_shares - shares
            sell_qty = np.clip(-delta, 0, None)
            buy_qty = np.clip(delta, 0, None)
            sell_notional = 0.0
            buy_notional = 0.0

        # Cash-hold rule: if strategy wants stock exposure but cannot buy even 1 share,
        # stop trading and keep current cash through the remaining dates.
        if long_only:
            wanted_stock = bool(np.any(target_w > 0.0))
            has_any_share = bool(np.any(desired_shares > 0))
            if wanted_stock and not has_any_share:
                tradable_target = day_mask & (target_w > 0.0)
                candidate_prices = current_prices[tradable_target]
                candidate_prices = candidate_prices[
                    np.isfinite(candidate_prices) & (candidate_prices > 1e-12)
                ]
                if candidate_prices.size > 0:
                    min_buy_cost = float(candidate_prices.min() * (1.0 + buy_fee_rate))
                    if max_affordable_buy + 1e-12 < min_buy_cost:
                        # ``desired_shares`` is all cash at this point.  Settle
                        # every executable sale before entering permanent cash
                        # mode; merely zeroing ``shares`` would erase holdings
                        # without crediting proceeds, fees, or turnover.  Cash
                        # already includes any terminal settlement above.
                        cash += sell_notional - sell_notional * sell_fee_rate
                        shares = desired_shares.copy()
                        traded_notional = (
                            sell_notional
                            + terminal_buy_notional
                            + terminal_sell_notional
                        )
                        equity_cash = max(float(cash), 1e-12)
                        strategy_returns[t] = np.float32(
                            np.log(equity_cash / equity_before)
                        )
                        turnovers[t] = np.float32(traded_notional / equity_before)
                        stock_weights_history[t] = 0.0
                        cash_hold_mode = True
                        if collect_holdings:
                            records.append(
                                HoldingsRecord(
                                    date=date_text[t],
                                    symbol="CASH",
                                    shares=int(
                                        _floor_to_int64(cash, non_negative=True).item()
                                    ),
                                    price=1.0,
                                    market_value=float(cash),
                                    holding_ratio=1.0 if cash > 0 else 0.0,
                                    is_cash=True,
                                )
                            )
                        continue

        buy_fee = buy_fee_rate * buy_notional
        sell_fee = sell_fee_rate * sell_notional
        fee = buy_fee + sell_fee
        traded_notional = (
            buy_notional
            + sell_notional
            + terminal_buy_notional
            + terminal_sell_notional
        )

        shares = desired_shares
        cash = cash + sell_notional - sell_fee - buy_notional - buy_fee
        if cash < 0 and abs(cash) < 1e-7:
            cash = 0.0

        stock_market_values = shares.astype(np.float64) * current_prices
        equity_after_trade = float(cash + stock_market_values.sum())
        equity_after_trade = max(equity_after_trade, 1e-12)

        # These are realised accounting weights, not another model target.
        # Activation/L1 was already applied before sizing orders.  Applying it
        # again here distorts short portfolios (for example, stock=-1 and
        # cash=2 must remain those exact equity ratios and sum to one).
        stock_holding_ratio = stock_market_values / equity_after_trade
        cash_ratio = float(cash / equity_after_trade)

        stock_weights_history[t] = stock_holding_ratio.astype(np.float32)
        turnovers[t] = float(traded_notional / equity_before)

        if collect_holdings:
            day_rows: list[HoldingsRecord] = []
            day_rows.append(
                HoldingsRecord(
                    date=date_text[t],
                    symbol="CASH",
                    shares=int(_floor_to_int64(cash, non_negative=True).item()),
                    price=1.0,
                    market_value=float(cash),
                    holding_ratio=cash_ratio,
                    is_cash=True,
                )
            )
            nonzero = np.flatnonzero(shares != 0)
            for idx in nonzero.tolist():
                mv = float(stock_market_values[idx])
                price_out = float(current_prices[idx])
                if tw_symbol_mask[idx]:
                    price_out = float(_round_half_up(price_out, decimals=2).item())
                day_rows.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol=symbols[idx],
                        shares=int(shares[idx]),
                        price=price_out,
                        market_value=mv,
                        holding_ratio=float(stock_holding_ratio[idx]),
                        is_cash=False,
                    )
                )
            day_rows.sort(key=holding_record_abs_sort_key)
            records.extend(day_rows)

        # PnL follows the canonical return label (adj close when available).
        # close_prices are reserved for execution prices, integer share sizing,
        # turnover, and the holdings report.
        simple_returns = np.expm1(r[t])
        simple_returns = np.where(np.isfinite(simple_returns), simple_returns, 0.0)
        next_prices = current_prices * (1.0 + simple_returns)
        next_prices = np.where(
            np.isfinite(next_prices) & (next_prices > 1e-12),
            next_prices,
            current_prices,
        )

        equity_end = float(
            equity_after_trade + np.dot(stock_market_values, simple_returns)
        )
        net_simple_return = equity_end / equity_before - 1.0
        strategy_returns[t] = _portfolio_simple_returns_to_log_numpy(
            np.asarray([net_simple_return], dtype=np.float32)
        )[0]
        wealth_factor = 1.0 + net_simple_return
        if (
            not math.isfinite(wealth_factor)
            or wealth_factor <= _MIN_PORTFOLIO_WEALTH_FACTOR
        ):
            portfolio_alive = False
            shares.fill(0)
            cash = 0.0
        current_prices = next_prices

    return (
        BacktestResult(
            strategy_returns=strategy_returns,
            benchmark_returns=b.astype(np.float32),
            turnovers=turnovers,
            weights_history=stock_weights_history,
        ),
        records,
    )
