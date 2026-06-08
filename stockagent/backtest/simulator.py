from __future__ import annotations

import logging
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import torch
from torch.utils.checkpoint import checkpoint as checkpoint_fn

from stockagent.backtest.cpp_long_short import (
    cpp_long_short_enabled,
    run_long_short_cpp_autograd,
)


INT64_MIN_FLOAT_SAFE = np.nextafter(float(np.iinfo(np.int64).min), 0.0)
INT64_MAX_FLOAT_SAFE = np.nextafter(float(np.iinfo(np.int64).max + 1), 0.0)
SCAN_CHUNK_CANDIDATES = (64, 128, 256, 512)

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
        ],
    ],
] = {}
_SCAN_COMPILE_FAILED: set[tuple] = set()
_SCAN_COMPILE_STATS: dict[str, int] = {
    "hits": 0,
    "misses": 0,
    "failures": 0,
    "disabled": 0,
}


def _round_half_up(values: np.ndarray | float, decimals: int = 2) -> np.ndarray:
    """Round with half-up semantics (0.5 always rounds away from zero)."""
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


def _clip_to_int64_storage_bounds(values: np.ndarray | float, *, non_negative: bool = False) -> np.ndarray:
    """Clip numeric values to safe float bounds that can be cast to int64."""
    arr = np.nan_to_num(
        np.asarray(values, dtype=np.float64),
        nan=0.0,
        posinf=INT64_MAX_FLOAT_SAFE,
        neginf=0.0 if non_negative else INT64_MIN_FLOAT_SAFE,
    )
    lower = 0.0 if non_negative else INT64_MIN_FLOAT_SAFE
    return np.clip(arr, lower, INT64_MAX_FLOAT_SAFE)


def _floor_to_int64(values: np.ndarray | float, *, non_negative: bool = False) -> np.ndarray:
    """Floor and cast to int64 after clipping strictly to int64 storage bounds."""
    clipped = _clip_to_int64_storage_bounds(values, non_negative=non_negative)
    return np.floor(clipped).astype(np.int64)


def _trunc_to_int64(values: np.ndarray | float) -> np.ndarray:
    """Truncate toward zero and cast to int64 within safe storage bounds."""
    clipped = _clip_to_int64_storage_bounds(values, non_negative=False)
    return np.trunc(clipped).astype(np.int64)


def _resolve_exposure_budget(gross_leverage: float) -> float:
    """Return the effective absolute exposure budget in [0, 1]."""
    return min(1.0, max(0.0, float(gross_leverage)))


def _normalize_target_weights_numpy(
    weights: np.ndarray,
    *,
    long_only: bool,
    gross_budget: float,
) -> np.ndarray:
    """Normalize target weights via tanh + L1 and apply gross budget."""
    out = np.tanh(np.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32, copy=False))
    if long_only:
        out = np.clip(out, 0.0, None)
    l1 = np.abs(out).sum(axis=1, keepdims=True).astype(np.float32)
    out = out / np.clip(l1, 1e-12, None)
    out *= np.float32(gross_budget)
    return out.astype(np.float32, copy=False)


def _normalize_target_weights_row_numpy(
    weights_row: np.ndarray,
    *,
    long_only: bool,
    gross_budget: float,
) -> np.ndarray:
    """Single-row variant of tanh + L1 normalization for integer-share path."""
    row = np.tanh(np.nan_to_num(weights_row, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float64, copy=False))
    if long_only:
        row = np.clip(row, 0.0, None)
    l1 = float(np.abs(row).sum(dtype=np.float64))
    if l1 > 1e-12:
        row = row / l1
    else:
        row = np.zeros_like(row, dtype=np.float64)
    row *= float(gross_budget)
    return row


def _normalize_target_weights_torch(
    weights: torch.Tensor,
    *,
    long_only: bool,
    gross_budget: float,
) -> torch.Tensor:
    """Torch normalization via tanh + L1 and gross budget scaling."""
    out = torch.tanh(weights)
    if long_only:
        out = out.clamp_min(0.0)
    l1 = out.abs().sum(dim=1, keepdim=True).clamp_min(1e-12)
    out = out / l1
    leverage = torch.as_tensor(gross_budget, device=out.device, dtype=out.dtype)
    out = out * leverage
    return out


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


def _apply_turnover_cap_torch(
    prev_weights: torch.Tensor,
    target_weights: torch.Tensor,
    max_turnover_ratio: float,
) -> torch.Tensor:
    if max_turnover_ratio <= 0.0:
        return target_weights

    deltas = target_weights - prev_weights
    turnovers = deltas.abs().sum(dim=1, keepdim=True)
    cap = torch.as_tensor(max_turnover_ratio, device=turnovers.device, dtype=turnovers.dtype)
    scale = torch.ones_like(turnovers)
    scale = torch.where(turnovers > cap, cap / turnovers.clamp_min(1e-12), scale)
    scale = scale.clamp_(0.0, 1.0)
    return prev_weights + deltas * scale


def _env_flag(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in {"0", "false", "off", "no"}


def _env_int(name: str, default: int = 0) -> int:
    raw = os.environ.get(name, str(default)).strip()
    try:
        return int(raw)
    except Exception:
        return default


def _torch_dynamo_is_compiling() -> bool:
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
    env_bin = Path(sys.executable).resolve().parent
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
    prepend = [part for part in entries if part and Path(part).exists() and part not in existing_parts]
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
            return False
    return enabled


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


def _mark_static_shape(tensor: torch.Tensor | None, dims: list[int] | None = None) -> None:
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
    scan_chunk_size: int,
    record_weights_history: bool,
) -> tuple:
    return (
        str(weights.device),
        int(weights.size(1)),
        str(weights.dtype),
        bool(long_only),
        float(max_turnover_ratio),
        float(gross_budget),
        int(scan_chunk_size),
        bool(record_weights_history),
    )


def _scan_runner_factory(
    *,
    long_only: bool,
    max_turnover_ratio: float,
    gross_budget: float,
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
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        if long_only:
            return _vectorized_backtest_torch_scan_long_only(
                weights,
                future_returns,
                tradable_mask,
                can_buy_mask,
                can_sell_mask,
                prev_init=prev_init,
                max_turnover_ratio=max_turnover_ratio,
                scan_chunk_size=scan_chunk_size,
                record_weights_history=record_weights_history,
            )
        return _vectorized_backtest_torch_scan_long_short(
            weights,
            future_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            prev_init=prev_init,
            max_turnover_ratio=max_turnover_ratio,
            gross_budget=gross_budget,
            scan_chunk_size=scan_chunk_size,
            record_weights_history=record_weights_history,
        )

    return _runner


def _autotune_scan_chunk_size(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    long_only: bool,
    max_turnover_ratio: float,
    gross_budget: float,
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
    prev_probe = torch.zeros_like(w_probe[0])

    candidates = [c for c in SCAN_CHUNK_CANDIDATES if c <= probe_rows]
    if not candidates:
        candidates = [max(1, probe_rows)]

    def _measure(chunk_size: int) -> float:
        runner = _scan_runner_factory(
            long_only=long_only,
            max_turnover_ratio=max_turnover_ratio,
            gross_budget=gross_budget,
            scan_chunk_size=chunk_size,
            record_weights_history=True,
        )
        with torch.inference_mode():
            _ = runner(w_probe, r_probe, t_probe, buy_probe, sell_probe, prev_probe)
            if weights.device.type == "cuda":
                torch.cuda.synchronize(weights.device)
            start = time.perf_counter()
            _ = runner(w_probe, r_probe, t_probe, buy_probe, sell_probe, prev_probe)
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
    scan_chunk_size: int,
    record_weights_history: bool,
):
    base_runner = _scan_runner_factory(
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        gross_budget=gross_budget,
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

    if not _compile_enabled() or weights.device.type != "cuda" or not hasattr(torch, "compile"):
        return base_runner

    key = _scan_compile_key(
        weights,
        long_only,
        max_turnover_ratio,
        gross_budget,
        scan_chunk_size,
        record_weights_history,
    )
    if key in _SCAN_COMPILE_FAILED:
        _SCAN_COMPILE_STATS["disabled"] += 1
        if _compile_verbose():
            print(
                "[backtest compile] disabled after previous failure for "
                f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, record_weights={record_weights_history})"
            )
        return base_runner

    cached = _SCAN_COMPILED_CACHE.get(key)
    if cached is not None:
        _SCAN_COMPILE_STATS["hits"] += 1
        if _compile_verbose():
            print(
                "[backtest compile] cache hit for "
                f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, record_weights={record_weights_history})"
            )
        return cached

    try:
        _SCAN_COMPILE_STATS["misses"] += 1
        # Keep symbol axis static while allowing varying time/batch length.
        _mark_static_shape(weights, [1])
        if _compile_verbose():
            print(
                "[backtest compile] compiling fixed-shape runner for "
                f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, record_weights={record_weights_history})"
            )
        _configure_inductor_cudagraphs()
        compiled = torch.compile(base_runner, mode="reduce-overhead", dynamic=True)
        _SCAN_COMPILED_CACHE[key] = compiled
        return compiled
    except Exception:
        _SCAN_COMPILE_FAILED.add(key)
        _SCAN_COMPILE_STATS["failures"] += 1
        print(
            "[backtest compile] compile failed, falling back to eager for "
            f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, record_weights={record_weights_history})"
        )
        return base_runner


def _fallback_scan_runner_after_runtime_failure(
    *,
    error: Exception,
    weights: torch.Tensor,
    long_only: bool,
    max_turnover_ratio: float,
    gross_budget: float,
    scan_chunk_size: int,
    record_weights_history: bool,
):
    key = _scan_compile_key(
        weights,
        long_only,
        max_turnover_ratio,
        gross_budget,
        scan_chunk_size,
        record_weights_history,
    )
    _SCAN_COMPILE_FAILED.add(key)
    _SCAN_COMPILED_CACHE.pop(key, None)
    _SCAN_COMPILE_STATS["failures"] += 1
    print(
        "[backtest compile] runtime failed, falling back to eager for "
        f"shape=(T={int(weights.size(0))}, S={int(weights.size(1))}, "
        f"dtype={weights.dtype}, long_only={long_only}, scan_chunk={scan_chunk_size}, "
        f"record_weights={record_weights_history}): {type(error).__name__}: {str(error)[:300]}"
    )
    return _scan_runner_factory(
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        gross_budget=gross_budget,
        scan_chunk_size=scan_chunk_size,
        record_weights_history=record_weights_history,
    )


@dataclass(slots=True)
class BacktestResult:
    """Container for a single backtest simulation run."""

    strategy_returns: np.ndarray   # [T] net daily returns after costs
    benchmark_returns: np.ndarray  # [T] universe-average daily returns
    turnovers: np.ndarray          # [T] total absolute weight change per day
    weights_history: np.ndarray    # [T, S] realised portfolio weights


@dataclass(slots=True)
class BacktestResultTensor:
    """Torch tensor container for a single backtest simulation run."""

    strategy_returns: torch.Tensor   # [T]
    benchmark_returns: torch.Tensor  # [T]
    turnovers: torch.Tensor          # [T]
    weights_history: torch.Tensor    # [T, S], may be empty when caller disables history recording.
    final_weights: torch.Tensor | None = None  # [S], realised weights after the final simulated day.

    def to_numpy(self) -> BacktestResult:
        return BacktestResult(
            strategy_returns=self.strategy_returns.detach().cpu().numpy().astype(np.float32),
            benchmark_returns=self.benchmark_returns.detach().cpu().numpy().astype(np.float32),
            turnovers=self.turnovers.detach().cpu().numpy().astype(np.float32),
            weights_history=self.weights_history.detach().cpu().numpy().astype(np.float32),
        )


@dataclass(slots=True)
class HoldingsRecord:
    """Single holding record for one date/symbol, sorted by holding ratio."""

    date: str
    symbol: str
    shares: int
    price: float
    market_value: float
    holding_ratio: float
    is_cash: bool


def _vectorized_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    can_buy_mask: np.ndarray | None,
    can_sell_mask: np.ndarray | None,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    gross_leverage: float = 1.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    target_weights = np.asarray(weights, dtype=np.float32).copy()
    gross_budget = _resolve_exposure_budget(gross_leverage)
    tradable = tradable_mask.astype(bool)
    buy_mask = tradable if can_buy_mask is None else can_buy_mask.astype(bool)
    sell_mask = tradable if can_sell_mask is None else can_sell_mask.astype(bool)

    target_weights = _normalize_target_weights_numpy(
        target_weights,
        long_only=long_only,
        gross_budget=gross_budget,
    )

    t_len, n_symbols = target_weights.shape
    weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)
    buy_turnovers = np.zeros((t_len,), dtype=np.float32)
    sell_turnovers = np.zeros((t_len,), dtype=np.float32)

    prev = np.zeros((n_symbols,), dtype=np.float32)
    for t in range(t_len):
        target_t = target_weights[t].copy()
        tradable_t = tradable[t]
        # If symbol is not tradable today, keep previous holdings instead of forcing liquidation.
        target_t[~tradable_t] = prev[~tradable_t]

        delta = target_t - prev
        buy_t = buy_mask[t]
        sell_t = sell_mask[t]
        delta[(delta > 0.0) & ~buy_t] = 0.0
        delta[(delta < 0.0) & ~sell_t] = 0.0

        if long_only:
            sell_deltas = np.clip(delta, None, 0.0)
            base_after_sells = prev + sell_deltas
            buy_deltas = np.clip(delta, 0.0, None)
            buy_sum = float(buy_deltas.sum(dtype=np.float32))
            buy_capacity = max(0.0, 1.0 - float(base_after_sells.sum(dtype=np.float32)))
            if buy_sum > buy_capacity and buy_sum > 0.0:
                buy_deltas *= np.float32(buy_capacity / buy_sum)
            delta = sell_deltas + buy_deltas

        next_weights = prev + delta
        if max_turnover_ratio > 0.0:
            next_weights = _apply_turnover_cap_numpy(prev[None, :], next_weights[None, :], max_turnover_ratio)[0]
            delta = next_weights - prev

        if not long_only:
            gross_next = float(np.abs(next_weights).sum(dtype=np.float32))
            if gross_next > gross_budget and gross_next > 0.0:
                next_weights = next_weights * np.float32(gross_budget / gross_next)
                delta = next_weights - prev

        weights_history[t] = next_weights.astype(np.float32, copy=False)
        buy_turnovers[t] = np.clip(delta, 0.0, None).sum(dtype=np.float32)
        sell_turnovers[t] = np.clip(-delta, 0.0, None).sum(dtype=np.float32)
        prev = next_weights.astype(np.float32, copy=False)

    turnovers = (buy_turnovers + sell_turnovers).astype(np.float32)
    gross = np.einsum("ts,ts->t", weights_history, future_returns, dtype=np.float32)
    strategy_returns = gross - buy_fee_rate * buy_turnovers - sell_fee_rate * sell_turnovers
    return strategy_returns.astype(np.float32), turnovers, weights_history


def _vectorized_backtest_torch_scan_long_only(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    prev_init: torch.Tensor | None = None,
    max_turnover_ratio: float = 0.0,
    scan_chunk_size: int = 256,
    record_weights_history: bool = True,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    future_returns_t = future_returns.to(device=weights.device, dtype=weights.dtype)
    target_weights = weights
    tradable = tradable_mask
    buy_mask = tradable if can_buy_mask is None else can_buy_mask
    sell_mask = tradable if can_sell_mask is None else can_sell_mask

    t_len, n_symbols = target_weights.shape
    dtype = target_weights.dtype
    device = target_weights.device
    prev = prev_init.to(device=device, dtype=dtype) if prev_init is not None else torch.zeros_like(target_weights[0])
    weights_history = (
        torch.empty((t_len, n_symbols), device=device, dtype=dtype)
        if record_weights_history
        else torch.empty((0, n_symbols), device=device, dtype=dtype)
    )
    buy_turnovers = torch.empty((t_len,), device=device, dtype=dtype)
    sell_turnovers = torch.empty((t_len,), device=device, dtype=dtype)
    gross_returns = torch.empty((t_len,), device=device, dtype=dtype)
    cap = torch.as_tensor(max_turnover_ratio, device=device, dtype=dtype)
    chunk_size = max(1, int(scan_chunk_size))
    one = torch.ones((), device=device, dtype=dtype)

    for start in range(0, t_len, chunk_size):
        end = min(start + chunk_size, t_len)
        target_chunk = target_weights[start:end]
        tradable_chunk = tradable[start:end]
        buy_chunk = buy_mask[start:end]
        sell_chunk = sell_mask[start:end]

        for offset in range(end - start):
            idx = start + offset
            target_t = torch.where(tradable_chunk[offset], target_chunk[offset], prev)

            delta = target_t - prev
            buy_delta = delta.clamp_min(0.0) * buy_chunk[offset].to(dtype=dtype)
            sell_delta = delta.clamp_max(0.0) * sell_chunk[offset].to(dtype=dtype)
            base_after_sells = prev + sell_delta
            buy_sum = buy_delta.sum()
            buy_capacity = (one - base_after_sells.sum()).clamp_min(0.0)
            buy_scale = torch.minimum(
                torch.ones_like(buy_sum),
                buy_capacity / buy_sum.clamp_min(1e-12),
            )
            delta = sell_delta + buy_delta * buy_scale

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
            buy_turnovers[idx] = delta.clamp_min(0.0).sum()
            sell_turnovers[idx] = (-delta).clamp_min(0.0).sum()
            gross_returns[idx] = (next_weights * future_returns_t[idx]).sum()
            prev = next_weights

    turnovers = buy_turnovers + sell_turnovers
    return turnovers, buy_turnovers, sell_turnovers, gross_returns, weights_history, prev


def _vectorized_backtest_torch_scan_long_short(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    prev_init: torch.Tensor | None = None,
    max_turnover_ratio: float = 0.0,
    gross_budget: float = 1.0,
    scan_chunk_size: int = 256,
    record_weights_history: bool = True,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    future_returns_t = future_returns.to(device=weights.device, dtype=weights.dtype)

    target_weights = weights
    tradable = tradable_mask
    buy_mask = tradable if can_buy_mask is None else can_buy_mask
    sell_mask = tradable if can_sell_mask is None else can_sell_mask

    t_len, n_symbols = target_weights.shape
    dtype = target_weights.dtype
    device = target_weights.device
    prev = prev_init.to(device=device, dtype=dtype) if prev_init is not None else torch.zeros_like(target_weights[0])
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

        for offset in range(end - start):
            idx = start + offset
            target_t = torch.where(tradable_chunk[offset], target_chunk[offset], prev)

            delta = target_t - prev
            buy_delta = delta.clamp_min(0.0) * buy_chunk[offset].to(dtype=dtype)
            sell_delta = delta.clamp_max(0.0) * sell_chunk[offset].to(dtype=dtype)
            delta = sell_delta + buy_delta

            next_weights = prev + delta
            if max_turnover_ratio > 0.0:
                turnover = delta.abs().sum()
                turnover_scale = torch.minimum(
                    torch.ones_like(turnover),
                    cap / turnover.clamp_min(1e-12),
                )
                next_weights = prev + delta * turnover_scale
                delta = next_weights - prev

            gross_next = next_weights.abs().sum()
            gross_scale = torch.minimum(
                torch.ones_like(gross_next),
                gross_cap / gross_next.clamp_min(1e-12),
            )
            next_weights = next_weights * gross_scale
            delta = next_weights - prev

            if record_weights_history:
                weights_history[idx] = next_weights
            buy_turnovers[idx] = delta.clamp_min(0.0).sum()
            sell_turnovers[idx] = (-delta).clamp_min(0.0).sum()
            gross_returns[idx] = (next_weights * future_returns_t[idx]).sum()
            prev = next_weights

    turnovers = buy_turnovers + sell_turnovers
    return turnovers, buy_turnovers, sell_turnovers, gross_returns, weights_history, prev


def _prepare_scan_inputs(
    weights: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    long_only: bool,
    gross_leverage: float,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    compute_dtype = _supported_scan_dtype(weights.dtype)
    target_weights = weights.to(dtype=compute_dtype)
    gross_budget = _resolve_exposure_budget(gross_leverage)
    device = target_weights.device
    tradable = tradable_mask.to(device=device, dtype=torch.bool)
    buy_mask = tradable if can_buy_mask is None else can_buy_mask.to(device=device, dtype=torch.bool)
    sell_mask = tradable if can_sell_mask is None else can_sell_mask.to(device=device, dtype=torch.bool)

    target_weights = _normalize_target_weights_torch(
        target_weights,
        long_only=long_only,
        gross_budget=gross_budget,
    )

    return target_weights, tradable, buy_mask, sell_mask


def _vectorized_backtest_torch(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor | None,
    can_sell_mask: torch.Tensor | None,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    gross_leverage: float = 1.0,
    scan_chunk_size: int | None = None,
    return_weights_history: bool = True,
    dense_mask_constraints: bool = False,
    initial_weights: torch.Tensor | None = None,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    gross_budget = _resolve_exposure_budget(gross_leverage)
    effective_max_turnover_ratio = float(max_turnover_ratio)
    max_possible_turnover = 2.0 * gross_budget
    if effective_max_turnover_ratio >= max_possible_turnover:
        effective_max_turnover_ratio = 0.0
    prepped_weights, prepped_tradable, prepped_buy, prepped_sell = _prepare_scan_inputs(
        weights,
        tradable_mask,
        can_buy_mask,
        can_sell_mask,
        long_only,
        gross_leverage,
    )
    prev_init = (
        torch.nan_to_num(
            initial_weights.to(device=prepped_weights.device, dtype=prepped_weights.dtype),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
        if initial_weights is not None
        else torch.zeros_like(prepped_weights[0])
    )

    # Fast path: no tradability/side restrictions and no turnover cap.
    # In this case, each day's realised target equals model target, so we can
    # compute turnover/returns via pure tensor ops without recurrent scan.
    use_dense_fast_path = effective_max_turnover_ratio <= 0.0 and bool(dense_mask_constraints)
    if use_dense_fast_path:
        returns_t = future_returns.to(device=prepped_weights.device, dtype=prepped_weights.dtype)
        deltas = torch.empty_like(prepped_weights)
        deltas[0] = prepped_weights[0] - prev_init
        if int(prepped_weights.size(0)) > 1:
            deltas[1:] = prepped_weights[1:] - prepped_weights[:-1]

        buy_turnovers = deltas.clamp_min(0.0).sum(dim=1)
        sell_turnovers = (-deltas).clamp_min(0.0).sum(dim=1)
        turnovers = buy_turnovers + sell_turnovers
        gross_returns = (prepped_weights * returns_t).sum(dim=1)
        strategy_returns = gross_returns - float(buy_fee_rate) * buy_turnovers - float(sell_fee_rate) * sell_turnovers

        if return_weights_history:
            weights_history = prepped_weights
        else:
            weights_history = prepped_weights.new_empty((0, prepped_weights.size(1)))

        returns_dtype = prepped_weights.dtype
        return (
            strategy_returns.to(returns_dtype),
            turnovers.to(returns_dtype),
            weights_history.to(returns_dtype),
            prepped_weights[-1].to(returns_dtype),
        )

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
            long_only,
            effective_max_turnover_ratio,
            gross_budget,
        )
    )
    use_cpp_long_short = (
        not _torch_dynamo_is_compiling()
        and cpp_long_short_enabled()
        and not long_only
        and prepped_weights.device.type == "cuda"
        and initial_weights is None
    )
    if use_cpp_long_short:
        try:
            strategy_returns, turnovers, weights_history = run_long_short_cpp_autograd(
                prepped_weights,
                future_returns.to(device=prepped_weights.device, dtype=prepped_weights.dtype),
                prepped_tradable,
                prepped_buy,
                prepped_sell,
                buy_fee_rate,
                sell_fee_rate,
                effective_max_turnover_ratio,
                gross_budget,
            )
            final_weights = weights_history[-1]
            if not return_weights_history:
                weights_history = prepped_weights.new_empty((0, prepped_weights.size(1)))
            return strategy_returns, turnovers, weights_history, final_weights
        except Exception as e:
            if _compile_verbose():
                print(f"[backtest cpp] long-short extension failed, falling back to eager scan: {e}")

    checkpoint_rows = _checkpoint_chunk_rows()
    use_checkpoint = (
        checkpoint_rows > 0
        and torch.is_grad_enabled()
        and prepped_weights.requires_grad
    )

    if use_checkpoint:
        # Checkpoint recomputation and cudagraph-captured compiled functions can
        # conflict in backward. Use eager runner for this path to keep training stable.
        runner = _scan_runner_factory(
            long_only=long_only,
            max_turnover_ratio=effective_max_turnover_ratio,
            gross_budget=gross_budget,
            scan_chunk_size=resolved_chunk,
            record_weights_history=return_weights_history,
        )
    else:
        if initial_weights is not None:
            # Recurrent/stateful training feeds previous-step portfolio state into
            # the next call. Keeping this path eager avoids CUDA graph output
            # aliasing/overwrite hazards from compiled scan runners.
            runner = _scan_runner_factory(
                long_only=long_only,
                max_turnover_ratio=effective_max_turnover_ratio,
                gross_budget=gross_budget,
                scan_chunk_size=resolved_chunk,
                record_weights_history=return_weights_history,
            )
        else:
            runner = _resolve_scan_runner(
                prepped_weights,
                long_only=long_only,
                max_turnover_ratio=effective_max_turnover_ratio,
                gross_budget=gross_budget,
                scan_chunk_size=resolved_chunk,
                record_weights_history=return_weights_history,
            )

    if not use_checkpoint:
        try:
            turnovers, buy_turnovers, sell_turnovers, gross_returns, weights_history, final_weights = runner(
                prepped_weights,
                future_returns,
                prepped_tradable,
                prepped_buy,
                prepped_sell,
                prev_init,
            )
        except Exception as e:
            if _torch_dynamo_is_compiling() or not (
                _compile_enabled() and prepped_weights.device.type == "cuda" and hasattr(torch, "compile")
            ):
                raise
            runner = _fallback_scan_runner_after_runtime_failure(
                error=e,
                weights=prepped_weights,
                long_only=long_only,
                max_turnover_ratio=effective_max_turnover_ratio,
                gross_budget=gross_budget,
                scan_chunk_size=resolved_chunk,
                record_weights_history=return_weights_history,
            )
            turnovers, buy_turnovers, sell_turnovers, gross_returns, weights_history, final_weights = runner(
                prepped_weights,
                future_returns,
                prepped_tradable,
                prepped_buy,
                prepped_sell,
                prev_init,
            )
    else:
        chunk_rows = max(1, int(checkpoint_rows))
        turnovers_chunks: list[torch.Tensor] = []
        buy_chunks: list[torch.Tensor] = []
        sell_chunks: list[torch.Tensor] = []
        gross_chunks: list[torch.Tensor] = []
        weights_chunks: list[torch.Tensor] = [] if return_weights_history else []
        prev = prev_init

        for start in range(0, int(prepped_weights.size(0)), chunk_rows):
            end = min(start + chunk_rows, int(prepped_weights.size(0)))
            w_chunk = prepped_weights[start:end]
            r_chunk = future_returns[start:end]
            t_chunk = prepped_tradable[start:end]
            b_chunk = prepped_buy[start:end]
            s_chunk = prepped_sell[start:end]

            chunk_out = checkpoint_fn(
                runner,
                w_chunk,
                r_chunk,
                t_chunk,
                b_chunk,
                s_chunk,
                prev,
                use_reentrant=False,
                preserve_rng_state=False,
            )
            t_out, b_out, s_out, g_out, w_out, last_w = chunk_out
            turnovers_chunks.append(t_out)
            buy_chunks.append(b_out)
            sell_chunks.append(s_out)
            gross_chunks.append(g_out)
            if return_weights_history:
                weights_chunks.append(w_out)
            prev = last_w

        turnovers = torch.cat(turnovers_chunks, dim=0)
        buy_turnovers = torch.cat(buy_chunks, dim=0)
        sell_turnovers = torch.cat(sell_chunks, dim=0)
        gross_returns = torch.cat(gross_chunks, dim=0)
        if return_weights_history:
            weights_history = torch.cat(weights_chunks, dim=0)
        else:
            weights_history = prepped_weights.new_empty((0, prepped_weights.size(1)))
        final_weights = prev

    returns_dtype = prepped_weights.dtype
    strategy_returns = gross_returns - float(buy_fee_rate) * buy_turnovers - float(sell_fee_rate) * sell_turnovers
    return (
        strategy_returns.to(returns_dtype),
        turnovers.to(returns_dtype),
        weights_history.to(returns_dtype),
        final_weights.to(returns_dtype),
    )


def run_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    gross_leverage: float = 1.0,
    can_buy_mask: np.ndarray | None = None,
    can_sell_mask: np.ndarray | None = None,
) -> BacktestResult:
    """Simulate daily portfolio execution from model weights."""
    strategy_returns, turnovers, weights_history = _vectorized_backtest(
        weights,
        future_returns,
        tradable_mask,
        can_buy_mask,
        can_sell_mask,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        gross_leverage=gross_leverage,
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
    gross_leverage: float = 1.0,
    can_buy_mask: torch.Tensor | None = None,
    can_sell_mask: torch.Tensor | None = None,
    scan_chunk_size: int | None = None,
    return_weights_history: bool = True,
    dense_mask_constraints: bool = False,
    initial_weights: torch.Tensor | None = None,
) -> BacktestResultTensor:
    """Simulate daily portfolio execution from model weights in torch."""
    strategy_returns, turnovers, weights_history, final_weights = _vectorized_backtest_torch(
        weights,
        future_returns,
        tradable_mask,
        can_buy_mask,
        can_sell_mask,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        gross_leverage=gross_leverage,
        scan_chunk_size=scan_chunk_size,
        return_weights_history=return_weights_history,
        dense_mask_constraints=dense_mask_constraints,
        initial_weights=initial_weights,
    )

    return BacktestResultTensor(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns.to(device=strategy_returns.device, dtype=strategy_returns.dtype),
        turnovers=turnovers,
        weights_history=weights_history,
        final_weights=final_weights,
    )


def run_backtest_integer_shares(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    can_buy_mask: np.ndarray | None = None,
    can_sell_mask: np.ndarray | None = None,
    *,
    initial_capital: float = 1_000_000.0,
    buy_fee_rate: float = 0.001425,
    sell_fee_rate: float = 0.004425,
    long_only: bool = True,
    max_turnover_ratio: float = 0.0,
    gross_leverage: float = 1.0,
    close_prices: np.ndarray | None = None,
    symbols: list[str] | None = None,
    dates: np.ndarray | None = None,
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    """Daily backtest with integer shares, virtual cash, and daily fee settlement.

    Trading assumptions:
    - Initial capital is cash only.
    - Stock shares are integer lots: floor(target_value / current_price).
    - Buy and sell fees are charged separately by buy_fee_rate/sell_fee_rate.
    - Cash is a virtual asset with 0 daily return.
    """
    w = np.asarray(weights, dtype=np.float64)
    r = np.nan_to_num(np.asarray(future_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    m = np.asarray(tradable_mask, dtype=bool)
    buy_m = m if can_buy_mask is None else np.asarray(can_buy_mask, dtype=bool)
    sell_m = m if can_sell_mask is None else np.asarray(can_sell_mask, dtype=bool)
    b = np.nan_to_num(np.asarray(benchmark_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)

    t_len, n_symbols = w.shape
    if symbols is None:
        symbols = [f"SYM_{idx:04d}" for idx in range(n_symbols)]
    tw_symbol_mask = np.asarray([_is_tw_symbol(sym) for sym in symbols], dtype=bool)
    if dates is None:
        date_text = [f"t{idx:04d}" for idx in range(t_len)]
    else:
        date_text = [str(np.datetime_as_string(np.asarray(d, dtype="datetime64[D]"), unit="D")) for d in dates]

    strategy_returns = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    stock_weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)

    if close_prices is not None:
        price_matrix = np.nan_to_num(np.asarray(close_prices, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
        if price_matrix.shape != (t_len, n_symbols):
            raise ValueError(
                "close_prices shape must match (num_days, num_symbols): "
                f"expected {(t_len, n_symbols)}, got {price_matrix.shape}"
            )
        if np.any(tw_symbol_mask):
            price_matrix[:, tw_symbol_mask] = _round_half_up(price_matrix[:, tw_symbol_mask], decimals=2)
        current_prices = np.where(price_matrix[0] > 1e-12, price_matrix[0], 1.0)
    else:
        price_matrix = None
        current_prices = np.ones(n_symbols, dtype=np.float64)
    shares = np.zeros(n_symbols, dtype=np.int64)
    cash = float(initial_capital)
    cash_hold_mode = False

    records: list[HoldingsRecord] = []
    gross_leverage = _resolve_exposure_budget(gross_leverage)

    for t in range(t_len):
        if cash_hold_mode:
            strategy_returns[t] = 0.0
            turnovers[t] = 0.0
            stock_weights_history[t] = 0.0
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
            current_prices = np.where(price_matrix[t] > 1e-12, price_matrix[t], current_prices)

        day_mask = m[t]
        target_w = np.nan_to_num(w[t], nan=0.0, posinf=0.0, neginf=0.0)
        target_w[~day_mask] = 0.0

        target_w = _normalize_target_weights_row_numpy(
            target_w,
            long_only=long_only,
            gross_budget=gross_leverage,
        )

        equity_before = float(cash + np.dot(shares.astype(np.float64), current_prices))
        equity_before = max(equity_before, 1e-12)

        desired_value = equity_before * target_w
        safe_prices = np.where(current_prices > 1e-12, current_prices, np.inf)
        raw_target_shares = desired_value / safe_prices
        desired_shares = _floor_to_int64(raw_target_shares, non_negative=True) if long_only else _trunc_to_int64(raw_target_shares)

        # Non-tradable symbols keep existing shares.
        desired_shares[~day_mask] = shares[~day_mask]

        can_buy_day = buy_m[t]
        can_sell_day = sell_m[t]
        delta = desired_shares - shares
        desired_shares[(delta > 0) & ~can_buy_day] = shares[(delta > 0) & ~can_buy_day]
        desired_shares[(delta < 0) & ~can_sell_day] = shares[(delta < 0) & ~can_sell_day]

        delta = desired_shares - shares
        if max_turnover_ratio > 0.0:
            traded_notional_before_cap = float(np.dot(np.abs(delta).astype(np.float64), current_prices))
            max_traded_notional = float(equity_before * max_turnover_ratio)
            if traded_notional_before_cap > max_traded_notional + 1e-9 and traded_notional_before_cap > 0.0:
                scale = max(0.0, max_traded_notional / traded_notional_before_cap)
                scaled_delta = np.sign(delta.astype(np.float64)) * np.floor(np.abs(delta.astype(np.float64)) * scale)
                desired_shares = shares + scaled_delta.astype(np.int64)
                delta = desired_shares - shares

        # Risk-budget guardrail: enforce |long| + |short| <= gross_leverage using
        # current-day prices before fees and next-day return realization.
        if not long_only:
            gross_notional = float(np.dot(np.abs(desired_shares).astype(np.float64), current_prices))
            gross_cap_notional = float(equity_before * gross_leverage)
            if gross_notional > gross_cap_notional + 1e-9 and gross_notional > 0.0:
                scale = max(0.0, gross_cap_notional / gross_notional)
                desired_shares = _trunc_to_int64(desired_shares.astype(np.float64) * scale)
                delta = desired_shares - shares

        sell_qty = np.clip(-delta, 0, None)
        buy_qty = np.clip(delta, 0, None)

        sell_notional = float(np.dot(sell_qty.astype(np.float64), current_prices))
        buy_notional = float(np.dot(buy_qty.astype(np.float64), current_prices))

        available_cash = cash + sell_notional - sell_notional * sell_fee_rate
        max_affordable_buy = available_cash / (1.0 + buy_fee_rate) if buy_fee_rate >= 0.0 else available_cash

        if buy_notional > max_affordable_buy + 1e-9 and buy_notional > 0.0:
            scale = max(0.0, max_affordable_buy / buy_notional)
            scaled_buy_qty = buy_qty.astype(np.float64) * scale
            buy_qty = _floor_to_int64(scaled_buy_qty, non_negative=True)
            desired_shares = shares - sell_qty + buy_qty
            delta = desired_shares - shares
            sell_qty = np.clip(-delta, 0, None)
            buy_qty = np.clip(delta, 0, None)
            sell_notional = float(np.dot(sell_qty.astype(np.float64), current_prices))
            buy_notional = float(np.dot(buy_qty.astype(np.float64), current_prices))

        # Guardrail: if this trade plan would make same-day post-trade equity
        # non-positive, skip rebalancing for the day to keep position/equity
        # ratios well-defined and bounded.
        tentative_cash = cash + sell_notional - sell_notional * sell_fee_rate - buy_notional - buy_notional * buy_fee_rate
        tentative_equity_after_trade = float(tentative_cash + np.dot(desired_shares.astype(np.float64), current_prices))
        if (not np.isfinite(tentative_equity_after_trade)) or tentative_equity_after_trade <= 1e-9:
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
                tradable_target = (day_mask & (target_w > 0.0))
                candidate_prices = current_prices[tradable_target]
                candidate_prices = candidate_prices[np.isfinite(candidate_prices) & (candidate_prices > 1e-12)]
                if candidate_prices.size > 0:
                    min_buy_cost = float(candidate_prices.min() * (1.0 + buy_fee_rate))
                    if max_affordable_buy + 1e-12 < min_buy_cost:
                        strategy_returns[t] = 0.0
                        turnovers[t] = 0.0
                        stock_weights_history[t] = 0.0
                        shares.fill(0)
                        cash_hold_mode = True
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

        buy_fee = buy_fee_rate * buy_notional
        sell_fee = sell_fee_rate * sell_notional
        fee = buy_fee + sell_fee
        traded_notional = buy_notional + sell_notional

        shares = desired_shares
        cash = cash + sell_notional - sell_fee - buy_notional - buy_fee
        if cash < 0 and abs(cash) < 1e-7:
            cash = 0.0

        stock_market_values = shares.astype(np.float64) * current_prices
        equity_after_trade = float(cash + stock_market_values.sum())
        equity_after_trade = max(equity_after_trade, 1e-12)

        # Output normalization for holdings report:
        # 1) tanh keeps signed direction in (-1, 1)
        # 2) L1 normalization keeps total absolute exposure at 1.
        stock_holding_ratio_raw = stock_market_values / equity_after_trade
        cash_ratio_raw = float(cash / equity_after_trade)
        ratio_vec = np.empty(n_symbols + 1, dtype=np.float64)
        ratio_vec[0] = cash_ratio_raw
        ratio_vec[1:] = stock_holding_ratio_raw
        ratio_vec = np.tanh(ratio_vec)
        l1 = float(np.sum(np.abs(ratio_vec), dtype=np.float64))
        if l1 > 1e-12:
            ratio_vec /= l1
        else:
            ratio_vec.fill(0.0)

        cash_ratio = float(ratio_vec[0])
        stock_holding_ratio = ratio_vec[1:]

        stock_weights_history[t] = stock_holding_ratio.astype(np.float32)
        turnovers[t] = float(traded_notional / equity_before)

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
        day_rows.sort(key=lambda item: item.holding_ratio, reverse=True)
        records.extend(day_rows)

        # Keep PnL calculation aligned with realized holdings ratio.
        if price_matrix is not None and (t + 1) < t_len:
            next_prices = np.where(price_matrix[t + 1] > 1e-12, price_matrix[t + 1], current_prices)
            simple_returns = np.divide(
                next_prices - current_prices,
                current_prices,
                out=np.zeros_like(current_prices, dtype=np.float64),
                where=current_prices > 1e-12,
            )
        else:
            simple_returns = np.expm1(r[t])
            simple_returns = np.where(np.isfinite(simple_returns), simple_returns, 0.0)
            next_prices = current_prices * (1.0 + simple_returns)
            next_prices = np.where(np.isfinite(next_prices) & (next_prices > 1e-12), next_prices, current_prices)

        equity_end = float(equity_after_trade + np.dot(stock_market_values, simple_returns))
        equity_end = max(equity_end, 1e-12)

        strategy_returns[t] = np.float32(np.log(equity_end / equity_before))
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
