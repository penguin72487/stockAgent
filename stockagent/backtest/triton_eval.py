from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import torch

from stockagent.runtime_env import normalize_cuda_env

try:  # pragma: no cover - optional acceleration dependency
    import triton
    import triton.language as tl
except Exception:  # pragma: no cover - import failure path is runtime-gated
    triton = None
    tl = None


MAX_TRITON_EVAL_SYMBOLS = 8192


def _prepare_triton_toolchain() -> None:
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
    existing_parts = [part for part in os.environ.get("PATH", "").split(os.pathsep) if part]
    prepend = [part for part in entries if part and Path(part).exists() and part not in existing_parts]
    if prepend:
        os.environ["PATH"] = os.pathsep.join([*prepend, os.environ.get("PATH", "")])


def triton_eval_available() -> bool:
    if triton is None or tl is None or not torch.cuda.is_available():
        return False
    _prepare_triton_toolchain()
    ptxas_blackwell = os.environ.get("TRITON_PTXAS_BLACKWELL_PATH")
    if ptxas_blackwell and Path(ptxas_blackwell).exists():
        return True
    return shutil.which("ptxas") is not None



def _next_power_of_2(value: int) -> int:
    if triton is None:
        raise RuntimeError("Triton is not available.")
    return int(triton.next_power_of_2(value))


def _num_warps(block: int) -> int:
    if block >= 4096:
        return 16
    if block >= 2048:
        return 8
    if block >= 512:
        return 4
    return 2


if triton is not None and tl is not None:

    @triton.jit
    def _long_short_eval_scan_kernel(
        weights,
        future_log_returns,
        tradable_mask,
        can_buy_mask,
        can_sell_mask,
        volume_limit_weights,
        initial_weights,
        strategy_returns,
        turnovers,
        weights_history,
        final_weights,
        T: tl.constexpr,
        S: tl.constexpr,
        BLOCK: tl.constexpr,
        HAS_INITIAL: tl.constexpr,
        HAS_VOLUME: tl.constexpr,
        RECORD_HISTORY: tl.constexpr,
        BUY_FEE: tl.constexpr,
        SELL_FEE: tl.constexpr,
        MAX_TURNOVER: tl.constexpr,
        GROSS_BUDGET: tl.constexpr,
    ):
        offsets = tl.arange(0, BLOCK)
        symbol_mask = offsets < S
        prev = tl.zeros((BLOCK,), dtype=tl.float32)
        if HAS_INITIAL:
            prev = tl.load(initial_weights + offsets, mask=symbol_mask, other=0.0).to(tl.float32)

        for t in range(0, T):
            base = t * S + offsets
            target = tl.load(weights + base, mask=symbol_mask, other=0.0).to(tl.float32)
            log_ret = tl.load(future_log_returns + base, mask=symbol_mask, other=0.0).to(tl.float32)
            finite_ret = (log_ret == log_ret) & (log_ret > -3.4028234663852886e38) & (
                log_ret < 3.4028234663852886e38
            )
            log_ret = tl.where(finite_ret, log_ret, 0.0)
            simple_ret = tl.exp(log_ret) - 1.0

            tradable = tl.load(tradable_mask + base, mask=symbol_mask, other=0) != 0
            can_buy = tl.load(can_buy_mask + base, mask=symbol_mask, other=0) != 0
            can_sell = tl.load(can_sell_mask + base, mask=symbol_mask, other=0) != 0

            target_t = tl.where(tradable, target, prev)
            delta = target_t - prev
            constrained = tl.where((delta > 0.0) & (~can_buy), prev, target_t)

            down = constrained < prev
            reduce_long = down & (prev > 0.0) & (constrained >= 0.0) & (~can_sell)
            constrained = tl.where(reduce_long, prev, constrained)

            cross_from_long = down & (prev > 0.0) & (constrained < 0.0)
            constrained = tl.where(cross_from_long & (~can_sell), prev, constrained)

            # This eval kernel supports the current default where short-open
            # permission follows can_sell. Explicit short-open masks fall back to
            # the canonical torch scan.
            open_or_increase_short = down & (prev <= 0.0) & (constrained < prev) & (~can_sell)
            constrained = tl.where(open_or_increase_short, prev, constrained)

            delta = constrained - prev
            next_weights = prev + delta
            if HAS_VOLUME:
                volume_cap = tl.load(volume_limit_weights + base, mask=symbol_mask, other=float("inf")).to(tl.float32)
                finite_cap = (volume_cap == volume_cap) & (volume_cap >= 0.0) & (
                    volume_cap < 3.4028234663852886e38
                )
                abs_delta = tl.abs(delta)
                cap_safe = tl.where(finite_cap, tl.maximum(volume_cap, 0.0), abs_delta)
                capped_abs = tl.minimum(abs_delta, cap_safe)
                delta = tl.where(delta < 0.0, -capped_abs, capped_abs)
                next_weights = prev + delta
            if MAX_TURNOVER > 0.0:
                turnover_raw = tl.sum(tl.abs(delta), axis=0)
                turnover_scale = tl.minimum(1.0, MAX_TURNOVER / tl.maximum(turnover_raw, 1.0e-12))
                next_weights = prev + delta * turnover_scale
                delta = next_weights - prev

            gross_next = tl.sum(tl.abs(next_weights), axis=0)
            gross_scale = tl.minimum(1.0, GROSS_BUDGET / tl.maximum(gross_next, 1.0e-12))
            next_weights = next_weights * gross_scale
            delta = next_weights - prev

            buy_turnover = tl.sum(tl.maximum(delta, 0.0), axis=0)
            sell_turnover = tl.sum(tl.maximum(-delta, 0.0), axis=0)
            turnover = buy_turnover + sell_turnover
            gross_return = tl.sum(next_weights * simple_ret, axis=0)
            net_simple = gross_return - BUY_FEE * buy_turnover - SELL_FEE * sell_turnover
            net_simple = tl.maximum(net_simple, -0.999999)
            strategy_return = tl.log(1.0 + net_simple)

            tl.store(strategy_returns + t, strategy_return)
            tl.store(turnovers + t, turnover)
            if RECORD_HISTORY:
                tl.store(weights_history + base, next_weights, mask=symbol_mask)
            prev = next_weights

        tl.store(final_weights + offsets, prev, mask=symbol_mask)


def run_long_short_eval_triton(
    weights: torch.Tensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_budget: float,
    return_weights_history: bool,
    initial_weights: torch.Tensor | None = None,
    volume_limit_weights: torch.Tensor | None = None,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    if not triton_eval_available():
        raise RuntimeError("Triton eval backtest requires CUDA and Triton.")
    if weights.device.type != "cuda":
        raise RuntimeError("Triton eval backtest requires CUDA tensors.")
    if weights.dim() != 2:
        raise RuntimeError("Triton eval backtest expects [T, S] weights.")
    if int(weights.size(1)) > MAX_TRITON_EVAL_SYMBOLS:
        raise RuntimeError(
            f"Triton eval backtest supports at most {MAX_TRITON_EVAL_SYMBOLS} symbols; "
            f"got {int(weights.size(1))}."
        )
    if weights.requires_grad or future_log_returns.requires_grad:
        raise RuntimeError("Triton eval backtest is an inference/eval path and does not support autograd.")
    if volume_limit_weights is not None and volume_limit_weights.requires_grad:
        raise RuntimeError("Triton eval backtest volume limits must not require gradients.")

    t_len = int(weights.size(0))
    symbols = int(weights.size(1))
    block = _next_power_of_2(symbols)
    weights_c = weights.contiguous()
    returns_c = future_log_returns.to(device=weights.device, dtype=weights.dtype).contiguous()
    tradable_c = tradable_mask.to(device=weights.device, dtype=torch.bool).contiguous()
    buy_c = can_buy_mask.to(device=weights.device, dtype=torch.bool).contiguous()
    sell_c = can_sell_mask.to(device=weights.device, dtype=torch.bool).contiguous()
    if tuple(returns_c.shape) != tuple(weights_c.shape):
        raise RuntimeError(f"future_log_returns shape must match weights: {tuple(returns_c.shape)} != {tuple(weights_c.shape)}")
    for name, tensor in (("tradable_mask", tradable_c), ("can_buy_mask", buy_c), ("can_sell_mask", sell_c)):
        if tuple(tensor.shape) != tuple(weights_c.shape):
            raise RuntimeError(f"{name} shape must match weights: {tuple(tensor.shape)} != {tuple(weights_c.shape)}")

    if initial_weights is None:
        initial_c = torch.empty((symbols,), device=weights.device, dtype=weights.dtype)
        has_initial = False
    else:
        if tuple(initial_weights.shape) != (symbols,):
            raise RuntimeError(
                f"initial_weights shape must be ({symbols},), got {tuple(initial_weights.shape)}."
            )
        initial_c = torch.nan_to_num(
            initial_weights.to(device=weights.device, dtype=weights.dtype),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        ).contiguous()
        has_initial = True
    if volume_limit_weights is None:
        volume_c = torch.empty((0,), device=weights.device, dtype=weights.dtype)
        has_volume = False
    else:
        if tuple(volume_limit_weights.shape) != tuple(weights_c.shape):
            raise RuntimeError(
                f"volume_limit_weights shape must match weights: "
                f"{tuple(volume_limit_weights.shape)} != {tuple(weights_c.shape)}"
            )
        volume_c = volume_limit_weights.to(device=weights.device, dtype=weights.dtype).contiguous()
        has_volume = True

    strategy_returns = torch.empty((t_len,), device=weights.device, dtype=weights.dtype)
    turnovers = torch.empty((t_len,), device=weights.device, dtype=weights.dtype)
    if return_weights_history:
        weights_history = torch.empty((t_len, symbols), device=weights.device, dtype=weights.dtype)
    else:
        weights_history = torch.empty((0, symbols), device=weights.device, dtype=weights.dtype)
    final_weights = torch.empty((symbols,), device=weights.device, dtype=weights.dtype)

    _long_short_eval_scan_kernel[(1,)](
        weights_c,
        returns_c,
        tradable_c,
        buy_c,
        sell_c,
        volume_c,
        initial_c,
        strategy_returns,
        turnovers,
        weights_history,
        final_weights,
        T=t_len,
        S=symbols,
        BLOCK=block,
        HAS_INITIAL=has_initial,
        HAS_VOLUME=has_volume,
        RECORD_HISTORY=bool(return_weights_history),
        BUY_FEE=float(buy_fee_rate),
        SELL_FEE=float(sell_fee_rate),
        MAX_TURNOVER=float(max_turnover_ratio),
        GROSS_BUDGET=float(gross_budget),
        num_warps=_num_warps(block),
        num_stages=3,
    )
    return strategy_returns, turnovers, weights_history, final_weights
