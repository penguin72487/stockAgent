#!/usr/bin/env python3
"""Benchmark the production Taiwan settlement executors.

The cash benchmark exercises the same bounded recurrent ``torch.compile``
chunks used by training.  It deliberately does not wrap the full horizon in a
second compile: doing so unrolls the T+2 recurrence into a graph proportional
to ``rows`` and measures a path production never executes.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections.abc import Callable
from pathlib import Path

import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.tw_continuous import (  # noqa: E402
    get_tw_continuous_compile_stats,
    run_tw_cash_continuous,
    run_tw_day_trade_continuous,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=32)
    parser.add_argument("--symbols", type=int, default=2304)
    parser.add_argument("--chunk-rows", type=int, default=8)
    parser.add_argument("--claim-queue-sessions", type=int, default=256)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260716)
    parser.add_argument("--device", default="cuda")
    return parser.parse_args()


def _synchronize(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def _timed_backward(
    function: Callable[[torch.Tensor], torch.Tensor], source: torch.Tensor
) -> float:
    argument = source.detach().clone().requires_grad_(True)
    _synchronize(source.device)
    started = time.perf_counter()
    function(argument).backward()
    _synchronize(source.device)
    return (time.perf_counter() - started) * 1000.0


def _measure(
    function: Callable[[torch.Tensor], torch.Tensor],
    source: torch.Tensor,
    *,
    warmup: int,
    repeats: int,
) -> tuple[float, list[float]]:
    for _ in range(max(warmup, 0)):
        _timed_backward(function, source)
    samples = [
        _timed_backward(function, source) for _ in range(max(repeats, 1))
    ]
    return statistics.median(samples), samples


def _value_and_gradient(
    function: Callable[[torch.Tensor], torch.Tensor], source: torch.Tensor
) -> tuple[torch.Tensor, torch.Tensor]:
    argument = source.detach().clone().requires_grad_(True)
    value = function(argument)
    value.backward()
    _synchronize(source.device)
    assert argument.grad is not None
    return value.detach(), argument.grad.detach()


def main() -> None:
    args = _arguments()
    if (
        args.rows <= 0
        or args.symbols <= 0
        or args.chunk_rows <= 0
        or args.claim_queue_sessions < 2
    ):
        raise ValueError(
            "rows, symbols, and chunk-rows must be positive and "
            "claim-queue-sessions must be at least 2"
        )
    device = torch.device(args.device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA benchmark requested but CUDA is unavailable")
    os.environ["STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS"] = str(
        args.chunk_rows
    )
    os.environ["STOCKAGENT_TW_COMPILE_SYMBOL_MAX"] = str(args.symbols)

    generator = torch.Generator(device=device).manual_seed(args.seed)
    shape = (args.rows, args.symbols)
    signed = torch.randn(shape, generator=generator, device=device)
    cash_weights = signed / signed.abs().sum(dim=1, keepdim=True) * 0.8
    day_weights = torch.randn(shape, generator=generator, device=device)
    day_weights = day_weights / day_weights.abs().sum(dim=1, keepdim=True) * 0.8
    cash_returns = torch.randn(shape, generator=generator, device=device) * 0.01
    day_returns = torch.randn(shape, generator=generator, device=device) * 0.01
    mask = torch.ones(shape, device=device, dtype=torch.bool)
    actions = torch.zeros(shape, device=device, dtype=torch.bool)
    force_cover = torch.zeros(shape, device=device, dtype=torch.bool)
    if args.rows > 2:
        actions[args.rows // 2, ::257] = True
        force_cover[max(1, args.rows // 3), 1::509] = True
    buy_fees = torch.full(
        (args.symbols,), 0.000855, device=device, dtype=torch.float32
    )
    cash_sell_fees = torch.full(
        (args.symbols,), 0.003855, device=device, dtype=torch.float32
    )
    day_sell_fees = torch.full(
        (args.symbols,), 0.002355, device=device, dtype=torch.float32
    )
    short_margin = torch.full(shape, 0.90, device=device)
    short_capacity = torch.full(shape, 0.02, device=device)
    dividend_yield = torch.zeros(shape, device=device)
    dividend_delay = torch.zeros(shape, device=device, dtype=torch.int64)
    dividend_row = min(max(args.rows // 3, 0), args.rows - 1)
    dividend_yield[dividend_row, ::257] = 0.02
    dividend_delay[dividend_row, ::257] = min(
        40, int(args.claim_queue_sessions)
    )

    def cash(argument: torch.Tensor) -> torch.Tensor:
        result = run_tw_cash_continuous(
            argument,
            cash_returns,
            mask,
            mask,
            mask,
            buy_fees,
            cash_sell_fees,
            can_short_open_mask=mask,
            force_short_cover_mask=force_cover,
            short_margin_rate=short_margin,
            short_capacity_weights=short_capacity,
            short_maintenance_ratio=1.30,
            short_handling_fee_rate=0.0002,
            unresolved_corporate_action_mask=actions,
            return_weights_history=False,
        )
        return result.strategy_returns.sum()

    def day(argument: torch.Tensor) -> torch.Tensor:
        result = run_tw_day_trade_continuous(
            argument,
            day_returns,
            mask,
            mask,
            mask,
            mask,
            mask,
            buy_fees,
            day_sell_fees,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
            return_weights_history=False,
        )
        return result.strategy_returns.sum()

    def cash_exact(argument: torch.Tensor) -> torch.Tensor:
        result = run_tw_cash_continuous(
            argument,
            cash_returns,
            mask,
            mask,
            mask,
            buy_fees,
            cash_sell_fees,
            can_short_open_mask=mask,
            force_short_cover_mask=force_cover,
            short_margin_rate=short_margin,
            short_capacity_weights=short_capacity,
            short_maintenance_ratio=1.30,
            short_handling_fee_rate=0.0002,
            unresolved_corporate_action_mask=actions,
            cash_dividend_yield=dividend_yield,
            cash_dividend_payment_delay_sessions=dividend_delay,
            claim_queue_sessions=int(args.claim_queue_sessions),
            return_weights_history=False,
        )
        return result.strategy_returns.sum()

    report: dict[str, object] = {
        "device": str(device),
        "device_name": (
            torch.cuda.get_device_name(device) if device.type == "cuda" else "CPU"
        ),
        "torch": torch.__version__,
        "shape": list(shape),
        "chunk_rows": args.chunk_rows,
        "claim_queue_sessions": args.claim_queue_sessions,
        "warmup": args.warmup,
        "repeats": args.repeats,
    }

    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "0"
    eager_value, eager_gradient = _value_and_gradient(cash, cash_weights)
    eager_ms, eager_samples = _measure(
        cash, cash_weights, warmup=args.warmup, repeats=args.repeats
    )

    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1"
    get_tw_continuous_compile_stats(reset=True)
    cold_compile_ms = _timed_backward(cash, cash_weights)
    compiled_value, compiled_gradient = _value_and_gradient(cash, cash_weights)
    compiled_ms, compiled_samples = _measure(
        cash, cash_weights, warmup=args.warmup, repeats=args.repeats
    )
    report["tw_cash_margin_short"] = {
        "cold_compile_and_backward_ms": cold_compile_ms,
        "eager_median_ms": eager_ms,
        "compiled_median_ms": compiled_ms,
        "steady_state_speedup": eager_ms / compiled_ms,
        "max_output_abs_diff": float((eager_value - compiled_value).abs().cpu()),
        "max_gradient_abs_diff": float(
            (eager_gradient - compiled_gradient).abs().max().cpu()
        ),
        "eager_min_ms": min(eager_samples),
        "compiled_min_ms": min(compiled_samples),
        "compile_stats": get_tw_continuous_compile_stats(),
    }

    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "0"
    exact_eager_value, exact_eager_gradient = _value_and_gradient(
        cash_exact, cash_weights
    )
    exact_eager_ms, exact_eager_samples = _measure(
        cash_exact,
        cash_weights,
        warmup=args.warmup,
        repeats=args.repeats,
    )

    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1"
    get_tw_continuous_compile_stats(reset=True)
    exact_cold_compile_ms = _timed_backward(cash_exact, cash_weights)
    exact_compiled_value, exact_compiled_gradient = _value_and_gradient(
        cash_exact, cash_weights
    )
    exact_compiled_ms, exact_compiled_samples = _measure(
        cash_exact,
        cash_weights,
        warmup=args.warmup,
        repeats=args.repeats,
    )
    report["tw_cash_margin_short_exact_dividend"] = {
        "cold_compile_and_backward_ms": exact_cold_compile_ms,
        "eager_median_ms": exact_eager_ms,
        "compiled_median_ms": exact_compiled_ms,
        "steady_state_speedup": exact_eager_ms / exact_compiled_ms,
        "max_output_abs_diff": float(
            (exact_eager_value - exact_compiled_value).abs().cpu()
        ),
        "max_gradient_abs_diff": float(
            (exact_eager_gradient - exact_compiled_gradient).abs().max().cpu()
        ),
        "eager_min_ms": min(exact_eager_samples),
        "compiled_min_ms": min(exact_compiled_samples),
        "compile_stats": get_tw_continuous_compile_stats(),
    }

    # Day trade currently has no bounded compiled kernel.  Report its honest
    # eager cost instead of benchmarking a full-horizon wrapper that production
    # explicitly forbids.
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "0"
    day_ms, day_samples = _measure(
        day, day_weights, warmup=args.warmup, repeats=args.repeats
    )
    report["tw_day_trade"] = {
        "executor": "eager",
        "median_ms": day_ms,
        "min_ms": min(day_samples),
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
