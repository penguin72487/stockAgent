#!/usr/bin/env python3
"""Benchmark the bounded dual-session CUDA executor against its eager oracle."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import fields
from pathlib import Path
from typing import Callable

import numpy as np
import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.tw_dual_session import (  # noqa: E402
    TaiwanDualSessionResult,
    run_tw_cash_dual_session,
    run_tw_overnight_dual_session,
)
from stockagent.backtest.tw_dual_session_compiled import (  # noqa: E402
    clear_tw_dual_session_compile_cache,
    get_tw_dual_session_compile_stats,
    run_tw_cash_dual_session_compiled,
    run_tw_overnight_dual_session_compiled,
)
from stockagent.backtest.tw_commission_rebate import (  # noqa: E402
    commission_rebate_calendar,
)


Runner = Callable[..., TaiwanDualSessionResult]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("tw_cash", "tw_overnight"),
        default="tw_cash",
    )
    parser.add_argument("--rows", type=int, default=32)
    parser.add_argument("--symbols", type=int, default=2735)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--commission-rate", type=float, default=0.001425)
    parser.add_argument("--commission-discount", type=float, default=0.2)
    parser.add_argument(
        "--commission-rebate-timing",
        choices=("monthly_15th", "daily_close"),
        default="monthly_15th",
    )
    parser.add_argument(
        "--backward",
        action="store_true",
        help="Measure forward plus backward instead of no-grad forward.",
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Retain per-phase audit histories (disabled in the train hot path).",
    )
    return parser.parse_args()


def _make_case(args: argparse.Namespace) -> dict[str, object]:
    if args.rows <= 0 or args.symbols <= 0 or args.repeats <= 0:
        raise ValueError("rows, symbols, and repeats must be positive")
    if args.commission_rate < 0.0:
        raise ValueError("commission-rate must be non-negative")
    if not 0.0 <= args.commission_discount <= 1.0:
        raise ValueError("commission-discount must be between 0 and 1")
    device = torch.device("cuda")
    generator = torch.Generator(device=device).manual_seed(args.seed)
    base = torch.linspace(-1.0, 1.0, args.symbols, device=device)
    base = base / base.abs().sum().clamp_min(1.0) * 0.70
    phases = 2 if args.mode == "tw_cash" else 3
    actions = torch.zeros(
        (args.rows, phases, args.symbols),
        device=device,
    )
    if args.mode == "tw_cash":
        actions[:, 0] = base
        actions[:, 1] = base.roll(17) * 0.85
    else:
        actions[:, 0] = 0.75
        actions[:, 1] = base * 0.55
        actions[:, 2] = base * 0.25
    phase_mask = torch.ones(
        (args.rows, 2, args.symbols),
        device=device,
        dtype=torch.bool,
    )
    case: dict[str, object] = {
        "actions": actions,
        "overnight_log_returns": (
            torch.randn(
                (args.rows, args.symbols),
                generator=generator,
                device=device,
            )
            * 0.004
        ).clamp(-0.02, 0.02),
        "intraday_log_returns": (
            torch.randn(
                (args.rows, args.symbols),
                generator=generator,
                device=device,
            )
            * 0.008
        ).clamp(-0.04, 0.04),
        "tradable_mask": phase_mask,
        "can_buy_mask": phase_mask,
        "can_sell_mask": phase_mask,
        "buy_fee_rates": args.commission_rate,
        "sell_fee_rates": args.commission_rate + 0.003,
        "commission_rebate_rates": (
            args.commission_rate * (1.0 - args.commission_discount)
        ),
        "commission_rebate_timing": args.commission_rebate_timing,
        "can_short_open_mask": phase_mask,
        "short_margin_rate": 0.90,
        "short_capacity_weights": torch.ones(
            (args.rows, args.symbols),
            device=device,
        ),
        "short_handling_fee_rate": 0.0008,
        "return_weights_history": args.history,
    }
    if args.commission_rebate_timing == "monthly_15th":
        # Business-day sessions are sufficient for the timing benchmark: the
        # executor consumes the same precomputed calendar tensors as training,
        # while exchange-holiday accuracy remains a dataset concern.
        dates = np.busday_offset(
            np.datetime64("2026-01-02", "D"),
            np.arange(args.rows),
            roll="forward",
        )
        month_ids, payment_eligible = commission_rebate_calendar(dates)
        case["session_month_ids"] = torch.as_tensor(
            month_ids,
            device=device,
            dtype=torch.int64,
        )
        case["commission_rebate_payment_eligible_mask"] = torch.as_tensor(
            payment_eligible,
            device=device,
            dtype=torch.bool,
        )
    return case


def _objective(result: TaiwanDualSessionResult) -> torch.Tensor:
    return (
        result.strategy_returns.sum()
        + 0.03 * result.turnovers.sum()
        + 0.01 * result.final_weights.square().sum()
        + 0.001 * result.final_equity_scale
    )


def _run_once(
    runner: Runner,
    case: dict[str, object],
    *,
    backward: bool,
    compiled: bool,
) -> tuple[TaiwanDualSessionResult, torch.Tensor | None, float]:
    local = dict(case)
    actions = local["actions"].detach().clone()
    actions.requires_grad_(backward)
    local["actions"] = actions
    if compiled:
        local["strict_compile"] = True
    torch.cuda.synchronize()
    started = time.perf_counter()
    if backward:
        result = runner(**local)
        _objective(result).backward()
        gradient = actions.grad.detach().clone()
    else:
        with torch.no_grad():
            result = runner(**local)
        gradient = None
    torch.cuda.synchronize()
    return result, gradient, time.perf_counter() - started


def _max_tensor_differences(
    actual: TaiwanDualSessionResult,
    expected: TaiwanDualSessionResult,
) -> dict[str, float]:
    differences: dict[str, float] = {}
    for field in fields(expected):
        expected_value = getattr(expected, field.name)
        actual_value = getattr(actual, field.name)
        if not isinstance(expected_value, torch.Tensor):
            continue
        if expected_value.numel() == 0:
            differences[field.name] = 0.0
        elif expected_value.dtype == torch.bool:
            differences[field.name] = float(
                (~torch.eq(actual_value, expected_value)).sum().item()
            )
        else:
            differences[field.name] = float(
                (actual_value - expected_value).abs().max().item()
            )
    return differences


def main() -> None:
    args = _parse_args()
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required")
    case = _make_case(args)
    if args.mode == "tw_cash":
        eager_runner = run_tw_cash_dual_session
        compiled_runner = run_tw_cash_dual_session_compiled
    else:
        eager_runner = run_tw_overnight_dual_session
        compiled_runner = run_tw_overnight_dual_session_compiled

    _run_once(
        eager_runner,
        case,
        backward=args.backward,
        compiled=False,
    )
    eager_result, eager_gradient, _ = _run_once(
        eager_runner,
        case,
        backward=args.backward,
        compiled=False,
    )
    eager_times = [
        _run_once(
            eager_runner,
            case,
            backward=args.backward,
            compiled=False,
        )[2]
        for _ in range(args.repeats)
    ]

    torch._dynamo.reset()
    torch._dynamo.utils.counters.clear()
    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    compiled_result, compiled_gradient, cold_time = _run_once(
        compiled_runner,
        case,
        backward=args.backward,
        compiled=True,
    )
    compiled_times = [
        _run_once(
            compiled_runner,
            case,
            backward=args.backward,
            compiled=True,
        )[2]
        for _ in range(args.repeats)
    ]
    graph_breaks = sum(torch._dynamo.utils.counters["graph_break"].values())
    unique_graphs = torch._dynamo.utils.counters["stats"]["unique_graphs"]
    eager_median = statistics.median(eager_times)
    compiled_median = statistics.median(compiled_times)
    gradient_difference = None
    if eager_gradient is not None and compiled_gradient is not None:
        gradient_difference = float(
            (compiled_gradient - eager_gradient).abs().max().item()
        )

    print(
        json.dumps(
            {
                "mode": args.mode,
                "rows": args.rows,
                "symbols": args.symbols,
                "backward": args.backward,
                "history": args.history,
                "commission_rate": args.commission_rate,
                "commission_discount": args.commission_discount,
                "commission_rebate_timing": args.commission_rebate_timing,
                "device": torch.cuda.get_device_name(),
                "dtype": str(case["actions"].dtype),
                "cold_seconds": cold_time,
                "eager_seconds": eager_times,
                "compiled_seconds": compiled_times,
                "eager_median_seconds": eager_median,
                "compiled_median_seconds": compiled_median,
                "steady_speedup": eager_median / compiled_median,
                "max_abs_differences": _max_tensor_differences(
                    compiled_result,
                    eager_result,
                ),
                "action_gradient_max_abs_difference": gradient_difference,
                "graph_breaks": graph_breaks,
                "unique_graphs": unique_graphs,
                "compile_stats": get_tw_dual_session_compile_stats(),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
