#!/usr/bin/env python3
"""Benchmark fixed-block sizes for the differentiable TW daily cash ledger."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import sys
import time

import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.tw_day_trade_daily import (
    get_tw_day_trade_daily_compile_stats,
    run_tw_day_trade_daily_execution,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--block-rows", type=int, required=True)
    parser.add_argument("--rows", type=int, default=128)
    parser.add_argument("--symbols", type=int, default=2_744)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--ready-token", type=Path, default=None)
    parser.add_argument("--start-token", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.block_rows <= 0 or args.rows <= 0 or args.symbols <= 0:
        raise ValueError("block rows, rows, and symbols must be positive")
    if args.repeats <= 0:
        raise ValueError("repeats must be positive")
    os.environ["STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS"] = str(
        args.block_rows
    )
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1"
    os.environ["STOCKAGENT_STRICT_NO_FALLBACK"] = "1"

    device = torch.device(args.device)
    torch.manual_seed(20260815)
    generator = torch.Generator(device=device).manual_seed(20260815)
    shape = (args.rows, args.symbols)
    raw = torch.randn(shape, device=device, generator=generator)
    base_weights = raw / raw.abs().sum(dim=1, keepdim=True).clamp_min(1.0e-8)
    returns = 0.02 * torch.randn(shape, device=device, generator=generator)
    mask = torch.rand(shape, device=device, generator=generator) > 0.02
    close_buy = mask & (
        torch.rand(shape, device=device, generator=generator) > 0.01
    )
    close_sell = mask & (
        torch.rand(shape, device=device, generator=generator) > 0.01
    )
    fee = torch.full((args.symbols,), 0.001425 * 0.2, device=device)
    day_sell_fee = fee + 0.0015
    normal_sell_fee = fee + 0.003
    rebate = torch.zeros(args.symbols, device=device)
    volume = torch.full(shape, 0.01, device=device)

    def one_iteration() -> tuple[torch.Tensor, torch.Tensor]:
        weights = base_weights.detach().clone().requires_grad_(True)
        result = run_tw_day_trade_daily_execution(
            weights,
            returns,
            mask,
            close_buy,
            close_sell,
            mask,
            mask,
            mask,
            mask,
            fee,
            day_sell_fee,
            normal_sell_fee,
            rebate,
            volume,
            max_turnover_ratio=2.0,
            margin_financing_ratio=0.60,
            margin_financing_annual_rate=0.16,
            margin_short_handling_fee_rate=0.001,
            margin_short_annual_borrow_rate=0.20,
            settlement_lag_sessions=2,
        )
        loss = -result.strategy_returns.mean()
        loss.backward()
        if weights.grad is None:
            raise RuntimeError("benchmark produced no action gradient")
        return result.strategy_returns.detach(), weights.grad.detach()

    get_tw_day_trade_daily_compile_stats(reset=True)
    torch.cuda.reset_peak_memory_stats(device)
    torch.cuda.synchronize(device)
    first_started = time.perf_counter()
    reference_returns, reference_gradient = one_iteration()
    torch.cuda.synchronize(device)
    first_s = time.perf_counter() - first_started

    if (args.ready_token is None) != (args.start_token is None):
        raise ValueError("ready-token and start-token must be provided together")
    if args.ready_token is not None and args.start_token is not None:
        args.ready_token.touch()
        while not args.start_token.exists():
            time.sleep(0.01)

    elapsed: list[float] = []
    for _ in range(args.repeats):
        torch.cuda.synchronize(device)
        started = time.perf_counter()
        observed_returns, observed_gradient = one_iteration()
        torch.cuda.synchronize(device)
        elapsed.append(time.perf_counter() - started)
        torch.testing.assert_close(observed_returns, reference_returns)
        torch.testing.assert_close(observed_gradient, reference_gradient)

    payload = {
        "block_rows": args.block_rows,
        "rows": args.rows,
        "symbols": args.symbols,
        "first_forward_backward_s": first_s,
        "steady_median_forward_backward_s": statistics.median(elapsed),
        "steady_min_forward_backward_s": min(elapsed),
        "steady_samples_s": elapsed,
        "peak_allocated_gib": torch.cuda.max_memory_allocated(device) / 1024**3,
        "return_checksum": float(reference_returns.double().sum().cpu()),
        "gradient_l2": float(torch.linalg.vector_norm(reference_gradient).cpu()),
        "compile_stats": get_tw_day_trade_daily_compile_stats(),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
