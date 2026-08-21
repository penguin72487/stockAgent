"""Reproducible eager/compiled benchmark for the crypto perpetual ledger."""

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

from stockagent.backtest.crypto_perpetual import run_crypto_perpetual_torch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=128)
    parser.add_argument("--symbols", type=int, default=396)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument(
        "--output",
        default="artifacts/benchmarks/bybit_crypto_perpetual_executor.json",
    )
    return parser.parse_args()


def _run(
    base_target: torch.Tensor,
    effective: torch.Tensor,
    price: torch.Tensor,
    mask: torch.Tensor,
    *,
    compiled: bool,
) -> tuple[float, torch.Tensor, torch.Tensor, torch.Tensor]:
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1" if compiled else "0"
    target = base_target.detach().clone().requires_grad_(True)
    torch.cuda.synchronize()
    started = time.perf_counter()
    result = run_crypto_perpetual_torch(
        target,
        effective,
        price,
        mask,
        mask,
        mask,
        mask,
        torch.zeros_like(mask),
        buy_fee_rate=0.00055,
        sell_fee_rate=0.00055,
        long_only=False,
        maximum_gross=1.0,
    )
    loss = -result.strategy_simple_returns.mean()
    loss.backward()
    torch.cuda.synchronize()
    elapsed = time.perf_counter() - started
    assert target.grad is not None
    return (
        elapsed,
        result.strategy_simple_returns.detach(),
        result.final_weights.detach(),
        target.grad.detach(),
    )


def main() -> None:
    args = parse_args()
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required for the compiled executor benchmark")
    if args.rows < 4 or args.symbols < 1 or args.repeats < 1:
        raise ValueError("rows>=4, symbols>=1 and repeats>=1 are required")
    torch.manual_seed(20260821)
    device = torch.device("cuda")
    raw = torch.randn((args.rows, args.symbols), device=device)
    base_target = 0.8 * raw / raw.abs().sum(dim=1, keepdim=True).clamp_min(1e-12)
    price_simple = torch.randn_like(base_target) * 0.015
    funding = torch.randn_like(base_target) * 0.00005
    effective_simple = price_simple - funding
    effective = torch.log1p(effective_simple.clamp_min(-0.95))
    price = torch.log1p(price_simple.clamp_min(-0.95))
    mask = torch.ones_like(base_target, dtype=torch.bool)

    eager = [
        _run(base_target, effective, price, mask, compiled=False)
        for _ in range(args.repeats)
    ]
    first_compiled = _run(base_target, effective, price, mask, compiled=True)
    steady = [
        _run(base_target, effective, price, mask, compiled=True)
        for _ in range(args.repeats)
    ]
    steady_times = [item[0] for item in steady]
    eager_times = [item[0] for item in eager]
    eager_reference = eager[-1]
    reference = steady[-1]
    report = {
        "device": torch.cuda.get_device_name(device),
        "torch_version": torch.__version__,
        "rows": args.rows,
        "symbols": args.symbols,
        "repeats": args.repeats,
        "eager_forward_backward_seconds": eager_times,
        "eager_forward_backward_median_seconds": statistics.median(eager_times),
        "compiled_first_forward_backward_seconds": first_compiled[0],
        "compiled_steady_seconds": steady_times,
        "compiled_steady_median_seconds": statistics.median(steady_times),
        "steady_speedup_vs_eager": statistics.median(eager_times)
        / statistics.median(steady_times),
        "max_abs_simple_return_difference": (eager_reference[1] - reference[1])
        .abs()
        .max()
        .item(),
        "max_abs_final_weight_difference": (eager_reference[2] - reference[2])
        .abs()
        .max()
        .item(),
        "max_abs_action_gradient_difference": (eager_reference[3] - reference[3])
        .abs()
        .max()
        .item(),
        "block_rows": 4,
        "cuda_graphs": False,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
