#!/usr/bin/env python3
"""Two-GPU forward/gradient oracle for the symbol-sharded Taiwan ledger.

Run with::

    torchrun --standalone --nproc_per_node=2 \
      scripts/validate_distributed_symbol_sharded_ledger.py

The check deliberately compares against the eager full-symbol ledger.  Every
rank evaluates the replicated global loss, so functional collective backward
sums ``world_size`` identical loss seeds.  DDP's parameter-gradient averaging
cancels that factor in training; this direct action-gradient test verifies the
factor explicitly instead of hiding it in a custom VJP.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import torch
import torch.distributed as dist

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.tw_dual_session import run_tw_cash_dual_session
from stockagent.backtest.tw_dual_session_compiled import (
    get_tw_dual_session_compile_stats,
    run_tw_cash_dual_session_compiled,
)
from stockagent.training.trainer import (
    _time_to_symbol_all_to_all_autograd,
    _time_to_symbol_all_to_all_no_grad_many,
)


def _gather_symbol_shards(value: torch.Tensor) -> torch.Tensor:
    parts = [torch.empty_like(value) for _ in range(dist.get_world_size())]
    dist.all_gather(parts, value.contiguous())
    return torch.cat(parts, dim=-1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--compiled",
        action="store_true",
        help="also require the fullgraph one-day ledger kernel and backward",
    )
    args = parser.parse_args()
    dist.init_process_group("nccl")
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    if world_size != 2:
        raise RuntimeError(f"validation requires exactly two ranks; got {world_size}")
    local_rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(local_rank)
    device = torch.device("cuda", local_rank)

    rows = 8
    symbols = 4
    local_rows = rows // world_size
    row_start = rank * local_rows
    row_stop = row_start + local_rows
    generator = torch.Generator(device="cpu").manual_seed(20260822)
    raw = torch.rand((rows, 2, symbols), generator=generator, dtype=torch.float64)
    global_actions_value = (raw / raw.sum(dim=-1, keepdim=True) * 0.45).to(device)
    overnight = (
        torch.linspace(-0.003, 0.004, rows * symbols, dtype=torch.float64)
        .reshape(rows, symbols)
        .to(device)
    )
    intraday = (
        torch.linspace(0.005, -0.002, rows * symbols, dtype=torch.float64)
        .reshape(rows, symbols)
        .to(device)
    )
    phase_mask = torch.ones((rows, 2, symbols), device=device, dtype=torch.bool)

    full_actions = global_actions_value.detach().clone().requires_grad_(True)
    full = run_tw_cash_dual_session(
        full_actions,
        overnight,
        intraday,
        phase_mask,
        phase_mask,
        phase_mask,
        0.001,
        0.002,
        settlement_lag_sessions=2,
        gross_budget=1.0,
        return_weights_history=True,
        _detach_initial_state=False,
    )
    full_loss = full.strategy_returns.sum()
    full_loss.backward()
    if full_actions.grad is None:
        raise RuntimeError("full-symbol oracle produced no action gradient")

    local_actions = (
        global_actions_value[row_start:row_stop].detach().clone().requires_grad_(True)
    )
    sharded_actions = _time_to_symbol_all_to_all_autograd(local_actions)
    sharded_overnight, sharded_intraday, sharded_phase_mask = (
        _time_to_symbol_all_to_all_no_grad_many(
            (
                overnight[row_start:row_stop],
                intraday[row_start:row_stop],
                phase_mask[row_start:row_stop].to(dtype=torch.float64),
            )
        )
    )
    sharded_phase_mask = sharded_phase_mask.to(dtype=torch.bool)
    sharded = run_tw_cash_dual_session(
        sharded_actions,
        sharded_overnight,
        sharded_intraday,
        sharded_phase_mask,
        sharded_phase_mask,
        sharded_phase_mask,
        0.001,
        0.002,
        settlement_lag_sessions=2,
        gross_budget=1.0,
        return_weights_history=True,
        _detach_initial_state=False,
        _symbol_sharded=True,
    )

    torch.testing.assert_close(
        sharded.strategy_returns,
        full.strategy_returns,
        rtol=1.0e-10,
        atol=1.0e-11,
    )
    torch.testing.assert_close(
        sharded.turnovers,
        full.turnovers,
        rtol=1.0e-10,
        atol=1.0e-11,
    )
    torch.testing.assert_close(
        _gather_symbol_shards(sharded.final_weights),
        full.final_weights,
        rtol=1.0e-10,
        atol=1.0e-11,
    )
    sharded_loss = sharded.strategy_returns.sum()
    sharded_loss.backward()
    if local_actions.grad is None:
        raise RuntimeError("symbol-sharded ledger produced no action gradient")
    expected_local_gradient = (
        full_actions.grad[row_start:row_stop] * float(world_size)
    )
    torch.testing.assert_close(
        local_actions.grad,
        expected_local_gradient,
        rtol=2.0e-8,
        atol=2.0e-10,
    )

    compile_stats: dict[str, int] | None = None
    if args.compiled:
        compiled_actions = (
            sharded_actions.detach().to(dtype=torch.float32).repeat(4, 1, 1)
        ).requires_grad_(True)
        compiled_overnight = sharded_overnight.to(dtype=torch.float32).repeat(4, 1)
        compiled_intraday = sharded_intraday.to(dtype=torch.float32).repeat(4, 1)
        compiled_mask = sharded_phase_mask.repeat(4, 1, 1)
        compiled = run_tw_cash_dual_session_compiled(
            compiled_actions,
            compiled_overnight,
            compiled_intraday,
            compiled_mask,
            compiled_mask,
            compiled_mask,
            0.001,
            0.002,
            settlement_lag_sessions=2,
            gross_budget=1.0,
            return_weights_history=False,
            strict_compile=True,
            _symbol_sharded=True,
        )
        compiled.strategy_returns.sum().backward()
        if compiled_actions.grad is None or not bool(
            torch.isfinite(compiled_actions.grad).all().item()
        ):
            raise RuntimeError("compiled symbol-sharded ledger backward is invalid")
        compile_stats = get_tw_dual_session_compile_stats()
        if int(compile_stats["compiled_day_calls"]) != 32:
            raise RuntimeError(
                "compiled symbol-sharded ledger did not execute every day: "
                f"stats={compile_stats}"
            )

    max_forward_error = float(
        (sharded.strategy_returns - full.strategy_returns).abs().max().item()
    )
    max_gradient_error = float(
        (local_actions.grad - expected_local_gradient).abs().max().item()
    )
    if rank == 0:
        print(
            "distributed symbol-sharded ledger validation passed: "
            f"rows={rows} symbols={symbols} world_size={world_size} "
            f"max_forward_error={max_forward_error:.3e} "
            f"max_scaled_gradient_error={max_gradient_error:.3e} "
            f"compile_stats={compile_stats}",
            flush=True,
        )
    dist.barrier()
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
