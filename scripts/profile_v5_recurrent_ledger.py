#!/usr/bin/env python3
"""Profile the V5 compiled recurrent ledger at its production tensor shape.

This is a systems microbenchmark, not a strategy backtest.  It preserves the
stateful day-trade-carry ABI (two auctions, T+2, volume, margin debt, short
collateral, corporate-action/dividend channels) while using deterministic
synthetic values so kernel traces are reproducible and contain no market data.
"""

from __future__ import annotations

import argparse
from dataclasses import fields, replace
import gc
import json
from pathlib import Path
import statistics
import sys

import torch
from torch import nn

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.tw_dual_session_compiled import (
    clear_tw_dual_session_compile_cache,
    get_tw_dual_session_compile_stats,
    run_tw_cash_dual_session_compiled,
)
from stockagent.backtest import tw_dual_session_compiled as compiled_ledger


def _is_power_of_two(value: int) -> bool:
    return value > 0 and value & (value - 1) == 0


def _inputs(rows: int, symbols: int, device: torch.device) -> dict[str, object]:
    generator = torch.Generator(device="cpu").manual_seed(20260823)
    raw = torch.randn((rows, symbols), generator=generator, dtype=torch.float32)
    open_actions = raw / raw.abs().sum(dim=1, keepdim=True).clamp_min(1.0)
    actions = torch.stack((open_actions, torch.zeros_like(open_actions)), dim=1)
    time = torch.arange(rows, dtype=torch.float32).unsqueeze(1)
    symbol = torch.arange(symbols, dtype=torch.float32).unsqueeze(0)
    overnight = torch.sin(time * 0.071 + symbol * 0.003) * 0.002
    intraday = torch.cos(time * 0.053 - symbol * 0.002) * 0.004
    daily_true = torch.ones((rows, symbols), dtype=torch.bool)
    phase_true = daily_true[:, None, :].expand(-1, 2, -1)
    phase_false = torch.zeros_like(phase_true)
    return {
        "actions_value": actions.to(device),
        "overnight_log_returns": overnight.to(device),
        "intraday_log_returns": intraday.to(device),
        "tradable_mask": phase_true.to(device),
        "can_buy_mask": phase_true.to(device),
        "can_sell_mask": phase_true.to(device),
        "buy_fee_rates": 0.000855,
        "sell_fee_rates": 0.003855,
        "commission_rebate_rates": 0.00057,
        "commission_rebate_timing": "daily_close",
        "can_short_open_mask": phase_true.to(device),
        "force_short_cover_mask": phase_false.to(device),
        "short_margin_rate": 0.90,
        "short_capacity_weights": torch.full(
            (rows, symbols), float("inf"), device=device
        ),
        "short_handling_fee_rate": 0.0,
        "day_trade_carry_normal_sell_fee_rates": 0.003855,
        "day_trade_margin_financing_ratio": 0.60,
        "day_trade_margin_financing_annual_rate": 0.16,
        "day_trade_margin_short_handling_fee_rate": 0.001,
        "day_trade_margin_short_annual_borrow_rate": 0.20,
        "unresolved_corporate_action_mask": phase_false.to(device),
        "cash_dividend_yield": torch.zeros((rows, symbols), device=device),
        "cash_dividend_payment_delay_sessions": torch.zeros(
            (rows, symbols), device=device, dtype=torch.int64
        ),
        "force_exit_mask": phase_false.to(device),
        "settlement_lag_sessions": 2,
        "gross_budget": 1.0,
        "max_turnover_ratio": 0.0,
        "volume_limit_weights": torch.full(
            (rows, symbols), 0.04, device=device
        ),
        "state_advance_mask": torch.ones(rows, device=device, dtype=torch.bool),
        "return_weights_history": False,
        "strict_compile": True,
    }


def _run_once(inputs: dict[str, object]) -> tuple[float, float, float]:
    actions = torch.as_tensor(inputs["actions_value"]).detach().clone()
    actions.requires_grad_(True)
    kwargs = {key: value for key, value in inputs.items() if key != "actions_value"}
    start = torch.cuda.Event(enable_timing=True)
    forward_end = torch.cuda.Event(enable_timing=True)
    backward_end = torch.cuda.Event(enable_timing=True)
    start.record()
    result = run_tw_cash_dual_session_compiled(actions, **kwargs)
    objective = result.strategy_returns.mean()
    forward_end.record()
    objective.backward()
    backward_end.record()
    backward_end.synchronize()
    if actions.grad is None or not bool(torch.isfinite(actions.grad).all().item()):
        raise RuntimeError("compiled recurrent ledger produced an invalid gradient")
    forward_ms = float(start.elapsed_time(forward_end))
    backward_ms = float(forward_end.elapsed_time(backward_end))
    return forward_ms, backward_ms, float(actions.grad.abs().sum().item())


def _prepared(inputs: dict[str, object], actions: torch.Tensor):
    kwargs = {key: value for key, value in inputs.items() if key != "actions_value"}
    kwargs.pop("strict_compile", None)
    return compiled_ledger._prepare_inputs(
        mode="tw_cash",
        actions=actions,
        short_maintenance_ratio=1.30,
        session_month_ids=None,
        commission_rebate_payment_eligible_mask=None,
        claim_queue_sessions=None,
        initial_weights=None,
        initial_cash=None,
        initial_payables=None,
        initial_receivables=None,
        initial_commission_rebate_current=None,
        initial_commission_rebate_due=None,
        initial_commission_rebate_month_id=None,
        initial_alive=None,
        initial_equity_scale=None,
        initial_short_sale_collateral=None,
        initial_short_margin_collateral=None,
        initial_long_margin_debt=None,
        symbol_sharded=False,
        **kwargs,
    )


class _PreparedLedgerProbe(nn.Module):
    """Tensor-only wrapper required by ``make_graphed_callables``."""

    def __init__(self, template, tensor_fields: tuple[str, ...]) -> None:
        super().__init__()
        self.template = template
        self.tensor_fields = tensor_fields

    def forward(self, *values: torch.Tensor) -> tuple[torch.Tensor, ...]:
        prepared = replace(
            self.template,
            **dict(zip(self.tensor_fields, values, strict=True)),
        )
        result = compiled_ledger._run_prepared_compiled(
            prepared,
            strict_compile=True,
        )
        return (
            result.strategy_returns,
            result.turnovers,
            result.final_weights,
            result.final_cash,
            result.final_payables,
            result.final_receivables,
            result.final_short_sale_collateral,
            result.final_short_margin_collateral,
            result.final_long_margin_debt,
            result.final_equity_scale,
            result.final_commission_rebate_current,
            result.final_commission_rebate_due,
        )


def _cuda_graph_probe(inputs: dict[str, object]) -> dict[str, float]:
    # Warm only the regional Inductor kernel with a distinct autograd leaf.
    warm_actions = torch.as_tensor(inputs["actions_value"]).detach().clone()
    warm_actions.requires_grad_(True)
    warm_prepared = _prepared(inputs, warm_actions)
    tensor_fields = tuple(
        field.name
        for field in fields(warm_prepared)
        if isinstance(getattr(warm_prepared, field.name), torch.Tensor)
    )
    warm_module = _PreparedLedgerProbe(warm_prepared, tensor_fields)
    warm_args = tuple(getattr(warm_prepared, name) for name in tensor_fields)
    warm_output = warm_module(*warm_args)
    warm_output[0].mean().backward()
    del warm_output, warm_args, warm_module, warm_prepared, warm_actions
    gc.collect()
    torch.cuda.synchronize()

    # Capture uses fresh leaves, avoiding an AccumulateGrad node tied to the
    # default stream before make_graphed_callables creates its side stream.
    sample_actions = torch.as_tensor(inputs["actions_value"]).detach().clone()
    sample_actions.requires_grad_(True)
    sample_prepared = _prepared(inputs, sample_actions)
    module = _PreparedLedgerProbe(sample_prepared, tensor_fields)
    sample_args = tuple(getattr(sample_prepared, name) for name in tensor_fields)
    graphed = torch.cuda.make_graphed_callables(
        module,
        sample_args,
        num_warmup_iters=3,
        allow_unused_input=True,
    )

    runtime_actions = torch.as_tensor(inputs["actions_value"]).detach().clone()
    runtime_actions.requires_grad_(True)
    runtime_prepared = _prepared(inputs, runtime_actions)
    runtime_args = tuple(getattr(runtime_prepared, name) for name in tensor_fields)
    start = torch.cuda.Event(enable_timing=True)
    forward_end = torch.cuda.Event(enable_timing=True)
    backward_end = torch.cuda.Event(enable_timing=True)
    start.record()
    output = graphed(*runtime_args)
    forward_end.record()
    output[0].mean().backward()
    backward_end.record()
    backward_end.synchronize()
    if runtime_actions.grad is None:
        raise RuntimeError("CUDA-graph probe produced no action gradient")
    return {
        "forward_ms": float(start.elapsed_time(forward_end)),
        "backward_ms": float(forward_end.elapsed_time(backward_end)),
        "gradient_l1": float(runtime_actions.grad.abs().sum().item()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=128)
    parser.add_argument("--symbols", type=int, default=2746)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--trace", type=Path)
    parser.add_argument(
        "--probe-cuda-graph",
        action="store_true",
        help="Capture the whole repeated-day forward/backward with make_graphed_callables.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/profiles/v5_recurrent_ledger/profile.json"),
    )
    args = parser.parse_args()
    if not _is_power_of_two(args.rows) or args.rows % 32 != 0:
        raise ValueError("--rows must be a power of two and a multiple of 32")
    if args.symbols <= 0 or args.warmup < 1 or args.repeats < 1:
        raise ValueError("symbols, warmup, and repeats must be positive")
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required; CPU fallback is forbidden")

    device = torch.device("cuda", 0)
    torch.cuda.set_device(device)
    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    inputs = _inputs(args.rows, args.symbols, device)
    for _ in range(args.warmup):
        _run_once(inputs)

    torch.cuda.reset_peak_memory_stats(device)
    observations = [_run_once(inputs) for _ in range(args.repeats)]
    trace_top: list[dict[str, object]] = []
    if args.trace is not None:
        args.trace.parent.mkdir(parents=True, exist_ok=True)
        with torch.profiler.profile(
            activities=(
                torch.profiler.ProfilerActivity.CPU,
                torch.profiler.ProfilerActivity.CUDA,
            ),
            record_shapes=True,
            profile_memory=True,
        ) as profiler:
            _run_once(inputs)
        profiler.export_chrome_trace(str(args.trace))
        for event in profiler.key_averages().table(
            sort_by="self_cuda_time_total", row_limit=25
        ).splitlines():
            trace_top.append({"table_row": event})

    forward = [row[0] for row in observations]
    backward = [row[1] for row in observations]
    payload = {
        "rows": args.rows,
        "symbols": args.symbols,
        "warmup": args.warmup,
        "repeats": args.repeats,
        "forward_ms_median": statistics.median(forward),
        "backward_ms_median": statistics.median(backward),
        "forward_backward_ms_median": statistics.median(
            [row[0] + row[1] for row in observations]
        ),
        "forward_ms_observations": forward,
        "backward_ms_observations": backward,
        "gradient_l1_observations": [row[2] for row in observations],
        "peak_allocated_bytes": int(torch.cuda.max_memory_allocated(device)),
        "peak_reserved_bytes": int(torch.cuda.max_memory_reserved(device)),
        "compile_stats": get_tw_dual_session_compile_stats(),
        "trace": None if args.trace is None else str(args.trace),
        "profiler_table": trace_top,
        "cuda_graph_probe": (
            _cuda_graph_probe(inputs) if args.probe_cuda_graph else None
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
