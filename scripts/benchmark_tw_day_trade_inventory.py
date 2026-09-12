#!/usr/bin/env python3
"""Bounded FIFO execution microbenchmark, NOT a complete training benchmark.

Uses the same physical ledger in eager chronological and integrated paths.
Real training optimization still requires canonical full-epoch measurements.
"""

from __future__ import annotations

import argparse
from dataclasses import fields, replace
from datetime import date
import json
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import torch
from downloader.artifact_io import atomic_write_json
from stockagent.backtest.tw_day_trade_inventory import (
    DayTradeInventoryState,
    append_inventory_fill,
    convert_inventory_to_margin,
    inventory_nav,
    inventory_path_nav,
    reduce_inventory_fifo,
    reduce_inventory_fifo_path,
    validate_inventory_state,
)


def fixture(symbols: int, cohorts: int, device: str):
    state = DayTradeInventoryState.empty(symbols, device=device)
    v = torch.ones(symbols, dtype=torch.float64, device=device)
    day = date(2026, 8, 1).toordinal()
    for cohort in range(cohorts):
        direction = torch.where(
            torch.arange(symbols, device=device) % 2 == 0, 1.0, -1.0
        )
        state = append_inventory_fill(
            state,
            signed_shares=direction * (1000 + 1000 * cohort) * v,
            price=v * (1000 + cohort * 5),
            buy_fee_rate=v * 0.001425,
            day_sell_fee_rate=v * 0.002925,
            normal_sell_fee_rate=v * 0.004425,
            rebate_rate=v * 0.00114,
            day=day + cohort,
        )
        state = convert_inventory_to_margin(state, day=day + cohort)
    prices = (
        (
            1000
            + torch.arange(270, device=device, dtype=torch.float64).remainder(9)
            * 0.123456
        )
        .expand(symbols, -1)
        .contiguous()
    )
    capacity = torch.zeros_like(prices)
    capacity[:, 1:20] = 1000
    capacity[:, 260:265] = 2000
    capacity[:, 269] = 3000
    return state, prices, capacity


def sequential(state, prices, capacity):
    for minute in range(prices.shape[1]):
        state = reduce_inventory_fifo(
            state,
            requested_shares=state.shares.abs(),
            price=prices[:, minute],
            capacity_shares=capacity[:, minute],
        ).state
    return state


def integrated(state, prices, capacity):
    return reduce_inventory_fifo_path(
        state, prices=prices, capacity_shares=capacity
    ).reduction.state


def chronological_curve(state, prices, capacity):
    curve = []
    for minute in range(prices.shape[1]):
        state = reduce_inventory_fifo(
            state,
            requested_shares=state.shares.abs(),
            price=prices[:, minute],
            capacity_shares=capacity[:, minute],
        ).state
        curve.append(
            inventory_nav(state, initial_capital=1e12, marks=prices[:, minute])
        )
    return torch.stack(curve)


def integrated_curve(state, prices, capacity):
    result = reduce_inventory_fifo_path(state, prices=prices, capacity_shares=capacity)
    return inventory_path_nav(
        state,
        prices=prices,
        minute_filled_shares=result.minute_filled_shares,
        marks=prices,
        initial_capital=1e12,
    )


def run(args):
    if args.device.startswith("cuda") and not torch.cuda.is_available():
        raise RuntimeError("CUDA is required; no CPU fallback")
    torch.set_num_threads(args.threads)
    state, prices, capacity = fixture(args.symbols, args.cohorts, args.device)
    compiled = torch.compile(
        integrated, fullgraph=True, dynamic=False, options={"triton.cudagraphs": False}
    )

    def sync():
        if args.device.startswith("cuda"):
            torch.cuda.synchronize(args.device)

    results, timings = {}, {}
    # No data-dependent assertions, device-to-host state transfer, or fallback
    # in a measured call. Reference acceptance is outside its timing interval.
    for name, fn in (
        ("chronological_eager", sequential),
        ("integrated_eager", integrated),
        ("integrated_inductor", compiled),
    ):
        print(f"[fifo-benchmark] phase=forward variant={name}", flush=True)
        samples = []
        for _ in range(args.repeats + 1):
            sync()
            start = time.perf_counter()
            result = fn(state, prices, capacity)
            sync()
            samples.append(time.perf_counter() - start)
        validate_inventory_state(result, symbols=args.symbols)
        results[name] = result
        timings[name] = {
            "cold_s": samples[0],
            "steady_median_s": statistics.median(samples[1:]),
            "steady_samples_s": samples[1:],
        }
    reference = results["chronological_eager"]
    for result in results.values():
        for key in state.__dataclass_fields__:
            torch.testing.assert_close(
                getattr(result, key), getattr(reference, key), rtol=1e-10, atol=1e-6
            )
    # Backward is measured independently, with the same exact-forward state.
    gradients, backward = {}, {}
    for name, fn in (
        ("chronological_eager", sequential),
        ("integrated_eager", integrated),
        ("integrated_inductor", compiled),
    ):
        print(f"[fifo-benchmark] phase=forward_backward variant={name}", flush=True)
        samples = []
        for _ in range(args.repeats + 1):
            from dataclasses import replace

            leaf = state.cohorts.detach().clone().requires_grad_()
            supplied = replace(state, cohorts=leaf)
            sync()
            start = time.perf_counter()
            result = fn(supplied, prices, capacity)
            loss = -inventory_nav(
                result, initial_capital=1e12, marks=prices[:, -1]
            ).log()
            loss.backward()
            sync()
            samples.append(time.perf_counter() - start)
            if not bool(torch.isfinite(leaf.grad).all()):
                raise RuntimeError("nonfinite FIFO gradient")
            gradients[name] = leaf.grad
        backward[name] = {
            "cold_s": samples[0],
            "steady_median_s": statistics.median(samples[1:]),
            "steady_samples_s": samples[1:],
        }
    # Quantity/capacity equality is a nonsmooth point; audit gradients on the
    # fixture's basis and entry-cost accounting fields, and separately test
    # quantity derivatives away from boundaries in the semantic test suite.
    for gradient in gradients.values():
        torch.testing.assert_close(
            gradient[..., 1:3],
            gradients["chronological_eager"][..., 1:3],
            rtol=1e-7,
            atol=1e-12,
        )
    curves, curve_timings = {}, {}
    for name, fn in (
        ("chronological_eager", chronological_curve),
        ("integrated_eager", integrated_curve),
        (
            "integrated_inductor",
            torch.compile(
                integrated_curve,
                fullgraph=True,
                dynamic=False,
                options={"triton.cudagraphs": False},
            ),
        ),
    ):
        print(f"[fifo-benchmark] phase=minute_nav variant={name}", flush=True)
        samples = []
        for _ in range(args.repeats + 1):
            sync()
            start = time.perf_counter()
            curve = fn(state, prices, capacity)
            sync()
            samples.append(time.perf_counter() - start)
        curves[name] = curve
        curve_timings[name] = {
            "cold_s": samples[0],
            "steady_median_s": statistics.median(samples[1:]),
            "steady_samples_s": samples[1:],
        }
    for curve in curves.values():
        # The synthetic capital is 1e12 TWD; allow several FP64 ULPs, not a
        # relative tolerance proportional to that large artificial capital.
        torch.testing.assert_close(
            curve, curves["chronological_eager"], rtol=0, atol=1e-3
        )
    return {
        "kind": "fifo_kernel_microbenchmark_not_training",
        "status": "passed",
        "symbols": args.symbols,
        "cohorts": args.cohorts,
        "minutes": 270,
        "torch": torch.__version__,
        "device": args.device,
        "gpu": torch.cuda.get_device_name(args.device)
        if args.device.startswith("cuda")
        else None,
        "forward": timings,
        "forward_backward": backward,
        "minute_nav": curve_timings,
        "full_epoch_measured": False,
        "train_py_inventory_integration_complete": False,
    }


def run_session(args):
    """Measure the whole daily executor, including default protection and carry.

    Synthetic inventory and prices: not a real-data epoch or a latency promise.
    One fixed date/shape is compiled; date/shape reuse is not inferred from it.
    """
    from stockagent.backtest.tw_day_trade_carry import (
        DayTradeCarrySession, DayTradeCarryState, execute_carry_session,
    )
    if not args.device.startswith("cuda") or not torch.cuda.is_available():
        raise RuntimeError("session benchmark requires CUDA; no CPU fallback")
    torch.set_num_threads(args.threads)
    inventory, prices, capacity = fixture(args.symbols, args.cohorts, args.device)
    capital = 1e12
    v = prices[:, 0].clone()
    day = int(inventory.observed_day.detach().cpu()) + 1
    state = DayTradeCarryState(inventory, inventory_nav(inventory,
        initial_capital=capital, marks=v), torch.ones((), device=args.device, dtype=torch.bool),
        capital, day - 1)
    session = DayTradeCarrySession(day, v, v, v, torch.full_like(v, 20_000),
        torch.full_like(v, 900), torch.full_like(v, 1100), torch.zeros_like(v),
        torch.stack((prices, prices), dim=1).transpose(1, 2),
        torch.stack((capacity, capacity), dim=1).transpose(1, 2), prices)
    session.validate_shape(args.symbols, torch.device(args.device))
    state.validate(symbols=args.symbols, device=torch.device(args.device), initial_capital=capital)
    weights = torch.where(torch.arange(args.symbols, device=args.device) % 2 == 0,
                          .5 / args.symbols, -.5 / args.symbols).double()

    def daily(w, account):
        return execute_carry_session(account, session, weights=w, can_enter=torch.ones_like(v),
            buy_fee_rate=torch.full_like(v, .001425), day_sell_fee_rate=torch.full_like(v, .002925),
            normal_sell_fee_rate=torch.full_like(v, .004425), rebate_rate=torch.full_like(v, .00114),
            initial_capital=capital)

    compile_options = {"triton.cudagraphs": False}
    if args.disable_coalesce_tiling:
        compile_options["triton.coalesce_tiling_analysis"] = False
    if args.max_fusion_size is not None:
        compile_options["max_fusion_size"] = args.max_fusion_size
    compiled = torch.compile(daily, fullgraph=True, dynamic=False, options=compile_options)
    timings = {}
    for backward in (False, True):
        reference = None
        phase = "forward_backward" if backward else "forward"
        for name, fn in (("eager", daily), ("inductor", compiled)):
            print(f"[fifo-session-benchmark] phase={phase} variant={name}", flush=True)
            samples = []
            torch.cuda.reset_peak_memory_stats(args.device)
            for _ in range(args.repeats + 1):
                leaf = weights.detach().clone().requires_grad_(backward)
                cohort_leaf = state.inventory.cohorts.detach().clone().requires_grad_(backward)
                account = replace(state, inventory=replace(state.inventory, cohorts=cohort_leaf))
                torch.cuda.synchronize(args.device)
                started = time.perf_counter()
                with torch.set_grad_enabled(backward):
                    result, curve, notional = fn(leaf, account)
                    if backward:
                        (-curve[-1] / capital).backward()
                torch.cuda.synchronize(args.device)
                samples.append(time.perf_counter() - started)
            validate_inventory_state(result.inventory, symbols=args.symbols)
            assert bool(torch.isfinite(curve).all().cpu())
            assert bool(result.alive.cpu()) and bool((curve > 0).all().cpu())
            if reference is None:
                reference = (result, curve, notional, leaf.grad, cohort_leaf.grad)
            else:
                for field in fields(result.inventory):
                    torch.testing.assert_close(getattr(result.inventory, field.name),
                        getattr(reference[0].inventory, field.name), rtol=0, atol=1e-3)
                torch.testing.assert_close(curve, reference[1], rtol=0, atol=1e-3)
                torch.testing.assert_close(notional, reference[2], rtol=1e-12, atol=1e-3)
                if backward:
                    torch.testing.assert_close(leaf.grad, reference[3], rtol=1e-6, atol=1e-7)
                    torch.testing.assert_close(cohort_leaf.grad, reference[4], rtol=1e-6, atol=1e-10)
            if backward:
                assert leaf.grad is not None and bool(torch.isfinite(leaf.grad).all().cpu())
                assert cohort_leaf.grad is not None and bool(torch.isfinite(cohort_leaf.grad).all().cpu())
                assert bool((cohort_leaf.grad.abs().sum() > 0).cpu())
            timings[f"{phase}_{name}"] = {"cold_s": samples[0],
                "steady_median_s": statistics.median(samples[1:]),
                "steady_samples_s": samples[1:],
                "peak_allocated_bytes": torch.cuda.max_memory_allocated(args.device)}
    return {"kind": "synthetic_fifo_whole_session_microbenchmark_not_training", "status": "passed",
        "symbols": args.symbols, "cohorts": args.cohorts, "minutes": 270,
        "synthetic_initial_capital": capital, "torch": torch.__version__, "device": args.device,
        "gpu": torch.cuda.get_device_name(args.device), "timings": timings,
        "compile_options": compile_options,
        "exit_source_stride": list(session.exit_prices.stride()),
        "backward_inputs": ["model_weights", "carried_physical_cohorts"],
        "full_epoch_measured": False, "train_py_inventory_integration_complete": False,
        "dynamic_date_and_cohort_shape_reuse_verified": False}


def run_prepared_batch(args):
    """Measure only eliminated identity copies, never infer epoch throughput."""
    from stockagent.backtest.tw_day_trade_carry import DayTradeCarrySession
    from stockagent.training.day_trade_carry_bridge import PreparedDayTradeCarrySource
    from stockagent.training.windowed import WindowedSplitTensors

    device = torch.device(args.device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA required; no CPU fallback")
    torch.set_num_threads(args.threads)
    # Deliberately synthetic. All days may share immutable buffers; the copied
    # reference still produces the separate allocations removed by the patch.
    _, prices, capacity = fixture(args.symbols, 1, "cpu")
    v = prices[:, 0].clone()
    day = date(2026, 8, 1).toordinal()
    template = DayTradeCarrySession(day, v, v, v, torch.full_like(v, 20_000),
        torch.full_like(v, 900), torch.full_like(v, 1100), torch.zeros_like(v),
        torch.stack((prices, prices), dim=2), torch.stack((capacity, capacity), dim=2), prices)
    days = int(args.batch_days)
    source = PreparedDayTradeCarrySource(tuple(replace(template, day=day + i) for i in range(days + 1)),
        tuple(str(i) for i in range(args.symbols)), "synthetic-identity-batch-benchmark-v1")
    mask = torch.ones((days + 1, args.symbols), dtype=torch.bool)
    split = WindowedSplitTensors(features=torch.ones((days + 1, args.symbols, 1)),
        valid_indices=torch.arange(1, days + 1), future_log_returns=torch.zeros_like(mask, dtype=torch.float32),
        tradable_mask=mask, can_buy_mask=mask, can_sell_mask=mask, can_short_open_mask=mask,
        benchmark=torch.zeros(days + 1), lookback=1, execution_mode="tw_day_trade",
        day_trade_eligible_mask=mask, day_trade_can_buy_open_mask=mask, day_trade_can_sell_open_mask=mask)
    index = torch.arange(args.symbols)

    def copied_reference():
        return tuple(replace(session, **{f.name: getattr(session, f.name).index_select(0, index).to(device)
            for f in fields(session) if isinstance(getattr(session, f.name), torch.Tensor)})
            for session in source.sessions[1:])

    def shared_source():
        return source.batch(split, 0, days, device)[0]

    timings = {}
    for name, fn in (("identity_index_select_reference", copied_reference), ("shared_source", shared_source)):
        samples = []
        for _ in range(args.repeats + 1):
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            started = time.perf_counter()
            result = fn()
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            samples.append(time.perf_counter() - started)
            del result
        timings[name] = {"cold_s": samples[0], "steady_median_s": statistics.median(samples[1:]),
                         "steady_samples_s": samples[1:]}
    reference, actual = copied_reference(), shared_source()
    for left, right in zip(reference, actual):
        for field in fields(left):
            value = getattr(left, field.name)
            if isinstance(value, torch.Tensor):
                torch.testing.assert_close(value, getattr(right, field.name), rtol=0, atol=0, equal_nan=True)
    return {"kind": "synthetic_prepared_batch_copy_microbenchmark_not_training", "status": "passed",
        "symbols": args.symbols, "batch_days": days, "threads": args.threads,
        "device": args.device, "torch": torch.__version__, "timings": timings,
        "source_days_share_storage": True, "full_epoch_measured": False,
        "train_py_inventory_integration_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--symbols", type=int, default=2754)
    parser.add_argument("--cohorts", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--whole-session", action="store_true")
    parser.add_argument("--prepared-batch", action="store_true")
    parser.add_argument("--batch-days", type=int, default=16)
    parser.add_argument("--disable-coalesce-tiling", action="store_true")
    parser.add_argument("--max-fusion-size", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.symbols, args.cohorts, args.repeats, args.threads, args.batch_days) < 1:
        parser.error("sizes/repeats/threads must be positive")
    if args.whole_session and args.prepared_batch:
        parser.error("choose whole-session or prepared-batch, not both")
    if args.max_fusion_size is not None and args.max_fusion_size < 1:
        parser.error("max-fusion-size must be positive")
    try:
        result = (run_prepared_batch(args) if args.prepared_batch
                  else run_session(args) if args.whole_session else run(args))
    except Exception as exc:
        atomic_write_json(
            args.output,
            {
                "kind": "fifo_kernel_microbenchmark_not_training",
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "attempt_parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "full_epoch_measured": False,
                "train_py_inventory_integration_complete": False,
            },
        )
        raise
    atomic_write_json(args.output, result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
