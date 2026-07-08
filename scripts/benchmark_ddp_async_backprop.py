#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from functools import partial
from pathlib import Path

import torch
import torch.distributed as dist
import torch.distributed.nn.functional as dist_nn_f
try:
    import torch.distributed._functional_collectives as dist_fc
except Exception:  # pragma: no cover - older torch fallback
    dist_fc = None
from torch.nn.parallel import DistributedDataParallel as DDP

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.models.factory import build_model
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.fused_loss import fused_log_utility_loss_tensor
from stockagent.training.trainer import (
    _autocast_context,
    _can_enable_torch_compile,
    _detach_portfolio_state,
    _extract_weights_and_aux,
    _resolve_amp_dtype,
)
from stockagent.training.windowed import dataset_to_windowed_tensors


def _env_rank() -> tuple[int, int, int]:
    rank = int(os.environ.get("RANK", "0"))
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    local_rank = int(os.environ.get("LOCAL_RANK", str(rank)))
    return rank, world_size, local_rank


def _is_rank0() -> bool:
    return _env_rank()[0] == 0


def _print_rank0(message: str) -> None:
    if _is_rank0():
        print(message, flush=True)


def _sync(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)
        try:
            dist.barrier(device_ids=[device.index if device.index is not None else torch.cuda.current_device()])
            return
        except TypeError:
            pass
    dist.barrier()


def _all_gather_autograd(x: torch.Tensor) -> torch.Tensor:
    if dist_fc is not None and hasattr(dist_fc, "all_gather_tensor"):
        return dist_fc.all_gather_tensor(x, 0, group=dist.group.WORLD)
    return torch.cat(tuple(dist_nn_f.all_gather(x)), dim=0)


def _all_gather_no_grad(x: torch.Tensor) -> torch.Tensor:
    parts = [torch.empty_like(x) for _ in range(dist.get_world_size())]
    dist.all_gather(parts, x)
    return torch.cat(parts, dim=0)


def _gb(num_bytes: int) -> float:
    return float(num_bytes) / 1024**3


def _build_panel_from_config(config):
    return build_panel(
        config.data.parquet_root,
        use_rapids=config.data.use_rapids,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tradable_mode=config.data.tradable_mode,
        trading_volume_policy=config.data.trading_volume_policy,
        security_filter=config.data.security_filter,
        strict_no_fallback=config.training.strict_no_fallback,
        panel_backend=config.data.panel_backend,
        panel_load_workers=config.data.panel_load_workers,
        external_feature_path=(
            config.data.tw_public_feature_path if config.data.use_tw_public_features else None
        ),
        external_market_symbol=config.data.tw_public_market_symbol,
        feature_include=config.data.feature_include,
        feature_exclude=config.data.feature_exclude,
    )


def _make_ddp_model(model: torch.nn.Module, device: torch.device, bucket_cap_mb: int) -> DDP:
    kwargs = {
        "device_ids": [device.index] if device.type == "cuda" else None,
        "output_device": device.index if device.type == "cuda" else None,
        "bucket_cap_mb": int(bucket_cap_mb),
        "find_unused_parameters": False,
    }
    try:
        return DDP(model, static_graph=True, **kwargs)
    except TypeError:
        return DDP(model, **kwargs)


def _make_fused_loss(config):
    return partial(
        fused_log_utility_loss_tensor,
        buy_fee_rate=config.trading.buy_fee_rate,
        sell_fee_rate=config.trading.sell_fee_rate,
        long_only=config.trading.long_only,
        max_turnover_ratio=config.trading.max_turnover_ratio,
        gross_leverage=1.0,
        min_trade_weight=config.trading.min_trade_weight,
        portfolio_activation=config.training.loss_portfolio_activation,
        gamma_sharpe=config.evaluation.gamma_sharpe,
        gamma_turnover=config.evaluation.gamma_turnover,
        concentration_weight=config.training.multitask_loss.concentration_weight,
        manual_backward=bool(getattr(config.training, "fused_log_utility_manual_backward", False)),
    )


def _run_benchmark(args: argparse.Namespace) -> dict[str, object]:
    rank, world_size, local_rank = _env_rank()
    if world_size < 2:
        raise RuntimeError("Launch with torchrun --standalone --nproc_per_node=2 ...")
    if not torch.cuda.is_available():
        raise RuntimeError("This real hotpath benchmark requires CUDA/NCCL.")
    torch.cuda.set_device(local_rank)
    device = torch.device("cuda", local_rank)
    dist.init_process_group("nccl")

    try:
        config = load_config(args.config)
        rank_cpu_threads = int(
            args.rank_cpu_threads
            if args.rank_cpu_threads is not None
            else max(1, int(getattr(config.environment, "cpu_threads", 1)) // world_size)
        )
        torch.set_num_threads(max(1, rank_cpu_threads))
        try:
            torch.set_num_interop_threads(max(1, min(rank_cpu_threads, 32)))
        except RuntimeError:
            pass
        if int(getattr(config.data, "panel_load_workers", 0)) > 1:
            config.data.panel_load_workers = max(1, int(config.data.panel_load_workers) // world_size)
        global_batch_size = int(args.global_batch_size or config.training.batch_size_train)
        if global_batch_size % world_size != 0:
            raise ValueError(
                f"global batch size must be divisible by world_size; got {global_batch_size=} {world_size=}"
            )
        local_batch_size = global_batch_size // world_size
        amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)

        _print_rank0(
            "[ddp-hotpath] building panel/windowed tensors "
            f"config={args.config} global_batch={global_batch_size} local_batch={local_batch_size} "
            f"rank_cpu_threads={rank_cpu_threads} panel_workers_per_rank={config.data.panel_load_workers}"
        )
        panel = _build_panel_from_config(config)
        folds = build_expanding_year_folds(
            dates=panel.dates,
            min_train_years=config.walk_forward.min_train_years,
            val_years=config.walk_forward.val_years,
            require_future_test_year=config.walk_forward.require_future_test_year,
        )
        fold = folds[min(max(0, int(args.fold_index)), len(folds) - 1)]
        split = dataset_to_windowed_tensors(CrossSectionalDataset(panel, fold.train_indices, config.training.lookback))
        if bool(args.cache_on_device):
            split = split.to_device_cache(device)

        rows = len(split)
        full_batches = rows // global_batch_size
        if full_batches <= 0:
            raise RuntimeError(f"not enough rows for one full global batch: rows={rows}, batch={global_batch_size}")
        timed_batches = max(1, min(int(args.batches), full_batches))
        warmup_batches = max(0, int(args.warmup))

        torch.manual_seed(int(args.seed))
        model = build_model(
            config=config,
            lookback=config.training.lookback,
            num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,
            feature_names=panel.feature_names,
        ).to(device)
        compile_order = str(args.compile_order)
        mode = None if str(args.compile_mode) == "default" else str(args.compile_mode)
        if bool(args.compile_model):
            can_compile, reason = _can_enable_torch_compile(device)
            if not can_compile:
                raise RuntimeError(f"torch.compile requested but unavailable: {reason}")
            if compile_order == "model_before_ddp":
                model = torch.compile(model, mode=mode, dynamic=False)
        ddp_model = _make_ddp_model(model, device, bucket_cap_mb=int(args.bucket_cap_mb))
        train_model: torch.nn.Module = ddp_model
        if bool(args.compile_model) and compile_order == "ddp_before_compile":
            train_model = torch.compile(ddp_model, mode=mode, dynamic=False)

        try:
            optimizer = torch.optim.AdamW(
                ddp_model.parameters(),
                lr=float(config.training.learning_rate),
                weight_decay=float(config.training.weight_decay),
                fused=True,
            )
        except TypeError:
            optimizer = torch.optim.AdamW(
                ddp_model.parameters(),
                lr=float(config.training.learning_rate),
                weight_decay=float(config.training.weight_decay),
            )
        fused_loss_fn = _make_fused_loss(config)
        if bool(args.compile_loss):
            fused_loss_fn = torch.compile(
                fused_loss_fn,
                dynamic=False,
                fullgraph=True,
                options={"triton.cudagraphs": False},
            )

        if device.type == "cuda":
            torch.cuda.reset_peak_memory_stats(device)
        portfolio_prev_weights: torch.Tensor | None = None
        loss_value = float("nan")
        step_times: list[float] = []

        total_steps = warmup_batches + timed_batches
        _sync(device)
        for step in range(total_steps):
            batch_idx = step % full_batches
            global_start = batch_idx * global_batch_size
            local_start = global_start + rank * local_batch_size
            local_end = local_start + local_batch_size
            if step == warmup_batches:
                _sync(device)
                if device.type == "cuda":
                    torch.cuda.reset_peak_memory_stats(device)

            step_start = time.perf_counter()
            batch = split.batch_by_rows(local_start, local_end, device=device, non_blocking=True)
            optimizer.zero_grad(set_to_none=True)
            with _autocast_context(device, amp_dtype):
                model_output = train_model(batch["x"], batch["tradable_mask"])
                local_weights, _ = _extract_weights_and_aux(model_output)
                weights = _all_gather_autograd(local_weights.contiguous())
                future_returns = _all_gather_no_grad(batch["future_log_returns"].contiguous())
                tradable_mask = _all_gather_no_grad(batch["tradable_mask"].contiguous())
                can_buy_mask = _all_gather_no_grad(batch["can_buy_mask"].contiguous())
                can_sell_mask = _all_gather_no_grad(batch["can_sell_mask"].contiguous())
                sample_mask = _all_gather_no_grad(batch["sample_mask"].contiguous())
                loss, next_prev = fused_loss_fn(
                    weights,
                    future_returns,
                    tradable_mask,
                    can_buy_mask,
                    can_sell_mask,
                    sample_mask,
                    torch.zeros(weights.size(1), device=device, dtype=weights.dtype)
                    if portfolio_prev_weights is None
                    else portfolio_prev_weights,
                )
            loss.backward()
            if float(config.training.grad_clip_norm) > 0:
                torch.nn.utils.clip_grad_norm_(model.parameters(), float(config.training.grad_clip_norm))
            optimizer.step()
            portfolio_prev_weights = _detach_portfolio_state(next_prev)
            loss_value = float(loss.detach().float().cpu().item())
            _sync(device)
            if step >= warmup_batches:
                step_times.append(time.perf_counter() - step_start)

        elapsed = float(sum(step_times))
        peak = int(torch.cuda.max_memory_allocated(device)) if device.type == "cuda" else 0
        result: dict[str, object] = {
            "strategy": "ddp_autograd_all_gather_full_loss",
            "world_size": world_size,
            "rank0_device": str(device),
            "fold_id": int(fold.fold_id),
            "train_years": list(map(int, fold.train_years)),
            "rows": int(rows),
            "symbols": int(panel.num_symbols),
            "features": int(len(panel.feature_names)),
            "lookback": int(config.training.lookback),
            "global_batch_size": int(global_batch_size),
            "local_batch_size": int(local_batch_size),
            "timed_batches": int(timed_batches),
            "warmup_batches": int(warmup_batches),
            "compile_model": bool(args.compile_model),
            "compile_order": str(args.compile_order),
            "compile_loss": bool(args.compile_loss),
            "cache_on_device": bool(args.cache_on_device),
            "rank_cpu_threads": int(rank_cpu_threads),
            "panel_workers_per_rank": int(config.data.panel_load_workers),
            "loss_scale": 1.0,
            "elapsed_s": round(elapsed, 6),
            "s_per_global_batch": round(elapsed / max(1, timed_batches), 6),
            "global_rows_per_s": round((timed_batches * global_batch_size) / max(elapsed, 1e-12), 3),
            "rank0_peak_vram_gb": round(_gb(peak), 4),
            "last_loss": round(loss_value, 8),
        }
        if rank == 0:
            print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
        return result
    finally:
        if dist.is_initialized():
            dist.destroy_process_group()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark the near-2x DDP async-backprop hotpath with canonical full-batch log-utility loss."
    )
    parser.add_argument("--config", default="configs/markets/tw_parallel.yaml")
    parser.add_argument("--fold-index", type=int, default=23)
    parser.add_argument("--global-batch-size", type=int, default=None)
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260707)
    parser.add_argument("--bucket-cap-mb", type=int, default=4)
    parser.add_argument("--rank-cpu-threads", type=int, default=None)
    parser.add_argument("--cache-on-device", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--compile-model", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--compile-mode", default="default", choices=("default", "reduce-overhead"))
    parser.add_argument(
        "--compile-order",
        default="model_before_ddp",
        choices=("model_before_ddp", "ddp_before_compile"),
    )
    parser.add_argument("--compile-loss", action=argparse.BooleanOptionalAction, default=False)
    _run_benchmark(parser.parse_args())


if __name__ == "__main__":
    main()
