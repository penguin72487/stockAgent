#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import sys
import time
from pathlib import Path

import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import load_config
from stockagent.models.factory import build_model
from stockagent.training.trainer import _autocast_context, _extract_weights_and_aux, _resolve_amp_dtype


def _sync(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def _peak_bytes(device: torch.device) -> int:
    return int(torch.cuda.max_memory_allocated(device)) if device.type == "cuda" else 0


def _make_model(config, *, query_mode: str, device: torch.device, features: int, symbols: int):
    cfg = copy.deepcopy(config)
    cfg.training.model_name = "transformer_base_portfolio"
    cfg.training.transformer_base_portfolio.attention_mode = "market_token"
    cfg.training.transformer_base_portfolio.temporal_pooling = "last"
    cfg.training.transformer_base_portfolio.temporal_query_mode = query_mode
    cfg.training.transformer_base_portfolio.dynamic_market_tokens = True
    cfg.training.transformer_base_portfolio.dynamic_latent_tokens = False
    return build_model(
        config=cfg,
        lookback=int(cfg.training.lookback),
        num_features=int(features),
        num_symbols=int(symbols),
    ).to(device)


def _window_indices(date_indices: torch.Tensor, lookback: int) -> torch.Tensor:
    offsets = torch.arange(lookback - 1, -1, -1, device=date_indices.device, dtype=torch.long)
    return date_indices[:, None] - offsets[None, :]


def _measure_case(
    *,
    case: str,
    model,
    features_cpu: torch.Tensor,
    date_indices_cpu: torch.Tensor,
    mask_cpu: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    mode: str,
    warmup: int,
    iters: int,
) -> dict[str, float | int | str]:
    lookback = int(model.lookback)
    batch_size = int(date_indices_cpu.numel())
    transfer_s = 0.0
    forward_s = 0.0
    total_s = 0.0

    if device.type == "cuda":
        torch.cuda.reset_peak_memory_stats(device)

    with torch.inference_mode():
        for step in range(int(warmup) + int(iters)):
            timed = step >= int(warmup)
            if timed and step == int(warmup) and device.type == "cuda":
                torch.cuda.reset_peak_memory_stats(device)

            total_start = time.perf_counter()
            transfer_start = time.perf_counter()
            if mode == "window":
                window_idx = _window_indices(date_indices_cpu, lookback)
                x = features_cpu[window_idx].contiguous().to(device=device, non_blocking=(device.type == "cuda"))
                mask = mask_cpu.to(device=device, non_blocking=(device.type == "cuda"))
                date_indices = None
            else:
                x = None
                date_indices = date_indices_cpu.to(device=device, non_blocking=(device.type == "cuda"))
                mask = mask_cpu.to(device=device, non_blocking=(device.type == "cuda"))
            _sync(device)
            transfer_elapsed = time.perf_counter() - transfer_start

            forward_start = time.perf_counter()
            with _autocast_context(device, amp_dtype):
                if mode == "window":
                    if x is None:
                        raise RuntimeError("window case did not build x")
                    output = model(x, mask, return_aux=False)
                else:
                    if date_indices is None:
                        raise RuntimeError("panel case did not build date_indices")
                    output = model.forward_from_panel(features_cpu, date_indices, mask, return_aux=False)
                weights, _ = _extract_weights_and_aux(output)
                if not torch.isfinite(weights).all():
                    raise RuntimeError(f"{case} produced non-finite weights")
            _sync(device)
            forward_elapsed = time.perf_counter() - forward_start
            total_elapsed = time.perf_counter() - total_start

            if timed:
                transfer_s += transfer_elapsed
                forward_s += forward_elapsed
                total_s += total_elapsed

    elapsed = max(total_s, 1e-12)
    return {
        "case": case,
        "mode": mode,
        "batch_size": batch_size,
        "lookback": lookback,
        "symbols": int(features_cpu.size(1)),
        "features": int(features_cpu.size(2)),
        "iters": int(iters),
        "transfer_ms": round(transfer_s / max(1, int(iters)) * 1000.0, 4),
        "model_forward_ms": round(forward_s / max(1, int(iters)) * 1000.0, 4),
        "total_ms": round(total_s / max(1, int(iters)) * 1000.0, 4),
        "rows_per_sec": round((batch_size * int(iters)) / elapsed, 3),
        "peak_cuda_memory_gb": round(_peak_bytes(device) / 1024**3, 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark window-first vs panel-first Transformer forward paths.")
    parser.add_argument("--config", default="configs/experiment_baseline.yaml")
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--total-days", type=int, default=128)
    parser.add_argument("--symbols", type=int, default=512)
    parser.add_argument("--features", type=int, default=21)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--iters", type=int, default=20)
    parser.add_argument("--seed", type=int, default=23)
    args = parser.parse_args()

    config = load_config(args.config)
    device = torch.device(args.device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but torch.cuda.is_available() is false")

    torch.manual_seed(int(args.seed))
    if device.type == "cuda":
        torch.cuda.manual_seed_all(int(args.seed))

    lookback = int(config.training.lookback)
    batch_size = int(args.batch_size) if args.batch_size is not None else int(config.training.batch_size_train)
    total_days = max(int(args.total_days), lookback + batch_size)
    start_date = lookback - 1
    end_date = start_date + batch_size
    if end_date > total_days:
        raise ValueError("total-days must be at least lookback + batch-size - 1")

    features_cpu = torch.randn(total_days, int(args.symbols), int(args.features), dtype=torch.float32)
    date_indices_cpu = torch.arange(start_date, end_date, dtype=torch.long)
    mask_cpu = torch.ones(batch_size, int(args.symbols), dtype=torch.bool)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)

    exact_model = _make_model(
        config,
        query_mode="full_then_last",
        device=device,
        features=int(args.features),
        symbols=int(args.symbols),
    ).eval()
    last_only_model = _make_model(
        config,
        query_mode="last_only",
        device=device,
        features=int(args.features),
        symbols=int(args.symbols),
    ).eval()
    last_only_model.load_state_dict(exact_model.state_dict(), strict=False)

    results = [
        _measure_case(
            case="A_window_first",
            model=exact_model,
            features_cpu=features_cpu,
            date_indices_cpu=date_indices_cpu,
            mask_cpu=mask_cpu,
            device=device,
            amp_dtype=amp_dtype,
            mode="window",
            warmup=max(0, int(args.warmup)),
            iters=max(1, int(args.iters)),
        ),
        _measure_case(
            case="B_panel_forward_exact",
            model=exact_model,
            features_cpu=features_cpu,
            date_indices_cpu=date_indices_cpu,
            mask_cpu=mask_cpu,
            device=device,
            amp_dtype=amp_dtype,
            mode="panel",
            warmup=max(0, int(args.warmup)),
            iters=max(1, int(args.iters)),
        ),
        _measure_case(
            case="C_panel_forward_last_only",
            model=last_only_model,
            features_cpu=features_cpu,
            date_indices_cpu=date_indices_cpu,
            mask_cpu=mask_cpu,
            device=device,
            amp_dtype=amp_dtype,
            mode="panel",
            warmup=max(0, int(args.warmup)),
            iters=max(1, int(args.iters)),
        ),
    ]
    for result in results:
        print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
