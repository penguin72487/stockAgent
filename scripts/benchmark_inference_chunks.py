#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
import os
import resource
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import torch

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.models.factory import build_model
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.trainer import (
    TimingBreakdown,
    _PanelSlabForwardWrapper,
    _align_panel_to_state_dict_universe,
    _configure_backtest_runtime_from_config,
    _evaluate_windowed_tensor_batch,
    _load_checkpoint,
    _load_state_dict,
    _model_supports_panel_slab_forward,
    _prepare_windowed_split,
    _resolve_amp_dtype,
    _resolve_device,
    _resolve_inference_backtest_chunk_rows,
)
from stockagent.training.windowed import WindowedSplitTensors, dataset_to_windowed_tensors


def _configure_cuda_runtime() -> None:
    if not torch.cuda.is_available():
        return
    torch.set_float32_matmul_precision("high")
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True
    torch.backends.cudnn.benchmark = True
    if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_flash_sdp"):
        torch.backends.cuda.enable_flash_sdp(True)
    if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_mem_efficient_sdp"):
        torch.backends.cuda.enable_mem_efficient_sdp(True)
    if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_math_sdp"):
        torch.backends.cuda.enable_math_sdp(True)


def _latest_checkpoint_fold(output_dir: Path) -> int:
    fold_ids: list[int] = []
    for path in output_dir.glob("fold_*/checkpoint_best.pt"):
        try:
            fold_ids.append(int(path.parent.name.split("_")[-1]))
        except ValueError:
            continue
    if not fold_ids:
        raise FileNotFoundError(f"No checkpoint_best.pt found below {output_dir}")
    return max(fold_ids)


def _default_candidates(num_symbols: int) -> list[int]:
    if num_symbols >= 10000:
        return [1, 2, 4, 8, 12, 16, 24, 32]
    if num_symbols >= 2000:
        return [4, 8, 16, 24, 32, 48, 64, 96, 128]
    return [16, 32, 64, 96, 128, 160, 192, 256]


def _default_rows(num_symbols: int, requested: int | None) -> int:
    if requested is not None:
        return max(1, int(requested))
    if num_symbols >= 10000:
        return 256
    if num_symbols >= 2000:
        return 1024
    return 4096


def _rss_gb() -> float:
    return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0**2


def _run_case(
    *,
    model: torch.nn.Module,
    panel_slab_model: torch.nn.Module | None,
    split: WindowedSplitTensors,
    config: Any,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    chunk_rows: int,
    benchmark_rows: int,
    warmup: bool,
) -> dict[str, Any]:
    backtest_chunk_rows = _resolve_inference_backtest_chunk_rows(config, chunk_rows)
    if device.type == "cuda":
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats(device)

    if warmup:
        warm_timing = TimingBreakdown()
        warm_bt, warm_ic, warm_met = _evaluate_windowed_tensor_batch(
            model,
            panel_slab_model,
            split,
            device,
            amp_dtype,
            non_blocking,
            config.trading.long_only,
            config.trading.buy_fee_rate,
            config.trading.sell_fee_rate,
            config.trading.max_turnover_ratio,
            config.trading.gross_leverage,
            chunk_rows=chunk_rows,
            backtest_chunk_rows=backtest_chunk_rows,
            compute_ic=True,
            compute_metrics_summary=True,
            return_weights_history=True,
            timing_out=warm_timing,
        )
        del warm_bt, warm_ic, warm_met, warm_timing
        gc.collect()
        if device.type == "cuda":
            torch.cuda.synchronize(device)
            torch.cuda.empty_cache()
            torch.cuda.reset_peak_memory_stats(device)

    timing = TimingBreakdown()
    started = time.perf_counter()
    bt, ic, metrics = _evaluate_windowed_tensor_batch(
        model,
        panel_slab_model,
        split,
        device,
        amp_dtype,
        non_blocking,
        config.trading.long_only,
        config.trading.buy_fee_rate,
        config.trading.sell_fee_rate,
        config.trading.max_turnover_ratio,
        config.trading.gross_leverage,
        chunk_rows=chunk_rows,
        backtest_chunk_rows=backtest_chunk_rows,
        compute_ic=True,
        compute_metrics_summary=True,
        return_weights_history=True,
        timing_out=timing,
    )
    if device.type == "cuda":
        torch.cuda.synchronize(device)
    elapsed_s = time.perf_counter() - started

    peak_alloc_gb = 0.0
    peak_reserved_gb = 0.0
    if device.type == "cuda":
        peak_alloc_gb = float(torch.cuda.max_memory_allocated(device)) / 1024.0**3
        peak_reserved_gb = float(torch.cuda.max_memory_reserved(device)) / 1024.0**3

    del bt, ic, metrics
    return {
        "chunk_rows": int(chunk_rows),
        "backtest_chunk_rows": int(backtest_chunk_rows),
        "elapsed_s": round(float(elapsed_s), 6),
        "rows_per_s": round(float(benchmark_rows) / max(float(elapsed_s), 1e-12), 3),
        "forward_s": round(float(timing.forward_s), 6),
        "transfer_s": round(float(timing.transfer_s), 6),
        "backtest_s": round(float(timing.backtest_s), 6),
        "ic_s": round(float(timing.ic_s), 6),
        "peak_alloc_gb": round(peak_alloc_gb, 4),
        "peak_reserved_gb": round(peak_reserved_gb, 4),
        "rss_gb": round(_rss_gb(), 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark live/inference model_chunk_rows by market.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--fold", type=int, default=None, help="Checkpoint fold id. Defaults to latest checkpoint.")
    parser.add_argument("--rows", type=int, default=None, help="Benchmark rows. Defaults scale with symbol count.")
    parser.add_argument("--candidates", default=None, help="Comma-separated chunk row candidates.")
    parser.add_argument("--no-warmup", action="store_true", help="Skip one untimed pass per candidate.")
    parser.add_argument("--backtest-compile", action="store_true", help="Enable compiled backtest for this benchmark.")
    parser.add_argument("--output", default=None, help="Optional JSON output path.")
    args = parser.parse_args()

    _configure_cuda_runtime()
    config = load_config(args.config)
    _configure_backtest_runtime_from_config(config)
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1" if bool(args.backtest_compile) else "0"
    os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = "1" if bool(args.backtest_compile) else "0"
    os.environ["STOCKAGENT_BACKTEST_VERBOSE"] = "0"
    os.environ["STOCKAGENT_AUTO_TORCH_COMPILE_SHARPE"] = "0"
    os.environ["STOCKAGENT_COMPILE_LOSS"] = "0"

    device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    non_blocking = bool(config.training.non_blocking_transfer and device.type == "cuda")
    output_dir = Path(config.runner.output_dir)
    fold_id = int(args.fold) if args.fold is not None else _latest_checkpoint_fold(output_dir)

    panel = build_panel(
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
    )
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    fold = next((item for item in folds if item.fold_id == fold_id), None)
    if fold is None:
        raise ValueError(f"fold_id={fold_id} not found; available={[item.fold_id for item in folds]}")

    checkpoint_path = output_dir / f"fold_{fold_id:02d}" / "checkpoint_best.pt"
    checkpoint = _load_checkpoint(checkpoint_path)
    state_dict = checkpoint.get("model_state_dict")
    if not isinstance(state_dict, dict):
        raise ValueError(f"Checkpoint has no model_state_dict: {checkpoint_path}")
    panel = _align_panel_to_state_dict_universe(
        panel,
        output_dir / f"fold_{fold_id:02d}",
        state_dict,
        context=f"inference benchmark {Path(args.config).stem} fold {fold_id}",
    )

    dataset = CrossSectionalDataset(panel, fold.test_indices, config.training.lookback)
    base_split = dataset_to_windowed_tensors(dataset)
    benchmark_rows = min(_default_rows(panel.num_symbols, args.rows), len(base_split))
    if benchmark_rows <= 0:
        raise ValueError(f"fold_id={fold_id} has no inference rows")
    split = WindowedSplitTensors(
        features=base_split.features,
        valid_indices=base_split.valid_indices[:benchmark_rows],
        future_log_returns=base_split.future_log_returns,
        tradable_mask=base_split.tradable_mask,
        can_buy_mask=base_split.can_buy_mask,
        can_sell_mask=base_split.can_sell_mask,
        benchmark=base_split.benchmark,
        lookback=base_split.lookback,
    )
    split = _prepare_windowed_split(
        split,
        device,
        non_blocking,
        name=f"inference benchmark {Path(args.config).stem} fold {fold_id}",
    )

    model = build_model(
        config=config,
        lookback=config.training.lookback,
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
    ).to(device)
    _load_state_dict(model, state_dict)
    model.eval()
    panel_slab_model = _PanelSlabForwardWrapper(model) if _model_supports_panel_slab_forward(model) else None

    if args.candidates:
        candidates = [int(item.strip()) for item in str(args.candidates).split(",") if item.strip()]
    else:
        candidates = _default_candidates(panel.num_symbols)
    candidates = sorted({max(1, int(value)) for value in candidates if int(value) <= benchmark_rows})

    summary: dict[str, Any] = {
        "config": str(args.config),
        "fold_id": fold_id,
        "benchmark_rows": int(benchmark_rows),
        "total_test_rows": int(len(base_split)),
        "symbols": int(panel.num_symbols),
        "features": int(len(panel.feature_names)),
        "lookback": int(config.training.lookback),
        "device": str(device),
        "amp_dtype": str(amp_dtype),
        "panel_slab_forward": bool(panel_slab_model is not None),
        "backtest_compile": bool(args.backtest_compile),
        "candidates": candidates,
        "results": [],
    }
    print(json.dumps({key: value for key, value in summary.items() if key != "results"}, sort_keys=True), flush=True)

    for chunk_rows in candidates:
        try:
            result = _run_case(
                model=model,
                panel_slab_model=panel_slab_model,
                split=split,
                config=config,
                device=device,
                amp_dtype=amp_dtype,
                non_blocking=non_blocking,
                chunk_rows=chunk_rows,
                benchmark_rows=benchmark_rows,
                warmup=not bool(args.no_warmup),
            )
        except RuntimeError as exc:
            if device.type == "cuda":
                torch.cuda.empty_cache()
            result = {
                "chunk_rows": int(chunk_rows),
                "error": str(exc).splitlines()[0][:300],
            }
        summary["results"].append(result)
        print(json.dumps(result, sort_keys=True), flush=True)
        gc.collect()

    valid = [row for row in summary["results"] if "rows_per_s" in row]
    summary["best"] = max(valid, key=lambda row: float(row["rows_per_s"])) if valid else None
    print("BEST " + json.dumps(summary["best"], sort_keys=True), flush=True)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)


if __name__ == "__main__":
    main()
