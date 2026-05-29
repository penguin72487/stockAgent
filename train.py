from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path

import torch

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.training.trainer import run_inference, run_training


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the stockAgent baseline model")
    parser.add_argument("--config", default="configs/experiment_baseline.yaml", help="Path to experiment config")
    parser.add_argument("--output-dir", default=None, help="Directory for training outputs (override config.runner.output_dir)")
    parser.add_argument(
        "--mode",
        choices=("train", "infer"),
        default=None,
        help="Execution mode (override config.runner.mode): train model or run pure inference",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override config.runner.resume for fold checkpoint resume behavior",
    )
    parser.add_argument(
        "--post-train-infer",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override config.runner.post_train_infer after training",
    )
    parser.add_argument(
        "--profile-timing",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Print detailed timing breakdowns for train/val/test stages",
    )
    parser.add_argument(
        "--start-fold",
        type=int,
        default=None,
        help="Start from this fold id (inclusive), e.g. --start-fold 7",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    # Keep runtime switches consistent with YAML config.
    os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = "1" if config.training.backtest_autotune else "0"
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1" if config.training.backtest_compile else "0"
    os.environ["STOCKAGENT_BACKTEST_VERBOSE"] = "1" if config.training.backtest_verbose else "0"
    os.environ["STOCKAGENT_BACKTEST_CHECKPOINT_CHUNK_ROWS"] = str(config.training.backtest_checkpoint_chunk_rows)
    os.environ["STOCKAGENT_AUTO_TORCH_COMPILE_SHARPE"] = "1" if config.training.auto_torch_compile_sharpe else "0"
    if config.training.compile_loss is not None:
        os.environ["STOCKAGENT_COMPILE_LOSS"] = "1" if config.training.compile_loss else "0"

    output_dir = args.output_dir if args.output_dir is not None else config.runner.output_dir
    mode = args.mode if args.mode is not None else config.runner.mode
    resume = args.resume if args.resume is not None else config.runner.resume
    post_train_infer = args.post_train_infer if args.post_train_infer is not None else config.runner.post_train_infer
    start_fold = args.start_fold if args.start_fold is not None else config.runner.start_fold

    if config.runner.require_cuda and not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA is required by config (runner.require_cuda=true), "
            "but torch.cuda.is_available() is False. "
            "Please run on a GPU-enabled environment."
        )
    if config.environment.device == "cuda" and not torch.cuda.is_available():
        config.environment.device = "cpu"
        print("[runner] CUDA unavailable; falling back to CPU because runner.require_cuda=false")

    panel = build_panel(
        config.data.parquet_root,
        use_rapids=config.data.use_rapids,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tradable_mode=config.data.tradable_mode,
    )
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    if start_fold is not None:
        if start_fold < 1:
            raise ValueError(f"start_fold must be >= 1, got {start_fold}")
        total_folds = len(folds)
        folds = [fold for fold in folds if fold.fold_id >= start_fold]
        if not folds:
            raise ValueError(
                f"start_fold={start_fold} is out of range (total folds: {total_folds})"
            )
        print(
            f"[runner] start_fold={start_fold}: selected {len(folds)}/{total_folds} folds"
        )
    if mode == "infer":
        results = run_inference(panel, folds, config, output_dir)
    else:
        results = run_training(panel, folds, config, output_dir, resume=resume, profile_timing=args.profile_timing)
        if post_train_infer:
            print("[post-train] running inference+plot pass on saved models...")
            results = run_inference(panel, folds, config, output_dir)

    summary_path = Path(output_dir) / "summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump([asdict(result) for result in results], handle, indent=2)

    for result in results:
        configured_gross_leverage = float(config.trading.gross_leverage)
        leveraged_sharpe = float(result.test_metrics.get("sharpe", 0.0))
        leveraged_sortino = float(result.test_metrics.get("sortino", 0.0))
        print(
            json.dumps(
                {
                    "fold_id": result.fold_id,
                    "train_years": result.train_years,
                    "val_years": result.val_years,
                    "test_years": result.test_years,
                    "best_val_loss": result.best_val_loss,
                    "configured_gross_leverage": configured_gross_leverage,
                    "leveraged_sharpe": leveraged_sharpe,
                    "leveraged_sortino": leveraged_sortino,
                    "test_metrics": result.test_metrics,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()