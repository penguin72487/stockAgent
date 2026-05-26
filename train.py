from __future__ import annotations

import argparse
import json
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
    parser.add_argument("--output-dir", default="artifacts", help="Directory for training outputs")
    parser.add_argument(
        "--mode",
        choices=("train", "infer"),
        default="train",
        help="Execution mode: train model or run pure inference from saved checkpoints/models",
    )
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True, help="Resume from fold checkpoints when available")
    parser.add_argument(
        "--post-train-infer",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After training completes, run one inference+plot pass on the same folds",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if config.environment.device == "cuda" and not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA was requested in config (environment.device=cuda), "
            "but torch.cuda.is_available() is False. "
            "Please run on a GPU-enabled environment."
        )
    panel = build_panel(
        config.data.parquet_root,
        use_rapids=config.data.use_rapids,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tw_limit_up_down_guard=config.data.tw_limit_up_down_guard,
    )
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    if args.mode == "infer":
        results = run_inference(panel, folds, config, args.output_dir)
    else:
        results = run_training(panel, folds, config, args.output_dir, resume=args.resume)
        if args.post_train_infer:
            print("[post-train] running inference+plot pass on saved models...")
            results = run_inference(panel, folds, config, args.output_dir)

    summary_path = Path(args.output_dir) / "summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump([asdict(result) for result in results], handle, indent=2)

    for result in results:
        print(
            json.dumps(
                {
                    "fold_id": result.fold_id,
                    "train_years": result.train_years,
                    "val_years": result.val_years,
                    "test_years": result.test_years,
                    "best_val_loss": result.best_val_loss,
                    "test_metrics": result.test_metrics,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()