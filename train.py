from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
import re

import numpy as np
import torch

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.training.linear_runner import run_training_linear
from stockagent.training.rl_runner import run_training_rl
from stockagent.training.trainer import run_training
from stockagent.training.xgboost_runner import run_training_xgboost


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the stockAgent baseline model")
    parser.add_argument("--config", default="configs/experiment_baseline.yaml", help="Path to experiment config")
    parser.add_argument("--output-dir", default="artifacts", help="Directory for training outputs")
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True, help="Resume from fold checkpoints when available")
    parser.add_argument(
        "--start-fold",
        type=int,
        default=3,
        help="Skip folds before this id (default: 3). Use 1 to run all folds.",
    )
    return parser.parse_args()


def _extract_symbol_code(name: str) -> str | None:
    text = (name or "").strip().upper()
    if re.fullmatch(r"\d{4}", text):
        return text
    match = re.search(r"(\d{4})", text)
    if match:
        return match.group(1)
    return None


def _apply_benchmark_override(panel, benchmark_name: str, benchmark_required: bool, benchmark_source: str) -> None:
    benchmark_mode = (benchmark_source or benchmark_name).strip().lower()

    if benchmark_mode in {"universe_average_return", "derived_from_panel", "universe_average"}:
        # Panel default is daily average return over all tradable symbols.
        print("[benchmark] using universe average daily return (all tradable symbols)")
        return

    if benchmark_mode in {"universe_cumulative_return", "universe_cumulative", "all_symbols_cumulative"}:
        returns = np.nan_to_num(panel.returns_1d, nan=0.0, posinf=0.0, neginf=0.0)
        tradable = panel.tradable_mask.astype(bool)
        panel.benchmark_returns = np.where(tradable, returns, 0.0).sum(axis=1).astype(np.float32)
        print("[benchmark] using universe cumulative daily return (sum across all tradable symbols)")
        return

    # Keep current universe benchmark unless user explicitly points to a symbol like 0050.
    if benchmark_name.strip().lower() in {"universe_average_return", "derived_from_panel", "universe_average"}:
        return

    code = _extract_symbol_code(benchmark_name)
    if code is None:
        if benchmark_required:
            raise ValueError(f"Unsupported benchmark_name: {benchmark_name}")
        print(f"[benchmark] unsupported benchmark_name={benchmark_name}, fallback to universe average")
        return

    symbol_index = {symbol: idx for idx, symbol in enumerate(panel.symbols)}
    idx = symbol_index.get(code)
    if idx is None:
        if benchmark_required:
            raise ValueError(f"Benchmark symbol {code} not found in panel symbols")
        print(f"[benchmark] symbol {code} not found, fallback to universe average")
        return

    returns = np.nan_to_num(panel.returns_1d[:, idx], nan=0.0, posinf=0.0, neginf=0.0)
    tradable = panel.tradable_mask[:, idx].astype(bool)
    panel.benchmark_returns = np.where(tradable, returns, 0.0).astype(np.float32)
    print(f"[benchmark] using symbol {code} as benchmark ({int(tradable.sum())} tradable days)")


def main() -> None:
    args = parse_args()
    if args.start_fold < 1:
        raise ValueError("--start-fold must be >= 1")

    config = load_config(args.config)
    if config.environment.device == "cuda" and not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA was requested in config (environment.device=cuda), "
            "but torch.cuda.is_available() is False. "
            "Please run on a GPU-enabled environment."
        )
    panel = build_panel(config.data.parquet_root)
    _apply_benchmark_override(
        panel,
        config.data.benchmark_name,
        config.data.benchmark_required,
        config.data.benchmark_source,
    )

    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    folds = [fold for fold in folds if fold.fold_id >= args.start_fold]
    if not folds:
        raise ValueError(f"No folds available after applying --start-fold {args.start_fold}")

    model_name = config.training.model_name.strip().lower()
    if model_name == "xgboost":
        results = run_training_xgboost(panel, folds, config, args.output_dir)
    elif model_name in {"ridge", "elasticnet"}:
        results = run_training_linear(panel, folds, config, args.output_dir, model_name=model_name)
    elif model_name in {"ppo", "ddpg", "td3", "sac"}:
        results = run_training_rl(panel, folds, config, args.output_dir, resume=args.resume)
    else:
        results = run_training(panel, folds, config, args.output_dir, resume=args.resume)

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