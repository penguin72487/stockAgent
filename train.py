from __future__ import annotations

import argparse
import json
import os
import random
from dataclasses import asdict
from pathlib import Path

import numpy as np
import torch

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.runtime_env import normalize_cuda_env
from stockagent.training.trainer import run_inference, run_training


def _resolve_cpu_thread_count(raw: object | None) -> int | None:
    if raw is None:
        return None
    value = str(raw).strip().lower()
    if value in {"", "0", "auto", "none", "off", "false"}:
        return None
    threads = int(value)
    if threads < 1:
        raise ValueError(f"CPU thread count must be >= 1, got {threads}")
    return threads


def _configure_cpu_parallelism(*, cpu_threads: int | None, compile_threads: int | None) -> None:
    resolved_cpu_threads = cpu_threads
    if resolved_cpu_threads is None:
        raw_env = os.environ.get("STOCKAGENT_CPU_THREADS")
        if raw_env:
            resolved_cpu_threads = _resolve_cpu_thread_count(raw_env)
    if resolved_cpu_threads is None:
        resolved_cpu_threads = max(1, int(os.cpu_count() or 1))

    for env_name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[env_name] = str(resolved_cpu_threads)
    torch.set_num_threads(resolved_cpu_threads)
    try:
        torch.set_num_interop_threads(resolved_cpu_threads)
    except RuntimeError:
        # Inter-op threads can only be set before parallel work starts.
        pass

    resolved_compile_threads = compile_threads
    if resolved_compile_threads is None:
        raw_env = os.environ.get("STOCKAGENT_TORCH_COMPILE_THREADS")
        if raw_env:
            resolved_compile_threads = _resolve_cpu_thread_count(raw_env)
    if resolved_compile_threads is None:
        resolved_compile_threads = resolved_cpu_threads
    os.environ["TORCHINDUCTOR_COMPILE_THREADS"] = str(resolved_compile_threads)
    try:
        import torch._inductor.config as inductor_config  # type: ignore

        inductor_config.compile_threads = int(resolved_compile_threads)
    except Exception:
        pass
    print(
        "[runtime] cpu_parallelism "
        f"torch_threads={torch.get_num_threads()} "
        f"interop_threads={torch.get_num_interop_threads()} "
        f"inductor_compile_threads={os.environ.get('TORCHINDUCTOR_COMPILE_THREADS')}"
    )


def _configure_cuda_runtime() -> None:
    normalize_cuda_env()
    if not torch.cuda.is_available():
        return
    torch.set_float32_matmul_precision("high")
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True
    # Keep convolution autotuner on for mostly-stable shapes.
    torch.backends.cudnn.benchmark = True
    torch.backends.cudnn.deterministic = False
    for attr in (
        "allow_fp16_reduced_precision_reduction",
        "allow_bf16_reduced_precision_reduction",
    ):
        if hasattr(torch.backends.cuda.matmul, attr):
            setattr(torch.backends.cuda.matmul, attr, True)
    # Prefer fused attention kernels when available.
    if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_flash_sdp"):
        torch.backends.cuda.enable_flash_sdp(True)
    if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_mem_efficient_sdp"):
        torch.backends.cuda.enable_mem_efficient_sdp(True)
    if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_math_sdp"):
        torch.backends.cuda.enable_math_sdp(True)


def _set_global_seed(seed: int) -> None:
    seed = int(seed)
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the stockAgent baseline model")
    parser.add_argument("--config", default="configs/markets/tw.yaml", help="Path to experiment config")
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
        "--retrain-completed-folds",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="When resuming, ignore completed fold markers but still load checkpoints.",
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
        "--debug-timing-sync",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Synchronize CUDA at train iteration boundaries to diagnose async timing attribution.",
    )
    parser.add_argument(
        "--start-fold",
        type=int,
        default=None,
        help="Start from this fold id (inclusive), e.g. --start-fold 7",
    )
    parser.add_argument("--max-folds", type=int, default=None, help="Run at most this many folds after --start-fold filtering.")
    parser.add_argument("--epochs", type=int, default=None, help="Override training.epochs for benchmark/smoke runs.")
    parser.add_argument("--seed", type=int, default=None, help="Override training.seed for PyTorch/NumPy/Python RNGs.")
    parser.add_argument(
        "--cpu-threads",
        type=int,
        default=None,
        help="Set PyTorch/BLAS CPU worker threads. Defaults to all logical CPUs.",
    )
    parser.add_argument(
        "--torch-compile-threads",
        type=int,
        default=None,
        help="Set TorchInductor compile worker threads. Defaults to --cpu-threads/all logical CPUs.",
    )
    parser.add_argument(
        "--explain-after-each-fold",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.explain_after_each_fold.",
    )
    parser.add_argument("--explain-max-rows", type=int, default=None, help="Override training.explain_max_rows.")
    parser.add_argument("--explain-ig-steps", type=int, default=None, help="Override training.explain_ig_steps.")
    parser.add_argument("--explain-ig-batch-size", type=int, default=None, help="Override training.explain_ig_batch_size.")
    parser.add_argument(
        "--explain-perturb",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.explain_perturb.",
    )
    parser.add_argument(
        "--explain-perturb-batch-size",
        type=int,
        default=None,
        help="Override training.explain_perturb_batch_size.",
    )
    parser.add_argument(
        "--explain-perturb-max-auto-batch-size",
        type=int,
        default=None,
        help="Override training.explain_perturb_max_auto_batch_size.",
    )
    parser.add_argument(
        "--explain-perturb-max-input-elements",
        type=int,
        default=None,
        help="Override training.explain_perturb_max_input_elements.",
    )
    parser.add_argument(
        "--explain-umap",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.explain_umap_enabled.",
    )
    parser.add_argument("--explain-umap-max-points", type=int, default=None, help="Override training.explain_umap_max_points.")
    parser.add_argument(
        "--explain-umap-max-projections",
        type=int,
        default=None,
        help="Override training.explain_umap_max_projections; 0 means no projection-count limit.",
    )
    parser.add_argument(
        "--explain-write-plots",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.explain_write_plots.",
    )
    parser.add_argument(
        "--explain-standard-plots",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.explain_standard_plots.",
    )
    parser.add_argument(
        "--explain-cross-asset",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.explain_cross_asset_enabled.",
    )
    parser.add_argument(
        "--explain-cross-asset-max-sources",
        type=int,
        default=None,
        help="Override training.explain_cross_asset_max_sources.",
    )
    parser.add_argument(
        "--explain-cross-asset-max-targets",
        type=int,
        default=None,
        help="Override training.explain_cross_asset_max_targets.",
    )
    parser.add_argument(
        "--explain-cross-asset-source-chunk-size",
        type=int,
        default=None,
        help="Override training.explain_cross_asset_source_chunk_size.",
    )
    parser.add_argument(
        "--explain-cross-asset-shocks",
        default=None,
        help="Comma-separated override for training.explain_cross_asset_shocks.",
    )
    parser.add_argument(
        "--explain-cross-asset-graph-backend",
        choices=("auto", "polars", "cugraph"),
        default=None,
        help="Override training.explain_cross_asset_graph_backend.",
    )
    parser.add_argument(
        "--explain-cross-asset-graph-benchmark-min-edges",
        type=int,
        default=None,
        help="Override training.explain_cross_asset_graph_benchmark_min_edges.",
    )
    parser.add_argument(
        "--explain-cross-asset-graph-explainability",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.explain_cross_asset_graph_explainability.",
    )
    parser.add_argument(
        "--explain-cross-asset-graph-betweenness-max-vertices",
        type=int,
        default=None,
        help="Override training.explain_cross_asset_graph_betweenness_max_vertices.",
    )
    parser.add_argument(
        "--explain-cross-asset-graph-plot-max-nodes",
        type=int,
        default=None,
        help="Override training.explain_cross_asset_graph_plot_max_nodes.",
    )
    parser.add_argument(
        "--save-daily-weights-csv",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Compatibility alias for training.save_daily_weights_table.",
    )
    parser.add_argument(
        "--save-integer-share-heavy-csv",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Compatibility alias for writing integer daily weights and holdings detail tables.",
    )
    parser.add_argument(
        "--save-daily-weights-table",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.save_daily_weights_table.",
    )
    parser.add_argument(
        "--save-integer-share-detail-tables",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.save_integer_share_daily_weights_table and save_integer_share_holdings_table.",
    )
    parser.add_argument(
        "--table-output-format",
        choices=("csv", "parquet"),
        default=None,
        help="Override training.table_output_format for large fold detail tables.",
    )
    parser.add_argument(
        "--backtest-artifact-compression",
        choices=("none", "compressed"),
        default=None,
        help="Override training.backtest_artifact_compression for .npz backtest artifacts.",
    )
    parser.add_argument(
        "--defer-epoch-curve-plot-until-end",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.defer_epoch_curve_plot_until_end.",
    )
    parser.add_argument(
        "--postprocess-benchmark-after-fold",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.postprocess_benchmark_after_fold.",
    )
    parser.add_argument(
        "--postprocess-benchmark-after-best-val",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.postprocess_benchmark_after_best_val.",
    )
    parser.add_argument(
        "--eval-backtest-chunk-rows",
        type=int,
        default=None,
        help="Override training.eval_backtest_chunk_rows.",
    )
    parser.add_argument(
        "--eval-backtest-chunk-rows-auto",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.eval_backtest_chunk_rows_auto.",
    )
    parser.add_argument(
        "--eval-backtest-compile",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Compile eval/test backtest scans. Model and loss compile are controlled separately.",
    )
    parser.add_argument(
        "--backtest-compile",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.backtest_compile.",
    )
    parser.add_argument(
        "--backtest-compile-stateful",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.backtest_compile_stateful.",
    )
    parser.add_argument(
        "--backtest-compile-dynamic",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.backtest_compile_dynamic.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _configure_cpu_parallelism(cpu_threads=args.cpu_threads, compile_threads=args.torch_compile_threads)
    os.environ["STOCKAGENT_CONFIG_PATH"] = str(Path(args.config).resolve())
    config = load_config(args.config)
    if args.seed is not None:
        config.training.seed = int(args.seed)
    _set_global_seed(int(config.training.seed))
    if args.epochs is not None:
        if args.epochs < 1:
            raise ValueError(f"--epochs must be >= 1, got {args.epochs}")
        config.training.epochs = int(args.epochs)
    if args.debug_timing_sync is not None:
        config.training.debug_timing_sync = bool(args.debug_timing_sync)
    if args.explain_after_each_fold is not None:
        config.training.explain_after_each_fold = bool(args.explain_after_each_fold)
    if args.explain_max_rows is not None:
        config.training.explain_max_rows = max(1, int(args.explain_max_rows))
    if args.explain_ig_steps is not None:
        config.training.explain_ig_steps = max(0, int(args.explain_ig_steps))
    if args.explain_ig_batch_size is not None:
        config.training.explain_ig_batch_size = max(0, int(args.explain_ig_batch_size))
    if args.explain_perturb is not None:
        config.training.explain_perturb = bool(args.explain_perturb)
    if args.explain_perturb_batch_size is not None:
        config.training.explain_perturb_batch_size = max(0, int(args.explain_perturb_batch_size))
    if args.explain_perturb_max_auto_batch_size is not None:
        config.training.explain_perturb_max_auto_batch_size = max(1, int(args.explain_perturb_max_auto_batch_size))
    if args.explain_perturb_max_input_elements is not None:
        config.training.explain_perturb_max_input_elements = max(1, int(args.explain_perturb_max_input_elements))
    if args.explain_umap is not None:
        config.training.explain_umap_enabled = bool(args.explain_umap)
    if args.explain_umap_max_points is not None:
        config.training.explain_umap_max_points = max(0, int(args.explain_umap_max_points))
    if args.explain_umap_max_projections is not None:
        config.training.explain_umap_max_projections = max(0, int(args.explain_umap_max_projections))
    if args.explain_write_plots is not None:
        config.training.explain_write_plots = bool(args.explain_write_plots)
    if args.explain_standard_plots is not None:
        config.training.explain_standard_plots = bool(args.explain_standard_plots)
    if args.explain_cross_asset is not None:
        config.training.explain_cross_asset_enabled = bool(args.explain_cross_asset)
    if args.explain_cross_asset_max_sources is not None:
        config.training.explain_cross_asset_max_sources = max(1, int(args.explain_cross_asset_max_sources))
    if args.explain_cross_asset_max_targets is not None:
        config.training.explain_cross_asset_max_targets = max(1, int(args.explain_cross_asset_max_targets))
    if args.explain_cross_asset_source_chunk_size is not None:
        config.training.explain_cross_asset_source_chunk_size = max(1, int(args.explain_cross_asset_source_chunk_size))
    if args.explain_cross_asset_shocks is not None:
        config.training.explain_cross_asset_shocks = [
            value.strip().lower() for value in str(args.explain_cross_asset_shocks).split(",") if value.strip()
        ]
    if args.explain_cross_asset_graph_backend is not None:
        config.training.explain_cross_asset_graph_backend = str(args.explain_cross_asset_graph_backend)
    if args.explain_cross_asset_graph_benchmark_min_edges is not None:
        config.training.explain_cross_asset_graph_benchmark_min_edges = max(
            0,
            int(args.explain_cross_asset_graph_benchmark_min_edges),
        )
    if args.explain_cross_asset_graph_explainability is not None:
        config.training.explain_cross_asset_graph_explainability = bool(args.explain_cross_asset_graph_explainability)
    if args.explain_cross_asset_graph_betweenness_max_vertices is not None:
        config.training.explain_cross_asset_graph_betweenness_max_vertices = max(
            0,
            int(args.explain_cross_asset_graph_betweenness_max_vertices),
        )
    if args.explain_cross_asset_graph_plot_max_nodes is not None:
        config.training.explain_cross_asset_graph_plot_max_nodes = max(
            5,
            int(args.explain_cross_asset_graph_plot_max_nodes),
        )
    if args.save_daily_weights_csv is not None:
        config.training.save_daily_weights_csv = bool(args.save_daily_weights_csv)
        config.training.save_daily_weights_table = bool(args.save_daily_weights_csv)
    if args.save_integer_share_heavy_csv is not None:
        config.training.save_integer_share_daily_weights_csv = bool(args.save_integer_share_heavy_csv)
        config.training.save_integer_share_holdings_csv = bool(args.save_integer_share_heavy_csv)
        config.training.save_integer_share_daily_weights_table = bool(args.save_integer_share_heavy_csv)
        config.training.save_integer_share_holdings_table = bool(args.save_integer_share_heavy_csv)
    if args.save_daily_weights_table is not None:
        config.training.save_daily_weights_table = bool(args.save_daily_weights_table)
    if args.save_integer_share_detail_tables is not None:
        config.training.save_integer_share_daily_weights_table = bool(args.save_integer_share_detail_tables)
        config.training.save_integer_share_holdings_table = bool(args.save_integer_share_detail_tables)
    if args.table_output_format is not None:
        config.training.table_output_format = str(args.table_output_format)
    if args.backtest_artifact_compression is not None:
        config.training.backtest_artifact_compression = str(args.backtest_artifact_compression)
    if args.defer_epoch_curve_plot_until_end is not None:
        config.training.defer_epoch_curve_plot_until_end = bool(args.defer_epoch_curve_plot_until_end)
    if args.postprocess_benchmark_after_fold is not None:
        config.training.postprocess_benchmark_after_fold = bool(args.postprocess_benchmark_after_fold)
    if args.postprocess_benchmark_after_best_val is not None:
        config.training.postprocess_benchmark_after_best_val = bool(args.postprocess_benchmark_after_best_val)
    if args.eval_backtest_chunk_rows is not None:
        config.training.eval_backtest_chunk_rows = max(1, int(args.eval_backtest_chunk_rows))
    if args.eval_backtest_chunk_rows_auto is not None:
        config.training.eval_backtest_chunk_rows_auto = bool(args.eval_backtest_chunk_rows_auto)
    if args.eval_backtest_compile is not None:
        config.training.eval_backtest_compile = bool(args.eval_backtest_compile)
        os.environ["STOCKAGENT_EVAL_BACKTEST_COMPILE"] = "1" if bool(args.eval_backtest_compile) else "0"
    if args.backtest_compile is not None:
        config.training.backtest_compile = bool(args.backtest_compile)
    if args.backtest_compile_stateful is not None:
        config.training.backtest_compile_stateful = bool(args.backtest_compile_stateful)
    if args.backtest_compile_dynamic is not None:
        config.training.backtest_compile_dynamic = bool(args.backtest_compile_dynamic)
    _configure_cuda_runtime()

    # Keep runtime switches consistent with YAML config.
    os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = "1" if config.training.backtest_autotune else "0"
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1" if config.training.backtest_compile else "0"
    os.environ["STOCKAGENT_BACKTEST_VERBOSE"] = "1" if config.training.backtest_verbose else "0"
    os.environ["STOCKAGENT_STRICT_NO_FALLBACK"] = "1" if config.training.strict_no_fallback else "0"
    os.environ["STOCKAGENT_BACKTEST_CHECKPOINT_CHUNK_ROWS"] = str(config.training.backtest_checkpoint_chunk_rows)
    os.environ["STOCKAGENT_AUTO_TORCH_COMPILE_SHARPE"] = "1" if config.training.auto_torch_compile_sharpe else "0"
    if config.training.compile_loss is not None:
        os.environ["STOCKAGENT_COMPILE_LOSS"] = "1" if config.training.compile_loss else "0"

    output_dir = args.output_dir if args.output_dir is not None else config.runner.output_dir
    mode = args.mode if args.mode is not None else config.runner.mode
    resume = args.resume if args.resume is not None else config.runner.resume
    if args.retrain_completed_folds is not None:
        os.environ["STOCKAGENT_RETRAIN_COMPLETED_FOLDS"] = "1" if bool(args.retrain_completed_folds) else "0"
    post_train_infer = args.post_train_infer if args.post_train_infer is not None else config.runner.post_train_infer
    start_fold = args.start_fold if args.start_fold is not None else config.runner.start_fold

    if config.runner.require_cuda and not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA is required by config (runner.require_cuda=true), "
            "but torch.cuda.is_available() is False. "
            "Please run on a GPU-enabled environment."
        )
    if config.environment.device == "cuda" and not torch.cuda.is_available():
        if config.training.strict_no_fallback:
            raise RuntimeError(
                "CUDA is unavailable while environment.device='cuda'; "
                "strict_no_fallback=true so CPU fallback is disabled."
            )
        config.environment.device = "cpu"
        print("[runner] CUDA unavailable; falling back to CPU because runner.require_cuda=false")

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
        external_feature_path=(
            config.data.tw_public_feature_path if config.data.use_tw_public_features else None
        ),
        external_market_symbol=config.data.tw_public_market_symbol,
        feature_include=config.data.feature_include,
        feature_exclude=config.data.feature_exclude,
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
    if args.max_folds is not None:
        if args.max_folds < 1:
            raise ValueError(f"--max-folds must be >= 1, got {args.max_folds}")
        original_count = len(folds)
        folds = folds[: int(args.max_folds)]
        print(f"[runner] max_folds={args.max_folds}: selected {len(folds)}/{original_count} folds")
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
        configured_leverage = float(config.trading.leverage)
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
                    "configured_leverage": configured_leverage,
                    "leveraged_sharpe": leveraged_sharpe,
                    "leveraged_sortino": leveraged_sortino,
                    "test_metrics": result.test_metrics,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
