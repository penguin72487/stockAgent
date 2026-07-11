from __future__ import annotations

import argparse
import atexit
import json
import os
import random
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np

# Set allocator policy before torch can initialize CUDA. Expandable segments
# reduce fragmentation across compiled DDP train/eval phases.
if "PYTORCH_CUDA_ALLOC_CONF" not in os.environ:
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = os.environ.get("PYTORCH_ALLOC_CONF", "expandable_segments:True")
os.environ.setdefault("PYTORCH_ALLOC_CONF", os.environ["PYTORCH_CUDA_ALLOC_CONF"])
import torch

from stockagent.config import load_config
from stockagent.runtime_env import normalize_cuda_env


def _normalize_multi_gpu_strategy(value: object) -> str:
    strategy = str(value or "none").strip().lower().replace("-", "_")
    aliases = {
        "": "none",
        "0": "none",
        "false": "none",
        "off": "none",
        "no": "none",
        "single": "none",
        "single_gpu": "none",
        "ddp": "distributed_data_parallel",
        "distributed": "distributed_data_parallel",
        "torch_ddp": "distributed_data_parallel",
    }
    return aliases.get(strategy, strategy)


def _maybe_relaunch_for_ddp(config, args: argparse.Namespace) -> None:
    strategy = _normalize_multi_gpu_strategy(getattr(config.training, "multi_gpu_strategy", "none"))
    if args.multi_gpu_strategy is not None:
        strategy = _normalize_multi_gpu_strategy(args.multi_gpu_strategy)
    if strategy != "distributed_data_parallel":
        return
    if int(os.environ.get("WORLD_SIZE", "1")) > 1 or os.environ.get("STOCKAGENT_DDP_LAUNCHED") == "1":
        return
    if not torch.cuda.is_available():
        raise RuntimeError("training.multi_gpu_strategy=distributed_data_parallel requires CUDA")

    visible_count = int(torch.cuda.device_count())
    device_ids = list(range(visible_count))
    if len(device_ids) < 2:
        raise RuntimeError(
            "training.multi_gpu_strategy=distributed_data_parallel needs at least two visible CUDA devices; "
            f"resolved device_ids={device_ids}, cuda_device_count={visible_count}"
        )

    env = os.environ.copy()
    env["STOCKAGENT_DDP_LAUNCHED"] = "1"
    env.setdefault("CUDA_VISIBLE_DEVICES", ",".join(str(device_id) for device_id in device_ids))
    world_size = len(device_ids)
    configured_cpu_threads = _resolve_cpu_thread_count(
        args.cpu_threads if args.cpu_threads is not None else getattr(config.environment, "cpu_threads", None)
    )
    resolved_cpu_threads = _resolve_process_thread_count(
        configured_cpu_threads,
        inherited_names=("STOCKAGENT_CPU_THREADS", "OMP_NUM_THREADS"),
        local_world_size=world_size,
        environ=env,
    )
    env["STOCKAGENT_CPU_THREADS"] = str(resolved_cpu_threads)
    configured_compile_threads = _resolve_cpu_thread_count(
        args.torch_compile_threads
        if args.torch_compile_threads is not None
        else getattr(config.environment, "torch_compile_threads", None)
    )
    resolved_compile_threads = _resolve_process_thread_count(
        configured_compile_threads,
        inherited_names=(
            "STOCKAGENT_TORCH_COMPILE_THREADS",
            "TORCHINDUCTOR_COMPILE_THREADS",
        ),
        local_world_size=world_size,
        environ=env,
        fallback=resolved_cpu_threads,
    )
    env["STOCKAGENT_TORCH_COMPILE_THREADS"] = str(resolved_compile_threads)
    for env_name in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        env[env_name] = str(resolved_cpu_threads)
    # Panel loading parallelizes across symbol files via panel_load_workers.
    # Keep Polars/Rayon inner pools small unless explicitly overridden, otherwise
    # DDP can multiply into workers * inner_threads * ranks.
    polars_threads = env.get("STOCKAGENT_POLARS_THREADS", "1")
    env["POLARS_MAX_THREADS"] = polars_threads
    env["RAYON_NUM_THREADS"] = polars_threads
    env["TORCHINDUCTOR_COMPILE_THREADS"] = str(resolved_compile_threads)

    cmd = [
        sys.executable,
        "-m",
        "torch.distributed.run",
        "--standalone",
        "--nproc_per_node",
        str(world_size),
        *sys.argv,
    ]
    print(
        "[ddp] relaunching under torchrun "
        f"world_size={world_size} device_ids={device_ids} "
        f"cmd={' '.join(cmd)}",
        flush=True,
    )
    os.execvpe(sys.executable, cmd, env)


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


def _available_cpu_count() -> int:
    """Return this process's affinity-aware CPU capacity."""
    get_affinity = getattr(os, "sched_getaffinity", None)
    if callable(get_affinity):
        try:
            return max(1, len(get_affinity(0)))
        except (OSError, TypeError):
            pass
    return max(1, int(os.cpu_count() or 1))


def _local_world_size(active_strategy: str | None = None) -> int:
    if active_strategy is not None and active_strategy != "distributed_data_parallel":
        return 1
    raw = os.environ.get("LOCAL_WORLD_SIZE", os.environ.get("WORLD_SIZE", "1"))
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 1


def _resolve_process_thread_count(
    configured_total: int | None,
    *,
    inherited_names: Sequence[str],
    local_world_size: int,
    environ: Mapping[str, str] | None = None,
    fallback: int | None = None,
) -> int:
    """Resolve one rank's thread budget without multiplying host-wide pools.

    Explicit config/CLI values are host-wide budgets and are divided across
    local ranks. Inherited OMP/Inductor values are already process-local
    contracts, so they are preserved. With neither, CPU affinity is divided by
    LOCAL_WORLD_SIZE (not global WORLD_SIZE, which is wrong on multi-node jobs).
    """
    ranks = max(1, int(local_world_size))
    if configured_total is not None:
        return max(1, int(configured_total) // ranks)

    source = os.environ if environ is None else environ
    for name in inherited_names:
        raw = source.get(name)
        if raw is None:
            continue
        resolved = _resolve_cpu_thread_count(raw)
        if resolved is not None:
            return resolved

    if fallback is not None:
        return max(1, int(fallback))
    return max(1, _available_cpu_count() // ranks)


def _configure_cpu_parallelism(
    *,
    cpu_threads: int | None,
    compile_threads: int | None,
    local_world_size: int = 1,
) -> None:
    resolved_cpu_threads = _resolve_process_thread_count(
        cpu_threads,
        inherited_names=("STOCKAGENT_CPU_THREADS", "OMP_NUM_THREADS"),
        local_world_size=local_world_size,
    )
    os.environ["STOCKAGENT_CPU_THREADS"] = str(resolved_cpu_threads)

    for env_name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[env_name] = str(resolved_cpu_threads)

    raw_polars_threads = os.environ.get("STOCKAGENT_POLARS_THREADS")
    resolved_polars_threads = _resolve_cpu_thread_count(raw_polars_threads) if raw_polars_threads else 1
    # Panel loading already parallelizes across symbol parquet files. Keep
    # Polars/Rayon single-threaded per file to avoid 128 outer workers each
    # spawning a full inner CPU pool.
    os.environ["POLARS_MAX_THREADS"] = str(resolved_polars_threads)
    os.environ["RAYON_NUM_THREADS"] = str(resolved_polars_threads)

    torch.set_num_threads(resolved_cpu_threads)
    try:
        torch.set_num_interop_threads(resolved_cpu_threads)
    except RuntimeError:
        # Inter-op threads can only be set before parallel work starts.
        pass

    resolved_compile_threads = _resolve_process_thread_count(
        compile_threads,
        inherited_names=(
            "STOCKAGENT_TORCH_COMPILE_THREADS",
            "TORCHINDUCTOR_COMPILE_THREADS",
        ),
        local_world_size=local_world_size,
        fallback=resolved_cpu_threads,
    )
    os.environ["STOCKAGENT_TORCH_COMPILE_THREADS"] = str(resolved_compile_threads)
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
        f"inductor_compile_threads={os.environ.get('TORCHINDUCTOR_COMPILE_THREADS')} "
        f"polars_threads={os.environ.get('POLARS_MAX_THREADS')} "
        f"rayon_threads={os.environ.get('RAYON_NUM_THREADS')}"
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


def _distributed_ready() -> bool:
    return torch.distributed.is_available() and torch.distributed.is_initialized()


def _distributed_rank() -> int:
    if _distributed_ready():
        return int(torch.distributed.get_rank())
    return int(os.environ.get("RANK", "0"))


def _distributed_world_size() -> int:
    if _distributed_ready():
        return int(torch.distributed.get_world_size())
    return int(os.environ.get("WORLD_SIZE", "1"))


def _destroy_distributed_at_exit() -> None:
    if _distributed_ready():
        torch.distributed.destroy_process_group()


def _maybe_init_distributed_for_panel(active_strategy: str, config) -> None:
    if active_strategy != "distributed_data_parallel":
        return
    if int(os.environ.get("WORLD_SIZE", "1")) <= 1:
        return
    if _distributed_ready():
        return
    device_name = str(getattr(config.environment, "device", "cpu")).strip().lower()
    backend = "nccl" if device_name == "cuda" and torch.cuda.is_available() else "gloo"
    if backend == "nccl":
        local_rank = int(os.environ.get("LOCAL_RANK", "0"))
        if local_rank < 0 or local_rank >= torch.cuda.device_count():
            raise RuntimeError(
                f"LOCAL_RANK={local_rank} is unavailable; "
                f"visible CUDA device_count={torch.cuda.device_count()}"
            )
        torch.cuda.set_device(local_rank)
    torch.distributed.init_process_group(backend=backend)
    atexit.register(_destroy_distributed_at_exit)


def _build_panel_kwargs(config) -> dict:
    use_tw_public_features = bool(config.data.use_tw_public_features)
    use_tw_public_rules = bool(config.data.use_tw_public_rules)
    use_tw_public_data = use_tw_public_features or use_tw_public_rules
    return {
        "benchmark_name": config.data.benchmark_name,
        "usd_only_trading_pairs": config.data.usd_only_trading_pairs,
        "tradable_mode": config.data.tradable_mode,
        "trading_volume_policy": config.data.trading_volume_policy,
        "security_filter": config.data.security_filter,
        "strict_no_fallback": config.training.strict_no_fallback,
        "panel_backend": config.data.panel_backend,
        "panel_load_workers": config.data.panel_load_workers,
        "external_feature_path": (
            config.data.tw_public_feature_path if use_tw_public_data else None
        ),
        "external_market_symbol": config.data.tw_public_market_symbol,
        "external_include_features": use_tw_public_features,
        "external_include_rules": use_tw_public_rules,
        "external_data_required": use_tw_public_data,
        "feature_include": config.data.feature_include,
        "feature_exclude": config.data.feature_exclude,
        "feature_zero_fill": config.data.feature_zero_fill,
    }


def _build_panel_rank_coordinated(build_panel, config, active_strategy: str):
    kwargs = _build_panel_kwargs(config)
    if active_strategy != "distributed_data_parallel" or _distributed_world_size() <= 1:
        return build_panel(config.data.parquet_root, **kwargs)

    rank = _distributed_rank()
    world_size = _distributed_world_size()
    panel = None
    rank0_error: Exception | None = None
    if rank == 0:
        print(
            f"[panel-ddp] rank0 builds or loads panel cache first; "
            f"{world_size - 1} rank(s) wait to avoid duplicate materialization",
            flush=True,
        )
        try:
            panel = build_panel(config.data.parquet_root, **kwargs)
            if panel is None:
                raise RuntimeError("rank0 panel builder returned None")
        except Exception as exc:  # synchronize failure before any rank advances
            rank0_error = exc

    _raise_if_distributed_phase_failed("rank0_build", rank0_error)

    worker_error: Exception | None = None
    if rank != 0:
        print(f"[panel-ddp] rank{rank} loading panel after rank0 cache barrier", flush=True)
        try:
            panel = build_panel(config.data.parquet_root, **kwargs)
            if panel is None:
                raise RuntimeError(f"rank{rank} panel loader returned None")
        except Exception as exc:  # every rank reports before the shared decision
            worker_error = exc

    _raise_if_distributed_phase_failed("worker_cache_load", worker_error)
    if panel is None:  # defensive: synchronized phases should make this unreachable
        raise RuntimeError(f"DDP panel build produced no panel on rank {rank}")
    return panel


def _raise_if_distributed_phase_failed(phase: str, local_error: Exception | None) -> None:
    """Make every rank take the same error branch after a coordinated phase."""
    if not _distributed_ready() or _distributed_world_size() <= 1:
        if local_error is not None:
            raise RuntimeError(
                f"DDP panel phase {phase!r} failed: "
                f"{type(local_error).__name__}: {local_error}"
            ) from local_error
        return

    local_status = {
        "rank": int(_distributed_rank()),
        "phase": str(phase),
        "ok": local_error is None,
        "error": (
            None
            if local_error is None
            else f"{type(local_error).__name__}: {local_error}"
        ),
    }
    statuses: list[dict | None] = [None] * _distributed_world_size()
    torch.distributed.all_gather_object(statuses, local_status)
    failures = sorted(
        (status for status in statuses if status is not None and not bool(status.get("ok"))),
        key=lambda status: int(status.get("rank", -1)),
    )
    if not failures:
        return
    details = "; ".join(
        f"rank{int(status.get('rank', -1))}: {status.get('error', 'unknown error')}"
        for status in failures
    )
    message = f"DDP panel phase {phase!r} failed consistently across ranks ({details})"
    if local_error is not None:
        raise RuntimeError(message) from local_error
    raise RuntimeError(message)


def _set_global_seed(seed: int) -> None:
    seed = int(seed)
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def _rank_seed(base_seed: int, rank: int) -> int:
    # NumPy's legacy global RNG requires a 32-bit seed. The mapping remains
    # stable across hosts and gives each DDP rank a distinct stochastic stream.
    return (int(base_seed) + int(rank)) % (2**32)


def _set_rank_local_seed(base_seed: int, active_strategy: str) -> int:
    rank = (
        _distributed_rank()
        if active_strategy == "distributed_data_parallel" and _distributed_ready()
        else 0
    )
    resolved = _rank_seed(base_seed, rank)
    _set_global_seed(resolved)
    print(
        f"[runtime] rng_seed base={int(base_seed)} rank={rank} resolved={resolved}",
        flush=True,
    )
    return resolved


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
        help=(
            "Set a host-wide PyTorch/BLAS thread budget (divided across local DDP ranks). "
            "Default: inherited OMP_NUM_THREADS, otherwise CPU affinity/local rank count."
        ),
    )
    parser.add_argument(
        "--torch-compile-threads",
        type=int,
        default=None,
        help=(
            "Set a host-wide TorchInductor compile-thread budget (divided across local DDP ranks). "
            "Default: inherited Inductor setting, otherwise the per-rank CPU budget."
        ),
    )
    parser.add_argument(
        "--multi-gpu-strategy",
        choices=("none", "distributed_data_parallel", "ddp"),
        default=None,
        help="Override training.multi_gpu_strategy for efficiency A/B runs.",
    )
    parser.add_argument("--batch-size-train", type=int, default=None, help="Override training.batch_size_train.")
    parser.add_argument("--batch-size-eval", type=int, default=None, help="Override training.batch_size_eval.")
    parser.add_argument(
        "--cache-train-tensors-on-gpu",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.cache_train_tensors_on_gpu.",
    )
    parser.add_argument(
        "--cache-eval-tensors-on-gpu",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override training.cache_eval_tensors_on_gpu.",
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
    os.environ["STOCKAGENT_CONFIG_PATH"] = str(Path(args.config).resolve())
    config = load_config(args.config)
    _maybe_relaunch_for_ddp(config, args)
    config_strategy = _normalize_multi_gpu_strategy(getattr(config.training, "multi_gpu_strategy", "none"))
    cli_strategy = _normalize_multi_gpu_strategy(args.multi_gpu_strategy) if args.multi_gpu_strategy is not None else None
    active_strategy = cli_strategy or config_strategy
    configured_cpu_threads = args.cpu_threads if args.cpu_threads is not None else config.environment.cpu_threads
    compile_threads = (
        args.torch_compile_threads
        if args.torch_compile_threads is not None
        else config.environment.torch_compile_threads
    )
    _configure_cpu_parallelism(
        cpu_threads=configured_cpu_threads,
        compile_threads=compile_threads,
        local_world_size=_local_world_size(active_strategy),
    )

    from stockagent.data.panel import build_panel
    from stockagent.data.walkforward import (
        build_expanding_year_folds,
        validate_walk_forward_year_contract,
    )
    from stockagent.training.trainer import run_inference, run_training

    if args.seed is not None:
        config.training.seed = int(args.seed)
    if args.multi_gpu_strategy is not None:
        config.training.multi_gpu_strategy = _normalize_multi_gpu_strategy(args.multi_gpu_strategy)
    if args.batch_size_train is not None:
        if args.batch_size_train < 1:
            raise ValueError(f"--batch-size-train must be >= 1, got {args.batch_size_train}")
        config.training.batch_size_train = int(args.batch_size_train)
    if args.batch_size_eval is not None:
        if args.batch_size_eval < 1:
            raise ValueError(f"--batch-size-eval must be >= 1, got {args.batch_size_eval}")
        config.training.batch_size_eval = int(args.batch_size_eval)
    if args.cache_train_tensors_on_gpu is not None:
        config.training.cache_train_tensors_on_gpu = bool(args.cache_train_tensors_on_gpu)
    if args.cache_eval_tensors_on_gpu is not None:
        config.training.cache_eval_tensors_on_gpu = bool(args.cache_eval_tensors_on_gpu)
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
        config.training.save_daily_weights_table = bool(args.save_daily_weights_csv)
    if args.save_integer_share_heavy_csv is not None:
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
    _maybe_init_distributed_for_panel(active_strategy, config)
    # DDP model parameters are synchronized by the DDP constructor later, but
    # rank-local dropout/augmentation streams must not be identical. Seed only
    # after process-group initialization so rank is authoritative.
    _set_rank_local_seed(int(config.training.seed), active_strategy)

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

    panel = _build_panel_rank_coordinated(build_panel, config, active_strategy)
    validate_walk_forward_year_contract(
        panel.dates,
        expected_first_year=config.walk_forward.expected_first_year,
        require_contiguous_years=config.walk_forward.require_contiguous_years,
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

    dist_ready = torch.distributed.is_available() and torch.distributed.is_initialized()
    is_rank0 = (not dist_ready) or int(torch.distributed.get_rank()) == 0
    if is_rank0:
        summary_path = Path(output_dir) / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        with summary_path.open("w", encoding="utf-8") as handle:
            json.dump([asdict(result) for result in results], handle, indent=2)

        for result in results:
            reporting_leverage = float(config.trading.reporting_leverage)
            canonical_sharpe = float(result.test_metrics.get("sharpe", 0.0))
            canonical_sortino = float(result.test_metrics.get("sortino", 0.0))
            print(
                json.dumps(
                    {
                        "fold_id": result.fold_id,
                        "train_years": result.train_years,
                        "val_years": result.val_years,
                        "test_years": result.test_years,
                        "best_val_loss": result.best_val_loss,
                        "reporting_leverage_multiplier": reporting_leverage,
                        "canonical_sharpe": canonical_sharpe,
                        "canonical_sortino": canonical_sortino,
                        "test_metrics": result.test_metrics,
                    },
                    ensure_ascii=False,
                )
            )
    if dist_ready:
        torch.distributed.destroy_process_group()


if __name__ == "__main__":
    main()
