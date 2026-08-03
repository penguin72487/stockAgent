from __future__ import annotations

import argparse
import atexit
import json
import math
import os
import time
import traceback
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Callable

import numpy as np
import torch
from tqdm.auto import tqdm

from stockagent.config import ExperimentConfig, load_config
from stockagent.data.panel import PanelData, build_panel
from stockagent.data.walkforward import WalkForwardFold, build_expanding_year_folds
from stockagent.explainability import (
    _align_panel_to_checkpoint_universe,
    _clear_explainability_runtime_cache,
    _first_test_year_dataset,
    _device_from_config,
    _estimated_first_test_year_explain_rows,
    _fold_dir,
    _sample_dataset_source,
    load_model_from_checkpoint,
)
from stockagent.explainability_cross_asset import (
    DEFAULT_SHOCKS,
    CrossAssetTransmissionSettings,
    abstract_cross_asset_transmission,
)


_FIXED_SPLIT = "test"
_FIXED_SAMPLE_METHOD = "even"


def _resolve_cuda_autocast_dtype(
    config: ExperimentConfig,
    device: torch.device,
) -> tuple[torch.dtype | None, str, str]:
    raw = str(getattr(config.environment, "amp_dtype", "none") or "none")
    configured = raw.strip().lower().replace("torch.", "")
    aliases = {
        "bfloat16": "bf16",
        "float16": "fp16",
        "half": "fp16",
        "float32": "none",
        "fp32": "none",
        "full": "none",
        "tf32": "none",
    }
    configured = aliases.get(configured, configured)
    if configured not in {"bf16", "fp16", "none"}:
        raise ValueError(
            "environment.amp_dtype for cross-asset must resolve to bf16, fp16, or none; "
            f"got {raw!r}"
        )
    if device.type != "cuda" or configured == "none":
        return None, configured, "none"
    dtype = torch.bfloat16 if configured == "bf16" else torch.float16
    return dtype, configured, configured


def _cuda_autocast_context(device: torch.device, amp_dtype: torch.dtype | None):
    if device.type != "cuda" or amp_dtype is None:
        return nullcontext()
    return torch.autocast(device_type="cuda", dtype=amp_dtype)


def _build_panel(config: ExperimentConfig) -> PanelData:
    return build_panel(
        config.data.parquet_root,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tradable_mode=config.data.tradable_mode,
        trading_volume_policy=config.data.trading_volume_policy,
        security_filter=config.data.security_filter,
        strict_no_fallback=config.training.strict_no_fallback,
        panel_backend=config.data.panel_backend,
        panel_load_workers=config.data.panel_load_workers,
        external_feature_path=(
            config.data.tw_public_feature_path
            if config.data.use_tw_public_features or config.data.use_tw_public_rules
            else None
        ),
        external_market_symbol=config.data.tw_public_market_symbol,
        external_include_features=config.data.use_tw_public_features,
        external_include_rules=config.data.use_tw_public_rules,
        external_data_required=(config.data.use_tw_public_features or config.data.use_tw_public_rules),
        feature_include=config.data.feature_include,
        feature_exclude=config.data.feature_exclude,
        feature_zero_fill=config.data.feature_zero_fill,
        feature_shift_next_session=config.data.feature_shift_next_session,
        panel_start_date=config.data.panel_start_date,
    )


def _default_output_root(training_output_dir: Path) -> Path:
    return Path(training_output_dir) / "explainability"


def _settings(
    args: argparse.Namespace,
    *,
    progress_enabled: bool | None = None,
) -> CrossAssetTransmissionSettings:
    return CrossAssetTransmissionSettings(
        enabled=True,
        progress_enabled=bool(args.progress if progress_enabled is None else progress_enabled),
        compact_artifacts=True,
        max_sources=max(0, int(args.max_sources)),
        max_targets=max(0, int(args.max_targets)),
        source_chunk_size=max(1, int(args.source_chunk_size)),
        row_chunk_size=max(0, int(args.row_chunk_size)),
        max_repeated_rows=max(1, int(args.max_repeated_rows)),
        counterfactual_compile=bool(args.counterfactual_compile),
        perturb_scale=float(args.perturb_scale),
        shocks=tuple(value.strip().lower() for value in str(args.shocks).split(",") if value.strip()),
        attention_flow=bool(args.attention_flow),
        attention_capture_rows=max(0, int(args.attention_capture_rows)),
        attention_capture_max_elements=max(1, int(args.attention_capture_max_elements)),
        validated_transmission=bool(args.validated_transmission),
        role_embedding=bool(args.role_embedding),
        graph_backend=str(args.graph_backend),
        graph_benchmark_min_edges=max(0, int(args.graph_benchmark_min_edges)),
        graph_explainability=bool(args.graph_explainability),
        graph_betweenness_max_vertices=0,
        graph_plot_max_nodes=0,
    )


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


def _distributed_local_rank() -> int:
    return int(os.environ.get("LOCAL_RANK", "0"))


def _destroy_process_group() -> None:
    if not _distributed_ready():
        return
    try:
        torch.distributed.destroy_process_group()
    except Exception:
        # A failed peer may already have forced NCCL to abort. Destruction is
        # best-effort and must not hide the original cross-asset exception.
        pass


def _initialize_process_group() -> bool:
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    if world_size <= 1:
        return False
    if not torch.distributed.is_available():
        raise RuntimeError("WORLD_SIZE > 1 but torch.distributed is unavailable")
    if not torch.cuda.is_available():
        raise RuntimeError("torchrun cross-asset execution requires CUDA and the NCCL backend")
    local_rank = _distributed_local_rank()
    device_count = int(torch.cuda.device_count())
    if local_rank < 0 or local_rank >= device_count:
        raise RuntimeError(
            f"LOCAL_RANK={local_rank} is unavailable; visible CUDA devices={device_count}"
        )
    torch.cuda.set_device(local_rank)
    if _distributed_ready():
        backend = str(torch.distributed.get_backend()).lower()
        if backend != "nccl":
            raise RuntimeError(f"Cross-asset torchrun requires NCCL, but the initialized backend is {backend!r}")
        return False
    os.environ.setdefault("TORCH_NCCL_ASYNC_ERROR_HANDLING", "1")
    torch.distributed.init_process_group(backend="nccl")
    atexit.register(_destroy_process_group)
    return True


def _rank_local_device(
    config: ExperimentConfig,
    override: str | None,
) -> torch.device:
    world_size = _distributed_world_size()
    if world_size <= 1:
        return _device_from_config(config, override)
    requested = str(override or config.environment.device or "cuda").strip().lower()
    if not requested.startswith("cuda"):
        raise RuntimeError(
            f"torchrun cross-asset execution requires a CUDA device, got {requested!r}"
        )
    local_rank = _distributed_local_rank()
    torch.cuda.set_device(local_rank)
    return torch.device(f"cuda:{local_rank}")


def _configure_compile_threads(config: ExperimentConfig, world_size: int) -> int | None:
    host_budget = getattr(config.environment, "torch_compile_threads", None)
    if host_budget is None:
        return None
    per_rank = max(1, int(host_budget) // max(1, int(world_size)))
    os.environ["STOCKAGENT_TORCH_COMPILE_THREADS"] = str(per_rank)
    os.environ["TORCHINDUCTOR_COMPILE_THREADS"] = str(per_rank)
    try:
        import torch._inductor.config as inductor_config

        inductor_config.compile_threads = int(per_rank)
    except Exception:
        pass
    return per_rank


def _configure_rank_cpu_runtime(world_size: int) -> dict[str, int]:
    """Give each local GPU rank disjoint physical cores and matched thread pools."""
    try:
        affinity = sorted(int(cpu) for cpu in os.sched_getaffinity(0))
    except (AttributeError, OSError):
        affinity = list(range(max(1, int(os.cpu_count() or 1))))
    core_to_cpus: dict[tuple[int, int], list[int]] = {}
    for cpu in affinity:
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        try:
            package = int((topology / "physical_package_id").read_text().strip())
            core = int((topology / "core_id").read_text().strip())
        except (OSError, ValueError):
            package, core = 0, cpu
        core_to_cpus.setdefault((package, core), []).append(cpu)

    local_world_size = max(1, int(os.environ.get("LOCAL_WORLD_SIZE", world_size)))
    local_rank = max(0, int(os.environ.get("LOCAL_RANK", "0")))
    ordered_cores = sorted(core_to_cpus)
    cores_per_rank = math.ceil(len(ordered_cores) / local_world_size)
    assigned_cores = ordered_cores[
        local_rank * cores_per_rank : (local_rank + 1) * cores_per_rank
    ]
    assigned_cpus = sorted(
        cpu
        for core in assigned_cores
        for cpu in core_to_cpus[core]
    )
    if assigned_cpus and local_world_size > 1:
        try:
            os.sched_setaffinity(0, assigned_cpus)
        except (AttributeError, OSError):
            pass

    default_threads = max(1, len(assigned_cores) or len(ordered_cores))
    try:
        threads = max(
            1,
            int(os.environ.get("STOCKAGENT_CROSS_ASSET_CPU_THREADS", default_threads)),
        )
    except ValueError:
        threads = default_threads
    for name in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "POLARS_MAX_THREADS",
    ):
        os.environ[name] = str(threads)
    torch.set_num_threads(threads)
    try:
        torch.set_num_interop_threads(min(4, threads))
    except RuntimeError:
        pass
    try:
        import pyarrow as pa

        pa.set_cpu_count(threads)
    except Exception:
        pass
    return {
        "threads": int(threads),
        "physical_cores": int(len(assigned_cores)),
        "logical_cpus": int(len(assigned_cpus) or len(affinity)),
    }


def _configured_fold_ids(
    folds: list[WalkForwardFold],
    training_output_dir: Path,
) -> list[int]:
    fold_ids = sorted(int(fold.fold_id) for fold in folds)
    if len(fold_ids) != len(set(fold_ids)):
        raise ValueError(f"Configured walk-forward folds contain duplicate ids: {fold_ids}")
    missing = [
        fold_id
        for fold_id in fold_ids
        if not (_fold_dir(training_output_dir, fold_id) / "checkpoint_best.pt").is_file()
    ]
    if missing:
        missing_paths = [
            str(_fold_dir(training_output_dir, fold_id) / "checkpoint_best.pt")
            for fold_id in missing
        ]
        raise FileNotFoundError(
            "Cross-asset requires canonical checkpoint_best.pt for every configured fold; "
            f"missing_fold_ids={missing}, missing_paths={missing_paths}"
        )
    if not fold_ids:
        raise ValueError("No configured walk-forward folds were available")
    return fold_ids


def _fold_row_weights(
    fold_ids: list[int],
    folds_by_id: dict[int, WalkForwardFold],
    panel: PanelData,
    *,
    lookback: int,
    execution_mode: str = "naive",
    lookback_context: str = "split_only",
    short_capacity_limit_enabled: bool = True,
    tw_corporate_action_mode: str = "avoid",
) -> dict[int, int]:
    resolved_lookback = int(lookback)
    if resolved_lookback <= 0:
        raise ValueError(f"lookback must be positive, got {lookback!r}")
    if not hasattr(panel, "tradable_mask"):
        # Preserve the lightweight dates-only scheduling contract used by
        # callers that do not own a materialized PanelData instance. Real
        # runs use the dataset-backed estimator below so execution masks and
        # panel-history target ownership remain exact.
        phase_feature_lag = (
            0 if str(execution_mode).strip().lower() == "naive" else 1
        )
        panel_dates = np.asarray(panel.dates, dtype="datetime64[D]")
        weights: dict[int, int] = {}
        for fold_id in fold_ids:
            fold = folds_by_id[int(fold_id)]
            indices = np.asarray(fold.test_indices, dtype=np.int64)
            if indices.size == 0:
                weights[int(fold_id)] = 0
                continue
            years = panel_dates[indices].astype("datetime64[Y]")
            first_year_indices = indices[years == years.min()]
            history_origin = (
                0
                if str(lookback_context).strip().lower() == "panel_history"
                else int(first_year_indices[0])
            )
            min_valid_index = (
                history_origin + resolved_lookback - 1 + phase_feature_lag
            )
            weights[int(fold_id)] = int(
                np.count_nonzero(first_year_indices >= min_valid_index)
            )
        return weights
    return {
        int(fold_id): int(
            _estimated_first_test_year_explain_rows(
                folds_by_id[int(fold_id)],
                panel,
                lookback=resolved_lookback,
                max_rows=0,
                execution_mode=execution_mode,
                lookback_context=lookback_context,
                short_capacity_limit_enabled=short_capacity_limit_enabled,
                tw_corporate_action_mode=tw_corporate_action_mode,
            )
        )
        for fold_id in fold_ids
    }


def _lpt_fold_assignments(
    fold_ids: list[int],
    row_weights: dict[int, int],
    world_size: int,
) -> tuple[list[list[int]], list[int]]:
    rank_count = max(1, int(world_size))
    assignments: list[list[int]] = [[] for _ in range(rank_count)]
    loads = [0 for _ in range(rank_count)]
    weighted_folds = [
        (int(fold_id), max(0, int(row_weights.get(int(fold_id), 0))))
        for fold_id in fold_ids
    ]
    for fold_id, weight in sorted(weighted_folds, key=lambda item: (-item[1], item[0])):
        target_rank = min(range(rank_count), key=lambda rank: (loads[rank], rank))
        assignments[target_rank].append(fold_id)
        loads[target_rank] += weight
    return assignments, loads


def _gather_round_payloads(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not _distributed_ready():
        return [payload]
    gathered: list[dict[str, Any] | None] = [None] * _distributed_world_size()
    torch.distributed.all_gather_object(gathered, payload)
    return [item for item in gathered if item is not None]


def _execute_fold_rounds(
    assignments: list[list[int]],
    *,
    rank: int,
    run_fold: Callable[[int], dict[str, Any]],
    gather_payloads: Callable[[dict[str, Any]], list[dict[str, Any]]] = _gather_round_payloads,
) -> list[dict[str, Any]]:
    if rank < 0 or rank >= len(assignments):
        raise ValueError(f"rank={rank} is outside assignments with world_size={len(assignments)}")
    max_rounds = max((len(rank_folds) for rank_folds in assignments), default=0)
    all_timings: list[dict[str, Any]] = []
    for round_index in range(max_rounds):
        fold_id = assignments[rank][round_index] if round_index < len(assignments[rank]) else None
        payload: dict[str, Any] = {
            "rank": int(rank),
            "round": int(round_index),
            "fold_id": int(fold_id) if fold_id is not None else None,
            "timing": None,
            "error": None,
        }
        if fold_id is not None:
            try:
                payload["timing"] = run_fold(int(fold_id))
            except BaseException as exc:
                payload["error"] = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": "".join(traceback.format_exception(exc))[-12000:],
                }
        # Every live rank enters exactly one collective per round, including an
        # idle rank and a rank whose local fold raised. This prevents the common
        # failure mode where healthy ranks wait forever at a final gather.
        round_payloads = gather_payloads(payload)
        errors = [item for item in round_payloads if item.get("error") is not None]
        if errors:
            details = "; ".join(
                f"rank={item.get('rank')} fold={item.get('fold_id')} "
                f"{item['error'].get('type')}: {item['error'].get('message')}"
                for item in errors
            )
            raise RuntimeError(f"Cross-asset distributed fold round failed: {details}")
        all_timings.extend(
            item["timing"]
            for item in round_payloads
            if isinstance(item.get("timing"), dict)
        )
    unique: dict[int, dict[str, Any]] = {}
    for timing in all_timings:
        fold_id = int(timing["fold_id"])
        if fold_id in unique:
            raise RuntimeError(f"Fold {fold_id} produced duplicate distributed timing payloads")
        unique[fold_id] = timing
    return [unique[fold_id] for fold_id in sorted(unique)]


def _run_fold(
    *,
    args: argparse.Namespace,
    config: ExperimentConfig,
    panel: PanelData,
    fold: WalkForwardFold,
    training_output_dir: Path,
    cross_asset_output_root: Path,
    device: torch.device,
    settings: CrossAssetTransmissionSettings,
    amp_dtype: torch.dtype | None,
    amp_dtype_name: str,
) -> dict[str, Any]:
    fold_id = int(fold.fold_id)
    checkpoint_path = _fold_dir(training_output_dir, fold_id) / "checkpoint_best.pt"
    fold_panel = _align_panel_to_checkpoint_universe(
        panel,
        training_output_dir,
        fold_id,
        checkpoint_path,
    )
    model, checkpoint_info = load_model_from_checkpoint(
        config,
        fold_panel,
        checkpoint_path,
        device,
        strict=bool(args.strict),
    )
    dataset_kwargs: dict[str, Any] = {}
    trading_config = getattr(config, "trading", None)
    if trading_config is not None:
        dataset_kwargs.update(
            execution_mode=getattr(trading_config, "execution_mode", "naive"),
            short_capacity_limit_enabled=getattr(
                trading_config,
                "tw_short_capacity_limit_enabled",
                True,
            ),
            tw_corporate_action_mode=getattr(
                trading_config,
                "tw_corporate_action_mode",
                "avoid",
            ),
        )
    walk_forward_config = getattr(config, "walk_forward", None)
    if walk_forward_config is not None:
        dataset_kwargs["lookback_context"] = getattr(
            walk_forward_config,
            "lookback_context",
            "split_only",
        )
    dataset = _first_test_year_dataset(
        fold_panel,
        fold,
        config.training.lookback,
        **dataset_kwargs,
    )
    split_rows = len(dataset)
    batch_source = _sample_dataset_source(
        dataset,
        0,
        _FIXED_SAMPLE_METHOD,
    )
    date_indices = batch_source.date_indices
    dates = [
        str(np.datetime_as_string(fold_panel.dates[int(idx)], unit="D"))
        for idx in date_indices
    ]
    years = sorted({int(date[:4]) for date in dates})
    expected_year = min(int(year) for year in fold.test_years)
    if years != [expected_year]:
        raise RuntimeError(
            f"Fold {fold_id} violated the first-test-calendar-year contract: "
            f"expected={expected_year}, observed={years}"
        )
    destination = cross_asset_output_root / f"fold_{fold_id:02d}_{_FIXED_SPLIT}"
    started = time.perf_counter()
    try:
        with _cuda_autocast_context(device, amp_dtype):
            summary = abstract_cross_asset_transmission(
                model,
                batch_source,
                feature_names=fold_panel.feature_names,
                symbols=fold_panel.symbols,
                dates=dates,
                output_dir=destination,
                settings=settings,
                device=device,
            )
    finally:
        del model, batch_source, dataset
        _clear_explainability_runtime_cache()
    elapsed = float(time.perf_counter() - started)
    rank = _distributed_rank()
    timing = {
        "fold_id": fold_id,
        "split": _FIXED_SPLIT,
        "test_year": expected_year,
        "checkpoint": str(checkpoint_path),
        "checkpoint_epoch": checkpoint_info.get("checkpoint_epoch"),
        "first_test_year_only": True,
        "exhaustive_dates": True,
        "amp_dtype": str(amp_dtype_name),
        "sample_rows": len(dates),
        "split_rows": split_rows,
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
        "sources": int(summary.get("sources", 0)),
        "targets": int(summary.get("targets", 0)),
        "elapsed_s": elapsed,
        "module_output": str(destination / "abstract_cross_asset_transmission"),
    }
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "cross_asset_runner_timing.json").write_text(
        json.dumps(timing, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(
        f"[rank {rank} cross-asset fold {fold_id}] "
        f"year={expected_year} rows={len(dates)} sources={timing['sources']} "
        f"targets={timing['targets']} elapsed={elapsed:.3f}s "
        f"output={timing['module_output']}"
    )
    return timing


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run cross-asset explainability for every configured fold, using every valid "
            "date in each fold's first test calendar year."
        )
    )
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output-dir", default=None, type=Path, help="Training/checkpoint artifact root.")
    parser.add_argument("--cross-asset-output-dir", default=None, type=Path)
    parser.add_argument("--device", default=None)
    parser.add_argument("--max-sources", "--cross-asset-max-sources", dest="max_sources", default=0, type=int)
    parser.add_argument("--max-targets", "--cross-asset-max-targets", dest="max_targets", default=0, type=int)
    parser.add_argument("--source-chunk-size", default=128, type=int)
    parser.add_argument("--row-chunk-size", default=0, type=int)
    parser.add_argument("--max-repeated-rows", default=4096, type=int)
    parser.add_argument("--counterfactual-compile", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--perturb-scale", default=1.0, type=float)
    parser.add_argument("--shocks", default=",".join(DEFAULT_SHOCKS))
    parser.add_argument("--attention-flow", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--attention-capture-rows", default=0, type=int)
    parser.add_argument("--attention-capture-max-elements", default=2_000_000, type=int)
    parser.add_argument("--validated-transmission", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--role-embedding", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--graph-backend", default="cugraph", choices=("auto", "polars", "cugraph"))
    parser.add_argument("--graph-benchmark-min-edges", default=1_000_000, type=int)
    parser.add_argument("--graph-explainability", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--strict", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--progress", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args(argv)


def _jsonable_settings(args: argparse.Namespace) -> dict[str, Any]:
    return {
        key: str(value) if isinstance(value, Path) else value
        for key, value in vars(args).items()
    }


def _write_manifest_atomic(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def _run(args: argparse.Namespace) -> None:
    rank = _distributed_rank()
    world_size = _distributed_world_size()
    cpu_runtime = _configure_rank_cpu_runtime(world_size)
    config = load_config(args.config)
    compile_threads = _configure_compile_threads(config, world_size)
    training_output_dir = Path(args.output_dir or config.runner.output_dir)
    cross_asset_output_root = Path(
        args.cross_asset_output_dir or _default_output_root(training_output_dir)
    )
    panel = _build_panel(config)
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year,
    )
    selected_fold_ids = _configured_fold_ids(folds, training_output_dir)
    folds_by_id = {int(fold.fold_id): fold for fold in folds}
    row_weights = _fold_row_weights(
        selected_fold_ids,
        folds_by_id,
        panel,
        lookback=config.training.lookback,
        execution_mode=config.trading.execution_mode,
        lookback_context=config.walk_forward.lookback_context,
        short_capacity_limit_enabled=config.trading.tw_short_capacity_limit_enabled,
        tw_corporate_action_mode=config.trading.tw_corporate_action_mode,
    )
    assignments, estimated_loads = _lpt_fold_assignments(
        selected_fold_ids,
        row_weights,
        world_size,
    )
    device = _rank_local_device(config, args.device)
    amp_dtype, configured_amp_dtype, actual_amp_dtype = _resolve_cuda_autocast_dtype(
        config,
        device,
    )
    if device.type == "cuda":
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
    settings = _settings(
        args,
        progress_enabled=bool(args.progress and rank == 0),
    )
    if rank == 0:
        print(
            "[cross-asset] fixed coverage profile: "
            f"folds={selected_fold_ids}, split={_FIXED_SPLIT}, "
            "test_years=first_calendar_year_only, dates=all_valid, "
            f"sources={'all' if settings.max_sources <= 0 else settings.max_sources}, "
            f"targets={'all' if settings.max_targets <= 0 else settings.max_targets}, "
            f"world_size={world_size}, assignments={assignments}, "
            f"estimated_rows={estimated_loads}, compile_threads_per_rank={compile_threads}, "
            f"cpu_threads_per_rank={cpu_runtime['threads']}, "
            f"amp={actual_amp_dtype}, "
            f"output={cross_asset_output_root}"
        )
    print(f"[rank {rank}] cross-asset folds={assignments[rank]} device={device}")

    started = time.perf_counter()

    def run_local_fold(fold_id: int) -> dict[str, Any]:
        return _run_fold(
            args=args,
            config=config,
            panel=panel,
            fold=folds_by_id[int(fold_id)],
            training_output_dir=training_output_dir,
            cross_asset_output_root=cross_asset_output_root,
            device=device,
            settings=settings,
            amp_dtype=amp_dtype,
            amp_dtype_name=actual_amp_dtype,
        )

    local_progress = tqdm(
        total=len(assignments[rank]),
        desc=f"Rank {rank} cross-asset folds",
        unit="fold",
        disable=not bool(args.progress and rank == 0),
    )

    def run_local_fold_with_progress(fold_id: int) -> dict[str, Any]:
        timing = run_local_fold(fold_id)
        local_progress.update(1)
        return timing

    try:
        timings = _execute_fold_rounds(
            assignments,
            rank=rank,
            run_fold=run_local_fold_with_progress,
        )
    finally:
        local_progress.close()

    observed_fold_ids = [int(timing["fold_id"]) for timing in timings]
    if observed_fold_ids != selected_fold_ids:
        raise RuntimeError(
            "Distributed cross-asset timings did not cover every configured fold exactly once: "
            f"expected={selected_fold_ids}, observed={observed_fold_ids}"
        )
    if rank != 0:
        return
    gpu_runtime = []
    if torch.cuda.is_available():
        for index in range(torch.cuda.device_count()):
            properties = torch.cuda.get_device_properties(index)
            gpu_runtime.append(
                {
                    "index": int(index),
                    "name": str(properties.name),
                    "total_memory_gb": float(properties.total_memory) / (1024**3),
                    "compute_capability": [int(properties.major), int(properties.minor)],
                }
            )
    manifest = {
        "config": str(args.config),
        "training_output_dir": str(training_output_dir),
        "cross_asset_output_root": str(cross_asset_output_root),
        "coverage_contract": {
            "folds": "all_configured",
            "checkpoint": "canonical_checkpoint_best",
            "split": _FIXED_SPLIT,
            "test_years": "first_calendar_year_only",
            "dates": "all_valid_after_in_split_lookback",
        },
        "artifact_contract": {
            "compact_artifacts": True,
            "numeric_edges": "canonical_parquet_with_lookup",
            "dense_matrix_duplicates": False,
            "graph_nodes": "all",
            "graph_edges": "all",
            "top_k_graph_figures": False,
        },
        "folds": selected_fold_ids,
        "world_size": world_size,
        "fold_assignments": assignments,
        "estimated_row_weights": row_weights,
        "estimated_rank_loads": estimated_loads,
        "compile_threads_per_rank": compile_threads,
        "cpu_runtime_per_rank": cpu_runtime,
        "configured_amp_dtype": configured_amp_dtype,
        "amp_dtype": actual_amp_dtype,
        "runtime": {
            "torch": str(torch.__version__),
            "cuda": str(torch.version.cuda),
            "gpus": gpu_runtime,
        },
        "settings": _jsonable_settings(args),
        "fold_timings": timings,
        "total_elapsed_s": float(time.perf_counter() - started),
    }
    manifest_path = cross_asset_output_root / "cross_asset_run_manifest.json"
    _write_manifest_atomic(manifest_path, manifest)
    print(f"cross-asset manifest: {manifest_path}")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    initialized_here = _initialize_process_group()
    try:
        _run(args)
    finally:
        if initialized_here:
            _destroy_process_group()


if __name__ == "__main__":
    main()
