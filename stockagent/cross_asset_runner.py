from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import torch
from tqdm.auto import tqdm

from stockagent.config import ExperimentConfig, load_config
from stockagent.data.panel import PanelData, build_panel
from stockagent.data.walkforward import WalkForwardFold, build_expanding_year_folds
from stockagent.explainability import (
    _align_panel_to_checkpoint_universe,
    _available_checkpoint_folds,
    _clear_explainability_runtime_cache,
    _dataset_for_split,
    _device_from_config,
    _fold_dir,
    _sample_dataset,
    load_model_from_checkpoint,
)
from stockagent.explainability_cross_asset import (
    DEFAULT_SHOCKS,
    CrossAssetTransmissionSettings,
    abstract_cross_asset_transmission,
)


DEFAULT_CROSS_ASSET_ROOT = Path("artifacts/cross_asset")


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
        panel_start_date=config.data.panel_start_date,
    )


def _default_output_root(training_output_dir: Path) -> Path:
    return DEFAULT_CROSS_ASSET_ROOT / Path(training_output_dir).name


def _settings(args: argparse.Namespace) -> CrossAssetTransmissionSettings:
    return CrossAssetTransmissionSettings(
        enabled=True,
        progress_enabled=bool(args.progress),
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
        graph_betweenness_max_vertices=max(0, int(args.graph_betweenness_max_vertices)),
        graph_plot_max_nodes=max(5, int(args.graph_plot_max_nodes)),
    )


def _fold_ids(
    folds: list[WalkForwardFold],
    training_output_dir: Path,
    fold_id: int | None,
    checkpoint: Path | None,
) -> list[int]:
    if checkpoint is not None and fold_id is None:
        raise ValueError("--checkpoint requires --fold so its walk-forward split is unambiguous")
    if fold_id is not None:
        wanted = int(fold_id)
        if wanted not in {int(fold.fold_id) for fold in folds}:
            raise ValueError(f"Fold {wanted} is not present in the configured walk-forward folds")
        path = Path(checkpoint) if checkpoint is not None else _fold_dir(training_output_dir, wanted) / "checkpoint_best.pt"
        if not path.is_file():
            raise FileNotFoundError(path)
        return [wanted]
    available = _available_checkpoint_folds(folds, training_output_dir)
    if not available:
        raise FileNotFoundError(f"No fold checkpoint_best.pt found under {training_output_dir}")
    return available


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
) -> dict[str, Any]:
    fold_id = int(fold.fold_id)
    split = str(args.split).strip().lower()
    checkpoint_path = (
        Path(args.checkpoint)
        if args.checkpoint is not None
        else _fold_dir(training_output_dir, fold_id) / "checkpoint_best.pt"
    )
    fold_panel = _align_panel_to_checkpoint_universe(panel, training_output_dir, fold_id, checkpoint_path)
    model, checkpoint_info = load_model_from_checkpoint(
        config,
        fold_panel,
        checkpoint_path,
        device,
        strict=bool(args.strict),
    )
    dataset = _dataset_for_split(
        fold_panel,
        fold,
        split,
        config.training.lookback,
        first_test_year_only=bool(args.first_test_year_only),
    )
    split_rows = len(dataset)
    batch, date_indices = _sample_dataset(
        dataset,
        int(args.max_rows),
        str(args.sample_method),
        progress_enabled=bool(args.progress),
    )
    dates = [str(np.datetime_as_string(fold_panel.dates[int(idx)], unit="D")) for idx in date_indices]
    destination = cross_asset_output_root / f"fold_{fold_id:02d}_{split}"
    started = time.perf_counter()
    try:
        summary = abstract_cross_asset_transmission(
            model,
            batch,
            feature_names=fold_panel.feature_names,
            symbols=fold_panel.symbols,
            dates=dates,
            output_dir=destination,
            settings=settings,
            device=device,
        )
    finally:
        del model, batch, dataset
        _clear_explainability_runtime_cache()
    elapsed = float(time.perf_counter() - started)
    timing = {
        "fold_id": fold_id,
        "split": split,
        "checkpoint": str(checkpoint_path),
        "checkpoint_epoch": checkpoint_info.get("checkpoint_epoch"),
        "first_test_year_only": bool(args.first_test_year_only),
        "sample_method": str(args.sample_method),
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
        f"[Cross-asset fold {fold_id}] rows={len(dates)} sources={timing['sources']} "
        f"targets={timing['targets']} elapsed={elapsed:.3f}s output={timing['module_output']}"
    )
    return timing


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run only cross-asset explainability in an independent artifact area."
    )
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output-dir", default=None, type=Path, help="Training/checkpoint artifact root.")
    parser.add_argument("--cross-asset-output-dir", default=None, type=Path)
    parser.add_argument("--fold", default=None, type=int)
    parser.add_argument("--checkpoint", default=None, type=Path)
    parser.add_argument("--split", default="test", choices=("train", "val", "test"))
    parser.add_argument("--device", default=None)
    parser.add_argument("--max-rows", default=0, type=int, help="0 processes every selected split date.")
    parser.add_argument("--sample-method", default="even", choices=("even", "first", "last"))
    parser.add_argument(
        "--first-test-year-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="For test splits, process only the first test year (default: true).",
    )
    parser.add_argument("--max-sources", "--cross-asset-max-sources", dest="max_sources", default=0, type=int)
    parser.add_argument("--max-targets", "--cross-asset-max-targets", dest="max_targets", default=0, type=int)
    parser.add_argument("--source-chunk-size", default=16, type=int)
    parser.add_argument("--row-chunk-size", default=0, type=int)
    parser.add_argument("--max-repeated-rows", default=48, type=int)
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
    parser.add_argument("--graph-betweenness-max-vertices", default=512, type=int)
    parser.add_argument("--graph-plot-max-nodes", default=80, type=int)
    parser.add_argument("--strict", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--progress", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
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
    )
    selected_fold_ids = _fold_ids(folds, training_output_dir, args.fold, args.checkpoint)
    folds_by_id = {int(fold.fold_id): fold for fold in folds}
    device = _device_from_config(config, args.device)
    if device.type == "cuda":
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
    settings = _settings(args)
    print(
        "[cross-asset] standalone profile: "
        f"folds={selected_fold_ids}, split={args.split}, "
        f"dates={'all' if int(args.max_rows) <= 0 else int(args.max_rows)}, "
        f"first_test_year_only={bool(args.first_test_year_only)}, "
        f"sources={'all' if settings.max_sources <= 0 else settings.max_sources}, "
        f"targets={'all' if settings.max_targets <= 0 else settings.max_targets}, "
        f"output={cross_asset_output_root}"
    )
    timings: list[dict[str, Any]] = []
    started = time.perf_counter()
    for fold_id in tqdm(
        selected_fold_ids,
        desc="Cross-asset folds",
        unit="fold",
        disable=not bool(args.progress),
    ):
        timings.append(
            _run_fold(
                args=args,
                config=config,
                panel=panel,
                fold=folds_by_id[fold_id],
                training_output_dir=training_output_dir,
                cross_asset_output_root=cross_asset_output_root,
                device=device,
                settings=settings,
            )
        )
    manifest = {
        "config": str(args.config),
        "training_output_dir": str(training_output_dir),
        "cross_asset_output_root": str(cross_asset_output_root),
        "folds": selected_fold_ids,
        "settings": vars(args),
        "fold_timings": timings,
        "total_elapsed_s": float(time.perf_counter() - started),
    }
    manifest["settings"] = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in manifest["settings"].items()
    }
    cross_asset_output_root.mkdir(parents=True, exist_ok=True)
    (cross_asset_output_root / "cross_asset_run_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"cross-asset manifest: {cross_asset_output_root / 'cross_asset_run_manifest.json'}")


if __name__ == "__main__":
    main()
