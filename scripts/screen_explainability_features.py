from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import torch
from tqdm.auto import tqdm

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent import explainability as explain


def _build_panel_and_folds(config: Any):
    panel = build_panel(
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
        external_data_required=(
            config.data.use_tw_public_features or config.data.use_tw_public_rules
        ),
        feature_include=config.data.feature_include,
        feature_exclude=config.data.feature_exclude,
        feature_zero_fill=config.data.feature_zero_fill,
        panel_start_date=config.data.panel_start_date,
    )
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year,
    )
    return panel, folds


def _importance_frame(values: np.ndarray, feature_names: list[str], value_name: str) -> pl.DataFrame:
    values = np.nan_to_num(np.asarray(values, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    total = float(values.sum())
    shares = values / total if total > 0.0 else np.zeros_like(values)
    return (
        pl.DataFrame(
            {
                "feature": feature_names,
                value_name: values,
                "share": shares,
            }
        )
        .with_columns(
            pl.col("share").rank(method="ordinal", descending=True).cast(pl.Int64).alias("rank")
        )
        .sort(value_name, descending=True)
    )


def _screen_source(
    model: torch.nn.Module,
    source: explain.ExplainDatasetBatchSource,
    *,
    feature_names: list[str],
    device: torch.device,
    amp_dtype: str,
    row_chunk_size: int,
    ig_steps: int,
    ig_batch_size: int,
    progress_enabled: bool,
    rank: int,
    fold_id: int,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    started = time.perf_counter()
    lookback = int(source.lookback)
    feature_count = len(feature_names)
    grad_sum = np.zeros((lookback, feature_count), dtype=np.float64)
    ig_sum = np.zeros((lookback, feature_count), dtype=np.float64)
    denominator = 0
    row_chunk_size = max(1, int(row_chunk_size))
    progress = tqdm(
        total=len(source),
        desc=f"rank {rank} fold {fold_id:02d} feature importance",
        unit="date",
        position=rank,
        leave=True,
        disable=not progress_enabled,
    )
    for start in range(0, len(source), row_chunk_size):
        end = min(len(source), start + row_chunk_size)
        batch = explain._move_batch(source.materialize(start, end), device)
        x = torch.nan_to_num(batch["x"].float(), nan=0.0, posinf=0.0, neginf=0.0)
        mask = batch["tradable_mask"].to(device=device, dtype=torch.bool)
        with explain._explain_autocast_context(device, amp_dtype):
            with torch.no_grad():
                weights, _, _ = explain._forward_outputs(model, x, mask, return_aux=False)
            selected, direction, gross_weight = explain._selection_from_weights(weights.detach(), mask)
            grad_attr = explain._gradient_x_input_attribution(
                model,
                x,
                mask,
                selected,
                direction,
                gross_weight,
            )
            ig_attr = explain._integrated_gradients_attribution(
                model,
                x,
                mask,
                selected,
                direction,
                gross_weight,
                steps=ig_steps,
                batch_size=ig_batch_size,
                progress_enabled=False,
            )
        grad_sum += grad_attr.abs().sum(dim=(0, 2)).double().cpu().numpy()
        ig_sum += ig_attr.abs().sum(dim=(0, 2)).double().cpu().numpy()
        denominator += int(x.size(0)) * int(x.size(2))
        del batch, x, mask, weights, selected, direction, gross_weight, grad_attr, ig_attr
        progress.update(end - start)
    progress.close()
    scale = 1.0 / max(1, denominator)
    grad_feature = grad_sum.sum(axis=0) * scale
    ig_feature = ig_sum.sum(axis=0) * scale
    elapsed = float(time.perf_counter() - started)
    timing = {
        "fold_id": int(fold_id),
        "rows": int(len(source)),
        "symbols": int(source.num_symbols),
        "features": int(source.num_features),
        "lookback": int(source.lookback),
        "row_chunk_size": int(row_chunk_size),
        "ig_steps": int(ig_steps),
        "ig_batch_size": int(ig_batch_size),
        "amp_dtype": str(amp_dtype),
        "elapsed_s": elapsed,
        "dates_per_s": float(len(source)) / max(elapsed, 1e-9),
    }
    return (
        _importance_frame(grad_feature, feature_names, "grad_x_input_abs"),
        _importance_frame(ig_feature, feature_names, "integrated_gradients_abs"),
        timing,
    )


def _valid_reuse_fold(
    fold_id: int,
    *,
    expected_rows: int,
    explainability_root: Path,
    ig_steps: int,
) -> bool:
    fold_dir = explainability_root / f"fold_{fold_id:02d}_test"
    summary_path = fold_dir / "summary.json"
    gradient_path = fold_dir / "feature_importance_gradient.csv"
    ig_path = fold_dir / "feature_importance_integrated_gradients.csv"
    if not summary_path.is_file() or not gradient_path.is_file() or not ig_path.is_file():
        return False
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        metadata = summary.get("metadata", {}) or {}
        if metadata.get("first_test_year_only") is not True:
            return False
        complete_coverage = (
            float(metadata.get("sampled_date_coverage", 0.0)) >= 1.0 - 1e-12
            or (
                int(metadata.get("sample_rows", -1)) > 0
                and int(metadata.get("sample_rows", -1)) == int(metadata.get("split_rows", -2))
            )
            # CrossSectionalDataset currently reports one fewer row than the
            # analytical N-lookback+1 estimate. Accept that known boundary
            # convention while still rejecting genuinely sampled runs.
            or int(summary.get("rows", -1)) in {int(expected_rows), int(expected_rows) - 1}
        )
        return (
            complete_coverage
            and int(summary.get("ig_steps", -1)) == int(ig_steps)
            and pl.read_csv(gradient_path, n_rows=0).schema.get("share") is not None
            and pl.read_csv(ig_path, n_rows=0).schema.get("share") is not None
        )
    except Exception:
        return False


def _method_path(
    fold_id: int,
    filename: str,
    *,
    screening_root: Path,
    explainability_root: Path,
) -> Path:
    screened = screening_root / f"fold_{fold_id:02d}_test" / filename
    if screened.is_file():
        return screened
    reused = explainability_root / f"fold_{fold_id:02d}_test" / filename
    if reused.is_file():
        return reused
    raise FileNotFoundError(f"Missing Fold {fold_id} feature importance: {filename}")


def _aggregate(
    fold_ids: list[int],
    feature_names: list[str],
    *,
    screening_root: Path,
    explainability_root: Path,
) -> Path:
    method_specs = (
        ("gradient", "feature_importance_gradient.csv", "grad_x_input_abs"),
        ("ig", "feature_importance_integrated_gradients.csv", "integrated_gradients_abs"),
    )
    aggregate: pl.DataFrame | None = None
    for prefix, filename, value_col in method_specs:
        frames = []
        for fold_id in fold_ids:
            path = _method_path(
                fold_id,
                filename,
                screening_root=screening_root,
                explainability_root=explainability_root,
            )
            frames.append(
                pl.read_csv(path)
                .select("feature", value_col, "share")
                .with_columns(
                    pl.lit(fold_id).cast(pl.Int64).alias("fold"),
                    pl.col("share").rank(method="ordinal", descending=True).cast(pl.Int64).alias("rank"),
                )
            )
        all_folds = pl.concat(frames)
        all_folds.write_csv(screening_root / f"all_folds_{prefix}.csv")
        summary = all_folds.group_by("feature").agg(
            pl.col("fold").n_unique().alias(f"{prefix}_folds"),
            pl.col(value_col).max().alias(f"{prefix}_max_value"),
            pl.col("share").mean().alias(f"{prefix}_mean_share"),
            pl.col("share").median().alias(f"{prefix}_median_share"),
            pl.col("share").max().alias(f"{prefix}_max_share"),
            pl.col("rank").min().alias(f"{prefix}_best_rank"),
            (pl.col(value_col) > 0.0).sum().alias(f"{prefix}_nonzero_folds"),
        )
        aggregate = summary if aggregate is None else aggregate.join(summary, on="feature", how="full", coalesce=True)
    if aggregate is None:
        raise RuntimeError("No feature-importance rows were available to aggregate.")
    feature_count = len(feature_names)
    expected_folds = len(fold_ids)
    zero_in_all_folds = (
        (pl.col("gradient_folds") == expected_folds)
        & (pl.col("ig_folds") == expected_folds)
        & (pl.col("gradient_max_value") == 0.0)
        & (pl.col("ig_max_value") == 0.0)
    )
    aggregate = (
        aggregate.with_columns(
            zero_in_all_folds.alias("zero_in_all_folds"),
            zero_in_all_folds.alias("disable_candidate"),
            pl.max_horizontal("gradient_max_share", "ig_max_share").alias("max_share_any_method_fold"),
        )
        .with_columns(
            (pl.col("max_share_any_method_fold") * feature_count).alias("max_fraction_of_uniform_share")
        )
        .sort(
            ["disable_candidate", "max_share_any_method_fold"],
            descending=[True, False],
        )
    )
    # Fold count depends on the configured panel horizon.  Encoding a fixed
    # "21folds" suffix made shorter, otherwise valid experiments look as if
    # they had coverage they did not actually compute.
    output_path = screening_root / f"feature_importance_{expected_folds}folds.csv"
    aggregate.write_csv(output_path)
    per_fold_rows: dict[str, int] = {}
    for fold_id in fold_ids:
        screened_summary = screening_root / f"fold_{fold_id:02d}_test" / "summary.json"
        reused_summary = explainability_root / f"fold_{fold_id:02d}_test" / "summary.json"
        summary_path = screened_summary if screened_summary.is_file() else reused_summary
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        metadata = summary.get("metadata", {}) or {}
        if metadata.get("first_test_year_only", summary.get("first_test_year_only")) is not True:
            raise ValueError(f"Fold {fold_id} summary is not restricted to the first test year: {summary_path}")
        rows = summary.get("rows", metadata.get("sample_rows", summary.get("sample_rows")))
        per_fold_rows[str(fold_id)] = int(rows)
    manifest = {
        "folds": fold_ids,
        "feature_count": feature_count,
        "methods": [spec[0] for spec in method_specs],
        "coverage": "first calendar year of each fold's test split only",
        "first_test_year_only": True,
        "total_rows": sum(per_fold_rows.values()),
        "per_fold_rows": per_fold_rows,
        "selection_rule": (
            "keep a feature when Gradient x Input or IG is non-zero in at least one fold; "
            "disable only exact zero across both methods and every fold"
        ),
        "zero_in_all_folds": int(aggregate.filter(pl.col("zero_in_all_folds")).height),
        "disable_candidates": int(aggregate.filter(pl.col("disable_candidate")).height),
        "aggregate_file": output_path.name,
    }
    (screening_root / "feature_screening_summary.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Screen feature importance across every checkpoint fold using the complete first "
            "calendar year of each test split."
        )
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--screening-dir", type=Path, default=None)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--cpu-threads", type=int, default=16)
    parser.add_argument("--row-chunk-size", type=int, default=16)
    parser.add_argument("--amp-dtype", choices=("bf16", "fp16", "fp32"), default="bf16")
    parser.add_argument("--ig-steps", type=int, default=8)
    parser.add_argument("--ig-batch-size", type=int, default=2)
    parser.add_argument("--reuse-complete-explainability", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--progress", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    initialized_here = explain._initialize_explainability_process_group()
    rank = explain._distributed_rank()
    world_size = explain._distributed_world_size()
    if int(args.cpu_threads) > 0:
        torch.set_num_threads(max(1, int(args.cpu_threads)))
    config = load_config(args.config)
    panel, folds = _build_panel_and_folds(config)
    folds_by_id = {int(fold.fold_id): fold for fold in folds}
    fold_ids = sorted(folds_by_id)
    checkpoint_fold_ids = set(explain._available_checkpoint_folds(folds, args.output_dir))
    missing_checkpoints = sorted(set(fold_ids).difference(checkpoint_fold_ids))
    if missing_checkpoints:
        raise FileNotFoundError(
            "Feature screening requires every configured fold checkpoint; missing: "
            f"{missing_checkpoints} under {args.output_dir}"
        )
    screening_root = args.screening_dir or (args.output_dir / "feature_screening_first_test_year")
    screening_root.mkdir(parents=True, exist_ok=True)
    explainability_root = args.output_dir / "explainability"
    reusable: list[int] = []
    if bool(args.reuse_complete_explainability):
        for fold_id in fold_ids:
            expected_rows = explain._estimated_first_test_year_explain_rows(
                folds_by_id[fold_id],
                panel,
                lookback=config.training.lookback,
                max_rows=0,
            )
            if _valid_reuse_fold(
                fold_id,
                expected_rows=expected_rows,
                explainability_root=explainability_root,
                ig_steps=int(args.ig_steps),
            ):
                reusable.append(fold_id)
    missing = [fold_id for fold_id in fold_ids if fold_id not in reusable]
    assignments, estimated_loads = explain._balanced_fold_assignments(
        missing,
        folds_by_id,
        panel,
        world_size=world_size,
        lookback=config.training.lookback,
        max_rows=0,
    )
    if rank == 0:
        print(
            f"feature screening: folds={fold_ids}, reused={reusable}, "
            f"assignments={assignments}, estimated_rows={estimated_loads}"
        )
    device = explain._device_from_config(config, args.device)
    if device.type == "cuda":
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
    local_fold_ids = assignments[rank]
    for fold_id in local_fold_ids:
        checkpoint_path = args.output_dir / f"fold_{fold_id:02d}" / "checkpoint_best.pt"
        fold_panel = explain._align_panel_to_checkpoint_universe(
            panel,
            args.output_dir,
            fold_id,
            checkpoint_path,
        )
        model, checkpoint_info = explain.load_model_from_checkpoint(
            config,
            fold_panel,
            checkpoint_path,
            device,
            strict=True,
        )
        dataset = explain._first_test_year_dataset(
            fold_panel,
            folds_by_id[fold_id],
            config.training.lookback,
            execution_mode=getattr(config.trading, "execution_mode", "naive"),
            lookback_context=config.walk_forward.lookback_context,
            short_capacity_limit_enabled=bool(
                getattr(config.trading, "tw_short_capacity_limit_enabled", True)
            ),
            tw_corporate_action_mode=str(
                getattr(config.trading, "tw_corporate_action_mode", "avoid")
            ),
        )
        source = explain._sample_dataset_source(dataset, 0, "even")
        gradient, ig, timing = _screen_source(
            model,
            source,
            feature_names=fold_panel.feature_names,
            device=device,
            amp_dtype=str(args.amp_dtype),
            row_chunk_size=int(args.row_chunk_size),
            ig_steps=int(args.ig_steps),
            ig_batch_size=int(args.ig_batch_size),
            progress_enabled=bool(args.progress),
            rank=rank,
            fold_id=fold_id,
        )
        fold_output = screening_root / f"fold_{fold_id:02d}_test"
        fold_output.mkdir(parents=True, exist_ok=True)
        gradient.write_csv(fold_output / "feature_importance_gradient.csv")
        ig.write_csv(fold_output / "feature_importance_integrated_gradients.csv")
        timing["checkpoint"] = str(checkpoint_path)
        timing["checkpoint_info"] = checkpoint_info
        timing["first_test_year_only"] = True
        timing["split"] = "test"
        timing["split_rows"] = len(dataset)
        timing["sampled_date_coverage"] = float(len(source)) / max(1, len(dataset))
        selected_dates = fold_panel.dates[source.date_indices]
        timing["date_start"] = (
            str(np.datetime_as_string(selected_dates[0], unit="D")) if len(selected_dates) else None
        )
        timing["date_end"] = (
            str(np.datetime_as_string(selected_dates[-1], unit="D")) if len(selected_dates) else None
        )
        timing["test_year"] = (
            int(str(np.datetime_as_string(selected_dates[0], unit="Y"))) if len(selected_dates) else None
        )
        (fold_output / "summary.json").write_text(
            json.dumps(explain._to_builtin(timing), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(
            f"[rank {rank}] Fold {fold_id:02d} complete: rows={len(source)}, "
            f"elapsed={timing['elapsed_s']:.1f}s, dates/s={timing['dates_per_s']:.2f}"
        )
        del model, dataset, source, gradient, ig
        explain._clear_explainability_runtime_cache()
    explain._distributed_barrier()
    if rank == 0:
        output_path = _aggregate(
            fold_ids,
            list(panel.feature_names),
            screening_root=screening_root,
            explainability_root=explainability_root,
        )
        print(f"feature screening output: {output_path}")
    if initialized_here:
        explain._destroy_explainability_process_group()


if __name__ == "__main__":
    main()
