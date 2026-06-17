from __future__ import annotations

import argparse
import gc
import inspect
import json
import math
import os
import time
import warnings
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import numpy as np
import polars as pl
import torch
from torch import nn

from stockagent.config import ExperimentConfig, load_config
from stockagent.backtest.gpu_plot import (
    rapids_datashader_available,
    run_cuml_umap,
    save_heatmap_points_datashader,
    save_line_series_datashader,
    save_scatter_datashader,
)
from stockagent.data.panel import PanelData, build_panel
from stockagent.data.walkforward import WalkForwardFold, build_expanding_year_folds
from stockagent.models.factory import build_model
from stockagent.training.dataset import CrossSectionalDataset, collate_batch


def _clear_explainability_runtime_cache() -> None:
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        try:
            torch.cuda.ipc_collect()
        except Exception:
            pass


PAPER_TOKENS = {
    "surface": "#FCFCFD",
    "panel": "#FFFFFF",
    "ink": "#1F2430",
    "muted": "#6F768A",
    "grid": "#E6E8F0",
    "axis": "#D7DBE7",
    "blue_xlight": "#EAF1FE",
    "blue_light": "#CEDFFE",
    "blue_base": "#A3BEFA",
    "blue_mid": "#5477C4",
    "blue_dark": "#2E4780",
    "gold_xlight": "#FFF4C2",
    "gold_light": "#FFEA8F",
    "gold_base": "#FFE15B",
    "gold_mid": "#B8A037",
    "gold_dark": "#736422",
    "orange_xlight": "#FFEDDE",
    "orange_base": "#F0986E",
    "orange_mid": "#CC6F47",
    "orange_dark": "#804126",
    "olive_base": "#A3D576",
    "olive_mid": "#71B436",
    "olive_dark": "#386411",
    "pink_base": "#F390CA",
    "pink_mid": "#BD569B",
    "pink_dark": "#8A3A6F",
    "neutral_light": "#E2E5EA",
    "neutral_base": "#C5CAD3",
    "neutral_mid": "#7A828F",
    "neutral_dark": "#464C55",
}

_MATPLOTLIB_TRANSFORM_DOT_WARNING = r".*invalid value encountered in dot.*"


def _sanitize_matplotlib_axis_limits(fig: Any) -> None:
    for ax in getattr(fig, "axes", ()):
        axis_specs = (
            ("x", ax.get_xlim, ax.set_xlim),
            ("y", ax.get_ylim, ax.set_ylim),
        )
        for axis_name, getter, setter in axis_specs:
            try:
                lo, hi = getter()
            except Exception:
                continue
            if np.isfinite([lo, hi]).all() and lo != hi:
                continue
            default = (1e-12, 1.0) if (axis_name == "y" and ax.get_yscale() == "log") else (0.0, 1.0)
            try:
                setter(*default)
            except Exception:
                pass


def _safe_matplotlib_tight_layout(fig: Any) -> None:
    _sanitize_matplotlib_axis_limits(fig)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=_MATPLOTLIB_TRANSFORM_DOT_WARNING,
            category=RuntimeWarning,
        )
        fig.tight_layout()


def _save_matplotlib_figure(fig: Any, output_path: Path, **kwargs: Any) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _sanitize_matplotlib_axis_limits(fig)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=_MATPLOTLIB_TRANSFORM_DOT_WARNING,
            category=RuntimeWarning,
        )
        fig.savefig(output_path, **kwargs)

FEATURE_GROUP_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Return", ("logret", "return", "ret_", "_ret")),
    ("Volume", ("volume", "vol", "turnover", "amount")),
    ("Candlestick", ("body", "clv", "kline", "candle")),
    ("Shadow", ("shadow", "upper", "lower")),
    ("Position", ("range", "rank", "zscore", "position", "price_level")),
)


@dataclass(slots=True)
class ExplainabilitySettings:
    top_k: int = 20
    max_rows: int = 32
    ig_steps: int = 8
    ig_batch_size: int = 0
    perturb: bool = True
    perturb_batch_size: int = 0
    perturb_max_auto_batch_size: int = 5
    perturb_max_input_elements: int = 32_000_000
    sample_method: str = "even"
    first_test_year_only: bool = True
    report_style: str = "paper"
    plot_theme: str = "paper"
    standard_plots: bool = True
    interactive_plots: bool = False
    shap_enabled: bool = True
    shap_mode: str = "score_head_surrogate"
    case_study_top_k: int = 5
    regime_analysis: bool = True
    fold_stability: bool = True
    umap_enabled: bool = True
    umap_max_points: int = 10000
    umap_max_projections: int = 0
    umap_n_neighbors: int = 15
    umap_min_dist: float = 0.1
    cross_asset_enabled: bool = True
    cross_asset_max_sources: int = 24
    cross_asset_max_targets: int = 24
    cross_asset_top_edges: int = 150
    cross_asset_source_chunk_size: int = 2
    cross_asset_perturb_scale: float = 1.0
    cross_asset_shocks: tuple[str, ...] = field(
        default_factory=lambda: ("zero", "momentum", "gap", "volume", "volatility", "liquidity")
    )
    cross_asset_attention_flow: bool = True
    cross_asset_attention_capture_rows: int = 4
    cross_asset_validated_transmission: bool = True
    cross_asset_role_embedding: bool = True
    strict_no_fallback: bool = False


@dataclass(slots=True)
class LoadedExplanationContext:
    config: ExperimentConfig
    panel: PanelData
    folds: list[WalkForwardFold]
    fold: WalkForwardFold
    split: str
    checkpoint_path: Path
    output_dir: Path


def _cross_asset_settings_from_explainability(settings: ExplainabilitySettings):
    from stockagent.explainability_cross_asset import CrossAssetTransmissionSettings

    shocks = tuple(str(value).strip().lower() for value in settings.cross_asset_shocks if str(value).strip())
    return CrossAssetTransmissionSettings(
        enabled=bool(settings.cross_asset_enabled),
        max_sources=max(1, int(settings.cross_asset_max_sources)),
        max_targets=max(1, int(settings.cross_asset_max_targets)),
        top_edges=max(1, int(settings.cross_asset_top_edges)),
        source_chunk_size=max(1, int(settings.cross_asset_source_chunk_size)),
        perturb_scale=float(settings.cross_asset_perturb_scale),
        shocks=shocks or ("zero", "momentum", "gap", "volume", "volatility", "liquidity"),
        attention_flow=bool(settings.cross_asset_attention_flow),
        attention_capture_rows=max(1, int(settings.cross_asset_attention_capture_rows)),
        validated_transmission=bool(settings.cross_asset_validated_transmission),
        role_embedding=bool(settings.cross_asset_role_embedding),
    )


def settings_from_training_config(training: Any) -> ExplainabilitySettings:
    """Build the post-training explainability settings from TrainingConfig.

    Keep these defaults aligned with explain_model.py CLI defaults, not with the
    historical ExplainabilitySettings dataclass defaults.
    """

    return ExplainabilitySettings(
        top_k=int(getattr(training, "explain_top_k", 20)),
        max_rows=int(getattr(training, "explain_max_rows", 32)),
        ig_steps=int(getattr(training, "explain_ig_steps", 0)),
        ig_batch_size=int(getattr(training, "explain_ig_batch_size", 1)),
        perturb=bool(getattr(training, "explain_perturb", False)),
        perturb_batch_size=int(getattr(training, "explain_perturb_batch_size", 1)),
        perturb_max_auto_batch_size=int(getattr(training, "explain_perturb_max_auto_batch_size", 1)),
        perturb_max_input_elements=int(getattr(training, "explain_perturb_max_input_elements", 8_000_000)),
        sample_method=str(getattr(training, "explain_sample_method", "even")),
        first_test_year_only=bool(getattr(training, "explain_first_test_year_only", True)),
        report_style=str(getattr(training, "explain_report_style", "none")),
        plot_theme=str(getattr(training, "explain_plot_theme", "paper")),
        standard_plots=bool(getattr(training, "explain_standard_plots", False)),
        interactive_plots=bool(getattr(training, "explain_interactive_plots", False)),
        shap_enabled=bool(getattr(training, "explain_shap_enabled", False)),
        shap_mode=str(getattr(training, "explain_shap_mode", "score_head_surrogate")),
        case_study_top_k=int(getattr(training, "explain_case_study_top_k", 5)),
        regime_analysis=bool(getattr(training, "explain_regime_analysis", False)),
        fold_stability=bool(getattr(training, "explain_fold_stability", False)),
        umap_enabled=bool(getattr(training, "explain_umap_enabled", False)),
        umap_max_points=int(getattr(training, "explain_umap_max_points", 1000)),
        umap_max_projections=int(getattr(training, "explain_umap_max_projections", 0)),
        umap_n_neighbors=int(getattr(training, "explain_umap_n_neighbors", 15)),
        umap_min_dist=float(getattr(training, "explain_umap_min_dist", 0.1)),
        cross_asset_enabled=bool(getattr(training, "explain_cross_asset_enabled", False)),
        cross_asset_max_sources=int(getattr(training, "explain_cross_asset_max_sources", 8)),
        cross_asset_max_targets=int(getattr(training, "explain_cross_asset_max_targets", 8)),
        cross_asset_top_edges=int(getattr(training, "explain_cross_asset_top_edges", 150)),
        cross_asset_source_chunk_size=int(getattr(training, "explain_cross_asset_source_chunk_size", 1)),
        cross_asset_perturb_scale=float(getattr(training, "explain_cross_asset_perturb_scale", 1.0)),
        cross_asset_shocks=tuple(getattr(training, "explain_cross_asset_shocks", ())),
        cross_asset_attention_flow=bool(getattr(training, "explain_cross_asset_attention_flow", True)),
        cross_asset_attention_capture_rows=int(getattr(training, "explain_cross_asset_attention_capture_rows", 1)),
        cross_asset_validated_transmission=bool(
            getattr(training, "explain_cross_asset_validated_transmission", True)
        ),
        cross_asset_role_embedding=bool(getattr(training, "explain_cross_asset_role_embedding", False)),
    )


def _to_builtin(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _to_builtin(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_to_builtin(item) for item in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, torch.Tensor):
        if value.numel() == 1:
            return value.detach().cpu().item()
        return value.detach().cpu().tolist()
    return value


def _mark_elapsed(timing: dict[str, float], key: str, start: float) -> None:
    timing[key] = float(time.perf_counter() - start)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(out):
        return default
    return out


def _safe_corrcoef(left: np.ndarray, right: np.ndarray) -> float:
    left = np.asarray(left, dtype=np.float64)
    right = np.asarray(right, dtype=np.float64)
    valid = np.isfinite(left) & np.isfinite(right)
    if int(valid.sum()) < 3:
        return 0.0
    left = left[valid]
    right = right[valid]
    left_std = float(left.std(ddof=0))
    right_std = float(right.std(ddof=0))
    if left_std <= 0.0 or right_std <= 0.0:
        return 0.0
    left_centered = left - float(left.mean())
    right_centered = right - float(right.mean())
    corr = float(np.mean(left_centered * right_centered) / (left_std * right_std))
    return _safe_float(corr)


def _normalize_plot_backend(value: str | None) -> str:
    normalized = str(value or "auto").strip().lower()
    if normalized in {"rapids", "datashader", "gpu", "gpu_datashader"}:
        normalized = "rapids_datashader"
    if normalized not in {"auto", "matplotlib", "rapids_datashader"}:
        raise ValueError("plot_backend must be one of: auto, matplotlib, rapids_datashader")
    return normalized


def _normalize_report_style(value: str | None) -> str:
    normalized = str(value or "paper").strip().lower()
    if normalized not in {"paper", "standard", "none"}:
        raise ValueError("explain_report_style must be one of: paper, standard, none")
    return normalized


def _normalize_plot_theme(value: str | None) -> str:
    normalized = str(value or "paper").strip().lower()
    if normalized not in {"paper", "standard"}:
        raise ValueError("explain_plot_theme must be one of: paper, standard")
    return normalized


def _normalize_shap_mode(value: str | None) -> str:
    normalized = str(value or "score_head_surrogate").strip().lower()
    if normalized in {"surrogate", "score_head"}:
        normalized = "score_head_surrogate"
    if normalized not in {"score_head_surrogate", "off", "none"}:
        raise ValueError("explain_shap_mode must be one of: score_head_surrogate, off, none")
    return normalized


def _feature_group(feature: str) -> str:
    lowered = str(feature).lower()
    for group, patterns in FEATURE_GROUP_PATTERNS:
        if any(pattern in lowered for pattern in patterns):
            return group
    return "Other"


def _feature_label(feature: str) -> str:
    return f"{_feature_group(feature)} / {feature}"


def _lookback_label(value: Any) -> str:
    try:
        offset = int(value)
    except (TypeError, ValueError):
        return str(value)
    return f"t-{offset}"


def _empty_frame(columns: list[str] | tuple[str, ...] | None = None) -> pl.DataFrame:
    if columns is None:
        return pl.DataFrame()
    return pl.DataFrame({str(column): [] for column in columns})


def _is_empty_frame(frame: pl.DataFrame | None) -> bool:
    return frame is None or frame.is_empty()


def _concat_frames(frames: list[pl.DataFrame]) -> pl.DataFrame:
    pieces = [frame for frame in frames if frame is not None and not frame.is_empty()]
    if not pieces:
        return pl.DataFrame()
    return pl.concat(pieces, how="diagonal_relaxed")


def _numeric_expr(column: str) -> pl.Expr:
    value = pl.col(column).cast(pl.Float64, strict=False).fill_nan(None)
    return pl.when(value.is_finite()).then(value).otherwise(None)


def _with_numeric(frame: pl.DataFrame, *columns: str) -> pl.DataFrame:
    expressions = [_numeric_expr(column).alias(column) for column in columns if column in frame.columns]
    return frame.with_columns(expressions) if expressions else frame


def _numeric_numpy(frame: pl.DataFrame, column: str, *, default: float = 0.0) -> np.ndarray:
    if column not in frame.columns:
        return np.full(frame.height, float(default), dtype=np.float64)
    values = frame.select(_numeric_expr(column).fill_null(float(default)).alias(column)).to_series().to_numpy()
    return np.nan_to_num(
        np.asarray(values, dtype=np.float64),
        nan=float(default),
        posinf=float(default),
        neginf=float(default),
    )


def _numeric_sum(frame: pl.DataFrame, column: str) -> float:
    if column not in frame.columns or frame.is_empty():
        return 0.0
    value = frame.select(_numeric_expr(column).fill_null(0.0).sum()).item()
    return _safe_float(value)


def _numeric_max(frame: pl.DataFrame, column: str) -> float:
    if column not in frame.columns or frame.is_empty():
        return 0.0
    value = frame.select(_numeric_expr(column).fill_null(0.0).max()).item()
    return _safe_float(value)


def _first_row(frame: pl.DataFrame) -> dict[str, Any]:
    return frame.row(0, named=True) if not frame.is_empty() else {}


def _with_feature_labels(frame: pl.DataFrame, feature_col: str = "feature") -> pl.DataFrame:
    if feature_col not in frame.columns:
        return frame
    return frame.with_columns(
        [
            pl.col(feature_col)
            .cast(pl.String)
            .map_elements(_feature_group, return_dtype=pl.String)
            .alias("feature_group"),
            pl.col(feature_col)
            .cast(pl.String)
            .map_elements(_feature_label, return_dtype=pl.String)
            .alias("feature_label"),
        ]
    )


def _write_csv(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(path)


def _frame_to_dict(frame: pl.DataFrame) -> dict[str, list[Any]]:
    return frame.to_dict(as_series=False)


def _to_plot_data(frame: pl.DataFrame) -> dict[str, list[Any]]:
    return frame.to_dict(as_series=False)


def _string_list(frame: pl.DataFrame, column: str) -> list[str]:
    if column not in frame.columns:
        return []
    return [str(value) for value in frame.get_column(column).to_list()]


def _top_values_by_sum(frame: pl.DataFrame, group_col: str, value_col: str, limit: int) -> list[Any]:
    if _is_empty_frame(frame) or group_col not in frame.columns or value_col not in frame.columns:
        return []
    grouped = (
        _with_numeric(frame, value_col)
        .drop_nulls(subset=[group_col, value_col])
        .group_by(group_col)
        .agg(pl.col(value_col).sum().alias("__sum"))
        .sort("__sum", descending=True)
        .head(limit)
    )
    return grouped.get_column(group_col).to_list() if not grouped.is_empty() else []


def _render_table_markdown(frame: pl.DataFrame, limit: int = 20) -> str:
    if _is_empty_frame(frame):
        return "_No rows._"
    data = frame.head(limit)
    columns = [str(column) for column in data.columns]
    if not columns:
        return "_No rows._"
    rows = data.to_dicts()

    def fmt(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.6g}"
        return str(value).replace("\n", " ").replace("|", "\\|")

    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(fmt(row.get(column)) for column in columns) + " |" for row in rows]
    return "\n".join([header, separator, *body])


def _pivot_sum_matrix(
    frame: pl.DataFrame,
    *,
    index_col: str,
    column_col: str,
    value_col: str,
    index_order: list[Any] | None = None,
    column_order: list[Any] | None = None,
) -> tuple[list[Any], list[Any], np.ndarray]:
    if _is_empty_frame(frame) or not {index_col, column_col, value_col}.issubset(frame.columns):
        return [], [], np.zeros((0, 0), dtype=np.float64)
    grouped = (
        _with_numeric(frame, value_col)
        .drop_nulls(subset=[index_col, column_col, value_col])
        .group_by([index_col, column_col])
        .agg(pl.col(value_col).sum().alias(value_col))
    )
    if grouped.is_empty():
        return [], [], np.zeros((0, 0), dtype=np.float64)
    index_values = index_order if index_order is not None else grouped.get_column(index_col).unique(maintain_order=True).to_list()
    column_values = column_order if column_order is not None else sorted(grouped.get_column(column_col).unique().to_list())
    index_pos = {str(value): idx for idx, value in enumerate(index_values)}
    column_pos = {str(value): idx for idx, value in enumerate(column_values)}
    matrix = np.zeros((len(index_values), len(column_values)), dtype=np.float64)
    for row in grouped.to_dicts():
        i = index_pos.get(str(row.get(index_col)))
        j = column_pos.get(str(row.get(column_col)))
        if i is not None and j is not None:
            matrix[i, j] += _safe_float(row.get(value_col))
    return index_values, column_values, matrix


def _use_datashader_for_explainability(plot_backend: str, *, estimated_points: int = 0) -> bool:
    normalized = _normalize_plot_backend(plot_backend)
    if normalized == "matplotlib":
        return False
    available = rapids_datashader_available(require_cuda=True)
    if normalized == "rapids_datashader" and not available:
        raise RuntimeError(
            "RAPIDS/cuDF/Datashader with CUDA was requested for explainability, but it is unavailable."
        )
    if normalized == "auto" and int(estimated_points) < 100_000:
        return False
    return bool(available)


def _device_from_config(config: ExperimentConfig, override: str | None = None) -> torch.device:
    requested = (override or config.environment.device or "cpu").strip().lower()
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested for explanation, but torch.cuda.is_available() is False.")
    return torch.device(requested)


def _move_batch(batch: dict[str, torch.Tensor], device: torch.device) -> dict[str, torch.Tensor]:
    return {key: value.to(device=device, non_blocking=(device.type == "cuda")) for key, value in batch.items()}


def _slice_batch_rows(batch: dict[str, torch.Tensor], start: int, end: int) -> dict[str, torch.Tensor]:
    start = max(0, int(start))
    end = max(start, int(end))
    sliced: dict[str, torch.Tensor] = {}
    for key, value in batch.items():
        if torch.is_tensor(value) and value.ndim > 0 and int(value.size(0)) >= end:
            sliced[key] = value[start:end]
        else:
            sliced[key] = value
    return sliced


def _cuda_mem_get_info(device: torch.device) -> tuple[int, int] | None:
    if device.type != "cuda" or not torch.cuda.is_available():
        return None
    try:
        return torch.cuda.mem_get_info(device)
    except TypeError:
        return torch.cuda.mem_get_info()


def _auto_explain_row_chunk_size(
    batch: dict[str, torch.Tensor],
    settings: ExplainabilitySettings,
    device: torch.device,
) -> tuple[int, dict[str, Any]]:
    x = batch.get("x")
    if not torch.is_tensor(x) or x.ndim != 4:
        return 1, {"reason": "missing_x"}
    n_rows = int(x.size(0))
    if n_rows <= 1:
        return max(1, n_rows), {"reason": "single_row", "rows": n_rows}
    override = os.environ.get("STOCKAGENT_EXPLAIN_ROW_CHUNK_SIZE")
    if override:
        try:
            value = max(1, min(n_rows, int(override)))
            return value, {"reason": "env_override", "rows": n_rows, "row_chunk_size": value}
        except ValueError:
            pass
    if device.type != "cuda" or not torch.cuda.is_available():
        return n_rows, {"reason": "non_cuda", "rows": n_rows, "row_chunk_size": n_rows}

    lookback = int(x.size(1))
    n_symbols = int(x.size(2))
    n_features = int(x.size(3))
    bytes_per_row = max(1, lookback * n_symbols * n_features * 4)
    mem_info = _cuda_mem_get_info(device)
    free_bytes, total_bytes = mem_info if mem_info is not None else (0, 0)
    gib = 1024**3
    # Empirical full-universe profiling on this model shows attribution activations
    # cost roughly 45-50x the raw input row. Keep a fixed workspace reserve for
    # the model, CUDA kernels, UMAP, and allocator fragmentation.
    activation_multiplier = 48.0
    if int(settings.ig_steps) <= 0 and not bool(settings.perturb):
        activation_multiplier = 28.0
    workspace_reserve = 3.0 * gib
    usable_bytes = max(0.0, min(float(free_bytes) * 0.70, float(total_bytes) * 0.65) - workspace_reserve)
    estimated = int(max(1.0, usable_bytes / (bytes_per_row * activation_multiplier)))
    if n_symbols >= 10_000:
        estimated = 1
    elif n_symbols >= 4_000:
        estimated = min(estimated, 8)
    row_chunk_size = max(1, min(n_rows, estimated))
    return row_chunk_size, {
        "reason": "cuda_budget",
        "rows": n_rows,
        "row_chunk_size": row_chunk_size,
        "lookback": lookback,
        "symbols": n_symbols,
        "features": n_features,
        "bytes_per_row": int(bytes_per_row),
        "activation_multiplier": activation_multiplier,
        "free_gb": float(free_bytes) / gib,
        "total_gb": float(total_bytes) / gib,
        "workspace_reserve_gb": float(workspace_reserve) / gib,
    }


def _call_model(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    *,
    return_aux: bool | None = None,
) -> Any:
    if return_aux is None:
        return model(x, mask)
    try:
        return model(x, mask, return_aux=return_aux)
    except TypeError:
        signature = inspect.signature(model.forward)
        if "return_aux" in signature.parameters:
            return model(x, mask, return_aux=return_aux)
        return model(x, mask)


def _normalize_model_output(output: Any) -> tuple[torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
    aux: dict[str, torch.Tensor] = {}
    if isinstance(output, dict):
        weights = output.get("weights")
        if weights is None:
            raise ValueError("Model output dict does not include 'weights'.")
        scores = output.get("score_logits", output.get("rank_logits", output.get("scores", weights)))
        nested_aux = output.get("aux")
        if isinstance(nested_aux, dict):
            aux.update({str(key): value for key, value in nested_aux.items() if torch.is_tensor(value)})
        aux.update({str(key): value for key, value in output.items() if torch.is_tensor(value)})
        return weights, scores, aux
    if isinstance(output, tuple):
        if len(output) < 1:
            raise ValueError("Model returned an empty tuple.")
        weights = output[0]
        scores = output[1] if len(output) >= 2 and torch.is_tensor(output[1]) else weights
        if len(output) >= 3 and isinstance(output[2], dict):
            aux.update({str(key): value for key, value in output[2].items() if torch.is_tensor(value)})
        return weights, scores, aux
    if torch.is_tensor(output):
        return output, output, aux
    raise TypeError(f"Unsupported model output type: {type(output)!r}")


def _forward_outputs(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    *,
    return_aux: bool = True,
) -> tuple[torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
    output = _call_model(model, x, mask, return_aux=return_aux)
    weights, scores, aux = _normalize_model_output(output)
    return (
        torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0),
        torch.nan_to_num(scores, nan=0.0, posinf=0.0, neginf=0.0),
        aux,
    )


def _selection_from_weights(weights: torch.Tensor, mask: torch.Tensor, top_k: int) -> tuple[torch.Tensor, torch.Tensor]:
    top_k = max(1, min(int(top_k), int(weights.size(1))))
    valid_weights = weights.masked_fill(~mask, 0.0)
    values, indices = valid_weights.abs().topk(k=top_k, dim=1)
    selected = torch.zeros_like(weights, dtype=torch.bool)
    selected.scatter_(1, indices, values > 0.0)
    direction = torch.sign(valid_weights).masked_fill(~selected, 0.0)
    return selected, direction


def _decision_target(scores: torch.Tensor, selected: torch.Tensor, direction: torch.Tensor) -> torch.Tensor:
    selected_f = selected.to(dtype=scores.dtype)
    denom = selected_f.sum().clamp_min(1.0)
    return (scores * direction.to(dtype=scores.dtype) * selected_f).sum() / denom


def _gradient_x_input_attribution(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    selected: torch.Tensor,
    direction: torch.Tensor,
) -> torch.Tensor:
    model.zero_grad(set_to_none=True)
    x_grad = x.detach().clone().requires_grad_(True)
    _, scores, _ = _forward_outputs(model, x_grad, mask, return_aux=False)
    target = _decision_target(scores, selected, direction)
    grad = torch.autograd.grad(target, x_grad, retain_graph=False, create_graph=False)[0]
    return torch.nan_to_num((grad * x_grad).detach(), nan=0.0, posinf=0.0, neginf=0.0)


def _auto_repeat_chunk_size(
    x: torch.Tensor,
    total_items: int,
    requested: int,
    *,
    max_auto: int,
    max_input_elements: int,
) -> int:
    total_items = max(1, int(total_items))
    requested = int(requested)
    if requested > 0:
        return max(1, min(total_items, requested))
    per_item_elements = max(1, int(x.numel()))
    by_budget = max(1, int(max_input_elements) // per_item_elements)
    return max(1, min(total_items, int(max_auto), by_budget))


def _repeat_first_dim(tensor: torch.Tensor, repeats: int) -> torch.Tensor:
    repeats = max(1, int(repeats))
    return tensor.unsqueeze(0).expand((repeats,) + tuple(tensor.shape)).reshape(
        repeats * int(tensor.size(0)),
        *tuple(tensor.shape[1:]),
    )


def _is_cuda_oom(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return isinstance(exc, RuntimeError) and "out of memory" in msg and ("cuda" in msg or "cublas" in msg)


def _cuda_oom_fallback_settings(settings: ExplainabilitySettings) -> ExplainabilitySettings | None:
    if (
        int(settings.ig_steps) <= 0
        and not bool(settings.perturb)
        and not bool(settings.umap_enabled)
    ):
        return None
    return replace(
        settings,
        ig_steps=0,
        ig_batch_size=1,
        perturb=False,
        perturb_batch_size=1,
        perturb_max_auto_batch_size=1,
        perturb_max_input_elements=min(int(settings.perturb_max_input_elements), 8_000_000),
        umap_enabled=False,
        umap_max_points=min(int(settings.umap_max_points), 1000),
        umap_max_projections=0,
    )


def _append_summary_warning(result: dict[str, Any], warning: str) -> None:
    summary = result.setdefault("summary", {})
    warnings = list(summary.get("warnings", []) or [])
    if warning not in warnings:
        warnings.append(warning)
    summary["warnings"] = warnings


def _integrated_gradients_attribution(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    selected: torch.Tensor,
    direction: torch.Tensor,
    steps: int,
    batch_size: int = 0,
) -> torch.Tensor:
    steps = max(0, int(steps))
    if steps <= 0:
        return torch.zeros_like(x)
    chunk_size = _auto_repeat_chunk_size(
        x,
        steps,
        int(batch_size),
        max_auto=4,
        max_input_elements=16_000_000,
    )
    total_grad = torch.zeros_like(x)
    for start in range(1, steps + 1, chunk_size):
        end = min(steps, start + chunk_size - 1)
        alpha = torch.arange(start, end + 1, device=x.device, dtype=x.dtype) / float(steps)
        repeats = int(alpha.numel())
        model.zero_grad(set_to_none=True)
        x_step = (alpha.view(repeats, 1, 1, 1, 1) * x.detach().unsqueeze(0)).reshape(
            repeats * int(x.size(0)),
            *tuple(x.shape[1:]),
        )
        x_step = x_step.detach().requires_grad_(True)
        mask_step = _repeat_first_dim(mask, repeats)
        selected_step = _repeat_first_dim(selected, repeats)
        direction_step = _repeat_first_dim(direction, repeats)
        _, scores, _ = _forward_outputs(model, x_step, mask_step, return_aux=False)
        target = _decision_target(scores, selected_step, direction_step) * float(repeats)
        grad = torch.autograd.grad(target, x_step, retain_graph=False, create_graph=False)[0]
        grad = torch.nan_to_num(grad.detach(), nan=0.0, posinf=0.0, neginf=0.0)
        total_grad = total_grad + grad.reshape(repeats, *tuple(x.shape)).sum(dim=0)
    return x * (total_grad / float(steps))


def _feature_time_frame(
    attribution: torch.Tensor,
    feature_names: list[str],
    metric_name: str,
) -> pl.DataFrame:
    values = attribution.detach().abs().mean(dim=(0, 2)).cpu().numpy()
    rows: list[dict[str, Any]] = []
    for time_idx in range(values.shape[0]):
        for feat_idx, feature in enumerate(feature_names):
            rows.append(
                {
                    "lookback_index": int(time_idx),
                    "lookback_from_end": int(values.shape[0] - 1 - time_idx),
                    "feature": feature,
                    "feature_group": _feature_group(feature),
                    "feature_label": _feature_label(feature),
                    metric_name: float(values[time_idx, feat_idx]),
                }
            )
    return pl.DataFrame(rows)



def _feature_summary_frame(feature_time: pl.DataFrame, metric_name: str) -> pl.DataFrame:
    if _is_empty_frame(feature_time):
        return _empty_frame(["feature", "feature_group", "feature_label", metric_name, "share"])
    summary = feature_time.group_by("feature").agg(
        _numeric_expr(metric_name).fill_null(0.0).sum().alias(metric_name)
    )
    summary = _with_feature_labels(summary)
    total = _numeric_sum(summary, metric_name)
    share_expr = (pl.col(metric_name) / total) if total > 0.0 else pl.lit(0.0)
    return summary.with_columns(share_expr.alias("share")).sort(metric_name, descending=True)



def _time_summary_frame(feature_time: pl.DataFrame, metric_name: str) -> pl.DataFrame:
    if _is_empty_frame(feature_time):
        return _empty_frame(["lookback_index", "lookback_from_end", metric_name, "share"])
    summary = feature_time.group_by(["lookback_index", "lookback_from_end"]).agg(
        _numeric_expr(metric_name).fill_null(0.0).sum().alias(metric_name)
    )
    total = _numeric_sum(summary, metric_name)
    share_expr = (pl.col(metric_name) / total) if total > 0.0 else pl.lit(0.0)
    return summary.with_columns(share_expr.alias("share")).sort("lookback_index")



def _perturbation_importance(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    base_weights: torch.Tensor,
    base_scores: torch.Tensor,
    feature_names: list[str],
    batch_size: int = 0,
    *,
    max_auto_batch_size: int = 16,
    max_input_elements: int = 96_000_000,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    perturbations = [
        (time_idx, feat_idx, feature)
        for time_idx in range(int(x.size(1)))
        for feat_idx, feature in enumerate(feature_names)
    ]
    diagnostics: dict[str, Any] = {
        "num_perturbations": int(len(perturbations)),
        "requested_batch_size": int(batch_size),
        "max_auto_batch_size": int(max_auto_batch_size),
        "max_input_elements": int(max_input_elements),
        "chunk_size": 0,
        "final_chunk_size": 0,
        "forward_batches": 0,
        "attempted_forward_batches": 0,
        "oom_retries": 0,
        "oom_chunk_sizes": [],
    }
    if not perturbations:
        frame = pl.DataFrame(rows)
        return frame, pl.DataFrame(), diagnostics
    chunk_size = _auto_repeat_chunk_size(
        x,
        len(perturbations),
        int(batch_size),
        max_auto=max(1, int(max_auto_batch_size)),
        max_input_elements=max(1, int(max_input_elements)),
    )
    diagnostics["chunk_size"] = int(chunk_size)
    base_weights = base_weights.detach()
    base_scores = base_scores.detach()
    with torch.no_grad():
        start = 0
        while start < len(perturbations):
            chunk = perturbations[start : start + chunk_size]
            repeats = len(chunk)
            try:
                diagnostics["attempted_forward_batches"] = int(diagnostics["attempted_forward_batches"]) + 1
                x_perturbed = x.detach().unsqueeze(0).expand((repeats,) + tuple(x.shape)).clone()
                for local_idx, (time_idx, feat_idx, _) in enumerate(chunk):
                    x_perturbed[local_idx, :, time_idx, :, feat_idx] = 0.0
                x_perturbed = x_perturbed.reshape(repeats * int(x.size(0)), *tuple(x.shape[1:]))
                mask_perturbed = _repeat_first_dim(mask, repeats)
                weights_p, scores_p, _ = _forward_outputs(model, x_perturbed, mask_perturbed, return_aux=False)
                weights_p = weights_p.reshape(repeats, *tuple(base_weights.shape))
                scores_p = scores_p.reshape(repeats, *tuple(base_scores.shape))
                weight_deltas = (weights_p - base_weights.unsqueeze(0)).abs().mean(dim=(1, 2)).detach().cpu().numpy()
                score_deltas = (scores_p - base_scores.unsqueeze(0)).abs().mean(dim=(1, 2)).detach().cpu().numpy()
            except RuntimeError as exc:
                if not _is_cuda_oom(exc) or chunk_size <= 1:
                    raise
                diagnostics["oom_retries"] = int(diagnostics["oom_retries"]) + 1
                diagnostics["oom_chunk_sizes"].append(int(chunk_size))
                chunk_size = max(1, int(chunk_size) // 2)
                diagnostics["final_chunk_size"] = int(chunk_size)
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                continue
            diagnostics["forward_batches"] = int(diagnostics["forward_batches"]) + 1
            diagnostics["final_chunk_size"] = int(chunk_size)
            for local_idx, (time_idx, _, feature) in enumerate(chunk):
                rows.append(
                    {
                        "lookback_index": int(time_idx),
                        "lookback_from_end": int(x.size(1) - 1 - time_idx),
                        "feature": feature,
                        "feature_group": _feature_group(feature),
                        "feature_label": _feature_label(feature),
                        "weight_abs_delta": float(weight_deltas[local_idx]),
                        "score_abs_delta": float(score_deltas[local_idx]),
                    }
                )
            start += repeats
    frame = pl.DataFrame(rows)
    if frame.is_empty():
        return frame, pl.DataFrame(), diagnostics
    summary = frame.group_by("feature").agg(
        [
            _numeric_expr("weight_abs_delta").fill_null(0.0).sum().alias("weight_abs_delta"),
            _numeric_expr("score_abs_delta").fill_null(0.0).sum().alias("score_abs_delta"),
        ]
    )
    summary = _with_feature_labels(summary)
    total = _numeric_sum(summary, "weight_abs_delta")
    share_expr = (pl.col("weight_abs_delta") / total) if total > 0.0 else pl.lit(0.0)
    summary = summary.with_columns(share_expr.alias("weight_delta_share")).sort("weight_abs_delta", descending=True)
    return frame, summary, diagnostics



def _feature_correlations(
    x: torch.Tensor,
    scores: torch.Tensor,
    weights: torch.Tensor,
    mask: torch.Tensor,
    feature_names: list[str],
) -> pl.DataFrame:
    x_last = x[:, -1].detach().float()
    x_mean = x.detach().float().mean(dim=1)
    mask_flat = mask.detach().bool().reshape(-1).cpu().numpy()
    score_np = scores.detach().float().reshape(-1).cpu().numpy()
    weight_np = weights.detach().float().reshape(-1).cpu().numpy()
    rows: list[dict[str, Any]] = []
    for source_name, values in (("last", x_last), ("lookback_mean", x_mean)):
        values_np = values.reshape(-1, values.size(-1)).cpu().numpy()
        for feat_idx, feature in enumerate(feature_names):
            feat = values_np[:, feat_idx]
            valid = mask_flat & np.isfinite(feat) & np.isfinite(score_np) & np.isfinite(weight_np)
            if valid.sum() < 3:
                score_corr = 0.0
                weight_corr = 0.0
            else:
                score_corr = _safe_corrcoef(feat[valid], score_np[valid])
                weight_corr = _safe_corrcoef(feat[valid], weight_np[valid])
            rows.append(
                {
                    "source": source_name,
                    "feature": feature,
                    "score_corr": score_corr,
                    "weight_corr": weight_corr,
                    "abs_score_corr": abs(score_corr),
                    "abs_weight_corr": abs(weight_corr),
                }
            )
    frame = pl.DataFrame(rows)
    return frame.sort(["abs_score_corr", "abs_weight_corr"], descending=[True, True]) if not frame.is_empty() else frame



def _decision_rows(
    weights: torch.Tensor,
    scores: torch.Tensor,
    returns: torch.Tensor,
    mask: torch.Tensor,
    dates: list[str],
    symbols: list[str],
    top_k: int,
) -> pl.DataFrame:
    top_k = max(1, min(int(top_k), int(weights.size(1))))
    rows: list[dict[str, Any]] = []
    weights_cpu = weights.detach().cpu()
    scores_cpu = scores.detach().cpu()
    returns_cpu = returns.detach().cpu()
    mask_cpu = mask.detach().cpu()
    for row_idx, date in enumerate(dates):
        row_weights = weights_cpu[row_idx]
        candidate_idx = torch.topk(row_weights.abs(), k=top_k).indices.tolist()
        for rank, sym_idx in enumerate(candidate_idx, start=1):
            weight = float(row_weights[sym_idx])
            future_return = float(returns_cpu[row_idx, sym_idx])
            side = "long" if weight > 0 else ("short" if weight < 0 else "flat")
            rows.append(
                {
                    "date": date,
                    "rank_abs_weight": rank,
                    "symbol": symbols[int(sym_idx)],
                    "side": side,
                    "weight": weight,
                    "score": float(scores_cpu[row_idx, sym_idx]),
                    "future_log_return": future_return,
                    "gross_contribution": weight * future_return,
                    "tradable": bool(mask_cpu[row_idx, sym_idx]),
                }
            )
    return pl.DataFrame(rows)



def _portfolio_summary(weights: torch.Tensor, returns: torch.Tensor, mask: torch.Tensor) -> dict[str, float]:
    safe_weights = torch.nan_to_num(weights.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
    safe_returns = torch.nan_to_num(returns.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
    active = mask.detach().bool()
    active_count = active.to(torch.float32).sum(dim=1).clamp_min(1.0)
    gross = safe_weights.abs().sum(dim=1)
    net = safe_weights.sum(dim=1)
    long_gross = safe_weights.clamp_min(0.0).sum(dim=1)
    short_gross = (-safe_weights.clamp_max(0.0)).sum(dim=1)
    hhi_scaled = safe_weights.pow(2).sum(dim=1) * active_count
    max_abs = safe_weights.abs().max(dim=1).values
    daily_return = (safe_weights * safe_returns).sum(dim=1)
    turnover = (safe_weights[1:] - safe_weights[:-1]).abs().sum(dim=1) if safe_weights.size(0) > 1 else safe_weights.new_zeros((0,))
    return {
        "rows": float(safe_weights.size(0)),
        "symbols": float(safe_weights.size(1)),
        "mean_gross": float(gross.mean().cpu()),
        "mean_abs_net": float(net.abs().mean().cpu()),
        "mean_long_gross": float(long_gross.mean().cpu()),
        "mean_short_gross": float(short_gross.mean().cpu()),
        "mean_scaled_hhi": float(hhi_scaled.mean().cpu()),
        "max_abs_weight_mean": float(max_abs.mean().cpu()),
        "max_abs_weight_max": float(max_abs.max().cpu()),
        "mean_daily_log_return": float(daily_return.mean().cpu()),
        "mean_turnover_proxy": float(turnover.mean().cpu()) if turnover.numel() else 0.0,
        "untradable_abs_weight_sum": float(safe_weights.masked_fill(active, 0.0).abs().sum().cpu()),
    }


def _aux_summary(aux: dict[str, torch.Tensor]) -> tuple[pl.DataFrame, dict[str, pl.DataFrame]]:
    rows: list[dict[str, Any]] = []
    dim_frames: dict[str, pl.DataFrame] = {}
    for name, value in sorted(aux.items()):
        if not torch.is_tensor(value) or value.numel() == 0:
            continue
        tensor = torch.nan_to_num(value.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
        finite = torch.isfinite(value.detach()).float().mean()
        abs_tensor = tensor.abs()
        rows.append(
            {
                "name": name,
                "shape": "x".join(str(int(dim)) for dim in tensor.shape),
                "mean": float(tensor.mean().cpu()),
                "std": float(tensor.std(unbiased=False).cpu()) if tensor.numel() > 1 else 0.0,
                "mean_abs": float(abs_tensor.mean().cpu()),
                "max_abs": float(abs_tensor.max().cpu()),
                "zero_fraction": float((abs_tensor < 1e-8).float().mean().cpu()),
                "finite_fraction": float(finite.cpu()),
            }
        )
        if tensor.ndim >= 3:
            by_dim = abs_tensor.reshape(-1, tensor.shape[-1]).mean(dim=0).cpu().numpy()
            total = float(by_dim.sum())
            dim_frame = pl.DataFrame(
                {
                    "dim": np.arange(by_dim.shape[0], dtype=np.int64),
                    "mean_abs": by_dim,
                    "share": by_dim / total if total > 0.0 else np.zeros_like(by_dim),
                }
            )
            dim_frames[name] = dim_frame.sort("mean_abs", descending=True)
    summary = pl.DataFrame(rows)
    if not summary.is_empty() and "mean_abs" in summary.columns:
        summary = summary.sort("mean_abs", descending=True)
    return summary, dim_frames



def _aux_point_metadata(
    *,
    name: str,
    shape: tuple[int, ...],
    flat_indices: np.ndarray,
    symbols: list[str],
    dates: list[str],
) -> dict[str, list[Any]]:
    rows = int(shape[0]) if len(shape) >= 1 else 0
    second = int(shape[1]) if len(shape) >= 2 else 0
    meta: dict[str, list[Any]] = {
        "tensor": [name] * int(flat_indices.size),
        "flat_index": flat_indices.astype(np.int64).tolist(),
    }
    if len(shape) == 3 and second == len(symbols):
        symbol_idx = flat_indices % max(1, second)
        date_idx = flat_indices // max(1, second)
        meta["point_type"] = ["stock"] * int(flat_indices.size)
        meta["date"] = [dates[int(idx)] if 0 <= int(idx) < len(dates) else "" for idx in date_idx]
        meta["symbol"] = [symbols[int(idx)] if 0 <= int(idx) < len(symbols) else str(int(idx)) for idx in symbol_idx]
        meta["token_index"] = symbol_idx.astype(np.int64).tolist()
    elif len(shape) == 3:
        token_idx = flat_indices % max(1, second)
        date_idx = flat_indices // max(1, second)
        meta["point_type"] = ["token"] * int(flat_indices.size)
        meta["date"] = [dates[int(idx)] if 0 <= int(idx) < len(dates) else "" for idx in date_idx]
        meta["token_index"] = token_idx.astype(np.int64).tolist()
    elif len(shape) == 4:
        steps = int(shape[1])
        n_symbols = int(shape[2])
        per_row = max(1, steps * n_symbols)
        row_idx = flat_indices // per_row
        rem = flat_indices % per_row
        lookback_idx = rem // max(1, n_symbols)
        symbol_idx = rem % max(1, n_symbols)
        meta["point_type"] = ["time_stock"] * int(flat_indices.size)
        meta["date"] = [dates[int(idx)] if 0 <= int(idx) < len(dates) else "" for idx in row_idx]
        meta["lookback_index"] = lookback_idx.astype(np.int64).tolist()
        meta["lookback_from_end"] = (steps - 1 - lookback_idx).astype(np.int64).tolist()
        meta["symbol"] = [symbols[int(idx)] if 0 <= int(idx) < len(symbols) else str(int(idx)) for idx in symbol_idx]
        meta["token_index"] = symbol_idx.astype(np.int64).tolist()
    else:
        meta["point_type"] = ["vector"] * int(flat_indices.size)
        if rows > 0:
            meta["date"] = [dates[int(idx)] if 0 <= int(idx) < len(dates) else "" for idx in (flat_indices % rows)]
    return meta


def _aux_umap_projection_frames(
    aux: dict[str, torch.Tensor],
    *,
    symbols: list[str],
    dates: list[str],
    settings: ExplainabilitySettings,
    device: torch.device,
) -> tuple[dict[str, pl.DataFrame], list[dict[str, Any]], list[str], dict[str, Any]]:
    timing: dict[str, Any] = {
        "enabled": bool(settings.umap_enabled),
        "eligible_tensors": 0,
        "projected_tensors": 0,
        "skipped_by_projection_limit": 0,
        "max_points": int(settings.umap_max_points),
        "max_projections": int(settings.umap_max_projections),
        "per_projection_s": {},
    }
    if not bool(settings.umap_enabled):
        return {}, [], ["cuML UMAP projections disabled by settings."], timing
    if device.type != "cuda":
        return {}, [], ["cuML UMAP projections require CUDA; skipped because explainability device is not CUDA."], timing

    max_points = max(0, int(settings.umap_max_points))
    timing["max_points"] = int(max_points)
    if max_points < 4:
        return {}, [], ["cuML UMAP projections skipped because explain_umap_max_points < 4."], timing

    projection_frames: dict[str, pl.DataFrame] = {}
    summaries: list[dict[str, Any]] = []
    warnings: list[str] = []
    preferred_order = (
        "stock_embedding",
        "market_tokens",
        "latent_factors",
        "z_stock",
        "z_market_context",
        "token_embedding",
        "z_factor_context",
        "dynamic_market_queries",
        "dynamic_latent_queries",
        "dynamic_market_delta",
        "dynamic_latent_delta",
    )
    preferred_names = set(preferred_order)
    eligible: list[tuple[str, torch.Tensor]] = []
    for name in preferred_order:
        value = aux.get(name)
        if torch.is_tensor(value) and value.ndim >= 3 and int(value.shape[-1]) >= 2:
            eligible.append((name, value))
    for name, value in sorted(aux.items()):
        if name in preferred_names:
            continue
        if not torch.is_tensor(value) or value.ndim < 3 or int(value.shape[-1]) < 2:
            continue
        eligible.append((name, value))
    timing["eligible_tensors"] = int(len(eligible))
    max_projections = int(settings.umap_max_projections)
    if max_projections > 0 and len(eligible) > max_projections:
        timing["skipped_by_projection_limit"] = int(len(eligible) - max_projections)
        skipped_names = [name for name, _ in eligible[max_projections:]]
        warnings.append(
            "cuML UMAP projection limit skipped aux tensors: "
            + ", ".join(skipped_names[:8])
            + ("..." if len(skipped_names) > 8 else "")
        )
        eligible = eligible[:max_projections]
    for name, value in eligible:
        projection_start = time.perf_counter()
        tensor = torch.nan_to_num(value.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
        original_shape = tuple(int(dim) for dim in tensor.shape)
        flat = tensor.reshape(-1, original_shape[-1])
        n_points = int(flat.size(0))
        if n_points < 4:
            warnings.append(f"{name}: fewer than 4 vectors; cuML UMAP skipped.")
            continue
        if n_points > max_points:
            sample_idx = torch.linspace(0, n_points - 1, max_points, device=flat.device).round().to(torch.long)
            flat_sample = flat.index_select(0, sample_idx)
        else:
            sample_idx = torch.arange(n_points, device=flat.device, dtype=torch.long)
            flat_sample = flat
        if flat_sample.device.type != "cuda":
            flat_sample = flat_sample.to(device=device, non_blocking=True)
            sample_idx = sample_idx.to(device=device, non_blocking=True)
        try:
            embedding = run_cuml_umap(
                flat_sample,
                n_neighbors=int(settings.umap_n_neighbors),
                min_dist=float(settings.umap_min_dist),
                random_state=42,
            )
        except Exception as exc:
            warnings.append(f"{name}: cuML UMAP failed: {type(exc).__name__}: {exc}")
            timing["per_projection_s"][name] = float(time.perf_counter() - projection_start)
            continue
        sample_idx_cpu = sample_idx.detach().cpu().numpy().astype(np.int64, copy=False)
        embedding_cpu = embedding.get()
        meta = _aux_point_metadata(
            name=name,
            shape=original_shape,
            flat_indices=sample_idx_cpu,
            symbols=symbols,
            dates=dates,
        )
        frame = pl.DataFrame(meta).with_columns(
            [
                pl.Series("umap_x", embedding_cpu[:, 0].astype(np.float32, copy=False)),
                pl.Series("umap_y", embedding_cpu[:, 1].astype(np.float32, copy=False)),
                pl.lit(int(sample_idx_cpu.size)).alias("sampled_points"),
                pl.lit(int(n_points)).alias("original_points"),
            ]
        )
        projection_frames[name] = frame
        x_std = float(np.nanstd(_numeric_numpy(frame, "umap_x")))
        y_std = float(np.nanstd(_numeric_numpy(frame, "umap_y")))
        summaries.append(
            {
                "name": name,
                "shape": "x".join(str(dim) for dim in original_shape),
                "original_points": int(n_points),
                "sampled_points": int(sample_idx_cpu.size),
                "method": "cuml_umap",
                "n_neighbors": int(min(max(2, int(settings.umap_n_neighbors)), int(sample_idx_cpu.size) - 1)),
                "min_dist": float(settings.umap_min_dist),
                "umap_x_std": x_std,
                "umap_y_std": y_std,
                "near_collapsed": bool(max(x_std, y_std) < 1e-4),
            }
        )
        timing["projected_tensors"] = int(timing["projected_tensors"]) + 1
        timing["per_projection_s"][name] = float(time.perf_counter() - projection_start)
        if max(x_std, y_std) < 1e-4:
            warnings.append(f"{name}: cuML UMAP projection is nearly collapsed; inspect aux tensor and token gates.")
    if not projection_frames and not warnings:
        warnings.append("No eligible transformer aux tensors were found for cuML UMAP projection.")
    return projection_frames, summaries, warnings, timing


def _stock_contribution_frame(
    weights: torch.Tensor,
    returns: torch.Tensor,
    mask: torch.Tensor,
    symbols: list[str],
) -> pl.DataFrame:
    contribution = (weights.detach().float() * returns.detach().float()).masked_fill(~mask.detach().bool(), 0.0)
    mean_weight = weights.detach().float().mean(dim=0)
    mean_abs_weight = weights.detach().float().abs().mean(dim=0)
    total_contribution = contribution.sum(dim=0)
    active_count = mask.detach().bool().sum(dim=0).clamp_min(1)
    n_symbols = int(mean_weight.numel())
    symbol_values = list(symbols[:n_symbols])
    if len(symbol_values) < n_symbols:
        symbol_values.extend(str(idx) for idx in range(len(symbol_values), n_symbols))
    frame = pl.DataFrame(
        {
            "symbol": symbol_values,
            "mean_weight": mean_weight.cpu().numpy(),
            "mean_abs_weight": mean_abs_weight.cpu().numpy(),
            "total_gross_contribution": total_contribution.cpu().numpy(),
            "mean_contribution_when_active": (total_contribution / active_count).cpu().numpy(),
            "active_count": active_count.cpu().numpy().astype(np.int64, copy=False),
        }
    )
    return frame.sort("mean_abs_weight", descending=True)



def _make_warnings(
    portfolio: dict[str, float],
    feature_summary: pl.DataFrame,
    time_summary: pl.DataFrame,
    corr: pl.DataFrame,
    aux_summary: pl.DataFrame,
) -> list[str]:
    warnings: list[str] = []
    if portfolio.get("untradable_abs_weight_sum", 0.0) > 1e-5:
        warnings.append("Non-zero weights were assigned to untradable symbols.")
    if portfolio.get("mean_abs_net", 0.0) > 0.25:
        warnings.append("Average absolute net exposure is high for a long/short portfolio.")
    if portfolio.get("max_abs_weight_max", 0.0) > 0.35:
        warnings.append("At least one day has a very concentrated single-symbol weight.")
    if portfolio.get("mean_turnover_proxy", 0.0) > 1.5:
        warnings.append("Turnover proxy is high; strategy may be relying on unstable daily flips.")
    if not _is_empty_frame(feature_summary):
        row = _first_row(feature_summary)
        if _safe_float(row.get("share", 0.0)) > 0.55:
            warnings.append(f"Feature attribution is dominated by one feature: {row.get('feature')}.")
    if not _is_empty_frame(time_summary):
        row = _first_row(time_summary)
        if _safe_float(row.get("share", 0.0)) > 0.70:
            warnings.append("Attribution is dominated by a single lookback day.")
    if not _is_empty_frame(corr):
        row = _first_row(corr)
        if max(_safe_float(row.get("abs_score_corr")), _safe_float(row.get("abs_weight_corr"))) > 0.75:
            warnings.append(
                f"Strong simple correlation detected: {row.get('source')}:{row.get('feature')} "
                f"(score_corr={_safe_float(row.get('score_corr')):.3f}, weight_corr={_safe_float(row.get('weight_corr')):.3f})."
            )
    if not _is_empty_frame(aux_summary) and "zero_fraction" in aux_summary.columns:
        collapsed = aux_summary.filter(_numeric_expr("zero_fraction") > 0.95)
        if not collapsed.is_empty() and "name" in collapsed.columns:
            warnings.append(
                "Some auxiliary representations are near-zero/collapsed: "
                + ", ".join(collapsed.get_column("name").cast(pl.String).head(5).to_list())
            )
    if not warnings:
        warnings.append("No rule-of-thumb anomaly was triggered; inspect tables before trusting the strategy.")
    return warnings



def _daily_portfolio_frame(
    weights: torch.Tensor,
    returns: torch.Tensor,
    mask: torch.Tensor,
    dates: list[str],
) -> pl.DataFrame:
    weights_f = weights.detach().float().masked_fill(~mask.detach().bool(), 0.0)
    returns_f = returns.detach().float().masked_fill(~mask.detach().bool(), 0.0)
    active = mask.detach().bool().sum(dim=1).clamp_min(1)
    strategy_return = (weights_f * returns_f).sum(dim=1)
    market_return = returns_f.sum(dim=1) / active.to(dtype=returns_f.dtype)
    long_gross = weights_f.clamp_min(0.0).sum(dim=1)
    short_gross = (-weights_f.clamp_max(0.0)).sum(dim=1)
    gross = weights_f.abs().sum(dim=1)
    net = weights_f.sum(dim=1)
    max_abs_weight = weights_f.abs().amax(dim=1)
    hhi = (weights_f.abs().square().sum(dim=1) / gross.clamp_min(1e-12).square()).nan_to_num(0.0)
    turnover = torch.zeros_like(gross)
    if int(weights_f.size(0)) > 1:
        turnover[1:] = (weights_f[1:] - weights_f[:-1]).abs().sum(dim=1)
    rows: list[dict[str, Any]] = []
    for idx, date in enumerate(dates):
        rows.append(
            {
                "date": date,
                "strategy_log_return": float(strategy_return[idx].cpu()),
                "market_log_return": float(market_return[idx].cpu()),
                "gross_exposure": float(gross[idx].cpu()),
                "net_exposure": float(net[idx].cpu()),
                "long_gross": float(long_gross[idx].cpu()),
                "short_gross": float(short_gross[idx].cpu()),
                "turnover_proxy": float(turnover[idx].cpu()),
                "max_abs_weight": float(max_abs_weight[idx].cpu()),
                "hhi": float(hhi[idx].cpu()),
            }
        )
    return pl.DataFrame(rows)



def _regime_analysis_frame(daily: pl.DataFrame) -> pl.DataFrame:
    required = {"market_log_return", "strategy_log_return", "turnover_proxy", "gross_exposure", "net_exposure"}
    if _is_empty_frame(daily) or not required.issubset(daily.columns):
        return pl.DataFrame()
    data = _with_numeric(daily, *required)
    data = data.with_columns(
        pl.when(pl.col("market_log_return") > 0.001)
        .then(pl.lit("market_up"))
        .when(pl.col("market_log_return") < -0.001)
        .then(pl.lit("market_down"))
        .otherwise(pl.lit("market_flat"))
        .alias("market_direction")
    )
    abs_market = np.abs(_numeric_numpy(data, "market_log_return", default=np.nan))
    valid = abs_market[np.isfinite(abs_market)]
    if valid.size >= 3 and float(np.max(valid)) > float(np.min(valid)):
        labels = ["low_abs_market_move", "mid_abs_market_move", "high_abs_market_move"]
        try:
            q = min(3, int(valid.size))
            edges = np.unique(np.quantile(valid, np.linspace(0.0, 1.0, q + 1)))
            if edges.size <= 2:
                bucket_values = ["single_vol_bucket"] * data.height
            else:
                bins = np.searchsorted(edges[1:-1], abs_market, side="right")
                bucket_values = [labels[min(int(idx), len(labels) - 1)] if np.isfinite(value) else "single_vol_bucket" for idx, value in zip(bins, abs_market, strict=False)]
        except ValueError:
            bucket_values = ["single_vol_bucket"] * data.height
    else:
        bucket_values = ["single_vol_bucket"] * data.height
    data = data.with_columns(pl.Series("volatility_bucket", bucket_values))
    summaries: list[pl.DataFrame] = []
    for dimension in ("market_direction", "volatility_bucket"):
        grouped = data.group_by(dimension).agg(
            [
                pl.len().alias("rows"),
                pl.col("strategy_log_return").mean().alias("mean_strategy_log_return"),
                pl.col("market_log_return").mean().alias("mean_market_log_return"),
                pl.col("turnover_proxy").mean().alias("mean_turnover_proxy"),
                pl.col("gross_exposure").mean().alias("mean_gross_exposure"),
                pl.col("net_exposure").mean().alias("mean_net_exposure"),
                (pl.col("strategy_log_return") > 0.0).mean().alias("hit_rate"),
            ]
        )
        summaries.append(
            grouped.with_columns(
                [
                    pl.lit(dimension).alias("dimension"),
                    pl.col(dimension).cast(pl.String).alias("regime"),
                ]
            ).select(
                [
                    "dimension",
                    "regime",
                    "rows",
                    "mean_strategy_log_return",
                    "mean_market_log_return",
                    "mean_turnover_proxy",
                    "mean_gross_exposure",
                    "mean_net_exposure",
                    "hit_rate",
                ]
            )
        )
    return _concat_frames(summaries)



def _case_study_frame(decisions: pl.DataFrame, daily: pl.DataFrame, top_k: int) -> pl.DataFrame:
    if _is_empty_frame(decisions) or _is_empty_frame(daily) or "date" not in decisions.columns:
        return pl.DataFrame()
    top_k = max(1, int(top_k))
    selected: list[tuple[str, str]] = []

    def add_selected(case_type: str, column: str, *, descending: bool) -> None:
        if column not in daily.columns or daily.is_empty():
            return
        row = _first_row(_with_numeric(daily, column).sort(column, descending=descending).head(1))
        if row:
            selected.append((case_type, str(row.get("date"))))

    add_selected("best_strategy_day", "strategy_log_return", descending=True)
    add_selected("worst_strategy_day", "strategy_log_return", descending=False)
    add_selected("highest_turnover_day", "turnover_proxy", descending=True)
    add_selected("highest_gross_exposure_day", "gross_exposure", descending=True)

    unique_selected: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in selected:
        if item not in seen:
            unique_selected.append(item)
            seen.add(item)
    pieces: list[pl.DataFrame] = []
    daily_rows = {str(row.get("date")): row for row in daily.to_dicts()}
    for case_type, date in unique_selected:
        chunk = decisions.filter(pl.col("date").cast(pl.String) == date)
        if chunk.is_empty():
            continue
        chunk = _with_numeric(chunk, "weight")
        chunk = chunk.with_columns(pl.col("weight").abs().alias("abs_weight")).sort("abs_weight", descending=True).head(top_k)
        chunk = chunk.with_columns(pl.lit(case_type).alias("case_type")).select(["case_type", *[col for col in chunk.columns if col != "case_type"]])
        daily_row = daily_rows.get(date)
        if daily_row is not None:
            for col in ("strategy_log_return", "market_log_return", "turnover_proxy", "gross_exposure", "net_exposure"):
                chunk = chunk.with_columns(pl.lit(daily_row.get(col)).alias(f"case_{col}"))
        pieces.append(chunk)
    return _concat_frames(pieces)



def _feature_time_top_cells(frame: pl.DataFrame, metric_name: str, top_n: int = 20) -> pl.DataFrame:
    required = {"feature", "lookback_from_end", metric_name}
    if _is_empty_frame(frame) or not required.issubset(frame.columns):
        return pl.DataFrame()
    data = _with_numeric(frame, metric_name).drop_nulls(subset=[metric_name]).sort(metric_name, descending=True).head(top_n)
    if data.is_empty():
        return data
    total = _numeric_sum(frame, metric_name)
    data = data.with_columns(
        [
            ((pl.col(metric_name) / total) if total > 0.0 else pl.lit(0.0)).alias("share"),
            pl.col("lookback_from_end").map_elements(_lookback_label, return_dtype=pl.String).alias("lookback_label"),
        ]
    )
    return _with_feature_labels(data)



def _trust_check_frame(
    portfolio: dict[str, float],
    feature_summary: pl.DataFrame,
    time_summary: pl.DataFrame,
    corr: pl.DataFrame,
    aux_summary: pl.DataFrame,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []

    def add_check(name: str, value: float, threshold: float, comparator: str, interpretation: str) -> None:
        passed = value <= threshold if comparator == "<=" else value >= threshold
        rows.append(
            {
                "check": name,
                "value": float(value),
                "threshold": float(threshold),
                "rule": f"{comparator} {threshold:g}",
                "status": "pass" if passed else "warn",
                "interpretation": interpretation,
            }
        )

    add_check("untradable_abs_weight_sum", float(portfolio.get("untradable_abs_weight_sum", 0.0)), 1e-5, "<=", "Should be zero; non-zero means the mask/tradability logic leaked into actual positions.")
    add_check("max_abs_weight_max", float(portfolio.get("max_abs_weight_max", 0.0)), 0.35, "<=", "Large single-name weights can indicate shortcut learning or unstable concentration.")
    add_check("mean_turnover_proxy", float(portfolio.get("mean_turnover_proxy", 0.0)), 1.5, "<=", "High turnover makes net performance highly fee-sensitive and less trustworthy.")
    if not _is_empty_frame(feature_summary) and "share" in feature_summary.columns:
        add_check("top_feature_attribution_share", _safe_float(_first_row(feature_summary).get("share", 0.0)), 0.55, "<=", "A single dominant feature can be a sign that the model learned a narrow rule.")
    if not _is_empty_frame(time_summary) and "share" in time_summary.columns:
        row = _first_row(time_summary.sort("share", descending=True))
        add_check("top_lookback_day_attribution_share", _safe_float(row.get("share", 0.0)), 0.70, "<=", "A single dominant day can mean the temporal model is mostly ignoring the lookback window.")
    if not _is_empty_frame(corr) and {"abs_score_corr", "abs_weight_corr"}.issubset(corr.columns):
        corr_values = corr.select([_numeric_expr("abs_score_corr"), _numeric_expr("abs_weight_corr")]).to_numpy()
        corr_max = float(np.nanmax(corr_values)) if corr_values.size else 0.0
        add_check("max_simple_feature_score_weight_corr", corr_max, 0.75, "<=", "High raw correlation can reveal price-level, liquidity, or other simple shortcut rules.")
    if not _is_empty_frame(aux_summary) and "zero_fraction" in aux_summary.columns:
        add_check("max_aux_zero_fraction", _numeric_max(aux_summary, "zero_fraction"), 0.95, "<=", "Near-zero aux tensors can indicate collapsed latent/market token representations.")
    return pl.DataFrame(rows)



def _score_head_surrogate_shap(
    x: torch.Tensor,
    scores: torch.Tensor,
    mask: torch.Tensor,
    feature_names: list[str],
    *,
    enabled: bool,
    mode: str,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any], list[str]]:
    mode = _normalize_shap_mode(mode)
    if not enabled or mode in {"off", "none"}:
        return pl.DataFrame(), pl.DataFrame(), {"enabled": bool(enabled), "method": "skipped"}, []
    warnings: list[str] = []

    x_cpu = x.detach().float().cpu()
    scores_cpu = scores.detach().float().cpu()
    mask_cpu = mask.detach().bool().cpu()
    aggregates: list[tuple[str, torch.Tensor]] = [("last", x_cpu[:, -1]), ("lookback_mean", x_cpu.mean(dim=1))]
    if int(x_cpu.size(1)) > 1:
        aggregates.append(("lookback_delta", x_cpu[:, -1] - x_cpu[:, 0]))
    components: list[np.ndarray] = []
    component_meta: list[tuple[str, str]] = []
    for source, values in aggregates:
        components.append(values.numpy())
        component_meta.extend((source, feature) for feature in feature_names)
    design = np.concatenate(components, axis=-1).reshape(-1, len(component_meta))
    target = scores_cpu.reshape(-1).numpy()
    valid = mask_cpu.reshape(-1).numpy().astype(bool)
    finite = valid & np.isfinite(target) & np.isfinite(design).all(axis=1)
    design = design[finite]
    target = target[finite]
    if design.shape[0] < max(20, 2 * design.shape[1]):
        message = "SHAP skipped because there are too few valid stock-date observations for a surrogate model."
        return pl.DataFrame(), pl.DataFrame(), {"enabled": True, "method": "skipped", "valid_rows": int(design.shape[0])}, [message]
    max_fit_rows = min(20000, int(design.shape[0]))
    if design.shape[0] > max_fit_rows:
        idx = np.linspace(0, design.shape[0] - 1, max_fit_rows).round().astype(np.int64)
        design_fit = design[idx]
        target_fit = target[idx]
    else:
        design_fit = design
        target_fit = target
    mean = design_fit.mean(axis=0, keepdims=True)
    std = design_fit.std(axis=0, keepdims=True)
    std = np.where(std < 1e-8, 1.0, std)
    design_z = (design_fit - mean) / std
    target_std = float(np.std(target_fit))
    if target_std < 1e-10:
        message = "SHAP skipped because score targets are nearly constant."
        return pl.DataFrame(), pl.DataFrame(), {"enabled": True, "method": "skipped", "valid_rows": int(design.shape[0])}, [message]
    target_mean = float(np.mean(target_fit))
    target_centered = target_fit - target_mean
    alpha = 1e-3
    xtx = design_z.T @ design_z
    rhs = design_z.T @ target_centered
    try:
        coef = np.linalg.solve(xtx + alpha * np.eye(xtx.shape[0], dtype=np.float64), rhs)
    except np.linalg.LinAlgError:
        coef = np.linalg.lstsq(xtx + alpha * np.eye(xtx.shape[0], dtype=np.float64), rhs, rcond=None)[0]
    pred = design_z @ coef + target_mean
    ss_res = float(np.sum((target_fit - pred) ** 2))
    ss_tot = float(np.sum((target_fit - target_mean) ** 2))
    r2 = _safe_float(1.0 - ss_res / ss_tot if ss_tot > 1e-20 else 0.0)
    sample_rows = min(5000, int(design_z.shape[0]))
    sample_idx = np.linspace(0, design_z.shape[0] - 1, sample_rows).round().astype(np.int64)
    sample_z = design_z[sample_idx]
    background = design_z[np.linspace(0, design_z.shape[0] - 1, min(1024, int(design_z.shape[0]))).round().astype(np.int64)]
    method = "linear_surrogate_closed_form"
    shap_values = (sample_z - background.mean(axis=0, keepdims=True)) * coef.reshape(1, -1)
    component_rows: list[dict[str, Any]] = []
    abs_values = np.nan_to_num(np.abs(shap_values), nan=0.0, posinf=0.0, neginf=0.0).mean(axis=0)
    for idx, (source, feature) in enumerate(component_meta):
        component_rows.append(
            {
                "source": source,
                "feature": feature,
                "feature_group": _feature_group(feature),
                "feature_label": _feature_label(feature),
                "shap_abs": float(abs_values[idx]),
                "surrogate_coef": float(coef.reshape(-1)[idx]),
            }
        )
    component_frame = pl.DataFrame(component_rows).sort("shap_abs", descending=True)
    summary = component_frame.group_by("feature").agg(pl.col("shap_abs").sum().alias("shap_abs"))
    summary = _with_feature_labels(summary)
    total = _numeric_sum(summary, "shap_abs")
    share_expr = (pl.col("shap_abs") / total) if total > 0.0 else pl.lit(0.0)
    summary = summary.with_columns(share_expr.alias("share"))
    if not component_frame.is_empty():
        top_source = (
            component_frame.sort("shap_abs", descending=True)
            .unique(subset=["feature"], keep="first", maintain_order=True)
            .select(["feature", pl.col("source").alias("top_source")])
        )
        summary = summary.join(top_source, on="feature", how="left")
    summary = summary.with_columns([pl.lit(r2).alias("surrogate_r2"), pl.lit(method).alias("method")])
    info = {
        "enabled": True,
        "method": method,
        "mode": mode,
        "valid_rows": int(design.shape[0]),
        "fit_rows": int(design_fit.shape[0]),
        "sample_rows": int(sample_rows),
        "num_components": int(design.shape[1]),
        "surrogate_r2": r2,
    }
    if r2 < 0.20:
        warnings.append(
            f"Score-head surrogate SHAP has low R2 ({r2:.3f}); use it as a rough global diagnostic, not a faithful local explanation."
        )
    return summary.sort("shap_abs", descending=True), component_frame, info, warnings



def explain_batch(
    model: nn.Module,
    batch: dict[str, torch.Tensor],
    *,
    feature_names: list[str],
    symbols: list[str],
    dates: list[str],
    settings: ExplainabilitySettings | None = None,
    device: torch.device | None = None,
) -> dict[str, Any]:
    total_start = time.perf_counter()
    timing: dict[str, float] = {}
    settings = settings or ExplainabilitySettings()
    device = device or next(model.parameters()).device
    model.eval()
    stage_start = time.perf_counter()
    batch = _move_batch(batch, device)
    x = torch.nan_to_num(batch["x"].float(), nan=0.0, posinf=0.0, neginf=0.0)
    returns = torch.nan_to_num(batch["future_log_returns"].float(), nan=0.0, posinf=0.0, neginf=0.0)
    mask = batch["tradable_mask"].to(device=device, dtype=torch.bool)
    _mark_elapsed(timing, "prepare_batch_s", stage_start)

    stage_start = time.perf_counter()
    with torch.no_grad():
        weights, scores, aux = _forward_outputs(model, x, mask, return_aux=True)
    selected, direction = _selection_from_weights(weights.detach(), mask, settings.top_k)
    _mark_elapsed(timing, "base_forward_s", stage_start)

    stage_start = time.perf_counter()
    grad_attr = _gradient_x_input_attribution(model, x, mask, selected, direction)
    grad_ft = _feature_time_frame(grad_attr, feature_names, "grad_x_input_abs")
    grad_feature = _feature_summary_frame(grad_ft, "grad_x_input_abs")
    grad_time = _time_summary_frame(grad_ft, "grad_x_input_abs")
    _mark_elapsed(timing, "gradient_s", stage_start)

    stage_start = time.perf_counter()
    if int(settings.ig_steps) > 0:
        ig_attr = _integrated_gradients_attribution(
            model,
            x,
            mask,
            selected,
            direction,
            settings.ig_steps,
            settings.ig_batch_size,
        )
        ig_ft = _feature_time_frame(ig_attr, feature_names, "integrated_gradients_abs")
        ig_feature = _feature_summary_frame(ig_ft, "integrated_gradients_abs")
        ig_time = _time_summary_frame(ig_ft, "integrated_gradients_abs")
    else:
        ig_ft = pl.DataFrame()
        ig_feature = pl.DataFrame()
        ig_time = pl.DataFrame()
    _mark_elapsed(timing, "integrated_gradients_s", stage_start)

    stage_start = time.perf_counter()
    if settings.perturb:
        perturb_ft, perturb_feature, perturb_diagnostics = _perturbation_importance(
            model,
            x,
            mask,
            weights,
            scores,
            feature_names,
            settings.perturb_batch_size,
            max_auto_batch_size=settings.perturb_max_auto_batch_size,
            max_input_elements=settings.perturb_max_input_elements,
        )
    else:
        perturb_ft = pl.DataFrame()
        perturb_feature = pl.DataFrame()
        perturb_diagnostics = {
            "num_perturbations": 0,
            "requested_batch_size": int(settings.perturb_batch_size),
            "max_auto_batch_size": int(settings.perturb_max_auto_batch_size),
            "max_input_elements": int(settings.perturb_max_input_elements),
            "chunk_size": 0,
            "final_chunk_size": 0,
            "forward_batches": 0,
            "attempted_forward_batches": 0,
            "oom_retries": 0,
            "oom_chunk_sizes": [],
        }
    _mark_elapsed(timing, "perturbation_s", stage_start)

    stage_start = time.perf_counter()
    shap_feature, shap_components, shap_info, shap_warnings = _score_head_surrogate_shap(
        x,
        scores,
        mask,
        feature_names,
        enabled=bool(settings.shap_enabled),
        mode=str(settings.shap_mode),
    )
    _mark_elapsed(timing, "surrogate_shap_s", stage_start)

    stage_start = time.perf_counter()
    corr = _feature_correlations(x, scores, weights, mask, feature_names)
    decisions = _decision_rows(weights, scores, returns, mask, dates, symbols, settings.top_k)
    stock_contrib = _stock_contribution_frame(weights, returns, mask, symbols)
    portfolio = _portfolio_summary(weights, returns, mask)
    daily = _daily_portfolio_frame(weights, returns, mask, dates)
    regime = _regime_analysis_frame(daily) if bool(settings.regime_analysis) else pl.DataFrame()
    case_studies = _case_study_frame(decisions, daily, int(settings.case_study_top_k))
    _mark_elapsed(timing, "tabular_diagnostics_s", stage_start)

    stage_start = time.perf_counter()
    aux_frame, aux_dim_frames = _aux_summary(aux)
    aux_projection_frames, aux_projection_summary, aux_projection_warnings, aux_projection_timing = _aux_umap_projection_frames(
        aux,
        symbols=symbols,
        dates=dates,
        settings=settings,
        device=device,
    )
    _mark_elapsed(timing, "aux_diagnostics_s", stage_start)

    stage_start = time.perf_counter()
    warnings = _make_warnings(portfolio, grad_feature, grad_time, corr, aux_frame)
    warnings.extend(aux_projection_warnings)
    warnings.extend(shap_warnings)
    trust_checks = _trust_check_frame(portfolio, grad_feature, grad_time, corr, aux_frame)
    grad_top_cells = _feature_time_top_cells(grad_ft, "grad_x_input_abs")
    ig_top_cells = _feature_time_top_cells(ig_ft, "integrated_gradients_abs")
    perturb_top_cells = _feature_time_top_cells(perturb_ft, "weight_abs_delta")
    attribution_lookback = 0
    if not _is_empty_frame(grad_ft) and "lookback_from_end" in grad_ft.columns:
        attribution_lookback = int(_numeric_max(grad_ft, "lookback_from_end") + 1)
    _mark_elapsed(timing, "postprocess_s", stage_start)
    timing["total_s"] = float(time.perf_counter() - total_start)

    return {
        "summary": {
            "portfolio": portfolio,
            "rows": len(dates),
            "top_k": int(settings.top_k),
            "ig_steps": int(settings.ig_steps),
            "ig_batch_size": int(settings.ig_batch_size),
            "report_style": _normalize_report_style(settings.report_style),
            "plot_theme": _normalize_plot_theme(settings.plot_theme),
            "standard_plots": bool(settings.standard_plots),
            "interactive_plots": bool(settings.interactive_plots),
            "shap_enabled": bool(settings.shap_enabled),
            "perturb_batch_size": int(settings.perturb_batch_size),
            "perturb_max_auto_batch_size": int(settings.perturb_max_auto_batch_size),
            "perturb_max_input_elements": int(settings.perturb_max_input_elements),
            "perturb_diagnostics": perturb_diagnostics,
            "shap_mode": _normalize_shap_mode(settings.shap_mode),
            "shap_info": shap_info,
            "case_study_top_k": int(settings.case_study_top_k),
            "regime_analysis": bool(settings.regime_analysis),
            "fold_stability": bool(settings.fold_stability),
            "attribution_lookback": attribution_lookback,
            "umap_enabled": bool(settings.umap_enabled),
            "umap_max_points": int(settings.umap_max_points),
            "umap_max_projections": int(settings.umap_max_projections),
            "umap_method": "cuml_umap",
            "aux_projection_summary": aux_projection_summary,
            "aux_projection_timing": aux_projection_timing,
            "timing": timing,
            "warnings": warnings,
        },
        "frames": {
            "feature_time_gradient": grad_ft,
            "feature_importance_gradient": grad_feature,
            "time_importance_gradient": grad_time,
            "feature_time_integrated_gradients": ig_ft,
            "feature_importance_integrated_gradients": ig_feature,
            "time_importance_integrated_gradients": ig_time,
            "feature_time_perturbation": perturb_ft,
            "feature_importance_perturbation": perturb_feature,
            "feature_importance_shap": shap_feature,
            "shap_components": shap_components,
            "top_feature_time_gradient_cells": grad_top_cells,
            "top_feature_time_integrated_gradients_cells": ig_top_cells,
            "top_feature_time_perturbation_cells": perturb_top_cells,
            "feature_correlations": corr,
            "top_decisions": decisions,
            "daily_portfolio": daily,
            "regime_analysis": regime,
            "decision_case_studies": case_studies,
            "trust_checks": trust_checks,
            "stock_contributions": stock_contrib,
            "aux_summary": aux_frame,
        },
        "aux_dim_frames": aux_dim_frames,
        "aux_projection_frames": aux_projection_frames,
        "_core": {
            "weights": weights.detach().cpu(),
            "scores": scores.detach().cpu(),
            "returns": returns.detach().cpu(),
            "mask": mask.detach().cpu(),
        },
    }


def _weighted_feature_time_from_chunks(
    chunk_results: list[tuple[dict[str, Any], int]],
    frame_name: str,
    metric_names: tuple[str, ...],
    total_rows: int,
) -> pl.DataFrame:
    pieces: list[pl.DataFrame] = []
    for result, rows in chunk_results:
        frame = result.get("frames", {}).get(frame_name, pl.DataFrame())
        if _is_empty_frame(frame):
            continue
        data = frame.clone()
        expressions = []
        for metric_name in metric_names:
            if metric_name in data.columns:
                expressions.append((_numeric_expr(metric_name).fill_null(0.0) * float(rows)).alias(metric_name))
        if expressions:
            data = data.with_columns(expressions)
        pieces.append(data)
    if not pieces:
        return pl.DataFrame()
    combined = _concat_frames(pieces)
    group_cols = [col for col in ("lookback_index", "lookback_from_end", "feature", "feature_group", "feature_label") if col in combined.columns]
    value_cols = [col for col in metric_names if col in combined.columns]
    if not group_cols or not value_cols:
        return combined
    out = combined.group_by(group_cols).agg([_numeric_expr(col).fill_null(0.0).sum().alias(col) for col in value_cols])
    denom = max(1.0, float(total_rows))
    return out.with_columns([(pl.col(col) / denom).alias(col) for col in value_cols])



def _combine_perturbation_summary(frame: pl.DataFrame) -> pl.DataFrame:
    if _is_empty_frame(frame):
        return pl.DataFrame()
    value_cols = [col for col in ("weight_abs_delta", "score_abs_delta") if col in frame.columns]
    if not value_cols:
        return pl.DataFrame()
    summary = frame.group_by("feature").agg([_numeric_expr(col).fill_null(0.0).sum().alias(col) for col in value_cols])
    summary = _with_feature_labels(summary)
    total = _numeric_sum(summary, "weight_abs_delta") if "weight_abs_delta" in summary.columns else 0.0
    share_expr = (pl.col("weight_abs_delta") / total) if total > 0.0 else pl.lit(0.0)
    summary = summary.with_columns(share_expr.alias("weight_delta_share"))
    return summary.sort("weight_abs_delta", descending=True) if "weight_abs_delta" in summary.columns else summary



def _combine_shap_feature_from_chunks(
    chunk_results: list[tuple[dict[str, Any], int]],
    total_rows: int,
) -> pl.DataFrame:
    pieces: list[pl.DataFrame] = []
    for result, rows in chunk_results:
        frame = result.get("frames", {}).get("feature_importance_shap", pl.DataFrame())
        if _is_empty_frame(frame) or "feature" not in frame.columns or "shap_abs" not in frame.columns:
            continue
        pieces.append(frame.with_columns((_numeric_expr("shap_abs").fill_null(0.0) * float(rows)).alias("shap_abs")))
    if not pieces:
        return pl.DataFrame()
    combined = _concat_frames(pieces)
    summary = combined.group_by("feature").agg(pl.col("shap_abs").sum().alias("shap_abs"))
    summary = summary.with_columns((pl.col("shap_abs") / max(1.0, float(total_rows))).alias("shap_abs"))
    summary = _with_feature_labels(summary)
    total = _numeric_sum(summary, "shap_abs")
    share_expr = (pl.col("shap_abs") / total) if total > 0.0 else pl.lit(0.0)
    return summary.with_columns(share_expr.alias("share")).sort("shap_abs", descending=True)



def _concat_chunk_frame(
    chunk_results: list[tuple[dict[str, Any], int]],
    frame_name: str,
    *,
    add_chunk_id: bool = False,
) -> pl.DataFrame:
    pieces: list[pl.DataFrame] = []
    for chunk_id, (result, _) in enumerate(chunk_results):
        frame = result.get("frames", {}).get(frame_name, pl.DataFrame())
        if _is_empty_frame(frame):
            continue
        data = frame.clone()
        if add_chunk_id:
            data = data.with_columns(pl.lit(int(chunk_id)).alias("explain_chunk_id")).select(["explain_chunk_id", *data.columns])
        pieces.append(data)
    return _concat_frames(pieces)



def _combine_aux_summary_from_chunks(chunk_results: list[tuple[dict[str, Any], int]]) -> pl.DataFrame:
    pieces: list[pl.DataFrame] = []
    for result, rows in chunk_results:
        frame = result.get("frames", {}).get("aux_summary", pl.DataFrame())
        if _is_empty_frame(frame) or "name" not in frame.columns:
            continue
        pieces.append(frame.with_columns(pl.lit(float(rows)).alias("_rows")))
    if not pieces:
        return pl.DataFrame()
    combined = _concat_frames(pieces)
    rows: list[dict[str, Any]] = []
    weighted_cols = ["mean", "std", "mean_abs", "zero_fraction", "finite_fraction"]
    for group in combined.partition_by("name", as_dict=False):
        name = _first_row(group).get("name")
        weights = _numeric_numpy(group, "_rows", default=0.0)
        denom = float(np.nansum(weights)) or 1.0
        row: dict[str, Any] = {"name": name, "shape": str(_first_row(group).get("shape", ""))}
        for col in weighted_cols:
            if col in group.columns:
                values = _numeric_numpy(group, col, default=0.0)
                row[col] = float(np.nansum(values * weights) / denom)
        if "max_abs" in group.columns:
            row["max_abs"] = _numeric_max(group, "max_abs")
        rows.append(row)
    out = pl.DataFrame(rows)
    return out.sort("mean_abs", descending=True) if "mean_abs" in out.columns else out



def _combine_aux_dim_frames_from_chunks(chunk_results: list[tuple[dict[str, Any], int]]) -> dict[str, pl.DataFrame]:
    by_name: dict[str, list[pl.DataFrame]] = {}
    for result, rows in chunk_results:
        for name, frame in result.get("aux_dim_frames", {}).items():
            if _is_empty_frame(frame) or "dim" not in frame.columns:
                continue
            by_name.setdefault(str(name), []).append(frame.with_columns(pl.lit(float(rows)).alias("_rows")))
    out: dict[str, pl.DataFrame] = {}
    for name, pieces in by_name.items():
        combined = _concat_frames(pieces)
        rows = []
        for group in combined.partition_by("dim", as_dict=False):
            row0 = _first_row(group)
            weights = _numeric_numpy(group, "_rows", default=0.0)
            values = _numeric_numpy(group, "mean_abs", default=0.0)
            denom = float(np.nansum(weights)) or 1.0
            rows.append({"dim": int(row0.get("dim", 0)), "mean_abs": float(np.nansum(values * weights) / denom)})
        frame = pl.DataFrame(rows).sort("mean_abs", descending=True)
        total = _numeric_sum(frame, "mean_abs")
        share_expr = (pl.col("mean_abs") / total) if total > 0.0 else pl.lit(0.0)
        out[name] = frame.with_columns(share_expr.alias("share"))
    return out



def _sum_chunk_timings(chunk_results: list[tuple[dict[str, Any], int]]) -> dict[str, float]:
    timings: dict[str, float] = {}
    for result, _ in chunk_results:
        for key, value in result.get("summary", {}).get("timing", {}).items():
            try:
                timings[key] = timings.get(key, 0.0) + float(value)
            except (TypeError, ValueError):
                continue
    return timings


def _merge_perturb_diagnostics(chunk_results: list[tuple[dict[str, Any], int]]) -> dict[str, Any]:
    merged = {
        "num_perturbations": 0,
        "requested_batch_size": 0,
        "max_auto_batch_size": 0,
        "max_input_elements": 0,
        "chunk_size": 0,
        "final_chunk_size": 0,
        "forward_batches": 0,
        "attempted_forward_batches": 0,
        "oom_retries": 0,
        "oom_chunk_sizes": [],
    }
    for result, _ in chunk_results:
        diag = result.get("summary", {}).get("perturb_diagnostics", {})
        merged["num_perturbations"] = max(int(merged["num_perturbations"]), int(diag.get("num_perturbations", 0) or 0))
        for key in ("requested_batch_size", "max_auto_batch_size", "max_input_elements", "chunk_size", "final_chunk_size"):
            merged[key] = max(int(merged[key]), int(diag.get(key, 0) or 0))
        for key in ("forward_batches", "attempted_forward_batches", "oom_retries"):
            merged[key] = int(merged[key]) + int(diag.get(key, 0) or 0)
        merged["oom_chunk_sizes"].extend(int(v) for v in diag.get("oom_chunk_sizes", []) or [])
    return merged


def _combine_chunked_explainability_results(
    chunk_results: list[tuple[dict[str, Any], int]],
    *,
    batch: dict[str, torch.Tensor],
    feature_names: list[str],
    symbols: list[str],
    dates: list[str],
    settings: ExplainabilitySettings,
    row_chunk_diagnostics: dict[str, Any],
    total_elapsed_s: float,
) -> dict[str, Any]:
    total_rows = max(1, len(dates))
    grad_ft = _weighted_feature_time_from_chunks(
        chunk_results,
        "feature_time_gradient",
        ("grad_x_input_abs",),
        total_rows,
    )
    ig_ft = _weighted_feature_time_from_chunks(
        chunk_results,
        "feature_time_integrated_gradients",
        ("integrated_gradients_abs",),
        total_rows,
    )
    perturb_ft = _weighted_feature_time_from_chunks(
        chunk_results,
        "feature_time_perturbation",
        ("weight_abs_delta", "score_abs_delta"),
        total_rows,
    )
    grad_feature = _feature_summary_frame(grad_ft, "grad_x_input_abs")
    grad_time = _time_summary_frame(grad_ft, "grad_x_input_abs")
    ig_feature = _feature_summary_frame(ig_ft, "integrated_gradients_abs")
    ig_time = _time_summary_frame(ig_ft, "integrated_gradients_abs")
    perturb_feature = _combine_perturbation_summary(perturb_ft)
    shap_feature = _combine_shap_feature_from_chunks(chunk_results, total_rows)

    weights = torch.cat([result["_core"]["weights"] for result, _ in chunk_results], dim=0)
    scores = torch.cat([result["_core"]["scores"] for result, _ in chunk_results], dim=0)
    returns = torch.cat([result["_core"]["returns"] for result, _ in chunk_results], dim=0)
    mask = torch.cat([result["_core"]["mask"] for result, _ in chunk_results], dim=0).bool()
    x_cpu = torch.nan_to_num(batch["x"].detach().float(), nan=0.0, posinf=0.0, neginf=0.0)

    corr = _feature_correlations(x_cpu, scores, weights, mask, feature_names)
    decisions = _decision_rows(weights, scores, returns, mask, dates, symbols, settings.top_k)
    stock_contrib = _stock_contribution_frame(weights, returns, mask, symbols)
    portfolio = _portfolio_summary(weights, returns, mask)
    daily = _daily_portfolio_frame(weights, returns, mask, dates)
    regime = _regime_analysis_frame(daily) if bool(settings.regime_analysis) else pl.DataFrame()
    case_studies = _case_study_frame(decisions, daily, int(settings.case_study_top_k))
    aux_frame = _combine_aux_summary_from_chunks(chunk_results)
    aux_dim_frames = _combine_aux_dim_frames_from_chunks(chunk_results)
    first_result = chunk_results[0][0]
    aux_projection_frames = {
        name: frame
        for name, frame in first_result.get("aux_projection_frames", {}).items()
        if not _is_empty_frame(frame)
    }

    warnings = _make_warnings(portfolio, grad_feature, grad_time, corr, aux_frame)
    warnings.append(
        "Explainability ran with row microbatching to fit the full stock universe in GPU memory; aux UMAP projections use the first row chunk because UMAP coordinates are not additive across chunks."
    )
    for result, _ in chunk_results:
        warnings.extend(str(item) for item in result.get("summary", {}).get("warnings", []) if str(item) not in warnings)

    trust_checks = _trust_check_frame(portfolio, grad_feature, grad_time, corr, aux_frame)
    attribution_lookback = 0
    if not _is_empty_frame(grad_ft) and "lookback_from_end" in grad_ft.columns:
        attribution_lookback = int(_numeric_max(grad_ft, "lookback_from_end") + 1)

    timing = _sum_chunk_timings(chunk_results)
    timing["total_s"] = float(total_elapsed_s)
    timing["row_microbatch_chunks"] = float(len(chunk_results))

    shap_components = _concat_chunk_frame(chunk_results, "shap_components", add_chunk_id=True)
    aux_projection_summary = first_result.get("summary", {}).get("aux_projection_summary", [])
    aux_projection_timing = first_result.get("summary", {}).get("aux_projection_timing", {})
    perturb_diagnostics = _merge_perturb_diagnostics(chunk_results)

    return {
        "summary": {
            "portfolio": portfolio,
            "rows": len(dates),
            "top_k": int(settings.top_k),
            "ig_steps": int(settings.ig_steps),
            "ig_batch_size": int(settings.ig_batch_size),
            "report_style": _normalize_report_style(settings.report_style),
            "plot_theme": _normalize_plot_theme(settings.plot_theme),
            "standard_plots": bool(settings.standard_plots),
            "interactive_plots": bool(settings.interactive_plots),
            "shap_enabled": bool(settings.shap_enabled),
            "perturb_batch_size": int(settings.perturb_batch_size),
            "perturb_max_auto_batch_size": int(settings.perturb_max_auto_batch_size),
            "perturb_max_input_elements": int(settings.perturb_max_input_elements),
            "perturb_diagnostics": perturb_diagnostics,
            "shap_mode": _normalize_shap_mode(settings.shap_mode),
            "shap_info": {"mode": "row_microbatch_combined", "chunks": int(len(chunk_results))},
            "case_study_top_k": int(settings.case_study_top_k),
            "regime_analysis": bool(settings.regime_analysis),
            "fold_stability": bool(settings.fold_stability),
            "attribution_lookback": attribution_lookback,
            "umap_enabled": bool(settings.umap_enabled),
            "umap_max_points": int(settings.umap_max_points),
            "umap_max_projections": int(settings.umap_max_projections),
            "umap_method": "cuml_umap",
            "aux_projection_summary": aux_projection_summary,
            "aux_projection_timing": aux_projection_timing,
            "row_chunking": row_chunk_diagnostics,
            "timing": timing,
            "warnings": warnings,
        },
        "frames": {
            "feature_time_gradient": grad_ft,
            "feature_importance_gradient": grad_feature,
            "time_importance_gradient": grad_time,
            "feature_time_integrated_gradients": ig_ft,
            "feature_importance_integrated_gradients": ig_feature,
            "time_importance_integrated_gradients": ig_time,
            "feature_time_perturbation": perturb_ft,
            "feature_importance_perturbation": perturb_feature,
            "feature_importance_shap": shap_feature,
            "shap_components": shap_components,
            "top_feature_time_gradient_cells": _feature_time_top_cells(grad_ft, "grad_x_input_abs"),
            "top_feature_time_integrated_gradients_cells": _feature_time_top_cells(ig_ft, "integrated_gradients_abs"),
            "top_feature_time_perturbation_cells": _feature_time_top_cells(perturb_ft, "weight_abs_delta"),
            "feature_correlations": corr,
            "top_decisions": decisions,
            "daily_portfolio": daily,
            "regime_analysis": regime,
            "decision_case_studies": case_studies,
            "trust_checks": trust_checks,
            "stock_contributions": stock_contrib,
            "aux_summary": aux_frame,
        },
        "aux_dim_frames": aux_dim_frames,
        "aux_projection_frames": aux_projection_frames,
    }


def explain_batch_row_chunked(
    model: nn.Module,
    batch: dict[str, torch.Tensor],
    *,
    feature_names: list[str],
    symbols: list[str],
    dates: list[str],
    settings: ExplainabilitySettings,
    device: torch.device,
) -> dict[str, Any]:
    n_rows = int(batch["x"].size(0))
    effective_settings = settings
    fallback_warning: str | None = None
    used_fallback = False

    while True:
        total_start = time.perf_counter()
        row_chunk_size, diagnostics = _auto_explain_row_chunk_size(batch, effective_settings, device)
        if used_fallback:
            diagnostics = {**diagnostics, "cuda_oom_fallback": True}
        try:
            if row_chunk_size >= n_rows:
                result = explain_batch(
                    model,
                    batch,
                    feature_names=feature_names,
                    symbols=symbols,
                    dates=dates,
                    settings=effective_settings,
                    device=device,
                )
                result["summary"]["row_chunking"] = diagnostics
                if fallback_warning is not None:
                    _append_summary_warning(result, fallback_warning)
                return result

            print(
                "[explain] row microbatching enabled: "
                f"rows={n_rows}, row_chunk_size={row_chunk_size}, symbols={diagnostics.get('symbols')}, "
                f"free_gb={diagnostics.get('free_gb', 0.0):.2f}"
            )
            chunk_results: list[tuple[dict[str, Any], int]] = []
            for chunk_id, start in enumerate(range(0, n_rows, row_chunk_size), start=1):
                end = min(n_rows, start + row_chunk_size)
                chunk_settings = effective_settings if chunk_id == 1 else replace(effective_settings, umap_enabled=False)
                chunk = _slice_batch_rows(batch, start, end)
                chunk_dates = dates[start:end]
                result = explain_batch(
                    model,
                    chunk,
                    feature_names=feature_names,
                    symbols=symbols,
                    dates=chunk_dates,
                    settings=chunk_settings,
                    device=device,
                )
                chunk_results.append((result, end - start))
                del chunk, result
                _clear_explainability_runtime_cache()
                print(f"[explain] completed row chunk {chunk_id}: rows {start}:{end}")

            diagnostics = {**diagnostics, "chunk_count": len(chunk_results)}
            combined = _combine_chunked_explainability_results(
                chunk_results,
                batch=batch,
                feature_names=feature_names,
                symbols=symbols,
                dates=dates,
                settings=effective_settings,
                row_chunk_diagnostics=diagnostics,
                total_elapsed_s=float(time.perf_counter() - total_start),
            )
            if fallback_warning is not None:
                _append_summary_warning(combined, fallback_warning)
            return combined
        except RuntimeError as exc:
            if not _is_cuda_oom(exc) or used_fallback:
                raise
            if bool(effective_settings.strict_no_fallback):
                raise RuntimeError(
                    "CUDA OOM during explainability; strict_no_fallback=true so "
                    "VRAM-safe degraded explainability fallback is disabled."
                ) from exc
            fallback_settings = _cuda_oom_fallback_settings(effective_settings)
            if fallback_settings is None:
                raise
            used_fallback = True
            effective_settings = fallback_settings
            fallback_warning = (
                "CUDA OOM during explainability; retried with VRAM-safe fallback "
                "(Integrated Gradients disabled, perturbation disabled, UMAP disabled)."
            )
            _clear_explainability_runtime_cache()
            print(f"[explain] {fallback_warning}")


def _write_markdown_report(
    path: Path,
    *,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    frames: dict[str, pl.DataFrame],
) -> None:
    def _render_frame(frame: pl.DataFrame) -> str:
        return _render_table_markdown(frame, limit=20)

    warnings = summary.get("warnings", [])
    portfolio = summary.get("portfolio", {})
    aux_projection_summary = summary.get("aux_projection_summary", [])
    lines: list[str] = []
    lines.append("# Model Explainability Report")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    for key, value in metadata.items():
        lines.append(f"- **{key}**: `{value}`")
    lines.append("")
    lines.append("## What This Explains")
    lines.append("")
    lines.extend(
        [
            "- Portfolio decisions: weights, scores, long/short side, future return contribution.",
            "- Feature and lookback-day attribution: gradient x input and Integrated Gradients.",
            "- Perturbation sensitivity: score/weight changes when each feature-day slice is zeroed.",
            "- Auxiliary representations: branch/latent tensor norms and collapse checks.",
            "- cuML UMAP projections: 2D maps of transformer aux tensors such as stock embeddings, latent factors, market tokens, and dynamic token deltas.",
            "- Plausibility warnings: concentration, exposure, turnover proxy, single-feature dominance, simple feature correlations.",
        ]
    )
    lines.append("")
    lines.append("## How To Read The Diagnostics")
    lines.append("")
    lines.extend(
        [
            "- `gradient x input`: fast local sensitivity around the sampled decisions; useful for spotting dominant features or single-day dependence.",
            "- `integrated gradients`: smoother attribution from a zero baseline to the actual window; usually more stable than raw gradients but costs multiple forward/backward passes.",
            "- `perturbation weight_abs_delta`: decision-level sensitivity after zeroing one feature-day slice; prefer this over score delta when masked scores use sentinel values.",
            "- `feature_correlations`: simple linear checks between raw feature values and score/weight; high values can reveal price-level or liquidity shortcuts.",
            "- `aux_summary` and `aux_dims`: tensor norm and dimension usage checks; very high zero fraction or one dominant dimension can indicate collapsed representations.",
            "- `aux_projections`: cuML UMAP maps of high-dimensional transformer states; collapsed clouds, isolated single-token islands, or date-only bands deserve manual inspection.",
        ]
    )
    lines.append("")
    lines.append("## Backend Notes")
    lines.append("")
    lines.extend(
        [
            "- Batch artifacts are static PNG/CSV. Datashader is used for dense explainability visuals when available.",
            "- cuML UMAP is the dimensionality-reduction method for aux projections. If CUDA/cuML is unavailable, projection tables are not fabricated.",
            "- Plotly is best reserved for interactive dashboards. PyQtGraph is best for live training curves from scalar streams, not fold artifact generation.",
            "- Surrogate SHAP is computed from a fitted score-head linear surrogate; exact model SHAP is avoided because full-market tensor windows make it expensive.",
        ]
    )
    lines.append("")
    lines.append("## Warnings")
    lines.append("")
    for warning in warnings:
        lines.append(f"- {warning}")
    lines.append("")
    lines.append("## Portfolio Summary")
    lines.append("")
    for key, value in portfolio.items():
        lines.append(f"- `{key}`: {value:.6g}" if isinstance(value, float) else f"- `{key}`: {value}")
    lines.append("")
    if aux_projection_summary:
        lines.append("## cuML UMAP Aux Projections")
        lines.append("")
        lines.append(_render_frame(pl.DataFrame(aux_projection_summary)))
        lines.append("")
    plots = summary.get("plots_generated", [])
    if plots:
        lines.append("## Plots")
        lines.append("")
        for plot in plots:
            lines.append(f"- `{plot}`")
        lines.append("")
    for name in (
        "feature_importance_gradient",
        "feature_importance_integrated_gradients",
        "feature_importance_perturbation",
        "feature_correlations",
        "top_decisions",
        "stock_contributions",
        "aux_summary",
    ):
        frame = frames.get(name)
        if _is_empty_frame(frame):
            continue
        lines.append(f"## {name}")
        lines.append("")
        lines.append(_render_frame(frame))
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")



@lru_cache(maxsize=1)
def _setup_paper_plotting() -> tuple[Any, Any]:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_theme(
        context="paper",
        style="whitegrid",
        rc={
            "figure.facecolor": PAPER_TOKENS["surface"],
            "axes.facecolor": PAPER_TOKENS["panel"],
            "axes.edgecolor": PAPER_TOKENS["axis"],
            "axes.labelcolor": PAPER_TOKENS["ink"],
            "xtick.color": PAPER_TOKENS["muted"],
            "ytick.color": PAPER_TOKENS["muted"],
            "grid.color": PAPER_TOKENS["grid"],
            "grid.linestyle": "-",
            "font.family": "sans-serif",
            "font.sans-serif": ["Aptos", "Inter", "Segoe UI", "DejaVu Sans", "Arial"],
            "savefig.facecolor": PAPER_TOKENS["surface"],
            "savefig.bbox": "tight",
        },
    )
    return plt, sns


def _add_paper_header(fig: Any, ax: Any, title: str, subtitle: str) -> None:
    import textwrap

    ax.set_title("")
    title_wrapped = textwrap.fill(str(title).strip(), width=88, break_long_words=False)
    subtitle_wrapped = textwrap.fill(str(subtitle).strip(), width=124, break_long_words=False)
    title_lines = title_wrapped.count("\n") + 1
    subtitle_lines = subtitle_wrapped.count("\n") + 1
    fig.subplots_adjust(top=max(0.58, 0.84 - 0.035 * max(0, title_lines - 1) - 0.025 * max(0, subtitle_lines - 1)))
    left = ax.get_position().x0
    fig.text(left, 0.975, title_wrapped, ha="left", va="top", fontsize=15, fontweight="bold", color=PAPER_TOKENS["ink"])
    fig.text(left, 0.925, subtitle_wrapped, ha="left", va="top", fontsize=10.5, color=PAPER_TOKENS["muted"])


def _finish_paper_axes(ax: Any) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(PAPER_TOKENS["axis"])
    ax.spines["bottom"].set_color(PAPER_TOKENS["axis"])
    ax.tick_params(axis="both", labelsize=9)


def _format_share(value: Any) -> str:
    return f"{100.0 * _safe_float(value):.1f}%"


def _paper_scope(metadata: dict[str, Any], summary: dict[str, Any]) -> str:
    parts = []
    for key in ("fold_id", "split", "date_start", "date_end"):
        value = metadata.get(key)
        if value is not None:
            parts.append(f"{key}={value}")
    rows = summary.get("rows")
    if rows is not None:
        parts.append(f"sample_rows={rows}")
    return "; ".join(parts)


def _global_attribution_table(frames: dict[str, pl.DataFrame]) -> pl.DataFrame:
    tables: list[pl.DataFrame] = []

    def add(frame_name: str, value_col: str, share_col: str, prefix: str) -> None:
        frame = frames.get(frame_name, pl.DataFrame())
        if _is_empty_frame(frame) or "feature" not in frame.columns or value_col not in frame.columns:
            return
        cols = ["feature", value_col]
        if "share" in frame.columns:
            cols.append("share")
        if "weight_delta_share" in frame.columns:
            cols.append("weight_delta_share")
        data = frame.select(cols).rename({value_col: f"{prefix}_value"})
        if "share" in data.columns:
            data = data.rename({"share": f"{prefix}_share"})
        if "weight_delta_share" in data.columns:
            data = data.rename({"weight_delta_share": f"{prefix}_share"})
        if f"{prefix}_share" not in data.columns:
            total = _numeric_sum(data, f"{prefix}_value")
            share_expr = (pl.col(f"{prefix}_value") / total) if total > 0.0 else pl.lit(0.0)
            data = data.with_columns(share_expr.alias(f"{prefix}_share"))
        tables.append(data)

    add("feature_importance_gradient", "grad_x_input_abs", "share", "gradient")
    add("feature_importance_integrated_gradients", "integrated_gradients_abs", "share", "integrated_gradients")
    add("feature_importance_perturbation", "weight_abs_delta", "weight_delta_share", "perturbation_weight")
    add("feature_importance_shap", "shap_abs", "share", "shap")
    if not tables:
        return pl.DataFrame()
    out = tables[0]
    for table in tables[1:]:
        out = out.join(table, on="feature", how="full", coalesce=True)
    out = _with_feature_labels(out)
    share_cols = [col for col in out.columns if col.endswith("_share")]
    if share_cols:
        out = out.with_columns([pl.col(col).fill_null(0.0).alias(col) for col in share_cols])
        out = out.with_columns(pl.mean_horizontal([pl.col(col) for col in share_cols]).alias("mean_available_share"))
    else:
        out = out.with_columns(pl.lit(0.0).alias("mean_available_share"))
    return out.sort("mean_available_share", descending=True)



def _write_paper_tables(
    output_dir: Path,
    *,
    frames: dict[str, pl.DataFrame],
    summary: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, str]:
    table_dir = output_dir / "paper_tables"
    table_dir.mkdir(parents=True, exist_ok=True)
    tables: dict[str, pl.DataFrame] = {}
    tables["global_feature_attribution"] = _global_attribution_table(frames)
    top_cell_frames: list[pl.DataFrame] = []
    for name, method in (
        ("top_feature_time_gradient_cells", "gradient_x_input"),
        ("top_feature_time_integrated_gradients_cells", "integrated_gradients"),
        ("top_feature_time_perturbation_cells", "perturbation_weight_delta"),
    ):
        frame = frames.get(name, pl.DataFrame())
        if not _is_empty_frame(frame):
            top_cell_frames.append(frame.with_columns(pl.lit(method).alias("method")).select(["method", *frame.columns]))
    tables["feature_time_top_cells"] = _concat_frames(top_cell_frames)
    for name in (
        "daily_portfolio",
        "regime_analysis",
        "decision_case_studies",
        "trust_checks",
        "feature_correlations",
        "feature_importance_shap",
        "shap_components",
        "aux_summary",
    ):
        tables[name] = frames.get(name, pl.DataFrame())
    lookback_expected = metadata.get("config_lookback")
    lookback_observed = summary.get("attribution_lookback")
    tables["lookback_consistency"] = pl.DataFrame(
        [
            {
                "config_lookback": lookback_expected,
                "attribution_lookback": lookback_observed,
                "status": "match" if lookback_expected is None or int(lookback_expected) == int(lookback_observed or 0) else "warn",
                "interpretation": "Attribution days should match the configured lookback; mismatch means the artifact is not lookback-complete or came from an older run.",
            }
        ]
    )
    written: dict[str, str] = {}
    for name, table in tables.items():
        if _is_empty_frame(table):
            continue
        path = table_dir / f"{name}.csv"
        _write_csv(table, path)
        written[name] = str(path.relative_to(output_dir))
    return written



def _plot_paper_global_attribution(table: pl.DataFrame, output_path: Path, *, subtitle: str) -> None:
    if _is_empty_frame(table) or "feature_label" not in table.columns:
        return
    share_cols = [
        ("gradient_share", "Grad x input"),
        ("integrated_gradients_share", "Integrated gradients"),
        ("perturbation_weight_share", "Perturbation"),
        ("shap_share", "Surrogate SHAP"),
    ]
    available = [(col, label) for col, label in share_cols if col in table.columns]
    if not available:
        return
    data = table.head(14)
    melted: list[pl.DataFrame] = []
    for col, label in available:
        melted.append(
            data.select(
                [
                    pl.col("feature_label").cast(pl.String).alias("feature_label"),
                    _numeric_expr(col).alias("share"),
                ]
            )
            .drop_nulls(subset=["feature_label", "share"])
            .with_columns(pl.lit(label).alias("method"))
        )
    plot_data = _concat_frames(melted)
    if plot_data.is_empty():
        return
    plt, sns = _setup_paper_plotting()
    feature_count = int(data.select(pl.col("feature_label").n_unique()).item())
    fig_height = max(5.2, 0.42 * feature_count + 2.1)
    fig, ax = plt.subplots(figsize=(12.5, fig_height), dpi=160)
    palette = {
        "Grad x input": PAPER_TOKENS["blue_mid"],
        "Integrated gradients": PAPER_TOKENS["gold_mid"],
        "Perturbation": PAPER_TOKENS["orange_mid"],
        "Surrogate SHAP": PAPER_TOKENS["olive_mid"],
    }
    order = _string_list(data, "feature_label")[::-1]
    methods = _string_list(plot_data.select(pl.col("method").unique(maintain_order=True)), "method")
    sns.barplot(
        data=_to_plot_data(plot_data),
        y="feature_label",
        x="share",
        hue="method",
        order=order,
        palette={key: palette[key] for key in methods if key in palette},
        ax=ax,
    )
    ax.set_xlabel("Attribution share")
    ax.set_ylabel("")
    ax.xaxis.set_major_formatter(lambda value, _: f"{100.0 * value:.0f}%")
    ax.grid(True, axis="x", color=PAPER_TOKENS["grid"], linewidth=0.8)
    ax.legend(loc="lower right", frameon=True, fontsize=8)
    _add_paper_header(
        fig,
        ax,
        "Global feature attribution agrees on the dominant decision signals",
        subtitle,
    )
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_feature_time_heatmap(
    frame: pl.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
    subtitle: str,
    top_features: int = 24,
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if _is_empty_frame(frame) or not required.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["feature", "lookback_from_end", value_col]), value_col, "lookback_from_end")
    data = data.drop_nulls(subset=["feature", "lookback_from_end", value_col])
    if data.is_empty():
        return
    top = _top_values_by_sum(data, "feature", value_col, top_features)
    if not top:
        return
    data = data.filter(pl.col("feature").is_in(top)).with_columns(
        pl.col("feature").cast(pl.String).map_elements(_feature_label, return_dtype=pl.String).alias("feature_label")
    )
    ordered_labels = [_feature_label(str(feature)) for feature in top]
    column_order = sorted(data.get_column("lookback_from_end").unique().to_list())
    labels, columns, matrix = _pivot_sum_matrix(
        data,
        index_col="feature_label",
        column_col="lookback_from_end",
        value_col=value_col,
        index_order=ordered_labels,
        column_order=column_order,
    )
    if matrix.size == 0:
        return
    plt, sns = _setup_paper_plotting()
    from matplotlib.colors import LinearSegmentedColormap

    fig_height = max(5.0, 0.38 * len(labels) + 2.0)
    fig, ax = plt.subplots(figsize=(12.2, fig_height), dpi=170)
    cmap = LinearSegmentedColormap.from_list(
        "paper_blue_gold",
        [PAPER_TOKENS["blue_xlight"], PAPER_TOKENS["blue_base"], PAPER_TOKENS["blue_dark"], PAPER_TOKENS["gold_mid"]],
    )
    vmax = float(np.nanpercentile(matrix, 98))
    if vmax <= 0.0:
        vmax = None
    sns.heatmap(
        matrix,
        cmap=cmap,
        vmin=0.0,
        vmax=vmax,
        linewidths=0.7,
        linecolor=PAPER_TOKENS["panel"],
        cbar_kws={"label": value_col},
        ax=ax,
        xticklabels=[_lookback_label(column) for column in columns],
        yticklabels=[str(label) for label in labels],
    )
    ax.set_xlabel("Lookback day (t-0 = latest day before decision)")
    ax.set_ylabel("")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
    ax.tick_params(axis="y", labelsize=8)
    _add_paper_header(fig, ax, title, subtitle)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_time_importance(frame: pl.DataFrame, *, output_path: Path, value_col: str, subtitle: str) -> None:
    if _is_empty_frame(frame) or not {"lookback_from_end", value_col}.issubset(frame.columns):
        return
    numeric_cols = [value_col, "lookback_from_end"]
    if "share" in frame.columns:
        numeric_cols.append("share")
    data = _with_numeric(frame, *numeric_cols).drop_nulls(subset=[value_col, "lookback_from_end"]).sort("lookback_from_end")
    if data.is_empty():
        return
    plt, sns = _setup_paper_plotting()
    fig, ax = plt.subplots(figsize=(10.5, 5.2), dpi=160)
    y_col = "share" if "share" in data.columns else value_col
    sns.barplot(data=_to_plot_data(data), x="lookback_from_end", y=y_col, color=PAPER_TOKENS["blue_mid"], ax=ax)
    ax.set_xlabel("Lookback day (t-0 = latest)")
    ax.set_ylabel("Attribution share" if "share" in data.columns else value_col)
    ax.set_xticks(np.arange(data.height))
    ax.set_xticklabels([_lookback_label(value) for value in data.get_column("lookback_from_end").to_list()])
    if "share" in data.columns:
        ax.yaxis.set_major_formatter(lambda value, _: f"{100.0 * value:.0f}%")
        for patch, value in zip(ax.patches, _numeric_numpy(data, "share"), strict=False):
            ax.text(patch.get_x() + patch.get_width() / 2.0, patch.get_height(), _format_share(value), ha="center", va="bottom", fontsize=8, color=PAPER_TOKENS["ink"])
    _add_paper_header(fig, ax, "Temporal attribution across the lookback window", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_feature_correlations(frame: pl.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if _is_empty_frame(frame) or not {"feature", "source", "score_corr", "weight_corr"}.issubset(frame.columns):
        return
    data = _with_numeric(frame, "score_corr", "weight_corr").with_columns(
        pl.max_horizontal(pl.col("score_corr").abs(), pl.col("weight_corr").abs()).alias("max_abs_corr")
    )
    data = data.drop_nulls(subset=["max_abs_corr"]).sort("max_abs_corr", descending=True).head(18)
    if data.is_empty():
        return
    data = data.with_columns(
        pl.concat_str([pl.col("source").cast(pl.String), pl.lit(" / "), pl.col("feature").cast(pl.String)]).alias("label")
    )
    plot = data.select(["label", "score_corr", "weight_corr"]).unpivot(
        index=["label"], on=["score_corr", "weight_corr"], variable_name="target", value_name="corr"
    )
    plt, sns = _setup_paper_plotting()
    fig_height = max(5.2, 0.36 * data.height + 2.0)
    fig, ax = plt.subplots(figsize=(11.5, fig_height), dpi=160)
    sns.barplot(
        data=_to_plot_data(plot),
        y="label",
        x="corr",
        hue="target",
        order=_string_list(data, "label")[::-1],
        palette={"score_corr": PAPER_TOKENS["blue_mid"], "weight_corr": PAPER_TOKENS["pink_mid"]},
        ax=ax,
    )
    ax.axvline(0.0, color=PAPER_TOKENS["neutral_dark"], linewidth=1.0)
    ax.set_xlabel("Correlation")
    ax.set_ylabel("")
    ax.legend(loc="lower right", frameon=True, fontsize=8)
    _add_paper_header(fig, ax, "Simple feature correlations test for shortcut rules", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_trust_checks(frame: pl.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if _is_empty_frame(frame) or not {"check", "value", "status"}.issubset(frame.columns):
        return
    data = _with_numeric(frame, "value").drop_nulls(subset=["value"])
    if data.is_empty():
        return
    plt, sns = _setup_paper_plotting()
    fig_height = max(4.8, 0.48 * data.height + 2.0)
    fig, ax = plt.subplots(figsize=(11.5, fig_height), dpi=160)
    palette = {"pass": PAPER_TOKENS["blue_mid"], "warn": PAPER_TOKENS["orange_mid"]}
    sns.barplot(data=_to_plot_data(data), y="check", x="value", hue="status", dodge=False, palette=palette, ax=ax)
    for row_idx, row in enumerate(data.iter_rows(named=True)):
        ax.text(_safe_float(row.get("value")), row_idx, f"  {row.get('rule', '')}", va="center", ha="left", fontsize=8, color=PAPER_TOKENS["muted"])
    ax.set_xlabel("Measured value")
    ax.set_ylabel("")
    ax.legend(loc="lower right", frameon=True, fontsize=8)
    _add_paper_header(fig, ax, "Strategy trust checks highlight concentration, masking, and shortcut risks", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_regime(frame: pl.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if _is_empty_frame(frame) or not {"dimension", "regime", "mean_strategy_log_return"}.issubset(frame.columns):
        return
    data = _with_numeric(frame, "mean_strategy_log_return").drop_nulls(subset=["mean_strategy_log_return"])
    if data.is_empty():
        return
    data = data.with_columns(
        pl.concat_str([pl.col("dimension").cast(pl.String), pl.lit(" / "), pl.col("regime").cast(pl.String)]).alias("label")
    )
    plt, sns = _setup_paper_plotting()
    fig_height = max(4.8, 0.42 * data.height + 2.0)
    fig, ax = plt.subplots(figsize=(11.5, fig_height), dpi=160)
    colors = [PAPER_TOKENS["blue_mid"] if value >= 0 else PAPER_TOKENS["orange_mid"] for value in _numeric_numpy(data, "mean_strategy_log_return")]
    sns.barplot(data=_to_plot_data(data), y="label", x="mean_strategy_log_return", palette=colors, hue="label", legend=False, ax=ax)
    ax.axvline(0.0, color=PAPER_TOKENS["neutral_dark"], linewidth=1.0)
    ax.set_xlabel("Mean strategy log return")
    ax.set_ylabel("")
    _add_paper_header(fig, ax, "Performance by market regime checks whether the rule survives different states", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_case_studies(frame: pl.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if _is_empty_frame(frame) or not {"case_type", "symbol", "gross_contribution"}.issubset(frame.columns):
        return
    data = _with_numeric(frame, "gross_contribution").drop_nulls(subset=["gross_contribution"])
    if data.is_empty():
        return
    data = data.with_columns(
        pl.concat_str([pl.col("case_type").cast(pl.String), pl.lit(" / "), pl.col("symbol").cast(pl.String)]).alias("label")
    ).sort("gross_contribution")
    plt, sns = _setup_paper_plotting()
    fig_height = max(5.2, 0.28 * data.height + 2.0)
    fig, ax = plt.subplots(figsize=(12, fig_height), dpi=160)
    colors = [PAPER_TOKENS["blue_mid"] if value >= 0 else PAPER_TOKENS["orange_mid"] for value in _numeric_numpy(data, "gross_contribution")]
    sns.barplot(data=_to_plot_data(data), y="label", x="gross_contribution", palette=colors, hue="label", legend=False, ax=ax)
    ax.axvline(0.0, color=PAPER_TOKENS["neutral_dark"], linewidth=1.0)
    ax.set_xlabel("Weight × future log return")
    ax.set_ylabel("")
    _add_paper_header(fig, ax, "Case-study trades show which names drove wins and losses", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_aux_summary(frame: pl.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if _is_empty_frame(frame) or not {"name", "mean_abs"}.issubset(frame.columns):
        return
    data = _with_numeric(frame, "mean_abs").drop_nulls(subset=["mean_abs"]).sort("mean_abs", descending=True).head(24)
    if data.is_empty():
        return
    plt, sns = _setup_paper_plotting()
    fig_height = max(4.8, 0.36 * data.height + 2.0)
    fig, ax = plt.subplots(figsize=(11, fig_height), dpi=160)
    sns.barplot(data=_to_plot_data(data), y="name", x="mean_abs", color=PAPER_TOKENS["olive_mid"], ax=ax)
    ax.set_xlabel("Mean absolute activation")
    ax.set_ylabel("")
    _add_paper_header(fig, ax, "Latent and market-token diagnostics check whether representations collapse", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_all_paper_figures(
    output_dir: Path,
    *,
    frames: dict[str, pl.DataFrame],
    summary: dict[str, Any],
    metadata: dict[str, Any],
    paper_tables: dict[str, str],
    plot_timing: dict[str, float] | None = None,
) -> list[str]:
    plot_dir = output_dir / "plots_paper"
    plot_dir.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    scope = _paper_scope(metadata, summary)

    def _time_plot(name: str, fn: Callable[[], None], out_path: Path) -> None:
        item_start = time.perf_counter()
        fn()
        if plot_timing is not None:
            plot_timing[name] = float(time.perf_counter() - item_start)
        if out_path.exists():
            generated.append(out_path)

    global_table = _global_attribution_table(frames)
    out = plot_dir / "global_feature_attribution.png"
    _time_plot(
        "global_feature_attribution_s",
        lambda: _plot_paper_global_attribution(global_table, out, subtitle=f"Share of total attribution by method; {scope}"),
        out,
    )
    heatmap_specs = [
        (
            "feature_time_gradient",
            "grad_x_input_abs",
            "Feature-time heatmap shows where local gradient sensitivity concentrates",
            "Mean absolute gradient × input across selected top-weight decisions; t-0 is the latest input day.",
        ),
        (
            "feature_time_integrated_gradients",
            "integrated_gradients_abs",
            "Integrated gradients test whether the same feature-days matter along the input path",
            "Mean absolute integrated gradients from zero baseline; brighter cells indicate stronger contribution.",
        ),
        (
            "feature_time_perturbation",
            "weight_abs_delta",
            "Perturbation heatmap shows which feature-days move portfolio weights",
            "Mean absolute weight change after zeroing one feature-day slice; preferred over raw score deltas.",
        ),
    ]
    for frame_name, value_col, title, subtitle in heatmap_specs:
        out = plot_dir / f"{frame_name}_{value_col}_heatmap.png"
        _time_plot(
            f"{frame_name}_{value_col}_heatmap_s",
            lambda frame_name=frame_name, value_col=value_col, title=title, subtitle=subtitle, out=out: _plot_paper_feature_time_heatmap(
                frames.get(frame_name, pl.DataFrame()),
                output_path=out,
                value_col=value_col,
                title=title,
                subtitle=f"{subtitle} {scope}",
            ),
            out,
        )
    out = plot_dir / "time_importance_gradient.png"
    _time_plot(
        "time_importance_gradient_s",
        lambda: _plot_paper_time_importance(
            frames.get("time_importance_gradient", pl.DataFrame()),
            output_path=out,
            value_col="grad_x_input_abs",
            subtitle=f"Share of gradient × input by lookback day; {scope}",
        ),
        out,
    )
    out = plot_dir / "feature_correlations_shortcut_checks.png"
    _time_plot(
        "feature_correlations_shortcut_checks_s",
        lambda: _plot_paper_feature_correlations(frames.get("feature_correlations", pl.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    out = plot_dir / "trust_checks.png"
    _time_plot(
        "trust_checks_s",
        lambda: _plot_paper_trust_checks(frames.get("trust_checks", pl.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    out = plot_dir / "regime_analysis.png"
    _time_plot(
        "regime_analysis_s",
        lambda: _plot_paper_regime(frames.get("regime_analysis", pl.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    out = plot_dir / "decision_case_studies.png"
    _time_plot(
        "decision_case_studies_s",
        lambda: _plot_paper_case_studies(frames.get("decision_case_studies", pl.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    out = plot_dir / "aux_token_diagnostics.png"
    _time_plot(
        "aux_token_diagnostics_s",
        lambda: _plot_paper_aux_summary(frames.get("aux_summary", pl.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    return [str(path.relative_to(output_dir)) for path in generated]


PAPER_FIGURE_GUIDE: dict[str, tuple[str, str, str]] = {
    "global_feature_attribution.png": (
        "Compares global feature importance across gradient x input, Integrated Gradients, perturbation, and surrogate SHAP.",
        "Features that stay near the top across methods are more credible than features that appear in only one diagnostic.",
        "One feature taking more than half of total attribution, or SHAP disagreeing completely with perturbation, suggests a narrow or unstable rule.",
    ),
    "feature_time_gradient_grad_x_input_abs_heatmap.png": (
        "Measures local sensitivity by feature and lookback day for the selected top-weight decisions.",
        "Read rows as feature families and columns as days before the decision; brighter cells mean stronger local influence.",
        "A blank-looking chart, one isolated column, or one isolated feature row means the model may be ignoring most of the lookback window.",
    ),
    "feature_time_integrated_gradients_integrated_gradients_abs_heatmap.png": (
        "Measures path-integrated attribution from a zero baseline to the actual input window.",
        "Use it as a smoother confirmation of the gradient heatmap; repeated bright regions across both charts are more trustworthy.",
        "Large disagreement with gradient and perturbation means the explanation is locally unstable.",
    ),
    "feature_time_perturbation_weight_abs_delta_heatmap.png": (
        "Measures how much portfolio weights change when a feature-day slice is zeroed.",
        "This is closest to trading behavior because it observes the final position change, not only score movement.",
        "Huge score deltas with tiny weight deltas, or sensitivity only to raw liquidity/price-like fields, is suspicious.",
    ),
    "time_importance_gradient.png": (
        "Aggregates attribution by lookback day.",
        "A healthy temporal model should use a pattern over several days unless the strategy is intentionally one-day reactive.",
        "A single day dominating the whole bar chart suggests the model is effectively temporal-only at one lag.",
    ),
    "feature_correlations_shortcut_checks.png": (
        "Checks simple linear correlation between raw feature values and model scores/weights.",
        "High absolute correlations are not proof of leakage, but they are a fast shortcut detector.",
        "Very high correlation with raw price level, raw volume, or liquidity proxies means the model may not generalize cross-sectionally.",
    ),
    "trust_checks.png": (
        "Summarizes concentration, turnover, mask leakage, attribution dominance, and aux collapse checks.",
        "Blue/pass is acceptable by rule of thumb; orange/warn deserves manual inspection before trusting the strategy.",
        "Warnings in mask leakage, concentration, or turnover can invalidate backtest conclusions even if returns look good.",
    ),
    "regime_analysis.png": (
        "Splits sampled decisions by market direction and volatility regime.",
        "The strategy is more credible if the rule has understandable behavior in up/down and high/low volatility states.",
        "Performance that only appears in one tiny regime bucket may be overfit.",
    ),
    "decision_case_studies.png": (
        "Shows which symbols drove selected best/worst/high-turnover days.",
        "Use it to inspect whether winning and losing trades match the claimed signal logic.",
        "Repeated losses from similar names or very concentrated single-name contributions suggest unstable decision rules.",
    ),
    "aux_token_diagnostics.png": (
        "Checks activation magnitude for latent factors, market tokens, and transformer auxiliary tensors.",
        "Non-zero, non-dominant representations suggest tokens are being used rather than collapsed.",
        "Near-zero or single-dimension dominance suggests latent/market tokens are not absorbing meaningful market regime information.",
    ),
}


def _render_frame_markdown(frame: pl.DataFrame, limit: int = 20) -> str:
    return _render_table_markdown(frame, limit=limit)



def _paper_executive_summary(
    *,
    frames: dict[str, pl.DataFrame],
    summary: dict[str, Any],
    metadata: dict[str, Any],
) -> list[str]:
    lines: list[str] = []
    portfolio = summary.get("portfolio", {})
    global_table = _global_attribution_table(frames)
    if not _is_empty_frame(global_table):
        top = _first_row(global_table)
        lines.append(
            f"- The strongest global signal is `{top.get('feature')}` ({top.get('feature_group')}); "
            f"mean available attribution share is {_format_share(top.get('mean_available_share', 0.0))}."
        )
    shap = frames.get("feature_importance_shap", pl.DataFrame())
    if not _is_empty_frame(shap):
        row = _first_row(shap)
        r2 = _safe_float(row.get("surrogate_r2", summary.get("shap_info", {}).get("surrogate_r2", 0.0)))
        lines.append(
            f"- Score-head surrogate SHAP top feature is `{row.get('feature')}` with surrogate R2={r2:.3f}; "
            "treat it as global evidence, not exact full-Transformer SHAP."
        )
    else:
        shap_info = summary.get("shap_info", {})
        lines.append(f"- Surrogate SHAP was not produced: `{shap_info.get('error', shap_info.get('method', 'skipped'))}`.")
    if portfolio:
        lines.append(
            "- Portfolio behavior: "
            f"gross={_safe_float(portfolio.get('mean_gross')):.3f}, "
            f"abs net={_safe_float(portfolio.get('mean_abs_net')):.3f}, "
            f"turnover proxy={_safe_float(portfolio.get('mean_turnover_proxy')):.3f}, "
            f"max single-name weight={_safe_float(portfolio.get('max_abs_weight_max')):.3f}."
        )
    config_lookback = metadata.get("config_lookback")
    attribution_lookback = summary.get("attribution_lookback")
    if config_lookback is not None and attribution_lookback is not None and int(config_lookback) != int(attribution_lookback):
        lines.append(
            f"- Lookback warning: config lookback is {config_lookback}, but this artifact only contains "
            f"{attribution_lookback} attribution days. Do not cite it as a complete lookback-{config_lookback} explanation."
        )
    warnings = summary.get("warnings", [])
    if warnings:
        lines.append(f"- Main warning: {warnings[0]}")
    if not lines:
        lines.append("- No explainability rows were available; inspect data loading and model output hooks.")
    return lines



def _write_paper_report(
    path: Path,
    *,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    frames: dict[str, pl.DataFrame],
    paper_tables: dict[str, str],
    paper_plots: list[str],
) -> None:
    lines: list[str] = []
    lines.append("# Paper-Grade Model Explainability Report")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.extend(_paper_executive_summary(frames=frames, summary=summary, metadata=metadata))
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    for key, value in metadata.items():
        lines.append(f"- **{key}**: `{value}`")
    lines.append(f"- **attribution_lookback**: `{summary.get('attribution_lookback')}`")
    lines.append(f"- **shap_method**: `{summary.get('shap_info', {}).get('method', 'unknown')}`")
    lines.append("")
    lines.append("## Figure Reading Guide")
    lines.append("")
    for plot in paper_plots:
        name = Path(plot).name
        guide = PAPER_FIGURE_GUIDE.get(name)
        if guide is None:
            continue
        lines.append(f"### {name}")
        lines.append("")
        lines.append(f"- **What it measures**: {guide[0]}")
        lines.append(f"- **How to read it**: {guide[1]}")
        lines.append(f"- **What would be suspicious**: {guide[2]}")
        lines.append("")
    lines.append("## Trust And Sanity Checks")
    lines.append("")
    lines.append(_render_frame_markdown(frames.get("trust_checks", pl.DataFrame()), limit=30))
    lines.append("")
    lines.append("## Global Attribution Table")
    lines.append("")
    lines.append(_render_frame_markdown(_global_attribution_table(frames), limit=20))
    lines.append("")
    lines.append("## Top Feature-Time Cells")
    lines.append("")
    feature_time_tables: list[pl.DataFrame] = []
    for key, method in (
        ("top_feature_time_gradient_cells", "gradient_x_input"),
        ("top_feature_time_integrated_gradients_cells", "integrated_gradients"),
        ("top_feature_time_perturbation_cells", "perturbation_weight_delta"),
    ):
        frame = frames.get(key, pl.DataFrame())
        if not _is_empty_frame(frame):
            feature_time_tables.append(frame.with_columns(pl.lit(method).alias("method")).select(["method", *frame.columns]))
    lines.append(_render_frame_markdown(_concat_frames(feature_time_tables), limit=30))
    lines.append("")
    lines.append("## Regime Analysis")
    lines.append("")
    lines.append(_render_frame_markdown(frames.get("regime_analysis", pl.DataFrame()), limit=30))
    lines.append("")
    lines.append("## Decision Case Studies")
    lines.append("")
    lines.append(_render_frame_markdown(frames.get("decision_case_studies", pl.DataFrame()), limit=30))
    lines.append("")
    lines.append("## Output Files")
    lines.append("")
    lines.append("### Paper Plots")
    lines.extend(f"- `{plot}`" for plot in paper_plots)
    lines.append("")
    lines.append("### Paper Tables")
    lines.extend(f"- `{path}`" for path in paper_tables.values())
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")



def _write_paper_summary(
    path: Path,
    *,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    paper_tables: dict[str, str],
    paper_plots: list[str],
) -> None:
    payload = {
        "metadata": _to_builtin(metadata),
        "paper_tables": paper_tables,
        "paper_plots": paper_plots,
        "attribution_lookback": summary.get("attribution_lookback"),
        "shap_info": summary.get("shap_info", {}),
        "warnings": summary.get("warnings", []),
    }
    path.write_text(json.dumps(_to_builtin(payload), indent=2, ensure_ascii=False), encoding="utf-8")


def write_fold_stability_outputs(explainability_root: Path, *, strict_no_fallback: bool = False) -> Path | None:
    root = Path(explainability_root)
    fold_dirs = sorted(path for path in root.glob("fold_*_test") if path.is_dir())
    rows: list[pl.DataFrame] = []
    for fold_dir in fold_dirs:
        path = fold_dir / "paper_tables" / "global_feature_attribution.csv"
        if not path.exists():
            if strict_no_fallback:
                raise FileNotFoundError(
                    f"{path} is required for fold stability when strict_no_fallback=true; "
                    "legacy feature_importance_gradient.csv fallback is disabled."
                )
            fallback = fold_dir / "feature_importance_gradient.csv"
            if not fallback.exists():
                continue
            table = pl.read_csv(fallback)
            if "share" not in table.columns:
                continue
            table = table.rename({"share": "gradient_share"}).with_columns(
                [
                    pl.col("gradient_share").alias("mean_available_share"),
                    pl.col("feature").cast(pl.String).map_elements(_feature_group, return_dtype=pl.String).alias("feature_group"),
                    pl.col("feature").cast(pl.String).map_elements(_feature_label, return_dtype=pl.String).alias("feature_label"),
                ]
            )
        else:
            table = pl.read_csv(path)
        if table.is_empty() or "feature" not in table.columns:
            continue
        fold_id = fold_dir.name.removeprefix("fold_").removesuffix("_test")
        table = table.with_columns(pl.lit(int(fold_id)).alias("fold_id"))
        if "mean_available_share" in table.columns:
            table = table.with_columns(pl.col("mean_available_share").rank(method="min", descending=True).alias("rank"))
        else:
            table = table.with_row_index("rank", offset=1)
        rows.append(table)
    if not rows:
        return None
    combined = _concat_frames(rows)
    summary = (
        combined.group_by("feature")
        .agg(
            [
                pl.col("fold_id").n_unique().alias("folds_present"),
                pl.col("rank").mean().alias("mean_rank"),
                pl.col("rank").std().alias("std_rank"),
                _numeric_expr("mean_available_share").mean().alias("mean_share"),
                _numeric_expr("mean_available_share").std().alias("std_share"),
            ]
        )
        .sort(["mean_rank", "mean_share"], descending=[False, True])
    )
    summary = _with_feature_labels(summary)
    output_dir = root / "paper_fold_stability"
    table_dir = output_dir / "paper_tables"
    plot_dir = output_dir / "plots_paper"
    table_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(combined, table_dir / "fold_feature_attribution_long.csv")
    _write_csv(summary, table_dir / "fold_feature_stability.csv")
    plt, sns = _setup_paper_plotting()
    data = summary.head(20)
    if not data.is_empty():
        fig_height = max(5.0, 0.36 * data.height + 2.0)
        fig, ax = plt.subplots(figsize=(11.5, fig_height), dpi=160)
        sns.barplot(data=_frame_to_dict(data), y="feature_label", x="mean_share", color=PAPER_TOKENS["blue_mid"], ax=ax)
        ax.set_xlabel("Mean attribution share across folds")
        ax.set_ylabel("")
        ax.xaxis.set_major_formatter(lambda value, _: f"{100.0 * value:.0f}%")
        fold_count = int(combined.select(pl.col("fold_id").n_unique()).item())
        _add_paper_header(fig, ax, "Fold stability shows whether the same features remain important", f"Computed across {fold_count} fold explainability outputs.")
        _finish_paper_axes(ax)
        _save_matplotlib_figure(fig, plot_dir / "fold_stability_feature_share.png")
        plt.close(fig)
    report = [
        "# Paper Fold Stability Summary",
        "",
        f"- folds: `{int(combined.select(pl.col('fold_id').n_unique()).item())}`",
        f"- features: `{int(summary.select(pl.col('feature').n_unique()).item())}`",
        "",
        "## Most Stable Features",
        "",
        _render_frame_markdown(summary, limit=30),
        "",
    ]
    (output_dir / "paper_fold_stability_report.md").write_text("\n".join(report), encoding="utf-8")
    return output_dir



def _safe_plot_filename(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(name))


def _plot_barh(
    frame: pl.DataFrame,
    *,
    output_path: Path,
    label_col: str,
    value_col: str,
    title: str,
    top_n: int = 30,
) -> None:
    if _is_empty_frame(frame) or label_col not in frame.columns or value_col not in frame.columns:
        return
    data = _with_numeric(frame.select([label_col, value_col]), value_col)
    data = data.drop_nulls(subset=[label_col, value_col]).sort(value_col, descending=True).head(top_n)
    if data.is_empty():
        return
    labels = _string_list(data, label_col)
    values = _numeric_numpy(data, value_col)
    fig_height = max(4.0, 0.28 * data.height + 1.5)
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, fig_height), dpi=130)
    ax.barh(labels[::-1], values[::-1])
    ax.set_title(title)
    ax.set_xlabel(value_col)
    ax.grid(True, axis="x", alpha=0.25)
    _safe_matplotlib_tight_layout(fig)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_time_importance(
    frame: pl.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
) -> None:
    if _is_empty_frame(frame) or value_col not in frame.columns or "lookback_from_end" not in frame.columns:
        return
    data = _with_numeric(frame.select(["lookback_from_end", value_col]), "lookback_from_end", value_col)
    data = data.drop_nulls(subset=["lookback_from_end", value_col]).sort("lookback_from_end")
    if data.is_empty():
        return
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=130)
    ax.bar(_string_list(data, "lookback_from_end"), _numeric_numpy(data, value_col))
    ax.set_title(title)
    ax.set_xlabel("lookback_from_end (0 = latest)")
    ax.set_ylabel(value_col)
    ax.grid(True, axis="y", alpha=0.25)
    _safe_matplotlib_tight_layout(fig)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_feature_time_heatmap(
    frame: pl.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
    top_features: int = 30,
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if _is_empty_frame(frame) or not required.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["feature", "lookback_from_end", value_col]), "lookback_from_end", value_col)
    data = data.drop_nulls(subset=["feature", "lookback_from_end", value_col])
    if data.is_empty():
        return
    top = _top_values_by_sum(data, "feature", value_col, top_features)
    if not top:
        return
    data = data.filter(pl.col("feature").is_in(top))
    column_order = sorted(data.get_column("lookback_from_end").unique().to_list())
    labels, columns, matrix = _pivot_sum_matrix(
        data,
        index_col="feature",
        column_col="lookback_from_end",
        value_col=value_col,
        index_order=top,
        column_order=column_order,
    )
    if matrix.size == 0:
        return
    import matplotlib.pyplot as plt

    fig_height = max(4.0, 0.30 * len(labels) + 1.5)
    fig, ax = plt.subplots(figsize=(9, fig_height), dpi=130)
    image = ax.imshow(matrix, aspect="auto", interpolation="nearest")
    ax.set_title(title)
    ax.set_xlabel("lookback_from_end (0 = latest)")
    ax.set_ylabel("feature")
    ax.set_xticks(np.arange(len(columns)))
    ax.set_xticklabels([str(col) for col in columns])
    ax.set_yticks(np.arange(len(labels)))
    ax.set_yticklabels([str(idx) for idx in labels])
    fig.colorbar(image, ax=ax, fraction=0.025, pad=0.02)
    _safe_matplotlib_tight_layout(fig)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_feature_time_heatmap_datashader(
    frame: pl.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
    top_features: int = 40,
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if _is_empty_frame(frame) or not required.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["feature", "lookback_from_end", value_col]), "lookback_from_end", value_col)
    data = data.drop_nulls(subset=["feature", "lookback_from_end", value_col])
    if data.is_empty():
        return
    top = [str(value) for value in _top_values_by_sum(data, "feature", value_col, top_features)]
    if not top:
        return
    data = data.with_columns(pl.col("feature").cast(pl.String).alias("feature")).filter(pl.col("feature").is_in(top))
    feature_to_y = {feature: len(top) - 1 - idx for idx, feature in enumerate(top)}
    data = data.with_columns(
        pl.col("feature")
        .map_elements(lambda value: feature_to_y.get(str(value)), return_dtype=pl.Int64)
        .alias("feature_y")
    ).drop_nulls(subset=["feature_y"])
    if data.is_empty():
        return
    save_heatmap_points_datashader(
        _numeric_numpy(data, "lookback_from_end"),
        _numeric_numpy(data, "feature_y"),
        _numeric_numpy(data, value_col),
        output_path=output_path,
        title=title,
        x_label="lookback_from_end (0 = latest)",
        y_label=value_col,
        y_labels=[(feature_to_y[feature], feature) for feature in top],
        width=1100,
        height=max(520, min(1400, 24 * len(top) + 180)),
    )


def _plot_feature_correlations(frame: pl.DataFrame, output_path: Path) -> None:
    if _is_empty_frame(frame) or "feature" not in frame.columns:
        return
    data = frame
    if "abs_score_corr" not in data.columns:
        return
    data = _with_numeric(data, "abs_score_corr", "score_corr", "weight_corr").sort("abs_score_corr", descending=True).head(30)
    if data.is_empty():
        return
    import matplotlib.pyplot as plt

    labels = _string_list(
        data.with_columns(pl.concat_str([pl.col("source").cast(pl.String), pl.lit(":"), pl.col("feature").cast(pl.String)]).alias("__label")),
        "__label",
    )
    y = np.arange(data.height)
    fig_height = max(4.0, 0.28 * data.height + 1.5)
    fig, ax = plt.subplots(figsize=(10, fig_height), dpi=130)
    ax.barh(y - 0.18, _numeric_numpy(data, "score_corr"), height=0.35, label="score_corr")
    if "weight_corr" in data.columns:
        ax.barh(y + 0.18, _numeric_numpy(data, "weight_corr"), height=0.35, label="weight_corr")
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.axvline(0.0, color="black", linewidth=0.8)
    ax.set_title("Top Simple Feature Correlations")
    ax.set_xlabel("correlation")
    ax.grid(True, axis="x", alpha=0.25)
    ax.legend()
    _safe_matplotlib_tight_layout(fig)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_decision_exposure(frame: pl.DataFrame, output_path: Path) -> None:
    if _is_empty_frame(frame) or not {"date", "side", "weight"}.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["date", "side", "weight"]), "weight")
    data = data.with_columns(pl.col("weight").abs().alias("abs_weight")).drop_nulls(subset=["date", "side", "abs_weight"]).sort("date")
    rows, columns, matrix = _pivot_sum_matrix(
        data,
        index_col="date",
        column_col="side",
        value_col="abs_weight",
        index_order=data.get_column("date").unique(maintain_order=True).to_list(),
        column_order=["long", "short", "flat"],
    )
    if matrix.size == 0:
        return
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4.8), dpi=130)
    bottom = np.zeros(len(rows))
    for side in ("long", "short", "flat"):
        if side not in columns:
            continue
        values = matrix[:, columns.index(side)]
        ax.bar(np.arange(len(rows)), values, bottom=bottom, label=side)
        bottom = bottom + values
    ax.set_title("Top Decision Absolute Exposure By Side")
    ax.set_xlabel("sampled date index")
    ax.set_ylabel("sum abs(weight)")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    _safe_matplotlib_tight_layout(fig)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_decision_exposure_datashader(frame: pl.DataFrame, output_path: Path) -> None:
    if _is_empty_frame(frame) or not {"date", "side", "weight"}.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["date", "side", "weight"]), "weight")
    data = data.with_columns(pl.col("weight").abs().alias("abs_weight")).drop_nulls(subset=["date", "side", "abs_weight"]).sort("date")
    rows, columns, matrix = _pivot_sum_matrix(
        data,
        index_col="date",
        column_col="side",
        value_col="abs_weight",
        index_order=data.get_column("date").unique(maintain_order=True).to_list(),
        column_order=["long", "short", "flat"],
    )
    if matrix.size == 0:
        return
    x = np.arange(len(rows), dtype=np.float64)
    colors = {"long": "#1f77b4", "short": "#d62728", "flat": "#7f7f7f"}
    series = [
        (side, x, matrix[:, columns.index(side)], colors[side])
        for side in ("long", "short", "flat")
        if side in columns
    ]
    if not series:
        return
    save_line_series_datashader(
        series,
        output_path=output_path,
        title="Top Decision Absolute Exposure By Side",
        y_label="sum abs(weight)",
        width=1100,
        height=520,
    )


def _plot_aux_dim_datashader(frame: pl.DataFrame, *, output_path: Path, title: str) -> None:
    if _is_empty_frame(frame) or not {"dim", "mean_abs"}.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["dim", "mean_abs"]), "dim", "mean_abs")
    data = data.drop_nulls(subset=["dim", "mean_abs"]).sort("dim")
    if data.is_empty():
        return
    save_line_series_datashader(
        [("mean_abs", _numeric_numpy(data, "dim"), _numeric_numpy(data, "mean_abs"), "#2171b5")],
        output_path=output_path,
        title=title,
        y_label="mean_abs",
        width=1000,
        height=420,
    )


def _plot_aux_projection_datashader(frame: pl.DataFrame, *, output_path: Path, title: str) -> None:
    if _is_empty_frame(frame) or not {"umap_x", "umap_y"}.issubset(frame.columns):
        return
    data = _with_numeric(frame, "umap_x", "umap_y").drop_nulls(subset=["umap_x", "umap_y"])
    if data.is_empty():
        return
    colors = {
        "stock": "#1f77b4",
        "token": "#9467bd",
        "time_stock": "#2ca02c",
        "vector": "#ff7f0e",
    }
    series = []
    if "point_type" in data.columns:
        point_types = data.get_column("point_type").cast(pl.String).unique(maintain_order=True).to_list()
        typed = data.with_columns(pl.col("point_type").cast(pl.String).alias("point_type"))
        for point_type in point_types:
            group = typed.filter(pl.col("point_type") == point_type)
            color = colors.get(str(point_type), "#17becf")
            series.append(
                (
                    str(point_type),
                    _numeric_numpy(group, "umap_x"),
                    _numeric_numpy(group, "umap_y"),
                    color,
                )
            )
    else:
        series.append(("points", _numeric_numpy(data, "umap_x"), _numeric_numpy(data, "umap_y"), "#1f77b4"))
    save_scatter_datashader(series, output_path=output_path, title=title, width=1100, height=760)


def _plot_all_explanation_figures(
    frames: dict[str, pl.DataFrame],
    aux_dim_frames: dict[str, pl.DataFrame],
    output_dir: Path,
    *,
    aux_projection_frames: dict[str, pl.DataFrame] | None = None,
    plot_backend: str = "auto",
    plot_timing: dict[str, Any] | None = None,
    strict_no_fallback: bool = False,
) -> list[str]:
    normalized_backend = _normalize_plot_backend(plot_backend)
    estimated_points = sum(len(frame) for frame in frames.values() if frame is not None)
    estimated_points += sum(len(frame) for frame in aux_dim_frames.values() if frame is not None)
    estimated_points += sum(len(frame) for frame in (aux_projection_frames or {}).values() if frame is not None)
    use_datashader = _use_datashader_for_explainability(normalized_backend, estimated_points=estimated_points)
    if plot_timing is not None:
        plot_timing["backend"] = "rapids_datashader" if use_datashader else "matplotlib"
        plot_timing["estimated_points"] = int(estimated_points)
        plot_timing.setdefault("datashader_fallbacks", [])
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
    except Exception as exc:
        skip_path = output_dir / "plots_skipped.txt"
        skip_path.parent.mkdir(parents=True, exist_ok=True)
        skip_path.write_text(f"matplotlib unavailable: {exc}\n", encoding="utf-8")
        return [str(skip_path.relative_to(output_dir.parent))]

    plot_dir = output_dir / "plots"
    generated: list[Path] = []

    stage_start = time.perf_counter()
    specs = [
        ("feature_importance_gradient", "feature", "grad_x_input_abs", "Gradient x Input Feature Importance"),
        ("feature_importance_integrated_gradients", "feature", "integrated_gradients_abs", "Integrated Gradients Feature Importance"),
        ("feature_importance_perturbation", "feature", "weight_abs_delta", "Perturbation Feature Importance (Weight Delta)"),
        ("feature_importance_perturbation", "feature", "score_abs_delta", "Perturbation Feature Importance (Score Delta)"),
        ("stock_contributions", "symbol", "mean_abs_weight", "Top Stocks By Mean Absolute Weight"),
        ("stock_contributions", "symbol", "total_gross_contribution", "Top Stocks By Total Gross Contribution"),
        ("aux_summary", "name", "mean_abs", "Auxiliary Representation Mean Abs"),
    ]
    for frame_name, label_col, value_col, title in specs:
        frame = frames.get(frame_name, pl.DataFrame())
        out = plot_dir / f"{frame_name}_{value_col}.png"
        _plot_barh(frame, output_path=out, label_col=label_col, value_col=value_col, title=title)
        if out.exists():
            generated.append(out)
    if plot_timing is not None:
        plot_timing["bar_specs_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    time_specs = [
        ("time_importance_gradient", "grad_x_input_abs", "Gradient x Input By Lookback Day"),
        ("time_importance_integrated_gradients", "integrated_gradients_abs", "Integrated Gradients By Lookback Day"),
    ]
    for frame_name, value_col, title in time_specs:
        out = plot_dir / f"{frame_name}.png"
        _plot_time_importance(frames.get(frame_name, pl.DataFrame()), output_path=out, value_col=value_col, title=title)
        if out.exists():
            generated.append(out)
    if plot_timing is not None:
        plot_timing["time_specs_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    heatmap_specs = [
        ("feature_time_gradient", "grad_x_input_abs", "Gradient x Input Feature-Time Heatmap"),
        ("feature_time_integrated_gradients", "integrated_gradients_abs", "Integrated Gradients Feature-Time Heatmap"),
        ("feature_time_perturbation", "weight_abs_delta", "Perturbation Weight Delta Feature-Time Heatmap"),
        ("feature_time_perturbation", "score_abs_delta", "Perturbation Score Delta Feature-Time Heatmap"),
    ]
    for frame_name, value_col, title in heatmap_specs:
        out = plot_dir / f"{frame_name}_{value_col}_heatmap.png"
        frame = frames.get(frame_name, pl.DataFrame())
        if use_datashader:
            try:
                _plot_feature_time_heatmap_datashader(frame, output_path=out, value_col=value_col, title=title)
            except Exception as exc:
                if strict_no_fallback:
                    raise RuntimeError(
                        f"Datashader plot failed for {out.name}; strict_no_fallback=true so "
                        "matplotlib fallback is disabled."
                    ) from exc
                if plot_timing is not None:
                    plot_timing.setdefault("datashader_fallbacks", []).append(
                        {"plot": out.name, "error": f"{type(exc).__name__}: {exc}"}
                    )
                _plot_feature_time_heatmap(frame, output_path=out, value_col=value_col, title=title)
        else:
            _plot_feature_time_heatmap(frame, output_path=out, value_col=value_col, title=title)
        if out.exists():
            generated.append(out)
    if plot_timing is not None:
        plot_timing["heatmap_specs_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    out = plot_dir / "feature_correlations.png"
    _plot_feature_correlations(frames.get("feature_correlations", pl.DataFrame()), out)
    if out.exists():
        generated.append(out)
    if plot_timing is not None:
        plot_timing["feature_correlations_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    out = plot_dir / "top_decisions_exposure_by_side.png"
    decision_frame = frames.get("top_decisions", pl.DataFrame())
    if use_datashader:
        try:
            _plot_decision_exposure_datashader(decision_frame, out)
        except Exception as exc:
            if strict_no_fallback:
                raise RuntimeError(
                    f"Datashader plot failed for {out.name}; strict_no_fallback=true so "
                    "matplotlib fallback is disabled."
                ) from exc
            if plot_timing is not None:
                plot_timing.setdefault("datashader_fallbacks", []).append(
                    {"plot": out.name, "error": f"{type(exc).__name__}: {exc}"}
                )
            _plot_decision_exposure(decision_frame, out)
    else:
        _plot_decision_exposure(decision_frame, out)
    if out.exists():
        generated.append(out)
    if plot_timing is not None:
        plot_timing["decision_exposure_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    aux_plot_dir = plot_dir / "aux_dims"
    for name, frame in aux_dim_frames.items():
        out = aux_plot_dir / f"{_safe_plot_filename(name)}.png"
        if use_datashader:
            try:
                _plot_aux_dim_datashader(frame, output_path=out, title=f"Aux Dimension Profile: {name}")
            except Exception as exc:
                if strict_no_fallback:
                    raise RuntimeError(
                        f"Datashader plot failed for {out.name}; strict_no_fallback=true so "
                        "matplotlib fallback is disabled."
                    ) from exc
                if plot_timing is not None:
                    plot_timing.setdefault("datashader_fallbacks", []).append(
                        {"plot": out.name, "error": f"{type(exc).__name__}: {exc}"}
                    )
                _plot_barh(
                    frame,
                    output_path=out,
                    label_col="dim",
                    value_col="mean_abs",
                    title=f"Aux Dimension Importance: {name}",
                    top_n=32,
                )
        else:
            _plot_barh(
                frame,
                output_path=out,
                label_col="dim",
                value_col="mean_abs",
                title=f"Aux Dimension Importance: {name}",
                top_n=32,
            )
        if out.exists():
            generated.append(out)
    if plot_timing is not None:
        plot_timing["aux_dims_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    projection_plot_dir = plot_dir / "aux_umap"
    for name, frame in (aux_projection_frames or {}).items():
        out = projection_plot_dir / f"{_safe_plot_filename(name)}.png"
        if use_datashader:
            try:
                _plot_aux_projection_datashader(
                    frame,
                    output_path=out,
                    title=f"cuML UMAP Projection: {name}",
                )
            except Exception as exc:
                if strict_no_fallback:
                    raise RuntimeError(
                        f"Datashader plot failed for {out.name}; strict_no_fallback=true so "
                        "matplotlib fallback is disabled."
                    ) from exc
                if plot_timing is not None:
                    plot_timing.setdefault("datashader_fallbacks", []).append(
                        {"plot": out.name, "error": f"{type(exc).__name__}: {exc}"}
                    )
                if _is_empty_frame(frame) or not {"umap_x", "umap_y"}.issubset(frame.columns):
                    continue
                import matplotlib.pyplot as plt

                data = _with_numeric(frame, "umap_x", "umap_y").drop_nulls(subset=["umap_x", "umap_y"])
                if data.is_empty():
                    continue
                fig, ax = plt.subplots(figsize=(8, 6), dpi=130)
                ax.scatter(_numeric_numpy(data, "umap_x"), _numeric_numpy(data, "umap_y"), s=4, alpha=0.5)
                ax.set_title(f"cuML UMAP Projection: {name}")
                ax.set_xlabel("umap_x")
                ax.set_ylabel("umap_y")
                _safe_matplotlib_tight_layout(fig)
                out.parent.mkdir(parents=True, exist_ok=True)
                _save_matplotlib_figure(fig, out)
                plt.close(fig)
        else:
            if _is_empty_frame(frame) or not {"umap_x", "umap_y"}.issubset(frame.columns):
                continue
            import matplotlib.pyplot as plt

            data = _with_numeric(frame, "umap_x", "umap_y").drop_nulls(subset=["umap_x", "umap_y"])
            if data.is_empty():
                continue
            fig, ax = plt.subplots(figsize=(8, 6), dpi=130)
            ax.scatter(_numeric_numpy(data, "umap_x"), _numeric_numpy(data, "umap_y"), s=4, alpha=0.5)
            ax.set_title(f"cuML UMAP Projection: {name}")
            ax.set_xlabel("umap_x")
            ax.set_ylabel("umap_y")
            _safe_matplotlib_tight_layout(fig)
            out.parent.mkdir(parents=True, exist_ok=True)
            _save_matplotlib_figure(fig, out)
            plt.close(fig)
        if out.exists():
            generated.append(out)
    if plot_timing is not None:
        plot_timing["aux_umap_s"] = float(time.perf_counter() - stage_start)

    return [str(path.relative_to(output_dir)) for path in generated]


def write_explanation_outputs(
    result: dict[str, Any],
    output_dir: Path,
    *,
    metadata: dict[str, Any] | None = None,
    write_plots: bool = True,
    write_standard_plots: bool = True,
    plot_backend: str = "auto",
    report_style: str | None = None,
    plot_theme: str | None = None,
    strict_no_fallback: bool = False,
) -> None:
    write_start = time.perf_counter()
    write_timing: dict[str, Any] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = metadata or {}
    frames: dict[str, pl.DataFrame] = result["frames"]
    aux_dim_frames: dict[str, pl.DataFrame] = result.get("aux_dim_frames", {})
    aux_projection_frames: dict[str, pl.DataFrame] = result.get("aux_projection_frames", {})
    stage_start = time.perf_counter()
    for name, frame in frames.items():
        if not _is_empty_frame(frame):
            _write_csv(frame, output_dir / f"{name}.csv")
    aux_dir = output_dir / "aux_dims"
    for name, frame in aux_dim_frames.items():
        aux_dir.mkdir(parents=True, exist_ok=True)
        safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name)
        _write_csv(frame, aux_dir / f"{safe_name}.csv")
    projection_dir = output_dir / "aux_projections"
    for name, frame in aux_projection_frames.items():
        if not _is_empty_frame(frame):
            projection_dir.mkdir(parents=True, exist_ok=True)
            safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name)
            _write_csv(frame, projection_dir / f"{safe_name}.csv")
    _mark_elapsed(write_timing, "csv_s", stage_start)
    stage_start = time.perf_counter()
    standard_plot_details: dict[str, Any] = {}
    plots_generated = (
        _plot_all_explanation_figures(
            frames,
            aux_dim_frames,
            output_dir,
            aux_projection_frames=aux_projection_frames,
            plot_backend=plot_backend,
            plot_timing=standard_plot_details,
            strict_no_fallback=bool(strict_no_fallback),
        )
        if write_plots and write_standard_plots
        else []
    )
    _mark_elapsed(write_timing, "plots_s", stage_start)
    write_timing["standard_plot_details"] = standard_plot_details
    resolved_report_style = _normalize_report_style(report_style or result["summary"].get("report_style", "paper"))
    resolved_plot_theme = _normalize_plot_theme(plot_theme or result["summary"].get("plot_theme", "paper"))
    summary = {
        **result["summary"],
        "metadata": metadata,
        "plot_backend": _normalize_plot_backend(plot_backend),
        "report_style": resolved_report_style,
        "plot_theme": resolved_plot_theme,
        "standard_plots": bool(write_standard_plots),
        "plots_generated": plots_generated,
        "write_timing": write_timing,
    }
    paper_tables: dict[str, str] = {}
    paper_plots: list[str] = []
    if resolved_report_style == "paper":
        stage_start = time.perf_counter()
        paper_tables = _write_paper_tables(
            output_dir,
            frames=frames,
            summary=summary,
            metadata=metadata,
        )
        _mark_elapsed(write_timing, "paper_tables_s", stage_start)
        if write_plots:
            stage_start = time.perf_counter()
            paper_plot_details: dict[str, float] = {}
            paper_plots = _plot_all_paper_figures(
                output_dir,
                frames=frames,
                summary=summary,
                metadata=metadata,
                paper_tables=paper_tables,
                plot_timing=paper_plot_details,
            )
            _mark_elapsed(write_timing, "paper_plots_s", stage_start)
            write_timing["paper_plot_details"] = paper_plot_details
        else:
            write_timing["paper_plot_details"] = {}
        summary["paper_tables"] = paper_tables
        summary["paper_plots"] = paper_plots
    else:
        write_timing["paper_tables_s"] = 0.0
        write_timing["paper_plots_s"] = 0.0
        write_timing["paper_plot_details"] = {}
    stage_start = time.perf_counter()
    _write_markdown_report(
        output_dir / "report.md",
        metadata=metadata,
        summary=summary,
        frames=frames,
    )
    _mark_elapsed(write_timing, "report_md_s", stage_start)
    if resolved_report_style == "paper":
        stage_start = time.perf_counter()
        _write_paper_report(
            output_dir / "paper_explainability_report.md",
            metadata=metadata,
            summary=summary,
            frames=frames,
            paper_tables=paper_tables,
            paper_plots=paper_plots,
        )
        _mark_elapsed(write_timing, "paper_report_md_s", stage_start)
        stage_start = time.perf_counter()
        _write_paper_summary(
            output_dir / "paper_explainability_summary.json",
            metadata=metadata,
            summary=summary,
            paper_tables=paper_tables,
            paper_plots=paper_plots,
        )
        _mark_elapsed(write_timing, "paper_summary_json_s", stage_start)
    else:
        write_timing["paper_report_md_s"] = 0.0
        write_timing["paper_summary_json_s"] = 0.0
    write_timing["total_s"] = float(time.perf_counter() - write_start)
    stage_start = time.perf_counter()
    (output_dir / "summary.json").write_text(
        json.dumps(_to_builtin(summary), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _mark_elapsed(write_timing, "summary_json_s", stage_start)
    write_timing["total_s"] = float(time.perf_counter() - write_start)
    result["summary"]["write_timing"] = write_timing
    (output_dir / "explainability_timing.json").write_text(
        json.dumps(_to_builtin({"compute_timing": result["summary"].get("timing", {}), "write_timing": write_timing}), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _strip_orig_mod_prefix(state_dict: dict[str, Any]) -> dict[str, Any]:
    if not state_dict:
        return state_dict
    if all(str(key).startswith("_orig_mod.") for key in state_dict.keys()):
        return {str(key).removeprefix("_orig_mod."): value for key, value in state_dict.items()}
    return state_dict


def load_model_from_checkpoint(
    config: ExperimentConfig,
    panel: PanelData,
    checkpoint_path: Path,
    device: torch.device,
    *,
    strict: bool = False,
) -> tuple[nn.Module, dict[str, Any]]:
    model = build_model(
        config=config,
        lookback=config.training.lookback,
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
    ).to(device)
    checkpoint = torch.load(checkpoint_path, map_location="cpu")
    state_dict = checkpoint.get("model_state_dict", checkpoint)
    state_dict = _strip_orig_mod_prefix(state_dict)
    incompatible = model.load_state_dict(state_dict, strict=strict)
    model.eval()
    info = {
        "checkpoint_epoch": checkpoint.get("epoch"),
        "checkpoint_best_val_loss": checkpoint.get("best_val_loss"),
        "missing_keys": list(getattr(incompatible, "missing_keys", [])),
        "unexpected_keys": list(getattr(incompatible, "unexpected_keys", [])),
    }
    return model, info


def _fold_dir(output_dir: Path, fold_id: int) -> Path:
    return output_dir / f"fold_{int(fold_id):02d}"


def _select_fold_and_checkpoint(
    folds: list[WalkForwardFold],
    output_dir: Path,
    fold_id: int | None,
    checkpoint: Path | None,
) -> tuple[WalkForwardFold, Path]:
    if fold_id is None:
        candidates = []
        for fold in folds:
            ckpt = _fold_dir(output_dir, fold.fold_id) / "checkpoint_best.pt"
            if ckpt.exists():
                candidates.append((fold.fold_id, fold, ckpt))
        if not candidates:
            raise FileNotFoundError(f"No fold checkpoint_best.pt found under {output_dir}")
        _, fold, ckpt = sorted(candidates, key=lambda item: item[0])[-1]
        return fold, Path(checkpoint) if checkpoint is not None else ckpt
    matches = [fold for fold in folds if int(fold.fold_id) == int(fold_id)]
    if not matches:
        raise ValueError(f"fold={fold_id} is not present; available folds={[fold.fold_id for fold in folds]}")
    fold = matches[0]
    ckpt = Path(checkpoint) if checkpoint is not None else _fold_dir(output_dir, fold.fold_id) / "checkpoint_best.pt"
    if not ckpt.exists():
        raise FileNotFoundError(f"Checkpoint not found: {ckpt}")
    return fold, ckpt


def _available_checkpoint_folds(folds: list[WalkForwardFold], output_dir: Path) -> list[int]:
    available: list[int] = []
    for fold in folds:
        ckpt = _fold_dir(output_dir, fold.fold_id) / "checkpoint_best.pt"
        if ckpt.exists():
            available.append(int(fold.fold_id))
    return sorted(available)


def _first_year_indices(panel: PanelData, indices: np.ndarray) -> np.ndarray:
    if indices.size == 0:
        return indices
    dates = np.asarray(panel.dates[indices], dtype="datetime64[D]").astype(object)
    years = np.array([int(date.year) for date in dates], dtype=np.int32)
    first_year = int(years.min())
    return indices[years == first_year]


def _dataset_for_split(
    panel: PanelData,
    fold: WalkForwardFold,
    split: str,
    lookback: int,
    *,
    first_test_year_only: bool = True,
) -> CrossSectionalDataset:
    split_norm = split.strip().lower()
    if split_norm == "train":
        indices = fold.train_indices
    elif split_norm == "val":
        indices = fold.val_indices
    elif split_norm == "test":
        indices = fold.test_indices
        if first_test_year_only:
            indices = _first_year_indices(panel, indices)
    else:
        raise ValueError("split must be one of: train, val, test")
    return CrossSectionalDataset(panel, indices, lookback)


def _sample_dataset(dataset: CrossSectionalDataset, max_rows: int, method: str) -> tuple[dict[str, torch.Tensor], np.ndarray]:
    n_rows = len(dataset)
    n_take = max(1, min(int(max_rows), n_rows))
    method = method.strip().lower()
    if method == "last":
        positions = np.arange(n_rows - n_take, n_rows, dtype=np.int64)
    elif method == "first":
        positions = np.arange(0, n_take, dtype=np.int64)
    else:
        positions = np.linspace(0, n_rows - 1, n_take, dtype=np.int64)
    samples = [dataset[int(pos)] for pos in positions]
    batch = collate_batch(samples)
    return batch, dataset.valid_indices[positions]


def load_explanation_context(
    *,
    config_path: Path,
    output_dir: Path | None,
    fold_id: int | None,
    checkpoint: Path | None,
    split: str,
) -> LoadedExplanationContext:
    config = load_config(config_path)
    resolved_output_dir = Path(output_dir if output_dir is not None else config.runner.output_dir)
    panel = build_panel(
        config.data.parquet_root,
        use_rapids=config.data.use_rapids,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tradable_mode=config.data.tradable_mode,
        trading_volume_policy=config.data.trading_volume_policy,
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
    fold, checkpoint_path = _select_fold_and_checkpoint(folds, resolved_output_dir, fold_id, checkpoint)
    return LoadedExplanationContext(
        config=config,
        panel=panel,
        folds=folds,
        fold=fold,
        split=split,
        checkpoint_path=checkpoint_path,
        output_dir=resolved_output_dir,
    )


def _write_cross_asset_skip(
    cross_asset_dir: Path,
    *,
    reason: str,
    message: str,
    error: str | None = None,
) -> dict[str, Any]:
    cross_asset_dir.mkdir(parents=True, exist_ok=True)
    skipped: dict[str, Any] = {
        "enabled": False,
        "module": "abstract_cross_asset_transmission",
        "skipped_reason": reason,
    }
    if error is not None:
        skipped["error"] = error
    (cross_asset_dir / "abstract_cross_asset_summary.json").write_text(
        json.dumps(skipped, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (cross_asset_dir / "abstract_cross_asset_report.md").write_text(
        "# Abstract Cross-Asset Transmission\n\n" + message.rstrip() + "\n",
        encoding="utf-8",
    )
    return skipped


def run_loaded_model_explanation(
    *,
    config: ExperimentConfig,
    panel: PanelData,
    fold: WalkForwardFold,
    model: nn.Module,
    checkpoint_path: Path,
    output_dir: Path,
    split: str,
    explain_output_dir: Path | None,
    settings: ExplainabilitySettings,
    write_plots: bool = True,
    plot_backend: str | None = None,
    device: torch.device | None = None,
    checkpoint_info: dict[str, Any] | None = None,
    timing_file_name: str | None = None,
    write_fold_stability: bool = False,
) -> Path:
    total_start = time.perf_counter()
    config_strict_no_fallback = bool(getattr(config.training, "strict_no_fallback", False))
    if config_strict_no_fallback and not bool(settings.strict_no_fallback):
        settings = replace(settings, strict_no_fallback=True)
    device = device or next(model.parameters()).device
    split_norm = split.strip().lower()
    runner_timing: dict[str, float | str | int | bool] = {
        "fold_id": int(fold.fold_id),
        "split": split_norm,
        "enabled": True,
        "loaded_model_reused": True,
    }
    sample_start = time.perf_counter()
    dataset = _dataset_for_split(
        panel,
        fold,
        split_norm,
        config.training.lookback,
        first_test_year_only=settings.first_test_year_only,
    )
    batch, date_indices = _sample_dataset(dataset, settings.max_rows, settings.sample_method)
    dates = [str(np.datetime_as_string(panel.dates[int(idx)], unit="D")) for idx in date_indices]
    runner_timing["sample_s"] = float(time.perf_counter() - sample_start)
    runner_timing["sample_rows"] = int(len(dates))
    runner_timing["ig_steps"] = int(settings.ig_steps)
    runner_timing["perturb"] = bool(settings.perturb)
    runner_timing["write_plots"] = bool(write_plots)

    was_training = model.training
    model.eval()
    compute_start = time.perf_counter()
    try:
        result = explain_batch_row_chunked(
            model,
            batch,
            feature_names=panel.feature_names,
            symbols=panel.symbols,
            dates=dates,
            settings=settings,
            device=device,
        )
    finally:
        if was_training:
            model.train()
    runner_timing["compute_s"] = float(time.perf_counter() - compute_start)

    destination = explain_output_dir or (
        output_dir
        / "explainability"
        / f"fold_{int(fold.fold_id):02d}_{split_norm}"
    )
    metadata = {
        "model_name": config.training.model_name,
        "fold_id": int(fold.fold_id),
        "split": split_norm,
        "checkpoint": str(checkpoint_path),
        "device": str(device),
        "sample_rows": int(len(dates)),
        "first_test_year_only": bool(settings.first_test_year_only),
        "config_lookback": int(config.training.lookback),
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
        **(checkpoint_info or {}),
    }
    resolved_plot_backend = plot_backend or str(getattr(config.training, "plot_backend", "auto"))
    write_start = time.perf_counter()
    write_explanation_outputs(
        result,
        destination,
        metadata=metadata,
        write_plots=write_plots,
        write_standard_plots=bool(settings.standard_plots),
        plot_backend=resolved_plot_backend,
        report_style=settings.report_style,
        plot_theme=settings.plot_theme,
    )
    runner_timing["write_s"] = float(time.perf_counter() - write_start)
    cross_asset_summary: dict[str, Any] = {}
    if bool(settings.cross_asset_enabled):
        from stockagent.explainability_cross_asset import abstract_cross_asset_transmission

        row_chunking = result.get("summary", {}).get("row_chunking", {})
        cross_asset_dir = destination / "abstract_cross_asset_transmission"
        skip_cross_asset = bool(isinstance(row_chunking, dict) and row_chunking.get("cuda_oom_fallback"))
        cross_asset_start = time.perf_counter()
        if skip_cross_asset:
            if bool(settings.strict_no_fallback):
                raise RuntimeError(
                    "Cross-asset explainability would be skipped because main explainability used CUDA OOM fallback; "
                    "strict_no_fallback=true so skip fallback is disabled."
                )
            cross_asset_summary = _write_cross_asset_skip(
                cross_asset_dir,
                reason="main_explainability_cuda_oom_fallback",
                message="Skipped because main explainability already required CUDA OOM fallback.",
            )
            print("[explain] cross-asset skipped after CUDA OOM fallback in main explainability")
        else:
            try:
                cross_asset_summary = abstract_cross_asset_transmission(
                    model,
                    batch,
                    feature_names=panel.feature_names,
                    symbols=panel.symbols,
                    dates=dates,
                    output_dir=destination,
                    settings=_cross_asset_settings_from_explainability(settings),
                    device=device,
                )
            except RuntimeError as exc:
                if not _is_cuda_oom(exc):
                    raise
                if bool(settings.strict_no_fallback):
                    raise RuntimeError(
                        "CUDA OOM during cross-asset explainability; strict_no_fallback=true so "
                        "skipped-output fallback is disabled."
                    ) from exc
                _clear_explainability_runtime_cache()
                cross_asset_summary = _write_cross_asset_skip(
                    cross_asset_dir,
                    reason="cuda_oom",
                    message="Skipped after CUDA out-of-memory during cross-asset analysis.",
                    error=str(exc),
                )
                print("[explain] cross-asset skipped after CUDA OOM")
        runner_timing["cross_asset_s"] = float(time.perf_counter() - cross_asset_start)
    else:
        runner_timing["cross_asset_s"] = 0.0

    if write_fold_stability and bool(settings.fold_stability):
        stability_start = time.perf_counter()
        stability_dir = write_fold_stability_outputs(output_dir / "explainability")
        runner_timing["fold_stability_s"] = float(time.perf_counter() - stability_start)
        runner_timing["fold_stability_output"] = str(stability_dir) if stability_dir is not None else ""
    else:
        runner_timing["fold_stability_s"] = 0.0

    runner_timing["total_s"] = float(time.perf_counter() - total_start)
    if timing_file_name:
        timing_path = destination / timing_file_name
        timing_path.write_text(
            json.dumps(
                _to_builtin(
                    {
                        **runner_timing,
                        "compute_timing": result.get("summary", {}).get("timing", {}),
                        "write_timing": result.get("summary", {}).get("write_timing", {}),
                        "cross_asset_summary": cross_asset_summary,
                    }
                ),
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        print(
            f"[Fold {fold.fold_id}] explainability timing: "
            f"total={float(runner_timing['total_s']):.3f}s "
            f"compute={float(runner_timing['compute_s']):.3f}s "
            f"write={float(runner_timing['write_s']):.3f}s "
            f"cross_asset={float(runner_timing['cross_asset_s']):.3f}s "
            f"stability={float(runner_timing['fold_stability_s']):.3f}s "
            f"json={timing_path}"
        )

    destination_out = destination
    del result, batch, dataset, model
    _clear_explainability_runtime_cache()
    return destination_out


def run_checkpoint_explanation(
    *,
    config_path: Path,
    output_dir: Path | None,
    fold_id: int | None,
    checkpoint: Path | None,
    split: str,
    explain_output_dir: Path | None,
    settings: ExplainabilitySettings,
    device_override: str | None = None,
    strict: bool = False,
    write_plots: bool = True,
    plot_backend: str | None = None,
) -> Path:
    context = load_explanation_context(
        config_path=config_path,
        output_dir=output_dir,
        fold_id=fold_id,
        checkpoint=checkpoint,
        split=split,
    )
    device = _device_from_config(context.config, device_override)
    if device.type == "cuda":
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
    model, checkpoint_info = load_model_from_checkpoint(
        context.config,
        context.panel,
        context.checkpoint_path,
        device,
        strict=strict,
    )
    return run_loaded_model_explanation(
        config=context.config,
        panel=context.panel,
        fold=context.fold,
        model=model,
        checkpoint_path=context.checkpoint_path,
        output_dir=context.output_dir,
        split=split,
        explain_output_dir=explain_output_dir,
        settings=settings,
        write_plots=write_plots,
        plot_backend=plot_backend,
        device=device,
        checkpoint_info=checkpoint_info,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explain a trained stockAgent model checkpoint.")
    parser.add_argument("--config", default="configs/experiment_baseline.yaml", type=Path)
    parser.add_argument("--output-dir", default=None, type=Path)
    parser.add_argument("--fold", default=None, type=int, help="Fold id. If omitted, explains all folds with checkpoint_best.pt.")
    parser.add_argument("--checkpoint", default=None, type=Path, help="Optional explicit checkpoint path.")
    parser.add_argument("--split", default="test", choices=("train", "val", "test"))
    parser.add_argument("--explain-output-dir", default=None, type=Path)
    parser.add_argument("--device", default=None, help="Override config environment.device, e.g. cuda or cpu.")
    parser.add_argument("--top-k", default=20, type=int)
    parser.add_argument("--max-rows", default=32, type=int)
    parser.add_argument("--ig-steps", default=8, type=int)
    parser.add_argument("--ig-batch-size", default=0, type=int, help="Batch IG alpha steps together; 0 selects an automatic safe chunk size.")
    parser.add_argument("--sample-method", default="even", choices=("even", "first", "last"))
    parser.add_argument("--all-test-years", action="store_true", help="For --split test, explain all test years instead of only the first test year.")
    parser.add_argument(
        "--perturb",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run feature perturbation sensitivity; on by default for complete offline explainability.",
    )
    parser.add_argument("--perturb-batch-size", default=0, type=int, help="Batch feature-day perturbations together; 0 selects an automatic safe chunk size.")
    parser.add_argument("--perturb-max-auto-batch-size", default=5, type=int)
    parser.add_argument("--perturb-max-input-elements", default=32_000_000, type=int)
    parser.add_argument(
        "--plots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write PNG plots; on by default for complete offline explainability.",
    )
    parser.add_argument("--report-style", default="paper", choices=("paper", "standard", "none"))
    parser.add_argument("--plot-theme", default="paper", choices=("paper", "standard"))
    parser.add_argument(
        "--standard-plots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write the legacy plots/ PNG set in addition to paper plots.",
    )
    parser.add_argument("--no-interactive-plots", action="store_true", help="Keep explainability output static only.")
    parser.add_argument(
        "--shap",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run score-head surrogate SHAP; on by default for complete offline explainability.",
    )
    parser.add_argument("--shap-mode", default="score_head_surrogate", choices=("score_head_surrogate", "off", "none"))
    parser.add_argument("--case-study-top-k", default=5, type=int)
    parser.add_argument(
        "--regime-analysis",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run regime-analysis tables and plots; on by default for complete offline explainability.",
    )
    parser.add_argument(
        "--fold-stability",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write cross-fold attribution-stability summary; on by default for complete offline explainability.",
    )
    parser.add_argument(
        "--plot-backend",
        default=None,
        choices=("auto", "matplotlib", "rapids_datashader"),
        help="PNG plot backend. auto uses RAPIDS Datashader for dense plots when CUDA is available.",
    )
    parser.add_argument(
        "--umap",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run cuML UMAP aux projections; on by default for complete offline explainability.",
    )
    parser.add_argument("--umap-max-points", default=10000, type=int)
    parser.add_argument("--umap-max-projections", default=0, type=int, help="Maximum aux tensors to project with UMAP; 0 means no limit.")
    parser.add_argument("--umap-n-neighbors", default=15, type=int)
    parser.add_argument("--umap-min-dist", default=0.1, type=float)
    parser.add_argument(
        "--cross-asset",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write abstract_cross_asset_transmission outputs; on by default for complete offline explainability.",
    )
    parser.add_argument("--cross-asset-max-sources", default=24, type=int)
    parser.add_argument("--cross-asset-max-targets", default=24, type=int)
    parser.add_argument("--cross-asset-top-edges", default=150, type=int)
    parser.add_argument("--cross-asset-source-chunk-size", default=2, type=int)
    parser.add_argument("--cross-asset-perturb-scale", default=1.0, type=float)
    parser.add_argument(
        "--cross-asset-shocks",
        default="zero,momentum,gap,volume,volatility,liquidity",
        help="Comma-separated abstract shocks to run.",
    )
    parser.add_argument(
        "--cross-asset-attention-flow",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use captured attention as transmission evidence when available.",
    )
    parser.add_argument("--cross-asset-attention-capture-rows", default=4, type=int)
    parser.add_argument(
        "--cross-asset-validated-transmission",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Multiply perturbation evidence by attention evidence when available.",
    )
    parser.add_argument(
        "--cross-asset-role-embedding",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write latent role embedding table and plot when aux tensors are available.",
    )
    parser.add_argument("--strict", action="store_true", help="Load checkpoint with strict=True.")
    parser.add_argument(
        "--strict-no-fallback",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fail instead of using degraded explainability, plotting, or cross-asset fallback paths.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = ExplainabilitySettings(
        top_k=args.top_k,
        max_rows=args.max_rows,
        ig_steps=args.ig_steps,
        ig_batch_size=args.ig_batch_size,
        perturb=bool(args.perturb),
        perturb_batch_size=args.perturb_batch_size,
        perturb_max_auto_batch_size=args.perturb_max_auto_batch_size,
        perturb_max_input_elements=args.perturb_max_input_elements,
        sample_method=args.sample_method,
        first_test_year_only=not args.all_test_years,
        report_style=args.report_style,
        plot_theme=args.plot_theme,
        standard_plots=bool(args.standard_plots),
        interactive_plots=not args.no_interactive_plots,
        shap_enabled=bool(args.shap),
        shap_mode=args.shap_mode,
        case_study_top_k=args.case_study_top_k,
        regime_analysis=bool(args.regime_analysis),
        fold_stability=bool(args.fold_stability),
        umap_enabled=bool(args.umap),
        umap_max_points=args.umap_max_points,
        umap_max_projections=args.umap_max_projections,
        umap_n_neighbors=args.umap_n_neighbors,
        umap_min_dist=args.umap_min_dist,
        cross_asset_enabled=bool(args.cross_asset),
        cross_asset_max_sources=max(1, int(args.cross_asset_max_sources)),
        cross_asset_max_targets=max(1, int(args.cross_asset_max_targets)),
        cross_asset_top_edges=max(1, int(args.cross_asset_top_edges)),
        cross_asset_source_chunk_size=max(1, int(args.cross_asset_source_chunk_size)),
        cross_asset_perturb_scale=float(args.cross_asset_perturb_scale),
        cross_asset_shocks=tuple(
            value.strip().lower() for value in str(args.cross_asset_shocks).split(",") if value.strip()
        ),
        cross_asset_attention_flow=bool(args.cross_asset_attention_flow),
        cross_asset_attention_capture_rows=max(1, int(args.cross_asset_attention_capture_rows)),
        cross_asset_validated_transmission=bool(args.cross_asset_validated_transmission),
        cross_asset_role_embedding=bool(args.cross_asset_role_embedding),
        strict_no_fallback=bool(args.strict_no_fallback),
    )
    # Default behavior: if neither --fold nor --checkpoint is provided,
    # run explainability for all folds that have checkpoint_best.pt.
    run_all_folds = args.fold is None and args.checkpoint is None
    if run_all_folds:
        config = load_config(args.config)
        resolved_output_dir = Path(args.output_dir if args.output_dir is not None else config.runner.output_dir)
        panel = build_panel(
            config.data.parquet_root,
            use_rapids=config.data.use_rapids,
            benchmark_name=config.data.benchmark_name,
            usd_only_trading_pairs=config.data.usd_only_trading_pairs,
            tradable_mode=config.data.tradable_mode,
            trading_volume_policy=config.data.trading_volume_policy,
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
        fold_ids = _available_checkpoint_folds(folds, resolved_output_dir)
        if not fold_ids:
            raise FileNotFoundError(f"No fold checkpoint_best.pt found under {resolved_output_dir}")

        device = _device_from_config(config, args.device)
        if device.type == "cuda":
            torch.set_float32_matmul_precision("high")
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True
        folds_by_id = {int(fold.fold_id): fold for fold in folds}
        print(f"explaining folds: {fold_ids}")
        for fold_id in fold_ids:
            fold_output_dir = args.explain_output_dir
            if fold_output_dir is not None:
                fold_output_dir = Path(fold_output_dir) / f"fold_{int(fold_id):02d}_{args.split.strip().lower()}"
            checkpoint_path = _fold_dir(resolved_output_dir, fold_id) / "checkpoint_best.pt"
            model: nn.Module | None = None
            try:
                model, checkpoint_info = load_model_from_checkpoint(
                    config,
                    panel,
                    checkpoint_path,
                    device,
                    strict=args.strict,
                )
                out_dir = run_loaded_model_explanation(
                    config=config,
                    panel=panel,
                    fold=folds_by_id[int(fold_id)],
                    model=model,
                    checkpoint_path=checkpoint_path,
                    output_dir=resolved_output_dir,
                    split=args.split,
                    explain_output_dir=fold_output_dir,
                    settings=settings,
                    write_plots=bool(args.plots),
                    plot_backend=args.plot_backend,
                    device=device,
                    checkpoint_info=checkpoint_info,
                )
            finally:
                if model is not None:
                    del model
                _clear_explainability_runtime_cache()
            print(f"explainability output (fold {fold_id}): {out_dir}")
        if settings.fold_stability:
            stability_dir = write_fold_stability_outputs(
                resolved_output_dir / "explainability",
                strict_no_fallback=bool(settings.strict_no_fallback),
            )
            if stability_dir is not None:
                print(f"fold stability output: {stability_dir}")
        return

    out_dir = run_checkpoint_explanation(
        config_path=args.config,
        output_dir=args.output_dir,
        fold_id=args.fold,
        checkpoint=args.checkpoint,
        split=args.split,
        explain_output_dir=args.explain_output_dir,
        settings=settings,
        device_override=args.device,
        strict=bool(args.strict or args.strict_no_fallback),
        write_plots=bool(args.plots),
        plot_backend=args.plot_backend,
    )
    print(f"explainability output: {out_dir}")


if __name__ == "__main__":
    main()
