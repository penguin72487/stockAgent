from __future__ import annotations

import argparse
import atexit
import csv
import gc
import inspect
import json
import math
import os
import time
import warnings
from contextlib import nullcontext
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import numpy as np
import polars as pl
import torch
from torch import nn
from tqdm.auto import tqdm

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
_DEFAULT_MARKET_ARTIFACTS_ROOT = Path("artifacts/markets")
_DEFAULT_MARKET_CONFIG_ROOT = Path("configs/markets")
_PLOT_ASPECT_RATIO = 17.0 / 6.0
_DEFAULT_PLOT_HEIGHT = 6.0
_DEFAULT_PLOT_HEIGHT_PX = 600
_DEFAULT_PLOT_WIDTH_PX = int(round(_DEFAULT_PLOT_HEIGHT_PX * _PLOT_ASPECT_RATIO))


def _figsize_17_6(height: float = _DEFAULT_PLOT_HEIGHT) -> tuple[float, float]:
    height = max(1.0, float(height))
    return height * _PLOT_ASPECT_RATIO, height


def _figsize_for_rows(
    row_count: int,
    *,
    width: float = 20.0,
    row_height: float = 0.28,
    overhead: float = 2.5,
    min_height: float = 6.0,
    max_height: float = 80.0,
) -> tuple[float, float]:
    """Return a readable portrait-capable canvas for dense labelled plots."""
    height = max(float(min_height), float(overhead) + float(row_height) * max(1, int(row_count)))
    return max(8.0, float(width)), min(float(max_height), height)


def _plot_width_px_17_6(height: int) -> int:
    return max(32, int(round(max(32, int(height)) * _PLOT_ASPECT_RATIO)))


def _pad_saved_image_to_17_6(path: Path) -> None:
    try:
        from PIL import Image

        with Image.open(path) as image:
            width, height = image.size
            if width <= 0 or height <= 0:
                return
            current = width / height
            if abs(current - _PLOT_ASPECT_RATIO) < 0.002:
                return
            if current < _PLOT_ASPECT_RATIO:
                target_width = int(round(height * _PLOT_ASPECT_RATIO))
                target_height = height
            else:
                target_width = width
                target_height = int(round(width / _PLOT_ASPECT_RATIO))
            target_width = max(width, target_width)
            target_height = max(height, target_height)
            canvas = Image.new(image.mode if image.mode in {"RGB", "RGBA"} else "RGB", (target_width, target_height), "white")
            canvas.paste(image.convert(canvas.mode), ((target_width - width) // 2, (target_height - height) // 2))
            canvas.save(path)
    except Exception:
        return


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
    pad_to_standard_aspect = bool(kwargs.pop("pad_to_standard_aspect", True))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _sanitize_matplotlib_axis_limits(fig)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=_MATPLOTLIB_TRANSFORM_DOT_WARNING,
            category=RuntimeWarning,
        )
        fig.savefig(output_path, **kwargs)
    if pad_to_standard_aspect:
        _pad_saved_image_to_17_6(output_path)

FEATURE_GROUP_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Return", ("logret", "return", "ret_", "_ret")),
    ("Volume", ("volume", "vol", "turnover", "amount")),
    ("Candlestick", ("body", "clv", "kline", "candle")),
    ("Shadow", ("shadow", "upper", "lower")),
    ("Position", ("range", "rank", "zscore", "position", "price_level")),
)


@dataclass(slots=True)
class ExplainabilitySettings:
    progress_enabled: bool = True
    max_rows: int = 0
    row_chunk_size: int = 0
    amp_dtype: str = "fp32"
    compile_model: bool = False
    ig_steps: int = 8
    ig_batch_size: int = 0
    perturb: bool = True
    perturb_batch_size: int = 0
    # Measured RTX 5070 Ti fold-21 throughput optimum.  These are aggregate
    # work-set budgets: _auto_repeat_chunk_size divides them by the current
    # date microbatch, preserving the same safe/fast total repeated rows.
    perturb_max_auto_batch_size: int = 48
    perturb_max_input_elements: int = 576_000_000
    counterfactual_compile: bool = True
    sample_method: str = "even"
    first_test_year_only: bool = False
    report_style: str = "paper"
    plot_theme: str = "paper"
    standard_plots: bool = True
    interactive_plots: bool = False
    shap_enabled: bool = True
    shap_mode: str = "score_head_surrogate"
    regime_analysis: bool = True
    fold_stability: bool = True
    umap_enabled: bool = True
    umap_max_points: int = 0
    umap_max_projections: int = 0
    umap_n_neighbors: int = 15
    umap_min_dist: float = 0.1
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


@dataclass(frozen=True, slots=True)
class MarketExplainabilityRun:
    market: str
    config_path: Path
    output_dir: Path


@dataclass(slots=True)
class ExplainDatasetBatchSource:
    """Lazily materialize selected explainability dates in bounded host chunks.

    Standalone explainability can cover thousands of dates.  Materializing all
    overlapping ``[lookback, symbols, features]`` windows first can consume
    hundreds of GiB even though the GPU path is row-microbatched later.  This
    source keeps the canonical dataset semantics while deferring window copies
    until a bounded chunk is actually consumed.
    """

    dataset: CrossSectionalDataset
    positions: np.ndarray

    def __post_init__(self) -> None:
        self.positions = np.asarray(self.positions, dtype=np.int64)

    def __len__(self) -> int:
        return int(self.positions.size)

    @property
    def num_rows(self) -> int:
        return len(self)

    @property
    def lookback(self) -> int:
        return int(self.dataset.lookback)

    @property
    def num_symbols(self) -> int:
        return int(self.dataset.features_t.size(1))

    @property
    def num_features(self) -> int:
        return int(self.dataset.features_t.size(2))

    @property
    def date_indices(self) -> np.ndarray:
        return self.dataset.valid_indices[self.positions]

    def materialize(self, start: int, end: int) -> dict[str, torch.Tensor]:
        start = max(0, int(start))
        end = min(len(self), max(start, int(end)))
        if end <= start:
            raise ValueError("ExplainDatasetBatchSource cannot materialize an empty row range.")
        samples = [self.dataset[int(pos)] for pos in self.positions[start:end]]
        return collate_batch(samples)


def settings_from_training_config(training: Any) -> ExplainabilitySettings:
    """Build the post-training explainability settings from TrainingConfig.

    Keep these defaults aligned with explain_model.py CLI defaults, not with the
    historical ExplainabilitySettings dataclass defaults.
    """

    return ExplainabilitySettings(
        max_rows=int(getattr(training, "explain_max_rows", 32)),
        row_chunk_size=int(getattr(training, "explain_row_chunk_size", 0)),
        ig_steps=int(getattr(training, "explain_ig_steps", 0)),
        ig_batch_size=int(getattr(training, "explain_ig_batch_size", 1)),
        perturb=bool(getattr(training, "explain_perturb", False)),
        perturb_batch_size=int(getattr(training, "explain_perturb_batch_size", 1)),
        perturb_max_auto_batch_size=int(getattr(training, "explain_perturb_max_auto_batch_size", 1)),
        perturb_max_input_elements=int(getattr(training, "explain_perturb_max_input_elements", 8_000_000)),
        counterfactual_compile=bool(getattr(training, "explain_counterfactual_compile", False)),
        sample_method=str(getattr(training, "explain_sample_method", "even")),
        first_test_year_only=bool(getattr(training, "explain_first_test_year_only", True)),
        report_style=str(getattr(training, "explain_report_style", "none")),
        plot_theme=str(getattr(training, "explain_plot_theme", "paper")),
        standard_plots=bool(getattr(training, "explain_standard_plots", False)),
        interactive_plots=bool(getattr(training, "explain_interactive_plots", False)),
        shap_enabled=bool(getattr(training, "explain_shap_enabled", False)),
        shap_mode=str(getattr(training, "explain_shap_mode", "score_head_surrogate")),
        regime_analysis=bool(getattr(training, "explain_regime_analysis", False)),
        fold_stability=bool(getattr(training, "explain_fold_stability", False)),
        umap_enabled=bool(getattr(training, "explain_umap_enabled", False)),
        umap_max_points=int(getattr(training, "explain_umap_max_points", 1000)),
        umap_max_projections=int(getattr(training, "explain_umap_max_projections", 0)),
        umap_n_neighbors=int(getattr(training, "explain_umap_n_neighbors", 15)),
        umap_min_dist=float(getattr(training, "explain_umap_min_dist", 0.1)),
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


def _values_by_sum(frame: pl.DataFrame, group_col: str, value_col: str) -> list[Any]:
    if _is_empty_frame(frame) or group_col not in frame.columns or value_col not in frame.columns:
        return []
    grouped = (
        _with_numeric(frame, value_col)
        .drop_nulls(subset=[group_col, value_col])
        .group_by(group_col)
        .agg(pl.col(value_col).sum().alias("__sum"))
        .sort("__sum", descending=True)
    )
    return grouped.get_column(group_col).to_list() if not grouped.is_empty() else []


def _render_table_markdown(frame: pl.DataFrame, limit: int | None = 20) -> str:
    if _is_empty_frame(frame):
        return "_No rows._"
    data = frame if limit is None else frame.head(limit)
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
    if requested.startswith("cuda") and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested for explanation, but torch.cuda.is_available() is False.")
    if requested == "cuda" and int(os.environ.get("WORLD_SIZE", "1")) > 1:
        requested = f"cuda:{int(os.environ.get('LOCAL_RANK', '0'))}"
    return torch.device(requested)


def _distributed_explainability_ready() -> bool:
    return torch.distributed.is_available() and torch.distributed.is_initialized()


def _distributed_rank() -> int:
    if _distributed_explainability_ready():
        return int(torch.distributed.get_rank())
    return int(os.environ.get("RANK", "0"))


def _distributed_world_size() -> int:
    if _distributed_explainability_ready():
        return int(torch.distributed.get_world_size())
    return int(os.environ.get("WORLD_SIZE", "1"))


def _distributed_barrier() -> None:
    if not _distributed_explainability_ready():
        return
    if torch.distributed.get_backend() == "nccl":
        torch.distributed.barrier(device_ids=[torch.cuda.current_device()])
    else:
        torch.distributed.barrier()


def _destroy_explainability_process_group() -> None:
    if _distributed_explainability_ready():
        torch.distributed.destroy_process_group()


def _initialize_explainability_process_group() -> bool:
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    if world_size <= 1 or _distributed_explainability_ready():
        return False
    if not torch.distributed.is_available():
        raise RuntimeError("WORLD_SIZE > 1 but torch.distributed is unavailable.")
    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    if torch.cuda.is_available():
        if local_rank < 0 or local_rank >= torch.cuda.device_count():
            raise RuntimeError(
                f"LOCAL_RANK={local_rank} is unavailable; visible CUDA devices={torch.cuda.device_count()}"
            )
        torch.cuda.set_device(local_rank)
        backend = "nccl"
    else:
        backend = "gloo"
    torch.distributed.init_process_group(backend=backend)
    atexit.register(_destroy_explainability_process_group)
    return True


def _normalize_explain_amp_dtype(value: str | None, config: ExperimentConfig | None = None) -> str:
    normalized = str(value or "config").strip().lower().replace("torch.", "")
    if normalized == "config":
        configured = getattr(getattr(config, "environment", None), "amp_dtype", "bf16")
        normalized = str(configured or "bf16").strip().lower().replace("torch.", "")
    aliases = {
        "bfloat16": "bf16",
        "float16": "fp16",
        "half": "fp16",
        "float32": "fp32",
        "full": "fp32",
        "none": "fp32",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"bf16", "fp16", "fp32"}:
        raise ValueError("amp_dtype must be one of: config, bf16, fp16, fp32")
    return normalized


def _explain_autocast_context(device: torch.device, amp_dtype: str):
    normalized = _normalize_explain_amp_dtype(amp_dtype)
    if device.type != "cuda" or normalized == "fp32":
        return nullcontext()
    dtype = torch.bfloat16 if normalized == "bf16" else torch.float16
    return torch.autocast(device_type="cuda", dtype=dtype)


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


def _surrogate_input_summary(x: torch.Tensor) -> torch.Tensor:
    """Keep the exact first/last/mean surrogate inputs without the full window."""
    value = x.detach().float()
    if int(value.size(1)) <= 1:
        return value[:, :1].cpu()
    first = value[:, 0]
    last = value[:, -1]
    mean = value.mean(dim=1)
    # A synthetic three-step window preserves the exact first, last, and mean
    # consumed by _score_head_surrogate_shap/_feature_correlations.
    middle = 3.0 * mean - first - last
    return torch.stack((first, middle, last), dim=1).cpu()


def _cuda_mem_get_info(device: torch.device) -> tuple[int, int] | None:
    if device.type != "cuda" or not torch.cuda.is_available():
        return None
    try:
        return torch.cuda.mem_get_info(device)
    except TypeError:
        return torch.cuda.mem_get_info()


def _auto_explain_row_chunk_size_from_shape(
    *,
    n_rows: int,
    lookback: int,
    n_symbols: int,
    n_features: int,
    settings: ExplainabilitySettings,
    device: torch.device,
) -> tuple[int, dict[str, Any]]:
    n_rows = max(1, int(n_rows))
    lookback = max(1, int(lookback))
    n_symbols = max(1, int(n_symbols))
    n_features = max(1, int(n_features))
    if n_rows <= 1:
        return max(1, n_rows), {"reason": "single_row", "rows": n_rows}
    override = os.environ.get("STOCKAGENT_EXPLAIN_ROW_CHUNK_SIZE")
    if override:
        try:
            value = max(1, min(n_rows, int(override)))
            return value, {"reason": "env_override", "rows": n_rows, "row_chunk_size": value}
        except ValueError:
            pass
    requested = int(settings.row_chunk_size)
    if requested > 0:
        value = max(1, min(n_rows, requested))
        return value, {"reason": "settings", "rows": n_rows, "row_chunk_size": value}

    bytes_per_row = max(1, lookback * n_symbols * n_features * 4)
    if device.type != "cuda" or not torch.cuda.is_available():
        # Keep exhaustive CPU runs bounded too.  This is a host materialization
        # bound, not a date-coverage limit.
        host_budget = 1 * 1024**3
        value = max(1, min(n_rows, host_budget // bytes_per_row))
        return value, {
            "reason": "host_budget",
            "rows": n_rows,
            "row_chunk_size": value,
            "bytes_per_row": int(bytes_per_row),
        }

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


def _auto_explain_row_chunk_size(
    batch: dict[str, torch.Tensor],
    settings: ExplainabilitySettings,
    device: torch.device,
) -> tuple[int, dict[str, Any]]:
    x = batch.get("x")
    if not torch.is_tensor(x) or x.ndim != 4:
        return 1, {"reason": "missing_x"}
    return _auto_explain_row_chunk_size_from_shape(
        n_rows=int(x.size(0)),
        lookback=int(x.size(1)),
        n_symbols=int(x.size(2)),
        n_features=int(x.size(3)),
        settings=settings,
        device=device,
    )


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


def _embedded_explainability_api(model: nn.Module) -> nn.Module | None:
    """Return the underlying model when exact preprojected forward is supported."""
    candidates = (model, getattr(model, "module", None), getattr(model, "_orig_mod", None))
    required = (
        "project_features_for_explainability",
        "embed_projected_for_explainability",
        "forward_from_embedded_explainability",
    )
    for candidate in candidates:
        if candidate is not None and all(callable(getattr(candidate, name, None)) for name in required):
            return candidate
    return None


def _forward_embedded_outputs(
    model: nn.Module,
    embedded: torch.Tensor,
    mask: torch.Tensor,
    *,
    compile_forward: bool = False,
) -> tuple[torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
    if compile_forward and callable(getattr(model, "forward_from_embedded_explainability_compiled", None)):
        output = model.forward_from_embedded_explainability_compiled(embedded, mask)
    else:
        output = model.forward_from_embedded_explainability(
            embedded,
            mask,
            return_aux=False,
        )
    weights, scores, aux = _normalize_model_output(output)
    return (
        torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0),
        torch.nan_to_num(scores, nan=0.0, posinf=0.0, neginf=0.0),
        aux,
    )


def _selection_from_weights(
    weights: torch.Tensor,
    mask: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Select the attribution target without silently dropping small positions.

    Every finite, tradable, non-zero position is included and weighted by its
    share of gross exposure.
    """
    valid_weights = weights.masked_fill(~mask, 0.0)
    finite_nonzero = mask & torch.isfinite(valid_weights) & (valid_weights != 0.0)
    selected = finite_nonzero
    direction = torch.sign(valid_weights).masked_fill(~selected, 0.0)
    gross_weight = valid_weights.abs().masked_fill(~selected, 0.0)
    return selected, direction, gross_weight


def _decision_target(
    scores: torch.Tensor,
    selected: torch.Tensor,
    direction: torch.Tensor,
    gross_weight: torch.Tensor,
) -> torch.Tensor:
    target_weight = gross_weight.to(dtype=scores.dtype).masked_fill(~selected, 0.0)
    denom = target_weight.sum().clamp_min(torch.finfo(scores.dtype).eps)
    return (scores * direction.to(dtype=scores.dtype) * target_weight).sum() / denom


def _gradient_x_input_attribution(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    selected: torch.Tensor,
    direction: torch.Tensor,
    gross_weight: torch.Tensor,
) -> torch.Tensor:
    model.zero_grad(set_to_none=True)
    x_grad = x.detach().clone().requires_grad_(True)
    _, scores, _ = _forward_outputs(model, x_grad, mask, return_aux=False)
    target = _decision_target(scores, selected, direction, gross_weight)
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
    gross_weight: torch.Tensor,
    steps: int,
    batch_size: int = 0,
    *,
    progress_enabled: bool = True,
) -> torch.Tensor:
    steps = max(0, int(steps))
    if steps <= 0:
        return torch.zeros_like(x)
    chunk_size = _auto_repeat_chunk_size(
        x,
        steps,
        int(batch_size),
        # Fold-21 profiling peaks at eight alpha rows for one explained date.
        # The element budget automatically scales that to two alpha rows for
        # the normal three-date VRAM microbatch.
        max_auto=8,
        max_input_elements=96_000_000,
    )
    total_grad = torch.zeros_like(x)
    starts = range(1, steps + 1, chunk_size)
    for start in tqdm(
        starts,
        total=math.ceil(steps / chunk_size),
        desc="Integrated Gradients",
        unit="batch",
        leave=False,
        disable=not progress_enabled,
    ):
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
        gross_weight_step = _repeat_first_dim(gross_weight, repeats)
        _, scores, _ = _forward_outputs(model, x_step, mask_step, return_aux=False)
        target = _decision_target(scores, selected_step, direction_step, gross_weight_step) * float(repeats)
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
    progress_enabled: bool = True,
    compile_forward: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    stage_start = time.perf_counter()
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
        "elapsed_s": 0.0,
        "perturbations_per_s": 0.0,
        "embedded_fast_path": False,
        "compiled_forward": False,
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
    # Treat max_auto_batch_size as an aggregate repeated-row budget, not a
    # per-date repeat count.  This keeps the compiled Transformer batch fixed
    # at 48 for B=1/2/3 date chunks (48/24/16 counterfactuals respectively).
    chunk_size = min(
        chunk_size,
        max(1, int(max_auto_batch_size) // max(1, int(x.size(0)))),
    )
    diagnostics["chunk_size"] = int(chunk_size)
    base_weights = base_weights.detach()
    base_scores = base_scores.detach()
    embedded_api = _embedded_explainability_api(model)
    base_projected: torch.Tensor | None = None
    base_embedded: torch.Tensor | None = None
    if embedded_api is not None:
        with torch.no_grad():
            base_projected = embedded_api.project_features_for_explainability(x.detach())
            base_embedded = embedded_api.embed_projected_for_explainability(base_projected)
        diagnostics["embedded_fast_path"] = True
        diagnostics["compiled_forward"] = bool(
            compile_forward
            and callable(getattr(embedded_api, "forward_from_embedded_explainability_compiled", None))
        )
    progress = tqdm(
        total=len(perturbations),
        desc="Feature-time perturbation",
        unit="cell",
        leave=False,
        disable=not progress_enabled,
    )
    with torch.no_grad():
        start = 0
        while start < len(perturbations):
            chunk = perturbations[start : start + chunk_size]
            repeats = len(chunk)
            work_chunk = chunk + ([chunk[-1]] * (chunk_size - repeats) if repeats < chunk_size else [])
            work_repeats = len(work_chunk)
            try:
                diagnostics["attempted_forward_batches"] = int(diagnostics["attempted_forward_batches"]) + 1
                mask_perturbed = _repeat_first_dim(mask, work_repeats)
                if embedded_api is not None and base_projected is not None and base_embedded is not None:
                    # Reproject only the R changed [B,S,F] time slices, then
                    # reuse the base [B,L,S,D] embedding for the unchanged
                    # market.  This replaces O(R*B*L*S*F) copying/projection
                    # with O(B*L*S*F + R*B*S*F + R*B*L*S*D).
                    time_indices = torch.as_tensor(
                        [item[0] for item in work_chunk], device=x.device, dtype=torch.long
                    )
                    changed_slices = x.detach().index_select(1, time_indices).permute(1, 0, 2, 3).contiguous()
                    for local_idx, (_, feat_idx, _) in enumerate(work_chunk):
                        changed_slices[local_idx, :, :, feat_idx] = 0.0
                    changed_projected = embedded_api.project_features_for_explainability(changed_slices)
                    base_projected_slices = base_projected.index_select(1, time_indices).permute(1, 0, 2, 3)
                    embedded_perturbed = base_embedded.unsqueeze(0).expand(
                        (work_repeats,) + tuple(base_embedded.shape)
                    ).clone()
                    for local_idx, (time_idx, _, _) in enumerate(work_chunk):
                        embedded_perturbed[local_idx, :, time_idx] += (
                            changed_projected[local_idx] - base_projected_slices[local_idx]
                        )
                    embedded_perturbed = embedded_perturbed.reshape(
                        work_repeats * int(x.size(0)), *tuple(base_embedded.shape[1:])
                    )
                    weights_p, scores_p, _ = _forward_embedded_outputs(
                        embedded_api,
                        embedded_perturbed,
                        mask_perturbed,
                        compile_forward=bool(compile_forward),
                    )
                else:
                    x_perturbed = x.detach().unsqueeze(0).expand((work_repeats,) + tuple(x.shape)).clone()
                    for local_idx, (time_idx, feat_idx, _) in enumerate(work_chunk):
                        x_perturbed[local_idx, :, time_idx, :, feat_idx] = 0.0
                    x_perturbed = x_perturbed.reshape(work_repeats * int(x.size(0)), *tuple(x.shape[1:]))
                    weights_p, scores_p, _ = _forward_outputs(
                        model,
                        x_perturbed,
                        mask_perturbed,
                        return_aux=False,
                    )
                weights_p = weights_p.reshape(work_repeats, *tuple(base_weights.shape))[:repeats]
                scores_p = scores_p.reshape(work_repeats, *tuple(base_scores.shape))[:repeats]
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
            progress.update(repeats)
            progress.set_postfix(forwards=diagnostics["forward_batches"], batch=chunk_size, refresh=False)
    progress.close()
    elapsed_s = float(time.perf_counter() - stage_start)
    diagnostics["elapsed_s"] = elapsed_s
    diagnostics["perturbations_per_s"] = float(len(perturbations)) / max(elapsed_s, 1e-9)
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
    *,
    progress_enabled: bool = False,
) -> pl.DataFrame:
    x_last = x[:, -1].detach().float()
    x_mean = x.detach().float().mean(dim=1)
    mask_flat = mask.detach().bool().reshape(-1).cpu().numpy()
    score_np = scores.detach().float().reshape(-1).cpu().numpy()
    weight_np = weights.detach().float().reshape(-1).cpu().numpy()
    rows: list[dict[str, Any]] = []
    for source_name, values in (("last", x_last), ("lookback_mean", x_mean)):
        values_np = values.reshape(-1, values.size(-1)).cpu().numpy()
        for feat_idx, feature in tqdm(
            enumerate(feature_names),
            total=len(feature_names),
            desc=f"Feature correlations: {source_name}",
            unit="feature",
            leave=False,
            disable=not progress_enabled,
        ):
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



def _decision_inventory(
    weights: torch.Tensor,
    scores: torch.Tensor,
    returns: torch.Tensor,
    mask: torch.Tensor,
    dates: list[str],
    symbols: list[str],
    selected: torch.Tensor | None = None,
) -> pl.DataFrame:
    """Return every explained date-symbol decision, including flat/masked rows."""
    weights_cpu = torch.nan_to_num(weights.detach().float().cpu(), nan=0.0, posinf=0.0, neginf=0.0)
    scores_cpu = torch.nan_to_num(scores.detach().float().cpu(), nan=0.0, posinf=0.0, neginf=0.0)
    returns_cpu = torch.nan_to_num(returns.detach().float().cpu(), nan=0.0, posinf=0.0, neginf=0.0)
    mask_cpu = mask.detach().bool().cpu()
    selected_cpu = (
        (mask_cpu & (weights_cpu != 0.0))
        if selected is None
        else selected.detach().bool().cpu()
    )
    rows, n_symbols = int(weights_cpu.size(0)), int(weights_cpu.size(1))
    order = weights_cpu.abs().argsort(dim=1, descending=True)
    ranks = torch.empty_like(order)
    rank_values = torch.arange(1, n_symbols + 1, dtype=order.dtype).view(1, -1).expand(rows, -1)
    ranks.scatter_(1, order, rank_values)
    weight_np = weights_cpu.numpy().reshape(-1)
    return_np = returns_cpu.numpy().reshape(-1)
    return pl.DataFrame(
        {
            "date": np.repeat(np.asarray(dates, dtype=object), n_symbols),
            "symbol": np.tile(np.asarray(symbols, dtype=object), rows),
            "rank_abs_weight": ranks.numpy().reshape(-1),
            "side": np.where(weight_np > 0.0, "long", np.where(weight_np < 0.0, "short", "flat")),
            "weight": weight_np,
            "abs_weight": np.abs(weight_np),
            "score": scores_cpu.numpy().reshape(-1),
            "future_log_return": return_np,
            "gross_contribution": weight_np * return_np,
            "tradable": mask_cpu.numpy().reshape(-1),
            "nonzero_position": weight_np != 0.0,
            "selected_for_attribution": selected_cpu.numpy().reshape(-1),
        }
    )


def _exposure_coverage_curve(
    weights: torch.Tensor,
    mask: torch.Tensor,
    points: int = 101,
    *,
    progress_enabled: bool = False,
) -> pl.DataFrame:
    """Average cumulative gross-exposure coverage using every valid position."""
    weights_np = torch.nan_to_num(weights.detach().float(), nan=0.0, posinf=0.0, neginf=0.0).abs().cpu().numpy()
    mask_np = mask.detach().bool().cpu().numpy()
    grid = np.linspace(0.0, 1.0, max(2, int(points)), dtype=np.float64)
    curves: list[np.ndarray] = []
    row_progress = tqdm(
        zip(weights_np, mask_np, strict=True),
        total=int(weights_np.shape[0]),
        desc="Exposure coverage: dates",
        unit="date",
        leave=False,
        disable=not progress_enabled,
    )
    for row_weights, row_mask in row_progress:
        valid = np.sort(row_weights[row_mask])[::-1]
        total = float(valid.sum())
        if valid.size == 0 or total <= 0.0:
            curves.append(np.zeros_like(grid))
            continue
        cumulative = np.concatenate(([0.0], np.cumsum(valid, dtype=np.float64) / total))
        fractions = np.linspace(0.0, 1.0, valid.size + 1, dtype=np.float64)
        curves.append(np.interp(grid, fractions, cumulative))
    row_progress.close()
    mean_curve = np.mean(np.stack(curves), axis=0) if curves else np.zeros_like(grid)
    return pl.DataFrame(
        {
            "fraction_of_tradable_names": grid,
            "mean_cumulative_gross_exposure": mean_curve,
        }
    )


def _position_distribution_frame(inventory: pl.DataFrame) -> pl.DataFrame:
    if _is_empty_frame(inventory):
        return pl.DataFrame()
    rows: list[dict[str, Any]] = []
    for side in ("all_nonzero", "long", "short"):
        data = inventory.filter(pl.col("nonzero_position") & pl.col("tradable"))
        if side != "all_nonzero":
            data = data.filter(pl.col("side") == side)
        values = _numeric_numpy(data, "abs_weight", default=0.0)
        values = values[np.isfinite(values)]
        if values.size == 0:
            continue
        row: dict[str, Any] = {"side": side, "count": int(values.size), "mean_abs_weight": float(values.mean())}
        for quantile in (0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0):
            row[f"q{int(quantile * 100):02d}"] = float(np.quantile(values, quantile))
        rows.append(row)
    return pl.DataFrame(rows)


def _completeness_frame(
    *,
    weights: torch.Tensor,
    mask: torch.Tensor,
    selected: torch.Tensor,
    inventory: pl.DataFrame,
    lookback: int,
    feature_count: int,
    grad_ft: pl.DataFrame,
    ig_ft: pl.DataFrame,
    perturb_ft: pl.DataFrame,
) -> pl.DataFrame:
    safe_weights = torch.nan_to_num(weights.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
    eligible = mask.detach().bool() & (safe_weights != 0.0)
    selected = selected.detach().bool() & eligible
    total_gross = safe_weights.abs().masked_fill(~eligible, 0.0).sum()
    selected_gross = safe_weights.abs().masked_fill(~selected, 0.0).sum()
    expected_feature_time = max(0, int(lookback) * int(feature_count))
    return pl.DataFrame(
        [
            {
                "sample_rows": int(weights.size(0)),
                "symbols_per_row": int(weights.size(1)),
                "decision_inventory_rows": int(len(inventory)),
                "expected_decision_inventory_rows": int(weights.numel()),
                "eligible_nonzero_positions": int(eligible.sum().cpu()),
                "attributed_positions": int(selected.sum().cpu()),
                "position_count_coverage": float(selected.sum().cpu()) / max(1, int(eligible.sum().cpu())),
                "gross_exposure_coverage": float((selected_gross / total_gross.clamp_min(1e-12)).cpu()),
                "expected_feature_time_cells": expected_feature_time,
                "gradient_feature_time_cells": int(len(grad_ft)),
                "integrated_gradients_feature_time_cells": int(len(ig_ft)),
                "perturbation_feature_time_cells": int(len(perturb_ft)),
            }
        ]
    )



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


def _aux_summary(
    aux: dict[str, torch.Tensor],
    *,
    progress_enabled: bool = False,
) -> tuple[pl.DataFrame, dict[str, pl.DataFrame]]:
    rows: list[dict[str, Any]] = []
    dim_frames: dict[str, pl.DataFrame] = {}
    aux_items = sorted(aux.items())
    aux_progress = tqdm(
        aux_items,
        total=len(aux_items),
        desc="Aux tensor statistics",
        unit="tensor",
        leave=False,
        disable=not progress_enabled,
    )
    for name, value in aux_progress:
        aux_progress.set_postfix(tensor=name, refresh=False)
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
    aux_progress.close()
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
    for name, value in tqdm(
        eligible,
        total=len(eligible),
        desc="Aux UMAP",
        unit="tensor",
        leave=False,
        disable=not bool(settings.progress_enabled),
    ):
        projection_start = time.perf_counter()
        tensor = torch.nan_to_num(value.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
        original_shape = tuple(int(dim) for dim in tensor.shape)
        flat = tensor.reshape(-1, original_shape[-1])
        n_points = int(flat.size(0))
        if n_points < 4:
            warnings.append(f"{name}: fewer than 4 vectors; cuML UMAP skipped.")
            continue
        if max_points > 0 and n_points > max_points:
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
                verbose=bool(settings.progress_enabled),
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



def _case_study_frame(decisions: pl.DataFrame, daily: pl.DataFrame) -> pl.DataFrame:
    if _is_empty_frame(decisions) or _is_empty_frame(daily) or "date" not in decisions.columns:
        return pl.DataFrame()
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
        chunk = chunk.with_columns(pl.col("weight").abs().alias("abs_weight")).sort("abs_weight", descending=True)
        chunk = chunk.with_columns(pl.lit(case_type).alias("case_type")).select(["case_type", *[col for col in chunk.columns if col != "case_type"]])
        daily_row = daily_rows.get(date)
        if daily_row is not None:
            for col in ("strategy_log_return", "market_log_return", "turnover_proxy", "gross_exposure", "net_exposure"):
                chunk = chunk.with_columns(pl.lit(daily_row.get(col)).alias(f"case_{col}"))
        pieces.append(chunk)
    return _concat_frames(pieces)



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
    progress_enabled: bool = True,
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
    design_fit = design
    target_fit = target
    row_count, component_count = design_fit.shape
    block_size = min(65_536, max(1, row_count))
    block_starts = range(0, row_count, block_size)
    feature_sum = np.zeros(component_count, dtype=np.float64)
    feature_sum_sq = np.zeros(component_count, dtype=np.float64)
    for start in tqdm(
        block_starts,
        total=math.ceil(row_count / block_size),
        desc="SHAP feature statistics",
        unit="block",
        leave=False,
        disable=not progress_enabled,
    ):
        block = design_fit[start : start + block_size]
        feature_sum += block.sum(axis=0, dtype=np.float64)
        feature_sum_sq += np.square(block).sum(axis=0, dtype=np.float64)
    mean = (feature_sum / float(row_count)).reshape(1, -1)
    variance = np.maximum(feature_sum_sq / float(row_count) - np.square(mean.reshape(-1)), 0.0)
    std = np.sqrt(variance).reshape(1, -1)
    std = np.where(std < 1e-8, 1.0, std)
    target_std = float(np.std(target_fit))
    if target_std < 1e-10:
        message = "SHAP skipped because score targets are nearly constant."
        return pl.DataFrame(), pl.DataFrame(), {"enabled": True, "method": "skipped", "valid_rows": int(design.shape[0])}, [message]
    target_mean = float(np.mean(target_fit))
    target_centered = target_fit - target_mean
    alpha = 1e-3
    xtx = np.zeros((component_count, component_count), dtype=np.float64)
    rhs = np.zeros(component_count, dtype=np.float64)
    for start in tqdm(
        range(0, row_count, block_size),
        total=math.ceil(row_count / block_size),
        desc="SHAP ridge fit",
        unit="block",
        leave=False,
        disable=not progress_enabled,
    ):
        end = min(row_count, start + block_size)
        z_block = (design_fit[start:end] - mean) / std
        xtx += z_block.T @ z_block
        rhs += z_block.T @ target_centered[start:end]
    try:
        coef = np.linalg.solve(xtx + alpha * np.eye(xtx.shape[0], dtype=np.float64), rhs)
    except np.linalg.LinAlgError:
        coef = np.linalg.lstsq(xtx + alpha * np.eye(xtx.shape[0], dtype=np.float64), rhs, rcond=None)[0]
    ss_res = 0.0
    for start in tqdm(
        range(0, row_count, block_size),
        total=math.ceil(row_count / block_size),
        desc="SHAP surrogate validation",
        unit="block",
        leave=False,
        disable=not progress_enabled,
    ):
        end = min(row_count, start + block_size)
        z_block = (design_fit[start:end] - mean) / std
        pred_block = z_block @ coef + target_mean
        ss_res += float(np.sum((target_fit[start:end] - pred_block) ** 2))
    ss_tot = float(np.sum((target_fit - target_mean) ** 2))
    r2 = _safe_float(1.0 - ss_res / ss_tot if ss_tot > 1e-20 else 0.0)
    sample_rows = int(row_count)
    # The standardized fit design has zero mean by construction, so the exact
    # full-data background is the origin without retaining a sampled background.
    background_mean = np.zeros((1, component_count), dtype=np.float64)
    method = "linear_surrogate_closed_form"
    component_rows: list[dict[str, Any]] = []
    abs_sum = np.zeros(component_count, dtype=np.float64)
    for start in tqdm(
        range(0, row_count, block_size),
        total=math.ceil(row_count / block_size),
        desc="SHAP attribution rows",
        unit="block",
        leave=False,
        disable=not progress_enabled,
    ):
        z_block = (design_fit[start : start + block_size] - mean) / std
        shap_block = (z_block - background_mean) * coef.reshape(1, -1)
        abs_sum += np.nan_to_num(np.abs(shap_block), nan=0.0, posinf=0.0, neginf=0.0).sum(axis=0)
    abs_values = abs_sum / float(row_count)
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
    stage_progress = tqdm(
        total=9,
        desc="Explain methods",
        unit="stage",
        leave=False,
        disable=not bool(settings.progress_enabled),
    )

    def complete_stage(name: str, elapsed_key: str) -> None:
        stage_progress.update(1)
        stage_progress.set_postfix(
            stage=name,
            last_s=f"{timing.get(elapsed_key, 0.0):.2f}",
            refresh=True,
        )

    stage_progress.set_postfix(stage="prepare_batch", refresh=True)
    stage_start = time.perf_counter()
    batch = _move_batch(batch, device)
    x = torch.nan_to_num(batch["x"].float(), nan=0.0, posinf=0.0, neginf=0.0)
    returns = torch.nan_to_num(batch["future_log_returns"].float(), nan=0.0, posinf=0.0, neginf=0.0)
    mask = batch["tradable_mask"].to(device=device, dtype=torch.bool)
    _mark_elapsed(timing, "prepare_batch_s", stage_start)
    complete_stage("base_forward", "prepare_batch_s")

    stage_start = time.perf_counter()
    with torch.no_grad():
        weights, scores, aux = _forward_outputs(model, x, mask, return_aux=True)
    selected, direction, attribution_gross_weight = _selection_from_weights(
        weights.detach(),
        mask,
    )
    _mark_elapsed(timing, "base_forward_s", stage_start)
    complete_stage("gradient", "base_forward_s")

    stage_start = time.perf_counter()
    grad_attr = _gradient_x_input_attribution(
        model,
        x,
        mask,
        selected,
        direction,
        attribution_gross_weight,
    )
    grad_ft = _feature_time_frame(grad_attr, feature_names, "grad_x_input_abs")
    del grad_attr
    grad_feature = _feature_summary_frame(grad_ft, "grad_x_input_abs")
    grad_time = _time_summary_frame(grad_ft, "grad_x_input_abs")
    _mark_elapsed(timing, "gradient_s", stage_start)
    complete_stage("integrated_gradients", "gradient_s")

    stage_start = time.perf_counter()
    if int(settings.ig_steps) > 0:
        ig_attr = _integrated_gradients_attribution(
            model,
            x,
            mask,
            selected,
            direction,
            attribution_gross_weight,
            settings.ig_steps,
            settings.ig_batch_size,
            progress_enabled=bool(settings.progress_enabled),
        )
        ig_ft = _feature_time_frame(ig_attr, feature_names, "integrated_gradients_abs")
        ig_feature = _feature_summary_frame(ig_ft, "integrated_gradients_abs")
        ig_time = _time_summary_frame(ig_ft, "integrated_gradients_abs")
        del ig_attr
    else:
        ig_ft = pl.DataFrame()
        ig_feature = pl.DataFrame()
        ig_time = pl.DataFrame()
    _mark_elapsed(timing, "integrated_gradients_s", stage_start)
    complete_stage("perturbation", "integrated_gradients_s")

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
            progress_enabled=bool(settings.progress_enabled),
            compile_forward=bool(settings.counterfactual_compile),
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
    complete_stage("surrogate_shap", "perturbation_s")

    stage_start = time.perf_counter()
    shap_feature, shap_components, shap_info, shap_warnings = _score_head_surrogate_shap(
        x,
        scores,
        mask,
        feature_names,
        enabled=bool(settings.shap_enabled),
        mode=str(settings.shap_mode),
        progress_enabled=bool(settings.progress_enabled),
    )
    _mark_elapsed(timing, "surrogate_shap_s", stage_start)
    complete_stage("tabular_diagnostics", "surrogate_shap_s")

    stage_start = time.perf_counter()
    corr = _feature_correlations(
        x,
        scores,
        weights,
        mask,
        feature_names,
        progress_enabled=bool(settings.progress_enabled),
    )
    decision_inventory = _decision_inventory(weights, scores, returns, mask, dates, symbols, selected)
    stock_contrib = _stock_contribution_frame(weights, returns, mask, symbols)
    portfolio = _portfolio_summary(weights, returns, mask)
    daily = _daily_portfolio_frame(weights, returns, mask, dates)
    exposure_coverage = _exposure_coverage_curve(
        weights,
        mask,
        progress_enabled=bool(settings.progress_enabled),
    )
    position_distribution = _position_distribution_frame(decision_inventory)
    regime = _regime_analysis_frame(daily) if bool(settings.regime_analysis) else pl.DataFrame()
    case_studies = _case_study_frame(decision_inventory, daily)
    _mark_elapsed(timing, "tabular_diagnostics_s", stage_start)
    complete_stage("aux_diagnostics", "tabular_diagnostics_s")

    stage_start = time.perf_counter()
    aux_frame, aux_dim_frames = _aux_summary(
        aux,
        progress_enabled=bool(settings.progress_enabled),
    )
    aux_projection_frames, aux_projection_summary, aux_projection_warnings, aux_projection_timing = _aux_umap_projection_frames(
        aux,
        symbols=symbols,
        dates=dates,
        settings=settings,
        device=device,
    )
    _mark_elapsed(timing, "aux_diagnostics_s", stage_start)
    complete_stage("postprocess", "aux_diagnostics_s")

    stage_start = time.perf_counter()
    warnings = _make_warnings(portfolio, grad_feature, grad_time, corr, aux_frame)
    warnings.extend(aux_projection_warnings)
    warnings.extend(shap_warnings)
    trust_checks = _trust_check_frame(portfolio, grad_feature, grad_time, corr, aux_frame)
    attribution_lookback = 0
    if not _is_empty_frame(grad_ft) and "lookback_from_end" in grad_ft.columns:
        attribution_lookback = int(_numeric_max(grad_ft, "lookback_from_end") + 1)
    completeness = _completeness_frame(
        weights=weights,
        mask=mask,
        selected=selected,
        inventory=decision_inventory,
        lookback=int(x.size(1)),
        feature_count=len(feature_names),
        grad_ft=grad_ft,
        ig_ft=ig_ft,
        perturb_ft=perturb_ft,
    )
    _mark_elapsed(timing, "postprocess_s", stage_start)
    complete_stage("complete", "postprocess_s")
    stage_progress.close()
    timing["total_s"] = float(time.perf_counter() - total_start)

    return {
        "summary": {
            "portfolio": portfolio,
            "rows": len(dates),
            "attribution_scope": "all_tradable_nonzero_positions_gross_weighted",
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
            "feature_correlations": corr,
            "decision_inventory": decision_inventory,
            "explainability_completeness": completeness,
            "exposure_coverage_curve": exposure_coverage,
            "position_distribution": position_distribution,
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
            "x_summary": _surrogate_input_summary(x),
            "aux": {
                str(name): value.detach().float().cpu()
                for name, value in aux.items()
                if torch.is_tensor(value) and value.ndim >= 3 and int(value.shape[-1]) >= 2
            },
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
        "elapsed_s": 0.0,
        "perturbations_per_s": 0.0,
    }
    for result, _ in chunk_results:
        diag = result.get("summary", {}).get("perturb_diagnostics", {})
        merged["num_perturbations"] = max(int(merged["num_perturbations"]), int(diag.get("num_perturbations", 0) or 0))
        for key in ("requested_batch_size", "max_auto_batch_size", "max_input_elements", "chunk_size", "final_chunk_size"):
            merged[key] = max(int(merged[key]), int(diag.get(key, 0) or 0))
        for key in ("forward_batches", "attempted_forward_batches", "oom_retries"):
            merged[key] = int(merged[key]) + int(diag.get(key, 0) or 0)
        merged["elapsed_s"] = float(merged["elapsed_s"]) + float(diag.get("elapsed_s", 0.0) or 0.0)
        merged["oom_chunk_sizes"].extend(int(v) for v in diag.get("oom_chunk_sizes", []) or [])
    total_perturbations = int(merged["num_perturbations"]) * max(1, len(chunk_results))
    merged["perturbations_per_s"] = total_perturbations / max(float(merged["elapsed_s"]), 1e-9)
    return merged


def _combine_chunked_explainability_results(
    chunk_results: list[tuple[dict[str, Any], int]],
    *,
    lookback: int,
    feature_names: list[str],
    symbols: list[str],
    dates: list[str],
    settings: ExplainabilitySettings,
    device: torch.device,
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
    weights = torch.cat([result["_core"].pop("weights") for result, _ in chunk_results], dim=0)
    scores = torch.cat([result["_core"].pop("scores") for result, _ in chunk_results], dim=0)
    returns = torch.cat([result["_core"].pop("returns") for result, _ in chunk_results], dim=0)
    mask = torch.cat([result["_core"].pop("mask") for result, _ in chunk_results], dim=0).bool()
    x_cpu = torch.cat([result["_core"].pop("x_summary") for result, _ in chunk_results], dim=0)
    x_cpu = torch.nan_to_num(x_cpu.float(), nan=0.0, posinf=0.0, neginf=0.0)

    shap_feature, shap_components, shap_info, shap_warnings = _score_head_surrogate_shap(
        x_cpu,
        scores,
        mask,
        feature_names,
        enabled=bool(settings.shap_enabled),
        mode=str(settings.shap_mode),
        progress_enabled=bool(settings.progress_enabled),
    )

    corr = _feature_correlations(
        x_cpu,
        scores,
        weights,
        mask,
        feature_names,
        progress_enabled=bool(settings.progress_enabled),
    )
    selected, _, _ = _selection_from_weights(
        weights,
        mask,
    )
    decision_inventory = _decision_inventory(weights, scores, returns, mask, dates, symbols, selected)
    stock_contrib = _stock_contribution_frame(weights, returns, mask, symbols)
    portfolio = _portfolio_summary(weights, returns, mask)
    daily = _daily_portfolio_frame(weights, returns, mask, dates)
    exposure_coverage = _exposure_coverage_curve(
        weights,
        mask,
        progress_enabled=bool(settings.progress_enabled),
    )
    position_distribution = _position_distribution_frame(decision_inventory)
    regime = _regime_analysis_frame(daily) if bool(settings.regime_analysis) else pl.DataFrame()
    case_studies = _case_study_frame(decision_inventory, daily)
    aux_frame = _combine_aux_summary_from_chunks(chunk_results)
    aux_dim_frames = _combine_aux_dim_frames_from_chunks(chunk_results)
    combined_aux: dict[str, torch.Tensor] = {}
    aux_names = sorted(
        {
            name
            for result, _ in chunk_results
            for name in result.get("_core", {}).get("aux", {})
        }
    )
    aux_combine_progress = tqdm(
        aux_names,
        total=len(aux_names),
        desc="Combine aux tensors",
        unit="tensor",
        leave=False,
        disable=not bool(settings.progress_enabled),
    )
    for name in aux_combine_progress:
        aux_combine_progress.set_postfix(tensor=name, refresh=False)
        tensors = [
            result["_core"]["aux"][name]
            for result, _ in chunk_results
            if name in result.get("_core", {}).get("aux", {})
        ]
        if tensors and all(tensor.ndim == tensors[0].ndim and tensor.shape[1:] == tensors[0].shape[1:] for tensor in tensors):
            combined_aux[name] = torch.cat(tensors, dim=0)
    aux_combine_progress.close()
    for result, _ in chunk_results:
        result.get("_core", {}).pop("aux", None)
    aux_projection_frames, aux_projection_summary, aux_projection_warnings, aux_projection_timing = (
        _aux_umap_projection_frames(
            combined_aux,
            symbols=symbols,
            dates=dates,
            settings=settings,
            device=device,
        )
    )

    warnings = _make_warnings(portfolio, grad_feature, grad_time, corr, aux_frame)
    warnings.append(
        "Explainability used row microbatching to fit the full stock universe in GPU memory; all row chunks were aggregated before global SHAP and aux UMAP output."
    )
    warnings.extend(shap_warnings)
    warnings.extend(aux_projection_warnings)
    for result, _ in chunk_results:
        for item in result.get("summary", {}).get("warnings", []):
            warning = str(item)
            if warning == "cuML UMAP projections disabled by settings.":
                continue
            if warning not in warnings:
                warnings.append(warning)

    trust_checks = _trust_check_frame(portfolio, grad_feature, grad_time, corr, aux_frame)
    attribution_lookback = 0
    if not _is_empty_frame(grad_ft) and "lookback_from_end" in grad_ft.columns:
        attribution_lookback = int(_numeric_max(grad_ft, "lookback_from_end") + 1)
    completeness = _completeness_frame(
        weights=weights,
        mask=mask,
        selected=selected,
        inventory=decision_inventory,
        lookback=int(lookback),
        feature_count=len(feature_names),
        grad_ft=grad_ft,
        ig_ft=ig_ft,
        perturb_ft=perturb_ft,
    )

    timing = _sum_chunk_timings(chunk_results)
    timing["total_s"] = float(total_elapsed_s)
    timing["row_microbatch_chunks"] = float(len(chunk_results))

    perturb_diagnostics = _merge_perturb_diagnostics(chunk_results)

    return {
        "summary": {
            "portfolio": portfolio,
            "rows": len(dates),
            "attribution_scope": "all_tradable_nonzero_positions_gross_weighted",
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
            "feature_correlations": corr,
            "decision_inventory": decision_inventory,
            "explainability_completeness": completeness,
            "exposure_coverage_curve": exposure_coverage,
            "position_distribution": position_distribution,
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
            row_starts = range(0, n_rows, row_chunk_size)
            row_progress = tqdm(
                row_starts,
                total=math.ceil(n_rows / row_chunk_size),
                desc="Explain date chunks",
                unit="chunk",
                disable=not bool(effective_settings.progress_enabled),
            )
            for chunk_id, start in enumerate(row_progress, start=1):
                end = min(n_rows, start + row_chunk_size)
                # UMAP is run once after all aux tensors are concatenated, so its
                # coordinate system covers every row instead of one microbatch.
                chunk_settings = replace(effective_settings, umap_enabled=False)
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
                # Results are already detached to CPU.  Keep CUDA's allocator
                # cache warm across equal-shaped date chunks; empty_cache/ipc_collect
                # here forced a device sync and reallocation every microbatch.
                row_progress.set_postfix(rows=f"{end}/{n_rows}", refresh=False)

            diagnostics = {**diagnostics, "chunk_count": len(chunk_results)}
            combined = _combine_chunked_explainability_results(
                chunk_results,
                lookback=int(batch["x"].size(1)),
                feature_names=feature_names,
                symbols=symbols,
                dates=dates,
                settings=effective_settings,
                device=device,
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


def _compact_explain_chunk_result(result: dict[str, Any]) -> None:
    required_frames = {
        "feature_time_gradient",
        "feature_time_integrated_gradients",
        "feature_time_perturbation",
        "aux_summary",
    }
    frames = result.get("frames", {})
    result["frames"] = {
        name: frame
        for name, frame in frames.items()
        if name in required_frames
    }
    result["aux_projection_frames"] = {}


def explain_batch_source_chunked(
    model: nn.Module,
    source: ExplainDatasetBatchSource,
    *,
    feature_names: list[str],
    symbols: list[str],
    dates: list[str],
    settings: ExplainabilitySettings,
    device: torch.device,
) -> dict[str, Any]:
    """Explain a lazy date source without ever collating the full host batch."""
    n_rows = len(source)
    if n_rows <= 0:
        raise ValueError("Explainability source has no rows.")
    row_chunk_size, diagnostics = _auto_explain_row_chunk_size_from_shape(
        n_rows=n_rows,
        lookback=source.lookback,
        n_symbols=source.num_symbols,
        n_features=source.num_features,
        settings=settings,
        device=device,
    )
    diagnostics = {**diagnostics, "lazy_host_materialization": True}
    print(
        "[explain] lazy date streaming enabled: "
        f"rows={n_rows}, row_chunk_size={row_chunk_size}, symbols={source.num_symbols}, "
        f"features={source.num_features}"
    )

    total_start = time.perf_counter()
    chunk_results: list[tuple[dict[str, Any], int]] = []
    start = 0
    progress = tqdm(
        total=n_rows,
        desc="Explain date chunks",
        unit="date",
        disable=not bool(settings.progress_enabled),
    )
    while start < n_rows:
        end = min(n_rows, start + row_chunk_size)
        batch = source.materialize(start, end)
        chunk_settings = replace(
            settings,
            umap_enabled=False,
            shap_enabled=False,
            regime_analysis=False,
        )
        try:
            result = explain_batch(
                model,
                batch,
                feature_names=feature_names,
                symbols=symbols,
                dates=dates[start:end],
                settings=chunk_settings,
                device=device,
            )
        except RuntimeError as exc:
            if not _is_cuda_oom(exc) or row_chunk_size <= 1:
                raise
            # Reducing execution shape preserves every method and every date;
            # it is therefore safe even under strict_no_fallback.
            row_chunk_size = max(1, row_chunk_size // 2)
            diagnostics = {
                **diagnostics,
                "oom_retries": int(diagnostics.get("oom_retries", 0)) + 1,
                "final_row_chunk_size": row_chunk_size,
            }
            del batch
            _clear_explainability_runtime_cache()
            print(f"[explain] CUDA OOM; retrying the same dates with row_chunk_size={row_chunk_size}")
            continue
        _compact_explain_chunk_result(result)
        chunk_results.append((result, end - start))
        del batch, result
        start = end
        _clear_explainability_runtime_cache()
        progress.update(end - progress.n)
        progress.set_postfix(chunk=row_chunk_size, refresh=False)
    progress.close()

    diagnostics = {
        **diagnostics,
        "chunk_count": len(chunk_results),
        "final_row_chunk_size": row_chunk_size,
    }
    return _combine_chunked_explainability_results(
        chunk_results,
        lookback=source.lookback,
        feature_names=feature_names,
        symbols=symbols,
        dates=dates,
        settings=settings,
        device=device,
        row_chunk_diagnostics=diagnostics,
        total_elapsed_s=float(time.perf_counter() - total_start),
    )


def _write_markdown_report(
    path: Path,
    *,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    frames: dict[str, pl.DataFrame],
) -> None:
    def _render_frame(frame: pl.DataFrame) -> str:
        return _render_table_markdown(frame, limit=None)

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
            "- Portfolio decisions: decision_inventory.csv contains every explained date-symbol row, including flat and masked names.",
            "- Attribution target: every tradable non-zero position, gross-exposure weighted; no rank-based truncation is used.",
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
            "- `gradient x input`: fast local sensitivity around the explained decisions; useful for spotting dominant features or single-day dependence.",
            "- `integrated gradients`: smoother attribution from a zero baseline to the actual window; usually more stable than raw gradients but costs multiple forward/backward passes.",
            "- `perturbation weight_abs_delta`: decision-level sensitivity after zeroing one feature-day slice; prefer this over score delta when masked scores use sentinel values.",
            "- `feature_correlations`: simple linear checks between raw feature values and score/weight; high values can reveal price-level or liquidity shortcuts.",
            "- `aux_summary` and `aux_dims`: tensor norm and dimension usage checks; very high zero fraction or one dominant dimension can indicate collapsed representations.",
            "- `aux_projections`: cuML UMAP maps of high-dimensional transformer states; collapsed clouds, isolated single-token islands, or date-only bands deserve manual inspection.",
            "- `explainability_completeness`: verify position/gross coverage is 100%, inventory row counts match, and enabled feature-time methods contain lookback × feature cells.",
            "- `exposure_coverage_curve`: uses all names; a steep curve means the strategy is concentrated.",
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
        "explainability_completeness",
        "position_distribution",
        "feature_importance_gradient",
        "feature_importance_integrated_gradients",
        "feature_importance_perturbation",
        "feature_correlations",
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
    # Reserve a fixed physical header height. A fixed fractional top margin
    # wastes many inches on portrait plots whose height grows with row count.
    fig_height = max(1.0, float(fig.get_size_inches()[1]))
    title_top_in = 0.22
    subtitle_top_in = title_top_in + 0.34 * title_lines + 0.18
    header_height_in = subtitle_top_in + 0.23 * subtitle_lines + 0.35
    top = max(0.45, min(0.94, 1.0 - header_height_in / fig_height))
    title_y = max(top + 0.08, 1.0 - title_top_in / fig_height)
    subtitle_y = max(top + 0.03, 1.0 - subtitle_top_in / fig_height)
    fig.subplots_adjust(top=top)
    left = ax.get_position().x0
    fig.text(left, title_y, title_wrapped, ha="left", va="top", fontsize=15, fontweight="bold", color=PAPER_TOKENS["ink"])
    fig.text(left, subtitle_y, subtitle_wrapped, ha="left", va="top", fontsize=10.5, color=PAPER_TOKENS["muted"])


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


def _feature_attribution_coverage_curve(table: pl.DataFrame) -> pl.DataFrame:
    """Cumulative curve over all features, never a Top-N truncation."""
    if _is_empty_frame(table) or "mean_available_share" not in table.columns:
        return pl.DataFrame()
    values = _numeric_numpy(table, "mean_available_share", default=0.0)
    values = np.clip(np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0), 0.0, None)
    total = float(values.sum())
    cumulative = np.cumsum(values) / total if total > 0.0 else np.zeros_like(values)
    count = max(1, values.size)
    return table.select(["feature", "feature_group", "feature_label", "mean_available_share"]).with_columns(
        [
            pl.Series("feature_rank", np.arange(1, values.size + 1, dtype=np.int64)),
            pl.Series("fraction_of_features", np.arange(1, values.size + 1, dtype=np.float64) / count),
            pl.Series("cumulative_mean_attribution_share", cumulative),
        ]
    )



def _write_paper_tables(
    output_dir: Path,
    *,
    frames: dict[str, pl.DataFrame],
    summary: dict[str, Any],
    metadata: dict[str, Any],
    progress_enabled: bool = False,
) -> dict[str, str]:
    table_dir = output_dir / "paper_tables"
    table_dir.mkdir(parents=True, exist_ok=True)
    tables: dict[str, pl.DataFrame] = {}
    tables["global_feature_attribution"] = _global_attribution_table(frames)
    tables["feature_attribution_coverage_curve"] = _feature_attribution_coverage_curve(
        tables["global_feature_attribution"]
    )
    for name in (
        "explainability_completeness",
        "exposure_coverage_curve",
        "position_distribution",
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
    completeness = tables.get("explainability_completeness", pl.DataFrame())
    if not _is_empty_frame(completeness):
        split_rows = int(metadata.get("split_rows", metadata.get("sample_rows", 0)) or 0)
        sample_rows = int(metadata.get("sample_rows", 0) or 0)
        completeness = completeness.with_columns(
            [
                pl.lit(split_rows).alias("split_rows"),
                pl.lit(sample_rows / max(1, split_rows)).alias("sampled_date_coverage"),
                pl.lit(str(summary.get("attribution_scope", "unknown"))).alias("attribution_scope"),
            ]
        )
        tables["explainability_completeness"] = completeness
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
    table_progress = tqdm(
        total=len(tables),
        desc="Paper tables",
        unit="table",
        leave=False,
        disable=not progress_enabled,
    )
    for name, table in tables.items():
        table_progress.set_postfix(table=name, rows=len(table), refresh=False)
        if _is_empty_frame(table):
            table_progress.update(1)
            continue
        path = table_dir / f"{name}.csv"
        _write_csv(table, path)
        written[name] = str(path.relative_to(output_dir))
        table_progress.update(1)
    table_progress.close()
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
    data = table
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
    fig, ax = plt.subplots(
        figsize=_figsize_for_rows(feature_count, width=22.0, row_height=0.30, overhead=2.8),
        dpi=160,
    )
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
    _save_matplotlib_figure(fig, output_path, pad_to_standard_aspect=False)
    plt.close(fig)


def _plot_paper_feature_time_heatmap(
    frame: pl.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
    subtitle: str,
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if _is_empty_frame(frame) or not required.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["feature", "lookback_from_end", value_col]), value_col, "lookback_from_end")
    data = data.drop_nulls(subset=["feature", "lookback_from_end", value_col])
    if data.is_empty():
        return
    features = _values_by_sum(data, "feature", value_col)
    if not features:
        return
    data = data.with_columns(
        pl.col("feature").cast(pl.String).map_elements(_feature_label, return_dtype=pl.String).alias("feature_label")
    )
    ordered_labels = [_feature_label(str(feature)) for feature in features]
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

    fig, ax = plt.subplots(
        figsize=_figsize_for_rows(len(labels), width=24.0, row_height=0.25, overhead=3.0),
        dpi=170,
    )
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
    _save_matplotlib_figure(fig, output_path, pad_to_standard_aspect=False)
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
    fig, ax = plt.subplots(figsize=_figsize_17_6(), dpi=160)
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
    data = data.drop_nulls(subset=["max_abs_corr"]).sort("max_abs_corr", descending=True)
    if data.is_empty():
        return
    data = data.with_columns(
        pl.concat_str([pl.col("source").cast(pl.String), pl.lit(" / "), pl.col("feature").cast(pl.String)]).alias("label")
    )
    plot = data.select(["label", "score_corr", "weight_corr"]).unpivot(
        index=["label"], on=["score_corr", "weight_corr"], variable_name="target", value_name="corr"
    )
    plt, sns = _setup_paper_plotting()
    fig, ax = plt.subplots(
        figsize=_figsize_for_rows(data.height, width=22.0, row_height=0.24, overhead=2.8),
        dpi=160,
    )
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
    _save_matplotlib_figure(fig, output_path, pad_to_standard_aspect=False)
    plt.close(fig)


def _plot_paper_trust_checks(frame: pl.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if _is_empty_frame(frame) or not {"check", "value", "status"}.issubset(frame.columns):
        return
    data = _with_numeric(frame, "value").drop_nulls(subset=["value"])
    if data.is_empty():
        return
    plt, sns = _setup_paper_plotting()
    fig_height = max(4.8, 0.48 * data.height + 2.0)
    fig, ax = plt.subplots(figsize=_figsize_17_6(fig_height), dpi=160)
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
    fig, ax = plt.subplots(figsize=_figsize_17_6(fig_height), dpi=160)
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
    data = data.sort(["case_type", "abs_weight"], descending=[False, True]).with_columns(
        pl.int_range(pl.len()).over("case_type").alias("position_rank")
    )
    plt, _ = _setup_paper_plotting()
    fig, ax = plt.subplots(figsize=_figsize_17_6(), dpi=160)
    for case_type, group in data.partition_by("case_type", as_dict=True).items():
        label = str(case_type[0] if isinstance(case_type, tuple) else case_type)
        ax.scatter(
            _numeric_numpy(group, "position_rank"),
            _numeric_numpy(group, "gross_contribution"),
            s=8,
            alpha=0.45,
            label=label,
        )
    ax.axvline(0.0, color=PAPER_TOKENS["neutral_dark"], linewidth=1.0)
    ax.axhline(0.0, color=PAPER_TOKENS["neutral_dark"], linewidth=1.0)
    ax.set_xlabel("All positions ordered by absolute weight within case day")
    ax.set_ylabel("Weight × future log return")
    ax.legend(loc="best", fontsize=8)
    _add_paper_header(fig, ax, "Complete case-day contribution distributions", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_aux_summary(frame: pl.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if _is_empty_frame(frame) or not {"name", "mean_abs"}.issubset(frame.columns):
        return
    data = _with_numeric(frame, "mean_abs").drop_nulls(subset=["mean_abs"]).sort("mean_abs", descending=True)
    if data.is_empty():
        return
    plt, sns = _setup_paper_plotting()
    fig_height = max(4.8, 0.36 * data.height + 2.0)
    fig, ax = plt.subplots(figsize=_figsize_17_6(fig_height), dpi=160)
    sns.barplot(data=_to_plot_data(data), y="name", x="mean_abs", color=PAPER_TOKENS["olive_mid"], ax=ax)
    ax.set_xlabel("Mean absolute activation")
    ax.set_ylabel("")
    _add_paper_header(fig, ax, "Latent and market-token diagnostics check whether representations collapse", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path)
    plt.close(fig)


def _plot_paper_coverage_curve(
    frame: pl.DataFrame,
    *,
    output_path: Path,
    x_col: str,
    y_col: str,
    title: str,
    subtitle: str,
    x_label: str,
) -> None:
    if _is_empty_frame(frame) or not {x_col, y_col}.issubset(frame.columns):
        return
    data = _with_numeric(frame, x_col, y_col).drop_nulls(subset=[x_col, y_col]).sort(x_col)
    if data.is_empty():
        return
    plt, _ = _setup_paper_plotting()
    fig, ax = plt.subplots(figsize=_figsize_17_6(), dpi=160)
    ax.plot(
        _numeric_numpy(data, x_col),
        _numeric_numpy(data, y_col),
        color=PAPER_TOKENS["blue_mid"],
        linewidth=2.2,
    )
    ax.plot([0.0, 1.0], [0.0, 1.0], color=PAPER_TOKENS["neutral_mid"], linestyle="--", linewidth=1.0)
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.02)
    ax.set_xlabel(x_label)
    ax.set_ylabel("Cumulative share")
    ax.xaxis.set_major_formatter(lambda value, _: f"{100.0 * value:.0f}%")
    ax.yaxis.set_major_formatter(lambda value, _: f"{100.0 * value:.0f}%")
    ax.grid(True, color=PAPER_TOKENS["grid"], linewidth=0.8)
    _add_paper_header(fig, ax, title, subtitle)
    _finish_paper_axes(ax)
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
    progress_enabled: bool = False,
) -> list[str]:
    plot_dir = output_dir / "plots_paper"
    plot_dir.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    scope = _paper_scope(metadata, summary)
    paper_progress = tqdm(
        total=12,
        desc="Paper plots",
        unit="plot",
        leave=False,
        disable=not progress_enabled,
    )

    def _time_plot(name: str, fn: Callable[[], None], out_path: Path) -> None:
        item_start = time.perf_counter()
        fn()
        if plot_timing is not None:
            plot_timing[name] = float(time.perf_counter() - item_start)
        if out_path.exists():
            generated.append(out_path)
        paper_progress.update(1)
        paper_progress.set_postfix(plot=out_path.name, refresh=False)

    global_table = _global_attribution_table(frames)
    out = plot_dir / "global_feature_attribution.png"
    _time_plot(
        "global_feature_attribution_s",
        lambda: _plot_paper_global_attribution(global_table, out, subtitle=f"Share of total attribution by method; {scope}"),
        out,
    )
    out = plot_dir / "feature_attribution_coverage_curve.png"
    _time_plot(
        "feature_attribution_coverage_curve_s",
        lambda: _plot_paper_coverage_curve(
            _feature_attribution_coverage_curve(global_table),
            output_path=out,
            x_col="fraction_of_features",
            y_col="cumulative_mean_attribution_share",
            title="All-feature cumulative attribution coverage",
            subtitle=f"Every feature is included; {scope}",
            x_label="Fraction of all features",
        ),
        out,
    )
    out = plot_dir / "portfolio_exposure_coverage_curve.png"
    _time_plot(
        "portfolio_exposure_coverage_curve_s",
        lambda: _plot_paper_coverage_curve(
            frames.get("exposure_coverage_curve", pl.DataFrame()),
            output_path=out,
            x_col="fraction_of_tradable_names",
            y_col="mean_cumulative_gross_exposure",
            title="All-position cumulative gross-exposure coverage",
            subtitle=f"Every tradable name is included before aggregation; {scope}",
            x_label="Fraction of tradable names, largest positions first",
        ),
        out,
    )
    heatmap_specs = [
        (
            "feature_time_gradient",
            "grad_x_input_abs",
            "Feature-time heatmap shows where local gradient sensitivity concentrates",
            "Mean absolute gradient × input for the full gross-weighted explained portfolio; t-0 is the latest input day.",
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
    paper_progress.close()
    return [str(path.relative_to(output_dir)) for path in generated]


PAPER_FIGURE_GUIDE: dict[str, tuple[str, str, str]] = {
    "global_feature_attribution.png": (
        "Compares global feature importance across gradient x input, Integrated Gradients, perturbation, and surrogate SHAP.",
        "The bars are a compact reading index; use global_feature_attribution.csv and the cumulative curve for the complete feature set.",
        "One feature taking more than half of total attribution, or SHAP disagreeing completely with perturbation, suggests a narrow or unstable rule.",
    ),
    "feature_attribution_coverage_curve.png": (
        "Accumulates attribution across every feature, sorted from strongest to weakest.",
        "A steep early rise means a few features explain most decisions; a gradual curve means information is distributed broadly.",
        "Near-total attribution from only a tiny fraction of features can indicate a brittle shortcut and should be checked across folds.",
    ),
    "portfolio_exposure_coverage_curve.png": (
        "Accumulates gross exposure across every tradable name, largest position first.",
        "Compare the blue curve with the diagonal: the farther above it, the more concentrated the portfolio.",
        "If a very small fraction of names carries nearly all gross exposure, Top-N examples are not representative enough for trust decisions.",
    ),
    "feature_time_gradient_grad_x_input_abs_heatmap.png": (
        "Measures local sensitivity by feature and lookback day for all tradable non-zero positions, weighted by gross exposure.",
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
        "Splits explained decisions by market direction and volatility regime.",
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


def _render_frame_markdown(frame: pl.DataFrame, limit: int | None = 20) -> str:
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
    lines.append("## Completeness Audit")
    lines.append("")
    lines.append(_render_frame_markdown(frames.get("explainability_completeness", pl.DataFrame()), limit=30))
    lines.append("")
    lines.append("## Global Attribution Table")
    lines.append("")
    lines.append(_render_frame_markdown(_global_attribution_table(frames), limit=None))
    lines.append("")
    lines.append("## Regime Analysis")
    lines.append("")
    lines.append(_render_frame_markdown(frames.get("regime_analysis", pl.DataFrame()), limit=30))
    lines.append("")
    lines.append("## Decision Case Studies")
    lines.append("")
    lines.append("Complete rows are stored in `paper_tables/decision_case_studies.csv`; the report does not duplicate the full table.")
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


def write_fold_stability_outputs(
    explainability_root: Path,
    *,
    strict_no_fallback: bool = False,
    progress_enabled: bool = False,
) -> Path | None:
    root = Path(explainability_root)
    fold_dirs = sorted(path for path in root.glob("fold_*_test") if path.is_dir())
    rows: list[pl.DataFrame] = []
    fold_progress = tqdm(
        fold_dirs,
        total=len(fold_dirs),
        desc="Fold stability: read folds",
        unit="fold",
        leave=False,
        disable=not progress_enabled,
    )
    for fold_dir in fold_progress:
        fold_progress.set_postfix(fold=fold_dir.name, refresh=False)
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
    fold_progress.close()
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
    data = summary
    if not data.is_empty():
        fig, ax = plt.subplots(
            figsize=_figsize_for_rows(data.height, width=22.0, row_height=0.30, overhead=2.8),
            dpi=160,
        )
        sns.barplot(data=_frame_to_dict(data), y="feature_label", x="mean_share", color=PAPER_TOKENS["blue_mid"], ax=ax)
        ax.set_xlabel("Mean attribution share across folds")
        ax.set_ylabel("")
        ax.xaxis.set_major_formatter(lambda value, _: f"{100.0 * value:.0f}%")
        fold_count = int(combined.select(pl.col("fold_id").n_unique()).item())
        _add_paper_header(fig, ax, "Fold stability shows whether the same features remain important", f"Computed across {fold_count} fold explainability outputs.")
        _finish_paper_axes(ax)
        _save_matplotlib_figure(
            fig,
            plot_dir / "fold_stability_feature_share.png",
            pad_to_standard_aspect=False,
        )
        plt.close(fig)
    report = [
        "# Paper Fold Stability Summary",
        "",
        f"- folds: `{int(combined.select(pl.col('fold_id').n_unique()).item())}`",
        f"- features: `{int(summary.select(pl.col('feature').n_unique()).item())}`",
        "",
        "## Most Stable Features",
        "",
        _render_frame_markdown(summary, limit=None),
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
) -> None:
    if _is_empty_frame(frame) or label_col not in frame.columns or value_col not in frame.columns:
        return
    data = _with_numeric(frame.select([label_col, value_col]), value_col)
    data = data.drop_nulls(subset=[label_col, value_col]).sort(value_col, descending=True)
    if data.is_empty():
        return
    labels = _string_list(data, label_col)
    values = _numeric_numpy(data, value_col)
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(
        figsize=_figsize_for_rows(data.height, width=20.0, row_height=0.26, overhead=2.0),
        dpi=130,
    )
    ax.barh(labels[::-1], values[::-1])
    ax.set_title(title)
    ax.set_xlabel(value_col)
    ax.grid(True, axis="x", alpha=0.25)
    _safe_matplotlib_tight_layout(fig)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path, pad_to_standard_aspect=False)
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

    fig, ax = plt.subplots(figsize=_figsize_17_6(), dpi=130)
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
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if _is_empty_frame(frame) or not required.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["feature", "lookback_from_end", value_col]), "lookback_from_end", value_col)
    data = data.drop_nulls(subset=["feature", "lookback_from_end", value_col])
    if data.is_empty():
        return
    features = _values_by_sum(data, "feature", value_col)
    if not features:
        return
    column_order = sorted(data.get_column("lookback_from_end").unique().to_list())
    labels, columns, matrix = _pivot_sum_matrix(
        data,
        index_col="feature",
        column_col="lookback_from_end",
        value_col=value_col,
        index_order=features,
        column_order=column_order,
    )
    if matrix.size == 0:
        return
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(
        figsize=_figsize_for_rows(len(labels), width=22.0, row_height=0.24, overhead=2.2),
        dpi=130,
    )
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
    _save_matplotlib_figure(fig, output_path, pad_to_standard_aspect=False)
    plt.close(fig)


def _plot_feature_time_heatmap_datashader(
    frame: pl.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if _is_empty_frame(frame) or not required.issubset(frame.columns):
        return
    data = _with_numeric(frame.select(["feature", "lookback_from_end", value_col]), "lookback_from_end", value_col)
    data = data.drop_nulls(subset=["feature", "lookback_from_end", value_col])
    if data.is_empty():
        return
    features = [str(value) for value in _values_by_sum(data, "feature", value_col)]
    if not features:
        return
    data = data.with_columns(pl.col("feature").cast(pl.String).alias("feature"))
    feature_to_y = {feature: len(features) - 1 - idx for idx, feature in enumerate(features)}
    data = data.with_columns(
        pl.col("feature")
        .map_elements(lambda value: feature_to_y.get(str(value)), return_dtype=pl.Int64)
        .alias("feature_y")
    ).drop_nulls(subset=["feature_y"])
    if data.is_empty():
        return
    plot_height = max(520, min(3200, 24 * len(features) + 180))
    save_heatmap_points_datashader(
        _numeric_numpy(data, "lookback_from_end"),
        _numeric_numpy(data, "feature_y"),
        _numeric_numpy(data, value_col),
        output_path=output_path,
        title=title,
        x_label="lookback_from_end (0 = latest)",
        y_label=value_col,
        y_labels=[(feature_to_y[feature], feature) for feature in features],
        width=_plot_width_px_17_6(plot_height),
        height=plot_height,
    )


def _plot_feature_correlations(frame: pl.DataFrame, output_path: Path) -> None:
    if _is_empty_frame(frame) or "feature" not in frame.columns:
        return
    data = frame
    if "abs_score_corr" not in data.columns:
        return
    data = _with_numeric(data, "abs_score_corr", "score_corr", "weight_corr").sort("abs_score_corr", descending=True)
    if data.is_empty():
        return
    import matplotlib.pyplot as plt

    labels = _string_list(
        data.with_columns(pl.concat_str([pl.col("source").cast(pl.String), pl.lit(":"), pl.col("feature").cast(pl.String)]).alias("__label")),
        "__label",
    )
    y = np.arange(data.height)
    fig, ax = plt.subplots(
        figsize=_figsize_for_rows(data.height, width=22.0, row_height=0.22, overhead=2.2),
        dpi=130,
    )
    ax.barh(y - 0.18, _numeric_numpy(data, "score_corr"), height=0.35, label="score_corr")
    if "weight_corr" in data.columns:
        ax.barh(y + 0.18, _numeric_numpy(data, "weight_corr"), height=0.35, label="weight_corr")
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.axvline(0.0, color="black", linewidth=0.8)
    ax.set_title("Complete Simple Feature Correlations")
    ax.set_xlabel("correlation")
    ax.grid(True, axis="x", alpha=0.25)
    ax.legend()
    _safe_matplotlib_tight_layout(fig)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _save_matplotlib_figure(fig, output_path, pad_to_standard_aspect=False)
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

    fig, ax = plt.subplots(figsize=_figsize_17_6(), dpi=130)
    bottom = np.zeros(len(rows))
    for side in ("long", "short", "flat"):
        if side not in columns:
            continue
        values = matrix[:, columns.index(side)]
        ax.bar(np.arange(len(rows)), values, bottom=bottom, label=side)
        bottom = bottom + values
    ax.set_title("Complete Decision Absolute Exposure By Side")
    ax.set_xlabel("explained date index")
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
        title="Complete Decision Absolute Exposure By Side",
        y_label="sum abs(weight)",
        width=_plot_width_px_17_6(520),
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
        width=_plot_width_px_17_6(420),
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
    save_scatter_datashader(
        series,
        output_path=output_path,
        title=title,
        width=_DEFAULT_PLOT_WIDTH_PX,
        height=_DEFAULT_PLOT_HEIGHT_PX,
    )


def _plot_all_explanation_figures(
    frames: dict[str, pl.DataFrame],
    aux_dim_frames: dict[str, pl.DataFrame],
    output_dir: Path,
    *,
    aux_projection_frames: dict[str, pl.DataFrame] | None = None,
    plot_backend: str = "auto",
    plot_timing: dict[str, Any] | None = None,
    strict_no_fallback: bool = False,
    progress_enabled: bool = False,
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
        ("aux_summary", "name", "mean_abs", "Auxiliary Representation Mean Abs"),
    ]
    plot_progress = tqdm(
        total=len(specs) + 2 + 4 + 2 + len(aux_dim_frames) + len(aux_projection_frames or {}),
        desc="Standard plots",
        unit="plot",
        leave=False,
        disable=not progress_enabled,
    )
    for frame_name, label_col, value_col, title in specs:
        frame = frames.get(frame_name, pl.DataFrame())
        out = plot_dir / f"{frame_name}_{value_col}.png"
        _plot_barh(frame, output_path=out, label_col=label_col, value_col=value_col, title=title)
        if out.exists():
            generated.append(out)
        plot_progress.update(1)
        plot_progress.set_postfix(plot=out.name, refresh=False)
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
        plot_progress.update(1)
        plot_progress.set_postfix(plot=out.name, refresh=False)
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
        plot_progress.update(1)
        plot_progress.set_postfix(plot=out.name, refresh=False)
    if plot_timing is not None:
        plot_timing["heatmap_specs_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    out = plot_dir / "feature_correlations.png"
    _plot_feature_correlations(frames.get("feature_correlations", pl.DataFrame()), out)
    if out.exists():
        generated.append(out)
    plot_progress.update(1)
    plot_progress.set_postfix(plot=out.name, refresh=False)
    if plot_timing is not None:
        plot_timing["feature_correlations_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    out = plot_dir / "decision_inventory_exposure_by_side.png"
    decision_frame = frames.get("decision_inventory", pl.DataFrame())
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
    plot_progress.update(1)
    plot_progress.set_postfix(plot=out.name, refresh=False)
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
                    title=f"Complete Aux Dimension Profile: {name}",
                )
        else:
            _plot_barh(
                frame,
                output_path=out,
                label_col="dim",
                value_col="mean_abs",
                title=f"Complete Aux Dimension Profile: {name}",
            )
        if out.exists():
            generated.append(out)
        plot_progress.update(1)
        plot_progress.set_postfix(plot=out.name, refresh=False)
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
                    plot_progress.update(1)
                    plot_progress.set_postfix(plot=out.name, skipped=True, refresh=False)
                    continue
                import matplotlib.pyplot as plt

                data = _with_numeric(frame, "umap_x", "umap_y").drop_nulls(subset=["umap_x", "umap_y"])
                if data.is_empty():
                    plot_progress.update(1)
                    plot_progress.set_postfix(plot=out.name, skipped=True, refresh=False)
                    continue
                fig, ax = plt.subplots(figsize=_figsize_17_6(), dpi=130)
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
                plot_progress.update(1)
                plot_progress.set_postfix(plot=out.name, skipped=True, refresh=False)
                continue
            import matplotlib.pyplot as plt

            data = _with_numeric(frame, "umap_x", "umap_y").drop_nulls(subset=["umap_x", "umap_y"])
            if data.is_empty():
                plot_progress.update(1)
                plot_progress.set_postfix(plot=out.name, skipped=True, refresh=False)
                continue
            fig, ax = plt.subplots(figsize=_figsize_17_6(), dpi=130)
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
        plot_progress.update(1)
        plot_progress.set_postfix(plot=out.name, refresh=False)
    if plot_timing is not None:
        plot_timing["aux_umap_s"] = float(time.perf_counter() - stage_start)

    plot_progress.close()
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
    progress_enabled: bool = False,
) -> None:
    write_start = time.perf_counter()
    write_timing: dict[str, Any] = {}
    write_progress = tqdm(
        total=6,
        desc="Write explainability",
        unit="stage",
        leave=False,
        disable=not progress_enabled,
    )

    def complete_write_stage(name: str) -> None:
        write_progress.update(1)
        write_progress.set_postfix(stage=name, refresh=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = metadata or {}
    frames: dict[str, pl.DataFrame] = result["frames"]
    aux_dim_frames: dict[str, pl.DataFrame] = result.get("aux_dim_frames", {})
    aux_projection_frames: dict[str, pl.DataFrame] = result.get("aux_projection_frames", {})
    table_progress = tqdm(
        total=len(frames) + len(aux_dim_frames) + len(aux_projection_frames),
        desc="Write tables",
        unit="table",
        leave=False,
        disable=not progress_enabled,
    )
    stage_start = time.perf_counter()
    for name, frame in frames.items():
        if not _is_empty_frame(frame):
            _write_csv(frame, output_dir / f"{name}.csv")
        table_progress.update(1)
        table_progress.set_postfix(table=name, rows=len(frame), refresh=False)
    aux_dir = output_dir / "aux_dims"
    for name, frame in aux_dim_frames.items():
        aux_dir.mkdir(parents=True, exist_ok=True)
        safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name)
        _write_csv(frame, aux_dir / f"{safe_name}.csv")
        table_progress.update(1)
        table_progress.set_postfix(table=f"aux:{name}", rows=len(frame), refresh=False)
    projection_dir = output_dir / "aux_projections"
    for name, frame in aux_projection_frames.items():
        if not _is_empty_frame(frame):
            projection_dir.mkdir(parents=True, exist_ok=True)
            safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name)
            _write_csv(frame, projection_dir / f"{safe_name}.csv")
        table_progress.update(1)
        table_progress.set_postfix(table=f"umap:{name}", rows=len(frame), refresh=False)
    table_progress.close()
    _mark_elapsed(write_timing, "csv_s", stage_start)
    complete_write_stage("standard_plots")
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
            progress_enabled=progress_enabled,
        )
        if write_plots and write_standard_plots
        else []
    )
    _mark_elapsed(write_timing, "plots_s", stage_start)
    complete_write_stage("paper_artifacts")
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
            progress_enabled=progress_enabled,
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
                progress_enabled=progress_enabled,
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
    complete_write_stage("reports")
    stage_start = time.perf_counter()
    _write_markdown_report(
        output_dir / "report.md",
        metadata=metadata,
        summary=summary,
        frames=frames,
    )
    _mark_elapsed(write_timing, "report_md_s", stage_start)
    complete_write_stage("paper_report")
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
    complete_write_stage("summary_json")
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
    complete_write_stage("complete")
    write_progress.close()


def _strip_orig_mod_prefix(state_dict: dict[str, Any]) -> dict[str, Any]:
    if not state_dict:
        return state_dict
    if all(str(key).startswith("_orig_mod.") for key in state_dict.keys()):
        return {str(key).removeprefix("_orig_mod."): value for key, value in state_dict.items()}
    return state_dict


def _adapt_dynamic_symbol_position_state(
    model: nn.Module,
    state_dict: dict[str, Any],
    *,
    strict: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if strict or not bool(getattr(model, "allow_dynamic_symbols", False)):
        return state_dict, []
    model_state = model.state_dict()
    adapted: dict[str, Any] | None = None
    adjustments: list[dict[str, Any]] = []
    for key, value in state_dict.items():
        if not str(key).endswith("symbol_position"):
            continue
        target = model_state.get(str(key))
        if not torch.is_tensor(value) or not torch.is_tensor(target):
            continue
        if tuple(value.shape) == tuple(target.shape):
            continue
        if value.ndim != 4 or target.ndim != 4:
            continue
        if (value.size(0), value.size(1), value.size(3)) != (target.size(0), target.size(1), target.size(3)):
            continue
        if adapted is None:
            adapted = dict(state_dict)
        resized = target.detach().clone()
        copy_symbols = min(int(value.size(2)), int(target.size(2)))
        resized[:, :, :copy_symbols, :] = value[:, :, :copy_symbols, :].to(dtype=resized.dtype)
        adapted[str(key)] = resized
        adjustments.append(
            {
                "key": str(key),
                "checkpoint_shape": list(value.shape),
                "model_shape": list(target.shape),
                "copied_symbols": int(copy_symbols),
            }
        )
    return (adapted if adapted is not None else state_dict), adjustments


def load_model_from_checkpoint(
    config: ExperimentConfig,
    panel: PanelData,
    checkpoint_path: Path,
    device: torch.device,
    *,
    strict: bool = False,
) -> tuple[nn.Module, dict[str, Any]]:
    # Keep explainability under the same schema/model compatibility contract as
    # inference and live signals.  Import lazily because trainer imports this
    # module lazily for post-fold reports as well.
    from stockagent.training.trainer import (
        _checkpoint_manifest,
        _validate_checkpoint_manifest,
    )

    checkpoint = torch.load(checkpoint_path, map_location="cpu")
    _validate_checkpoint_manifest(
        checkpoint,
        _checkpoint_manifest(panel, config, include_data_content=False),
        checkpoint_path=checkpoint_path,
        scope="model",
    )
    model = build_model(
        config=config,
        lookback=config.training.lookback,
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
        feature_names=panel.feature_names,
    ).to(device)
    state_dict = checkpoint.get("model_state_dict", checkpoint)
    state_dict = _strip_orig_mod_prefix(state_dict)
    state_dict, adapted_state_keys = _adapt_dynamic_symbol_position_state(model, state_dict, strict=strict)
    incompatible = model.load_state_dict(state_dict, strict=strict)
    model.eval()
    info = {
        "checkpoint_epoch": checkpoint.get("epoch"),
        "checkpoint_best_val_loss": checkpoint.get("best_val_loss"),
        "missing_keys": list(getattr(incompatible, "missing_keys", [])),
        "unexpected_keys": list(getattr(incompatible, "unexpected_keys", [])),
        "adapted_state_keys": adapted_state_keys,
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


def _checkpoint_symbol_count(checkpoint_path: Path) -> int | None:
    checkpoint = torch.load(checkpoint_path, map_location="cpu")
    state_dict = checkpoint.get("model_state_dict", checkpoint) if isinstance(checkpoint, dict) else checkpoint
    if not isinstance(state_dict, dict):
        return None
    for key, value in state_dict.items():
        if str(key).endswith("symbol_position") and torch.is_tensor(value) and value.ndim == 4:
            return int(value.size(2))
    return None


def _daily_weight_symbols(path: Path) -> list[str]:
    if path.suffix.lower() == ".parquet":
        import pyarrow.parquet as pq

        return [str(column) for column in pq.read_schema(path).names if str(column) != "date"]
    if path.suffix.lower() == ".parquet":
        columns = pl.read_parquet_schema(path).names()
        return [str(column) for column in columns if str(column) != "date"]
    with path.open("r", encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle))
    return [str(column) for column in header if str(column) != "date"]


def _daily_weight_table_path(fold_dir: Path) -> Path | None:
    for candidate in (fold_dir / "daily_weights.parquet", fold_dir / "daily_weights.csv"):
        if candidate.is_file():
            return candidate
    return None


def _subset_panel_symbols(panel: PanelData, symbols: list[str]) -> PanelData:
    index_by_symbol = {str(symbol): idx for idx, symbol in enumerate(panel.symbols)}
    missing = [symbol for symbol in symbols if symbol not in index_by_symbol]
    if missing:
        preview = ", ".join(missing[:10])
        raise ValueError(
            f"Cannot align panel to checkpoint universe; {len(missing)} symbols from the daily_weights table "
            f"are missing in the current panel: {preview}"
        )
    indices = np.asarray([index_by_symbol[symbol] for symbol in symbols], dtype=np.int64)
    return PanelData(
        dates=panel.dates,
        symbols=list(symbols),
        feature_names=list(panel.feature_names),
        features=panel.features[:, indices, :],
        returns_1d=panel.returns_1d[:, indices],
        tradable_mask=panel.tradable_mask[:, indices],
        alive_mask=panel.alive_mask[:, indices],
        benchmark_returns=panel.benchmark_returns,
        close_prices=panel.close_prices[:, indices],
        can_buy_mask=panel.can_buy_mask[:, indices] if panel.can_buy_mask is not None else None,
        can_sell_mask=panel.can_sell_mask[:, indices] if panel.can_sell_mask is not None else None,
        can_short_open_mask=(
            panel.can_short_open_mask[:, indices] if panel.can_short_open_mask is not None else None
        ),
        force_short_cover_mask=(
            panel.force_short_cover_mask[:, indices] if panel.force_short_cover_mask is not None else None
        ),
        force_exit_mask=(
            panel.force_exit_mask[:, indices]
            if panel.force_exit_mask is not None
            else None
        ),
    )


def _align_panel_to_checkpoint_universe(panel: PanelData, output_dir: Path, fold_id: int, checkpoint_path: Path) -> PanelData:
    expected_symbols = _checkpoint_symbol_count(checkpoint_path)
    if expected_symbols is None:
        return panel
    fold_dir = _fold_dir(output_dir, fold_id)
    weights_path = _daily_weight_table_path(fold_dir)
    if weights_path is None:
        if int(panel.num_symbols) != int(expected_symbols):
            raise ValueError(
                f"Checkpoint expects {expected_symbols} symbols but current panel has {panel.num_symbols}; "
                f"cannot align because daily_weights.parquet/csv is missing in {fold_dir}."
            )
        return panel

    trained_symbols = _daily_weight_symbols(weights_path)
    # symbol_position can be the compact train-union capacity while the saved
    # daily weight table is the authoritative full evaluation universe.  This
    # mirrors inference alignment: never reject a valid ordered weight schema
    # merely because its width differs from the positional tensor checkpoint.
    if trained_symbols == list(panel.symbols):
        return panel

    current_symbols = set(panel.symbols)
    trained_set = set(trained_symbols)
    if not trained_set.issubset(current_symbols):
        missing = sorted(trained_set - current_symbols)
        preview = ", ".join(missing[:10])
        raise ValueError(
            f"Cannot align panel to checkpoint universe; {len(missing)} trained symbols are missing "
            f"from the current panel: {preview}"
        )

    extras = [symbol for symbol in panel.symbols if symbol not in trained_set]
    if int(panel.num_symbols) != int(expected_symbols) or extras:
        preview = ", ".join(extras[:10])
        print(
            "[explain] aligning panel symbols to checkpoint universe from "
            f"{weights_path}: current={panel.num_symbols}, checkpoint={expected_symbols}, "
            f"removed={len(extras)}" + (f" ({preview})" if preview else "")
        )
    else:
        print(f"[explain] reordering panel symbols to match checkpoint universe from {weights_path}")
    return _subset_panel_symbols(panel, trained_symbols)


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


def _sample_dataset_positions(
    dataset: CrossSectionalDataset,
    max_rows: int,
    method: str,
) -> np.ndarray:
    n_rows = len(dataset)
    # Non-positive is the exhaustive standalone default. A positive value is an
    # explicit reduced run requested by the caller.
    n_take = n_rows if int(max_rows) <= 0 else max(1, min(int(max_rows), n_rows))
    method = method.strip().lower()
    if method == "last":
        positions = np.arange(n_rows - n_take, n_rows, dtype=np.int64)
    elif method == "first":
        positions = np.arange(0, n_take, dtype=np.int64)
    else:
        positions = np.linspace(0, n_rows - 1, n_take, dtype=np.int64)
    return positions


def _sample_dataset_source(
    dataset: CrossSectionalDataset,
    max_rows: int,
    method: str,
) -> ExplainDatasetBatchSource:
    return ExplainDatasetBatchSource(
        dataset=dataset,
        positions=_sample_dataset_positions(dataset, max_rows, method),
    )


def _sample_dataset(
    dataset: CrossSectionalDataset,
    max_rows: int,
    method: str,
    *,
    progress_enabled: bool = False,
) -> tuple[dict[str, torch.Tensor], np.ndarray]:
    source = _sample_dataset_source(dataset, max_rows, method)
    samples = [
        dataset[int(pos)]
        for pos in tqdm(
            source.positions,
            desc="Materialize explain dates",
            unit="date",
            leave=False,
            disable=not progress_enabled,
        )
    ]
    batch = collate_batch(samples)
    return batch, source.date_indices


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
    )
    fold, checkpoint_path = _select_fold_and_checkpoint(folds, resolved_output_dir, fold_id, checkpoint)
    panel = _align_panel_to_checkpoint_universe(panel, resolved_output_dir, fold.fold_id, checkpoint_path)
    return LoadedExplanationContext(
        config=config,
        panel=panel,
        folds=folds,
        fold=fold,
        split=split,
        checkpoint_path=checkpoint_path,
        output_dir=resolved_output_dir,
    )


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
    timing_file_name: str | None = "explainability_runner_timing.json",
    write_fold_stability: bool = False,
) -> Path:
    total_start = time.perf_counter()
    config_strict_no_fallback = bool(getattr(config.training, "strict_no_fallback", False))
    if config_strict_no_fallback and not bool(settings.strict_no_fallback):
        settings = replace(settings, strict_no_fallback=True)
    device = device or next(model.parameters()).device
    if device.type == "cuda" and bool(getattr(config.environment, "use_tensor_cores", False)):
        # Match the training runtime and let Blackwell tensor cores accelerate
        # the repeated FP32 explainability matmuls without changing storage.
        torch.set_float32_matmul_precision("high")
    resolved_amp_dtype = _normalize_explain_amp_dtype(settings.amp_dtype, config)
    settings = replace(settings, amp_dtype=resolved_amp_dtype)
    split_norm = split.strip().lower()
    runner_timing: dict[str, float | str | int | bool] = {
        "fold_id": int(fold.fold_id),
        "split": split_norm,
        "enabled": True,
        "loaded_model_reused": True,
        "amp_dtype": resolved_amp_dtype,
        "lazy_host_materialization": True,
    }
    runner_progress = tqdm(
        total=4,
        desc=f"Fold {int(fold.fold_id):02d} pipeline",
        unit="stage",
        leave=False,
        disable=not bool(settings.progress_enabled),
    )
    runner_progress.set_postfix(stage="dataset", refresh=True)
    sample_start = time.perf_counter()
    dataset = _dataset_for_split(
        panel,
        fold,
        split_norm,
        config.training.lookback,
        first_test_year_only=settings.first_test_year_only,
    )
    batch_source = _sample_dataset_source(
        dataset,
        settings.max_rows,
        settings.sample_method,
    )
    date_indices = batch_source.date_indices
    dates = [str(np.datetime_as_string(panel.dates[int(idx)], unit="D")) for idx in date_indices]
    runner_timing["sample_s"] = float(time.perf_counter() - sample_start)
    runner_timing["sample_rows"] = int(len(dates))
    runner_timing["split_rows"] = int(len(dataset))
    runner_timing["ig_steps"] = int(settings.ig_steps)
    runner_timing["perturb"] = bool(settings.perturb)
    runner_timing["write_plots"] = bool(write_plots)
    runner_progress.update(1)
    runner_progress.set_postfix(stage="model_explanations", refresh=True)

    was_training = model.training
    model.eval()
    execution_model = model
    compile_start = time.perf_counter()
    if bool(settings.compile_model):
        if device.type != "cuda":
            raise RuntimeError("--compile-model requires a CUDA explainability device.")
        execution_model = torch.compile(
            model,
            dynamic=False,
            options={"triton.cudagraphs": False},
        )
    runner_timing["compile_wrapper_s"] = float(time.perf_counter() - compile_start)
    runner_timing["compile_model"] = bool(settings.compile_model)
    if device.type == "cuda":
        torch.cuda.reset_peak_memory_stats(device)
    compute_start = time.perf_counter()
    try:
        with _explain_autocast_context(device, resolved_amp_dtype):
            result = explain_batch_source_chunked(
                execution_model,
                batch_source,
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
    runner_timing["compute_dates_per_s"] = float(len(dates)) / max(float(runner_timing["compute_s"]), 1e-9)
    if device.type == "cuda":
        runner_timing["main_peak_cuda_gb"] = float(torch.cuda.max_memory_allocated(device)) / (1024**3)
    runner_progress.update(1)
    runner_progress.set_postfix(stage="write_outputs", refresh=True)

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
        "amp_dtype": resolved_amp_dtype,
        "compile_model": bool(settings.compile_model),
        "sample_rows": int(len(dates)),
        "split_rows": int(len(dataset)),
        "sampled_date_coverage": float(len(dates)) / max(1, int(len(dataset))),
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
        strict_no_fallback=bool(settings.strict_no_fallback),
        progress_enabled=bool(settings.progress_enabled),
    )
    runner_timing["write_s"] = float(time.perf_counter() - write_start)
    runner_progress.update(1)
    runner_progress.set_postfix(stage="fold_stability", refresh=True)

    if write_fold_stability and bool(settings.fold_stability):
        stability_start = time.perf_counter()
        stability_dir = write_fold_stability_outputs(output_dir / "explainability")
        runner_timing["fold_stability_s"] = float(time.perf_counter() - stability_start)
        runner_timing["fold_stability_output"] = str(stability_dir) if stability_dir is not None else ""
    else:
        runner_timing["fold_stability_s"] = 0.0
    runner_progress.update(1)
    runner_progress.close()

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
            f"stability={float(runner_timing['fold_stability_s']):.3f}s "
            f"dates_per_s={float(runner_timing['compute_dates_per_s']):.4f} "
            f"peak_cuda_gb={float(runner_timing.get('main_peak_cuda_gb', 0.0)):.2f} "
            f"json={timing_path}"
        )

    destination_out = destination
    del result, batch_source, dataset, execution_model, model
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


def _artifact_path_market_name(path: Path, artifacts_root: Path) -> str | None:
    try:
        rel = Path(path).resolve().relative_to(Path(artifacts_root).resolve())
    except ValueError:
        return None
    if not rel.parts:
        return None
    return rel.parts[0]


def _market_output_has_checkpoint(output_dir: Path, fold_id: int | None = None) -> bool:
    output_dir = Path(output_dir)
    if fold_id is not None:
        return (output_dir / f"fold_{int(fold_id):02d}" / "checkpoint_best.pt").is_file()
    return any(output_dir.glob("fold_*/checkpoint_best.pt"))


def _market_config_path(market: str, config_root: Path) -> Path:
    config_path = Path(config_root) / f"{market}.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(f"Missing config for market artifact '{market}': {config_path}")
    return config_path


def _discover_market_runs(
    artifacts_root: Path,
    config_root: Path,
    *,
    output_dir: Path | None = None,
    checkpoint: Path | None = None,
    fold_id: int | None = None,
) -> list[MarketExplainabilityRun]:
    artifacts_root = Path(artifacts_root)
    config_root = Path(config_root)

    if checkpoint is not None:
        checkpoint_path = Path(checkpoint)
        market = _artifact_path_market_name(checkpoint_path, artifacts_root)
        if market is None:
            raise ValueError(
                "--checkpoint without --config must point under "
                f"{artifacts_root}/<market>/... so the matching market config can be inferred."
            )
        return [
            MarketExplainabilityRun(
                market=market,
                config_path=_market_config_path(market, config_root),
                output_dir=artifacts_root / market,
            )
        ]

    if output_dir is not None:
        resolved_output_dir = Path(output_dir)
        market = _artifact_path_market_name(resolved_output_dir, artifacts_root) or resolved_output_dir.name
        if not _market_output_has_checkpoint(resolved_output_dir, fold_id=fold_id):
            target = f"fold_{int(fold_id):02d}/checkpoint_best.pt" if fold_id is not None else "fold_*/checkpoint_best.pt"
            raise FileNotFoundError(f"No {target} found under {resolved_output_dir}")
        return [
            MarketExplainabilityRun(
                market=market,
                config_path=_market_config_path(market, config_root),
                output_dir=resolved_output_dir,
            )
        ]

    if not artifacts_root.is_dir():
        raise FileNotFoundError(f"Market artifacts root does not exist: {artifacts_root}")

    runs: list[MarketExplainabilityRun] = []
    missing_configs: list[str] = []
    for candidate in sorted((path for path in artifacts_root.iterdir() if path.is_dir()), key=lambda path: path.name):
        if not _market_output_has_checkpoint(candidate, fold_id=fold_id):
            continue
        config_path = config_root / f"{candidate.name}.yaml"
        if not config_path.is_file():
            missing_configs.append(f"{candidate.name} ({config_path})")
            continue
        runs.append(MarketExplainabilityRun(candidate.name, config_path, candidate))

    if missing_configs:
        raise FileNotFoundError(
            "Missing configs for market artifact directories: " + ", ".join(missing_configs)
        )
    if not runs:
        target = f"fold_{int(fold_id):02d}/checkpoint_best.pt" if fold_id is not None else "fold_*/checkpoint_best.pt"
        raise FileNotFoundError(f"No market artifact directories with {target} found under {artifacts_root}")
    return runs


def _estimated_fold_explain_rows(
    fold: WalkForwardFold,
    panel: PanelData,
    *,
    split: str,
    lookback: int,
    first_test_year_only: bool,
    max_rows: int,
) -> int:
    split_norm = split.strip().lower()
    if split_norm == "train":
        indices = fold.train_indices
    elif split_norm == "val":
        indices = fold.val_indices
    else:
        indices = fold.test_indices
        if first_test_year_only:
            indices = _first_year_indices(panel, indices)
    rows = max(0, int(len(indices)) - max(1, int(lookback)) + 1)
    if int(max_rows) > 0:
        rows = min(rows, int(max_rows))
    return rows


def _balanced_fold_assignments(
    fold_ids: list[int],
    folds_by_id: dict[int, WalkForwardFold],
    panel: PanelData,
    *,
    world_size: int,
    split: str,
    lookback: int,
    first_test_year_only: bool,
    max_rows: int,
) -> tuple[list[list[int]], list[int]]:
    world_size = max(1, int(world_size))
    assignments: list[list[int]] = [[] for _ in range(world_size)]
    loads = [0 for _ in range(world_size)]
    weighted = [
        (
            int(fold_id),
            _estimated_fold_explain_rows(
                folds_by_id[int(fold_id)],
                panel,
                split=split,
                lookback=lookback,
                first_test_year_only=first_test_year_only,
                max_rows=max_rows,
            ),
        )
        for fold_id in fold_ids
    ]
    for fold_id, rows in sorted(weighted, key=lambda item: (-item[1], item[0])):
        rank = min(range(world_size), key=lambda candidate: (loads[candidate], candidate))
        assignments[rank].append(fold_id)
        loads[rank] += int(rows)
    for rank_folds in assignments:
        rank_folds.sort()
    return assignments, loads


def _run_explainability_for_config(
    args: argparse.Namespace,
    settings: ExplainabilitySettings,
    *,
    config_path: Path,
    output_dir: Path | None = None,
    explain_output_dir: Path | None = None,
) -> None:
    rank = _distributed_rank()
    world_size = _distributed_world_size()
    rank_settings = replace(
        settings,
        progress_enabled=bool(settings.progress_enabled and rank == 0),
    )
    # Default behavior: if neither --fold nor --checkpoint is provided,
    # run explainability for all folds that have checkpoint_best.pt.
    run_all_folds = args.fold is None and args.checkpoint is None
    if run_all_folds:
        setup_progress = tqdm(
            total=3,
            desc="Explain setup",
            unit="stage",
            disable=not bool(rank_settings.progress_enabled),
        )
        setup_progress.set_postfix(stage="load_config", refresh=True)
        config = load_config(config_path)
        setup_progress.update(1)
        setup_progress.set_postfix(stage="build_panel", refresh=True)
        resolved_output_dir = Path(output_dir if output_dir is not None else config.runner.output_dir)
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
        setup_progress.update(1)
        setup_progress.set_postfix(stage="build_folds", refresh=True)
        folds = build_expanding_year_folds(
            dates=panel.dates,
            min_train_years=config.walk_forward.min_train_years,
            val_years=config.walk_forward.val_years,
            require_future_test_year=config.walk_forward.require_future_test_year,
        )
        fold_ids = _available_checkpoint_folds(folds, resolved_output_dir)
        if not fold_ids:
            raise FileNotFoundError(f"No fold checkpoint_best.pt found under {resolved_output_dir}")
        setup_progress.update(1)
        setup_progress.close()

        device = _device_from_config(config, args.device)
        if device.type == "cuda":
            torch.set_float32_matmul_precision("high")
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True
        folds_by_id = {int(fold.fold_id): fold for fold in folds}
        assignments, estimated_loads = _balanced_fold_assignments(
            fold_ids,
            folds_by_id,
            panel,
            world_size=world_size,
            split=args.split,
            lookback=config.training.lookback,
            first_test_year_only=bool(settings.first_test_year_only),
            max_rows=int(settings.max_rows),
        )
        local_fold_ids = assignments[rank]
        if rank == 0:
            print(
                f"distributed explainability: world_size={world_size}, "
                f"assignments={assignments}, estimated_rows={estimated_loads}"
            )
        print(f"[rank {rank}] explaining folds: {local_fold_ids} on {device}")
        for fold_id in tqdm(
            local_fold_ids,
            desc=f"Rank {rank} explain folds",
            unit="fold",
            disable=not bool(rank_settings.progress_enabled),
        ):
            fold_stage = tqdm(
                total=3,
                desc=f"Fold {int(fold_id):02d} stages",
                unit="stage",
                leave=False,
                disable=not bool(rank_settings.progress_enabled),
            )
            fold_stage.set_postfix(stage="load_checkpoint", refresh=True)
            fold_output_dir = explain_output_dir
            if fold_output_dir is not None:
                fold_output_dir = Path(fold_output_dir) / f"fold_{int(fold_id):02d}_{args.split.strip().lower()}"
            checkpoint_path = _fold_dir(resolved_output_dir, fold_id) / "checkpoint_best.pt"
            model: nn.Module | None = None
            try:
                fold_panel = _align_panel_to_checkpoint_universe(
                    panel,
                    resolved_output_dir,
                    fold_id,
                    checkpoint_path,
                )
                model, checkpoint_info = load_model_from_checkpoint(
                    config,
                    fold_panel,
                    checkpoint_path,
                    device,
                    strict=bool(args.strict or args.strict_no_fallback),
                )
                fold_stage.update(1)
                fold_stage.set_postfix(stage="explain_all_modules", refresh=True)
                out_dir = run_loaded_model_explanation(
                    config=config,
                    panel=fold_panel,
                    fold=folds_by_id[int(fold_id)],
                    model=model,
                    checkpoint_path=checkpoint_path,
                    output_dir=resolved_output_dir,
                    split=args.split,
                    explain_output_dir=fold_output_dir,
                    settings=rank_settings,
                    write_plots=bool(args.plots),
                    plot_backend=args.plot_backend,
                    device=device,
                    checkpoint_info=checkpoint_info,
                )
                fold_stage.update(1)
                fold_stage.set_postfix(stage="cleanup", refresh=True)
            finally:
                if model is not None:
                    del model
                _clear_explainability_runtime_cache()
                fold_stage.update(1)
                fold_stage.close()
            print(f"[rank {rank}] explainability output (fold {fold_id}): {out_dir}")
        _distributed_barrier()
        if rank == 0 and settings.fold_stability:
            stability_root = (
                Path(explain_output_dir)
                if explain_output_dir is not None
                else resolved_output_dir / "explainability"
            )
            stability_dir = write_fold_stability_outputs(
                stability_root,
                strict_no_fallback=bool(settings.strict_no_fallback),
                progress_enabled=bool(settings.progress_enabled),
            )
            if stability_dir is not None:
                print(f"fold stability output: {stability_dir}")
        return

    if rank == 0:
        out_dir = run_checkpoint_explanation(
            config_path=config_path,
            output_dir=output_dir,
            fold_id=args.fold,
            checkpoint=args.checkpoint,
            split=args.split,
            explain_output_dir=explain_output_dir,
            settings=rank_settings,
            device_override=args.device,
            strict=bool(args.strict or args.strict_no_fallback),
            write_plots=bool(args.plots),
            plot_backend=args.plot_backend,
        )
        print(f"explainability output: {out_dir}")
    _distributed_barrier()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explain a trained stockAgent model checkpoint.")
    parser.add_argument(
        "--config",
        default=None,
        type=Path,
        help=(
            "Experiment config. If omitted, discover market artifact directories under "
            "--market-artifacts-root and pair them with configs from --market-config-root."
        ),
    )
    parser.add_argument("--market-artifacts-root", default=_DEFAULT_MARKET_ARTIFACTS_ROOT, type=Path)
    parser.add_argument("--market-config-root", default=_DEFAULT_MARKET_CONFIG_ROOT, type=Path)
    parser.add_argument("--output-dir", default=None, type=Path)
    parser.add_argument("--fold", default=None, type=int, help="Fold id. If omitted, explains all folds with checkpoint_best.pt.")
    parser.add_argument("--checkpoint", default=None, type=Path, help="Optional explicit checkpoint path.")
    parser.add_argument("--split", default="test", choices=("train", "val", "test"))
    parser.add_argument("--explain-output-dir", default=None, type=Path)
    parser.add_argument("--device", default=None, help="Override config environment.device, e.g. cuda or cpu.")
    parser.add_argument(
        "--cpu-threads",
        default=0,
        type=int,
        help="PyTorch intra/inter-op threads per rank; 0 keeps the runtime/environment default.",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show ETA/throughput progress bars for every long-running explainability stage.",
    )
    parser.add_argument(
        "--max-rows",
        default=0,
        type=int,
        help="Dates per fold; 0 (default) processes every date. Positive values are explicit reduced runs.",
    )
    parser.add_argument(
        "--row-chunk-size",
        default=0,
        type=int,
        help="Dates per GPU attribution chunk; 0 selects a VRAM-aware size without reducing date coverage.",
    )
    parser.add_argument(
        "--amp-dtype",
        default="config",
        choices=("config", "bf16", "fp16", "fp32"),
        help="Explainability compute precision. config uses environment.amp_dtype from the experiment config.",
    )
    parser.add_argument(
        "--compile-model",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Compile repeated model forwards with CUDA graphs disabled. Off by default until the target shape is profiled.",
    )
    parser.add_argument("--ig-steps", default=8, type=int)
    parser.add_argument("--ig-batch-size", default=0, type=int, help="Batch IG alpha steps together; 0 selects an automatic safe chunk size.")
    parser.add_argument("--sample-method", default="even", choices=("even", "first", "last"))
    parser.add_argument(
        "--first-test-year-only",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Restrict test explanation to its first year; disabled by default so all configured test years are covered.",
    )
    parser.add_argument(
        "--all-test-years",
        dest="first_test_year_only",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--perturb",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run feature perturbation sensitivity; on by default for complete offline explainability.",
    )
    parser.add_argument("--perturb-batch-size", default=0, type=int, help="Batch feature-day perturbations together; 0 selects an automatic safe chunk size.")
    parser.add_argument(
        "--perturb-max-auto-batch-size",
        default=48,
        type=int,
        help="Maximum automatic perturbation batch; tuned for the RTX 5070 Ti work-set plateau.",
    )
    parser.add_argument(
        "--perturb-max-input-elements",
        default=576_000_000,
        type=int,
        help="Aggregate perturbation input-element budget across the current date microbatch.",
    )
    parser.add_argument(
        "--counterfactual-compile",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Compile the fixed-shape embedded perturbation forward hotpath.",
    )
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
    parser.add_argument("--umap-max-points", default=0, type=int, help="Points per aux projection; 0 means all points.")
    parser.add_argument("--umap-max-projections", default=0, type=int, help="Maximum aux tensors to project with UMAP; 0 means no limit.")
    parser.add_argument("--umap-n-neighbors", default=15, type=int)
    parser.add_argument("--umap-min-dist", default=0.1, type=float)
    parser.add_argument("--strict", action="store_true", help="Load checkpoint with strict=True.")
    parser.add_argument(
        "--strict-no-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail instead of using degraded explainability or plotting fallback paths.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    initialized_here = _initialize_explainability_process_group()
    if int(args.cpu_threads) > 0:
        cpu_threads = max(1, int(args.cpu_threads))
        torch.set_num_threads(cpu_threads)
        try:
            torch.set_num_interop_threads(cpu_threads)
        except RuntimeError:
            # PyTorch only allows setting inter-op threads before parallel work
            # starts; intra-op still honors the explicit per-rank budget.
            pass
    settings = ExplainabilitySettings(
        progress_enabled=bool(args.progress),
        max_rows=args.max_rows,
        row_chunk_size=max(0, int(args.row_chunk_size)),
        amp_dtype=str(args.amp_dtype),
        compile_model=bool(args.compile_model),
        ig_steps=args.ig_steps,
        ig_batch_size=args.ig_batch_size,
        perturb=bool(args.perturb),
        perturb_batch_size=args.perturb_batch_size,
        perturb_max_auto_batch_size=args.perturb_max_auto_batch_size,
        perturb_max_input_elements=args.perturb_max_input_elements,
        counterfactual_compile=bool(args.counterfactual_compile),
        sample_method=args.sample_method,
        first_test_year_only=bool(args.first_test_year_only),
        report_style=args.report_style,
        plot_theme=args.plot_theme,
        standard_plots=bool(args.standard_plots),
        interactive_plots=not args.no_interactive_plots,
        shap_enabled=bool(args.shap),
        shap_mode=args.shap_mode,
        regime_analysis=bool(args.regime_analysis),
        fold_stability=bool(args.fold_stability),
        umap_enabled=bool(args.umap),
        umap_max_points=args.umap_max_points,
        umap_max_projections=args.umap_max_projections,
        umap_n_neighbors=args.umap_n_neighbors,
        umap_min_dist=args.umap_min_dist,
        strict_no_fallback=bool(args.strict_no_fallback),
    )
    if _distributed_rank() == 0:
        print(
            "[explain] standalone profile: "
            f"dates={'all' if settings.max_rows <= 0 else settings.max_rows}, "
            f"row_chunk={'auto' if settings.row_chunk_size <= 0 else settings.row_chunk_size}, "
            f"amp={settings.amp_dtype}, compile_model={settings.compile_model}, "
            f"test_years={'first_only' if settings.first_test_year_only else 'all'}, "
            f"IG={settings.ig_steps}, perturb={settings.perturb}, SHAP={settings.shap_enabled}, "
            f"UMAP={'disabled' if not settings.umap_enabled else ('all_points' if settings.umap_max_points <= 0 else settings.umap_max_points)}, "
            f"strict_no_fallback={settings.strict_no_fallback}, "
            f"world_size={_distributed_world_size()}"
        )

    if args.config is None:
        market_runs = _discover_market_runs(
            args.market_artifacts_root,
            args.market_config_root,
            output_dir=args.output_dir,
            checkpoint=args.checkpoint,
            fold_id=args.fold,
        )
        print(f"explaining market artifact directories under {args.market_artifacts_root}: {len(market_runs)} found")
        multi_market = len(market_runs) > 1
        for run in market_runs:
            explain_output_dir = args.explain_output_dir
            if explain_output_dir is not None and multi_market:
                explain_output_dir = Path(explain_output_dir) / run.market
            print(f"[{run.market}] config={run.config_path} output_dir={run.output_dir}")
            _run_explainability_for_config(
                args,
                settings,
                config_path=run.config_path,
                output_dir=run.output_dir,
                explain_output_dir=explain_output_dir,
            )
        if initialized_here:
            _destroy_explainability_process_group()
        return

    _run_explainability_for_config(
        args,
        settings,
        config_path=args.config,
        output_dir=args.output_dir,
        explain_output_dir=args.explain_output_dir,
    )
    if initialized_here:
        _destroy_explainability_process_group()


if __name__ == "__main__":
    main()
