from __future__ import annotations

import json
import math
import os
import time
import warnings
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import torch
from torch import nn
from tqdm.auto import tqdm

from stockagent.models.normalization import (
    dual_branch_softmax,
    masked_cross_sectional_mean,
    masked_cash_entmax15_weights,
    masked_l1_projection_weights,
    masked_signed_action_weights,
    masked_softmax,
)


MODULE_NAME = "abstract_cross_asset_transmission"
DEFAULT_SHOCKS = ("zero", "momentum", "gap", "volume", "volatility", "liquidity")
_MATPLOTLIB_TRANSFORM_DOT_WARNING = r".*invalid value encountered in dot.*"
_MATPLOTLIB_TIGHT_LAYOUT_AXES_WARNING = (
    r"This figure includes Axes that are not compatible with tight_layout.*"
)
_GRAPH_BACKENDS = {"auto", "polars", "cugraph"}
_GRAPH_EDGE_KEY_COLUMNS = ["shock", "source_index", "target_index"]
_GRAPH_EDGE_SORT_COLUMNS = ["validated_transmission", "shock", "source_index", "target_index"]
_GRAPH_EDGE_SORT_DESCENDING = [True, False, False, False]
_PLOT_ASPECT_RATIO = 17.0 / 6.0
_DEFAULT_PLOT_HEIGHT = 6.0


def _figsize_17_6(height: float = _DEFAULT_PLOT_HEIGHT) -> tuple[float, float]:
    height = max(1.0, float(height))
    return height * _PLOT_ASPECT_RATIO, height


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


@dataclass(slots=True)
class CrossAssetTransmissionSettings:
    enabled: bool = True
    progress_enabled: bool = True
    max_sources: int = 0
    max_targets: int = 0
    source_chunk_size: int = 16
    row_chunk_size: int = 0
    max_repeated_rows: int = 48
    counterfactual_compile: bool = False
    perturb_scale: float = 1.0
    shocks: tuple[str, ...] = DEFAULT_SHOCKS
    attention_flow: bool = True
    attention_capture_rows: int = 0
    attention_capture_max_elements: int = 2_000_000
    validated_transmission: bool = True
    role_embedding: bool = True
    graph_backend: str = "cugraph"
    graph_benchmark_min_edges: int = 1_000_000
    graph_explainability: bool = True
    # Compatibility-only fields. Full graph computation and plotting no
    # longer truncate by vertex count or visual Top-K limits.
    graph_betweenness_max_vertices: int = 0
    graph_plot_max_nodes: int = 0
    # Production runner mode: retain every numeric metric in one canonical
    # edge Parquet plus lookup tables, without duplicating it into 60 dense
    # matrix files per fold.  The library default stays verbose for backwards
    # compatibility with direct callers and focused tests.
    compact_artifacts: bool = False


@dataclass(slots=True)
class _GraphProcessingResult:
    backend: str
    edges: pl.DataFrame
    source_summary: pl.DataFrame
    target_summary: pl.DataFrame
    node_metrics: pl.DataFrame
    benchmark: dict[str, Any]


@dataclass(slots=True)
class _GraphExplainabilityResult:
    backend: str
    graph_edges: pl.DataFrame
    node_metrics: pl.DataFrame
    community_summary: pl.DataFrame
    community_edges: pl.DataFrame
    summary: dict[str, Any]


@dataclass(slots=True)
class _ShockAccumulator:
    shock: str
    feature_indices: list[int]
    buffers: dict[str, torch.Tensor]
    row_weight_totals: torch.Tensor
    chunk_size: int
    compile_forward: bool = False
    forward_batches: int = 0
    compiled_forward_batches: int = 0
    eager_forward_batches: int = 0
    oom_retries: int = 0
    elapsed_s: float = 0.0
    finalize_s: float = 0.0


def _is_cuda_oom(exc: BaseException) -> bool:
    message = str(exc).lower()
    return isinstance(exc, RuntimeError) and "out of memory" in message and ("cuda" in message or "cublas" in message)


def _auto_row_chunk_size(n_rows: int, n_symbols: int, settings: CrossAssetTransmissionSettings) -> tuple[int, dict[str, Any]]:
    n_rows = max(1, int(n_rows))
    override = os.environ.get("STOCKAGENT_CROSS_ASSET_ROW_CHUNK_SIZE")
    if override:
        try:
            value = max(1, min(n_rows, int(override)))
            return value, {"reason": "env_override", "row_chunk_size": value, "rows": n_rows}
        except ValueError:
            pass
    requested = int(settings.row_chunk_size)
    if requested > 0:
        value = max(1, min(n_rows, requested))
        return value, {"reason": "settings", "row_chunk_size": value, "rows": n_rows}
    source_chunk = max(1, int(settings.source_chunk_size))
    max_repeated_rows = max(1, int(settings.max_repeated_rows))
    value = max(1, min(n_rows, max_repeated_rows // source_chunk))
    if int(n_symbols) >= 10_000:
        value = 1
    return value, {
        "reason": "repeated_row_budget",
        "row_chunk_size": value,
        "rows": n_rows,
        "symbols": int(n_symbols),
        "source_chunk_size": source_chunk,
        "max_repeated_rows": max_repeated_rows,
    }


def _is_lazy_batch_source(batch: Any) -> bool:
    return callable(getattr(batch, "materialize", None)) and all(
        hasattr(batch, name)
        for name in ("num_rows", "lookback", "num_symbols", "num_features")
    )


def _cross_asset_batch_shape(batch: Any) -> tuple[int, int, int, int]:
    if _is_lazy_batch_source(batch):
        return (
            int(batch.num_rows),
            int(batch.lookback),
            int(batch.num_symbols),
            int(batch.num_features),
        )
    if not isinstance(batch, Mapping) or not torch.is_tensor(batch.get("x")):
        raise TypeError(
            "batch must be a tensor mapping or expose materialize(start, end), "
            "num_rows, lookback, num_symbols, and num_features"
        )
    x = batch["x"]
    if x.ndim != 4:
        raise ValueError(f"batch['x'] must have shape [rows, lookback, symbols, features], got {tuple(x.shape)}")
    return tuple(int(value) for value in x.shape)


def _materialize_cross_asset_rows(
    batch: Any,
    start: int,
    end: int,
    *,
    total_rows: int,
    num_symbols: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    if _is_lazy_batch_source(batch):
        rows = batch.materialize(int(start), int(end))
    else:
        rows = {
            key: (
                value[int(start) : int(end)]
                if torch.is_tensor(value) and value.ndim > 0 and int(value.size(0)) == int(total_rows)
                else value
            )
            for key, value in batch.items()
        }
    if not isinstance(rows, Mapping):
        raise TypeError("batch.materialize(start, end) must return a tensor mapping")
    x_raw = rows.get("x")
    mask_raw = rows.get("tradable_mask")
    if not torch.is_tensor(x_raw) or not torch.is_tensor(mask_raw):
        raise ValueError("materialized explainability rows must include tensor x and tradable_mask")
    expected_rows = int(end) - int(start)
    if int(x_raw.size(0)) != expected_rows or int(mask_raw.size(0)) != expected_rows:
        raise ValueError(
            "materialized explainability row count mismatch: "
            f"expected={expected_rows}, x={int(x_raw.size(0))}, mask={int(mask_raw.size(0))}"
        )
    x_cpu = torch.nan_to_num(
        x_raw.detach().to(device="cpu", dtype=torch.float32),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    mask_cpu = mask_raw.detach().to(device="cpu", dtype=torch.bool)
    returns_raw = rows.get("future_log_returns")
    if torch.is_tensor(returns_raw):
        returns_cpu = torch.nan_to_num(
            returns_raw.detach().to(device="cpu", dtype=torch.float32),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
    else:
        returns_cpu = torch.zeros((expected_rows, int(num_symbols)), dtype=torch.float32)
    return x_cpu, mask_cpu, returns_cpu


def _sanitize_tensor(value: torch.Tensor) -> torch.Tensor:
    return torch.nan_to_num(value.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)


def _to_numpy(value: torch.Tensor) -> np.ndarray:
    return np.nan_to_num(value.detach().float().cpu().numpy(), nan=0.0, posinf=0.0, neginf=0.0)


def _sanitize_matplotlib_axis_limits(fig: Any) -> None:
    for ax in getattr(fig, "axes", ()):
        for axis_name, getter, setter in (
            ("x", ax.get_xlim, ax.set_xlim),
            ("y", ax.get_ylim, ax.set_ylim),
        ):
            try:
                lo, hi = getter()
            except Exception:
                continue
            if np.isfinite([lo, hi]).all() and lo != hi:
                continue
            default_limits = (1e-12, 1.0) if axis_name == "y" and ax.get_yscale() == "log" else (0.0, 1.0)
            try:
                setter(*default_limits)
            except Exception:
                continue


def _safe_matplotlib_tight_layout(fig: Any) -> None:
    _sanitize_matplotlib_axis_limits(fig)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=_MATPLOTLIB_TRANSFORM_DOT_WARNING,
            category=RuntimeWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message=_MATPLOTLIB_TIGHT_LAYOUT_AXES_WARNING,
            category=UserWarning,
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
    _pad_saved_image_to_17_6(output_path)


def _call_model(model: nn.Module, x: torch.Tensor, mask: torch.Tensor, *, return_aux: bool = True) -> Any:
    try:
        return model(x, mask, return_aux=return_aux)
    except TypeError:
        return model(x, mask)


def _normalize_output(output: Any) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
    aux: dict[str, torch.Tensor] = {}
    if isinstance(output, tuple):
        weights = output[0]
        scores = output[1] if len(output) > 1 else weights
        if len(output) > 2 and isinstance(output[2], dict):
            aux = {str(k): v for k, v in output[2].items() if torch.is_tensor(v)}
        scores = aux.get("score_logits", scores)
    elif isinstance(output, Mapping):
        weights = output.get("weights", output.get("portfolio_weights"))
        if weights is None:
            raise ValueError("Model output is missing weights/portfolio_weights.")
        scores = output.get("score_logits", output.get("rank_logits", output.get("scores", weights)))
        aux_raw = output.get("aux", {})
        if isinstance(aux_raw, Mapping):
            aux.update({str(k): v for k, v in aux_raw.items() if torch.is_tensor(v)})
        aux.update({str(k): v for k, v in output.items() if torch.is_tensor(v)})
    else:
        weights = output
        scores = output
    weights_t = _sanitize_tensor(weights)
    scores_t = _sanitize_tensor(scores)
    rank_t = _sanitize_tensor(aux.get("rank_logits", scores_t))
    centered_t = _sanitize_tensor(aux.get("centered_score_logits", scores_t))
    return weights_t, scores_t, rank_t, centered_t, aux


def _forward_outputs(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    *,
    return_aux: bool = True,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
    output = _call_model(model, x, mask, return_aux=return_aux)
    return _normalize_output(output)


def _embedded_explainability_api(model: nn.Module) -> nn.Module | None:
    candidates = (model, getattr(model, "module", None), getattr(model, "_orig_mod", None))
    required = (
        "project_features_for_explainability",
        "embed_projected_for_explainability",
        "forward_from_embedded_explainability",
    )
    for candidate in candidates:
        support_check = getattr(
            candidate,
            "supports_embedded_explainability_reuse",
            None,
        )
        supported = not callable(support_check) or bool(support_check())
        if (
            candidate is not None
            and supported
            and all(callable(getattr(candidate, name, None)) for name in required)
        ):
            return candidate
    return None


def _stock_embedding_explainability_api(model: nn.Module) -> nn.Module | None:
    candidates = (model, getattr(model, "module", None), getattr(model, "_orig_mod", None))
    required = (
        "temporal_stock_embeddings_for_explainability",
        "forward_from_stock_embeddings_explainability",
    )
    for candidate in candidates:
        support_check = getattr(
            candidate,
            "supports_embedded_explainability_reuse",
            None,
        )
        supported = not callable(support_check) or bool(support_check())
        if (
            candidate is not None
            and supported
            and all(callable(getattr(candidate, name, None)) for name in required)
        ):
            return candidate
    return None


def _forward_embedded_outputs(
    model: nn.Module,
    embedded: torch.Tensor,
    mask: torch.Tensor,
    *,
    compile_forward: bool = False,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
    if compile_forward and callable(getattr(model, "forward_from_embedded_explainability_compiled", None)):
        output = model.forward_from_embedded_explainability_compiled(embedded, mask)
    else:
        output = model.forward_from_embedded_explainability(
            embedded,
            mask,
            return_aux=False,
        )
    return _normalize_output(output)


def _forward_stock_embedding_outputs(
    model: nn.Module,
    stock_embeddings: torch.Tensor,
    mask: torch.Tensor,
    *,
    compile_forward: bool = False,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
    if compile_forward and callable(
        getattr(model, "forward_from_stock_embeddings_explainability_compiled", None)
    ):
        output = model.forward_from_stock_embeddings_explainability_compiled(
            stock_embeddings,
            mask,
        )
    else:
        output = model.forward_from_stock_embeddings_explainability(
            stock_embeddings,
            mask,
            return_aux=False,
        )
    return _normalize_output(output)


def _portfolio_weights_from_scores(model: nn.Module, scores: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    scores = _sanitize_tensor(scores)
    mask = mask.to(device=scores.device, dtype=torch.bool)
    temp = float(getattr(model, "default_temperature", 1.0))
    temp = max(0.05, temp)
    activation = str(getattr(model, "portfolio_activation", "identity"))
    mode = str(getattr(model, "portfolio_mode", "long_short")).strip().lower()
    output_mode = str(getattr(model, "portfolio_output_mode", "activation_l1")).strip().lower().replace("-", "_")
    scale_projection_by_active_count = bool(
        getattr(model, "projection_l1_scale_by_active_count", False)
    )
    if mode in {"long", "long_only", "longonly"}:
        target_logits = (scores / temp).masked_fill(~mask, 0.0)
        if output_mode == "logits":
            return target_logits
        if output_mode == "signed_softmax":
            return masked_signed_action_weights(target_logits, mask, transform="softmax", long_only=True).masked_fill(~mask, 0.0)
        if output_mode == "signed_sparsemax":
            return masked_signed_action_weights(target_logits, mask, transform="sparsemax", long_only=True).masked_fill(~mask, 0.0)
        if output_mode == "signed_entmax15":
            return masked_signed_action_weights(target_logits, mask, transform="entmax15", long_only=True).masked_fill(~mask, 0.0)
        if output_mode == "cash_entmax15":
            return masked_cash_entmax15_weights(
                target_logits,
                mask,
                short_mask=torch.zeros_like(mask),
            ).masked_fill(~mask, 0.0)
        if output_mode == "projection_l1":
            return masked_l1_projection_weights(
                target_logits,
                mask,
                long_only=True,
                scale_by_active_count=scale_projection_by_active_count,
            ).masked_fill(~mask, 0.0)
        weight_activation = "identity" if output_mode == "l1" else activation
        return masked_softmax(scores / temp, mask, activation=weight_activation).masked_fill(~mask, 0.0)
    centered = (
        scores - masked_cross_sectional_mean(scores, mask)
        if bool(getattr(model, "center_long_short_logits", True))
        else scores
    )
    target_logits = (centered / temp).masked_fill(~mask, 0.0)
    if output_mode == "logits":
        return target_logits
    if output_mode == "signed_softmax":
        return masked_signed_action_weights(target_logits, mask, transform="softmax", long_only=False).masked_fill(~mask, 0.0)
    if output_mode == "signed_sparsemax":
        return masked_signed_action_weights(target_logits, mask, transform="sparsemax", long_only=False).masked_fill(~mask, 0.0)
    if output_mode == "signed_entmax15":
        return masked_signed_action_weights(target_logits, mask, transform="entmax15", long_only=False).masked_fill(~mask, 0.0)
    if output_mode == "cash_entmax15":
        return masked_cash_entmax15_weights(
            target_logits,
            mask,
            short_mask=mask,
        ).masked_fill(~mask, 0.0)
    if output_mode == "projection_l1":
        return masked_l1_projection_weights(
            target_logits,
            mask,
            long_only=False,
            scale_by_active_count=scale_projection_by_active_count,
        ).masked_fill(~mask, 0.0)
    weight_activation = "identity" if output_mode == "l1" else activation
    return dual_branch_softmax(centered / temp, mask, activation=weight_activation).masked_fill(~mask, 0.0)


def _rank_positions(scores: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    masked = scores.masked_fill(~mask.bool(), -torch.inf)
    order = torch.argsort(masked, dim=1, descending=True)
    ranks = torch.empty_like(order, dtype=torch.float32)
    values = torch.arange(int(scores.size(1)), device=scores.device, dtype=torch.float32).expand_as(order)
    ranks.scatter_(1, order, values)
    return ranks.masked_fill(~mask.bool(), float(scores.size(1)))


def _feature_indices_for_shock(feature_names: list[str], shock: str) -> list[int]:
    lowered = [str(name).lower() for name in feature_names]
    shock = str(shock).strip().lower()
    if shock == "zero":
        return list(range(len(feature_names)))
    patterns = {
        "momentum": ("logret", "return", "ret"),
        "gap": ("open", "gap", "max", "min"),
        "volume": ("volume", "vol"),
        "volatility": ("max", "min", "shadow", "range", "clv"),
        "liquidity": ("volume", "turnover", "amount", "liquid"),
    }.get(shock, (shock,))
    return [idx for idx, name in enumerate(lowered) if any(pattern in name for pattern in patterns)]


def _apply_shock(
    x: torch.Tensor,
    source_local: int,
    source_symbol: int,
    feature_indices: list[int],
    *,
    shock: str,
    scale: float,
    feature_std: torch.Tensor,
) -> None:
    if not feature_indices:
        return
    view = x[source_local, :, :, source_symbol, :]
    idx = torch.as_tensor(feature_indices, device=x.device, dtype=torch.long)
    shock = str(shock).strip().lower()
    if shock == "zero":
        view.index_fill_(2, idx, 0.0)
        return
    std = feature_std[feature_indices].to(device=x.device, dtype=x.dtype).reshape(1, 1, -1)
    signed_scale = -float(scale) if shock == "liquidity" else float(scale)
    view.index_copy_(2, idx, view.index_select(2, idx) + signed_scale * std)


def _apply_shock_to_source_features(
    source_features: torch.Tensor,
    feature_indices: list[int],
    *,
    shock: str,
    scale: float,
    feature_std: torch.Tensor,
) -> None:
    """Apply the canonical shock to [source,row,lookback,feature] slices."""
    if not feature_indices:
        return
    idx = torch.as_tensor(feature_indices, device=source_features.device, dtype=torch.long)
    if str(shock).strip().lower() == "zero":
        source_features.index_fill_(-1, idx, 0.0)
        return
    std = feature_std[feature_indices].to(
        device=source_features.device,
        dtype=source_features.dtype,
    ).reshape(1, 1, 1, -1)
    signed_scale = -float(scale) if str(shock).strip().lower() == "liquidity" else float(scale)
    source_features.index_copy_(
        -1,
        idx,
        source_features.index_select(-1, idx) + signed_scale * std,
    )


def _select_symbols(
    weights: torch.Tensor,
    scores: torch.Tensor,
    mask: torch.Tensor,
    *,
    max_sources: int,
    max_targets: int,
) -> tuple[list[int], list[int], np.ndarray]:
    active = mask.bool().any(dim=0)
    score = weights.abs().mean(dim=0) + 0.05 * scores.abs().mean(dim=0)
    score = score.masked_fill(~active, -torch.inf)
    n_active = int(active.sum().detach().cpu().item())
    if n_active <= 0:
        return [], [], np.zeros(int(weights.size(1)), dtype=np.float32)
    requested_sources = int(max_sources)
    requested_targets = int(max_targets)
    n_sources = n_active if requested_sources <= 0 else min(requested_sources, n_active)
    n_targets = n_active if requested_targets <= 0 else min(requested_targets, n_active)
    source_idx = torch.topk(score, k=n_sources).indices.detach().cpu().tolist()
    target_idx = torch.topk(score, k=n_targets).indices.detach().cpu().tolist()
    return [int(i) for i in source_idx], [int(i) for i in target_idx], _to_numpy(score)


def _mean_over_batch_tensor(value: torch.Tensor) -> torch.Tensor:
    """Reduce scenario rows without forcing a CUDA synchronization/D2H copy."""
    return torch.nan_to_num(
        value.mean(dim=1, dtype=torch.float32),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )


def _empty_metric_buffers(
    n_sources: int,
    n_targets: int,
    *,
    device: torch.device | str = "cpu",
) -> dict[str, torch.Tensor]:
    return {
        name: torch.zeros((n_sources, n_targets), dtype=torch.float32, device=device)
        for name in (
            "score_abs",
            "score_signed",
            "weight_total_abs",
            "weight_total_signed",
            "weight_reallocation_abs",
            "weight_residual_abs",
            "rank_abs",
            "flip_prob",
            "transmission_pnl",
        )
    }


def _shock_source_chunk_metrics(
    model: nn.Module,
    x_row: torch.Tensor,
    mask_row: torch.Tensor,
    returns_row: torch.Tensor,
    base_weights_row: torch.Tensor,
    base_scores_row: torch.Tensor,
    base_rank_pos_row: torch.Tensor,
    feature_std: torch.Tensor,
    selected_targets: torch.Tensor,
    chunk_sources: list[int],
    feature_indices: list[int],
    *,
    shock: str,
    perturb_scale: float,
    embedded_api: nn.Module | None = None,
    base_projected_row: torch.Tensor | None = None,
    base_embedded_row: torch.Tensor | None = None,
    base_stock_embeddings_row: torch.Tensor | None = None,
    compile_forward: bool = False,
    max_repeated_rows: int = 0,
) -> dict[str, torch.Tensor]:
    repeats = len(chunk_sources)
    row_count = int(x_row.size(0))
    n_symbols = int(x_row.size(2))
    with torch.no_grad():
        work_sources = list(chunk_sources)
        if compile_forward and embedded_api is not None:
            compiled_sources = math.ceil(max(1, int(max_repeated_rows)) / max(1, row_count))
            padded_count = max(repeats, compiled_sources)
            work_sources.extend([work_sources[-1]] * (padded_count - repeats))
        work_repeats = len(work_sources)
        mask_rep = mask_row.unsqueeze(0).expand(work_repeats, *tuple(mask_row.shape)).reshape(
            work_repeats * row_count,
            n_symbols,
        )
        if embedded_api is not None and base_projected_row is not None and base_embedded_row is not None:
            source_tensor = torch.as_tensor(work_sources, device=x_row.device, dtype=torch.long)
            changed_sources = x_row.index_select(2, source_tensor).permute(2, 0, 1, 3).contiguous()
            _apply_shock_to_source_features(
                changed_sources,
                feature_indices,
                shock=shock,
                scale=float(perturb_scale),
                feature_std=feature_std,
            )
            changed_projected = embedded_api.project_features_for_explainability(changed_sources)
            source_projected = base_projected_row.index_select(2, source_tensor).permute(2, 0, 1, 3)
            source_embedded = base_embedded_row.index_select(2, source_tensor).permute(2, 0, 1, 3)
            changed_source_embedded = source_embedded + changed_projected - source_projected
            if base_stock_embeddings_row is not None and callable(
                getattr(embedded_api, "temporal_stock_embeddings_for_explainability", None)
            ) and callable(
                getattr(embedded_api, "forward_from_stock_embeddings_explainability", None)
            ):
                # The single-stock temporal stage is independent of tradability.
                # The canonical full-universe mask is applied in the cached
                # post-temporal forward below; using an all-true S=1 mask also
                # avoids an all-false attention row for inactive sources.
                source_mask = torch.ones(
                    (work_repeats * row_count, 1),
                    device=x_row.device,
                    dtype=torch.bool,
                )
                changed_stock = embedded_api.temporal_stock_embeddings_for_explainability(
                    changed_source_embedded.reshape(
                        work_repeats * row_count,
                        int(x_row.size(1)),
                        1,
                        int(changed_source_embedded.size(-1)),
                    ),
                    source_mask,
                ).reshape(work_repeats, row_count, -1)
                stock_rep = base_stock_embeddings_row.unsqueeze(0).expand(
                    (work_repeats,) + tuple(base_stock_embeddings_row.shape)
                ).clone()
                local = torch.arange(work_repeats, device=x_row.device)
                stock_rep[local, :, source_tensor, :] = changed_stock
                weights_p, scores_p, rank_p, _centered_p, _aux_p = (
                    _forward_stock_embedding_outputs(
                        embedded_api,
                        stock_rep.reshape(
                            work_repeats * row_count,
                            *tuple(base_stock_embeddings_row.shape[1:]),
                        ),
                        mask_rep,
                        compile_forward=compile_forward,
                    )
                )
            else:
                embedded_rep = base_embedded_row.unsqueeze(0).expand(
                    (work_repeats,) + tuple(base_embedded_row.shape)
                ).clone()
                local = torch.arange(work_repeats, device=x_row.device)
                embedded_rep[local, :, :, source_tensor, :] = changed_source_embedded
                embedded_rep = embedded_rep.reshape(
                    work_repeats * row_count,
                    *tuple(base_embedded_row.shape[1:]),
                )
                weights_p, scores_p, rank_p, _centered_p, _aux_p = _forward_embedded_outputs(
                    embedded_api,
                    embedded_rep,
                    mask_rep,
                    compile_forward=compile_forward,
                )
        else:
            x_rep = x_row.detach().unsqueeze(0).expand((work_repeats,) + tuple(x_row.shape)).clone()
            for local_idx, source_symbol_idx in enumerate(work_sources):
                _apply_shock(
                    x_rep,
                    local_idx,
                    source_symbol_idx,
                    feature_indices,
                    shock=shock,
                    scale=float(perturb_scale),
                    feature_std=feature_std,
                )
            x_rep = x_rep.reshape(work_repeats * row_count, *tuple(x_row.shape[1:]))
            weights_p, scores_p, rank_p, _centered_p, _aux_p = _forward_outputs(
                model,
                x_rep,
                mask_rep,
                return_aux=False,
            )
        weights_p = weights_p.reshape(work_repeats, row_count, n_symbols)[:repeats]
        scores_p = scores_p.reshape(work_repeats, row_count, n_symbols)[:repeats]
        rank_p = rank_p.reshape(work_repeats, row_count, n_symbols)[:repeats]
        mask_rep = mask_rep.reshape(work_repeats, row_count, n_symbols)[:repeats].reshape(
            repeats * row_count,
            n_symbols,
        )

        score_delta = scores_p - base_scores_row.unsqueeze(0)
        weight_delta = weights_p - base_weights_row.unsqueeze(0)
        pert_rank_pos = _rank_positions(
            rank_p.reshape(repeats * row_count, n_symbols),
            mask_rep,
        ).reshape(repeats, row_count, n_symbols)
        rank_delta = pert_rank_pos - base_rank_pos_row.unsqueeze(0)

        norm_scores = base_scores_row.unsqueeze(0).expand(repeats, -1, -1).clone()
        local_sources = torch.arange(repeats, device=x_row.device)
        actual_source_tensor = torch.as_tensor(
            chunk_sources,
            device=x_row.device,
            dtype=torch.long,
        )
        norm_scores[local_sources, :, actual_source_tensor] = scores_p[
            local_sources,
            :,
            actual_source_tensor,
        ]
        norm_weights = _portfolio_weights_from_scores(
            model,
            norm_scores.reshape(repeats * row_count, n_symbols),
            mask_rep,
        ).reshape(repeats, row_count, n_symbols)
        realloc_delta = norm_weights - base_weights_row.unsqueeze(0)
        residual_delta = weight_delta - realloc_delta
        base_target_weight = base_weights_row.index_select(1, selected_targets).unsqueeze(0)
        pert_target_weight = weights_p.index_select(2, selected_targets)
        target_returns = returns_row.index_select(1, selected_targets).unsqueeze(0)
        return {
            "score_abs": _mean_over_batch_tensor(score_delta.index_select(2, selected_targets).abs()),
            "score_signed": _mean_over_batch_tensor(score_delta.index_select(2, selected_targets)),
            "weight_total_abs": _mean_over_batch_tensor(weight_delta.index_select(2, selected_targets).abs()),
            "weight_total_signed": _mean_over_batch_tensor(weight_delta.index_select(2, selected_targets)),
            "weight_reallocation_abs": _mean_over_batch_tensor(realloc_delta.index_select(2, selected_targets).abs()),
            "weight_residual_abs": _mean_over_batch_tensor(residual_delta.index_select(2, selected_targets).abs()),
            "rank_abs": _mean_over_batch_tensor(rank_delta.index_select(2, selected_targets).abs()),
            "flip_prob": _mean_over_batch_tensor((base_target_weight * pert_target_weight < 0).float()),
            "transmission_pnl": _mean_over_batch_tensor(
                weight_delta.index_select(2, selected_targets) * target_returns
            ),
        }


def _compute_attention_flow_from_captures(
    captures: list[dict[str, object]],
    *,
    n_symbols: int,
) -> tuple[np.ndarray | None, list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    direct: list[np.ndarray] = []
    market_to_source: list[np.ndarray] = []
    target_to_market: list[np.ndarray] = []
    for capture in captures:
        name = str(capture.get("name", ""))
        attn = capture.get("attention")
        if not torch.is_tensor(attn):
            continue
        arr = np.nan_to_num(attn.numpy().astype(np.float64, copy=False), nan=0.0, posinf=0.0, neginf=0.0)
        if arr.ndim != 3:
            continue
        mean_attn = arr.mean(axis=0)
        q_tokens, k_tokens = mean_attn.shape
        rows.append({"name": name, "query_tokens": int(q_tokens), "key_tokens": int(k_tokens)})
        if q_tokens == n_symbols and k_tokens == n_symbols:
            direct.append(mean_attn.T)
        elif q_tokens < n_symbols and k_tokens == n_symbols:
            market_to_source.append(mean_attn)
        elif q_tokens == n_symbols and k_tokens < n_symbols:
            target_to_market.append(mean_attn)
    flows: list[np.ndarray] = []
    flows.extend(direct)
    if market_to_source and target_to_market:
        for a_ms in market_to_source:
            for a_tm in target_to_market:
                common = min(a_ms.shape[0], a_tm.shape[1])
                if common > 0:
                    flows.append(a_ms[:common, :].T @ a_tm[:, :common].T)
    if not flows:
        warnings.append("No compatible stock-to-stock or stock-market-stock attention captures were available.")
        return None, rows, warnings
    flow = np.nan_to_num(np.mean(np.stack(flows, axis=0), axis=0), nan=0.0, posinf=0.0, neginf=0.0)
    return flow.astype(np.float32, copy=False), rows, warnings


def _capture_attention_flow(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    *,
    n_symbols: int,
    rows: int,
    max_elements: int,
) -> tuple[np.ndarray | None, list[dict[str, Any]], list[str]]:
    if not hasattr(model, "configure_attention_capture") or not hasattr(model, "pop_attention_capture"):
        return None, [], ["Model does not expose attention capture hooks."]
    try:
        model.configure_attention_capture(True, max_rows=max(1, int(rows)), max_elements=max(1, int(max_elements)))
        with torch.no_grad():
            _forward_outputs(model, x[: max(1, int(rows))], mask[: max(1, int(rows))], return_aux=True)
        captures = model.pop_attention_capture()
    except Exception as exc:
        return None, [], [f"Attention capture failed: {type(exc).__name__}: {exc}"]
    finally:
        try:
            model.configure_attention_capture(False)
        except Exception:
            pass
    return _compute_attention_flow_from_captures(captures, n_symbols=n_symbols)


def _role_embedding_frame(
    aux: dict[str, torch.Tensor],
    symbols: list[str],
    importance: np.ndarray,
) -> tuple[pl.DataFrame, list[str]]:
    warnings: list[str] = []
    tensor = None
    source_name = ""
    for name in ("z_stock", "stock_embedding", "z_market_context"):
        value = aux.get(name)
        if torch.is_tensor(value) and value.ndim == 3:
            tensor = _to_numpy(value.mean(dim=0))
            source_name = name
            break
    if tensor is None:
        return pl.DataFrame(), ["No stock-level aux tensor was available for role embedding."]
    centered = tensor - tensor.mean(axis=0, keepdims=True)
    try:
        _, _, vt = np.linalg.svd(centered, full_matrices=False)
        basis = vt[:2].T if vt.shape[0] >= 2 else np.pad(vt[:1].T, ((0, 0), (0, 1)))
        coords = centered @ basis
    except Exception as exc:
        warnings.append(f"Role PCA failed: {type(exc).__name__}: {exc}")
        coords = np.zeros((tensor.shape[0], 2), dtype=np.float32)
    rows = [
        {
            "symbol": symbols[idx] if idx < len(symbols) else str(idx),
            "symbol_index": int(idx),
            "role_x": float(coords[idx, 0]),
            "role_y": float(coords[idx, 1]),
            "role_norm": float(np.linalg.norm(tensor[idx])),
            "selection_importance": float(importance[idx]) if idx < importance.size else 0.0,
            "source_tensor": source_name,
        }
        for idx in range(tensor.shape[0])
    ]
    return pl.DataFrame(rows), warnings


def _write_frame_csv_or_parquet(path: Path, frame: pl.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Large complete S² tables are dramatically faster and smaller in Parquet;
    # small human-inspection tables keep their CSV form.
    if int(frame.height) * max(1, int(frame.width)) >= 5_000_000:
        parquet_path = path.with_suffix(".parquet")
        pq.write_table(frame.to_arrow(), parquet_path, compression="zstd")
        if path.exists():
            path.unlink()
        return parquet_path
    try:
        frame.write_csv(path)
        parquet_path = path.with_suffix(".parquet")
        if parquet_path.exists():
            parquet_path.unlink()
        return path
    except Exception as exc:
        if "nested" not in str(exc).lower():
            raise
        parquet_path = path.with_suffix(".parquet")
        pq.write_table(frame.to_arrow(), parquet_path, compression="snappy")
        if path.exists():
            path.unlink()
        return parquet_path


def _write_matrix_csv(path: Path, matrix: np.ndarray, source_symbols: list[str], target_symbols: list[str]) -> None:
    data: dict[str, Any] = {"source_symbol": list(source_symbols)}
    data.update({str(symbol): matrix[:, idx] for idx, symbol in enumerate(target_symbols)})
    frame = pl.DataFrame(data)
    _write_frame_csv_or_parquet(path, frame)


def _sparse_axis_ticks(labels: list[str], *, max_ticks: int = 64) -> tuple[np.ndarray, list[str]]:
    count = len(labels)
    if count <= max(1, int(max_ticks)):
        positions = np.arange(count, dtype=np.int32)
    else:
        positions = np.unique(
            np.linspace(0, count - 1, num=max(2, int(max_ticks)), dtype=np.int32)
        )
    return positions, [str(labels[int(position)]) for position in positions]


def _shock_summary_csv_frame(shock_summaries: list[dict[str, Any]]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in shock_summaries:
        out = dict(row)
        matched = out.get("matched_features")
        if isinstance(matched, list | tuple):
            out["matched_features"] = ";".join(str(item) for item in matched)
        rows.append(out)
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def _plot_heatmap(path: Path, matrix: np.ndarray, title: str, source_symbols: list[str], target_symbols: list[str]) -> None:
    if matrix.size == 0:
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    row_count = matrix.shape[0]
    column_count = matrix.shape[1]
    data = matrix
    fig_h = min(12.0, max(5.5, 0.30 * row_count + 2.0, (0.32 * column_count + 3.0) / _PLOT_ASPECT_RATIO))
    fig, ax = plt.subplots(figsize=_figsize_17_6(fig_h), dpi=140)
    vmax = float(np.nanmax(np.abs(data))) if data.size else 1.0
    if vmax <= 0:
        vmax = 1.0
    image = ax.imshow(data, aspect="auto", cmap="magma", vmin=0.0, vmax=vmax)
    ax.set_title(title)
    ax.set_xlabel("target stock j")
    ax.set_ylabel("source stock i")
    x_positions, x_labels = _sparse_axis_ticks(target_symbols, max_ticks=72)
    y_positions, y_labels = _sparse_axis_ticks(source_symbols, max_ticks=72)
    ax.set_xticks(x_positions, x_labels, rotation=90, fontsize=7)
    ax.set_yticks(y_positions, y_labels, fontsize=7)
    fig.colorbar(image, ax=ax, shrink=0.8)
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _plot_graph_node_importance(
    path: Path,
    node_metrics: pl.DataFrame,
) -> None:
    if node_metrics.is_empty() or "pagerank" not in node_metrics.columns:
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    data = node_metrics.sort("pagerank", descending=True)
    labels = data["symbol"].cast(pl.String).to_list() if "symbol" in data.columns else data["symbol_index"].cast(pl.String).to_list()
    pagerank = data["pagerank"].fill_null(0.0).to_numpy().astype(np.float64, copy=False)
    hub = data["hub_score"].fill_null(0.0).to_numpy().astype(np.float64, copy=False) if "hub_score" in data.columns else np.zeros_like(pagerank)
    authority = (
        data["authority_score"].fill_null(0.0).to_numpy().astype(np.float64, copy=False)
        if "authority_score" in data.columns
        else np.zeros_like(pagerank)
    )
    x = np.arange(data.height)
    fig, ax = plt.subplots(figsize=_figsize_17_6(10.0), dpi=180)
    ax.plot(x, pagerank, linewidth=1.0, label="PageRank")
    ax.plot(x, hub, linewidth=0.9, alpha=0.85, label="Hub")
    ax.plot(x, authority, linewidth=0.9, alpha=0.85, label="Authority")
    tick_positions, tick_labels = _sparse_axis_ticks(labels, max_ticks=72)
    ax.set_xticks(tick_positions, tick_labels, rotation=90, fontsize=6)
    ax.set_xlim(-0.5, max(0.5, data.height - 0.5))
    ax.set_ylabel("normalized graph score")
    ax.set_xlabel("every graph node, ordered by PageRank")
    ax.set_title(f"Complete Cross-Asset Graph Node Importance ({data.height} nodes)")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(alpha=0.18)
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _plot_graph_community_flow(path: Path, community_edges: pl.DataFrame) -> None:
    if community_edges.is_empty():
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    communities = sorted(
        {
            int(value)
            for column in ("source_community", "target_community")
            if column in community_edges.columns
            for value in community_edges[column].drop_nulls().to_list()
        }
    )
    if not communities:
        return
    matrix = np.zeros((len(communities), len(communities)), dtype=np.float64)
    positions = pl.DataFrame(
        {
            "community_id": np.asarray(communities, dtype=np.int64),
            "matrix_position": np.arange(len(communities), dtype=np.int64),
        }
    )
    mapped = (
        community_edges.select(
            pl.col("source_community").cast(pl.Int64),
            pl.col("target_community").cast(pl.Int64),
            pl.col("edge_weight").cast(pl.Float64).fill_null(0.0),
        )
        .join(
            positions.rename(
                {"community_id": "source_community", "matrix_position": "source_position"}
            ),
            on="source_community",
            how="inner",
        )
        .join(
            positions.rename(
                {"community_id": "target_community", "matrix_position": "target_position"}
            ),
            on="target_community",
            how="inner",
        )
    )
    if not mapped.is_empty():
        np.add.at(
            matrix,
            (mapped["source_position"].to_numpy(), mapped["target_position"].to_numpy()),
            mapped["edge_weight"].to_numpy(),
        )
    fig_h = min(18.0, max(
        5.0,
        0.45 * len(communities) + 2.5,
        (0.45 * len(communities) + 3.0) / _PLOT_ASPECT_RATIO,
    ))
    fig, ax = plt.subplots(figsize=_figsize_17_6(fig_h), dpi=140)
    vmax = float(np.nanmax(matrix)) if matrix.size else 1.0
    if vmax <= 0:
        vmax = 1.0
    image = ax.imshow(matrix, aspect="auto", cmap="viridis", vmin=0.0, vmax=vmax)
    ax.set_title("Cross-Asset Community Transmission Flow")
    ax.set_xlabel("target community")
    ax.set_ylabel("source community")
    community_labels = [str(value) for value in communities]
    tick_positions, tick_labels = _sparse_axis_ticks(community_labels, max_ticks=72)
    ax.set_xticks(tick_positions, tick_labels, rotation=90, fontsize=7)
    ax.set_yticks(tick_positions, tick_labels, fontsize=7)
    fig.colorbar(image, ax=ax, shrink=0.8)
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _plot_graph_topology(path: Path, graph_edges: pl.DataFrame, node_metrics: pl.DataFrame) -> None:
    """Render every directed edge as a complete source-by-target adjacency map."""
    if graph_edges.is_empty() or node_metrics.is_empty():
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    sources = node_metrics.sort("weighted_out_degree", descending=True)
    targets = node_metrics.sort("weighted_in_degree", descending=True)
    data = graph_edges.filter(pl.col("source_index") != pl.col("target_index"))
    if data.is_empty():
        return
    source_ids_ordered = sources["symbol_index"].cast(pl.Int64).to_numpy()
    target_ids_ordered = targets["symbol_index"].cast(pl.Int64).to_numpy()
    if source_ids_ordered.size == 0 or target_ids_ordered.size == 0:
        return
    maximum_index = int(
        max(
            source_ids_ordered.max(),
            target_ids_ordered.max(),
            data["source_index"].max(),
            data["target_index"].max(),
        )
    )
    source_positions = np.full(maximum_index + 1, -1, dtype=np.int32)
    target_positions = np.full(maximum_index + 1, -1, dtype=np.int32)
    source_positions[source_ids_ordered] = np.arange(source_ids_ordered.size, dtype=np.int32)
    target_positions[target_ids_ordered] = np.arange(target_ids_ordered.size, dtype=np.int32)
    source_ids = data["source_index"].cast(pl.Int64).to_numpy()
    target_ids = data["target_index"].cast(pl.Int64).to_numpy()
    valid = (
        (source_ids >= 0)
        & (source_ids <= maximum_index)
        & (target_ids >= 0)
        & (target_ids <= maximum_index)
        & (source_positions[source_ids] >= 0)
        & (target_positions[target_ids] >= 0)
    )
    source_ids = source_ids[valid]
    target_ids = target_ids[valid]
    weights = data["edge_weight"].fill_null(0.0).to_numpy().astype(np.float32, copy=False)[valid]
    if not weights.size:
        return
    matrix = np.zeros(
        (source_ids_ordered.size, target_ids_ordered.size),
        dtype=np.float32,
    )
    np.add.at(
        matrix,
        (source_positions[source_ids], target_positions[target_ids]),
        weights,
    )
    positive = matrix[matrix > 0.0]
    vmax = float(np.nanmax(positive)) if positive.size else 1.0
    if vmax <= 0.0:
        vmax = 1.0
    fig, ax = plt.subplots(figsize=_figsize_17_6(12.0), dpi=180)
    image = ax.imshow(matrix, aspect="auto", cmap="viridis", vmin=0.0, vmax=vmax)
    source_labels = sources["symbol"].cast(pl.String).to_list()
    target_labels = targets["symbol"].cast(pl.String).to_list()
    x_positions, x_labels = _sparse_axis_ticks(target_labels, max_ticks=72)
    y_positions, y_labels = _sparse_axis_ticks(source_labels, max_ticks=72)
    ax.set_xticks(x_positions, x_labels, rotation=90, fontsize=6)
    ax.set_yticks(y_positions, y_labels, fontsize=6)
    ax.set_xlabel("every target / receiver, ordered by weighted in-degree")
    ax.set_ylabel("every source / transmitter, ordered by weighted out-degree")
    ax.set_title(
        f"Complete Cross-Asset Directed Topology ({source_ids_ordered.size} nodes, {weights.size} inter-symbol edges)"
    )
    fig.colorbar(image, ax=ax, shrink=0.78, label="edge weight")
    ax.text(
        0.0,
        -0.16,
        "Every inter-symbol edge is included. Rows send influence and columns receive it; no Top-K selection is applied.",
        transform=ax.transAxes,
        fontsize=8,
        color="#4b5563",
        ha="left",
        va="top",
    )
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _plot_graph_transmission_matrix(path: Path, graph_edges: pl.DataFrame, node_metrics: pl.DataFrame) -> None:
    if graph_edges.is_empty() or node_metrics.is_empty():
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    rank_column = "pagerank" if "pagerank" in node_metrics.columns else "weighted_in_degree"
    sort_columns = ["community_id", rank_column] if "community_id" in node_metrics.columns else [rank_column]
    descending = [False, True] if "community_id" in node_metrics.columns else [True]
    ordered = node_metrics.sort(sort_columns, descending=descending)
    ids = [int(value) for value in ordered["symbol_index"].to_list()]
    if not ids:
        return
    labels = ordered["symbol"].cast(pl.String).to_list() if "symbol" in ordered.columns else [str(value) for value in ids]
    matrix = np.zeros((len(ids), len(ids)), dtype=np.float64)
    # Map and accumulate the complete edge set with native columnar joins.  The
    # previous Python ``to_dicts`` loop could spend minutes iterating millions of
    # edges without yielding progress and duplicated the edge table as objects.
    positions = pl.DataFrame(
        {
            "symbol_index": np.asarray(ids, dtype=np.int64),
            "matrix_position": np.arange(len(ids), dtype=np.int64),
        }
    )
    mapped_edges = (
        graph_edges.select(
            pl.col("source_index").cast(pl.Int64),
            pl.col("target_index").cast(pl.Int64),
            pl.col("edge_weight").cast(pl.Float64).fill_null(0.0),
        )
        .join(
            positions.rename({"symbol_index": "source_index", "matrix_position": "source_position"}),
            on="source_index",
            how="inner",
        )
        .join(
            positions.rename({"symbol_index": "target_index", "matrix_position": "target_position"}),
            on="target_index",
            how="inner",
        )
    )
    if not mapped_edges.is_empty():
        np.add.at(
            matrix,
            (
                mapped_edges["source_position"].to_numpy(),
                mapped_edges["target_position"].to_numpy(),
            ),
            mapped_edges["edge_weight"].to_numpy(),
        )
    if not np.any(matrix):
        return
    vmax = float(np.nanmax(matrix)) if np.any(matrix > 0.0) else 1.0
    if vmax <= 0:
        vmax = 1.0
    fig_size = max(8.0, min(14.0, 0.38 * len(ids) + 4.0))
    fig, ax = plt.subplots(figsize=_figsize_17_6(fig_size), dpi=140)
    image = ax.imshow(matrix, aspect="equal", cmap="magma", vmin=0.0, vmax=vmax)
    ax.set_title("Full Cross-Asset Transmission Matrix")
    ax.set_xlabel("target / receiver")
    ax.set_ylabel("source / transmitter")
    tick_positions, tick_labels = _sparse_axis_ticks(labels, max_ticks=72)
    ax.set_xticks(tick_positions, tick_labels, rotation=90, fontsize=6)
    ax.set_yticks(tick_positions, tick_labels, fontsize=6)
    ax.tick_params(length=0)
    if len(ids) <= 128:
        ax.set_xticks(np.arange(-0.5, len(ids), 1), minor=True)
        ax.set_yticks(np.arange(-0.5, len(ids), 1), minor=True)
        ax.grid(which="minor", color="white", linestyle="-", linewidth=0.35, alpha=0.35)
    fig.colorbar(image, ax=ax, shrink=0.78, label="edge weight")
    ax.text(
        0.0,
        -0.16,
        "Rows send influence, columns receive influence. This matrix shows the full selected graph without edge crossings.",
        transform=ax.transAxes,
        fontsize=8,
        color="#4b5563",
        ha="left",
        va="top",
    )
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _plot_graph_self_influence(
    path: Path,
    graph_edges: pl.DataFrame,
) -> None:
    if graph_edges.is_empty():
        return
    self_edges = graph_edges.filter(pl.col("source_index") == pl.col("target_index")).sort(
        ["edge_weight", "source_index"],
        descending=[True, False],
    )
    if self_edges.is_empty():
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    data = self_edges
    labels = data["source_symbol"].cast(pl.String).to_list()
    weights = data["edge_weight"].fill_null(0.0).to_numpy().astype(np.float64, copy=False)
    x = np.arange(data.height)
    fig, ax = plt.subplots(figsize=_figsize_17_6(10.0), dpi=180)
    ax.plot(x, weights, color="#4777b3", linewidth=0.9)
    ax.fill_between(x, weights, color="#4777b3", alpha=0.18)
    tick_positions, tick_labels = _sparse_axis_ticks(labels, max_ticks=72)
    ax.set_xticks(tick_positions, tick_labels, rotation=90, fontsize=6)
    ax.set_xlim(-0.5, max(0.5, data.height - 0.5))
    ax.set_ylabel("self-loop edge weight")
    ax.set_xlabel("every graph node, ordered by self influence")
    ax.set_title(f"Complete Cross-Asset Graph Self Influence ({data.height} nodes)")
    ax.text(
        0.99,
        0.02,
        "Self-loops are separated from graph_topology.png; every self edge is included here.",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=8,
        color="#4b5563",
    )
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _normalize_matrix(matrix: np.ndarray) -> np.ndarray:
    matrix = np.nan_to_num(np.asarray(matrix, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    denom = float(np.nanmax(np.abs(matrix))) if matrix.size else 0.0
    return (np.abs(matrix) / denom).astype(np.float32) if denom > 0 else np.zeros_like(matrix, dtype=np.float32)


def _resolve_graph_backend(settings: CrossAssetTransmissionSettings) -> tuple[str, list[str]]:
    warnings_out: list[str] = []
    raw = os.environ.get("STOCKAGENT_CROSS_ASSET_GRAPH_BACKEND", settings.graph_backend)
    backend = str(raw).strip().lower()
    if backend not in _GRAPH_BACKENDS:
        warnings_out.append(f"Invalid cross-asset graph backend {raw!r}; using cugraph.")
        backend = "cugraph"
    return backend, warnings_out


def _resolve_graph_min_edges(settings: CrossAssetTransmissionSettings) -> int:
    raw = os.environ.get(
        "STOCKAGENT_CROSS_ASSET_GRAPH_BENCHMARK_MIN_EDGES",
        settings.graph_benchmark_min_edges,
    )
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return max(0, int(settings.graph_benchmark_min_edges))


def _sort_edges_polars(edges: pl.DataFrame) -> pl.DataFrame:
    if edges.is_empty():
        return edges
    return edges.sort(_GRAPH_EDGE_SORT_COLUMNS, descending=_GRAPH_EDGE_SORT_DESCENDING)


def _summary_by_polars(edges: pl.DataFrame, key: str) -> pl.DataFrame:
    if edges.is_empty():
        return pl.DataFrame()
    return edges.group_by(key).agg(pl.col("validated_transmission").sum()).sort(key)


def _process_edges_polars(edges: pl.DataFrame) -> _GraphProcessingResult:
    start = time.perf_counter()
    sorted_edges = _sort_edges_polars(edges)
    source_summary = _summary_by_polars(sorted_edges, "source_symbol")
    target_summary = _summary_by_polars(sorted_edges, "target_symbol")
    elapsed_s = float(time.perf_counter() - start)
    return _GraphProcessingResult(
        backend="polars",
        edges=sorted_edges,
        source_summary=source_summary,
        target_summary=target_summary,
        node_metrics=pl.DataFrame(),
        benchmark={"elapsed_s": elapsed_s},
    )


def _cudf_to_polars(frame: Any) -> pl.DataFrame:
    if frame is None:
        return pl.DataFrame()
    if isinstance(frame, pl.DataFrame):
        return frame
    if hasattr(frame, "to_arrow"):
        return pl.from_arrow(frame.to_arrow())
    raise TypeError(f"Unsupported cuDF conversion source: {type(frame).__name__}")


def _polars_to_cudf(frame: pl.DataFrame) -> Any:
    import cudf  # type: ignore[import-not-found]

    return cudf.DataFrame.from_arrow(frame.to_arrow())


def _cugraph_metric_to_polars(metric: Any, *, rename: dict[str, str]) -> pl.DataFrame:
    frame = _cudf_to_polars(metric)
    return frame.rename({source: target for source, target in rename.items() if source in frame.columns})


def _process_edges_cugraph(edges: pl.DataFrame) -> _GraphProcessingResult:
    start = time.perf_counter()
    import cugraph  # type: ignore[import-not-found]

    gdf = _polars_to_cudf(edges)
    sorted_gdf = gdf.sort_values(_GRAPH_EDGE_SORT_COLUMNS, ascending=[False, True, True, True])
    source_summary_gdf = (
        gdf.groupby("source_symbol")["validated_transmission"]
        .sum()
        .reset_index()
        .sort_values("source_symbol")
    )
    target_summary_gdf = (
        gdf.groupby("target_symbol")["validated_transmission"]
        .sum()
        .reset_index()
        .sort_values("target_symbol")
    )

    graph_edges = (
        gdf.groupby(["source_index", "target_index"])["validated_transmission"]
        .sum()
        .reset_index()
    )
    try:
        graph = cugraph.Graph(directed=True, store_transposed=True)
    except TypeError:
        graph = cugraph.Graph(directed=True)
    graph.from_cudf_edgelist(
        graph_edges,
        source="source_index",
        destination="target_index",
        edge_attr="validated_transmission",
        renumber=True,
    )

    pagerank_error: str | None = None
    pagerank_frame = pl.DataFrame({"symbol_index": [], "pagerank": []}, schema={"symbol_index": pl.Int64, "pagerank": pl.Float64})
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=r".*Pagerank expects.*", category=UserWarning)
            pagerank_frame = _cugraph_metric_to_polars(
                cugraph.pagerank(graph),
                rename={"vertex": "symbol_index"},
            )
    except Exception as exc:
        pagerank_error = f"{type(exc).__name__}: {exc}"

    source_degree = _cudf_to_polars(
        graph_edges.groupby("source_index")["validated_transmission"]
        .sum()
        .reset_index()
        .rename(columns={"source_index": "symbol_index", "validated_transmission": "weighted_out_degree"})
    )
    target_degree = _cudf_to_polars(
        graph_edges.groupby("target_index")["validated_transmission"]
        .sum()
        .reset_index()
        .rename(columns={"target_index": "symbol_index", "validated_transmission": "weighted_in_degree"})
    )
    symbol_lookup: dict[int, str] = {}
    for row in edges.select(["source_index", "source_symbol"]).unique().to_dicts():
        symbol_lookup[int(row["source_index"])] = str(row["source_symbol"])
    for row in edges.select(["target_index", "target_symbol"]).unique().to_dicts():
        symbol_lookup[int(row["target_index"])] = str(row["target_symbol"])
    node_metrics = pl.DataFrame(
        [{"symbol_index": idx, "symbol": symbol_lookup[idx]} for idx in sorted(symbol_lookup)],
        schema={"symbol_index": pl.Int64, "symbol": pl.String},
    )
    node_metrics = (
        node_metrics.join(source_degree, on="symbol_index", how="left")
        .join(target_degree, on="symbol_index", how="left")
        .join(pagerank_frame, on="symbol_index", how="left")
    )
    for column in ("weighted_out_degree", "weighted_in_degree", "pagerank"):
        if column not in node_metrics.columns:
            node_metrics = node_metrics.with_columns(pl.lit(0.0).alias(column))
    node_metrics = node_metrics.with_columns(
        pl.col("weighted_out_degree").fill_null(0.0),
        pl.col("weighted_in_degree").fill_null(0.0),
        pl.col("pagerank").fill_null(0.0),
    ).sort("symbol_index")

    elapsed_s = float(time.perf_counter() - start)
    benchmark: dict[str, Any] = {
        "elapsed_s": elapsed_s,
        "graph_vertices": int(graph.number_of_vertices()),
        "graph_edges": int(graph.number_of_edges()),
    }
    if pagerank_error is not None:
        benchmark["pagerank_error"] = pagerank_error
    return _GraphProcessingResult(
        backend="cugraph",
        edges=_cudf_to_polars(sorted_gdf),
        source_summary=_cudf_to_polars(source_summary_gdf),
        target_summary=_cudf_to_polars(target_summary_gdf),
        node_metrics=node_metrics,
        benchmark=benchmark,
    )


def _frames_match_on_value(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    keys: list[str],
    value: str,
    rtol: float = 1e-6,
    atol: float = 1e-9,
) -> tuple[bool, str]:
    if left.height != right.height:
        return False, f"height mismatch: {left.height} != {right.height}"
    if left.is_empty() and right.is_empty():
        return True, "ok"
    left_sorted = left.select(keys + [value]).sort(keys)
    right_sorted = right.select(keys + [value]).sort(keys)
    if left_sorted.select(keys).to_dicts() != right_sorted.select(keys).to_dicts():
        return False, "key mismatch"
    left_values = left_sorted[value].to_numpy().astype(np.float64, copy=False)
    right_values = right_sorted[value].to_numpy().astype(np.float64, copy=False)
    if not np.allclose(left_values, right_values, rtol=rtol, atol=atol, equal_nan=True):
        max_abs = float(np.max(np.abs(left_values - right_values))) if left_values.size else 0.0
        return False, f"value mismatch: max_abs={max_abs:.6g}"
    return True, "ok"


def _validate_graph_outputs(polars_result: _GraphProcessingResult, cugraph_result: _GraphProcessingResult) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    ok, message = _frames_match_on_value(
        polars_result.edges,
        cugraph_result.edges,
        keys=_GRAPH_EDGE_KEY_COLUMNS,
        value="validated_transmission",
    )
    checks["edges"] = {"ok": ok, "message": message}
    ok, message = _frames_match_on_value(
        polars_result.source_summary,
        cugraph_result.source_summary,
        keys=["source_symbol"],
        value="validated_transmission",
    )
    checks["source_summary"] = {"ok": ok, "message": message}
    ok, message = _frames_match_on_value(
        polars_result.target_summary,
        cugraph_result.target_summary,
        keys=["target_symbol"],
        value="validated_transmission",
    )
    checks["target_summary"] = {"ok": ok, "message": message}
    ok = all(bool(item["ok"]) for item in checks.values())
    return {"ok": ok, "checks": checks}


def _process_cross_asset_graph_edges(
    edges: pl.DataFrame,
    settings: CrossAssetTransmissionSettings,
) -> _GraphProcessingResult:
    polars_result = _process_edges_polars(edges)
    backend, backend_warnings = _resolve_graph_backend(settings)
    min_edges = _resolve_graph_min_edges(settings)
    benchmark: dict[str, Any] = {
        "requested_backend": backend,
        "selected_backend": "polars",
        "selection_reason": "polars_baseline",
        "edge_count": int(edges.height),
        "benchmark_min_edges": int(min_edges),
        "backends": {"polars": polars_result.benchmark},
        "warnings": backend_warnings,
    }
    selected = polars_result

    if edges.is_empty():
        benchmark["selection_reason"] = "empty_edges"
        selected.benchmark = benchmark
        return selected
    if backend == "polars":
        benchmark["selection_reason"] = "backend_polars"
        selected.benchmark = benchmark
        return selected
    if backend == "auto" and int(edges.height) < min_edges:
        benchmark["selection_reason"] = "below_min_edges"
        selected.benchmark = benchmark
        return selected

    try:
        cugraph_result = _process_edges_cugraph(edges)
    except Exception as exc:
        benchmark["selection_reason"] = "cugraph_failed"
        benchmark["backends"]["cugraph"] = {
            "available": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if backend == "cugraph":
            raise RuntimeError(f"cuGraph graph backend was requested but failed: {type(exc).__name__}: {exc}") from exc
        selected.benchmark = benchmark
        return selected

    benchmark["backends"]["cugraph"] = cugraph_result.benchmark | {"available": True}
    validation = _validate_graph_outputs(polars_result, cugraph_result)
    benchmark["validation"] = validation
    if not bool(validation["ok"]):
        benchmark["selection_reason"] = "validation_failed"
        if backend == "cugraph":
            raise RuntimeError(f"cuGraph graph backend validation failed: {validation}")
        benchmark["warnings"].append("cuGraph output did not match Polars baseline; using Polars output.")
        selected.benchmark = benchmark
        return selected

    polars_elapsed = float(polars_result.benchmark.get("elapsed_s", math.inf))
    cugraph_elapsed = float(cugraph_result.benchmark.get("elapsed_s", math.inf))
    if cugraph_elapsed > 0 and math.isfinite(polars_elapsed):
        benchmark["polars_to_cugraph_elapsed_ratio"] = float(polars_elapsed / cugraph_elapsed)
    if backend == "cugraph":
        selected = cugraph_result
        benchmark["selection_reason"] = "backend_cugraph"
    elif cugraph_elapsed < polars_elapsed:
        selected = cugraph_result
        benchmark["selection_reason"] = "cugraph_faster"
    else:
        benchmark["selection_reason"] = "polars_faster_or_equal"
    benchmark["selected_backend"] = selected.backend
    selected.benchmark = benchmark
    return selected


def _aggregate_graph_edges(edges: pl.DataFrame) -> pl.DataFrame:
    if edges.is_empty():
        return pl.DataFrame()
    dominant_shock = (
        edges.sort("validated_transmission", descending=True)
        .group_by(["source_index", "target_index"])
        .agg(pl.first("shock").alias("dominant_shock"))
    )
    graph_edges = (
        edges.group_by(["source_index", "target_index"])
        .agg(
            pl.first("source_symbol").alias("source_symbol"),
            pl.first("target_symbol").alias("target_symbol"),
            pl.col("validated_transmission").sum().alias("edge_weight"),
            pl.col("validated_transmission").mean().alias("edge_weight_mean"),
            pl.col("validated_transmission").max().alias("edge_weight_max"),
            pl.len().alias("shock_count"),
        )
        .join(dominant_shock, on=["source_index", "target_index"], how="left")
        .sort(["edge_weight", "source_index", "target_index"], descending=[True, False, False])
    )
    return graph_edges


def _graph_base_node_frame(graph_edges: pl.DataFrame) -> pl.DataFrame:
    if graph_edges.is_empty():
        return pl.DataFrame()
    source_nodes = graph_edges.select(
        pl.col("source_index").alias("symbol_index"),
        pl.col("source_symbol").alias("symbol"),
    )
    target_nodes = graph_edges.select(
        pl.col("target_index").alias("symbol_index"),
        pl.col("target_symbol").alias("symbol"),
    )
    return pl.concat([source_nodes, target_nodes], how="vertical").unique("symbol_index").sort("symbol_index")


def _assign_graph_roles(node_metrics: pl.DataFrame) -> pl.DataFrame:
    if node_metrics.is_empty():
        return node_metrics
    frame = node_metrics.with_columns(
        (pl.col("weighted_out_degree").fill_null(0.0) - pl.col("weighted_in_degree").fill_null(0.0)).alias(
            "net_transmitter_score"
        )
    )
    numeric_columns = [
        "weighted_out_degree",
        "weighted_in_degree",
        "pagerank",
        "hub_score",
        "authority_score",
        "betweenness_centrality",
    ]
    thresholds: dict[str, float] = {}
    for column in numeric_columns:
        if column in frame.columns:
            thresholds[column] = float(
                frame.select(pl.col(column).fill_null(0.0).fill_nan(0.0).quantile(0.75)).item()
            )
    roles: list[str] = []
    for row in frame.iter_rows(named=True):
        out_degree = float(row.get("weighted_out_degree", 0.0) or 0.0)
        in_degree = float(row.get("weighted_in_degree", 0.0) or 0.0)
        pagerank = float(row.get("pagerank", 0.0) or 0.0)
        hub = float(row.get("hub_score", 0.0) or 0.0)
        authority = float(row.get("authority_score", 0.0) or 0.0)
        betweenness = float(row.get("betweenness_centrality", 0.0) or 0.0)
        if betweenness >= thresholds.get("betweenness_centrality", math.inf) and betweenness > 0:
            roles.append("bridge")
        elif pagerank >= thresholds.get("pagerank", math.inf) and pagerank > 0:
            roles.append("systemic_receiver")
        elif out_degree >= thresholds.get("weighted_out_degree", math.inf) and hub >= thresholds.get("hub_score", -math.inf):
            roles.append("transmitter")
        elif in_degree >= thresholds.get("weighted_in_degree", math.inf) and authority >= thresholds.get("authority_score", -math.inf):
            roles.append("receiver")
        elif out_degree > in_degree:
            roles.append("net_source")
        elif in_degree > out_degree:
            roles.append("net_sink")
        else:
            roles.append("balanced")
    return frame.with_columns(pl.Series("primary_role", roles))


def _build_polars_graph_explainability(
    edges: pl.DataFrame,
    *,
    reason: str = "polars_fallback",
    preaggregated: bool = False,
) -> _GraphExplainabilityResult:
    start = time.perf_counter()
    graph_edges = edges if preaggregated else _aggregate_graph_edges(edges)
    nodes = _graph_base_node_frame(graph_edges)
    if graph_edges.is_empty() or nodes.is_empty():
        summary = {
            "enabled": True,
            "backend": "polars",
            "reason": "empty_edges",
            "elapsed_s": float(time.perf_counter() - start),
            "algorithms": ["weighted_degree"],
            "skipped_algorithms": [],
        }
        return _GraphExplainabilityResult("polars", graph_edges, nodes, pl.DataFrame(), pl.DataFrame(), summary)
    source_degree = (
        graph_edges.group_by("source_index")
        .agg(pl.col("edge_weight").sum().alias("weighted_out_degree"))
        .rename({"source_index": "symbol_index"})
    )
    target_degree = (
        graph_edges.group_by("target_index")
        .agg(pl.col("edge_weight").sum().alias("weighted_in_degree"))
        .rename({"target_index": "symbol_index"})
    )
    node_metrics = (
        nodes.join(source_degree, on="symbol_index", how="left")
        .join(target_degree, on="symbol_index", how="left")
        .with_columns(
            pl.col("weighted_out_degree").fill_null(0.0),
            pl.col("weighted_in_degree").fill_null(0.0),
        )
    )
    total_out = float(node_metrics["weighted_out_degree"].sum()) or 1.0
    total_in = float(node_metrics["weighted_in_degree"].sum()) or 1.0
    node_metrics = node_metrics.with_columns(
        (pl.col("weighted_out_degree") / total_out).alias("hub_score"),
        (pl.col("weighted_in_degree") / total_in).alias("authority_score"),
        ((pl.col("weighted_out_degree") + pl.col("weighted_in_degree")) / (total_out + total_in)).alias("pagerank"),
        pl.lit(0).alias("community_id"),
    )
    node_metrics = _assign_graph_roles(node_metrics).sort("pagerank", descending=True)
    community_edges = pl.DataFrame(
        [{"source_community": 0, "target_community": 0, "edge_weight": float(graph_edges["edge_weight"].sum()), "edge_count": int(graph_edges.height)}]
    )
    community_summary = pl.DataFrame(
        [
            {
                "community_id": 0,
                "node_count": int(node_metrics.height),
                "total_pagerank": float(node_metrics["pagerank"].sum()),
                "total_hub_score": float(node_metrics["hub_score"].sum()),
                "total_authority_score": float(node_metrics["authority_score"].sum()),
                "weighted_out_degree": float(node_metrics["weighted_out_degree"].sum()),
                "weighted_in_degree": float(node_metrics["weighted_in_degree"].sum()),
                "symbols": ", ".join(node_metrics["symbol"].cast(pl.String).to_list()),
            }
        ]
    )
    summary = {
        "enabled": True,
        "backend": "polars",
        "reason": reason,
        "graph_vertices": int(node_metrics.height),
        "graph_edges": int(graph_edges.height),
        "algorithms": ["weighted_degree"],
        "skipped_algorithms": ["pagerank", "hits", "eigenvector_centrality", "louvain", "strongly_connected_components"],
        "elapsed_s": float(time.perf_counter() - start),
    }
    return _GraphExplainabilityResult("polars", graph_edges, node_metrics, community_summary, community_edges, summary)


def _from_cudf_edgelist(graph: Any, frame: Any, *, store_transposed: bool | None = None) -> None:
    kwargs: dict[str, Any] = {
        "source": "source_index",
        "destination": "target_index",
        "edge_attr": "edge_weight",
        "renumber": True,
    }
    if store_transposed is not None:
        kwargs["store_transposed"] = bool(store_transposed)
    try:
        graph.from_cudf_edgelist(frame, **kwargs)
    except TypeError:
        kwargs.pop("store_transposed", None)
        graph.from_cudf_edgelist(frame, **kwargs)


def _merge_metric_frame(base: pl.DataFrame, metric: Any, *, rename: dict[str, str]) -> pl.DataFrame:
    metric_frame = _cugraph_metric_to_polars(metric, rename=rename)
    return base.join(metric_frame, on="symbol_index", how="left")


def _build_cugraph_graph_explainability(
    edges: pl.DataFrame,
    settings: CrossAssetTransmissionSettings,
    *,
    preaggregated: bool = False,
) -> _GraphExplainabilityResult:
    start = time.perf_counter()
    import cugraph  # type: ignore[import-not-found]

    graph_edges = edges if preaggregated else _aggregate_graph_edges(edges)
    nodes = _graph_base_node_frame(graph_edges)
    if graph_edges.is_empty() or nodes.is_empty():
        summary = {
            "enabled": True,
            "backend": "cugraph",
            "reason": "empty_edges",
            "elapsed_s": float(time.perf_counter() - start),
            "algorithms": [],
            "skipped_algorithms": [],
        }
        return _GraphExplainabilityResult("cugraph", graph_edges, nodes, pl.DataFrame(), pl.DataFrame(), summary)

    graph_gdf = _polars_to_cudf(graph_edges.select(["source_index", "target_index", "edge_weight"]))
    directed = cugraph.Graph(directed=True)
    _from_cudf_edgelist(directed, graph_gdf, store_transposed=True)
    undirected = cugraph.Graph(directed=False)
    _from_cudf_edgelist(undirected, graph_gdf, store_transposed=False)

    source_degree = (
        graph_edges.group_by("source_index")
        .agg(pl.col("edge_weight").sum().alias("weighted_out_degree"))
        .rename({"source_index": "symbol_index"})
    )
    target_degree = (
        graph_edges.group_by("target_index")
        .agg(pl.col("edge_weight").sum().alias("weighted_in_degree"))
        .rename({"target_index": "symbol_index"})
    )
    node_metrics = nodes.join(source_degree, on="symbol_index", how="left").join(target_degree, on="symbol_index", how="left")
    source_vertices = set(int(value) for value in graph_edges["source_index"].unique().to_list())
    target_vertices = set(int(value) for value in graph_edges["target_index"].unique().to_list())
    complete_cartesian_graph = (
        source_vertices == target_vertices
        and len(source_vertices) == int(nodes.height)
        and int(graph_edges.height) == int(nodes.height) * int(nodes.height)
    )

    algorithms: list[str] = []
    skipped: list[dict[str, str]] = []
    algorithm_progress = tqdm(
        total=9,
        desc="cuGraph algorithms",
        unit="algorithm",
        leave=False,
        disable=not bool(settings.progress_enabled),
    )

    def add_metric(name: str, fn: Any, rename: dict[str, str]) -> None:
        nonlocal node_metrics
        algorithm_progress.set_postfix(algorithm=name, refresh=True)
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=r".*expects the 'store_transposed'.*", category=UserWarning)
                warnings.filterwarnings("ignore", category=PendingDeprecationWarning)
                metric = fn()
            node_metrics = _merge_metric_frame(node_metrics, metric, rename=rename)
            algorithms.append(name)
        except Exception as exc:
            skipped.append({"algorithm": name, "reason": f"{type(exc).__name__}: {exc}"})
        finally:
            algorithm_progress.update(1)

    add_metric("pagerank", lambda: cugraph.pagerank(directed), {"vertex": "symbol_index"})
    add_metric("hits", lambda: cugraph.hits(directed), {"vertex": "symbol_index", "hubs": "hub_score", "authorities": "authority_score"})
    add_metric(
        "eigenvector_centrality",
        lambda: cugraph.eigenvector_centrality(directed),
        {"vertex": "symbol_index"},
    )
    if complete_cartesian_graph:
        # The exhaustive all-source/all-target contract produces a complete
        # directed topology.  Several unweighted graph algorithms then have
        # exact closed forms; launching O(V+E) / O(VE) kernels cannot add
        # information.  Weighted PageRank/HITS/eigenvector/Louvain still run.
        component_label = min(source_vertices) if source_vertices else 0
        vertex_count = int(nodes.height)
        triangle_count = max(0, (vertex_count - 1) * (vertex_count - 2) // 2)
        node_metrics = node_metrics.with_columns(
            pl.lit(component_label).alias("strong_component_id"),
            pl.lit(component_label).alias("weak_component_id"),
            pl.lit(max(0, vertex_count - 1)).alias("core_number"),
            pl.lit(triangle_count).alias("triangle_count"),
        )
        for name in (
            "strongly_connected_components_closed_form",
            "weakly_connected_components_closed_form",
            "core_number_closed_form",
            "triangle_count_closed_form",
        ):
            algorithms.append(name)
            algorithm_progress.set_postfix(algorithm=name, refresh=False)
            algorithm_progress.update(1)
    else:
        add_metric(
            "strongly_connected_components",
            lambda: cugraph.strongly_connected_components(directed),
            {"vertex": "symbol_index", "labels": "strong_component_id"},
        )
        add_metric(
            "weakly_connected_components",
            lambda: cugraph.weakly_connected_components(undirected),
            {"vertex": "symbol_index", "labels": "weak_component_id"},
        )
        add_metric("core_number", lambda: cugraph.core_number(undirected), {"vertex": "symbol_index"})
        add_metric("triangle_count", lambda: cugraph.triangle_count(undirected), {"vertex": "symbol_index", "counts": "triangle_count"})

    modularity: float | None = None
    algorithm_progress.set_postfix(algorithm="louvain_or_leiden", refresh=True)
    try:
        community_frame, modularity = cugraph.louvain(undirected)
        node_metrics = _merge_metric_frame(
            node_metrics,
            community_frame,
            rename={"vertex": "symbol_index", "partition": "community_id"},
        )
        algorithms.append("louvain")
    except Exception as exc:
        skipped.append({"algorithm": "louvain", "reason": f"{type(exc).__name__}: {exc}"})
        try:
            community_frame, modularity = cugraph.leiden(undirected)
            node_metrics = _merge_metric_frame(
                node_metrics,
                community_frame,
                rename={"vertex": "symbol_index", "partition": "community_id"},
            )
            algorithms.append("leiden")
        except Exception as leiden_exc:
            skipped.append({"algorithm": "leiden", "reason": f"{type(leiden_exc).__name__}: {leiden_exc}"})
    algorithm_progress.update(1)

    graph_vertex_count = int(nodes.height)
    graph_edge_count = int(graph_edges.height)
    if complete_cartesian_graph:
        node_metrics = node_metrics.with_columns(pl.lit(0.0).alias("betweenness_centrality"))
        algorithms.append("betweenness_centrality_closed_form")
        algorithm_progress.set_postfix(algorithm="betweenness_centrality_closed_form", refresh=False)
        algorithm_progress.update(1)
    else:
        add_metric(
            "betweenness_centrality",
            lambda: cugraph.betweenness_centrality(directed),
            {"vertex": "symbol_index"},
        )
    algorithm_progress.close()

    for column in (
        "weighted_out_degree",
        "weighted_in_degree",
        "pagerank",
        "hub_score",
        "authority_score",
        "eigenvector_centrality",
        "betweenness_centrality",
        "core_number",
        "triangle_count",
    ):
        if column not in node_metrics.columns:
            node_metrics = node_metrics.with_columns(pl.lit(0.0).alias(column))
    node_metrics = node_metrics.with_columns(
        pl.col(column).fill_null(0.0).fill_nan(0.0).alias(column)
        for column in (
            "weighted_out_degree",
            "weighted_in_degree",
            "pagerank",
            "hub_score",
            "authority_score",
            "eigenvector_centrality",
            "betweenness_centrality",
            "core_number",
            "triangle_count",
        )
    )
    if "community_id" not in node_metrics.columns:
        node_metrics = node_metrics.with_columns(pl.lit(0).alias("community_id"))
    node_metrics = node_metrics.with_columns(pl.col("community_id").fill_null(0).cast(pl.Int64))
    for column in ("strong_component_id", "weak_component_id"):
        if column not in node_metrics.columns:
            node_metrics = node_metrics.with_columns(pl.lit(0).alias(column))
    node_metrics = node_metrics.with_columns(
        pl.col("strong_component_id").fill_null(0).cast(pl.Int64),
        pl.col("weak_component_id").fill_null(0).cast(pl.Int64),
    )

    node_metrics = _assign_graph_roles(node_metrics).sort("pagerank", descending=True)
    src_comm = node_metrics.select(
        pl.col("symbol_index").alias("source_index"),
        pl.col("community_id").alias("source_community"),
    )
    dst_comm = node_metrics.select(
        pl.col("symbol_index").alias("target_index"),
        pl.col("community_id").alias("target_community"),
    )
    community_edges = (
        graph_edges.join(src_comm, on="source_index", how="left")
        .join(dst_comm, on="target_index", how="left")
        .group_by(["source_community", "target_community"])
        .agg(pl.col("edge_weight").sum(), pl.len().alias("edge_count"))
        .sort("edge_weight", descending=True)
    )
    community_rows: list[dict[str, Any]] = []
    for community_id in sorted(int(value) for value in node_metrics["community_id"].drop_nulls().unique().to_list()):
        members = node_metrics.filter(pl.col("community_id") == community_id)
        outgoing = community_edges.filter(pl.col("source_community") == community_id)
        incoming = community_edges.filter(pl.col("target_community") == community_id)
        internal = community_edges.filter(
            (pl.col("source_community") == community_id) & (pl.col("target_community") == community_id)
        )
        community_rows.append(
            {
                "community_id": int(community_id),
                "node_count": int(members.height),
                "total_pagerank": float(members["pagerank"].sum()) if "pagerank" in members.columns else 0.0,
                "total_hub_score": float(members["hub_score"].sum()) if "hub_score" in members.columns else 0.0,
                "total_authority_score": float(members["authority_score"].sum()) if "authority_score" in members.columns else 0.0,
                "weighted_out_degree": float(members["weighted_out_degree"].sum()),
                "weighted_in_degree": float(members["weighted_in_degree"].sum()),
                "external_out_weight": float(outgoing.filter(pl.col("target_community") != community_id)["edge_weight"].sum()),
                "external_in_weight": float(incoming.filter(pl.col("source_community") != community_id)["edge_weight"].sum()),
                "internal_weight": float(internal["edge_weight"].sum()) if not internal.is_empty() else 0.0,
                "symbols": ", ".join(members.sort("pagerank", descending=True)["symbol"].cast(pl.String).to_list()),
            }
        )
    community_summary = pl.DataFrame(community_rows).sort("total_pagerank", descending=True) if community_rows else pl.DataFrame()
    summary = {
        "enabled": True,
        "backend": "cugraph",
        "graph_vertices": graph_vertex_count,
        "graph_edges": graph_edge_count,
        "complete_cartesian_graph": bool(complete_cartesian_graph),
        "algorithms": algorithms,
        "skipped_algorithms": skipped,
        "modularity": float(modularity) if modularity is not None else None,
        "elapsed_s": float(time.perf_counter() - start),
    }
    return _GraphExplainabilityResult("cugraph", graph_edges, node_metrics, community_summary, community_edges, summary)


def _build_graph_explainability(
    edges: pl.DataFrame,
    settings: CrossAssetTransmissionSettings,
    *,
    preaggregated: bool = False,
) -> _GraphExplainabilityResult:
    if not bool(settings.graph_explainability):
        summary = {"enabled": False, "backend": "disabled", "reason": "settings"}
        return _GraphExplainabilityResult("disabled", pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), summary)
    backend, backend_warnings = _resolve_graph_backend(settings)
    if backend == "polars":
        result = _build_polars_graph_explainability(
            edges,
            reason="backend_polars",
            preaggregated=preaggregated,
        )
        result.summary["warnings"] = backend_warnings
        return result
    try:
        result = _build_cugraph_graph_explainability(
            edges,
            settings,
            preaggregated=preaggregated,
        )
        result.summary["warnings"] = backend_warnings
        return result
    except Exception as exc:
        if backend == "cugraph":
            raise RuntimeError(f"cuGraph graph explainability was requested but failed: {type(exc).__name__}: {exc}") from exc
        result = _build_polars_graph_explainability(
            edges,
            reason="cugraph_failed",
            preaggregated=preaggregated,
        )
        result.summary["warnings"] = backend_warnings + [f"cuGraph graph explainability failed: {type(exc).__name__}: {exc}"]
        return result


def abstract_cross_asset_transmission(
    model: nn.Module,
    batch: Any,
    *,
    feature_names: list[str],
    symbols: list[str],
    dates: list[str],
    output_dir: Path,
    settings: CrossAssetTransmissionSettings | None = None,
    device: torch.device | None = None,
) -> dict[str, Any]:
    settings = settings or CrossAssetTransmissionSettings()
    destination = Path(output_dir) / MODULE_NAME
    tables_dir = destination / "tables"
    matrices_dir = destination / "matrices"
    plots_dir = destination / "plots"
    for path in (tables_dir, matrices_dir, plots_dir):
        path.mkdir(parents=True, exist_ok=True)
    if not bool(settings.enabled):
        summary = {"enabled": False, "module": MODULE_NAME}
        (destination / "abstract_cross_asset_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary

    total_start = time.perf_counter()
    timing: dict[str, Any] = {"per_shock_s": {}}
    pipeline_progress = tqdm(
        total=5,
        desc="Cross-asset pipeline",
        unit="stage",
        disable=not bool(settings.progress_enabled),
    )
    pipeline_progress.set_postfix(stage="base_forward", refresh=True)
    device = device or next(model.parameters()).device
    n_rows, lookback, n_symbols, n_features = _cross_asset_batch_shape(batch)
    if n_rows <= 0:
        raise ValueError("cross-asset explainability requires at least one row")
    warnings: list[str] = []
    row_chunk_size, row_chunk_info = _auto_row_chunk_size(n_rows, n_symbols, settings)
    if row_chunk_size < n_rows:
        warnings.append(
            f"Cross-asset transmission used row microbatching: row_chunk_size={row_chunk_size}, rows={n_rows}."
        )
    force_single_source_chunk = int(n_symbols) >= 10_000
    if force_single_source_chunk and int(settings.source_chunk_size) > 1:
        warnings.append(
            "Cross-asset transmission capped source_chunk_size to 1 for a large stock universe to avoid repeated-input VRAM blowups."
        )

    was_training = model.training
    model.eval()
    embedded_api = _embedded_explainability_api(model)
    stock_embedding_api = _stock_embedding_explainability_api(model)
    if stock_embedding_api is not embedded_api or str(
        getattr(stock_embedding_api, "attention_mode", "")
    ).strip().lower() in {"full", "axial"}:
        stock_embedding_api = None
    weight_parts: list[torch.Tensor] = []
    score_parts: list[torch.Tensor] = []
    rank_parts: list[torch.Tensor] = []
    mask_parts: list[torch.Tensor] = []
    aux_parts: dict[str, list[torch.Tensor]] = {}
    attention_flow_sum: np.ndarray | None = None
    attention_rows_seen = 0
    attention_rows: list[dict[str, Any]] = []
    requested_attention_rows = int(settings.attention_capture_rows)
    attention_total_rows = (
        n_rows
        if requested_attention_rows <= 0
        else min(n_rows, requested_attention_rows)
    )
    attention_capture_supported = bool(
        settings.attention_flow
        and callable(getattr(model, "configure_attention_capture", None))
        and callable(getattr(model, "pop_attention_capture", None))
    )
    if bool(settings.attention_flow) and not attention_capture_supported:
        warnings.append("Model does not expose attention capture hooks.")
    attention_processing_s = 0.0
    feature_sum = torch.zeros(n_features, dtype=torch.float64, device=device)
    feature_sum_sq = torch.zeros(n_features, dtype=torch.float64, device=device)
    feature_count = 0
    base_forward_start = time.perf_counter()
    with torch.no_grad():
        for base_chunk_id, row_start in enumerate(tqdm(
            range(0, n_rows, row_chunk_size),
            total=math.ceil(n_rows / row_chunk_size),
            desc="Cross-asset base forward",
            unit="chunk",
            leave=False,
            disable=not bool(settings.progress_enabled),
        ), start=1):
            row_end = min(n_rows, row_start + row_chunk_size)
            x_cpu_row, mask_cpu_row, returns_cpu_row = _materialize_cross_asset_rows(
                batch,
                row_start,
                row_end,
                total_rows=n_rows,
                num_symbols=n_symbols,
            )
            mask_parts.append(mask_cpu_row)
            x_row = x_cpu_row.to(device=device, non_blocking=(device.type == "cuda"))
            mask_row = mask_cpu_row.to(device=device, non_blocking=(device.type == "cuda"))
            # Reduce in FP32 on the accelerator and accumulate only the small
            # feature vectors in FP64.  Converting the full [B,L,S,F] slab to
            # CPU FP64 was a major preprocessing bottleneck.
            feature_sum += x_row.sum(dim=(0, 1, 2), dtype=torch.float32).to(torch.float64)
            feature_sum_sq += x_row.square().sum(dim=(0, 1, 2), dtype=torch.float32).to(torch.float64)
            feature_count += int(x_row.numel() // max(1, n_features))
            capture_rows = (
                max(0, min(row_end, attention_total_rows) - row_start)
                if attention_capture_supported
                else 0
            )
            capture_enabled = False
            captures: list[dict[str, object]] = []
            if capture_rows > 0:
                try:
                    model.configure_attention_capture(
                        True,
                        max_rows=capture_rows,
                        max_elements=max(1, int(settings.attention_capture_max_elements)),
                    )
                    capture_enabled = True
                except Exception as exc:
                    warnings.append(
                        f"Attention capture setup failed: {type(exc).__name__}: {exc}"
                    )
            try:
                weights_row, scores_row, rank_row, _centered_row, aux_row = _forward_outputs(
                    model,
                    x_row,
                    mask_row,
                    return_aux=bool(settings.role_embedding or capture_enabled),
                )
            finally:
                if capture_enabled:
                    try:
                        captures = model.pop_attention_capture()
                    except Exception as exc:
                        warnings.append(
                            f"Attention capture collection failed: {type(exc).__name__}: {exc}"
                        )
                    try:
                        model.configure_attention_capture(False)
                    except Exception:
                        pass
            if captures:
                attention_process_start = time.perf_counter()
                chunk_flow, chunk_rows, attention_warnings = _compute_attention_flow_from_captures(
                    captures,
                    n_symbols=n_symbols,
                )
                for row in chunk_rows:
                    attention_rows.append(
                        {**row, "chunk_id": base_chunk_id, "rows": capture_rows}
                    )
                if chunk_flow is not None:
                    weighted = chunk_flow.astype(np.float64, copy=False) * float(capture_rows)
                    attention_flow_sum = (
                        weighted
                        if attention_flow_sum is None
                        else attention_flow_sum + weighted
                    )
                    attention_rows_seen += capture_rows
                warnings.extend(attention_warnings)
                attention_processing_s += float(time.perf_counter() - attention_process_start)
            weight_parts.append(weights_row.detach().cpu())
            score_parts.append(scores_row.detach().cpu())
            rank_parts.append(rank_row.detach().cpu())
            if bool(settings.role_embedding):
                for key, value in aux_row.items():
                    if torch.is_tensor(value):
                        aux_parts.setdefault(str(key), []).append(value.detach().cpu())
            del x_cpu_row, mask_cpu_row, returns_cpu_row
            del x_row, mask_row, weights_row, scores_row, rank_row, aux_row
    aux: dict[str, torch.Tensor] = {}
    for name, tensors in aux_parts.items():
        if tensors and all(
            tensor.ndim == tensors[0].ndim and tensor.shape[1:] == tensors[0].shape[1:]
            for tensor in tensors
        ):
            aux[name] = torch.cat(tensors, dim=0)
    mask_cpu = torch.cat(mask_parts, dim=0)
    base_weights = torch.cat(weight_parts, dim=0).masked_fill(~mask_cpu, 0.0)
    base_scores = torch.cat(score_parts, dim=0).masked_fill(~mask_cpu, 0.0)
    base_rank = torch.cat(rank_parts, dim=0)
    base_rank_pos = _rank_positions(base_rank, mask_cpu)
    timing["base_forward_s"] = float(time.perf_counter() - base_forward_start)
    pipeline_progress.update(1)
    pipeline_progress.set_postfix(stage="attention", refresh=True)
    source_idx, target_idx, importance = _select_symbols(
        base_weights,
        base_scores,
        mask_cpu,
        max_sources=settings.max_sources,
        max_targets=settings.max_targets,
    )
    source_symbols = [symbols[idx] if idx < len(symbols) else str(idx) for idx in source_idx]
    target_symbols = [symbols[idx] if idx < len(symbols) else str(idx) for idx in target_idx]
    if not source_idx or not target_idx:
        warnings.append("No active source/target symbols were available.")

    if feature_count > 1:
        variance = (
            feature_sum_sq - feature_sum.square() / float(feature_count)
        ) / float(feature_count - 1)
        feature_std = variance.clamp_min(0.0).sqrt().to(dtype=torch.float32).clamp_min(1e-6)
    else:
        feature_std = torch.full((n_features,), 1e-6, dtype=torch.float32, device=device)
    attention_flow = (
        (attention_flow_sum / float(attention_rows_seen)).astype(np.float32, copy=False)
        if attention_flow_sum is not None and attention_rows_seen > 0
        else None
    )
    timing["attention_s"] = float(attention_processing_s)
    timing["attention_fused_with_base"] = bool(settings.attention_flow)
    pipeline_progress.update(1)
    pipeline_progress.set_postfix(stage="shocks", refresh=True)
    if attention_flow is None:
        attention_selected = np.zeros((len(source_idx), len(target_idx)), dtype=np.float32)
    else:
        attention_selected = attention_flow[np.ix_(source_idx, target_idx)].astype(np.float32, copy=False)
    attention_frame = pl.DataFrame(attention_rows)
    _write_frame_csv_or_parquet(tables_dir / "attention_capture_summary.csv", attention_frame)
    if bool(settings.compact_artifacts):
        attention_path = matrices_dir / "attention_flow.npy"
        np.save(attention_path, attention_selected, allow_pickle=False)
        for stale_path in (
            matrices_dir / "attention_flow.csv",
            matrices_dir / "attention_flow.parquet",
        ):
            if stale_path.exists():
                stale_path.unlink()
    else:
        _write_matrix_csv(
            matrices_dir / "attention_flow.csv",
            attention_selected,
            source_symbols,
            target_symbols,
        )

    all_edges: list[pl.DataFrame] = []
    shock_summaries: list[dict[str, Any]] = []
    requested_shocks = tuple(str(shock).strip().lower() for shock in settings.shocks if str(shock).strip())
    initial_source_chunk_size = 1 if force_single_source_chunk else max(1, int(settings.source_chunk_size))
    shock_states: list[_ShockAccumulator] = []
    for shock in requested_shocks:
        feature_idx = _feature_indices_for_shock(feature_names, shock)
        if not feature_idx:
            warnings.append(f"{shock}: no matching features; skipped.")
            continue
        shock_states.append(
            _ShockAccumulator(
                shock=shock,
                feature_indices=feature_idx,
                buffers=_empty_metric_buffers(
                    len(source_idx),
                    len(target_idx),
                    device=device,
                ),
                row_weight_totals=torch.zeros(
                    len(source_idx),
                    dtype=torch.float32,
                    device=device,
                ),
                chunk_size=initial_source_chunk_size,
                compile_forward=bool(settings.counterfactual_compile and embedded_api is not None),
            )
        )

    selected_targets = torch.as_tensor(target_idx, device=device, dtype=torch.long)
    feature_std_device = feature_std.to(device=device, non_blocking=(device.type == "cuda"))
    shock_compute_start = time.perf_counter()
    shock_progress = tqdm(
        total=int(n_rows * len(source_idx) * len(shock_states)),
        desc="Cross-asset shocks",
        unit="source-row",
        disable=not bool(settings.progress_enabled),
    )
    for row_start in range(0, n_rows, row_chunk_size):
        row_end = min(n_rows, row_start + row_chunk_size)
        row_count = row_end - row_start
        x_cpu_row, mask_cpu_row, returns_cpu_row = _materialize_cross_asset_rows(
            batch,
            row_start,
            row_end,
            total_rows=n_rows,
            num_symbols=n_symbols,
        )
        x_row = x_cpu_row.to(device=device, non_blocking=(device.type == "cuda"))
        mask_row = mask_cpu_row.to(device=device, non_blocking=(device.type == "cuda"))
        returns_row = returns_cpu_row.to(device=device, non_blocking=(device.type == "cuda"))
        base_weights_row = base_weights[row_start:row_end].to(
            device=device, non_blocking=(device.type == "cuda")
        )
        base_scores_row = base_scores[row_start:row_end].to(
            device=device, non_blocking=(device.type == "cuda")
        )
        base_rank_pos_row = base_rank_pos[row_start:row_end].to(
            device=device, non_blocking=(device.type == "cuda")
        )
        with torch.no_grad():
            if embedded_api is not None:
                base_projected_row = embedded_api.project_features_for_explainability(x_row)
                base_embedded_row = embedded_api.embed_projected_for_explainability(base_projected_row)
                if stock_embedding_api is not None:
                    base_stock_embeddings_row = (
                        stock_embedding_api.temporal_stock_embeddings_for_explainability(
                            base_embedded_row,
                            mask_row,
                        )
                    )
                else:
                    base_stock_embeddings_row = None
            else:
                base_projected_row = None
                base_embedded_row = None
                base_stock_embeddings_row = None
        for state in shock_states:
            state_start = time.perf_counter()
            source_pos = 0
            # Keep one fixed compiled aggregate shape.  A ragged final row
            # chunk would otherwise create a second Inductor/CUDA-graph pool
            # (and the 8K probe already approached the 32 GiB device limit).
            compile_this_row = bool(
                state.compile_forward and row_count == row_chunk_size
            )
            while source_pos < len(source_idx):
                chunk_sources = source_idx[source_pos : source_pos + state.chunk_size]
                repeats = len(chunk_sources)
                sl = slice(source_pos, source_pos + repeats)
                try:
                    metrics = _shock_source_chunk_metrics(
                        model,
                        x_row,
                        mask_row,
                        returns_row,
                        base_weights_row,
                        base_scores_row,
                        base_rank_pos_row,
                        feature_std_device,
                        selected_targets,
                        chunk_sources,
                        state.feature_indices,
                        shock=state.shock,
                        perturb_scale=float(settings.perturb_scale),
                        embedded_api=embedded_api,
                        base_projected_row=base_projected_row,
                        base_embedded_row=base_embedded_row,
                        base_stock_embeddings_row=base_stock_embeddings_row,
                        compile_forward=compile_this_row,
                        max_repeated_rows=int(settings.max_repeated_rows),
                    )
                except RuntimeError as exc:
                    if not _is_cuda_oom(exc) or state.chunk_size <= 1:
                        raise
                    state.oom_retries += 1
                    state.chunk_size = max(1, state.chunk_size // 2)
                    state.compile_forward = False
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                    continue
                state.forward_batches += 1
                if compile_this_row:
                    state.compiled_forward_batches += 1
                else:
                    state.eager_forward_batches += 1
                row_weight = float(row_count)
                for metric_name, values in metrics.items():
                    state.buffers[metric_name][sl, :].add_(values, alpha=row_weight)
                state.row_weight_totals[sl] += row_weight
                source_pos += repeats
                shock_progress.update(row_count * repeats)
            state.elapsed_s += float(time.perf_counter() - state_start)
            shock_progress.set_postfix(
                shock=state.shock,
                chunk=state.chunk_size,
                oom=state.oom_retries,
                refresh=False,
            )
        del x_cpu_row, mask_cpu_row, returns_cpu_row
        del x_row, mask_row, returns_row, base_weights_row, base_scores_row, base_rank_pos_row
        del base_projected_row, base_embedded_row, base_stock_embeddings_row
    shock_progress.close()
    if device.type == "cuda":
        torch.cuda.synchronize(device)
    timing["shock_compute_s"] = float(time.perf_counter() - shock_compute_start)

    source_count = len(source_idx)
    target_count = len(target_idx)
    edge_count_per_shock = source_count * target_count
    try:
        stream_edge_threshold = max(
            1,
            int(os.environ.get("STOCKAGENT_CROSS_ASSET_STREAM_EDGE_THRESHOLD", "5000000")),
        )
    except ValueError:
        stream_edge_threshold = 5_000_000
    stream_raw_edges = bool(settings.compact_artifacts) or (
        edge_count_per_shock * max(1, len(shock_states)) * 16 >= stream_edge_threshold
    )
    edge_writer: pq.ParquetWriter | None = None
    edge_metrics_parquet = tables_dir / "edge_metrics.parquet"
    graph_weight_sum = np.zeros((source_count, target_count), dtype=np.float32)
    graph_weight_max = np.full((source_count, target_count), -np.inf, dtype=np.float32)
    graph_dominant_index = np.full((source_count, target_count), -1, dtype=np.int16)
    processed_shocks: list[str] = []
    edge_source_symbols = (
        None
        if bool(settings.compact_artifacts)
        else np.repeat(np.asarray(source_symbols, dtype=object), target_count)
    )
    edge_target_symbols = (
        None
        if bool(settings.compact_artifacts)
        else np.tile(np.asarray(target_symbols, dtype=object), source_count)
    )
    edge_source_indices = np.repeat(np.asarray(source_idx, dtype=np.int32), target_count)
    edge_target_indices = np.tile(np.asarray(target_idx, dtype=np.int32), source_count)
    edge_attention_flow = attention_selected.reshape(-1).astype(np.float32, copy=False)

    for state in shock_states:
        shock_finalize_start = time.perf_counter()
        denom = state.row_weight_totals[:, None].clamp_min_(1.0)
        metric_names = tuple(state.buffers)
        # One packed D2H transfer per shock replaces one synchronization per
        # metric per source chunk (the former dominant launch/sync overhead).
        packed_buffers = torch.stack(
            [state.buffers[name] / denom for name in metric_names],
            dim=0,
        ).cpu().numpy()
        buffers = {
            name: packed_buffers[index]
            for index, name in enumerate(metric_names)
        }
        perturbation_evidence = _normalize_matrix(buffers["weight_residual_abs"])
        if bool(settings.validated_transmission) and attention_flow is not None:
            validated = perturbation_evidence * _normalize_matrix(attention_selected)
        else:
            validated = perturbation_evidence
        if not bool(settings.compact_artifacts):
            for metric_name, matrix in tqdm(
                buffers.items(),
                total=len(buffers),
                desc=f"Shock {state.shock}: write matrices",
                unit="matrix",
                leave=False,
                disable=not bool(settings.progress_enabled),
            ):
                _write_matrix_csv(
                    matrices_dir / f"{state.shock}_{metric_name}.csv",
                    matrix,
                    source_symbols,
                    target_symbols,
                )
            _write_matrix_csv(
                matrices_dir / f"{state.shock}_validated_transmission.csv",
                validated,
                source_symbols,
                target_symbols,
            )
        _plot_heatmap(
            plots_dir / f"{state.shock}_validated_transmission.png",
            validated,
            f"{state.shock} validated transmission",
            source_symbols,
            target_symbols,
        )
        _plot_heatmap(
            plots_dir / f"{state.shock}_weight_residual_abs.png",
            buffers["weight_residual_abs"],
            f"{state.shock} residual cross-stock influence",
            source_symbols,
            target_symbols,
        )

        # Build the complete Cartesian edge table with columnar NumPy arrays.
        # Avoiding millions of Python dictionaries materially reduces both wall
        # time and peak host memory for full-universe S² output.
        edge_count = source_count * target_count
        if bool(settings.compact_artifacts):
            edge_columns: dict[str, Any] = {
                "shock_index": np.full(edge_count, len(processed_shocks), dtype=np.int8),
                "source_index": edge_source_indices,
                "target_index": edge_target_indices,
                "validated_transmission": validated.reshape(-1).astype(np.float32, copy=False),
            }
        else:
            edge_columns = {
                "shock": np.full(edge_count, state.shock, dtype=object),
                "source_symbol": edge_source_symbols,
                "target_symbol": edge_target_symbols,
                "source_index": edge_source_indices,
                "target_index": edge_target_indices,
                "attention_flow": edge_attention_flow,
                "validated_transmission": validated.reshape(-1).astype(np.float32, copy=False),
            }
        edge_columns.update(
            {name: matrix.reshape(-1).astype(np.float32, copy=False) for name, matrix in buffers.items()}
        )
        edge_frame = pl.DataFrame(edge_columns)
        if stream_raw_edges:
            arrow_table = edge_frame.to_arrow()
            if edge_writer is None:
                edge_metrics_parquet.parent.mkdir(parents=True, exist_ok=True)
                edge_writer = pq.ParquetWriter(
                    edge_metrics_parquet,
                    arrow_table.schema,
                    compression="zstd",
                )
                csv_path = tables_dir / "edge_metrics.csv"
                if csv_path.exists():
                    csv_path.unlink()
            edge_writer.write_table(arrow_table)
        else:
            all_edges.append(edge_frame)
        shock_position = len(processed_shocks)
        better = validated > graph_weight_max
        graph_dominant_index[better] = shock_position
        graph_weight_max = np.maximum(graph_weight_max, validated)
        graph_weight_sum += validated
        processed_shocks.append(state.shock)
        state.finalize_s = float(time.perf_counter() - shock_finalize_start)
        state.elapsed_s += state.finalize_s
        shock_summaries.append(
            {
                "shock": state.shock,
                "matched_features": [feature_names[idx] for idx in state.feature_indices],
                "matched_feature_count": int(len(state.feature_indices)),
                "source_chunk_size_final": int(state.chunk_size),
                "row_chunk_size": int(row_chunk_size),
                "forward_batches": int(state.forward_batches),
                "compiled_forward_batches": int(state.compiled_forward_batches),
                "eager_forward_batches": int(state.eager_forward_batches),
                "oom_retries": int(state.oom_retries),
                "max_validated_transmission": float(validated.max()) if validated.size else 0.0,
                "finalize_s": float(state.finalize_s),
                "elapsed_s": float(state.elapsed_s),
            }
        )
        timing["per_shock_s"][state.shock] = float(state.elapsed_s)

    timing["shock_finalize_s"] = float(sum(state.finalize_s for state in shock_states))

    if edge_writer is not None:
        edge_writer.close()
    if bool(settings.compact_artifacts):
        _write_frame_csv_or_parquet(
            tables_dir / "source_lookup.csv",
            pl.DataFrame(
                {
                    "source_position": np.arange(source_count, dtype=np.int32),
                    "source_index": np.asarray(source_idx, dtype=np.int32),
                    "source_symbol": source_symbols,
                }
            ),
        )
        _write_frame_csv_or_parquet(
            tables_dir / "target_lookup.csv",
            pl.DataFrame(
                {
                    "target_position": np.arange(target_count, dtype=np.int32),
                    "target_index": np.asarray(target_idx, dtype=np.int32),
                    "target_symbol": target_symbols,
                }
            ),
        )
        _write_frame_csv_or_parquet(
            tables_dir / "shock_lookup.csv",
            pl.DataFrame(
                {
                    "shock_index": np.arange(len(processed_shocks), dtype=np.int8),
                    "shock": processed_shocks,
                }
            ),
        )
    raw_edges = pl.concat(all_edges, how="diagonal_relaxed") if all_edges else pl.DataFrame()
    if processed_shocks:
        if edge_source_symbols is None:
            edge_source_symbols = np.repeat(np.asarray(source_symbols, dtype=object), target_count)
        if edge_target_symbols is None:
            edge_target_symbols = np.tile(np.asarray(target_symbols, dtype=object), source_count)
        graph_edge_count = source_count * target_count
        dominant_names = np.asarray(processed_shocks, dtype=object)[graph_dominant_index.reshape(-1)]
        graph_edges_preaggregated = pl.DataFrame(
            {
                "source_index": edge_source_indices,
                "target_index": edge_target_indices,
                "source_symbol": edge_source_symbols,
                "target_symbol": edge_target_symbols,
                "edge_weight": graph_weight_sum.reshape(-1),
                "edge_weight_mean": (graph_weight_sum / float(len(processed_shocks))).reshape(-1),
                "edge_weight_max": graph_weight_max.reshape(-1),
                "shock_count": np.full(graph_edge_count, len(processed_shocks), dtype=np.int16),
                "dominant_shock": dominant_names,
            }
        )
    else:
        graph_edges_preaggregated = pl.DataFrame()
    pipeline_progress.update(1)
    pipeline_progress.set_postfix(stage="graph", refresh=True)
    graph_start = time.perf_counter()
    graph_progress = tqdm(
        total=2,
        desc="Cross-asset graph",
        unit="stage",
        disable=not bool(settings.progress_enabled),
    )
    if stream_raw_edges:
        requested_backend, backend_warnings = _resolve_graph_backend(settings)
        selected_backend = "cugraph" if requested_backend in {"auto", "cugraph"} else "polars"
        source_summary = pl.DataFrame(
            {
                "source_symbol": source_symbols,
                "validated_transmission": graph_weight_sum.sum(axis=1),
            }
        ).sort("source_symbol")
        target_summary = pl.DataFrame(
            {
                "target_symbol": target_symbols,
                "validated_transmission": graph_weight_sum.sum(axis=0),
            }
        ).sort("target_symbol")
        graph_result = _GraphProcessingResult(
            backend=selected_backend,
            edges=pl.DataFrame(),
            source_summary=source_summary,
            target_summary=target_summary,
            node_metrics=pl.DataFrame(),
            benchmark={
                "requested_backend": requested_backend,
                "selected_backend": selected_backend,
                "selection_reason": "streamed_raw_edges_and_online_graph_reduction",
                "edge_count": int(edge_count_per_shock * len(processed_shocks)),
                "graph_edge_count": int(graph_edges_preaggregated.height),
                "raw_edge_storage": str(edge_metrics_parquet),
                "warnings": backend_warnings,
            },
        )
    else:
        graph_result = _process_cross_asset_graph_edges(raw_edges, settings)
    graph_progress.update(1)
    graph_progress.set_postfix(stage="metrics", refresh=False)
    edges = graph_result.edges
    warnings.extend(str(warning) for warning in graph_result.benchmark.get("warnings", ()))
    if not stream_raw_edges:
        _write_frame_csv_or_parquet(tables_dir / "edge_metrics.csv", edges)

    _write_frame_csv_or_parquet(tables_dir / "source_summary.csv", graph_result.source_summary)
    _write_frame_csv_or_parquet(tables_dir / "target_summary.csv", graph_result.target_summary)
    graph_explainability = _build_graph_explainability(
        graph_edges_preaggregated,
        settings,
        preaggregated=True,
    )
    graph_progress.update(1)
    graph_progress.close()
    timing["graph_s"] = float(time.perf_counter() - graph_start)
    pipeline_progress.update(1)
    pipeline_progress.set_postfix(stage="write_reports", refresh=True)
    write_reports_start = time.perf_counter()
    for warning in graph_explainability.summary.get("warnings", ()):
        warnings.append(str(warning))
    if not graph_explainability.graph_edges.is_empty():
        _write_frame_csv_or_parquet(tables_dir / "graph_edges.csv", graph_explainability.graph_edges)
    if not graph_explainability.node_metrics.is_empty():
        _write_frame_csv_or_parquet(tables_dir / "graph_node_metrics.csv", graph_explainability.node_metrics)
        _plot_graph_node_importance(
            plots_dir / "graph_node_importance.png",
            graph_explainability.node_metrics,
        )
    if not graph_explainability.community_summary.is_empty():
        _write_frame_csv_or_parquet(tables_dir / "graph_community_summary.csv", graph_explainability.community_summary)
    if not graph_explainability.community_edges.is_empty():
        _write_frame_csv_or_parquet(tables_dir / "graph_community_edges.csv", graph_explainability.community_edges)
        _plot_graph_community_flow(plots_dir / "graph_community_flow.png", graph_explainability.community_edges)
    if not graph_explainability.graph_edges.is_empty() and not graph_explainability.node_metrics.is_empty():
        _plot_graph_topology(
            plots_dir / "graph_topology.png",
            graph_explainability.graph_edges,
            graph_explainability.node_metrics,
        )
        _plot_graph_transmission_matrix(
            plots_dir / "graph_transmission_matrix.png",
            graph_explainability.graph_edges,
            graph_explainability.node_metrics,
        )
        _plot_graph_self_influence(
            plots_dir / "graph_self_influence.png",
            graph_explainability.graph_edges,
        )
    _write_frame_csv_or_parquet(tables_dir / "shock_summary.csv", _shock_summary_csv_frame(shock_summaries))

    role_warnings: list[str] = []
    if bool(settings.role_embedding):
        role_frame, role_warnings = _role_embedding_frame(aux, symbols, importance)
        _write_frame_csv_or_parquet(tables_dir / "role_embeddings.csv", role_frame)
        if not role_frame.is_empty():
            try:
                import matplotlib.pyplot as plt
                fig, ax = plt.subplots(figsize=_figsize_17_6(), dpi=140)
                ax.scatter(
                    role_frame["role_x"].to_numpy(),
                    role_frame["role_y"].to_numpy(),
                    s=16,
                    alpha=0.75,
                )
                ax.set_title("Latent Stock Role Embedding")
                ax.set_xlabel("role_x")
                ax.set_ylabel("role_y")
                _safe_matplotlib_tight_layout(fig)
                _save_matplotlib_figure(fig, plots_dir / "role_embeddings.png")
                plt.close(fig)
            except Exception as exc:
                role_warnings.append(f"Role embedding plot failed: {type(exc).__name__}: {exc}")
    warnings.extend(role_warnings)

    timing["write_reports_s"] = float(time.perf_counter() - write_reports_start)
    timing["total_s"] = float(time.perf_counter() - total_start)
    shock_compute_s = max(float(timing.get("shock_compute_s", 0.0)), 1e-9)
    timing["source_date_shocks_per_s"] = (
        float(n_rows * len(source_idx) * len(shock_states)) / shock_compute_s
    )
    timing["sources_per_s"] = (
        float(len(source_idx) * len(shock_states)) / shock_compute_s
    )
    summary = {
        "enabled": True,
        "module": MODULE_NAME,
        "rows": int(n_rows),
        "lookback": int(lookback),
        "num_symbols": int(n_symbols),
        "num_features": int(n_features),
        "sources": int(len(source_idx)),
        "targets": int(len(target_idx)),
        "shocks": list(requested_shocks),
        "settings": asdict(settings),
        "row_chunking": row_chunk_info,
        "temporal_stock_cache": bool(stock_embedding_api is not None),
        "artifact_layout": "compact_numeric_edges" if settings.compact_artifacts else "verbose_matrices",
        "shock_summaries": shock_summaries,
        "attention_available": bool(attention_flow is not None),
        "attention_capture_rows": attention_rows,
        "graph_backend": graph_result.backend,
        "graph_benchmark": graph_result.benchmark,
        "graph_explainability": graph_explainability.summary,
        "graph_figure_contract": {
            "node_selection": "all",
            "edge_selection": "all",
            "top_k": False,
            "color_value_clipping": False,
            "sparse_labels_are_layout_only": True,
        },
        "timing": timing,
        "warnings": warnings,
        "elapsed_s": float(timing["total_s"]),
    }
    (destination / "abstract_cross_asset_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    report_lines = [
        "# Abstract Cross-Asset Transmission",
        "",
        "Rows are source stocks being perturbed; columns are target stocks affected by the model.",
        "",
        f"- Sources analyzed: {len(source_idx)}",
        f"- Targets analyzed: {len(target_idx)}",
        f"- Shocks: {', '.join(requested_shocks)}",
        f"- Attention evidence available: {bool(attention_flow is not None)}",
        f"- Graph backend selected: {graph_result.backend}",
        f"- Full graph explainability backend: {graph_explainability.backend}",
        f"- Elapsed seconds: {summary['elapsed_s']:.3f}",
        "",
        "## Metric Definitions",
        "",
        "- `weight_total_abs`: absolute total target weight movement.",
        "- `weight_reallocation_abs`: movement explained by portfolio normalization after changing only the source score.",
        "- `weight_residual_abs`: remaining cross-stock influence after removing normalization-only reallocation.",
        "- `validated_transmission`: normalized perturbation evidence multiplied by attention evidence when available.",
        "- `transmission_pnl`: target weight delta times target future return, averaged over sampled rows.",
        "",
        "## Full Graph Explainability",
        "",
        "- `weighted_out_degree`: total validated transmission sent by a stock across the full graph.",
        "- `weighted_in_degree`: total validated transmission received by a stock across the full graph.",
        "- `pagerank`: recursively important receivers of cross-asset influence.",
        "- `hub_score`: stocks that point to important receivers; high values indicate transmitters.",
        "- `authority_score`: stocks receiving influence from important transmitters.",
        "- `betweenness_centrality`: bridge stocks that sit on shortest transmission paths; exhaustive graphs use the exact complete-graph closed form and other graphs run the full cuGraph calculation.",
        "- `community_id`: Louvain/Leiden-style transmission community from the full weighted graph.",
        "- `primary_role`: rule-based label derived from the graph metrics: transmitter, receiver, bridge, systemic receiver, net source, net sink, or balanced.",
        "- `graph_topology.png`: every inter-symbol edge in a directed source-by-target adjacency map; no Top-K node or edge filtering.",
        "- `graph_transmission_matrix.png`: complete in-scope asset-level graph as a matrix, avoiding node-link edge crossings.",
        "- `graph_self_influence.png`: self-loop influence separated from the topology so cross-symbol flow remains legible.",
        "",
    ]
    if not graph_explainability.node_metrics.is_empty():
        report_lines.extend(["## Complete Graph Nodes", ""])
        for row in graph_explainability.node_metrics.sort("pagerank", descending=True).to_dicts():
            report_lines.append(
                f"- `{row.get('symbol', row.get('symbol_index'))}` role={row.get('primary_role', 'n/a')}, "
                f"pagerank={float(row.get('pagerank', 0.0) or 0.0):.4f}, "
                f"hub={float(row.get('hub_score', 0.0) or 0.0):.4f}, "
                f"authority={float(row.get('authority_score', 0.0) or 0.0):.4f}, "
                f"net={float(row.get('net_transmitter_score', 0.0) or 0.0):.4g}"
            )
        report_lines.append("")
    if not graph_explainability.community_summary.is_empty():
        report_lines.extend(["## Graph Communities", ""])
        for row in graph_explainability.community_summary.to_dicts():
            report_lines.append(
                f"- community `{int(row.get('community_id', 0) or 0)}` nodes={int(row.get('node_count', 0) or 0)}, "
                f"pagerank={float(row.get('total_pagerank', 0.0) or 0.0):.4f}, "
                f"internal={float(row.get('internal_weight', 0.0) or 0.0):.4g}, "
                f"symbols={row.get('symbols', '')}"
            )
        report_lines.append("")
    if warnings:
        report_lines.extend(["## Warnings", ""])
        report_lines.extend([f"- {warning}" for warning in warnings])
    (destination / "abstract_cross_asset_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    pipeline_progress.update(1)
    pipeline_progress.close()
    if was_training:
        model.train()
    return summary
