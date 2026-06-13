from __future__ import annotations

import argparse
import gc
import inspect
import json
import math
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
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


def _use_datashader_for_explainability(plot_backend: str) -> bool:
    normalized = _normalize_plot_backend(plot_backend)
    if normalized == "matplotlib":
        return False
    available = rapids_datashader_available(require_cuda=True)
    if normalized == "rapids_datashader" and not available:
        raise RuntimeError(
            "RAPIDS/cuDF/Datashader with CUDA was requested for explainability, but it is unavailable."
        )
    return bool(available)


def _device_from_config(config: ExperimentConfig, override: str | None = None) -> torch.device:
    requested = (override or config.environment.device or "cpu").strip().lower()
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested for explanation, but torch.cuda.is_available() is False.")
    return torch.device(requested)


def _move_batch(batch: dict[str, torch.Tensor], device: torch.device) -> dict[str, torch.Tensor]:
    return {key: value.to(device=device, non_blocking=(device.type == "cuda")) for key, value in batch.items()}


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
) -> pd.DataFrame:
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
    return pd.DataFrame(rows)


def _feature_summary_frame(feature_time: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    if feature_time.empty:
        return pd.DataFrame(columns=["feature", "feature_group", "feature_label", metric_name, "share"])
    summary = feature_time.groupby("feature", as_index=False)[metric_name].sum()
    summary["feature_group"] = summary["feature"].map(_feature_group)
    summary["feature_label"] = summary["feature"].map(_feature_label)
    total = float(summary[metric_name].sum())
    summary["share"] = summary[metric_name] / total if total > 0.0 else 0.0
    return summary.sort_values(metric_name, ascending=False)


def _time_summary_frame(feature_time: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    if feature_time.empty:
        return pd.DataFrame(columns=["lookback_index", "lookback_from_end", metric_name, "share"])
    summary = feature_time.groupby(["lookback_index", "lookback_from_end"], as_index=False)[metric_name].sum()
    total = float(summary[metric_name].sum())
    summary["share"] = summary[metric_name] / total if total > 0.0 else 0.0
    return summary.sort_values("lookback_index")


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
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
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
        frame = pd.DataFrame(rows)
        return frame, pd.DataFrame(), diagnostics
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
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame, pd.DataFrame(), diagnostics
    summary = frame.groupby("feature", as_index=False)[["weight_abs_delta", "score_abs_delta"]].sum()
    summary["feature_group"] = summary["feature"].map(_feature_group)
    summary["feature_label"] = summary["feature"].map(_feature_label)
    total = float(summary["weight_abs_delta"].sum())
    summary["weight_delta_share"] = summary["weight_abs_delta"] / total if total > 0.0 else 0.0
    summary = summary.sort_values("weight_abs_delta", ascending=False)
    return frame, summary, diagnostics


def _feature_correlations(
    x: torch.Tensor,
    scores: torch.Tensor,
    weights: torch.Tensor,
    mask: torch.Tensor,
    feature_names: list[str],
) -> pd.DataFrame:
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
                score_corr = float(np.corrcoef(feat[valid], score_np[valid])[0, 1])
                weight_corr = float(np.corrcoef(feat[valid], weight_np[valid])[0, 1])
                score_corr = _safe_float(score_corr)
                weight_corr = _safe_float(weight_corr)
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
    return pd.DataFrame(rows).sort_values(["abs_score_corr", "abs_weight_corr"], ascending=False)


def _decision_rows(
    weights: torch.Tensor,
    scores: torch.Tensor,
    returns: torch.Tensor,
    mask: torch.Tensor,
    dates: list[str],
    symbols: list[str],
    top_k: int,
) -> pd.DataFrame:
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
    return pd.DataFrame(rows)


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


def _aux_summary(aux: dict[str, torch.Tensor]) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    rows: list[dict[str, Any]] = []
    dim_frames: dict[str, pd.DataFrame] = {}
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
            dim_frames[name] = pd.DataFrame(
                {
                    "dim": np.arange(by_dim.shape[0], dtype=np.int64),
                    "mean_abs": by_dim,
                    "share": by_dim / total if total > 0.0 else np.zeros_like(by_dim),
                }
            ).sort_values("mean_abs", ascending=False)
    return pd.DataFrame(rows).sort_values("mean_abs", ascending=False), dim_frames


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
) -> tuple[dict[str, pd.DataFrame], list[dict[str, Any]], list[str], dict[str, Any]]:
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

    projection_frames: dict[str, pd.DataFrame] = {}
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
        frame = pd.DataFrame(meta)
        frame["umap_x"] = embedding_cpu[:, 0].astype(np.float32, copy=False)
        frame["umap_y"] = embedding_cpu[:, 1].astype(np.float32, copy=False)
        frame["sampled_points"] = int(sample_idx_cpu.size)
        frame["original_points"] = int(n_points)
        projection_frames[name] = frame
        x_std = float(np.nanstd(frame["umap_x"].to_numpy(dtype=np.float64)))
        y_std = float(np.nanstd(frame["umap_y"].to_numpy(dtype=np.float64)))
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
) -> pd.DataFrame:
    contribution = (weights.detach().float() * returns.detach().float()).masked_fill(~mask.detach().bool(), 0.0)
    mean_weight = weights.detach().float().mean(dim=0)
    mean_abs_weight = weights.detach().float().abs().mean(dim=0)
    total_contribution = contribution.sum(dim=0)
    active_count = mask.detach().bool().sum(dim=0).clamp_min(1)
    rows = []
    for idx, symbol in enumerate(symbols):
        rows.append(
            {
                "symbol": symbol,
                "mean_weight": float(mean_weight[idx].cpu()),
                "mean_abs_weight": float(mean_abs_weight[idx].cpu()),
                "total_gross_contribution": float(total_contribution[idx].cpu()),
                "mean_contribution_when_active": float((total_contribution[idx] / active_count[idx]).cpu()),
                "active_count": int(active_count[idx].cpu()),
            }
        )
    return pd.DataFrame(rows).sort_values("mean_abs_weight", ascending=False)


def _make_warnings(
    portfolio: dict[str, float],
    feature_summary: pd.DataFrame,
    time_summary: pd.DataFrame,
    corr: pd.DataFrame,
    aux_summary: pd.DataFrame,
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
    if not feature_summary.empty and float(feature_summary.iloc[0].get("share", 0.0)) > 0.55:
        warnings.append(
            f"Feature attribution is dominated by one feature: {feature_summary.iloc[0]['feature']}."
        )
    if not time_summary.empty and float(time_summary.iloc[0].get("share", 0.0)) > 0.70:
        warnings.append("Attribution is dominated by a single lookback day.")
    if not corr.empty:
        row = corr.iloc[0]
        if max(float(row["abs_score_corr"]), float(row["abs_weight_corr"])) > 0.75:
            warnings.append(
                f"Strong simple correlation detected: {row['source']}:{row['feature']} "
                f"(score_corr={row['score_corr']:.3f}, weight_corr={row['weight_corr']:.3f})."
            )
    if not aux_summary.empty:
        collapsed = aux_summary[aux_summary["zero_fraction"] > 0.95]
        if not collapsed.empty:
            warnings.append(
                "Some auxiliary representations are near-zero/collapsed: "
                + ", ".join(collapsed["name"].astype(str).head(5).tolist())
            )
    if not warnings:
        warnings.append("No rule-of-thumb anomaly was triggered; inspect tables before trusting the strategy.")
    return warnings


def _daily_portfolio_frame(
    weights: torch.Tensor,
    returns: torch.Tensor,
    mask: torch.Tensor,
    dates: list[str],
) -> pd.DataFrame:
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
    return pd.DataFrame(rows)


def _regime_analysis_frame(daily: pd.DataFrame) -> pd.DataFrame:
    required = {"market_log_return", "strategy_log_return", "turnover_proxy", "gross_exposure", "net_exposure"}
    if daily.empty or not required.issubset(daily.columns):
        return pd.DataFrame()
    data = daily.copy()
    data["market_direction"] = np.where(
        data["market_log_return"] > 0.001,
        "market_up",
        np.where(data["market_log_return"] < -0.001, "market_down", "market_flat"),
    )
    abs_market = data["market_log_return"].abs()
    if int(abs_market.notna().sum()) >= 3 and float(abs_market.max()) > float(abs_market.min()):
        try:
            data["volatility_bucket"] = pd.qcut(
                abs_market,
                q=min(3, int(abs_market.notna().sum())),
                labels=False,
                duplicates="drop",
            ).map({0: "low_abs_market_move", 1: "mid_abs_market_move", 2: "high_abs_market_move"})
        except ValueError:
            data["volatility_bucket"] = "single_vol_bucket"
    else:
        data["volatility_bucket"] = "single_vol_bucket"
    rows: list[dict[str, Any]] = []
    for dimension in ("market_direction", "volatility_bucket"):
        for regime, group in data.groupby(dimension, dropna=False):
            rows.append(
                {
                    "dimension": dimension,
                    "regime": str(regime),
                    "rows": int(len(group)),
                    "mean_strategy_log_return": float(group["strategy_log_return"].mean()),
                    "mean_market_log_return": float(group["market_log_return"].mean()),
                    "mean_turnover_proxy": float(group["turnover_proxy"].mean()),
                    "mean_gross_exposure": float(group["gross_exposure"].mean()),
                    "mean_net_exposure": float(group["net_exposure"].mean()),
                    "hit_rate": float((group["strategy_log_return"] > 0.0).mean()),
                }
            )
    return pd.DataFrame(rows)


def _case_study_frame(decisions: pd.DataFrame, daily: pd.DataFrame, top_k: int) -> pd.DataFrame:
    if decisions.empty or daily.empty or "date" not in decisions.columns:
        return pd.DataFrame()
    top_k = max(1, int(top_k))
    selected: list[tuple[str, str]] = []
    if "strategy_log_return" in daily.columns:
        best = daily.sort_values("strategy_log_return", ascending=False).head(1)
        worst = daily.sort_values("strategy_log_return", ascending=True).head(1)
        if not best.empty:
            selected.append(("best_strategy_day", str(best.iloc[0]["date"])))
        if not worst.empty:
            selected.append(("worst_strategy_day", str(worst.iloc[0]["date"])))
    if "turnover_proxy" in daily.columns:
        turnover = daily.sort_values("turnover_proxy", ascending=False).head(1)
        if not turnover.empty:
            selected.append(("highest_turnover_day", str(turnover.iloc[0]["date"])))
    if "gross_exposure" in daily.columns:
        gross = daily.sort_values("gross_exposure", ascending=False).head(1)
        if not gross.empty:
            selected.append(("highest_gross_exposure_day", str(gross.iloc[0]["date"])))
    unique_selected: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in selected:
        if item not in seen:
            unique_selected.append(item)
            seen.add(item)
    rows: list[pd.DataFrame] = []
    daily_small = daily.set_index("date", drop=False)
    for case_type, date in unique_selected:
        chunk = decisions[decisions["date"].astype(str) == date].copy()
        if chunk.empty:
            continue
        chunk["abs_weight"] = pd.to_numeric(chunk.get("weight", 0.0), errors="coerce").abs()
        chunk = chunk.sort_values("abs_weight", ascending=False).head(top_k)
        chunk.insert(0, "case_type", case_type)
        if date in daily_small.index:
            daily_row = daily_small.loc[date]
            if isinstance(daily_row, pd.DataFrame):
                daily_row = daily_row.iloc[0]
            for col in ("strategy_log_return", "market_log_return", "turnover_proxy", "gross_exposure", "net_exposure"):
                chunk[f"case_{col}"] = daily_row.get(col)
        rows.append(chunk)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _feature_time_top_cells(frame: pd.DataFrame, metric_name: str, top_n: int = 20) -> pd.DataFrame:
    required = {"feature", "lookback_from_end", metric_name}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()
    data = frame.copy()
    data[metric_name] = pd.to_numeric(data[metric_name], errors="coerce")
    data = data.dropna(subset=[metric_name]).sort_values(metric_name, ascending=False).head(top_n)
    if data.empty:
        return data
    total = float(pd.to_numeric(frame[metric_name], errors="coerce").fillna(0.0).sum())
    data["share"] = data[metric_name] / total if total > 0.0 else 0.0
    data["lookback_label"] = data["lookback_from_end"].map(_lookback_label)
    if "feature_group" not in data.columns:
        data["feature_group"] = data["feature"].map(_feature_group)
    if "feature_label" not in data.columns:
        data["feature_label"] = data["feature"].map(_feature_label)
    return data


def _trust_check_frame(
    portfolio: dict[str, float],
    feature_summary: pd.DataFrame,
    time_summary: pd.DataFrame,
    corr: pd.DataFrame,
    aux_summary: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    def add_check(name: str, value: float, threshold: float, comparator: str, interpretation: str) -> None:
        if comparator == "<=":
            passed = value <= threshold
        else:
            passed = value >= threshold
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

    add_check(
        "untradable_abs_weight_sum",
        float(portfolio.get("untradable_abs_weight_sum", 0.0)),
        1e-5,
        "<=",
        "Should be zero; non-zero means the mask/tradability logic leaked into actual positions.",
    )
    add_check(
        "max_abs_weight_max",
        float(portfolio.get("max_abs_weight_max", 0.0)),
        0.35,
        "<=",
        "Large single-name weights can indicate shortcut learning or unstable concentration.",
    )
    add_check(
        "mean_turnover_proxy",
        float(portfolio.get("mean_turnover_proxy", 0.0)),
        1.5,
        "<=",
        "High turnover makes net performance highly fee-sensitive and less trustworthy.",
    )
    if not feature_summary.empty and "share" in feature_summary.columns:
        add_check(
            "top_feature_attribution_share",
            float(feature_summary.iloc[0].get("share", 0.0)),
            0.55,
            "<=",
            "A single dominant feature can be a sign that the model learned a narrow rule.",
        )
    if not time_summary.empty and "share" in time_summary.columns:
        add_check(
            "top_lookback_day_attribution_share",
            float(time_summary.sort_values("share", ascending=False).iloc[0].get("share", 0.0)),
            0.70,
            "<=",
            "A single dominant day can mean the temporal model is mostly ignoring the lookback window.",
        )
    if not corr.empty and {"abs_score_corr", "abs_weight_corr"}.issubset(corr.columns):
        corr_max = float(np.nanmax(corr[["abs_score_corr", "abs_weight_corr"]].to_numpy(dtype=np.float64)))
        add_check(
            "max_simple_feature_score_weight_corr",
            corr_max,
            0.75,
            "<=",
            "High raw correlation can reveal price-level, liquidity, or other simple shortcut rules.",
        )
    if not aux_summary.empty and "zero_fraction" in aux_summary.columns:
        add_check(
            "max_aux_zero_fraction",
            float(pd.to_numeric(aux_summary["zero_fraction"], errors="coerce").fillna(0.0).max()),
            0.95,
            "<=",
            "Near-zero aux tensors can indicate collapsed latent/market token representations.",
        )
    return pd.DataFrame(rows)


def _score_head_surrogate_shap(
    x: torch.Tensor,
    scores: torch.Tensor,
    mask: torch.Tensor,
    feature_names: list[str],
    *,
    enabled: bool,
    mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], list[str]]:
    mode = _normalize_shap_mode(mode)
    if not enabled or mode in {"off", "none"}:
        return pd.DataFrame(), pd.DataFrame(), {"enabled": bool(enabled), "method": "skipped"}, []
    warnings: list[str] = []

    x_cpu = x.detach().float().cpu()
    scores_cpu = scores.detach().float().cpu()
    mask_cpu = mask.detach().bool().cpu()
    aggregates: list[tuple[str, torch.Tensor]] = [
        ("last", x_cpu[:, -1]),
        ("lookback_mean", x_cpu.mean(dim=1)),
    ]
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
        return pd.DataFrame(), pd.DataFrame(), {"enabled": True, "method": "skipped", "valid_rows": int(design.shape[0])}, [message]
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
        return pd.DataFrame(), pd.DataFrame(), {"enabled": True, "method": "skipped", "valid_rows": int(design.shape[0])}, [message]
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
    component_frame = pd.DataFrame(component_rows).sort_values("shap_abs", ascending=False)
    summary = component_frame.groupby("feature", as_index=False)["shap_abs"].sum()
    summary["feature_group"] = summary["feature"].map(_feature_group)
    summary["feature_label"] = summary["feature"].map(_feature_label)
    total = float(summary["shap_abs"].sum())
    summary["share"] = summary["shap_abs"] / total if total > 0.0 else 0.0
    if not component_frame.empty:
        top_source = component_frame.sort_values("shap_abs", ascending=False).drop_duplicates("feature")
        summary = summary.merge(top_source[["feature", "source"]].rename(columns={"source": "top_source"}), on="feature", how="left")
    summary["surrogate_r2"] = r2
    summary["method"] = method
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
    return summary.sort_values("shap_abs", ascending=False), component_frame, info, warnings


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
        ig_ft = pd.DataFrame()
        ig_feature = pd.DataFrame()
        ig_time = pd.DataFrame()
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
        perturb_ft = pd.DataFrame()
        perturb_feature = pd.DataFrame()
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
    regime = _regime_analysis_frame(daily) if bool(settings.regime_analysis) else pd.DataFrame()
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
    if not grad_ft.empty and "lookback_from_end" in grad_ft.columns:
        attribution_lookback = int(pd.to_numeric(grad_ft["lookback_from_end"], errors="coerce").max() + 1)
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
    }


def _write_markdown_report(
    path: Path,
    *,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    frames: dict[str, pd.DataFrame],
) -> None:
    def _render_frame(frame: pd.DataFrame) -> str:
        try:
            return frame.head(20).to_markdown(index=False)
        except ImportError:
            return "```text\n" + frame.head(20).to_string(index=False) + "\n```"

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
        lines.append(_render_frame(pd.DataFrame(aux_projection_summary)))
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
        if frame is None or frame.empty:
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


def _global_attribution_table(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    tables: list[pd.DataFrame] = []

    def add(frame_name: str, value_col: str, share_col: str, prefix: str) -> None:
        frame = frames.get(frame_name, pd.DataFrame())
        if frame.empty or "feature" not in frame.columns or value_col not in frame.columns:
            return
        cols = ["feature", value_col]
        if "share" in frame.columns:
            cols.append("share")
        if "weight_delta_share" in frame.columns:
            cols.append("weight_delta_share")
        data = frame[cols].copy()
        data = data.rename(columns={value_col: f"{prefix}_value"})
        if "share" in data.columns:
            data = data.rename(columns={"share": f"{prefix}_share"})
        if "weight_delta_share" in data.columns:
            data = data.rename(columns={"weight_delta_share": f"{prefix}_share"})
        if f"{prefix}_share" not in data.columns:
            total = float(pd.to_numeric(data[f"{prefix}_value"], errors="coerce").fillna(0.0).sum())
            data[f"{prefix}_share"] = data[f"{prefix}_value"] / total if total > 0.0 else 0.0
        tables.append(data)

    add("feature_importance_gradient", "grad_x_input_abs", "share", "gradient")
    add("feature_importance_integrated_gradients", "integrated_gradients_abs", "share", "integrated_gradients")
    add("feature_importance_perturbation", "weight_abs_delta", "weight_delta_share", "perturbation_weight")
    add("feature_importance_shap", "shap_abs", "share", "shap")
    if not tables:
        return pd.DataFrame()
    out = tables[0]
    for table in tables[1:]:
        out = out.merge(table, on="feature", how="outer")
    out["feature_group"] = out["feature"].map(_feature_group)
    out["feature_label"] = out["feature"].map(_feature_label)
    share_cols = [col for col in out.columns if col.endswith("_share")]
    out[share_cols] = out[share_cols].fillna(0.0)
    out["mean_available_share"] = out[share_cols].mean(axis=1) if share_cols else 0.0
    return out.sort_values("mean_available_share", ascending=False)


def _write_paper_tables(
    output_dir: Path,
    *,
    frames: dict[str, pd.DataFrame],
    summary: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, str]:
    table_dir = output_dir / "paper_tables"
    table_dir.mkdir(parents=True, exist_ok=True)
    tables: dict[str, pd.DataFrame] = {}
    tables["global_feature_attribution"] = _global_attribution_table(frames)
    top_cell_frames: list[pd.DataFrame] = []
    for name, method in (
        ("top_feature_time_gradient_cells", "gradient_x_input"),
        ("top_feature_time_integrated_gradients_cells", "integrated_gradients"),
        ("top_feature_time_perturbation_cells", "perturbation_weight_delta"),
    ):
        frame = frames.get(name, pd.DataFrame())
        if frame is not None and not frame.empty:
            chunk = frame.copy()
            chunk.insert(0, "method", method)
            top_cell_frames.append(chunk)
    tables["feature_time_top_cells"] = pd.concat(top_cell_frames, ignore_index=True) if top_cell_frames else pd.DataFrame()
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
        tables[name] = frames.get(name, pd.DataFrame())
    lookback_expected = metadata.get("config_lookback")
    lookback_observed = summary.get("attribution_lookback")
    tables["lookback_consistency"] = pd.DataFrame(
        [
            {
                "config_lookback": lookback_expected,
                "attribution_lookback": lookback_observed,
                "status": "match"
                if lookback_expected is None or int(lookback_expected) == int(lookback_observed or 0)
                else "warn",
                "interpretation": "Attribution days should match the configured lookback; mismatch means the artifact is not lookback-complete or came from an older run.",
            }
        ]
    )
    written: dict[str, str] = {}
    for name, table in tables.items():
        if table is None or table.empty:
            continue
        path = table_dir / f"{name}.csv"
        table.to_csv(path, index=False)
        written[name] = str(path.relative_to(output_dir))
    return written


def _plot_paper_global_attribution(table: pd.DataFrame, output_path: Path, *, subtitle: str) -> None:
    if table.empty:
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
    data = table.head(14).copy()
    melted = []
    for col, label in available:
        chunk = data[["feature_label", col]].copy()
        chunk["method"] = label
        chunk = chunk.rename(columns={col: "share"})
        melted.append(chunk)
    plot_data = pd.concat(melted, ignore_index=True)
    plt, sns = _setup_paper_plotting()
    fig_height = max(5.2, 0.42 * data["feature_label"].nunique() + 2.1)
    fig, ax = plt.subplots(figsize=(12.5, fig_height), dpi=160)
    palette = {
        "Grad x input": PAPER_TOKENS["blue_mid"],
        "Integrated gradients": PAPER_TOKENS["gold_mid"],
        "Perturbation": PAPER_TOKENS["orange_mid"],
        "Surrogate SHAP": PAPER_TOKENS["olive_mid"],
    }
    order = data["feature_label"].tolist()[::-1]
    sns.barplot(
        data=plot_data,
        y="feature_label",
        x="share",
        hue="method",
        order=order,
        palette={key: palette[key] for key in plot_data["method"].unique()},
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
    fig.savefig(output_path)
    plt.close(fig)


def _plot_paper_feature_time_heatmap(
    frame: pd.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
    subtitle: str,
    top_features: int = 24,
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if frame.empty or not required.issubset(frame.columns):
        return
    data = frame.copy()
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data["lookback_from_end"] = pd.to_numeric(data["lookback_from_end"], errors="coerce")
    data = data.dropna(subset=[value_col, "lookback_from_end"])
    if data.empty:
        return
    top = data.groupby("feature")[value_col].sum().sort_values(ascending=False).head(top_features).index
    data = data[data["feature"].isin(top)].copy()
    data["feature_label"] = data["feature"].map(_feature_label)
    pivot = data.pivot_table(
        index="feature_label",
        columns="lookback_from_end",
        values=value_col,
        aggfunc="sum",
        fill_value=0.0,
    )
    ordered_labels = [_feature_label(feature) for feature in top if _feature_label(feature) in pivot.index]
    pivot = pivot.loc[ordered_labels]
    pivot = pivot.reindex(columns=sorted(pivot.columns))
    if pivot.empty:
        return
    plt, sns = _setup_paper_plotting()
    from matplotlib.colors import LinearSegmentedColormap

    fig_height = max(5.0, 0.38 * len(pivot) + 2.0)
    fig, ax = plt.subplots(figsize=(12.2, fig_height), dpi=170)
    cmap = LinearSegmentedColormap.from_list(
        "paper_blue_gold",
        [PAPER_TOKENS["blue_xlight"], PAPER_TOKENS["blue_base"], PAPER_TOKENS["blue_dark"], PAPER_TOKENS["gold_mid"]],
    )
    vmax = float(np.nanpercentile(pivot.to_numpy(dtype=np.float64), 98))
    if vmax <= 0.0:
        vmax = None
    sns.heatmap(
        pivot,
        cmap=cmap,
        vmin=0.0,
        vmax=vmax,
        linewidths=0.7,
        linecolor=PAPER_TOKENS["panel"],
        cbar_kws={"label": value_col},
        ax=ax,
    )
    ax.set_xlabel("Lookback day (t-0 = latest day before decision)")
    ax.set_ylabel("")
    ax.set_xticklabels([_lookback_label(label.get_text()) for label in ax.get_xticklabels()], rotation=0)
    ax.tick_params(axis="y", labelsize=8)
    _add_paper_header(fig, ax, title, subtitle)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_paper_time_importance(frame: pd.DataFrame, *, output_path: Path, value_col: str, subtitle: str) -> None:
    if frame.empty or not {"lookback_from_end", value_col}.issubset(frame.columns):
        return
    data = frame.copy()
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data["lookback_from_end"] = pd.to_numeric(data["lookback_from_end"], errors="coerce")
    data = data.dropna(subset=[value_col, "lookback_from_end"]).sort_values("lookback_from_end")
    if data.empty:
        return
    plt, sns = _setup_paper_plotting()
    fig, ax = plt.subplots(figsize=(10.5, 5.2), dpi=160)
    sns.barplot(data=data, x="lookback_from_end", y="share" if "share" in data.columns else value_col, color=PAPER_TOKENS["blue_mid"], ax=ax)
    ax.set_xlabel("Lookback day (t-0 = latest)")
    ax.set_ylabel("Attribution share" if "share" in data.columns else value_col)
    ax.set_xticks(np.arange(len(data)))
    ax.set_xticklabels([_lookback_label(value) for value in data["lookback_from_end"].tolist()])
    if "share" in data.columns:
        ax.yaxis.set_major_formatter(lambda value, _: f"{100.0 * value:.0f}%")
        for patch, value in zip(ax.patches, data["share"].to_numpy(dtype=np.float64), strict=False):
            ax.text(patch.get_x() + patch.get_width() / 2.0, patch.get_height(), _format_share(value), ha="center", va="bottom", fontsize=8, color=PAPER_TOKENS["ink"])
    _add_paper_header(fig, ax, "Temporal attribution across the lookback window", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_paper_feature_correlations(frame: pd.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if frame.empty or not {"feature", "source", "score_corr", "weight_corr"}.issubset(frame.columns):
        return
    data = frame.copy()
    data["max_abs_corr"] = pd.to_numeric(data[["score_corr", "weight_corr"]].abs().max(axis=1), errors="coerce")
    data = data.dropna(subset=["max_abs_corr"]).sort_values("max_abs_corr", ascending=False).head(18)
    if data.empty:
        return
    data["label"] = data["source"].astype(str) + " / " + data["feature"].astype(str)
    plot = data.melt(id_vars=["label"], value_vars=["score_corr", "weight_corr"], var_name="target", value_name="corr")
    plt, sns = _setup_paper_plotting()
    fig_height = max(5.2, 0.36 * data.shape[0] + 2.0)
    fig, ax = plt.subplots(figsize=(11.5, fig_height), dpi=160)
    sns.barplot(
        data=plot,
        y="label",
        x="corr",
        hue="target",
        order=data["label"].tolist()[::-1],
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
    fig.savefig(output_path)
    plt.close(fig)


def _plot_paper_trust_checks(frame: pd.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if frame.empty or not {"check", "value", "status"}.issubset(frame.columns):
        return
    data = frame.copy()
    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    data = data.dropna(subset=["value"])
    if data.empty:
        return
    plt, sns = _setup_paper_plotting()
    fig_height = max(4.8, 0.48 * len(data) + 2.0)
    fig, ax = plt.subplots(figsize=(11.5, fig_height), dpi=160)
    palette = {"pass": PAPER_TOKENS["blue_mid"], "warn": PAPER_TOKENS["orange_mid"]}
    sns.barplot(data=data, y="check", x="value", hue="status", dodge=False, palette=palette, ax=ax)
    for row_idx, row in data.reset_index(drop=True).iterrows():
        ax.text(float(row["value"]), row_idx, f"  {row['rule']}", va="center", ha="left", fontsize=8, color=PAPER_TOKENS["muted"])
    ax.set_xlabel("Measured value")
    ax.set_ylabel("")
    ax.legend(loc="lower right", frameon=True, fontsize=8)
    _add_paper_header(fig, ax, "Strategy trust checks highlight concentration, masking, and shortcut risks", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_paper_regime(frame: pd.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if frame.empty or not {"dimension", "regime", "mean_strategy_log_return"}.issubset(frame.columns):
        return
    data = frame.copy()
    data["mean_strategy_log_return"] = pd.to_numeric(data["mean_strategy_log_return"], errors="coerce")
    data = data.dropna(subset=["mean_strategy_log_return"])
    if data.empty:
        return
    data["label"] = data["dimension"].astype(str) + " / " + data["regime"].astype(str)
    plt, sns = _setup_paper_plotting()
    fig_height = max(4.8, 0.42 * len(data) + 2.0)
    fig, ax = plt.subplots(figsize=(11.5, fig_height), dpi=160)
    colors = [PAPER_TOKENS["blue_mid"] if value >= 0 else PAPER_TOKENS["orange_mid"] for value in data["mean_strategy_log_return"]]
    sns.barplot(data=data, y="label", x="mean_strategy_log_return", palette=colors, hue="label", legend=False, ax=ax)
    ax.axvline(0.0, color=PAPER_TOKENS["neutral_dark"], linewidth=1.0)
    ax.set_xlabel("Mean strategy log return")
    ax.set_ylabel("")
    _add_paper_header(fig, ax, "Performance by market regime checks whether the rule survives different states", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_paper_case_studies(frame: pd.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if frame.empty or not {"case_type", "symbol", "gross_contribution"}.issubset(frame.columns):
        return
    data = frame.copy()
    data["gross_contribution"] = pd.to_numeric(data["gross_contribution"], errors="coerce")
    data = data.dropna(subset=["gross_contribution"])
    if data.empty:
        return
    data["label"] = data["case_type"].astype(str) + " / " + data["symbol"].astype(str)
    data = data.sort_values("gross_contribution")
    plt, sns = _setup_paper_plotting()
    fig_height = max(5.2, 0.28 * len(data) + 2.0)
    fig, ax = plt.subplots(figsize=(12, fig_height), dpi=160)
    colors = [PAPER_TOKENS["blue_mid"] if value >= 0 else PAPER_TOKENS["orange_mid"] for value in data["gross_contribution"]]
    sns.barplot(data=data, y="label", x="gross_contribution", palette=colors, hue="label", legend=False, ax=ax)
    ax.axvline(0.0, color=PAPER_TOKENS["neutral_dark"], linewidth=1.0)
    ax.set_xlabel("Weight × future log return")
    ax.set_ylabel("")
    _add_paper_header(fig, ax, "Case-study trades show which names drove wins and losses", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_paper_aux_summary(frame: pd.DataFrame, *, output_path: Path, subtitle: str) -> None:
    if frame.empty or not {"name", "mean_abs"}.issubset(frame.columns):
        return
    data = frame.copy()
    data["mean_abs"] = pd.to_numeric(data["mean_abs"], errors="coerce")
    data = data.dropna(subset=["mean_abs"]).sort_values("mean_abs", ascending=False).head(24)
    if data.empty:
        return
    plt, sns = _setup_paper_plotting()
    fig_height = max(4.8, 0.36 * len(data) + 2.0)
    fig, ax = plt.subplots(figsize=(11, fig_height), dpi=160)
    sns.barplot(data=data, y="name", x="mean_abs", color=PAPER_TOKENS["olive_mid"], ax=ax)
    ax.set_xlabel("Mean absolute activation")
    ax.set_ylabel("")
    _add_paper_header(fig, ax, "Latent and market-token diagnostics check whether representations collapse", subtitle)
    _finish_paper_axes(ax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_all_paper_figures(
    output_dir: Path,
    *,
    frames: dict[str, pd.DataFrame],
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
                frames.get(frame_name, pd.DataFrame()),
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
            frames.get("time_importance_gradient", pd.DataFrame()),
            output_path=out,
            value_col="grad_x_input_abs",
            subtitle=f"Share of gradient × input by lookback day; {scope}",
        ),
        out,
    )
    out = plot_dir / "feature_correlations_shortcut_checks.png"
    _time_plot(
        "feature_correlations_shortcut_checks_s",
        lambda: _plot_paper_feature_correlations(frames.get("feature_correlations", pd.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    out = plot_dir / "trust_checks.png"
    _time_plot(
        "trust_checks_s",
        lambda: _plot_paper_trust_checks(frames.get("trust_checks", pd.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    out = plot_dir / "regime_analysis.png"
    _time_plot(
        "regime_analysis_s",
        lambda: _plot_paper_regime(frames.get("regime_analysis", pd.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    out = plot_dir / "decision_case_studies.png"
    _time_plot(
        "decision_case_studies_s",
        lambda: _plot_paper_case_studies(frames.get("decision_case_studies", pd.DataFrame()), output_path=out, subtitle=scope),
        out,
    )
    out = plot_dir / "aux_token_diagnostics.png"
    _time_plot(
        "aux_token_diagnostics_s",
        lambda: _plot_paper_aux_summary(frames.get("aux_summary", pd.DataFrame()), output_path=out, subtitle=scope),
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


def _render_frame_markdown(frame: pd.DataFrame, limit: int = 20) -> str:
    if frame is None or frame.empty:
        return "_No rows._"
    try:
        return frame.head(limit).to_markdown(index=False)
    except ImportError:
        return "```text\n" + frame.head(limit).to_string(index=False) + "\n```"


def _paper_executive_summary(
    *,
    frames: dict[str, pd.DataFrame],
    summary: dict[str, Any],
    metadata: dict[str, Any],
) -> list[str]:
    lines: list[str] = []
    portfolio = summary.get("portfolio", {})
    global_table = _global_attribution_table(frames)
    if not global_table.empty:
        top = global_table.iloc[0]
        lines.append(
            f"- The strongest global signal is `{top['feature']}` ({top['feature_group']}); "
            f"mean available attribution share is {_format_share(top.get('mean_available_share', 0.0))}."
        )
    shap = frames.get("feature_importance_shap", pd.DataFrame())
    if shap is not None and not shap.empty:
        row = shap.iloc[0]
        r2 = _safe_float(row.get("surrogate_r2", summary.get("shap_info", {}).get("surrogate_r2", 0.0)))
        lines.append(
            f"- Score-head surrogate SHAP top feature is `{row['feature']}` with surrogate R2={r2:.3f}; "
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
    frames: dict[str, pd.DataFrame],
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
    lines.append(_render_frame_markdown(frames.get("trust_checks", pd.DataFrame()), limit=30))
    lines.append("")
    lines.append("## Global Attribution Table")
    lines.append("")
    lines.append(_render_frame_markdown(_global_attribution_table(frames), limit=20))
    lines.append("")
    lines.append("## Top Feature-Time Cells")
    lines.append("")
    feature_time_tables = []
    for key, method in (
        ("top_feature_time_gradient_cells", "gradient_x_input"),
        ("top_feature_time_integrated_gradients_cells", "integrated_gradients"),
        ("top_feature_time_perturbation_cells", "perturbation_weight_delta"),
    ):
        frame = frames.get(key, pd.DataFrame())
        if frame is not None and not frame.empty:
            chunk = frame.copy()
            chunk.insert(0, "method", method)
            feature_time_tables.append(chunk)
    lines.append(_render_frame_markdown(pd.concat(feature_time_tables, ignore_index=True) if feature_time_tables else pd.DataFrame(), limit=30))
    lines.append("")
    lines.append("## Regime Analysis")
    lines.append("")
    lines.append(_render_frame_markdown(frames.get("regime_analysis", pd.DataFrame()), limit=30))
    lines.append("")
    lines.append("## Decision Case Studies")
    lines.append("")
    lines.append(_render_frame_markdown(frames.get("decision_case_studies", pd.DataFrame()), limit=30))
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


def write_fold_stability_outputs(explainability_root: Path) -> Path | None:
    root = Path(explainability_root)
    fold_dirs = sorted(path for path in root.glob("fold_*_test") if path.is_dir())
    rows: list[pd.DataFrame] = []
    for fold_dir in fold_dirs:
        path = fold_dir / "paper_tables" / "global_feature_attribution.csv"
        if not path.exists():
            fallback = fold_dir / "feature_importance_gradient.csv"
            if not fallback.exists():
                continue
            table = pd.read_csv(fallback)
            if "share" not in table.columns:
                continue
            table = table.rename(columns={"share": "gradient_share"})
            table["mean_available_share"] = table["gradient_share"]
            table["feature_group"] = table["feature"].map(_feature_group)
            table["feature_label"] = table["feature"].map(_feature_label)
        else:
            table = pd.read_csv(path)
        if table.empty or "feature" not in table.columns:
            continue
        fold_id = fold_dir.name.removeprefix("fold_").removesuffix("_test")
        table = table.copy()
        table["fold_id"] = int(fold_id)
        table["rank"] = table["mean_available_share"].rank(ascending=False, method="min") if "mean_available_share" in table.columns else table.index + 1
        rows.append(table)
    if not rows:
        return None
    combined = pd.concat(rows, ignore_index=True)
    summary = (
        combined.groupby("feature", as_index=False)
        .agg(
            folds_present=("fold_id", "nunique"),
            mean_rank=("rank", "mean"),
            std_rank=("rank", "std"),
            mean_share=("mean_available_share", "mean"),
            std_share=("mean_available_share", "std"),
        )
        .sort_values(["mean_rank", "mean_share"], ascending=[True, False])
    )
    summary["feature_group"] = summary["feature"].map(_feature_group)
    summary["feature_label"] = summary["feature"].map(_feature_label)
    output_dir = root / "paper_fold_stability"
    table_dir = output_dir / "paper_tables"
    plot_dir = output_dir / "plots_paper"
    table_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    combined.to_csv(table_dir / "fold_feature_attribution_long.csv", index=False)
    summary.to_csv(table_dir / "fold_feature_stability.csv", index=False)
    plt, sns = _setup_paper_plotting()
    data = summary.head(20).copy()
    if not data.empty:
        fig_height = max(5.0, 0.36 * len(data) + 2.0)
        fig, ax = plt.subplots(figsize=(11.5, fig_height), dpi=160)
        sns.barplot(data=data, y="feature_label", x="mean_share", color=PAPER_TOKENS["blue_mid"], ax=ax)
        ax.set_xlabel("Mean attribution share across folds")
        ax.set_ylabel("")
        ax.xaxis.set_major_formatter(lambda value, _: f"{100.0 * value:.0f}%")
        _add_paper_header(
            fig,
            ax,
            "Fold stability shows whether the same features remain important",
            f"Computed across {combined['fold_id'].nunique()} fold explainability outputs.",
        )
        _finish_paper_axes(ax)
        fig.savefig(plot_dir / "fold_stability_feature_share.png")
        plt.close(fig)
    report = [
        "# Paper Fold Stability Summary",
        "",
        f"- folds: `{combined['fold_id'].nunique()}`",
        f"- features: `{summary['feature'].nunique()}`",
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
    frame: pd.DataFrame,
    *,
    output_path: Path,
    label_col: str,
    value_col: str,
    title: str,
    top_n: int = 30,
) -> None:
    if frame.empty or label_col not in frame.columns or value_col not in frame.columns:
        return
    data = frame[[label_col, value_col]].dropna().copy()
    if data.empty:
        return
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data = data.dropna().sort_values(value_col, ascending=False).head(top_n)
    if data.empty:
        return
    fig_height = max(4.0, 0.28 * len(data) + 1.5)
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, fig_height), dpi=130)
    ax.barh(data[label_col].astype(str)[::-1], data[value_col].to_numpy()[::-1])
    ax.set_title(title)
    ax.set_xlabel(value_col)
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_time_importance(
    frame: pd.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
) -> None:
    if frame.empty or value_col not in frame.columns or "lookback_from_end" not in frame.columns:
        return
    data = frame[["lookback_from_end", value_col]].dropna().copy()
    if data.empty:
        return
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data = data.dropna().sort_values("lookback_from_end")
    if data.empty:
        return
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=130)
    ax.bar(data["lookback_from_end"].astype(str), data[value_col].to_numpy())
    ax.set_title(title)
    ax.set_xlabel("lookback_from_end (0 = latest)")
    ax.set_ylabel(value_col)
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_feature_time_heatmap(
    frame: pd.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
    top_features: int = 30,
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if frame.empty or not required.issubset(frame.columns):
        return
    data = frame[list(required)].dropna().copy()
    if data.empty:
        return
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data = data.dropna()
    if data.empty:
        return
    top = (
        data.groupby("feature")[value_col]
        .sum()
        .sort_values(ascending=False)
        .head(top_features)
        .index
    )
    data = data[data["feature"].isin(top)]
    pivot = data.pivot_table(
        index="feature",
        columns="lookback_from_end",
        values=value_col,
        aggfunc="sum",
        fill_value=0.0,
    )
    pivot = pivot.loc[top]
    if pivot.empty:
        return
    import matplotlib.pyplot as plt

    fig_height = max(4.0, 0.30 * len(pivot) + 1.5)
    fig, ax = plt.subplots(figsize=(9, fig_height), dpi=130)
    image = ax.imshow(pivot.to_numpy(), aspect="auto", interpolation="nearest")
    ax.set_title(title)
    ax.set_xlabel("lookback_from_end (0 = latest)")
    ax.set_ylabel("feature")
    ax.set_xticks(np.arange(pivot.shape[1]))
    ax.set_xticklabels([str(col) for col in pivot.columns])
    ax.set_yticks(np.arange(pivot.shape[0]))
    ax.set_yticklabels([str(idx) for idx in pivot.index])
    fig.colorbar(image, ax=ax, fraction=0.025, pad=0.02)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_feature_time_heatmap_datashader(
    frame: pd.DataFrame,
    *,
    output_path: Path,
    value_col: str,
    title: str,
    top_features: int = 40,
) -> None:
    required = {"feature", "lookback_from_end", value_col}
    if frame.empty or not required.issubset(frame.columns):
        return
    data = frame[list(required)].dropna().copy()
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data["lookback_from_end"] = pd.to_numeric(data["lookback_from_end"], errors="coerce")
    data = data.dropna()
    if data.empty:
        return
    top = (
        data.groupby("feature")[value_col]
        .sum()
        .sort_values(ascending=False)
        .head(top_features)
        .index.astype(str)
        .tolist()
    )
    if not top:
        return
    data["feature"] = data["feature"].astype(str)
    data = data[data["feature"].isin(top)].copy()
    feature_to_y = {feature: len(top) - 1 - idx for idx, feature in enumerate(top)}
    data["feature_y"] = data["feature"].map(feature_to_y)
    data = data.dropna(subset=["feature_y"])
    if data.empty:
        return
    save_heatmap_points_datashader(
        data["lookback_from_end"].to_numpy(dtype=np.float64),
        data["feature_y"].to_numpy(dtype=np.float64),
        data[value_col].to_numpy(dtype=np.float64),
        output_path=output_path,
        title=title,
        x_label="lookback_from_end (0 = latest)",
        y_label=value_col,
        y_labels=[(feature_to_y[feature], feature) for feature in top],
        width=1100,
        height=max(520, min(1400, 24 * len(top) + 180)),
    )


def _plot_feature_correlations(frame: pd.DataFrame, output_path: Path) -> None:
    if frame.empty or "feature" not in frame.columns:
        return
    data = frame.copy()
    if "abs_score_corr" not in data.columns:
        return
    data = data.sort_values("abs_score_corr", ascending=False).head(30)
    if data.empty:
        return
    import matplotlib.pyplot as plt

    labels = (data["source"].astype(str) + ":" + data["feature"].astype(str)).to_numpy()
    y = np.arange(len(data))
    fig_height = max(4.0, 0.28 * len(data) + 1.5)
    fig, ax = plt.subplots(figsize=(10, fig_height), dpi=130)
    ax.barh(y - 0.18, data["score_corr"].to_numpy(), height=0.35, label="score_corr")
    if "weight_corr" in data.columns:
        ax.barh(y + 0.18, data["weight_corr"].to_numpy(), height=0.35, label="weight_corr")
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.axvline(0.0, color="black", linewidth=0.8)
    ax.set_title("Top Simple Feature Correlations")
    ax.set_xlabel("correlation")
    ax.grid(True, axis="x", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_decision_exposure(frame: pd.DataFrame, output_path: Path) -> None:
    if frame.empty or not {"date", "side", "weight"}.issubset(frame.columns):
        return
    data = frame.copy()
    data["abs_weight"] = pd.to_numeric(data["weight"], errors="coerce").abs()
    pivot = data.pivot_table(index="date", columns="side", values="abs_weight", aggfunc="sum", fill_value=0.0)
    if pivot.empty:
        return
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4.8), dpi=130)
    bottom = np.zeros(len(pivot))
    for side in ("long", "short", "flat"):
        if side not in pivot.columns:
            continue
        values = pivot[side].to_numpy()
        ax.bar(np.arange(len(pivot)), values, bottom=bottom, label=side)
        bottom = bottom + values
    ax.set_title("Top Decision Absolute Exposure By Side")
    ax.set_xlabel("sampled date index")
    ax.set_ylabel("sum abs(weight)")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def _plot_decision_exposure_datashader(frame: pd.DataFrame, output_path: Path) -> None:
    if frame.empty or not {"date", "side", "weight"}.issubset(frame.columns):
        return
    data = frame.copy()
    data["abs_weight"] = pd.to_numeric(data["weight"], errors="coerce").abs()
    data = data.dropna(subset=["abs_weight"])
    pivot = data.pivot_table(index="date", columns="side", values="abs_weight", aggfunc="sum", fill_value=0.0)
    if pivot.empty:
        return
    x = np.arange(len(pivot), dtype=np.float64)
    colors = {"long": "#1f77b4", "short": "#d62728", "flat": "#7f7f7f"}
    series = [
        (side, x, pivot[side].to_numpy(dtype=np.float64), colors[side])
        for side in ("long", "short", "flat")
        if side in pivot.columns
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


def _plot_aux_dim_datashader(frame: pd.DataFrame, *, output_path: Path, title: str) -> None:
    if frame.empty or not {"dim", "mean_abs"}.issubset(frame.columns):
        return
    data = frame[["dim", "mean_abs"]].dropna().copy()
    data["dim"] = pd.to_numeric(data["dim"], errors="coerce")
    data["mean_abs"] = pd.to_numeric(data["mean_abs"], errors="coerce")
    data = data.dropna().sort_values("dim")
    if data.empty:
        return
    save_line_series_datashader(
        [("mean_abs", data["dim"].to_numpy(dtype=np.float64), data["mean_abs"].to_numpy(dtype=np.float64), "#2171b5")],
        output_path=output_path,
        title=title,
        y_label="mean_abs",
        width=1000,
        height=420,
    )


def _plot_aux_projection_datashader(frame: pd.DataFrame, *, output_path: Path, title: str) -> None:
    if frame.empty or not {"umap_x", "umap_y"}.issubset(frame.columns):
        return
    data = frame.copy()
    data["umap_x"] = pd.to_numeric(data["umap_x"], errors="coerce")
    data["umap_y"] = pd.to_numeric(data["umap_y"], errors="coerce")
    data = data.dropna(subset=["umap_x", "umap_y"])
    if data.empty:
        return
    colors = {
        "stock": "#1f77b4",
        "token": "#9467bd",
        "time_stock": "#2ca02c",
        "vector": "#ff7f0e",
    }
    series = []
    if "point_type" in data.columns:
        for point_type, group in data.groupby("point_type"):
            color = colors.get(str(point_type), "#17becf")
            series.append(
                (
                    str(point_type),
                    group["umap_x"].to_numpy(dtype=np.float64),
                    group["umap_y"].to_numpy(dtype=np.float64),
                    color,
                )
            )
    else:
        series.append(("points", data["umap_x"].to_numpy(dtype=np.float64), data["umap_y"].to_numpy(dtype=np.float64), "#1f77b4"))
    save_scatter_datashader(series, output_path=output_path, title=title, width=1100, height=760)


def _plot_all_explanation_figures(
    frames: dict[str, pd.DataFrame],
    aux_dim_frames: dict[str, pd.DataFrame],
    output_dir: Path,
    *,
    aux_projection_frames: dict[str, pd.DataFrame] | None = None,
    plot_backend: str = "auto",
) -> list[str]:
    normalized_backend = _normalize_plot_backend(plot_backend)
    use_datashader = _use_datashader_for_explainability(normalized_backend)
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
        frame = frames.get(frame_name, pd.DataFrame())
        out = plot_dir / f"{frame_name}_{value_col}.png"
        _plot_barh(frame, output_path=out, label_col=label_col, value_col=value_col, title=title)
        if out.exists():
            generated.append(out)

    time_specs = [
        ("time_importance_gradient", "grad_x_input_abs", "Gradient x Input By Lookback Day"),
        ("time_importance_integrated_gradients", "integrated_gradients_abs", "Integrated Gradients By Lookback Day"),
    ]
    for frame_name, value_col, title in time_specs:
        out = plot_dir / f"{frame_name}.png"
        _plot_time_importance(frames.get(frame_name, pd.DataFrame()), output_path=out, value_col=value_col, title=title)
        if out.exists():
            generated.append(out)

    heatmap_specs = [
        ("feature_time_gradient", "grad_x_input_abs", "Gradient x Input Feature-Time Heatmap"),
        ("feature_time_integrated_gradients", "integrated_gradients_abs", "Integrated Gradients Feature-Time Heatmap"),
        ("feature_time_perturbation", "weight_abs_delta", "Perturbation Weight Delta Feature-Time Heatmap"),
        ("feature_time_perturbation", "score_abs_delta", "Perturbation Score Delta Feature-Time Heatmap"),
    ]
    for frame_name, value_col, title in heatmap_specs:
        out = plot_dir / f"{frame_name}_{value_col}_heatmap.png"
        frame = frames.get(frame_name, pd.DataFrame())
        if use_datashader:
            try:
                _plot_feature_time_heatmap_datashader(frame, output_path=out, value_col=value_col, title=title)
            except Exception:
                if normalized_backend == "rapids_datashader":
                    raise
                _plot_feature_time_heatmap(frame, output_path=out, value_col=value_col, title=title)
        else:
            _plot_feature_time_heatmap(frame, output_path=out, value_col=value_col, title=title)
        if out.exists():
            generated.append(out)

    out = plot_dir / "feature_correlations.png"
    _plot_feature_correlations(frames.get("feature_correlations", pd.DataFrame()), out)
    if out.exists():
        generated.append(out)

    out = plot_dir / "top_decisions_exposure_by_side.png"
    decision_frame = frames.get("top_decisions", pd.DataFrame())
    if use_datashader:
        try:
            _plot_decision_exposure_datashader(decision_frame, out)
        except Exception:
            if normalized_backend == "rapids_datashader":
                raise
            _plot_decision_exposure(decision_frame, out)
    else:
        _plot_decision_exposure(decision_frame, out)
    if out.exists():
        generated.append(out)

    aux_plot_dir = plot_dir / "aux_dims"
    for name, frame in aux_dim_frames.items():
        out = aux_plot_dir / f"{_safe_plot_filename(name)}.png"
        if use_datashader:
            try:
                _plot_aux_dim_datashader(frame, output_path=out, title=f"Aux Dimension Profile: {name}")
            except Exception:
                if normalized_backend == "rapids_datashader":
                    raise
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

    projection_plot_dir = plot_dir / "aux_umap"
    for name, frame in (aux_projection_frames or {}).items():
        out = projection_plot_dir / f"{_safe_plot_filename(name)}.png"
        if use_datashader:
            _plot_aux_projection_datashader(
                frame,
                output_path=out,
                title=f"cuML UMAP Projection: {name}",
            )
        else:
            if frame.empty or not {"umap_x", "umap_y"}.issubset(frame.columns):
                continue
            import matplotlib.pyplot as plt

            fig, ax = plt.subplots(figsize=(8, 6), dpi=130)
            ax.scatter(frame["umap_x"], frame["umap_y"], s=4, alpha=0.5)
            ax.set_title(f"cuML UMAP Projection: {name}")
            ax.set_xlabel("umap_x")
            ax.set_ylabel("umap_y")
            fig.tight_layout()
            out.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(out)
            plt.close(fig)
        if out.exists():
            generated.append(out)

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
) -> None:
    write_start = time.perf_counter()
    write_timing: dict[str, Any] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = metadata or {}
    frames: dict[str, pd.DataFrame] = result["frames"]
    aux_dim_frames: dict[str, pd.DataFrame] = result.get("aux_dim_frames", {})
    aux_projection_frames: dict[str, pd.DataFrame] = result.get("aux_projection_frames", {})
    stage_start = time.perf_counter()
    for name, frame in frames.items():
        if frame is not None and not frame.empty:
            frame.to_csv(output_dir / f"{name}.csv", index=False)
    aux_dir = output_dir / "aux_dims"
    for name, frame in aux_dim_frames.items():
        aux_dir.mkdir(parents=True, exist_ok=True)
        safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name)
        frame.to_csv(aux_dir / f"{safe_name}.csv", index=False)
    projection_dir = output_dir / "aux_projections"
    for name, frame in aux_projection_frames.items():
        if frame is not None and not frame.empty:
            projection_dir.mkdir(parents=True, exist_ok=True)
            safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name)
            frame.to_csv(projection_dir / f"{safe_name}.csv", index=False)
    _mark_elapsed(write_timing, "csv_s", stage_start)
    stage_start = time.perf_counter()
    plots_generated = (
        _plot_all_explanation_figures(
            frames,
            aux_dim_frames,
            output_dir,
            aux_projection_frames=aux_projection_frames,
            plot_backend=plot_backend,
        )
        if write_plots and write_standard_plots
        else []
    )
    _mark_elapsed(write_timing, "plots_s", stage_start)
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
    dataset = _dataset_for_split(
        context.panel,
        context.fold,
        split,
        context.config.training.lookback,
        first_test_year_only=settings.first_test_year_only,
    )
    batch, date_indices = _sample_dataset(dataset, settings.max_rows, settings.sample_method)
    dates = [str(np.datetime_as_string(context.panel.dates[int(idx)], unit="D")) for idx in date_indices]
    result = explain_batch(
        model,
        batch,
        feature_names=context.panel.feature_names,
        symbols=context.panel.symbols,
        dates=dates,
        settings=settings,
        device=device,
    )
    destination = explain_output_dir or (
        context.output_dir
        / "explainability"
        / f"fold_{int(context.fold.fold_id):02d}_{split.strip().lower()}"
    )
    metadata = {
        "model_name": context.config.training.model_name,
        "fold_id": int(context.fold.fold_id),
        "split": split,
        "checkpoint": str(context.checkpoint_path),
        "device": str(device),
        "sample_rows": int(len(dates)),
        "first_test_year_only": bool(settings.first_test_year_only),
        "config_lookback": int(context.config.training.lookback),
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
        **checkpoint_info,
    }
    resolved_plot_backend = plot_backend or str(getattr(context.config.training, "plot_backend", "auto"))
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
    if bool(settings.cross_asset_enabled):
        from stockagent.explainability_cross_asset import abstract_cross_asset_transmission

        abstract_cross_asset_transmission(
            model,
            batch,
            feature_names=context.panel.feature_names,
            symbols=context.panel.symbols,
            dates=dates,
            output_dir=destination,
            settings=_cross_asset_settings_from_explainability(settings),
            device=device,
        )
    destination_out = destination
    del result, batch, dataset, model
    _clear_explainability_runtime_cache()
    return destination_out


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
    parser.add_argument("--no-perturb", action="store_true", help="Skip feature perturbation sensitivity.")
    parser.add_argument("--perturb-batch-size", default=0, type=int, help="Batch feature-day perturbations together; 0 selects an automatic safe chunk size.")
    parser.add_argument("--perturb-max-auto-batch-size", default=16, type=int)
    parser.add_argument("--perturb-max-input-elements", default=32_000_000, type=int)
    parser.add_argument("--no-plots", action="store_true", help="Skip PNG plot generation.")
    parser.add_argument("--report-style", default="paper", choices=("paper", "standard", "none"))
    parser.add_argument("--plot-theme", default="paper", choices=("paper", "standard"))
    parser.add_argument(
        "--standard-plots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write the legacy plots/ PNG set in addition to paper plots.",
    )
    parser.add_argument("--no-interactive-plots", action="store_true", help="Keep explainability output static only.")
    parser.add_argument("--no-shap", action="store_true", help="Skip score-head surrogate SHAP.")
    parser.add_argument("--shap-mode", default="score_head_surrogate", choices=("score_head_surrogate", "off", "none"))
    parser.add_argument("--case-study-top-k", default=5, type=int)
    parser.add_argument("--no-regime-analysis", action="store_true", help="Skip regime-analysis tables and plots.")
    parser.add_argument("--no-fold-stability", action="store_true", help="Skip cross-fold attribution-stability summary.")
    parser.add_argument(
        "--plot-backend",
        default=None,
        choices=("auto", "matplotlib", "rapids_datashader"),
        help="PNG plot backend. auto uses RAPIDS Datashader for dense plots when CUDA is available.",
    )
    parser.add_argument("--no-umap", action="store_true", help="Skip cuML UMAP aux projections.")
    parser.add_argument("--umap-max-points", default=10000, type=int)
    parser.add_argument("--umap-max-projections", default=0, type=int, help="Maximum aux tensors to project with UMAP; 0 means no limit.")
    parser.add_argument("--umap-n-neighbors", default=15, type=int)
    parser.add_argument("--umap-min-dist", default=0.1, type=float)
    parser.add_argument(
        "--cross-asset",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write abstract_cross_asset_transmission outputs. Use --no-cross-asset to disable.",
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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = ExplainabilitySettings(
        top_k=args.top_k,
        max_rows=args.max_rows,
        ig_steps=args.ig_steps,
        ig_batch_size=args.ig_batch_size,
        perturb=not args.no_perturb,
        perturb_batch_size=args.perturb_batch_size,
        perturb_max_auto_batch_size=args.perturb_max_auto_batch_size,
        perturb_max_input_elements=args.perturb_max_input_elements,
        sample_method=args.sample_method,
        first_test_year_only=not args.all_test_years,
        report_style=args.report_style,
        plot_theme=args.plot_theme,
        standard_plots=bool(args.standard_plots),
        interactive_plots=not args.no_interactive_plots,
        shap_enabled=not args.no_shap,
        shap_mode=args.shap_mode,
        case_study_top_k=args.case_study_top_k,
        regime_analysis=not args.no_regime_analysis,
        fold_stability=not args.no_fold_stability,
        umap_enabled=not args.no_umap,
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

        print(f"explaining folds: {fold_ids}")
        for fold_id in fold_ids:
            fold_output_dir = args.explain_output_dir
            if fold_output_dir is not None:
                fold_output_dir = Path(fold_output_dir) / f"fold_{int(fold_id):02d}_{args.split.strip().lower()}"
            try:
                out_dir = run_checkpoint_explanation(
                    config_path=args.config,
                    output_dir=args.output_dir,
                    fold_id=fold_id,
                    checkpoint=None,
                    split=args.split,
                    explain_output_dir=fold_output_dir,
                    settings=settings,
                    device_override=args.device,
                    strict=args.strict,
                    write_plots=not args.no_plots,
                    plot_backend=args.plot_backend,
                )
            finally:
                _clear_explainability_runtime_cache()
            print(f"explainability output (fold {fold_id}): {out_dir}")
        if settings.fold_stability:
            stability_dir = write_fold_stability_outputs(resolved_output_dir / "explainability")
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
        strict=args.strict,
        write_plots=not args.no_plots,
        plot_backend=args.plot_backend,
    )
    print(f"explainability output: {out_dir}")


if __name__ == "__main__":
    main()
