from __future__ import annotations

import argparse
import inspect
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import torch
from torch import nn

from stockagent.config import ExperimentConfig, load_config
from stockagent.data.panel import PanelData, build_panel
from stockagent.data.walkforward import WalkForwardFold, build_expanding_year_folds
from stockagent.models.factory import build_model
from stockagent.training.dataset import CrossSectionalDataset, collate_batch


@dataclass(slots=True)
class ExplainabilitySettings:
    top_k: int = 20
    max_rows: int = 32
    ig_steps: int = 8
    perturb: bool = True
    sample_method: str = "even"
    first_test_year_only: bool = True


@dataclass(slots=True)
class LoadedExplanationContext:
    config: ExperimentConfig
    panel: PanelData
    folds: list[WalkForwardFold]
    fold: WalkForwardFold
    split: str
    checkpoint_path: Path
    output_dir: Path


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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(out):
        return default
    return out


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
    _, scores, _ = _forward_outputs(model, x_grad, mask, return_aux=True)
    target = _decision_target(scores, selected, direction)
    grad = torch.autograd.grad(target, x_grad, retain_graph=False, create_graph=False)[0]
    return torch.nan_to_num((grad * x_grad).detach(), nan=0.0, posinf=0.0, neginf=0.0)


def _integrated_gradients_attribution(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    selected: torch.Tensor,
    direction: torch.Tensor,
    steps: int,
) -> torch.Tensor:
    steps = max(0, int(steps))
    if steps <= 0:
        return torch.zeros_like(x)
    baseline = torch.zeros_like(x)
    total_grad = torch.zeros_like(x)
    for step in range(1, steps + 1):
        alpha = float(step) / float(steps)
        x_step = (baseline + alpha * (x - baseline)).detach().requires_grad_(True)
        _, scores, _ = _forward_outputs(model, x_step, mask, return_aux=True)
        target = _decision_target(scores, selected, direction)
        grad = torch.autograd.grad(target, x_step, retain_graph=False, create_graph=False)[0]
        total_grad = total_grad + torch.nan_to_num(grad.detach(), nan=0.0, posinf=0.0, neginf=0.0)
    return (x - baseline) * (total_grad / float(steps))


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
                    metric_name: float(values[time_idx, feat_idx]),
                }
            )
    return pd.DataFrame(rows)


def _feature_summary_frame(feature_time: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    if feature_time.empty:
        return pd.DataFrame(columns=["feature", metric_name, "share"])
    summary = feature_time.groupby("feature", as_index=False)[metric_name].sum()
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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    with torch.no_grad():
        for time_idx in range(int(x.size(1))):
            for feat_idx, feature in enumerate(feature_names):
                x_perturbed = x.clone()
                x_perturbed[:, time_idx, :, feat_idx] = 0.0
                weights_p, scores_p, _ = _forward_outputs(model, x_perturbed, mask, return_aux=False)
                rows.append(
                    {
                        "lookback_index": int(time_idx),
                        "lookback_from_end": int(x.size(1) - 1 - time_idx),
                        "feature": feature,
                        "weight_abs_delta": float((weights_p - base_weights).abs().mean().detach().cpu()),
                        "score_abs_delta": float((scores_p - base_scores).abs().mean().detach().cpu()),
                    }
                )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame, pd.DataFrame()
    summary = frame.groupby("feature", as_index=False)[["weight_abs_delta", "score_abs_delta"]].sum()
    total = float(summary["weight_abs_delta"].sum())
    summary["weight_delta_share"] = summary["weight_abs_delta"] / total if total > 0.0 else 0.0
    summary = summary.sort_values("weight_abs_delta", ascending=False)
    return frame, summary


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
    settings = settings or ExplainabilitySettings()
    device = device or next(model.parameters()).device
    model.eval()
    batch = _move_batch(batch, device)
    x = torch.nan_to_num(batch["x"].float(), nan=0.0, posinf=0.0, neginf=0.0)
    returns = torch.nan_to_num(batch["future_log_returns"].float(), nan=0.0, posinf=0.0, neginf=0.0)
    mask = batch["tradable_mask"].to(device=device, dtype=torch.bool)

    with torch.no_grad():
        weights, scores, aux = _forward_outputs(model, x, mask, return_aux=True)
    selected, direction = _selection_from_weights(weights.detach(), mask, settings.top_k)

    grad_attr = _gradient_x_input_attribution(model, x, mask, selected, direction)
    grad_ft = _feature_time_frame(grad_attr, feature_names, "grad_x_input_abs")
    grad_feature = _feature_summary_frame(grad_ft, "grad_x_input_abs")
    grad_time = _time_summary_frame(grad_ft, "grad_x_input_abs")

    if int(settings.ig_steps) > 0:
        ig_attr = _integrated_gradients_attribution(model, x, mask, selected, direction, settings.ig_steps)
        ig_ft = _feature_time_frame(ig_attr, feature_names, "integrated_gradients_abs")
        ig_feature = _feature_summary_frame(ig_ft, "integrated_gradients_abs")
        ig_time = _time_summary_frame(ig_ft, "integrated_gradients_abs")
    else:
        ig_ft = pd.DataFrame()
        ig_feature = pd.DataFrame()
        ig_time = pd.DataFrame()

    if settings.perturb:
        perturb_ft, perturb_feature = _perturbation_importance(model, x, mask, weights, scores, feature_names)
    else:
        perturb_ft = pd.DataFrame()
        perturb_feature = pd.DataFrame()

    corr = _feature_correlations(x, scores, weights, mask, feature_names)
    decisions = _decision_rows(weights, scores, returns, mask, dates, symbols, settings.top_k)
    stock_contrib = _stock_contribution_frame(weights, returns, mask, symbols)
    portfolio = _portfolio_summary(weights, returns, mask)
    aux_frame, aux_dim_frames = _aux_summary(aux)
    warnings = _make_warnings(portfolio, grad_feature, grad_time, corr, aux_frame)

    return {
        "summary": {
            "portfolio": portfolio,
            "rows": len(dates),
            "top_k": int(settings.top_k),
            "ig_steps": int(settings.ig_steps),
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
            "feature_correlations": corr,
            "top_decisions": decisions,
            "stock_contributions": stock_contrib,
            "aux_summary": aux_frame,
        },
        "aux_dim_frames": aux_dim_frames,
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
            "- Plausibility warnings: concentration, exposure, turnover proxy, single-feature dominance, simple feature correlations.",
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


def _plot_all_explanation_figures(
    frames: dict[str, pd.DataFrame],
    aux_dim_frames: dict[str, pd.DataFrame],
    output_dir: Path,
) -> list[str]:
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
        _plot_feature_time_heatmap(frames.get(frame_name, pd.DataFrame()), output_path=out, value_col=value_col, title=title)
        if out.exists():
            generated.append(out)

    out = plot_dir / "feature_correlations.png"
    _plot_feature_correlations(frames.get("feature_correlations", pd.DataFrame()), out)
    if out.exists():
        generated.append(out)

    out = plot_dir / "top_decisions_exposure_by_side.png"
    _plot_decision_exposure(frames.get("top_decisions", pd.DataFrame()), out)
    if out.exists():
        generated.append(out)

    aux_plot_dir = plot_dir / "aux_dims"
    for name, frame in aux_dim_frames.items():
        out = aux_plot_dir / f"{_safe_plot_filename(name)}.png"
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

    return [str(path.relative_to(output_dir)) for path in generated]


def write_explanation_outputs(
    result: dict[str, Any],
    output_dir: Path,
    *,
    metadata: dict[str, Any] | None = None,
    write_plots: bool = True,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = metadata or {}
    frames: dict[str, pd.DataFrame] = result["frames"]
    aux_dim_frames: dict[str, pd.DataFrame] = result.get("aux_dim_frames", {})
    for name, frame in frames.items():
        if frame is not None and not frame.empty:
            frame.to_csv(output_dir / f"{name}.csv", index=False)
    aux_dir = output_dir / "aux_dims"
    for name, frame in aux_dim_frames.items():
        aux_dir.mkdir(parents=True, exist_ok=True)
        safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name)
        frame.to_csv(aux_dir / f"{safe_name}.csv", index=False)
    plots_generated = (
        _plot_all_explanation_figures(frames, aux_dim_frames, output_dir)
        if write_plots
        else []
    )
    summary = {**result["summary"], "metadata": metadata, "plots_generated": plots_generated}
    (output_dir / "summary.json").write_text(
        json.dumps(_to_builtin(summary), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_markdown_report(
        output_dir / "report.md",
        metadata=metadata,
        summary=summary,
        frames=frames,
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
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
        **checkpoint_info,
    }
    write_explanation_outputs(result, destination, metadata=metadata, write_plots=write_plots)
    return destination


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
    parser.add_argument("--sample-method", default="even", choices=("even", "first", "last"))
    parser.add_argument("--all-test-years", action="store_true", help="For --split test, explain all test years instead of only the first test year.")
    parser.add_argument("--no-perturb", action="store_true", help="Skip feature perturbation sensitivity.")
    parser.add_argument("--no-plots", action="store_true", help="Skip PNG plot generation.")
    parser.add_argument("--strict", action="store_true", help="Load checkpoint with strict=True.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = ExplainabilitySettings(
        top_k=args.top_k,
        max_rows=args.max_rows,
        ig_steps=args.ig_steps,
        perturb=not args.no_perturb,
        sample_method=args.sample_method,
        first_test_year_only=not args.all_test_years,
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
            )
            print(f"explainability output (fold {fold_id}): {out_dir}")
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
    )
    print(f"explainability output: {out_dir}")


if __name__ == "__main__":
    main()
