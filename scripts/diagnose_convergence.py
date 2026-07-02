#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.models.factory import build_model
from stockagent.models.normalization import (
    masked_activation_l1_weights,
    masked_l1_projection_weights,
    masked_signed_action_weights,
    normalize_portfolio_activation,
)
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import (
    _align_panel_to_state_dict_universe,
    _autocast_context,
    _call_model,
    _compute_metrics_from_tensors,
    _configure_backtest_runtime_from_config,
    _extract_weights_and_aux,
    _load_checkpoint,
    _load_state_dict,
    _resolve_amp_dtype,
    _resolve_device,
    _training_loss_portfolio_activation,
)
from stockagent.training.windowed import WindowedSplitTensors, dataset_to_windowed_tensors


DEFAULT_JACOBIAN_MODES = "activation_l1,l1,signed_softmax,signed_entmax15,signed_sparsemax,projection_l1"


def _parse_csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _to_builtin(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _to_builtin(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_builtin(item) for item in value]
    if isinstance(value, np.ndarray):
        return [_to_builtin(item) for item in value.tolist()]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, torch.Tensor):
        if value.numel() == 1:
            return _to_builtin(value.detach().cpu().item())
        return _to_builtin(value.detach().cpu().tolist())
    return value


def _finite_float(value: Any, default: float = float("nan")) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _configure_runtime(config: Any, *, backtest_compile: bool, reduced_log_utility: bool) -> None:
    config.training.backtest_compile = bool(backtest_compile)
    config.training.backtest_autotune = bool(backtest_compile)
    config.training.backtest_compile_stateful = bool(backtest_compile)
    config.training.backtest_compile_dynamic = False
    os.environ["STOCKAGENT_LOSS_REDUCED_LOG_UTILITY"] = "1" if reduced_log_utility else "0"
    os.environ["STOCKAGENT_COMPILE_LOSS"] = "0"
    _configure_backtest_runtime_from_config(config)
    if torch.cuda.is_available():
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        torch.backends.cudnn.benchmark = True


def _select_indices(total_rows: int, rows: int, method: str, seed: int) -> torch.Tensor:
    total_rows = int(total_rows)
    rows = max(1, min(int(rows), total_rows))
    method = str(method).strip().lower()
    if total_rows <= 0:
        raise ValueError("Selected split has no rows after lookback filtering")
    if method == "first":
        values = np.arange(rows, dtype=np.int64)
    elif method == "last":
        values = np.arange(total_rows - rows, total_rows, dtype=np.int64)
    elif method == "even":
        values = np.unique(np.linspace(0, total_rows - 1, num=rows, dtype=np.int64))
        if values.size < rows:
            missing = rows - values.size
            tail = np.setdiff1d(np.arange(total_rows, dtype=np.int64), values, assume_unique=True)[-missing:]
            values = np.sort(np.concatenate([values, tail]))
    elif method == "random":
        rng = np.random.default_rng(int(seed))
        values = np.sort(rng.choice(total_rows, size=rows, replace=False).astype(np.int64))
    else:
        raise ValueError("sample method must be one of: first, last, even, random")
    return torch.as_tensor(values, dtype=torch.long)


def _split_indices(fold: Any, split: str) -> np.ndarray:
    split = str(split).strip().lower()
    if split == "train":
        return fold.train_indices
    if split == "val":
        return fold.val_indices
    if split == "test":
        return fold.test_indices
    raise ValueError("split must be one of train, val, or test")


def _batch_from_windowed(
    split: WindowedSplitTensors,
    row_indices: torch.Tensor,
    *,
    device: torch.device,
    non_blocking: bool,
) -> dict[str, torch.Tensor]:
    return split.batch_by_batch_indices(row_indices, device=device, non_blocking=non_blocking)


def _date_metadata(panel: Any, split: WindowedSplitTensors, row_indices: torch.Tensor) -> dict[str, Any]:
    date_indices = split.valid_indices[row_indices].detach().cpu().numpy().astype(np.int64)
    dates = [str(panel.dates[int(idx)]) for idx in date_indices]
    return {
        "row_count": int(row_indices.numel()),
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
        "date_indices": date_indices.tolist(),
        "dates": dates,
    }


def _loss_kwargs(config: Any, *, loss_portfolio_activation: str) -> dict[str, Any]:
    mt = config.training.multitask_loss
    fg = config.training.factor_generalization_loss
    ae = config.training.portfolio_autoencoder_loss
    ev = config.evaluation
    return {
        "long_only": bool(config.trading.long_only),
        "buy_fee_rate": float(config.trading.buy_fee_rate),
        "sell_fee_rate": float(config.trading.sell_fee_rate),
        "max_turnover_ratio": float(config.trading.max_turnover_ratio),
        "gross_leverage": 1.0,
        "min_trade_weight": float(config.trading.min_trade_weight),
        "portfolio_activation": loss_portfolio_activation,
        "gamma_sharpe": float(ev.gamma_sharpe),
        "gamma_excess": float(ev.gamma_excess),
        "gamma_cvar": float(ev.gamma_cvar),
        "cvar_alpha": float(ev.cvar_alpha),
        "gamma_drawdown": float(ev.gamma_drawdown),
        "drawdown_target": float(ev.drawdown_target),
        "gamma_turnover": float(ev.gamma_turnover),
        "gamma_underperformance": float(ev.gamma_underperformance),
        "excess_target": float(ev.excess_target),
        "cvar_budget": float(ev.cvar_budget),
        "drawdown_budget": float(ev.drawdown_budget),
        "turnover_budget": float(ev.turnover_budget),
        "gamma_cvar_budget": float(ev.gamma_cvar_budget),
        "gamma_drawdown_budget": float(ev.gamma_drawdown_budget),
        "gamma_turnover_budget": float(ev.gamma_turnover_budget),
        "objective": str(config.training.loss_type),
        "rank_ic_weight": float(mt.rank_ic_weight),
        "return_rank_ic_weight": float(mt.return_rank_ic_weight),
        "direction_weight": float(mt.direction_weight),
        "volatility_regime_weight": float(mt.volatility_regime_weight),
        "concentration_weight": float(mt.concentration_weight),
        "regime_up_threshold": float(mt.regime_up_threshold),
        "regime_down_threshold": float(mt.regime_down_threshold),
        "factor_slope_tstat_weight": float(fg.slope_tstat_weight),
        "factor_rank_ic_weight": float(fg.rank_ic_weight),
        "factor_sharpe_weight": float(fg.factor_sharpe_weight),
        "factor_block_stability_weight": float(fg.block_stability_weight),
        "factor_regime_stability_weight": float(fg.regime_stability_weight),
        "factor_consistency_weight": float(fg.consistency_weight),
        "factor_net_exposure_weight": float(fg.net_exposure_weight),
        "factor_gross_exposure_weight": float(fg.gross_exposure_weight),
        "factor_concentration_weight": float(fg.concentration_weight),
        "factor_turnover_weight": float(fg.turnover_weight),
        "factor_score_l2_weight": float(fg.score_l2_weight),
        "factor_temperature": float(fg.factor_temperature),
        "factor_block_count": int(fg.block_count),
        "factor_worst_fraction": float(fg.worst_fraction),
        "autoencoder_cost_rate": float(ae.cost_rate),
        "autoencoder_lambda_turnover": float(ae.lambda_turnover),
        "autoencoder_lambda_concentration": float(ae.lambda_concentration),
        "autoencoder_lambda_latent": float(ae.lambda_latent),
    }


def _forward_loss(
    *,
    model: torch.nn.Module,
    batch: dict[str, torch.Tensor],
    config: Any,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    loss_portfolio_activation: str,
    return_aux: bool,
    retain_output_grad: bool = False,
) -> tuple[torch.Tensor, torch.Tensor, dict[str, torch.Tensor] | None]:
    with _autocast_context(device, amp_dtype):
        model_output = _call_model(model, batch["x"], batch["tradable_mask"], return_aux=return_aux)
        weights, aux = _extract_weights_and_aux(model_output)
        if retain_output_grad and weights.requires_grad:
            weights.retain_grad()
        loss = risk_aware_loss(
            weights,
            batch["future_log_returns"],
            batch["tradable_mask"],
            benchmark_returns=batch["benchmark"],
            can_buy_mask=batch["can_buy_mask"],
            can_sell_mask=batch["can_sell_mask"],
            sample_mask=batch.get("sample_mask"),
            aux_outputs=aux,
            **_loss_kwargs(config, loss_portfolio_activation=loss_portfolio_activation),
        )
    return loss, weights, aux


def _model_norms(model: torch.nn.Module) -> dict[str, float]:
    param_sq = 0.0
    grad_sq = 0.0
    grad_abs_max = 0.0
    grad_nonfinite = 0
    grad_zero = 0
    grad_count = 0
    top: list[dict[str, Any]] = []
    for name, param in model.named_parameters():
        if not param.requires_grad:
            continue
        param_f = param.detach().float()
        param_sq += float(param_f.square().sum().cpu())
        grad = param.grad
        if grad is None:
            continue
        grad_f = grad.detach().float()
        finite = torch.isfinite(grad_f)
        grad_count += int(grad_f.numel())
        grad_nonfinite += int((~finite).sum().detach().cpu().item())
        clean = torch.nan_to_num(grad_f, nan=0.0, posinf=0.0, neginf=0.0)
        gnorm = float(torch.linalg.vector_norm(clean).detach().cpu())
        grad_sq += gnorm * gnorm
        grad_abs_max = max(grad_abs_max, float(clean.abs().max().detach().cpu()) if clean.numel() else 0.0)
        grad_zero += int((clean.abs() <= 1e-12).sum().detach().cpu().item())
        top.append(
            {
                "name": name,
                "grad_norm": gnorm,
                "param_norm": float(torch.linalg.vector_norm(param_f).detach().cpu()),
                "grad_abs_max": float(clean.abs().max().detach().cpu()) if clean.numel() else 0.0,
            }
        )
    param_norm = math.sqrt(max(param_sq, 0.0))
    grad_norm = math.sqrt(max(grad_sq, 0.0))
    top.sort(key=lambda row: float(row["grad_norm"]), reverse=True)
    return {
        "param_norm": param_norm,
        "grad_norm": grad_norm,
        "grad_to_param_ratio": grad_norm / max(param_norm, 1e-12),
        "grad_abs_max": grad_abs_max,
        "grad_nonfinite_count": grad_nonfinite,
        "grad_element_count": grad_count,
        "grad_nonfinite_ratio": grad_nonfinite / max(1, grad_count),
        "grad_zero_ratio": grad_zero / max(1, grad_count),
        "top_grad_parameters": top[:10],
    }


def _snapshot_params(model: torch.nn.Module) -> list[torch.Tensor]:
    return [param.detach().clone() for param in model.parameters() if param.requires_grad]


def _update_norm(model: torch.nn.Module, before: list[torch.Tensor]) -> float:
    sq = 0.0
    idx = 0
    for param in model.parameters():
        if not param.requires_grad:
            continue
        delta = param.detach().float() - before[idx].to(device=param.device, dtype=torch.float32)
        sq += float(delta.square().sum().detach().cpu())
        idx += 1
    return math.sqrt(max(sq, 0.0))


def _weight_diagnostics(weights: torch.Tensor, eps: float = 1e-12) -> dict[str, float]:
    if weights.numel() == 0:
        return {
            "avg_positions": 0.0,
            "avg_gross": 0.0,
            "avg_long_gross": 0.0,
            "avg_short_gross": 0.0,
            "avg_net": 0.0,
            "avg_max_abs_weight": 0.0,
            "avg_hhi": 0.0,
        }
    w = torch.nan_to_num(weights.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
    abs_w = w.abs()
    gross = abs_w.sum(dim=1)
    norm_abs = abs_w / gross.clamp_min(float(eps)).unsqueeze(1)
    return {
        "avg_positions": float((abs_w > 0.0).sum(dim=1).float().mean().detach().cpu()),
        "avg_gross": float(gross.mean().detach().cpu()),
        "avg_long_gross": float(w.clamp_min(0.0).sum(dim=1).mean().detach().cpu()),
        "avg_short_gross": float((-w.clamp_max(0.0)).sum(dim=1).mean().detach().cpu()),
        "avg_net": float(w.sum(dim=1).mean().detach().cpu()),
        "avg_max_abs_weight": float(abs_w.max(dim=1).values.mean().detach().cpu()),
        "avg_hhi": float(norm_abs.square().sum(dim=1).mean().detach().cpu()),
    }


def _gradient_audit(
    *,
    model: torch.nn.Module,
    batch: dict[str, torch.Tensor],
    config: Any,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    loss_portfolio_activation: str,
    return_aux: bool,
) -> dict[str, Any]:
    model.train()
    model.zero_grad(set_to_none=True)
    loss, weights, _ = _forward_loss(
        model=model,
        batch=batch,
        config=config,
        device=device,
        amp_dtype=amp_dtype,
        loss_portfolio_activation=loss_portfolio_activation,
        return_aux=return_aux,
        retain_output_grad=True,
    )
    loss.float().backward()
    stats = _model_norms(model)
    output_grad = weights.grad
    if output_grad is not None:
        output_clean = torch.nan_to_num(output_grad.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
        stats.update(
            {
                "output_grad_norm": float(torch.linalg.vector_norm(output_clean).detach().cpu()),
                "output_grad_abs_mean": float(output_clean.abs().mean().detach().cpu()),
                "output_grad_abs_max": float(output_clean.abs().max().detach().cpu()),
            }
        )
    else:
        stats.update(
            {
                "output_grad_norm": None,
                "output_grad_abs_mean": None,
                "output_grad_abs_max": None,
            }
        )
    stats["loss"] = float(loss.detach().float().cpu())
    stats["weights"] = _weight_diagnostics(weights)
    model.zero_grad(set_to_none=True)
    return stats


def _one_batch_overfit(
    *,
    model: torch.nn.Module,
    batch: dict[str, torch.Tensor],
    config: Any,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    loss_portfolio_activation: str,
    return_aux: bool,
    steps: int,
    learning_rate: float,
    weight_decay: float,
    grad_clip_norm: float,
    log_every: int,
) -> dict[str, Any]:
    optimizer = torch.optim.AdamW(model.parameters(), lr=float(learning_rate), weight_decay=float(weight_decay))
    rows: list[dict[str, Any]] = []
    losses: list[float] = []
    started = time.perf_counter()
    steps = max(1, int(steps))
    log_every = max(1, int(log_every))

    model.train()
    for step in range(1, steps + 1):
        optimizer.zero_grad(set_to_none=True)
        record = step == 1 or step == steps or step % log_every == 0
        before = _snapshot_params(model) if record else []
        loss, weights, _ = _forward_loss(
            model=model,
            batch=batch,
            config=config,
            device=device,
            amp_dtype=amp_dtype,
            loss_portfolio_activation=loss_portfolio_activation,
            return_aux=return_aux,
            retain_output_grad=False,
        )
        loss_f = loss.float()
        if not torch.isfinite(loss_f):
            rows.append({"step": step, "loss": float(loss_f.detach().cpu()), "error": "non_finite_loss"})
            break
        loss_f.backward()
        grad_stats = _model_norms(model)
        clip_return = None
        if float(grad_clip_norm) > 0.0:
            clip_return = torch.nn.utils.clip_grad_norm_(
                model.parameters(),
                max_norm=float(grad_clip_norm),
                error_if_nonfinite=False,
            )
        optimizer.step()
        if device.type == "cuda":
            torch.cuda.synchronize(device)
        loss_value = float(loss_f.detach().cpu())
        losses.append(loss_value)
        if record:
            param_norm_after = _model_norms(model)["param_norm"]
            update_norm = _update_norm(model, before) if before else float("nan")
            rows.append(
                {
                    "step": step,
                    "loss": loss_value,
                    "grad_norm": grad_stats["grad_norm"],
                    "grad_to_param_ratio": grad_stats["grad_to_param_ratio"],
                    "clip_total_norm": None if clip_return is None else float(clip_return.detach().cpu()),
                    "update_norm": update_norm,
                    "update_to_param_ratio": update_norm / max(param_norm_after, 1e-12),
                    "output_avg_gross": _weight_diagnostics(weights)["avg_gross"],
                }
            )

    elapsed_s = time.perf_counter() - started
    first_loss = losses[0] if losses else float("nan")
    last_loss = losses[-1] if losses else float("nan")
    best_loss = min(losses) if losses else float("nan")
    return {
        "steps_requested": int(steps),
        "steps_completed": int(len(losses)),
        "elapsed_s": float(elapsed_s),
        "first_loss": first_loss,
        "last_loss": last_loss,
        "best_loss": best_loss,
        "absolute_loss_delta": last_loss - first_loss if losses else float("nan"),
        "relative_loss_delta": (last_loss - first_loss) / max(abs(first_loss), 1e-12) if losses else float("nan"),
        "records": rows,
    }


def _mode_weights(mode: str, scores: torch.Tensor, mask: torch.Tensor, *, long_only: bool, activation: str) -> torch.Tensor:
    mode = str(mode).strip().lower().replace("-", "_")
    if mode in {"activation_l1", "activated_l1"}:
        return masked_activation_l1_weights(scores, mask, long_only=long_only, activation=activation)
    if mode in {"l1", "raw_l1", "identity_l1", "logits"}:
        return masked_activation_l1_weights(scores, mask, long_only=long_only, activation="identity")
    if mode in {"signed_softmax", "softmax"}:
        return masked_signed_action_weights(scores, mask, transform="softmax", long_only=long_only)
    if mode in {"signed_entmax", "signed_entmax15", "entmax", "entmax15"}:
        return masked_signed_action_weights(scores, mask, transform="entmax15", long_only=long_only)
    if mode in {"signed_sparsemax", "sparsemax"}:
        return masked_signed_action_weights(scores, mask, transform="sparsemax", long_only=long_only)
    if mode in {"projection_l1", "l1_projection", "project_l1"}:
        return masked_l1_projection_weights(scores, mask, long_only=long_only, radius=1.0)
    raise ValueError(f"Unsupported jacobian mode: {mode}")


def _jacobian_probe(
    *,
    batch: dict[str, torch.Tensor],
    modes: list[str],
    sources: list[str],
    seed: int,
    long_only: bool,
    activation: str,
    score_scale: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    mask = batch["tradable_mask"].bool()
    returns = torch.nan_to_num(batch["future_log_returns"].float(), nan=0.0, posinf=0.0, neginf=0.0)
    device = returns.device
    generator = torch.Generator(device="cpu")
    generator.manual_seed(int(seed))
    random_cpu = torch.randn(tuple(returns.shape), generator=generator, dtype=torch.float32)
    random_scores = random_cpu.to(device=device) * float(score_scale)
    zero_scores = torch.zeros_like(returns)

    for source in sources:
        source_norm = str(source).strip().lower()
        base = zero_scores if source_norm == "zero" else random_scores
        for mode in modes:
            scores = base.detach().clone().requires_grad_(True)
            weights = _mode_weights(
                mode,
                scores,
                mask,
                long_only=long_only,
                activation=activation,
            )
            objective = (weights * returns).sum(dim=1).mean()
            objective.backward()
            grad = torch.nan_to_num(scores.grad.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
            rows.append(
                {
                    "source": source_norm,
                    "mode": mode,
                    "objective": float(objective.detach().cpu()),
                    "score_grad_norm": float(torch.linalg.vector_norm(grad).detach().cpu()),
                    "score_grad_abs_mean": float(grad.abs().mean().detach().cpu()),
                    "score_grad_abs_max": float(grad.abs().max().detach().cpu()),
                    **_weight_diagnostics(weights),
                }
            )
    return rows


def _backtest_from_weights(
    *,
    weights: torch.Tensor,
    batch: dict[str, torch.Tensor],
    config: Any,
    portfolio_activation: str,
    return_weights_history: bool,
):
    return run_backtest_torch(
        weights,
        batch["future_log_returns"],
        batch["tradable_mask"].bool(),
        batch["benchmark"],
        buy_fee_rate=float(config.trading.buy_fee_rate),
        sell_fee_rate=float(config.trading.sell_fee_rate),
        long_only=bool(config.trading.long_only),
        max_turnover_ratio=float(config.trading.max_turnover_ratio),
        gross_leverage=1.0,
        min_trade_weight=float(config.trading.min_trade_weight),
        portfolio_activation=portfolio_activation,
        can_buy_mask=batch["can_buy_mask"].bool(),
        can_sell_mask=batch["can_sell_mask"].bool(),
        return_weights_history=return_weights_history,
    )


def _return_contribution_probe(
    *,
    model: torch.nn.Module,
    batch: dict[str, torch.Tensor],
    dates: list[str],
    config: Any,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    loss_portfolio_activation: str,
    return_aux: bool,
    top_k: int,
) -> dict[str, Any]:
    model.eval()
    with torch.inference_mode():
        with _autocast_context(device, amp_dtype):
            model_output = _call_model(model, batch["x"], batch["tradable_mask"], return_aux=return_aux)
            weights, _ = _extract_weights_and_aux(model_output)
        bt = _backtest_from_weights(
            weights=weights,
            batch=batch,
            config=config,
            portfolio_activation=loss_portfolio_activation,
            return_weights_history=True,
        )
        metrics = _compute_metrics_from_tensors(bt.strategy_returns, bt.benchmark_returns, bt.turnovers)
        returns = torch.nan_to_num(bt.strategy_returns.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
        abs_returns = returns.abs()
        total_abs = float(abs_returns.sum().detach().cpu())
        k = max(1, min(int(top_k), int(returns.numel())))
        top = torch.topk(abs_returns, k=k, largest=True)
        top_rows: list[dict[str, Any]] = []
        for rank, idx in enumerate(top.indices.detach().cpu().tolist(), start=1):
            ret = float(returns[idx].detach().cpu())
            top_rows.append(
                {
                    "rank": rank,
                    "row": int(idx),
                    "date": dates[idx] if idx < len(dates) else None,
                    "strategy_log_return": ret,
                    "abs_share": abs(ret) / max(total_abs, 1e-12),
                    "turnover": float(bt.turnovers[idx].detach().float().cpu()),
                }
            )
        top5_share = sum(float(row["abs_share"]) for row in top_rows[: min(5, len(top_rows))])
        top10_share = sum(float(row["abs_share"]) for row in top_rows[: min(10, len(top_rows))])
        return {
            "metrics": metrics,
            "weights": _weight_diagnostics(bt.weights_history),
            "top_abs_return_days": top_rows,
            "top5_abs_share": float(top5_share),
            "top10_abs_share": float(top10_share),
            "mean_strategy_log_return": float(returns.mean().detach().cpu()) if returns.numel() else 0.0,
            "std_strategy_log_return": float(returns.std(unbiased=False).detach().cpu()) if returns.numel() else 0.0,
        }


def _null_baseline_probe(
    *,
    model: torch.nn.Module,
    batch: dict[str, torch.Tensor],
    config: Any,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    loss_portfolio_activation: str,
    return_aux: bool,
    trials: int,
    seed: int,
) -> dict[str, Any]:
    trials = max(0, int(trials))
    model.eval()
    with torch.inference_mode():
        with _autocast_context(device, amp_dtype):
            model_output = _call_model(model, batch["x"], batch["tradable_mask"], return_aux=return_aux)
            weights, _ = _extract_weights_and_aux(model_output)
        original_bt = _backtest_from_weights(
            weights=weights,
            batch=batch,
            config=config,
            portfolio_activation=loss_portfolio_activation,
            return_weights_history=False,
        )
        original = _compute_metrics_from_tensors(
            original_bt.strategy_returns,
            original_bt.benchmark_returns,
            original_bt.turnovers,
        )

        rng = torch.Generator(device=device)
        rng.manual_seed(int(seed) + 17)
        rows: list[dict[str, Any]] = []
        for trial in range(trials):
            row_perm = torch.randperm(weights.size(0), generator=rng, device=device)
            sym_perm = torch.randperm(weights.size(1), generator=rng, device=device)

            date_batch = dict(batch)
            date_batch["future_log_returns"] = batch["future_log_returns"][row_perm]
            date_batch["benchmark"] = batch["benchmark"][row_perm]
            date_bt = _backtest_from_weights(
                weights=weights,
                batch=date_batch,
                config=config,
                portfolio_activation=loss_portfolio_activation,
                return_weights_history=False,
            )
            rows.append(
                {
                    "trial": trial + 1,
                    "kind": "date_shuffle_returns",
                    **_compute_metrics_from_tensors(date_bt.strategy_returns, date_bt.benchmark_returns, date_bt.turnovers),
                }
            )

            sym_batch = dict(batch)
            sym_batch["future_log_returns"] = batch["future_log_returns"][:, sym_perm]
            sym_bt = _backtest_from_weights(
                weights=weights,
                batch=sym_batch,
                config=config,
                portfolio_activation=loss_portfolio_activation,
                return_weights_history=False,
            )
            rows.append(
                {
                    "trial": trial + 1,
                    "kind": "symbol_shuffle_returns",
                    **_compute_metrics_from_tensors(sym_bt.strategy_returns, sym_bt.benchmark_returns, sym_bt.turnovers),
                }
            )

            random_scores = torch.randn(
                weights.shape,
                generator=rng,
                device=device,
                dtype=torch.float32,
            )
            random_weights = masked_activation_l1_weights(
                random_scores,
                batch["tradable_mask"].bool(),
                long_only=bool(config.trading.long_only),
                activation="identity",
            )
            random_bt = _backtest_from_weights(
                weights=random_weights,
                batch=batch,
                config=config,
                portfolio_activation="pre_normalized",
                return_weights_history=False,
            )
            rows.append(
                {
                    "trial": trial + 1,
                    "kind": "random_l1_scores",
                    **_compute_metrics_from_tensors(random_bt.strategy_returns, random_bt.benchmark_returns, random_bt.turnovers),
                }
            )

    grouped: dict[str, dict[str, float]] = {}
    for kind in sorted({str(row["kind"]) for row in rows}):
        subset = [row for row in rows if row["kind"] == kind]
        grouped[kind] = {
            "sharpe_mean": float(np.mean([_finite_float(row.get("sharpe"), 0.0) for row in subset])),
            "cumulative_return_mean": float(np.mean([_finite_float(row.get("cumulative_return"), 0.0) for row in subset])),
            "turnover_mean": float(np.mean([_finite_float(row.get("turnover"), 0.0) for row in subset])),
        }
    return {"original": original, "trials": rows, "summary": grouped}


def _load_or_build_model(
    *,
    config: Any,
    panel: Any,
    fold_dir: Path,
    checkpoint_path: Path | None,
    init: str,
    device: torch.device,
) -> tuple[torch.nn.Module, Any, dict[str, Any]]:
    init = str(init).strip().lower()
    candidate = checkpoint_path if checkpoint_path is not None else fold_dir / "checkpoint_best.pt"
    state_dict: dict[str, Any] | None = None
    checkpoint_info: dict[str, Any] = {"init": init, "checkpoint_path": None, "loaded_checkpoint": False}
    if init in {"checkpoint", "auto"} and candidate is not None and candidate.exists():
        checkpoint = _load_checkpoint(candidate)
        raw_state = checkpoint.get("model_state_dict")
        if not isinstance(raw_state, dict):
            raise ValueError(f"Checkpoint has no model_state_dict: {candidate}")
        state_dict = raw_state
        panel = _align_panel_to_state_dict_universe(panel, fold_dir, state_dict, context="convergence diagnostic")
        checkpoint_info.update(
            {
                "checkpoint_path": str(candidate),
                "loaded_checkpoint": True,
                "checkpoint_epoch": checkpoint.get("epoch"),
                "checkpoint_best_val_loss": checkpoint.get("best_val_loss"),
            }
        )
    elif init == "checkpoint":
        raise FileNotFoundError(f"Checkpoint not found: {candidate}")
    else:
        checkpoint_info["init"] = "scratch" if init == "auto" else init

    model = build_model(
        config=config,
        lookback=config.training.lookback,
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
    ).to(device)
    if state_dict is not None:
        _load_state_dict(model, state_dict)
    return model, panel, checkpoint_info


def _diagnosis_notes(payload: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    audit = payload.get("gradient_audit", {})
    grad_norm = _finite_float(audit.get("grad_norm"))
    grad_ratio = _finite_float(audit.get("grad_to_param_ratio"))
    out_grad = _finite_float(audit.get("output_grad_abs_mean"))
    if math.isfinite(grad_norm) and grad_norm < 1e-8:
        notes.append("Gradient norm is near zero; the current objective/output path may be saturated or flat.")
    if math.isfinite(grad_ratio) and grad_ratio < 1e-8:
        notes.append("Gradient-to-parameter ratio is extremely small; learning rate or output normalization may be too weak.")
    if math.isfinite(out_grad) and out_grad < 1e-8:
        notes.append("Output gradient is near zero; score-to-portfolio transform is likely attenuating credit assignment.")

    overfit = payload.get("one_batch_overfit", {})
    rel_delta = _finite_float(overfit.get("relative_loss_delta"))
    if math.isfinite(rel_delta):
        if rel_delta >= -0.01:
            notes.append("One-batch overfit barely improved; first suspect LR, gradients, masking, or loss implementation.")
        elif rel_delta <= -0.25:
            notes.append("One-batch overfit improves strongly; optimizer can fit the sample, so non-convergence is more likely generalization/noise.")

    contribution = payload.get("return_contribution", {})
    top5 = _finite_float(contribution.get("top5_abs_share"))
    if math.isfinite(top5) and top5 > 0.5:
        notes.append("Top 5 days dominate absolute strategy returns; tail days may dominate gradients.")
    top10 = _finite_float(contribution.get("top10_abs_share"))
    if math.isfinite(top10) and top10 > 0.75:
        notes.append("Top 10 days dominate the sample; return loss is likely sparse/noisy for this batch.")

    nulls = payload.get("null_baselines", {})
    original = nulls.get("original", {}) if isinstance(nulls, dict) else {}
    original_sharpe = _finite_float(original.get("sharpe"))
    random_summary = (nulls.get("summary") or {}).get("random_l1_scores", {}) if isinstance(nulls, dict) else {}
    random_sharpe = _finite_float(random_summary.get("sharpe_mean"))
    if math.isfinite(original_sharpe) and math.isfinite(random_sharpe) and abs(original_sharpe - random_sharpe) < 0.2:
        notes.append("Model score Sharpe is close to random L1 baseline on this batch; signal may be weak or not yet learned.")

    if not notes:
        notes.append("No single obvious failure mode triggered; compare the detailed probes across configs/folds.")
    return notes


def _write_markdown(path: Path, payload: dict[str, Any]) -> None:
    notes = payload.get("diagnosis_notes", [])
    overfit = payload.get("one_batch_overfit", {})
    audit = payload.get("gradient_audit", {})
    contribution = payload.get("return_contribution", {})
    lines = [
        "# Convergence Diagnostic",
        "",
        "## Run",
        "",
        f"- config: `{payload['config']}`",
        f"- fold: `{payload['fold_id']}`",
        f"- split: `{payload['split']}`",
        f"- rows: `{payload['sample']['row_count']}`",
        f"- date range: `{payload['sample']['date_start']}` to `{payload['sample']['date_end']}`",
        f"- init: `{payload['checkpoint']['init']}`",
        f"- loaded checkpoint: `{payload['checkpoint']['loaded_checkpoint']}`",
        f"- loss_type: `{payload['loss_type']}`",
        f"- loss_portfolio_activation: `{payload['loss_portfolio_activation']}`",
        "",
        "## Diagnosis Notes",
        "",
    ]
    lines.extend(f"- {note}" for note in notes)
    lines.extend(
        [
            "",
            "## Gradient Audit",
            "",
            f"- loss: `{_finite_float(audit.get('loss')):.8f}`",
            f"- grad_norm: `{_finite_float(audit.get('grad_norm')):.6g}`",
            f"- grad_to_param_ratio: `{_finite_float(audit.get('grad_to_param_ratio')):.6g}`",
            f"- output_grad_abs_mean: `{_finite_float(audit.get('output_grad_abs_mean')):.6g}`",
            f"- grad_nonfinite_ratio: `{_finite_float(audit.get('grad_nonfinite_ratio'), 0.0):.6g}`",
            "",
            "Top gradient parameters:",
            "",
            "| rank | parameter | grad_norm | param_norm |",
            "| ---: | --- | ---: | ---: |",
        ]
    )
    for idx, row in enumerate(audit.get("top_grad_parameters", [])[:10], start=1):
        lines.append(
            f"| {idx} | `{row['name']}` | {float(row['grad_norm']):.6g} | {float(row['param_norm']):.6g} |"
        )

    lines.extend(
        [
            "",
            "## One-Batch Overfit",
            "",
            f"- steps_completed: `{overfit.get('steps_completed')}`",
            f"- first_loss: `{_finite_float(overfit.get('first_loss')):.8f}`",
            f"- best_loss: `{_finite_float(overfit.get('best_loss')):.8f}`",
            f"- last_loss: `{_finite_float(overfit.get('last_loss')):.8f}`",
            f"- relative_loss_delta: `{_finite_float(overfit.get('relative_loss_delta')):.4f}`",
            "",
            "| step | loss | grad_norm | update_to_param |",
            "| ---: | ---: | ---: | ---: |",
        ]
    )
    for row in overfit.get("records", []):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("step")),
                    f"{_finite_float(row.get('loss')):.8f}",
                    f"{_finite_float(row.get('grad_norm')):.6g}",
                    f"{_finite_float(row.get('update_to_param_ratio')):.6g}",
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Return Contribution",
            "",
            f"- top5_abs_share: `{_finite_float(contribution.get('top5_abs_share')):.4f}`",
            f"- top10_abs_share: `{_finite_float(contribution.get('top10_abs_share')):.4f}`",
            f"- sample_sharpe: `{_finite_float((contribution.get('metrics') or {}).get('sharpe')):.4f}`",
            "",
            "| rank | date | strategy_log_return | abs_share | turnover |",
            "| ---: | --- | ---: | ---: | ---: |",
        ]
    )
    for row in contribution.get("top_abs_return_days", [])[:10]:
        lines.append(
            f"| {row['rank']} | `{row['date']}` | {float(row['strategy_log_return']):+.6f} | "
            f"{float(row['abs_share']):.4f} | {float(row['turnover']):.4f} |"
        )

    lines.extend(
        [
            "",
            "## Jacobian Probe",
            "",
            "| source | mode | grad_abs_mean | grad_abs_max | avg_positions | avg_gross |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload.get("jacobian_probe", []):
        lines.append(
            f"| `{row['source']}` | `{row['mode']}` | {float(row['score_grad_abs_mean']):.6g} | "
            f"{float(row['score_grad_abs_max']):.6g} | {float(row['avg_positions']):.1f} | "
            f"{float(row['avg_gross']):.4f} |"
        )

    nulls = payload.get("null_baselines", {})
    lines.extend(
        [
            "",
            "## Null Baselines",
            "",
            f"- original_sharpe: `{_finite_float((nulls.get('original') or {}).get('sharpe')):.4f}`",
            "",
            "| kind | sharpe_mean | cumulative_return_mean | turnover_mean |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for kind, row in (nulls.get("summary") or {}).items():
        lines.append(
            f"| `{kind}` | {float(row['sharpe_mean']):+.4f} | "
            f"{float(row['cumulative_return_mean']):+.4f} | {float(row['turnover_mean']):.4f} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose why an end-to-end portfolio objective does or does not converge.")
    parser.add_argument("--config", default="configs/markets/tw.yaml")
    parser.add_argument("--fold", type=int, default=25)
    parser.add_argument("--split", choices=("train", "val", "test"), default="train")
    parser.add_argument("--rows", type=int, default=16)
    parser.add_argument("--sample-method", choices=("first", "last", "even", "random"), default="last")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--init", choices=("scratch", "checkpoint", "auto"), default="scratch")
    parser.add_argument("--checkpoint", type=Path, default=None)
    parser.add_argument("--device", default=None)
    parser.add_argument("--return-aux", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--overfit-steps", type=int, default=80)
    parser.add_argument("--overfit-log-every", type=int, default=10)
    parser.add_argument("--learning-rate", type=float, default=None)
    parser.add_argument("--weight-decay", type=float, default=None)
    parser.add_argument("--grad-clip-norm", type=float, default=None)
    parser.add_argument("--skip-overfit", action="store_true")
    parser.add_argument("--skip-jacobian", action="store_true")
    parser.add_argument("--skip-null", action="store_true")
    parser.add_argument("--null-trials", type=int, default=3)
    parser.add_argument("--tail-top-k", type=int, default=10)
    parser.add_argument("--jacobian-modes", default=DEFAULT_JACOBIAN_MODES)
    parser.add_argument("--jacobian-sources", default="zero,random")
    parser.add_argument("--jacobian-score-scale", type=float, default=1.0)
    parser.add_argument("--backtest-compile", action="store_true")
    parser.add_argument("--reduced-log-utility", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    torch.manual_seed(int(args.seed))
    np.random.seed(int(args.seed))

    config = load_config(args.config)
    if args.device is not None:
        config.environment.device = str(args.device)
    _configure_runtime(
        config,
        backtest_compile=bool(args.backtest_compile),
        reduced_log_utility=bool(args.reduced_log_utility),
    )
    device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    non_blocking = bool(config.training.non_blocking_transfer and device.type == "cuda")
    loss_portfolio_activation = _training_loss_portfolio_activation(config)
    loss_portfolio_activation = normalize_portfolio_activation(loss_portfolio_activation)

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
    )
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    fold = next((item for item in folds if int(item.fold_id) == int(args.fold)), None)
    if fold is None:
        raise ValueError(f"fold={args.fold} not found; available={[fold.fold_id for fold in folds]}")

    output_root = Path(config.runner.output_dir)
    fold_dir = output_root / f"fold_{int(args.fold):02d}"
    model, panel, checkpoint_info = _load_or_build_model(
        config=config,
        panel=panel,
        fold_dir=fold_dir,
        checkpoint_path=args.checkpoint,
        init=args.init,
        device=device,
    )

    dataset = CrossSectionalDataset(panel, _split_indices(fold, args.split), config.training.lookback)
    split = dataset_to_windowed_tensors(dataset)
    row_indices = _select_indices(len(split), args.rows, args.sample_method, args.seed)
    batch = _batch_from_windowed(split, row_indices, device=device, non_blocking=non_blocking)
    sample_meta = _date_metadata(panel, split, row_indices)

    print(
        json.dumps(
            {
                "config": str(args.config),
                "fold": int(args.fold),
                "split": args.split,
                "rows": int(sample_meta["row_count"]),
                "date_start": sample_meta["date_start"],
                "date_end": sample_meta["date_end"],
                "device": str(device),
                "amp_dtype": str(amp_dtype),
                "init": checkpoint_info["init"],
                "loaded_checkpoint": checkpoint_info["loaded_checkpoint"],
                "loss_portfolio_activation": loss_portfolio_activation,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    gradient_audit = _gradient_audit(
        model=model,
        batch=batch,
        config=config,
        device=device,
        amp_dtype=amp_dtype,
        loss_portfolio_activation=loss_portfolio_activation,
        return_aux=bool(args.return_aux),
    )
    print(f"gradient_audit loss={gradient_audit['loss']:.8f} grad_norm={gradient_audit['grad_norm']:.6g}", flush=True)

    overfit_result: dict[str, Any]
    overfit_model = copy.deepcopy(model).to(device)
    if args.skip_overfit:
        overfit_result = {"skipped": True}
    else:
        overfit_result = _one_batch_overfit(
            model=overfit_model,
            batch=batch,
            config=config,
            device=device,
            amp_dtype=amp_dtype,
            loss_portfolio_activation=loss_portfolio_activation,
            return_aux=bool(args.return_aux),
            steps=int(args.overfit_steps),
            learning_rate=float(args.learning_rate if args.learning_rate is not None else config.training.learning_rate),
            weight_decay=float(args.weight_decay if args.weight_decay is not None else config.training.weight_decay),
            grad_clip_norm=float(args.grad_clip_norm if args.grad_clip_norm is not None else config.training.grad_clip_norm),
            log_every=int(args.overfit_log_every),
        )
        print(
            "one_batch_overfit "
            f"first={overfit_result['first_loss']:.8f} "
            f"last={overfit_result['last_loss']:.8f} "
            f"rel_delta={overfit_result['relative_loss_delta']:.4f}",
            flush=True,
        )

    return_contribution = _return_contribution_probe(
        model=model,
        batch=batch,
        dates=sample_meta["dates"],
        config=config,
        device=device,
        amp_dtype=amp_dtype,
        loss_portfolio_activation=loss_portfolio_activation,
        return_aux=bool(args.return_aux),
        top_k=int(args.tail_top_k),
    )
    print(
        "return_contribution "
        f"top5_abs_share={return_contribution['top5_abs_share']:.4f} "
        f"top10_abs_share={return_contribution['top10_abs_share']:.4f}",
        flush=True,
    )

    if args.skip_jacobian:
        jacobian = []
    else:
        jacobian = _jacobian_probe(
            batch=batch,
            modes=_parse_csv(args.jacobian_modes),
            sources=_parse_csv(args.jacobian_sources),
            seed=int(args.seed),
            long_only=bool(config.trading.long_only),
            activation=loss_portfolio_activation,
            score_scale=float(args.jacobian_score_scale),
        )
        print(f"jacobian_probe rows={len(jacobian)}", flush=True)

    if args.skip_null:
        nulls = {"skipped": True}
    else:
        nulls = _null_baseline_probe(
            model=model,
            batch=batch,
            config=config,
            device=device,
            amp_dtype=amp_dtype,
            loss_portfolio_activation=loss_portfolio_activation,
            return_aux=bool(args.return_aux),
            trials=int(args.null_trials),
            seed=int(args.seed),
        )
        print(f"null_baselines trials={len(nulls.get('trials', []))}", flush=True)

    out_dir = args.output_dir
    if out_dir is None:
        market_name = Path(str(args.config)).stem
        out_dir = Path("artifacts") / "diagnostics" / market_name / f"fold_{int(args.fold):02d}" / str(args.split)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "convergence_diagnostics.json"
    md_path = out_dir / "convergence_report.md"

    payload: dict[str, Any] = {
        "config": str(args.config),
        "fold_id": int(args.fold),
        "split": str(args.split),
        "sample_method": str(args.sample_method),
        "sample": sample_meta,
        "device": str(device),
        "amp_dtype": str(amp_dtype),
        "loss_type": str(config.training.loss_type),
        "model_name": str(config.training.model_name),
        "portfolio_output_mode": str(getattr(config.training.transformer_base_portfolio, "portfolio_output_mode", "")),
        "loss_portfolio_activation": loss_portfolio_activation,
        "trading_portfolio_activation": str(config.trading.portfolio_activation),
        "min_trade_weight": float(config.trading.min_trade_weight),
        "learning_rate": float(args.learning_rate if args.learning_rate is not None else config.training.learning_rate),
        "weight_decay": float(args.weight_decay if args.weight_decay is not None else config.training.weight_decay),
        "grad_clip_norm": float(args.grad_clip_norm if args.grad_clip_norm is not None else config.training.grad_clip_norm),
        "checkpoint": checkpoint_info,
        "gradient_audit": gradient_audit,
        "one_batch_overfit": overfit_result,
        "return_contribution": return_contribution,
        "jacobian_probe": jacobian,
        "null_baselines": nulls,
    }
    payload["diagnosis_notes"] = _diagnosis_notes(payload)
    payload["outputs"] = {"json": str(json_path), "markdown": str(md_path)}

    json_path.write_text(json.dumps(_to_builtin(payload), indent=2, ensure_ascii=False), encoding="utf-8")
    _write_markdown(md_path, _to_builtin(payload))
    print(json.dumps(payload["outputs"], sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
