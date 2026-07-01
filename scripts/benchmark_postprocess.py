#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.models.factory import build_model
from stockagent.models.normalization import normalize_portfolio_activation
from stockagent.training.dataset import CrossSectionalDataset
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
    _resolve_inference_backtest_chunk_rows,
    _resolve_inference_model_chunk_rows,
)
from stockagent.training.windowed import WindowedSplitTensors, dataset_to_windowed_tensors


DEFAULT_ACTIVATIONS = "identity,softsign,tanh,isru,erf,atan,gd"
DEFAULT_THRESHOLDS = "0,0.0001,0.00025,0.0005,0.001,0.0025,0.005,0.01,0.02"
BEST_HIGHER_IS_BETTER = {
    "sharpe",
    "sortino",
    "calmar",
    "cumulative_return",
    "annualized_return",
    "cagr",
    "daily_hit_rate",
    "excess_return_vs_benchmark",
    "max_drawdown",
}
BEST_LOWER_IS_BETTER = {"turnover"}
SUPPORTED_RANK_METRICS = BEST_HIGHER_IS_BETTER | BEST_LOWER_IS_BETTER
METRIC_ALIASES = {
    "sharp": "sharpe",
    "cum_return": "cumulative_return",
    "return": "cumulative_return",
    "returns": "cumulative_return",
    "total_return": "cumulative_return",
    "total_returns": "cumulative_return",
}


def _configure_runtime(backtest_compile: bool) -> None:
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1" if backtest_compile else "0"
    os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = "1" if backtest_compile else "0"
    os.environ["STOCKAGENT_BACKTEST_VERBOSE"] = "0"
    os.environ["STOCKAGENT_AUTO_TORCH_COMPILE_SHARPE"] = "0"
    os.environ["STOCKAGENT_COMPILE_LOSS"] = "0"
    if torch.cuda.is_available():
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        torch.backends.cudnn.benchmark = True
        if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_flash_sdp"):
            torch.backends.cuda.enable_flash_sdp(True)
        if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_mem_efficient_sdp"):
            torch.backends.cuda.enable_mem_efficient_sdp(True)
        if hasattr(torch.backends, "cuda") and hasattr(torch.backends.cuda, "enable_math_sdp"):
            torch.backends.cuda.enable_math_sdp(True)


def _parse_activations(raw: str) -> list[str]:
    activations: list[str] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        normalized = normalize_portfolio_activation(item)
        if normalized == "gudermannian":
            normalized = "gd"
        if normalized not in activations:
            activations.append(normalized)
    if not activations:
        raise ValueError("At least one activation is required")
    return activations


def _parse_thresholds(raw: str) -> list[float]:
    values = sorted({max(0.0, float(item.strip())) for item in raw.split(",") if item.strip()})
    if not values:
        raise ValueError("At least one threshold is required")
    return values


def _normalize_metric_name(raw: str) -> str:
    name = str(raw).strip().lower().replace("-", "_")
    return METRIC_ALIASES.get(name, name)


def _parse_plot_metrics(raw: str | None, rank_metric: str) -> list[str]:
    values: list[str] = []
    source = raw if raw is not None else rank_metric
    for item in str(source).split(","):
        metric = _normalize_metric_name(item)
        if not metric:
            continue
        if metric not in SUPPORTED_RANK_METRICS:
            raise ValueError(f"Unsupported plot metric: {metric}")
        if metric not in values:
            values.append(metric)
    if rank_metric not in values:
        values.insert(0, rank_metric)
    if not values:
        raise ValueError("At least one plot metric is required")
    return values


def _ensure_transformer_base_portfolio_config(config: Any) -> None:
    model_name = str(config.training.model_name).strip().lower().replace("-", "_")
    if model_name not in {"transformer_base_portfolio", "transformer_base_portfolio_model", "tbp"}:
        raise ValueError(
            "Post-processing sweep needs raw model scores. This script currently supports "
            f"transformer_base_portfolio, got model_name={config.training.model_name!r}."
        )
    config.training.transformer_base_portfolio.return_aux = False
    config.training.transformer_base_portfolio.return_aux_details = False


def _select_fold_indices(fold: Any, split: str) -> Any:
    if split == "train":
        return fold.train_indices
    if split == "val":
        return fold.val_indices
    if split == "test":
        return fold.test_indices
    raise ValueError("split must be one of train, val, or test")


def _collect_raw_scores(
    *,
    model: torch.nn.Module,
    split: Any,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    chunk_rows: int,
) -> dict[str, torch.Tensor]:
    model.eval()
    total_rows = len(split)
    if total_rows <= 0:
        raise ValueError("Selected split has no rows after lookback filtering")
    chunk_rows = max(1, int(chunk_rows))

    scores_chunks: list[torch.Tensor] = []
    returns_chunks: list[torch.Tensor] = []
    tradable_chunks: list[torch.Tensor] = []
    can_buy_chunks: list[torch.Tensor] = []
    can_sell_chunks: list[torch.Tensor] = []
    benchmark_chunks: list[torch.Tensor] = []

    with torch.inference_mode():
        for start in range(0, total_rows, chunk_rows):
            end = min(start + chunk_rows, total_rows)
            batch = split.batch_by_rows(start, end, device=device, non_blocking=non_blocking)
            with _autocast_context(device, amp_dtype):
                model_output = _call_model(model, batch["x"], batch["tradable_mask"], return_aux=False)
                raw_scores, _ = _extract_weights_and_aux(model_output)
            scores_chunks.append(raw_scores.detach().to(device=device, dtype=torch.float32))
            returns_chunks.append(batch["future_log_returns"].detach().to(device=device, dtype=torch.float32))
            tradable_chunks.append(batch["tradable_mask"].detach().to(device=device, dtype=torch.bool))
            can_buy_chunks.append(batch["can_buy_mask"].detach().to(device=device, dtype=torch.bool))
            can_sell_chunks.append(batch["can_sell_mask"].detach().to(device=device, dtype=torch.bool))
            benchmark_chunks.append(batch["benchmark"].detach().to(device=device, dtype=torch.float32))

    if device.type == "cuda":
        torch.cuda.synchronize(device)

    return {
        "scores": torch.cat(scores_chunks, dim=0).contiguous(),
        "future_returns": torch.cat(returns_chunks, dim=0).contiguous(),
        "tradable_mask": torch.cat(tradable_chunks, dim=0).contiguous(),
        "can_buy_mask": torch.cat(can_buy_chunks, dim=0).contiguous(),
        "can_sell_mask": torch.cat(can_sell_chunks, dim=0).contiguous(),
        "benchmark": torch.cat(benchmark_chunks, dim=0).contiguous(),
    }


def _weight_diagnostics(weights: torch.Tensor) -> dict[str, float]:
    if weights.numel() == 0:
        return {
            "avg_positions": 0.0,
            "avg_gross": 0.0,
            "avg_long_gross": 0.0,
            "avg_short_gross": 0.0,
            "avg_max_abs_weight": 0.0,
        }
    abs_w = weights.abs().to(torch.float64)
    positions = (abs_w > 0.0).sum(dim=1).to(torch.float64)
    long_gross = weights.clamp_min(0.0).sum(dim=1).to(torch.float64)
    short_gross = (-weights.clamp_max(0.0)).sum(dim=1).to(torch.float64)
    return {
        "avg_positions": float(positions.mean().item()),
        "avg_gross": float(abs_w.sum(dim=1).mean().item()),
        "avg_long_gross": float(long_gross.mean().item()),
        "avg_short_gross": float(short_gross.mean().item()),
        "avg_max_abs_weight": float(abs_w.max(dim=1).values.mean().item()),
    }


def _run_single_backtest(
    *,
    buffers: dict[str, torch.Tensor],
    config: Any,
    activation: str,
    threshold: float,
    scan_chunk_size: int | None,
    return_weights_history: bool = False,
) -> Any:
    return run_backtest_torch(
        buffers["scores"],
        buffers["future_returns"],
        buffers["tradable_mask"],
        buffers["benchmark"],
        buy_fee_rate=config.trading.buy_fee_rate,
        sell_fee_rate=config.trading.sell_fee_rate,
        long_only=config.trading.long_only,
        max_turnover_ratio=config.trading.max_turnover_ratio,
        gross_leverage=1.0,
        min_trade_weight=float(threshold),
        portfolio_activation=activation,
        can_buy_mask=buffers["can_buy_mask"],
        can_sell_mask=buffers["can_sell_mask"],
        scan_chunk_size=scan_chunk_size,
        return_weights_history=return_weights_history,
    )


def _run_row_backtest(
    *,
    row: dict[str, Any],
    buffers_by_mode: dict[str, dict[str, torch.Tensor]],
    config: Any,
    scan_chunk_size: int | None,
    return_weights_history: bool = False,
) -> Any:
    mode = str(row.get("mode", "raw_logits"))
    buffers = buffers_by_mode[mode]
    return _run_single_backtest(
        buffers=buffers,
        config=config,
        activation=str(row["activation"]),
        threshold=float(row["min_trade_weight"]),
        scan_chunk_size=scan_chunk_size,
        return_weights_history=return_weights_history,
    )


def _to_numpy_1d(values: torch.Tensor) -> np.ndarray:
    arr = values.detach().to(dtype=torch.float64).cpu().numpy()
    return np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0).reshape(-1)


def _equity_from_log_returns(values: torch.Tensor) -> np.ndarray:
    returns = _to_numpy_1d(values)
    return np.exp(np.clip(np.cumsum(returns), -60.0, 60.0))


def _import_pyplot() -> Any:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    return plt


def _plot_best_equity_curve(
    *,
    path: Path,
    dates: np.ndarray,
    backtest: Any,
    best: dict[str, Any],
    rank_metric: str,
) -> None:
    plt = _import_pyplot()
    strategy_equity = _equity_from_log_returns(backtest.strategy_returns)
    benchmark_equity = _equity_from_log_returns(backtest.benchmark_returns)
    x_values: np.ndarray | list[int]
    if len(dates) == len(strategy_equity):
        x_values = dates
    else:
        x_values = list(range(len(strategy_equity)))

    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(11.5, 6.0))
    ax.plot(x_values, strategy_equity, label="strategy", linewidth=2.0)
    ax.plot(x_values, benchmark_equity, label="benchmark", linewidth=1.5, alpha=0.85)
    ax.axhline(1.0, color="black", linewidth=0.8, alpha=0.35)
    ax.set_title(
        "Best postprocess equity "
        f"({rank_metric}: {float(best.get(rank_metric, 0.0)):+.4f}, "
        f"{best.get('candidate', best['activation'])}, threshold={float(best['min_trade_weight']):.6g})"
    )
    ax.set_ylabel("Growth of 1.0")
    ax.grid(True, linewidth=0.5, alpha=0.25)
    ax.legend(loc="best")
    details = "\n".join(
        [
            f"Sharpe: {float(best.get('sharpe', 0.0)):+.4f}",
            f"Cum return: {float(best.get('cumulative_return', 0.0)) * 100.0:+.2f}%",
            f"Max DD: {float(best.get('max_drawdown', 0.0)) * 100.0:+.2f}%",
            f"Turnover: {float(best.get('turnover', 0.0)):.4f}",
            f"Avg positions: {float(best.get('avg_positions', 0.0)):.1f}",
        ]
    )
    ax.text(
        0.015,
        0.985,
        details,
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "0.8", "alpha": 0.9},
    )
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def _plot_metric_heatmap(
    *,
    path: Path,
    rows: list[dict[str, Any]],
    activations: list[str],
    thresholds: list[float],
    metric: str,
) -> None:
    plt = _import_pyplot()
    values = np.full((len(activations), len(thresholds)), np.nan, dtype=np.float64)
    activation_idx = {name: idx for idx, name in enumerate(activations)}
    threshold_idx = {float(value): idx for idx, value in enumerate(thresholds)}
    for row in rows:
        i = activation_idx.get(str(row.get("candidate", row["activation"])))
        j = threshold_idx.get(float(row["min_trade_weight"]))
        if i is not None and j is not None:
            values[i, j] = float(row.get(metric, np.nan))

    path.parent.mkdir(parents=True, exist_ok=True)
    width = max(8.0, 0.78 * len(thresholds) + 2.5)
    height = max(4.8, 0.52 * len(activations) + 2.0)
    fig, ax = plt.subplots(figsize=(width, height))
    cmap = "magma_r" if metric in BEST_LOWER_IS_BETTER else "viridis"
    image = ax.imshow(values, aspect="auto", cmap=cmap)
    ax.set_title(f"Postprocess {metric} by activation and threshold")
    ax.set_xlabel("min_trade_weight")
    ax.set_ylabel("candidate")
    ax.set_xticks(range(len(thresholds)))
    ax.set_xticklabels([f"{value:g}" for value in thresholds], rotation=35, ha="right")
    ax.set_yticks(range(len(activations)))
    ax.set_yticklabels(activations)
    for i in range(values.shape[0]):
        for j in range(values.shape[1]):
            value = values[i, j]
            if np.isfinite(value):
                label = f"{value:.2f}" if abs(value) >= 10 else f"{value:.3f}"
                ax.text(j, i, label, ha="center", va="center", fontsize=7, color="white")
    fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def _plot_top_results_bar(
    *,
    path: Path,
    rows_sorted: list[dict[str, Any]],
    metric: str,
    top_n: int,
) -> None:
    if not rows_sorted:
        return
    plt = _import_pyplot()
    top_rows = rows_sorted[: max(1, int(top_n))]
    labels = [f"{row.get('candidate', row['activation'])}\n{float(row['min_trade_weight']):g}" for row in top_rows]
    values = [float(row.get(metric, 0.0)) for row in top_rows]

    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(max(10.0, 0.62 * len(top_rows)), 5.6))
    bars = ax.bar(range(len(top_rows)), values, color="#3d6f91")
    best_color = "#b8472b" if metric not in BEST_LOWER_IS_BETTER else "#2f855a"
    bars[0].set_color(best_color)
    ax.set_title(f"Top {len(top_rows)} postprocess settings by {metric}")
    ax.set_ylabel(metric)
    ax.set_xticks(range(len(top_rows)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(True, axis="y", linewidth=0.5, alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def _write_plots(
    *,
    output_dir: Path,
    split: str,
    rows_sorted: list[dict[str, Any]],
    best: dict[str, Any] | None,
    buffers_by_mode: dict[str, dict[str, torch.Tensor]],
    config: Any,
    scan_chunk_size: int | None,
    dates: np.ndarray,
    activations: list[str],
    thresholds: list[float],
    rank_metric: str,
    top_n: int,
) -> dict[str, str]:
    outputs: dict[str, str] = {}
    heatmap_path = output_dir / f"{split}_{rank_metric}_heatmap.png"
    _plot_metric_heatmap(
        path=heatmap_path,
        rows=rows_sorted,
        activations=activations,
        thresholds=thresholds,
        metric=rank_metric,
    )
    outputs["rank_metric_heatmap"] = str(heatmap_path)

    top_path = output_dir / f"{split}_top{max(1, int(top_n))}_{rank_metric}.png"
    _plot_top_results_bar(path=top_path, rows_sorted=rows_sorted, metric=rank_metric, top_n=top_n)
    outputs["top_results_bar"] = str(top_path)

    if best is not None:
        backtest = _run_row_backtest(
            row=best,
            buffers_by_mode=buffers_by_mode,
            config=config,
            scan_chunk_size=scan_chunk_size,
            return_weights_history=False,
        )
        best_buffers = buffers_by_mode[str(best.get("mode", "raw_logits"))]
        if best_buffers["scores"].device.type == "cuda":
            torch.cuda.synchronize(best_buffers["scores"].device)
        best_path = output_dir / f"{split}_best_{rank_metric}_equity_curve.png"
        _plot_best_equity_curve(
            path=best_path,
            dates=dates,
            backtest=backtest,
            best=best,
            rank_metric=rank_metric,
        )
        outputs["best_equity_curve"] = str(best_path)
    return outputs


def _write_metric_plots(
    *,
    output_dir: Path,
    split: str,
    rows: list[dict[str, Any]],
    buffers_by_mode: dict[str, dict[str, torch.Tensor]],
    config: Any,
    scan_chunk_size: int | None,
    dates: np.ndarray,
    activations: list[str],
    thresholds: list[float],
    metrics: list[str],
    top_n: int,
) -> dict[str, dict[str, str]]:
    outputs: dict[str, dict[str, str]] = {}
    for metric in metrics:
        rows_sorted = sorted(
            rows,
            key=lambda row: float(row.get(metric, 0.0)),
            reverse=metric not in BEST_LOWER_IS_BETTER,
        )
        outputs[metric] = _write_plots(
            output_dir=output_dir,
            split=split,
            rows_sorted=rows_sorted,
            best=_best_by_metric(rows, metric),
            buffers_by_mode=buffers_by_mode,
            config=config,
            scan_chunk_size=scan_chunk_size,
            dates=dates,
            activations=activations,
            thresholds=thresholds,
            rank_metric=metric,
            top_n=top_n,
        )
    return outputs


def _run_sweep(
    *,
    buffers: dict[str, torch.Tensor],
    mode: str,
    model_output_mode: str,
    activations: list[str],
    thresholds: list[float],
    config: Any,
    scan_chunk_size: int | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scores = buffers["scores"]
    future_returns = buffers["future_returns"]
    tradable_mask = buffers["tradable_mask"]
    can_buy_mask = buffers["can_buy_mask"]
    can_sell_mask = buffers["can_sell_mask"]
    benchmark = buffers["benchmark"]

    for activation in activations:
        if mode == "trained_output":
            candidate = f"trained:{model_output_mode}"
        else:
            candidate = f"raw:{activation}"
        for threshold in thresholds:
            started = time.perf_counter()
            bt = run_backtest_torch(
                scores,
                future_returns,
                tradable_mask,
                benchmark,
                buy_fee_rate=config.trading.buy_fee_rate,
                sell_fee_rate=config.trading.sell_fee_rate,
                long_only=config.trading.long_only,
                max_turnover_ratio=config.trading.max_turnover_ratio,
                gross_leverage=1.0,
                min_trade_weight=float(threshold),
                portfolio_activation=activation,
                can_buy_mask=can_buy_mask,
                can_sell_mask=can_sell_mask,
                scan_chunk_size=scan_chunk_size,
                return_weights_history=True,
            )
            if scores.device.type == "cuda":
                torch.cuda.synchronize(scores.device)
            elapsed_s = time.perf_counter() - started
            metrics = _compute_metrics_from_tensors(bt.strategy_returns, bt.benchmark_returns, bt.turnovers)
            diagnostics = _weight_diagnostics(bt.weights_history)
            row: dict[str, Any] = {
                "mode": mode,
                "model_output_mode": model_output_mode,
                "candidate": candidate,
                "activation": activation,
                "min_trade_weight": float(threshold),
                "elapsed_s": float(elapsed_s),
                **metrics,
                **diagnostics,
            }
            rows.append(row)
            print(
                " ".join(
                    [
                        f"mode={mode}",
                        f"candidate={candidate}",
                        f"activation={activation}",
                        f"threshold={threshold:g}",
                        f"sharpe={row['sharpe']:+.4f}",
                        f"cum={row['cumulative_return']:+.4f}",
                        f"mdd={row['max_drawdown']:+.4f}",
                        f"turnover={row['turnover']:.4f}",
                        f"positions={row['avg_positions']:.1f}",
                    ]
                ),
                flush=True,
            )
    return rows


def _best_by_metric(rows: list[dict[str, Any]], metric: str) -> dict[str, Any] | None:
    if not rows:
        return None
    if metric in BEST_LOWER_IS_BETTER:
        return min(rows, key=lambda row: float(row.get(metric, float("inf"))))
    return max(rows, key=lambda row: float(row.get(metric, float("-inf"))))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _format_pct(value: Any) -> str:
    return f"{float(value) * 100.0:+.2f}%"


def _write_markdown(path: Path, summary: dict[str, Any], rows: list[dict[str, Any]], rank_metric: str) -> None:
    sorted_rows = sorted(rows, key=lambda row: float(row.get(rank_metric, 0.0)), reverse=rank_metric not in BEST_LOWER_IS_BETTER)
    best = summary.get("best_by_rank_metric")
    lines = [
        "# Post-processing Benchmark",
        "",
        f"- config: `{summary['config']}`",
        f"- fold: `{summary['fold_id']}`",
        f"- split: `{summary['split']}`",
        f"- rows: `{summary['rows']}`",
        f"- date range: `{summary['date_start']}` to `{summary['date_end']}`",
        f"- symbols: `{summary['symbols']}`",
        f"- rank metric: `{rank_metric}`",
        "",
    ]
    if best:
        lines.extend(
            [
                "## Best",
                "",
                f"- candidate: `{best.get('candidate', best['activation'])}`",
                f"- mode: `{best.get('mode', 'raw_logits')}`",
                f"- activation: `{best['activation']}`",
                f"- min_trade_weight: `{best['min_trade_weight']}`",
                f"- sharpe: `{float(best['sharpe']):+.4f}`",
                f"- cumulative_return: `{_format_pct(best['cumulative_return'])}`",
                f"- max_drawdown: `{_format_pct(best['max_drawdown'])}`",
                f"- turnover: `{float(best['turnover']):.4f}`",
                f"- avg_positions: `{float(best['avg_positions']):.1f}`",
                "",
            ]
        )
    plot_metrics = list(summary.get("plot_metrics", [rank_metric]))
    best_by_metric = summary.get("best_by_metric", {})
    if best_by_metric and plot_metrics:
        lines.extend(
            [
                "## Best By Metric",
                "",
                "| metric | candidate | mode | activation | threshold | sharpe | sortino | cum_return | max_drawdown | turnover | avg_positions |",
                "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for metric in plot_metrics:
            row = best_by_metric.get(metric)
            if not row:
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{metric}`",
                        f"`{row.get('candidate', row['activation'])}`",
                        f"`{row.get('mode', 'raw_logits')}`",
                        f"`{row['activation']}`",
                        f"{float(row['min_trade_weight']):.6g}",
                        f"{float(row['sharpe']):+.4f}",
                        f"{float(row['sortino']):+.4f}",
                        _format_pct(row["cumulative_return"]),
                        _format_pct(row["max_drawdown"]),
                        f"{float(row['turnover']):.4f}",
                        f"{float(row['avg_positions']):.1f}",
                    ]
                )
                + " |"
            )
        lines.append("")
    plot_outputs = summary.get("outputs", {}).get("plots", {})
    if plot_outputs:
        lines.extend(["## Plots", ""])
        for name, plot_path in plot_outputs.items():
            if isinstance(plot_path, dict):
                for child_name, child_path in plot_path.items():
                    lines.append(f"- {name}/{child_name}: `{child_path}`")
            else:
                lines.append(f"- {name}: `{plot_path}`")
        lines.append("")
    lines.extend(
        [
            "## Top Results",
            "",
            "| rank | candidate | mode | activation | threshold | sharpe | cum_return | max_drawdown | turnover | avg_positions |",
            "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for idx, row in enumerate(sorted_rows[:20], start=1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    f"`{row.get('candidate', row['activation'])}`",
                    f"`{row.get('mode', 'raw_logits')}`",
                    f"`{row['activation']}`",
                    f"{float(row['min_trade_weight']):.6g}",
                    f"{float(row['sharpe']):+.4f}",
                    _format_pct(row["cumulative_return"]),
                    _format_pct(row["max_drawdown"]),
                    f"{float(row['turnover']):.4f}",
                    f"{float(row['avg_positions']):.1f}",
                ]
            )
            + " |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark portfolio post-processing activation and threshold settings.")
    parser.add_argument("--config", default="configs/markets/tw.yaml")
    parser.add_argument("--fold", type=int, default=25)
    parser.add_argument("--split", choices=["train", "val", "test"], default="test")
    parser.add_argument("--activations", default=DEFAULT_ACTIVATIONS)
    parser.add_argument("--thresholds", default=DEFAULT_THRESHOLDS)
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=None,
        help="Model inference chunk rows. Default follows training.eval_model_chunk_rows from YAML.",
    )
    parser.add_argument(
        "--scan-chunk-size",
        type=int,
        default=None,
        help="Backtest scan chunk size. Default follows training.eval_backtest_chunk_rows from YAML.",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Optional smoke-test row cap.")
    parser.add_argument("--rank-metric", default="sharpe")
    parser.add_argument(
        "--plot-metrics",
        default=None,
        help="Comma-separated metrics to plot. Default plots only --rank-metric.",
    )
    parser.add_argument("--backtest-compile", action="store_true")
    parser.add_argument("--plots", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--plot-top-n", type=int, default=20)
    parser.add_argument(
        "--output-root",
        default=None,
        help="Training artifact root that contains fold_XX directories. Default follows config.runner.output_dir.",
    )
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    _configure_runtime(backtest_compile=bool(args.backtest_compile))
    activations = _parse_activations(args.activations)
    thresholds = _parse_thresholds(args.thresholds)
    rank_metric = _normalize_metric_name(args.rank_metric)
    if rank_metric not in SUPPORTED_RANK_METRICS:
        raise ValueError(f"Unsupported rank metric: {rank_metric}")
    plot_metrics = _parse_plot_metrics(args.plot_metrics, rank_metric)

    config = load_config(args.config)
    _ensure_transformer_base_portfolio_config(config)
    _configure_backtest_runtime_from_config(config)
    _configure_runtime(backtest_compile=bool(args.backtest_compile))
    original_output_mode = str(config.training.transformer_base_portfolio.portfolio_output_mode)
    trained_activation = normalize_portfolio_activation(config.trading.portfolio_activation)

    device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    non_blocking = bool(config.training.non_blocking_transfer and device.type == "cuda")
    output_root = Path(args.output_root) if args.output_root else Path(config.runner.output_dir)
    fold_dir = output_root / f"fold_{int(args.fold):02d}"
    checkpoint_path = fold_dir / "checkpoint_best.pt"

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
    fold = next((item for item in folds if item.fold_id == int(args.fold)), None)
    if fold is None:
        raise ValueError(f"fold_id={args.fold} not found; available={[item.fold_id for item in folds]}")

    checkpoint = _load_checkpoint(checkpoint_path)
    state_dict = checkpoint.get("model_state_dict")
    if not isinstance(state_dict, dict):
        raise ValueError(f"Checkpoint has no model_state_dict: {checkpoint_path}")
    panel = _align_panel_to_state_dict_universe(panel, fold_dir, state_dict, context=f"postprocess benchmark fold {args.fold}")

    indices = _select_fold_indices(fold, args.split)
    dataset = CrossSectionalDataset(panel, indices, config.training.lookback)
    split = dataset_to_windowed_tensors(dataset)
    if args.max_rows is not None:
        max_rows = max(1, int(args.max_rows))
        split = WindowedSplitTensors(
            features=split.features,
            valid_indices=split.valid_indices[:max_rows],
            future_log_returns=split.future_log_returns,
            tradable_mask=split.tradable_mask,
            can_buy_mask=split.can_buy_mask,
            can_sell_mask=split.can_sell_mask,
            benchmark=split.benchmark,
            lookback=split.lookback,
            sample_mask=None if split.sample_mask is None else split.sample_mask[:max_rows],
        )
    if len(split) <= 0:
        raise ValueError(f"fold_id={args.fold} split={args.split} has no rows")
    chunk_rows = (
        max(1, int(args.chunk_rows))
        if args.chunk_rows is not None
        else _resolve_inference_model_chunk_rows(config, len(split))
    )
    scan_chunk_size = (
        max(1, int(args.scan_chunk_size))
        if args.scan_chunk_size is not None
        else _resolve_inference_backtest_chunk_rows(config, chunk_rows)
    )

    model = build_model(
        config=config,
        lookback=config.training.lookback,
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
    ).to(device)
    _load_state_dict(model, state_dict)

    date_indices = split.valid_indices.detach().cpu().numpy()
    dates = np.asarray(panel.dates[date_indices])
    date_start = str(panel.dates[int(date_indices[0])])
    date_end = str(panel.dates[int(date_indices[-1])])
    print(
        json.dumps(
            {
                "config": str(args.config),
                "fold_id": int(args.fold),
                "split": args.split,
                "rows": len(split),
                "symbols": int(panel.num_symbols),
                "lookback": int(config.training.lookback),
                "date_start": date_start,
                "date_end": date_end,
                "device": str(device),
                "amp_dtype": str(amp_dtype),
                "chunk_rows": int(chunk_rows),
                "scan_chunk_size": int(scan_chunk_size),
                "activations": activations,
                "thresholds": thresholds,
                "trained_output_mode": original_output_mode,
                "trained_activation": trained_activation,
                "plot_metrics": plot_metrics,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    started = time.perf_counter()
    trained_buffers = _collect_raw_scores(
        model=model,
        split=split,
        device=device,
        amp_dtype=amp_dtype,
        non_blocking=non_blocking,
        chunk_rows=chunk_rows,
    )
    trained_inference_elapsed_s = time.perf_counter() - started
    print(f"trained_output_inference_s={trained_inference_elapsed_s:.3f}", flush=True)

    if not hasattr(model, "portfolio_output_mode"):
        raise ValueError(f"Model does not expose portfolio_output_mode: {type(model).__name__}")
    model.portfolio_output_mode = "logits"  # type: ignore[attr-defined]
    started = time.perf_counter()
    raw_buffers = _collect_raw_scores(
        model=model,
        split=split,
        device=device,
        amp_dtype=amp_dtype,
        non_blocking=non_blocking,
        chunk_rows=chunk_rows,
    )
    raw_inference_elapsed_s = time.perf_counter() - started
    print(f"raw_score_inference_s={raw_inference_elapsed_s:.3f}", flush=True)

    buffers_by_mode = {
        "trained_output": trained_buffers,
        "raw_logits": raw_buffers,
    }
    trained_rows = _run_sweep(
        buffers=trained_buffers,
        mode="trained_output",
        model_output_mode=original_output_mode,
        activations=[trained_activation],
        thresholds=thresholds,
        config=config,
        scan_chunk_size=scan_chunk_size,
    )
    raw_rows = _run_sweep(
        buffers=raw_buffers,
        mode="raw_logits",
        model_output_mode="logits",
        activations=activations,
        thresholds=thresholds,
        config=config,
        scan_chunk_size=scan_chunk_size,
    )
    rows = trained_rows + raw_rows
    best_by = {
        metric: _best_by_metric(rows, metric)
        for metric in [
            "sharpe",
            "cumulative_return",
            "annualized_return",
            "sortino",
            "calmar",
            "max_drawdown",
            "turnover",
        ]
    }
    best_rank = _best_by_metric(rows, rank_metric)

    output_dir = Path(args.output_dir) if args.output_dir else fold_dir / "postprocess_benchmark"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{args.split}_activation_threshold_sweep.csv"
    json_path = output_dir / f"{args.split}_activation_threshold_sweep.json"
    md_path = output_dir / f"{args.split}_activation_threshold_sweep.md"

    summary = {
        "config": str(args.config),
        "fold_id": int(args.fold),
        "split": args.split,
        "rows": int(len(split)),
        "symbols": int(panel.num_symbols),
        "features": int(len(panel.feature_names)),
        "lookback": int(config.training.lookback),
        "date_start": date_start,
        "date_end": date_end,
        "device": str(device),
        "amp_dtype": str(amp_dtype),
        "chunk_rows": int(chunk_rows),
        "scan_chunk_size": int(scan_chunk_size),
        "activations": activations,
        "thresholds": thresholds,
        "trained_output_mode": original_output_mode,
        "trained_activation": trained_activation,
        "rank_metric": rank_metric,
        "plot_metrics": plot_metrics,
        "inference_elapsed_s": float(trained_inference_elapsed_s + raw_inference_elapsed_s),
        "trained_output_inference_elapsed_s": float(trained_inference_elapsed_s),
        "raw_logits_inference_elapsed_s": float(raw_inference_elapsed_s),
        "best_by_rank_metric": best_rank,
        "best_by_metric": best_by,
        "outputs": {
            "csv": str(csv_path),
            "json": str(json_path),
            "markdown": str(md_path),
        },
    }
    rows_sorted = sorted(
        rows,
        key=lambda row: float(row.get(rank_metric, 0.0)),
        reverse=rank_metric not in BEST_LOWER_IS_BETTER,
    )
    if args.plots:
        try:
            summary["outputs"]["plots"] = _write_metric_plots(
                output_dir=output_dir,
                split=args.split,
                rows=rows,
                buffers_by_mode=buffers_by_mode,
                config=config,
                scan_chunk_size=scan_chunk_size,
                dates=dates,
                activations=list(dict.fromkeys([row["candidate"] for row in rows])),
                thresholds=thresholds,
                metrics=plot_metrics,
                top_n=int(args.plot_top_n),
            )
        except Exception as exc:
            summary["outputs"]["plot_error"] = repr(exc)
            print(f"plot_error={exc!r}", flush=True)
    _write_csv(csv_path, rows_sorted)
    json_path.write_text(json.dumps({"summary": summary, "results": rows_sorted}, indent=2), encoding="utf-8")
    _write_markdown(md_path, summary, rows_sorted, rank_metric)

    print("BEST " + json.dumps(best_rank, sort_keys=True), flush=True)
    print(
        "BEST_BY_METRIC "
        + json.dumps({metric: best_by.get(metric) for metric in plot_metrics}, sort_keys=True),
        flush=True,
    )
    print(json.dumps(summary["outputs"], sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
