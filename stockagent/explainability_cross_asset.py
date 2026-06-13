from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import torch
from torch import nn

from stockagent.models.normalization import dual_branch_softmax, masked_cross_sectional_mean, masked_softmax


MODULE_NAME = "abstract_cross_asset_transmission"
DEFAULT_SHOCKS = ("zero", "momentum", "gap", "volume", "volatility", "liquidity")


@dataclass(slots=True)
class CrossAssetTransmissionSettings:
    enabled: bool = True
    max_sources: int = 32
    max_targets: int = 32
    top_edges: int = 200
    source_chunk_size: int = 4
    row_chunk_size: int = 0
    max_repeated_rows: int = 8
    perturb_scale: float = 1.0
    shocks: tuple[str, ...] = DEFAULT_SHOCKS
    attention_flow: bool = True
    attention_capture_rows: int = 4
    attention_capture_max_elements: int = 2_000_000
    validated_transmission: bool = True
    role_embedding: bool = True
    plot_top_k: int = 30


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
        value = min(value, 4)
    return value, {
        "reason": "repeated_row_budget",
        "row_chunk_size": value,
        "rows": n_rows,
        "symbols": int(n_symbols),
        "source_chunk_size": source_chunk,
        "max_repeated_rows": max_repeated_rows,
    }


def _sanitize_tensor(value: torch.Tensor) -> torch.Tensor:
    return torch.nan_to_num(value.detach().float(), nan=0.0, posinf=0.0, neginf=0.0)


def _to_numpy(value: torch.Tensor) -> np.ndarray:
    return np.nan_to_num(value.detach().float().cpu().numpy(), nan=0.0, posinf=0.0, neginf=0.0)


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


def _portfolio_weights_from_scores(model: nn.Module, scores: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    scores = _sanitize_tensor(scores)
    mask = mask.to(device=scores.device, dtype=torch.bool)
    temp = float(getattr(model, "default_temperature", 1.0))
    temp = max(0.05, temp)
    mode = str(getattr(model, "portfolio_mode", "long_short")).strip().lower()
    if mode in {"long", "long_only", "longonly"}:
        return masked_softmax(scores / temp, mask).masked_fill(~mask, 0.0)
    centered = scores - masked_cross_sectional_mean(scores, mask)
    return dual_branch_softmax(centered / temp, mask).masked_fill(~mask, 0.0)


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
    n_sources = min(max(1, int(max_sources)), n_active)
    n_targets = min(max(1, int(max_targets)), n_active)
    source_idx = torch.topk(score, k=n_sources).indices.detach().cpu().tolist()
    target_idx = torch.topk(score, k=n_targets).indices.detach().cpu().tolist()
    return [int(i) for i in source_idx], [int(i) for i in target_idx], _to_numpy(score)


def _mean_over_batch(value: torch.Tensor) -> np.ndarray:
    return _to_numpy(value.mean(dim=1))


def _empty_metric_buffers(n_sources: int, n_targets: int) -> dict[str, np.ndarray]:
    return {
        "score_abs": np.zeros((n_sources, n_targets), dtype=np.float32),
        "score_signed": np.zeros((n_sources, n_targets), dtype=np.float32),
        "weight_total_abs": np.zeros((n_sources, n_targets), dtype=np.float32),
        "weight_total_signed": np.zeros((n_sources, n_targets), dtype=np.float32),
        "weight_reallocation_abs": np.zeros((n_sources, n_targets), dtype=np.float32),
        "weight_residual_abs": np.zeros((n_sources, n_targets), dtype=np.float32),
        "rank_abs": np.zeros((n_sources, n_targets), dtype=np.float32),
        "flip_prob": np.zeros((n_sources, n_targets), dtype=np.float32),
        "transmission_pnl": np.zeros((n_sources, n_targets), dtype=np.float32),
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
) -> tuple[pd.DataFrame, list[str]]:
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
        return pd.DataFrame(), ["No stock-level aux tensor was available for role embedding."]
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
    return pd.DataFrame(rows), warnings


def _write_matrix_csv(path: Path, matrix: np.ndarray, source_symbols: list[str], target_symbols: list[str]) -> None:
    frame = pd.DataFrame(matrix, index=source_symbols, columns=target_symbols)
    frame.index.name = "source_symbol"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path)


def _plot_heatmap(path: Path, matrix: np.ndarray, title: str, source_symbols: list[str], target_symbols: list[str]) -> None:
    if matrix.size == 0:
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    max_rows = min(30, matrix.shape[0])
    max_cols = min(30, matrix.shape[1])
    data = matrix[:max_rows, :max_cols]
    fig_w = max(7.0, 0.32 * max_cols + 3.0)
    fig_h = max(5.5, 0.30 * max_rows + 2.0)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=140)
    vmax = float(np.nanpercentile(np.abs(data), 98)) if data.size else 1.0
    if vmax <= 0:
        vmax = 1.0
    image = ax.imshow(data, aspect="auto", cmap="magma", vmin=0.0, vmax=vmax)
    ax.set_title(title)
    ax.set_xlabel("target stock j")
    ax.set_ylabel("source stock i")
    ax.set_xticks(range(max_cols), [str(v) for v in target_symbols[:max_cols]], rotation=90, fontsize=7)
    ax.set_yticks(range(max_rows), [str(v) for v in source_symbols[:max_rows]], fontsize=7)
    fig.colorbar(image, ax=ax, shrink=0.8)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _plot_top_edges(path: Path, edges: pd.DataFrame) -> None:
    if edges.empty:
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    data = edges.head(30).copy()
    labels = data["shock"].astype(str) + " " + data["source_symbol"].astype(str) + " -> " + data["target_symbol"].astype(str)
    fig, ax = plt.subplots(figsize=(11, max(5, 0.28 * len(data) + 1.5)), dpi=140)
    ax.barh(np.arange(len(data)), data["validated_transmission"].to_numpy(dtype=np.float64))
    ax.set_yticks(np.arange(len(data)), labels, fontsize=7)
    ax.invert_yaxis()
    ax.set_xlabel("validated transmission")
    ax.set_title("Top Abstract Cross-Asset Transmission Edges")
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _normalize_matrix(matrix: np.ndarray) -> np.ndarray:
    matrix = np.nan_to_num(np.asarray(matrix, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    denom = float(np.nanmax(np.abs(matrix))) if matrix.size else 0.0
    return (np.abs(matrix) / denom).astype(np.float32) if denom > 0 else np.zeros_like(matrix, dtype=np.float32)


def abstract_cross_asset_transmission(
    model: nn.Module,
    batch: dict[str, torch.Tensor],
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
    device = device or next(model.parameters()).device
    x_cpu = torch.nan_to_num(batch["x"].detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
    mask_cpu = batch["tradable_mask"].detach().to(dtype=torch.bool)
    returns_cpu = torch.zeros_like(mask_cpu, dtype=torch.float32)
    if "future_log_returns" in batch:
        returns_cpu = torch.nan_to_num(batch["future_log_returns"].detach().float(), nan=0.0, posinf=0.0, neginf=0.0)
    n_rows, lookback, n_symbols, n_features = (
        int(x_cpu.size(0)),
        int(x_cpu.size(1)),
        int(x_cpu.size(2)),
        int(x_cpu.size(3)),
    )
    warnings: list[str] = []
    row_chunk_size, row_chunk_info = _auto_row_chunk_size(n_rows, n_symbols, settings)
    if row_chunk_size < n_rows:
        warnings.append(
            f"Cross-asset transmission used row microbatching: row_chunk_size={row_chunk_size}, rows={n_rows}."
        )

    was_training = model.training
    model.eval()
    weight_parts: list[torch.Tensor] = []
    score_parts: list[torch.Tensor] = []
    rank_parts: list[torch.Tensor] = []
    aux: dict[str, torch.Tensor] = {}
    with torch.no_grad():
        for row_start in range(0, n_rows, row_chunk_size):
            row_end = min(n_rows, row_start + row_chunk_size)
            x_row = x_cpu[row_start:row_end].to(device=device, non_blocking=(device.type == "cuda"))
            mask_row = mask_cpu[row_start:row_end].to(device=device, non_blocking=(device.type == "cuda"))
            weights_row, scores_row, rank_row, _centered_row, aux_row = _forward_outputs(
                model,
                x_row,
                mask_row,
                return_aux=not bool(aux),
            )
            weight_parts.append(weights_row.detach().cpu())
            score_parts.append(scores_row.detach().cpu())
            rank_parts.append(rank_row.detach().cpu())
            if not aux:
                aux = {str(key): value.detach().cpu() for key, value in aux_row.items() if torch.is_tensor(value)}
            del x_row, mask_row, weights_row, scores_row, rank_row, aux_row
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
    if was_training:
        model.train()
    base_weights = torch.cat(weight_parts, dim=0).masked_fill(~mask_cpu, 0.0)
    base_scores = torch.cat(score_parts, dim=0).masked_fill(~mask_cpu, 0.0)
    base_rank = torch.cat(rank_parts, dim=0)
    base_rank_pos = _rank_positions(base_rank, mask_cpu)
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

    feature_std = x_cpu.detach().float().std(dim=(0, 1, 2)).clamp_min(1e-6)
    attention_flow = None
    attention_rows: list[dict[str, Any]] = []
    if bool(settings.attention_flow):
        attention_rows_n = max(1, min(n_rows, int(settings.attention_capture_rows), row_chunk_size))
        x_attention = x_cpu[:attention_rows_n].to(device=device, non_blocking=(device.type == "cuda"))
        mask_attention = mask_cpu[:attention_rows_n].to(device=device, non_blocking=(device.type == "cuda"))
        attention_flow, attention_rows, attention_warnings = _capture_attention_flow(
            model,
            x_attention,
            mask_attention,
            n_symbols=n_symbols,
            rows=attention_rows_n,
            max_elements=settings.attention_capture_max_elements,
        )
        del x_attention, mask_attention
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        warnings.extend(attention_warnings)
    if attention_flow is None:
        attention_selected = np.zeros((len(source_idx), len(target_idx)), dtype=np.float32)
    else:
        attention_selected = attention_flow[np.ix_(source_idx, target_idx)].astype(np.float32, copy=False)
    attention_frame = pd.DataFrame(attention_rows)
    attention_frame.to_csv(tables_dir / "attention_capture_summary.csv", index=False)
    _write_matrix_csv(matrices_dir / "attention_flow.csv", attention_selected, source_symbols, target_symbols)

    all_edges: list[pd.DataFrame] = []
    shock_summaries: list[dict[str, Any]] = []
    requested_shocks = tuple(str(shock).strip().lower() for shock in settings.shocks if str(shock).strip())
    for shock in requested_shocks:
        shock_start = time.perf_counter()
        feature_idx = _feature_indices_for_shock(feature_names, shock)
        if not feature_idx:
            warnings.append(f"{shock}: no matching features; skipped.")
            continue
        buffers = _empty_metric_buffers(len(source_idx), len(target_idx))
        chunk_size = max(1, int(settings.source_chunk_size))
        source_pos = 0
        forward_batches = 0
        oom_retries = 0
        while source_pos < len(source_idx):
            chunk_sources = source_idx[source_pos : source_pos + chunk_size]
            repeats = len(chunk_sources)
            sl = slice(source_pos, source_pos + repeats)
            selected_targets = torch.as_tensor(target_idx, device=device, dtype=torch.long)
            accum = {name: np.zeros((repeats, len(target_idx)), dtype=np.float64) for name in buffers}
            row_weight_total = 0.0
            retry_source_chunk = False
            for row_start in range(0, n_rows, row_chunk_size):
                row_end = min(n_rows, row_start + row_chunk_size)
                row_count = row_end - row_start
                try:
                    with torch.no_grad():
                        x_row = x_cpu[row_start:row_end].to(device=device, non_blocking=(device.type == "cuda"))
                        mask_row = mask_cpu[row_start:row_end].to(device=device, non_blocking=(device.type == "cuda"))
                        returns_row = returns_cpu[row_start:row_end].to(device=device, non_blocking=(device.type == "cuda"))
                        base_weights_row = base_weights[row_start:row_end].to(device=device, non_blocking=(device.type == "cuda"))
                        base_scores_row = base_scores[row_start:row_end].to(device=device, non_blocking=(device.type == "cuda"))
                        base_rank_pos_row = base_rank_pos[row_start:row_end].to(device=device, non_blocking=(device.type == "cuda"))
                        feature_std_row = feature_std.to(device=device, non_blocking=(device.type == "cuda"))
                        x_rep = x_row.detach().unsqueeze(0).expand((repeats,) + tuple(x_row.shape)).clone()
                        for local_idx, source_symbol_idx in enumerate(chunk_sources):
                            _apply_shock(
                                x_rep,
                                local_idx,
                                source_symbol_idx,
                                feature_idx,
                                shock=shock,
                                scale=float(settings.perturb_scale),
                                feature_std=feature_std_row,
                            )
                        x_rep = x_rep.reshape(repeats * row_count, lookback, n_symbols, n_features)
                        mask_rep = mask_row.unsqueeze(0).expand(repeats, *tuple(mask_row.shape)).reshape(
                            repeats * row_count,
                            n_symbols,
                        )
                        weights_p, scores_p, rank_p, _centered_p, _aux_p = _forward_outputs(
                            model,
                            x_rep,
                            mask_rep,
                            return_aux=False,
                        )
                        weights_p = weights_p.reshape(repeats, row_count, n_symbols)
                        scores_p = scores_p.reshape(repeats, row_count, n_symbols)
                        rank_p = rank_p.reshape(repeats, row_count, n_symbols)
                except RuntimeError as exc:
                    if not _is_cuda_oom(exc) or chunk_size <= 1:
                        raise
                    oom_retries += 1
                    chunk_size = max(1, chunk_size // 2)
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                    retry_source_chunk = True
                    break
                forward_batches += 1
                score_delta = scores_p - base_scores_row.unsqueeze(0)
                weight_delta = weights_p - base_weights_row.unsqueeze(0)
                pert_rank_pos = _rank_positions(rank_p.reshape(repeats * row_count, n_symbols), mask_rep).reshape(
                    repeats,
                    row_count,
                    n_symbols,
                )
                rank_delta = pert_rank_pos - base_rank_pos_row.unsqueeze(0)

                norm_scores = base_scores_row.unsqueeze(0).expand(repeats, -1, -1).clone()
                for local_idx, source_symbol_idx in enumerate(chunk_sources):
                    norm_scores[local_idx, :, source_symbol_idx] = scores_p[local_idx, :, source_symbol_idx]
                norm_weights = _portfolio_weights_from_scores(
                    model,
                    norm_scores.reshape(repeats * row_count, n_symbols),
                    mask_rep,
                ).reshape(repeats, row_count, n_symbols)
                realloc_delta = norm_weights - base_weights_row.unsqueeze(0)
                residual_delta = weight_delta - realloc_delta
                row_weight = float(row_count)
                accum["score_abs"] += row_weight * _mean_over_batch(score_delta.index_select(2, selected_targets).abs())
                accum["score_signed"] += row_weight * _mean_over_batch(score_delta.index_select(2, selected_targets))
                accum["weight_total_abs"] += row_weight * _mean_over_batch(
                    weight_delta.index_select(2, selected_targets).abs()
                )
                accum["weight_total_signed"] += row_weight * _mean_over_batch(weight_delta.index_select(2, selected_targets))
                accum["weight_reallocation_abs"] += row_weight * _mean_over_batch(
                    realloc_delta.index_select(2, selected_targets).abs()
                )
                accum["weight_residual_abs"] += row_weight * _mean_over_batch(
                    residual_delta.index_select(2, selected_targets).abs()
                )
                accum["rank_abs"] += row_weight * _mean_over_batch(rank_delta.index_select(2, selected_targets).abs())
                base_target_weight = base_weights_row.index_select(1, selected_targets).unsqueeze(0)
                pert_target_weight = weights_p.index_select(2, selected_targets)
                accum["flip_prob"] += row_weight * _mean_over_batch((base_target_weight * pert_target_weight < 0).float())
                target_returns = returns_row.index_select(1, selected_targets).unsqueeze(0)
                accum["transmission_pnl"] += row_weight * _mean_over_batch(
                    weight_delta.index_select(2, selected_targets) * target_returns
                )
                row_weight_total += row_weight
                del (
                    x_row,
                    mask_row,
                    returns_row,
                    base_weights_row,
                    base_scores_row,
                    base_rank_pos_row,
                    feature_std_row,
                    x_rep,
                    mask_rep,
                    weights_p,
                    scores_p,
                    rank_p,
                )
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            if retry_source_chunk:
                continue
            denom = max(1.0, row_weight_total)
            for metric_name, values in accum.items():
                buffers[metric_name][sl, :] = (values / denom).astype(np.float32, copy=False)
            source_pos += repeats

        perturbation_evidence = _normalize_matrix(buffers["weight_residual_abs"])
        if bool(settings.validated_transmission) and attention_flow is not None:
            validated = perturbation_evidence * _normalize_matrix(attention_selected)
        else:
            validated = perturbation_evidence
        for metric_name, matrix in buffers.items():
            _write_matrix_csv(matrices_dir / f"{shock}_{metric_name}.csv", matrix, source_symbols, target_symbols)
        _write_matrix_csv(matrices_dir / f"{shock}_validated_transmission.csv", validated, source_symbols, target_symbols)
        _plot_heatmap(plots_dir / f"{shock}_validated_transmission.png", validated, f"{shock} validated transmission", source_symbols, target_symbols)
        _plot_heatmap(plots_dir / f"{shock}_weight_residual_abs.png", buffers["weight_residual_abs"], f"{shock} residual cross-stock influence", source_symbols, target_symbols)

        edge_rows: list[dict[str, Any]] = []
        for i, source_symbol in enumerate(source_symbols):
            for j, target_symbol in enumerate(target_symbols):
                row = {
                    "shock": shock,
                    "source_symbol": source_symbol,
                    "target_symbol": target_symbol,
                    "source_index": int(source_idx[i]),
                    "target_index": int(target_idx[j]),
                    "attention_flow": float(attention_selected[i, j]) if attention_selected.size else 0.0,
                    "validated_transmission": float(validated[i, j]),
                }
                row.update({name: float(matrix[i, j]) for name, matrix in buffers.items()})
                edge_rows.append(row)
        edge_frame = pd.DataFrame(edge_rows)
        all_edges.append(edge_frame)
        shock_summaries.append(
            {
                "shock": shock,
                "matched_features": [feature_names[idx] for idx in feature_idx],
                "source_chunk_size_final": int(chunk_size),
                "row_chunk_size": int(row_chunk_size),
                "forward_batches": int(forward_batches),
                "oom_retries": int(oom_retries),
                "max_validated_transmission": float(validated.max()) if validated.size else 0.0,
                "elapsed_s": float(time.perf_counter() - shock_start),
            }
        )

    edges = pd.concat(all_edges, ignore_index=True) if all_edges else pd.DataFrame()
    if not edges.empty:
        edges = edges.sort_values("validated_transmission", ascending=False)
        edges.to_csv(tables_dir / "edge_metrics.csv", index=False)
        top_edges = edges.head(max(1, int(settings.top_edges))).copy()
        top_edges.to_csv(tables_dir / "top_edges.csv", index=False)
        _plot_top_edges(plots_dir / "top_edges.png", top_edges)
    else:
        top_edges = pd.DataFrame()
        edges.to_csv(tables_dir / "edge_metrics.csv", index=False)
        top_edges.to_csv(tables_dir / "top_edges.csv", index=False)

    source_summary = edges.groupby("source_symbol", as_index=False)["validated_transmission"].sum() if not edges.empty else pd.DataFrame()
    target_summary = edges.groupby("target_symbol", as_index=False)["validated_transmission"].sum() if not edges.empty else pd.DataFrame()
    source_summary.to_csv(tables_dir / "source_summary.csv", index=False)
    target_summary.to_csv(tables_dir / "target_summary.csv", index=False)
    pd.DataFrame(shock_summaries).to_csv(tables_dir / "shock_summary.csv", index=False)

    role_warnings: list[str] = []
    if bool(settings.role_embedding):
        role_frame, role_warnings = _role_embedding_frame(aux, symbols, importance)
        role_frame.to_csv(tables_dir / "role_embeddings.csv", index=False)
        if not role_frame.empty:
            try:
                import matplotlib.pyplot as plt
                fig, ax = plt.subplots(figsize=(8, 6), dpi=140)
                ax.scatter(role_frame["role_x"], role_frame["role_y"], s=16, alpha=0.75)
                for row in role_frame.nlargest(20, "selection_importance").itertuples(index=False):
                    ax.text(float(row.role_x), float(row.role_y), str(row.symbol), fontsize=7)
                ax.set_title("Latent Stock Role Embedding")
                ax.set_xlabel("role_x")
                ax.set_ylabel("role_y")
                fig.tight_layout()
                fig.savefig(plots_dir / "role_embeddings.png")
                plt.close(fig)
            except Exception as exc:
                role_warnings.append(f"Role embedding plot failed: {type(exc).__name__}: {exc}")
    warnings.extend(role_warnings)

    top_preview = top_edges.head(20).to_dict(orient="records") if not top_edges.empty else []
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
        "shock_summaries": shock_summaries,
        "attention_available": bool(attention_flow is not None),
        "attention_capture_rows": attention_rows,
        "top_edges": top_preview,
        "warnings": warnings,
        "elapsed_s": float(time.perf_counter() - total_start),
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
    ]
    if top_preview:
        report_lines.extend(["## Top Edges", ""])
        for row in top_preview[:10]:
            report_lines.append(
                f"- `{row['shock']}` {row['source_symbol']} -> {row['target_symbol']}: "
                f"validated={float(row['validated_transmission']):.4f}, "
                f"residual={float(row['weight_residual_abs']):.4g}, pnl={float(row['transmission_pnl']):.4g}"
            )
        report_lines.append("")
    if warnings:
        report_lines.extend(["## Warnings", ""])
        report_lines.extend([f"- {warning}" for warning in warnings])
    (destination / "abstract_cross_asset_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return summary
