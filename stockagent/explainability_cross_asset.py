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

from stockagent.models.normalization import dual_branch_softmax, masked_cross_sectional_mean, masked_softmax


MODULE_NAME = "abstract_cross_asset_transmission"
DEFAULT_SHOCKS = ("zero", "momentum", "gap", "volume", "volatility", "liquidity")
_MATPLOTLIB_TRANSFORM_DOT_WARNING = r".*invalid value encountered in dot.*"
_GRAPH_BACKENDS = {"auto", "polars", "cugraph"}
_GRAPH_EDGE_KEY_COLUMNS = ["shock", "source_index", "target_index"]
_GRAPH_EDGE_SORT_COLUMNS = ["validated_transmission", "shock", "source_index", "target_index"]
_GRAPH_EDGE_SORT_DESCENDING = [True, False, False, False]


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
    graph_backend: str = "auto"
    graph_benchmark_min_edges: int = 1_000_000
    graph_explainability: bool = True
    graph_betweenness_max_vertices: int = 512
    graph_plot_max_nodes: int = 80


@dataclass(slots=True)
class _GraphProcessingResult:
    backend: str
    edges: pl.DataFrame
    top_edges: pl.DataFrame
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
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _plot_top_edges(path: Path, edges: pl.DataFrame) -> None:
    if edges.is_empty():
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    data = edges.head(30)
    labels = [
        f"{row['shock']} {row['source_symbol']} -> {row['target_symbol']}"
        for row in data.select(["shock", "source_symbol", "target_symbol"]).to_dicts()
    ]
    fig, ax = plt.subplots(figsize=(11, max(5, 0.28 * data.height + 1.5)), dpi=140)
    ax.barh(np.arange(data.height), data["validated_transmission"].to_numpy().astype(np.float64, copy=False))
    ax.set_yticks(np.arange(data.height), labels, fontsize=7)
    ax.invert_yaxis()
    ax.set_xlabel("validated transmission")
    ax.set_title("Top Abstract Cross-Asset Transmission Edges")
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _plot_graph_node_importance(path: Path, node_metrics: pl.DataFrame) -> None:
    if node_metrics.is_empty() or "pagerank" not in node_metrics.columns:
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    data = node_metrics.sort("pagerank", descending=True).head(25)
    labels = data["symbol"].cast(pl.String).to_list() if "symbol" in data.columns else data["symbol_index"].cast(pl.String).to_list()
    pagerank = data["pagerank"].fill_null(0.0).to_numpy().astype(np.float64, copy=False)
    hub = data["hub_score"].fill_null(0.0).to_numpy().astype(np.float64, copy=False) if "hub_score" in data.columns else np.zeros_like(pagerank)
    authority = (
        data["authority_score"].fill_null(0.0).to_numpy().astype(np.float64, copy=False)
        if "authority_score" in data.columns
        else np.zeros_like(pagerank)
    )
    y = np.arange(data.height)
    fig, ax = plt.subplots(figsize=(11, max(6, 0.28 * data.height + 2.0)), dpi=140)
    ax.barh(y - 0.23, pagerank, height=0.22, label="PageRank")
    ax.barh(y, hub, height=0.22, label="Hub")
    ax.barh(y + 0.23, authority, height=0.22, label="Authority")
    ax.set_yticks(y, labels, fontsize=7)
    ax.invert_yaxis()
    ax.set_xlabel("normalized graph score")
    ax.set_title("Cross-Asset Graph Node Importance")
    ax.legend(loc="lower right", fontsize=8)
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
    index = {community: pos for pos, community in enumerate(communities)}
    matrix = np.zeros((len(communities), len(communities)), dtype=np.float64)
    for row in community_edges.to_dicts():
        src = int(row.get("source_community", 0))
        dst = int(row.get("target_community", 0))
        if src in index and dst in index:
            matrix[index[src], index[dst]] += float(row.get("edge_weight", 0.0) or 0.0)
    fig, ax = plt.subplots(figsize=(max(6.0, 0.45 * len(communities) + 3.0), max(5.0, 0.45 * len(communities) + 2.5)), dpi=140)
    vmax = float(np.nanpercentile(matrix, 98)) if matrix.size else 1.0
    if vmax <= 0:
        vmax = 1.0
    image = ax.imshow(matrix, aspect="auto", cmap="viridis", vmin=0.0, vmax=vmax)
    ax.set_title("Cross-Asset Community Transmission Flow")
    ax.set_xlabel("target community")
    ax.set_ylabel("source community")
    ax.set_xticks(range(len(communities)), [str(value) for value in communities], fontsize=7)
    ax.set_yticks(range(len(communities)), [str(value) for value in communities], fontsize=7)
    fig.colorbar(image, ax=ax, shrink=0.8)
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _select_graph_backbone_edges(graph_edges: pl.DataFrame, *, max_edges: int, per_node: int = 2) -> pl.DataFrame:
    if graph_edges.is_empty():
        return pl.DataFrame()
    data = graph_edges.filter(pl.col("source_index") != pl.col("target_index")).sort(
        ["edge_weight", "source_index", "target_index"],
        descending=[True, False, False],
    )
    if data.is_empty():
        return data

    rows: list[dict[str, Any]] = []
    rows.extend(data.head(max(1, max_edges // 3)).to_dicts())
    for column in ("source_index", "target_index"):
        for node_id in sorted(int(value) for value in data[column].unique().to_list()):
            rows.extend(
                data.filter(pl.col(column) == node_id)
                .sort(["edge_weight", "source_index", "target_index"], descending=[True, False, False])
                .head(max(1, int(per_node)))
                .to_dicts()
            )

    deduped: dict[tuple[int, int], dict[str, Any]] = {}
    for row in rows:
        key = (int(row["source_index"]), int(row["target_index"]))
        if key not in deduped or float(row.get("edge_weight", 0.0) or 0.0) > float(
            deduped[key].get("edge_weight", 0.0) or 0.0
        ):
            deduped[key] = row
    if not deduped:
        return pl.DataFrame()
    return pl.DataFrame(list(deduped.values())).sort(
        ["edge_weight", "source_index", "target_index"],
        descending=[True, False, False],
    ).head(max_edges)


def _plot_graph_topology(path: Path, graph_edges: pl.DataFrame, node_metrics: pl.DataFrame, *, max_nodes: int) -> None:
    if graph_edges.is_empty() or node_metrics.is_empty():
        return
    try:
        import matplotlib.pyplot as plt
        from matplotlib.patches import FancyArrowPatch
    except Exception:
        return
    max_nodes = max(5, int(max_nodes))
    rank_column = "pagerank" if "pagerank" in node_metrics.columns else "weighted_out_degree"
    data = _select_graph_backbone_edges(graph_edges, max_edges=max(12, min(max_nodes + 8, 40)), per_node=1)
    if data.is_empty():
        return
    selected_ids = sorted(
        {
            int(value)
            for column in ("source_index", "target_index")
            for value in data[column].drop_nulls().to_list()
        }
    )
    if len(selected_ids) > max_nodes:
        ranked_ids = (
            node_metrics.filter(pl.col("symbol_index").is_in(selected_ids))
            .sort(rank_column, descending=True)
            .head(max_nodes)["symbol_index"]
            .to_list()
        )
        selected_ids = sorted(int(value) for value in ranked_ids)
        data = data.filter(pl.col("source_index").is_in(selected_ids) & pl.col("target_index").is_in(selected_ids))
    selected = node_metrics.filter(pl.col("symbol_index").is_in(selected_ids)).select(
        ["symbol_index", "symbol", rank_column]
        + (["community_id"] if "community_id" in node_metrics.columns else [])
    )
    node_rows = {int(row["symbol_index"]): row for row in selected.to_dicts()}

    source_strength: dict[int, float] = {}
    target_strength: dict[int, float] = {}
    for row in data.to_dicts():
        src = int(row["source_index"])
        dst = int(row["target_index"])
        weight = float(row.get("edge_weight", 0.0) or 0.0)
        source_strength[src] = source_strength.get(src, 0.0) + weight
        target_strength[dst] = target_strength.get(dst, 0.0) + weight
    if not source_strength or not target_strength:
        return
    sources = sorted(source_strength, key=lambda node: (-source_strength[node], str(node_rows.get(node, {}).get("symbol", node))))
    targets = sorted(target_strength, key=lambda node: (-target_strength[node], str(node_rows.get(node, {}).get("symbol", node))))
    max_rows = max(len(sources), len(targets))
    fig, ax = plt.subplots(figsize=(13.5, max(7.0, 0.32 * max_rows + 2.0)), dpi=150)
    ax.set_xlim(-0.34, 1.34)
    ax.set_ylim(-0.06, 1.08)
    source_y = {
        node: float(value)
        for node, value in zip(sources, np.linspace(0.96, 0.04, num=max(1, len(sources))))
    }
    target_y = {
        node: float(value)
        for node, value in zip(targets, np.linspace(0.96, 0.04, num=max(1, len(targets))))
    }
    weights = [float(row.get("edge_weight", 0.0) or 0.0) for row in data.to_dicts()]
    max_weight = max(weights) if weights else 1.0

    cmap = plt.get_cmap("tab20")
    communities = sorted(
        {
            int(row.get("community_id", 0) or 0)
            for row in node_rows.values()
        }
    )
    community_color = {community: cmap(idx % 20) for idx, community in enumerate(communities)}

    for row in sorted(data.to_dicts(), key=lambda value: float(value.get("edge_weight", 0.0) or 0.0)):
        src = int(row["source_index"])
        dst = int(row["target_index"])
        if src not in source_y or dst not in target_y:
            continue
        weight = float(row.get("edge_weight", 0.0) or 0.0)
        scaled = math.sqrt(weight / max_weight) if max_weight > 0 else 0.0
        rad = 0.12 if source_y[src] <= target_y[dst] else -0.12
        arrow = FancyArrowPatch(
            (0.08, source_y[src]),
            (0.92, target_y[dst]),
            arrowstyle="-|>",
            mutation_scale=7.0 + 4.0 * scaled,
            linewidth=0.45 + 2.4 * scaled,
            alpha=0.18 + 0.36 * scaled,
            color="#5f6b7a",
            connectionstyle=f"arc3,rad={rad}",
            zorder=1,
        )
        ax.add_patch(arrow)

    def node_size(node: int, strengths: Mapping[int, float]) -> float:
        max_strength = max(strengths.values()) if strengths else 1.0
        return 80.0 + 420.0 * math.sqrt(float(strengths.get(node, 0.0)) / max_strength)

    for node in sources:
        row = node_rows.get(node, {})
        color = community_color.get(int(row.get("community_id", 0) or 0), "#8da0cb")
        ax.scatter(0.04, source_y[node], s=node_size(node, source_strength), color=color, edgecolor="#222222", linewidth=0.7, zorder=3)
        ax.text(-0.01, source_y[node], str(row.get("symbol", node)), ha="right", va="center", fontsize=7)
    for node in targets:
        row = node_rows.get(node, {})
        color = community_color.get(int(row.get("community_id", 0) or 0), "#8da0cb")
        ax.scatter(0.96, target_y[node], s=node_size(node, target_strength), color=color, edgecolor="#222222", linewidth=0.7, zorder=3)
        ax.text(1.01, target_y[node], str(row.get("symbol", node)), ha="left", va="center", fontsize=7)

    ax.text(0.04, 1.025, "Source / transmitter", ha="center", va="bottom", fontsize=9, weight="bold")
    ax.text(0.96, 1.025, "Target / receiver", ha="center", va="bottom", fontsize=9, weight="bold")
    ax.set_title("Cross-Asset Transmission Backbone Flow")
    ax.text(
        0.01,
        0.01,
        "Backbone flow only: strongest inter-symbol paths. Full dense graph remains in graph_edges.csv and graph_transmission_matrix.png.",
        transform=ax.transAxes,
        fontsize=8,
        color="#4b5563",
        ha="left",
        va="bottom",
    )
    ax.axis("off")
    _safe_matplotlib_tight_layout(fig)
    _save_matplotlib_figure(fig, path)
    plt.close(fig)


def _plot_graph_transmission_matrix(path: Path, graph_edges: pl.DataFrame, node_metrics: pl.DataFrame, *, max_nodes: int) -> None:
    if graph_edges.is_empty() or node_metrics.is_empty():
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    rank_column = "pagerank" if "pagerank" in node_metrics.columns else "weighted_in_degree"
    sort_columns = ["community_id", rank_column] if "community_id" in node_metrics.columns else [rank_column]
    descending = [False, True] if "community_id" in node_metrics.columns else [True]
    ordered = node_metrics.sort(sort_columns, descending=descending).head(max(5, int(max_nodes)))
    ids = [int(value) for value in ordered["symbol_index"].to_list()]
    if not ids:
        return
    index = {node_id: pos for pos, node_id in enumerate(ids)}
    labels = ordered["symbol"].cast(pl.String).to_list() if "symbol" in ordered.columns else [str(value) for value in ids]
    matrix = np.zeros((len(ids), len(ids)), dtype=np.float64)
    for row in graph_edges.filter(pl.col("source_index").is_in(ids) & pl.col("target_index").is_in(ids)).to_dicts():
        src = int(row["source_index"])
        dst = int(row["target_index"])
        matrix[index[src], index[dst]] += float(row.get("edge_weight", 0.0) or 0.0)
    if not np.any(matrix):
        return
    vmax = float(np.nanpercentile(matrix[matrix > 0.0], 97)) if np.any(matrix > 0.0) else 1.0
    if vmax <= 0:
        vmax = 1.0
    fig_size = max(8.0, min(14.0, 0.38 * len(ids) + 4.0))
    fig, ax = plt.subplots(figsize=(fig_size + 1.5, fig_size), dpi=140)
    image = ax.imshow(matrix, aspect="equal", cmap="magma", vmin=0.0, vmax=vmax)
    ax.set_title("Full Cross-Asset Transmission Matrix")
    ax.set_xlabel("target / receiver")
    ax.set_ylabel("source / transmitter")
    ax.set_xticks(np.arange(len(ids)), labels, rotation=90, fontsize=6)
    ax.set_yticks(np.arange(len(ids)), labels, fontsize=6)
    ax.tick_params(length=0)
    ax.set_xticks(np.arange(-0.5, len(ids), 1), minor=True)
    ax.set_yticks(np.arange(-0.5, len(ids), 1), minor=True)
    ax.grid(which="minor", color="white", linestyle="-", linewidth=0.35, alpha=0.35)
    fig.colorbar(image, ax=ax, shrink=0.78, label="edge weight, clipped at p97")
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


def _plot_graph_self_influence(path: Path, graph_edges: pl.DataFrame) -> None:
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
    data = self_edges.head(30)
    labels = data["source_symbol"].cast(pl.String).to_list()
    weights = data["edge_weight"].fill_null(0.0).to_numpy().astype(np.float64, copy=False)
    y = np.arange(data.height)
    fig, ax = plt.subplots(figsize=(11, max(5.5, 0.28 * data.height + 1.8)), dpi=140)
    ax.barh(y, weights, color="#4777b3")
    ax.set_yticks(y, labels, fontsize=7)
    ax.invert_yaxis()
    ax.set_xlabel("self-loop edge weight")
    ax.set_title("Cross-Asset Graph Self Influence")
    ax.text(
        0.99,
        0.02,
        "Self-loops are excluded from the backbone topology to keep cross-symbol transmission readable.",
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
        warnings_out.append(f"Invalid cross-asset graph backend {raw!r}; using auto.")
        backend = "auto"
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


def _process_edges_polars(edges: pl.DataFrame, *, top_n: int) -> _GraphProcessingResult:
    start = time.perf_counter()
    sorted_edges = _sort_edges_polars(edges)
    top_edges = sorted_edges.head(top_n) if not sorted_edges.is_empty() else pl.DataFrame()
    source_summary = _summary_by_polars(sorted_edges, "source_symbol")
    target_summary = _summary_by_polars(sorted_edges, "target_symbol")
    elapsed_s = float(time.perf_counter() - start)
    return _GraphProcessingResult(
        backend="polars",
        edges=sorted_edges,
        top_edges=top_edges,
        source_summary=source_summary,
        target_summary=target_summary,
        node_metrics=pl.DataFrame(),
        benchmark={"elapsed_s": elapsed_s},
    )


def _cudf_to_polars(frame: Any) -> pl.DataFrame:
    return pl.from_pandas(frame.to_pandas())


def _process_edges_cugraph(edges: pl.DataFrame, *, top_n: int) -> _GraphProcessingResult:
    start = time.perf_counter()
    import cudf  # type: ignore[import-not-found]
    import cugraph  # type: ignore[import-not-found]
    import pandas as pd

    gdf = cudf.from_pandas(edges.to_pandas())
    sorted_gdf = gdf.sort_values(_GRAPH_EDGE_SORT_COLUMNS, ascending=[False, True, True, True])
    top_gdf = sorted_gdf.head(top_n)
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
    pagerank_pdf = pd.DataFrame(columns=["symbol_index", "pagerank"])
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=r".*Pagerank expects.*", category=UserWarning)
            pagerank_pdf = (
                cugraph.pagerank(graph)
                .rename(columns={"vertex": "symbol_index"})
                .to_pandas()
            )
    except Exception as exc:
        pagerank_error = f"{type(exc).__name__}: {exc}"

    source_degree_pdf = (
        graph_edges.groupby("source_index")["validated_transmission"]
        .sum()
        .reset_index()
        .rename(columns={"source_index": "symbol_index", "validated_transmission": "weighted_out_degree"})
        .to_pandas()
    )
    target_degree_pdf = (
        graph_edges.groupby("target_index")["validated_transmission"]
        .sum()
        .reset_index()
        .rename(columns={"target_index": "symbol_index", "validated_transmission": "weighted_in_degree"})
        .to_pandas()
    )
    symbol_lookup: dict[int, str] = {}
    for row in edges.select(["source_index", "source_symbol"]).unique().to_dicts():
        symbol_lookup[int(row["source_index"])] = str(row["source_symbol"])
    for row in edges.select(["target_index", "target_symbol"]).unique().to_dicts():
        symbol_lookup[int(row["target_index"])] = str(row["target_symbol"])
    node_pdf = pd.DataFrame({"symbol_index": sorted(symbol_lookup)})
    node_pdf["symbol"] = node_pdf["symbol_index"].map(symbol_lookup)
    node_pdf = node_pdf.merge(source_degree_pdf, on="symbol_index", how="left")
    node_pdf = node_pdf.merge(target_degree_pdf, on="symbol_index", how="left")
    node_pdf = node_pdf.merge(pagerank_pdf, on="symbol_index", how="left")
    for column in ("weighted_out_degree", "weighted_in_degree", "pagerank"):
        if column in node_pdf:
            node_pdf[column] = node_pdf[column].fillna(0.0)
    node_metrics = pl.from_pandas(node_pdf).sort("symbol_index")

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
        top_edges=_cudf_to_polars(top_gdf),
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
    top_n = max(1, int(settings.top_edges))
    polars_result = _process_edges_polars(edges, top_n=top_n)
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
        cugraph_result = _process_edges_cugraph(edges, top_n=top_n)
    except Exception as exc:
        benchmark["selection_reason"] = "cugraph_failed"
        benchmark["backends"]["cugraph"] = {
            "available": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if backend == "cugraph":
            benchmark["warnings"].append("cuGraph backend failed; using Polars output.")
        selected.benchmark = benchmark
        return selected

    benchmark["backends"]["cugraph"] = cugraph_result.benchmark | {"available": True}
    validation = _validate_graph_outputs(polars_result, cugraph_result)
    benchmark["validation"] = validation
    if not bool(validation["ok"]):
        benchmark["selection_reason"] = "validation_failed"
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
    pdf = frame.to_pandas()
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
        if column in pdf:
            thresholds[column] = float(pdf[column].fillna(0.0).quantile(0.75))
    roles: list[str] = []
    for _, row in pdf.iterrows():
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
    pdf["primary_role"] = roles
    return pl.from_pandas(pdf)


def _build_polars_graph_explainability(edges: pl.DataFrame, *, reason: str = "polars_fallback") -> _GraphExplainabilityResult:
    start = time.perf_counter()
    graph_edges = _aggregate_graph_edges(edges)
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
                "top_symbols": ", ".join(node_metrics.head(8)["symbol"].cast(pl.String).to_list()),
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


def _merge_metric_pdf(base: Any, metric: Any, *, rename: dict[str, str]) -> Any:
    metric_pdf = metric.rename(columns=rename).to_pandas()
    return base.merge(metric_pdf, on="symbol_index", how="left")


def _build_cugraph_graph_explainability(edges: pl.DataFrame, settings: CrossAssetTransmissionSettings) -> _GraphExplainabilityResult:
    start = time.perf_counter()
    import cudf  # type: ignore[import-not-found]
    import cugraph  # type: ignore[import-not-found]
    import pandas as pd

    graph_edges = _aggregate_graph_edges(edges)
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

    edge_pdf = graph_edges.to_pandas()
    graph_gdf = cudf.from_pandas(edge_pdf[["source_index", "target_index", "edge_weight"]])
    directed = cugraph.Graph(directed=True)
    _from_cudf_edgelist(directed, graph_gdf, store_transposed=True)
    undirected = cugraph.Graph(directed=False)
    _from_cudf_edgelist(undirected, graph_gdf, store_transposed=False)

    node_pdf = nodes.to_pandas()
    source_degree_pdf = (
        edge_pdf.groupby("source_index", as_index=False)["edge_weight"]
        .sum()
        .rename(columns={"source_index": "symbol_index", "edge_weight": "weighted_out_degree"})
    )
    target_degree_pdf = (
        edge_pdf.groupby("target_index", as_index=False)["edge_weight"]
        .sum()
        .rename(columns={"target_index": "symbol_index", "edge_weight": "weighted_in_degree"})
    )
    node_pdf = node_pdf.merge(source_degree_pdf, on="symbol_index", how="left")
    node_pdf = node_pdf.merge(target_degree_pdf, on="symbol_index", how="left")

    algorithms: list[str] = []
    skipped: list[dict[str, str]] = []

    def add_metric(name: str, fn: Any, rename: dict[str, str]) -> None:
        nonlocal node_pdf
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=r".*expects the 'store_transposed'.*", category=UserWarning)
                warnings.filterwarnings("ignore", category=PendingDeprecationWarning)
                metric = fn()
            node_pdf = _merge_metric_pdf(node_pdf, metric, rename=rename)
            algorithms.append(name)
        except Exception as exc:
            skipped.append({"algorithm": name, "reason": f"{type(exc).__name__}: {exc}"})

    add_metric("pagerank", lambda: cugraph.pagerank(directed), {"vertex": "symbol_index"})
    add_metric("hits", lambda: cugraph.hits(directed), {"vertex": "symbol_index", "hubs": "hub_score", "authorities": "authority_score"})
    add_metric(
        "eigenvector_centrality",
        lambda: cugraph.eigenvector_centrality(directed),
        {"vertex": "symbol_index"},
    )
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
    try:
        community_frame, modularity = cugraph.louvain(undirected)
        node_pdf = _merge_metric_pdf(
            node_pdf,
            community_frame,
            rename={"vertex": "symbol_index", "partition": "community_id"},
        )
        algorithms.append("louvain")
    except Exception as exc:
        skipped.append({"algorithm": "louvain", "reason": f"{type(exc).__name__}: {exc}"})
        try:
            community_frame, modularity = cugraph.leiden(undirected)
            node_pdf = _merge_metric_pdf(
                node_pdf,
                community_frame,
                rename={"vertex": "symbol_index", "partition": "community_id"},
            )
            algorithms.append("leiden")
        except Exception as leiden_exc:
            skipped.append({"algorithm": "leiden", "reason": f"{type(leiden_exc).__name__}: {leiden_exc}"})

    max_betweenness_vertices = max(0, int(settings.graph_betweenness_max_vertices))
    graph_vertex_count = int(nodes.height)
    graph_edge_count = int(graph_edges.height)
    if graph_vertex_count <= max_betweenness_vertices:
        add_metric(
            "betweenness_centrality",
            lambda: cugraph.betweenness_centrality(directed),
            {"vertex": "symbol_index"},
        )
    else:
        skipped.append(
            {
                "algorithm": "betweenness_centrality",
                "reason": f"graph_vertices>{max_betweenness_vertices}",
            }
        )

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
        if column not in node_pdf:
            node_pdf[column] = 0.0
        node_pdf[column] = node_pdf[column].fillna(0.0)
    if "community_id" not in node_pdf:
        node_pdf["community_id"] = 0
    node_pdf["community_id"] = node_pdf["community_id"].fillna(0).astype("int64")
    for column in ("strong_component_id", "weak_component_id"):
        if column not in node_pdf:
            node_pdf[column] = 0
        node_pdf[column] = node_pdf[column].fillna(0).astype("int64")

    node_metrics = _assign_graph_roles(pl.from_pandas(node_pdf)).sort("pagerank", descending=True)
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
                "top_symbols": ", ".join(members.sort("pagerank", descending=True).head(8)["symbol"].cast(pl.String).to_list()),
            }
        )
    community_summary = pl.DataFrame(community_rows).sort("total_pagerank", descending=True) if community_rows else pl.DataFrame()
    summary = {
        "enabled": True,
        "backend": "cugraph",
        "graph_vertices": graph_vertex_count,
        "graph_edges": graph_edge_count,
        "algorithms": algorithms,
        "skipped_algorithms": skipped,
        "modularity": float(modularity) if modularity is not None else None,
        "elapsed_s": float(time.perf_counter() - start),
    }
    return _GraphExplainabilityResult("cugraph", graph_edges, node_metrics, community_summary, community_edges, summary)


def _build_graph_explainability(
    edges: pl.DataFrame,
    settings: CrossAssetTransmissionSettings,
) -> _GraphExplainabilityResult:
    if not bool(settings.graph_explainability):
        summary = {"enabled": False, "backend": "disabled", "reason": "settings"}
        return _GraphExplainabilityResult("disabled", pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), summary)
    backend, backend_warnings = _resolve_graph_backend(settings)
    if backend == "polars":
        result = _build_polars_graph_explainability(edges, reason="backend_polars")
        result.summary["warnings"] = backend_warnings
        return result
    try:
        result = _build_cugraph_graph_explainability(edges, settings)
        result.summary["warnings"] = backend_warnings
        return result
    except Exception as exc:
        result = _build_polars_graph_explainability(edges, reason="cugraph_failed")
        result.summary["warnings"] = backend_warnings + [f"cuGraph graph explainability failed: {type(exc).__name__}: {exc}"]
        return result


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
    force_single_source_chunk = int(n_symbols) >= 10_000
    if force_single_source_chunk and int(settings.source_chunk_size) > 1:
        warnings.append(
            "Cross-asset transmission capped source_chunk_size to 1 for a large stock universe to avoid repeated-input VRAM blowups."
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
    attention_frame = pl.DataFrame(attention_rows)
    _write_frame_csv_or_parquet(tables_dir / "attention_capture_summary.csv", attention_frame)
    _write_matrix_csv(matrices_dir / "attention_flow.csv", attention_selected, source_symbols, target_symbols)

    all_edges: list[pl.DataFrame] = []
    shock_summaries: list[dict[str, Any]] = []
    requested_shocks = tuple(str(shock).strip().lower() for shock in settings.shocks if str(shock).strip())
    for shock in requested_shocks:
        shock_start = time.perf_counter()
        feature_idx = _feature_indices_for_shock(feature_names, shock)
        if not feature_idx:
            warnings.append(f"{shock}: no matching features; skipped.")
            continue
        buffers = _empty_metric_buffers(len(source_idx), len(target_idx))
        chunk_size = 1 if force_single_source_chunk else max(1, int(settings.source_chunk_size))
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
        edge_frame = pl.DataFrame(edge_rows)
        all_edges.append(edge_frame)
        shock_summaries.append(
            {
                "shock": shock,
                "matched_features": [feature_names[idx] for idx in feature_idx],
                "matched_feature_count": int(len(feature_idx)),
                "source_chunk_size_final": int(chunk_size),
                "row_chunk_size": int(row_chunk_size),
                "forward_batches": int(forward_batches),
                "oom_retries": int(oom_retries),
                "max_validated_transmission": float(validated.max()) if validated.size else 0.0,
                "elapsed_s": float(time.perf_counter() - shock_start),
            }
        )

    raw_edges = pl.concat(all_edges, how="diagonal_relaxed") if all_edges else pl.DataFrame()
    graph_result = _process_cross_asset_graph_edges(raw_edges, settings)
    edges = graph_result.edges
    top_edges = graph_result.top_edges
    warnings.extend(str(warning) for warning in graph_result.benchmark.get("warnings", ()))
    _write_frame_csv_or_parquet(tables_dir / "edge_metrics.csv", edges)
    _write_frame_csv_or_parquet(tables_dir / "top_edges.csv", top_edges)
    if not top_edges.is_empty():
        _plot_top_edges(plots_dir / "top_edges.png", top_edges)

    _write_frame_csv_or_parquet(tables_dir / "source_summary.csv", graph_result.source_summary)
    _write_frame_csv_or_parquet(tables_dir / "target_summary.csv", graph_result.target_summary)
    graph_explainability = _build_graph_explainability(edges, settings)
    for warning in graph_explainability.summary.get("warnings", ()):
        warnings.append(str(warning))
    if not graph_explainability.graph_edges.is_empty():
        _write_frame_csv_or_parquet(tables_dir / "graph_edges.csv", graph_explainability.graph_edges)
    if not graph_explainability.node_metrics.is_empty():
        _write_frame_csv_or_parquet(tables_dir / "graph_node_metrics.csv", graph_explainability.node_metrics)
        _plot_graph_node_importance(plots_dir / "graph_node_importance.png", graph_explainability.node_metrics)
    if not graph_explainability.community_summary.is_empty():
        _write_frame_csv_or_parquet(tables_dir / "graph_community_summary.csv", graph_explainability.community_summary)
    if not graph_explainability.community_edges.is_empty():
        _write_frame_csv_or_parquet(tables_dir / "graph_community_edges.csv", graph_explainability.community_edges)
        _plot_graph_community_flow(plots_dir / "graph_community_flow.png", graph_explainability.community_edges)
    if not graph_explainability.graph_edges.is_empty() and not graph_explainability.node_metrics.is_empty():
        graph_backbone_edges = _select_graph_backbone_edges(
            graph_explainability.graph_edges,
            max_edges=max(12, min(int(settings.graph_plot_max_nodes) + 8, 40)),
            per_node=1,
        )
        if not graph_backbone_edges.is_empty():
            _write_frame_csv_or_parquet(tables_dir / "graph_backbone_edges.csv", graph_backbone_edges)
        _plot_graph_topology(
            plots_dir / "graph_topology.png",
            graph_explainability.graph_edges,
            graph_explainability.node_metrics,
            max_nodes=int(settings.graph_plot_max_nodes),
        )
        _plot_graph_transmission_matrix(
            plots_dir / "graph_transmission_matrix.png",
            graph_explainability.graph_edges,
            graph_explainability.node_metrics,
            max_nodes=int(settings.graph_plot_max_nodes),
        )
        _plot_graph_self_influence(plots_dir / "graph_self_influence.png", graph_explainability.graph_edges)
    _write_frame_csv_or_parquet(tables_dir / "shock_summary.csv", _shock_summary_csv_frame(shock_summaries))

    role_warnings: list[str] = []
    if bool(settings.role_embedding):
        role_frame, role_warnings = _role_embedding_frame(aux, symbols, importance)
        _write_frame_csv_or_parquet(tables_dir / "role_embeddings.csv", role_frame)
        if not role_frame.is_empty():
            try:
                import matplotlib.pyplot as plt
                fig, ax = plt.subplots(figsize=(8, 6), dpi=140)
                ax.scatter(
                    role_frame["role_x"].to_numpy(),
                    role_frame["role_y"].to_numpy(),
                    s=16,
                    alpha=0.75,
                )
                for row in role_frame.sort("selection_importance", descending=True).head(20).to_dicts():
                    ax.text(float(row["role_x"]), float(row["role_y"]), str(row["symbol"]), fontsize=7)
                ax.set_title("Latent Stock Role Embedding")
                ax.set_xlabel("role_x")
                ax.set_ylabel("role_y")
                _safe_matplotlib_tight_layout(fig)
                _save_matplotlib_figure(fig, plots_dir / "role_embeddings.png")
                plt.close(fig)
            except Exception as exc:
                role_warnings.append(f"Role embedding plot failed: {type(exc).__name__}: {exc}")
    warnings.extend(role_warnings)

    top_preview = top_edges.head(20).to_dicts() if not top_edges.is_empty() else []
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
        "graph_backend": graph_result.backend,
        "graph_benchmark": graph_result.benchmark,
        "graph_explainability": graph_explainability.summary,
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
        "- `betweenness_centrality`: bridge stocks that sit on shortest transmission paths when the graph is small enough for exact computation.",
        "- `community_id`: Louvain/Leiden-style transmission community from the full weighted graph.",
        "- `primary_role`: rule-based label derived from the graph metrics: transmitter, receiver, bridge, systemic receiver, net source, net sink, or balanced.",
        "- `graph_topology.png`: readable source-to-target backbone flow; the complete dense graph remains in `graph_edges.csv`.",
        "- `graph_transmission_matrix.png`: full selected asset-level graph as a matrix, avoiding node-link edge crossings.",
        "- `graph_self_influence.png`: self-loop influence separated from the topology so cross-symbol flow remains legible.",
        "",
    ]
    if not graph_explainability.node_metrics.is_empty():
        report_lines.extend(["## Top Graph Nodes", ""])
        for row in graph_explainability.node_metrics.sort("pagerank", descending=True).head(10).to_dicts():
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
        for row in graph_explainability.community_summary.head(10).to_dicts():
            report_lines.append(
                f"- community `{int(row.get('community_id', 0) or 0)}` nodes={int(row.get('node_count', 0) or 0)}, "
                f"pagerank={float(row.get('total_pagerank', 0.0) or 0.0):.4f}, "
                f"internal={float(row.get('internal_weight', 0.0) or 0.0):.4g}, "
                f"top={row.get('top_symbols', '')}"
            )
        report_lines.append("")
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
