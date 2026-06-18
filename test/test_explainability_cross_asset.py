from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import torch
from torch import nn

from stockagent.explainability_cross_asset import (
    MODULE_NAME,
    CrossAssetTransmissionSettings,
    abstract_cross_asset_transmission,
    _build_graph_explainability,
    _process_cross_asset_graph_edges,
)
from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel


class IndependentScoringModel(nn.Module):
    portfolio_mode = "long"
    default_temperature = 1.0

    def __init__(self, num_features: int) -> None:
        super().__init__()
        self.coef = nn.Parameter(torch.arange(1, num_features + 1, dtype=torch.float32))

    def _scores(self, x: torch.Tensor) -> torch.Tensor:
        return (x[:, -1] * self.coef).sum(dim=-1)

    def forward(self, x: torch.Tensor, mask: torch.Tensor, return_aux: bool | None = None):
        del return_aux
        x = torch.nan_to_num(x.float(), nan=0.0, posinf=0.0, neginf=0.0)
        scores = self._scores(x).masked_fill(~mask.bool(), -1e9)
        weights = torch.softmax(scores, dim=1).masked_fill(~mask.bool(), 0.0)
        centered = scores - scores.mean(dim=1, keepdim=True)
        return {
            "weights": weights,
            "score_logits": scores,
            "rank_logits": scores,
            "centered_score_logits": centered,
            "aux": {"z_stock": x[:, -1]},
        }


class CrossStockToyModel(IndependentScoringModel):
    def __init__(self, num_features: int, *, source: int = 0, target: int = 1, strength: float = 5.0) -> None:
        super().__init__(num_features)
        self.source = int(source)
        self.target = int(target)
        self.strength = float(strength)

    def _scores(self, x: torch.Tensor) -> torch.Tensor:
        scores = super()._scores(x)
        scores[:, self.target] = scores[:, self.target] + self.strength * x[:, -1, self.source, 0]
        return scores


def _batch(rows: int = 4, lookback: int = 3, symbols: int = 5, features: int = 4) -> dict[str, torch.Tensor]:
    torch.manual_seed(17)
    x = torch.randn(rows, lookback, symbols, features)
    x[:, -1, 0, 0] = 2.0
    return {
        "x": x,
        "future_log_returns": torch.randn(rows, symbols) * 0.01,
        "tradable_mask": torch.ones(rows, symbols, dtype=torch.bool),
    }


def _feature_names(features: int = 4) -> list[str]:
    return ["ret_feature", "volume_feature", "range_feature", "open_gap_feature"][:features]


def _symbols(symbols: int = 5) -> list[str]:
    return [f"S{idx}" for idx in range(symbols)]


def _dates(rows: int = 4) -> list[str]:
    return [f"2026-01-{idx + 1:02d}" for idx in range(rows)]


def _matrix_csv(path: Path) -> tuple[list[str], list[str], np.ndarray]:
    frame = pl.read_csv(path)
    source_symbols = frame["source_symbol"].cast(pl.String).to_list()
    target_symbols = [column for column in frame.columns if column != "source_symbol"]
    values = frame.select(target_symbols).to_numpy().astype(np.float64, copy=False)
    return source_symbols, target_symbols, values


def _graph_edge_frame(symbols: int = 4, shocks: tuple[str, ...] = ("zero", "volume")) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for shock_pos, shock in enumerate(shocks):
        for source_idx in range(symbols):
            for target_idx in range(symbols):
                rows.append(
                    {
                        "shock": shock,
                        "source_symbol": f"S{source_idx}",
                        "target_symbol": f"S{target_idx}",
                        "source_index": source_idx,
                        "target_index": target_idx,
                        "validated_transmission": float(
                            (source_idx + 1) * (target_idx + 2) + shock_pos
                        )
                        / 100.0,
                    }
                )
    return pl.DataFrame(rows)


def test_cross_asset_graph_auto_keeps_polars_below_benchmark_min_edges() -> None:
    edges = _graph_edge_frame()
    result = _process_cross_asset_graph_edges(
        edges,
        CrossAssetTransmissionSettings(
            top_edges=3,
            graph_backend="auto",
            graph_benchmark_min_edges=edges.height + 1,
        ),
    )

    assert result.backend == "polars"
    assert result.benchmark["selection_reason"] == "below_min_edges"
    assert result.benchmark["backends"]["polars"]["elapsed_s"] >= 0
    assert result.top_edges.height == 3
    assert result.top_edges["validated_transmission"].to_list() == sorted(
        result.top_edges["validated_transmission"].to_list(),
        reverse=True,
    )


def test_cross_asset_graph_cugraph_matches_polars_when_available() -> None:
    pytest.importorskip("cudf")
    pytest.importorskip("cugraph")
    if not torch.cuda.is_available():
        pytest.skip("cuGraph graph processing requires CUDA in this environment.")

    edges = _graph_edge_frame(symbols=5)
    result = _process_cross_asset_graph_edges(
        edges,
        CrossAssetTransmissionSettings(
            top_edges=5,
            graph_backend="cugraph",
            graph_benchmark_min_edges=0,
        ),
    )

    assert result.backend == "cugraph"
    assert result.benchmark["selected_backend"] == "cugraph"
    assert result.benchmark["validation"]["ok"] is True
    assert result.benchmark["backends"]["cugraph"]["graph_vertices"] == 5
    assert result.benchmark["backends"]["cugraph"]["graph_edges"] == 25
    assert not result.node_metrics.is_empty()
    assert {"symbol_index", "symbol", "weighted_out_degree", "weighted_in_degree", "pagerank"}.issubset(
        set(result.node_metrics.columns)
    )


def test_cross_asset_full_graph_cugraph_explainability_when_available() -> None:
    pytest.importorskip("cudf")
    pytest.importorskip("cugraph")
    if not torch.cuda.is_available():
        pytest.skip("cuGraph graph explainability requires CUDA in this environment.")

    edges = _graph_edge_frame(symbols=6)
    result = _build_graph_explainability(
        edges,
        CrossAssetTransmissionSettings(
            graph_backend="cugraph",
            graph_benchmark_min_edges=0,
            graph_betweenness_max_vertices=100,
        ),
    )

    assert result.backend == "cugraph"
    assert result.summary["graph_vertices"] == 6
    assert result.summary["graph_edges"] == 36
    assert {"pagerank", "hits", "louvain"}.issubset(set(result.summary["algorithms"]))
    assert not result.graph_edges.is_empty()
    assert not result.node_metrics.is_empty()
    assert not result.community_summary.is_empty()
    assert not result.community_edges.is_empty()
    assert {
        "symbol_index",
        "symbol",
        "weighted_out_degree",
        "weighted_in_degree",
        "pagerank",
        "hub_score",
        "authority_score",
        "community_id",
        "primary_role",
    }.issubset(set(result.node_metrics.columns))


def test_independent_model_off_diagonal_score_influence_near_zero(tmp_path: Path) -> None:
    batch = _batch()
    summary = abstract_cross_asset_transmission(
        IndependentScoringModel(num_features=4),
        batch,
        feature_names=_feature_names(),
        symbols=_symbols(),
        dates=_dates(),
        output_dir=tmp_path,
        settings=CrossAssetTransmissionSettings(
            max_sources=5,
            max_targets=5,
            source_chunk_size=2,
            shocks=("zero",),
            attention_flow=False,
            role_embedding=False,
        ),
        device=torch.device("cpu"),
    )

    assert summary["enabled"] is True
    source_symbols, target_symbols, values = _matrix_csv(tmp_path / MODULE_NAME / "matrices" / "zero_score_abs.csv")
    for source_idx, source_symbol in enumerate(source_symbols):
        for target_idx, target_symbol in enumerate(target_symbols):
            if source_symbol != target_symbol:
                assert abs(float(values[source_idx, target_idx])) < 1e-6


def test_cross_stock_toy_detects_injected_source_to_target_dependency(tmp_path: Path) -> None:
    batch = _batch()
    abstract_cross_asset_transmission(
        CrossStockToyModel(num_features=4, source=0, target=1, strength=7.0),
        batch,
        feature_names=_feature_names(),
        symbols=_symbols(),
        dates=_dates(),
        output_dir=tmp_path,
        settings=CrossAssetTransmissionSettings(
            max_sources=5,
            max_targets=5,
            source_chunk_size=2,
            shocks=("zero",),
            attention_flow=False,
            role_embedding=False,
        ),
        device=torch.device("cpu"),
    )

    edges = pl.read_csv(tmp_path / MODULE_NAME / "tables" / "edge_metrics.csv")
    injected = edges.filter((pl.col("source_index") == 0) & (pl.col("target_index") == 1)).row(0, named=True)
    unrelated = edges.filter((pl.col("source_index") == 2) & (pl.col("target_index") == 1)).row(0, named=True)
    assert float(injected["score_abs"]) > 5.0
    assert float(injected["score_abs"]) > float(unrelated["score_abs"]) + 1.0


def test_cross_asset_shape_nan_safety_and_missing_shocks(tmp_path: Path) -> None:
    batch = _batch(rows=3, lookback=2, symbols=4, features=4)
    batch["x"][0, 0, 0, 0] = float("nan")
    batch["x"][1, 1, 2, 1] = float("inf")
    batch["tradable_mask"][2, 3] = False
    summary = abstract_cross_asset_transmission(
        IndependentScoringModel(num_features=4),
        batch,
        feature_names=_feature_names(),
        symbols=_symbols(4),
        dates=_dates(3),
        output_dir=tmp_path,
        settings=CrossAssetTransmissionSettings(
            max_sources=3,
            max_targets=3,
            source_chunk_size=2,
            shocks=("zero", "liquidity", "not_a_feature"),
            attention_flow=False,
            role_embedding=True,
        ),
        device=torch.device("cpu"),
    )

    assert summary["sources"] <= 3
    assert summary["targets"] <= 3
    assert any("not_a_feature" in warning for warning in summary["warnings"])
    _, _, values = _matrix_csv(tmp_path / MODULE_NAME / "matrices" / "zero_weight_residual_abs.csv")
    assert np.isfinite(values).all()


def _tiny_transformer() -> TransformerBasePortfolioModel:
    return TransformerBasePortfolioModel(
        lookback=3,
        num_features=5,
        num_symbols=4,
        d_model=12,
        attention_mode="market_token",
        use_flash_attention=True,
        use_time_pos=True,
        use_symbol_pos=True,
        input_dropout=0.0,
        sdpa_batch_limit=128,
        norm_type="rmsnorm",
        ffn_type="swiglu",
        qk_norm=True,
        rope_temporal=True,
        rope_base=10000.0,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=1,
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        cross_layers=1,
        cross_heads=2,
        cross_ffn_mult=1,
        joint_layers=1,
        joint_heads=2,
        joint_ffn_mult=1,
        latent_layers=1,
        num_latent_factors=2,
        num_market_tokens=2,
        market_layers=1,
        dynamic_latent_tokens=True,
        dynamic_market_tokens=True,
        dynamic_token_hidden_mult=1,
        dynamic_token_gate_init=0.1,
        dynamic_token_dropout=0.0,
        head_hidden_dim=12,
        head_layers=1,
        dropout=0.0,
        default_temperature=1.0,
        portfolio_mode="long_short",
        max_full_tokens=256,
        checkpoint_blocks=False,
        return_aux=True,
        return_aux_details=True,
        runtime_shape_check=True,
        allow_dynamic_symbols=True,
    ).eval()


def test_attention_capture_smoke() -> None:
    torch.manual_seed(19)
    model = _tiny_transformer()
    model.configure_attention_capture(True, max_rows=1, max_elements=10000)
    x = torch.randn(2, 3, 4, 5)
    mask = torch.ones(2, 4, dtype=torch.bool)
    with torch.no_grad():
        out = model(x, mask, return_aux=True)
    captures = model.pop_attention_capture()
    model.configure_attention_capture(False)

    weights = out[0] if isinstance(out, tuple) else out["weights"]
    assert weights.shape == (2, 4)
    assert captures
    assert all(torch.is_tensor(item["attention"]) for item in captures)
    assert all(item["attention"].ndim == 3 for item in captures)


def test_cross_asset_output_writing(tmp_path: Path) -> None:
    batch = _batch()
    abstract_cross_asset_transmission(
        IndependentScoringModel(num_features=4),
        batch,
        feature_names=_feature_names(),
        symbols=_symbols(),
        dates=_dates(),
        output_dir=tmp_path,
        settings=CrossAssetTransmissionSettings(
            max_sources=3,
            max_targets=3,
            top_edges=4,
            source_chunk_size=1,
            shocks=("zero",),
            attention_flow=False,
            role_embedding=True,
        ),
        device=torch.device("cpu"),
    )

    base = tmp_path / MODULE_NAME
    summary = json.loads((base / "abstract_cross_asset_summary.json").read_text(encoding="utf-8"))
    assert summary["module"] == MODULE_NAME
    assert summary["graph_backend"] == "polars"
    assert summary["graph_benchmark"]["selection_reason"] == "below_min_edges"
    assert summary["graph_explainability"]["enabled"] is True
    assert summary["graph_explainability"]["backend"] in {"cugraph", "polars"}
    assert (base / "abstract_cross_asset_report.md").exists()
    assert (base / "tables" / "top_edges.csv").exists()
    assert (base / "tables" / "graph_edges.csv").exists()
    assert (base / "tables" / "graph_node_metrics.csv").exists()
    assert (base / "tables" / "graph_community_summary.csv").exists()
    assert (base / "tables" / "graph_community_edges.csv").exists()
    assert (base / "tables" / "shock_summary.csv").exists()
    assert (base / "tables" / "role_embeddings.csv").exists()
    assert (base / "matrices" / "zero_score_abs.csv").exists()
    assert (base / "matrices" / "zero_validated_transmission.csv").exists()
    shock_summary = pl.read_csv(base / "tables" / "shock_summary.csv")
    assert shock_summary["matched_feature_count"].to_list() == [4]
    assert shock_summary["matched_features"].to_list() == ["ret_feature;volume_feature;range_feature;open_gap_feature"]
