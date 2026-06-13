from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch import nn

from stockagent.explainability_cross_asset import (
    MODULE_NAME,
    CrossAssetTransmissionSettings,
    abstract_cross_asset_transmission,
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
    matrix = pd.read_csv(tmp_path / MODULE_NAME / "matrices" / "zero_score_abs.csv", index_col=0)
    for source_symbol in matrix.index:
        for target_symbol in matrix.columns:
            if source_symbol != target_symbol:
                assert abs(float(matrix.loc[source_symbol, target_symbol])) < 1e-6


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

    edges = pd.read_csv(tmp_path / MODULE_NAME / "tables" / "edge_metrics.csv")
    injected = edges[(edges["source_index"] == 0) & (edges["target_index"] == 1)].iloc[0]
    unrelated = edges[(edges["source_index"] == 2) & (edges["target_index"] == 1)].iloc[0]
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
    matrix = pd.read_csv(tmp_path / MODULE_NAME / "matrices" / "zero_weight_residual_abs.csv", index_col=0)
    assert np.isfinite(matrix.to_numpy(dtype=np.float64)).all()


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
    assert (base / "abstract_cross_asset_report.md").exists()
    assert (base / "tables" / "top_edges.csv").exists()
    assert (base / "tables" / "role_embeddings.csv").exists()
    assert (base / "matrices" / "zero_score_abs.csv").exists()
    assert (base / "matrices" / "zero_validated_transmission.csv").exists()
