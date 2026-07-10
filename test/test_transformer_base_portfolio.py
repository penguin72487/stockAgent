#!/usr/bin/env python3
"""Smoke tests for the scalable Transformer-base portfolio model."""

from pathlib import Path

import pytest
import torch

from stockagent.config import load_config
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.transformer_base_portfolio import (
    PortfolioRMSNorm,
    SwiGLUFeedForward,
    TransformerBasePortfolioModel,
    _sanitize_scores_to_dtype,
)
from stockagent.models.normalization import (
    masked_activation_l1_weights,
    masked_l1_projection_weights,
    masked_signed_action_weights,
)
from stockagent.training.trainer import _extract_weights_and_aux
from stockagent.training.windowed import WindowedSplitTensors


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _make_model(**overrides) -> TransformerBasePortfolioModel:
    torch.manual_seed(23)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(23)
    params = {
        "lookback": 6,
        "num_features": 11,
        "num_symbols": 13,
        "d_model": 24,
        "attention_mode": "latent",
        "use_flash_attention": True,
        "use_time_pos": True,
        "use_symbol_pos": True,
        "input_dropout": 0.0,
        "sdpa_batch_limit": 4096,
        "norm_type": "rmsnorm",
        "ffn_type": "swiglu",
        "qk_norm": True,
        "rope_temporal": True,
        "rope_base": 10000.0,
        "temporal_layers": 1,
        "temporal_heads": 2,
        "temporal_ffn_mult": 1,
        "temporal_pooling": "attention",
        "temporal_query_mode": "full_then_last",
        "cross_layers": 1,
        "cross_heads": 2,
        "cross_ffn_mult": 1,
        "joint_layers": 1,
        "joint_heads": 2,
        "joint_ffn_mult": 1,
        "latent_layers": 1,
        "num_latent_factors": 4,
        "num_market_tokens": 2,
        "market_layers": 1,
        "head_hidden_dim": 24,
        "head_layers": 1,
        "dropout": 0.0,
        "default_temperature": 1.0,
        "portfolio_mode": "long_short",
        "max_full_tokens": 512,
        "checkpoint_blocks": False,
        "return_aux": True,
        "return_aux_details": True,
        "runtime_shape_check": True,
        "allow_dynamic_symbols": True,
    }
    params.update(overrides)
    return TransformerBasePortfolioModel(**params).to(_device())


@pytest.mark.parametrize("mode", ["full", "axial", "latent", "market_token", "temporal_only"])
def test_attention_modes_forward(mode: str) -> None:
    device = _device()
    model = _make_model(attention_mode=mode).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        out = model(x, mask)

    weights, aux = _extract_weights_and_aux(out)
    assert weights.shape == (2, 13)
    assert aux is not None
    assert aux["score_logits"].shape == (2, 13)
    assert torch.isfinite(weights).all()
    assert weights[1, 10:].abs().max().item() < 1e-6
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)
    assert bool((weights > 0).any().item())
    assert bool((weights < 0).any().item())


def test_full_mode_token_guard() -> None:
    device = _device()
    model = _make_model(attention_mode="full", max_full_tokens=8).eval()
    x = torch.randn(1, 6, 13, 11, device=device)
    mask = torch.ones(1, 13, dtype=torch.bool, device=device)

    with pytest.raises(ValueError, match="attention_mode=full"):
        model(x, mask)


def test_categorical_feature_embedding_forward_path() -> None:
    device = _device()
    model = _make_model(
        num_features=5,
        categorical_feature_indices=[2, 4],
        categorical_embedding_dim=3,
        categorical_embedding_cardinality=16,
    ).eval()
    x = torch.randn(2, 6, 13, 5, device=device)
    x[..., 2] = torch.randint(0, 10, x[..., 2].shape, device=device).float()
    x[..., 4] = torch.randint(0, 2, x[..., 4].shape, device=device).float()
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)

    with torch.no_grad():
        out = model(x, mask)

    weights, _ = _extract_weights_and_aux(out)
    assert model.categorical_feature_indices == (2, 4)
    assert len(model.categorical_embeddings) == 2
    assert weights.shape == (2, 13)
    assert torch.isfinite(weights).all()


def test_sdpa_batch_chunking_matches_unchunked_eval() -> None:
    device = _device()
    unchunked = _make_model(sdpa_batch_limit=0).eval()
    chunked = _make_model(sdpa_batch_limit=3).eval()
    chunked.load_state_dict(unchunked.state_dict())
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 9:] = False

    with torch.no_grad():
        out_a = unchunked(x, mask)
        out_b = chunked(x, mask)

    assert torch.allclose(out_a["weights"], out_b["weights"], atol=1e-5, rtol=1e-5)
    assert torch.allclose(out_a["score_logits"], out_b["score_logits"], atol=1e-5, rtol=1e-5)


def test_modern_components_and_learned_token_aux() -> None:
    device = _device()
    model = _make_model(attention_mode="latent").eval()
    assert isinstance(model.temporal_blocks[0].norm_query, PortfolioRMSNorm)
    assert isinstance(model.temporal_blocks[0].ffn, SwiGLUFeedForward)
    assert model.temporal_blocks[0].attn.qk_norm is True
    assert model.rope_temporal is True
    assert model.latent_queries is not None
    assert tuple(model.latent_queries.shape) == (1, 4, 24)
    assert model.market_queries is not None
    assert tuple(model.market_queries.shape) == (1, 2, 24)
    assert hasattr(model, "score_head")
    assert not hasattr(model, "mu_head")
    assert not hasattr(model, "sigma_head")
    assert not hasattr(model, "confidence_head")

    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    with torch.no_grad():
        out = model(x, mask)

    aux = out["aux"]
    assert aux["stock_embedding"].shape == (2, 13, 24)
    assert aux["factor_tokens"].shape == (2, 4, 24)
    assert aux["latent_factors"].shape == (2, 4, 24)
    assert torch.allclose(aux["factor_tokens"], aux["latent_factors"], atol=0.0, rtol=0.0)
    assert aux["market_tokens"].shape == (2, 2, 24)
    assert aux["z_factor_context"].shape == (2, 13, 24)
    assert aux["z_market_context"].shape == (2, 13, 24)
    assert aux["stock_market_gate"].shape == (2, 13, 1)
    assert aux["z_market_delta"].shape == (2, 13, 24)
    assert aux["score_logits"].shape == (2, 13)
    assert aux["rank_logits"].shape == (2, 13)
    assert "dynamic_latent_delta" not in aux
    assert "dynamic_market_delta" not in aux


@pytest.mark.parametrize("mode", ["latent", "market_token"])
def test_learned_token_attention_aux_contract(mode: str) -> None:
    device = _device()
    model = _make_model(attention_mode=mode).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 9:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    assert weights.shape == (2, 13)
    assert weights[1, 9:].abs().max().item() < 1e-6
    assert aux["stock_embedding"].shape == (2, 13, 24)
    assert aux["market_tokens"].shape == (2, 2, 24)
    assert aux["z_market_context"].shape == (2, 13, 24)
    assert aux["stock_market_gate"].shape == (2, 13, 1)
    if mode == "latent":
        assert aux["factor_tokens"].shape == (2, 4, 24)
        assert aux["latent_factors"].shape == (2, 4, 24)
        assert aux["z_factor_context"].shape == (2, 13, 24)
    else:
        assert "factor_tokens" not in aux
        assert "latent_factors" not in aux
        assert "z_factor_context" not in aux
    assert "dynamic_latent_delta" not in aux
    assert "dynamic_market_delta" not in aux


@pytest.mark.parametrize(
    "deprecated_kwarg",
    [
        "dynamic_latent_tokens",
        "dynamic_market_tokens",
        "dynamic_token_hidden_mult",
        "dynamic_token_gate_init",
        "dynamic_token_dropout",
    ],
)
def test_dynamic_token_kwargs_are_not_accepted(deprecated_kwarg: str) -> None:
    with pytest.raises(TypeError):
        _make_model(**{deprecated_kwarg: True})


def test_aux_details_false_keeps_training_output_light() -> None:
    device = _device()
    model = _make_model(attention_mode="latent", return_aux=True, return_aux_details=False).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)

    with torch.no_grad():
        light_out = model(x, mask)
        weights, scores, aux = model(x, mask, return_aux=True)

    assert set(light_out) == {
        "weights",
        "scores",
        "score_logits",
        "rank_logits",
        "centered_score_logits",
    }
    assert "aux" not in light_out
    assert torch.allclose(light_out["weights"], weights, atol=1e-6, rtol=1e-6)
    assert torch.allclose(light_out["scores"], scores, atol=1e-6, rtol=1e-6)
    assert aux["token_embedding"].shape == (2, 6, 13, 24)
    assert aux["factor_tokens"].shape == (2, 4, 24)
    assert aux["latent_factors"].shape == (2, 4, 24)
    assert aux["market_tokens"].shape == (2, 2, 24)
    assert aux["stock_market_gate"].shape == (2, 13, 1)


def _windows_from_panel(features: torch.Tensor, date_indices: torch.Tensor, lookback: int) -> torch.Tensor:
    return torch.stack(
        [
            features[int(idx.item()) - lookback + 1 : int(idx.item()) + 1]
            for idx in date_indices.detach().cpu()
        ],
        dim=0,
    )


@pytest.mark.parametrize("date_values", [[5, 6, 7], [5, 8, 10]])
def test_forward_from_panel_equivalence(date_values: list[int]) -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        return_aux=True,
        return_aux_details=False,
    ).eval()
    features = torch.randn(14, 13, 11)
    date_indices = torch.tensor(date_values, dtype=torch.long, device=device)
    x = _windows_from_panel(features, date_indices, model.lookback).to(device=device)
    mask = torch.ones(len(date_values), 13, dtype=torch.bool, device=device)
    mask[-1, 10:] = False

    with torch.no_grad():
        weights_a, scores_a, aux_a = model(x, mask, return_aux=True)
        weights_b, scores_b, aux_b = model.forward_from_panel(features, date_indices, mask, return_aux=True)

    assert torch.allclose(weights_a, weights_b, atol=1e-5, rtol=1e-5)
    assert torch.allclose(scores_a, scores_b, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_a["score_logits"], aux_b["score_logits"], atol=1e-5, rtol=1e-5)


def test_forward_from_panel_slab_equivalence_for_contiguous_rows() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        return_aux=True,
        return_aux_details=False,
    ).eval()
    features = torch.randn(14, 13, 11)
    date_indices = torch.tensor([5, 6, 7], dtype=torch.long, device=device)
    feature_slab = features.narrow(0, 0, 8)
    x = _windows_from_panel(features, date_indices, model.lookback).to(device=device)
    mask = torch.ones(3, 13, dtype=torch.bool, device=device)
    mask[-1, 10:] = False

    with torch.no_grad():
        weights_x, scores_x, aux_x = model(x, mask, return_aux=True)
        weights_panel, scores_panel, aux_panel = model.forward_from_panel(features, date_indices, mask, return_aux=True)
        weights_slab, scores_slab, aux_slab = model.forward_from_panel_slab(feature_slab, mask, return_aux=True)

    assert torch.allclose(weights_x, weights_panel, atol=1e-5, rtol=1e-5)
    assert torch.allclose(scores_x, scores_panel, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_x["score_logits"], aux_panel["score_logits"], atol=1e-5, rtol=1e-5)
    assert torch.allclose(weights_x, weights_slab, atol=1e-5, rtol=1e-5)
    assert torch.allclose(scores_x, scores_slab, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_x["score_logits"], aux_slab["score_logits"], atol=1e-5, rtol=1e-5)


def test_symbol_indices_preserve_full_universe_symbol_positions() -> None:
    device = _device()
    symbol_indices_cpu = torch.tensor([0, 2, 5, 8], dtype=torch.long)
    symbol_indices = symbol_indices_cpu.to(device=device)
    full = _make_model(
        attention_mode="market_token",
        num_symbols=13,
        allow_dynamic_symbols=False,
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        return_aux=True,
        return_aux_details=False,
    ).eval()
    compact = _make_model(
        attention_mode="market_token",
        num_symbols=int(symbol_indices_cpu.numel()),
        allow_dynamic_symbols=False,
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        return_aux=True,
        return_aux_details=False,
    ).eval()
    compact_state = compact.state_dict()
    for name, value in full.state_dict().items():
        if name == "symbol_position":
            compact_state[name].copy_(value.index_select(2, symbol_indices_cpu.to(device=value.device)))
        elif name in compact_state and tuple(compact_state[name].shape) == tuple(value.shape):
            compact_state[name].copy_(value)
    compact.load_state_dict(compact_state)

    x = torch.randn(2, 6, 4, 11, device=device)
    mask = torch.ones(2, 4, dtype=torch.bool, device=device)
    mask[1, 3:] = False
    feature_slab = torch.randn(7, 4, 11, device=device)

    with pytest.raises(ValueError, match="Expected num_symbols=13"):
        full(x, mask)

    with torch.no_grad():
        weights_full, scores_full, aux_full = full(x, mask, return_aux=True, symbol_indices=symbol_indices)
        weights_compact, scores_compact, aux_compact = compact(x, mask, return_aux=True)
        slab_full = full.forward_from_panel_slab(
            feature_slab,
            mask,
            return_aux=False,
            symbol_indices=symbol_indices,
        )
        slab_compact = compact.forward_from_panel_slab(feature_slab, mask, return_aux=False)

    assert torch.allclose(weights_full, weights_compact, atol=1e-5, rtol=1e-5)
    assert torch.allclose(scores_full, scores_compact, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_full["score_logits"], aux_compact["score_logits"], atol=1e-5, rtol=1e-5)
    assert torch.allclose(slab_full, slab_compact, atol=1e-5, rtol=1e-5)


@pytest.mark.parametrize("mode", ["axial", "latent", "market_token", "temporal_only"])
def test_last_pooling_fast_path_matches_full_temporal_path(mode: str) -> None:
    device = _device()
    model = _make_model(
        attention_mode=mode,
        temporal_pooling="last",
        temporal_layers=2,
        return_aux=True,
        return_aux_details=False,
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 11:] = False

    with torch.no_grad():
        fast_out = model(x, mask)
        full_weights, full_scores, full_aux = model(x, mask, return_aux=True)

    assert "aux" not in fast_out
    assert full_aux["token_embedding"].shape == (2, 6, 13, 24)
    assert torch.allclose(fast_out["weights"], full_weights, atol=1e-5, rtol=1e-5)
    assert torch.allclose(fast_out["scores"], full_scores, atol=1e-5, rtol=1e-5)


def test_last_only_temporal_shapes_and_finite() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        temporal_query_mode="last_only",
        return_aux=False,
        return_aux_details=False,
    ).eval()
    x = torch.randn(3, 6, 13, 11, device=device)
    mask = torch.ones(3, 13, dtype=torch.bool, device=device)
    mask[0, 9:] = False

    with torch.no_grad():
        weights = model(x, mask, return_aux=False)

    assert weights.shape == (3, 13)
    assert torch.isfinite(weights).all()
    assert weights[0, 9:].abs().max().item() < 1e-6


def test_market_token_fast_path_matches_generic_branch() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        return_aux=True,
        return_aux_details=True,
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 11:] = False
    safe_mask = mask.clone()
    h = model._embed_inputs(x)

    with torch.no_grad():
        z_fast, aux_fast = model._forward_market_token_fast(h, safe_mask, collect_aux=True)
        z_generic, aux_generic = model._forward_latent_or_market(
            h,
            safe_mask,
            use_latent=False,
            collect_aux=True,
        )

    assert torch.allclose(z_fast, z_generic, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_fast["market_tokens"], aux_generic["market_tokens"], atol=1e-5, rtol=1e-5)
    assert "latent_factors" not in aux_fast


def test_windowed_metadata_batch_has_no_x() -> None:
    features = torch.randn(8, 4, 3)
    valid_indices = torch.tensor([2, 3, 4, 6], dtype=torch.long)
    split = WindowedSplitTensors(
        features=features,
        valid_indices=valid_indices,
        future_log_returns=torch.randn(8, 4),
        tradable_mask=torch.ones(8, 4, dtype=torch.bool),
        can_buy_mask=torch.ones(8, 4, dtype=torch.bool),
        can_sell_mask=torch.ones(8, 4, dtype=torch.bool),
        benchmark=torch.randn(8),
        lookback=3,
        sample_mask=torch.tensor([True, False, True, True]),
    )

    batch = split.batch_metadata_by_rows(1, 3, torch.device("cpu"), non_blocking=False)
    indexed = split.batch_metadata_by_batch_indices(torch.tensor([0, 3]), torch.device("cpu"), non_blocking=False)

    assert "x" not in batch
    assert batch["date_indices"].tolist() == [3, 4]
    assert batch["date_start"].tolist() == [3]
    assert bool(batch["rows_are_contiguous"].item()) is True
    assert batch["sample_mask"].tolist() == [False, True]
    assert "x" not in indexed
    assert indexed["date_indices"].tolist() == [2, 6]
    assert indexed["date_start"].tolist() == [2]
    assert bool(indexed["rows_are_contiguous"].item()) is False

    compact = split.subset_symbols(torch.tensor([1, 3], dtype=torch.long))
    compact_batch = compact.batch_metadata_by_rows(1, 3, torch.device("cpu"), non_blocking=False)
    assert compact.features.shape == (8, 2, 3)
    assert compact.symbol_indices.tolist() == [1, 3]
    assert compact_batch["symbol_indices"].tolist() == [1, 3]
    assert compact_batch["tradable_mask"].shape == (2, 2)


def test_legacy_norm_and_ffn_can_be_configured() -> None:
    device = _device()
    model = _make_model(
        norm_type="layernorm",
        ffn_type="gelu",
        qk_norm=False,
        rope_temporal=False,
    ).eval()
    assert isinstance(model.temporal_blocks[0].norm_query, torch.nn.LayerNorm)
    assert not isinstance(model.temporal_blocks[0].ffn, SwiGLUFeedForward)
    assert model.temporal_blocks[0].attn.qk_norm is False

    x = torch.randn(1, 6, 13, 11, device=device)
    mask = torch.ones(1, 13, dtype=torch.bool, device=device)
    with torch.no_grad():
        out = model(x, mask)

    aux = out["aux"]
    assert "dynamic_latent_delta" not in aux
    assert "dynamic_market_delta" not in aux


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA-only SDPA batch-limit smoke")
def test_large_temporal_batch_uses_chunked_sdpa_without_cuda_invalid_argument() -> None:
    device = _device()
    model = _make_model(
        lookback=32,
        num_symbols=128,
        d_model=32,
        temporal_heads=4,
        cross_heads=4,
        joint_heads=4,
        head_hidden_dim=32,
        sdpa_batch_limit=1024,
        return_aux_details=False,
    ).eval()
    x = torch.randn(8, 32, 128, 11, device=device)
    mask = torch.ones(8, 128, dtype=torch.bool, device=device)

    with torch.no_grad():
        out = model(x, mask)
        torch.cuda.synchronize(device)

    assert out["weights"].shape == (8, 128)
    assert torch.isfinite(out["weights"]).all()


def test_long_only_mode_rejects_empty_rows() -> None:
    device = _device()
    model = _make_model(portfolio_mode="long_only").eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[0, :] = False

    with torch.no_grad(), pytest.raises(AssertionError, match="all-false row"):
        model(x, mask, return_aux=True)


def test_portfolio_output_mode_l1_uses_identity_l1_weights() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_activation="tanh",
        portfolio_output_mode="l1",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected = masked_activation_l1_weights(
        aux["centered_score_logits"],
        mask,
        long_only=False,
        activation="identity",
    )
    assert model.portfolio_output_mode == "l1"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)


def test_portfolio_output_mode_logits_returns_masked_centered_scores() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="logits",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected = aux["centered_score_logits"].masked_fill(~mask, 0.0)
    assert model.portfolio_output_mode == "logits"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_score_sanitization_preserves_finite_values_outside_legacy_clip() -> None:
    scores = torch.tensor([[-100.0, 100.0, float("nan"), float("inf"), -float("inf")]], dtype=torch.float32)

    sanitized = _sanitize_scores_to_dtype(scores)

    assert sanitized[0, 0].item() == -100.0
    assert sanitized[0, 1].item() == 100.0
    assert sanitized[0, 2].item() == 0.0
    assert sanitized[0, 3].item() == torch.finfo(torch.float32).max
    assert sanitized[0, 4].item() == torch.finfo(torch.float32).min


def test_l1_projection_preserves_large_finite_values_inside_radius() -> None:
    logits = torch.tensor([[100.0, -100.0]], dtype=torch.float32)
    mask = torch.ones_like(logits, dtype=torch.bool)

    projected = masked_l1_projection_weights(logits, mask, radius=250.0)

    assert torch.equal(projected, logits)


def test_long_short_logit_centering_can_be_disabled() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="logits",
        center_long_short_logits=False,
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected = aux["score_logits"].masked_fill(~mask, 0.0)
    assert not model.center_long_short_logits
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert torch.allclose(aux["centered_score_logits"], aux["score_logits"], atol=1e-6, rtol=1e-6)


def test_portfolio_output_mode_signed_softmax_matches_action_helper() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="signed_action_softmax",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected, expected_parts = masked_signed_action_weights(
        aux["centered_score_logits"],
        mask,
        transform="softmax",
        long_only=False,
        return_parts=True,
    )
    assert model.portfolio_output_mode == "signed_softmax"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert torch.allclose(aux["action_long_alloc"], expected_parts["action_long_alloc"], atol=1e-6, rtol=1e-6)
    assert torch.allclose(aux["action_short_alloc"], expected_parts["action_short_alloc"], atol=1e-6, rtol=1e-6)
    assert torch.allclose(aux["action_cash_alloc"], expected_parts["action_cash_alloc"], atol=1e-6, rtol=1e-6)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_portfolio_output_mode_signed_entmax_matches_sparse_action_helper() -> None:
    device = _device()
    direct_logits = torch.tensor([[8.0, 1.0, -8.0, 0.0]], dtype=torch.float32, device=device)
    direct_mask = torch.ones_like(direct_logits, dtype=torch.bool)
    _, direct_parts = masked_signed_action_weights(
        direct_logits,
        direct_mask,
        transform="entmax15",
        long_only=False,
        return_parts=True,
    )
    direct_actions = torch.cat(
        [
            direct_parts["action_long_alloc"],
            direct_parts["action_short_alloc"],
            direct_parts["action_cash_alloc"].view(1, 1),
        ],
        dim=1,
    )
    assert int((direct_actions <= 1e-7).sum().item()) >= 1

    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="signed_entmax",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected, _ = masked_signed_action_weights(
        aux["centered_score_logits"],
        mask,
        transform="entmax15",
        long_only=False,
        return_parts=True,
    )
    assert model.portfolio_output_mode == "signed_entmax15"
    assert torch.allclose(weights, expected, atol=1e-5, rtol=1e-5)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_portfolio_output_mode_signed_sparsemax_matches_sparse_action_helper() -> None:
    device = _device()
    direct_logits = torch.tensor([[4.0, 1.0, -4.0, 0.0]], dtype=torch.float32, device=device)
    direct_mask = torch.ones_like(direct_logits, dtype=torch.bool)
    _, direct_parts = masked_signed_action_weights(
        direct_logits,
        direct_mask,
        transform="sparsemax",
        long_only=False,
        return_parts=True,
    )
    direct_actions = torch.cat(
        [
            direct_parts["action_long_alloc"],
            direct_parts["action_short_alloc"],
            direct_parts["action_cash_alloc"].view(1, 1),
        ],
        dim=1,
    )
    assert int((direct_actions <= 1e-7).sum().item()) >= 1

    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="signed_sparsemax",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected, _ = masked_signed_action_weights(
        aux["centered_score_logits"],
        mask,
        transform="sparsemax",
        long_only=False,
        return_parts=True,
    )
    assert model.portfolio_output_mode == "signed_sparsemax"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_portfolio_output_mode_projection_l1_matches_projection_helper() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="differentiable_projection",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected = masked_l1_projection_weights(aux["centered_score_logits"], mask, long_only=False)
    assert model.portfolio_output_mode == "projection_l1"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert torch.allclose(aux["projection_gross_exposure"], weights.abs().sum(dim=1), atol=1e-6, rtol=1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_factory_builds_transformer_base_portfolio_model() -> None:
    cfg = load_config(Path("configs/experiment_baseline.yaml"))
    cfg.training.model_name = "transformer_base_portfolio"
    model = build_model(config=cfg, lookback=8, num_features=21, num_symbols=37)

    assert isinstance(model, TransformerBasePortfolioModel)
    assert model_hidden_dim_hint(cfg) == cfg.training.transformer_base_portfolio.d_model
    assert model.attention_mode == cfg.training.transformer_base_portfolio.attention_mode
    assert model.portfolio_mode == "long_short"
    assert model.sdpa_batch_limit == cfg.training.transformer_base_portfolio.sdpa_batch_limit
    assert model.norm_type == cfg.training.transformer_base_portfolio.norm_type
    assert model.ffn_type == cfg.training.transformer_base_portfolio.ffn_type
    assert model.qk_norm == cfg.training.transformer_base_portfolio.qk_norm
    assert model.rope_temporal == cfg.training.transformer_base_portfolio.rope_temporal
    assert model.temporal_query_mode == cfg.training.transformer_base_portfolio.temporal_query_mode
    assert model.portfolio_output_mode == cfg.training.transformer_base_portfolio.portfolio_output_mode
    if model.attention_mode == "market_token":
        assert len(model.latent_blocks) == 0
