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
)
from stockagent.training.trainer import _extract_weights_and_aux


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
        "dynamic_latent_tokens": True,
        "dynamic_market_tokens": True,
        "dynamic_token_hidden_mult": 2,
        "dynamic_token_gate_init": 0.1,
        "dynamic_token_dropout": 0.0,
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


def test_modern_components_and_dynamic_token_aux() -> None:
    device = _device()
    model = _make_model(attention_mode="latent").eval()
    assert isinstance(model.temporal_blocks[0].norm_query, PortfolioRMSNorm)
    assert isinstance(model.temporal_blocks[0].ffn, SwiGLUFeedForward)
    assert model.temporal_blocks[0].attn.qk_norm is True
    assert model.rope_temporal is True
    assert model.dynamic_latent_generator is not None
    assert model.dynamic_market_generator is not None

    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    with torch.no_grad():
        out = model(x, mask)

    aux = out["aux"]
    assert aux["dynamic_latent_delta"].shape == (2, 4, 24)
    assert aux["dynamic_latent_queries"].shape == (2, 4, 24)
    assert aux["dynamic_market_delta"].shape == (2, 2, 24)
    assert aux["dynamic_market_queries"].shape == (2, 2, 24)
    assert aux["dynamic_latent_summary_parts"].shape == (2, 3, 24)
    assert aux["dynamic_market_summary_parts"].shape == (2, 3, 24)
    assert 0.0 < float(aux["dynamic_latent_gate"].item()) < 1.0
    assert 0.0 < float(aux["dynamic_market_gate"].item()) < 1.0
    assert aux["stock_market_gate"].shape == (2, 13, 1)
    assert aux["z_market_delta"].shape == (2, 13, 24)
    assert aux["alpha_mu"].shape == (2, 13)
    assert aux["risk_sigma"].shape == (2, 13)
    assert aux["confidence"].shape == (2, 13)
    assert torch.isfinite(aux["risk_sigma"]).all()
    assert bool((aux["risk_sigma"] > 0.0).all().item())
    assert bool(((aux["confidence"] >= 0.0) & (aux["confidence"] <= 1.0)).all().item())


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
        "alpha_mu",
        "risk_sigma",
        "confidence_logits",
        "confidence",
    }
    assert "aux" not in light_out
    assert torch.allclose(light_out["weights"], weights, atol=1e-6, rtol=1e-6)
    assert torch.allclose(light_out["scores"], scores, atol=1e-6, rtol=1e-6)
    assert aux["token_embedding"].shape == (2, 6, 13, 24)
    assert aux["dynamic_latent_delta"].shape == (2, 4, 24)
    assert aux["dynamic_market_delta"].shape == (2, 2, 24)
    assert aux["stock_market_gate"].shape == (2, 13, 1)


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


def test_legacy_norm_ffn_and_static_tokens_can_be_configured() -> None:
    device = _device()
    model = _make_model(
        norm_type="layernorm",
        ffn_type="gelu",
        qk_norm=False,
        rope_temporal=False,
        dynamic_latent_tokens=False,
        dynamic_market_tokens=False,
    ).eval()
    assert isinstance(model.temporal_blocks[0].norm_query, torch.nn.LayerNorm)
    assert not isinstance(model.temporal_blocks[0].ffn, SwiGLUFeedForward)
    assert model.temporal_blocks[0].attn.qk_norm is False
    assert model.dynamic_latent_generator is None
    assert model.dynamic_market_generator is None

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


def test_long_only_mode_and_empty_rows_are_safe() -> None:
    device = _device()
    model = _make_model(portfolio_mode="long_only").eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[0, :] = False

    with torch.no_grad():
        weights, _, _ = model(x, mask, return_aux=True)

    assert torch.isfinite(weights).all()
    assert weights[0].abs().sum().item() == 0.0
    assert torch.all(weights >= 0.0)
    assert torch.allclose(weights[1:].sum(dim=1), torch.ones(1, device=device), atol=1e-5)


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
    assert model.dynamic_latent_tokens == cfg.training.transformer_base_portfolio.dynamic_latent_tokens
    assert model.dynamic_market_tokens == cfg.training.transformer_base_portfolio.dynamic_market_tokens
