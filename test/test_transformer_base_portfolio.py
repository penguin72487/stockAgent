#!/usr/bin/env python3
"""Smoke tests for the scalable Transformer-base portfolio model."""

from pathlib import Path

import pytest
import torch

from stockagent.config import load_config
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel
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
