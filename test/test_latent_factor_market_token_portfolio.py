#!/usr/bin/env python3
"""Smoke tests for the latent-factor + market-token portfolio model."""

from pathlib import Path

import torch

from stockagent.config import load_config
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.latent_factor_market_token_portfolio import LatentFactorMarketTokenPortfolioModel
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _extract_weights_and_aux


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _make_model(**overrides) -> LatentFactorMarketTokenPortfolioModel:
    torch.manual_seed(11)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(11)
    params = {
        "lookback": 10,
        "num_features": 21,
        "num_symbols": 100,
        "temporal_enabled": True,
        "temporal_dim": 16,
        "temporal_hidden_channels": 32,
        "temporal_dilations": [1, 2],
        "temporal_kernel_size": 3,
        "tabular_dim": 64,
        "tabular_hidden_dim": 128,
        "tabular_blocks": 2,
        "stock_embedding_dim": 64,
        "num_latent_factors": 16,
        "num_market_tokens": 4,
        "num_heads": 4,
        "ffn_mult": 2,
        "head_hidden_dim": 64,
        "head_layers": 1,
        "dropout": 0.1,
        "residual_scale": 0.5,
        "default_temperature": 1.0,
        "portfolio_mode": "long_short",
        "return_aux": True,
        "runtime_shape_check": True,
        "allow_dynamic_symbols": True,
    }
    params.update(overrides)
    return LatentFactorMarketTokenPortfolioModel(**params).to(_device())


def test_default_output_is_trainer_compatible() -> None:
    device = _device()
    model = _make_model().eval()
    x = torch.randn(2, 10, 100, 21, device=device)
    mask = torch.ones(2, 100, dtype=torch.bool, device=device)

    with torch.no_grad():
        out = model(x, mask)
        weights, aux = _extract_weights_and_aux(out)

    assert isinstance(out, dict)
    assert weights.shape == (2, 100)
    assert aux is not None
    assert aux["score_logits"].shape == (2, 100)
    assert aux["rank_logits"].shape == (2, 100)
    assert aux["latent_factors"].shape == (2, 16, 64)
    assert aux["market_tokens"].shape == (2, 4, 64)
    assert weights.min().item() < 0.0
    assert weights.max().item() > 0.0
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)


def test_shapes_mask_and_backward() -> None:
    device = _device()
    model = _make_model().train()
    x = torch.randn(4, 10, 100, 21, device=device)
    mask = torch.ones(4, 100, dtype=torch.bool, device=device)
    mask[:, 10:] = False
    returns = torch.randn(4, 100, device=device) * 0.01

    weights, scores, aux = model(x, mask, return_aux=True)
    assert weights.shape == (4, 100)
    assert scores.shape == (4, 100)
    assert aux["z_time"].shape == (4, 100, 16)
    assert aux["z_feat"].shape == (4, 100, 64)
    assert aux["z_stock"].shape == (4, 100, 64)
    assert aux["latent_factors"].shape == (4, 16, 64)
    assert aux["market_tokens"].shape == (4, 4, 64)
    assert aux["z_factor_context"].shape == (4, 100, 64)
    assert aux["z_market_context"].shape == (4, 100, 64)
    assert weights[:, 10:].abs().max().item() < 1e-6
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)
    assert weights[:, :10].min().item() < 0.0
    assert weights[:, :10].max().item() > 0.0

    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        objective="sharpe",
        long_only=False,
        aux_outputs={"score_logits": scores, "rank_logits": scores},
    )
    loss.backward()
    finite_grads = [
        param.grad.detach().isfinite().all()
        for param in model.parameters()
        if param.grad is not None
    ]
    assert finite_grads
    assert all(bool(ok.item()) for ok in finite_grads)


def test_dynamic_symbols_and_token_counts() -> None:
    device = _device()
    model = _make_model(num_latent_factors=32, num_market_tokens=8).eval()
    x = torch.randn(2, 10, 37, 21, device=device)
    mask = torch.ones(2, 37, dtype=torch.bool, device=device)

    with torch.no_grad():
        weights, scores, aux = model(x, mask, return_aux=True)

    assert weights.shape == (2, 37)
    assert scores.shape == (2, 37)
    assert aux["latent_factors"].shape == (2, 32, 64)
    assert aux["market_tokens"].shape == (2, 8, 64)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)


def test_long_only_and_empty_mask_rows_are_safe() -> None:
    device = _device()
    model = _make_model(portfolio_mode="long_only").eval()
    x = torch.randn(2, 10, 100, 21, device=device)
    mask = torch.ones(2, 100, dtype=torch.bool, device=device)
    mask[0, :] = False
    mask[1, 50:] = False

    with torch.no_grad():
        weights, _, _ = model(x, mask, return_aux=True)

    assert torch.isfinite(weights).all()
    assert weights[0].abs().sum().item() == 0.0
    assert weights[:, 50:].abs().max().item() < 1e-6
    assert torch.all(weights >= 0.0)
    assert torch.allclose(weights[1:].sum(dim=1), torch.ones(1, device=device), atol=1e-5)


def test_factory_builds_latent_factor_market_token_model() -> None:
    cfg = load_config(Path("configs/experiment_baseline.yaml"))
    cfg.training.model_name = "latent_factor_market_token_portfolio"
    model = build_model(config=cfg, lookback=10, num_features=21, num_symbols=37)

    assert isinstance(model, LatentFactorMarketTokenPortfolioModel)
    assert model_hidden_dim_hint(cfg) == cfg.training.latent_factor_market_token_portfolio.stock_embedding_dim


if __name__ == "__main__":
    print(f"Device: {_device()}")
    test_default_output_is_trainer_compatible()
    test_shapes_mask_and_backward()
    test_dynamic_symbols_and_token_counts()
    test_long_only_and_empty_mask_rows_are_safe()
    test_factory_builds_latent_factor_market_token_model()
    print("SUCCESS")
