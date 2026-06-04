#!/usr/bin/env python3
"""Smoke tests for the low-rank market Transformer portfolio model."""

from pathlib import Path

import torch

from stockagent.config import load_config
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.low_rank_market_transformer_portfolio import LowRankMarketTransformerPortfolioModel
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _extract_weights_and_aux


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _make_model(**overrides) -> LowRankMarketTransformerPortfolioModel:
    torch.manual_seed(17)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(17)
    params = {
        "lookback": 8,
        "num_features": 21,
        "num_symbols": 100,
        "feature_dim": 32,
        "temporal_layers": 1,
        "temporal_heads": 2,
        "temporal_ffn_dim": 64,
        "temporal_dropout": 0.1,
        "temporal_pooling": "last",
        "temporal_checkpoint": True,
        "stock_embedding_dim": 32,
        "num_latent_factors": 16,
        "num_market_tokens": 4,
        "cross_heads": 2,
        "cross_ffn_mult": 2,
        "head_hidden_dim": 32,
        "head_layers": 1,
        "dropout": 0.1,
        "default_temperature": 1.0,
        "portfolio_mode": "long_short",
        "return_aux": True,
        "return_aux_details": False,
        "runtime_shape_check": True,
        "allow_dynamic_symbols": True,
    }
    params.update(overrides)
    return LowRankMarketTransformerPortfolioModel(**params).to(_device())


def test_default_output_is_trainer_compatible() -> None:
    device = _device()
    model = _make_model().eval()
    x = torch.randn(2, 8, 100, 21, device=device)
    mask = torch.ones(2, 100, dtype=torch.bool, device=device)

    with torch.no_grad():
        out = model(x, mask)
        weights, aux = _extract_weights_and_aux(out)

    assert isinstance(out, dict)
    assert weights.shape == (2, 100)
    assert aux is not None
    assert aux["score_logits"].shape == (2, 100)
    assert aux["rank_logits"].shape == (2, 100)
    assert "z_time" not in aux
    assert "latent_factors" not in aux
    assert weights.min().item() < 0.0
    assert weights.max().item() > 0.0
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)


def test_shapes_mask_and_backward() -> None:
    device = _device()
    model = _make_model().train()
    x = torch.randn(4, 8, 100, 21, device=device)
    mask = torch.ones(4, 100, dtype=torch.bool, device=device)
    mask[:, 12:] = False
    returns = torch.randn(4, 100, device=device) * 0.01

    weights, scores, aux = model(x, mask, return_aux=True)
    assert weights.shape == (4, 100)
    assert scores.shape == (4, 100)
    assert aux["z_time"].shape == (4, 100, 32)
    assert aux["z_stock"].shape == (4, 100, 32)
    assert aux["latent_factors"].shape == (4, 16, 32)
    assert aux["market_tokens"].shape == (4, 4, 32)
    assert aux["z_factor_context"].shape == (4, 100, 32)
    assert aux["z_market_context"].shape == (4, 100, 32)
    assert weights[:, 12:].abs().max().item() < 1e-6
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)
    assert weights[:, :12].min().item() < 0.0
    assert weights[:, :12].max().item() > 0.0

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


def test_dynamic_symbols_attention_pooling_and_token_counts() -> None:
    device = _device()
    model = _make_model(
        temporal_pooling="attention",
        num_latent_factors=32,
        num_market_tokens=8,
    ).eval()
    x = torch.randn(2, 8, 37, 21, device=device)
    mask = torch.ones(2, 37, dtype=torch.bool, device=device)

    with torch.no_grad():
        weights, scores, aux = model(x, mask, return_aux=True)

    assert weights.shape == (2, 37)
    assert scores.shape == (2, 37)
    assert aux["latent_factors"].shape == (2, 32, 32)
    assert aux["market_tokens"].shape == (2, 8, 32)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)


def test_default_can_opt_into_detailed_aux() -> None:
    device = _device()
    model = _make_model(return_aux_details=True).eval()
    x = torch.randn(2, 8, 100, 21, device=device)
    mask = torch.ones(2, 100, dtype=torch.bool, device=device)

    with torch.no_grad():
        out = model(x, mask)

    assert isinstance(out, dict)
    assert out["z_time"].shape == (2, 100, 32)
    assert out["latent_factors"].shape == (2, 16, 32)
    assert out["market_tokens"].shape == (2, 4, 32)


def test_long_only_and_empty_mask_rows_are_safe() -> None:
    device = _device()
    model = _make_model(portfolio_mode="long_only").eval()
    x = torch.randn(2, 8, 100, 21, device=device)
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


def test_factory_builds_low_rank_market_transformer_model() -> None:
    cfg = load_config(Path("configs/experiment_baseline.yaml"))
    cfg.training.model_name = "low_rank_market_transformer_portfolio"
    model = build_model(config=cfg, lookback=8, num_features=21, num_symbols=37)

    assert isinstance(model, LowRankMarketTransformerPortfolioModel)
    assert model_hidden_dim_hint(cfg) == cfg.training.low_rank_market_transformer_portfolio.stock_embedding_dim


if __name__ == "__main__":
    print(f"Device: {_device()}")
    test_default_output_is_trainer_compatible()
    test_shapes_mask_and_backward()
    test_dynamic_symbols_attention_pooling_and_token_counts()
    test_default_can_opt_into_detailed_aux()
    test_long_only_and_empty_mask_rows_are_safe()
    test_factory_builds_low_rank_market_transformer_model()
    print("SUCCESS")
