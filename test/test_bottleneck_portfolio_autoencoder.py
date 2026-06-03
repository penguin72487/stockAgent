#!/usr/bin/env python3
"""Forward and loss smoke tests for BottleneckPortfolioAutoencoder."""

import sys
from pathlib import Path

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stockagent.models.bottleneck_portfolio_autoencoder import BottleneckPortfolioAutoencoder
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _extract_weights_and_aux


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _make_model(**overrides) -> BottleneckPortfolioAutoencoder:
    torch.manual_seed(17)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(17)
    params = {
        "lookback": 20,
        "num_features": 21,
        "num_symbols": 100,
        "d_model": 128,
        "z_dim": 32,
        "temporal_type": "gru",
        "temporal_layers": 1,
        "asset_encoder_layers": 2,
        "n_heads": 4,
        "dropout": 0.1,
        "long_short": True,
        "noise_std": 0.01,
        "return_aux": True,
        "runtime_shape_check": True,
        "allow_dynamic_symbols": True,
    }
    params.update(overrides)
    return BottleneckPortfolioAutoencoder(**params).to(_device())


def test_long_only_forward_shape_mask_and_normalization() -> None:
    device = _device()
    model = _make_model(long_short=False).eval()
    x = torch.randn(4, 20, 100, 21, device=device)
    tradable_mask = torch.ones(4, 100, dtype=torch.bool, device=device)
    tradable_mask[:, 80:] = False

    with torch.no_grad():
        out = model(x, tradable_mask)
        weights, aux = _extract_weights_and_aux(out)

    assert weights.shape == (4, 100)
    assert aux is not None
    assert aux["z"].shape == (4, 100, 32)
    assert torch.all(weights >= 0.0)
    assert torch.allclose(weights.sum(dim=1), torch.ones(4, device=device), atol=1e-5)
    assert weights[:, 80:].abs().max().item() < 1e-7


def test_long_short_forward_shape_mask_and_normalization() -> None:
    device = _device()
    model = _make_model(long_short=True).eval()
    x = torch.randn(4, 20, 100, 21, device=device)
    tradable_mask = torch.ones(4, 100, dtype=torch.bool, device=device)
    tradable_mask[:, 60:] = False

    with torch.no_grad():
        weights, scores, aux = model(x, tradable_mask, return_aux=True)

    assert weights.shape == (4, 100)
    assert scores.shape == (4, 100)
    assert aux["z"].shape == (4, 100, 32)
    assert torch.allclose(weights.abs().sum(dim=1), torch.ones(4, device=device), atol=1e-5)
    assert weights[:, 60:].abs().max().item() < 1e-7


def test_tcn_temporal_option_and_empty_mask_row() -> None:
    device = _device()
    model = _make_model(temporal_type="tcn", d_model=32, z_dim=8, asset_encoder_layers=1).eval()
    x = torch.randn(4, 20, 100, 21, device=device)
    tradable_mask = torch.ones(4, 100, dtype=torch.bool, device=device)
    tradable_mask[0] = False

    with torch.no_grad():
        weights = model(x, tradable_mask, return_aux=False)

    assert weights.shape == (4, 100)
    assert torch.isfinite(weights).all()
    assert weights[0].abs().sum().item() == 0.0
    assert torch.allclose(weights[1:].abs().sum(dim=1), torch.ones(3, device=device), atol=1e-5)


def test_portfolio_autoencoder_loss_backward() -> None:
    device = _device()
    model = _make_model(d_model=64, z_dim=16, asset_encoder_layers=1).train()
    x = torch.randn(4, 20, 100, 21, device=device)
    tradable_mask = torch.ones(4, 100, dtype=torch.bool, device=device)
    returns = torch.randn(4, 100, device=device) * 0.01

    out = model(x, tradable_mask)
    weights, aux = _extract_weights_and_aux(out)
    loss = risk_aware_loss(
        weights,
        returns,
        tradable_mask,
        can_buy_mask=tradable_mask,
        can_sell_mask=tradable_mask,
        long_only=False,
        objective="portfolio_autoencoder",
        aux_outputs=aux,
        autoencoder_cost_rate=0.001425,
        autoencoder_lambda_turnover=0.1,
        autoencoder_lambda_concentration=0.01,
        autoencoder_lambda_latent=0.001,
    )
    assert torch.isfinite(loss).all()
    loss.backward()

    finite_grads = [
        param.grad.detach().isfinite().all()
        for param in model.parameters()
        if param.grad is not None
    ]
    assert finite_grads
    assert all(bool(ok.item()) for ok in finite_grads)


if __name__ == "__main__":
    print(f"Device: {_device()}")
    test_long_only_forward_shape_mask_and_normalization()
    test_long_short_forward_shape_mask_and_normalization()
    test_tcn_temporal_option_and_empty_mask_row()
    test_portfolio_autoencoder_loss_backward()
    print("SUCCESS")
