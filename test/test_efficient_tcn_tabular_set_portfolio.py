#!/usr/bin/env python3
"""Smoke tests for the low-complexity TCN + Tabular ResNet + LiteISAB model."""

import torch

from stockagent.models.efficient_tcn_tabular_set_portfolio import EfficientTCNTabularSetPortfolioModel
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _extract_weights_and_aux


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _make_model(**overrides) -> EfficientTCNTabularSetPortfolioModel:
    torch.manual_seed(7)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(7)
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
        "model_dim": 64,
        "set_enabled": True,
        "num_inducing_points": 16,
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
    return EfficientTCNTabularSetPortfolioModel(**params).to(_device())


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
    assert aux["z_fused"].shape == (4, 100, 64)
    assert aux["z_set"].shape == (4, 100, 64)
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


def test_ablation_modes_and_dynamic_symbols() -> None:
    device = _device()
    x = torch.randn(2, 10, 37, 21, device=device)
    mask = torch.ones(2, 37, dtype=torch.bool, device=device)

    for overrides in (
        {"temporal_enabled": False},
        {"set_enabled": False},
        {"num_inducing_points": 8},
    ):
        model = _make_model(**overrides).eval()
        with torch.no_grad():
            weights, scores, aux = model(x, mask, return_aux=True)
        assert weights.shape == (2, 37)
        assert scores.shape == (2, 37)
        assert aux["z_feat"].shape[:2] == (2, 37)
        assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)


def test_long_only_mode_still_sums_to_one() -> None:
    device = _device()
    model = _make_model(portfolio_mode="long_only").eval()
    x = torch.randn(2, 10, 100, 21, device=device)
    mask = torch.ones(2, 100, dtype=torch.bool, device=device)
    mask[:, 50:] = False

    with torch.no_grad():
        weights, _, _ = model(x, mask, return_aux=True)

    assert weights[:, 50:].abs().max().item() < 1e-6
    assert torch.all(weights >= 0.0)
    assert torch.allclose(weights.sum(dim=1), torch.ones(2, device=device), atol=1e-5)


if __name__ == "__main__":
    print(f"Device: {_device()}")
    test_default_output_is_trainer_compatible()
    test_shapes_mask_and_backward()
    test_ablation_modes_and_dynamic_symbols()
    test_long_only_mode_still_sums_to_one()
    print("SUCCESS")
