#!/usr/bin/env python3
"""Smoke tests for the Gradient Boosted Portfolio Transformer."""

from pathlib import Path

import torch

from stockagent.config import load_config
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.gradient_boosted_portfolio_transformer import GradientBoostedPortfolioTransformer
from stockagent.training.trainer import _extract_weights_and_aux


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _make_model(**overrides) -> GradientBoostedPortfolioTransformer:
    torch.manual_seed(37)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(37)
    params = {
        "lookback": 5,
        "num_features": 7,
        "num_symbols": 11,
        "d_model": 16,
        "temporal_layers": 1,
        "temporal_heads": 2,
        "temporal_ffn_mult": 1,
        "market_layers": 1,
        "market_heads": 2,
        "market_ffn_mult": 1,
        "num_market_tokens": 2,
        "head_hidden_dim": 16,
        "head_layers": 1,
        "dropout": 0.0,
        "input_dropout": 0.0,
        "use_time_pos": True,
        "use_symbol_pos": False,
        "dynamic_market_tokens": True,
        "dynamic_token_gate_init": 0.1,
        "num_residual_stages": 2,
        "stage_eta": [0.5, 0.25],
        "trainable_eta": True,
        "eta_max": 1.0,
        "detach_stage_condition": True,
        "default_temperature": 1.0,
        "portfolio_mode": "long_short",
        "portfolio_activation": "pre_normalized",
        "portfolio_output_mode": "projection_l1",
        "center_final_logits": True,
        "return_aux": True,
        "return_aux_details": True,
        "runtime_shape_check": True,
        "allow_dynamic_symbols": True,
    }
    params.update(overrides)
    return GradientBoostedPortfolioTransformer(**params).to(_device())


def test_gradient_boosted_transformer_forward_masks_and_projects() -> None:
    device = _device()
    model = _make_model().eval()
    x = torch.randn(2, 5, 11, 7, device=device)
    mask = torch.ones(2, 11, dtype=torch.bool, device=device)
    mask[1, 8:] = False

    with torch.no_grad():
        out = model(x, mask)

    weights, aux = _extract_weights_and_aux(out)
    assert weights.shape == (2, 11)
    assert aux is not None
    assert aux["aggregate_logits"].shape == (2, 11)
    assert aux["boost_eta"].shape == (2,)
    assert torch.isfinite(weights).all()
    assert weights[1, 8:].abs().max().item() < 1e-6
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)


def test_residual_stage_deltas_are_zero_initialized() -> None:
    device = _device()
    model = _make_model().eval()
    x = torch.randn(2, 5, 11, 7, device=device)
    mask = torch.ones(2, 11, dtype=torch.bool, device=device)

    with torch.no_grad():
        _, _, aux = model(x, mask, return_aux=True)

    assert aux["delta_logits_1"].abs().max().item() < 1e-7
    assert aux["delta_logits_2"].abs().max().item() < 1e-7


def test_panel_slab_path_matches_materialized_forward() -> None:
    device = _device()
    model = _make_model().eval()
    feature_slab = torch.randn(8, 11, 7, device=device)
    x = feature_slab.unfold(0, 5, 1).permute(0, 3, 1, 2).contiguous()
    mask = torch.ones(4, 11, dtype=torch.bool, device=device)
    mask[2, 9:] = False

    with torch.no_grad():
        direct = model(x, mask)
        slab = model.forward_from_panel_slab(feature_slab, mask)

    direct_weights, _ = _extract_weights_and_aux(direct)
    slab_weights, _ = _extract_weights_and_aux(slab)
    assert torch.allclose(direct_weights, slab_weights, atol=1e-6, rtol=1e-6)


def test_panel_date_index_path_materializes_full_lookback_window() -> None:
    device = _device()
    model = _make_model().eval()
    features = torch.randn(12, 11, 7, device=device)
    date_indices = torch.tensor([4, 5, 6, 7], dtype=torch.long, device=device)
    offsets = torch.arange(4, -1, -1, dtype=torch.long, device=device)
    x = features[date_indices[:, None] - offsets[None, :]]
    mask = torch.ones(4, 11, dtype=torch.bool, device=device)
    mask[1, 8:] = False

    with torch.no_grad():
        direct = model(x, mask)
        panel = model.forward_from_panel(features, date_indices, mask)

    direct_weights, _ = _extract_weights_and_aux(direct)
    panel_weights, _ = _extract_weights_and_aux(panel)
    assert torch.allclose(direct_weights, panel_weights, atol=1e-6, rtol=1e-6)


def test_all_masked_rows_keep_attention_and_gradients_finite() -> None:
    device = _device()
    model = _make_model().train()
    x = torch.randn(3, 5, 11, 7, device=device)
    mask = torch.ones(3, 11, dtype=torch.bool, device=device)
    mask[1] = False

    weights, scores, aux = model(x, mask, return_aux=True)

    assert torch.isfinite(weights).all()
    assert torch.isfinite(scores).all()
    assert torch.isfinite(aux["base_market_tokens"]).all()
    assert torch.isfinite(aux["stage_1_market_tokens"]).all()
    assert weights[1].abs().sum().item() == 0.0

    loss = weights.square().sum()
    assert torch.isfinite(loss)
    loss.backward()

    for param in model.parameters():
        if param.grad is not None:
            assert torch.isfinite(param.grad).all()


def test_factory_builds_gradient_boosted_portfolio_transformer() -> None:
    cfg = load_config(Path("configs/experiment_baseline.yaml"))
    cfg.training.model_name = "gradient_boosted_portfolio_transformer"
    cfg.training.gradient_boosted_portfolio_transformer.d_model = 16
    cfg.training.gradient_boosted_portfolio_transformer.temporal_heads = 2
    cfg.training.gradient_boosted_portfolio_transformer.market_heads = 2
    cfg.training.gradient_boosted_portfolio_transformer.num_residual_stages = 2
    cfg.training.gradient_boosted_portfolio_transformer.stage_eta = [0.5, 0.25]
    model = build_model(config=cfg, lookback=5, num_features=7, num_symbols=11)

    assert isinstance(model, GradientBoostedPortfolioTransformer)
    assert model_hidden_dim_hint(cfg) == 16
    assert model.num_residual_stages == 2
