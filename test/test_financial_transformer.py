#!/usr/bin/env python3
"""Smoke tests for the Financial Transformer portfolio model."""

from dataclasses import fields
from pathlib import Path

import torch
import yaml

from stockagent.config import TransformerBasePortfolioModelConfig, load_config
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.financial_transformer import FinancialTransformerModel


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _make_model(**overrides) -> FinancialTransformerModel:
    torch.manual_seed(29)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(29)
    params = {
        "lookback": 5,
        "num_features": 10,
        "num_symbols": 7,
        "d_model": 24,
        "attention_mode": "market_token",
        "use_flash_attention": True,
        "use_time_pos": True,
        "use_symbol_pos": True,
        "input_dropout": 0.0,
        "sdpa_batch_limit": 4096,
        "norm_type": "rmsnorm",
        "ffn_type": "swiglu",
        "qk_norm": True,
        "rope_temporal": True,
        "temporal_layers": 1,
        "temporal_heads": 2,
        "temporal_ffn_mult": 1,
        "temporal_pooling": "attention",
        "temporal_query_mode": "full_then_last",
        "cross_heads": 2,
        "cross_ffn_mult": 1,
        "latent_layers": 1,
        "num_latent_factors": 4,
        "num_market_tokens": 2,
        "market_layers": 1,
        "head_hidden_dim": 24,
        "head_layers": 1,
        "dropout": 0.0,
        "portfolio_mode": "long_short",
        "portfolio_output_mode": "activation_l1",
        "return_aux": True,
        "return_aux_details": True,
        "runtime_shape_check": True,
        "allow_dynamic_symbols": True,
        "candle_dropout": 0.0,
    }
    params.update(overrides)
    return FinancialTransformerModel(**params).to(_device())


def test_financial_transformer_forward_aux_and_mask() -> None:
    device = _device()
    model = _make_model().eval()
    x = torch.randn(3, 5, 7, 10, device=device)
    mask = torch.ones(3, 7, dtype=torch.bool, device=device)
    mask[1, 5:] = False

    with torch.no_grad():
        weights, scores, aux = model(x, mask, return_aux=True)

    assert weights.shape == (3, 7)
    assert scores.shape == (3, 7)
    assert weights[1, 5:].abs().max().item() == 0.0
    assert aux["candle_tokens"].shape == (3, 5, 7, 1, 24)
    assert aux["candle_token_weights"].shape == (3, 5, 7, 1)
    assert aux["candle_embedding"].shape == (3, 5, 7, 24)
    assert "market_tokens" in aux
    assert "stock_market_gate" in aux


def test_financial_transformer_last_pooling_panel_slab_forward() -> None:
    device = _device()
    model = _make_model(
        temporal_pooling="last",
        temporal_query_mode="last_only",
        return_aux=False,
        return_aux_details=False,
    ).eval()
    feature_slab = torch.randn(8, 7, 10, device=device)
    mask = torch.ones(4, 7, dtype=torch.bool, device=device)
    mask[2, 6] = False

    with torch.no_grad():
        weights = model.forward_from_panel_slab(feature_slab, mask)

    assert weights.shape == (4, 7)
    assert weights[2, 6].item() == 0.0


def test_checkpointed_mean_pooling_compiles_fullgraph_without_capture_side_effect() -> None:
    device = _device()
    model = _make_model(
        checkpoint_blocks=True,
        temporal_pooling="mean",
        temporal_query_mode="full_then_last",
        return_aux=False,
        return_aux_details=False,
    ).train()
    compiled = torch.compile(model, backend="eager", fullgraph=True)
    x = torch.randn(2, 5, 7, 10, device=device, requires_grad=True)
    mask = torch.ones(2, 7, dtype=torch.bool, device=device)

    weights = compiled(x, mask)
    weights.square().sum().backward()

    assert x.grad is not None
    assert torch.isfinite(x.grad).all()


def test_candle_encoder_directly_replaces_feature_projection() -> None:
    device = _device()
    model = _make_model().eval()
    x = torch.randn(2, 5, 7, 10, device=device)

    with torch.no_grad():
        weights, _scores, aux = model(x, return_aux=True)

    assert weights.shape == (2, 7)
    assert aux["candle_tokens"].shape == (2, 5, 7, 1, 24)
    assert aux["candle_token_weights"].shape == (2, 5, 7, 1)
    assert torch.equal(aux["candle_token_weights"], torch.ones_like(aux["candle_token_weights"]))
    assert not hasattr(model, "feature_proj")
    assert model.candle_encoder.joint_input_dim == 10
    assert model.candle_encoder.joint_projection.proj.in_features == 10
    assert model.candle_encoder.joint_projection.proj.out_features == 48


def test_financial_transformer_panel_paths_match_materialized_windows() -> None:
    device = _device()
    model = _make_model(return_aux=False, return_aux_details=False).eval()
    feature_slab = torch.randn(8, 7, 10, device=device)
    date_indices = torch.arange(4, 8, dtype=torch.long, device=device)
    windows = feature_slab.unfold(0, 5, 1).permute(0, 3, 1, 2).contiguous()
    mask = torch.ones(4, 7, dtype=torch.bool, device=device)
    mask[1, 6] = False

    with torch.no_grad():
        materialized = model(windows, mask)
        panel = model.forward_from_panel(feature_slab, date_indices, mask)
        slab = model.forward_from_panel_slab(feature_slab, mask)

    torch.testing.assert_close(panel, materialized, rtol=1e-5, atol=1e-6)
    torch.testing.assert_close(slab, materialized, rtol=1e-5, atol=1e-6)


def test_factory_builds_financial_transformer_model() -> None:
    cfg = load_config(Path("configs/experiment_baseline.yaml"))
    cfg.training.model_name = "financial_transformer"
    cfg.training.financial_transformer.d_model = 24
    cfg.training.financial_transformer.temporal_heads = 2
    cfg.training.financial_transformer.cross_heads = 2
    cfg.training.financial_transformer.market_layers = 1
    cfg.training.financial_transformer.num_market_tokens = 2
    cfg.training.financial_transformer.attention_mode = "market_token"
    cfg.training.financial_transformer.return_aux = False
    feature_names = [
        "open",
        "high",
        "low",
        "close",
        "return_1d",
        "volume",
        "twpub_financial_eps",
        "twpub_financial_debt_ratio",
        "custom_signal",
    ]

    model = build_model(
        config=cfg,
        lookback=5,
        num_features=len(feature_names),
        num_symbols=7,
        feature_names=feature_names,
    )

    assert isinstance(model, FinancialTransformerModel)
    assert model_hidden_dim_hint(cfg) == 24
    assert model.candle_encoder.d_model == 24


def test_candle_encoder_uses_every_feature_jointly() -> None:
    device = _device()
    model = _make_model().eval()
    base = torch.zeros(1, 5, 7, 10, device=device)

    with torch.no_grad():
        _, _, base_aux = model(base, return_aux=True)
        for feature_idx in range(base.size(-1)):
            changed = base.clone()
            changed[..., feature_idx] = 1.0
            _, _, changed_aux = model(changed, return_aux=True)
            assert not torch.equal(
                base_aux["candle_tokens"],
                changed_aux["candle_tokens"],
            )


def test_candle_encoder_jointly_embeds_categorical_features() -> None:
    device = _device()
    model = _make_model(
        categorical_feature_indices=[2, 7],
        categorical_embedding_dim=4,
    ).eval()
    base = torch.zeros(1, 5, 7, 10, device=device)
    changed = base.clone()
    changed[..., 2] = 3.0

    with torch.no_grad():
        _, _, base_aux = model(base, return_aux=True)
        _, _, changed_aux = model(changed, return_aux=True)

    assert model.candle_encoder.joint_input_dim == 16
    assert not torch.equal(base_aux["candle_embedding"], changed_aux["candle_embedding"])


def test_all_resolved_experiment_configs_define_matching_financial_transformer() -> None:
    config_paths = [Path("configs/experiment_baseline.yaml")]
    config_paths.extend(sorted(Path("configs/markets").glob("*.yaml")))

    for path in config_paths:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if "financial_transformer" in raw.get("training", {}):
            assert "financial_transformer" in raw["training"], path
        else:
            # Thin market variants may still override unrelated training
            # scalars while inheriting the complete model contract.
            assert raw.get("base_config"), path

        config = load_config(path)
        transformer_base = config.training.transformer_base_portfolio
        financial = config.training.financial_transformer
        for config_field in fields(TransformerBasePortfolioModelConfig):
            assert getattr(financial, config_field.name) == getattr(
                transformer_base,
                config_field.name,
            ), (path, config_field.name)
