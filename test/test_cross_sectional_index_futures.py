from __future__ import annotations

import pytest
import torch

from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_ACTION_COUNT,
    TAIFEX_INDEX_FUTURES_CONTEXT_FEATURE_DIM,
)
from stockagent.models.cross_sectional_index_futures import (
    CrossSectionalIndexFuturesModel,
)


def test_cross_sectional_head_emits_one_bounded_exposure() -> None:
    model = CrossSectionalIndexFuturesModel(
        lookback=3,
        num_features=4,
        num_symbols=5,
        d_model=8,
        attention_mode="market_token",
        use_latent_factors=False,
        use_market_tokens=True,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        cross_layers=1,
        cross_heads=2,
        cross_ffn_mult=2,
        joint_layers=1,
        joint_heads=2,
        joint_ffn_mult=2,
        latent_layers=1,
        num_latent_factors=2,
        num_market_tokens=2,
        market_layers=1,
        head_hidden_dim=8,
        head_layers=1,
        dropout=0.0,
        max_abs_exposure=0.75,
        use_flash_attention=False,
    )
    features = torch.randn(7, 5, 4, requires_grad=True)
    end_indices = torch.tensor([2, 3, 4])
    mask = torch.ones(3, 5, dtype=torch.bool)
    mask[-1] = False
    pseudo_weights = model.forward_from_panel(features, end_indices, mask)
    exposure = pseudo_weights.sum(dim=-1)

    assert pseudo_weights.shape == (3, 5)
    assert exposure.shape == (3,)
    assert torch.all(exposure.abs() <= 0.75 + 1e-6)
    assert torch.all(pseudo_weights[-1] == 0.0)
    assert exposure[-1].item() == 0.0
    exposure[:2].sum().backward()
    assert features.grad is not None
    assert torch.isfinite(features.grad).all()


def test_joint_stock_futures_path_emits_18_direct_actions() -> None:
    model = CrossSectionalIndexFuturesModel(
        lookback=3,
        num_features=4,
        num_symbols=5,
        d_model=8,
        attention_mode="market_token",
        use_latent_factors=False,
        use_market_tokens=True,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        cross_layers=1,
        cross_heads=2,
        cross_ffn_mult=2,
        joint_layers=1,
        joint_heads=2,
        joint_ffn_mult=2,
        latent_layers=1,
        num_latent_factors=2,
        num_market_tokens=2,
        market_layers=1,
        head_hidden_dim=8,
        head_layers=1,
        dropout=0.0,
        max_abs_exposure=0.75,
        use_flash_attention=False,
    )
    stocks = torch.randn(7, 5, 4, requires_grad=True)
    context_width = 25
    futures = torch.randn(
        3,
        context_width,
        TAIFEX_INDEX_FUTURES_CONTEXT_FEATURE_DIM,
        requires_grad=True,
    )
    futures_mask = torch.ones(
        3, context_width, dtype=torch.bool
    )
    futures_mask[:, -2:] = False
    actions = model.forward_from_panel(
        stocks,
        torch.tensor([2, 3, 4]),
        torch.ones(3, 5, dtype=torch.bool),
        portfolio_context={
            "candidate_features": futures,
            "candidate_mask": futures_mask,
        },
    )

    assert actions.shape == (3, TAIFEX_INDEX_FUTURES_ACTION_COUNT)
    assert torch.all(actions[:, -2:] == 0.0)
    assert torch.all(actions.abs().sum(dim=-1) <= 0.75 + 1e-6)
    actions.square().sum().backward()
    assert stocks.grad is not None and torch.isfinite(stocks.grad).all()
    assert futures.grad is not None and torch.isfinite(futures.grad).all()


def test_directional_allocation_has_cash_gate_and_no_self_cancellation() -> None:
    model = CrossSectionalIndexFuturesModel(
        lookback=3,
        num_features=4,
        num_symbols=5,
        d_model=8,
        attention_mode="market_token",
        use_latent_factors=False,
        use_market_tokens=True,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        cross_layers=1,
        cross_heads=2,
        cross_ffn_mult=2,
        joint_layers=1,
        joint_heads=2,
        joint_ffn_mult=2,
        latent_layers=1,
        num_latent_factors=2,
        num_market_tokens=2,
        market_layers=1,
        head_hidden_dim=8,
        head_layers=1,
        dropout=0.0,
        max_abs_exposure=0.75,
        futures_action_mode="directional_allocation",
        use_flash_attention=False,
    )
    stocks = torch.randn(7, 5, 4, requires_grad=True)
    futures = torch.randn(
        3,
        25,
        TAIFEX_INDEX_FUTURES_CONTEXT_FEATURE_DIM,
        requires_grad=True,
    )
    futures_mask = torch.ones(3, 25, dtype=torch.bool)
    futures_mask[:, -2:] = False
    actions, _scores, aux = model.forward_from_panel(
        stocks,
        torch.tensor([2, 3, 4]),
        torch.ones(3, 5, dtype=torch.bool),
        portfolio_context={
            "candidate_features": futures,
            "candidate_mask": futures_mask,
        },
        return_aux=True,
    )

    gross = actions.abs().sum(dim=-1)
    net = actions.sum(dim=-1).abs()
    assert torch.allclose(gross, net, atol=1e-6)
    assert torch.all(gross <= 0.75 + 1e-6)
    assert torch.all(actions[:, -2:] == 0.0)
    assert torch.allclose(
        aux["futures_action_allocation"].sum(dim=-1),
        torch.ones(3),
        atol=1e-6,
    )
    assert torch.allclose(gross, aux["futures_exposure"].abs(), atol=1e-6)
    actions.square().sum().backward()
    assert stocks.grad is not None and torch.isfinite(stocks.grad).all()
    assert futures.grad is not None and torch.isfinite(futures.grad).all()


def test_softsign_directional_policy_retains_extreme_finite_gradients() -> None:
    model = CrossSectionalIndexFuturesModel(
        lookback=3,
        num_features=4,
        num_symbols=5,
        d_model=8,
        attention_mode="market_token",
        use_latent_factors=False,
        use_market_tokens=True,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        cross_layers=1,
        cross_heads=2,
        cross_ffn_mult=2,
        joint_layers=1,
        joint_heads=2,
        joint_ffn_mult=2,
        latent_layers=1,
        num_latent_factors=2,
        num_market_tokens=2,
        market_layers=1,
        head_hidden_dim=8,
        head_layers=1,
        dropout=0.0,
        max_abs_exposure=1.0,
        futures_action_mode="directional_allocation",
        futures_exposure_activation="softsign",
        futures_allocation_logit_scale=2.0,
        futures_allocation_temperature=2.0,
        use_flash_attention=False,
    )

    exposure_logit = torch.tensor(20.0, requires_grad=True)
    exposure = model._activate_futures_exposure(exposure_logit)
    exposure.backward()
    assert 0.0 < exposure.item() < 1.0
    assert exposure_logit.grad is not None
    assert exposure_logit.grad.item() > 0.0

    scores = torch.linspace(-20.0, 20.0, 18, requires_grad=True).unsqueeze(0)
    scores.retain_grad()
    mask = torch.ones_like(scores, dtype=torch.bool)
    logits = model._allocation_logits(scores, mask)
    probabilities = torch.softmax(logits, dim=-1)
    objective = torch.sum(probabilities * torch.linspace(-1.0, 1.0, 18))
    objective.backward()

    assert torch.max(torch.abs(logits)).item() < 2.0
    assert probabilities.max().item() < 1.0
    assert scores.grad is not None
    assert torch.count_nonzero(scores.grad) == scores.numel()


def test_required_joint_context_has_no_dead_pooling_parameters() -> None:
    model = CrossSectionalIndexFuturesModel(
        lookback=3,
        num_features=4,
        num_symbols=5,
        d_model=8,
        attention_mode="market_token",
        use_latent_factors=False,
        use_market_tokens=True,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        cross_layers=1,
        cross_heads=2,
        cross_ffn_mult=2,
        joint_layers=1,
        joint_heads=2,
        joint_ffn_mult=2,
        latent_layers=1,
        num_latent_factors=2,
        num_market_tokens=2,
        market_layers=1,
        head_hidden_dim=8,
        head_layers=1,
        dropout=0.0,
        futures_action_mode="directional_allocation",
        futures_require_joint_context=True,
        use_flash_attention=False,
    )

    assert model.futures_pool_score is None
    assert not any(
        name.startswith("futures_pool_score")
        for name, _parameter in model.named_parameters()
    )
    with pytest.raises(ValueError, match="requires causal futures"):
        model.forward_from_panel(
            torch.randn(7, 5, 4),
            torch.tensor([2, 3, 4]),
            torch.ones(3, 5, dtype=torch.bool),
        )
