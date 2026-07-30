from __future__ import annotations

import torch

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
    exposure = model.forward_from_panel(features, end_indices, mask)

    assert exposure.shape == (3,)
    assert torch.all(exposure.abs() <= 0.75 + 1e-6)
    assert exposure[-1].item() == 0.0
    exposure[:2].sum().backward()
    assert features.grad is not None
    assert torch.isfinite(features.grad).all()

