#!/usr/bin/env python3
"""Tests for pure score-rank loss."""

import torch

from stockagent.training.loss import masked_ic_loss, risk_aware_loss
from stockagent.training.trainer import _objective_metric_key, _normalize_risk_objective


def test_pure_rank_ignores_auxiliary_multitask_terms() -> None:
    torch.manual_seed(23)
    weights = torch.softmax(torch.randn(3, 7), dim=1)
    returns = torch.randn(3, 7) * 0.01
    mask = torch.ones(3, 7, dtype=torch.bool)
    score_logits = torch.randn(3, 7)
    rank_logits = torch.randn(3, 7)

    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        objective="pure_rank",
        aux_outputs={"score_logits": score_logits, "rank_logits": rank_logits},
        rank_ic_weight=1.0,
        direction_weight=999.0,
        volatility_regime_weight=999.0,
        concentration_weight=999.0,
    )
    expected = masked_ic_loss(rank_logits, returns, mask)

    assert torch.allclose(loss, expected)


def test_pure_rank_aliases_and_metric_key() -> None:
    assert _normalize_risk_objective("pure_rank") == "pure_rank"
    assert _normalize_risk_objective("rank_only") == "pure_rank"
    assert _normalize_risk_objective("score_rank") == "pure_rank"
    assert _objective_metric_key("pure_rank") == "rank_ic"
