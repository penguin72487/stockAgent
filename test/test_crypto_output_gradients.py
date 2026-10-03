"""Output geometry reaches the canonical crypto ledger, not a proxy loss.

These are tiny synthetic gradient checks, not training or return claims.
Historical learned_cash must retain its exact sign-only singleton contract;
the existing score_entmax_cash_v2 is a separate opt-in action ABI.
"""
from __future__ import annotations

import pytest
import torch

from stockagent.models.normalization import (
    masked_cash_entmax15_weights,
    masked_learned_cash_weights,
)
from stockagent.training.loss import risk_aware_loss


def _v2(scores: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    return masked_cash_entmax15_weights(
        scores,
        mask,
        preserve_fp32_output=True,
        preserve_zero_score_gradient=True,
    )


def _loss(
    weights: torch.Tensor,
    mask: torch.Tensor,
    *,
    simple_return: float = 0.03,
    fee: float = 0.00055,
) -> torch.Tensor:
    labels = torch.log1p(torch.full_like(weights, simple_return))
    return risk_aware_loss(
        weights,
        labels,
        mask,
        benchmark_returns=weights.new_zeros(weights.size(0)),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        long_only=False,
        buy_fee_rate=fee,
        sell_fee_rate=fee,
        gross_leverage=1.0,
        portfolio_activation="pre_normalized",
        execution_mode="crypto_perpetual",
        objective="log_utility",
        overnight_log_returns=labels,
        log_utility_periods_per_year=365.0,
        gamma_turnover=0.0,
        concentration_weight=0.0,
        rank_ic_weight=0.0,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        # Match the active v6/v7 allocator. Its flat-previous-account branch
        # bypasses the no-trade band, so first-entry derivatives must survive.
        crypto_stateful_proximal_allocator=True,
    )


@pytest.mark.parametrize("score", [-1.0, -0.1, 0.1, 1.0])
def test_legacy_learned_cash_singleton_cannot_learn_direction(score: float) -> None:
    scores = torch.tensor([[score]], requires_grad=True)
    cash = torch.zeros(1, requires_grad=True)
    mask = torch.ones_like(scores, dtype=torch.bool)
    weights, _, _ = masked_learned_cash_weights(scores, cash, mask)
    score_grad, cash_grad = torch.autograd.grad(_loss(weights, mask), (scores, cash))
    assert weights.item() == pytest.approx(0.5 if score > 0 else -0.5)
    assert score_grad.item() == pytest.approx(0.0, abs=1e-6)
    assert torch.isfinite(cash_grad).all()
    assert abs(cash_grad.item()) > 0.1


def test_legacy_learned_cash_exact_zero_has_no_direction_or_cash_gradient() -> None:
    scores = torch.zeros((1, 1), requires_grad=True)
    cash = torch.zeros(1, requires_grad=True)
    mask = torch.ones_like(scores, dtype=torch.bool)
    weights, residual_cash, _ = masked_learned_cash_weights(scores, cash, mask)
    score_grad, cash_grad = torch.autograd.grad(_loss(weights, mask), (scores, cash))
    assert weights.item() == 0.0
    assert residual_cash.item() == 1.0
    assert score_grad.item() == 0.0
    assert cash_grad.item() == 0.0


@pytest.mark.parametrize("score", [-1.0, -0.1, 0.0, 0.1, 1.0])
@pytest.mark.parametrize("simple_return", [-0.03, 0.03])
def test_v2_singleton_crypto_loss_gradient_matches_finite_difference(
    score: float, simple_return: float,
) -> None:
    scores = torch.tensor([[score]], requires_grad=True)
    mask = torch.ones_like(scores, dtype=torch.bool)
    loss = _loss(_v2(scores, mask), mask, simple_return=simple_return)
    gradient = torch.autograd.grad(loss, scores)[0].item()
    step = 1e-3
    finite_difference = (
        _loss(_v2(scores.detach() + step, mask), mask, simple_return=simple_return)
        - _loss(_v2(scores.detach() - step, mask), mask, simple_return=simple_return)
    ).item() / (2.0 * step)
    assert torch.isfinite(loss)
    assert abs(gradient) > 1.0
    assert gradient * simple_return < 0.0
    assert gradient == pytest.approx(finite_difference, rel=4e-3, abs=2e-3)


@pytest.mark.parametrize("width", [1, 397])
def test_v2_exact_cash_can_learn_either_direction_for_any_candidate_count(width: int) -> None:
    score = torch.tensor(0.0, requires_grad=True)
    scores = score.expand(1, width)
    mask = torch.ones_like(scores, dtype=torch.bool)
    weights = _v2(scores, mask)
    assert torch.equal(weights, torch.zeros_like(weights))
    gradient = torch.autograd.grad(_loss(weights, mask, fee=0.0), score)[0]
    # Identical candidates split the same signed portfolio; candidate count
    # cannot erase the derivative of their common conviction at zero.
    assert gradient.item() == pytest.approx(-365.0 * 0.03, rel=2e-5)


def test_v2_masked_padding_has_zero_actions_and_gradients() -> None:
    scores = torch.full((1, 397), 999.0, requires_grad=True)
    with torch.no_grad():
        scores[0, 17] = 0.1
    mask = torch.zeros_like(scores, dtype=torch.bool)
    mask[0, 17] = True
    weights = _v2(scores, mask)
    gradient = torch.autograd.grad(_loss(weights, mask), scores)[0]
    assert weights[0, 17].item() == pytest.approx(0.1 / 1.1)
    assert torch.count_nonzero(weights[~mask]).item() == 0
    assert torch.count_nonzero(gradient[~mask]).item() == 0
    assert gradient[0, 17].item() < 0.0


@pytest.mark.parametrize("width", [1, 397])
@pytest.mark.parametrize("score", [-10.0, -0.1, 0.0, 0.1, 10.0])
def test_v2_replication_preserves_signed_gross_and_residual_cash(width: int, score: float) -> None:
    scores = torch.full((1, width), score)
    mask = torch.ones_like(scores, dtype=torch.bool)
    weights = _v2(scores, mask)
    gross = weights.abs().sum().item()
    signed = weights.sum().item()
    assert torch.isfinite(weights).all()
    assert signed == pytest.approx(score / (1.0 + abs(score)), rel=2e-6, abs=1e-7)
    assert gross == pytest.approx(abs(score) / (1.0 + abs(score)), rel=2e-6, abs=1e-7)
    assert 0.0 <= gross < 1.0
    assert 0.0 < 1.0 - gross <= 1.0


def test_v2_all_masked_market_remains_cash_with_zero_gradient() -> None:
    scores = torch.tensor([[0.1, -0.3]], requires_grad=True)
    mask = torch.zeros_like(scores, dtype=torch.bool)
    weights = _v2(scores, mask)
    gradient = torch.autograd.grad(_loss(weights, mask), scores)[0]
    assert torch.equal(weights, torch.zeros_like(weights))
    assert torch.equal(gradient, torch.zeros_like(gradient))
