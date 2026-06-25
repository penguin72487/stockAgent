import math

import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.models.normalization import (
    apply_portfolio_activation,
    dual_branch_softmax,
    masked_activation_l1_weights,
    masked_softmax,
    masked_softsign_l1_weights,
    masked_tanh_l1_weights,
    normalize_portfolio_activation,
)


def test_masked_softsign_l1_long_short_weights_use_softsign_direction_and_l1_norm() -> None:
    logits = torch.tensor(
        [
            [2.0, -1.0, 0.5, -0.25],
            [-0.75, -0.50, 0.0, 1.5],
        ],
        dtype=torch.float32,
    )
    mask = torch.tensor(
        [
            [True, True, False, True],
            [True, False, False, True],
        ]
    )

    weights = masked_softsign_l1_weights(logits, mask, long_only=False)
    expected_raw = (logits / (1.0 + logits.abs())).masked_fill(~mask, 0.0)
    expected = expected_raw / expected_raw.abs().sum(dim=1, keepdim=True).clamp_min(1e-8)

    assert torch.allclose(weights, expected, atol=1e-7, rtol=1e-6)
    assert torch.allclose(weights.abs().sum(dim=1), torch.ones(2), atol=1e-6)
    assert bool((weights > 0.0).any().item())
    assert bool((weights < 0.0).any().item())


def test_legacy_portfolio_normalizers_use_default_identity_l1() -> None:
    logits = torch.tensor([[1.0, -2.0, 0.25]], dtype=torch.float32)
    mask = torch.tensor([[True, True, True]])

    long_short = dual_branch_softmax(logits, mask)
    expected_long_short = masked_activation_l1_weights(logits, mask, long_only=False, activation="identity")
    assert torch.allclose(long_short, expected_long_short, atol=1e-7, rtol=1e-6)
    assert torch.allclose(long_short.abs().sum(dim=1), torch.ones(1), atol=1e-6)

    long_only = masked_softmax(logits, mask)
    expected_long_only = masked_activation_l1_weights(logits, mask, long_only=True, activation="identity")
    assert torch.allclose(long_only, expected_long_only, atol=1e-7, rtol=1e-6)
    assert torch.all(long_only >= 0.0)
    assert torch.allclose(long_only.abs().sum(dim=1), torch.ones(1), atol=1e-6)


def test_masked_tanh_l1_name_remains_explicit_tanh_l1_helper() -> None:
    logits = torch.tensor([[1.0, -2.0, 0.25]], dtype=torch.float32)
    mask = torch.tensor([[True, True, True]])

    assert torch.allclose(
        masked_tanh_l1_weights(logits, mask, long_only=False),
        masked_activation_l1_weights(logits, mask, long_only=False, activation="tanh"),
        atol=1e-7,
        rtol=1e-6,
    )


def test_softsign_l1_empty_rows_are_zero() -> None:
    logits = torch.tensor([[1.0, -2.0, 0.25]], dtype=torch.float32)
    mask = torch.zeros_like(logits, dtype=torch.bool)

    weights = masked_softsign_l1_weights(logits, mask, long_only=False)

    assert torch.allclose(weights, torch.zeros_like(weights))


def test_tensor_backtest_normalizes_targets_with_default_identity_l1() -> None:
    target_scores = torch.tensor([[2.0, -1.0, 0.5]], dtype=torch.float32)
    returns = torch.zeros_like(target_scores)
    tradable = torch.ones_like(target_scores, dtype=torch.bool)
    benchmark = torch.zeros((1,), dtype=torch.float32)

    result = run_backtest_torch(
        target_scores,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        min_trade_weight=0.0,
    )

    expected = masked_activation_l1_weights(target_scores, tradable, long_only=False, activation="identity")
    assert torch.allclose(result.weights_history, expected, atol=1e-7, rtol=1e-6)


def test_portfolio_activation_formulas_match_supported_switches() -> None:
    x = torch.tensor([[-1.5, -0.5, 0.0, 0.5, 1.5]], dtype=torch.float32)
    expected = {
        "identity": x,
        "tanh": torch.tanh(x),
        "softsign": x / (1.0 + x.abs()),
        "isru": x / torch.sqrt(1.0 + x.square()),
        "erf": torch.erf(x * (math.sqrt(math.pi) / 2.0)),
        "atan": (2.0 / math.pi) * torch.atan(x * (math.pi / 2.0)),
        "gd": (2.0 / math.pi) * torch.atan(torch.sinh(x * (math.pi / 2.0))),
    }

    for activation, expected_values in expected.items():
        actual = apply_portfolio_activation(x, activation)
        assert torch.allclose(actual, expected_values, atol=1e-7, rtol=1e-6)
        if activation != "identity":
            assert bool((actual.abs() <= 1.0).all().item())


def test_identity_activation_keeps_large_finite_scores_unclipped() -> None:
    x = torch.tensor([[200.0, -50.0, float("nan"), float("inf"), float("-inf")]], dtype=torch.float32)

    actual = apply_portfolio_activation(x, "identity")

    expected = torch.tensor([[200.0, -50.0, 0.0, 0.0, 0.0]], dtype=torch.float32)
    assert torch.allclose(actual, expected)


def test_portfolio_activation_aliases_normalize() -> None:
    assert normalize_portfolio_activation("arctan") == "atan"
    assert normalize_portfolio_activation("gd") == "gudermannian"
    assert normalize_portfolio_activation("inverse_sqrt") == "isru"
    assert normalize_portfolio_activation("none") == "identity"
    assert normalize_portfolio_activation(None) == "identity"


def test_tensor_backtest_portfolio_activation_switch_changes_target_normalizer() -> None:
    target_scores = torch.tensor([[2.0, -1.0, 0.5]], dtype=torch.float32)
    returns = torch.zeros_like(target_scores)
    tradable = torch.ones_like(target_scores, dtype=torch.bool)
    benchmark = torch.zeros((1,), dtype=torch.float32)

    tanh_result = run_backtest_torch(
        target_scores,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="tanh",
    )
    isru_result = run_backtest_torch(
        target_scores,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="isru",
    )

    expected_tanh = masked_activation_l1_weights(target_scores, tradable, long_only=False, activation="tanh")
    expected_isru = masked_activation_l1_weights(target_scores, tradable, long_only=False, activation="isru")
    assert torch.allclose(tanh_result.weights_history, expected_tanh, atol=1e-7, rtol=1e-6)
    assert torch.allclose(isru_result.weights_history, expected_isru, atol=1e-7, rtol=1e-6)
    assert not torch.allclose(tanh_result.weights_history, isru_result.weights_history)


def test_tensor_backtest_identity_activation_is_raw_l1_postprocess() -> None:
    target_scores = torch.tensor([[200.0, -1.0, 0.5]], dtype=torch.float32)
    returns = torch.zeros_like(target_scores)
    tradable = torch.ones_like(target_scores, dtype=torch.bool)
    benchmark = torch.zeros((1,), dtype=torch.float32)

    result = run_backtest_torch(
        target_scores,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="identity",
    )

    expected = masked_activation_l1_weights(target_scores, tradable, long_only=False, activation="identity")
    assert torch.allclose(result.weights_history, expected, atol=1e-7, rtol=1e-6)
