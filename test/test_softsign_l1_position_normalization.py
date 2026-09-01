import math

import pytest
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.models.normalization import (
    apply_portfolio_activation,
    dual_branch_softmax,
    masked_activation_l1_weights,
    masked_cash_entmax15_weights,
    masked_l1_projection_weights,
    masked_signed_action_weights,
    masked_softmax,
    masked_softsign_l1_weights,
    masked_tanh_l1_weights,
    normalize_portfolio_activation,
)
from stockagent.portfolio_contract import normalize_portfolio_mode, normalize_portfolio_output_mode


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


def test_signed_action_softmax_allocates_long_short_and_cash_actions() -> None:
    logits = torch.tensor([[3.0, 0.0, -3.0]], dtype=torch.float32)
    mask = torch.tensor([[True, True, True]])

    weights, parts = masked_signed_action_weights(
        logits,
        mask,
        transform="softmax",
        long_only=False,
        return_parts=True,
    )

    action_sum = parts["action_long_alloc"].sum(dim=1) + parts["action_short_alloc"].sum(dim=1) + parts["action_cash_alloc"]
    assert torch.allclose(action_sum, torch.ones_like(action_sum), atol=1e-6)
    assert torch.allclose(weights, parts["action_long_alloc"] - parts["action_short_alloc"], atol=1e-7)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert weights[0, 0] > 0.0
    assert weights[0, 2] < 0.0


def test_signed_action_entmax_can_return_sparse_actions() -> None:
    logits = torch.tensor([[8.0, 1.0, -8.0, 0.0]], dtype=torch.float32)
    mask = torch.ones_like(logits, dtype=torch.bool)

    weights, parts = masked_signed_action_weights(
        logits,
        mask,
        transform="entmax15",
        long_only=False,
        return_parts=True,
    )

    action_sum = parts["action_long_alloc"].sum(dim=1) + parts["action_short_alloc"].sum(dim=1) + parts["action_cash_alloc"]
    zero_actions = torch.cat([parts["action_long_alloc"], parts["action_short_alloc"], parts["action_cash_alloc"].view(1, 1)], dim=1)
    assert torch.allclose(action_sum, torch.ones_like(action_sum), atol=1e-5)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert int((zero_actions <= 1e-7).sum().item()) >= 1


def test_signed_action_entmax_short_mask_keeps_long_only_assets_out_of_short_book() -> None:
    logits = torch.tensor([[-8.0, -9.0]], dtype=torch.float32)
    mask = torch.ones_like(logits, dtype=torch.bool)
    short_mask = torch.zeros_like(mask)

    weights, parts = masked_signed_action_weights(
        logits,
        mask,
        transform="entmax15",
        long_only=False,
        short_mask=short_mask,
        return_parts=True,
    )

    torch.testing.assert_close(weights, torch.zeros_like(weights))
    torch.testing.assert_close(
        parts["action_short_alloc"],
        torch.zeros_like(parts["action_short_alloc"]),
    )
    torch.testing.assert_close(
        parts["action_cash_alloc"],
        torch.ones_like(parts["action_cash_alloc"]),
    )


def test_cash_entmax_zero_evidence_is_cash_and_candidate_count_is_invariant() -> None:
    for width in (2, 4_102):
        zeros = torch.zeros(1, width, dtype=torch.float32, requires_grad=True)
        mask = torch.ones_like(zeros, dtype=torch.bool)
        weights, parts = masked_cash_entmax15_weights(
            zeros,
            mask,
            short_mask=mask,
            radius=0.98,
            return_parts=True,
        )
        torch.testing.assert_close(weights, torch.zeros_like(weights))
        torch.testing.assert_close(
            parts["cash_entmax_cash_fraction"],
            torch.ones_like(parts["cash_entmax_cash_fraction"]),
        )

    small = torch.tensor([[1.0, -1.0]], dtype=torch.float32)
    wide = small.repeat(1, 2_051)
    small_weights, small_parts = masked_cash_entmax15_weights(
        small,
        torch.ones_like(small, dtype=torch.bool),
        radius=0.98,
        return_parts=True,
    )
    wide_weights, wide_parts = masked_cash_entmax15_weights(
        wide,
        torch.ones_like(wide, dtype=torch.bool),
        radius=0.98,
        return_parts=True,
    )
    torch.testing.assert_close(
        small_parts["cash_entmax_risk_fraction"],
        wide_parts["cash_entmax_risk_fraction"],
    )
    torch.testing.assert_close(
        small_weights.abs().sum(dim=1),
        wide_weights.abs().sum(dim=1),
        atol=1e-5,
        rtol=1e-5,
    )

    sparse_small = torch.tensor([[8.0, 0.0]], dtype=torch.float32)
    sparse_wide = torch.zeros(1, 4_102, dtype=torch.float32)
    sparse_wide[0, 0] = 8.0
    sparse_small_weights = masked_cash_entmax15_weights(
        sparse_small,
        torch.ones_like(sparse_small, dtype=torch.bool),
        radius=0.98,
    )
    sparse_wide_weights = masked_cash_entmax15_weights(
        sparse_wide,
        torch.ones_like(sparse_wide, dtype=torch.bool),
        radius=0.98,
    )
    torch.testing.assert_close(
        sparse_small_weights.abs().sum(dim=1),
        sparse_wide_weights.abs().sum(dim=1),
        atol=1e-5,
        rtol=1e-5,
    )


def test_signed_action_entmax_amplifies_uniform_large_universe_gradient_vs_softmax() -> None:
    num_symbols = 512
    mask = torch.ones(1, num_symbols, dtype=torch.bool)
    returns = torch.linspace(-1.0, 1.0, num_symbols, dtype=torch.float32).view(1, num_symbols)

    def _grad_abs_mean(transform: str) -> torch.Tensor:
        logits = torch.zeros(1, num_symbols, dtype=torch.float32, requires_grad=True)
        weights = masked_signed_action_weights(logits, mask, transform=transform, long_only=False)
        objective = (weights * returns).sum()
        objective.backward()
        return logits.grad.abs().mean()

    softmax_grad = _grad_abs_mean("softmax")
    entmax_grad = _grad_abs_mean("entmax15")

    assert entmax_grad > softmax_grad * 10.0


def test_signed_action_sparsemax_can_return_sparse_actions() -> None:
    logits = torch.tensor([[4.0, 1.0, -4.0, 0.0]], dtype=torch.float32)
    mask = torch.ones_like(logits, dtype=torch.bool)

    weights, parts = masked_signed_action_weights(
        logits,
        mask,
        transform="sparsemax",
        long_only=False,
        return_parts=True,
    )

    action_sum = parts["action_long_alloc"].sum(dim=1) + parts["action_short_alloc"].sum(dim=1) + parts["action_cash_alloc"]
    zero_actions = torch.cat([parts["action_long_alloc"], parts["action_short_alloc"], parts["action_cash_alloc"].view(1, 1)], dim=1)
    assert torch.allclose(action_sum, torch.ones_like(action_sum), atol=1e-6)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert int((zero_actions <= 1e-7).sum().item()) >= 1


def test_long_only_signed_softmax_keeps_gradient_when_every_stock_logit_is_negative() -> None:
    logits = torch.tensor(
        [[-8.0, -7.0, -6.0, -5.0]],
        dtype=torch.float32,
        requires_grad=True,
    )
    mask = torch.ones_like(logits, dtype=torch.bool)

    weights, parts = masked_signed_action_weights(
        logits,
        mask,
        transform="softmax",
        long_only=True,
        return_parts=True,
    )
    objective = (weights * torch.tensor([[0.1, -0.2, 0.3, -0.4]])).sum()
    objective.backward()

    assert torch.all(weights >= 0.0)
    assert torch.all(weights.sum(dim=1) < 1.0)
    assert torch.all(parts["action_cash_alloc"] > 0.0)
    assert logits.grad is not None
    assert torch.isfinite(logits.grad).all()
    assert torch.count_nonzero(logits.grad).item() == logits.numel()


def test_l1_projection_preserves_cash_inside_ball_and_sparsifies_outside_ball() -> None:
    inside = torch.tensor([[0.2, -0.3, 0.1]], dtype=torch.float32)
    outside = torch.tensor([[3.0, 1.0, -0.5, 0.1]], dtype=torch.float32)

    inside_projected = masked_l1_projection_weights(inside, torch.ones_like(inside, dtype=torch.bool), long_only=False)
    outside_projected = masked_l1_projection_weights(outside, torch.ones_like(outside, dtype=torch.bool), long_only=False)

    assert torch.allclose(inside_projected, inside, atol=1e-7, rtol=1e-6)
    assert torch.all(outside_projected.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert int((outside_projected.abs() <= 1e-7).sum().item()) >= 1


def test_l1_projection_enforces_radius_after_extreme_float32_cancellation() -> None:
    logits = torch.linspace(
        -1.0e15,
        1.0e15,
        1936,
        dtype=torch.float32,
    ).reshape(1, -1)
    projected = masked_l1_projection_weights(
        logits,
        torch.ones_like(logits, dtype=torch.bool),
        long_only=False,
    )

    assert torch.isfinite(projected).all()
    assert projected.abs().sum().item() <= 1.0 + 1.0e-6


@pytest.mark.parametrize("input_dtype", [torch.float16, torch.bfloat16])
def test_l1_projection_keeps_portfolio_weights_float32_under_amp_dtypes(
    input_dtype: torch.dtype,
) -> None:
    logits = torch.tensor(
        [[0.2, -0.3, 0.1]], dtype=input_dtype, requires_grad=True
    )
    weights = masked_l1_projection_weights(
        logits,
        torch.ones_like(logits, dtype=torch.bool),
        long_only=False,
    )

    assert weights.dtype == torch.float32
    weights.sum().backward()
    assert logits.grad is not None
    assert torch.isfinite(logits.grad).all()


def test_l1_projection_active_count_scale_preserves_cash_and_dense_gradient() -> None:
    logits = torch.tensor(
        [[0.5, -0.5, 0.5, -0.5], [0.5, -0.5, 9.0, -9.0]],
        dtype=torch.float64,
        requires_grad=True,
    )
    mask = torch.tensor(
        [[True, True, True, True], [True, True, False, False]],
    )

    weights = masked_l1_projection_weights(
        logits,
        mask,
        long_only=False,
        scale_by_active_count=True,
    )
    objective_coefficients = torch.tensor(
        [[0.2, -0.4, 0.1, 0.3], [0.6, -0.2, 0.0, 0.0]],
        dtype=torch.float64,
    )
    (weights * objective_coefficients).sum().backward()

    torch.testing.assert_close(
        weights,
        torch.tensor(
            [[0.125, -0.125, 0.125, -0.125], [0.25, -0.25, 0.0, 0.0]],
            dtype=torch.float64,
        ),
    )
    torch.testing.assert_close(
        weights.abs().sum(dim=1),
        torch.tensor([0.5, 0.5], dtype=torch.float64),
    )
    assert logits.grad is not None
    torch.testing.assert_close(
        logits.grad,
        torch.tensor(
            [[0.05, -0.10, 0.025, 0.075], [0.30, -0.10, 0.0, 0.0]],
            dtype=torch.float64,
        ),
    )


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


def test_identity_activation_keeps_finite_scores_and_uses_dtype_bounds_for_infinities() -> None:
    x = torch.tensor([[200.0, -50.0, float("nan"), float("inf"), float("-inf")]], dtype=torch.float32)

    actual = apply_portfolio_activation(x, "identity")

    finfo = torch.finfo(torch.float32)
    expected = torch.tensor([[200.0, -50.0, 0.0, finfo.max, finfo.min]], dtype=torch.float32)
    assert torch.allclose(actual, expected)


def test_portfolio_activation_aliases_normalize() -> None:
    assert normalize_portfolio_activation("arctan") == "atan"
    assert normalize_portfolio_activation("gd") == "gudermannian"
    assert normalize_portfolio_activation("inverse_sqrt") == "isru"
    assert normalize_portfolio_activation("none") == "identity"
    assert normalize_portfolio_activation("preserve_weights") == "pre_normalized"
    assert normalize_portfolio_activation(None) == "identity"


def test_portfolio_mode_contract_normalizes_shared_aliases() -> None:
    assert normalize_portfolio_mode("long") == "long_only"
    assert normalize_portfolio_mode("long-and-short") == "long_short"
    assert normalize_portfolio_output_mode("raw_scores") == "logits"
    assert normalize_portfolio_output_mode("explicit_cash_l1") == "cash_l1"
    assert normalize_portfolio_output_mode("differentiable_projection") == "projection_l1"
    assert normalize_portfolio_output_mode("signed_action_entmax15") == "signed_entmax15"


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


def test_tensor_backtest_pre_normalized_activation_preserves_underinvested_weights() -> None:
    target_weights = torch.tensor([[0.2, -0.3, 0.0]], dtype=torch.float32)
    asset_log_returns = torch.log1p(torch.tensor([[0.10, -0.20, 0.0]], dtype=torch.float32))
    tradable = torch.ones_like(target_weights, dtype=torch.bool)
    benchmark = torch.zeros((1,), dtype=torch.float32)

    result = run_backtest_torch(
        target_weights,
        asset_log_returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
    )

    expected_simple = torch.tensor(0.2 * 0.10 + (-0.3) * (-0.20), dtype=torch.float32)
    expected_log = torch.log1p(expected_simple)
    assert torch.allclose(result.weights_history, target_weights, atol=1e-7, rtol=1e-6)
    assert torch.allclose(result.strategy_returns, expected_log.view(1), atol=1e-7, rtol=1e-6)
