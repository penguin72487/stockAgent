import torch

from stockagent.models.normalization import dual_branch_softmax, masked_softmax, masked_tanh_l1_weights


def test_masked_tanh_l1_long_short_weights_use_tanh_direction_and_l1_norm() -> None:
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

    weights = masked_tanh_l1_weights(logits, mask, long_only=False)
    expected_raw = torch.tanh(logits).masked_fill(~mask, 0.0)
    expected = expected_raw / expected_raw.abs().sum(dim=1, keepdim=True).clamp_min(1e-8)

    assert torch.allclose(weights, expected, atol=1e-7, rtol=1e-6)
    assert torch.allclose(weights.abs().sum(dim=1), torch.ones(2), atol=1e-6)
    assert bool((weights > 0.0).any().item())
    assert bool((weights < 0.0).any().item())


def test_legacy_portfolio_normalizers_now_use_tanh_l1() -> None:
    logits = torch.tensor([[1.0, -2.0, 0.25]], dtype=torch.float32)
    mask = torch.tensor([[True, True, True]])

    long_short = dual_branch_softmax(logits, mask)
    expected_long_short = masked_tanh_l1_weights(logits, mask, long_only=False)
    assert torch.allclose(long_short, expected_long_short, atol=1e-7, rtol=1e-6)
    assert torch.allclose(long_short.abs().sum(dim=1), torch.ones(1), atol=1e-6)

    long_only = masked_softmax(logits, mask)
    expected_long_only = masked_tanh_l1_weights(logits, mask, long_only=True)
    assert torch.allclose(long_only, expected_long_only, atol=1e-7, rtol=1e-6)
    assert torch.all(long_only >= 0.0)
    assert torch.allclose(long_only.abs().sum(dim=1), torch.ones(1), atol=1e-6)


def test_tanh_l1_empty_rows_are_zero() -> None:
    logits = torch.tensor([[1.0, -2.0, 0.25]], dtype=torch.float32)
    mask = torch.zeros_like(logits, dtype=torch.bool)

    weights = masked_tanh_l1_weights(logits, mask, long_only=False)

    assert torch.allclose(weights, torch.zeros_like(weights))
