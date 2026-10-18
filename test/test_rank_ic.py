"""Independent rank-correlation oracles for sparse portfolio diagnostics."""
import warnings

import numpy as np
import pytest
from scipy.stats import ConstantInputWarning, spearmanr
import torch

from stockagent.evaluation.metrics import compute_ic_series_torch


@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_sparse_tied_ic_matches_scipy_and_symbol_permutations(device):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    rng = np.random.default_rng(42)
    scores = rng.integers(-2, 3, (12, 97)).astype(np.float32)
    returns = rng.integers(-4, 5, scores.shape).astype(np.float32)
    scores[0] = 0  # A flat closing account has no cross-sectional signal.
    returns[1] = 1
    scores[2, 7] = np.nan
    returns[3, 8] = np.inf
    mask = rng.random(scores.shape) > .2
    mask[4] = False
    mask[5] = False
    mask[5, 0] = True
    expected = []
    for s, r, m in zip(scores, returns, mask):
        valid = m & np.isfinite(s) & np.isfinite(r)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConstantInputWarning)
            value = spearmanr(s[valid], r[valid]).statistic if valid.sum() >= 2 else np.nan
        # The reporting API uses zero for absent/constant signals.
        expected.append(float(value) if np.isfinite(value) else 0.0)
    def evaluate(order):
        return compute_ic_series_torch(
            torch.as_tensor(scores[:, order], device=device),
            torch.as_tensor(returns[:, order], device=device),
            torch.as_tensor(mask[:, order], device=device),
        ).cpu().numpy()
    np.testing.assert_allclose(evaluate(np.arange(97)), expected, atol=2e-7, rtol=0)
    np.testing.assert_allclose(evaluate(rng.permutation(97)), expected, atol=2e-7, rtol=0)


def test_all_flat_accounts_do_not_invent_negative_ic():
    scores = torch.zeros(149, 2757)
    returns = torch.randn_like(scores)
    result = compute_ic_series_torch(scores, returns, torch.ones_like(scores, dtype=torch.bool))
    assert torch.equal(result, torch.zeros(149))
