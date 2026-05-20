from __future__ import annotations

import numpy as np


def top_k_equal_weight(
    scores: np.ndarray,
    tradable_mask: np.ndarray,
    top_k: int,
) -> np.ndarray:
    """Select top-k stocks by score and assign equal weight.

    Args:
        scores: alpha scores per symbol [S]
        tradable_mask: boolean tradability mask [S]
        top_k: maximum number of positions

    Returns:
        weights: float32 weight vector [S] summing to ≤ 1
    """
    weights = np.zeros(len(scores), dtype=np.float32)
    valid_idx = np.flatnonzero(tradable_mask.astype(bool) & np.isfinite(scores))
    if valid_idx.size == 0:
        return weights
    k = min(top_k, valid_idx.size)
    chosen = valid_idx[np.argsort(scores[valid_idx])[-k:]]
    weights[chosen] = 1.0 / k
    return weights
