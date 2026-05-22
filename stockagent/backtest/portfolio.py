from __future__ import annotations

import numpy as np


def top_k_equal_weight(
    scores: np.ndarray,
    tradable_mask: np.ndarray,
    top_k: int,
    allow_short: bool = False,
) -> np.ndarray:
    """Assign equal gross-weight across all valid tradable stocks.

    Args:
        scores: alpha scores per symbol [S]
        tradable_mask: boolean tradability mask [S]
        top_k: retained for API compatibility and ignored
        allow_short: if True, also allocate equal-weight short positions

    Returns:
        weights: float32 weight vector [S] with gross exposure ≤ 1
    """
    weights = np.zeros(len(scores), dtype=np.float32)
    valid_idx = np.flatnonzero(tradable_mask.astype(bool) & np.isfinite(scores))
    if valid_idx.size == 0:
        return weights
    half = 0.5 if allow_short else 1.0
    if allow_short:
        long_idx = valid_idx[scores[valid_idx] >= 0]
        short_idx = valid_idx[scores[valid_idx] < 0]
        if long_idx.size > 0:
            weights[long_idx] = half / float(long_idx.size)
        if short_idx.size > 0:
            weights[short_idx] = -half / float(short_idx.size)
        if long_idx.size == 0 and short_idx.size == 0:
            weights[valid_idx] = 0.0
    else:
        weights[valid_idx] = half / float(valid_idx.size)
    return weights
