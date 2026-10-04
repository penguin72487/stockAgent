"""The shared causal, whole-contract entry-capacity calculation."""
from __future__ import annotations

import numpy as np


def whole_contract_trade_capacity(previous_volume, participation: float) -> np.ndarray:
    """Keep the loader's exact FP64 calculation before execution-tape packing.

    Missing/nonfinite preceding-session volume permits no new order. Today's
    volume is not an entry input. Existing positions do not depend on this cap.
    """
    participation = float(participation)
    if not np.isfinite(participation) or not 0.0 < participation <= 1.0:
        raise ValueError("integer all-futures max_volume_participation must be in (0,1]")
    prior = np.nan_to_num(np.asarray(previous_volume, dtype=np.float64),
        nan=0.0, posinf=0.0, neginf=0.0)
    return np.floor(np.clip(prior, 0.0, None) * participation)
