"""Shared reporting normalization; never changes execution or training losses."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import torch


RETURN_METRICS_CONTRACT_VERSION = 2
# Match the existing NumPy report's finite floor for log-risk statistics.
# Arithmetic cumulative return and drawdown still round to a total loss.
ZERO_NAV_LOG = float(np.log(np.finfo(np.float64).tiny))


def clean_log_returns(values: np.ndarray) -> np.ndarray:
    raw = np.asarray(values, dtype=np.float64)
    clean = np.nan_to_num(raw, nan=0.0, posinf=0.0, neginf=ZERO_NAV_LOG)
    zero_nav = np.flatnonzero(raw <= ZERO_NAV_LOG)
    if zero_nav.size:
        first = int(zero_nav[0])
        # Includes historical float-min sentinels. Default is absorbing even
        # when an old adapter left nonzero observations after this row.
        clean[first] = ZERO_NAV_LOG - float(clean[:first].sum())
        clean[first + 1:] = 0.0
    return clean


def clean_log_returns_torch(values: torch.Tensor) -> torch.Tensor:
    """Apply the same reporting policy on-device without narrowing FP64."""
    import torch

    raw = values.to(dtype=torch.float64)
    clean = torch.nan_to_num(raw, nan=0.0, posinf=0.0, neginf=0.0)
    default = raw <= ZERO_NAV_LOG
    default_count = default.to(dtype=torch.int64).cumsum(dim=0)
    prefix = torch.where(default, 0.0, clean).cumsum(dim=0)
    return torch.where(
        default & (default_count == 1),
        ZERO_NAV_LOG - prefix,
        torch.where(default_count > 0, 0.0, clean),
    )
