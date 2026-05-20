from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch
from torch.utils.data import Dataset

from stockagent.data.panel import PanelData


def _zscore_cross_section(log_returns: np.ndarray, tradable_mask: np.ndarray) -> np.ndarray:
    """Cross-sectional z-score of log returns for training stability.

    Normalises each day's distribution to mean=0, std=1 so the MSE loss
    is scale-invariant across regimes.
    """
    out = np.zeros_like(log_returns, dtype=np.float32)
    valid_idx = np.flatnonzero(tradable_mask & np.isfinite(log_returns))
    if valid_idx.size < 2:
        return out
    vals = log_returns[valid_idx].astype(np.float64)
    mean = vals.mean()
    std = vals.std() + 1e-8
    out[valid_idx] = ((vals - mean) / std).astype(np.float32)
    return out


@dataclass(slots=True)
class BatchSample:
    x: torch.Tensor              # [lookback, S, F]  feature window
    y: torch.Tensor              # [S]  cross-sectional z-scored log return (train target)
    future_log_returns: torch.Tensor  # [S]  raw log returns (backtest / IC)
    tradable_mask: torch.Tensor  # [S]  bool
    benchmark: torch.Tensor      # scalar  universe-average log return


class CrossSectionalDataset(Dataset[BatchSample]):
    def __init__(self, panel: PanelData, date_indices: np.ndarray, lookback: int) -> None:
        self.panel = panel
        self.lookback = lookback
        self.date_indices = np.array(sorted(date_indices.tolist()), dtype=np.int64)
        self.valid_indices = self.date_indices[self.date_indices >= lookback - 1]

    def __len__(self) -> int:
        return int(self.valid_indices.size)

    def __getitem__(self, index: int) -> BatchSample:
        date_idx = int(self.valid_indices[index])
        start_idx = date_idx - self.lookback + 1
        x = self.panel.features[start_idx : date_idx + 1]          # [L, S, F]
        log_ret = self.panel.returns_1d[date_idx]                   # [S]
        tradable = self.panel.tradable_mask[date_idx] & np.isfinite(log_ret)
        y = _zscore_cross_section(log_ret, tradable)
        future_log = np.nan_to_num(log_ret, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)

        return BatchSample(
            x=torch.from_numpy(x.copy()),
            y=torch.from_numpy(y.copy()),
            future_log_returns=torch.from_numpy(future_log.copy()),
            tradable_mask=torch.from_numpy(tradable.copy()),
            benchmark=torch.tensor(float(self.panel.benchmark_returns[date_idx]), dtype=torch.float32),
        )


def collate_batch(samples: list[BatchSample]) -> dict[str, torch.Tensor]:
    return {
        "x": torch.stack([s.x for s in samples]),
        "y": torch.stack([s.y for s in samples]),
        "future_log_returns": torch.stack([s.future_log_returns for s in samples]),
        "tradable_mask": torch.stack([s.tradable_mask for s in samples]),
        "benchmark": torch.stack([s.benchmark for s in samples]),
    }
