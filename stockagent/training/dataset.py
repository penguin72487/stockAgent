from __future__ import annotations

import numpy as np
import torch
from torch.utils.data import Dataset

from stockagent.data.panel import PanelData


class CrossSectionalDataset(Dataset[dict[str, torch.Tensor]]):
    def __init__(self, panel: PanelData, date_indices: np.ndarray, lookback: int) -> None:
        self.lookback = lookback
        self.date_indices = np.array(sorted(date_indices.tolist()), dtype=np.int64)
        # Keep only indices that have a full lookback window inside this fold.
        fold_start_idx = int(self.date_indices[0])
        min_valid_idx = fold_start_idx + lookback - 1
        self.valid_indices = self.date_indices[self.date_indices >= min_valid_idx]

        returns = np.nan_to_num(panel.returns_1d, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32, copy=False)
        tradable = panel.tradable_mask & np.isfinite(panel.returns_1d)

        # Cache tensors once to avoid per-item numpy copies.
        self.features_t = torch.from_numpy(panel.features)
        self.future_log_returns_t = torch.from_numpy(returns)
        self.tradable_mask_t = torch.from_numpy(tradable)
        self.benchmark_t = torch.from_numpy(panel.benchmark_returns.astype(np.float32, copy=False))

    def __len__(self) -> int:
        return int(self.valid_indices.size)

    def __getitem__(self, index: int) -> dict[str, torch.Tensor]:
        date_idx = int(self.valid_indices[index])
        start_idx = date_idx - self.lookback + 1
        return {
            "x": self.features_t[start_idx : date_idx + 1],
            "future_log_returns": self.future_log_returns_t[date_idx],
            "tradable_mask": self.tradable_mask_t[date_idx],
            "benchmark": self.benchmark_t[date_idx],
        }


def collate_batch(samples: list[dict[str, torch.Tensor]]) -> dict[str, torch.Tensor]:
    return {
        "x": torch.stack([s["x"] for s in samples]),
        "future_log_returns": torch.stack([s["future_log_returns"] for s in samples]),
        "tradable_mask": torch.stack([s["tradable_mask"] for s in samples]),
        "benchmark": torch.stack([s["benchmark"] for s in samples]),
    }
