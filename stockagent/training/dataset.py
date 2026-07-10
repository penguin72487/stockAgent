from __future__ import annotations

import numpy as np
import torch
from torch.utils.data import Dataset

from stockagent.data.panel import PanelData


class CrossSectionalDataset(Dataset[dict[str, torch.Tensor]]):
    def __init__(
        self,
        panel: PanelData,
        date_indices: np.ndarray,
        lookback: int,
        *,
        allow_empty: bool = False,
    ) -> None:
        self.lookback = int(lookback)
        self.date_indices = np.array(sorted(np.asarray(date_indices, dtype=np.int64).tolist()), dtype=np.int64)
        tradable = panel.tradable_mask & np.isfinite(panel.returns_1d)
        force_exit = (
            panel.force_exit_mask
            if panel.force_exit_mask is not None
            else np.zeros_like(tradable, dtype=bool)
        )
        if self.date_indices.size == 0:
            valid_indices = self.date_indices
            if not allow_empty:
                raise ValueError("Fold has no dates after split filtering.")
        else:
            # Keep only indices that have a full lookback window inside this fold.
            fold_start_idx = int(self.date_indices[0])
            min_valid_idx = fold_start_idx + self.lookback - 1
            valid_indices = self.date_indices[self.date_indices >= min_valid_idx]
            if valid_indices.size > 0:
                executable_or_terminal = (
                    tradable[valid_indices].any(axis=1)
                    | force_exit[valid_indices].any(axis=1)
                )
                valid_indices = valid_indices[executable_or_terminal]
        self.valid_indices = valid_indices

        if len(self.valid_indices) == 0 and not allow_empty:
            raise ValueError(
                f"Fold has insufficient data for lookback={self.lookback}. Need at least {self.lookback} dates."
            )

        returns = np.nan_to_num(
            panel.returns_1d,
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
            copy=True,
        ).astype(np.float32, copy=False)
        if panel.can_buy_mask is None or panel.can_sell_mask is None:
            raise ValueError(
                "PanelData must provide can_buy_mask and can_sell_mask; no-fallback dataset path "
                "does not infer side masks from tradable_mask"
            )
        can_buy = panel.can_buy_mask
        can_sell = panel.can_sell_mask
        can_short_open = (
            panel.can_short_open_mask
            if panel.can_short_open_mask is not None
            else can_sell.copy()
        )
        force_short_cover = (
            panel.force_short_cover_mask
            if panel.force_short_cover_mask is not None
            else np.zeros_like(tradable, dtype=bool)
        )
        # build_panel sanitizes feature NaN/inf values before caching.  Re-running
        # torch.nan_to_num here would duplicate the full panel for every split.
        features = panel.features.astype(np.float32, copy=False)
        if not features.flags.c_contiguous:
            features = np.ascontiguousarray(features)
        self.features_t = torch.from_numpy(features)
        self.future_log_returns_t = torch.from_numpy(returns)
        daily_volumes = getattr(panel, "daily_volumes", None)
        if daily_volumes is None:
            volume_notional = np.full_like(panel.close_prices, np.inf, dtype=np.float32)
        else:
            daily_volumes_arr = np.asarray(daily_volumes, dtype=np.float32)
            close_prices_arr = np.asarray(panel.close_prices, dtype=np.float32)
            volume_notional = (daily_volumes_arr * close_prices_arr).astype(np.float32, copy=False)
        self.volume_notional_t = torch.from_numpy(volume_notional)
        self.tradable_mask_t = torch.from_numpy(tradable)
        self.can_buy_mask_t = torch.from_numpy(can_buy)
        self.can_sell_mask_t = torch.from_numpy(can_sell)
        self.can_short_open_mask_t = torch.from_numpy(can_short_open)
        self.force_short_cover_mask_t = torch.from_numpy(force_short_cover)
        self.force_exit_mask_t = torch.from_numpy(force_exit)
        self.benchmark_t = torch.from_numpy(panel.benchmark_returns.astype(np.float32, copy=False))

    def __len__(self) -> int:
        return int(self.valid_indices.size)

    def __getitem__(self, index: int) -> dict[str, torch.Tensor]:
        date_idx = int(self.valid_indices[index])
        start_idx = date_idx - self.lookback + 1
        return {
            "x": self.features_t[start_idx : date_idx + 1],
            "future_log_returns": self.future_log_returns_t[date_idx],
            "volume_notional": self.volume_notional_t[date_idx],
            "tradable_mask": self.tradable_mask_t[date_idx],
            "can_buy_mask": self.can_buy_mask_t[date_idx],
            "can_sell_mask": self.can_sell_mask_t[date_idx],
            "can_short_open_mask": self.can_short_open_mask_t[date_idx],
            "force_short_cover_mask": self.force_short_cover_mask_t[date_idx],
            "force_exit_mask": self.force_exit_mask_t[date_idx],
            "benchmark": self.benchmark_t[date_idx],
        }


def collate_batch(
    samples: list[dict[str, torch.Tensor]],
    batch_size: int | None = None,
) -> dict[str, torch.Tensor]:
    if batch_size is None or len(samples) >= batch_size:
        return {
            "x": torch.stack([s["x"] for s in samples]),
            "future_log_returns": torch.stack([s["future_log_returns"] for s in samples]),
            "volume_notional": torch.stack([s["volume_notional"] for s in samples]),
            "tradable_mask": torch.stack([s["tradable_mask"] for s in samples]),
            "can_buy_mask": torch.stack([s["can_buy_mask"] for s in samples]),
            "can_sell_mask": torch.stack([s["can_sell_mask"] for s in samples]),
            "can_short_open_mask": torch.stack([s["can_short_open_mask"] for s in samples]),
            "force_short_cover_mask": torch.stack([s["force_short_cover_mask"] for s in samples]),
            "force_exit_mask": torch.stack([s["force_exit_mask"] for s in samples]),
            "benchmark": torch.stack([s["benchmark"] for s in samples]),
            "sample_mask": torch.ones(len(samples), dtype=torch.bool),
        }

    pad_count = batch_size - len(samples)
    template = samples[0]

    def _pad_tensor_list(name: str) -> torch.Tensor:
        values = [s[name] for s in samples]
        padding = [torch.zeros_like(template[name]) for _ in range(pad_count)]
        return torch.stack(values + padding)

    return {
        "x": _pad_tensor_list("x"),
        "future_log_returns": _pad_tensor_list("future_log_returns"),
        "volume_notional": _pad_tensor_list("volume_notional"),
        "tradable_mask": _pad_tensor_list("tradable_mask"),
        "can_buy_mask": _pad_tensor_list("can_buy_mask"),
        "can_sell_mask": _pad_tensor_list("can_sell_mask"),
        "can_short_open_mask": _pad_tensor_list("can_short_open_mask"),
        "force_short_cover_mask": _pad_tensor_list("force_short_cover_mask"),
        "force_exit_mask": _pad_tensor_list("force_exit_mask"),
        "benchmark": _pad_tensor_list("benchmark"),
        "sample_mask": torch.tensor([True] * len(samples) + [False] * pad_count, dtype=torch.bool),
    }
