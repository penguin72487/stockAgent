from __future__ import annotations

from dataclasses import dataclass

import torch

from stockagent.training.dataset import CrossSectionalDataset


@dataclass(slots=True)
class WindowedSplitTensors:
    features: torch.Tensor
    valid_indices: torch.Tensor
    future_log_returns: torch.Tensor
    tradable_mask: torch.Tensor
    can_buy_mask: torch.Tensor
    can_sell_mask: torch.Tensor
    benchmark: torch.Tensor
    lookback: int
    sample_mask: torch.Tensor | None = None

    def __post_init__(self) -> None:
        self.lookback = int(self.lookback)
        if self.lookback < 1:
            raise ValueError("lookback must be >= 1")
        if self.features.dim() != 3:
            raise ValueError(f"features must have shape [T,S,F], got {tuple(self.features.shape)}")
        if self.valid_indices.dim() != 1:
            raise ValueError("valid_indices must be 1D")

    def __len__(self) -> int:
        return int(self.valid_indices.numel())

    @property
    def num_symbols(self) -> int:
        return int(self.features.size(1))

    def to_device_cache(self, device: torch.device, non_blocking: bool = True) -> "WindowedSplitTensors":
        return WindowedSplitTensors(
            features=self.features.to(device=device, non_blocking=non_blocking),
            valid_indices=self.valid_indices.to(device=device, non_blocking=non_blocking),
            future_log_returns=self.future_log_returns.to(device=device, non_blocking=non_blocking),
            tradable_mask=self.tradable_mask.to(device=device, non_blocking=non_blocking),
            can_buy_mask=self.can_buy_mask.to(device=device, non_blocking=non_blocking),
            can_sell_mask=self.can_sell_mask.to(device=device, non_blocking=non_blocking),
            benchmark=self.benchmark.to(device=device, non_blocking=non_blocking),
            lookback=self.lookback,
            sample_mask=(
                None if self.sample_mask is None else self.sample_mask.to(device=device, non_blocking=non_blocking)
            ),
        )

    def pin_memory(self) -> "WindowedSplitTensors":
        def _pin(tensor: torch.Tensor) -> torch.Tensor:
            if tensor.device.type != "cpu" or tensor.is_pinned():
                return tensor
            try:
                return tensor.pin_memory()
            except torch.AcceleratorError as exc:
                if "out of memory" not in str(exc).lower():
                    raise
                return tensor

        return WindowedSplitTensors(
            features=_pin(self.features),
            valid_indices=_pin(self.valid_indices),
            future_log_returns=_pin(self.future_log_returns),
            tradable_mask=_pin(self.tradable_mask),
            can_buy_mask=_pin(self.can_buy_mask),
            can_sell_mask=_pin(self.can_sell_mask),
            benchmark=_pin(self.benchmark),
            lookback=self.lookback,
            sample_mask=None if self.sample_mask is None else _pin(self.sample_mask),
        )

    def _window_indices_for_rows(self, row_indices: torch.Tensor) -> torch.Tensor:
        row_indices = row_indices.to(device=self.valid_indices.device, dtype=torch.long)
        date_idx = self.valid_indices[row_indices]
        offsets = torch.arange(
            self.lookback - 1,
            -1,
            -1,
            device=date_idx.device,
            dtype=torch.long,
        )
        return date_idx[:, None] - offsets[None, :]

    def _batch_from_row_indices(
        self,
        row_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
        *,
        contiguous_x: bool = True,
    ) -> dict[str, torch.Tensor]:
        source_device = self.features.device
        row_indices = row_indices.to(device=source_device, dtype=torch.long)
        window_idx = self._window_indices_for_rows(row_indices)
        date_idx = self.valid_indices[row_indices]

        x = self.features[window_idx]
        if contiguous_x:
            x = x.contiguous()
        if self.sample_mask is None:
            sample_mask = torch.ones(int(row_indices.numel()), dtype=torch.bool, device=source_device)
        else:
            sample_mask = self.sample_mask[row_indices]
        return {
            "x": x.to(device=device, non_blocking=non_blocking),
            "future_log_returns": self.future_log_returns[date_idx].to(device=device, non_blocking=non_blocking),
            "tradable_mask": self.tradable_mask[date_idx].to(device=device, non_blocking=non_blocking),
            "can_buy_mask": self.can_buy_mask[date_idx].to(device=device, non_blocking=non_blocking),
            "can_sell_mask": self.can_sell_mask[date_idx].to(device=device, non_blocking=non_blocking),
            "benchmark": self.benchmark[date_idx].to(device=device, non_blocking=non_blocking),
            "sample_mask": sample_mask.to(device=device, non_blocking=non_blocking),
        }

    def batch_by_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
    ) -> dict[str, torch.Tensor]:
        if end < start:
            raise ValueError("end must be >= start")
        rows = torch.arange(int(start), int(end), dtype=torch.long, device=self.valid_indices.device)
        return self._batch_from_row_indices(rows, device, non_blocking)

    def batch_by_batch_indices(
        self,
        batch_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
    ) -> dict[str, torch.Tensor]:
        return self._batch_from_row_indices(batch_indices.reshape(-1), device, non_blocking)

    def materialize_windows(
        self,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        if len(self) == 0:
            empty_x = self.features.new_empty((0, self.lookback, self.features.size(1), self.features.size(2)))
            empty_2d = self.future_log_returns.new_empty((0, self.features.size(1)))
            empty_mask = self.tradable_mask.new_empty((0, self.features.size(1)))
            empty_bench = self.benchmark.new_empty((0,))
            return empty_x, empty_2d, empty_mask, empty_mask.clone(), empty_mask.clone(), empty_bench

        row_indices = torch.arange(len(self), dtype=torch.long, device=self.valid_indices.device)
        window_idx = self._window_indices_for_rows(row_indices)
        date_idx = self.valid_indices[row_indices]
        return (
            self.features[window_idx].contiguous(),
            self.future_log_returns[date_idx],
            self.tradable_mask[date_idx],
            self.can_buy_mask[date_idx],
            self.can_sell_mask[date_idx],
            self.benchmark[date_idx],
        )


def dataset_to_windowed_tensors(dataset: CrossSectionalDataset) -> WindowedSplitTensors:
    return WindowedSplitTensors(
        features=dataset.features_t,
        valid_indices=torch.as_tensor(dataset.valid_indices, dtype=torch.long),
        future_log_returns=dataset.future_log_returns_t,
        tradable_mask=dataset.tradable_mask_t,
        can_buy_mask=dataset.can_buy_mask_t,
        can_sell_mask=dataset.can_sell_mask_t,
        benchmark=dataset.benchmark_t,
        lookback=dataset.lookback,
    )
