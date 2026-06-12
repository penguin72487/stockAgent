from __future__ import annotations

from dataclasses import dataclass, field

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
    _window_offsets: torch.Tensor = field(init=False, repr=False)
    _valid_indices_are_contiguous: bool = field(init=False, repr=False)
    _first_valid_index: int = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.lookback = int(self.lookback)
        if self.lookback < 1:
            raise ValueError("lookback must be >= 1")
        if self.features.dim() != 3:
            raise ValueError(f"features must have shape [T,S,F], got {tuple(self.features.shape)}")
        if self.valid_indices.dim() != 1:
            raise ValueError("valid_indices must be 1D")
        self._window_offsets = torch.arange(
            self.lookback - 1,
            -1,
            -1,
            device=self.valid_indices.device,
            dtype=torch.long,
        )
        if int(self.valid_indices.numel()) == 0:
            self._valid_indices_are_contiguous = True
            self._first_valid_index = 0
        else:
            self._first_valid_index = int(self.valid_indices[0].detach().cpu().item())
            if int(self.valid_indices.numel()) == 1:
                self._valid_indices_are_contiguous = True
            else:
                expected_last = self._first_valid_index + int(self.valid_indices.numel()) - 1
                actual_last = int(self.valid_indices[-1].detach().cpu().item())
                if actual_last != expected_last:
                    self._valid_indices_are_contiguous = False
                else:
                    diffs = self.valid_indices[1:] - self.valid_indices[:-1]
                    self._valid_indices_are_contiguous = bool(torch.all(diffs == 1).detach().cpu().item())

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
        return date_idx[:, None] - self._window_offsets[None, :]

    def _window_view_for_contiguous_rows(
        self,
        start: int,
        end: int,
        *,
        contiguous_x: bool,
    ) -> tuple[torch.Tensor, int, torch.Tensor]:
        batch_rows = int(end) - int(start)
        source_device = self.features.device
        date_start = self._first_valid_index + int(start)
        feature_start = date_start - self.lookback + 1
        if batch_rows <= 0 or feature_start < 0:
            raise ValueError("invalid contiguous window slice")
        source = self.features.narrow(0, feature_start, batch_rows + self.lookback - 1)
        x = source.unfold(0, self.lookback, 1).permute(0, 3, 1, 2)
        if contiguous_x:
            x = x.contiguous()
        if self.sample_mask is None:
            sample_mask = torch.ones(batch_rows, dtype=torch.bool, device=source_device)
        else:
            sample_mask = self.sample_mask.narrow(0, int(start), batch_rows)
        return x, date_start, sample_mask

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

    def _batch_metadata_from_row_indices(
        self,
        row_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
    ) -> dict[str, torch.Tensor]:
        source_device = self.valid_indices.device
        row_indices = row_indices.to(device=source_device, dtype=torch.long)
        date_idx = self.valid_indices[row_indices]
        if self.sample_mask is None:
            sample_mask = torch.ones(int(row_indices.numel()), dtype=torch.bool, device=source_device)
        else:
            sample_mask = self.sample_mask[row_indices]
        return {
            "date_indices": date_idx.to(device=device, non_blocking=non_blocking),
            "future_log_returns": self.future_log_returns[date_idx].to(device=device, non_blocking=non_blocking),
            "tradable_mask": self.tradable_mask[date_idx].to(device=device, non_blocking=non_blocking),
            "can_buy_mask": self.can_buy_mask[date_idx].to(device=device, non_blocking=non_blocking),
            "can_sell_mask": self.can_sell_mask[date_idx].to(device=device, non_blocking=non_blocking),
            "benchmark": self.benchmark[date_idx].to(device=device, non_blocking=non_blocking),
            "sample_mask": sample_mask.to(device=device, non_blocking=non_blocking),
        }

    def _batch_from_contiguous_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        *,
        contiguous_x: bool = True,
    ) -> dict[str, torch.Tensor]:
        x, date_start, sample_mask = self._window_view_for_contiguous_rows(
            start,
            end,
            contiguous_x=contiguous_x,
        )
        rows = int(end) - int(start)
        return {
            "x": x.to(device=device, non_blocking=non_blocking),
            "future_log_returns": self.future_log_returns.narrow(0, date_start, rows).to(
                device=device,
                non_blocking=non_blocking,
            ),
            "tradable_mask": self.tradable_mask.narrow(0, date_start, rows).to(
                device=device,
                non_blocking=non_blocking,
            ),
            "can_buy_mask": self.can_buy_mask.narrow(0, date_start, rows).to(
                device=device,
                non_blocking=non_blocking,
            ),
            "can_sell_mask": self.can_sell_mask.narrow(0, date_start, rows).to(
                device=device,
                non_blocking=non_blocking,
            ),
            "benchmark": self.benchmark.narrow(0, date_start, rows).to(device=device, non_blocking=non_blocking),
            "sample_mask": sample_mask.to(device=device, non_blocking=non_blocking),
        }

    def _batch_metadata_from_contiguous_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
    ) -> dict[str, torch.Tensor]:
        rows = int(end) - int(start)
        date_start = self._first_valid_index + int(start)
        source_device = self.valid_indices.device
        if rows < 0:
            raise ValueError("end must be >= start")
        if self.sample_mask is None:
            sample_mask = torch.ones(rows, dtype=torch.bool, device=source_device)
        else:
            sample_mask = self.sample_mask.narrow(0, int(start), rows)
        date_indices = torch.arange(date_start, date_start + rows, dtype=torch.long, device=source_device)
        return {
            "date_indices": date_indices.to(device=device, non_blocking=non_blocking),
            "future_log_returns": self.future_log_returns.narrow(0, date_start, rows).to(
                device=device,
                non_blocking=non_blocking,
            ),
            "tradable_mask": self.tradable_mask.narrow(0, date_start, rows).to(
                device=device,
                non_blocking=non_blocking,
            ),
            "can_buy_mask": self.can_buy_mask.narrow(0, date_start, rows).to(
                device=device,
                non_blocking=non_blocking,
            ),
            "can_sell_mask": self.can_sell_mask.narrow(0, date_start, rows).to(
                device=device,
                non_blocking=non_blocking,
            ),
            "benchmark": self.benchmark.narrow(0, date_start, rows).to(device=device, non_blocking=non_blocking),
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
        if self._valid_indices_are_contiguous:
            return self._batch_from_contiguous_rows(start, end, device, non_blocking)
        rows = torch.arange(int(start), int(end), dtype=torch.long, device=self.valid_indices.device)
        return self._batch_from_row_indices(rows, device, non_blocking)

    def batch_metadata_by_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
    ) -> dict[str, torch.Tensor]:
        if end < start:
            raise ValueError("end must be >= start")
        if self._valid_indices_are_contiguous:
            return self._batch_metadata_from_contiguous_rows(start, end, device, non_blocking)
        rows = torch.arange(int(start), int(end), dtype=torch.long, device=self.valid_indices.device)
        return self._batch_metadata_from_row_indices(rows, device, non_blocking)

    def batch_by_batch_indices(
        self,
        batch_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
    ) -> dict[str, torch.Tensor]:
        return self._batch_from_row_indices(batch_indices.reshape(-1), device, non_blocking)

    def batch_metadata_by_batch_indices(
        self,
        batch_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
    ) -> dict[str, torch.Tensor]:
        return self._batch_metadata_from_row_indices(batch_indices.reshape(-1), device, non_blocking)

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
        date_idx = self.valid_indices[row_indices]
        if self._valid_indices_are_contiguous:
            x, _, _ = self._window_view_for_contiguous_rows(0, len(self), contiguous_x=True)
        else:
            window_idx = self._window_indices_for_rows(row_indices)
            x = self.features[window_idx].contiguous()
        return (
            x,
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
