from __future__ import annotations

from dataclasses import dataclass
from random import Random

import torch

from stockagent.training.dataset import CrossSectionalDataset


@dataclass(slots=True)
class TimeBlockBatchSpec:
    context_start: int
    context_end: int
    target_start: int
    target_end: int
    target_offset: int
    row_start: int
    row_end: int
    reset_state: bool = False

    @property
    def target_len(self) -> int:
        return int(self.target_end) - int(self.target_start)


@dataclass(slots=True)
class TimeBlockSplit:
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

    def pin_memory(self) -> "TimeBlockSplit":
        def _pin(tensor: torch.Tensor) -> torch.Tensor:
            if tensor.device.type != "cpu" or tensor.is_pinned():
                return tensor
            try:
                return tensor.pin_memory()
            except torch.AcceleratorError as exc:
                if "out of memory" not in str(exc).lower():
                    raise
                return tensor

        return TimeBlockSplit(
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

    def to_device_cache(self, device: torch.device, non_blocking: bool = True) -> "TimeBlockSplit":
        return TimeBlockSplit(
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

    def iter_blocks(
        self,
        target_block_size: int,
        *,
        shuffle: bool = False,
        seed: int = 0,
    ) -> list[TimeBlockBatchSpec]:
        target_block_size = max(1, int(target_block_size))
        valid = self.valid_indices.detach().cpu().to(dtype=torch.long)
        if valid.numel() == 0:
            return []

        runs: list[tuple[int, int]] = []
        run_start = 0
        for idx in range(1, int(valid.numel())):
            if int(valid[idx]) != int(valid[idx - 1]) + 1:
                runs.append((run_start, idx))
                run_start = idx
        runs.append((run_start, int(valid.numel())))

        specs: list[TimeBlockBatchSpec] = []
        for row_start, row_end in runs:
            first_in_run = True
            for block_row_start in range(row_start, row_end, target_block_size):
                block_row_end = min(block_row_start + target_block_size, row_end)
                target_start = int(valid[block_row_start])
                target_end = int(valid[block_row_end - 1]) + 1
                context_start = target_start - self.lookback + 1
                if context_start < 0:
                    raise ValueError(
                        f"valid index {target_start} does not have lookback={self.lookback} context"
                    )
                specs.append(
                    TimeBlockBatchSpec(
                        context_start=context_start,
                        context_end=target_end,
                        target_start=target_start,
                        target_end=target_end,
                        target_offset=target_start - context_start,
                        row_start=block_row_start,
                        row_end=block_row_end,
                        reset_state=first_in_run,
                    )
                )
                first_in_run = False
        if shuffle:
            Random(int(seed)).shuffle(specs)
        return specs

    def get_block(
        self,
        spec: TimeBlockBatchSpec,
        device: torch.device,
        non_blocking: bool,
    ) -> dict[str, torch.Tensor | int]:
        source_device = self.features.device
        target_slice = slice(int(spec.target_start), int(spec.target_end))
        row_slice = slice(int(spec.row_start), int(spec.row_end))
        if self.sample_mask is None:
            sample_mask = torch.ones(int(spec.row_end) - int(spec.row_start), dtype=torch.bool, device=source_device)
        else:
            sample_mask = self.sample_mask[row_slice]
        return {
            "x_context": self.features[int(spec.context_start) : int(spec.context_end)].to(
                device=device,
                non_blocking=non_blocking,
            ),
            "future_log_returns": self.future_log_returns[target_slice].to(device=device, non_blocking=non_blocking),
            "tradable_mask": self.tradable_mask[target_slice].to(device=device, non_blocking=non_blocking),
            "can_buy_mask": self.can_buy_mask[target_slice].to(device=device, non_blocking=non_blocking),
            "can_sell_mask": self.can_sell_mask[target_slice].to(device=device, non_blocking=non_blocking),
            "benchmark": self.benchmark[target_slice].to(device=device, non_blocking=non_blocking),
            "sample_mask": sample_mask.to(device=device, non_blocking=non_blocking),
            "target_offset": int(spec.target_offset),
            "target_len": int(spec.target_len),
            "context_positions": torch.arange(
                int(spec.context_start),
                int(spec.context_end),
                dtype=torch.float32,
                device=source_device,
            ).to(device=device, non_blocking=non_blocking),
        }


def dataset_to_time_block_split(dataset: CrossSectionalDataset) -> TimeBlockSplit:
    return TimeBlockSplit(
        features=dataset.features_t,
        valid_indices=torch.as_tensor(dataset.valid_indices, dtype=torch.long),
        future_log_returns=dataset.future_log_returns_t,
        tradable_mask=dataset.tradable_mask_t,
        can_buy_mask=dataset.can_buy_mask_t,
        can_sell_mask=dataset.can_sell_mask_t,
        benchmark=dataset.benchmark_t,
        lookback=dataset.lookback,
    )
