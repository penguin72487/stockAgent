"""Device-side finite observations for an opted-in trajectory update."""
from __future__ import annotations

import os
from collections.abc import Iterable

import torch
import torch.distributed as dist


TRAJECTORY_DEVICE_FINITE_GUARD_ABI = "per_batch_device_flags_before_trajectory_update_v1"
TRAJECTORY_DEVICE_FINITE_GUARD_ENV = "STOCKAGENT_TRAJECTORY_DEVICE_FINITE_GUARD"


def trajectory_device_finite_guard_enabled() -> bool:
    value = os.environ.get(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, "0").strip().lower()
    if value not in {"0", "1", "false", "true", "off", "on", "no", "yes"}:
        raise ValueError(f"invalid boolean {TRAJECTORY_DEVICE_FINITE_GUARD_ENV}")
    return value in {"1", "true", "on", "yes"}


def tensors_finite_flag(tensors: Iterable[torch.Tensor]) -> torch.Tensor | None:
    """Enqueue the same infinity-norm check without reading a device scalar."""
    values = list(tensors)
    if not values:
        return None
    try:
        norms = torch._foreach_norm(values, float("inf"))
        return torch.isfinite(torch.stack(norms)).all()
    except (RuntimeError, TypeError):
        flag = torch.isfinite(values[0]).all()
        for value in values[1:]:
            flag = torch.logical_and(flag, torch.isfinite(value).all())
        return flag


class TrajectoryFiniteGuard:
    """Observe every loss/gradient; reject on all ranks before any update.

    No learned result is cached. Parameters stay fixed across these observations.
    An invalid intermediate observation stays invalid even if a later gradient
    is replaced or zeroed. The ordinary finalizer retains its pre/post-step checks.
    """

    def __init__(self, device: torch.device) -> None:
        self.flag = torch.ones((), device=device, dtype=torch.int32)
        self.first_bad_batch = torch.full((), -1, device=device, dtype=torch.int64)

    def _observe(self, finite: torch.Tensor, batch_index: int) -> None:
        first = (self.first_bad_batch < 0) & ~finite
        self.first_bad_batch.copy_(torch.where(
            first, torch.full_like(self.first_bad_batch, batch_index), self.first_bad_batch,
        ))
        self.flag.bitwise_and_(finite.to(dtype=self.flag.dtype))

    def observe_loss(self, loss: torch.Tensor, batch_index: int) -> None:
        self._observe(torch.isfinite(loss.detach()).all(), batch_index)

    def observe_gradients(self, model: torch.nn.Module, batch_index: int) -> None:
        flag = tensors_finite_flag(
            parameter.grad.detach() for parameter in model.parameters()
            if parameter.grad is not None
        )
        if flag is not None:
            self._observe(flag, batch_index)

    def require_finite(self, optimizer: torch.optim.Optimizer, *, distributed: bool = False) -> None:
        if distributed and dist.is_available() and dist.is_initialized():
            dist.all_reduce(self.flag, op=dist.ReduceOp.MIN)
        if not bool(self.flag.item()):
            optimizer.zero_grad(set_to_none=True)
            raise FloatingPointError(
                "non-finite loss or gradient during full-trajectory accumulation "
                f"(device guard; first local bad batch={int(self.first_bad_batch.item())})"
            )
