"""Autograd-safe scalar collectives for symbol-sharded finance ledgers.

The recurrent Taiwan ledgers shard only the symbol axis.  Account-level
quantities (NAV, cash capacity, maintenance requirements, and settlement
claims) are reconstructed from packed local scalar contributions.  Keeping
the reduction boundary explicit prevents accidentally reducing replicated
cash/T+2 state a second time.

This module deliberately does not implement a custom VJP.  Floating-point
SUM collectives use PyTorch's autograd-enabled functional collective, so the
backward collective is owned by PyTorch and remains visible to AOTAutograd.
"""

from __future__ import annotations

from collections.abc import Iterable
import os

import torch
import torch.distributed as dist

try:
    import torch.distributed._functional_collectives as dist_fc
except ImportError:  # pragma: no cover - older PyTorch compatibility
    dist_fc = None

try:
    import torch.distributed.nn.functional as dist_nn_f
except ImportError:  # pragma: no cover - older PyTorch compatibility
    dist_nn_f = None


def symbol_sharded_world_size(enabled: bool) -> int:
    """Return the active symbol-shard count and fail closed when misconfigured."""

    if not enabled:
        return 1
    if not dist.is_available() or not dist.is_initialized():
        raise RuntimeError(
            "symbol-sharded ledger requires an initialized torch.distributed "
            "process group"
        )
    world_size = int(dist.get_world_size())
    if world_size <= 1:
        raise RuntimeError(
            "symbol-sharded ledger requires world_size greater than one"
        )
    return world_size


def _functional_sum(values: torch.Tensor) -> torch.Tensor:
    """Autograd-preserving SUM all-reduce with a realized tensor result."""

    group = dist.group.WORLD
    if dist_fc is not None and hasattr(dist_fc, "all_reduce"):
        reduced = dist_fc.all_reduce(values, "sum", group=group)
        if hasattr(dist_fc, "wait_tensor"):
            reduced = dist_fc.wait_tensor(reduced)
        return reduced
    if dist_nn_f is None:
        raise RuntimeError("PyTorch autograd distributed collectives are unavailable")
    return dist_nn_f.all_reduce(values, op=dist.ReduceOp.SUM, group=group)


def symbol_sharded_pack_scalars_enabled() -> bool:
    """Return the process-local scalar-coalescing experiment switch."""

    return os.environ.get("STOCKAGENT_SYMBOL_SHARDED_PACK_SCALARS", "1") != "0"


def symbol_sharded_skip_noop_collectives_enabled() -> bool:
    """Return whether statically empty account collectives are specialized out."""

    return (
        os.environ.get("STOCKAGENT_SYMBOL_SHARDED_SKIP_NOOP_COLLECTIVES", "1")
        != "0"
    )


def global_symbol_scalar_pack(
    values: Iterable[torch.Tensor],
    *,
    symbol_sharded: bool,
) -> torch.Tensor:
    """Pack local scalar contributions and return their global SUM.

    Every input must already be a scalar contribution from the local symbol
    shard.  Replicated account scalars must never be passed here.
    """

    packed_values = tuple(value.reshape(()) for value in values)
    if not packed_values:
        raise ValueError("global_symbol_scalar_pack requires at least one value")
    packed = torch.stack(packed_values)
    if not symbol_sharded:
        return packed
    symbol_sharded_world_size(True)
    if not symbol_sharded_pack_scalars_enabled() and len(packed_values) > 1:
        return torch.stack(tuple(_functional_sum(value) for value in packed_values))
    return _functional_sum(packed)


def global_symbol_sum(
    value: torch.Tensor,
    *,
    symbol_sharded: bool,
) -> torch.Tensor:
    """Return the global sum of one symbol-sharded tensor."""

    return global_symbol_scalar_pack(
        (value.sum(),),
        symbol_sharded=symbol_sharded,
    )[0]


def global_symbol_tensor_sum(
    value: torch.Tensor,
    *,
    symbol_sharded: bool,
) -> torch.Tensor:
    """Sum same-shaped local symbol contributions without collapsing time.

    This is used for row-wise portfolio norms: the symbol axis has already
    been reduced locally, while every rank owns the same chronological rows.
    """

    if not symbol_sharded:
        return value
    symbol_sharded_world_size(True)
    return _functional_sum(value)


def global_symbol_any(
    value: torch.Tensor,
    *,
    symbol_sharded: bool,
) -> torch.Tensor:
    """Return a replicated scalar OR over every symbol shard.

    Boolean branch decisions are non-differentiable.  A SUM of int32 flags is
    nevertheless expressed through the same functional collective so it can
    be captured together with compiled tensor code.
    """

    local = value.to(dtype=torch.int32).sum().clamp_max(1)
    if not symbol_sharded:
        return local > 0
    symbol_sharded_world_size(True)
    return _functional_sum(local.reshape(1))[0] > 0


def global_symbol_all(
    value: torch.Tensor,
    *,
    symbol_sharded: bool,
) -> torch.Tensor:
    """Return a replicated scalar AND over every symbol shard."""

    return ~global_symbol_any(~value.to(dtype=torch.bool), symbol_sharded=symbol_sharded)
