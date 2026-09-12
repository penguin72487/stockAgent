"""An explicit ATen prefix-sum boundary for the physical inventory executor.

PyTorch 2.11's Triton SplitScan codegen fails for the composed [S,270] ledger
with broadcast validity predicates. Keep the proven native device cumsum in
the full graph, with its exact reverse-prefix adjoint. This does not patch
global Inductor lowerings, move work to CPU, or disable compilation of the
remaining ledger. Eager calls retain the ordinary torch implementation.
"""
from __future__ import annotations

import torch
from torch import Tensor


@torch.library.custom_op("stockagent::inventory_prefix_sum", mutates_args=())
def inventory_prefix_sum(value: Tensor, dim: int) -> Tensor:
    if value.dtype != torch.float64:
        raise ValueError("physical inventory scan requires float64")
    return torch.cumsum(value, dim=dim)


@inventory_prefix_sum.register_fake
def _fake(value: Tensor, dim: int) -> Tensor:
    if value.dtype != torch.float64 or not -value.ndim <= dim < value.ndim:
        raise ValueError("invalid physical inventory scan dtype/dimension")
    return value.new_empty(value.shape)


def _setup_context(ctx, inputs, output):
    ctx.dim = inputs[1]


def _backward(ctx, grad):
    # d sum_{j<=i} x[j] / dx[k] = 1[k<=i]. No forward storage is needed.
    return inventory_prefix_sum(grad.flip((ctx.dim,)), ctx.dim).flip((ctx.dim,)), None


inventory_prefix_sum.register_autograd(_backward, setup_context=_setup_context)


def fifo_cumsum(value: Tensor, dim: int) -> Tensor:
    if torch.compiler.is_compiling():
        return inventory_prefix_sum(value, dim)
    return torch.cumsum(value, dim=dim)
