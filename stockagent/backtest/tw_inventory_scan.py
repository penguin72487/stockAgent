"""Explicit native ATen scan/index boundaries for the physical inventory executor.

PyTorch 2.11's Triton SplitScan codegen fails for the composed [S,270] ledger
with broadcast validity predicates. Keep the proven native device cumsum in
the full graph, with its exact reverse-prefix adjoint. This does not patch
global Inductor lowerings, move work to CPU, or disable compilation of the
remaining ledger. Eager calls retain the ordinary torch implementation.

The composed searchsorted/gather NAV kernel also produced uninitialized reads
under Compute Sanitizer at [2754,270] on Torch 2.11 / RTX 5090. Keep that
non-differentiable index lookup native as well: it prevents unsafe indirect
read fusion/buffer reuse without changing FIFO algebra or moving work to CPU.
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


@torch.library.custom_op("stockagent::inventory_searchsorted", mutates_args=())
def inventory_searchsorted(boundaries: Tensor, values: Tensor, right: bool) -> Tensor:
    if boundaries.dtype != torch.float64 or values.dtype != torch.float64:
        raise ValueError("physical inventory lookup requires float64")
    return torch.searchsorted(boundaries, values, right=right)


@inventory_searchsorted.register_fake
def _searchsorted_fake(boundaries: Tensor, values: Tensor, right: bool) -> Tensor:
    if boundaries.dtype != torch.float64 or values.dtype != torch.float64:
        raise ValueError("physical inventory lookup requires float64")
    return values.new_empty(values.shape, dtype=torch.int64)


def fifo_searchsorted(boundaries: Tensor, values: Tensor, *, right: bool = False) -> Tensor:
    # Search indices have no gradient; gathers still differentiate through
    # their original cash/price/quantity tensors in the surrounding graph.
    if torch.compiler.is_compiling():
        return inventory_searchsorted(boundaries, values, right)
    return torch.searchsorted(boundaries, values, right=right)


@torch.library.custom_op("stockagent::inventory_column_stack", mutates_args=())
def inventory_column_stack(columns: list[Tensor]) -> Tensor:
    # A native layout boundary: composed Triton cat/pointwise fusion on the
    # real [64,2754,12] ledger misplaced ENTRY_COST between symbol columns.
    # The algebra remains a stack, with its exact unbind adjoint below.
    return torch.stack(columns, dim=-1)


@inventory_column_stack.register_fake
def _column_stack_fake(columns: list[Tensor]) -> Tensor:
    return columns[0].new_empty((*columns[0].shape, len(columns)))


def _column_stack_backward(ctx, grad):
    return (list(grad.unbind(-1)),)


inventory_column_stack.register_autograd(_column_stack_backward)


def fifo_stack_columns(columns: list[Tensor]) -> Tensor:
    if torch.compiler.is_compiling():
        return inventory_column_stack(columns)
    return torch.stack(columns, dim=-1)
