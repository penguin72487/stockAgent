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
An explicit bounded Triton scan experiment retains that opaque boundary and
FP64 storage; its execution contract prevents native-optimizer resume.
"""
from __future__ import annotations

from functools import lru_cache
import os

import torch
from torch import Tensor


POSITIVE_PREFIX_ABI = 'opaque_int64_nonpositive_prefix_v1'


@torch.library.custom_op('stockagent::inventory_positive_prefix_v1', mutates_args=())
def inventory_positive_prefix(value: Tensor, dim: int = 0) -> Tensor:
    """Absorbing positive-state mask through the opaque canonical scan.

    Counting failed comparisons preserves boolean cumprod exactly, including
    NaNs and signed zero. Keeping the scan opaque also avoids a PyTorch 2.11
    batched cumprod/pointwise codegen failure in parallel session maps.
    """
    return (~(value > 0)).to(torch.int64).cumsum(dim) == 0


@inventory_positive_prefix.register_fake
def _positive_prefix_fake(value: Tensor, dim: int = 0) -> Tensor:
    if not -value.ndim <= dim < value.ndim:
        raise ValueError('invalid positive prefix dimension')
    return value.new_empty(value.shape, dtype=torch.bool)


def inventory_prefix_contract() -> str | None:
    backend = os.environ.get('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', 'native')
    if backend not in {'native', 'triton'}:
        raise ValueError('inventory prefix backend must be native or triton')
    return 'fp64_strided_short_axis_triton_v2' if backend == 'triton' else None


@lru_cache(maxsize=1)
def _prefix_kernel():
    import triton
    import triton.language as tl

    @triton.jit
    def kernel(source, target, ROWS: tl.constexpr, WIDTH: tl.constexpr,
               ROW_STRIDE: tl.constexpr, COLUMN_STRIDE: tl.constexpr,
               OUTPUT_ROW_STRIDE: tl.constexpr, OUTPUT_COLUMN_STRIDE: tl.constexpr,
               R: tl.constexpr, C: tl.constexpr):
        row = tl.program_id(0) * R + tl.arange(0, R)
        column = tl.arange(0, C)
        mask = (row[:, None] < ROWS) & (column[None, :] < WIDTH)
        values = tl.load(source + row[:, None] * ROW_STRIDE
                         + column[None, :] * COLUMN_STRIDE, mask, 0.)
        cumulative = tl.cumsum(values, axis=1)
        tl.store(target + row[:, None] * OUTPUT_ROW_STRIDE
                 + column[None, :] * OUTPUT_COLUMN_STRIDE, cumulative, mask)

    return kernel


def _device_prefix_sum(value: Tensor, dim: int) -> Tensor:
    """Bounded FP64 scan behind the existing opaque compiler boundary.

    Short cohort/opportunity axes fit one independent row scan. Preserve FP64,
    arbitrary positive input strides, output ownership
    and the existing reverse-prefix adjoint. Larger/ranked inputs keep ATen.
    This is an explicit numerical scheduling experiment, never a price cast.
    """
    contract = inventory_prefix_contract()
    if (contract is None or value.device.type != 'cuda' or value.ndim != 2
            or not -2 <= dim < 2 or not 0 < value.size(dim) <= 32):
        return torch.cumsum(value, dim=dim)
    import triton
    axis = dim % 2
    other = 1 - axis
    rows, width = value.size(other), value.size(axis)
    result = torch.empty(value.shape, dtype=value.dtype, device=value.device)
    if rows:
        columns = triton.next_power_of_2(width)
        row_tile = max(1, 128 // columns)
        _prefix_kernel()[(triton.cdiv(rows, row_tile),)](
            value, result, rows, width, value.stride(other), value.stride(axis),
            result.stride(other), result.stride(axis), row_tile, columns, num_warps=4)
    return result


@torch.library.custom_op("stockagent::inventory_prefix_sum", mutates_args=())
def inventory_prefix_sum(value: Tensor, dim: int) -> Tensor:
    if value.dtype != torch.float64:
        raise ValueError("physical inventory scan requires float64")
    return _device_prefix_sum(value, dim)


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
