"""Bit-preserving grouped copies into fixed financial CUDA Graph buffers."""
from __future__ import annotations

from functools import lru_cache
import os

import torch


def packed_session_copy_contract():
    backend = os.environ.get('STOCKAGENT_DAY_TRADE_GRAPH_PACKED_COPY', 'native')
    if backend == 'native':
        return None
    if backend != 'triton':
        raise ValueError('packed financial graph copy must be native or triton')
    return dict(version='bitwise_strided_session_pointer_table_v1', backend=backend)


@lru_cache(maxsize=1)
def _copy_kernel():
    import triton
    import triton.language as tl

    @triton.jit
    def copy_fields(metadata, count, start, BLOCK: tl.constexpr):
        base = metadata + tl.program_id(1) * 11
        src, dst = tl.load(base), tl.load(base + 1)
        size, width = tl.load(base + 2), tl.load(base + 3)
        s0, s1, s2, s3 = (tl.load(base + 4), tl.load(base + 5),
                          tl.load(base + 6), tl.load(base + 7))
        d1, d2, d3 = tl.load(base + 8), tl.load(base + 9), tl.load(base + 10)
        offset = tl.program_id(0) * BLOCK + tl.arange(0, BLOCK)
        if tl.program_id(0) * BLOCK < size:
            row = offset // (d1 * d2 * d3)
            index = (start + tl.minimum(row, count - 1)) * s0
            index += ((offset // (d2 * d3)) % d1) * s1
            index += ((offset // d3) % d2) * s2 + (offset % d3) * s3
            mask = offset < size
            if width == 8:
                value64 = tl.load(src.to(tl.pointer_type(tl.uint64)) + index, mask, 0)
                tl.store(dst.to(tl.pointer_type(tl.uint64)) + offset, value64, mask)
            elif width == 4:
                value32 = tl.load(src.to(tl.pointer_type(tl.uint32)) + index, mask, 0)
                tl.store(dst.to(tl.pointer_type(tl.uint32)) + offset, value32, mask)
            elif width == 2:
                value16 = tl.load(src.to(tl.pointer_type(tl.uint16)) + index, mask, 0)
                tl.store(dst.to(tl.pointer_type(tl.uint16)) + offset, value16, mask)
            else:
                value8 = tl.load(src.to(tl.pointer_type(tl.uint8)) + index, mask, 0)
                tl.store(dst.to(tl.pointer_type(tl.uint8)) + offset, value8, mask)
    return copy_fields


@torch.no_grad()
def copy_packed_session_fields_(destinations, sources, *, source_row, rows, cache):
    """Copy current source bits, padding by its last real row as native does.

    The caller proves packed-row provenance. Only pointer/shape metadata is
    cached, never source values, learned outputs or source Tensor references.
    Every invocation validates current geometry and records current streams;
    a freed source cannot be pinned by this byte-bounded metadata cache.
    Unsupported layouts retain the canonical per-field copy implementation.
    """
    if len(destinations) != len(sources) or not sources or rows < 1 or source_row < 0:
        return False
    device = destinations[0].device
    if device.type != 'cuda':
        return False
    records = []
    for destination, source in zip(destinations, sources, strict=True):
        if (source.layout != torch.strided or destination.layout != torch.strided
                or source.device != device or destination.device != device
                or source.dtype != destination.dtype or not destination.is_contiguous()
                or source.ndim < 1 or source.ndim > 4 or source.ndim != destination.ndim
                or source.shape[1:] != destination.shape[1:]
                or destination.size(0) < rows or source.size(0) < source_row + rows
                or min(source.stride()) < 0 or source.element_size() not in (1, 2, 4, 8)
                or not source.numel() or not destination.numel()
                or source.untyped_storage().data_ptr() == destination.untyped_storage().data_ptr()):
            return False
        stride = (*source.stride(), *((0,) * (4 - source.ndim)))
        shape = (*source.shape, *((1,) * (4 - source.ndim)))
        records.append((source.data_ptr(), destination.data_ptr(), destination.numel(),
                        source.element_size(), *stride, *shape[1:]))
    key = tuple(records)
    cached = cache.pop(key, None)
    if cached is None:
        with torch.inference_mode(False), torch.no_grad():
            host = torch.tensor(records, dtype=torch.int64, pin_memory=True)
            metadata = host.to(device, non_blocking=True)
        cached = metadata, max(item[2] for item in records)
    cache[key] = cached
    # At most 32 pointer tables or 256 KiB, whichever binds first. The GPU
    # payload is small even when the source planes themselves span GiB.
    while len(cache) > 32 or (len(cache) > 1 and
            sum(value[0].numel() * value[0].element_size() for value in cache.values()) > 256 * 1024):
        cache.popitem(last=False)
    metadata, elements = cached
    with torch.cuda.device(device):
        stream = torch.cuda.current_stream(device)
        metadata.record_stream(stream)
        for tensor in (*sources, *destinations):
            tensor.record_stream(stream)
        _copy_kernel()[((elements + 255) // 256, len(records))](metadata, rows, source_row, 256)
    return True
