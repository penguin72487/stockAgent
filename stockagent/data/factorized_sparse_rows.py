"""Bounded GPU cache of immutable, full-schema normalized source rows.

Only proven positive-zero storage cells are omitted. LayerNorm's zero-column
affine contribution is contracted by the model; features and parameters are
not selected. The fold-fitted normalization buffers are part of cache identity.
No learned embedding, weight, optimizer state, or future aggregate is cached.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass

import numpy as np
import torch

from stockagent.data.factorized_panel import _ColumnBlock


SPARSE_ROW_CONTRACT = "full_schema_csr_affine_stable_full_rows_gpu_resident_v2"


@dataclass
class _SparseBlock:
    start: int
    stop: int
    symbols: int
    features: int
    host_crow: np.ndarray
    crow: torch.Tensor
    columns: torch.Tensor
    values: torch.Tensor
    zero_value: torch.Tensor

    @property
    def nbytes(self):
        tensors = (self.crow, self.columns, self.values, self.zero_value)
        return sum(t.numel() * t.element_size() for t in tensors if t is not None)

    def slice_rows(self, start, stop):
        """Use views of resident CSR bytes, without raw input reconstruction."""
        first = (start - self.start) * self.symbols
        last = (stop - self.start) * self.symbols
        a, b = int(self.host_crow[first]), int(self.host_crow[last])
        crow = self.crow[first:last+1] - a
        columns = self.columns[a:b]
        values = self.values[a:b]
        # Torch 2.11 also requires the scoped policy acknowledgment; passing
        # only the constructor flag still emits its implicit-policy warning.
        with torch.sparse.check_sparse_tensor_invariants(False):
            matrix = torch.sparse_csr_tensor(crow, columns, values,
                size=(last-first, self.features), device=values.device, check_invariants=False)
        return matrix, self.zero_value[first:last]


class FactorizedSparseRowStore:
    """Cache physical date blocks; overlapping windows share source bytes."""

    def __init__(self, source, candle, device, *, budget_bytes):
        from torch import nn
        from stockagent.models.transformer_base_portfolio import PortfolioRMSNorm
        if not isinstance(candle.input_norm, (nn.LayerNorm, PortfolioRMSNorm)):
            raise ValueError("sparse rows require the full-schema LayerNorm/RMSNorm stem")
        if (not candle.projection_fp32 or candle.categorical_feature_indices
                or candle.feature_bottleneck_dim or candle.feature_svd_components
                or candle.extra_continuous_features
                or getattr(candle,"compact_projection_compute_dtype","fp32") != "fp32"
                or candle.causal_feature_window_rms_normalization
                or candle.causal_feature_compression != "none"):
            raise ValueError("sparse rows require immutable direct continuous FP32 input")
        if budget_bytes < 1:
            raise ValueError("sparse cache needs a positive bounded byte budget")
        self.source, self.device, self.budget_bytes = source, torch.device(device), int(budget_bytes)
        if self.device.type == "cuda" and self.device.index is None:
            self.device = torch.device("cuda", torch.cuda.current_device())
        self.layernorm = isinstance(candle.input_norm, nn.LayerNorm)
        self.eps = float(candle.input_norm.eps)
        self.sanitize = candle.sanitize_inputs
        self.normalize = candle.causal_feature_rms_normalization
        self.scale = (candle.causal_feature_rms_scale.detach().float().cpu().numpy().copy()
            if self.normalize else np.ones(source.shape[-1], dtype=np.float32))
        self.active = (candle.causal_feature_active_mask.detach().cpu().numpy().copy()
            if self.normalize else np.ones(source.shape[-1], dtype=np.bool_))
        self.buffer_identity = self._buffer_identity(candle)
        self.source_identity = source.content_fingerprint["sha256"]
        self.entries = OrderedDict()
        self.bytes = 0
        self.hits = self.misses = self.host_payload_bytes = 0
        # The resident representation replaces expanded host cache entries.
        # This releases runtime arrays, never source files or their receipts.
        source._cache.cache.clear()
        source._cache.bytes = 0

    @staticmethod
    def _buffer_identity(candle):
        return tuple((id(value), value._version) if value is not None else None
            for value in (getattr(candle,"causal_feature_rms_scale",None),
                          getattr(candle,"causal_feature_active_mask",None)))

    def validate(self, source, candle):
        if (source.content_fingerprint["sha256"] != self.source_identity
                or candle.candle_query.device != self.device
                or float(candle.input_norm.eps) != self.eps
                or bool(candle.causal_feature_rms_normalization) != self.normalize):
            raise RuntimeError("sparse source/normalizer changed; create a new fold-owned cache")
        identity = self._buffer_identity(candle)
        if identity != self.buffer_identity:
            # DDP broadcasts and best-checkpoint reloads may copy identical
            # buffers. A version change is a prompt for content verification,
            # not evidence that the train-fitted normalization changed.
            scale = (candle.causal_feature_rms_scale.detach().float().cpu().numpy()
                     if self.normalize else self.scale)
            active = (candle.causal_feature_active_mask.detach().cpu().numpy()
                      if self.normalize else self.active)
            if (not np.array_equal(scale.view(np.uint32), self.scale.view(np.uint32))
                    or not np.array_equal(active, self.active)):
                raise RuntimeError("sparse source/normalizer changed; create a new fold-owned cache")
            self.buffer_identity = identity

    def _build(self, block_id):
        from scipy.sparse import csr_matrix
        source = self.source
        block = source.manifest["blocks"][block_id]
        start = int(block["start"])
        count = (int(block["shape"][0]) if "shape" in block else
            (int(source._starts[block_id+1]) if block_id+1 < len(source._starts)
             else source.shape[0]) - start)
        stop = start + count
        symbols, features = source.shape[1:]
        row_parts, column_parts, value_parts = [], [], []

        def add(raw, logical_columns, stock_positions):
            # Source values stay FP32. Select by bits, retaining negative zero
            # and tiny observations; no magnitude threshold is permitted.
            raw = np.asarray(raw, dtype=np.float32)
            logical_columns = np.asarray(logical_columns, dtype=np.int64)
            d, s, k = np.nonzero(raw.view(np.uint32) != 0)
            if not len(d):
                return
            columns = logical_columns[k]
            values = raw[d, s, k].copy()
            if self.sanitize:
                np.nan_to_num(values, copy=False, nan=0., posinf=0., neginf=0.)
            if self.normalize:
                values = (values / self.scale[columns]) * self.active[columns]
            if not np.isfinite(values).all():
                raise ValueError("sparse source preprocessing produced nonfinite inputs")
            row_parts.append((d * symbols + stock_positions[s]).astype(np.int32))
            column_parts.append(columns.astype(np.int32))
            value_parts.append(values)

        base_width = source.base.shape[-1]
        positions = np.arange(symbols, dtype=np.int64)
        add(source.base[start:stop, source.symbol_indices], np.arange(base_width), positions)
        common_start = base_width + len(source.manifest["individual_channels"])
        common = np.broadcast_to(source.common[start:stop, None, :],
            (count, symbols, source.common.shape[-1]))
        add(common, np.arange(common_start, features), positions)
        for cache_id, sstart, send in source._symbol_blocks[block_id]:
            selected = np.flatnonzero((source.symbol_indices >= sstart) & (source.symbol_indices < send))
            if not selected.size:
                continue
            data = source._cache.get(cache_id)
            if isinstance(data, _ColumnBlock):
                raw, columns = data.values, data.columns
            else:
                raw, columns = data, np.arange(data.shape[-1])
            stocks = source.symbol_indices[selected] - sstart
            # Keep a view when the source universe is contiguous.
            if stocks.size and np.all(np.diff(stocks) == 1):
                raw = raw[:, stocks[0]:stocks[-1]+1]
            else:
                raw = raw[:, stocks]
            add(raw, np.asarray(columns, dtype=np.int64)+base_width, selected)
            # Every payload has been copied into this source-only representation.
            # Do not retain a second, much wider CPU column-union copy.
            old = source._cache.cache.pop(cache_id, None)
            if old is not None:
                source._cache.bytes -= old.nbytes
        shape = (count * symbols, features)
        if value_parts:
            matrix = csr_matrix((np.concatenate(value_parts),
                (np.concatenate(row_parts), np.concatenate(column_parts))), shape=shape)
        else:
            matrix = csr_matrix(shape, dtype=np.float32)
        # Rectangles have disjoint coordinates; duplicates would change inputs.
        if matrix.nnz != sum(len(part) for part in value_parts):
            raise ValueError("sparse storage rectangles overlap logical cells")
        del row_parts, column_parts, value_parts
        crow = np.asarray(matrix.indptr, dtype=np.int32)
        columns = np.asarray(matrix.indices, dtype=np.int32)
        values = np.asarray(matrix.data, dtype=np.float32)
        # Prove CSR invariants once on the host; slice offsets preserve them.
        # Explicitly avoid a CUDA scalar synchronization per steady invocation.
        if (crow[0] != 0 or crow[-1] != len(values) or np.any(np.diff(crow) < 0)
                or np.any(columns < 0) or np.any(columns >= features)
                or not matrix.has_canonical_format):
            raise ValueError("invalid normalized source CSR coordinates")
        rows = np.repeat(np.arange(shape[0], dtype=np.int32), np.diff(crow))
        raw64 = values.astype(np.float64)
        if self.layernorm:
            mean = np.bincount(rows, weights=raw64, minlength=shape[0]) / float(features)
            centered = raw64 - mean[rows]
            variance = (np.bincount(rows, weights=centered*centered, minlength=shape[0])
                + (features - np.diff(crow)) * mean*mean) / float(features)
            inverse = 1. / np.sqrt(variance + self.eps)
            prepared64 = raw64 * inverse[rows]
            full_rows = np.diff(crow) == features
            full_entries = full_rows[rows]
            prepared64[full_entries] = centered[full_entries] * inverse[rows[full_entries]]
            prepared = prepared64.astype(np.float32)
            zero_value = (-mean * inverse).astype(np.float32)
            # If every logical column is stored, there is no omitted affine
            # term. Do not amplify tiny sum-schedule rounding by a huge mean
            # on nearly constant full rows.
            zero_value[full_rows] = 0.
            # For every other row, at least one logical zero contributes to
            # variance: |mean / std| <= sqrt(F / omitted_count). Therefore
            # the affine mean correction cannot diverge on a near-constant
            # row. Full rows use explicit centering before their GEMM.
        else:
            variance = np.bincount(rows, weights=raw64*raw64, minlength=shape[0]) / float(features)
            inverse = 1. / np.sqrt(variance + self.eps)
            prepared = (raw64 * inverse[rows]).astype(np.float32)
            zero_value = np.zeros(shape[0], dtype=np.float32)
        required = crow.nbytes + columns.nbytes + prepared.nbytes + zero_value.nbytes
        if required > self.budget_bytes:
            raise MemoryError("one immutable sparse date block exceeds the GPU cache budget")
        while self.entries and self.bytes + required > self.budget_bytes:
            _, old = self.entries.popitem(last=False)
            self.bytes -= old.nbytes

        def upload(array):
            # Evaluation may be the first consumer of a physical date block.
            # Resident constants must remain usable by a later training adjoint.
            with torch.inference_mode(False), torch.no_grad():
                host = torch.from_numpy(array)
                if self.device.type == "cuda":
                    host = host.pin_memory()
                return host.to(self.device, non_blocking=self.device.type == "cuda")

        entry = _SparseBlock(start, stop, symbols, features, crow,
            upload(crow), upload(columns), upload(prepared),
            upload(zero_value))
        self.bytes += entry.nbytes
        self.host_payload_bytes += required
        print(f"[sparse-source] block={block_id} dates={start}:{stop} nnz={len(prepared)} "
              f"logical_cells={shape[0]*features} resident_bytes={self.bytes} "
              f"contract={SPARSE_ROW_CONTRACT}", flush=True)
        return entry

    def slices(self, rows):
        """Chronological source rows, including the canonical repeated tail."""
        rows = np.asarray(rows, dtype=np.int64)
        if rows.ndim != 1 or not rows.size or np.any(rows < 0) or np.any(rows >= self.source.shape[0]):
            raise ValueError("sparse input requires valid nonempty source rows")
        # Split at repeated/reordered dates as well as physical date boundaries.
        blocks = np.searchsorted(self.source._starts, rows, side="right") - 1
        cuts = [0, *(np.flatnonzero((np.diff(rows) != 1) | (np.diff(blocks) != 0))+1), len(rows)]
        for a, b in zip(cuts[:-1], cuts[1:], strict=True):
            block_id = int(blocks[a])
            entry = self.entries.pop(block_id, None)
            if entry is None:
                self.misses += 1
                entry = self._build(block_id)
            else:
                self.hits += 1
            self.entries[block_id] = entry
            yield entry.slice_rows(int(rows[a]), int(rows[b-1])+1)

