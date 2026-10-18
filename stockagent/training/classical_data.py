"""Lossless, bounded tabular access to the canonical causal feature windows.

The physical panel is stored once. Lagged rows are gathered only when consumed;
no feature selection, temporal summaries, price interpolation, or label shift is
performed here. Implicit CSR cells are numeric zero, never missing observations.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import json
import fcntl
import os
from pathlib import Path
import time

import numpy as np
from scipy import sparse

from downloader.artifact_io import atomic_write_json
from stockagent.training.dataset import execution_feature_lag


CLASSICAL_INPUT_CONTRACT = "canonical_full_lag_numeric_zero_csr_v1"


def _sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(8 * 1024**2), b""):
            h.update(b)
    return h.hexdigest()


def _source_csr(source, start, stop):
    """Assemble disjoint verified storage rectangles, without a wide dense slab."""
    rows = np.arange(start, stop, dtype=np.int64)
    symbols, width = source.shape[1:]
    rr, cc, vv = [], [], []

    def add(values, columns, positions):
        values = np.nan_to_num(np.asarray(values, dtype=np.float32),
                               nan=0.0, posinf=0.0, neginf=0.0)
        d, s, f = np.nonzero(values)
        if len(d):
            rr.append(d * symbols + np.asarray(positions)[s])
            cc.append(np.asarray(columns)[f])
            vv.append(values[d, s, f])

    if getattr(source, "_stockagent_factorized_features", False):
        positions = np.arange(symbols)
        base_width = source.base.shape[-1]
        add(source.base[rows[:, None], source.symbol_indices],
            np.arange(base_width), positions)
        common_start = base_width + len(source.manifest["individual_channels"])
        add(np.broadcast_to(source.common[rows, None, :],
                            (len(rows), symbols, source.common.shape[-1])),
            np.arange(common_start, width), positions)
        for selected, positions, columns, values in source.compact_transfer_rectangles(rows):
            # A rectangle may cover only part of this date block.
            values = np.nan_to_num(np.asarray(values, dtype=np.float32),
                                   nan=0.0, posinf=0.0, neginf=0.0)
            d, s, f = np.nonzero(values)
            if len(d):
                rr.append(np.asarray(selected)[d] * symbols + np.asarray(positions)[s])
                cc.append(np.asarray(columns)[f])
                vv.append(values[d, s, f])
    else:
        add(np.asarray(source[start:stop]), np.arange(width), np.arange(symbols))
    shape = ((stop - start) * symbols, width)
    if not vv:
        return sparse.csr_matrix(shape, dtype=np.float32)
    result = sparse.csr_matrix((np.concatenate(vv), (np.concatenate(rr), np.concatenate(cc))),
                               shape=shape)
    if result.nnz != sum(len(v) for v in vv):
        raise ValueError("classical source rectangles overlap")
    result.sort_indices()
    return result


class PhysicalCSRStore:
    """Source-bound mmap blocks shared by all model families and folds."""
    def __init__(self, source, root, *, block_rows=16, cache_bytes=2 * 1024**3):
        from stockagent.data.panel_cache import array_content_fingerprint
        self.source = source
        self.shape = tuple(source.shape)
        identity = (source.content_fingerprint if hasattr(source, "content_fingerprint")
                    else array_content_fingerprint(source))
        self.identity = dict(contract=CLASSICAL_INPUT_CONTRACT, source=identity,
                             block_rows=int(block_rows), sanitize="nonfinite_to_neutral_zero")
        key = hashlib.sha256(json.dumps(self.identity, sort_keys=True).encode()).hexdigest()
        self.root = Path(root) / key
        self.root.mkdir(parents=True, exist_ok=True)
        self.block_rows = int(block_rows)
        if self.block_rows < 1 or cache_bytes < 0:
            raise ValueError("invalid classical cache limits")
        self.cache_bytes = int(cache_bytes)
        self.cache = OrderedDict()
        self.bytes = 0
        self.verified = set()
        self.hits = self.misses = 0

    def block(self, block_id):
        if not 0 <= int(block_id) < (self.shape[0] + self.block_rows - 1) // self.block_rows:
            raise IndexError("physical CSR block outside source")
        if block_id in self.cache:
            self.hits += 1
            self.cache.move_to_end(block_id)
            return self.cache[block_id]
        self.misses += 1
        start = int(block_id) * self.block_rows
        stop = min(start + self.block_rows, self.shape[0])
        folder = self.root / f"block_{block_id:05d}"
        receipt = folder / "complete.json"
        folder.mkdir(exist_ok=True)
        with (folder / "build.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if not receipt.exists():
                self._build_block(folder, receipt, start, stop)
        proof = json.loads(receipt.read_text())
        if proof["identity"] != self.identity or (proof["start"], proof["stop"]) != (start, stop):
            raise ValueError("classical source-cache identity mismatch")
        arrays = []
        for name in ("data", "indices", "indptr"):
            path = folder / f"{name}.npy"
            p = proof["files"][name]
            if block_id not in self.verified and _sha(path) != p["sha256"]:
                raise ValueError(f"corrupt classical source cache: {path}")
            a = np.load(path, mmap_mode="r", allow_pickle=False)
            if str(a.dtype) != p["dtype"] or list(a.shape) != p["shape"]:
                raise ValueError("classical source-cache array ABI mismatch")
            arrays.append(a)
        value = sparse.csr_matrix(tuple(arrays), shape=tuple(proof["shape"]), copy=False)
        if ((block_id not in self.verified and not value.has_canonical_format)
                or value.nnz != proof["nnz"]):
            raise ValueError("invalid classical source CSR")
        value.has_sorted_indices = True
        value.has_canonical_format = True
        self.verified.add(block_id)
        size = sum(a.nbytes for a in arrays)
        while self.cache and self.bytes + size > self.cache_bytes:
            _, old = self.cache.popitem(last=False)
            self.bytes -= old.data.nbytes + old.indices.nbytes + old.indptr.nbytes
        if size <= self.cache_bytes:
            self.cache[block_id] = value
            self.bytes += size
        return value

    def _build_block(self, folder, receipt, start, stop):
            began = time.perf_counter()
            value = _source_csr(self.source, start, stop)
            folder.mkdir(exist_ok=True)
            files = {}
            for name, a in (("indptr", value.indptr), ("indices", value.indices), ("data", value.data)):
                path = folder / f"{name}.npy"
                temporary = folder / f"{name}.{os.getpid()}.tmp.npy"
                np.save(temporary, a, allow_pickle=False)
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                temporary.replace(path)
                files[name] = dict(sha256=_sha(path), dtype=str(a.dtype), shape=list(a.shape))
            atomic_write_json(receipt, dict(identity=self.identity, start=start, stop=stop,
                shape=list(value.shape), nnz=int(value.nnz), files=files,
                build_s=time.perf_counter()-began))
    def rows(self, dates, symbols):
        dates, symbols = np.asarray(dates), np.asarray(symbols)
        if dates.ndim != 1 or dates.shape != symbols.shape:
            raise ValueError("date/symbol row coordinates must be matching vectors")
        if np.any(dates < 0) or np.any(dates >= self.shape[0]):
            raise ValueError("feature-window date escaped source calendar")
        if np.any(symbols < 0) or np.any(symbols >= self.shape[1]):
            raise ValueError("feature-window symbol escaped source universe")
        ids = dates // self.block_rows
        parts, positions = [], []
        for bid in np.unique(ids):
            pos = np.flatnonzero(ids == bid)
            local = (dates[pos] % self.block_rows) * self.shape[1] + symbols[pos]
            parts.append(self.block(int(bid))[local])
            positions.append(pos)
        if not parts:
            return sparse.csr_matrix((0, self.shape[-1]), dtype=np.float32)
        result = sparse.vstack(parts, format="csr")
        order = np.concatenate(positions)
        if np.any(order != np.arange(len(order))):
            result = result[np.argsort(order)]
        return result

    def row_counts(self):
        """Read verified row headers once; admission never expands any window."""
        if hasattr(self, "_row_counts"):
            return self._row_counts
        counts = np.empty(self.shape[:2], dtype=np.int32)
        for bid in range((self.shape[0] + self.block_rows - 1) // self.block_rows):
            folder = self.root / f"block_{bid:05d}"
            receipt = folder / "complete.json"
            if not receipt.exists():
                self.block(bid)
            proof = json.loads(receipt.read_text())
            start, stop = bid*self.block_rows, min((bid+1)*self.block_rows,self.shape[0])
            if proof['identity'] != self.identity or (proof['start'],proof['stop'])!=(start,stop):
                raise ValueError("classical row-count source identity mismatch")
            path=folder/'indptr.npy'
            if _sha(path)!=proof['files']['indptr']['sha256']:
                raise ValueError("corrupt classical CSR row headers")
            ptr=np.load(path,allow_pickle=False)
            delta=np.diff(ptr)
            if (len(ptr)!=(stop-start)*self.shape[1]+1 or ptr[0]!=0
                or ptr[-1]!=proof['nnz'] or np.any(delta<0) or np.any(delta>self.shape[2])):
                raise ValueError("invalid classical CSR row counts")
            counts[start:stop]=delta.reshape(stop-start,self.shape[1])
        self._row_counts=counts
        return counts


@dataclass
class CausalWindowMatrix:
    store: PhysicalCSRStore
    dataset: object
    training: bool = True
    batch_rows: int = 2048

    def __post_init__(self):
        ds = self.dataset
        self.dates = np.asarray(ds.valid_indices, dtype=np.int64)
        self.symbols = int(self.store.shape[1])
        self.lookback = int(ds.lookback)
        self.features = int(self.store.shape[-1])
        labels = ds.future_log_returns_t[self.dates].numpy()
        mask = ds.tradable_mask_t[self.dates].numpy().astype(bool)
        # Financial training uses the physical ledger rather than this
        # per-stock auxiliary target. A supervised baseline can fit only
        # observed labels; never turn an unavailable price/return into zero.
        self.unobserved_tradable_labels = int(np.sum(mask & ~np.isfinite(labels)))
        self.flat = (np.flatnonzero((mask & np.isfinite(labels)).reshape(-1)) if self.training
                     else np.arange(len(self.dates) * self.symbols))
        self.y = np.asarray(labels.reshape(-1)[self.flat], dtype=np.float32)
        self.row_dates = self.dates[self.flat // self.symbols]
        self.row_symbols = self.flat % self.symbols
        self.groups = np.bincount(self.flat // self.symbols, minlength=len(self.dates))
        self.shape = (len(self.flat), self.lookback * self.features)
        self.feature_end = self.row_dates + 1 - execution_feature_lag(ds.execution_mode)
        if not len(self.flat):
            raise ValueError("No tradable samples available for classical training")
        if np.any(self.feature_end < self.lookback):
            raise ValueError("classical lookback has insufficient causal history")

    def rows(self, selection):
        index = (np.arange(*selection.indices(self.shape[0]), dtype=np.int64)
                 if isinstance(selection, slice) else np.asarray(selection))
        index = np.atleast_1d(index)
        # Gather all lag coordinates together: each physical block is visited
        # once rather than once per lag. Consecutive lag rows are then a CSR
        # reshape; the data buffer is reused and only column offsets change.
        dates = self.feature_end[index,None] - self.lookback + np.arange(self.lookback)
        symbols = np.repeat(self.row_symbols[index], self.lookback)
        x = self.store.rows(dates.reshape(-1), symbols)
        lag_offsets = np.tile(np.arange(self.lookback,dtype=x.indices.dtype), len(index))*self.features
        columns = x.indices + np.repeat(lag_offsets,np.diff(x.indptr))
        pointers = np.ascontiguousarray(x.indptr[::self.lookback])
        result = sparse.csr_matrix((x.data,columns,pointers),
            shape=(len(index),self.shape[1]),copy=False)
        result.has_sorted_indices=True
        result.has_canonical_format=True
        return result

    def batches(self):
        for start in range(0, self.shape[0], self.batch_rows):
            stop = min(start + self.batch_rows, self.shape[0])
            yield start, stop, self.rows(slice(start, stop))

    def rank_labels(self, levels=32):
        from scipy.stats import rankdata
        result = np.empty(len(self.y), dtype=np.int32)
        start = 0
        for size in self.groups:
            stop = start + int(size)
            if size:
                result[start:stop] = np.minimum(levels - 1,
                    ((rankdata(self.y[start:stop], method="average") - 1) * levels / size).astype(int))
            start = stop
        return result

    def materialize(self, *, budget_bytes):
        """Two passes and one allocation; never concatenate a second full matrix."""
        counts = np.empty(self.shape[0], dtype=np.int64)
        counts.fill(0)
        physical = self.store.row_counts()
        for lag in range(self.lookback):
            counts += physical[self.feature_end-self.lookback+lag,self.row_symbols]
        indptr = np.empty(self.shape[0] + 1, dtype=np.int64)
        indptr[0] = 0
        np.cumsum(counts, out=indptr[1:])
        nnz = int(indptr[-1])
        needed = nnz * 12 + indptr.nbytes  # scipy may require 64-bit indices
        if needed > int(budget_bytes):
            raise MemoryError(f"full-lag CSR needs {needed} bytes; budget={budget_bytes}; no inputs removed")
        data = np.empty(nnz, dtype=np.float32)
        indices = np.empty(nnz, dtype=np.int64)
        for a, b, x in self.batches():
            data[indptr[a]:indptr[b]] = x.data
            indices[indptr[a]:indptr[b]] = x.indices
        return sparse.csr_matrix((data, indices, indptr), shape=self.shape, copy=False)
