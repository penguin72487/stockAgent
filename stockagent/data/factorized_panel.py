"""Bounded, factorized [date, instrument, feature] storage for the usual trainer.

This is a storage adapter, not a sparse-event executor or a different sampler.
Shared features live once at [T,G]; individual features are compressed, immutable
date blocks. Only requested slabs become dense tensors. Raw observations remain
separate nullable tables. Model inputs may carry explicit observation metadata
or use the versioned value-only policy; missing numerical inputs remain neutral
zeros, not fabricated raw observations.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import io
import json
from pathlib import Path
from typing import Any

import numpy as np
import torch

CONTRACT = "tw_factorized_nullable_panel_lag1_v1"
VALUE_ONLY_CONTRACT = "tw_factorized_nullable_panel_values_lag1_v2"
SUPPORTED_CONTRACTS = {CONTRACT, VALUE_ONLY_CONTRACT}


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def write_array_block(path: Path, values: np.ndarray, *, omit_zero_columns: bool = False,
                      logical_columns: np.ndarray | None = None,
                      logical_shape: tuple[int, int, int] | None = None) -> dict:
    """Lossless Float32 storage; never quantize training inputs to BF16."""
    import pyarrow as pa
    path.parent.mkdir(parents=True, exist_ok=True)
    values = np.ascontiguousarray(values, dtype=np.float32)
    serialized = io.BytesIO()
    extra = {}
    shape = tuple(values.shape)
    if logical_columns is not None:
        if not omit_zero_columns or logical_shape is None or values.ndim != 3:
            raise ValueError("compact coordinates require the lossless column codec and logical shape")
        logical_columns = np.asarray(logical_columns)
        if (logical_columns.ndim != 1 or logical_columns.dtype.kind not in "iu"
            or len(logical_columns) != values.shape[-1] or tuple(logical_shape[:2]) != values.shape[:2]
            or len(logical_shape) != 3 or logical_shape[-1] < 0
            or np.any(logical_columns < 0) or np.any(logical_columns >= logical_shape[-1])
            or np.any(np.diff(logical_columns.astype(np.int64)) <= 0)):
            raise ValueError("compact block logical coordinates/shape mismatch")
        shape = tuple(logical_shape)
    elif logical_shape is not None:
        raise ValueError("logical shape requires matching compact column coordinates")
    if omit_zero_columns:
        if values.ndim != 3:
            raise ValueError("column-elided blocks must be [date, symbol, feature]")
        # Preserve every bit, including negative zero. This is a storage
        # codec, not feature selection, a sparse neural model or execution.
        columns = np.flatnonzero(np.any(values.view(np.uint32) != 0, axis=(0, 1))).astype(np.uint32)
        stored = np.ascontiguousarray(values[..., columns])
        if logical_columns is not None:
            columns = logical_columns[columns].astype(np.uint32)
        np.savez(serialized, values=stored, columns=columns)
        extra = {"storage_format": "positive_zero_columns_npz_v1",
            "stored_shape": list(stored.shape), "stored_value_bytes": int(stored.nbytes),
            "stored_column_bytes": int(columns.nbytes)}
    else:
        np.save(serialized, values, allow_pickle=False)
    raw = serialized.getbuffer()
    codec = pa.Codec("zstd", compression_level=3)
    with path.open("xb") as stream:
        stream.write(codec.compress(raw))
    return {"path": path.name, "sha256": file_sha256(path),
            "bytes": path.stat().st_size, "shape": list(shape),
            "uncompressed_bytes": int(np.prod(shape, dtype=np.int64)) * 4, "serialized_bytes": len(raw), **extra}


@dataclass(frozen=True)
class _ColumnBlock:
    values: np.ndarray
    columns: np.ndarray

    @property
    def nbytes(self):
        return self.values.nbytes + self.columns.nbytes


@dataclass
class _BlockCache:
    root: Path
    blocks: list[dict]
    budget_bytes: int

    def __post_init__(self):
        self.cache: OrderedDict[int, np.ndarray | _ColumnBlock] = OrderedDict()
        self.bytes = 0
        self.hits = self.misses = self.bytes_read = 0

    def get(self, index: int) -> np.ndarray | _ColumnBlock:
        import pyarrow as pa
        if index in self.cache:
            self.hits += 1
            self.cache.move_to_end(index)
            return self.cache[index]
        proof = self.blocks[index]
        path = (self.root / proof["path"]).resolve(strict=True)
        if not path.is_relative_to(self.root):
            raise ValueError("factorized block escaped its immutable root")
        compressed = path.read_bytes()
        if hashlib.sha256(compressed).hexdigest() != proof["sha256"]:
            raise ValueError(f"factorized block changed: {path.name}")
        raw = pa.decompress(compressed, proof["serialized_bytes"], codec="zstd")
        loaded = np.load(io.BytesIO(raw.to_pybytes()), allow_pickle=False)
        if proof.get("storage_format") == "positive_zero_columns_npz_v1":
            if not isinstance(loaded, np.lib.npyio.NpzFile) or set(loaded.files) != {"values", "columns"}:
                raise ValueError("column-elided block payload differs from its codec")
            with loaded:
                values, columns = loaded["values"], loaded["columns"]
            if (list(values.shape) != proof["stored_shape"] or values.dtype != np.float32
                    or list(values.shape[:2]) != proof["shape"][:2]
                    or columns.dtype != np.uint32 or columns.ndim != 1
                    or len(columns) != values.shape[-1]
                    or (columns.size and (int(columns[-1]) >= proof["shape"][-1]
                                          or np.any(columns[1:] <= columns[:-1])))):
                raise ValueError("column-elided block shape/dtype/coordinates mismatch")
            columns.setflags(write=False)
            value = _ColumnBlock(values, columns)
        else:
            values = loaded
            if not isinstance(values, np.ndarray) or list(values.shape) != proof["shape"] or values.dtype != np.float32:
                raise ValueError("factorized block shape/dtype mismatch")
            value = values
        if not np.isfinite(values).all():
            raise ValueError("training blocks must be finite; raw missing observations remain NULL")
        values.setflags(write=False)
        self.misses += 1
        self.bytes_read += len(compressed)
        while self.cache and self.bytes + value.nbytes > self.budget_bytes:
            _, old = self.cache.popitem(last=False)
            self.bytes -= old.nbytes
        if value.nbytes <= self.budget_bytes:
            self.cache[index] = value
            self.bytes += value.nbytes
        return value


class FactorizedPanelFeatures:
    """NumPy-shaped view that refuses accidental whole-panel expansion."""
    _stockagent_factorized_features = True
    dtype = np.dtype("float32")
    ndim = 3

    def __init__(self, root: Path, manifest: dict, base: np.ndarray,
                 *, cache_bytes: int = 512 * 1024**2,
                 max_slab_bytes: int = 2 * 1024**3,
                 symbols: np.ndarray | None = None, cache: _BlockCache | None = None,
                 base_identity: dict | None = None, transfer_mode: str = "dense_cpu"):
        if transfer_mode not in {"dense_cpu", "compact_cuda", "compact_cuda_packed", "compact_cuda_cached"}:
            raise ValueError("factorized transfer mode must be dense_cpu, compact_cuda, compact_cuda_packed or compact_cuda_cached")
        self.transfer_mode = transfer_mode
        self.transfer_stats = {"chunks": 0, "logical_dense_bytes": 0, "host_payload_bytes": 0,
            "payload_cache_hits": 0, "payload_cache_admissions": 0, "payload_cache_peak_bytes": 0}
        self.root, self.manifest, self.base = root.resolve(), manifest, base
        self.symbol_indices = (np.arange(base.shape[1], dtype=np.int64)
                               if symbols is None else np.asarray(symbols, dtype=np.int64))
        self.max_slab_bytes = int(max_slab_bytes)
        self.common = np.load(self.root / manifest["common"]["path"], mmap_mode="r", allow_pickle=False)
        flat=[];self._symbol_blocks=[]
        for block in manifest["blocks"]:
            entries=[]
            for part in block.get("symbol_blocks",[{**block,"symbol_start":0}]):
                entries.append((len(flat),part["symbol_start"],part["symbol_start"]+part["shape"][1]))
                flat.append(part)
            self._symbol_blocks.append(entries)
        self._cache = cache or _BlockCache(self.root, flat, int(cache_bytes))
        self._starts = np.asarray([b["start"] for b in manifest["blocks"]], dtype=np.int64)
        self.shape = (int(base.shape[0]), int(len(self.symbol_indices)),
                      int(base.shape[2]) + len(manifest["individual_channels"]) + len(manifest["common_channels"]))
        if self.common.shape != (base.shape[0], len(manifest["common_channels"])):
            raise ValueError("shared feature calendar/schema mismatch")
        from stockagent.data.panel_cache import array_content_fingerprint
        self.base_identity = array_content_fingerprint(base) if base_identity is None else base_identity
        self.root_identity = _digest({"contract":manifest.get("contract",CONTRACT),"manifest":manifest,"base":self.base_identity})
        self.content_fingerprint = {"present": True, "shape": list(self.shape), "dtype": "float32",
            "sha256": _digest({"root":self.root_identity,"symbols":self.symbol_indices.tolist()}),
            "fingerprint_kind": "factorized_source_and_base_contract_not_dense_byte_hash"}

    @property
    def nbytes(self):
        return int(np.prod(self.shape, dtype=np.int64)) * 4

    def __array__(self, dtype=None, copy=None):
        raise TypeError("do not materialize a whole factorized panel; request bounded date slabs")

    def astype(self, dtype, copy=False):
        if np.dtype(dtype) != self.dtype or copy:
            raise ValueError("factorized features keep immutable Float32 storage")
        return self

    def subset_symbols(self, indices: np.ndarray):
        subset=object.__new__(type(self));subset.__dict__=dict(self.__dict__)
        subset.symbol_indices=self.symbol_indices[indices]
        subset.shape=(self.shape[0],len(subset.symbol_indices),self.shape[2])
        subset.content_fingerprint={**self.content_fingerprint,"shape":list(subset.shape),
            "sha256":_digest({"root":self.root_identity,"symbols":subset.symbol_indices.tolist()})}
        return subset

    def __getitem__(self, key):
        keys = key if isinstance(key, tuple) else (key,)
        rows = np.arange(self.shape[0], dtype=np.int64)[keys[0]]
        scalar = np.ndim(rows) == 0
        rows = np.asarray(rows).reshape(-1)
        needed = int(len(rows)) * self.shape[1] * self.shape[2] * 4
        if needed > self.max_slab_bytes:
            raise MemoryError(f"factorized slab needs {needed:,} bytes; bound is {self.max_slab_bytes:,}")
        result = np.zeros((len(rows), self.shape[1], self.shape[2]), dtype=np.float32)
        base_end = self.base.shape[-1]
        asset_end = base_end + len(self.manifest["individual_channels"])
        result[..., :base_end] = self.base[rows[:, None], self.symbol_indices[None, :]]
        if asset_end > base_end and rows.size:
            blocks = np.searchsorted(self._starts, rows, side="right") - 1
            for block_id in np.unique(blocks):
                selected = np.flatnonzero(blocks == block_id)
                offsets = rows[selected] - self._starts[block_id]
                for cache_id,sstart,send in self._symbol_blocks[block_id]:
                    positions=np.flatnonzero((self.symbol_indices>=sstart)&(self.symbol_indices<send))
                    if not positions.size:continue
                    data=self._cache.get(cache_id)
                    source_stocks = self.symbol_indices[positions] - sstart
                    if isinstance(data, _ColumnBlock):
                        result[np.ix_(selected, positions, base_end + data.columns.astype(np.int64))] = data.values[
                            offsets[:, None], source_stocks[None, :]]
                    else:
                        result[selected[:,None],positions[None,:],base_end:asset_end]=data[
                            offsets[:,None],source_stocks[None,:]]
        result[..., asset_end:] = self.common[rows, None, :]
        if scalar:
            result = result[0]
        if len(keys) > 1:
            result = result[(slice(None), *keys[1:])] if not scalar else result[keys[1:]]
        return result

    def as_torch(self):
        return FactorizedFeatureTensor(self)

    def compact_statistics_rows(self, rows):
        """Skip proven positive-zero columns in statistical reductions only.

        The model still receives its complete dense feature schema. PCA keeps
        the original logical observation count; omitted zeros contribute zero
        to its sums/products. RMS retains the full alive-cell denominator.
        """
        rows = np.asarray(rows, dtype=np.int64).reshape(-1)
        block_ids = np.searchsorted(self._starts, rows, side="right") - 1
        rectangles = []
        pieces = []
        individual_width = len(self.manifest["individual_channels"])
        for block_id in np.unique(block_ids):
            selected = np.flatnonzero(block_ids == block_id)
            offsets = rows[selected] - self._starts[block_id]
            for cache_id, sstart, send in self._symbol_blocks[block_id]:
                positions = np.flatnonzero((self.symbol_indices >= sstart) & (self.symbol_indices < send))
                if not positions.size:
                    continue
                data = self._cache.get(cache_id)
                columns = data.columns if isinstance(data, _ColumnBlock) else np.arange(individual_width)
                pieces.append(columns)
                rectangles.append((selected, offsets, positions, self.symbol_indices[positions] - sstart, data, columns))
        individual_columns = np.unique(np.concatenate(pieces)).astype(np.int64) if pieces else np.empty(0, dtype=np.int64)
        base_width = self.base.shape[-1]
        common_width = self.common.shape[-1]
        logical_columns = np.concatenate([np.arange(base_width), base_width + individual_columns,
            base_width + individual_width + np.arange(common_width)])
        needed = len(rows) * len(self.symbol_indices) * len(logical_columns) * 4
        if needed > self.max_slab_bytes:
            raise MemoryError("compact statistics slab exceeds the bounded host budget")
        result = np.zeros((len(rows), len(self.symbol_indices), len(logical_columns)), dtype=np.float32)
        result[..., :base_width] = self.base[rows[:, None], self.symbol_indices[None, :]]
        for selected, offsets, positions, stocks, data, columns in rectangles:
            values = data.values if isinstance(data, _ColumnBlock) else data
            output_columns = base_width + np.searchsorted(individual_columns, columns)
            result[np.ix_(selected, positions, output_columns)] = values[offsets[:, None], stocks[None, :]]
        if common_width:
            result[..., -common_width:] = self.common[rows, None, :]
        return logical_columns, result

    def compact_transfer_rectangles(self, rows):
        """Lossless storage rectangles, without a host-wide column union.

        This is transport, not sparsified model computation. Each rectangle
        owns distinct output coordinates; omitted columns are proven positive
        zeros by the verified codec. The caller reconstructs all channels.
        Contiguous source ranges remain views until the bounded pinned copy.
        """
        rows=np.asarray(rows,dtype=np.int64).reshape(-1)
        block_ids=np.searchsorted(self._starts,rows,side="right")-1
        for block_id in np.unique(block_ids):
            selected=np.flatnonzero(block_ids==block_id)
            offsets=rows[selected]-self._starts[block_id]
            for cache_id,sstart,send in self._symbol_blocks[block_id]:
                positions=np.flatnonzero((self.symbol_indices>=sstart)&(self.symbol_indices<send))
                if not positions.size:continue
                data=self._cache.get(cache_id)
                values=data.values if isinstance(data,_ColumnBlock) else data
                columns=data.columns if isinstance(data,_ColumnBlock) else np.arange(values.shape[-1])
                stocks=self.symbol_indices[positions]-sstart
                if not columns.size:continue
                if (np.all(np.diff(offsets)==1) and np.all(np.diff(stocks)==1)):
                    payload=values[offsets[0]:offsets[-1]+1,stocks[0]:stocks[-1]+1]
                else:payload=values[offsets[:,None],stocks[None,:]]
                yield selected,positions,np.asarray(columns,dtype=np.int64)+self.base.shape[-1],payload

    def iter_reduction_column_slabs(self, row_indices, *, symbol_rows=128):
        indices = np.asarray(row_indices, dtype=np.int64)
        block_ids = np.searchsorted(self._starts, indices, side="right") - 1
        for block in np.unique(block_ids):
            selected = indices[block_ids == block]
            for symbol_start in range(0, self.shape[1], symbol_rows):
                stop = min(self.shape[1], symbol_start + symbol_rows)
                subset = self.subset_symbols(np.arange(symbol_start, stop))
                columns, values = subset.compact_statistics_rows(selected)
                yield selected, slice(symbol_start, stop), columns, values

    def iter_reduction_slabs(self,row_indices,*,symbol_rows=128,date_rows=32):
        """Read each bounded physical rectangle, not an enormous dense cube."""
        indices=np.asarray(row_indices,dtype=np.int64)
        block_ids=np.searchsorted(self._starts,indices,side="right")-1
        for block in np.unique(block_ids):
            selected=indices[block_ids==block]
            for symbol_start in range(0,self.shape[1],symbol_rows):
                stop=min(self.shape[1],symbol_start+symbol_rows)
                yield selected,slice(symbol_start,stop),self.subset_symbols(np.arange(symbol_start,stop))[selected]


class FactorizedFeatureTensor:
    """CPU slab-source protocol consumed by canonical WindowedSplitTensors.

    This object stays outside torch.compile. Every model invocation still gets
    an ordinary contiguous torch.Tensor with the original model/loss ABI.
    """
    _stockagent_factorized_features = True
    device = torch.device("cpu")
    dtype = torch.float32

    def __init__(self, source: FactorizedPanelFeatures, *, pin_slabs=False):
        self.source, self.pin_slabs = source, pin_slabs

    @property
    def shape(self):
        return torch.Size(self.source.shape)

    @property
    def ndim(self):
        return 3

    def size(self, dim=None):
        return self.shape if dim is None else self.shape[dim]

    def dim(self):
        return 3

    def numel(self):
        return int(np.prod(self.shape, dtype=np.int64))

    def element_size(self):
        return 4

    def contiguous(self):
        return self

    def is_pinned(self):
        # Only bounded returned slabs are pinned; never lock the whole panel.
        return self.pin_slabs

    def pin_memory(self):
        return FactorizedFeatureTensor(self.source, pin_slabs=torch.cuda.is_available())

    def to(self, device=None, non_blocking=False, dtype=None, **kwargs):
        if (device is not None and torch.device(device).type != "cpu") or dtype not in (None, torch.float32):
            raise ValueError("factorized bases stay Float32 on CPU; transfer returned slabs only")
        return self

    def __getitem__(self, key):
        def cpu_index(item):
            return item.detach().cpu().numpy() if isinstance(item, torch.Tensor) else item
        key = tuple(cpu_index(x) for x in key) if isinstance(key, tuple) else cpu_index(key)
        value = torch.from_numpy(np.ascontiguousarray(self.source[key]))
        return value.pin_memory() if self.pin_slabs else value

    def narrow(self, dim, start, length):
        if dim != 0:
            raise ValueError("use subset_symbols for a factorized symbol selection")
        if start < 0 or length < 0 or start + length > self.shape[0]:
            raise IndexError("factorized date slab out of range")
        return self[start:start + length]

    def slab(self,start,length):
        return FactorizedPanelSlab(self.source,np.arange(start,start+length),pin_slabs=self.pin_slabs)

    def index_select(self, dim, index):
        if dim == 1:
            return FactorizedFeatureTensor(self.source.subset_symbols(index.detach().cpu().numpy()),
                                          pin_slabs=self.pin_slabs)
        if dim == 0:
            return self[index]
        raise ValueError("unsupported factorized feature selection")

    def new_empty(self, shape):
        return torch.empty(shape, dtype=self.dtype)


def _pack_transfer_payloads(values, *, pin_memory: bool, budget_bytes: int):
    """Pack exact value/coordinate bytes into one bounded staging allocation.

    Align every member to eight bytes so CUDA dtype views are valid. Nothing
    aliases an immutable source array. Padding is transport-only, never a model
    channel. Returning ordinary tensors keeps this operation outside compile.
    """
    layout=[]
    packet_bytes=0
    for value in values:
        if not isinstance(value,np.ndarray) or value.dtype not in (np.dtype("float32"),np.dtype("int64")):
            raise ValueError("compact transport requires Float32 values or Int64 coordinates")
        packet_bytes=(packet_bytes+7)//8*8
        dtype=torch.float32 if value.dtype==np.float32 else torch.int64
        layout.append((packet_bytes,int(value.nbytes),tuple(value.shape),dtype))
        packet_bytes+=int(value.nbytes)
    if packet_bytes>budget_bytes:
        raise MemoryError("factorized packet exceeds the bounded staging budget")
    packet=torch.empty(packet_bytes,dtype=torch.uint8,pin_memory=pin_memory)
    destination=packet.numpy()
    for value,(offset,_,shape,_) in zip(values,layout):
        view=np.ndarray(shape,dtype=value.dtype,buffer=destination,offset=offset)
        np.copyto(view,value,casting="no")
    return packet,layout


def _transfer_packet_views(packet, layout):
    return [packet.narrow(0,offset,length).view(dtype).view(shape)
            for offset,length,shape,dtype in layout]


def _reconstruct_transfer_packet(result, packet, layout, queued, base_width, common_start):
    """Reconstruct the same full-width FP32 tensor from immutable CUDA bytes."""
    if packet.device.type == "cuda":
        # Checkpoint recomputation may execute on a different autograd stream.
        packet.record_stream(torch.cuda.current_stream(packet.device))
    views = _transfer_packet_views(packet, layout)
    for operation, *arguments in queued:
        if operation == "base":
            result[..., :base_width].copy_(views[arguments[0]])
        elif operation == "common":
            result[..., common_start:].copy_(views[arguments[0]][:, None, :])
        elif operation == "rectangle":
            rows, stocks, payload, indices = arguments
            result[rows[0]:rows[-1]+1, stocks[0]:stocks[-1]+1].index_copy_(
                -1, views[indices], views[payload])
        else:
            rows, stocks, payload, indices = arguments
            result.index_put_((views[rows][:, None, None], views[stocks][None, :, None],
                views[indices][None, None, :]), views[payload])


class FactorizedPanelSlab:
    """Lazy slab passed at the eager I/O boundary, never inside compiled math."""
    _stockagent_factorized_slab=True
    dtype=torch.float32
    def __init__(self,source,rows,*,pin_slabs=False,device=None):
        self.source,self.rows,self.pin_slabs=source,np.asarray(rows,dtype=np.int64),pin_slabs
        self.device=torch.device("cpu") if device is None else torch.device(device)
        # One forward/checkpoint lifetime only: no stale observations across
        # batches, devices, optimizer updates or folds. Store compact bytes,
        # not the much larger dense input or learned activations.
        self._gpu_packets = {}
        self._gpu_packet_bytes = 0
        self._gpu_packet_budget = None
        self._retain_gpu_packets = True

    def _remember_gpu_packet(self, key, packet, layout, queued):
        if self._gpu_packet_budget is None:
            free, _ = torch.cuda.mem_get_info(self.device)
            self._gpu_packet_budget = min(self.source.max_slab_bytes, 2 * 1024**3, free // 8)
        size = packet.numel() * packet.element_size()
        if key in self._gpu_packets or self._gpu_packet_bytes + size > self._gpu_packet_budget:
            return False
        self._gpu_packets[key] = (packet, layout, queued)
        self._gpu_packet_bytes += size
        stats = self.source.transfer_stats
        stats["payload_cache_admissions"] += 1
        stats["payload_cache_peak_bytes"] = max(stats["payload_cache_peak_bytes"], self._gpu_packet_bytes)
        return True
    @property
    def shape(self):return torch.Size((len(self.rows),*self.source.shape[1:]))
    def size(self,dim=None):return self.shape if dim is None else self.shape[dim]
    def dim(self):return 3
    def to(self,device=None,**kwargs):
        return FactorizedPanelSlab(self.source,self.rows,pin_slabs=self.pin_slabs,device=device or self.device)
    def pad_end(self,count):
        return FactorizedPanelSlab(self.source,np.concatenate([self.rows,np.repeat(self.rows[-1],count)]),
                                  pin_slabs=self.pin_slabs,device=self.device)
    def stock_chunk(self,start,end):
        subset=self.source.subset_symbols(np.arange(start,end))
        logical_bytes=len(self.rows)*(end-start)*self.source.shape[2]*4
        if logical_bytes>self.source.max_slab_bytes:
            raise MemoryError("factorized transfer exceeds the bounded dense slab budget")
        if self.source.transfer_mode in {"compact_cuda","compact_cuda_packed","compact_cuda_cached"} and self.device.type=="cuda":
            # Storage omits only proven positive-zero columns, never a feature
            # selected from validation/test or a small-but-nonzero value.
            # Transfer this lossless representation, then reconstruct the same
            # contiguous full-width Float32 tensor before compiled model math.
            result=torch.zeros((len(self.rows),end-start,self.source.shape[2]),
                dtype=torch.float32,device=self.device)
            payload_bytes=0
            packed=self.source.transfer_mode in {"compact_cuda_packed", "compact_cuda_cached"}
            queued=[]
            if packed:
                base_width=subset.base.shape[-1]
                common_start=base_width+len(subset.manifest["individual_channels"])
                key = (start, end)
                cached = self._gpu_packets.get(key) if self.source.transfer_mode == "compact_cuda_cached" else None
                if cached is not None:
                    packet, layout, queued = cached
                    _reconstruct_transfer_packet(result, packet, layout, queued, base_width, common_start)
                    self.source.transfer_stats["chunks"] += 1
                    self.source.transfer_stats["logical_dense_bytes"] += logical_bytes
                    self.source.transfer_stats["payload_cache_hits"] += 1
                    return result
                values=[]
                def queue(value):
                    values.append(value)
                    return len(values)-1
                if base_width:
                    queued.append(("base",queue(subset.base[
                        self.rows[:,None],subset.symbol_indices[None,:]])))
                if subset.common.shape[-1]:
                    queued.append(("common",queue(subset.common[self.rows])))
                for rows,stocks,columns,value in subset.compact_transfer_rectangles(self.rows):
                    payload,indices=queue(value),queue(columns)
                    if np.all(np.diff(rows)==1) and np.all(np.diff(stocks)==1):
                        queued.append(("rectangle",rows,stocks,payload,indices))
                    else:
                        queued.append(("indexed",queue(rows),queue(stocks),payload,indices))
                packet,layout=_pack_transfer_payloads(values,pin_memory=self.pin_slabs,
                    budget_bytes=self.source.max_slab_bytes)
                payload_bytes=packet.numel()
                packet=packet.to(device=self.device,non_blocking=self.pin_slabs)
                if (self.source.transfer_mode == "compact_cuda_cached"
                        and self._retain_gpu_packets and torch.is_grad_enabled()):
                    self._remember_gpu_packet(key, packet, layout, queued)
                _reconstruct_transfer_packet(result, packet, layout, queued, base_width, common_start)
                self.source.transfer_stats["chunks"]+=1
                self.source.transfer_stats["logical_dense_bytes"]+=logical_bytes
                self.source.transfer_stats["host_payload_bytes"]+=payload_bytes
                return result
            def transfer(value):
                nonlocal payload_bytes
                # Copy once, straight into the bounded pinned allocation.
                # Do not alias immutable/read-only codec buffers or create an
                # intermediate contiguous array followed by a second copy.
                dtype=torch.float32 if value.dtype==np.float32 else torch.int64
                if value.dtype not in (np.dtype("float32"),np.dtype("int64")):
                    raise ValueError("compact transport requires Float32 values or Int64 coordinates")
                tensor=torch.empty(value.shape,dtype=dtype,pin_memory=self.pin_slabs)
                np.copyto(tensor.numpy(),value,casting="no")
                payload_bytes+=tensor.numel()*tensor.element_size()
                return tensor.to(device=self.device,non_blocking=self.pin_slabs)
            base_width=subset.base.shape[-1]
            if base_width:
                result[...,:base_width].copy_(transfer(subset.base[
                    self.rows[:,None],subset.symbol_indices[None,:]]))
            common_start=base_width+len(subset.manifest["individual_channels"])
            if subset.common.shape[-1]:
                result[...,common_start:].copy_(transfer(subset.common[self.rows])[:,None,:])
            for rows,stocks,columns,value in subset.compact_transfer_rectangles(self.rows):
                tensor=transfer(value);indices=transfer(columns)
                if np.all(np.diff(rows)==1) and np.all(np.diff(stocks)==1):
                    result[rows[0]:rows[-1]+1,stocks[0]:stocks[-1]+1].index_copy_(-1,indices,tensor)
                else:
                    row_indices=transfer(rows);stock_indices=transfer(stocks)
                    result.index_put_((row_indices[:,None,None],stock_indices[None,:,None],indices[None,None,:]),tensor)
            self.source.transfer_stats["chunks"]+=1
            self.source.transfer_stats["logical_dense_bytes"]+=logical_bytes
            self.source.transfer_stats["host_payload_bytes"]+=payload_bytes
            return result
        value=subset[self.rows]
        tensor=torch.from_numpy(np.ascontiguousarray(value))
        if self.pin_slabs and torch.cuda.is_available():tensor=tensor.pin_memory()
        return tensor.to(device=self.device,non_blocking=self.pin_slabs)


def factorized_member_proofs(manifest: dict) -> list[dict]:
    """One member inventory shared by attachment and incremental rebuilds."""
    proofs=[manifest["common"]]
    if manifest.get("feature_dictionary_sha256"):
        proofs.append({"path":"feature_dictionary.json","sha256":manifest["feature_dictionary_sha256"]})
    proofs.extend(manifest.get("observations",[]))
    proofs.extend(manifest.get("annual_events",[]))
    for block in manifest["blocks"]:proofs.extend(block.get("symbol_blocks",[block]))
    return proofs


def verify_factorized_members(root: Path, manifest: dict) -> None:
    """Verify every immutable model/raw member without expanding its arrays."""
    root=root.resolve(strict=True)
    proofs=factorized_member_proofs(manifest)
    for proof in proofs:
        member = (root / proof["path"]).resolve(strict=True)
        if not member.is_relative_to(root) or file_sha256(member) != proof["sha256"]:
            raise ValueError("factorized member identity mismatch")


def attach_factorized_features(panel, manifest_path: str | Path, *, transfer_mode: str = "dense_cpu"):
    """Attach model-only storage after the ordinary price/rule cache load."""
    path = Path(manifest_path).resolve(strict=True)
    manifest = json.loads(path.read_text())
    if manifest.get("contract") not in SUPPORTED_CONTRACTS or manifest.get("status") != "complete":
        raise ValueError("factorized panel contract/build is not complete")
    if manifest.get("contract") == VALUE_ONLY_CONTRACT:
        if manifest.get("model_channel_policy") != "value_only":
            raise ValueError("value-only factorized panel requires its explicit channel policy")
        quantity_count = len(manifest["individual_channels"]) + len(manifest["common_channels"])
        if (quantity_count != manifest["value_features"]
                or manifest["logical_model_channels"] != len(manifest["base_feature_names"]) + quantity_count):
            raise ValueError("value-only factorized channel count differs from its ABI")
    if (manifest.get("historical_point_in_time") is not False
            or manifest.get("research_only") is not True
            or manifest.get("feature_lag") != 1):
        raise ValueError("factorized research/lag contract mismatch")
    if manifest["symbols"] != panel.symbols or manifest["dates"] != [str(d)[:10] for d in panel.dates]:
        raise ValueError("factorized feature universe/calendar differs from executable panel")
    if manifest["base_feature_names"] != panel.feature_names:
        raise ValueError("factorized base features differ from the declared base ABI")
    root = path.parent
    verify_factorized_members(root,manifest)
    names = [*panel.feature_names, *manifest["individual_channels"], *manifest["common_channels"]]
    if len(names) != len(set(names)):
        raise ValueError("factorized logical features are not unique")
    rows = 0
    for block in manifest["blocks"]:
        if block["start"] != rows or block["shape"][1:] != [panel.num_symbols, len(manifest["individual_channels"])]:
            raise ValueError("factorized blocks are not a complete aligned partition")
        rows += block["shape"][0]
        if "symbol_blocks" in block:
            width=0
            for part in block["symbol_blocks"]:
                if part["symbol_start"]!=width or part["shape"][0]!=block["shape"][0] or part["shape"][2]!=len(manifest["individual_channels"]):
                    raise ValueError("factorized symbol blocks are not a complete aligned partition")
                width+=part["shape"][1]
            if width!=panel.num_symbols:raise ValueError("factorized symbol blocks omit securities")
    if rows != panel.num_dates:
        raise ValueError("factorized blocks do not cover the full executable calendar")
    features = FactorizedPanelFeatures(root, manifest, panel.features,
        cache_bytes=int(manifest.get("host_cache_bytes", 512 * 1024**2)),
        max_slab_bytes=int(manifest.get("max_slab_bytes",2*1024**3)),transfer_mode=transfer_mode)
    panel.features, panel.feature_names = features, names
    panel.content_fingerprints = {**(panel.content_fingerprints or {}), "features": features.content_fingerprint}
    # build_panel prints the executable base columns before this adapter runs.
    # Report the actual input width here, after all attachment checks succeed,
    # without dumping thousands of names or expanding the bounded storage.
    print(
        f"[panel-model] features ({len(names)}): "
        f"base={len(manifest['base_feature_names'])}, "
        f"individual={len(manifest['individual_channels'])}, "
        f"common={len(manifest['common_channels'])}; "
        f"factorized attachment complete, transfer={transfer_mode}, "
        f"research_only=True, historical_point_in_time=False",
        flush=True,
    )
    return panel
