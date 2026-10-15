"""Verified node-local derivatives of immutable normalized CSR observations."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import time
import uuid

import numpy as np

from downloader.artifact_io import atomic_write_json
from stockagent.data.panel_cache import _cache_writer_lock
from stockagent.storage_layout import admit_output

NORMALIZED_CSR_DISK_ABI = "source_and_fold_normalizer_bound_csr_arrays_v1"
_NAMES = ("crow", "columns", "values", "zero_value")


def _sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for part in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(part)
    return digest.hexdigest()


class NormalizedCSRDiskCache:
    def __init__(self, root, identity):
        self.identity = dict(contract=NORMALIZED_CSR_DISK_ABI,
                             numpy_version=np.__version__, **identity)
        digest = hashlib.sha256(json.dumps(self.identity, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        self.root = admit_output(Path(root), "cache") / digest
        self.stats = dict(hits=0, misses=0, writes=0, rejected=0,
                          verified_bytes=0, capacity_skips=0, read_s=0., write_s=0.)

    def _read(self, directory, expected):
        ready = directory / "READY.json"
        if not ready.is_file():
            return None
        manifest = json.loads(ready.read_text())
        if manifest["identity"] != self.identity or manifest["block"] != expected:
            raise ValueError("normalized CSR cache identity changed")
        result = {}
        for name in _NAMES:
            proof = manifest["arrays"][name]
            path = (directory / proof["path"]).resolve(strict=True)
            if not path.is_relative_to(directory.resolve()):
                raise ValueError("normalized CSR cache array escaped its block")
            if path.stat().st_size != proof["size"] or _sha256(path) != proof["sha256"]:
                raise ValueError(f"normalized CSR cache content changed: {name}")
            array = np.load(path, mmap_mode="r", allow_pickle=False)
            if list(array.shape) != proof["shape"] or array.dtype.str != proof["dtype"]:
                raise ValueError(f"normalized CSR cache array ABI changed: {name}")
            self.stats["verified_bytes"] += proof["size"]
            result[name] = array
        crow, columns, values, zero = (result[n] for n in _NAMES)
        rows, features = expected["shape"]
        if (crow.dtype != np.int32 or columns.dtype != np.int32
                or values.dtype != np.float32 or zero.dtype != np.float32
                or crow.shape != (rows + 1,) or zero.shape != (rows,)
                or columns.ndim != 1 or values.shape != columns.shape
                or crow[0] != 0 or crow[-1] != len(values)
                or np.any(np.diff(crow) < 0) or np.any(columns < 0)
                or np.any(columns >= features) or not np.isfinite(values).all()
                or not np.isfinite(zero).all()):
            raise ValueError("normalized CSR cache invariants failed")
        return result

    def load(self, block):
        started = time.perf_counter()
        directory = self.root / f"block_{block['id']:05d}"
        try:
            result = self._read(directory, block)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            # Retain the old generation and its rejection; rebuilding this
            # derivative never deletes a source observation or previous run.
            directory.mkdir(parents=True, exist_ok=True)
            atomic_write_json(directory / f"rejected-{uuid.uuid4().hex}.json",
                              dict(block=block, reason=f"{type(exc).__name__}: {exc}",
                                   observed_ready=(directory/'READY.json').read_text(errors='replace')))
            self.stats["rejected"] += 1
            result = None
        self.stats["hits" if result is not None else "misses"] += 1
        self.stats["read_s"] += time.perf_counter() - started
        return result

    def save(self, block, arrays):
        started = time.perf_counter()
        directory = self.root / f"block_{block['id']:05d}"
        directory.mkdir(parents=True, exist_ok=True)
        with _cache_writer_lock(directory):
            try:
                present = self._read(directory, block)
            except (OSError, ValueError, KeyError, TypeError):
                present = None
            if present is None:
                payload_bytes = sum(a.nbytes for a in arrays.values())
                if shutil.disk_usage(directory).free < payload_bytes + 10 * 1024**3:
                    self.stats["capacity_skips"] += 1
                    self.stats["write_s"] += time.perf_counter() - started
                    return
                generation = directory / f"generation-{uuid.uuid4().hex}"
                generation.mkdir()
                proofs = {}
                for name in _NAMES:
                    array = arrays[name]
                    path = generation / f"{name}.npy"
                    np.save(path, array, allow_pickle=False)
                    proofs[name] = dict(path=str(path.relative_to(directory)),
                        size=path.stat().st_size, sha256=_sha256(path),
                        shape=list(array.shape), dtype=array.dtype.str)
                atomic_write_json(directory / "READY.json", dict(
                    identity=self.identity, block=block, arrays=proofs))
                self.stats["writes"] += 1
        self.stats["write_s"] += time.perf_counter() - started
