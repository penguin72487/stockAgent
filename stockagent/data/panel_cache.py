from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from typing import Any
import uuid

import numpy as np

from stockagent.data import panel_chunks
from stockagent.data_sync.immutable_cache_files import (
    link_immutable_payload, payload_info as _info, stable_payload_hash as _stable_hash,
)


PANEL_CACHE_V2_DIRNAME = "panel_cache_v2"
PANEL_CACHE_V2_VARIANTS_DIRNAME = "variants"
REQUIRED_ARRAY_NAMES = (
    "dates",
    "features",
    "returns_1d",
    "tradable_mask",
    "can_buy_mask",
    "can_sell_mask",
    "can_short_open_mask",
    "force_short_cover_mask",
    "force_exit_mask",
    "alive_mask",
    "benchmark_returns",
    "close_prices",
    "daily_volumes",
)
OPTIONAL_ARRAY_NAMES = (
    "can_short_open_open_mask",
    "short_capacity_shares",
    "short_margin_rate",
    "open_prices",
    "intraday_returns",
    "day_trade_eligible_mask",
    "day_trade_can_short_open_mask",
    "day_trade_can_buy_open_mask",
    "day_trade_can_sell_open_mask",
    "raw_close_returns_1d",
    "corporate_action_avoidance_mask",
    "unresolved_corporate_action_mask",
    "cash_dividend_yield",
    "cash_dividend_payment_delay_sessions",
)
ARRAY_NAMES = (*REQUIRED_ARRAY_NAMES, *OPTIONAL_ARRAY_NAMES)


def array_content_fingerprint(value: np.ndarray | None) -> dict[str, Any]:
    """Hash complete logical C-order content without allocating an array copy."""

    if value is None:
        return {"present": False}
    array = np.asarray(value)
    digest = hashlib.sha256()
    header = {
        "present": True,
        "shape": [int(size) for size in array.shape],
        "dtype": str(array.dtype),
    }
    digest.update(
        json.dumps(header, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    if array.dtype.hasobject:
        for item in array.flat:
            encoded = json.dumps(item, ensure_ascii=False, default=str).encode(
                "utf-8"
            )
            digest.update(
                len(encoded).to_bytes(8, byteorder="little", signed=False)
            )
            digest.update(encoded)
    elif array.flags.c_contiguous:
        digest.update(memoryview(array.view(np.uint8).reshape(-1)))
    else:
        iterator = np.nditer(
            array,
            flags=["external_loop", "buffered", "zerosize_ok"],
            op_flags=["readonly"],
            order="C",
            buffersize=1 << 20,
        )
        for chunk in iterator:
            contiguous = np.ascontiguousarray(chunk)
            digest.update(memoryview(contiguous.view(np.uint8).reshape(-1)))
    header["sha256"] = digest.hexdigest()
    return header


def panel_cache_v2_dir(parquet_root: str | Path) -> Path:
    return Path(parquet_root) / PANEL_CACHE_V2_DIRNAME


def legacy_panel_cache_path(parquet_root: str | Path) -> Path:
    return Path(parquet_root) / "panel_cache.npz"


def legacy_panel_meta_path(parquet_root: str | Path) -> Path:
    return Path(parquet_root) / ".panel_meta.pkl"


def panel_cache_v2_meta_path(parquet_root: str | Path) -> Path:
    return panel_cache_v2_dir(parquet_root) / "meta.json"


def _panel_cache_v2_variant_id(
    *,
    source_hash: str,
    backend_key: str,
    version: int,
) -> str:
    """Return the content-contract key for one immutable panel variant."""

    payload = json.dumps(
        {
            "version": int(version),
            "source_hash": str(source_hash),
            "backend_key": str(backend_key),
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def panel_cache_v2_variant_meta_path(
    parquet_root: str | Path,
    *,
    source_hash: str,
    backend_key: str,
    version: int,
) -> Path:
    variant_id = _panel_cache_v2_variant_id(
        source_hash=source_hash,
        backend_key=backend_key,
        version=version,
    )
    return (
        panel_cache_v2_dir(parquet_root)
        / PANEL_CACHE_V2_VARIANTS_DIRNAME
        / f"{variant_id}.json"
    )


def _array_file(cache_dir: Path, name: str) -> Path:
    return cache_dir / f"{name}.npy"


def _json_file(cache_dir: Path, name: str) -> Path:
    return cache_dir / f"{name}.json"


def _write_json(path: Path, payload: Any, *, directory_commit_pending: bool = False) -> None:
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.replace(path)
        if not directory_commit_pending:
            _fsync_directory(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _fsync_directory(path: Path, *, required: bool = False) -> None:
    """Sync directory entries; a pre-publication commit must propagate failure."""

    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        if required:
            raise
        return
    try:
        os.fsync(fd)
    except OSError:
        if required:
            raise
    finally:
        os.close(fd)


@contextmanager
def _cache_writer_lock(cache_dir: Path):
    """Serialize generation commit/cleanup across training and live processes."""

    lock_path = cache_dir / ".write.lock"
    with lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _save_array(cache_dir: Path, name: str, array: np.ndarray, *, reuse=None,
                directory_commit_pending: bool = False) -> dict[str, Any]:
    path = _array_file(cache_dir, name)
    arr = np.asarray(array)
    fingerprint = array_content_fingerprint(arr)
    metadata = {"file": path.name, "shape": list(arr.shape), "dtype": str(arr.dtype),
                "content_fingerprint": fingerprint}
    if reuse is not None and reuse.allow_chunks and panel_chunks.can_chunk(arr):
        descriptor = panel_chunks.write_chunks(
            cache_dir, name, arr, fingerprint, reuse.chunk_paths, reuse.chunk_counters,
        )
        descriptor_path = cache_dir / f"{name}.chunks.json"
        _write_json(descriptor_path, descriptor, directory_commit_pending=directory_commit_pending)
        descriptor_path.chmod(0o400)
        with descriptor_path.open("rb") as stream:
            os.fsync(stream.fileno())
        metadata.update(file=descriptor_path.name, storage=panel_chunks.CHUNK_STORAGE)
        return metadata
    if reuse is not None and reuse.link(path, fingerprint):
        return metadata
    tmp = cache_dir / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        mmap = np.lib.format.open_memmap(
            tmp,
            mode="w+",
            dtype=arr.dtype,
            shape=arr.shape,
        )
        mmap[...] = arr
        mmap.flush()
        del mmap
        tmp.chmod(0o400)  # Immutable-by-caller; every later writer atomic-replaces.
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        tmp.replace(path)
        if not directory_commit_pending:
            _fsync_directory(cache_dir)
    finally:
        tmp.unlink(missing_ok=True)
    if reuse is not None:
        reuse.add(path, fingerprint, written=True)
    return metadata


class _ArrayReuse:
    """Current writer's index of committed arrays; no separate object store."""

    def __init__(self, cache_dir: Path, version: int, *, allow_chunks: bool = True):
        self.allow_chunks = allow_chunks
        self.paths: dict[str, list[Path]] = {}
        self.chunk_paths: dict[tuple[str, int], list[Path]] = {}
        self.chunk_counters = dict.fromkeys((
            "written_chunks", "reused_chunks", "written_chunk_bytes", "avoided_chunk_bytes",
        ), 0)
        self.reused = self.written = self.avoided_bytes = self.written_bytes = 0
        cache_dir = cache_dir.resolve()
        manifests = [cache_dir / "meta.json", *sorted((cache_dir / "variants").glob("*.json"))]
        for manifest in manifests:
            try:
                meta = _read_json(manifest)
                generation = meta.get("generation", "")
                if meta.get("version") != version or not re.fullmatch(r"[0-9a-f]{32}", generation):
                    continue
                for name, item in meta.get("arrays", {}).items():
                    if name not in ARRAY_NAMES or not isinstance(item, dict):
                        continue
                    if item.get("storage") == panel_chunks.CHUNK_STORAGE:
                        if item.get("file") == f"generations/{generation}/{name}.chunks.json":
                            panel_chunks.add_donors(cache_dir, item, self.chunk_paths)
                        continue
                    relative = f"generations/{generation}/{name}.npy"
                    path = cache_dir / relative
                    if item.get("file") != relative or path.resolve() != path:
                        continue
                    fingerprint = item.get("content_fingerprint")
                    if isinstance(fingerprint, dict) and fingerprint.get("present") is True:
                        self.add(path, fingerprint)
            except (OSError, ValueError, TypeError, AttributeError):
                continue

    @staticmethod
    def key(fingerprint: dict) -> str:
        return json.dumps(fingerprint, sort_keys=True, separators=(",", ":"), allow_nan=False)

    def add(self, path: Path, fingerprint: dict, *, written=False):
        paths = self.paths.setdefault(self.key(fingerprint), [])
        if path not in paths:
            paths.append(path)
        if written:
            self.written += 1
            self.written_bytes += path.stat().st_size

    def link(self, destination: Path, fingerprint: dict) -> bool:
        for path in list(self.paths.get(self.key(fingerprint), ())):
            try:
                before = _info(path)
                if before["mode"] & 0o222 or before["xattrs"]:
                    continue  # Do not chmod a legacy writable/external producer.
                array = np.load(path, mmap_mode="r", allow_pickle=False)
                actual = array_content_fingerprint(array)
                del array
                if actual != fingerprint or _info(path) != before:
                    continue
                sha256 = _stable_hash(path, before)
                if link_immutable_payload(path, destination, sha256=sha256, size=before["size"]):
                    self.reused += 1
                    self.avoided_bytes += before["size"]
                    self.add(destination, fingerprint)
                    return True
            except (FileNotFoundError, ValueError):
                continue
        return False


def save_panel_cache_v2(
    parquet_root: str | Path,
    panel_like: Any,
    *,
    source_hash: str,
    backend_key: str,
    version: int,
    chunk_storage: bool = True,
) -> Path:
    from stockagent.data_sync.cache_capacity import reserve_cache_write

    cache_dir = panel_cache_v2_dir(parquet_root)
    cache_dir.mkdir(parents=True, exist_ok=True)
    # Conservative serialization bound, including generated fallback masks,
    # headers/chunk manifests and metadata. Immutable reuse remains unchanged;
    # admission never evicts an accepted generation to make a new one fit.
    def maximum_growth():
        names = ("dates", "features", "returns_1d", "tradable_mask", "can_buy_mask",
                 "can_sell_mask", "can_short_open_mask", "force_short_cover_mask",
                 "force_exit_mask", "alive_mask", "benchmark_returns", "close_prices",
                 "daily_volumes", *OPTIONAL_ARRAY_NAMES)
        growth = sum(np.asarray(value).nbytes for name in set(names)
                     if (value := getattr(panel_like, name, None)) is not None)
        growth += np.asarray(panel_like.tradable_mask).size * 8
        return growth + np.asarray(panel_like.close_prices).size * 8 + 8 * 1024**2
    with _cache_writer_lock(cache_dir), reserve_cache_write(
            cache_dir, key=f"{source_hash}:{backend_key}:{version}",
            maximum_growth_bytes=maximum_growth, family="panel"):
        return _save_panel_cache_v2_locked(
            parquet_root,
            panel_like,
            source_hash=source_hash,
            backend_key=backend_key,
            version=version,
            chunk_storage=chunk_storage,
        )


def _save_panel_cache_v2_locked(
    parquet_root: str | Path,
    panel_like: Any,
    *,
    source_hash: str,
    backend_key: str,
    version: int,
    chunk_storage: bool = True,
) -> Path:
    cache_dir = panel_cache_v2_dir(parquet_root)
    cache_dir.mkdir(parents=True, exist_ok=True)
    generations_dir = cache_dir / "generations"
    generations_dir.mkdir(parents=True, exist_ok=True)
    generation = uuid.uuid4().hex
    generation_dir = generations_dir / generation
    generation_dir.mkdir()
    previous_meta = read_panel_cache_v2_meta(parquet_root)
    reuse = _ArrayReuse(cache_dir, int(version), allow_chunks=chunk_storage)

    arrays = {
        "dates": np.asarray(panel_like.dates),
        "features": np.asarray(panel_like.features),
        "returns_1d": np.asarray(panel_like.returns_1d),
        "tradable_mask": np.asarray(panel_like.tradable_mask),
        "can_buy_mask": np.asarray(
            panel_like.can_buy_mask if panel_like.can_buy_mask is not None else panel_like.tradable_mask
        ),
        "can_sell_mask": np.asarray(
            panel_like.can_sell_mask if panel_like.can_sell_mask is not None else panel_like.tradable_mask
        ),
        "can_short_open_mask": np.asarray(
            panel_like.can_short_open_mask
            if getattr(panel_like, "can_short_open_mask", None) is not None
            else panel_like.can_sell_mask
            if panel_like.can_sell_mask is not None
            else panel_like.tradable_mask
        ),
        "force_short_cover_mask": np.asarray(
            panel_like.force_short_cover_mask
            if getattr(panel_like, "force_short_cover_mask", None) is not None
            else np.zeros_like(panel_like.tradable_mask, dtype=bool)
        ),
        "force_exit_mask": np.asarray(
            panel_like.force_exit_mask
            if getattr(panel_like, "force_exit_mask", None) is not None
            else np.zeros_like(panel_like.tradable_mask, dtype=bool)
        ),
        "alive_mask": np.asarray(panel_like.alive_mask),
        "benchmark_returns": np.asarray(panel_like.benchmark_returns),
        "close_prices": np.asarray(panel_like.close_prices),
        "daily_volumes": np.asarray(
            getattr(panel_like, "daily_volumes", None)
            if getattr(panel_like, "daily_volumes", None) is not None
            else np.full_like(panel_like.close_prices, np.nan, dtype=np.float32)
        ),
    }
    for name in OPTIONAL_ARRAY_NAMES:
        value = getattr(panel_like, name, None)
        if value is not None:
            arrays[name] = np.asarray(value)
    variant_committed = False
    try:
        array_meta: dict[str, dict[str, Any]] = {}
        for name, array in arrays.items():
            item_meta = _save_array(generation_dir, name, array, reuse=reuse,
                                    directory_commit_pending=True)
            item_meta["file"] = str(
                Path("generations") / generation / item_meta["file"]
            )
            array_meta[name] = item_meta
        symbols_path = _json_file(generation_dir, "symbols")
        feature_names_path = _json_file(generation_dir, "feature_names")
        _write_json(symbols_path, list(panel_like.symbols), directory_commit_pending=True)
        _write_json(feature_names_path, list(panel_like.feature_names), directory_commit_pending=True)
        # No manifest can expose this private generation yet. Keep every file's
        # original data fsync, but commit its namespace once instead of once per
        # array/JSON. Both the contents and new generation name must be durable
        # BEFORE either variant/current pointer can refer to them. Failure is a
        # failed transaction, never an accepted partially durable generation.
        _fsync_directory(generation_dir, required=True)
        _fsync_directory(generations_dir, required=True)
        meta = {
            "version": int(version),
            "source_hash": str(source_hash),
            "backend_key": str(backend_key),
            "num_dates": int(panel_like.num_dates),
            "num_symbols": int(panel_like.num_symbols),
            "generation": generation,
            "symbols_file": str(Path("generations") / generation / symbols_path.name),
            "feature_names_file": str(
                Path("generations") / generation / feature_names_path.name
            ),
            "arrays": array_meta,
            "storage_build": {"contract": (
                                  "panel_time_chunk_reuse_v2"
                                  if sum(reuse.chunk_counters.values()) else "panel_array_reuse_v1"
                              ), "reused_arrays": reuse.reused,
                              "written_arrays": reuse.written, "avoided_array_bytes": reuse.avoided_bytes,
                              "written_array_bytes": reuse.written_bytes,
                              "directory_commit": "private_generation_batch_v1",
                              **reuse.chunk_counters},
        }
        if hasattr(panel_like, "content_fingerprints"):
            panel_like.content_fingerprints = {
                name: dict(item["content_fingerprint"])
                for name, item in array_meta.items()
            }
        variants_dir = cache_dir / PANEL_CACHE_V2_VARIANTS_DIRNAME
        variants_dir.mkdir(parents=True, exist_ok=True)
        _fsync_directory(cache_dir, required=True)
        _fsync_directory(cache_dir.parent, required=True)

        # Preserve the previous current-version contract before moving the
        # compatibility pointer.  Configurations that share one parquet root
        # must not invalidate and rebuild each other's multi-gigabyte panels.
        try:
            previous_version = int(previous_meta.get("version", -1)) if isinstance(previous_meta, dict) else -1
        except (TypeError, ValueError):
            previous_version = -1
        if (
            isinstance(previous_meta, dict)
            and previous_version == int(version)
            and isinstance(previous_meta.get("source_hash"), str)
            and isinstance(previous_meta.get("backend_key"), str)
            # A newer source snapshot replaces the same logical backend
            # contract.  Preserve only genuinely different variants; otherwise
            # daily data updates would retain one multi-GB generation forever.
            and str(previous_meta["backend_key"]) != str(backend_key)
        ):
            previous_variant_path = panel_cache_v2_variant_meta_path(
                parquet_root,
                source_hash=str(previous_meta["source_hash"]),
                backend_key=str(previous_meta["backend_key"]),
                version=int(previous_meta["version"]),
            )
            if not previous_variant_path.exists():
                _write_json(previous_variant_path, previous_meta)

        variant_meta_path = panel_cache_v2_variant_meta_path(
            parquet_root,
            source_hash=source_hash,
            backend_key=backend_key,
            version=version,
        )
        # The variant metadata is the durable content-addressed commit.  The
        # top-level metadata remains a backwards-compatible pointer to the
        # most recently saved variant.
        _write_json(variant_meta_path, meta)
        variant_committed = True
        _write_json(panel_cache_v2_meta_path(parquet_root), meta)
    except Exception:
        if not variant_committed:
            shutil.rmtree(generation_dir, ignore_errors=True)
        raise

    # Reclaim only generations no longer referenced by any live variant.
    # Replacing one logical backend contract is bounded to its latest source
    # snapshot, while switching feature/backend contracts keeps warm caches.
    try:
        referenced_generations = {generation}
        variants_dir = cache_dir / PANEL_CACHE_V2_VARIANTS_DIRNAME
        variants_to_retire = []
        if variants_dir.exists():
            for variant_path in variants_dir.glob("*.json"):
                try:
                    variant_meta = _read_json(variant_path)
                except (OSError, json.JSONDecodeError):
                    return cache_dir  # Unknown protection is not garbage.
                if (not isinstance(variant_meta, dict)
                        or not re.fullmatch(r"[0-9a-f]{32}", str(variant_meta.get("generation", "")))
                        or not isinstance(variant_meta.get("backend_key"), str)
                        or not isinstance(variant_meta.get("source_hash"), str)):
                    return cache_dir
                try:
                    variant_version = int(variant_meta.get("version", -1))
                except (TypeError, ValueError):
                    return cache_dir
                variant_backend_key = str(variant_meta.get("backend_key", ""))
                variant_source_hash = str(variant_meta.get("source_hash", ""))
                stale_same_contract = (
                    variant_version == int(version)
                    and variant_backend_key == str(backend_key)
                    and variant_source_hash != str(source_hash)
                )
                if variant_version != int(version) or stale_same_contract:
                    variants_to_retire.append(variant_path)
                    continue
                referenced = str(variant_meta.get("generation", "")).strip()
                if referenced:
                    referenced_generations.add(referenced)
        candidates = [candidate for candidate in generations_dir.iterdir()
                      if candidate.name not in referenced_generations]
        if any(not re.fullmatch(r"[0-9a-f]{32}", candidate.name)
               or candidate.is_symlink() or not candidate.is_dir() for candidate in candidates):
            return cache_dir
        legacy_paths = [*(_array_file(cache_dir, name) for name in ARRAY_NAMES),
                        _json_file(cache_dir, "symbols"), _json_file(cache_dir, "feature_names")]
        protected_paths = [*candidates, *variants_to_retire,
                           *(path for path in legacy_paths if path.exists())]
        if protected_paths:
            from stockagent.data_sync.materialized_cache import process_references_many
            from stockagent.data_sync.desync_snapshots import SnapshotError
            try:
                if process_references_many(protected_paths, include_current_process=True):
                    return cache_dir
            except (OSError, SnapshotError):
                return cache_dir  # New commit is valid; retirement is deferred.
        # Do not invalidate the previous contract until ALL metadata and live
        # readers have been checked. Invalid metadata and active mmap generations
        # remain intact; no reader is asked to recover after losing its input.
        for variant_path in variants_to_retire:
            variant_path.unlink()
        for candidate in candidates:
            shutil.rmtree(candidate)
        for name in ARRAY_NAMES:
            _array_file(cache_dir, name).unlink(missing_ok=True)
        _json_file(cache_dir, "symbols").unlink(missing_ok=True)
        _json_file(cache_dir, "feature_names").unlink(missing_ok=True)
    except OSError:
        # Cleanup is not part of the transaction. A later successful save can
        # reclaim any superseded generation or legacy payload.
        pass
    return cache_dir


def read_panel_cache_v2_meta(parquet_root: str | Path) -> dict[str, Any] | None:
    meta_path = panel_cache_v2_meta_path(parquet_root)
    if not meta_path.exists():
        return None
    try:
        meta = _read_json(meta_path)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(meta, dict):
        return None
    return meta


def _meta_matches_contract(
    meta: dict[str, Any],
    *,
    source_hash: str,
    backend_key: str,
    version: int,
) -> bool:
    try:
        cached_version = int(meta.get("version", -1))
    except (TypeError, ValueError):
        return False
    return (
        meta.get("source_hash") == str(source_hash)
        and cached_version == int(version)
        and meta.get("backend_key") == str(backend_key)
    )


def _read_panel_cache_v2_contract_meta(
    parquet_root: str | Path,
    *,
    source_hash: str,
    backend_key: str,
    version: int,
) -> tuple[dict[str, Any] | None, Path]:
    """Read the exact cache contract, falling back to the legacy latest pointer."""

    variant_path = panel_cache_v2_variant_meta_path(
        parquet_root,
        source_hash=source_hash,
        backend_key=backend_key,
        version=version,
    )
    if variant_path.exists():
        try:
            variant_meta = _read_json(variant_path)
        except (OSError, json.JSONDecodeError):
            variant_meta = None
        if isinstance(variant_meta, dict) and _meta_matches_contract(
            variant_meta,
            source_hash=source_hash,
            backend_key=backend_key,
            version=version,
        ):
            return variant_meta, variant_path

    current_path = panel_cache_v2_meta_path(parquet_root)
    current_meta = read_panel_cache_v2_meta(parquet_root)
    if isinstance(current_meta, dict) and _meta_matches_contract(
        current_meta,
        source_hash=source_hash,
        backend_key=backend_key,
        version=version,
    ):
        return current_meta, current_path
    return None, variant_path


def _load_panel_cache_v2_generation(
    cache_dir: Path,
    meta: dict[str, Any],
    *,
    mmap_mode: str | None,
) -> dict[str, Any]:
    symbols_file = str(meta.get("symbols_file", "symbols.json"))
    feature_names_file = str(meta.get("feature_names_file", "feature_names.json"))
    payload: dict[str, Any] = {
        "symbols": list(_read_json(cache_dir / symbols_file)),
        "feature_names": list(_read_json(cache_dir / feature_names_file)),
        "_content_fingerprints": {
            name: dict(item["content_fingerprint"])
            for name, item in meta.get("arrays", {}).items()
            if isinstance(item, dict)
            and isinstance(item.get("content_fingerprint"), dict)
        },
    }
    arrays_meta = meta.get("arrays", {})
    for name in REQUIRED_ARRAY_NAMES:
        payload[name] = load_panel_cache_array(
            cache_dir, arrays_meta.get(name, {"file": f"{name}.npy"}),
            mmap_mode=mmap_mode,
        )
    for name in OPTIONAL_ARRAY_NAMES:
        item_meta = arrays_meta.get(name)
        if not isinstance(item_meta, dict):
            payload[name] = None
            continue
        payload[name] = load_panel_cache_array(
            cache_dir, item_meta, mmap_mode=mmap_mode,
        )
    return payload


def load_panel_cache_array(
    cache_dir: str | Path, item: dict[str, Any], *, mmap_mode: str | None = "r",
) -> np.ndarray:
    """The shared array reader for Panel and explicitly pinned minute context."""
    cache_dir = Path(cache_dir)
    if item.get("storage") == panel_chunks.CHUNK_STORAGE:
        return panel_chunks.load_chunks(cache_dir, item, mmap_mode=mmap_mode)
    if item.get("storage") is not None:
        raise ValueError(f"unknown Panel array storage: {item.get('storage')!r}")
    return np.load(cache_dir / str(item["file"]), mmap_mode=mmap_mode, allow_pickle=False)


def load_panel_cache_v2(
    parquet_root: str | Path,
    *,
    mmap_mode: str | None = "r",
    source_hash: str | None = None,
    backend_key: str | None = None,
    version: int | None = None,
) -> dict[str, Any]:
    cache_dir = panel_cache_v2_dir(parquet_root)
    contract_values = (source_hash, backend_key, version)
    if any(value is not None for value in contract_values) and not all(
        value is not None for value in contract_values
    ):
        raise ValueError(
            "source_hash, backend_key, and version must be provided together"
        )
    last_error: FileNotFoundError | None = None
    for _ in range(3):
        if source_hash is None:
            meta = read_panel_cache_v2_meta(parquet_root)
        else:
            meta, _ = _read_panel_cache_v2_contract_meta(
                parquet_root,
                source_hash=str(source_hash),
                backend_key=str(backend_key),
                version=int(version),
            )
        if meta is None:
            last_error = FileNotFoundError(
                f"missing panel cache v2 metadata under {cache_dir}"
            )
            continue
        try:
            return _load_panel_cache_v2_generation(
                cache_dir,
                meta,
                mmap_mode=mmap_mode,
            )
        except FileNotFoundError as exc:
            # A concurrent writer may have committed and reclaimed the
            # generation whose metadata this reader sampled. Retry the entire
            # snapshot from the latest atomic metadata pointer.
            last_error = exc
    if last_error is not None:
        raise last_error
    raise FileNotFoundError(f"unable to load panel cache v2 under {cache_dir}")


def load_panel_cache_v2_manifest(
    manifest_path: str | Path,
    *,
    mmap_mode: str | None = "r",
    expected_version: int | None = None,
    expected_generation: str | None = None,
    expected_source_hash: str | None = None,
) -> dict[str, Any]:
    """Load one explicitly pinned immutable cache generation.

    Ordinary panel loading validates the current source files and backend key.
    A long-running experiment instead needs to keep reading the exact panel
    generation recorded when its first checkpoint was written, even after the
    live ``data_tw_public`` symlink advances. This entry point accepts a
    concrete metadata receipt, validates its identity and array ABI, and never
    follows the mutable top-level pointer.
    """

    path = Path(manifest_path).expanduser().resolve()
    try:
        meta = _read_json(path)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid pinned panel cache manifest: {path}") from exc
    if not isinstance(meta, dict):
        raise ValueError(f"pinned panel cache manifest must be a mapping: {path}")

    try:
        version = int(meta.get("version", -1))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"pinned panel cache manifest has invalid version: {path}"
        ) from exc
    generation = str(meta.get("generation", "")).strip()
    source_hash = str(meta.get("source_hash", "")).strip()
    if expected_version is not None and version != int(expected_version):
        raise ValueError(
            "pinned panel cache version mismatch: "
            f"expected={int(expected_version)} actual={version} path={path}"
        )
    if expected_generation is not None and generation != str(expected_generation):
        raise ValueError(
            "pinned panel cache generation mismatch: "
            f"expected={expected_generation} actual={generation} path={path}"
        )
    if expected_source_hash is not None and source_hash != str(expected_source_hash):
        raise ValueError(
            "pinned panel cache source hash mismatch: "
            f"expected={expected_source_hash} actual={source_hash} path={path}"
        )
    if not generation or not source_hash:
        raise ValueError(
            f"pinned panel cache manifest lacks generation/source_hash: {path}"
        )

    cache_dir = (
        path.parent.parent
        if path.parent.name == PANEL_CACHE_V2_VARIANTS_DIRNAME
        else path.parent
    )
    cache_root = cache_dir.resolve()
    referenced_files = [
        str(meta.get("symbols_file", "symbols.json")),
        str(meta.get("feature_names_file", "feature_names.json")),
    ]
    arrays_meta = meta.get("arrays")
    if not isinstance(arrays_meta, dict):
        raise ValueError(f"pinned panel cache manifest lacks arrays: {path}")
    for name in REQUIRED_ARRAY_NAMES:
        item = arrays_meta.get(name)
        if not isinstance(item, dict) or not str(item.get("file", "")).strip():
            raise ValueError(
                f"pinned panel cache manifest lacks required array {name!r}: {path}"
            )
    referenced_files.extend(
        str(file)
        for name, item in arrays_meta.items()
        if isinstance(item, dict)
        for file in panel_chunks.array_paths(cache_dir, item)
    )
    for relative in referenced_files:
        referenced = (cache_dir / relative).resolve()
        if not referenced.is_relative_to(cache_root):
            raise ValueError(
                f"pinned panel cache reference escapes cache directory: {relative}"
            )
        if not referenced.is_file():
            raise FileNotFoundError(
                f"pinned panel cache reference is missing: {referenced}"
            )

    payload = _load_panel_cache_v2_generation(
        cache_dir,
        meta,
        mmap_mode=mmap_mode,
    )
    for name, item in arrays_meta.items():
        if name not in payload or payload[name] is None or not isinstance(item, dict):
            continue
        array = np.asarray(payload[name])
        expected_shape = item.get("shape")
        expected_dtype = item.get("dtype")
        if expected_shape != [int(size) for size in array.shape]:
            raise ValueError(
                f"pinned panel cache shape mismatch for {name}: "
                f"expected={expected_shape} actual={list(array.shape)}"
            )
        if expected_dtype != str(array.dtype):
            raise ValueError(
                f"pinned panel cache dtype mismatch for {name}: "
                f"expected={expected_dtype} actual={array.dtype}"
            )
    payload["_pinned_manifest"] = {
        "path": str(path),
        "version": version,
        "generation": generation,
        "source_hash": source_hash,
    }
    return payload


def _panel_cache_required_paths(
    cache_dir: Path,
    meta: dict[str, Any],
    parquet_root: str | Path,
    *,
    meta_path: Path | None = None,
) -> list[Path]:
    arrays_meta = meta.get("arrays", {})
    required_paths = [
        panel_cache_v2_meta_path(parquet_root) if meta_path is None else meta_path,
        cache_dir / str(meta.get("symbols_file", "symbols.json")),
        cache_dir / str(meta.get("feature_names_file", "feature_names.json")),
    ]
    required_paths.extend(
        file for name in ARRAY_NAMES
        if name in REQUIRED_ARRAY_NAMES or isinstance(arrays_meta.get(name), dict)
        for file in panel_chunks.array_paths(
            cache_dir, arrays_meta.get(name, {"file": f"{name}.npy"}),
        )
    )
    return required_paths


def panel_cache_v2_is_valid(
    parquet_root: str | Path,
    *,
    source_hash: str,
    backend_key: str,
    version: int,
    source_paths: list[Path],
) -> bool:
    meta, selected_meta_path = _read_panel_cache_v2_contract_meta(
        parquet_root,
        source_hash=source_hash,
        backend_key=backend_key,
        version=version,
    )
    if meta is None:
        return False
    if not _meta_matches_contract(
        meta,
        source_hash=source_hash,
        backend_key=backend_key,
        version=version,
    ):
        return False
    cache_dir = panel_cache_v2_dir(parquet_root)
    try:
        required_paths = _panel_cache_required_paths(
            cache_dir, meta, parquet_root, meta_path=selected_meta_path,
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False
    if not all(path.exists() for path in required_paths):
        # Avoid a false invalidation when metadata changed during this check.
        latest_meta, latest_meta_path = _read_panel_cache_v2_contract_meta(
            parquet_root,
            source_hash=source_hash,
            backend_key=backend_key,
            version=version,
        )
        if latest_meta is None or latest_meta == meta:
            return False
        meta = latest_meta
        selected_meta_path = latest_meta_path
        if not _meta_matches_contract(
            meta,
            source_hash=source_hash,
            backend_key=backend_key,
            version=version,
        ):
            return False
        try:
            required_paths = _panel_cache_required_paths(
                cache_dir, meta, parquet_root, meta_path=selected_meta_path,
            )
        except (OSError, ValueError, TypeError, KeyError):
            return False
        if not all(path.exists() for path in required_paths):
            return False
    if not source_paths:
        return True
    newest_source_mtime = max(path.stat().st_mtime for path in source_paths)
    try:
        # A shared historical chunk correctly retains its original mtime.
        # Freshness belongs to the new source-hash-bound commit, not every
        # immutable payload's creation time. Requiring those times to advance
        # turns a successful incremental write into a perpetual cache miss.
        commit_mtime = selected_meta_path.stat().st_mtime
    except FileNotFoundError:
        return False
    return commit_mtime >= newest_source_mtime
