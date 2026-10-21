"""Bounded read-only dashboard invalidation, independent of trading processes."""

from __future__ import annotations

import ctypes
from collections import OrderedDict
import hashlib
import json
import marshal
import os
from pathlib import Path
import select
import stat as stat_module
import sys
import threading
from typing import Collection, Mapping

_SIGNATURE_CACHE: OrderedDict[Path, tuple[tuple[int, ...], bytes]] = OrderedDict()
_SIGNATURE_CACHE_LOCK = threading.Lock()
_MAX_SIGNATURES = 512

# Independent of response TTLs: each source invalidates only its own decoded
# snapshot. A heartbeat must not force decoding every unchanged receipt. This
# is a disposable read optimization, never a source SHA/acceptance receipt.
_JSON_SNAPSHOTS: OrderedDict[Path, tuple[tuple[int, ...], object, int]] = OrderedDict()
_JSON_SNAPSHOT_LOCK = threading.Lock()
_JSON_SOURCE_LOCKS = tuple(threading.Lock() for _ in range(32))
_JSON_MAX_RESIDENT_BYTES = 32 * 1024**2
_JSON_MAX_ENTRIES = 512
_JSON_RESIDENT_BYTES = 0
_JSON_COUNTS = {"hits": 0, "decodes": 0, "decoded_bytes": 0, "rejected_races": 0}


def _json_resident_charge(packet):
    # Only the owned immutable packet is resident, not a second mutable tree.
    # Account for it directly, plus the bounded LRU/path/metadata envelope.
    return 512 + sys.getsizeof(packet)


def json_snapshot_metrics() -> dict[str, int]:
    """Aggregate engineering telemetry; never expose private source paths."""
    with _JSON_SNAPSHOT_LOCK:
        return {**_JSON_COUNTS, "entries": len(_JSON_SNAPSHOTS),
                "resident_charge_bytes": _JSON_RESIDENT_BYTES}


def read_json_snapshot(path: Path, default=None, *, max_bytes: int | None = None):
    """Decode only changed sources and return a mutation-isolated JSON value.

    Full dev/inode/size/mtime/ctime fences cover append, truncation, in-place
    corrections, atomic replacement and symlink rotation. A partial/malformed
    write never installs a cache entry or returns an older snapshot as current.
    Same-source reads are single-flight; independent sources need no global
    I/O lock. Callers keep their original type, size and freshness policies.
    """
    global _JSON_RESIDENT_BYTES
    source = Path(path).absolute()
    if max_bytes is not None and max_bytes < 0:
        raise ValueError("JSON snapshot size limit must be nonnegative")
    with _JSON_SOURCE_LOCKS[hash(source) % len(_JSON_SOURCE_LOCKS)]:
        try:
            before = source.stat()
            signature = metadata_signature(before)
            if not stat_module.S_ISREG(before.st_mode) or (
                max_bytes is not None and before.st_size > max_bytes
            ):
                return default
            with _JSON_SNAPSHOT_LOCK:
                cached = _JSON_SNAPSHOTS.get(source)
                if cached is not None and cached[0] == signature:
                    _JSON_SNAPSHOTS.move_to_end(source)
                    _JSON_COUNTS["hits"] += 1
                    value = cached[1]
                else:
                    value = None
            if cached is not None and cached[0] == signature:
                # Never hand a shared mutable dict/list to a projection builder.
                # NEVER deserialize caller/file-supplied marshal bytes. This
                # ephemeral packet was generated here from json.loads-only
                # primitives, in this process/Python version. C-level cloning
                # preserves isolation without slower generic deep copying.
                result = marshal.loads(value)
                if signature == metadata_signature(source.stat()):
                    return result
                with _JSON_SNAPSHOT_LOCK:
                    _JSON_COUNTS["rejected_races"] += 1
                return default
            with source.open("rb") as stream:
                if signature != metadata_signature(os.fstat(stream.fileno())):
                    return default
                raw = stream.read() if max_bytes is None else stream.read(max_bytes + 1)
                finished = metadata_signature(os.fstat(stream.fileno()))
            if max_bytes is not None and len(raw) > max_bytes:
                return default
            value = json.loads(raw.decode("utf-8"))
            with _JSON_SNAPSHOT_LOCK:
                _JSON_COUNTS["decodes"] += 1
                _JSON_COUNTS["decoded_bytes"] += len(raw)
            if signature != finished or signature != metadata_signature(source.stat()):
                with _JSON_SNAPSHOT_LOCK:
                    _JSON_COUNTS["rejected_races"] += 1
                return default
            packet = marshal.dumps(value, 4)
            charge = _json_resident_charge(packet)
            if charge <= _JSON_MAX_RESIDENT_BYTES:
                with _JSON_SNAPSHOT_LOCK:
                    previous = _JSON_SNAPSHOTS.pop(source, None)
                    if previous is not None:
                        _JSON_RESIDENT_BYTES -= previous[2]
                    while _JSON_SNAPSHOTS and (
                        len(_JSON_SNAPSHOTS) >= _JSON_MAX_ENTRIES
                        or _JSON_RESIDENT_BYTES + charge > _JSON_MAX_RESIDENT_BYTES
                    ):
                        _, removed = _JSON_SNAPSHOTS.popitem(last=False)
                        _JSON_RESIDENT_BYTES -= removed[2]
                    _JSON_SNAPSHOTS[source] = (signature, packet, charge)
                    _JSON_RESIDENT_BYTES += charge
            # The caller owns the parse tree; the cache owns only immutable
            # bytes. Neither a hit nor a miss shares mutable dicts/lists.
            return value
        except (OSError, UnicodeError, ValueError):
            return default


def metadata_signature(metadata: os.stat_result) -> tuple[int, ...]:
    # ctime changes on in-place writes and mtime restoration on our POSIX
    # producer filesystem. Inode/device also bind atomic file replacement.
    return (metadata.st_dev, metadata.st_ino, metadata.st_size,
            metadata.st_mtime_ns, metadata.st_ctime_ns)


def file_signature(path: Path) -> tuple[object, ...] | None:
    """Retain a content digest without re-reading unchanged history per visitor.

    This is an invalidation hint, not a persisted data-integrity receipt.
    Cache a digest only after stable descriptor and path metadata agree.
    """
    try:
        metadata = path.stat()
        signature = metadata_signature(metadata)
        if not stat_module.S_ISREG(metadata.st_mode):
            return (*signature, b"")
        with _SIGNATURE_CACHE_LOCK:
            cached = _SIGNATURE_CACHE.get(path)
            if cached is not None and cached[0] == signature:
                _SIGNATURE_CACHE.move_to_end(path)
                return (*signature, cached[1])
        digest = hashlib.blake2b(digest_size=16)
        with path.open("rb") as stream:
            opened = metadata_signature(os.fstat(stream.fileno()))
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
            finished = metadata_signature(os.fstat(stream.fileno()))
        value = digest.digest()
        if signature == opened == finished == metadata_signature(path.stat()):
            with _SIGNATURE_CACHE_LOCK:
                _SIGNATURE_CACHE[path] = (signature, value)
                _SIGNATURE_CACHE.move_to_end(path)
                while len(_SIGNATURE_CACHE) > _MAX_SIGNATURES:
                    _SIGNATURE_CACHE.popitem(last=False)
        return (*opened, value)
    except OSError:
        return None


def metadata_file_signature(path: Path) -> tuple[int, ...] | None:
    """Cheap invalidation hint for large append-only or atomically replaced files.

    This is not a content-integrity proof. Inode, size, mtime and ctime cover
    the producer's append/replace contract without hashing hundreds of MiB on
    every market-data update.
    """
    try:
        return metadata_signature(path.stat())
    except OSError:
        return None


class DashboardUpdateHub:
    """One watcher and one latest generation per topic, never a per-client queue.

    Linux directory watches notice atomic replacements. Signature reconciliation
    also covers missing directories, inotify overflow, and non-Linux platforms.
    Notifications are hints: consumers still read the authoritative public DTO.
    """

    def __init__(
        self,
        paths: Mapping[str, tuple[Path, ...]],
        *,
        max_clients: int = 64,
        metadata_only_paths: Collection[Path] = (),
    ):
        self.paths = dict(paths)
        self.metadata_only_paths = frozenset(metadata_only_paths)
        self.max_clients = max_clients
        self._condition = threading.Condition()
        self._versions = dict.fromkeys(paths, 0)
        self._clients = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.native = False

    def acquire(self, topic: str) -> bool:
        with self._condition:
            if topic not in self.paths or self._stop.is_set() or self._clients >= self.max_clients:
                return False
            self._clients += 1
            if self._thread is None:
                self._thread = threading.Thread(target=self._watch, name="dashboard-updates", daemon=True)
                self._thread.start()
            return True

    def release(self) -> None:
        with self._condition:
            self._clients = max(0, self._clients - 1)

    @property
    def closed(self) -> bool:
        return self._stop.is_set()

    def publish(self, topic: str) -> None:
        with self._condition:
            self._versions[topic] += 1
            self._condition.notify_all()

    def signature(self, path: Path) -> tuple[object, ...] | None:
        if path in self.metadata_only_paths:
            return metadata_file_signature(path)
        return file_signature(path)

    def wait(self, topic: str, previous: int, timeout: float = 15.0) -> int:
        with self._condition:
            self._condition.wait_for(
                lambda: self.closed or self._versions[topic] != previous, timeout=timeout,
            )
            return self._versions[topic]

    def close(self) -> None:
        self._stop.set()
        with self._condition:
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _watch(self) -> None:
        fd = -1
        add_watch = None
        watched: dict[Path, tuple[int, int]] = {}
        signatures: dict[str, object] = {}
        try:
            try:
                libc = ctypes.CDLL(None, use_errno=True)
                init = libc.inotify_init1
                init.argtypes, init.restype = [ctypes.c_int], ctypes.c_int
                fd = int(init(os.O_NONBLOCK | os.O_CLOEXEC))
                add_watch = libc.inotify_add_watch
                add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
                add_watch.restype = ctypes.c_int
            except (AttributeError, OSError):
                pass
            while not self.closed:
                # Reattach if a producer creates or replaces its whole directory.
                for directory in {p.parent for paths in self.paths.values() for p in paths}:
                    signature = file_signature(directory)
                    identity = signature[:2] if signature else None
                    if fd >= 0 and add_watch is not None and identity and watched.get(directory) != identity:
                        if add_watch(fd, os.fsencode(directory), 0x8 | 0x80 | 0x100 | 0x200 | 0x400 | 0x800) >= 0:
                            watched[directory] = identity
                self.native = fd >= 0 and bool(watched)
                for topic, paths in self.paths.items():
                    signature = tuple(self.signature(path) for path in paths)
                    if signatures.get(topic) != signature:
                        signatures[topic] = signature
                        self.publish(topic)
                if self.native:
                    ready, _, _ = select.select([fd], [], [], 1.0)
                    if ready:
                        # Bounded drain: continuous writes must not starve reads.
                        for _ in range(4):
                            try:
                                if not os.read(fd, 65536):
                                    break
                            except BlockingIOError:
                                break
                else:
                    self._stop.wait(0.25)
        finally:
            if fd >= 0:
                os.close(fd)
