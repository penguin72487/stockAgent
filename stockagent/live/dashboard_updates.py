"""Bounded read-only dashboard invalidation, independent of trading processes."""

from __future__ import annotations

import ctypes
import hashlib
import os
from pathlib import Path
import select
import stat as stat_module
import threading
from typing import Mapping


def file_signature(path: Path) -> tuple[int, int, int, int, bytes] | None:
    try:
        metadata = path.stat()
        digest = (
            hashlib.blake2b(path.read_bytes(), digest_size=16).digest()
            if stat_module.S_ISREG(metadata.st_mode)
            else b""
        )
        return (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            digest,
        )
    except OSError:
        return None


class DashboardUpdateHub:
    """One watcher and one latest generation per topic, never a per-client queue.

    Linux directory watches notice atomic replacements. Signature reconciliation
    also covers missing directories, inotify overflow, and non-Linux platforms.
    Notifications are hints: consumers still read the authoritative public DTO.
    """

    def __init__(self, paths: Mapping[str, tuple[Path, ...]], *, max_clients: int = 64):
        self.paths = dict(paths)
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
                    signature = tuple(file_signature(path) for path in paths)
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
