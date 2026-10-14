"""Byte-bounded immutable raw tails shared by compatible live panel builders.

This does not accept a source generation or change features/units/causal masks.
The live caller must hold its canonical source read lease; offline callers must
own a frozen fixture. Generation checks are defense in depth, not a substitute
for that writer/reader contract. The live engine lazily owns one cache and
releases it through the existing serialized idle/deployment eviction path;
native loaded-code adoption remains distinct from offline output acceptance.
"""
from __future__ import annotations

from collections import OrderedDict
import errno
from pathlib import Path
import threading
from typing import Callable

import pyarrow as pa

from downloader.ohlcv_hot_tail import logical_parts, read_logical_parquet


def _identity(path: Path) -> tuple:
    parts = logical_parts(path)
    if not parts:
        raise FileNotFoundError(path)
    return tuple((str(part.absolute()), value.st_dev, value.st_ino, value.st_size,
                  value.st_mtime_ns, value.st_ctime_ns)
                 for part in parts for value in [part.stat()])


def _load(path: Path, rows: int) -> pa.Table:
    return read_logical_parquet(path, tail_rows=rows).to_arrow()


class LiveTailSourceCache:
    """Cache raw Arrow rows, not any model- or feature-specific transformation."""

    def __init__(self, max_bytes: int = 64 * 1024**2,
                 loader: Callable[[Path, int], pa.Table] = _load):
        if type(max_bytes) is not int or max_bytes < 0:
            raise ValueError("cache capacity must be a nonnegative byte count")
        self.max_bytes = max_bytes
        self._loader = loader
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple, tuple[pa.Table, int]] = OrderedDict()
        self._bytes = self._epoch = self._hits = self._misses = self._evictions = 0

    def read(self, path: Path, *, tail_rows: int) -> pa.Table:
        if type(tail_rows) is not int or tail_rows <= 0:
            raise ValueError("a positive integer tail length is required")
        path = Path(path)
        before = _identity(path)
        key = (str(path.absolute()), tail_rows, before)
        with self._lock:
            entry = self._entries.get(key)
            epoch = self._epoch
            if entry is not None:
                self._entries.move_to_end(key)
                self._hits += 1
            else:
                self._misses += 1
        table = entry[0] if entry is not None else self._loader(path, tail_rows)
        if not isinstance(table, pa.Table):
            raise TypeError("raw tail loader must return an Arrow table")
        # A sliced Arrow table can retain its entire decoded historical row
        # group. take() owns compact row buffers and preserves the schema and
        # every valid numeric bit; account actual buffers, not logical nbytes.
        if entry is None and self.max_bytes:
            table = table.take(pa.array(range(table.num_rows), type=pa.int64()))
        if _identity(path) != before:
            raise OSError(errno.EAGAIN, "source changed during raw tail cache read", str(path))
        if entry is None and self.max_bytes:
            size = table.get_total_buffer_size()
            with self._lock:
                if epoch == self._epoch and size <= self.max_bytes:
                    previous = self._entries.pop(key, None)
                    if previous is not None:
                        self._bytes -= previous[1]
                    while self._entries and self._bytes + size > self.max_bytes:
                        _, (_, removed) = self._entries.popitem(last=False)
                        self._bytes -= removed
                        self._evictions += 1
                    self._entries[key] = (table, size)
                    self._bytes += size
        return table

    def clear(self) -> None:
        with self._lock:
            self._epoch += 1
            self._entries.clear()
            self._bytes = self._hits = self._misses = self._evictions = 0

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"entries": len(self._entries), "resident_buffer_bytes": self._bytes,
                    "max_bytes": self.max_bytes, "hits": self._hits, "misses": self._misses,
                    "evictions": self._evictions, "epoch": self._epoch}
