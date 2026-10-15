"""Metadata-only OpenBB footer I/O; never reuse a file's validity.

The optional constructor optimization skips only PyArrow's payload-column
prefix index. Its private hook is opt-in for one inspected version/source pair;
unknown installations retain the ordinary public reader. Native footer parsing,
schema conversion, metadata, physical descriptors and source checks are intact.
"""
from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import inspect
from pathlib import Path
from threading import Lock

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

POLICY = "openbb_metadata_only_footer_io_v1"
_SUPPORTED_CONSTRUCTOR = {
    "__init__": "e62dc4172671a18d18c2c07df27c5bb0b189516ec4393abae3a00be761111a6b",
    "_build_nested_paths": "bed0eda086ad4de3b4ac7093e7ae3a1e88b2ee95522649d9783d718edebc1a7a",
}
_PUBLIC_READER = pq.ParquetFile


class _MetadataOnlyParquetFile(_PUBLIC_READER):
    def _build_nested_paths(self):
        # This index is used only by payload-column selection, not any footer
        # property. Do not expose this object as a general-purpose data reader.
        return {}

    def _no_payload(self, *_args, **_kwargs):
        raise RuntimeError("metadata-only footer cannot read payload rows")

    read = read_row_group = read_row_groups = iter_batches = scan_contents = _no_payload


def supported_metadata_constructor(reader) -> bool:
    if pa.__version__ != "25.0.0" or reader is not _PUBLIC_READER:
        return False
    try:
        return all(hashlib.sha256(inspect.getsource(getattr(reader, name)).encode()).hexdigest() == sha
                   for name, sha in _SUPPORTED_CONSTRUCTOR.items())
    except MemoryError:
        raise
    except Exception:
        return False


class FooterOpener:
    """One attempt's filesystem/capability, not a metadata or validity cache."""

    def __init__(self, *, reader=None, local_filesystem: bool = True, metadata_only: bool = True):
        self.reader = pq.ParquetFile if reader is None else reader
        self.optimized = metadata_only and supported_metadata_constructor(self.reader)
        self.filesystem = fs.LocalFileSystem() if local_filesystem else None
        self._lock = Lock()
        self.opens = self.metadata_only_opens = self.canonical_retries = 0

    def _count(self, *, optimized=False, retry=False):
        with self._lock:
            self.opens += 1
            self.metadata_only_opens += int(optimized)
            self.canonical_retries += int(retry)

    def __call__(self, path):
        reader = _MetadataOnlyParquetFile if self.optimized else self.reader
        options = {"filesystem": self.filesystem} if self.filesystem is not None else {}
        self._count(optimized=self.optimized)
        try:
            return reader(path, **options)
        except MemoryError:
            raise
        except Exception:
            if not options and not self.optimized:
                raise
            # Preserve the original URI, permission and malformed-reader error
            # on exceptional/unknown paths. A resource failure never retries.
            self._count(retry=True)
            return self.reader(path)

    def snapshot(self):
        return {"policy": POLICY, "metadata_only_supported": self.optimized,
                "local_filesystem": self.filesystem is not None,
                "actual_footer_opens": self.opens, "metadata_only_opens": self.metadata_only_opens,
                "canonical_retries": self.canonical_retries}


@dataclass(slots=True)
class FooterRead:
    row: object
    path: Path
    before: tuple | None = None
    parquet_file: object = None
    attempted: bool = False
    error: Exception | None = None


@contextmanager
def prefetched_footer_rows(rows, *, opener, signature, grouped_endpoint,
                           collect_proofs, workers=2, max_pending=4):
    """Read at most max_pending footers, return in original manifest order.

    SQLite and all proof/cache/state processing stay on the caller thread.
    Pending native metadata is bounded by count, not a decoded-byte guarantee.
    Cancellation joins only this pool and releases each owned file handle.
    """
    if type(workers) is not int or not 1 <= workers <= 2:
        raise ValueError("one or two footer workers required")
    if type(max_pending) is not int or not workers <= max_pending <= 4:
        raise ValueError("bounded footer queue (workers..4) required")
    source, pending = iter(rows), deque()
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="openbb-footer")

    def read(row):
        item = FooterRead(row=row, path=Path(str(row["output_path"])))
        try:
            if collect_proofs and row["endpoint"] == grouped_endpoint:
                item.before = signature(item.path)
            item.attempted = True
            item.parquet_file = opener(str(item.path))
        except Exception as exc:
            item.error = exc
        return item

    def close(item):
        if item is not None and item.parquet_file is not None:
            item.parquet_file.close()

    def submit():
        try:
            row = next(source)
        except StopIteration:
            return False
        pending.append(pool.submit(read, row))
        return True

    def ordered():
        for _ in range(max_pending):
            if not submit():
                break
        while pending:
            item = pending.popleft().result()
            try:
                yield item
            finally:
                close(item)
            submit()

    iterator = ordered()
    try:
        yield iterator
    finally:
        close_error = None
        try:
            iterator.close()
        except BaseException as exc:
            close_error = exc
        finally:
            # A failing close must not strand this attempt's other readers or
            # pool. This experimental prefetch path is not used by the serial
            # metadata-only production reader.
            pool.shutdown(wait=True, cancel_futures=True)
            while pending:
                future = pending.popleft()
                if future.cancelled():
                    continue
                try:
                    close(future.result())
                except BaseException as exc:
                    if close_error is None:
                        close_error = exc
        if close_error is not None:
            raise close_error
