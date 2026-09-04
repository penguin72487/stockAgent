"""Small, dependency-light primitives for publishing downloader artifacts.

Downloader workspaces are mutable, but readers must never observe a partially
written JSON, raw response, CSV, or Parquet file.  These helpers write beside
the destination and publish with ``os.replace`` so the rename stays on the same
filesystem.  They intentionally do not move data into the immutable packed
store; catalog audit and release publication remain a separate lifecycle.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import threading
from typing import Any


_COPY_CHUNK_BYTES = 1 << 20


def _temporary_path(path: Path) -> Path:
    """Return a collision-resistant sibling path for one writer thread."""

    return path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")


def _sync_parent(path: Path) -> None:
    """Best-effort directory sync used only for durable control artifacts."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path.parent, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(
    path: str | Path,
    payload: bytes,
    *,
    durable: bool = False,
) -> None:
    """Atomically replace ``path`` with complete bytes.

    ``durable=True`` also fsyncs the temporary file and parent directory.  Use
    it for small run receipts/checkpoints that must survive a sudden restart;
    bulk raw payloads and reproducible Parquet normally leave it disabled to
    avoid one storage barrier per response.
    """

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_path(target)
    try:
        with temporary.open("wb") as handle:
            handle.write(payload)
            if durable:
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(temporary, target)
        if durable:
            _sync_parent(target)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_text(
    path: str | Path,
    text: str,
    *,
    encoding: str = "utf-8",
    durable: bool = False,
) -> None:
    atomic_write_bytes(path, str(text).encode(encoding), durable=durable)


def atomic_write_json(
    path: str | Path,
    payload: Any,
    *,
    durable: bool = True,
    ensure_ascii: bool = False,
    sort_keys: bool = False,
) -> None:
    atomic_write_text(
        path,
        json.dumps(
            payload,
            ensure_ascii=ensure_ascii,
            indent=2,
            sort_keys=sort_keys,
        )
        + "\n",
        durable=durable,
    )


def atomic_write_parquet(
    path: str | Path,
    frame: Any,
    *,
    compression: str = "zstd",
    write_statistics: bool = True,
    durable: bool = False,
    **writer_options: Any,
) -> None:
    """Atomically publish a Polars DataFrame or PyArrow Table as Parquet."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_path(target)
    if isinstance(frame, pa.Table):
        table = frame
    elif hasattr(frame, "to_arrow"):
        table = frame.to_arrow()
    else:
        # Pandas remains necessary in a few official-source parsers.  Keep its
        # index out of the storage ABI just like DataFrame.to_parquet(index=False).
        table = pa.Table.from_pandas(frame, preserve_index=False)
    try:
        pq.write_table(
            table,
            temporary,
            compression=compression,
            write_statistics=write_statistics,
            **writer_options,
        )
        if durable:
            with temporary.open("rb") as handle:
                os.fsync(handle.fileno())
        os.replace(temporary, target)
        if durable:
            _sync_parent(target)
    finally:
        temporary.unlink(missing_ok=True)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: str | Path, *, chunk_bytes: int = _COPY_CHUNK_BYTES) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_bytes), b""):
            digest.update(chunk)
    return digest.hexdigest()
