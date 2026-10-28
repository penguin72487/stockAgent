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
from typing import Any, Callable, Iterable, Iterator


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


def durable_replace(source: str | Path, destination: str | Path) -> None:
    """Publish data durably *before* a durable receipt may point at it.

    Atomic rename alone only protects concurrent readers, not power loss. Both
    the data and the destination directory must reach storage before success.
    """
    source, destination = Path(source), Path(destination)
    with source.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(source, destination)
    _sync_parent(destination)


def table_from_records(rows: list[dict[str, Any]]) -> Any:
    """Preserve late top-level fields instead of inferring only from row one."""
    import pyarrow as pa

    if not rows:
        return pa.Table.from_pylist([])
    names = dict.fromkeys(name for row in rows for name in row)
    if any(not isinstance(name, str) for name in names):
        raise ValueError("record fields must be strings")
    if set(names) == set(rows[0]):
        return pa.Table.from_pylist(rows)
    # The sentinel contributes schema, never an observation or a synthetic zero.
    return pa.Table.from_pylist([dict.fromkeys(names), *rows]).slice(1)


def _open_temporary_stream(path: Path, mode: int | None) -> Any:
    if mode is None:
        return path.open("wb")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    return os.fdopen(os.open(path, flags, mode), "wb")


def atomic_write_bytes(
    path: str | Path,
    payload: bytes,
    *,
    durable: bool = False,
    mode: int | None = None,
) -> None:
    """Atomically replace ``path`` with complete bytes.

    ``durable=True`` also fsyncs the temporary file and parent directory.  Use
    it for small run receipts/checkpoints that must survive a sudden restart;
    bulk raw payloads and reproducible Parquet normally leave it disabled to
    avoid one storage barrier per response. ``mode`` creates a private sibling
    with restrictive permissions before writing any bytes; an existing sibling
    is never overwritten or removed in that explicit-permission path.
    """

    atomic_write_chunks(path, (payload,), durable=durable, mode=mode)


def atomic_write_chunks(
    path: str | Path,
    chunks: Iterable[bytes],
    *,
    durable: bool = False,
    mode: int | None = None,
) -> None:
    """Publish a complete byte stream with the same atomic/durable boundary.

    Encoding failures retain the previous destination and remove only this
    writer's temporary file. Explicit private creation still rejects an
    existing sibling before consuming any chunk.
    """

    target = Path(path)
    temporary = _temporary_path(target)
    created = False
    try:
        try:
            stream = _open_temporary_stream(temporary, mode)
        except FileNotFoundError:
            target.parent.mkdir(parents=True, exist_ok=True)
            stream = _open_temporary_stream(temporary, mode)
        created = True
        with stream as handle:
            for chunk in chunks:
                handle.write(chunk)
            if durable:
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(temporary, target)
        created = False
        if durable:
            _sync_parent(target)
    finally:
        if created:
            temporary.unlink(missing_ok=True)


def atomic_write_text(
    path: str | Path,
    text: str,
    *,
    encoding: str = "utf-8",
    durable: bool = False,
    mode: int | None = None,
) -> None:
    atomic_write_bytes(path, str(text).encode(encoding), durable=durable, mode=mode)


def atomic_write_json(
    path: str | Path,
    payload: Any,
    *,
    durable: bool = True,
    ensure_ascii: bool = False,
    sort_keys: bool = False,
    compact: bool = False,
    stream: bool = False,
) -> None:
    if stream:
        if not compact:
            raise ValueError("streamed JSON requires compact encoding")

        def chunks():
            yield from iter_compact_json_bytes(payload, ensure_ascii=ensure_ascii, sort_keys=sort_keys)
            yield b"\n"

        atomic_write_chunks(path, chunks(), durable=durable)
        return
    atomic_write_text(
        path,
        json.dumps(
            payload,
            ensure_ascii=ensure_ascii,
            indent=None if compact else 2,
            separators=(",", ":") if compact else None,
            sort_keys=sort_keys,
        )
        + "\n",
        durable=durable,
    )


def iter_compact_json_bytes(
    payload: Any, *, ensure_ascii: bool = False, sort_keys: bool = False,
) -> Iterator[bytes]:
    """Frame outer records; let the standard encoder own all scalar spelling.

    Encoding each list member in one call retains the C encoder's complete
    record path without materializing a catalog-sized text/bytes pair. Other
    values, including unusual JSON mapping keys, keep the standard fallback.
    The byte stream equals json.dumps with the same compact options.
    """

    encoder = json.JSONEncoder(ensure_ascii=ensure_ascii, sort_keys=sort_keys, separators=(",", ":"))

    def values(value):
        if type(value) in (list, tuple):
            yield b"["
            for index, member in enumerate(value):
                if index:
                    yield b","
                yield encoder.encode(member).encode("utf-8")
            yield b"]"
        else:
            yield encoder.encode(value).encode("utf-8")

    if type(payload) is dict and all(type(key) is str for key in payload):
        yield b"{"
        keys = sorted(payload) if sort_keys else payload
        for index, key in enumerate(keys):
            if index:
                yield b","
            yield encoder.encode(key).encode("utf-8")
            yield b":"
            yield from values(payload[key])
        yield b"}"
    else:
        yield from values(payload)


def archive_run_reports(archive_dir: str | Path, paths: tuple[Path, ...]) -> None:
    """Keep a durable per-run copy before mutable latest reports are replaced."""

    destination = Path(archive_dir)
    destination.mkdir(parents=True, exist_ok=True)
    for path in paths:
        atomic_write_bytes(destination / path.name, path.read_bytes(), durable=True)


def atomic_write_parquet(
    path: str | Path,
    frame: Any,
    *,
    compression: str = "zstd",
    write_statistics: bool = True,
    durable: bool = False,
    before_replace: Callable[[], None] | None = None,
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
    published = False
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
        if before_replace is not None:
            before_replace()
        os.replace(temporary, target)
        published = True
        if durable:
            _sync_parent(target)
    finally:
        if not published:
            temporary.unlink(missing_ok=True)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: str | Path, *, chunk_bytes: int = _COPY_CHUNK_BYTES) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_bytes), b""):
            digest.update(chunk)
    return digest.hexdigest()
