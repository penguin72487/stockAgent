"""Verified, disposable column projection of the canonical TAIFEX marks ledger.

The ledger and replay receipts remain authoritative and untouched. A changed
source is never assumed to be append-only: reuse verifies the entire retained
prefix SHA256, including in-place edits followed by an append. The only saved
columns are the existing public-curve parser's explicit schema; no values are
rounded, aggregated or quantized here. Query/range/accounting remain callers'.
"""
from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
from threading import Lock
from typing import Any, Iterator

import polars as pl

from downloader.artifact_io import atomic_write_bytes, durable_replace

CONTRACT = "taifex_history_projected_columns_v1"
CACHE_ENV = "STOCKAGENT_TAIFEX_DASHBOARD_CACHE_DIR"
ENABLE_ENV = "STOCKAGENT_TAIFEX_HISTORY_COLUMN_CACHE"
LAYOUT_ENV = "STOCKAGENT_TAIFEX_HISTORY_COLUMN_LAYOUT"
MIN_SOURCE_BYTES = 8 * 1024**2
MAX_DELTA_BYTES = 64 * 1024**2
_BLOCK_BYTES = 1024**2
MAX_GENERATION_ATTEMPTS = 3
_VERIFIED_PAYLOAD_LIMIT = 16
_VERIFIED_PAYLOADS: OrderedDict[tuple[str, tuple[int, ...], str], bool] = OrderedDict()
_VERIFIED_PAYLOAD_LOCK = Lock()


class _SourceGenerationChanged(OSError):
    """Retry a bounded writer race, never treat it as a verified cache hit."""


def _generation(info: os.stat_result) -> tuple[int, ...]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _open_regular(path: Path) -> int:
    # A corrupt cache path must not hang the read-only dashboard on a FIFO.
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ValueError("history cache input must be a regular file")
    return descriptor


def _prefix_digest(descriptor: int, size: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset < size:
        block = os.pread(descriptor, min(_BLOCK_BYTES, size - offset), offset)
        if not block:
            raise ValueError("history source truncated during proof")
        digest.update(block)
        offset += len(block)
    return digest.hexdigest()


def _source_digests(descriptor: int, size: int, previous_size: int) -> tuple[str, str]:
    """Verify the prefix and new complete source in one linear read, not two."""
    digest = hashlib.sha256()
    offset = 0
    previous = ""
    while offset < size:
        boundary = previous_size if offset < previous_size else size
        block = os.pread(descriptor, min(_BLOCK_BYTES, boundary - offset), offset)
        if not block:
            raise ValueError("history source truncated during proof")
        digest.update(block)
        offset += len(block)
        if offset == previous_size:
            previous = digest.copy().hexdigest()
    return previous, digest.hexdigest()


def _remember_verified_payload(path: str, generation: tuple[int, ...], expected: str,
                               verified: bool = True) -> None:
    """Reuse an in-process proof, never a receipt alone or an assumed append.

    The key includes device, inode, size, mtime AND ctime plus the expected
    SHA256. Only a completed digest fenced by matching descriptor/path
    generations may seed a true value. Restart loses all proofs; disk receipts
    still require independent source and payload verification.
    """
    key = path, generation, expected
    with _VERIFIED_PAYLOAD_LOCK:
        _VERIFIED_PAYLOADS[key] = verified
        _VERIFIED_PAYLOADS.move_to_end(key)
        while len(_VERIFIED_PAYLOADS) > _VERIFIED_PAYLOAD_LIMIT:
            _VERIFIED_PAYLOADS.popitem(last=False)


def _verified_payload(path: str, generation: tuple[int, ...], expected: str) -> bool:
    key = path, generation, expected
    with _VERIFIED_PAYLOAD_LOCK:
        if key in _VERIFIED_PAYLOADS:
            _VERIFIED_PAYLOADS.move_to_end(key)
            return _VERIFIED_PAYLOADS[key]
    descriptor = _open_regular(Path(path))
    try:
        verified = (_generation(os.fstat(descriptor)) == generation
                    and _prefix_digest(descriptor, generation[2]) == expected
                    and _generation(os.fstat(descriptor)) == generation
                    and _generation(Path(path).stat()) == generation)
    finally:
        os.close(descriptor)
    _remember_verified_payload(path, generation, expected, verified)
    return verified


def _receipt(path: Path, schema: dict[str, Any]) -> dict:
    try:
        descriptor = _open_regular(path)
        try:
            if os.fstat(descriptor).st_size > 64 * 1024:
                return {}
            value = json.loads(os.read(descriptor, 64 * 1024 + 1))
        finally:
            os.close(descriptor)
        if (not isinstance(value, dict) or value.get("contract") != CONTRACT
                or value.get("polars") != pl.__version__
                or value.get("columns") != [[name, str(dtype)] for name, dtype in schema.items()]):
            return {}
        generation = value.get("source_generation")
        if (not isinstance(generation, list) or len(generation) != 5
                or any(type(number) is not int or number < 0 for number in generation)
                or type(value.get("rows")) is not int or value["rows"] < 0
                or any(not isinstance(value.get(key), str) or len(value[key]) != 64
                       for key in ("source_sha256", "payload_sha256"))):
            return {}
        return value
    except (OSError, ValueError, TypeError):
        return {}


def _publish_receipt(path: Path, value: dict) -> None:
    atomic_write_bytes(path, (json.dumps(value, separators=(",", ":"), allow_nan=False)
                              + "\n").encode(), durable=True, mode=0o600)


def _prepare(source: Path, payload: Path, receipt: Path,
             schema: dict[str, Any]) -> int | None:
    """Return a pinned, verified Parquet inode. The caller holds the lease."""
    source_fd = _open_regular(source)
    old_fd = None
    temporary = None
    try:
        generation = _generation(os.fstat(source_fd))
        size = generation[2]
        if size < MIN_SOURCE_BYTES or os.pread(source_fd, 1, size - 1) != b"\n":
            return None
        old = _receipt(receipt, schema)
        if old.get("source") != str(source):
            old = {}
        if old:
            try:
                payload_generation = _generation(payload.stat())
                if not _verified_payload(str(payload), payload_generation, old["payload_sha256"]):
                    old = {}
            except OSError:
                old = {}
        if old and old["source_generation"] == list(generation):
            if _verified_payload(str(source), generation, old["source_sha256"]):
                return _open_regular(payload)
            old = {}

        previous_size = old["source_generation"][2] if old else 0
        incremental = bool(old and 0 < previous_size <= size
                           and size - previous_size <= MAX_DELTA_BYTES)
        prefix_sha, source_sha = _source_digests(source_fd, size, previous_size if incremental else 0)
        incremental = incremental and prefix_sha == old["source_sha256"]
        if (_generation(os.fstat(source_fd)) != generation
                or _generation(source.stat()) != generation):
            raise _SourceGenerationChanged("history source changed during prefix proof")
        if incremental and previous_size == size:
            _publish_receipt(receipt, {**old, "source_generation": list(generation)})
            _remember_verified_payload(str(source), generation, source_sha)
            return _open_regular(payload)

        old_fd = _open_regular(payload) if incremental else None
        if incremental:
            delta = os.pread(source_fd, size - previous_size, previous_size)
            if len(delta) != size - previous_size:
                return None
            tail = (pl.scan_ndjson(io.BytesIO(delta), schema=schema, low_memory=True)
                    if delta.strip() else pl.DataFrame(schema=schema).lazy())
            frame = pl.concat([pl.scan_parquet(f"/proc/self/fd/{old_fd}"), tail])
        else:
            frame = pl.scan_ndjson(f"/proc/self/fd/{source_fd}", schema=schema, low_memory=True)

        descriptor, name = tempfile.mkstemp(prefix=".history-columns-", suffix=".parquet", dir=payload.parent)
        os.close(descriptor)
        temporary = Path(name)
        frame.sink_parquet(temporary, compression="zstd", compression_level=3,
                          row_group_size=8192, maintain_order=True, engine="streaming")
        if (_generation(os.fstat(source_fd)) != generation
                or _generation(source.stat()) != generation):
            raise _SourceGenerationChanged("history source changed during column build")
        # The sink decoded every source row with the canonical schema. Check
        # the resulting schema/row-count footer and hash the complete payload;
        # pl.len() is a footer operation, not a second full row-group decode.
        check = pl.scan_parquet(temporary)
        if dict(check.collect_schema()) != schema:
            raise ValueError("history cache schema differs from canonical parser")
        rows = check.select(pl.len()).collect().item()
        new_fd = _open_regular(temporary)
        try:
            payload_sha = _prefix_digest(new_fd, os.fstat(new_fd).st_size)
        finally:
            os.close(new_fd)
        durable_replace(temporary, payload)
        temporary = None
        _publish_receipt(receipt, {"contract": CONTRACT, "source": str(source),
            "source_generation": list(generation), "source_sha256": source_sha,
            "payload_sha256": payload_sha, "rows": rows, "polars": pl.__version__,
            "columns": [[name, str(dtype)] for name, dtype in schema.items()]})
        # _source_digests already proved this entire source and both generation
        # fences passed. Do not rehash it on the next same-generation query.
        # The newly published payload is NOT seeded: its next reader still
        # independently verifies the renamed inode against the receipt SHA.
        _remember_verified_payload(str(source), generation, source_sha)
        return _open_regular(payload)
    finally:
        os.close(source_fd)
        if old_fd is not None:
            os.close(old_fd)
        if temporary is not None:
            temporary.unlink(missing_ok=True)  # Only this call's unpublished disposable temporary.


@contextmanager
def history_scan(source: Path, *, schema: dict[str, Any]) -> Iterator[pl.LazyFrame]:
    """Keep the inode and single-flight lease alive through the entire query.

    Cache errors preserve the original NDJSON reader and error classification;
    they cannot convert a corrupt source into a successful empty result.
    """
    root_text = os.environ.get(CACHE_ENV, "").strip()
    enabled = os.environ.get(ENABLE_ENV, "0").strip().lower() in {"1", "true", "yes"}
    if not root_text or not enabled:
        yield pl.scan_ndjson(source, schema=schema, low_memory=True)
        return
    source = source.resolve(strict=True)
    lock_fd = payload_fd = None
    segment_fds: list[int] = []
    try:
        try:
            root = Path(root_text).resolve(strict=True) / "history-columns-v1"
            root.mkdir(mode=0o700, exist_ok=True)
            if root.is_symlink():
                raise ValueError("history cache directory cannot be a symlink")
            os.chmod(root, 0o700)
            key = hashlib.sha256(str(source).encode()).hexdigest()
            payload, receipt = root / f"{key}.parquet", root / f"{key}.json"
            lock_fd = os.open(root / f"{key}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
                raise ValueError("history cache lease must be a regular file")
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            # A normal minute append can overlap the prefix proof. Re-observe
            # the complete generation instead of immediately allocating the
            # multi-GB raw parser. Never bypass the proof or wait indefinitely.
            for attempt in range(MAX_GENERATION_ATTEMPTS):
                try:
                    if os.environ.get(LAYOUT_ENV, "v1") == "base_delta_v2":
                        from stockagent.live.taifex_history_segments import prepare

                        segment_fds = prepare(source, payload, receipt, schema) or []
                    else:
                        payload_fd = _prepare(source, payload, receipt, schema)
                    break
                except _SourceGenerationChanged:
                    if attempt + 1 == MAX_GENERATION_ATTEMPTS:
                        raise
        except (OSError, ValueError, TypeError, pl.exceptions.PolarsError) as error:
            sys.stderr.write(f"taifex-history column_cache_unavailable error={type(error).__name__}\n")
        # Do not catch query failures across the yield or retry half a query.
        # The existing canonical parser decides fallback on source errors.
        if segment_fds:
            yield pl.concat([pl.scan_parquet(f"/proc/self/fd/{fd}") for fd in segment_fds],
                            how="vertical", parallel=False)
        else:
            yield (pl.scan_parquet(f"/proc/self/fd/{payload_fd}") if payload_fd is not None
                   else pl.scan_ndjson(source, schema=schema, low_memory=True))
    finally:
        for fd in segment_fds:
            os.close(fd)
        if payload_fd is not None:
            os.close(payload_fd)
        if lock_fd is not None:
            os.close(lock_fd)
