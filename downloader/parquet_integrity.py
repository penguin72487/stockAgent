"""Receipt/file integrity, separate from completeness and PIT eligibility."""
from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
import re
import time

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file


VERIFICATION_CACHE_CONTRACT = 1


def read_verification_cache(path: Path) -> dict:
    """Optional local acceleration evidence, never a substitute for receipts."""
    try:
        if path.is_symlink():
            return {}
        with path.open('rb') as stream:
            raw = stream.read(8 * 1024**2 + 1)
        if len(raw) > 8 * 1024**2:
            return {}
        payload = json.loads(raw)
        if (isinstance(payload, dict) and payload.get('contract_version') == VERIFICATION_CACHE_CONTRACT
                and isinstance(payload.get('files'), dict)):
            return payload['files']
    except (OSError, ValueError):
        pass
    return {}


def write_verification_cache(path: Path, cache: dict) -> bool:
    """Only background producers persist memoized checks; readers stay read-only.

    Atomic concurrent writers may discard another process's cache additions,
    causing a recheck, never accepting a different file or receipt. Bound the
    cache to today's proofs; source observations/receipts are never rewritten.
    """
    epoch = int(time.time() // 86400)
    files = {key: value for key, value in cache.items()
             if isinstance(value, list) and len(value) == 11 and value[-1] == epoch}
    if len(files) > 20000 or path.is_symlink():
        return False
    try:
        atomic_write_json(path, {'contract_version': VERIFICATION_CACHE_CONTRACT, 'files': files})
    except OSError:
        return False  # Optional cache failure must not fail a valid acquisition.
    return True


def signature(path: Path) -> tuple[int, ...]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


@lru_cache(maxsize=8192)
def _checked(path: str, before: tuple[int, ...], rows: int, size: int,
             digest: str, verify_hash: bool, verification_epoch: int = 0) -> str | None:
    source = Path(path)
    if before[2] < 12:
        return "empty_or_truncated_parquet"
    if before[2] != size:
        return "parquet_size_mismatch"
    try:
        footer = pq.ParquetFile(source)
        if footer.metadata.num_rows != rows:
            return "parquet_row_mismatch"
        if verify_hash and sha256_file(source) != digest:
            return "parquet_hash_mismatch"
        if signature(source) != before:
            return "file_changed_during_check"
    except (OSError, ValueError):
        return "unreadable_parquet"
    return None


def parquet_receipt_error(root: Path, receipt: dict, *, verify_hash: bool = True,
                          verification_cache: dict | None = None) -> str | None:
    """Return a reason on failure; warm checks are keyed by inode/ctime/mtime.

    The footer proves rows, not economic meaning. Legitimate nulls and measured
    zeros are never replaced. A caller must separately verify receipt identity.
    """
    relative = receipt.get("parquet_path")
    if not isinstance(relative, str) or not relative:
        return "missing_parquet_reference"
    part = Path(relative)
    if part.is_absolute() or ".." in part.parts:
        return "unsafe_parquet_path"
    source = root / part
    if source.is_symlink() or not source.resolve().is_relative_to(root.resolve()):
        return "unsafe_parquet_path"
    rows, size, digest = (receipt.get(key) for key in ("rows", "parquet_size_bytes", "sha256"))
    if (type(rows) is not int or rows < 0 or type(size) is not int or size < 12
            or not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None):
        return "invalid_parquet_receipt"
    try:
        before = signature(source)
    except FileNotFoundError:
        return "missing_parquet"
    except OSError:
        return "unreadable_parquet"
    resolved = str(source.resolve())
    # A positive memo binds the full receipt, file identity and mutation clocks.
    # Recheck at least daily even when filesystem metadata did not change.
    # Footer-only validation must never populate a SHA-256 verification memo.
    memo = verification_cache if verify_hash and isinstance(verification_cache, dict) else None
    epoch = int(time.time() // 86400) if memo is not None else 0
    binding = [VERIFICATION_CACHE_CONTRACT, resolved, *before, rows, size, digest, epoch]
    if memo is not None and memo.get(resolved) == binding:
        try:
            return None if signature(source) == before else 'file_changed_during_check'
        except OSError:
            return 'unreadable_parquet'
    result = _checked(resolved, before, rows, size, digest, verify_hash, epoch)
    if memo is not None:
        if result is None:
            memo[resolved] = binding
        else:
            memo.pop(resolved, None)
    return result
