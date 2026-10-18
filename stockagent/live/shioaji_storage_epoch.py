"""Shared mutation epoch for the existing continuous-futures download lock.

This is storage accounting metadata, never a market-data completeness receipt.
Every canonical mutation holds the original lock through its final manifest.
Its inode is retained: atomically replacing a lock would create two owners.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import stat
from typing import Iterator
import uuid


HISTORY_STORAGE_EPOCH = "shioaji_history_storage_mutation_epoch_v1"
MAX_EPOCH_BYTES = 512


def _root(path: Path) -> Path:
    selected = Path(os.path.abspath(path))
    if selected.resolve(strict=True) != selected or not selected.is_dir():
        raise ValueError("history storage root is redirected or not a directory")
    return selected


def _identity(info) -> list[int]:
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_mode, info.st_uid, info.st_nlink]


def _check_lock(path: Path, descriptor: int):
    info = os.fstat(descriptor)
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid != os.geteuid() or info.st_mode & 0o022
            or _identity(info) != _identity(path.lstat())):
        raise ValueError("history storage lock is untrusted or changed")
    return info


@contextmanager
def history_storage_writer(root: Path) -> Iterator[dict]:
    """Fence *all* mutations, including in-place repairs and failure paths.

    Persist a fresh nonce before touching data, so interruption after any write
    invalidates an older census. Failed/no-op attempts may cause a safe rescan;
    they cannot certify an unchanged tree. No login or provider is involved.
    """
    root = Path(root)
    selected = Path(os.path.abspath(root))
    ancestor = selected
    while not ancestor.exists():
        ancestor = ancestor.parent
    _root(ancestor)
    root.mkdir(parents=True, exist_ok=True)
    selected = _root(root)
    path = selected / "download.lock"
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        _check_lock(path, descriptor)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another history reader or downloader holds the lock") from exc
        _check_lock(path, descriptor)
        epoch = {"contract": HISTORY_STORAGE_EPOCH, "scope": selected.name,
                 "nonce": uuid.uuid4().hex}
        raw = json.dumps(epoch, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        os.ftruncate(descriptor, 0)
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written <= 0:
                raise OSError("short history epoch write")
            offset += written
        os.fsync(descriptor)
        yield epoch
    finally:
        os.close(descriptor)


@contextmanager
def history_storage_observation(root: Path) -> Iterator[dict | None]:
    """Read a stable producer epoch under the same nonblocking shared lock.

    Missing/legacy markers, active writers, redirects and untrusted locks never
    authorize cached reuse. Callers retain the full metadata scan fallback.
    """
    descriptor = None
    try:
        selected = _root(root)
        path = selected / "download.lock"
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        _check_lock(path, descriptor)
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        before = _check_lock(path, descriptor)
        raw = os.read(descriptor, MAX_EPOCH_BYTES + 1)
        if len(raw) > MAX_EPOCH_BYTES:
            raise ValueError("oversized history epoch")
        epoch = json.loads(raw)
        if (not isinstance(epoch, dict) or set(epoch) != {"contract", "scope", "nonce"}
                or epoch["contract"] != HISTORY_STORAGE_EPOCH or epoch["scope"] != selected.name
                or not isinstance(epoch["nonce"], str) or len(epoch["nonce"]) != 32
                or any(value not in "0123456789abcdef" for value in epoch["nonce"])):
            raise ValueError("unsupported history epoch")
        if _identity(before) != _identity(_check_lock(path, descriptor)):
            raise ValueError("history epoch changed during observation")
    except (OSError, ValueError, UnicodeError, RuntimeError, RecursionError):
        if descriptor is not None:
            os.close(descriptor)
            descriptor = None
        yield None
        return
    try:
        yield {"epoch": epoch, "lock_identity": _identity(before), "root": str(selected)}
    finally:
        if descriptor is not None:
            os.close(descriptor)
