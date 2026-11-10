"""Small, source-generation-bound cache for TAIFEX monitor JSON snapshots.

The periodic snapshot owner can reuse unchanged public source receipts across
process restarts. Cache hits stat the original before and after reading the
private cache, and never open its payload. A miss takes the same shared lock as
the collectors; a migration's exclusive owner can therefore make progress.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat

from downloader.artifact_io import atomic_write_bytes
from downloader.dataset_lock import DatasetLockPathChanged, DatasetLockTimeout
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync import taifex_workspace_guard
from stockagent.live.dashboard_updates import metadata_signature, read_json_snapshot
from stockagent.service_layout import service_path

CONTRACT = 'stockagent.taifex_monitor_snapshot_cache.v1'
MAX_BYTES = 16*1024**2
ALLOWED = frozenset({'manifest.json', 'progress.json', 'openapi_latest.json', 'vix_latest.json',
                     'rules/manifest.json', 'rules/progress.json'})
_MISSING = object()


@contextmanager
def _snapshot_guard(source: Path):
    """Share the existing lock without writing into the read-only source tree."""
    from stockagent.data_sync.workspace_source_handoff import guard_locked_path, guard_path

    root = taifex_workspace_guard.SOURCE
    guard_path(root)
    path = root/'.download.lock'
    try:
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except FileNotFoundError:
        # A fresh writable installation may create its initial lock normally.
        with taifex_workspace_guard.writer_guard(source, timeout_seconds=0):
            yield
        return
    with os.fdopen(fd, 'r') as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise DatasetLockTimeout('market workspace handoff owner holds its lock') from error
        try:
            try:
                held, current = os.fstat(handle.fileno()), path.lstat()
            except OSError as error:
                raise DatasetLockPathChanged('market workspace lock disappeared while reading') from error
            if (not stat.S_ISREG(current.st_mode)
                    or (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino)):
                raise DatasetLockPathChanged('market workspace lock changed while reading')
            guard_locked_path(root, handle)
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _scope(path: Path):
    path = Path(os.path.abspath(path))
    for root in path.parents:
        if root.name == 'data_taifex_public_history':
            if root != Path(os.path.abspath(taifex_workspace_guard.SOURCE)):
                return None
            relative = path.relative_to(root).as_posix()
            if relative in ALLOWED:
                return path, root.parent, relative
            break
    return None


def _serialized(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                      allow_nan=True).encode('utf-8')


def _signature(path: Path):
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        return None
    return list(metadata_signature(info))


def _entry_path(repo: Path, relative: str) -> Path:
    name = hashlib.sha256(relative.encode('utf-8')).hexdigest()+'.json'
    return service_path('data_monitor/taifex-source-snapshots-v1', root=repo)/name


def _cached(path: Path, entry: Path, before):
    try:
        if (entry.parent.is_symlink() or entry.parent.resolve(strict=True) != entry.parent
                or entry.is_symlink()):
            return _MISSING
        fd = os.open(entry, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_mode & 0o022 or info.st_size > 2*MAX_BYTES):
                return _MISSING
            value = json.loads(stream.read(2*MAX_BYTES+1))
        payload = value.get('payload') if isinstance(value, dict) else _MISSING
        if (not isinstance(value, dict) or value.get('contract') != CONTRACT
                or value.get('source') != str(path) or value.get('signature') != before
                or value.get('payload_sha256') != hashlib.sha256(_serialized(payload)).hexdigest()
                or _signature(path) != before):
            return _MISSING
        return payload
    except (OSError, TypeError, ValueError):
        return _MISSING


def read(path: Path, default=None, *, populate: bool = False):
    """Return a current source value or default; never reuse a stale cache."""
    scope = _scope(path)
    if scope is None:
        return read_json_snapshot(path, default)
    source, repo, relative = scope
    try:
        before = _signature(source)
    except OSError:
        return default
    if before is None:
        return default
    try:
        entry = _entry_path(repo, relative)
    except (OSError, ValueError):
        return default
    cached = _cached(source, entry, before) if before[2] <= MAX_BYTES else _MISSING
    if cached is not _MISSING:
        return cached
    try:
        with _snapshot_guard(source):
            before = _signature(source)
            value = read_json_snapshot(source, _MISSING)
            if before is None or value is _MISSING or _signature(source) != before:
                return default
            if populate and before[2] <= MAX_BYTES:
                payload = _serialized(value)
                document = {'contract': CONTRACT, 'source': str(source), 'signature': before,
                            'payload_sha256': hashlib.sha256(payload).hexdigest(), 'payload': value}
                try:
                    directory = entry.parent
                    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
                    if directory.is_symlink() or directory.resolve(strict=True) != directory:
                        raise SnapshotError('TAIFEX snapshot cache directory is redirected')
                    atomic_write_bytes(entry, _serialized(document), durable=False, mode=0o600)
                except (OSError, SnapshotError):
                    # A cache filesystem problem must not hide a current source
                    # value that was successfully read under the source lock.
                    pass
            return value
    except (OSError, ValueError, DatasetLockTimeout, DatasetLockPathChanged, SnapshotError):
        return default


def prime(repo: Path) -> None:
    """Refresh only changed fixed sources; called by the existing snapshot owner."""
    root = Path(repo)/'data_taifex_public_history'
    for relative in sorted(ALLOWED):
        read(root/relative, populate=True)
