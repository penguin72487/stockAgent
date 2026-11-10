"""Durable exact rescue-input references for the existing retention owner.

An enrolled source can be missing today and reappear later. Keep its identity
without creating a fake packed pin or blocking unrelated releases. Writers
must hold the original rescue owner and the shared packed publication owner;
retirement rechecks this registry inside that same publication boundary.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat

from stockagent.data_sync.desync_snapshots import SnapshotError, _canonical_json_bytes, atomic_write_bytes
from stockagent.data_sync.packed_snapshots import validate_slug

CONTRACT = 'stockagent.rescue_source_references.v1'
MARKER_CONTRACT = 'stockagent.rescue_source_references_enabled.v1'
REGISTRY_NAME = 'rescue-source-references-v1.json'
MARKER_NAME = 'rescue-source-references-enabled-v1.json'
MAX_BYTES = 1024*1024
MAX_BINDINGS = 4096


def _identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=False).encode()).hexdigest()


def _bindings(rows) -> list[dict]:
    if not isinstance(rows, list) or len(rows) > MAX_BINDINGS:
        raise SnapshotError('rescue source reference list exceeds its bound')
    result, snapshots = [], {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'dataset', 'snapshot_id', 'manifest_sha256'}:
            raise SnapshotError('rescue source reference has invalid fields')
        dataset = validate_slug(row['dataset'], 'rescue source dataset')
        snapshot = validate_slug(row['snapshot_id'], 'rescue source snapshot')
        digest = row['manifest_sha256']
        if not isinstance(digest, str) or re.fullmatch(r'[a-f0-9]{64}', digest) is None:
            raise SnapshotError('rescue source reference has invalid manifest SHA')
        previous = snapshots.get(snapshot)
        if previous is not None and previous != (dataset, digest):
            raise SnapshotError('rescue source snapshot has conflicting bindings')
        if previous is None:
            snapshots[snapshot] = (dataset, digest)
            result.append({'dataset': dataset, 'snapshot_id': snapshot, 'manifest_sha256': digest})
    return sorted(result, key=lambda item: (item['dataset'], item['snapshot_id'], item['manifest_sha256']))


def _read(path: Path, limit: int) -> dict | None:
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise SnapshotError('rescue reference metadata path is redirected')
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= limit:
            raise SnapshotError('rescue reference metadata is not a bounded regular file')
        raw = bytearray()
        while len(raw) <= limit:
            chunk = os.read(fd, min(65536, limit+1-len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        after, current = os.fstat(fd), path.lstat()
        fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if (len(raw) != before.st_size or any(getattr(before, key) != getattr(after, key)
                or getattr(before, key) != getattr(current, key) for key in fields)):
            raise SnapshotError('rescue reference metadata changed during read')
        def unique_fields(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise SnapshotError('duplicate rescue reference metadata field')
                value[key] = item
            return value
        value = json.loads(raw, object_pairs_hook=unique_fields)
        if not isinstance(value, dict):
            raise SnapshotError('rescue reference metadata is not an object')
        return value
    except (OSError, ValueError, TypeError, RecursionError) as error:
        raise SnapshotError('cannot read fixed rescue reference metadata') from error
    finally:
        os.close(fd)


def load(state_root: Path) -> list[dict]:
    root = Path(state_root).absolute()
    marker = _read(root/MARKER_NAME, 4096)
    value = _read(root/REGISTRY_NAME, MAX_BYTES)
    if marker is None and value is None:
        return []  # A legacy installation has not enrolled this protection.
    if marker is not None and (marker != {'contract': MARKER_CONTRACT, 'schema_version': 1}
                               or type(marker.get('schema_version')) is not int):
        raise SnapshotError('rescue reference protection marker is invalid')
    if value is None:
        raise SnapshotError('enabled rescue reference registry is missing')
    body = {k: v for k, v in value.items() if k != 'identity_sha256'}
    if (set(body) != {'contract', 'schema_version', 'bindings'} or body['contract'] != CONTRACT
            or type(body['schema_version']) is not int or body['schema_version'] != 1
            or value.get('identity_sha256') != _identity(body)):
        raise SnapshotError('rescue source reference registry identity is invalid')
    normalized = _bindings(body['bindings'])
    if normalized != body['bindings']:
        raise SnapshotError('rescue source references are duplicate or unsorted')
    return normalized


def _add_under_owner(state_root: Path, rows: list[dict]) -> list[dict]:
    """Add exact bindings monotonically while caller holds publication owner."""
    root = Path(state_root).absolute()
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('rescue reference state root unavailable or redirected')
    previous = load(root)
    normalized = _bindings(previous + rows)
    body = {'contract': CONTRACT, 'schema_version': 1, 'bindings': normalized}
    registry, marker = root/REGISTRY_NAME, root/MARKER_NAME
    if (normalized != previous or not registry.exists()
            or stat.S_IMODE(registry.lstat().st_mode) != 0o600):
        atomic_write_bytes(registry, _canonical_json_bytes({**body, 'identity_sha256': _identity(body)}),
                           mode=0o600, durable=True)
    if not marker.exists() or stat.S_IMODE(marker.lstat().st_mode) != 0o600:
        atomic_write_bytes(marker, _canonical_json_bytes({'contract': MARKER_CONTRACT, 'schema_version': 1}),
                           mode=0o600, durable=True)
    if load(root) != normalized:
        raise SnapshotError('rescue reference registry changed during commit')
    return normalized


def protect(cold: Path, state_root: Path, rows: list[dict], *, guard=None) -> list[dict]:
    """Commit references while holding the original shared publication lock."""
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync.cold_primary import _check_d_primary_mount

    rows = _bindings(rows)
    cold = Path(cold).absolute()
    check = guard or _check_d_primary_mount
    check(cold)
    owner = cold/'.local-state/locks/publish-retention-global.lock'
    if owner.is_symlink() or not owner.is_file():
        raise SnapshotError('rescue source publication owner unavailable or redirected')
    with packed._try_publication_lock(owner, shared=True):
        check(cold)
        result = _add_under_owner(state_root, rows)
        check(cold)
        return result
