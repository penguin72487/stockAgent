"""Withdraw an inactive Vast-owned old head after an exact Penguin repack.

Only the small publisher-head name is moved to the remote owner's local journal;
neither packed objects nor manifests are deleted. Penguin's existing retirement
owner makes the later, separately guarded decision about old D objects.
"""
from __future__ import annotations

from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import time

from stockagent.data_sync import materialized_cache as cache, packed_snapshots as p
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.foreign_head_handoff.v1'
_RETIRED_LAB203_DATASET = 'stockagent-migration-core'
_RETIRED_LAB203_MANIFEST = 'd2c97c13d2b9e04a30a2f2ac119f815f6a7d5939bd83fb3e795c20346f9cce56'
_RETIRED_LAB203_SNAPSHOT = 'stockagent-migration-core-20260812T030055076520750Z-l0-lab203-a4d130cbc3aacb1b'


def _regular(path: Path, *, maximum: int = 65536) -> bytes:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise SnapshotError('foreign-head metadata is missing or redirected')
    with path.open('rb') as source:
        data = source.read(maximum + 1)
    if len(data) > maximum:
        raise SnapshotError('foreign-head metadata is oversized')
    return data


def _request(value: dict) -> dict:
    required = {
            'dataset', 'old_manifest_sha256', 'old_snapshot_id',
            'new_manifest_sha256', 'new_snapshot_id'}
    if not isinstance(value, dict) or set(value) not in (required, required | {'old_publisher'}):
        raise SnapshotError('foreign-head request identity is invalid')
    result = dict(value)
    result['dataset'] = p.validate_slug(value['dataset'], 'dataset')
    for key in ('old_manifest_sha256', 'new_manifest_sha256'):
        result[key] = p._validate_hash(value[key], key)
    for key in ('old_snapshot_id', 'new_snapshot_id'):
        result[key] = p.validate_slug(value[key], key)
    if result['old_manifest_sha256'] == result['new_manifest_sha256']:
        raise SnapshotError('foreign-head successor is not distinct')
    if ('old_publisher' in result and (result['old_publisher'] != 'lab203'
            or result['dataset'] != _RETIRED_LAB203_DATASET
            or result['old_manifest_sha256'] != _RETIRED_LAB203_MANIFEST
            or result['old_snapshot_id'] != _RETIRED_LAB203_SNAPSHOT)):
        raise SnapshotError('foreign-head retired publisher is not the fixed enrolled generation')
    return result


def _old_publisher(request: dict) -> str:
    if 'old_publisher' not in request:
        return 'vastai1T'
    if (request.get('old_publisher') == 'lab203'
            and request.get('dataset') == _RETIRED_LAB203_DATASET
            and request.get('old_manifest_sha256') == _RETIRED_LAB203_MANIFEST
            and request.get('old_snapshot_id') == _RETIRED_LAB203_SNAPSHOT):
        return 'lab203'
    raise SnapshotError('foreign-head retired publisher is not the fixed enrolled generation')


def withdraw(cold: Path, materialized: Path, state_root: Path, request: dict, *,
             apply: bool = False, sync_ready=lambda: False, references=cache.process_references_many,
             pins=cache._pinned_snapshot_ids, hold=None) -> dict:
    """Check both generations under existing remote owners, then hand off one head."""
    if type(apply) is not bool:
        raise SnapshotError('foreign-head apply must be explicit')
    req = _request(request)
    cold, materialized, state_root = (Path(item).absolute() for item in (cold, materialized, state_root))
    for path in (cold, materialized, state_root):
        if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
            raise SnapshotError('foreign-head owner root is unavailable or redirected')
    operation = state_root/'operation.lock'
    fd = os.open(operation, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SnapshotError('foreign-head edge owner is not a regular file')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SnapshotError('foreign-head edge owner is busy') from None
        name, old_id, new_id = req['dataset'], req['old_snapshot_id'], req['new_snapshot_id']
        old_publisher = _old_publisher(req)
        locks = cold/'.local-state/locks'
        with ExitStack() as owners:
            owners.enter_context(p._try_publication_lock(locks/'publish-retention-global.lock', shared=True))
            owners.enter_context(p._try_publication_lock(locks/f'publish-{name}.lock'))
            state = json.loads(_regular(state_root/'state.json', maximum=4*1024**2))
            if not isinstance(state, dict) or state.get('mode') != 'index-only':
                raise SnapshotError('foreign-head peer is not the enrolled index-only owner')
            for key in ('hydrating', 'retained_payloads'):
                if not isinstance(state.get(key, {}), dict):
                    raise SnapshotError('foreign-head edge state is malformed')
            if state.get('hydrating', {}).get(name) or state.get('retained_payloads', {}).get(name):
                raise SnapshotError('foreign-head dataset has active hydration or retention')
            if sync_ready() is not True:
                raise SnapshotError('foreign-head peer synchronization is not current')
            old = p.resolve_packed_snapshot_id(cold, name, old_id, require_objects=False)
            new = p.resolve_packed_snapshot_id(cold, name, new_id, require_objects=False)
            if (old.manifest_sha256 != req['old_manifest_sha256']
                    or new.manifest_sha256 != req['new_manifest_sha256']
                    or old.manifest['publisher']['node_id'] != old_publisher
                    or new.manifest['publisher']['node_id'] != 'penguin'
                    or new.manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256')
                       != req['old_manifest_sha256']
                    or old.manifest['source'] != new.manifest['source']
                    or new.manifest.get('schema_version') not in (1, 2)):
                raise SnapshotError('foreign-head successor is not the exact reader-compatible repack')
            heads = cold/'heads'/name
            if heads.is_symlink() or heads.resolve(strict=True) != heads:
                raise SnapshotError('foreign-head namespace is redirected')
            old_head, new_head = heads/(old_publisher+'.json'), heads/'penguin.json'
            new_body = json.loads(_regular(new_head))
            if (new_body.get('dataset') != name or new_body.get('node_id') != 'penguin'
                    or new_body.get('snapshot_id') != new_id
                    or new_body.get('manifest_sha256') != req['new_manifest_sha256']):
                raise SnapshotError('foreign-head successor head differs')
            latest = p.resolve_latest_packed(cold, name, require_objects=False)
            if latest.manifest_sha256 != req['new_manifest_sha256']:
                raise SnapshotError('foreign-head successor is not the current winner')
            if old_id in pins(materialized):
                raise SnapshotError('foreign-head old version is pinned')
            lease = materialized/'.cache-state/leases'/name/(old_id+'.json')
            ready = materialized/name/('.'+old_id+'.READY.json')
            target = materialized/name/old_id
            if any(os.path.lexists(path) for path in (lease, ready, target)):
                raise SnapshotError('foreign-head old version has a local lease or materialization')
            refs = [target, old.manifest_path]
            for descriptor in [old.manifest['archive']['inventory'], *old.manifest['archive']['objects']]:
                payload = p._path_under(cold, descriptor['relpath'], 'old packed member')
                if os.path.lexists(payload):
                    if payload.is_symlink() or not payload.is_file():
                        raise SnapshotError('foreign-head old packed member is redirected')
                    refs.append(payload)
            if references(refs):
                raise SnapshotError('foreign-head old version is used by a running process')
            journal_dir = state_root/'foreign-head-handoffs'/name
            if apply:
                journal_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
                if (journal_dir.is_symlink() or journal_dir.resolve(strict=True) != journal_dir
                        or journal_dir.stat().st_dev != heads.stat().st_dev):
                    raise SnapshotError('foreign-head journal cannot atomically capture this head')
            journal = journal_dir/(req['old_manifest_sha256']+'.json')
            captured = journal_dir/(req['old_manifest_sha256']+'.head.json')
            if not os.path.lexists(old_head):
                if not apply or not journal.is_file() or not captured.is_file():
                    raise SnapshotError('foreign-head old publisher head is missing')
                previous = json.loads(_regular(journal))
                raw = _regular(captured)
                if (previous.get('contract') != CONTRACT or previous.get('request') != req
                        or previous.get('head_sha256') != hashlib.sha256(raw).hexdigest()):
                    raise SnapshotError('foreign-head interrupted journal differs')
                previous.update(state='handed_off', observed_at_epoch=time.time())
                atomic_write_json(journal, previous, durable=True)
                return previous
            raw = _regular(old_head)
            old_body = json.loads(raw)
            if (old_body.get('dataset') != name or old_body.get('node_id') != old_publisher
                    or old_body.get('snapshot_id') != old_id
                    or old_body.get('manifest_sha256') != req['old_manifest_sha256']):
                raise SnapshotError('foreign-head old publisher head differs')
            result = {'contract': CONTRACT, 'request': req, 'state': 'ready' if not apply else 'planned',
                      'head_sha256': hashlib.sha256(raw).hexdigest(), 'observed_at_epoch': time.time(),
                      'source_payload_deleted': False, 'D_old_objects_retired': False}
            if not apply:
                return hold(result, old_head) if hold is not None else result
            if os.path.lexists(captured):
                raise SnapshotError('foreign-head captured name already exists with an active old head')
            if os.path.lexists(journal):
                previous = json.loads(_regular(journal))
                if (previous.get('contract') != CONTRACT or previous.get('request') != req
                        or previous.get('state') != 'planned' or previous.get('head_sha256') != result['head_sha256']):
                    raise SnapshotError('foreign-head pending journal differs')
                result = previous
            else:
                atomic_write_json(journal, result, durable=True)
            # Rename captures the exact name atomically. If Syncthing changed
            # it after our check, restore it rather than withdrawing new work.
            os.rename(old_head, captured)
            p._fsync_directory(heads)
            p._fsync_directory(journal_dir)
            if _regular(captured) != raw:
                if not os.path.lexists(old_head):
                    os.rename(captured, old_head)
                    p._fsync_directory(heads)
                raise SnapshotError('foreign-head changed during capture')
            result.update(state='handed_off', observed_at_epoch=time.time())
            atomic_write_json(journal, result, durable=True)
            return result
    finally:
        os.close(fd)


def restore(cold: Path, state_root: Path, request: dict) -> dict:
    """Undo our own captured head when transport cannot propagate its removal."""
    req = _request(request)
    cold, state_root = Path(cold).absolute(), Path(state_root).absolute()
    name = req['dataset']
    journal_dir = state_root/'foreign-head-handoffs'/name
    journal = journal_dir/(req['old_manifest_sha256']+'.json')
    captured = journal_dir/(req['old_manifest_sha256']+'.head.json')
    fd = os.open(state_root/'operation.lock', os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks = cold/'.local-state/locks'
        with p._try_publication_lock(locks/'publish-retention-global.lock', shared=True), \
                p._try_publication_lock(locks/f'publish-{name}.lock'):
            value = json.loads(_regular(journal))
            if (value.get('contract') != CONTRACT or value.get('request') != req
                    or value.get('state') not in ('planned', 'handed_off', 'restoring', 'restored')):
                raise SnapshotError('foreign-head restore journal differs')
            head = cold/'heads'/name/(_old_publisher(req)+'.json')
            raw = _regular(captured) if os.path.lexists(captured) else _regular(head)
            if value.get('head_sha256') != hashlib.sha256(raw).hexdigest():
                raise SnapshotError('foreign-head restore bytes differ')
            body = json.loads(raw)
            if (body.get('node_id') != _old_publisher(req) or body.get('dataset') != name
                    or body.get('manifest_sha256') != req['old_manifest_sha256']):
                raise SnapshotError('foreign-head captured identity differs')
            if os.path.lexists(head):
                if _regular(head) != raw:
                    raise SnapshotError('foreign-head restore destination changed')
            else:
                value.update(state='restoring', observed_at_epoch=time.time())
                atomic_write_json(journal, value, durable=True)
                os.rename(captured, head)
                p._fsync_directory(head.parent)
                p._fsync_directory(journal_dir)
            value.update(state='restored', observed_at_epoch=time.time())
            atomic_write_json(journal, value, durable=True)
            return value
    finally:
        os.close(fd)


def confirm(cold: Path, materialized: Path, state_root: Path, request: dict, *,
            head_sha: str, sync_ready=lambda: False, references=cache.process_references_many,
            pins=cache._pinned_snapshot_ids) -> dict:
    """Confirm a source-initiated deletion reached this receive-only peer."""
    req = _request(request)
    head_sha = p._validate_hash(head_sha, 'original peer head SHA')
    cold, materialized, state_root = (Path(item).absolute() for item in (cold, materialized, state_root))
    for path in (cold, materialized, state_root):
        if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
            raise SnapshotError('foreign-head owner root is unavailable or redirected')
    fd = os.open(state_root/'operation.lock', os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SnapshotError('foreign-head edge owner is not a regular file')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SnapshotError('foreign-head edge owner is busy') from None
        name, old_id, new_id = req['dataset'], req['old_snapshot_id'], req['new_snapshot_id']
        old_publisher = _old_publisher(req)
        locks = cold/'.local-state/locks'
        with p._try_publication_lock(locks/'publish-retention-global.lock', shared=True), \
                p._try_publication_lock(locks/f'publish-{name}.lock'):
            state = json.loads(_regular(state_root/'state.json', maximum=4*1024**2))
            if not isinstance(state, dict) or state.get('mode') != 'index-only':
                raise SnapshotError('foreign-head peer is not the enrolled index-only owner')
            for key in ('hydrating', 'retained_payloads'):
                if not isinstance(state.get(key, {}), dict):
                    raise SnapshotError('foreign-head edge state is malformed')
            if state.get('hydrating', {}).get(name) or state.get('retained_payloads', {}).get(name):
                raise SnapshotError('foreign-head dataset has active hydration or retention')
            if sync_ready() is not True:
                raise SnapshotError('foreign-head peer synchronization is not current')
            old = p.resolve_packed_snapshot_id(cold, name, old_id, require_objects=False)
            new = p.resolve_packed_snapshot_id(cold, name, new_id, require_objects=False)
            heads = cold/'heads'/name
            if (heads.is_symlink() or heads.resolve(strict=True) != heads
                    or os.path.lexists(heads/(old_publisher+'.json'))):
                raise SnapshotError('foreign-head old peer head is still present')
            current = p.resolve_latest_packed(cold, name, require_objects=False)
            if (old.manifest_sha256 != req['old_manifest_sha256']
                    or new.manifest_sha256 != req['new_manifest_sha256']
                    or old.manifest['publisher']['node_id'] != old_publisher
                    or new.manifest['publisher']['node_id'] != 'penguin'
                    or new.manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256')
                       != req['old_manifest_sha256']
                    or old.manifest['source'] != new.manifest['source']
                    or new.manifest.get('schema_version') not in (1, 2)
                    or current.manifest_sha256 != req['new_manifest_sha256']):
                raise SnapshotError('foreign-head peer successor changed')
            if old_id in pins(materialized):
                raise SnapshotError('foreign-head old version is pinned')
            lease = materialized/'.cache-state/leases'/name/(old_id+'.json')
            ready = materialized/name/('.'+old_id+'.READY.json')
            target = materialized/name/old_id
            if any(os.path.lexists(path) for path in (lease, ready, target)):
                raise SnapshotError('foreign-head old version has a local lease or materialization')
            refs = [target, old.manifest_path]
            for descriptor in [old.manifest['archive']['inventory'], *old.manifest['archive']['objects']]:
                payload = p._path_under(cold, descriptor['relpath'], 'old packed member')
                if os.path.lexists(payload):
                    if payload.is_symlink() or not payload.is_file():
                        raise SnapshotError('foreign-head old packed member is redirected')
                    refs.append(payload)
            if references(refs):
                raise SnapshotError('foreign-head old version is used by a running process')
            return {'contract': CONTRACT, 'request': req, 'state': 'peer_confirmed', 'head_sha256': head_sha,
                    'observed_at_epoch': time.time(), 'source_payload_deleted': False, 'D_old_objects_retired': False}
    finally:
        os.close(fd)


def _remote_sync_ready(*, receive_only=False) -> bool:
    from scripts.configure_artifact_ingress_syncthing import credentials
    from scripts.manage_packed_edge import _convergence, _request_json
    url, key = credentials()
    folder = _request_json(url, key, '/rest/config/folders/stockagent-packed', {})
    # A receive-only replica cannot propagate a locally withdrawn head. Do
    # not create a false handoff; the authority must initiate that change.
    allowed = ('receiveonly',) if receive_only else ('sendreceive', 'sendonly')
    if not isinstance(folder, dict) or folder.get('type') not in allowed:
        return False
    result = _convergence(url, key, 'stockagent-packed', 'penguin')
    checks = result.get('checks') if isinstance(result, dict) else None
    return (result.get('ok') is True and isinstance(checks, dict)
            and bool(checks) and all(value is True for value in checks.values()))


def main() -> int:
    import argparse
    import sys
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--apply', action='store_true')
    group.add_argument('--restore', action='store_true')
    group.add_argument('--hold', action='store_true')
    group.add_argument('--confirm', action='store_true')
    parser.add_argument('--head-sha')
    args = parser.parse_args()
    raw = sys.stdin.buffer.readline(16385) if args.hold else sys.stdin.buffer.read(16385)
    if len(raw) > 16384:
        raise SnapshotError('foreign-head request is oversized')
    request = json.loads(raw)
    if args.restore:
        result = restore(Path('/srv/stockagent-packed'), Path('/var/lib/stockagent-packed-edge'), request)
    elif args.confirm:
        result = confirm(Path('/srv/stockagent-packed'), Path('/srv/stockagent-packed-materialized'),
                         Path('/var/lib/stockagent-packed-edge'), request, head_sha=args.head_sha,
                         sync_ready=lambda: _remote_sync_ready(receive_only=True))
    elif args.hold:
        import select
        def hold(result, old_head):
            print(json.dumps(result, sort_keys=True), flush=True)
            ready, _, _ = select.select([sys.stdin.buffer], [], [], 30)
            command = sys.stdin.buffer.readline(128).strip() if ready else b''
            if command == b'abort':
                return {**result, 'state': 'aborted', 'observed_at_epoch': time.time()}
            if command != b'await-source-scan':
                raise SnapshotError('foreign-head source did not commit its scan')
            deadline = time.monotonic()+60
            while time.monotonic() < deadline:
                if not os.path.lexists(old_head) and _remote_sync_ready(receive_only=True):
                    return {**result, 'state': 'peer_confirmed', 'observed_at_epoch': time.time()}
                time.sleep(1)
            raise SnapshotError('foreign-head source deletion has not synchronized')
        result = withdraw(Path('/srv/stockagent-packed'), Path('/srv/stockagent-packed-materialized'),
                          Path('/var/lib/stockagent-packed-edge'), request,
                          sync_ready=lambda: _remote_sync_ready(receive_only=True), hold=hold)
    else:
        result = withdraw(Path('/srv/stockagent-packed'), Path('/srv/stockagent-packed-materialized'),
                          Path('/var/lib/stockagent-packed-edge'), request,
                          apply=args.apply, sync_ready=_remote_sync_ready)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
