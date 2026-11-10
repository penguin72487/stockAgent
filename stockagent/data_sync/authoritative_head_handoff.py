"""Journal a source-owned head handoff while a receive-only peer holds its lease."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import time

from stockagent.data_sync import foreign_head_handoff as foreign, packed_snapshots as p, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.authoritative_head_handoff.v1'


def _paths(workspace: Path, cold: Path, req: dict):
    name, digest = req['dataset'], req['old_manifest_sha256']
    stage = workspace/'migration/authoritative-head-handoffs'/name/digest
    head = cold/'heads'/name/(foreign._old_publisher(req)+'.json')
    captured = cold/'.local-state/foreign-head-handoffs'/name/(digest+'.head.json')
    return stage, head, captured


def _record(stage: Path, req: dict) -> dict:
    value = json.loads(foreign._regular(stage/'journal.json'))
    if (not isinstance(value, dict) or value.get('contract') != CONTRACT or value.get('request') != req
            or value.get('identity_sha256') != stream_repack._identity(value)
            or value.get('state') not in ('planned', 'moved', 'peer_confirmed', 'completed', 'rolled_back')):
        raise SnapshotError('authoritative-head journal differs')
    return value


def _save(stage: Path, value: dict) -> None:
    value['updated_at_epoch'] = time.time()
    value['identity_sha256'] = stream_repack._identity(value)
    atomic_write_json(stage/'journal.json', value, durable=True)


def _prepare_dirs(stage: Path, captured: Path, head: Path) -> None:
    for directory in (stage, captured.parent):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if directory.is_symlink() or directory.resolve(strict=True) != directory:
            raise SnapshotError('authoritative-head journal path is redirected')
    if captured.parent.stat().st_dev != head.parent.stat().st_dev:
        raise SnapshotError('authoritative-head capture is not on the source filesystem')


def rollback(workspace: Path, cold: Path, materialized: Path, request: dict, *, notify) -> dict:
    """Restore an interrupted source head; caller holds the original D owners."""
    req = foreign._request(request)
    stage, head, captured = _paths(workspace, cold, req)
    value = _record(stage, req)
    if value['state'] in ('peer_confirmed', 'completed'):
        # A durable peer confirmation means retirement may already have begun.
        # Finish only the exact handoff pin, never recreate an old head.
        pin = stream_repack._repack_pin(materialized, req['dataset'], req['old_manifest_sha256'],
                                        req['old_snapshot_id'], stage, scoped=True)
        stream_repack._clear_repack_pin(pin)
        value['state'] = 'completed'
        _save(stage, value)
        return value
    if value['state'] == 'rolled_back':
        return value
    expected = value.get('old_head_sha256')
    if os.path.lexists(captured):
        raw = foreign._regular(captured)
        if hashlib.sha256(raw).hexdigest() != expected:
            raise SnapshotError('authoritative-head captured bytes changed')
        if os.path.lexists(head):
            if foreign._regular(head) != raw:
                raise SnapshotError('authoritative-head rollback destination changed')
            captured.unlink()
            p._fsync_directory(captured.parent)
        else:
            os.rename(captured, head)
            p._fsync_directory(head.parent)
            p._fsync_directory(captured.parent)
    elif not os.path.lexists(head) or hashlib.sha256(foreign._regular(head)).hexdigest() != expected:
        raise SnapshotError('authoritative-head rollback source is unavailable')
    # Once the original head is present, the normal retirement guard protects
    # it even if the notification cannot be sent immediately.
    notify(req)
    pin_path = materialized/'.stream-repack-pins'/req['dataset']/stream_repack._repack_pin_name(
        req['old_manifest_sha256'], stage, scoped=True)
    if os.path.lexists(pin_path):
        pin = stream_repack._repack_pin(materialized, req['dataset'], req['old_manifest_sha256'],
                                        req['old_snapshot_id'], stage, scoped=True)
        stream_repack._clear_repack_pin(pin)
    value['state'] = 'rolled_back'
    _save(stage, value)
    return value


def commit(workspace: Path, cold: Path, materialized: Path, request: dict, *, peer_head_sha: str, notify) -> dict:
    """Move the old head while retaining a durable pin until a later peer proof."""
    req = foreign._request(request)
    stage, head, captured = _paths(workspace, cold, req)
    _prepare_dirs(stage, captured, head)
    if os.path.lexists(stage/'journal.json'):
        previous = _record(stage, req)
        if previous['state'] != 'rolled_back' or os.path.lexists(captured):
            raise SnapshotError('authoritative-head transaction already exists')
        history = stage/'history'
        history.mkdir(mode=0o700, exist_ok=True)
        if history.is_symlink() or history.resolve(strict=True) != history:
            raise SnapshotError('authoritative-head history is redirected')
        archive = history/(previous['identity_sha256']+'.json')
        if os.path.lexists(archive):
            if json.loads(foreign._regular(archive)) != previous:
                raise SnapshotError('authoritative-head history differs')
        else:
            atomic_write_json(archive, previous, durable=True)
        (stage/'journal.json').unlink()
        p._fsync_directory(stage)
    elif os.path.lexists(captured):
        raise SnapshotError('authoritative-head capture already exists')
    raw = foreign._regular(head)
    body = json.loads(raw)
    if (body.get('dataset') != req['dataset'] or body.get('node_id') != foreign._old_publisher(req)
            or body.get('snapshot_id') != req['old_snapshot_id']
            or body.get('manifest_sha256') != req['old_manifest_sha256']):
        raise SnapshotError('authoritative-head source generation changed')
    if p._validate_hash(peer_head_sha, 'peer head SHA') != hashlib.sha256(raw).hexdigest():
        raise SnapshotError('authoritative-head peer and source head bytes differ')
    value = {'contract': CONTRACT, 'request': req, 'state': 'planned',
             'old_head_sha256': hashlib.sha256(raw).hexdigest(),
             'source_payload_deleted': False, 'D_old_objects_retired': False}
    _save(stage, value)
    pin = None
    try:
        pin = stream_repack._repack_pin(materialized, req['dataset'], req['old_manifest_sha256'],
                                        req['old_snapshot_id'], stage, scoped=True)
        os.rename(head, captured)
        p._fsync_directory(head.parent)
        p._fsync_directory(captured.parent)
        if foreign._regular(captured) != raw:
            raise SnapshotError('authoritative-head changed during atomic capture')
        value['state'] = 'moved'
        _save(stage, value)
        notify(req)
        return value
    except BaseException:
        rollback(workspace, cold, materialized, req, notify=notify)
        raise


def finalize(workspace: Path, cold: Path, materialized: Path, request: dict, ack: dict) -> dict:
    """Clear the exact old pin only after the peer reports the synchronized head."""
    req = foreign._request(request)
    stage, head, captured = _paths(workspace, cold, req)
    value = _record(stage, req)
    if value['state'] in ('peer_confirmed', 'completed'):
        return rollback(workspace, cold, materialized, req, notify=lambda _request: None)
    if (value['state'] != 'moved' or os.path.lexists(head) or not os.path.lexists(captured)
            or hashlib.sha256(foreign._regular(captured)).hexdigest() != value.get('old_head_sha256')):
        raise SnapshotError('authoritative-head source capture is not awaiting peer confirmation')
    if (not isinstance(ack, dict) or ack.get('contract') != foreign.CONTRACT
            or ack.get('request') != req or ack.get('state') != 'peer_confirmed'
            or ack.get('head_sha256') != value['old_head_sha256']
            or ack.get('source_payload_deleted') is not False or ack.get('D_old_objects_retired') is not False):
        raise SnapshotError('authoritative-head peer confirmation differs')
    pin_path = materialized/'.stream-repack-pins'/req['dataset']/stream_repack._repack_pin_name(
        req['old_manifest_sha256'], stage, scoped=True)
    pin = (pin_path, {'contract': stream_repack.CONTRACT+'.source-pin', 'dataset': req['dataset'],
        'manifest': {'snapshot_id': req['old_snapshot_id']},
        'manifest_sha256': req['old_manifest_sha256'], 'stage': str(stage)})
    stream_repack._check_repack_pin(pin)
    value.update(state='peer_confirmed', peer_head_sha256=ack['head_sha256'],
                 peer_observed_at_epoch=ack.get('observed_at_epoch'))
    _save(stage, value)  # Durable confirmation precedes the exact pin removal.
    stream_repack._clear_repack_pin(pin)
    value['state'] = 'completed'
    _save(stage, value)
    return value
