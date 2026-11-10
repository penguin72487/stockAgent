"""Atomically hand a prepared E candidate to the existing stream owner.

Both directories are on the enrolled E filesystem. The old incomplete stream
checkpoint is retained at the candidate's former path; no payload is copied or
deleted and the D head is not changed. A small durable journal makes an
interruption before or after the directory exchange distinguishable.
"""
from __future__ import annotations

import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from stockagent.data_sync import dedup_stream_candidate as candidate, packed_snapshots as p, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.dedup_stream_stage_handoff.v1'


def lock_path(candidate_parent: Path, dataset: str) -> Path:
    return Path(candidate_parent)/(p.validate_slug(dataset, 'dataset')+'.handoff.lock')


def _closed_history_metadata(stage: Path, dataset: str, expected: str) -> set[str]:
    """Retain validated history receipts that travelled with an old E stage."""
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    allowed = set()
    status = stage/'prepared-history-sweep.json'
    if status.exists() or status.is_symlink():
        value = json.loads(read_stable_bytes(status, maximum=65536))
        if (value.get('contract') != 'stockagent.source_local_prepared_history.v1'
                or value.get('dataset') != dataset or value.get('old_manifest_sha256') != expected
                or value.get('state') != 'source_hashed_E_packages_prepared'):
            raise SnapshotError('old E history status identity differs')
        allowed.add(status.name)
    for path in stage.glob('private-prepared-history-retirement-*.json'):
        match = re.fullmatch('private-prepared-history-retirement-([a-f0-9]{64})\\.json', path.name)
        if match is None:
            raise SnapshotError('old E history receipt name differs')
        value = json.loads(read_stable_bytes(path, maximum=16*1024**2))
        plan_name = 'retirement-plan-'+match[1]+'.json'
        plan = json.loads(read_stable_bytes(stage/plan_name, maximum=32*1024**2))
        if (value.get('contract') != 'stockagent.private_stream_repack_history.v1'
                or value.get('dataset') != dataset or value.get('new_manifest_sha256') != expected
                or value.get('state') != 'completed' or value.get('plan_ref') != plan_name
                or value.get('plan_sha256') != match[1] or s._identity(plan) != match[1]
                or value.get('removed_count') != len(plan.get('items', []))):
            raise SnapshotError('old E history receipt is incomplete or its plan differs')
        allowed.update({path.name, plan_name})
    return allowed


def _dir_identity(path: Path) -> list[int]:
    info = path.lstat()
    if (path.is_symlink() or path.resolve(strict=True) != path
            or not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700):
        raise SnapshotError('dedup handoff stage is redirected or not private')
    return [info.st_dev, info.st_ino]


def _same_directory(path: Path, recorded) -> bool:
    # A verified WSL remount may assign a new st_dev to the same E inode.
    return s._generation_matches(path, recorded, _dir_identity(path))


def _exchange(a: Path, b: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    rename = getattr(libc, 'renameat2', None)
    if rename is None:
        raise SnapshotError('atomic same-filesystem directory exchange is unavailable')
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(a), -100, os.fsencode(b), 2):  # AT_FDCWD, RENAME_EXCHANGE
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    p._fsync_directory(a.parent)
    if a.parent != b.parent:
        p._fsync_directory(b.parent)


def _sealed_legacy_preparation(stage: Path, dataset: str, expected: str, original: dict) -> dict | None:
    """Bind an inflated but complete normal encoding before any installation."""
    if not os.path.lexists(stage/'prepared.json'):
        return None
    prepared = s._read_prepared(stage, dataset, expected)
    if (prepared.get('packed_format') is not None
            or prepared.get('old_snapshot_id') != original['snapshot_id']
            or prepared.get('old_inventory_sha256', original['archive']['inventory']['sha256']) != original['archive']['inventory']['sha256']
            or prepared.get('files') != original['source']['files']
            or prepared.get('logical_bytes') != original['source']['logical_bytes']
            or any(os.path.lexists(stage/name) for name in (
                'install.json', 'temporary-authority.json', 'current-head-capacity-relief.json', 'cleanup.json'))):
        raise SnapshotError('sealed old E encoding already has an installation or differs from its source')
    return prepared


def handoff(cold: Path, candidate_stage: Path, stream_stage: Path, dataset: str, expected_sha: str, *,
            materialized: Path, guard=lambda: None) -> dict:
    """Move only names, under the same dataset owner and original source pin."""
    cold, candidate_stage, stream_stage = (Path(x).absolute() for x in (cold, candidate_stage, stream_stage))
    materialized = Path(materialized).absolute()
    dataset = p.validate_slug(dataset, 'dataset')
    expected_sha = p._validate_hash(expected_sha, 'original manifest SHA')
    if (candidate_stage == stream_stage or p._paths_overlap(cold, candidate_stage)
            or p._paths_overlap(cold, stream_stage) or candidate_stage.name != expected_sha
            or stream_stage.name != expected_sha or candidate_stage.parent.name != dataset
            or stream_stage.parent.name != dataset):
        raise SnapshotError('dedup handoff stage identity differs')
    journal_path = candidate_stage.parent/(dataset+'.handoff.json')
    locks = cold/'.local-state/locks'
    with p._try_publication_lock(lock_path(candidate_stage.parent, dataset)), \
            p._try_publication_lock(locks/'publish-retention-global.lock', shared=True), \
            p._try_publication_lock(locks/f'publish-{dataset}.lock'):
        guard()
        latest = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if latest.manifest_sha256 != expected_sha:
            raise SnapshotError('dedup handoff original head changed')
        pin_path = materialized/'.stream-repack-pins'/dataset/(expected_sha+'.pin.json')
        pin = {'contract': s.CONTRACT+'.source-pin', 'dataset': dataset,
               'manifest': {'snapshot_id': latest.manifest['snapshot_id']},
               'manifest_sha256': expected_sha, 'stage': str(stream_stage)}
        s._check_repack_pin((pin_path, pin))
        first, second = _dir_identity(candidate_stage), _dir_identity(stream_stage)
        if first[0] != second[0]:
            raise SnapshotError('dedup handoff directories are not on the same E filesystem')
        if os.path.lexists(journal_path):
            if journal_path.is_symlink() or not journal_path.is_file() or journal_path.stat().st_size > 16384:
                raise SnapshotError('dedup handoff journal is redirected')
            journal = json.loads(journal_path.read_text())
            if (not isinstance(journal, dict) or journal.get('contract') != CONTRACT
                    or journal.get('dataset') != dataset or journal.get('old_manifest_sha256') != expected_sha
                    or journal.get('identity_sha256') != s._identity(journal)
                    or journal.get('state') not in {'planned', 'exchanged'}
                    or journal.get('candidate_stage') != str(candidate_stage)
                    or journal.get('stream_stage') != str(stream_stage)):
                raise SnapshotError('dedup handoff journal identity differs')
        else:
            sealed = candidate.read_sealed(candidate_stage, dataset, expected_sha)
            prepared = s._read_prepared(candidate_stage, dataset, expected_sha)
            legacy_prepared = _sealed_legacy_preparation(stream_stage, dataset, expected_sha, latest.manifest)
            if legacy_prepared is not None:
                candidate_bytes = sum(item['bytes'] for item in prepared['packages']) + prepared['inventory']['bytes']
                legacy_bytes = sum(item['bytes'] for item in legacy_prepared['packages']) + legacy_prepared['inventory']['bytes']
                if candidate_bytes >= legacy_bytes:
                    raise SnapshotError('sealed dedup candidate does not reduce the old E encoding')
            legacy_intent = stream_stage/'intent.json'
            if (legacy_intent.is_symlink() or not legacy_intent.is_file()
                    or json.loads(legacy_intent.read_text()) != {
                        'contract': s.CONTRACT+'.intent', 'dataset': dataset, 'old_manifest_sha256': expected_sha}
                    or any(os.path.lexists(stream_stage/name) for name in
                           ('install.json', 'temporary-authority.json', 'current-head-capacity-relief.json'))
                    or prepared.get('candidate_identity_sha256') != sealed['identity_sha256']):
                raise SnapshotError('dedup handoff original checkpoint or candidate differs')
            if process_references_many([candidate_stage, stream_stage]):
                raise SnapshotError('dedup handoff stage has an active reader or writer')
            journal = {'contract': CONTRACT, 'state': 'planned', 'dataset': dataset,
                       'old_manifest_sha256': expected_sha, 'candidate_stage': str(candidate_stage),
                       'stream_stage': str(stream_stage), 'candidate_directory': first,
                       'legacy_directory': second, 'candidate_sha256': sealed['identity_sha256'],
                       'prepared_sha256': prepared['identity_sha256'],
                       'payload_copied': False, 'payload_deleted': False,
                       'observed_at_epoch': time.time()}
            if legacy_prepared is not None:
                journal['legacy_prepared_sha256'] = legacy_prepared['identity_sha256']
            journal['identity_sha256'] = s._identity(journal)
            atomic_write_json(journal_path, journal, durable=True)
        candidate_identity, legacy_identity = journal.get('candidate_directory'), journal.get('legacy_directory')
        if _same_directory(candidate_stage, candidate_identity) and _same_directory(stream_stage, legacy_identity):
            if journal['state'] != 'planned':
                raise SnapshotError('completed dedup handoff directories unexpectedly reverted')
            if process_references_many([candidate_stage, stream_stage]):
                raise SnapshotError('dedup handoff stage has an active reader or writer')
            if (candidate.read_sealed(candidate_stage, dataset, expected_sha)['identity_sha256'] != journal['candidate_sha256']
                    or s._read_prepared(candidate_stage, dataset, expected_sha)['identity_sha256'] != journal['prepared_sha256']):
                raise SnapshotError('dedup handoff prepared generation changed')
            legacy_prepared = _sealed_legacy_preparation(stream_stage, dataset, expected_sha, latest.manifest)
            if (legacy_prepared or {}).get('identity_sha256') != journal.get('legacy_prepared_sha256'):
                raise SnapshotError('dedup handoff sealed old E generation changed')
            _exchange(candidate_stage, stream_stage)
            first, second = _dir_identity(candidate_stage), _dir_identity(stream_stage)
        if (not _same_directory(candidate_stage, legacy_identity)
                or not _same_directory(stream_stage, candidate_identity)):
            raise SnapshotError('dedup handoff directory generations are unknown')
        if (candidate.read_sealed(stream_stage, dataset, expected_sha)['identity_sha256'] != journal['candidate_sha256']
                or s._read_prepared(stream_stage, dataset, expected_sha)['identity_sha256'] != journal['prepared_sha256']):
            raise SnapshotError('dedup handoff prepared generation changed after exchange')
        s._check_repack_pin((pin_path, pin))
        if journal['state'] != 'exchanged':
            journal.update(state='exchanged', exchanged_at_epoch=time.time())
            journal['identity_sha256'] = s._identity(journal)
            atomic_write_json(journal_path, journal, durable=True)
        return journal


def prune_legacy(cold: Path, legacy_stage: Path, stream_stage: Path, dataset: str, expected_sha: str, *,
                 materialized: Path, guard=lambda: None, progress=None) -> dict:
    """Retire only the incomplete old E encoding after exact candidate handoff.

    The current D head, sealed E payload, source pin and per-name generations
    are checked under the existing owner. This never reads or restores payload.
    The old checkpoint and bounded journal remain as evidence.
    """
    cold, legacy_stage, stream_stage = (Path(x).absolute() for x in (cold, legacy_stage, stream_stage))
    materialized = Path(materialized).absolute()
    dataset = p.validate_slug(dataset, 'dataset')
    expected_sha = p._validate_hash(expected_sha, 'original manifest SHA')
    if (legacy_stage == stream_stage or p._paths_overlap(cold, legacy_stage)
            or p._paths_overlap(cold, stream_stage) or legacy_stage.name != expected_sha
            or stream_stage.name != expected_sha or legacy_stage.parent.name != dataset
            or stream_stage.parent.name != dataset):
        raise SnapshotError('old E checkpoint retirement scope differs')
    handoff_path = legacy_stage.parent/(dataset+'.handoff.json')
    journal_path = legacy_stage.parent/(dataset+'.legacy-prune.json')
    locks = cold/'.local-state/locks'
    with p._try_publication_lock(lock_path(legacy_stage.parent, dataset)), \
            p._try_publication_lock(locks/'publish-retention-global.lock', shared=True), \
            p._try_publication_lock(locks/f'publish-{dataset}.lock'):
        guard()
        latest = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if latest.manifest_sha256 != expected_sha:
            raise SnapshotError('old E checkpoint source head changed')
        pin = materialized/'.stream-repack-pins'/dataset/(expected_sha+'.pin.json')
        s._check_repack_pin((pin, {'contract': s.CONTRACT+'.source-pin', 'dataset': dataset,
            'manifest': {'snapshot_id': latest.manifest['snapshot_id']},
            'manifest_sha256': expected_sha, 'stage': str(stream_stage)}))
        if handoff_path.is_symlink() or not handoff_path.is_file() or handoff_path.stat().st_size > 16384:
            raise SnapshotError('old E checkpoint handoff journal is missing or redirected')
        exchanged = json.loads(handoff_path.read_text())
        if (exchanged.get('contract') != CONTRACT or exchanged.get('state') != 'exchanged'
                or exchanged.get('identity_sha256') != s._identity(exchanged)
                or exchanged.get('dataset') != dataset or exchanged.get('old_manifest_sha256') != expected_sha
                or exchanged.get('candidate_stage') != str(legacy_stage)
                or exchanged.get('stream_stage') != str(stream_stage)
                or not _same_directory(legacy_stage, exchanged.get('legacy_directory'))
                or not _same_directory(stream_stage, exchanged.get('candidate_directory'))):
            raise SnapshotError('old E checkpoint handoff identity differs')
        sealed = candidate.read_sealed(stream_stage, dataset, expected_sha)
        prepared = s._read_prepared(stream_stage, dataset, expected_sha)
        if (sealed['identity_sha256'] != exchanged.get('candidate_sha256')
                or prepared['identity_sha256'] != exchanged.get('prepared_sha256')):
            raise SnapshotError('old E checkpoint replacement candidate differs')
        legacy_prepared = None
        if exchanged.get('legacy_prepared_sha256') is not None:
            # Payloads may already be partly unlinked by this journal. Check
            # the original sealed metadata here; each remaining file still
            # undergoes its exact per-name generation check below.
            legacy_prepared = s._prepared_metadata(legacy_stage, dataset, expected_sha)
            if (legacy_prepared.get('identity_sha256') != exchanged['legacy_prepared_sha256']
                    or legacy_prepared.get('old_snapshot_id') != latest.manifest['snapshot_id']
                    or legacy_prepared.get('old_inventory_sha256', latest.manifest['archive']['inventory']['sha256']) != latest.manifest['archive']['inventory']['sha256']
                    or legacy_prepared.get('files') != prepared['files']
                    or legacy_prepared.get('logical_bytes') != prepared['logical_bytes']
                    or any(os.path.lexists(legacy_stage/name) for name in (
                        'install.json', 'temporary-authority.json', 'current-head-capacity-relief.json', 'cleanup.json'))):
                raise SnapshotError('sealed old E encoding binding differs')
        elif os.path.lexists(legacy_stage/'prepared.json'):
            raise SnapshotError('old E checkpoint acquired an unbound sealed preparation')
        intent = legacy_stage/'intent.json'
        if (intent.is_symlink() or not intent.is_file()
                or json.loads(intent.read_text()) != {'contract': s.CONTRACT+'.intent',
                    'dataset': dataset, 'old_manifest_sha256': expected_sha}):
            raise SnapshotError('old E checkpoint intent differs')
        checkpoint = s._safe_stage_file(legacy_stage, 'checkpoint.sqlite')
        if any(os.path.lexists(legacy_stage/name) for name in ('checkpoint.sqlite-wal', 'checkpoint.sqlite-shm')):
            raise SnapshotError('old E checkpoint is still active')
        with sqlite3.connect(checkpoint.as_uri()+'?mode=ro', uri=True, timeout=0) as db:
            meta = dict(db.execute('SELECT key,value FROM meta'))
            packages = [json.loads(row[0]) for row in db.execute('SELECT body FROM packages ORDER BY number')]
        if (meta != {'contract': s.CONTRACT+'.checkpoint', 'dataset': dataset,
                     'old_manifest_sha256': expected_sha} or len(packages) > 100000):
            raise SnapshotError('old E checkpoint identity differs')
        if legacy_prepared is not None and packages != legacy_prepared['packages']:
            raise SnapshotError('sealed old E checkpoint package set differs')
        names = [item.get('name') for item in packages]
        if (len(names) != len(set(names)) or any(not isinstance(name, str)
                or re.fullmatch(r'pack-[0-9a-f]{64}\.zip', name) is None for name in names)):
            raise SnapshotError('old E checkpoint package list differs')
        if journal_path.is_symlink():
            raise SnapshotError('old E checkpoint retirement journal redirected')
        if journal_path.exists():
            journal = json.loads(journal_path.read_text())
            plan = journal.get('plan') if isinstance(journal, dict) else None
            if (not isinstance(plan, list) or journal.get('contract') != CONTRACT+'.legacy-prune'
                    or journal.get('dataset') != dataset or journal.get('old_manifest_sha256') != expected_sha
                    or journal.get('prepared_sha256') != prepared['identity_sha256']
                    or journal.get('plan_sha256') != hashlib.sha256(p._canonical_json_bytes(plan)).hexdigest()
                    or journal.get('state') not in {'removing', 'completed'}):
                raise SnapshotError('old E checkpoint retirement journal differs')
        else:
            allowed = {'intent.json', 'checkpoint.sqlite', *names}
            if legacy_prepared is not None:
                allowed.update({'prepared.json', legacy_prepared['inventory']['name']})
            allowed.update(_closed_history_metadata(legacy_stage, dataset, expected_sha))
            partials = []
            for path in legacy_stage.iterdir():
                if path.name in allowed:
                    continue
                if (re.fullmatch(r'pack-[0-9a-f]{32}\.partial', path.name) is None
                        or path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1):
                    raise SnapshotError('old E checkpoint contains an unknown member')
                partials.append(path)
            plan = []
            for item in packages:
                path = s._safe_stage_file(legacy_stage, item['name'])
                if not s._signature_matches(path, item.get('E_signature')):
                    raise SnapshotError('old E checkpoint sealed package generation changed')
                info = path.lstat()
                plan.append({'name': path.name, 'signature': s._signature(path),
                             'bytes': info.st_size, 'allocated_bytes': info.st_blocks*512})
            for path in sorted(partials):
                info = path.lstat()
                plan.append({'name': path.name, 'signature': s._signature(path),
                             'bytes': info.st_size, 'allocated_bytes': info.st_blocks*512})
            journal = {'contract': CONTRACT+'.legacy-prune', 'dataset': dataset,
                       'old_manifest_sha256': expected_sha, 'prepared_sha256': prepared['identity_sha256'],
                       'plan': plan, 'plan_sha256': hashlib.sha256(p._canonical_json_bytes(plan)).hexdigest(),
                       'removed': [], 'state': 'removing', 'independent_D_readback': False,
                       'current_D_head_removed': False, 'NAS_accepted': False}
        plan = journal['plan']
        plan_names = [item.get('name') for item in plan if isinstance(item, dict)]
        removed, pending = journal.get('removed'), journal.get('pending')
        if (len(plan_names) != len(plan) or len(set(plan_names)) != len(plan_names)
                or any(not isinstance(item['name'], str)
                       or re.fullmatch(r'pack-(?:[0-9a-f]{64}\.zip|[0-9a-f]{32}\.partial)', item['name']) is None
                       or not isinstance(item.get('signature'), list)
                       or any(type(item.get(field)) is not int or item[field] < 0
                              for field in ('bytes', 'allocated_bytes')) for item in plan)
                or {name for name in plan_names if name.endswith('.zip')} != set(names)
                or not isinstance(removed, list)
                or removed != plan_names[:len(removed)]
                or pending is not None and (len(removed) == len(plan_names) or pending != plan_names[len(removed)])
                or journal['state'] == 'completed' and len(removed) != len(plan_names)):
            raise SnapshotError('old E checkpoint retirement cursor differs')
        planned_packages = {item['name']: item for item in plan if item['name'].endswith('.zip')}
        if any(not s._generation_matches(legacy_stage/item['name'], item['E_signature'],
                                         planned_packages[item['name']]['signature'])
               for item in packages):
            raise SnapshotError('old E checkpoint retirement package binding differs')
        if process_references_many([legacy_stage]):
            raise SnapshotError('old E checkpoint has an active reader or writer')
        for item in plan[len(removed):]:
            guard()
            path = legacy_stage/item['name']
            if os.path.lexists(path):
                if (path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1
                        or not s._signature_matches(path, item.get('signature'))):
                    raise SnapshotError('old E checkpoint member generation changed')
                journal['pending'] = item['name']
                atomic_write_json(journal_path, journal, durable=True)
                path.unlink()
                p._fsync_directory(legacy_stage)
            elif journal.get('pending') != item['name']:
                raise SnapshotError('old E checkpoint member disappeared without intent')
            journal['removed'].append(item['name'])
            journal.pop('pending', None)
            atomic_write_json(journal_path, journal, durable=True)
            if progress is not None:
                progress({'completed_items': len(journal['removed']), 'total_items': len(plan),
                          'reclaimed_allocated_bytes': sum(x['allocated_bytes'] for x in plan[:len(journal['removed'])])})
        journal.update(state='completed', completed_at_epoch=journal.get('completed_at_epoch', time.time()),
                       reclaimed_allocated_bytes=sum(item['allocated_bytes'] for item in plan))
        atomic_write_json(journal_path, journal, durable=True)
        return journal
