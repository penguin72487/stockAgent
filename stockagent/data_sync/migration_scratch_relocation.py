"""Relocate one fixed, unfinished private SQLite preparation from C to E.

This changes placement only. It does not claim a D or NAS recovery, and does
not remove the SQLite source database or any cold object. The original
migration operation lock and its durable preparation remain authoritative.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from concurrent.futures import ThreadPoolExecutor

from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync import workspace_relocation as copy_owner
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes, atomic_write_json, sha256_file


def _read(path: Path):
    if path.is_symlink() or path.resolve() != path:
        raise SnapshotError('relocation metadata is redirected')
    return json.loads(path.read_bytes())


def _other_references(operation: Path, preparation: Path, original: Path):
    # Only owner metadata can bind this scratch path. The backup copy's own
    # marker contains its source database, not the staging path. Historical
    # receipts that do bind it must stay fixed and prohibit rebinding.
    files = [operation / 'progress.json']
    for name in ('preparations', 'publication', 'recovery', 'scratch-retirement'):
        files.extend((operation / name).glob('*.json'))
    needle = str(original).encode()
    for path in files:
        if path != preparation and path.is_file() and needle in path.read_bytes():
            raise SnapshotError('another fixed migration receipt references this scratch')


def relocate(operation: Path, dataset: str, source_relative: str, expected_sha256: str) -> dict:
    operation = operation.absolute()
    if (operation.parent != migration.MIGRATION_OPERATIONS
            or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]*', dataset)
            or not re.fullmatch(r'[0-9a-f]{64}', expected_sha256)
            or not source_relative or source_relative.startswith('/')
            or any(part in ('', '.', '..') for part in source_relative.split('/'))):
        raise SnapshotError('invalid fixed scratch relocation identity')
    with migration._operation_owner(operation):
        # A current operation may be about to resume. Never divert its owner
        # or extend its critical section with an unrelated old preparation.
        current = operation.parent.parent / 'current.json'
        if current.exists() and _read(current).get('operation_directory') == str(operation):
            raise SnapshotError('current migration operation cannot be relocated')
        preparation = operation / 'preparations' / (dataset + '.json')
        document = _read(preparation)
        if document.get('contract') != migration.CONTRACT or document.get('dataset') != dataset:
            raise SnapshotError('private preparation identity changed')
        matches = [row for row in document.get('sqlite_states', [])
                   if row.get('source_relative') == source_relative]
        if len(matches) != 1:
            raise SnapshotError('fixed SQLite preparation is absent or ambiguous')
        row = matches[0]
        proof = row.get('consistent_state', {})
        if (proof.get('snapshot_sha256') != expected_sha256
                or type(proof.get('snapshot_bytes')) is not int
                or proof['snapshot_bytes'] <= 0 or not row.get('snapshot_id')):
            raise SnapshotError('SQLite backup identity is incomplete or changed')
        progress = _read(operation / 'progress.json')
        state = progress.get('datasets', {}).get(dataset, {})
        if state.get('snapshot_id') or state.get('state') not in ('pending', 'waiting_retry'):
            raise SnapshotError('preparation has a fixed parent publication or active state')
        if (operation / 'recovery' / (dataset + '.json')).exists():
            raise SnapshotError('preparation already has a fixed recovery receipt')

        key = hashlib.sha256((dataset + '\0' + source_relative).encode()).hexdigest()[:24]
        evidence = operation / 'workspace-relocation' / key
        journal_path, final_path = evidence / 'journal.json', evidence / 'complete.json'
        journal = _read(journal_path) if journal_path.exists() else None
        bound_stage = Path(row['retained_snapshot_staging'])
        if journal is None:
            original = bound_stage
            allowed = operation / 'sqlite-snapshots'
            if (original.parent != allowed or original.resolve() != original
                    or not re.fullmatch(r'sqlite-[0-9a-f]{32}', original.name)):
                raise SnapshotError('SQLite scratch is not owned by this operation')
            if set(p.name for p in original.iterdir()) != {'backup-intent.json', 'snapshot.json', 'snapshot.sqlite3'}:
                raise SnapshotError('SQLite scratch has unexpected members')
            if _read(original / 'snapshot.json') != proof:
                raise SnapshotError('SQLite snapshot proof changed')
            backup = _read(original / 'backup-intent.json')
            if (backup.get('contract') != 'owned_sqlite_backup_intent_v1'
                    or backup.get('source_relative') != source_relative
                    or backup.get('target') != 'snapshot.sqlite3'):
                raise SnapshotError('SQLite backup intent changed')
            _other_references(operation, preparation, original)
            root = migration._migration_scratch_root(operation, create=True)
            if root == operation:
                raise SnapshotError('E workspace is not enrolled for this operation')
            from stockagent.data_sync.workspace_volume import admit
            admit(proof['snapshot_bytes'])
            destination = root / 'sqlite-snapshots' / original.name
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            file_stat = (original / 'snapshot.sqlite3').lstat()
            journal = {'contract': 'private_sqlite_workspace_relocation_v1', 'dataset': dataset,
                'source_relative': source_relative, 'snapshot_sha256': expected_sha256,
                'snapshot_id': row['snapshot_id'], 'source': str(original), 'destination': str(destination),
                'snapshot_bytes': proof['snapshot_bytes'], 'source_allocated_bytes': file_stat.st_blocks * 512,
                'state': 'planned', 'nas_acceptance': False, 'cold_recovery_acceptance': False}
            evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
            atomic_write_json(journal_path, journal)
        else:
            original, destination = Path(journal['source']), Path(journal['destination'])
            expected_root = migration._migration_scratch_root(operation, create=False)
            if (journal.get('contract') != 'private_sqlite_workspace_relocation_v1'
                    or journal.get('dataset') != dataset or journal.get('source_relative') != source_relative
                    or journal.get('snapshot_sha256') != expected_sha256 or journal.get('snapshot_id') != row['snapshot_id']
                    or original.parent != operation / 'sqlite-snapshots'
                    or destination != expected_root / 'sqlite-snapshots' / original.name
                    or bound_stage not in (original, destination)):
                raise SnapshotError('scratch relocation journal binding changed')

        if final_path.exists():
            final = _read(final_path)
            if (bound_stage != destination or final.get('snapshot_sha256') != expected_sha256
                    or not (destination / 'snapshot.sqlite3').is_file()):
                raise SnapshotError('completed scratch relocation binding changed')
            return final

        stream_path = evidence / 'stream-progress.json'
        last_report = [0.0, None]
        def report(relative, phase, count, total):
            if relative != 'snapshot.sqlite3':
                return
            now = time.monotonic()
            if phase != last_report[1] or count == total or now - last_report[0] >= 5:
                atomic_write_json(stream_path, {'state': phase, 'bytes_completed': count,
                    'bytes_total': total, 'observed_at_epoch': time.time(),
                    'acceptance': False, 'source_files_deleted': 0})
                last_report[:] = [now, phase]

        if journal['state'] == 'planned':
            atomic_write_json(stream_path, {'state': 'copy', 'bytes_completed': 0,
                'bytes_total': proof['snapshot_bytes'], 'observed_at_epoch': time.time(),
                'acceptance': False, 'source_files_deleted': 0})
            copied = copy_owner.copy(original, destination, evidence / 'copy', workers=2, progress=report)
            if not copied.get('all_source_sha256_and_destination_readback_verified'):
                raise SnapshotError('E relocation lacks independent readback')
            journal.update(state='copied', copy_receipt=str(evidence / 'copy/copy-result.json'))
            atomic_write_json(journal_path, journal)

        atomic_write_json(stream_path, {'state': 'checking_binding', 'bytes_total': proof['snapshot_bytes'],
            'observed_at_epoch': time.time(), 'acceptance': False, 'source_files_deleted': 0})
        target = destination / 'snapshot.sqlite3'
        if (target.is_symlink() or target.stat().st_size != proof['snapshot_bytes']
                or sha256_file(target) != expected_sha256 or _read(destination / 'snapshot.json') != proof):
            raise SnapshotError('E SQLite snapshot does not match its fixed original proof')
        from stockagent.data_sync.materialized_cache import process_references_many
        references = process_references_many([original / 'snapshot.sqlite3'])
        if references:
            atomic_write_json(stream_path, {'state': 'waiting_process_references',
                'bytes_total': proof['snapshot_bytes'], 'observed_at_epoch': time.time(),
                'acceptance': False, 'source_files_deleted': 0})
            return {'state': 'waiting_process_references', 'source_files_deleted': 0}

        if bound_stage == original:
            _other_references(operation, preparation, original)
            row['retained_snapshot_staging'] = str(destination)
            atomic_write_json(preparation, document)
        journal['state'] = 'bound_to_e'
        journal['preparation_sha256'] = sha256_file(preparation)
        atomic_write_json(journal_path, journal)

        source = original / 'snapshot.sqlite3'
        reclaimed = None
        if source.exists():
            # The exact original signature and content were fixed by the copy
            # owner. A hard link, changed generation or open reference blocks
            # deletion; this is a placement handoff, not a cold/NAS gate.
            with sqlite3.connect(evidence / 'copy/files.sqlite') as index:
                entry = index.execute("SELECT sig,sha,state FROM files WHERE path='snapshot.sqlite3'").fetchone()
            if (not entry or entry[1:] != (expected_sha256, 'verified')
                    or copy_owner.signature(source) != json.loads(entry[0])
                    or source.stat().st_nlink != 1 or process_references_many([source])):
                raise SnapshotError('original SQLite scratch changed or is in use')
            source.unlink()
            fd = os.open(original, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            reclaimed = journal['source_allocated_bytes']
        final = {'contract': journal['contract'], 'state': 'completed', 'dataset': dataset,
            'snapshot_sha256': expected_sha256, 'snapshot_bytes': proof['snapshot_bytes'],
            'wsl_allocated_bytes_reclaimed': reclaimed,
            'reclaimed_bytes_state': 'measured' if reclaimed is not None else 'unknown_after_interruption',
            'source_files_deleted': 0, 'owned_scratch_files_deleted': 1,
            'cold_objects_deleted': 0, 'nas_files_deleted': 0,
            'cold_recovery_acceptance': False, 'nas_acceptance': False,
            'observed_at_epoch': time.time()}
        atomic_write_json(final_path, final)
        return final


def controls_identity(controls: dict) -> str:
    """Stable, path-independent binding for the exact frozen observations."""
    rows = []
    for name, value in sorted(controls.items()):
        if (not isinstance(name, str) or not name or name.startswith('/')
                or any(part in ('', '.', '..') for part in name.split('/'))
                or not isinstance(value, dict) or type(value.get('bytes')) is not int
                or value['bytes'] < 0 or not re.fullmatch(r'[0-9a-f]{64}', str(value.get('sha256', '')))):
            raise SnapshotError('invalid frozen control identity')
        rows.append((name, value['bytes'], value['sha256']))
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def relocate_controls(operation: Path, dataset: str, expected_identity: str) -> dict:
    """Move one fixed old control preparation, not the source or a cold backup.

    The original operation lock, exact preparation identity, generation
    fences, independent E readback and process-reference checks all apply.
    A persisted binding precedes every unlink, so interrupted cleanup resumes
    against the same fixed index and never adopts an unverified replacement.
    """
    operation = operation.absolute()
    if (operation.parent != migration.MIGRATION_OPERATIONS
            or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]*', dataset)
            or not re.fullmatch(r'[0-9a-f]{64}', expected_identity)):
        raise SnapshotError('invalid fixed control relocation identity')
    with migration._operation_owner(operation):
        current = operation.parent.parent / 'current.json'
        if current.exists() and _read(current).get('operation_directory') == str(operation):
            raise SnapshotError('current migration operation cannot be relocated')
        preparation = operation / 'preparations' / (dataset + '.json')
        document = _read(preparation)
        controls = document.get('controls')
        if (document.get('contract') != migration.CONTRACT or document.get('dataset') != dataset
                or document.get('state') != 'prepared' or not isinstance(controls, dict)
                or not controls or controls_identity(controls) != expected_identity):
            raise SnapshotError('fixed control preparation identity changed')
        state = _read(operation / 'progress.json').get('datasets', {}).get(dataset, {})
        if state.get('snapshot_id') or state.get('state') not in ('pending', 'waiting_retry'):
            raise SnapshotError('preparation has a fixed parent publication or active state')
        if (operation / 'recovery' / (dataset + '.json')).exists():
            raise SnapshotError('preparation already has a fixed recovery receipt')

        key = hashlib.sha256(('controls\0' + dataset).encode()).hexdigest()[:24]
        evidence = operation / 'workspace-relocation' / key
        journal_path, final_path = evidence / 'journal.json', evidence / 'complete.json'
        journal = _read(journal_path) if journal_path.exists() else None
        bound_stage = Path(document['controls_staging'])
        if journal is None:
            original = bound_stage
            if (original.parent != operation / 'controls' or original.resolve() != original
                    or not re.fullmatch(r'controls-[0-9a-f]{32}', original.name)):
                raise SnapshotError('control scratch is not owned by this operation')
            intent = original.with_suffix('.intent.json')
            if intent.is_symlink() or not intent.is_file():
                raise SnapshotError('control freeze intent is absent or redirected')
            _other_references(operation, preparation, original)
            root = migration._migration_scratch_root(operation, create=True)
            if root == operation:
                raise SnapshotError('E workspace is not enrolled for this operation')
            from stockagent.data_sync.workspace_volume import admit
            admit(sum(row['bytes'] for row in controls.values()))
            destination = root / 'controls' / original.name
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            intent_bytes = intent.read_bytes()
            journal = {'contract': 'private_controls_workspace_relocation_v1', 'dataset': dataset,
                'controls_identity': expected_identity, 'source': str(original), 'destination': str(destination),
                'files': len(controls), 'logical_bytes': sum(row['bytes'] for row in controls.values()),
                'intent_sha256': hashlib.sha256(intent_bytes).hexdigest(), 'state': 'planned',
                'nas_acceptance': False, 'cold_recovery_acceptance': False}
            evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
            atomic_write_json(journal_path, journal)
        else:
            original, destination = Path(journal['source']), Path(journal['destination'])
            root = migration._migration_scratch_root(operation, create=False)
            if (journal.get('contract') != 'private_controls_workspace_relocation_v1'
                    or journal.get('dataset') != dataset or journal.get('controls_identity') != expected_identity
                    or original.parent != operation / 'controls'
                    or destination != root / 'controls' / original.name
                    or original.resolve() != original or destination.resolve() != destination
                    or bound_stage not in (original, destination)):
                raise SnapshotError('control relocation journal binding changed')
        for name, row in controls.items():
            if Path(row['path']) != bound_stage / name:
                raise SnapshotError('frozen control path changed')
        if final_path.exists():
            final = _read(final_path)
            if bound_stage != destination or final.get('controls_identity') != expected_identity:
                raise SnapshotError('completed control relocation binding changed')
            return final

        intent = original.with_suffix('.intent.json')
        target_intent = destination.with_suffix('.intent.json')
        intent_bytes = intent.read_bytes()
        if intent.is_symlink() or hashlib.sha256(intent_bytes).hexdigest() != journal['intent_sha256']:
            raise SnapshotError('control freeze intent changed')
        if target_intent.exists() or target_intent.is_symlink():
            if target_intent.is_symlink() or target_intent.read_bytes() != intent_bytes:
                raise SnapshotError('E control freeze intent differs')
        else:
            atomic_write_bytes(target_intent, intent_bytes, mode=0o600)

        stream_path = evidence / 'stream-progress.json'
        if journal['state'] == 'planned':
            copied = copy_owner.copy(original, destination, evidence / 'copy', workers=4)
            if not copied.get('all_source_sha256_and_destination_readback_verified'):
                raise SnapshotError('E control relocation lacks independent readback')
            journal.update(state='copied', copy_receipt=str(evidence / 'copy/copy-result.json'))
            atomic_write_json(journal_path, journal)

        with sqlite3.connect(evidence / 'copy/files.sqlite') as index:
            indexed = {name: (json.loads(sig), sha, state) for name, sig, sha, state in index.execute(
                "SELECT path,sig,sha,state FROM files WHERE kind='file'")}
        if set(indexed) != set(controls):
            raise SnapshotError('control scratch namespace differs from the fixed preparation')

        def verify_target(item):
            name, row = item
            path = destination / name
            before = copy_owner.signature(path)
            if (not stat.S_ISREG(before[2]) or before[3] != row['bytes']
                    or indexed[name][1:] != (row['sha256'], 'verified')
                    or sha256_file(path) != row['sha256'] or copy_owner.signature(path) != before):
                raise SnapshotError('E control readback or generation differs')
            return name, before

        atomic_write_json(stream_path, {'state': 'checking_binding', 'files_total': len(controls),
            'bytes_total': sum(row['bytes'] for row in controls.values()),
            'observed_at_epoch': time.time(), 'source_files_deleted': 0})
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix='control-readback') as pool:
            targets = dict(pool.map(verify_target, controls.items()))
        pending, inode_keys, reclaimed, missing = [], set(), 0, 0
        for name in controls:
            path = original / name
            if path.is_symlink() or path.resolve() != path:
                raise SnapshotError('original control path was redirected')
            if not path.exists():
                if journal['state'] != 'bound_to_e':
                    raise SnapshotError('original control disappeared before binding')
                missing += 1
                continue
            signature = copy_owner.signature(path)
            if signature != indexed[name][0] or signature[8] != 1:
                raise SnapshotError('original control generation or link changed')
            info = path.lstat()
            pending.append((name, path, signature, info.st_blocks * 512))
            inode_keys.add((info.st_dev, info.st_ino))
        from stockagent.data_sync.materialized_cache import process_references_many
        if pending and process_references_many([p for _, p, _, _ in pending], inode_keys=inode_keys):
            atomic_write_json(stream_path, {'state': 'waiting_process_references', 'files_total': len(controls),
                'observed_at_epoch': time.time(), 'source_files_deleted': 0})
            return {'state': 'waiting_process_references', 'source_files_deleted': 0}
        if bound_stage == original:
            _other_references(operation, preparation, original)
            for name, row in controls.items():
                row['path'] = str(destination / name)
            document['controls_staging'] = str(destination)
            atomic_write_json(preparation, document)
        journal['state'] = 'bound_to_e'
        journal['preparation_sha256'] = sha256_file(preparation)
        atomic_write_json(journal_path, journal)

        changed_directories = set()
        try:
            for name, path, before, allocated in pending:
                if (copy_owner.signature(path) != before
                        or copy_owner.signature(destination / name) != targets[name]):
                    raise SnapshotError('control generation changed before scratch retirement')
                path.unlink()
                changed_directories.add(path.parent)
                reclaimed += allocated
        finally:
            for parent in changed_directories:
                fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        final = {'contract': journal['contract'], 'state': 'completed', 'dataset': dataset,
            'controls_identity': expected_identity, 'files': len(controls),
            'logical_bytes': sum(row['bytes'] for row in controls.values()),
            'wsl_allocated_bytes_reclaimed': reclaimed,
            'reclaimed_bytes_state': 'partial_after_interruption' if missing else 'measured',
            'owned_scratch_files_deleted': len(pending), 'source_files_deleted': 0,
            'cold_objects_deleted': 0, 'nas_files_deleted': 0,
            'cold_recovery_acceptance': False, 'nas_acceptance': False, 'observed_at_epoch': time.time()}
        atomic_write_json(final_path, final)
        return final
