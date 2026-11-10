#!/usr/bin/env python3
"""Retire an exact D object name after its already-completed E handoff.

This does not decode archives or create backups. It never removes a referenced
manifest, head, E payload, or an object whose current usage is uncertain.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts import stage_d_cold_relocation as staging  # noqa: E402
from scripts.verify_backup_delivery import identity as delivery_identity  # noqa: E402
from stockagent.data_sync.backup_stream import _private_catalog_generation  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory  # noqa: E402
from stockagent.data_sync.materialized_cache import process_references_many  # noqa: E402
from stockagent.data_sync import stream_repack  # noqa: E402
from stockagent.data_sync.packed_backup import object_descriptor, same_file_signature, signature  # noqa: E402
from stockagent.data_sync.packed_snapshots import _try_publication_lock, _validate_manifest  # noqa: E402

CONTRACT = 'stockagent.d_cold_to_e_handoff.v1'


def _identity(value):
    body = {k: v for k, v in value.items() if k != 'identity_sha256'}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=False).encode()).hexdigest()


def _inside(root: Path, relative: str) -> Path:
    path = root/relative
    if (not isinstance(relative, str) or not relative.startswith('objects/')
            or '..' in Path(relative).parts or path.is_symlink()
            or path.resolve(strict=True) != path or not path.is_file()):
        raise SnapshotError('cold object path is missing or redirected')
    return path


def closed_E_handoff(plan: dict, result: dict, destination: Path) -> list[dict]:
    key = plan.get('identity_sha256')
    state = result.get('state')
    if (plan.get('contract') != staging.CONTRACT or key != _identity(plan)
            or result.get('contract') != staging.CONTRACT
            or state not in {'E_exact_object_copy_verified', 'E_write_complete_source_hashed'}
            or result.get('plan_identity_sha256') != key
            or result.get('destination') != str(destination)
            or result.get('D_names_deleted') != 0 or destination.is_symlink()
            or destination.resolve(strict=True) != destination):
        raise SnapshotError('completed E handoff differs from its fixed plan')
    planned = {(r['relative'], r['sha256'], r['bytes']) for r in plan['objects']}
    fast_signatures = {}
    if state == 'E_exact_object_copy_verified':
        envelope = staging._stable_json(destination/'backup-envelope.json')
        envelope_sha = envelope.get('identity_sha256')
        if (not isinstance(envelope_sha, str) or envelope_sha != result.get('delivery_identity_sha256')
                or envelope.get('source_plan_identity_sha256') != plan.get('catalog_identity_sha256')
                or delivery_identity({k: v for k, v in envelope.items() if k != 'identity_sha256'}) != envelope_sha
                or (destination/'READY').is_symlink()
                or (destination/'READY').read_text() != envelope_sha+'\n'):
            raise SnapshotError('E handoff envelope or READY differs')
        members = envelope['files']
        enclosed = {(r['relative'].removeprefix('cold/'), r['sha256'], r['bytes'])
                    for r in members if r['relative'].startswith('cold/')}
    else:
        envelope = staging._stable_json(destination/'write-complete.json')
        if (envelope.get('contract') != staging.CONTRACT+'.write-complete'
                or envelope.get('plan_identity_sha256') != key
                or envelope.get('identity_sha256') != result.get('write_identity_sha256')
                or _identity(envelope) != envelope.get('identity_sha256')
                or envelope.get('source_sha256_computed_while_copying') is not True
                or envelope.get('destination_independent_readback') is not False
                or envelope.get('archive_decoded') is not False):
            raise SnapshotError('E source-hashed write record differs')
        members = envelope['files']
        enclosed = {(r['relative'], r['sha256'], r['bytes']) for r in members}
        fast_signatures = {r['relative']: r.get('E_signature') for r in members}
    if (len(planned) != len(plan['objects']) or planned != enclosed
            or len(members) != len(planned) or plan['files'] != len(planned)
            or plan['bytes'] != sum(r['bytes'] for r in plan['objects'])
            or result.get('files') != plan['files'] or result.get('bytes') != plan['bytes']):
        raise SnapshotError('E handoff object set differs')
    completed_ns = int(result['completed_at_epoch'] * 1_000_000_000) + 1_024
    rows = []
    for row in plan['objects']:
        if object_descriptor(row['relative']) != row['sha256']:
            raise SnapshotError('E handoff object identity differs')
        copied = _inside(destination/'cold', row['relative'])
        info = copied.lstat()
        if (info.st_size != row['bytes'] or info.st_nlink != 1
                or info.st_ctime_ns > completed_ns or info.st_mtime_ns > completed_ns
                or (fast_signatures and not stream_repack._generation_matches(
                    copied, fast_signatures.get(row['relative']), list(signature(copied))))):
            raise SnapshotError('E handoff payload changed after completion')
        rows.append({**row, 'E_signature': list(signature(copied))})
    return rows


def _canonical_catalog_on_system_volume(catalog: Path, generation: list[int]) -> bool:
    """Bind the old service catalog to the current enrolled C service root."""
    if catalog.absolute() != Path('/var/lib/stockagent/backup-stream/catalog.json'):
        return False
    try:
        legacy = Path('/var/lib/stockagent')
        placed = ROOT/'service_data/stockagent'
        return (legacy.is_dir() and not legacy.is_symlink() and placed.is_dir()
                and not placed.is_symlink() and os.path.samefile(legacy, placed)
                and catalog.stat().st_dev == generation[0] == ROOT.stat().st_dev == Path('/').stat().st_dev)
    except OSError:
        return False


def _same_catalog_generation(catalog: Path, recorded, current) -> bool:
    if recorded == current and current is not None:
        return True
    # The private catalog and its signed observation are C service state.
    # Across WSL restarts st_dev can be renumbered; only that field may
    # change, and only when the original bind and system volume are intact.
    return (isinstance(recorded, list) and isinstance(current, list)
            and len(recorded) == len(current) == 9
            and all(type(value) is int and value >= 0 for value in recorded+current)
            and recorded[1:] == current[1:]
            and _canonical_catalog_on_system_volume(catalog, current))


def catalog_index(catalog: Path, observation: Path, targets: set[str], *, cache_path: Path | None = None):
    marker = staging._stable_json(observation)
    generation = _private_catalog_generation(catalog)
    if (marker.get('contract') != 'stockagent.backup_catalog_observation.v1'
            or generation is None or not _same_catalog_generation(catalog, marker.get('catalog_generation'), generation)):
        raise SnapshotError('cold catalog and fixed observation differ')
    cache = staging._stable_json(cache_path) if cache_path is not None and cache_path.is_file() else {}
    cache_body = {k: v for k, v in cache.items() if k != 'identity_sha256'}
    use_cache = (cache.get('contract') == CONTRACT+'.catalog-index'
        and _same_catalog_generation(catalog, cache.get('catalog_generation'), generation)
        and cache.get('catalog_identity_sha256') == marker.get('catalog_identity_sha256')
        and cache.get('identity_sha256') == hashlib.sha256(json.dumps(cache_body, sort_keys=True,
            separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
        and isinstance(cache.get('manifests'), dict) and isinstance(cache.get('orphans'), dict))
    if use_cache:
        metadata, orphans = cache['manifests'], cache['orphans']
    else:
        import ijson  # Metadata-only journal readers need no catalog parser.
        with catalog.open('rb') as stream:
            errors = next(ijson.items(stream, 'metadata_errors'), None)
        if errors != []:
            raise SnapshotError('cold catalog has unknown or invalid metadata')
        metadata, orphans = {}, {}
        with catalog.open('rb') as stream:
            for row in ijson.items(stream, 'files.item'):
                relative = row.get('relative', '')
                if relative.startswith('manifests/') and row.get('role') == 'cold_metadata':
                    if relative in metadata or not isinstance(row.get('signature'), list):
                        raise SnapshotError('cold catalog has duplicate or invalid manifest metadata')
                    metadata[relative] = row['signature']
                if row.get('role') == 'unreferenced_cold_object':
                    if relative in orphans:
                        raise SnapshotError('cold catalog contains a duplicate object name')
                    orphans[relative] = {'relative': relative, 'sha256': row.get('sha256'),
                                         'role': row['role']}
        if cache_path is not None:
            body = {'contract': CONTRACT+'.catalog-index', 'catalog_generation': generation,
                    'catalog_identity_sha256': marker.get('catalog_identity_sha256'),
                    'manifests': metadata, 'orphans': orphans}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True,
                separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
            atomic_write_json(cache_path, body, durable=True)
    target_roles = {relative: orphans[relative] for relative in targets if relative in orphans}
    if (_private_catalog_generation(catalog) != generation
            or not _same_catalog_generation(catalog,
                    staging._stable_json(observation).get('catalog_generation'), generation)
            or set(target_roles) != targets
            or any(row.get('role') != 'unreferenced_cold_object' for row in target_roles.values())):
        raise SnapshotError('catalog no longer proves all selected objects unreferenced')
    return metadata, target_roles, marker['catalog_identity_sha256']


def check_manifests(cold: Path, metadata: dict, targets: set[str]) -> dict:
    """Reuse unchanged catalog metadata; read only new or changed manifests."""
    count = checked = 0
    with os.scandir(cold/'manifests') as datasets:
        for dataset in datasets:
            if not dataset.is_dir(follow_symlinks=False):
                raise SnapshotError('cold manifest namespace has an unexpected entry')
            with os.scandir(dataset.path) as manifests:
                for item in manifests:
                    if not item.name.endswith('.json'):
                        continue
                    if not item.is_file(follow_symlinks=False):
                        raise SnapshotError('cold manifest is not a regular file')
                    path = Path(item.path)
                    relative = path.relative_to(cold).as_posix()
                    before = list(signature(path))
                    count += 1
                    if same_file_signature(metadata.get(relative), tuple(before)):
                        continue
                    with path.open('rb') as stream:
                        value = json.load(stream)
                    if list(signature(path)) != before:
                        raise SnapshotError('manifest changed during reference check')
                    _validate_manifest(value)
                    if value['dataset'] != dataset.name or value['snapshot_id'] != path.stem:
                        raise SnapshotError('manifest identity differs from its path')
                    refs = [value['archive']['inventory'], *value['archive']['objects']]
                    if targets.intersection(str(ref['relpath']) for ref in refs):
                        raise SnapshotError('a new or changed manifest uses a selected object')
                    checked += 1
    return {'manifest_count': count, 'new_or_changed_manifests_checked': checked}


@contextmanager
def _existing_lock(path: Path):
    fd = os.open(path, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or not os.path.samestat(info, path.lstat())):
            raise SnapshotError('existing original owner lock differs')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if not os.path.samestat(info, path.lstat()):
            raise SnapshotError('original owner lock changed')
        yield
    finally:
        os.close(fd)


def retire(plan: dict, result: dict, *, cold: Path, destination: Path, journal: Path,
           catalog: Path, observation: Path, extra_locks: tuple[Path, ...],
           authority_guard, apply: bool) -> dict:
    rows = closed_E_handoff(plan, result, destination)
    authority_guard()
    with ExitStack() as stack:
        stack.enter_context(_try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock'))
        for lock in extra_locks:
            stack.enter_context(_existing_lock(lock))
        authority_guard()
        state = None
        if journal.exists() or journal.is_symlink():
            state = staging._stable_json(journal)
            if (state.get('contract') != CONTRACT or state.get('plan_identity_sha256') != plan['identity_sha256']
                    or state.get('destination') != str(destination)
                    or state.get('state') not in {'handoff_started', 'D_names_removed_E_copy_retained'}
                    or not isinstance(state.get('removed'), list)):
                raise SnapshotError('existing handoff journal differs')
            if state['state'] == 'D_names_removed_E_copy_retained':
                return state
        removed = {r['relative']: r for r in state['removed']} if state else {}
        if len(removed) != len(state['removed'] if state else []):
            raise SnapshotError('handoff journal contains duplicate removed names')
        planned = {r['relative']: r for r in rows}
        if state and state.get('next_relative') is not None and state['next_relative'] not in planned:
            raise SnapshotError('handoff journal pending name is not planned')
        if any(name not in planned or item.get('bytes') != planned[name]['bytes'] for name, item in removed.items()):
            raise SnapshotError('handoff journal names an unplanned object')
        remaining = []
        for row in rows:
            source = cold/row['relative']
            if source.is_symlink():
                raise SnapshotError('cold object was redirected during resume')
            if row['relative'] in removed:
                if source.exists():
                    raise SnapshotError('previously removed cold name reappeared')
            elif not source.exists():
                if not state or state.get('next_relative') != row['relative']:
                    raise SnapshotError('unrecorded cold name disappeared')
                state['removed'].append({'relative': row['relative'], 'bytes': row['bytes'],
                    'last_link_allocated_bytes': None, 'reconciled_after_interruption': True})
                state.pop('next_relative', None)
                if apply:
                    atomic_write_json(journal, state, durable=True)
            else:
                remaining.append(row)
        targets = {r['relative'] for r in remaining}
        metadata, roles, catalog_identity = catalog_index(catalog, observation, targets,
                                                          cache_path=journal.parent/'catalog-index-cache.json')
        if any(roles[r['relative']]['sha256'] != r['sha256'] for r in remaining):
            raise SnapshotError('current catalog object identity differs')
        manifest_proof = check_manifests(cold, metadata, targets)
        sources = [_inside(cold, row['relative']) for row in remaining]
        inode_keys = set()
        for row, path in zip(remaining, sources, strict=True):
            info = path.lstat()
            if (not stream_repack._generation_matches(path, row['signature'], list(signature(path)))
                    or info.st_nlink != 1):
                raise SnapshotError('D object generation or link count changed')
            inode_keys.add((info.st_dev, info.st_ino))
        references = process_references_many(sources, inode_keys=inode_keys)
        if references:
            raise SnapshotError('a live process uses a selected cold object')
        if state is None:
            state = {'contract': CONTRACT, 'state': 'dry_run_passed',
                     'plan_identity_sha256': plan['identity_sha256'], 'catalog_identity_sha256': catalog_identity,
                     'destination': str(destination), 'files': len(rows), 'bytes': sum(r['bytes'] for r in rows),
                     'removed': [], 'manifest_check': manifest_proof, 'no_archive_decode': True,
                     'NAS_or_backup_required': False, 'observed_at_epoch': time.time()}
        else:
            state.update(catalog_identity_sha256=catalog_identity, manifest_check=manifest_proof)
        if not apply:
            return state
        state['state'] = 'handoff_started'
        atomic_write_json(journal, state, durable=True)
        for row, source in zip(remaining, sources, strict=True):
            authority_guard()
            copied = _inside(destination/'cold', row['relative'])
            if (not stream_repack._generation_matches(copied, row['E_signature'], list(signature(copied)))
                    or not stream_repack._generation_matches(source, row['signature'], list(signature(source)))):
                raise SnapshotError('source or E payload changed during handoff')
            info = source.lstat()
            state['next_relative'] = row['relative']
            atomic_write_json(journal, state, durable=True)
            os.unlink(source)
            _fsync_directory(source.parent)
            state['removed'].append({'relative': row['relative'], 'bytes': row['bytes'],
                                     'last_link_allocated_bytes': info.st_blocks * 512})
            state.pop('next_relative')
            atomic_write_json(journal, state, durable=True)
        state['state'] = 'D_names_removed_E_copy_retained'
        state['completed_at_epoch'] = time.time()
        atomic_write_json(journal, state, durable=True)
        return state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if len(args.batch) != 64 or any(c not in '0123456789abcdef' for c in args.batch):
        parser.error('batch must be a full fixed plan SHA-256')
    from stockagent.data_sync import workspace_volume as volume
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    config = volume.configuration()
    root = volume.path('migration')/'d-cold-object-staging-v1'
    plan = staging._stable_json(root/(args.batch+'.plan.json'))
    result = staging._stable_json(root/(args.batch+'.result.json'))
    def guard():
        _check_d_primary_mount(staging.COLD)
        volume.check(c=config)
    proof = retire(plan, result, cold=staging.COLD, destination=root/(args.batch+'.payload'),
        journal=root/(args.batch+'.handoff.json'), catalog=staging.CATALOG,
        observation=staging.OBSERVATION,
        extra_locks=(Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                     Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')),
        authority_guard=guard, apply=args.apply)
    print(json.dumps(proof, ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__':
    os.umask(0o077)
    raise SystemExit(main())
