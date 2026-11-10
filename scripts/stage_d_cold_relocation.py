#!/usr/bin/env python3
"""Stage a bounded D object wave to E; optionally hand off its D names.

The fast path hashes source bytes while writing and avoids destination rereads.
Objects without a release association remain retained on E, not dataset packs.
"""
from __future__ import annotations

import argparse
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
from stockagent.data_sync import stream_repack, workspace_volume as volume  # noqa: E402
from stockagent.data_sync.backup_stream import _private_catalog_generation  # noqa: E402
from stockagent.data_sync.completed_forward_capacity import reserve_capacity  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402
from stockagent.data_sync.offhost_backup import copy_verified_bytes, export_incremental_delivery  # noqa: E402
from stockagent.data_sync.packed_backup import object_descriptor, signature  # noqa: E402
from stockagent.data_sync.packed_snapshots import PackedPublicationBusy  # noqa: E402
from stockagent.data_sync.desync_snapshots import _fsync_directory  # noqa: E402

CATALOG = Path('/var/lib/stockagent/backup-stream/catalog.json')
OBSERVATION = Path('/var/lib/stockagent/backup-stream/catalog-observation.json')
LEDGER = Path('/var/lib/stockagent/lakehouse-control/source-replication-ledger.json')
COLD = Path('/srv/stockagent-packed')
CONTRACT = 'stockagent.d_cold_e_staging.v1'


def _signature(s):
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)


def _stable_json(path: Path):
    if path.is_symlink():
        raise SnapshotError('metadata source is redirected')
    with path.open('rb') as stream:
        before = _signature(os.fstat(stream.fileno()))
        value = json.load(stream)
        after = _signature(os.fstat(stream.fileno()))
    if before != after or after != _signature(path.stat()):
        raise SnapshotError('metadata source changed while reading')
    return value


def caught_up_unchanged(previous: dict, root: Path, catalog: Path, observation_path: Path) -> bool:
    from scripts.retire_staged_cold_objects import _same_catalog_generation
    key = previous.get('plan_identity_sha256', '')
    if not isinstance(key, str) or len(key) != 64 or any(c not in '0123456789abcdef' for c in key):
        return False
    try:
        plan = _stable_json(root/(key+'.plan.json'))
        observation = _stable_json(observation_path)
        if not isinstance(plan, dict) or not isinstance(observation, dict):
            return False
        generation = observation.get('catalog_generation')
        return (observation.get('contract') == 'stockagent.backup_catalog_observation.v1'
                and observation.get('catalog_identity_sha256') == plan.get('catalog_identity_sha256')
                and _same_catalog_generation(catalog, generation, _private_catalog_generation(catalog)))
    except (OSError, ValueError, SnapshotError):
        return False


def refresh_caught_up(previous: dict, *, now: float) -> dict:
    """Record the cheap recheck without changing the original scan clock."""
    return {**previous, 'last_checked_at_epoch': now, 'next_scan_epoch': now+60}


def select(catalog: Path, observation: Path, ledger_path: Path | None, maximum_bytes: int,
           *, excluded: frozenset[str] = frozenset()) -> dict:
    """Capture one stable bounded selection; this is not a deletion decision."""
    import ijson  # Only catalog enumeration requires the streaming parser.
    from scripts.retire_staged_cold_objects import _same_catalog_generation
    if type(maximum_bytes) is not int or not 0 < maximum_bytes <= 16 * 1024**3:
        raise SnapshotError('D-to-E batch must have a positive at-most-16-GiB bound')
    before_observation = _stable_json(observation)
    identity = before_observation.get('catalog_identity_sha256')
    generation = _private_catalog_generation(catalog)
    if (before_observation.get('contract') != 'stockagent.backup_catalog_observation.v1'
            or not _same_catalog_generation(catalog, before_observation.get('catalog_generation'), generation)
            or not isinstance(identity, str) or len(identity) != 64
            or any(c not in '0123456789abcdef' for c in identity)):
        raise SnapshotError('fixed catalog identity is unavailable')
    accepted = {}
    if ledger_path is not None:
        ledger = _stable_json(ledger_path)
        for delivery, row in ledger.get('deliveries', {}).items():
            if (row.get('nas_acceptance') or {}).get('state') == 'nas_archive_file_recovery_verified':
                for key in row.get('file_keys', []):
                    accepted.setdefault(key, delivery)
    if catalog.is_symlink():
        raise SnapshotError('cold catalog is redirected')
    oversized = oversized_bytes = 0
    with catalog.open('rb') as stream:
        before = _signature(os.fstat(stream.fileno()))
        rows = []
        for row in ijson.items(stream, 'files.item'):
            if row.get('role') != 'unreferenced_cold_object':
                continue
            relative, sha, count = row.get('relative'), row.get('sha256'), row.get('bytes')
            if (not isinstance(relative, str) or not isinstance(sha, str)
                    or object_descriptor(relative) != sha or type(count) is not int or count < 0
                    or not isinstance(row.get('signature'), list)):
                raise SnapshotError('invalid canonical cold object in catalog')
            delivery = accepted.get(relative + '@' + sha)
            if relative not in excluded:
                if count > maximum_bytes:
                    oversized += 1
                    oversized_bytes += count
                else:
                    rows.append({**row, **({'nas_delivery_identity': delivery} if delivery else {})})
        after = _signature(os.fstat(stream.fileno()))
    if before != after or after != _signature(catalog.stat()):
        raise SnapshotError('catalog changed while selecting the batch')
    after_observation = _stable_json(observation)
    if (after_observation.get('catalog_identity_sha256') != identity
            or not _same_catalog_generation(catalog, after_observation.get('catalog_generation'), generation)
            or _private_catalog_generation(catalog) != generation):
        raise SnapshotError('catalog observation changed while selecting the batch')
    rows.sort(key=lambda row: (-row['bytes'], row['relative']))
    chosen, size = [], 0
    for row in rows:
        if size + row['bytes'] <= maximum_bytes:
            chosen.append(row)
            size += row['bytes']
    if not chosen and oversized:
        raise SnapshotError('unstaged cold object exceeds the maximum wave size')
    if not chosen:
        raise SnapshotError('no unstaged object fits this bounded staging wave')
    chosen.sort(key=lambda row: row['relative'])
    body = {'contract': CONTRACT, 'catalog_identity_sha256': identity,
            'candidate_scope': 'currently_unreferenced_catalog_objects',
            'objects': chosen, 'bytes': size, 'files': len(chosen),
            'remaining_files_before_wave': len(rows) + oversized,
            'remaining_bytes_before_wave': sum(row['bytes'] for row in rows) + oversized_bytes,
            'oversized_files': oversized,
            'deletion_authorized': False, 'complete_dataset_reconstructed': False}
    raw = json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    return {**body, 'identity_sha256': hashlib.sha256(raw).hexdigest()}


def stage(plan: dict, *, cold: Path, destination: Path, native_source_read: bool = True) -> dict:
    """Use the existing closed-delivery copier and independent verifier."""
    result = export_incremental_delivery(cold, plan['objects'], destination,
        catalog_identity=plan['catalog_identity_sha256'], copy_workers=2, verify_workers=2,
        native_source_read=native_source_read)
    return {'contract': CONTRACT, 'state': 'E_exact_object_copy_verified',
            'plan_identity_sha256': plan['identity_sha256'], 'files': plan['files'],
            'bytes': plan['bytes'], 'delivery_identity_sha256': result['envelope_identity_sha256'],
            'destination': str(destination), 'completed_at_epoch': time.time(),
            'D_names_deleted': 0, 'deletion_authorized': False,
            'complete_dataset_reconstructed': False}


def _copy_small_native_group(cold: Path, destination: Path, rows: list[dict], reader_factory) -> list[dict]:
    """One ordered native D stream for up to 16 originals; hash each inline."""
    sources, targets = [], []
    for row in rows:
        source = cold/row['relative']
        if (source.is_symlink() or source.resolve(strict=True) != source
                or not stream_repack._generation_matches(source, row['signature'], list(signature(source)))):
            raise SnapshotError('cold object changed before grouped E staging')
        target = destination/'cold'/row['relative']
        target.parent.mkdir(parents=True, exist_ok=True)
        sources.append(source)
        targets.append(target)
    with reader_factory([(source, row['bytes']) for source, row in zip(sources, rows, strict=True)]) as reader:
        for row, target in zip(rows, targets, strict=True):
            partial = target.with_name(target.name+'.partial')
            digest, remaining = hashlib.sha256(), row['bytes']
            with partial.open('xb') as writer:
                while remaining:
                    block = reader.read(min(8 * 1024**2, remaining))
                    if not block:
                        raise SnapshotError('grouped native source ended early')
                    writer.write(block)
                    digest.update(block)
                    remaining -= len(block)
                writer.flush()
                os.fsync(writer.fileno())
            if digest.hexdigest() != row['sha256']:
                raise SnapshotError('grouped native source checksum differs')
    written = []
    for row, source, target in zip(rows, sources, targets, strict=True):
        if not stream_repack._generation_matches(source, row['signature'], list(signature(source))):
            raise SnapshotError('cold object changed after grouped E staging')
        os.rename(target.with_name(target.name+'.partial'), target)
        _fsync_directory(target.parent)
        written.append({'relative': row['relative'], 'sha256': row['sha256'],
                        'bytes': row['bytes'], 'E_signature': list(signature(target))})
    return written


def _resume_records(plan: dict, *, cold: Path, destination: Path) -> list[dict]:
    """Resume only journalled writes; recopy an unjournalled member from D."""
    from scripts.retire_staged_cold_objects import _identity
    if (plan.get('contract') != CONTRACT or plan.get('identity_sha256') != _identity(plan)
            or not isinstance(plan.get('objects'), list)
            or plan.get('files') != len(plan['objects'])
            or plan.get('bytes') != sum(row.get('bytes', -1) for row in plan['objects'])):
        raise SnapshotError('interrupted wave plan identity differs')
    wanted = {}
    for row in plan['objects']:
        relative, sha, size = row.get('relative'), row.get('sha256'), row.get('bytes')
        if (not isinstance(relative, str) or object_descriptor(relative) != sha
                or relative in wanted or type(size) is not int or size < 0):
            raise SnapshotError('interrupted wave member identity differs')
        source = cold/relative
        if (source.is_symlink() or source.resolve(strict=True) != source
                or not stream_repack._generation_matches(source, row.get('signature'), list(signature(source)))):
            raise SnapshotError('interrupted wave original changed; retain E files')
        wanted[relative] = row
    progress_path = destination/'write-progress.json'
    if progress_path.is_symlink():
        raise SnapshotError('interrupted wave progress is redirected')
    records = []
    if progress_path.exists():
        value = _stable_json(progress_path)
        if (not isinstance(value, dict) or value.get('contract') != CONTRACT+'.write-progress'
                or value.get('plan_identity_sha256') != plan['identity_sha256']
                or value.get('identity_sha256') != _identity(value)
                or not isinstance(value.get('files'), list)):
            raise SnapshotError('interrupted wave progress identity differs')
        records = value['files']
    retained = set()
    for row in records:
        if not isinstance(row, dict):
            raise SnapshotError('interrupted wave progress member differs')
        relative = row.get('relative')
        original = wanted.get(relative)
        if (original is None or relative in retained or row.get('sha256') != original['sha256']
                or row.get('bytes') != original['bytes']):
            raise SnapshotError('interrupted wave progress member differs')
        path = destination/'cold'/relative
        if (path.is_symlink() or path.resolve(strict=True) != path or not path.is_file()
                or path.stat().st_nlink != 1
                or not stream_repack._generation_matches(path, row.get('E_signature'), list(signature(path)))):
            raise SnapshotError('interrupted E member changed; retain it for inspection')
        retained.add(relative)
    # Every unjournalled exact name is disposable only while its unchanged D
    # original still exists. Unknown neighbours and redirected paths stay put.
    for relative in wanted.keys() - retained:
        path = destination/'cold'/relative
        for candidate in (path, path.with_name(path.name+'.partial')):
            if not os.path.lexists(candidate):
                continue
            info = candidate.lstat()
            if (candidate.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or candidate.resolve(strict=True) != candidate):
                raise SnapshotError('interrupted E name is unsafe; retain it for inspection')
            candidate.unlink()
    return records


def recover_fast_result(plan: dict, *, destination: Path) -> dict:
    """Reconstruct a missing result from the already durable write record."""
    from scripts.retire_staged_cold_objects import closed_E_handoff
    envelope = _stable_json(destination/'write-complete.json')
    result = {'contract': CONTRACT, 'state': 'E_write_complete_source_hashed',
              'plan_identity_sha256': plan['identity_sha256'], 'files': plan['files'],
              'bytes': plan['bytes'], 'write_identity_sha256': envelope.get('identity_sha256'),
              'destination': str(destination), 'completed_at_epoch': time.time(),
              'D_names_deleted': 0, 'deletion_authorized': False,
              'complete_dataset_reconstructed': False, 'destination_independent_readback': False}
    closed_E_handoff(plan, result, destination)
    return result


def stage_fast(plan: dict, *, cold: Path, destination: Path, progress=None, resume=False) -> dict:
    """Copy existing encoded objects once; no backup or destination reread."""
    if destination.is_symlink() or destination.resolve() != destination:
        raise SnapshotError('fast E destination is redirected')
    if destination.exists():
        if not resume or not destination.is_dir():
            raise SnapshotError('fast E destination must be a fresh independent directory')
        if (destination/'write-complete.json').exists():
            return recover_fast_result(plan, destination=destination)
        written = _resume_records(plan, cold=cold, destination=destination)
    else:
        destination.mkdir(parents=True, mode=0o700)
        written = []
    completed = {row['relative'] for row in written}
    def checkpoint():
        body = {'contract': CONTRACT+'.write-progress', 'plan_identity_sha256': plan['identity_sha256'],
                'files': written}
        raw = json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        atomic_write_json(destination/'write-progress.json',
                          {**body, 'identity_sha256': hashlib.sha256(raw).hexdigest()}, durable=True)
        if progress is not None:
            progress(len(written), sum(item['bytes'] for item in written))
    if written and progress is not None:
        progress(len(written), sum(item['bytes'] for item in written))
    items = iter(row for row in plan['objects'] if row['relative'] not in completed)
    pending = None
    while True:
        row = pending or next(items, None)
        pending = None
        if row is None:
            break
        if cold in {COLD, COLD/'.local-state/private-history'} and row['bytes'] <= 256 * 1024**2:
            group = [row]
            group_bytes = row['bytes']
            while len(group) < 16:
                candidate = next(items, None)
                if candidate is None:
                    break
                if group_bytes + candidate['bytes'] > 256 * 1024**2:
                    pending = candidate
                    break
                group.append(candidate)
                group_bytes += candidate['bytes']
            if len(group) > 1:
                from stockagent.data_sync.windows_cold_io import binary_reader_many
                written.extend(_copy_small_native_group(cold, destination, group, binary_reader_many))
                checkpoint()
                continue
        source = cold/row['relative']
        if (source.is_symlink() or source.resolve(strict=True) != source
                or not stream_repack._generation_matches(source, row['signature'], list(signature(source)))):
            raise SnapshotError('cold object changed before E staging')
        target = destination/'cold'/row['relative']
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_name(target.name+'.partial')
        copy_verified_bytes(source, partial, expected_sha256=row['sha256'],
                            expected_bytes=row['bytes'], native_windows_io=True)
        if not stream_repack._generation_matches(source, row['signature'], list(signature(source))):
            raise SnapshotError('cold object changed after E staging')
        os.rename(partial, target)
        _fsync_directory(target.parent)
        written.append({'relative': row['relative'], 'sha256': row['sha256'],
                        'bytes': row['bytes'], 'E_signature': list(signature(target))})
        checkpoint()
    body = {'contract': CONTRACT+'.write-complete', 'plan_identity_sha256': plan['identity_sha256'],
            'files': written, 'source_sha256_computed_while_copying': True,
            'destination_independent_readback': False, 'archive_decoded': False}
    raw = json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    manifest = {**body, 'identity_sha256': hashlib.sha256(raw).hexdigest()}
    atomic_write_json(destination/'write-complete.json', manifest, durable=True)
    return {'contract': CONTRACT, 'state': 'E_write_complete_source_hashed',
            'plan_identity_sha256': plan['identity_sha256'], 'files': plan['files'],
            'bytes': plan['bytes'], 'write_identity_sha256': manifest['identity_sha256'],
            'destination': str(destination), 'completed_at_epoch': time.time(),
            'D_names_deleted': 0, 'deletion_authorized': False,
            'complete_dataset_reconstructed': False, 'destination_independent_readback': False}


def staged_names(root: Path) -> frozenset[str]:
    """Avoid recopying completed E batches and retain interrupted payloads."""
    from scripts.retire_staged_cold_objects import closed_E_handoff
    names = set()
    for plan_path in root.glob('*.plan.json'):
        key = plan_path.name.removesuffix('.plan.json')
        plan = _stable_json(plan_path)
        destination = root/(key+'.payload')
        result_path = root/(key+'.result.json')
        if result_path.exists() or result_path.is_symlink():
            result = _stable_json(result_path)
            closed_E_handoff(plan, result, destination)
            names.update(row['relative'] for row in plan['objects'])
        elif destination.exists():
            # Preserve the unfinished bytes for later repair, and allow other
            # independent objects to continue without reporting these done.
            names.update(row['relative'] for row in plan['objects'])
    return frozenset(names)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-gib', type=int, default=4)
    parser.add_argument('--plan-only', action='store_true')
    parser.add_argument('--fast', action='store_true', help='One-pass source-hashed E writes; no destination reread.')
    parser.add_argument('--handoff', action='store_true', help='Retire this exact D wave after the source write completes.')
    parser.add_argument('--automatic', action='store_true', help='Resume pending handoffs and back off when caught up.')
    args = parser.parse_args()
    os.umask(0o077)
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_d_primary_mount(COLD)
    config = volume.configuration()
    migration = volume.path('migration', create=True)
    root = migration/'d-cold-object-staging-v1'
    root.mkdir(mode=0o700, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('E relocation staging root is redirected')
    lock = root/'stage-owner.lock'
    descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'a+b') as owner:
        try:
            fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'state': 'existing_staging_owner_busy'}), flush=True)
            return 0
        if not os.path.samestat(os.fstat(owner.fileno()), lock.lstat()):
            raise SnapshotError('staging owner changed')
        status_path = root/'scheduler-status.json'
        def status(state, **fields):
            atomic_write_json(status_path, {'contract': CONTRACT+'.scheduler', 'state': state,
                'observed_at_epoch': time.time(), **fields}, durable=True)
        if args.automatic and status_path.exists():
            previous = _stable_json(status_path)
            if previous.get('state') == 'caught_up':
                # A completed fixed catalog cannot acquire new members on its
                # own. Check its cheap file generation rather than rereading
                # the multi-GiB catalog every idle retry. A new catalog wakes
                # discovery immediately and is still checked by select().
                if caught_up_unchanged(previous, root, CATALOG, OBSERVATION):
                    atomic_write_json(status_path, refresh_caught_up(previous, now=time.time()), durable=True)
                    return 0
            if (previous.get('state') == 'interrupted_batches'
                    and time.time() < previous.get('next_scan_epoch', 0)):
                return 0
        def guard():
            _check_d_primary_mount(COLD)
            volume.check(c=config)
        def handoff(plan, result, destination, journal):
            from scripts.retire_staged_cold_objects import retire
            return retire(plan, result, cold=COLD, destination=destination,
                journal=journal, catalog=CATALOG, observation=OBSERVATION,
                extra_locks=(Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                             Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')),
                authority_guard=guard, apply=True)
        def copy_wave(plan, *, resume=False):
            key = plan['identity_sha256']
            destination, result_path = root/(key+'.payload'), root/(key+'.result.json')
            controls = migration/'provider-state-temporary-20261103/capacity-leases'
            budget = plan['bytes'] + 256 * 1024**2
            with reserve_capacity(controls, key=key, maximum_growth_bytes=budget,
                    capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
                volume.admit(budget + lease['other_reserved_growth_bytes'], c=config)
                status('copying', plan_identity_sha256=key, files_total=plan['files'],
                       bytes_total=plan['bytes'], files_completed=0, bytes_written=0)
                def progress(files, size):
                    status('copying', plan_identity_sha256=key, files_total=plan['files'],
                           bytes_total=plan['bytes'], files_completed=files, bytes_written=size)
                result = (stage_fast(plan, cold=COLD, destination=destination, progress=progress, resume=resume)
                          if args.fast else stage(plan, cold=COLD, destination=destination))
                _check_d_primary_mount(COLD)
                volume.check(c=config)
                atomic_write_json(result_path, result, durable=True)
            return result
        deferred = []
        if args.automatic and args.fast:
            # A crash during copying must not permanently strand a wave. Only
            # exact names under a fixed plan are reconciled; no E payload read.
            for plan_path in sorted(root.glob('*.plan.json')):
                key = plan_path.name.removesuffix('.plan.json')
                destination = root/(key+'.payload')
                if not destination.exists() or (root/(key+'.result.json')).exists():
                    continue
                try:
                    prior_plan = _stable_json(plan_path)
                    if key != prior_plan.get('identity_sha256'):
                        raise SnapshotError('interrupted wave filename differs from its plan')
                    result = copy_wave(prior_plan, resume=True)
                    if args.handoff:
                        handoff(prior_plan, result, destination, root/(key+'.handoff.json'))
                except (BlockingIOError, PackedPublicationBusy):
                    status('waiting_original_owner', plan_identity_sha256=key,
                           files_total=prior_plan['files'], bytes_total=prior_plan['bytes'])
                    return 0
                except (OSError, SnapshotError, ValueError, KeyError, TypeError) as error:
                    deferred.append({'plan_identity_sha256': key, 'reason': str(error)[:200]})
                    atomic_write_json(root/'scheduler-deferred.json', {'contract': CONTRACT+'.deferred',
                        'observed_at_epoch': time.time(), 'items': deferred}, durable=True)
        if args.handoff:
            for result_path in sorted(root.glob('*.result.json')):
                key = result_path.name.removesuffix('.result.json')
                journal = root/(key+'.handoff.json')
                if journal.exists() and _stable_json(journal).get('state') == 'D_names_removed_E_copy_retained':
                    continue
                prior_plan = _stable_json(root/(key+'.plan.json'))
                prior_result = _stable_json(result_path)
                status('resuming_handoff', plan_identity_sha256=key,
                       files_total=prior_plan['files'], bytes_total=prior_plan['bytes'])
                try:
                    handoff(prior_plan, prior_result, root/(key+'.payload'), journal)
                except (BlockingIOError, PackedPublicationBusy):
                    status('waiting_original_owner', plan_identity_sha256=key,
                           files_total=prior_plan['files'], bytes_total=prior_plan['bytes'])
                    return 0
                except SnapshotError as error:
                    deferred.append({'plan_identity_sha256': key, 'reason': str(error)[:200]})
                    atomic_write_json(root/'scheduler-deferred.json', {'contract': CONTRACT+'.deferred',
                        'observed_at_epoch': time.time(), 'items': deferred}, durable=True)
        excluded = staged_names(root)
        try:
            plan = select(CATALOG, OBSERVATION, None, args.max_gib * 1024**3, excluded=excluded)
        except SnapshotError as error:
            if args.automatic and str(error) == 'no unstaged object fits this bounded staging wave':
                partial = sum(1 for path in root.glob('*.plan.json')
                              if (root/(path.name.removesuffix('.plan.json')+'.payload')).exists()
                              and not (root/(path.name.removesuffix('.plan.json')+'.result.json')).exists())
                # Retain the last wave identity so the observer can report the
                # completed cohort instead of dropping its progress at idle.
                last = _stable_json(status_path) if status_path.exists() else {}
                last_wave = {key: last[key] for key in (
                    'plan_identity_sha256', 'files_total', 'bytes_total',
                    'files_completed', 'bytes_written') if key in last}
                status('caught_up' if partial == 0 and not deferred else 'interrupted_batches',
                       next_scan_epoch=time.time()+60, interrupted_batches=partial,
                       deferred_handoffs=len(deferred),
                       scope='only catalog-unreferenced objects; other datasets are not complete',
                       **last_wave)
                return 0
            raise
        key = plan['identity_sha256']
        plan_path = root/(key+'.plan.json')
        result_path = root/(key+'.result.json')
        destination = root/(key+'.payload')
        if plan_path.exists() and _stable_json(plan_path) != plan:
            raise SnapshotError('fixed plan identity conflicts')
        if not plan_path.exists():
            atomic_write_json(plan_path, plan, durable=True)
        print(json.dumps({'state': 'planned', 'files': plan['files'], 'bytes': plan['bytes'],
                          'plan_identity_sha256': key, 'D_names_deleted': 0}), flush=True)
        if args.plan_only:
            return 0
        if result_path.exists() or destination.exists():
            raise SnapshotError('existing batch retained; inspect its fixed result or interrupted payload')
        result = copy_wave(plan)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        if args.handoff:
            status('handoff', plan_identity_sha256=key, files_total=plan['files'],
                   bytes_total=plan['bytes'], files_completed=plan['files'], bytes_written=plan['bytes'])
            try:
                proof = handoff(plan, result, destination, root/(key+'.handoff.json'))
            except (BlockingIOError, PackedPublicationBusy):
                status('waiting_original_owner', plan_identity_sha256=key,
                       files_total=plan['files'], bytes_total=plan['bytes'])
                return 0
            status('wave_complete', plan_identity_sha256=key, files_total=plan['files'],
                   bytes_total=plan['bytes'], files_completed=plan['files'], bytes_written=plan['bytes'],
                   D_names_removed=len(proof['removed']))
            print(json.dumps({'state': proof['state'], 'files': len(proof['removed']),
                              'bytes': sum(row['bytes'] for row in proof['removed']),
                              'plan_identity_sha256': key}, ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        try:
            root = volume.path('migration')/'d-cold-object-staging-v1'
            path = root/'scheduler-status.json'
            previous = _stable_json(path) if path.is_file() else {}
            atomic_write_json(path, {**previous, 'contract': CONTRACT+'.scheduler', 'state': 'blocked',
                'error_type': type(error).__name__, 'observed_at_epoch': time.time()}, durable=True)
        except Exception:
            pass
        raise
