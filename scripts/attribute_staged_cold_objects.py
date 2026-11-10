#!/usr/bin/env python3
"""Attribute staged opaque objects using their existing inventory metadata.

This reads compressed inventories, not the source payloads. Matching an
object to an inventory does not identify a complete release or authorize
deletion; unmatched objects and every staged payload remain in place.
"""
from __future__ import annotations

from collections import defaultdict
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import retire_staged_cold_objects as retired, stage_d_cold_relocation as staging
from stockagent.data_sync import packed_snapshots as packed, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_backup import signature

CONTRACT = 'stockagent.staged_cold_orphan_attribution.v1'
MAX_LINE = 16 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 100_000
IMPLEMENTATION_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _refresh_recovery_candidates(root: Path, objects: dict, primary: dict, deferred: list):
    """Reuse this owner's lock; historical-index trouble cannot block local work."""
    try:
        from scripts.attribute_staged_recovery_index import refresh_with_status
        refresh_with_status(root, objects, primary, deferred)
    except (ImportError, OSError, ValueError, TypeError, KeyError, SnapshotError):
        pass


def collect(root: Path):
    """Require the canonical exact-copy and D-name-retirement receipts."""
    objects = {}
    deferred = []
    for plan_path in sorted(root.glob('*.plan.json')):
        try:
            plan = staging._stable_json(plan_path)
            key = plan.get('identity_sha256')
            if not isinstance(key, str) or plan_path.name != key+'.plan.json':
                raise SnapshotError('staged object plan name differs from identity')
            result = staging._stable_json(root/(key+'.result.json'))
            handoff = staging._stable_json(root/(key+'.handoff.json'))
            destination = root/(key+'.payload')
            if (handoff.get('contract') != retired.CONTRACT
                    or handoff.get('state') != 'D_names_removed_E_copy_retained'
                    or handoff.get('plan_identity_sha256') != key
                    or handoff.get('destination') != str(destination)):
                raise SnapshotError('staged object retirement receipt differs')
            removed_rows = handoff.get('removed')
            if not isinstance(removed_rows, list) or any(not isinstance(item, dict) for item in removed_rows):
                raise SnapshotError('staged object retirement member list is invalid')
            removed = {row.get('relative'): row.get('bytes') for row in removed_rows}
            if len(removed) != len(removed_rows):
                raise SnapshotError('staged object retirement has duplicate names')
            copied = retired.closed_E_handoff(plan, result, destination)
            if set(removed) != {row['relative'] for row in copied}:
                raise SnapshotError('staged object retirement is incomplete')
            additions = {}
            for row in copied:
                if removed[row['relative']] != row['bytes'] or row['sha256'] in objects or row['sha256'] in additions:
                    raise SnapshotError('staged object identity or length is duplicated')
                additions[row['sha256']] = {**row, 'path': destination/'cold'/row['relative'], 'batch': key}
            objects.update(additions)
        except (OSError, ValueError, KeyError, SnapshotError) as error:
            # One new incomplete batch must not hide the completed nine.
            deferred.append({'plan': plan_path.name, 'error_type': type(error).__name__})
    return objects, deferred


def attribute(objects: dict, *, progress=lambda _event: None):
    matches = defaultdict(lambda: {'references': 0, 'inventories': set(), 'examples': []})
    inventories = sorted((sha, row) for sha, row in objects.items()
                         if row['relative'].endswith('.jsonl.gz'))
    completed, errors = [], []
    for inventory_sha, item in inventories:
        local = defaultdict(lambda: {'references': 0, 'examples': []})
        try:
            path = item['path']
            before = list(signature(path))
            if before != item['E_signature']:
                raise SnapshotError('staged inventory generation changed before attribution')
            rows = 0
            with gzip.open(path, 'rb') as stream:
                while line := stream.readline(MAX_LINE+1):
                    if len(line) > MAX_LINE:
                        raise SnapshotError('staged inventory record exceeds the metadata bound')
                    row = json.loads(line)
                    if not isinstance(row, dict) or not isinstance(row.get('path'), str):
                        raise SnapshotError('staged inventory member is invalid')
                    if packed._safe_relative_path(row['path'], 'staged inventory member').as_posix() != row['path']:
                        raise SnapshotError('staged inventory path is noncanonical')
                    kind, storage = row.get('kind'), row.get('storage')
                    if kind == 'file':
                        if not isinstance(storage, dict) or type(row.get('size')) is not int or row['size'] < 0:
                            raise SnapshotError('staged inventory storage is invalid')
                        # Use the same v1/v2/v3 object-reference contract as
                        # restore. Real v2 chunks use object_sha256, not a
                        # separate sha256 field on each extent.
                        references = {digest for digest, _ in packed._storage_extents(storage, row['size'])}
                    elif kind in {'directory', 'symlink'}:
                        references = set()
                    else:
                        raise SnapshotError('staged inventory kind is invalid')
                    for digest in references & objects.keys():
                        match = local[digest]
                        match['references'] += 1
                        if len(match['examples']) < 4:
                            match['examples'].append(row['path'])
                    rows += 1
            if list(signature(path)) != before:
                raise SnapshotError('staged inventory changed while being attributed')
        except (OSError, ValueError, TypeError, EOFError, SnapshotError) as error:
            errors.append({'sha256': inventory_sha, 'error_type': type(error).__name__})
            continue
        for digest, value in local.items():
            match = matches[digest]
            match['references'] += value['references']
            match['inventories'].add(inventory_sha)
            match['examples'].extend(value['examples'][:4-len(match['examples'])])
        completed.append({'sha256': inventory_sha, 'rows': rows})
        progress({'inventories_completed': len(completed), 'inventories_total': len(inventories),
                  'attributed_objects': len(matches)})
    archives, archive_errors = [], []
    archive_items = sorted((sha, row) for sha, row in objects.items() if row['relative'].endswith('.zip'))
    for archive_sha, item in archive_items:
        try:
            before = list(signature(item['path']))
            if before != item['E_signature']:
                raise SnapshotError('staged archive generation changed before name census')
            groups, unknown = defaultdict(int), 0
            with zipfile.ZipFile(item['path']) as archive:
                members = archive.infolist()
                if len(members) > MAX_ARCHIVE_MEMBERS:
                    raise SnapshotError('staged archive member count exceeds metadata bound')
                for member in members:
                    if member.is_dir():
                        continue
                    name = member.filename
                    if packed._safe_relative_path(name, 'staged archive member').as_posix() != name:
                        raise SnapshotError('staged archive member path is noncanonical')
                    parts = name.split('/')
                    if len(parts) < 2:
                        unknown += 1
                    else:
                        group = '/'.join(parts[:2]) if parts[0] in {'training_prepared', 'raw', 'data'} else parts[0]
                        groups[group] += 1
            if list(signature(item['path'])) != before:
                raise SnapshotError('staged archive changed during name census')
            single = next(iter(groups)) if len(groups) == 1 and not unknown else None
            archives.append({'sha256': archive_sha, 'bytes': item['bytes'], 'members': sum(groups.values())+unknown,
                             'single_path_prefix_candidate': single,
                             'group_counts': dict(sorted(groups.items(), key=lambda pair: (-pair[1], pair[0]))[:5]),
                             'unclassified_members': unknown})
        except (OSError, ValueError, TypeError, EOFError, SnapshotError, zipfile.BadZipFile) as error:
            archive_errors.append({'sha256': archive_sha, 'error_type': type(error).__name__})
        progress({'inventories_completed': len(completed), 'inventories_total': len(inventories),
                  'archives_completed': len(archives)+len(archive_errors), 'archives_total': len(archive_items),
                  'attributed_objects': len(matches)})
    records = [{'sha256': sha, 'bytes': objects[sha]['bytes'], 'references': value['references'],
                'inventory_sha256': sorted(value['inventories']), 'example_paths': value['examples']}
               for sha, value in sorted(matches.items())]
    return {'inventories': completed, 'inventory_errors': errors, 'matches': records,
            'archive_name_census': archives, 'archive_errors': archive_errors,
            'archive_prefix_candidates': sum(item['single_path_prefix_candidate'] is not None for item in archives),
            'archive_mixed_or_unknown': sum(item['single_path_prefix_candidate'] is None for item in archives),
            'object_count': len(objects), 'object_bytes': sum(row['bytes'] for row in objects.values()),
            'attributed_objects': len(records), 'attributed_bytes': sum(row['bytes'] for row in records),
            'unattributed_objects': len(objects)-len(records),
            'payloads_read': False, 'complete_dataset_reconstructed': False, 'deletion_authorized': False}


def run(root: Path):
    root = Path(root).absolute()
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('staged object attribution root redirected')
    descriptor = os.open(root/'orphan-attribution-owner.lock',
                         os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('staged object attribution already has an owner') from error
        return _run_owned(root)
    finally:
        os.close(descriptor)


def _run_owned(root: Path):
    output = root/'orphan-attribution-v1.json'
    if output.is_symlink():
        raise SnapshotError('staged object attribution output redirected')
    objects, deferred = collect(root)
    key = hashlib.sha256(json.dumps({'objects': sorted((sha, row['bytes'], row['E_signature'])
        for sha, row in objects.items()), 'deferred': deferred}, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if output.is_file():
        try:
            previous = json.loads(output.read_text())
        except (OSError, ValueError):
            previous = {}
        if (isinstance(previous, dict) and previous.get('contract') == CONTRACT and previous.get('input_identity_sha256') == key
                and previous.get('implementation_sha256') == IMPLEMENTATION_SHA256
                and previous.get('state') == 'metadata_attributed'
                and (not previous.get('inventory_errors') and not previous.get('archive_errors')
                     or time.time()-previous.get('observed_at_epoch', 0) < 300)):
            previous['last_checked_at_epoch'] = time.time()
            atomic_write_json(output, previous, durable=True)
            _refresh_recovery_candidates(root, objects, previous, deferred)
            return previous
    last = 0.0
    def progress(event):
        nonlocal last
        if time.monotonic()-last >= 5:
            last = time.monotonic()
            atomic_write_json(root/'orphan-attribution-progress.json',
                {'contract': CONTRACT+'.progress', 'state': 'running', 'progress': event,
                 'observed_at_epoch': time.time()})
    result = attribute(objects, progress=progress)
    result.update(contract=CONTRACT, state='metadata_attributed', input_identity_sha256=key,
                  implementation_sha256=IMPLEMENTATION_SHA256, deferred_batches=deferred,
                  observed_at_epoch=time.time(), last_checked_at_epoch=time.time())
    atomic_write_json(output, result, durable=True)
    _refresh_recovery_candidates(root, objects, result, deferred)
    return result


def main():
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    root = workspace_volume.path('migration')/'d-cold-object-staging-v1'
    result = run(root)
    print(json.dumps({key: result[key] for key in ('state', 'object_count', 'object_bytes',
        'attributed_objects', 'attributed_bytes', 'unattributed_objects')}, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
