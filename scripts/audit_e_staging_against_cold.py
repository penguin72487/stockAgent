#!/usr/bin/env python3
"""Compare completed E staging indexes with D packed inventories, read-only.

This is a planning observation, not source freshness, object readback, backup
acceptance, a new publication, a deletion decision, or a physical-space bound.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from downloader.artifact_io import atomic_write_json
from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import workspace_relocation as relocation, workspace_volume as volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import _iter_inventory, resolve_latest_packed

COLD = Path('/srv/stockagent-packed')
SHA256 = re.compile(r'[0-9a-f]{64}\Z')


def _normalized_relative(name) -> bool:
    if (not isinstance(name, str) or not name or name.startswith('/') or name.endswith('/')
            or '//' in name or '\x00' in name):
        return False
    wrapped = '/' + name + '/'
    return '/./' not in wrapped and '/../' not in wrapped


def compare_index(index: Path, expected_files: int, expected_bytes: int, cold_rows, *, source_prefix='',
                  progress=None, guard=None, expected_symlinks=None) -> dict:
    """Merge two ordered inventories with bounded memory and no random probes."""
    if index.is_symlink() or not index.is_file():
        raise ValueError('staging index unavailable or redirected')
    wal = index.with_name(index.name + '-wal')
    if wal.is_symlink() or wal.exists() and wal.stat().st_size:
        raise ValueError('staging index has an uncheckpointed or redirected WAL')
    index_before = relocation.signature(index)
    if source_prefix and not _normalized_relative(source_prefix):
        raise ValueError('source prefix must be a normalized relative path')
    counts = {f'{kind}_{unit}': 0 for kind in ('exact', 'changed', 'e_only') for unit in ('files', 'bytes')}
    counts.update(d_only_files=0, d_non_file_entries=0)
    if expected_symlinks is not None and (type(expected_symlinks) is not int or expected_symlinks < 0):
        raise ValueError('staging symlink count is invalid')
    staged_links = {}
    if expected_symlinks is not None:
        counts.update(exact_symlinks=0, changed_symlinks=0, e_only_symlinks=0, d_only_symlinks=0)
    staged_count, staged_bytes, cold_count = 0, 0, 0
    last_report = 0.0

    def report(force=False):
        nonlocal last_report
        now = time.monotonic()
        if force or now-last_report >= 1:
            if guard is not None:
                guard()
            if progress is not None:
                progress({'staged_files_scanned': staged_count, 'staged_files_total': expected_files,
                          'staged_bytes_scanned': staged_bytes, 'staged_bytes_total': expected_bytes,
                          'cold_entries_scanned': cold_count})
            last_report = now

    def staged_rows(connection):
        nonlocal staged_count, staged_bytes
        previous = ''
        cursor = connection.execute("SELECT path,sha,bytes,state FROM files WHERE kind='file' ORDER BY path")
        for relative, digest, size, state in cursor:
            if (not _normalized_relative(relative) or relative <= previous
                    or not isinstance(digest, str) or SHA256.fullmatch(digest) is None
                    or type(size) is not int or size < 0 or state != 'verified'):
                raise ValueError('staging index has an invalid member')
            previous = relative
            staged_count += 1
            staged_bytes += size
            yield relative, digest, size

    def fixed_cold_rows():
        nonlocal cold_count
        previous = ''
        for row in cold_rows:
            cold_count += 1
            if cold_count % 4096 == 0:
                report()
            if not isinstance(row, dict) or not _normalized_relative(relative := row.get('path')) \
                    or relative <= previous:
                raise ValueError('cold inventory is not a sorted, normalized namespace')
            previous = relative
            if source_prefix:
                if not relative.startswith(source_prefix + '/'):
                    continue
                relative = relative[len(source_prefix) + 1:]
            if row.get('kind') != 'file':
                counts['d_non_file_entries'] += 1
                if expected_symlinks is not None and row.get('kind') == 'symlink':
                    target = row.get('target')
                    if not isinstance(target, str) or '\x00' in target:
                        raise ValueError('cold inventory has an invalid symlink identity')
                    expected = staged_links.pop(relative, None)
                    if expected is None:
                        counts['d_only_symlinks'] += 1
                    elif hashlib.sha256(os.fsencode(target)).hexdigest() == expected:
                        counts['exact_symlinks'] += 1
                    else:
                        counts['changed_symlinks'] += 1
                continue
            digest, size = row.get('sha256'), row.get('size')
            if (not isinstance(digest, str) or SHA256.fullmatch(digest) is None
                    or type(size) is not int or size < 0):
                raise ValueError('cold inventory has an invalid file identity')
            yield relative, digest, size

    with sqlite3.connect(f'file:{index}?mode=ro', uri=True) as connection:
        connection.execute('BEGIN')
        if expected_symlinks is not None:
            for relative, digest, size, state in connection.execute(
                    "SELECT path,sha,bytes,state FROM files WHERE kind='symlink' ORDER BY path"):
                if (not _normalized_relative(relative) or relative in staged_links
                        or not isinstance(digest, str) or SHA256.fullmatch(digest) is None
                        or size != 0 or state != 'verified'):
                    raise ValueError('staging index has an invalid symlink member')
                staged_links[relative] = digest
                if len(staged_links) > expected_symlinks:
                    raise ValueError('staging symlink count exceeds copy proof')
            if len(staged_links) != expected_symlinks:
                raise ValueError('staging symlink count differs from copy proof')
        report(force=True)
        stage, cold = iter(staged_rows(connection)), iter(fixed_cold_rows())
        a, b = next(stage, None), next(cold, None)
        steps = 0
        while a is not None or b is not None:
            if b is None or a is not None and a[0] < b[0]:
                counts['e_only_files'] += 1
                counts['e_only_bytes'] += a[2]
                a = next(stage, None)
            elif a is None or b[0] < a[0]:
                counts['d_only_files'] += 1
                b = next(cold, None)
            else:
                kind = 'exact' if a[1:] == b[1:] else 'changed'
                counts[kind + '_files'] += 1
                counts[kind + '_bytes'] += a[2]
                a, b = next(stage, None), next(cold, None)
            steps += 1
            if steps % 4096 == 0:
                report()
        report(force=True)
        if expected_symlinks is not None:
            counts['e_only_symlinks'] = len(staged_links)
    if staged_count != expected_files or staged_bytes != expected_bytes:
        raise ValueError('staging index count or bytes differ from copy proof')
    if relocation.signature(index) != index_before or wal.is_symlink() or wal.exists() and wal.stat().st_size:
        raise ValueError('staging index changed during the fixed comparison')
    if (sum(counts[k + '_files'] for k in ('exact', 'changed', 'e_only')) != expected_files
            or sum(counts[k + '_bytes'] for k in ('exact', 'changed', 'e_only')) != expected_bytes):
        raise ValueError('comparison does not conserve the staged file count and bytes')
    return counts


def audit(item: dict, workspace: Path, *, progress=None, guard=None, expected_head=None) -> dict:
    entries = queue.catalog_entries(item)
    catalog_source = item.get('catalog_source', item['source'])
    primary = [entry for entry in entries if entry.get('source') == catalog_source]
    if len(primary) != 1:
        raise ValueError('source has an ambiguous exact catalog binding')
    entry = primary[0]
    result = {'name': item['name'], 'dataset': entry['dataset'], 'source_files_deleted': 0,
              'D_recovery_verified': False, 'retirement_authorized': False}
    proof = queue.verified_report(item, workspace)
    if proof is None:
        return {**result, 'state': 'waiting_for_E_copy_proof'}
    result.update(staged_files=proof['files'], staged_bytes=proof['logical_bytes'],
                  staged_proof_at_epoch=proof['observed_at_epoch'])
    if entry.get('publish') is not True:
        return {**result, 'state': 'local_only_no_new_cold_publication'}
    snapshot = resolve_latest_packed(COLD, entry['dataset'], require_objects=True)
    identity = {'D_snapshot_id': snapshot.manifest['snapshot_id'],
                'D_manifest_sha256': snapshot.manifest_sha256,
                'D_inventory_sha256': snapshot.manifest['archive']['inventory']['sha256']}
    if expected_head is not None and any(identity[key] != expected_head.get(key) for key in identity):
        raise SnapshotError('D head changed before the inventory comparison began')
    rows = _iter_inventory(COLD, snapshot.manifest)
    relative = Path(item['source']).relative_to(Path(catalog_source))
    prefix = '' if relative == Path('.') else relative.as_posix()
    counts = compare_index(workspace / 'migration' / item['name'] / 'files.sqlite',
                           proof['files'], proof['logical_bytes'], rows, source_prefix=prefix,
                           progress=progress, guard=guard,
                           expected_symlinks=proof.get('symlink_names_preserved'))
    return {**result, 'state': 'inventory_compared', **identity, 'counts': counts,
            'D_inventory_source_prefix': prefix,
            'catalog_excluded_subtrees': entry.get('excluded_subtrees', []),
            'scope': 'historical staged index against fixed D inventory; not D object readback or current source'}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--name', default='e-stage-vs-d-inventory-20261104-v2.json')
    args = parser.parse_args()
    if re.fullmatch(r'[a-z0-9][a-z0-9-]{0,99}\.json', args.name) is None:
        parser.error('invalid private evidence name')
    volume.check()
    _check_d_primary_mount(COLD)
    workspace = Path(volume.configuration()['mount_point'])
    output = volume.path('verify') / args.name
    if output.exists() or output.is_symlink():
        parser.error('evidence name already exists')
    results = []
    for item in queue.load_sources():
        try:
            results.append(audit(item, workspace))
        except (OSError, ValueError, KeyError, TypeError, SnapshotError, sqlite3.Error) as error:
            results.append({'name': item['name'], 'state': 'comparison_unavailable',
                            'error_type': type(error).__name__, 'D_recovery_verified': False,
                            'retirement_authorized': False})
    value = {'contract': 'stockagent.e_stage_d_inventory_comparison.v2',
             'observed_at_utc': datetime.now(UTC).isoformat(), 'sources': results,
             'D_independent_restore_verified': False, 'retirement_authorized': False,
             'source_writes': 0, 'source_deletes': 0}
    atomic_write_json(output, value, durable=True)
    print(json.dumps(value, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
