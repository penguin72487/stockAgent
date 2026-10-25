#!/usr/bin/env python3
"""Compare complete two-version catalog materialization on the enrolled D disk.

Uses one fixed existing catalog delivery. It never exports the production PG,
publishes a release, notifies transport, or retires source/history. Every trial
seals and independently verifies both versions; private PG/native DuckLake
restores of both final candidate versions establish the separate recovery gate.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import statistics
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.control import lakehouse
from stockagent.data_sync.catalog_incremental import _files, _signature, copy_catalog_data
from stockagent.data_sync.immutable_replication import digest, read_manifest, safe, seal, verify


def allocated(root):
    seen, size = set(), 0
    for path in root.rglob('*'):
        metadata = path.lstat()
        if path.is_file() and (metadata.st_dev, metadata.st_ino) not in seen:
            seen.add((metadata.st_dev, metadata.st_ino))
            size += metadata.st_blocks * 512
    return size


def reference_files(root):
    """The previous complete serial census; an independent benchmark oracle."""
    if root.is_symlink() or not root.is_dir():
        raise ValueError('catalog data directory is missing or redirected')
    result = {}
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root).as_posix()
        path = safe(root, relative)
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError('catalog data contains a non-regular file')
        result[relative] = _signature(metadata)
    return result


def benchmark_stat_census(source, output, manifest):
    """Fixed complete census ABBA, with no payload copies or release writes."""
    implementation = ROOT / 'stockagent/data_sync/catalog_incremental.py'
    code = digest(implementation)
    expected = {name[5:]: row['bytes'] for name, row in manifest['files'].items()
                if name.startswith('data/')}
    output.mkdir(mode=0o700, parents=True)
    rows, observed = [], None
    for number, variant in enumerate(('reference', 'candidate', 'candidate', 'reference')):
        started, cpu = time.perf_counter(), time.process_time()
        found = (reference_files if variant == 'reference' else _files)(source / 'data')
        wall, cpu = time.perf_counter() - started, time.process_time() - cpu
        if ({name: row[2] for name, row in found.items()} != expected
                or observed is not None and found != observed):
            raise ValueError('full catalog metadata, exact set or source stability differs')
        observed = found
        row = {'variant': variant, 'wall_seconds': wall, 'cpu_seconds': cpu,
               'complete_metadata_files': len(found), 'full_signature_parity': True}
        rows.append(row)
        atomic_write_json(output / f'census-{number}.json', row)
        print(json.dumps(row), flush=True)
    if read_manifest(source) != manifest or digest(implementation) != code:
        raise ValueError('fixed catalog or implementation changed')
    result = {'state': 'accepted_read_only_catalog_stat_abba',
        'observed_at_utc': datetime.now(UTC).isoformat(),
        'source': str(source), 'delivery_identity_sha256': manifest['identity_sha256'],
        'implementation_sha256': code, 'trials': rows,
        'means': {variant: {key: statistics.mean(row[key] for row in rows if row['variant'] == variant)
                           for key in ('wall_seconds', 'cpu_seconds')}
                  for variant in ('reference', 'candidate')},
        'metadata_file_count': len(expected), 'source_payload_reads': 0,
        'production_payload_writes': 0, 'production_heads_changed': 0,
        'payloads_deleted': 0, 'whole_workflow_speedup_proven': False,
        'scope': 'Complete fixed catalog path/type/stat census only; no payload SHA, copy, '
                 'seal, native restore, transport or NAS timing or acceptance.'}
    atomic_write_json(output / 'acceptance.json', result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--stat-only', action='store_true',
                        help='Compare the complete read-only metadata census; no payload copy or restore')
    args = parser.parse_args(argv)
    c = lakehouse.configuration()
    lakehouse.guard(c)
    source, output = args.source.absolute(), args.output.absolute()
    if (source.parent != Path(c['lake_root']) / 'releases'
            or output.exists() or output.is_symlink()
            or not output.resolve().is_relative_to(ROOT / 'artifacts/operations')):
        parser.error('one exact enrolled release and a fresh private evidence root are required')
    manifest = read_manifest(source)
    if (source.name != 'lake-' + manifest['identity_sha256']
            or manifest['context'].get('kind') != 'ducklake_catalog_and_data'):
        raise ValueError('use a fixed native catalog release')
    if args.stat_only:
        result = benchmark_stat_census(source, output, manifest)
        print(json.dumps({'state': result['state'], 'means': result['means']}), flush=True)
        return 0
    logical = sum(row['bytes'] for row in manifest['files'].values())
    if shutil.disk_usage(Path(c['lake_root'])).free < 64 * 1024**3 + logical * 8:
        raise ValueError('preserve 64 GiB D headroom plus all eight logical versions')
    output.mkdir(mode=0o700, parents=True)
    workspace = Path(c['lake_root']) / ('.export-benchmark-v26-' + uuid.uuid4().hex)
    workspace.mkdir(mode=0o700)
    implementation = ROOT / 'stockagent/data_sync/catalog_incremental.py'
    code = digest(implementation)
    fixture = {'source': str(source), 'delivery_identity_sha256': manifest['identity_sha256'],
               'workspace': str(workspace), 'logical_bytes_per_version': logical,
               'implementation_sha256': code}
    atomic_write_json(output / 'fixture.json', fixture)
    if verify(source) != manifest:
        raise ValueError('fixed source verification differs')
    rows = []
    final_candidate = None
    for number, label in enumerate(('baseline', 'candidate', 'candidate', 'baseline')):
        trial = workspace / f'trial-{number}-{label}'
        trial.mkdir(mode=0o700)
        started = time.perf_counter()
        cpu = time.process_time()
        copies = []
        for version in range(2):
            target = trial / f'version-{version}'
            target.mkdir(mode=0o700)
            if label == 'baseline':
                shutil.copytree(source / 'data', target / 'data')
                expected = {name: row for name, row in manifest['files'].items() if name.startswith('data/')}
                counters = {'copied_bytes': sum(row['bytes'] for row in expected.values()),
                            'copied_files': len(expected), 'shared_bytes': 0, 'shared_files': 0}
            else:
                previous = trial / 'version-0' if version else None
                expected, counters = copy_catalog_data(source / 'data', target / 'data', previous=previous)
            for name in ('control', 'catalog-state.json'):
                if (source / name).is_dir():
                    shutil.copytree(source / name, target / name)
                else:
                    shutil.copy2(source / name, target / name)
            sealed = seal(target, manifest['context'])
            if (sealed != manifest or verify(target) != manifest
                    or expected != {name: row for name, row in sealed['files'].items() if name.startswith('data/')}):
                raise ValueError('complete catalog set or full SHA parity differs')
            copies.append(counters)
        row = {'label': label, 'wall_seconds': time.perf_counter() - started,
               'cpu_seconds': time.process_time() - cpu, 'versions': copies,
               'unique_allocated_bytes_two_versions': allocated(trial),
               'both_versions_full_SHA_and_exact_set_verified': True}
        if label == 'candidate':
            if copies[1]['copied_bytes'] != 0 or copies[1]['shared_files'] != copies[0]['logical_files']:
                raise ValueError('enrolled D did not share every unchanged immutable file')
            final_candidate = trial
        rows.append(row)
        atomic_write_json(output / f'trial-{number}.json', row)
        print(json.dumps(row), flush=True)
    pg_pid = subprocess.check_output(['systemctl', 'show', 'postgresql@18-main', '-p', 'MainPID', '--value'], text=True).strip()
    pg_bin = Path('/proc/' + pg_pid + '/exe').resolve().parent
    # Socket-only independent clusters on SSD; preserve diagnostic receipts.
    # The root is short and traversable by the existing isolated nobody role.
    restores = []
    for version in range(2):
        cluster = Path('/var/tmp') / ('sa-v26-catalog-restore-' + uuid.uuid4().hex)
        restored = lakehouse.restore_lake_delivery(final_candidate / f'version-{version}', cluster,
            extensions=Path(c['extensions']), pg_bin=pg_bin)
        restores.append({'version': version, 'cluster': str(cluster), 'proof': restored})
        atomic_write_json(output / f'native-restore-{version}.json', restores[-1])
    if (verify(source) != manifest or digest(implementation) != code
            or subprocess.check_output(['systemctl', 'show', 'postgresql@18-main', '-p', 'MainPID', '--value'], text=True).strip() != pg_pid):
        raise ValueError('source, implementation or production PG identity changed')
    means = {label: {key: statistics.mean(row[key] for row in rows if row['label'] == label)
                     for key in ('wall_seconds', 'cpu_seconds', 'unique_allocated_bytes_two_versions')}
             for label in ('baseline', 'candidate')}
    result = {'state': 'accepted_complete_catalog_incremental_abba',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'fixture': fixture, 'trials': rows,
        'means': means, 'native_restores': restores, 'all_eight_versions_independently_verified': True,
        'both_candidate_versions_native_DuckLake_and_PG_restored': True,
        'production_catalog_writes': 0, 'history_deleted': 0, 'NAS_acceptance_proven': False,
        'scope': 'Fixed full catalog data/control bytes on enrolled D; copy/seal/full-readback timings. '
                 'PG/native restores measured separately; excludes live PG export, transport and NAS.'}
    atomic_write_json(output / 'acceptance.json', result)
    print(json.dumps({'state': result['state'], 'means': means}), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
