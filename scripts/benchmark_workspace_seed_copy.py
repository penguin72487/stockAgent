#!/usr/bin/env python3
"""Measure the complete legacy-seed and direct E copy on disposable fixtures.

Creates synthetic files only. Both variants use the canonical full source SHA,
independent destination readback and namespace checks. This does not migrate,
delete or verify any production data, and is not a whole-machine benchmark.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.completed_forward_capacity import reserve_capacity


def main() -> None:
    volume.check()
    verify = volume.path('verify')
    output = verify / 'workspace-seed-strategy-abba-20261104-v1.json'
    if output.exists() or output.is_symlink():
        raise ValueError('retain the existing benchmark evidence')
    sample_bytes, files = 32 * 1024**2, 4
    source_bytes = sample_bytes * files
    # C temporary data and E copies both retain existing application reserves.
    if min(shutil.disk_usage('/tmp').free, shutil.disk_usage('/mnt/c').free) < 25 * 1024**3 + source_bytes:
        raise ValueError('C backing or guest benchmark headroom unavailable')
    budget = 4 * source_bytes + 256 * 1024**2
    leases = volume.path('migration') / 'provider-state-temporary-20261103/capacity-leases'
    key = hashlib.sha256(('seed-benchmark-' + uuid.uuid4().hex).encode()).hexdigest()
    with reserve_capacity(leases, key=key, maximum_growth_bytes=budget,
                          capacity_bytes=2**63-1,
                          minimum_free_bytes=volume.configuration()['minimum_free_bytes']) as lease:
        volume.admit(budget + lease['other_reserved_growth_bytes'])
        with tempfile.TemporaryDirectory(prefix='stockagent-seed-benchmark-', dir='/tmp') as tmp_c, \
             tempfile.TemporaryDirectory(prefix='.seed-benchmark-', dir=verify) as tmp_e:
            source = Path(tmp_c) / 'source'
            source.mkdir()
            block = os.urandom(1024**2)
            for index in range(files):
                with (source / f'{index:02}.bin').open('xb') as stream:
                    for _ in range(sample_bytes // len(block)):
                        stream.write(block)
                    stream.flush()
                    os.fsync(stream.fileno())
            rounds = []
            for index, method in enumerate(('seed_then_verify', 'direct', 'direct', 'seed_then_verify')):
                # Request clean-page eviction only for this benchmark's own C files.
                if hasattr(os, 'posix_fadvise') and hasattr(os, 'POSIX_FADV_DONTNEED'):
                    for path in source.iterdir():
                        with path.open('rb') as stream:
                            os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                destination = Path(tmp_e) / f'destination-{index}'
                evidence = Path(tmp_e) / f'evidence-{index}'
                started = time.monotonic()
                db = relocation.index(source, destination, evidence)
                db.close()
                if method == 'seed_then_verify':
                    subprocess.run(['rsync', '--archive', '--hard-links', '--no-inc-recursive',
                                    '--numeric-ids', '--ignore-existing', '--no-devices', '--no-specials',
                                    '--', str(source) + '/', str(destination) + '/'], check=True, timeout=600)
                proof = relocation.copy(source, destination, evidence, workers=4, sync_batch_files=512,
                                        verify_workers=4, deduplicate_hardlink_verification=method != 'direct')
                elapsed = time.monotonic() - started
                if (proof['files'] != files or proof['logical_bytes'] != source_bytes
                        or not proof['all_source_sha256_and_destination_readback_verified']
                        or not proof['source_namespace_unchanged']):
                    raise ValueError('benchmark copy did not pass the complete original proof')
                rounds.append({'method': method, 'seconds': elapsed, 'files': files,
                               'logical_bytes': source_bytes, 'full_readback_passed': True})
        means = {method: sum(row['seconds'] for row in rounds if row['method'] == method) / 2
                 for method in ('seed_then_verify', 'direct')}
        result = {'contract': 'stockagent.workspace_seed_copy_abba.v1', 'observed_at_epoch': time.time(),
                  'rounds': rounds, 'mean_seconds': means,
                  'fixture_only': True, 'production_sources_read': False,
                  'fixture_payloads_removed': True, 'production_sources_deleted': False,
                  'concurrent_workloads_may_affect_results': True,
                  'speed_scope': 'complete synthetic C-to-E copy and independent readback only'}
        atomic_write_json(output, result, durable=True)
        print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
