#!/usr/bin/env python3
"""Bounded read-only ABBA trial on actual retained D historical-head files.

This measures one metadata-read phase, not a backup cycle or recovery proof.
Every full byte string, file generation and pinned directory is rechecked.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.packed_backup import PinnedObjectSignatures

ROOT = Path('/srv/stockagent-packed')
HISTORY = ROOT / 'head-history/heads'


def sample_paths(limit: int) -> list[str]:
    """Select a bounded set; later complete reads still enforce no-follow."""
    selected = []
    with os.scandir(HISTORY) as datasets:
        for dataset in datasets:
            if not dataset.is_dir(follow_symlinks=False):
                continue
            with os.scandir(dataset.path) as nodes:
                for node in nodes:
                    if not node.is_dir(follow_symlinks=False):
                        continue
                    with os.scandir(node.path) as entries:
                        for entry in entries:
                            if re.fullmatch(r'[0-9a-f]{64}\.json', entry.name):
                                selected.append(Path(entry.path).relative_to(ROOT).as_posix())
                                if len(selected) >= limit:
                                    return sorted(selected)
    return sorted(selected)


def measure(paths: list[str], workers: int) -> tuple[dict, list[tuple]]:
    started, cpu = time.perf_counter(), time.process_time()
    with PinnedObjectSignatures(ROOT, parallel_signatures=workers > 1) as reader:
        def one(relative):
            raw, signature = reader.read_metadata(relative)
            return relative, hashlib.sha256(raw).hexdigest(), len(raw), signature
        executor = ThreadPoolExecutor(max_workers=workers) if workers > 1 else nullcontext(None)
        with executor as pool:
            rows = list(pool.map(one, paths)) if pool is not None else list(map(one, paths))
        reader.recheck()
    result = {'workers': workers, 'seconds': time.perf_counter() - started,
              'cpu_seconds': time.process_time() - cpu,
              'bytes_read': sum(row[2] for row in rows), 'files': len(rows),
              'filename_sha_mismatches': sum(Path(row[0]).stem != row[1] for row in rows)}
    return result, rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=96)
    parser.add_argument('--name', default='backup-head-read-abba-20261104-v1.json')
    args = parser.parse_args()
    if not 2 <= args.limit <= 256 or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,99}\.json', args.name):
        parser.error('limit or private evidence name exceeds the fixed scope')
    _check_d_primary_mount(ROOT)
    workspace_volume.check()
    output = workspace_volume.path('verify') / args.name
    if output.exists() or output.is_symlink():
        parser.error('retain the earlier measurement; choose a new evidence name')
    paths = sample_paths(args.limit)
    if len(paths) < 2:
        raise RuntimeError('fewer than two retained historical head paths available')
    results, baseline = [], None
    for workers in (1, 4, 4, 1):
        result, rows = measure(paths, workers)
        if baseline is None:
            baseline = rows
        elif rows != baseline:
            raise RuntimeError('historical head bytes or file generations changed during ABBA')
        results.append(result)
    identity = hashlib.sha256(json.dumps(baseline, separators=(',', ':'),
                                          ensure_ascii=True).encode()).hexdigest()
    serial = statistics.mean(row['seconds'] for row in results if row['workers'] == 1)
    parallel = statistics.mean(row['seconds'] for row in results if row['workers'] == 4)
    value = {'contract': 'stockagent.backup_head_read_abba.v1',
             'observed_at_utc': datetime.now(UTC).isoformat(), 'sample_files': len(paths),
             'sample_identity_sha256': identity, 'results': results,
             'serial_mean_seconds': serial, 'parallel_mean_seconds': parallel,
             'observed_phase_ratio': serial / parallel if parallel else None,
             'source_writes': 0, 'source_deletes': 0, 'NAS_acceptance_claimed': False,
             'scope': 'selected existing D historical-head full reads and ancestor rechecks only'}
    atomic_write_json(output, value, durable=True)
    print(json.dumps(value, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
