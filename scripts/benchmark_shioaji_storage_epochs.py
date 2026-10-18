"""Compare the complete unchanged history census, not a copied small fixture.

The original implementation is preserved/hash-checked before loading. Fresh
workers include import, metadata validation and the complete reported census.
Source files are never copied, opened for payload decoding, or written. Initial
cache fill is recorded separately from repeated version-validated reuse.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import socket
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json


def _sha(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _implementation_identity() -> dict:
    names = ('stockagent/live/shioaji_storage_monitor.py', 'stockagent/live/shioaji_storage_epoch.py',
             'downloader/download_shioaji_tx_futures_ticks.py', 'downloader/artifact_io.py',
             'scripts/benchmark_shioaji_storage_epochs.py')
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in names}


def _source_token(spec, monitor) -> str:
    """Canonical mutation/control identity, not a payload-integrity proof."""
    from stockagent.live.shioaji_storage_epoch import history_storage_observation
    records = []
    for parent in spec.roots:
        if not parent.is_dir():
            records.append([str(parent), None])
            continue
        info = parent.stat()
        records.append([str(parent), info.st_ino, info.st_mtime_ns, info.st_ctime_ns])
        for path in sorted(parent.iterdir()):
            if path.is_dir() and not path.is_symlink():
                with history_storage_observation(path) as observed:
                    records.append([str(path), monitor._inventory_controls(path, observed or {})])
            else:
                info = path.lstat()
                records.append([str(path), info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns])
    return _sha(records)


def _worker(args) -> dict:
    started = time.perf_counter()
    implementation = _implementation_identity()
    def no_network(*_args, **_kwargs):
        raise AssertionError('storage accounting must not open a provider connection')
    socket.socket.connect = no_network
    socket.socket.connect_ex = no_network
    socket.create_connection = no_network
    from stockagent.live import shioaji_storage_monitor as current
    reference_path = args.reference_root / 'original_shioaji_storage_monitor.py'
    record = next(row for row in json.loads((args.reference_root / 'original-code.json').read_bytes())['sources']
                  if row['source'] == 'stockagent/live/shioaji_storage_monitor.py')
    if hashlib.sha256(reference_path.read_bytes()).hexdigest() != record['sha256']:
        raise ValueError('the preserved original storage monitor changed')
    loader = importlib.util.spec_from_file_location('original_storage_monitor', reference_path)
    reference = importlib.util.module_from_spec(loader)
    sys.modules[loader.name] = reference
    loader.loader.exec_module(reference)
    spec = next(spec for spec in current.default_storage_datasets(ROOT) if spec.dataset_id == 'futures_history')
    before = _source_token(spec, current)
    observed = datetime.fromisoformat(args.observed_at)
    scan_started = time.perf_counter()
    if args.worker == 'reference':
        row = reference._scan_dataset(spec, now=observed)
    else:
        row = current._scan_dataset(spec, now=observed, inventory_cache=args.cache_dir)
    scan_seconds = time.perf_counter() - scan_started
    if before != _source_token(spec, current):
        raise RuntimeError('history source changed during the full census; no timing accepted')
    reuse = row.pop('inventory_reuse', None)
    forbidden = sorted(name for name in sys.modules if name.split('.')[0] in {'torch', 'cupy', 'shioaji'})
    if forbidden:
        raise AssertionError(f'storage accounting loaded compute/provider modules: {forbidden}')
    if implementation != _implementation_identity():
        raise RuntimeError('the implementation changed during the worker')
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {'label': args.worker, 'source_control_sha256': before,
            'implementation_sha256': implementation,
            'result_sha256': _sha(row), 'result': row, 'inventory_reuse': reuse,
            'wall_seconds': time.perf_counter() - started, 'scan_seconds': scan_seconds,
            'cpu_seconds_self_and_children': own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime,
            'peak_process_rss_bytes': 1024 * max(own.ru_maxrss, children.ru_maxrss),
            'not_cgroup_file_and_inode_cache_peak': True, 'provider_calls': 0,
            'loaded_compute_or_provider_modules': forbidden}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-root', type=Path, required=True)
    parser.add_argument('--cache-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--observed-at', default=datetime.now(UTC).isoformat())
    parser.add_argument('--worker', choices=('reference', 'candidate'))
    parser.add_argument('--reuse-only', action='store_true',
                        help='Compare ABBA after a separately retained full initial fill.')
    args = parser.parse_args()
    if args.worker:
        atomic_write_json(args.output, _worker(args))
        return 0
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    args.cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    trials = []
    # ABBA repeated comparison; first-fill is a distinct required work item.
    order = ('reference', 'candidate', 'candidate', 'reference') if args.reuse_only else (
        'reference', 'fill', 'candidate', 'candidate', 'reference')
    for index, label in enumerate(order):
        target = args.output.parent / f'{args.output.stem}-round-{index}-{label}.json'
        started = time.perf_counter()
        subprocess.run([sys.executable, str(Path(__file__).resolve()),
                        '--reference-root', str(args.reference_root), '--cache-dir', str(args.cache_dir),
                        '--observed-at', args.observed_at, '--output', str(target),
                        '--worker', 'reference' if label == 'reference' else 'candidate'],
                       check=True, cwd=ROOT)
        row = json.loads(target.read_bytes())
        row['trial'] = label
        row['fresh_process_wall_seconds'] = time.perf_counter() - started
        if trials and (row['result'] != trials[0]['result']
                       or row['source_control_sha256'] != trials[0]['source_control_sha256']
                       or row['implementation_sha256'] != trials[0]['implementation_sha256']):
            raise AssertionError('complete reported accounting or canonical source generation differed')
        if args.reuse_only and label == 'candidate' and (
                row['inventory_reuse']['scanned_epoch_roots']
                or row['inventory_reuse']['reused_epoch_roots'] == 0):
            raise AssertionError('a reuse-only trial unexpectedly needed a full epoch scan')
        trials.append(row)
        atomic_write_json(args.output, {'state': 'running', 'trials': trials})
        print(json.dumps({'trial': label, 'seconds': row['fresh_process_wall_seconds'],
                          'files': row['result']['files'], 'bytes': row['result']['bytes'],
                          'inventory_reuse': row['inventory_reuse']}), flush=True)
    means = {label: {key: statistics.mean(row[key] for row in trials if row['trial'] == label)
                     for key in ('fresh_process_wall_seconds', 'cpu_seconds_self_and_children', 'peak_process_rss_bytes')}
             for label in ('reference', 'candidate')}
    atomic_write_json(args.output, {'contract': 'shioaji_storage_complete_history_epoch_benchmark_v1',
                                   'state': 'accepted', 'scope': 'complete futures_history storage census only',
                                   'trials': trials, 'means': means, 'provider_calls': 0,
                                   'source_data_writes': 0, 'file_counts_bytes_growth_and_latest_exact': True,
                                   'not_market_data_integrity_acceptance': True,
                                   'not_complete_ten_dataset_native_workflow': True,
                                   'host': os.uname().nodename})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
