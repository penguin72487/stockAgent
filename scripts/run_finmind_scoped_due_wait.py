#!/usr/bin/env python3
"""Run bounded derivative settlement, master, snapshot or spread refresh cycles.

Keep one normal physical-capacity sampler while waiting for the existing
worker lock. This supervisor neither issues provider calls nor resets tasks.
"""
from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path
import time

from downloader import download_finmind_complement as owner
from downloader.artifact_io import sha256_file
from stockagent.data_sync.acquisition_capacity_sampler import AcquisitionCapacitySampler, capacity_retry_delay_seconds


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'
DATASETS = ('TaiwanFuturesFinalSettlementPrice', 'TaiwanOptionFinalSettlementPrice')
MASTER_DATASETS = ('TaiwanFutOptDailyInfo', 'TaiwanFutOptTickInfo')
SNAPSHOT_DATASETS = ('taiwan_futures_snapshot', 'taiwan_options_snapshot')
SPREAD_DATASETS = ('TaiwanFuturesSpreadTrading',)
SCOPES = {'settlement': DATASETS, 'instrument_master': MASTER_DATASETS,
          'derivative_snapshot': SNAPSHOT_DATASETS, 'futures_spread_market': SPREAD_DATASETS}
OWNER_FILES = tuple(REPO / path for path in (
    'scripts/run_finmind_scoped_due_wait.py', 'downloader/download_finmind_complement.py',
    'downloader/finmind_corrections.py', 'downloader/finmind_scheduling.py',
    'downloader/finmind_account.py', 'downloader/provider_priority.py',
    'stockagent/data_sync/acquisition_capacity_sampler.py', 'stockagent/data_sync/cache_capacity.py',
))


def run(root: Path, *, max_requests: int, max_wait_seconds: int, sampler=None,
        datasets=DATASETS) -> dict:
    if not 1 <= max_requests <= 64 or not 1 <= max_wait_seconds <= 3600:
        raise ValueError('a bounded scope and wait are required')
    if datasets not in SCOPES.values():
        raise ValueError('an exact derivative refresh scope is required')
    root = root.resolve()
    if not (root / 'queue.sqlite3').is_file() or not root.is_dir():
        raise ValueError('an existing canonical owner queue is required')
    pins = {path: sha256_file(path) for path in OWNER_FILES}
    capacity = sampler or AcquisitionCapacitySampler(root)
    deadline = time.monotonic() + max_wait_seconds
    while time.monotonic() < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            return {'state': 'owner_changed', 'provider_calls_by_waiter': 0}
        admission = capacity.initial_sample(timeout_seconds=29)
        if admission.get('state') != 'admitted':
            print(json.dumps({'state': 'capacity_wait', 'reason': admission.get('reason'),
                              'provider_calls_by_waiter': 0}), flush=True)
            time.sleep(min(capacity_retry_delay_seconds(admission), max(0, deadline - time.monotonic())))
            continue
        with (root / 'worker.lock').open('a+') as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print(json.dumps({'state': 'owner_lock_busy', 'provider_calls_by_waiter': 0}), flush=True)
                time.sleep(min(5, max(0, deadline - time.monotonic())))
                continue
            try:
                result = owner.run_once(root, max_requests=max_requests, datasets=datasets,
                                        capacity_sampler=capacity)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        reason = result.get('capacity_admission', {}).get('reason')
        print(json.dumps({'state': 'owner_pass', 'owner_state': result.get('state'),
                          'capacity_reason': reason, 'last_task': result.get('last_task'),
                          'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
        if result.get('state') == 'disk_guard' and reason in {'capacity_sample_pending', 'capacity_sample_expired'}:
            time.sleep(min(5, max(0, deadline - time.monotonic())))
            continue
        return result
    return {'state': 'finite_wait_elapsed', 'provider_calls_by_waiter': 0}


def run_batches(root: Path, *, max_requests: int, max_batches: int, max_wait_seconds: int,
                sampler=None, datasets=DATASETS) -> dict:
    """Continue one exact derivative dataset scope, with a shared deadline."""
    if not 1 <= max_batches <= 16 or not 1 <= max_wait_seconds <= 3600:
        raise ValueError('a bounded batch count and total wait are required')
    capacity = sampler or AcquisitionCapacitySampler(root.resolve())
    deadline = time.monotonic() + max_wait_seconds
    last = None
    for number in range(1, max_batches + 1):
        remaining = int(deadline - time.monotonic())
        if remaining < 1:
            return {'state': 'finite_wait_elapsed', 'completed_batches': number - 1,
                    'last_task': (last or {}).get('last_task'), 'provider_calls_by_waiter': 0}
        last = run(root, max_requests=max_requests, max_wait_seconds=remaining, sampler=capacity,
                   datasets=datasets)
        print(json.dumps({'state': 'batch_finished', 'batch': number, 'owner_state': last.get('state'),
                          'last_task': last.get('last_task'), 'provider_calls_by_waiter': 0},
                         ensure_ascii=False), flush=True)
        if last.get('state') != 'batch_complete':
            return dict(last, completed_batches=number - 1)
    return {'state': 'bounded_batches_complete', 'completed_batches': max_batches,
            'last_task': last.get('last_task'), 'provider_calls_by_waiter': 0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scope', choices=SCOPES, default='settlement')
    parser.add_argument('--max-requests', type=int, default=4)
    parser.add_argument('--max-batches', type=int, default=1)
    parser.add_argument('--max-wait-seconds', type=int, default=600)
    args = parser.parse_args()
    result = run_batches(ROOT, max_requests=args.max_requests, max_batches=args.max_batches,
                         max_wait_seconds=args.max_wait_seconds, datasets=SCOPES[args.scope])
    print(json.dumps({'state': result.get('state'), 'last_task': result.get('last_task'),
                      'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
    return 75 if result.get('state') in {'finite_wait_elapsed', 'owner_changed'} else 0


if __name__ == '__main__':
    raise SystemExit(main())
