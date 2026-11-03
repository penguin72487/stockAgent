#!/usr/bin/env python3
"""Run bounded, explicitly scoped FinMind refresh cycles.

Keep one normal physical-capacity sampler while waiting for the existing
worker lock. This supervisor neither issues provider calls nor resets tasks.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import fcntl
import json
from pathlib import Path
import time

from downloader import download_finmind_complement as owner
from downloader.artifact_io import sha256_file
from downloader.provider_priority import inspect
from stockagent.data_sync.acquisition_capacity_sampler import AcquisitionCapacitySampler, capacity_retry_delay_seconds


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'
DATASETS = ('TaiwanFuturesFinalSettlementPrice', 'TaiwanOptionFinalSettlementPrice')
MASTER_DATASETS = ('TaiwanFutOptDailyInfo', 'TaiwanFutOptTickInfo')
SNAPSHOT_DATASETS = ('taiwan_futures_snapshot', 'taiwan_options_snapshot')
SPREAD_DATASETS = ('TaiwanFuturesSpreadTrading',)
ASSET_SWAP_DUE_DATASETS = ('TaiwanAssetSwapOptionDaily', 'TaiwanAssetSwapFixedIncomeDaily')
RELEASE_PRECONDITION_DATASETS = (
    'EuropeStockInfo', 'GoldPrice', 'JapanStockInfo', 'TaiwanSecuritiesTraderInfo',
    'TaiwanStockActiveETFInfo', 'TaiwanStockConvertibleBondMonthlyAnalysis',
    'TaiwanStockDelisting', 'TaiwanStockInfo', 'TaiwanStockNews',
    'TaiwanStockParValueChange', 'TaiwanStockSplitPrice',
    'TaiwanStockTotalInstitutionalInvestors', 'TaiwanStockTotalMarginPurchaseShortSale',
    'UKStockInfo', 'USStockInfo', 'taiwan_stock_tick_snapshot',
)
SCOPES = {'settlement': DATASETS, 'instrument_master': MASTER_DATASETS,
          'derivative_snapshot': SNAPSHOT_DATASETS, 'futures_spread_market': SPREAD_DATASETS,
          'asset_swap_due': ASSET_SWAP_DUE_DATASETS,
          'release_preconditions': RELEASE_PRECONDITION_DATASETS}
OWNER_FILES = tuple(REPO / path for path in (
    'scripts/run_finmind_scoped_due_wait.py', 'downloader/download_finmind_complement.py',
    'downloader/finmind_errors.py', 'downloader/finmind_option_stream.py',
    'downloader/finmind_corrections.py', 'downloader/finmind_scheduling.py',
    'downloader/finmind_account.py', 'downloader/provider_priority.py',
    'stockagent/data_sync/acquisition_capacity_sampler.py', 'stockagent/data_sync/cache_capacity.py',
))


def run(root: Path, *, max_requests: int, max_wait_seconds: int, sampler=None,
        datasets=DATASETS) -> dict:
    if not 1 <= max_requests <= 64 or not 1 <= max_wait_seconds <= 3600:
        raise ValueError('a bounded scope and wait are required')
    if datasets not in SCOPES.values():
        raise ValueError('an exact permitted refresh scope is required')
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
        if result.get('state') == 'disk_guard':
            # A pipeline can stop on a stale sample while draining in-flight
            # receipts and return after the sample has refreshed, leaving no
            # reason in the owner summary. Keep the finite waiter alive, but
            # always reacquire the normal capacity gate and owner lock before
            # dispatching again; an unknown reason gets the conservative delay.
            delay = capacity_retry_delay_seconds(result.get('capacity_admission') or {})
            time.sleep(min(delay, max(0, deadline - time.monotonic())))
            continue
        return result
    return {'state': 'finite_wait_elapsed', 'provider_calls_by_waiter': 0}


def run_batches(root: Path, *, max_requests: int, max_batches: int, max_wait_seconds: int,
                sampler=None, datasets=DATASETS) -> dict:
    """Continue one exact dataset scope, with a shared deadline."""
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


def run_registered_priority(root: Path, *, request_id: str, max_requests: int,
                            max_wait_seconds: int, sampler=None) -> dict:
    """Dispatch only a registered request via the existing locked FinMind owner."""
    if not 1 <= max_requests <= 64 or not 1 <= max_wait_seconds <= 3600:
        raise ValueError('a bounded priority batch and wait are required')
    root = root.resolve()
    if not (root / 'queue.sqlite3').is_file():
        raise ValueError('an existing canonical owner queue is required')
    pins = {path: sha256_file(path) for path in OWNER_FILES}
    capacity = sampler or AcquisitionCapacitySampler(root)
    deadline = time.monotonic() + max_wait_seconds
    while time.monotonic() < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            return {'state': 'owner_changed', 'provider_calls_by_waiter': 0}
        status = inspect(root, request_id, now=datetime.now(UTC))
        if status.get('provider') != 'finmind' or status.get('request_id') != request_id:
            raise ValueError('priority receipt identity mismatch')
        if not status.get('priority_active'):
            return {'state': 'plan_expired', 'provider_calls_by_waiter': 0}
        items = status.get('items')
        if not isinstance(items, list) or not items:
            raise ValueError('priority request items missing')
        pending = []
        terminal = {'complete', 'observed_empty'}
        for item in items:
            key = tuple(json.loads(item['key']))
            if len(key) != 3 or not all(isinstance(value, str) for value in key):
                raise ValueError('invalid finite priority key')
            if item['state'] not in terminal:
                pending.append(key)
        if not pending:
            return {'state': 'native_tasks_terminal_unverified', 'items': len(items),
                    'data_completeness_claim': False, 'provider_calls_by_waiter': 0}
        batch = tuple(pending[:max_requests])
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
                result = owner.run_once(root, max_requests=len(batch), required_keys=batch,
                                        capacity_sampler=capacity, finite_priority_request_id=request_id)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        state = result.get('state')
        print(json.dumps({'state': 'owner_pass', 'owner_state': state, 'last_task': result.get('last_task'),
                          'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
        if state in {'invalid_token', 'not_entitled', 'invalid_request'}:
            return {'state': state, 'provider_calls_by_waiter': 0}
        # Re-read original task states even after a successful owner batch.
        delay = 0 if state == 'batch_complete' else 60
        if delay:
            time.sleep(min(delay, max(0, deadline - time.monotonic())))
    return {'state': 'finite_wait_elapsed', 'provider_calls_by_waiter': 0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scope', choices=SCOPES, default='settlement')
    parser.add_argument('--max-requests', type=int, default=4)
    parser.add_argument('--max-batches', type=int, default=1)
    parser.add_argument('--max-wait-seconds', type=int, default=600)
    parser.add_argument('--priority-request-id', help='Existing registered exact FinMind request; never opens tasks')
    args = parser.parse_args()
    if args.priority_request_id:
        result = run_registered_priority(ROOT, request_id=args.priority_request_id,
                                         max_requests=args.max_requests, max_wait_seconds=args.max_wait_seconds)
    else:
        result = run_batches(ROOT, max_requests=args.max_requests, max_batches=args.max_batches,
                             max_wait_seconds=args.max_wait_seconds, datasets=SCOPES[args.scope])
    print(json.dumps({'state': result.get('state'), 'last_task': result.get('last_task'),
                      'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
    return 75 if result.get('state') in {'finite_wait_elapsed', 'owner_changed', 'plan_expired',
                                         'invalid_token', 'not_entitled', 'invalid_request'} else 0


if __name__ == '__main__':
    raise SystemExit(main())
