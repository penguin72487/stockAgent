#!/usr/bin/env python3
"""Run one bounded derivative refresh through the existing Sponsor owner.

The supervisor does not query the provider itself. It uses the owner's
unchanged lock, capacity, quota, release and receipt checks.
"""
from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path
import time

from downloader import download_finmind_sponsor as owner
from downloader.artifact_io import sha256_file
from stockagent.data_sync.acquisition_capacity_sampler import AcquisitionCapacitySampler, capacity_retry_delay_seconds


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/sponsor'
DATASETS = (
    'TaiwanFuturesDaily', 'TaiwanFuturesDealerTradingVolumeDaily',
    'TaiwanFuturesInstitutionalInvestors', 'TaiwanFuturesInstitutionalInvestorsAfterHours',
    'TaiwanFuturesOpenInterestLargeTraders', 'TaiwanOptionDaily',
    'TaiwanOptionDealerTradingVolumeDaily', 'TaiwanOptionInstitutionalInvestors',
    'TaiwanOptionInstitutionalInvestorsAfterHours', 'TaiwanOptionOpenInterestLargeTraders',
    'TaiwanOptionVix',
)
OWNER_FILES = tuple(REPO / path for path in (
    'scripts/run_finmind_sponsor_derivative_due_wait.py', 'downloader/download_finmind_sponsor.py',
    'downloader/finmind_corrections.py', 'downloader/finmind_scheduling.py',
    'downloader/finmind_account.py', 'downloader/provider_priority.py',
    'stockagent/data_sync/acquisition_capacity_sampler.py', 'stockagent/data_sync/cache_capacity.py',
))


def run(root: Path, *, max_requests: int, max_wait_seconds: int, sampler=None) -> dict:
    if not 1 <= max_requests <= 16 or not 1 <= max_wait_seconds <= 1800:
        raise ValueError('a bounded derivative refresh and wait are required')
    root = root.resolve()
    if not (root / 'queue.sqlite3').is_file():
        raise ValueError('an existing canonical Sponsor queue is required')
    pins = {path: sha256_file(path) for path in OWNER_FILES}
    capacity = sampler or AcquisitionCapacitySampler(root)
    deadline = time.monotonic() + max_wait_seconds
    while time.monotonic() < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            return {'state': 'owner_changed', 'provider_calls_by_waiter': 0}
        admission = capacity.initial_sample(timeout_seconds=29)
        if admission.get('state') not in {'admitted', 'not_enrolled'}:
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
                result = owner.run_once(root, max_requests=max_requests, workers=1,
                                        datasets=DATASETS, capacity_sampler=capacity)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        print(json.dumps({'state': 'owner_pass', 'owner_state': result.get('state'),
                          'last_task': result.get('last_task'), 'provider_calls_by_waiter': 0},
                         ensure_ascii=False), flush=True)
        reason = result.get('capacity_admission', {}).get('reason')
        if result.get('state') == 'disk_guard' and reason in {'capacity_sample_pending', 'capacity_sample_expired'}:
            time.sleep(min(5, max(0, deadline - time.monotonic())))
            continue
        return result
    return {'state': 'finite_wait_elapsed', 'provider_calls_by_waiter': 0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-requests', type=int, default=12)
    parser.add_argument('--max-wait-seconds', type=int, default=600)
    args = parser.parse_args()
    result = run(ROOT, max_requests=args.max_requests, max_wait_seconds=args.max_wait_seconds)
    print(json.dumps({'state': result.get('state'), 'last_task': result.get('last_task'),
                      'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
    return 75 if result.get('state') in {'finite_wait_elapsed', 'owner_changed'} else 0


if __name__ == '__main__':
    raise SystemExit(main())
