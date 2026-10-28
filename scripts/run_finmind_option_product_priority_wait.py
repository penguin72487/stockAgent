#!/usr/bin/env python3
"""Run an existing finite option-product check through the canonical owner.

This bounded waiter never logs in or calls a provider itself. It leaves the
general maintenance pause untouched and stops when its original intent expires,
the pinned owner or evidence changes, or the owner reports an access failure.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

from downloader.artifact_io import sha256_file
from downloader.finmind_product_query_verification import permits
from downloader.finmind_option_finite_priority import MAX_OPTION_KEYS, capacity_probe, verified_request
from downloader.provider_priority import inspect
from stockagent.data_sync.acquisition_capacity_sampler import AcquisitionCapacitySampler, capacity_retry_delay_seconds


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'
OWNER_FILES = (REPO / 'downloader/download_finmind_complement.py',
               REPO / 'downloader/finmind_product_query_verification.py',
               REPO / 'downloader/finmind_option_finite_priority.py',
               REPO / 'downloader/provider_priority.py',
               REPO / 'downloader/finmind_corrections.py',
               REPO / 'downloader/finmind_source_conflict_rechecks.py',
               REPO / 'downloader/finmind_option_monthly_evidence.py',
               REPO / 'scripts/check_acquisition_capacity.py',
               REPO / 'stockagent/data_sync/acquisition_capacity_sampler.py',
               REPO / 'stockagent/data_sync/cache_capacity.py')
TERMINAL = frozenset({'complete', 'observed_empty'})


def owner_command(root: Path, keys: tuple, verification: Path | None = None,
                  priority: Path | None = None) -> list[str]:
    if (verification is None) != (priority is None):
        raise ValueError('both finite priority plans are required')
    command = [sys.executable, '-m', 'downloader.download_finmind_complement',
               '--root', str(root), '--max-requests', str(len(keys))]
    if verification is not None and priority is not None:
        command.extend(['--option-finite-priority-verification', str(verification),
                        '--option-finite-priority-plan', str(priority)])
    for dataset, data_id, partition in keys:
        command.extend(['--task-key', dataset, data_id, partition])
    return command


def remaining(status: dict, request_id: str, keys: tuple) -> tuple:
    if status.get('provider') != 'finmind' or status.get('request_id') != request_id:
        raise ValueError('priority receipt identity mismatch')
    rows = status.get('items')
    if not isinstance(rows, list):
        raise ValueError('priority receipt items missing')
    native = {}
    for row in rows:
        key = tuple(json.loads(row['key']))
        if key in native:
            raise ValueError('duplicate priority receipt key')
        native[key] = row['state']
    if set(native) != set(keys):
        raise ValueError('priority receipt keys differ')
    return tuple(key for key in keys if native[key] not in TERMINAL)


def ready_batch(root: Path, pending: tuple, now: datetime) -> tuple:
    """Filter cooldown/cohort states through the same native owner rules."""
    from downloader.finmind_retry_cohorts import admission_clause
    uri = (root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(uri, uri=True, timeout=5) as con:
        clause, args = admission_clause(con, now)
        rows = con.execute("SELECT data_id,partition FROM tasks WHERE dataset='TaiwanOptionDaily' "
                           "AND kind='id_history' AND state IN ('pending','failed') "
                           "AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?)" + clause,
                           (now.isoformat(), *args))
        eligible = {(item[0], item[1]) for item in rows}
    return tuple(key for key in pending if (key[1], key[2]) in eligible)[:MAX_OPTION_KEYS]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verification-plan', type=Path, required=True)
    parser.add_argument('--priority-plan', type=Path, required=True)
    args = parser.parse_args()
    verification_path, priority_path = args.verification_plan.absolute(), args.priority_plan.absolute()
    request_id, keys, _ = verified_request(verification_path, priority_path)
    pins = {path: sha256_file(path) for path in (*OWNER_FILES, verification_path, priority_path)}
    with sqlite3.connect((ROOT / 'queue.sqlite3').resolve().as_uri() + '?mode=ro', uri=True) as con:
        if not all(permits(con, key) for key in keys):
            raise ValueError('the exact product verification was not registered with the owner')
    probe_keys = keys[:MAX_OPTION_KEYS]
    sampler = AcquisitionCapacitySampler(
        ROOT, probe=lambda root: capacity_probe(root, verification_path, priority_path, probe_keys, len(probe_keys)))
    previous = None
    while True:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            print(json.dumps({'state': 'owner_or_plan_changed', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        # Also check original evidence on every dispatch boundary.
        verified_request(verification_path, priority_path)
        status = inspect(ROOT, request_id, now=datetime.now(UTC))
        pending = remaining(status, request_id, keys)
        if not pending:
            print(json.dumps({'state': 'native_tasks_terminal_unverified', 'items': len(keys),
                              'data_completeness_claim': False, 'provider_calls_by_waiter': 0}), flush=True)
            return 0
        if not status['priority_active']:
            print(json.dumps({'state': 'plan_expired', 'remaining': len(pending),
                              'provider_calls_by_waiter': 0}), flush=True)
            return 75
        batch = ready_batch(ROOT, pending, datetime.now(UTC))
        if not batch:
            print(json.dumps({'state': 'waiting_native_retry_or_blocked', 'remaining': len(pending),
                              'provider_calls_by_waiter': 0}), flush=True)
            time.sleep(60)
            continue
        capacity = sampler.initial_sample()
        marker = (capacity.get('state'), capacity.get('reason'), len(pending))
        if marker != previous:
            print(json.dumps({'state': 'capacity_wait' if marker[0] != 'admitted' else 'capacity_admitted',
                              'capacity_state': marker[0], 'reason': marker[1], 'remaining': marker[2],
                              'provider_calls_by_waiter': 0}), flush=True)
            previous = marker
        if marker[0] != 'admitted':
            time.sleep(capacity_retry_delay_seconds(capacity))
            continue
        child = subprocess.run(owner_command(ROOT, batch, verification_path, priority_path), cwd=REPO, text=True,
                               capture_output=True, timeout=1800)
        try:
            owner_state = json.loads(child.stdout.strip().splitlines()[-1]).get('state')
        except (IndexError, json.JSONDecodeError, AttributeError):
            owner_state = None
        print(json.dumps({'state': 'owner_pass', 'owner_state': owner_state, 'exit_code': child.returncode,
                          'stdout_tail': child.stdout[-1500:], 'stderr_tail': child.stderr[-500:],
                          'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
        if child.returncode not in {0, 2, 75} or owner_state in {'invalid_token', 'not_entitled'}:
            return child.returncode or 75
        time.sleep(10 if owner_state == 'batch_complete' else 60 if owner_state == 'disk_guard' else 300)


if __name__ == '__main__':
    raise SystemExit(main())
