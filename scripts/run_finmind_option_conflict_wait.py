#!/usr/bin/env python3
"""Wait for capacity, then run bounded source checks through the owner.

The waiter holds no worker/account lock and does not invoke a provider. The
existing Complement CLI owns the lock, capacity and quota checks, and retains
the original receipts. The sidecar's expiry bounds the entire operation.
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
from downloader.finmind_source_conflict_rechecks import (
    CONTRACT_BATCHES, PRODUCT_BATCHES, RECENT_BATCHES, SIDECAR, load_option_conflicts,
    load_option_contract_conflicts, load_option_product_conflicts, load_recent_option_conflicts,
)
from stockagent.data_sync.acquisition_capacity_sampler import AcquisitionCapacitySampler, capacity_retry_delay_seconds


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'
OWNERS = (REPO / 'downloader/download_finmind_complement.py',
          REPO / 'downloader/finmind_corrections.py',
          REPO / 'downloader/finmind_source_conflict_rechecks.py',
          REPO / 'downloader/finmind_option_monthly_evidence.py',
          REPO / 'scripts/run_finmind_option_conflict_wait.py')


def read_states(root: Path, requests: list[dict]) -> list[dict]:
    db = (root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(db, uri=True, timeout=5) as connection:
        table = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='finmind_correction_tasks'").fetchone()
        result = []
        for request in requests:
            day, identity = request['start_date'], request['correction_id']
            task = connection.execute("SELECT state,error_code FROM tasks WHERE dataset='TaiwanOptionTick' AND data_id='' AND partition=?",
                                      (day,)).fetchone()
            correction = (connection.execute("SELECT state FROM finmind_correction_tasks WHERE owner='complement' AND correction_id=? "
                                             "AND dataset='TaiwanOptionTick' AND data_id='' AND partition=?",
                                             (identity, day)).fetchone() if table else None)
            result.append({'date': day, 'task_state': task[0] if task else 'missing_task',
                           'error_code': task[1] if task else None,
                           'correction_state': correction[0] if correction else 'not_adopted'})
    return result


def done(states: list[dict]) -> bool:
    # The owner only records already_refreshed after verifying the existing
    # post-notice receipt. Treat it as terminal without re-fetching that key.
    terminal = {'repaired', 'already_refreshed', 'observed_empty_unverified', 'calendar_excluded_unverified'}
    return bool(states) and all(item['correction_state'] in terminal
                                or (item['correction_state'] == 'queued' and item['task_state'] == 'complete')
                                for item in states)


def active_plans(root: Path, now: datetime, product_paths: list[Path], contract_paths: list[Path],
                 recent_paths: list[Path] = ()) -> list[dict]:
    plans = []
    loaded = load_option_conflicts(root, now)
    if loaded:
        plans.append(loaded[0])
    for path in product_paths:
        loaded = load_option_product_conflicts(root, now, sidecar=path)
        if loaded:
            plans.append(loaded[0])
    for path in contract_paths:
        loaded = load_option_contract_conflicts(root, now, sidecar=path)
        if loaded:
            plans.append(loaded[0])
    for path in recent_paths:
        loaded = load_recent_option_conflicts(root, now, sidecar=path)
        if loaded:
            plans.append(loaded[0])
    return plans


def select_keys(requests: list[dict], states: list[dict]) -> list[tuple[str, str, str]]:
    """Select at most 64 distinct pending source days from the evidenced plans."""
    keys = []
    seen = set()
    for request, state in zip(requests, states, strict=True):
        key = ('TaiwanOptionTick', '', request['start_date'])
        if not done([state]) and key not in seen:
            keys.append(key)
            seen.add(key)
            if len(keys) == 64:
                break
    return keys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request-id', required=True)
    args = parser.parse_args()
    root = ROOT.resolve()
    sidecar = root / SIDECAR
    product_paths = sorted((root / PRODUCT_BATCHES).glob('*.json'))
    contract_paths = sorted((root / CONTRACT_BATCHES).glob('*.json'))
    recent_paths = sorted((root / RECENT_BATCHES).glob('*.json'))
    if len(product_paths) > 16 or len(contract_paths) > 16 or len(recent_paths) > 16:
        raise ValueError('too many product-conflict batches')
    plan_paths = ([sidecar] if sidecar.exists() else []) + product_paths + contract_paths + recent_paths
    priority_receipt = root / 'priority_queries' / f'{args.request_id}.json'
    pins = {str(path): sha256_file(path) for path in (*OWNERS, *plan_paths, priority_receipt)}
    plans = active_plans(root, datetime.now(UTC), product_paths, contract_paths, recent_paths)
    expiry = {str(path): datetime.fromisoformat(json.loads(path.read_text())['expires_at_utc'].replace('Z', '+00:00'))
              for path in plan_paths}
    sampler = AcquisitionCapacitySampler(root)
    previous = None
    while True:
        now = datetime.now(UTC)
        if (sorted((root / PRODUCT_BATCHES).glob('*.json')) != product_paths
                or sorted((root / CONTRACT_BATCHES).glob('*.json')) != contract_paths
                or sorted((root / RECENT_BATCHES).glob('*.json')) != recent_paths
                or any(sha256_file(Path(path)) != digest for path, digest in pins.items())):
            print(json.dumps({'state': 'owner_or_plan_changed', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        if not expiry or any(now >= value for value in expiry.values()):
            # Revalidate and drop expired batches, keeping still-valid intents.
            plans = active_plans(root, now, product_paths, contract_paths, recent_paths)
            expiry = {path: value for path, value in expiry.items() if now < value}
        if not plans:
            print(json.dumps({'state': 'plan_expired', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        requests = [request for plan in plans for request in plan['requests']]
        states = read_states(root, requests)
        if done(states):
            unverified = sum(item['correction_state'] not in {'repaired', 'already_refreshed'} for item in states)
            print(json.dumps({'state': 'source_rechecks_finished' if not unverified else 'source_unresolved_after_attempts',
                              'items': len(states),
                              'verified_repaired': sum(item['correction_state'] == 'repaired' for item in states),
                              'verified_already_refreshed': sum(item['correction_state'] == 'already_refreshed' for item in states),
                              'unverified': unverified,
                              'full_contract_or_training_completeness': False,
                              'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
            return 75 if unverified else 0
        capacity = sampler.initial_sample()
        marker = (capacity.get('state'), capacity.get('reason'))
        if marker != previous:
            print(json.dumps({'state': 'capacity_wait' if marker[0] != 'admitted' else 'capacity_admitted',
                              'capacity_state': marker[0], 'reason': marker[1], 'items': len(states),
                              'unadopted': sum(item['correction_state'] == 'not_adopted' for item in states),
                              'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
            previous = marker
        if marker[0] == 'admitted':
            # Recheck all source hashes at dispatch, not merely at wait start.
            plans = active_plans(root, now, product_paths, contract_paths, recent_paths)
            requests = [request for plan in plans for request in plan['requests']]
            states = read_states(root, requests)
            selected = select_keys(requests, states)
            if not selected:
                continue
            command = [sys.executable, '-m', 'downloader.download_finmind_complement', '--root', str(root),
                       '--max-requests', str(len(selected)), '--finite-priority-request-id', args.request_id]
            for key in selected:
                command.extend(['--task-key', *key])
            try:
                child = subprocess.run(command, cwd=REPO, text=True, capture_output=True, timeout=1800)
                try:
                    owner_state = json.loads(child.stdout.strip().splitlines()[-1]).get('state')
                except (IndexError, json.JSONDecodeError, AttributeError):
                    owner_state = None
                print(json.dumps({'state': 'owner_pass', 'exit_code': child.returncode,
                                  'stdout_tail': child.stdout[-2000:], 'stderr_tail': child.stderr[-1000:],
                                  'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
            except subprocess.TimeoutExpired:
                print(json.dumps({'state': 'owner_pass_timeout', 'provider_calls_by_waiter': 0}), flush=True)
                return 75
            if child.returncode not in {0, 2, 75}:
                return child.returncode
            # A fresh sample can finish after the short owner preflight. Retry
            # that deferred case promptly; completed finite batches can advance
            # immediately, while other unresolved states retain a longer wait.
            # The owner still repeats its own capacity and quota checks.
            time.sleep(60 if owner_state == 'disk_guard' else 5 if owner_state == 'batch_complete' else 300)
        else:
            time.sleep(capacity_retry_delay_seconds(capacity))


if __name__ == '__main__':
    raise SystemExit(main())
