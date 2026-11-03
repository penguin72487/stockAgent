#!/usr/bin/env python3
"""Wait for exact Sponsor daily-option rechecks using the canonical worker.

The waiter never accesses the provider. It preserves ordinary physical-disk,
quota, lock, release, cooldown and native correction requirements.
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
from downloader.finmind_option_daily_conflicts import load_daily_option_conflicts, validate_priority_keys


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/sponsor'
OWNER_FILES = (REPO / 'downloader/download_finmind_sponsor.py',
               REPO / 'downloader/download_finmind_complement.py',
               REPO / 'downloader/finmind_errors.py',
               REPO / 'downloader/finmind_corrections.py',
               REPO / 'downloader/finmind_option_daily_conflicts.py',
               REPO / 'downloader/provider_priority.py',
               REPO / 'scripts/check_acquisition_capacity.py',
               REPO / 'stockagent/data_sync/acquisition_capacity_sampler.py')


def native_states(root: Path, plan: dict) -> list[dict]:
    uri = (root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro'
    values = []
    with sqlite3.connect(uri, uri=True, timeout=5) as con:
        for request in plan['requests']:
            day = request['start_date']
            task = con.execute("SELECT state,next_attempt_at_utc,error_code FROM tasks "
                               "WHERE dataset='TaiwanOptionDaily' AND data_id='' AND partition=?", (day,)).fetchone()
            correction = con.execute("SELECT state FROM finmind_correction_tasks WHERE owner='sponsor' AND correction_id=? "
                                     "AND dataset='TaiwanOptionDaily' AND data_id='' AND partition=?",
                                     (request['correction_id'], day)).fetchone()
            if task is None or correction is None:
                raise ValueError('registered native option task or correction disappeared')
            values.append({'date': day, 'task_state': task[0], 'next_attempt_at_utc': task[1],
                           'error_code': task[2], 'correction_state': correction[0]})
    return values


def command(root: Path, request_id: str, key: tuple[str, str, str]) -> list[str]:
    return [sys.executable, '-m', 'downloader.download_finmind_sponsor', '--root', str(root),
            '--max-requests', '1', '--workers', '1', '--option-daily-conflict-request-id', request_id,
            '--task-key', *key]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request-id', required=True)
    args = parser.parse_args()
    pins = {path: sha256_file(path) for path in OWNER_FILES}
    while True:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            print(json.dumps({'state': 'owner_changed', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        now = datetime.now(UTC)
        loaded = load_daily_option_conflicts(ROOT, now)
        if not loaded:
            print(json.dumps({'state': 'plan_expired', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        plan, _ = loaded
        states = native_states(ROOT, plan)
        success = sum(row['correction_state'] in {'repaired', 'already_refreshed'} for row in states)
        pending = [row for row in states if row['correction_state'] == 'queued'
                   and row['task_state'] in {'pending', 'failed'}]
        if not pending:
            unresolved = len(states) - success
            print(json.dumps({'state': 'verified' if not unresolved else 'source_unresolved_after_attempts',
                              'verified': success, 'unresolved': unresolved, 'states': states,
                              'provider_calls_by_waiter': 0}), flush=True)
            return 0 if not unresolved else 75
        ready = [row for row in pending if not row['next_attempt_at_utc']
                 or datetime.fromisoformat(row['next_attempt_at_utc'].replace('Z', '+00:00')) <= now]
        if not ready:
            time.sleep(60)
            continue
        key = ('TaiwanOptionDaily', '', ready[0]['date'])
        validate_priority_keys(ROOT, args.request_id, (key,), now)
        # The owner performs its own fresh physical, account, quota and lock
        # preflights. No provider client or second scheduler lives here.
        child = subprocess.run(command(ROOT, args.request_id, key), cwd=REPO, text=True,
                               capture_output=True, timeout=1800)
        try:
            owner_state = json.loads(child.stdout.strip().splitlines()[-1]).get('state')
        except (IndexError, json.JSONDecodeError, AttributeError):
            owner_state = None
        print(json.dumps({'state': 'owner_pass', 'owner_state': owner_state, 'exit_code': child.returncode,
                          'date': key[2], 'stdout_tail': child.stdout[-1000:], 'stderr_tail': child.stderr[-500:],
                          'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
        if child.returncode not in {0, 2} or owner_state in {'invalid_token', 'not_entitled', 'account_unverified'}:
            return child.returncode or 75
        time.sleep(5 if owner_state == 'batch_complete' else 60 if owner_state == 'disk_guard' else 300)


if __name__ == '__main__':
    raise SystemExit(main())
