#!/usr/bin/env python3
"""Advance one finite metadata request through the native TEJ scheduler.

When a due incremental release precedes the requested discovery, start at most
one ordinary native owner task, then recheck capacity, the request and the
global selector. When the requested discovery is selected, use the existing
finite-discovery entrypoint. This supervisor is not a second desktop owner.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

from downloader.artifact_io import sha256_file
from downloader.dataset_lock import DatasetLockTimeout, exclusive_dataset_lock
from downloader.provider_priority import inspect
from downloader.tej_history import DesktopBridge, SOURCE_SCOPE_CONTRACT, _ready_task
from downloader.tej_incremental import PRIORITY as INCREMENTAL_PRIORITY
from downloader.tej_metadata_recovery import metadata_recovery_policy, recover_failed_metadata
from downloader.tej_startup import desktop_ready
from scripts.run_tej_finite_discovery_wait import OWNERS, classify_owner_result
from stockagent.data_sync.cache_capacity import acquisition_dispatch_status
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_tej'


def choice(row: dict | None, status: dict, *, metadata_recoverable: bool = False,
           source_states: dict[str, str] | None = None) -> str:
    """Classify the global selector without mutating the durable queue."""
    if status.get('provider') != 'tej' or not status.get('priority_active'):
        return 'priority_expired_or_invalid'
    requested = [x for x in status.get('items', []) if x.get('derived_from_requested_scope') is not True]
    if not requested:
        return 'invalid_empty_request'
    source_states = source_states or {}
    if any(state not in {'pending', 'running', 'complete'} for state in source_states.values()):
        return 'registered_source_blocked'
    if all(x.get('state') == 'complete' for x in requested) and all(
            state == 'complete' for state in source_states.values()):
        return 'complete'
    if any(x.get('state') in {'blocked', 'failed'} for x in requested):
        return 'recover_requested_metadata' if metadata_recoverable else 'requested_source_blocked'
    if row is None:
        return 'wait'
    pending = {x['key'] for x in requested if x.get('state') == 'pending'}
    if row['kind'] == 'discover' and row['task_id'] in pending:
        return 'requested_discovery'
    if row['kind'] == 'download' and row['priority'] == INCREMENTAL_PRIORITY:
        return 'prerequisite_incremental'
    if row['kind'] == 'download' and source_states.get(row['task_id']) == 'pending':
        return 'registered_source_prerequisite'
    return 'wait_for_other_global_work'


def verified_source_receipt(root: Path, receipt_path: Path) -> tuple[dict, Path]:
    """Bind a finite source receipt to its pinned plan and committed queue audit."""
    receipt = json.loads(receipt_path.read_text())
    if (receipt.get('contract') != 'finite_source_bound_priority_requests_v1'
            or receipt.get('global_plan_changed') is not False
            or receipt.get('desktop_actions') != 0 or receipt.get('unknown_outcomes_retried') != 0):
        raise ValueError('invalid finite source receipt')
    plan = (REPO / receipt['plan_path']).resolve()
    if not plan.is_relative_to((REPO / 'artifacts/markets').resolve()):
        raise ValueError('finite source plan must be in the research artifact root')
    if sha256_file(plan) != receipt.get('plan_sha256'):
        raise ValueError('finite source plan fingerprint changed')
    tasks = receipt.get('tasks')
    if not isinstance(tasks, list) or not tasks:
        raise ValueError('finite source receipt has no tasks')
    task_ids = [task.get('task_id') for task in tasks]
    if (any(not isinstance(task_id, str) or not task_id for task_id in task_ids)
            or len(set(task_ids)) != len(task_ids)):
        raise ValueError('finite source task identities invalid')
    db = (root / 'queue.sqlite3').resolve()
    with sqlite3.connect(f'{db.as_uri()}?mode=ro', uri=True, timeout=5) as con:
        row = con.execute('SELECT value FROM meta WHERE key=?',
                          ('finite_priority_plan:' + receipt['plan_sha256'],)).fetchone()
    if row is None or json.loads(row[0]) != receipt:
        raise ValueError('finite source receipt differs from committed queue audit')
    return receipt, plan


def source_task_states(root: Path, receipt: dict | None) -> dict[str, str]:
    """Read the exact registered tasks; never turn a failure into a retry."""
    if receipt is None:
        return {}
    db = (root / 'queue.sqlite3').resolve()
    states = {}
    with sqlite3.connect(f'{db.as_uri()}?mode=ro', uri=True, timeout=5) as con:
        for task in receipt['tasks']:
            row = con.execute('SELECT table_id,kind,scope_contract,state FROM tasks WHERE task_id=?',
                              (task['task_id'],)).fetchone()
            if row is None or row[:3] != (task['table_id'], 'download', SOURCE_SCOPE_CONTRACT):
                raise ValueError('registered finite source task identity changed')
            states[task['task_id']] = row[3]
    return states


def recoverable_metadata_task(root: Path, status: dict) -> str | None:
    """Select only one requested metadata failure, never an unknown data query."""
    if status.get('provider') != 'tej' or not status.get('priority_active'):
        return None
    requested = [item for item in status.get('items', [])
                 if item.get('derived_from_requested_scope') is not True]
    blocked = [item for item in requested if item.get('state') in {'blocked', 'failed'}]
    if len(blocked) != 1 or blocked[0].get('state') != 'blocked':
        return None
    task_id = blocked[0].get('key')
    if not isinstance(task_id, str):
        return None
    db = (root / 'queue.sqlite3').resolve()
    with sqlite3.connect(f'{db.as_uri()}?mode=ro', uri=True, timeout=5) as con:
        row = con.execute('SELECT kind,state,last_error_code FROM tasks WHERE task_id=?', (task_id,)).fetchone()
    return task_id if row == ('discover', 'blocked', 'metadata_preparation_requires_recovery') else None


def recover_requested_metadata(root: Path, request_id: str, task_id: str) -> dict:
    """Use the canonical proved-unsent recovery while holding its desktop lock."""
    with exclusive_dataset_lock(root / '.download.lock', provider='tej_smart_wizard', timeout_seconds=0):
        if recoverable_metadata_task(root, inspect(root, request_id)) != task_id:
            return {'state': 'metadata_scope_changed'}
        # Match the native scheduler's startup/transport admission. A recovery
        # attempt cannot authorize a query when the desktop is still hung.
        if not desktop_ready(root, require_recent=False):
            return {'state': 'metadata_desktop_not_ready'}
        config = json.loads((REPO / 'configs/tej_history.json').read_text())
        policy = metadata_recovery_policy(config)
        if policy is None:
            return {'state': 'metadata_recovery_policy_absent'}
        bridge = DesktopBridge(REPO, json.loads((root / 'desktop_session.json').read_text()))
        result = recover_failed_metadata(root, bridge, policy=policy, task_id=task_id)
        return result or {'state': 'metadata_recovery_not_due_or_unverified'}


def recoverable_owner_error(root: Path, request_id: str, pick: str, stdout: str) -> str | None:
    """Re-enter the proved-unsent recovery path only for a requested discovery.

    The native CLI exits nonzero when it records this metadata-only block.
    Unknown errors and data queries must still stop, rather than being replayed.
    The native recovery later verifies its negative-source proof under the lock.
    """
    if pick != 'requested_discovery':
        return None
    try:
        event = json.loads(stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return None
    if not isinstance(event, dict) or event.get('state') != 'metadata_preparation_requires_recovery':
        return None
    return recoverable_metadata_task(root, inspect(root, request_id))


def owner_desktop_unavailable_before_query(code: int, stdout: str) -> bool:
    """Recognize only the native CLI's proved-before-Preview wait outcome.

    The owner emits this state only after a BeforeDataQueryError and records a
    pending task with its own retry deadline. Other nonzero outcomes, including
    unknown or potentially submitted queries, must continue to stop the relay.
    """
    if code != 1:
        return False
    try:
        event = json.loads(stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return False
    return event == {'state': 'desktop_unavailable'}


def selected(root: Path) -> dict | None:
    """Read the same global selector against the existing queue, read-only."""
    db = (root / 'queue.sqlite3').resolve()
    with sqlite3.connect(f'{db.as_uri()}?mode=ro', uri=True, timeout=5) as con:
        con.row_factory = sqlite3.Row
        # Avoid repeatedly contending with a legitimate in-flight desktop
        # claim; its unknown-outcome recovery remains owned by the CLI.
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                       "(state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            return {'task_id': '', 'kind': 'inflight_or_unresolved', 'priority': 0}
        value = con.execute("SELECT value FROM meta WHERE key='runtime_policy'").fetchone()
        if value is None:
            raise ValueError('native runtime policy is absent')
        row = _ready_task(con, datetime.now(UTC).isoformat(), None, json.loads(value[0]))
        return None if row is None else {key: row[key] for key in ('task_id', 'kind', 'priority')}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request-id', required=True)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--source-priority-receipt', type=Path)
    parser.add_argument('--max-owner-attempts', type=int, default=100)
    args = parser.parse_args()
    if not 1 <= args.max_owner_attempts <= 100:
        raise ValueError('finite owner attempt bound must be 1..100')
    request_receipt = ROOT / 'priority_queries' / f'{args.request_id}.json'
    source_receipt, source_plan = (None, None)
    if args.source_priority_receipt:
        source_receipt, source_plan = verified_source_receipt(ROOT, args.source_priority_receipt.resolve())
    pin_paths = [*OWNERS, REPO / 'downloader/tej_startup.py',
                 REPO / 'stockagent/data_sync/windows_cold_io.py',
                 args.plan.resolve(), request_receipt, Path(__file__)]
    if source_plan:
        pin_paths.extend([source_plan, args.source_priority_receipt.resolve()])
    pins = {path: sha256_file(path) for path in pin_paths}
    if not os.environ.get('WSL_INTEROP') and Path('/run/WSL/1_interop').is_socket():
        os.environ['WSL_INTEROP'] = '/run/WSL/1_interop'
    owner_attempts = 0
    while owner_attempts < args.max_owner_attempts:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            print(json.dumps({'state': 'owner_or_scope_changed', 'owner_attempts': owner_attempts}), flush=True)
            return 75
        status = inspect(ROOT, args.request_id)
        active = subprocess.run(['systemctl', 'is-active', '--quiet', 'stockagent-tej-history.service'],
                                cwd=REPO, timeout=15, check=False)
        if active.returncode == 0:
            print(json.dumps({'state': 'existing_tej_owner_active'}), flush=True)
            time.sleep(60)
            continue
        if active.returncode not in {3, 4}:
            print(json.dumps({'state': 'service_state_unknown', 'exit_code': active.returncode}), flush=True)
            return 75
        try:
            metadata_task = recoverable_metadata_task(ROOT, status)
            pick = choice(selected(ROOT), status, metadata_recoverable=metadata_task is not None,
                          source_states=source_task_states(ROOT, source_receipt))
        except sqlite3.OperationalError as exc:
            if 'locked' not in str(exc).lower() and 'busy' not in str(exc).lower():
                raise
            print(json.dumps({'state': 'queue_read_busy'}), flush=True)
            time.sleep(30)
            continue
        if pick == 'complete':
            print(json.dumps({'state': 'requested_queue_work_complete_source_contents_unverified',
                              'owner_attempts': owner_attempts}), flush=True)
            return 0
        if pick in {'priority_expired_or_invalid', 'invalid_empty_request', 'requested_source_blocked',
                    'registered_source_blocked'}:
            print(json.dumps({'state': pick, 'owner_attempts': owner_attempts}), flush=True)
            return 75
        if pick not in {'requested_discovery', 'prerequisite_incremental', 'recover_requested_metadata',
                        'registered_source_prerequisite'}:
            print(json.dumps({'state': pick, 'owner_attempts': owner_attempts}), flush=True)
            time.sleep(30)
            continue
        try:
            capacity = acquisition_dispatch_status(ROOT)
        except CapacityDeferred as exc:
            capacity = {'state': 'deferred', 'reason': exc.details.get('reason')}
        if capacity.get('state') != 'admitted':
            print(json.dumps({'state': 'capacity_wait', 'capacity': capacity.get('state'),
                              'reason': capacity.get('reason')}), flush=True)
            time.sleep(60)
            continue
        if pick == 'recover_requested_metadata':
            try:
                result = recover_requested_metadata(ROOT, args.request_id, metadata_task)
            except DatasetLockTimeout:
                print(json.dumps({'state': 'lock_busy', 'owner_attempts': owner_attempts}), flush=True)
                time.sleep(5)
                continue
            if result.get('state') not in {'metadata_desktop_not_ready',
                                           'metadata_recovery_not_due_or_unverified'}:
                owner_attempts += 1
            print(json.dumps({'state': result.get('state'), 'task_id': metadata_task,
                              'owner_attempts': owner_attempts,
                              'market_data_queries_sent_by_recovery': 0}), flush=True)
            if result.get('state') in {'metadata_scope_changed', 'metadata_recovery_policy_absent'}:
                return 75
            time.sleep(60)
            continue
        cmd = [sys.executable, '-m', 'downloader.download_tej_history', 'run', '--root', str(ROOT),
               '--config', str(REPO / 'configs/tej_history.json'),
               '--session', str(ROOT / 'desktop_session.json'), '--max-tasks', '1']
        if pick == 'requested_discovery':
            cmd += ['--finite-discovery-request-id', args.request_id]
        # The CLI reacquires the canonical dataset lock and reselects globally.
        # A different owner can change the queue between snapshot and claim;
        # we never change task state or force a particular task here.
        child = subprocess.run(cmd, cwd=REPO, text=True, capture_output=True, check=False)
        state = classify_owner_result(child.returncode, child.stdout, child.stderr)
        if state == 'lock_busy':
            print(json.dumps({'state': state, 'owner_attempts': owner_attempts}), flush=True)
            time.sleep(5)
            continue
        owner_attempts += 1
        print(json.dumps({'state': state, 'selected_before_claim': pick, 'exit_code': child.returncode,
                          'owner_attempts': owner_attempts, 'stdout_tail': child.stdout[-500:],
                          'stderr_tail': child.stderr[-500:]}, ensure_ascii=False), flush=True)
        if state == 'error':
            if owner_desktop_unavailable_before_query(child.returncode, child.stdout):
                print(json.dumps({'state': 'wait_for_desktop_before_query',
                                  'owner_attempts': owner_attempts}), flush=True)
                # The next loop rechecks expiry, owner identity, global queue,
                # capacity and the native desktop lock before any new attempt.
                time.sleep(60)
                continue
            task_id = recoverable_owner_error(ROOT, args.request_id, pick, child.stdout)
            if task_id is not None:
                print(json.dumps({'state': 'requested_metadata_recovery_pending', 'task_id': task_id,
                                  'owner_attempts': owner_attempts,
                                  'market_data_queries_sent_by_recovery': 0}), flush=True)
                continue
            return child.returncode or 1
    print(json.dumps({'state': 'finite_owner_attempt_bound_reached', 'owner_attempts': owner_attempts}), flush=True)
    return 75


if __name__ == '__main__':
    raise SystemExit(main())
