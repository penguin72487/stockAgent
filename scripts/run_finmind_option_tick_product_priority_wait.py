#!/usr/bin/env python3
"""Supervise a finite, evidenced tick comparison through the canonical owner.

The waiter does not contact the provider or hold its lock while waiting.  A
successful native receipt is a source observation, not a training admission.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

from downloader.artifact_io import sha256_file
from downloader.provider_priority import inspect
from scripts.request_finmind_option_tick_product_checks import CONTRACT, DATASET
from stockagent.data_sync.acquisition_capacity_sampler import (
    AcquisitionCapacitySampler, capacity_retry_delay_seconds,
)


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'
FUTURES_CONTRACT = 'finmind_futures_tick_product_source_check_v1'
OWNERS = tuple(REPO / name for name in (
    'scripts/run_finmind_option_tick_product_priority_wait.py',
    'scripts/request_finmind_option_tick_product_checks.py',
    'scripts/audit_finmind_option_tick_product_checks.py',
    'scripts/audit_taifex_option_contract_presence.py',
    'scripts/request_finmind_futures_tick_product_checks.py',
    'scripts/audit_finmind_futures_tick_product_checks.py',
    'downloader/finmind_option_contract_keys.py',
    'downloader/finmind_corrections.py', 'downloader/finmind_source_conflict_rechecks.py',
    'downloader/finmind_futures_tick_conflicts.py',
    'downloader/download_finmind_complement.py', 'downloader/finmind_supplemental.py',
    'downloader/finmind_option_stream.py', 'downloader/finmind_errors.py',
    'downloader/finmind_storage_objects.py', 'downloader/provider_priority.py',
    'downloader/finmind_account.py', 'downloader/finmind_scheduling.py',
    'downloader/finmind_retry_cohorts.py', 'downloader/finmind_json_pipeline.py',
    'downloader/finmind_product_query_verification.py',
    'downloader/paused_provider_owners.py',
    'stockagent/data_sync/acquisition_capacity_sampler.py',
    'stockagent/data_sync/cache_capacity.py',
    'stockagent/data_sync/provider_workspace.py',
    'stockagent/data_sync/workspace_volume.py',
))
TERMINAL = frozenset({'complete', 'observed_empty'})


def checked_inputs(path: Path, root: Path, request_id: str, now: datetime) -> tuple[dict, dict]:
    plan = json.loads(path.read_bytes())
    dataset = {CONTRACT: DATASET, FUTURES_CONTRACT: 'TaiwanFuturesTick'}.get(plan.get('contract'))
    if dataset is None or plan.get('scope') != 'per_product_physical_day_source_comparison_only_no_training_injection':
        raise ValueError('source check plan has an incompatible contract')
    rows = plan.get('checks')
    if not isinstance(rows, list) or not 0 < len(rows) <= 64:
        raise ValueError('source comparison needs 1 to 64 explicit product days')
    commitments, additional_sources = {}, set()

    def declare(source: Path, digest: str) -> None:
        source = source.absolute()
        if source in commitments and commitments[source] != digest:
            raise ValueError('retained original has conflicting source commitments')
        commitments[source] = digest

    for source, digest in plan.get('additional_evidence_pins', {}).items():
        evidence = Path(source).resolve()
        if not evidence.is_relative_to(root.parents[1]):
            raise ValueError('additional source evidence changed')
        declare(evidence, digest)
        additional_sources.add(evidence.absolute())
    for row in rows:
        if row['dataset'] != dataset or not row['data_id'] or not row['audited_missing_contracts']:
            raise ValueError('invalid source comparison product key')
        if dataset == 'TaiwanFuturesTick' and ('baseline_parquet' not in row or 'baseline_parquet_sha256' not in row):
            raise ValueError('futures source check requires the pinned initial payload')
        pins = [('original_receipt', 'original_receipt_sha256'),
                ('original_parquet', 'original_parquet_sha256'),
                ('conflict_plan', 'conflict_plan_sha256')]
        if 'baseline_parquet' in row:
            pins.append(('baseline_parquet', 'baseline_parquet_sha256'))
        for field, digest in pins:
            declare(Path(row[field]), row[digest])
    # Multiple products on the same day share the original all-market source
    # and conflict plan. Verify each complete byte file once per readback,
    # retaining a fresh before/after generation check across the whole check.
    signature = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    verified = {}
    for source, digest in commitments.items():
        before = signature(source.stat())
        if sha256_file(source) != digest or signature(source.stat()) != before:
            if source in additional_sources:
                raise ValueError('additional source evidence changed')
            raise ValueError('retained original or conflict evidence changed')
        verified[source] = before
    status = inspect(root, request_id, now=now)
    expected = {(row['dataset'], row['data_id'], row['partition']) for row in rows}
    actual = {tuple(json.loads(row['key'])) for row in status['items']}
    if (status['provider'] != 'finmind' or status['expires_at_utc'] != plan['expires_at_utc']
            or len(expected) != len(rows) or len(actual) != len(status['items']) or actual != expected):
        raise ValueError('source and registered priority identities differ')
    if any(signature(source.stat()) != before for source, before in verified.items()):
        raise ValueError('retained original or conflict evidence changed during readback')
    return plan, status


def ready_keys(status: dict, now: datetime) -> tuple[tuple[str, str, str], ...]:
    ready = []
    for row in status['items']:
        due = row.get('next_attempt_at_utc')
        if row['state'] in {'pending', 'failed'} and (not due or datetime.fromisoformat(due) <= now):
            ready.append(tuple(json.loads(row['key'])))
    return tuple(ready[:64])


def owner_command(root: Path, request_id: str, keys: tuple) -> list[str]:
    result = [sys.executable, '-m', 'downloader.download_finmind_complement', '--root', str(root),
              '--max-requests', str(len(keys)), '--finite-priority-request-id', request_id]
    for key in keys:
        result.extend(['--task-key', *key])
    return result


def retry_delay(owner_state: str | None, root: Path, owner_started: datetime) -> tuple[int, str | None]:
    """Retry transient capacity sampling quickly, without weakening admission.

    The CLI summary omits the capacity reason. Its owner status contains it,
    but another process may have replaced that file; use only a recent,
    matching disk-guard observation. This controls waiting time only: the
    next owner still revalidates physical capacity, source scope and quota.
    """
    if owner_state == 'batch_complete':
        return 10, None
    if owner_state != 'disk_guard':
        return 300, None
    try:
        status = json.loads((root / 'status.json').read_text())
        observed = datetime.fromisoformat(status['observed_at_utc'])
        capacity = status.get('capacity_admission', {})
        if (status.get('state') != 'disk_guard' or observed.tzinfo is None
                or observed < owner_started or not isinstance(capacity, dict)):
            return 60, None
        reason = capacity.get('reason')
        return capacity_retry_delay_seconds(capacity), reason if isinstance(reason, str) else None
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return 60, None


def wait_until_owner_unlock(root: Path, *, timeout_seconds: float = 300, poll_seconds: float = 5) -> bool:
    """Observe the canonical lock only; never start a duplicate provider call."""
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError('positive finite lock wait required')
    path, deadline = root / 'worker.lock', time.monotonic() + timeout_seconds
    while True:
        try:
            with path.open('r') as handle:
                if not os.path.samestat(os.fstat(handle.fileno()), path.stat()):
                    return False
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    try:
                        return os.path.samestat(os.fstat(handle.fileno()), path.stat())
                    finally:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(poll_seconds, remaining))


def priority_dispatch_ready(root: Path, now: datetime) -> bool:
    """Inspect the same local reservation and quota without issuing a request."""
    from contextlib import closing
    from downloader.finmind_account import backfill_budget
    from downloader.finmind_product_query_verification import prioritize_source_checks
    from downloader.finmind_scheduling import _read_metadata, incremental_reservation

    try:
        plan = incremental_reservation(root.parent, now)
        with closing(sqlite3.connect((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro',
                                     uri=True, timeout=2)) as connection:
            plan = prioritize_source_checks(connection, plan, now, frozenset(), finite_query_ready=True)
        account = _read_metadata(root.parent / 'account_status.json')
        return bool(backfill_budget(account, root.parent, fixed_incremental_requests=plan['reserve_requests'],
                    in_flight=0, now=now, prioritize_due=True, reservation_plan=plan)['allowed'])
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error):
        return False


def wait_for_priority_dispatch(root: Path, *, timeout_seconds: float = 300, poll_seconds: float = 10,
                               monotonic=None, sleep=None, clock=None) -> bool:
    """Wake on verified local release/quota evidence, without starting an owner."""
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError('positive finite priority wait required')
    monotonic, sleep = monotonic or time.monotonic, sleep or time.sleep
    clock = clock or (lambda: datetime.now(UTC))
    deadline = monotonic() + timeout_seconds
    while (remaining := deadline - monotonic()) > 0:
        sleep(min(poll_seconds, remaining))
        if priority_dispatch_ready(root, clock()):
            return True
    return False


def run(path: Path, request_id: str, *, root: Path = ROOT, capacity=None,
        clock=None, sleep=None) -> int:
    """Wait without reopening the owner or hashing source payloads when blocked.

    The initial and every dispatch/terminal readback verify the full source.
    During an ordinary capacity or retry wait only the pinned implementation,
    priority state and canonical capacity sampler are read. The owner retains
    its independent, per-request capacity, source, quota and lock checks.
    """
    clock = clock or (lambda: datetime.now(UTC))
    sleep = sleep or time.sleep
    path = path.resolve()
    pins = {item: sha256_file(item) for item in (*OWNERS, path)}
    try:
        _, initial = checked_inputs(path, root, request_id, clock())
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({'state': 'source_evidence_changed_or_invalid', 'error': type(error).__name__,
                          'provider_calls_by_waiter': 0}), flush=True)
        return 75
    expires = datetime.fromisoformat(initial['expires_at_utc'])
    if expires.tzinfo is None:
        raise ValueError('source comparison requires a timezone-aware expiry')
    capacity = capacity or AcquisitionCapacitySampler(root)
    previous_capacity = None
    while True:
        if any(sha256_file(item) != digest for item, digest in pins.items()):
            print(json.dumps({'state': 'owner_or_plan_changed', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        now = clock()
        current = inspect(root, request_id, now=now)
        current_states = [row['state'] for row in current['items']]
        terminal = all(state in TERMINAL for state in current_states)
        if not terminal and current['priority_active'] and now < expires:
            if any(state not in TERMINAL | {'pending', 'failed', 'inflight', 'running'}
                   for state in current_states):
                print(json.dumps({'state': 'native_task_blocked', 'states': current_states,
                                  'provider_calls_by_waiter': 0}), flush=True)
                return 75
            if not ready_keys(current, now):
                sleep(min(60, max(0, (expires - now).total_seconds())))
                continue
            sample = capacity.initial_sample()
            if sample.get('state') not in {'admitted', 'not_enrolled'}:
                marker = (sample.get('state'), sample.get('reason'))
                if marker != previous_capacity:
                    print(json.dumps({'state': 'waiting_capacity_before_owner',
                                      'capacity': sample, 'provider_calls_by_waiter': 0,
                                      'owner_started': False}), flush=True)
                    previous_capacity = marker
                sleep(min(capacity_retry_delay_seconds(sample),
                          max(0, (expires - clock()).total_seconds())))
                continue
            previous_capacity = None
        try:
            _, status = checked_inputs(path, root, request_id, clock())
        except (OSError, ValueError, KeyError, TypeError) as error:
            print(json.dumps({'state': 'source_evidence_changed_or_invalid', 'error': type(error).__name__,
                              'provider_calls_by_waiter': 0}), flush=True)
            return 75
        states = [row['state'] for row in status['items']]
        if all(state in TERMINAL for state in states) or not status['priority_active'] or clock() >= expires:
            from downloader.artifact_io import atomic_write_json
            if json.loads(path.read_bytes())['contract'] == FUTURES_CONTRACT:
                from scripts.audit_finmind_futures_tick_product_checks import audit
            else:
                from scripts.audit_finmind_option_tick_product_checks import audit
            result = audit(path, root, request_id, clock())
            report = path.with_name(path.stem + '_readback.json')
            atomic_write_json(report, result)
            finished = all(state in TERMINAL for state in states)
            print(json.dumps({'state': 'native_results_read_back' if finished else 'plan_expired',
                              'states': states, 'readback': str(report),
                              'provider_calls_by_waiter': 0, 'training_ready': False}), flush=True)
            return 0 if finished else 75
        if any(state not in TERMINAL | {'pending', 'failed', 'inflight', 'running'} for state in states):
            print(json.dumps({'state': 'native_task_blocked', 'states': states, 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        keys = ready_keys(status, clock())
        if keys:
            # A capacity probe or source readback can take time. Recheck code
            # identity and expiry at the actual child-launch boundary.
            if any(sha256_file(item) != digest for item, digest in pins.items()):
                print(json.dumps({'state': 'owner_or_plan_changed', 'provider_calls_by_waiter': 0}), flush=True)
                return 75
            started = clock()
            if started >= expires:
                continue
            child = subprocess.run(owner_command(root, request_id, keys), cwd=REPO, text=True,
                                   capture_output=True, timeout=1800)
            try:
                owner_state = json.loads(child.stdout.strip().splitlines()[-1]).get('state')
            except (IndexError, json.JSONDecodeError, AttributeError):
                owner_state = None
            delay, capacity_reason = retry_delay(owner_state, root, started)
            print(json.dumps({'state': 'owner_pass', 'owner_state': owner_state, 'exit_code': child.returncode,
                              'capacity_reason': capacity_reason, 'retry_delay_seconds': delay,
                              'stdout_tail': child.stdout[-1200:], 'stderr_tail': child.stderr[-300:],
                              'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
            if child.returncode not in {0, 2} or owner_state in {'invalid_token', 'not_entitled', 'ip_banned'}:
                return child.returncode or 75
            if child.returncode == 2 and 'FinMind complement worker already running' in child.stderr:
                unlocked = wait_until_owner_unlock(root)
                print(json.dumps({'state': 'owner_unlock_observed' if unlocked else 'owner_unlock_unresolved',
                                  'provider_calls_by_waiter': 0}), flush=True)
                continue
            if owner_state == 'incremental_reserve':
                ready = wait_for_priority_dispatch(root, timeout_seconds=delay)
                print(json.dumps({'state': 'priority_dispatch_ready' if ready else 'priority_wait_elapsed',
                                  'provider_calls_by_waiter': 0}), flush=True)
            else:
                sleep(min(delay, max(0, (expires - clock()).total_seconds())))
        else:
            sleep(min(60, max(0, (expires - clock()).total_seconds())))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-plan', required=True, type=Path)
    parser.add_argument('--request-id', required=True)
    args = parser.parse_args()
    return run(args.source_plan, args.request_id)


if __name__ == '__main__':
    raise SystemExit(main())
