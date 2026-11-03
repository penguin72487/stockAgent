#!/usr/bin/env python3
"""Let bounded priority checks wait on the existing Sponsor release owner.

This finite coordinator does not call FinMind or reset any tasks. It only
asks the existing, locked Sponsor owner to process already-due releases when
an explicitly registered FinMind priority request remains unfinished.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import time

from downloader.artifact_io import sha256_file
from downloader.finmind_scheduling import incremental_reservation
from downloader.provider_priority import inspect
from scripts.run_finmind_sponsor_derivative_due_wait import SCOPES, run as sponsor_run


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'
OWNER_FILES = tuple(REPO / path for path in (
    'scripts/run_finmind_priority_due_bridge.py',
    'scripts/run_finmind_sponsor_derivative_due_wait.py',
    'downloader/download_finmind_sponsor.py', 'downloader/download_finmind_complement.py',
    'downloader/finmind_errors.py', 'downloader/finmind_corrections.py',
    'downloader/finmind_scheduling.py', 'downloader/finmind_account.py',
    'downloader/provider_priority.py', 'stockagent/data_sync/acquisition_capacity_sampler.py',
))
SCOPE_ORDER = ('release_preconditions', 'underlying_research', 'underlying_due', 'derivatives')
TERMINAL = frozenset({'complete', 'observed_empty'})
CONTINUABLE = TERMINAL | {'pending', 'failed', 'inflight', 'running'}
BACKLOG_CONTRACT = 'finmind_option_tick_product_source_check_v1'
RUNNING = frozenset({'queued', 'running'})
RUN_ENDED = frozenset({'succeeded', 'failed', 'cancelled', 'interrupted', 'timed_out'})


def due_scopes(plan: dict, now: datetime) -> tuple[str, ...]:
    """Cover only already-due Sponsor events, without duplicating overlaps."""
    if plan.get('queue_errors') or plan.get('observed_at_utc') != now.isoformat():
        return ()
    due = set()
    for event in plan.get('events', []):
        if event.get('owner') != 'sponsor' or not event.get('requests'):
            continue
        stamp = datetime.fromisoformat(event['due_at_utc'])
        if stamp.tzinfo is None:
            raise ValueError('unaware release time')
        if stamp <= now:
            due.add(event['dataset'])
    result = []
    for scope in SCOPE_ORDER:
        covered = due.intersection(SCOPES[scope])
        if covered:
            result.append(scope)
            due.difference_update(covered)
    return tuple(result)


def backlog_request(prefix: Path) -> tuple[str, set[tuple[str, str, str]]] | None:
    """Follow only the newest registered batch of the explicitly named backlog."""
    prefix = prefix.resolve()
    if not prefix.is_relative_to(REPO / 'artifacts/markets'):
        raise ValueError('backlog prefix must be in the markets artifact root')
    paths = sorted(prefix.parent.glob(prefix.name + '_batch_[0-9][0-9][0-9][0-9].json'))
    if not paths:
        return None
    path = paths[-1]
    registration = path.with_name(path.stem + '_registration.json')
    # The owner publishes the plan before its registration receipt.
    if not registration.exists():
        return None
    if path.is_symlink() or registration.is_symlink():
        raise ValueError('backlog source evidence must not be a symlink')
    plan, receipt = json.loads(path.read_bytes()), json.loads(registration.read_bytes())
    request_id = receipt.get('request_id')
    rows = plan.get('checks')
    if (plan.get('contract') != BACKLOG_CONTRACT or receipt.get('contract') != BACKLOG_CONTRACT
            or receipt.get('source_plan') != str(path.resolve())
            or plan.get('scope') != 'per_product_physical_day_source_comparison_only_no_training_injection'
            or not isinstance(request_id, str) or not re.fullmatch(r'[0-9a-f]{64}', request_id)
            or not isinstance(rows, list) or not 0 < len(rows) <= 64):
        raise ValueError('invalid backlog source plan or registration')
    keys = {(row['dataset'], row['data_id'], row['partition']) for row in rows}
    if (len(keys) != len(rows) or any(dataset != 'TaiwanOptionTick' or not product or not day
                                      for dataset, product, day in keys)):
        raise ValueError('invalid or duplicate backlog product keys')
    return request_id, keys


def backlog_run_state(record: Path, prefix: Path) -> str:
    """Read the existing workflow owner's state without controlling its process."""
    record, prefix = record.resolve(), prefix.resolve()
    if (record.name != 'run.json' or not record.is_relative_to(REPO / 'artifacts/operations/agent-workflow/runs')
            or not record.is_file()):
        raise ValueError('existing workflow run record required')
    doc = json.loads(record.read_bytes())
    argv, state = doc.get('argv'), doc.get('state')
    if (doc.get('repo_root') != str(REPO) or not isinstance(argv, list)
            or 'scripts/run_finmind_option_corrected_product_backlog.py' not in argv
            or '--output-prefix' not in argv or argv.index('--output-prefix') + 1 >= len(argv)
            or Path(argv[argv.index('--output-prefix') + 1]).resolve() != prefix
            or state not in RUNNING | RUN_ENDED):
        raise ValueError('backlog workflow identity or state differs')
    return state


def run(root: Path, *, request_ids: tuple[str, ...], max_wait_seconds: int,
        backlog_prefix: Path | None = None, backlog_run_record: Path | None = None,
        clock=None, monotonic=None, sleep=None) -> dict:
    if (not request_ids or len(set(request_ids)) != len(request_ids)
            or not 1 <= max_wait_seconds <= 7 * 86400 or (backlog_run_record and not backlog_prefix)):
        raise ValueError('unique registered request IDs and a bounded wait are required')
    root = root.resolve()
    if not (root / 'queue.sqlite3').is_file() or not (root.parent / 'sponsor' / 'queue.sqlite3').is_file():
        raise ValueError('existing Complement and Sponsor queues are required')
    clock = clock or (lambda: datetime.now(UTC))
    monotonic = monotonic or time.monotonic
    sleep = sleep or time.sleep
    pins = {path: sha256_file(path) for path in OWNER_FILES}
    deadline = monotonic() + max_wait_seconds
    while monotonic() < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            return {'state': 'owner_changed', 'provider_calls_by_bridge': 0}
        now = clock()
        pending = []
        run_state = backlog_run_state(backlog_run_record, backlog_prefix) if backlog_run_record else None
        backlog = backlog_request(backlog_prefix) if backlog_prefix and run_state not in RUN_ENDED else None
        active_ids = tuple(dict.fromkeys((*request_ids, *((backlog[0],) if backlog else ()))))
        for request_id in active_ids:
            status = inspect(root, request_id, now=now)
            if status.get('provider') != 'finmind' or status.get('request_id') != request_id:
                raise ValueError('priority request identity mismatch')
            items = status.get('items')
            if not isinstance(items, list) or not items:
                raise ValueError('priority request has no exact tasks')
            if backlog and request_id == backlog[0]:
                actual = {tuple(json.loads(item['key'])) for item in items}
                if len(actual) != len(items) or actual != backlog[1]:
                    raise ValueError('backlog and registered priority keys differ')
            states = {item.get('state') for item in items}
            if status.get('priority_active') and states <= CONTINUABLE and not states <= TERMINAL:
                pending.append(request_id)
        if not pending:
            if backlog_prefix and run_state not in RUN_ENDED:
                # Registration of a following bounded batch is asynchronous.
                # While idle this coordinator makes no provider calls.
                sleep(min(10, max(0, deadline - monotonic())))
                continue
            return {'state': 'backlog_run_ended' if run_state in RUN_ENDED else 'no_active_dispatchable_requests',
                    'backlog_run_state': run_state, 'provider_calls_by_bridge': 0,
                    'data_completeness_claim': False}
        plan = incremental_reservation(root.parent, now)
        scopes = due_scopes(plan, now)
        if plan.get('queue_errors'):
            print(json.dumps({'state': 'schedule_unverified', 'provider_calls_by_bridge': 0}), flush=True)
        for scope in scopes:
            remaining = int(deadline - monotonic())
            if remaining < 1:
                break
            result = sponsor_run(root.parent / 'sponsor', max_requests=16,
                                 max_wait_seconds=min(60, remaining), datasets=SCOPES[scope])
            print(json.dumps({'state': 'existing_sponsor_owner_pass', 'scope': scope,
                              'owner_state': result.get('state'), 'provider_calls_by_bridge': 0},
                             ensure_ascii=False), flush=True)
        remaining = deadline - monotonic()
        if remaining > 0:
            sleep(min(30, remaining))
    return {'state': 'finite_wait_elapsed', 'provider_calls_by_bridge': 0, 'data_completeness_claim': False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request-id', action='append', required=True)
    parser.add_argument('--backlog-prefix', type=Path)
    parser.add_argument('--backlog-run-record', type=Path)
    parser.add_argument('--max-wait-seconds', type=int, default=3600)
    args = parser.parse_args()
    result = run(ROOT, request_ids=tuple(args.request_id), max_wait_seconds=args.max_wait_seconds,
                 backlog_prefix=args.backlog_prefix, backlog_run_record=args.backlog_run_record)
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 75 if result['state'] == 'owner_changed' else 0


if __name__ == '__main__':
    raise SystemExit(main())
