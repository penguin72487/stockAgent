#!/usr/bin/env python3
"""Wait for one already-authorized finite source plan and enroll its retry.

This metadata-only continuation neither acquires the desktop nor retries any
provider request. It preserves the original owner and its source receipts.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.provider_priority import inspect
from downloader.tej_finite_retry import _object, _source_plan, enroll
from downloader.tej_history import connect


def wait(root: Path, plan: Path, receipt: Path, upstream: Path, priority: str, output: Path,
         hours: float, *, interval: float = 10) -> int:
    if not 0 < hours <= 168 or not 1 <= interval <= 60 or output.exists():
        raise ValueError('A bounded wait and new admission receipt are required')
    root = root.resolve(strict=True)
    pins = {p: sha256_file(p) for p in (Path(__file__).resolve(), REPO/'downloader/tej_finite_retry.py',
                                      REPO/'downloader/tej_history.py', REPO/'downloader/provider_priority.py')}
    deadline = datetime.now(UTC) + timedelta(hours=hours)
    print(json.dumps({'state': 'waiting_for_finite_source_receipt', 'deadline_utc': deadline.isoformat(),
                      'provider_queries': 0}), flush=True)
    while datetime.now(UTC) < deadline:
        if any(sha256_file(p) != digest for p, digest in pins.items()):
            raise ValueError('Pinned admission implementation changed')
        parent = inspect(root, priority)
        if parent.get('provider') != 'tej' or parent.get('priority_active') is not True:
            print(json.dumps({'state': 'original_priority_expired', 'provider_queries': 0}), flush=True)
            return 75
        if plan.is_file() and receipt.is_file():
            # Read only the small task index while waiting. Full plan and
            # dependency hashing happens at admission or terminal readback.
            native = _object(receipt, 1024**2)
            items = native.get('tasks')
            if not isinstance(items, list) or not 1 <= len(items) <= 1000:
                raise ValueError('Bounded finite source task index required')
            with closing(connect(root)) as con:
                tasks = [con.execute('SELECT * FROM tasks WHERE task_id=?', (item['task_id'],)).fetchone()
                         for item in items]
                if any(row is None for row in tasks) or len({row['table_id'] for row in tasks}) != 1:
                    raise ValueError('Finite receipt tasks or table identity changed')
                if all(row['state'] == 'complete' for row in tasks):
                    _source_plan(con, root, plan, receipt)
                    result = {'state': 'source_tasks_already_complete', 'tasks': len(tasks),
                              'provider_queries': 0, 'market_receipts_independently_verified': False}
                    atomic_write_json(output, result, durable=True)
                    print(json.dumps(result), flush=True)
                    return 0
                retry = con.execute('SELECT * FROM desktop_retry_windows WHERE table_id=?',
                                    (tasks[0]['table_id'],)).fetchone()
                owner = (con.execute('SELECT state,last_error_code FROM tasks WHERE task_id=?',
                                     (retry['task_id'],)).fetchone() if retry is not None else None)
                ready = (retry is not None and retry['error_code'] == 'metadata_retry_scheduled'
                         and owner is not None and owner['state'] == 'pending'
                         and owner['last_error_code'] == 'metadata_retry_scheduled'
                         and all(row['state'] in ('pending', 'complete') for row in tasks))
            if ready:
                try:
                    result = enroll(root, plan, receipt)
                except ValueError as exc:
                    # A concurrent owner may finish a task or replace the
                    # metadata attempt during the optimistic read; retry only
                    # those explicit local races, never a failed proof.
                    if str(exc) not in {'Metadata retry changed during finite admission',
                                        'Finite task changed during admission',
                                        'Metadata retry discovery owner changed'}:
                        raise
                    time.sleep(interval)
                    continue
                atomic_write_json(output, result, durable=True)
                print(json.dumps(result, ensure_ascii=False), flush=True)
                return 0
        elif upstream.is_file():
            state = json.loads(upstream.read_text()).get('state')
            if state in {'failed', 'cancelled', 'succeeded', 'complete', 'completed'}:
                print(json.dumps({'state': 'upstream_ended_without_finite_source_receipt',
                                  'upstream_state': state, 'provider_queries': 0}), flush=True)
                return 75
        time.sleep(interval)
    print(json.dumps({'state': 'finite_source_wait_expired', 'provider_queries': 0}), flush=True)
    return 75


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('root', 'plan', 'receipt', 'upstream', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--priority-request-id', required=True)
    parser.add_argument('--hours', type=float, default=24)
    args = parser.parse_args()
    raise SystemExit(wait(args.root, args.plan, args.receipt, args.upstream,
                          args.priority_request_id, args.output, args.hours))


if __name__ == '__main__':
    main()
