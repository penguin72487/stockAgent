#!/usr/bin/env python3
"""Safely load the finite-retry selector when ordinary capacity permits.

This supervisor never queries a provider or creates another desktop owner.
The existing systemd owner gets SIGTERM through its normal graceful restart;
its unfinished Preview must finish or enter its existing recovery contract.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_finite_retry import validate_before_claim
from downloader.tej_history import _ready_task, connect
from stockagent.data_sync.cache_capacity import acquisition_dispatch_status
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

UNIT = 'stockagent-tej-history.service'


def unit_state() -> dict:
    result = subprocess.run(['systemctl', 'show', UNIT, '-p', 'ActiveState', '-p', 'SubState', '-p', 'MainPID',
                             '-p', 'KillMode', '-p', 'TimeoutStopUSec'],
                            text=True, capture_output=True, timeout=15, check=True)
    return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)


def checked_capacity(root: Path) -> dict:
    try:
        return acquisition_dispatch_status(root)
    except CapacityDeferred as exc:
        return {'state': 'deferred', 'reason': exc.details.get('reason'), 'retryable': True}


def verify_target(root: Path, table_id: str) -> str:
    with closing(connect(root)) as con:
        policy = json.loads(con.execute("SELECT value FROM meta WHERE key='runtime_policy'").fetchone()[0])
        task = _ready_task(con, datetime.now(UTC).isoformat(), 'download', policy, table_id)
        if task is None:
            raise ValueError('No due verified finite task remains for this table')
        validate_before_claim(con, root, task)
        return task['task_id']


def wait(root: Path, table_id: str, output: Path, hours: float, *, interval: float = 60) -> int:
    if not 0 < hours <= 168 or not 1 <= interval <= 60 or output.exists():
        raise ValueError('A bounded wait and a new output receipt are required')
    root = root.resolve(strict=True)
    files = [REPO / 'scripts/runtime_env.sh', REPO / 'scripts/run_tej_history.sh',
             REPO / 'scripts/tej_smart_wizard_bridge.ps1', REPO / 'downloader/download_tej_history.py',
             REPO / 'downloader/provider_priority.py', REPO / 'downloader/dataset_lock.py',
             REPO / 'stockagent/data_sync/cache_capacity.py', REPO / 'configs/tej_history.json',
             Path(__file__).resolve(), *sorted((REPO / 'downloader').glob('tej_*.py'))]
    pins = {str(p): sha256_file(p) for p in files}
    deadline = datetime.now(UTC) + timedelta(hours=hours)
    initial = unit_state()
    initial_pid = initial.get('MainPID')
    print(json.dumps({'state': 'waiting_for_ordinary_capacity', 'initial_owner_pid': initial_pid,
                      'deadline_utc': deadline.isoformat(), 'provider_queries': 0}), flush=True)
    restart_sent = False
    while datetime.now(UTC) < deadline:
        if any(sha256_file(Path(p)) != digest for p, digest in pins.items()):
            raise ValueError('Pinned rollout implementation changed')
        unit = unit_state()
        pid = unit.get('MainPID')
        if (pid and pid not in {'0', initial_pid} and unit.get('ActiveState') == 'active'):
            status_path = root / 'scheduler_status.json'
            if status_path.is_file():
                status = json.loads(status_path.read_text())
                if str(status.get('owner_pid')) == pid:
                    result = {'state': 'new_canonical_owner_observed', 'owner_pid': pid,
                              'initial_owner_pid': initial_pid, 'graceful_restart_requested': restart_sent,
                              'observed_at_utc': datetime.now(UTC).isoformat(), 'provider_queries': 0,
                              'source_rows_adopted_by_rollout': False,
                              'source_receipts_still_require_readback': True, 'implementation_sha256': pins}
                    atomic_write_json(output, result, durable=True)
                    print(json.dumps(result, ensure_ascii=False), flush=True)
                    return 0
        if not restart_sent:
            capacity = checked_capacity(root)
            if capacity.get('state') == 'admitted':
                # Never change an intentionally stopped owner or a differently
                # configured unit. The established graceful-stop policy is the
                # reason we may update the same owner after this admission.
                if (unit.get('ActiveState') != 'active' or not pid or pid == '0' or pid != initial_pid
                        or unit.get('KillMode') != 'mixed' or unit.get('TimeoutStopUSec') != '31min 40s'):
                    print(json.dumps({'state': 'owner_not_ready_for_handoff', 'unit': unit,
                                      'provider_queries': 0}), flush=True)
                else:
                    target = verify_target(root, table_id)
                    # Recheck the original normal physical-backing guard just
                    # before requesting the restart. Never weaken its reserve.
                    latest = checked_capacity(root)
                    if latest.get('state') == 'admitted':
                        subprocess.run(['systemctl', 'restart', '--no-block', UNIT], timeout=20, check=True)
                        restart_sent = True
                        print(json.dumps({'state': 'graceful_owner_handoff_requested', 'target_task': target,
                                          'old_owner_pid': pid, 'provider_queries': 0}), flush=True)
            else:
                print(json.dumps({'state': 'waiting_for_ordinary_capacity',
                                  'reason': capacity.get('reason'), 'provider_queries': 0}), flush=True)
        time.sleep(interval)
    print(json.dumps({'state': 'handoff_wait_expired', 'graceful_restart_requested': restart_sent,
                      'provider_queries': 0}), flush=True)
    return 75


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--table-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--hours', type=float, default=24)
    args = parser.parse_args()
    raise SystemExit(wait(args.root, args.table_id, args.output, args.hours))


if __name__ == '__main__':
    main()
