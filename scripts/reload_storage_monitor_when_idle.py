#!/usr/bin/env python3
"""Load one fixed observer update after its existing census owner is idle.

This is a one-shot rollout helper. It never cancels an active census or takes
over a migration owner. A WSL restart independently loads the current code.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
CODE = ROOT/'stockagent/data_sync/storage_progress.py'
LOCK = Path('/run/lock/stockagent-wsl-retirement-wave-scheduler.lock')
MONITOR = 'stockagent-storage-monitor.service'
GATEWAY = 'stockagent-public-dashboards.service'
CONTRACT = 'stockagent.storage_observer_idle_reload.v1'


def unit_pid(unit: str) -> int:
    result = subprocess.run(['systemctl', 'show', unit, '--property=ActiveState', '--property=MainPID'],
                            capture_output=True, text=True, timeout=5, check=True)
    fields = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    value = fields.get('MainPID', '')
    return int(value) if fields.get('ActiveState') == 'active' and value.isdigit() else 0


def submit(units: list[str]) -> None:
    subprocess.run(['systemctl', '--no-block', 'try-restart', *units],
                   capture_output=True, text=True, timeout=10, check=True)


def census_running() -> bool:
    """Do not interrupt a remote census or a child not covered by the wave lock."""
    result = subprocess.run(['systemctl', 'show', MONITOR, '--property=ControlGroup', '--value'],
                            capture_output=True, text=True, timeout=5, check=True)
    group = result.stdout.strip()
    if group != '/system.slice/'+MONITOR:
        raise ValueError('observer cgroup identity differs')
    pids = (Path('/sys/fs/cgroup')/group.lstrip('/')/'cgroup.procs').read_text().split()
    if len(pids) > 1024 or any(not pid.isdigit() for pid in pids):
        raise ValueError('observer cgroup process list differs')
    for pid in pids:
        try:
            name = (Path('/proc')/pid/'comm').read_text().strip()
        except FileNotFoundError:
            continue
        if name in {'du', 'ssh', 'sshpass'}:
            return True
    return False


def attempt(expected_sha: str, expected_pids: dict[str, int], *, code: Path = CODE, lock: Path = LOCK,
            get_pid=unit_pid, launch=submit, scan_running=census_running, monitor_only=False) -> dict:
    """Make at most one nonblocking locked submission; never manufacture readiness."""
    if not re.fullmatch('[a-f0-9]{64}', expected_sha) or set(expected_pids) != {MONITOR, GATEWAY}:
        raise ValueError('invalid observer rollout identity')
    if any(type(pid) is not int or pid <= 0 for pid in expected_pids.values()):
        raise ValueError('observer rollout requires both original active PIDs')
    if type(monitor_only) is not bool:
        raise ValueError('observer rollout scope must be explicit')
    descriptor = os.open(lock, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1
                or not os.path.samestat(info, lock.lstat())):
            raise ValueError('census owner lock identity differs')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'contract': CONTRACT, 'state': 'waiting_existing_census', 'submitted_units': []}
        if not os.path.samestat(os.fstat(descriptor), lock.lstat()):
            return {'contract': CONTRACT, 'state': 'code_or_lock_changed', 'submitted_units': []}
        if code.is_symlink() or hashlib.sha256(code.read_bytes()).hexdigest() != expected_sha:
            return {'contract': CONTRACT, 'state': 'code_changed', 'submitted_units': []}
        if scan_running():
            return {'contract': CONTRACT, 'state': 'waiting_existing_census', 'submitted_units': []}
        # A unit that restarted while we waited has already passed the fixed
        # code's installation time. Do not restart it again or start a stopped unit.
        scope = (MONITOR,) if monitor_only else (MONITOR, GATEWAY)
        targets = [unit for unit in scope if get_pid(unit) == expected_pids[unit]]
        if targets:
            if code.is_symlink() or hashlib.sha256(code.read_bytes()).hexdigest() != expected_sha:
                return {'contract': CONTRACT, 'state': 'code_changed', 'submitted_units': []}
            launch(targets)
        return {'contract': CONTRACT, 'state': 'submitted' if targets else 'already_restarted',
                'submitted_units': targets, 'code_sha256': expected_sha,
                'readiness_confirmed': False}
    finally:
        os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--code-sha256', required=True)
    parser.add_argument('--monitor-pid', required=True, type=int)
    parser.add_argument('--gateway-pid', required=True, type=int)
    parser.add_argument('--monitor-only', action='store_true',
                        help='load collection-only changes without restarting the dashboard gateway')
    parser.add_argument('--wait-seconds', type=int, default=8*3600)
    args = parser.parse_args()
    if not 1 <= args.wait_seconds <= 12*3600:
        raise ValueError('invalid rollout wait bound')
    expected = {MONITOR: args.monitor_pid, GATEWAY: args.gateway_pid}
    deadline = time.monotonic()+args.wait_seconds
    while True:
        result = attempt(args.code_sha256, expected, monitor_only=args.monitor_only)
        if result['state'] != 'waiting_existing_census':
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return 0 if result['state'] in {'submitted', 'already_restarted'} else 1
        if time.monotonic() >= deadline:
            print(json.dumps({**result, 'state': 'waiting_timeout'}, ensure_ascii=False), flush=True)
            return 75
        time.sleep(min(5, max(0, deadline-time.monotonic())))


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(json.dumps({'contract': CONTRACT, 'state': 'error', 'error_type': type(error).__name__}),
              file=sys.stderr, flush=True)
        raise SystemExit(1)
