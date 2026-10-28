#!/usr/bin/env python3
"""Wait for registered TEJ discovery using only its canonical desktop owner.

This supervisor makes no source request. A zero-timeout collision with the
existing dataset writer is retried outside the lock, without starting or
stopping the desktop, changing queue priorities, or bypassing capacity.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from downloader.artifact_io import sha256_file
from downloader.provider_priority import inspect


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_tej'
OWNERS = tuple(REPO / path for path in (
    'scripts/runtime_env.sh', 'stockagent/data_sync/cache_capacity.py', 'configs/tej_history.json',
    'downloader/download_tej_history.py', 'downloader/tej_history.py', 'downloader/tej_incremental.py',
    'downloader/tej_scheduler.py', 'downloader/tej_planning.py', 'downloader/tej_metadata_recovery.py',
    'downloader/tej_desktop_attempts.py', 'downloader/provider_priority.py',
    'downloader/dataset_lock.py', 'scripts/tej_smart_wizard_bridge.ps1',
    'scripts/run_tej_finite_discovery_wait.py',
))


def classify_owner_result(code: int, stdout: str, stderr: str) -> str:
    """Retry only an evidenced pre-query dataset-lock collision."""
    if code in {0, 75}:
        return 'retry'
    events = []
    # The canonical dataset-lock logger writes its structured events to
    # stderr; inspect both streams without relying on the traceback alone.
    for line in (stdout + '\n' + stderr).splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and event.get('event') == 'dataset_lock':
            events.append(event)
    if (code == 1 and events and events[-1].get('provider') == 'tej_smart_wizard'
            and events[-1].get('reason') == 'dataset_writer_lock_timeout'
            and events[-1].get('state') == 'timeout'
            and 'downloader.dataset_lock.DatasetLockTimeout:' in stderr):
        return 'lock_busy'
    return 'error'


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request-id', required=True)
    parser.add_argument('--plan', type=Path, required=True)
    args = parser.parse_args()
    receipt = ROOT / 'priority_queries' / f'{args.request_id}.json'
    pins = {path: sha256_file(path) for path in (*OWNERS, args.plan.resolve(), receipt)}
    relay = Path('/run/WSL/1_interop')
    if not os.environ.get('WSL_INTEROP') and relay.is_socket():
        os.environ['WSL_INTEROP'] = str(relay)
    while True:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            print(json.dumps({'state': 'owner_or_scope_changed', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        status = inspect(ROOT, args.request_id)
        if status.get('provider') != 'tej' or not status.get('priority_active'):
            print(json.dumps({'state': 'priority_expired_or_invalid', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        requested = [item for item in status['items'] if item.get('derived_from_requested_scope') is not True]
        if requested and all(item['state'] == 'complete' for item in requested):
            print(json.dumps({'state': 'discovery_complete_source_contents_unverified',
                              'items': len(requested), 'provider_calls_by_waiter': 0}), flush=True)
            return 0
        active = subprocess.run(['systemctl', 'is-active', '--quiet', 'stockagent-tej-history.service'],
                                cwd=REPO, timeout=15, check=False)
        if active.returncode == 0:
            print(json.dumps({'state': 'existing_tej_owner_active', 'provider_calls_by_waiter': 0}), flush=True)
            time.sleep(60)
            continue
        if active.returncode not in {3, 4}:
            print(json.dumps({'state': 'service_state_unknown', 'exit_code': active.returncode,
                              'provider_calls_by_waiter': 0}), flush=True)
            return 75
        child = subprocess.run([sys.executable, '-m', 'downloader.download_tej_history', 'run',
                                '--finite-discovery-request-id', args.request_id],
                               cwd=REPO, text=True, capture_output=True, timeout=1800)
        state = classify_owner_result(child.returncode, child.stdout, child.stderr)
        print(json.dumps({'state': state, 'exit_code': child.returncode,
                          'stdout_tail': child.stdout[-1200:], 'stderr_tail': child.stderr[-600:],
                          'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
        if state == 'error':
            return child.returncode or 75
        time.sleep(60)


if __name__ == '__main__':
    raise SystemExit(main())
