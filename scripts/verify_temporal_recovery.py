#!/usr/bin/env python3
"""Actual supervised control-process crash and persisted-history recovery."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.control.lakehouse import configuration
from temporalio.client import Client


async def observe(c):
    client = await Client.connect(c['temporal_endpoint'], namespace=c['temporal_namespace'])
    if not await client.service_client.check_health():
        raise RuntimeError('frontend not ready')
    result = {}
    for name in ('stockagent-storage-lifecycle-v1', 'stockagent-source-replication-v1'):
        handle = client.get_workflow_handle(name)
        description = await handle.describe()
        history = await handle.fetch_history()
        result[name] = {'run_id': description.run_id, 'status': description.status.name,
                        'event_ids': [e.event_id for e in history.events],
                        'pending_activities': [
                            {'activity_id': p.activity_id, 'state': p.state, 'attempt': p.attempt,
                             'last_worker_identity': p.last_worker_identity}
                            for p in description.raw_description.pending_activities]}
    result['catalog_phase'] = await client.get_workflow_handle('stockagent-storage-lifecycle-v1').query('status')
    return result


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', choices=('server', 'worker'), required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--require-activity-retry', action='store_true',
                        help='also wait for interrupted activities to advance or retry on a new worker')
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('preserve prior fault evidence')
    c = configuration()
    unit = 'stockagent-temporal.service' if args.target == 'server' else 'stockagent-storage-lifecycle.service'
    show = lambda name, prop: subprocess.check_output(['systemctl', 'show', name, '--property=' + prop, '--value'], text=True).strip()
    pid, pg = show(unit, 'MainPID'), show('postgresql@18-main.service', 'MainPID')
    before = await observe(c)
    interrupted = {name: [p for p in row.get('pending_activities', []) if p['state'] == 2]
                   for name, row in before.items() if isinstance(row, dict)}
    if args.require_activity_retry and (args.target != 'worker' or not any(interrupted.values())):
        raise ValueError('retry acceptance needs a worker with an actually running activity')
    atomic_write_json(args.output.with_name(args.output.stem + '-before.json'), {'target': unit, 'main_pid': pid, 'workflows': before})
    started = time.perf_counter()
    subprocess.run(['systemctl', 'kill', '--kill-who=main', '--signal=SIGKILL', unit], check=True)
    deadline = time.monotonic() + 180
    last_error = None
    while time.monotonic() < deadline:
        await asyncio.sleep(2)
        try:
            if show(unit, 'ActiveState') != 'active' or show(unit, 'MainPID') == pid:
                continue
            after = await asyncio.wait_for(observe(c), timeout=15)
            for name in ('stockagent-storage-lifecycle-v1', 'stockagent-source-replication-v1'):
                if after[name]['run_id'] != before[name]['run_id'] or after[name]['status'] != 'RUNNING':
                    raise ValueError('workflow identity/status changed unexpectedly')
                if after[name]['event_ids'][:len(before[name]['event_ids'])] != before[name]['event_ids']:
                    raise ValueError('persisted history prefix differs')
                if args.require_activity_retry:
                    pending = {p['activity_id']: p for p in after[name]['pending_activities']}
                    for previous in interrupted.get(name, []):
                        current = pending.get(previous['activity_id'])
                        if current and (current['state'] != 2 or current['attempt'] <= previous['attempt']
                                or current['last_worker_identity'] == previous['last_worker_identity']):
                            raise RuntimeError('waiting for the interrupted activity heartbeat lease to recover')
                        if not current and len(after[name]['event_ids']) <= len(before[name]['event_ids']):
                            raise RuntimeError('interrupted activity has not advanced its persisted history')
            if show('postgresql@18-main.service', 'MainPID') != pg:
                raise ValueError('authority PostgreSQL unexpectedly restarted')
            result = {'state': 'accepted', 'target': unit, 'previous_pid': pid, 'current_pid': show(unit, 'MainPID'),
                      'systemd_restart_verified': True, 'same_workflow_runs_and_history_prefix_verified': True,
                      'interrupted_activity_retry_verified': bool(args.require_activity_retry),
                      'workflows': after, 'production_postgres_main_pid_unchanged': True,
                      'crash_to_successful_live_queries_seconds': time.perf_counter() - started,
                      'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'physical_host_reboot_tested': False}
            atomic_write_json(args.output, result)
            print(json.dumps({k: v for k, v in result.items() if k != 'workflows'}))
            return
        except Exception as error:
            last_error = type(error).__name__
    atomic_write_json(args.output, {'state': 'failed', 'target': unit, 'last_error_type': last_error})
    raise RuntimeError('control crash recovery not accepted within its bounded window')


if __name__ == '__main__':
    asyncio.run(main())
