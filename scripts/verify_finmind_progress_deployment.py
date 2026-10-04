"""Read-only deployment proof; never query a provider or modify the task queue."""
from __future__ import annotations

import argparse
from contextlib import closing
from collections import Counter
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess

import requests

from downloader.artifact_io import atomic_write_json
from downloader.download_finmind_complement import DISPATCH_CONTRACT_VERSION
from downloader.finmind_eta import SNAPSHOT_CONTRACT_VERSION


TARGETS = ('stockagent-finmind-complement.service', 'stockagent-finmind-sponsor.service',
           'stockagent-public-dashboards.service')
PROTECTED = ('stockagent-finmind-free.service', 'stockagent-shioaji-taifex-dashboard.service',
             'stockagent-tw-day-trade-simulation.service', 'stockagent-tw-overnight-simulation.service')
PROPERTIES = ('Id', 'ActiveState', 'SubState', 'MainPID', 'InvocationID', 'NRestarts', 'ActiveEnterTimestamp',
              'MemoryCurrent', 'CPUUsageNSec')


def service_state(unit: str) -> dict[str, str]:
    result = subprocess.run(['systemctl', 'show', unit, *[f'--property={key}' for key in PROPERTIES]],
                            check=True, capture_output=True, text=True, timeout=10)
    return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)


def fatal_log_counts(units: tuple[str, ...], since: datetime) -> dict[str, int]:
    """Only counts reach the receipt; never expose provider/error log text."""
    markers = ('traceback (most recent call last):', 'watchdog timeout',
               'out of memory', 'segmentation fault')
    result = {}
    for unit in units:
        output = subprocess.run(['journalctl', '--no-pager', '--output=json', '-u', unit,
                                 '--since', since.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')],
                                check=True, capture_output=True, text=True, timeout=10)
        result[unit] = sum(any(marker in str(json.loads(line).get('MESSAGE', '')).lower()
                              for marker in markers) for line in output.stdout.splitlines())
    return result


def process_resources(main_pid: str) -> dict:
    """Metadata-only child accounting; no process args, credentials or data."""
    pids = [int(main_pid)]
    index = 0
    while index < len(pids):
        pid = pids[index]
        index += 1
        try:
            children = Path(f'/proc/{pid}/task/{pid}/children').read_text().split()
            pids.extend(int(value) for value in children if int(value) not in pids)
        except OSError:
            pass
    resources = []
    for pid in pids:
        files = Counter()
        rss = None
        try:
            rss = int(Path(f'/proc/{pid}/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
            for fd in Path(f'/proc/{pid}/fd').iterdir():
                try:
                    name = fd.readlink().name
                except OSError:
                    continue
                if name in {'queue.sqlite3', 'queue.sqlite3-wal', 'queue.sqlite3-shm'}:
                    files[name] += 1
        except OSError:
            pass
        resources.append({'pid': pid, 'rss_bytes': rss, 'queue_descriptors': dict(files)})
    return {'processes': resources}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', choices=('before', 'after'), required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, help='Original pre-deployment receipt for a separate output directory')
    parser.add_argument('--base-url', default='http://127.0.0.1:8770')
    parser.add_argument('--target-unit', action='append', choices=TARGETS,
                        help='Restart subset; all other originally listed units are protected')
    args = parser.parse_args()
    now = datetime.now(UTC)
    targets = tuple(dict.fromkeys(args.target_unit)) if args.target_unit else TARGETS
    protected = (*PROTECTED, *(unit for unit in TARGETS if unit not in targets))
    states = {unit: service_state(unit) for unit in (*targets, *protected)}
    for unit, state in states.items():
        assert state['ActiveState'] == 'active' and state['SubState'] == 'running', unit
    response = requests.get(args.base_url.rstrip('/') + '/finmind/api/status', timeout=25)
    response.raise_for_status()
    public = response.json()
    assert public['read_only'] and not public['production_control_possible']
    acquisition = public['acquisition']
    status = json.loads(Path('data_finmind/complement/status.json').read_bytes())
    eta = json.loads(Path('data_finmind/eta_status.json').read_bytes())
    with closing(sqlite3.connect('file:data_finmind/complement/queue.sqlite3?mode=ro', uri=True, timeout=10)) as conn:
        conn.execute('PRAGMA query_only=ON')
        errors = [dict(dataset=row[0], state=row[1], error_code=row[2], tasks=row[3]) for row in conn.execute(
            "SELECT dataset,state,error_code,count(*) FROM tasks "
            "WHERE state IN ('failed','partial','blocked','retry_exhausted') "
            'GROUP BY dataset,state,error_code ORDER BY dataset,state,error_code')]
    proof = dict(schema_version=1, observed_at_utc=now.isoformat(), phase=args.phase,
                 services=states, target_units=list(targets), protected_units=list(protected),
                 process_resources={unit: process_resources(states[unit]['MainPID']) for unit in targets},
                 dispatch_contract_version=status.get('dispatch_contract_version'),
                 eta_contract_version=eta.get('snapshot_contract_version'),
                 public_contract_version=acquisition['completion_estimate'].get('schema_version'),
                 public_health=public['health'], public_generated_at_utc=public['generated_at_utc'],
                 read_only=public['read_only'], quota=public['quota'],
                 progress={key: acquisition.get(key) for key in (
                     'state', 'checked_tasks', 'total_tasks', 'materialized_tasks', 'unseeded_candidate_tasks',
                     'pending_tasks', 'materialized_pending_tasks', 'retry_exhausted_tasks',
                     'retained_rows', 'rows', 'latest_result')},
                 completion_estimate=acquisition['completion_estimate'], errors=errors,
                 provider_calls=0, queue_writes=0, all_history_complete_claim=False)
    if args.phase == 'after':
        before = json.loads((args.baseline or args.output_dir / 'deployment_before.json').read_bytes())
        since = datetime.fromisoformat(before['observed_at_utc'])
        assert tuple(before['target_units']) == targets
        for unit in protected:
            assert states[unit]['InvocationID'] == before['services'][unit]['InvocationID'], unit
            assert states[unit]['NRestarts'] == before['services'][unit]['NRestarts'], unit
        for unit in targets:
            assert states[unit]['InvocationID'] != before['services'][unit]['InvocationID'], unit
        assert status['dispatch_contract_version'] == DISPATCH_CONTRACT_VERSION
        assert datetime.fromisoformat(status['observed_at_utc']) >= since
        estimate = eta['estimate']
        assert eta['snapshot_contract_version'] == estimate['schema_version'] == SNAPSHOT_CONTRACT_VERSION
        assert datetime.fromisoformat(estimate['observed_at_utc']) >= since
        assert acquisition['completion_estimate']['schema_version'] == SNAPSHOT_CONTRACT_VERSION
        assert acquisition['completion_estimate']['state'] not in ('unavailable', 'stale')
        assert acquisition['checked_tasks'] >= before['progress']['checked_tasks']
        assert acquisition['materialized_tasks'] + acquisition['unseeded_candidate_tasks'] == acquisition['total_tasks']
        logs = fatal_log_counts(targets, since)
        assert not any(logs.values()), 'fatal_runtime_log_since_baseline'
        proof.update(passed=True, protected_invocations_unchanged=True, deployment_since_utc=since.isoformat(),
                     fatal_runtime_log_counts=logs,
                     newly_checked_tasks=acquisition['checked_tasks']-before['progress']['checked_tasks'],
                     browser_asset_sha256=hashlib.sha256(Path('services/finmind_dashboard/app.js').read_bytes()).hexdigest())
    atomic_write_json(args.output_dir / f'deployment_{args.phase}.json', proof)
    print({key: proof.get(key) for key in ('phase', 'passed', 'observed_at_utc', 'dispatch_contract_version',
                                          'eta_contract_version', 'public_contract_version', 'newly_checked_tasks')})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
