"""Seal bounded native opening-idle evidence after its original 600s boundary.

No scheduler, provider, service-control or source writes. A longer native sample
must actually span the old reinitialization deadline and preserve source clocks.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import subprocess

from downloader.artifact_io import atomic_write_json
from scripts.reload_finmind_at_idle import UNIT, identity, metadata


def window_proof(baseline, reuse):
    samples = reuse['samples']
    cycle = datetime.fromisoformat(reuse['observed_cycle_clocks'][0])
    old_deadline = cycle + timedelta(seconds=600)
    start = datetime.fromisoformat(reuse['started_at_utc'])
    end = datetime.fromisoformat(reuse['observed_at_utc'])
    valid = (baseline['state'] == reuse['state'] == 'observed'
             and baseline['native_owner_unchanged'] and reuse['native_owner_unchanged']
             and baseline['sample_seconds'] >= 120 and reuse['sample_seconds'] >= 600
             and len(baseline['observed_cycle_clocks']) >= 2
             and len(reuse['observed_cycle_clocks']) == 1 and start < old_deadline < end
             and baseline['last_actual_acquisition_unchanged'] and reuse['last_actual_acquisition_unchanged']
             and baseline['sampled_prior_source_files_unchanged'] and reuse['sampled_prior_source_files_unchanged']
             and baseline['preserved_source_files'] == reuse['preserved_source_files']
             and bool(samples) and all(row['phase'] == 'waiting' and row['reason'] == 'protected_opening'
                 and row['active_task'] is None and row['acquisition_state'] == 'protected_opening'
                 and row['cycle_observed_at_utc'] == cycle.isoformat() for row in samples)
             and 0 <= reuse['cpu_seconds'] / reuse['sample_seconds']
                 < baseline['cpu_seconds'] / baseline['sample_seconds'])
    return bool(valid), old_deadline.isoformat()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('baseline', 'reuse', 'reload', 'sql-readback', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--regression-run-id', required=True)
    args = parser.parse_args(argv)
    if args.output.exists() or not re.fullmatch(r'[a-zA-Z0-9_-]+', args.regression_run_id):
        parser.error('a new output and a valid regression run ID are required')
    baseline, reuse, control, sql = map(metadata, (args.baseline, args.reuse, args.reload, args.sql_readback))
    bounded, old_deadline = window_proof(baseline, reuse)
    current = identity(UNIT)
    runtime_path = Path('downloader/finmind_runtime.py').resolve()
    runtime_sha = hashlib.sha256(runtime_path.read_bytes()).hexdigest()
    native_log = subprocess.run(['journalctl', '-u', UNIT, '--since', control['restart_at_utc'],
        '--no-pager', '-n', '500', '-o', 'json'], capture_output=True, text=True, check=True, timeout=10)
    headers = []
    for line in native_log.stdout.splitlines():
        event = json.loads(line)
        if event.get('_SYSTEMD_INVOCATION_ID') != current['InvocationID']:
            continue
        try:
            body = json.loads(event.get('MESSAGE', ''))
        except (TypeError, ValueError):
            continue
        if isinstance(body, dict) and isinstance(body.get('finmind_protected_idle'), dict):
            headers.append(body['finmind_protected_idle'])
    native_code_and_deadline = (len(headers) == 1 and headers[0]['contract_version'] == 1
        and headers[0]['loaded_runtime_sha256'] == runtime_sha and headers[0]['calendar_recheck_seconds'] == 30
        and all(row['next_check_at_utc'] == headers[0]['until_utc'] for row in reuse['samples']))
    run_path = Path('artifacts/operations/agent-workflow/runs') / args.regression_run_id / 'run.json'
    run = metadata(run_path)
    log_path = Path(run['log'])
    passed_match = re.search(r'(\d+) passed(?: in|,)', log_path.read_text())
    regression_argv = ' '.join(run['argv'])
    tested = (run['task_id'] == 'running-services-optimization-20261013'
              and run['state'] == 'succeeded' and run['exit_code'] == 0
              and passed_match is not None and int(passed_match[1]) > 0
              and all(scope in regression_argv for scope in (
                  'test_(finmind|reload_finmind|summarize_finmind)',
                  'test/test_runtime_env.py', 'test/test_agent_workflow.py')))
    independent_sql = (sql['state'] == 'accepted_bounded_finmind_status_cache'
                       and sql['native_complete_status_equals_uncached_canonical_sql']
                       and sql['native_generation_and_status_stable'] and sql['protected_services_unchanged']
                       and not sql['gpu_device_handles'] and not sql['unreadable_sampled_pids'])
    owner = (control['state'] == 'canonical_owner_schema_and_status_verified'
             and baseline['native_owner'] == control['service_before']
             and reuse['native_owner'] == control['service_after'] == sql['native_owner'] == current)
    files = [args.baseline, args.reuse, args.reload, args.sql_readback, run_path, log_path, runtime_path,
             Path('downloader/finmind_scheduling.py'), Path(__file__)]
    passed = bounded and native_code_and_deadline and tested and independent_sql and owner
    report = {'schema_version': 1, 'observed_at_utc': datetime.now(UTC).isoformat(),
        'state': 'accepted_bounded_protected_idle' if passed else 'protected_idle_not_accepted',
        'window_proof_passed': bounded, 'old_reinitialization_deadline_utc': old_deadline,
        'native_loaded_code_and_deadline_verified': native_code_and_deadline,
        'native_owner_and_independent_sql_verified': bool(owner and independent_sql),
        'regression_passed': bool(tested), 'regression_passed_count': int(passed_match[1]) if passed_match else None,
        'native_wait_headers': headers,
        'baseline': {key: baseline[key] for key in ('sample_seconds', 'cpu_seconds', 'observed_cycle_clocks')},
        'reuse': {key: reuse[key] for key in ('sample_seconds', 'cpu_seconds', 'observed_cycle_clocks')},
        'evidence': [{'path': str(path.resolve()), 'bytes': path.stat().st_size,
                      'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for path in files],
        'observer_provider_calls': 0, 'observer_queue_writes': 0, 'observer_service_controls': 0,
        'caveats': ['Native Complement only; other owners need separate loaded-code acceptance.',
                    'Different-length idle windows with/without one avoided reinitialization; not API throughput gain.',
                    'Does not prove the future 09:10 acquisition restart, all-history completeness or all services optimized.']}
    atomic_write_json(args.output, report)
    print(json.dumps({key: value for key, value in report.items() if key != 'evidence'}))
    if not passed:
        raise ValueError('bounded_native_protected_idle_not_proven')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
