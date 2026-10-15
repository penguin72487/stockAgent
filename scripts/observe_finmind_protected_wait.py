"""Bounded read-only native FinMind idle-window evidence; never controls owners.

Keep only allowlisted status clocks/counters, not source bodies or credentials.
Compare like phases: an idle sample is not acquisition throughput evidence.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time

from downloader.artifact_io import atomic_write_json
from scripts.reload_finmind_at_idle import ROOT, UNIT, identity, prior_files


def status_clock(root):
    status = json.loads((root / 'status.json').read_bytes())
    heartbeat = json.loads((root / 'worker_status.json').read_bytes())
    return {"acquisition_state": status.get('state'), "cycle_observed_at_utc": status.get('observed_at_utc'),
            "last_task": status.get('last_task'), "active_task": status.get('active_task'),
            "phase": heartbeat.get('phase'), "reason": heartbeat.get('reason'),
            "heartbeat_at_utc": heartbeat.get('observed_at_utc'),
            "next_check_at_utc": heartbeat.get('next_check_at_utc')}


def counters(cgroup):
    usage = dict(line.split() for line in (cgroup / 'cpu.stat').read_text().splitlines())
    return {'cpu_usage_usec': int(usage['usage_usec']),
            'memory_current_bytes': int((cgroup / 'memory.current').read_text()),
            'swap_current_bytes': int((cgroup / 'memory.swap.current').read_text())}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=float, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if not 10 <= args.seconds <= 900 or args.output.exists():
        parser.error('duration 10..900s and a new output are required')
    generation = identity(UNIT)
    if generation['ActiveState'] != 'active':
        raise ValueError('native_owner_not_active')
    group = subprocess.run(['systemctl', 'show', UNIT, '-p', 'ControlGroup', '--value'],
                           capture_output=True, text=True, check=True, timeout=5).stdout.strip()
    cgroup = Path('/sys/fs/cgroup') / group.lstrip('/')
    initial_status = status_clock(ROOT)
    preserved = prior_files(ROOT, initial_status['last_task'])
    before = counters(cgroup)
    start_wall, start = datetime.now(UTC), time.monotonic()
    samples = []
    while True:
        samples.append({'elapsed_seconds': time.monotonic() - start, 'observed_at_utc': datetime.now(UTC).isoformat(),
                        **counters(cgroup), **status_clock(ROOT)})
        left = args.seconds - (time.monotonic() - start)
        if left <= 0:
            break
        time.sleep(min(2.0, left))
    after, elapsed = counters(cgroup), time.monotonic() - start
    same_owner = identity(UNIT) == generation
    files_unchanged = bool(preserved) and all(
        hashlib.sha256(Path(item['path']).read_bytes()).hexdigest() == item['sha256'] for item in preserved)
    last_unchanged = all(row['last_task'] == initial_status['last_task'] for row in samples)
    cycles = list(dict.fromkeys(row['cycle_observed_at_utc'] for row in samples))
    report = {'schema_version': 1, 'state': 'observed' if same_owner else 'native_owner_changed',
              'started_at_utc': start_wall.isoformat(), 'observed_at_utc': datetime.now(UTC).isoformat(),
              'native_owner': generation, 'native_owner_unchanged': same_owner, 'sample_seconds': elapsed,
              'cpu_seconds': (after['cpu_usage_usec'] - before['cpu_usage_usec']) / 1_000_000,
              'observed_cycle_clocks': cycles, 'last_actual_acquisition_unchanged': last_unchanged,
              'sampled_prior_source_files_unchanged': files_unchanged, 'preserved_source_files': preserved,
              'samples': samples, 'observer_provider_calls': 0, 'observer_queue_writes': 0, 'service_controls': 0,
              'caveats': ['Cgroup includes this owner and its children; a shared-host bounded window, not global optimum.',
                          'Two-second samples may miss shorter phases; unchanged last fetch is not all-history completion.',
                          'Read-only observer does not establish all Windows or Linux GPU context absence.']}
    atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ('state', 'sample_seconds', 'cpu_seconds',
                     'observed_cycle_clocks', 'last_actual_acquisition_unchanged', 'sampled_prior_source_files_unchanged')}))
    if not same_owner:
        raise ValueError('native_generation_changed_during_observation')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
