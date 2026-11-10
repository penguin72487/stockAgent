#!/usr/bin/env python3
"""After the E provider handoff, register and drain source-bound minute checks.

The existing FinLab priority registry, capacity gates, quota and account owner
remain authoritative. Waiting and registration make no provider request.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from downloader.artifact_io import atomic_write_json
from downloader.provider_priority import register
from scripts.plan_shioaji_positive_empty_finlab import ROOT, OUTPUT, build
from scripts.run_finlab_finite_priority_handoff import process_start_ticks
from stockagent.data_sync import provider_workspace as placement
from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.completed_forward_capacity import reserve_capacity


TASK = 'futures-full-period-study-20261030'
HANDOFF_RUN = os.environ.get('STOCKAGENT_PROVIDER_E_HANDOFF_RUN',
                             'provider-e-handoff-v5-20261103-20261103T124310-6bb4aaf0')
if not re.fullmatch(r'[A-Za-z0-9_-]+', HANDOFF_RUN):
    raise ValueError('handoff run identity must be a single workflow name')
FINITE_NAME = 'futures-finlab-shioaji-positive-empty-20261103'
RUNS = ROOT / 'artifacts/operations/agent-workflow/runs'
REPORT = OUTPUT.with_name('shioaji-positive-empty-finlab-handoff-20261103-v1.json')
REQUEST = OUTPUT.with_name('shioaji-positive-empty-finlab-priority-request-20261103-v1.json')
JOURNAL = OUTPUT.with_name('shioaji-positive-empty-finlab-handoff-journal-20261103-v1.json')
FINLAB = ROOT / 'data_finlab'


def validate_candidate(current, candidate):
    fields = ('official_daily_path', 'official_daily_sha256', 'manifest_root',
              'manifest_pins', 'source_gap_alias_dates', 'candidates', 'unresolved')
    if (candidate.get('contract') != 'shioaji_positive_empty_finlab_candidates_v1'
            or not candidate.get('candidates')
            or any(current.get(field) != candidate.get(field) for field in fields)):
        raise ValueError('source generations or candidate scope changed; regenerate the plan')


def items_without_invalid_receipts(candidate, root=FINLAB):
    from scripts.download_finlab_intraday import _partition_paths, _stored_receipt
    from downloader.provider_priority import _finlab_futures_partition
    items, invalid = [], []
    for candidate_row in candidate['candidates']:
        item = {'dataset': candidate_row['dataset'], 'partition': candidate_row['partition']}
        dataset, day = _finlab_futures_partition(item)
        receipt, _ = _partition_paths(root, dataset, day)
        if receipt.exists() and _stored_receipt(receipt, root, dataset, day, verify_hash=True) is None:
            invalid.append(item)
        else:
            items.append(item)
    return items, invalid


def make_request(items, now):
    if not items or now.tzinfo is None:
        raise ValueError('a nonempty, dated native request is required')
    return {'contract': 'provider_priority_query_v1', 'provider': 'finlab',
            'reason': 'Official monthly positive activity and Shioaji source-empty contradictions; '
                      'check existing FinLab minute partitions, without assuming executable ticks.',
            'created_at_utc': now.isoformat(), 'expires_at_utc': (now + timedelta(days=7)).isoformat(),
            'items': items}


def main():
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    lock_path = REPORT.with_suffix('.lock')
    with lock_path.open('a') as lock:
        # A successor may wait for the earlier observer; neither holds any
        # provider lock or performs a source query while waiting here.
        fcntl.flock(lock, fcntl.LOCK_EX)
        plan_bytes = OUTPUT.read_bytes()
        plan_sha = hashlib.sha256(plan_bytes).hexdigest()
        candidate = json.loads(plan_bytes)
        deadline = time.monotonic() + 12 * 3600
        while time.monotonic() < deadline:
            state = json.loads((RUNS / HANDOFF_RUN / 'run.json').read_text()).get('state')
            if state == 'succeeded':
                break
            if state in {'failed', 'cancelled', 'terminated'}:
                atomic_write_json(REPORT, {'state': 'provider_handoff_failed', 'handoff_state': state,
                                          'candidate_sha256': plan_sha, 'provider_calls': 0})
                return 75
            atomic_write_json(REPORT, {'state': 'waiting_original_e_handoff', 'handoff_state': state,
                                      'candidate_sha256': plan_sha, 'provider_calls': 0})
            time.sleep(30)
        else:
            atomic_write_json(REPORT, {'state': 'provider_handoff_wait_timeout',
                                      'candidate_sha256': plan_sha, 'provider_calls': 0})
            return 75
        if placement.MIGRATION_FENCE.exists():
            raise RuntimeError('E migration fence is still active')
        registry = placement.load()
        if set(registry['providers']) != set(placement.PROVIDERS):
            raise RuntimeError('all original provider E placements must be present')
        for name, entry in registry['providers'].items():
            placement.check_entry(name, entry)
        if hashlib.sha256(OUTPUT.read_bytes()).hexdigest() != plan_sha:
            raise RuntimeError('candidate plan changed while waiting')
        validate_candidate(build(), candidate)
        items, invalid = items_without_invalid_receipts(candidate)
        if not items:
            atomic_write_json(REPORT, {'state': 'no_admissible_candidates', 'invalid_local_receipts': invalid,
                                      'candidate_sha256': plan_sha, 'provider_calls': 0})
            return 75
        if REQUEST.exists():
            request = json.loads(REQUEST.read_text())
            if request['items'] != items:
                raise RuntimeError('saved native request differs; preserve it for review')
        else:
            request = make_request(items, datetime.now(timezone.utc))
            atomic_write_json(REQUEST, request)
        # Registration writes native E priority metadata. Share the same
        # reservation ledger as the active copies, checking both E physical
        # backing and WSL availability before that write.
        controls = volume.path('migration') / placement.GENERATION / 'capacity-leases'
        reserve = 128 * 1024**2
        with reserve_capacity(controls, key=hashlib.sha256(FINITE_NAME.encode()).hexdigest(),
                              maximum_growth_bytes=reserve, capacity_bytes=2**63-1,
                              minimum_free_bytes=volume.configuration()['minimum_free_bytes']) as lease:
            volume.admit(reserve + lease['other_reserved_growth_bytes'])
            result = register(FINLAB, request)
        identifier = result['request_id']
        states = Counter(row['state'] for row in result['items'])
        if JOURNAL.exists():
            journal = json.loads(JOURNAL.read_text())
            if journal['request_id'] != identifier:
                raise RuntimeError('handoff journal request differs')
        else:
            pid = os.getpid()
            ticks = process_start_ticks(pid)
            if ticks is None:
                raise RuntimeError('handoff predecessor process is unavailable')
            journal = {'request_id': identifier, 'predecessor_pid': pid, 'predecessor_start_ticks': ticks}
            atomic_write_json(JOURNAL, journal)
        argv = ['run_fintech_python', 'scripts/run_finlab_finite_priority_handoff.py',
                '--request-id', identifier, '--predecessor-pid', str(journal['predecessor_pid']),
                '--predecessor-start-ticks', str(journal['predecessor_start_ticks']), '--progress',
                'artifacts/operations/finlab-shioaji-positive-empty-20261103-v1.json',
                '--futures-workspace-only']
        previous = [json.loads(path.read_text()) for path in RUNS.glob(FINITE_NAME + '-*/run.json')]
        if len(previous) > 1 or any(row.get('task_id') != TASK or row.get('argv') != argv for row in previous):
            raise RuntimeError('finite owner continuation identity differs')
        if previous:
            launched = previous[0]
        else:
            command = ['stockagent-agent', 'run', '--backend', 'systemd', '--name', FINITE_NAME,
                       TASK, '--', *argv]
            launched = json.loads(subprocess.run(command, cwd=ROOT, check=True, capture_output=True,
                                                 text=True, timeout=60).stdout)
        atomic_write_json(REPORT, {'state': 'registered_to_canonical_finlab_owner',
            'candidate_sha256': plan_sha, 'request_id': identifier, 'request_plan': str(REQUEST),
            'registered_items': len(items), 'invalid_local_receipts': invalid,
            'initial_states': dict(states), 'finite_run_id': launched['run_id'],
            'provider_calls_by_handoff': 0, 'data_or_training_admission': False})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
