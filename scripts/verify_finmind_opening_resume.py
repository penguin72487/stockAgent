"""Read back native post-protection acquisitions without requesting provider data.

A running heartbeat proves wake-up, not successful acquisition. Require the
same observed owner, completed queue rows and independently verified receipts,
Parquet footers and full SHA-256 before accepting this bounded resume window.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json
from downloader.finmind_integrity import inspect_completed
from scripts.reload_finmind_at_idle import ROOT, UNIT, identity


def stamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('resume clocks require an explicit timezone')
    return result.astimezone(UTC)


def resume_window(observation):
    samples = observation.get('samples', [])
    if (observation.get('state') != 'observed'
            or observation.get('native_owner_unchanged') is not True
            or observation.get('sampled_prior_source_files_unchanged') is not True
            or observation.get('observer_provider_calls') != 0
            or observation.get('observer_queue_writes') != 0
            or observation.get('service_controls') != 0
            or not samples):
        raise ValueError('native observation boundaries not established')
    initial = samples[0]
    if initial.get('phase') != 'waiting' or initial.get('reason') != 'protected_opening':
        raise ValueError('window did not start in canonical opening protection')
    deadline = stamp(initial['next_check_at_utc'])
    if not stamp(initial['observed_at_utc']) < deadline < stamp(samples[-1]['observed_at_utc']):
        raise ValueError('window must straddle the protection deadline')
    running = next((row for row in samples if row.get('phase') == 'running'), None)
    if running is None:
        raise ValueError('native worker did not resume during this window')
    wake = stamp(running['heartbeat_at_utc'])
    if not 0 <= (wake - deadline).total_seconds() <= 5:
        raise ValueError('native wake-up missed the bounded deadline tolerance')
    completed = {}
    for sample in samples:
        task = sample.get('last_task')
        if (isinstance(task, dict) and task.get('status') == 'complete'
                and type(task.get('rows')) is int and task['rows'] > 0
                and stamp(task['observed_at_utc']) >= deadline):
            key = tuple(task[field] for field in ('dataset', 'data_id', 'partition'))
            completed.setdefault(key, task)
    if not completed:
        raise ValueError('heartbeat alone is not a completed acquisition')
    return deadline, wake, list(completed.values())


def completed_readback(connection, root, observed, deadline):
    names = ('dataset', 'data_id', 'partition')
    cursor = connection.execute('SELECT * FROM tasks WHERE dataset=? AND data_id=? AND partition=?',
                                tuple(observed[key] for key in names))
    row = cursor.fetchone()
    task = dict(zip([field[0] for field in cursor.description], row)) if row else {}
    if task.get('state') != 'complete' or task.get('rows') != observed['rows']:
        raise ValueError('native completed queue row differs from observed acquisition')
    error, raw = inspect_completed(root, task)
    if error is not None:
        raise ValueError('completed source readback rejected: ' + error)
    receipt = json.loads(raw)
    if stamp(receipt['fetched_at_utc']) < deadline:
        raise ValueError('old receipt is not post-protection acquisition evidence')
    return {**{key: task[key] for key in names}, 'rows': task['rows'],
            'fetched_at_utc': receipt['fetched_at_utc'], 'receipt_path': task['receipt_path'],
            'receipt_sha256': hashlib.sha256(raw).hexdigest(),
            **{key: receipt[key] for key in ('parquet_path', 'sha256', 'parquet_size_bytes',
                                           'source_first_date', 'source_last_date', 'coverage_claim')},
            'full_parquet_hash_and_footer_verified': True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observation', type=Path, required=True)
    parser.add_argument('--idle-acceptance', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error('a new output is required')
    raw_observation, raw_acceptance = args.observation.read_bytes(), args.idle_acceptance.read_bytes()
    observation, accepted = json.loads(raw_observation), json.loads(raw_acceptance)
    if (accepted.get('state') != 'accepted_bounded_protected_idle'
            or accepted.get('native_loaded_code_and_deadline_verified') is not True):
        raise ValueError('accepted native protected-idle generation is required')
    deadline, wake, completed = resume_window(observation)
    owner = identity(UNIT)
    if owner != observation['native_owner']:
        raise ValueError('native owner changed after observation')
    for item in observation['preserved_source_files']:
        path = Path(item['path']).resolve()
        if (not path.is_relative_to(ROOT.resolve()) or path.stat().st_size != item['bytes']
                or hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']):
            raise ValueError('previous source evidence was not preserved')
    uri = (ROOT / 'queue.sqlite3').resolve().as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True, timeout=.2)) as connection:
        proofs = [completed_readback(connection, ROOT, task, deadline) for task in completed[:3]]
    if (identity(UNIT) != owner or args.observation.read_bytes() != raw_observation
            or args.idle_acceptance.read_bytes() != raw_acceptance):
        raise ValueError('owner or input evidence changed during readback')
    report = {'schema_version': 1, 'state': 'accepted_bounded_native_opening_resume',
              'observed_at_utc': datetime.now(UTC).isoformat(), 'native_owner': owner,
              'protection_deadline_utc': deadline.isoformat(), 'native_running_heartbeat_utc': wake.isoformat(),
              'wake_delay_seconds': (wake - deadline).total_seconds(),
              'first_observed_complete_utc': completed[0]['observed_at_utc'],
              'first_complete_delay_seconds': (stamp(completed[0]['observed_at_utc']) - deadline).total_seconds(),
              'sampled_completed_acquisitions': proofs, 'previous_source_preserved': True,
              'evidence': [{'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}
                           for path, raw in ((args.observation, raw_observation), (args.idle_acceptance, raw_acceptance))],
              'observer_provider_calls': 0, 'observer_queue_writes': 0, 'observer_service_controls': 0,
              'caveats': ['Complement only, not all-history completeness or all FinMind owners.',
                          'Observed last-task samples are bounded, not every acquisition or an API throughput benchmark.',
                          'Bootstrap and source latency remain part of the observed first-completion delay.']}
    atomic_write_json(args.output, report, durable=True)
    print(json.dumps(report))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
