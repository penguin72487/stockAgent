#!/usr/bin/env python3
"""Build a resumable source-only lookup index from verified option events."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import sqlite3
import time

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_taifex_option_finmind_reference_window import candidate_policy
from scripts.run_finmind_option_reference_event_history import verify
from scripts.run_finmind_option_reference_history import window_dates
from stockagent.data.tw_option_full_carry_index import CONTRACT, add_events, canonical_rows, create_schema, rows_digest
from stockagent.data_sync.acquisition_capacity_sampler import AcquisitionCapacitySampler


def identity(plan_path: Path, plan: dict) -> dict:
    if (plan.get('contract') != 'finmind_option_reference_history_plan_v1'
            or plan.get('training_ready') is not False):
        raise ValueError('a pinned research-only history plan is required')
    policy = Path(plan['policy_path']).resolve()
    if sha256_file(policy) != plan['policy_sha256']:
        raise ValueError('candidate policy changed')
    _, linked = candidate_policy(policy)
    implementations = [Path(__file__).resolve(),
        Path('stockagent/data/tw_option_full_carry_index.py').resolve(),
        Path('stockagent/data/tw_option_reference_candidates.py').resolve(),
        Path('scripts/run_finmind_option_reference_event_history.py').resolve()]
    return {'contract': CONTRACT, 'plan_path': str(plan_path), 'plan_sha256': sha256_file(plan_path),
            'policy_path': str(policy), 'policy_sha256': plan['policy_sha256'],
            'policy_linked_pins': linked,
            'implementation_sha256': {str(path): sha256_file(path) for path in implementations}}


def validate_windows(plan: dict) -> None:
    previous = None
    if not plan.get('windows'):
        raise ValueError('at least one planned window is required')
    for window in plan['windows']:
        days = window_dates(window)
        if previous is not None and days[0] <= previous:
            raise ValueError('source windows are not strictly ordered')
        previous = days[-1]


def db_digest(conn: sqlite3.Connection, index: int) -> tuple[int, str]:
    cursor = conn.execute('SELECT product,series,strike,right,event_us,price,source_sha256 '
                          'FROM events WHERE source_window=? '
                          'ORDER BY product,series,strike,right,event_us,price,source_sha256', (index,))
    rows = cursor.fetchall()
    return len(rows), rows_digest(rows)


def run(root: Path, plan_path: Path, index_root: Path, output: Path, status: Path,
        *, max_wait_seconds: int, poll_seconds: int = 30) -> dict:
    root, plan_path, index_root, output, status = (p.resolve() for p in
                                                   (root, plan_path, index_root, output, status))
    workspace = root.parents[1]
    if (not output.is_relative_to(workspace / 'data_finmind' / 'research')
            or not status.is_relative_to(workspace / 'artifacts' / 'markets')
            or not 1 <= max_wait_seconds <= 7 * 86400 or not 1 <= poll_seconds <= 300):
        raise ValueError('research index, market status and bounded wait are required')
    plan = json.loads(plan_path.read_bytes())
    validate_windows(plan)
    expected_identity = identity(plan_path, plan)
    sampler = AcquisitionCapacitySampler(output.parent)
    admission = sampler.initial_sample(timeout_seconds=29)
    if admission.get('state') not in {'admitted', 'not_enrolled'}:
        raise RuntimeError('index capacity not admitted: ' + str(admission.get('reason')))
    if output.is_symlink() or status.is_symlink():
        raise ValueError('index and status must not be symlinks')
    output.mkdir(parents=True, exist_ok=True)
    database = output / 'events.sqlite3'
    if database.is_symlink():
        raise ValueError('index database must not be a symlink')
    deadline = time.monotonic() + max_wait_seconds
    expected_text = json.dumps(expected_identity, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    with (output / '.writer.lock').open('a+') as lock, closing(sqlite3.connect(database, timeout=30)) as conn:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=FULL')
        create_schema(conn)
        conn.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        conn.execute('CREATE TABLE IF NOT EXISTS windows (idx INTEGER PRIMARY KEY,start TEXT NOT NULL,end TEXT NOT NULL,'
                     'manifest_sha256 TEXT NOT NULL,payload_sha256 TEXT NOT NULL,rows INTEGER NOT NULL,'
                     'events_sha256 TEXT NOT NULL)')
        stored = conn.execute("SELECT value FROM metadata WHERE key='identity'").fetchone()
        if stored is None:
            if conn.execute('SELECT count(*) FROM events').fetchone()[0] or conn.execute('SELECT count(*) FROM windows').fetchone()[0]:
                raise ValueError('unidentified nonempty event index')
            conn.execute("INSERT INTO metadata VALUES ('identity',?)", (expected_text,))
        elif stored[0] != expected_text:
            raise ValueError('index plan or implementation identity changed')
        conn.commit()
        if conn.execute('PRAGMA quick_check').fetchone() != ('ok',):
            raise ValueError('event index integrity check failed')
        completed = conn.execute('SELECT idx,start,end,manifest_sha256,payload_sha256,rows,events_sha256 '
                                 'FROM windows ORDER BY idx').fetchall()
        if [item[0] for item in completed] != list(range(len(completed))):
            raise ValueError('event index checkpoint is not a contiguous prefix')
        for number, start, end, manifest_sha, payload_sha, count, digest in completed:
            group = plan['windows'][number]
            parent = index_root / f"{group['start']}_{group['end']}"
            manifest = parent / 'manifest.json'
            if (start != group['start'] or end != group['end'] or not manifest.is_file()
                    or sha256_file(manifest) != manifest_sha):
                raise ValueError('event index checkpoint parent changed')
            doc = json.loads(manifest.read_bytes())
            if sha256_file(Path(doc['data_path'])) != payload_sha or db_digest(conn, number) != (count, digest):
                raise ValueError('event index checkpoint payload or readback changed')
        if conn.execute('SELECT count(*) FROM events').fetchone()[0] != sum(row[5] for row in completed):
            raise ValueError('event index has events outside its checkpoint')

        def publish(state: str, *, reason=None) -> dict:
            info = {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                    'state': state, 'reason': reason, 'database': str(database), 'identity': expected_identity,
                    'verified_prefix_windows': len(completed), 'planned_windows': len(plan['windows']),
                    'last_indexed_date': completed[-1][2] if completed else None,
                    'events': sum(row[5] for row in completed), 'training_ready': False,
                    'known_missing_source_days': plan.get('not_planned_days', []),
                    'limitations': ['Indexed retained observations only; earlier or missing sources remain unknown.',
                        'Caller must separately prove dated contract eligibility, source completeness and execution.',
                        'No option margin, settlement, account or training admission is implied.']}
            atomic_write_json(status, info)
            return info

        publish('running')
        while len(completed) < len(plan['windows']) and time.monotonic() < deadline:
            number = len(completed)
            group = plan['windows'][number]
            parent = index_root / f"{group['start']}_{group['end']}"
            if not (parent / 'manifest.json').is_file():
                publish('waiting_for_verified_parent', reason=group['start'])
                time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
                continue
            admission = sampler.initial_sample(timeout_seconds=29)
            if admission.get('state') not in {'admitted', 'not_enrolled'}:
                publish('capacity_deferred', reason=admission.get('reason'))
                time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
                continue
            if identity(plan_path, plan) != expected_identity:
                raise ValueError('index plan, policy or implementation changed during build')
            checked = verify(parent, plan_path, plan, group, root)
            doc = json.loads((parent / 'manifest.json').read_bytes())
            frame = pl.read_parquet(doc['data_path'])
            planned_days = set(window_dates(group))
            if (frame.is_empty() or set(frame['source_date'].unique().to_list()) != planned_days
                    or frame.filter(pl.col('event_ts').dt.date() != pl.col('source_date')).height):
                raise ValueError('event dates differ from the verified parent')
            expected_count = frame.height
            expected_digest = rows_digest(sorted(canonical_rows(frame)))
            try:
                conn.execute('BEGIN IMMEDIATE')
                if add_events(conn, frame, source_window=number) != expected_count:
                    raise ValueError('event insertion lost source rows')
                if db_digest(conn, number) != (expected_count, expected_digest):
                    raise ValueError('independent index readback differs from parent events')
                if (sha256_file(parent / 'manifest.json') != checked['manifest_sha256']
                        or sha256_file(Path(doc['data_path'])) != checked['data_sha256']):
                    raise ValueError('event parent changed during index publication')
                record = (number, group['start'], group['end'], checked['manifest_sha256'],
                          checked['data_sha256'], expected_count, expected_digest)
                conn.execute('INSERT INTO windows VALUES (?,?,?,?,?,?,?)', record)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            completed.append(record)
            publish('running')
            print(json.dumps({'state': 'indexed_verified', 'index': number, 'start': group['start'],
                              'end': group['end'], 'events': expected_count}, ensure_ascii=False), flush=True)
        state = 'source_index_prefix_complete' if len(completed) == len(plan['windows']) else 'finite_wait_elapsed'
        result = publish(state)
        conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--index-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--status', type=Path, required=True)
    parser.add_argument('--max-wait-seconds', type=int, default=86400)
    args = parser.parse_args()
    result = run(args.root, args.plan, args.index_root, args.output, args.status,
                 max_wait_seconds=args.max_wait_seconds)
    print(json.dumps({'state': result['state'], 'verified_prefix_windows': result['verified_prefix_windows'],
                      'planned_windows': result['planned_windows'], 'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
