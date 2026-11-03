#!/usr/bin/env python3
"""Publish a pinned, read-only option-source union after its parent completes.

The wait is bounded and sends no provider requests.  Missing source dates and
retrospective fallback limitations remain explicit; this is not training data.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import time

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_finmind_option_full_carry_index import identity
from scripts.build_option_official_gap_overlay import check_strike_identity
from scripts.build_taifex_option_finmind_reference_window import candidate_policy
from stockagent.data.tw_option_full_carry_index import rows_digest
from stockagent.data.tw_option_source_union import connect_union, verified_overlay


CONTRACT = 'tw_option_source_union_manifest_v1'
IMPLEMENTATIONS = (Path(__file__), Path('stockagent/data/tw_option_source_union.py'),
                   Path('stockagent/data/tw_option_full_carry_index.py'),
                   Path('stockagent/data/tw_option_reference_candidates.py'),
                   Path('stockagent/data/tw_option_reference_gap_guard.py'),
                   Path('scripts/build_finmind_option_full_carry_index.py'),
                   Path('scripts/build_option_official_gap_overlay.py'),
                   Path('scripts/build_taifex_option_finmind_reference_window.py'),
                   Path('downloader/artifact_io.py'))


def validate_parent_checkpoints(conn: sqlite3.Connection, plan: dict, index_root: Path) -> dict:
    windows = plan.get('windows', [])
    records = conn.execute('SELECT idx,start,end,manifest_sha256,payload_sha256,rows,events_sha256 '
                           'FROM main.windows ORDER BY idx').fetchall()
    if not windows or len(records) != len(windows) or [r[0] for r in records] != list(range(len(windows))):
        raise ValueError('parent index is not the entire contiguous source plan')
    if conn.execute('PRAGMA quick_check').fetchone() != ('ok',):
        raise ValueError('parent index integrity check failed')
    for number, start, end, manifest_sha, payload_sha, rows, digest in records:
        window = windows[number]
        parent = index_root / f"{window['start']}_{window['end']}"
        manifest = parent / 'manifest.json'
        if (start != window['start'] or end != window['end'] or manifest.is_symlink()
                or not manifest.is_file() or sha256_file(manifest) != manifest_sha):
            raise ValueError('parent window manifest differs')
        doc = json.loads(manifest.read_bytes())
        payload = Path(doc['data_path'])
        indexed = conn.execute('SELECT product,series,strike,right,event_us,price,source_sha256 '
                               'FROM main.events WHERE source_window=? '
                               'ORDER BY product,series,strike,right,event_us,price,source_sha256',
                               (number,)).fetchall()
        if (payload.is_symlink() or not payload.is_file() or sha256_file(payload) != payload_sha
                or (len(indexed), rows_digest(indexed)) != (rows, digest)):
            raise ValueError('parent window payload or indexed rows differ')
    count = conn.execute('SELECT count(*) FROM main.events').fetchone()[0]
    if count != sum(r[5] for r in records):
        raise ValueError('parent index contains unchecked events')
    return {'windows': len(records), 'events': count, 'first_day': records[0][1],
            'last_day': records[-1][2]}


def _cross_source_strikes(conn: sqlite3.Connection, official: pl.DataFrame) -> dict:
    groups = official.select('product_raw', 'series_raw', 'right_raw').unique()
    conn.execute('CREATE TEMP TABLE union_groups (product TEXT,series TEXT,right TEXT, '
                 'PRIMARY KEY(product,series,right)) WITHOUT ROWID')
    conn.executemany('INSERT INTO union_groups VALUES (?,?,?)', list(groups.iter_rows()))
    rows = conn.execute('SELECT DISTINCT e.product,e.series,e.strike,e.right FROM temp.union_groups g '
                        'JOIN main.events e ON e.product=g.product AND e.series=g.series AND e.right=g.right')
    vendor = pl.DataFrame(rows.fetchall(), schema=['product_raw', 'series_raw', 'strike_raw', 'right_raw'],
                          orient='row')
    if vendor.is_empty():
        raise ValueError('no overlapping source groups for strike-identity verification')
    return check_strike_identity(official, vendor)


def build(database: Path, plan_path: Path, index_root: Path, status_path: Path,
          official_manifest: Path, recent_manifest: Path, output: Path) -> dict:
    database, plan_path, index_root, status_path, official_manifest, recent_manifest, output = (
        path.resolve() for path in (database, plan_path, index_root, status_path, official_manifest,
                                    recent_manifest, output))
    if output.exists() or output.is_symlink():
        raise FileExistsError('preserve earlier source union evidence')
    status = json.loads(status_path.read_bytes())
    plan = json.loads(plan_path.read_bytes())
    if status.get('state') != 'source_index_prefix_complete':
        raise ValueError('parent source index has not completed its planned windows')
    expected_identity = identity(plan_path, plan)
    sessions, _ = candidate_policy(Path(expected_identity['policy_path']))
    if status.get('identity') != expected_identity or status.get('database') != str(database):
        raise ValueError('parent status identity differs')
    official_day, recent_day = date(2026, 10, 6), date(2026, 10, 28)
    gaps = plan.get('not_planned_days', [])
    if official_day.isoformat() not in [row['date'] for row in gaps]:
        raise ValueError('official fallback must correspond to a recorded provider gap')
    official, official_pin = verified_overlay(official_manifest, official_day,
                                              policy_sha256=expected_identity['policy_sha256'])
    recent, recent_pin = verified_overlay(recent_manifest, recent_day,
                                          policy_sha256=expected_identity['policy_sha256'])
    code_pins = {str(p.resolve()): sha256_file(p) for p in IMPLEMENTATIONS}
    lock_path = database.parent / '.writer.lock'
    with lock_path.open('r') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        wal = Path(str(database) + '-wal')
        if wal.exists() and wal.stat().st_size:
            raise ValueError('parent write-ahead log is still present; retry after checkpoint')
        parent_sha = sha256_file(database)
        with connect_union(database, [(official_day, official), (recent_day, recent)]) as conn:
            saved = conn.execute("SELECT value FROM main.metadata WHERE key='identity'").fetchone()
            if not saved or json.loads(saved[0]) != expected_identity:
                raise ValueError('parent database identity differs')
            checkpoints = validate_parent_checkpoints(conn, plan, index_root)
            if not checkpoints['last_day'] < recent_day.isoformat():
                raise ValueError('recent source day must follow the completed parent plan')
            strikes = _cross_source_strikes(conn, official)
        if (sha256_file(database) != parent_sha or (wal.exists() and wal.stat().st_size)
                or any(sha256_file(Path(p)) != h for p, h in code_pins.items())):
            raise ValueError('parent or implementation changed during validation')
    fingerprint_input = {'contract': CONTRACT, 'parent_sha256': parent_sha,
                         'plan_sha256': expected_identity['plan_sha256'],
                         'policy_sha256': expected_identity['policy_sha256'],
                         'overlays': [official_pin, recent_pin], 'implementations': code_pins,
                         'session_windows': sessions}
    fingerprint = hashlib.sha256(json.dumps(fingerprint_input, ensure_ascii=False, sort_keys=True,
                                            separators=(',', ':')).encode()).hexdigest()
    result = {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'data_fingerprint': fingerprint, 'parent_database': str(database), 'parent_sha256': parent_sha,
              'parent_status': str(status_path), 'parent_identity': expected_identity,
              'parent_checkpoints': checkpoints, 'overlay_sources': [official_pin, recent_pin],
              'cross_source_strike_identity': strikes, 'implementation_sha256': code_pins,
              'session_windows': sessions,
              'known_provider_gap_days': gaps, 'retrospective_official_substitute_days': [official_day.isoformat()],
              'training_ready': False,
              'limitations': ['This combines observed source events without changing the parent or provider receipts.',
                  'The provider-gap flag remains even when a retrospective official substitute exists.',
                  'Source completeness, historic delivery, contract eligibility, execution and accounting remain unproven.']}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in ('database', 'plan', 'index-root', 'status', 'parent-run',
                     'official-manifest', 'recent-manifest', 'output'):
        parser.add_argument('--' + argument, type=Path, required=True)
    parser.add_argument('--max-wait-seconds', type=int, default=86400)
    args = parser.parse_args()
    if not 1 <= args.max_wait_seconds <= 7 * 86400:
        raise ValueError('a finite wait of at most seven days is required')
    start_pins = {str(p.resolve()): sha256_file(p) for p in IMPLEMENTATIONS}
    deadline = time.monotonic() + args.max_wait_seconds
    next_progress = 0.0
    while time.monotonic() < deadline:
        run = json.loads(args.parent_run.read_bytes())
        if run.get('state') == 'succeeded':
            if any(sha256_file(Path(p)) != h for p, h in start_pins.items()):
                raise ValueError('source union implementation changed during wait')
            wal = Path(str(args.database) + '-wal')
            if wal.exists() and wal.stat().st_size:
                if time.monotonic() >= next_progress:
                    print(json.dumps({'state': 'waiting_for_parent_checkpoint',
                                      'provider_calls_by_waiter': 0}), flush=True)
                    next_progress = time.monotonic() + 300
                time.sleep(min(30, max(0, deadline - time.monotonic())))
                continue
            result = build(args.database, args.plan, args.index_root, args.status,
                           args.official_manifest, args.recent_manifest, args.output)
            print(json.dumps({'state': 'verified_source_union_published', 'output': str(args.output),
                              'parent_checkpoints': result['parent_checkpoints'],
                              'training_ready': False}, ensure_ascii=False), flush=True)
            return
        if run.get('state') not in {'queued', 'running'}:
            raise ValueError('parent workflow ended without success: ' + str(run.get('state')))
        if time.monotonic() >= next_progress:
            status = json.loads(args.status.read_bytes())
            print(json.dumps({'state': 'waiting_for_parent_source_index',
                              'verified_prefix_windows': status.get('verified_prefix_windows'),
                              'planned_windows': status.get('planned_windows'),
                              'last_indexed_date': status.get('last_indexed_date'),
                              'provider_calls_by_waiter': 0}), flush=True)
            next_progress = time.monotonic() + 300
        time.sleep(min(30, max(0, deadline - time.monotonic())))
    raise TimeoutError('parent source index did not finish before the finite wait expired')


if __name__ == '__main__':
    main()
