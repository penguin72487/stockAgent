#!/usr/bin/env python3
"""Measure the discovery-error rollup on synthetic, in-memory data only."""
from __future__ import annotations

import json
import sqlite3
import time

from downloader.news_fulltext_discovery_errors import (
    advance_discovery_errors, discovery_errors, initialize_discovery_errors,
)


def main() -> None:
    active, archived, errors = 314_417, 62_614, 154
    db = sqlite3.connect(':memory:')
    db.execute('''CREATE TABLE discovery(url TEXT PRIMARY KEY,provider TEXT NOT NULL,kind TEXT NOT NULL,
        error_code TEXT,superseded_by_windows_json TEXT,due REAL NOT NULL)''')
    db.executemany('INSERT INTO discovery VALUES (?,?,?,?,?,?)', (
        (str(i), ('cnyes', 'globalvoices', 'ltn')[i % 3], 'rss_fulltext',
         'discovery_http_404' if i < errors * 2000 and i % 2000 == 0 else None,
         '[]' if i >= active else None, 0)
        for i in range(active + archived)))
    db.commit()
    initialize_discovery_errors(db)

    started, cpu_started = time.perf_counter(), time.process_time()
    slices = []
    while True:
        slice_started = time.perf_counter()
        result = advance_discovery_errors(db)
        slices.append(time.perf_counter() - slice_started)
        if result['state'] == 'ready':
            break
    build_wall = time.perf_counter() - started
    build_cpu = time.process_time() - cpu_started
    expected = {(p, k, e): n for p, k, e, n in db.execute('''SELECT provider,kind,error_code,count(*) FROM discovery
        WHERE error_code IS NOT NULL AND superseded_by_windows_json IS NULL GROUP BY provider,kind,error_code''')}
    observed = {(p, k, e): n for p, k, e, n in db.execute('SELECT provider,kind,error_code,rows FROM discovery_error_groups')}
    assert observed == expected and sum(expected.values()) == errors
    assert result['active_errors'] == errors and result['scanned_rows'] == active + archived

    repetitions = 1000
    started, cpu_started = time.perf_counter(), time.process_time()
    for _ in range(repetitions):
        assert discovery_errors(db, expected_active_errors=errors)['state'] == 'ready'
    status_wall = time.perf_counter() - started
    status_cpu = time.process_time() - cpu_started

    narrowed = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='discovery_error_update'").fetchone()[0]
    update_of = 'AFTER UPDATE OF provider,kind,error_code,superseded_by_windows_json,rowid,_rowid_,oid ON discovery'
    assert update_of in narrowed
    unnarrowed = narrowed.replace(update_of, 'AFTER UPDATE ON discovery')
    updates = []
    for variant in ('unnarrowed', 'narrowed', 'narrowed', 'unnarrowed'):
        db.execute('DROP TRIGGER discovery_error_update')
        db.execute(narrowed if variant == 'narrowed' else unnarrowed)
        db.commit()
        started, cpu_started = time.perf_counter(), time.process_time()
        db.execute('UPDATE discovery SET due=due+1 WHERE rowid<=30000')
        db.commit()
        updates.append({'variant': variant, 'wall_seconds': time.perf_counter() - started,
                        'cpu_seconds': time.process_time() - cpu_started})
    assert {(p, k, e): n for p, k, e, n in db.execute('SELECT provider,kind,error_code,rows FROM discovery_error_groups')} == expected
    db.close()
    print(json.dumps({
        'fixture': {'storage': 'SQLite in memory; synthetic', 'active_rows': active,
                    'archived_rows': archived, 'active_errors': errors, 'groups': len(expected)},
        'rollup': {'slices': len(slices), 'wall_seconds_excluding_scheduler_wait': build_wall,
                   'cpu_seconds': build_cpu, 'max_slice_wall_seconds': max(slices),
                   'rows_per_slice_limit': 2048, 'soft_slice_budget_seconds': .05,
                   'source_group_counts_match': True},
        'ready_projection': {'repetitions': repetitions, 'wall_seconds': status_wall,
                             'cpu_seconds': status_cpu},
        'due_only_update': {'rows_per_trial': 30000, 'abba_trials': updates},
        'limitations': ['No network or real production queue', 'Synthetic memory timings are not a production speed or ETA',
                        'Updates benchmark one statement and not complete acquisition throughput'],
    }, indent=2))


if __name__ == '__main__':
    main()
