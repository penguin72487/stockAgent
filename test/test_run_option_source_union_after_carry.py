from datetime import date, datetime
import json
import sqlite3
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts import run_option_source_union_after_carry as union_builder
from scripts.run_option_source_union_after_carry import validate_parent_checkpoints
from stockagent.data.tw_option_full_carry_index import add_events, canonical_rows, create_schema, rows_digest
from stockagent.data.tw_option_source_union import connect_union


def frame(day, source):
    return pl.DataFrame([{'event_ts': datetime.fromisoformat(day + 'T09:00:00').replace(
        tzinfo=ZoneInfo('Asia/Taipei')), 'source_date': date.fromisoformat(day),
        'product_raw': 'TXO', 'series_raw': '202610', 'strike_raw': '8000',
        'right_raw': 'C', 'price_raw': '10', 'source_sha256': source * 64}])


def test_parent_checkpoint_validation_ignores_overlay_and_detects_mutation(tmp_path):
    database = tmp_path / 'parent.sqlite3'
    root = tmp_path / 'index'
    folder = root / '2026-10-05_2026-10-05'
    folder.mkdir(parents=True)
    payload = folder / 'events.parquet'
    original = frame('2026-10-05', 'a')
    original.write_parquet(payload)
    manifest = folder / 'manifest.json'
    manifest.write_text(json.dumps({'data_path': str(payload)}))
    plan = {'windows': [{'start': '2026-10-05', 'end': '2026-10-05'}]}
    with sqlite3.connect(database) as conn:
        create_schema(conn)
        add_events(conn, original, source_window=0)
        conn.execute('CREATE TABLE windows(idx INTEGER PRIMARY KEY,start TEXT,end TEXT,manifest_sha256 TEXT,'
                     'payload_sha256 TEXT,rows INTEGER,events_sha256 TEXT)')
        conn.execute('INSERT INTO windows VALUES (?,?,?,?,?,?,?)',
                     (0, '2026-10-05', '2026-10-05', sha256_file(manifest), sha256_file(payload), 1,
                      rows_digest(sorted(canonical_rows(original)))))
    with connect_union(database, [(date(2026, 10, 6), frame('2026-10-06', 'b'))]) as conn:
        assert validate_parent_checkpoints(conn, plan, root)['events'] == 1
    payload.write_bytes(b'changed')
    with connect_union(database, [(date(2026, 10, 6), frame('2026-10-06', 'b'))]) as conn:
        with pytest.raises(ValueError, match='payload or indexed rows differ'):
            validate_parent_checkpoints(conn, plan, root)


def test_complete_builder_publishes_bound_source_only_manifest(tmp_path, monkeypatch):
    database = tmp_path / 'parent.sqlite3'
    root = tmp_path / 'index'
    windows = [{'start': day, 'end': day} for day in ('2026-10-05', '2026-10-27')]
    gaps = [{'date': '2026-10-06', 'prior_readback_status': 'observed_empty'}]
    plan_path = tmp_path / 'plan.json'
    plan_path.write_text(json.dumps({'windows': windows, 'not_planned_days': gaps}))
    policy_path = tmp_path / 'policy.json'
    policy_path.write_text('{}')
    expected_identity = {'contract': 'test', 'plan_path': str(plan_path), 'plan_sha256': sha256_file(plan_path),
                         'policy_path': str(policy_path), 'policy_sha256': sha256_file(policy_path),
                         'policy_linked_pins': {}}
    (tmp_path / '.writer.lock').touch()
    with sqlite3.connect(database) as conn:
        create_schema(conn)
        conn.execute('CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        conn.execute('INSERT INTO metadata VALUES (?,?)', ('identity', json.dumps(expected_identity)))
        conn.execute('CREATE TABLE windows(idx INTEGER PRIMARY KEY,start TEXT,end TEXT,manifest_sha256 TEXT,'
                     'payload_sha256 TEXT,rows INTEGER,events_sha256 TEXT)')
        for number, window in enumerate(windows):
            day = window['start']
            folder = root / f'{day}_{day}'
            folder.mkdir(parents=True)
            payload = folder / 'events.parquet'
            source = frame(day, 'a')
            source.write_parquet(payload)
            manifest = folder / 'manifest.json'
            manifest.write_text(json.dumps({'data_path': str(payload)}))
            add_events(conn, source, source_window=number)
            conn.execute('INSERT INTO windows VALUES (?,?,?,?,?,?,?)',
                         (number, day, day, sha256_file(manifest), sha256_file(payload), 1,
                          rows_digest(sorted(canonical_rows(source)))))
    status = tmp_path / 'status.json'
    status.write_text(json.dumps({'state': 'source_index_prefix_complete', 'identity': expected_identity,
                                  'database': str(database)}))
    monkeypatch.setattr(union_builder, 'identity', lambda _path, _plan: expected_identity)
    monkeypatch.setattr(union_builder, 'candidate_policy', lambda _path: ({'TXO': (525, 825)}, {}))

    def fake_overlay(path, day, *, policy_sha256):
        return frame(day.isoformat(), 'c'), {'manifest_path': str(path), 'selected_day': day.isoformat()}

    monkeypatch.setattr(union_builder, 'verified_overlay', fake_overlay)
    output = tmp_path / 'manifest.json'
    result = union_builder.build(database, plan_path, root, status, tmp_path / 'official.json',
                                 tmp_path / 'recent.json', output)
    assert result['parent_checkpoints'] == {'windows': 2, 'events': 2,
                                             'first_day': '2026-10-05', 'last_day': '2026-10-27'}
    assert result['known_provider_gap_days'] == gaps
    assert result['training_ready'] is False
    assert len(result['data_fingerprint']) == 64
    assert output.is_file()
