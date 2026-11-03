from datetime import date, datetime
from pathlib import Path
import hashlib
import json
import sqlite3
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.run_option_full_union_audit_after import compare_clock
from stockagent.data.tw_option_full_carry_index import add_events, create_schema, lookup
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_event_index import compact_decision_events
from stockagent.data.tw_option_source_union import (connect_union, lookup_pinned_union,
                                                     open_pinned_union, verified_overlay)


WINDOWS = {'TXO': (525, 825)}


def event(day, clock, price, *, strike='8000', source='a'):
    stamp = datetime.fromisoformat(f'{day}T{clock}').replace(tzinfo=ZoneInfo('Asia/Taipei'))
    return {'event_ts': stamp, 'source_date': stamp.date(), 'product_raw': 'TXO',
            'series_raw': '202610', 'strike_raw': strike, 'right_raw': 'C',
            'price_raw': str(price), 'source_sha256': source * 64}


def parent(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / 'parent.sqlite3'
    with sqlite3.connect(path) as conn:
        create_schema(conn)
        for day in sorted({row['source_date'] for row in rows}):
            frame = pl.DataFrame([row for row in rows if row['source_date'] == day])
            add_events(conn, compact_decision_events(frame, day=day, session_windows=WINDOWS))
    return path


def test_union_matches_original_source_and_keeps_parent_unchanged(tmp_path):
    base = [event('2026-10-05', '13:00:00', 10),
            event('2026-10-07', '08:45:00', 30, strike='8100'),
            event('2026-10-07', '08:45:00', 31, strike='8100', source='b')]
    added = [event('2026-10-06', '10:00:00', 20, source='c'),
             event('2026-10-06', '11:00:00', 21, strike='8100', source='c')]
    path = parent(tmp_path, base)
    original_hash = sha256_file(path)
    frame = pl.DataFrame(added)
    compact = compact_decision_events(frame, day=date(2026, 10, 6), session_windows=WINDOWS)
    compact = compact.with_columns(pl.lit(date(2026, 10, 6)).alias('source_date'))
    keys = [('TXO', '202610', '8000', 'C'), ('TXO', '202610', '8100', 'C')]
    with connect_union(path, [(date(2026, 10, 6), compact)]) as conn:
        for clock in ('08:45', '09:00'):
            actual = lookup(conn, keys, day=date(2026, 10, 7), clock=clock, session_windows=WINDOWS)
            expected = reference_candidates(pl.DataFrame(base + added), trading_date=date(2026, 10, 7),
                                            decision_time=clock, session_windows=WINDOWS)
            assert actual.equals(expected)
        tied = lookup(conn, keys, day=date(2026, 10, 7), clock='09:00', session_windows=WINDOWS)
        tied = tied.filter(pl.col('strike_decimal') == '8100')
        assert tied.item(0, 'reference_kind') == 'ambiguous_first_second'
        assert tied.item(0, 'reference_price_decimal') is None
    assert sha256_file(path) == original_hash
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT count(*) FROM events').fetchone()[0] == len(base)


def test_union_rejects_overlapping_or_misdated_sources(tmp_path):
    day = date(2026, 10, 5)
    path = parent(tmp_path, [event('2026-10-05', '13:00:00', 10)])
    frame = pl.DataFrame([event('2026-10-05', '12:00:00', 11, source='c')])
    with pytest.raises(ValueError, match='already contains'):
        connect_union(path, [(day, frame)])
    with pytest.raises(ValueError, match='unique'):
        connect_union(path, [(day, frame), (day, frame)])
    with pytest.raises(ValueError, match='dates differ'):
        connect_union(path, [(date(2026, 10, 6), frame)])


def test_verified_overlay_rejects_changed_source_and_policy(tmp_path):
    policy = tmp_path / 'policy.json'
    source = tmp_path / 'source.raw'
    receipt = tmp_path / 'receipt.json'
    policy.write_text('{}')
    source.write_text('real source')
    receipt.write_text('{}')
    day = date(2026, 10, 6)
    directory = tmp_path / 'index'
    directory.mkdir()
    data = directory / 'events.parquet'
    frame = pl.DataFrame([event('2026-10-06', '12:00:00', 11,
                                source='c')]).with_columns(pl.lit(sha256_file(source)).alias('source_sha256'))
    frame.write_parquet(data)
    doc = {'contract': 'taifex_option_calendar_event_index_v1', 'training_ready': False,
           'event_calendar_date': day.isoformat(), 'policy_path': str(policy),
           'policy_sha256': sha256_file(policy), 'policy_linked_pins': {}, 'implementation_sha256': {},
           'source_pins': [{'path': str(source), 'sha256': sha256_file(source),
                            'receipt_path': str(receipt), 'receipt_sha256': sha256_file(receipt)}],
           'data_path': str(data), 'data_sha256': sha256_file(data), 'rows': 1}
    manifest = directory / 'manifest.json'
    manifest.write_text(json.dumps(doc))
    selected, readback = verified_overlay(manifest, day, policy_sha256=sha256_file(policy))
    assert selected.equals(frame)
    assert readback['selected_events'] == 1
    with pytest.raises(ValueError, match='matching research-only'):
        verified_overlay(manifest, day, policy_sha256='f' * 64)
    source.write_text('changed source')
    with pytest.raises(ValueError, match='pin differs'):
        verified_overlay(manifest, day, policy_sha256=sha256_file(policy))


def test_official_overlay_uses_immutable_receipt_snapshot_after_live_refresh(tmp_path):
    policy, source, live = tmp_path / 'policy.json', tmp_path / 'source.raw', tmp_path / 'manifest.json'
    policy.write_text('{}')
    source.write_text('official file')
    live.write_text('old receipt')
    old_hash = sha256_file(live)
    versions = tmp_path / 'manifest_versions'
    versions.mkdir()
    snapshot = versions / f'{old_hash}.json'
    snapshot.write_bytes(live.read_bytes())
    folder = tmp_path / 'index'
    folder.mkdir()
    data = folder / 'events.parquet'
    pl.DataFrame([event('2026-10-06', '12:00:00', 11)]).with_columns(
        pl.lit(sha256_file(source)).alias('source_sha256')).write_parquet(data)
    doc = {'contract': 'taifex_option_calendar_event_index_v1', 'training_ready': False,
           'event_calendar_date': '2026-10-06', 'policy_path': str(policy),
           'policy_sha256': sha256_file(policy), 'source_manifest_path': str(live),
           'source_manifest_sha256': old_hash, 'policy_linked_pins': {}, 'implementation_sha256': {},
           'source_pins': [{'path': str(source), 'sha256': sha256_file(source),
                            'receipt_path': str(live), 'receipt_sha256': old_hash}],
           'data_path': str(data), 'data_sha256': sha256_file(data), 'rows': 1}
    manifest = folder / 'manifest.json'
    manifest.write_text(json.dumps(doc))
    live.write_text('new receipt')
    _, pin = verified_overlay(manifest, date(2026, 10, 6), policy_sha256=sha256_file(policy))
    assert pin['receipt_snapshots'][str(live)] == str(snapshot)
    snapshot.write_text('damaged receipt')
    with pytest.raises(ValueError, match='pin differs'):
        verified_overlay(manifest, date(2026, 10, 6), policy_sha256=sha256_file(policy))


def test_pinned_union_retains_gap_and_rejects_changed_parent(tmp_path):
    base_rows = [event('2026-10-05', '13:00:00', 10),
                 event('2026-10-07', '08:45:00', 30, strike='8100')]
    path = parent(tmp_path, base_rows)
    policy = tmp_path / 'policy.json'
    plan = tmp_path / 'plan.json'
    policy.write_text('{}')
    gaps = [{'date': '2026-10-06'}]
    plan.write_text(json.dumps({'not_planned_days': gaps}))
    identity = {'plan_path': str(plan), 'plan_sha256': sha256_file(plan),
                'policy_path': str(policy), 'policy_sha256': sha256_file(policy),
                'policy_linked_pins': {}}
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        conn.execute('INSERT INTO metadata VALUES (?,?)', ('identity', json.dumps(identity)))
        conn.execute('CREATE TABLE windows(idx INTEGER,start TEXT,end TEXT)')
        conn.execute('INSERT INTO windows VALUES (0,?,?)', ('2026-10-05', '2026-10-07'))
    (tmp_path / '.writer.lock').touch()

    overlays, source_frames = [], []
    for day, kind, source_character in [('2026-10-06', 'taifex_option_calendar_event_index_v1', 'c'),
                                         ('2026-10-28', 'tw_option_two_clock_event_index_v1', 'd')]:
        folder = tmp_path / day
        folder.mkdir()
        source, receipt, data = folder / 'raw', folder / 'receipt', folder / 'events.parquet'
        source.write_text(day)
        receipt.write_text('{}')
        frame = pl.DataFrame([event(day, '13:00:00', 20, source=source_character)]).with_columns(
            pl.lit(sha256_file(source)).alias('source_sha256'))
        source_frames.append(frame)
        frame.write_parquet(data)
        source_doc = {'contract': kind, 'training_ready': False, 'event_calendar_date': day,
                      'policy_path': str(policy), 'policy_sha256': sha256_file(policy),
                      'policy_linked_pins': {}, 'implementation_sha256': {},
                      'source_pins': [{'path': str(source), 'sha256': sha256_file(source),
                                       'receipt_path': str(receipt), 'receipt_sha256': sha256_file(receipt)}],
                      'data_path': str(data), 'data_sha256': sha256_file(data), 'rows': 1}
        source_manifest = folder / 'manifest.json'
        source_manifest.write_text(json.dumps(source_doc))
        _, pin = verified_overlay(source_manifest, date.fromisoformat(day), policy_sha256=sha256_file(policy))
        overlays.append(pin)
    doc = {'contract': 'tw_option_source_union_manifest_v1', 'training_ready': False,
           'parent_database': str(path), 'parent_sha256': sha256_file(path), 'parent_identity': identity,
           'parent_checkpoints': {'windows': 1, 'events': 2, 'first_day': '2026-10-05', 'last_day': '2026-10-07'},
           'overlay_sources': overlays, 'implementation_sha256': {}, 'session_windows': WINDOWS,
           'known_provider_gap_days': gaps}
    identity_input = {'contract': doc['contract'], 'parent_sha256': doc['parent_sha256'],
                      'plan_sha256': identity['plan_sha256'], 'policy_sha256': identity['policy_sha256'],
                      'overlays': overlays, 'implementations': {}, 'session_windows': WINDOWS}
    doc['data_fingerprint'] = hashlib.sha256(json.dumps(identity_input, ensure_ascii=False, sort_keys=True,
                                                        separators=(',', ':')).encode()).hexdigest()
    manifest = tmp_path / 'union.json'
    manifest.write_text(json.dumps(doc))
    with open_pinned_union(manifest) as (conn, frozen):
        output = lookup_pinned_union(conn, frozen, [('TXO', '202610', '8000', 'C')],
                                     day=date(2026, 10, 7), clock='08:45')
        assert output.item(0, 'trade_ts').date() == date(2026, 10, 6)
        assert output.item(0, 'known_missing_day_since_reference') is True
        expected = reference_candidates(pl.concat([pl.DataFrame(base_rows), *source_frames]),
                                        trading_date=date(2026, 10, 7), decision_time='08:45',
                                        session_windows=WINDOWS)
        audit = compare_clock(conn, frozen, expected, day=date(2026, 10, 7), clock='08:45')
        assert audit == {'contract_keys_compared': 1, 'reference_mismatches': 0,
                         'known_provider_gap_flags_retained': 1}
        with pytest.raises(ValueError, match='outside the pinned source horizon'):
            lookup_pinned_union(conn, frozen, [], day=date(2026, 10, 29), clock='08:45')
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO windows VALUES (1,'2026-10-08','2026-10-09')")
    with pytest.raises(ValueError, match='frozen parent index changed'):
        with open_pinned_union(manifest):
            pass
