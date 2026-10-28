"""Incremental discovery-error counts retain the canonical queue's semantics."""
import sqlite3
import time

import pytest

from downloader.news_fulltext import run_cycle
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config as config, http_fixture

from downloader.news_fulltext_discovery_errors import (
    advance_discovery_errors, discovery_errors, initialize_discovery_errors,
)


def database():
    db = sqlite3.connect(':memory:')
    db.executescript('''
        CREATE TABLE discovery(url TEXT PRIMARY KEY, provider TEXT NOT NULL, kind TEXT NOT NULL,
            error_code TEXT, superseded_by_windows_json TEXT, due REAL NOT NULL DEFAULT 0);
        CREATE TABLE discovery_static_counts(singleton INTEGER PRIMARY KEY, active_errors INTEGER NOT NULL);
        INSERT INTO discovery_static_counts VALUES(1,0);
    ''')
    return db


def expected(db):
    return {(provider, kind, code): count for provider, kind, code, count in db.execute('''SELECT provider,kind,error_code,count(*) FROM discovery
        WHERE error_code IS NOT NULL AND superseded_by_windows_json IS NULL
        GROUP BY provider,kind,error_code''')}


def grouped(db):
    return {(provider, kind, code): count for provider, kind, code, count in db.execute(
        'SELECT provider,kind,error_code,rows FROM discovery_error_groups')}


def insert(db, rowid, url, provider='one', kind='rss_fulltext', error=None, archived=None):
    db.execute('INSERT INTO discovery(rowid,url,provider,kind,error_code,superseded_by_windows_json) '
               'VALUES(?,?,?,?,?,?)', (rowid, url, provider, kind, error, archived))


def finish(db, limit=2):
    for _ in range(100):
        advance_discovery_errors(db, limit=limit, max_seconds=10)
        if discovery_errors(db)['state'] == 'ready':
            return
    raise AssertionError('rollup did not finish')


def test_incremental_build_tracks_changes_on_both_sides_of_cursor():
    with database() as db:
        insert(db, -10, 'a', error='discovery_http_404')
        insert(db, 1, 'b')
        insert(db, 2, 'c', provider='two', error='invalid_feed')
        insert(db, 3, 'd', error='transport_error', archived='[]')
        insert(db, 5, 'e', error='discovery_http_403')
        db.commit()
        initialize_discovery_errors(db)
        assert discovery_errors(db)['state'] == 'building'
        assert discovery_errors(db)['providers'] is None
        advance_discovery_errors(db, limit=2, max_seconds=10)
        db.execute("UPDATE discovery SET error_code='invalid_feed' WHERE url='a'")
        db.execute("DELETE FROM discovery WHERE url='b'")
        insert(db, -5, 'f', error='transport_error')  # inserted behind the cursor
        db.execute("UPDATE discovery SET error_code='invalid_feed',provider='three' WHERE url='e'")
        db.execute("UPDATE discovery SET superseded_by_windows_json=NULL WHERE url='d'")
        db.commit()
        finish(db)
        assert grouped(db) == expected(db)
        result = discovery_errors(db)
        assert result['state'] == 'ready' and result['active_errors'] == sum(expected(db).values())


def test_ready_updates_deletes_upserts_and_rollbacks_are_transactional():
    with database() as db:
        initialize_discovery_errors(db)
        insert(db, 1, 'a', error='invalid_feed')
        insert(db, 2, 'b', error='invalid_feed')
        db.commit()
        finish(db)
        before = grouped(db)
        db.execute('BEGIN')
        db.execute("UPDATE discovery SET error_code='discovery_http_404',provider='two' WHERE url='a'")
        db.execute("DELETE FROM discovery WHERE url='b'")
        insert(db, -10, 'c', error='transport_error')
        assert grouped(db) == expected(db)
        db.rollback()
        assert grouped(db) == before == expected(db)
        db.execute("INSERT INTO discovery(url,provider,kind,error_code) VALUES ('a','two','feed','transport_error') "
                   "ON CONFLICT(url) DO UPDATE SET provider=excluded.provider,kind=excluded.kind,error_code=excluded.error_code")
        db.execute("UPDATE discovery SET superseded_by_windows_json='[]' WHERE url='b'")
        db.commit()
        assert grouped(db) == expected(db)
        assert all(value > 0 for value in grouped(db).values())


@pytest.mark.parametrize('alias', ['rowid', '_rowid_', 'oid'])
def test_moving_rows_across_scan_cursor_is_tracked_for_every_rowid_alias(alias):
    with database() as db:
        insert(db, 1, 'a', error='invalid_feed')
        insert(db, 3, 'b', error='transport_error')
        db.commit()
        initialize_discovery_errors(db)
        advance_discovery_errors(db, limit=1, max_seconds=10)
        db.execute(f'UPDATE discovery SET {alias}=9 WHERE url=?', ('a',))
        db.execute(f'UPDATE discovery SET {alias}=-5 WHERE url=?', ('b',))
        db.commit()
        finish(db)
        assert grouped(db) == expected(db)
        db.execute(f'UPDATE discovery SET {alias}=5 WHERE url=?', ('a',))
        assert grouped(db) == expected(db)


def test_scanning_transaction_rolls_back_with_caller_and_resumes():
    with database() as db:
        for i in range(5):
            insert(db, i + 1, str(i), error='invalid_feed' if i % 2 else None)
        db.commit()
        initialize_discovery_errors(db)
        db.execute('BEGIN')
        advance_discovery_errors(db, limit=2, max_seconds=10)
        assert discovery_errors(db)['scanned_rows'] == 2
        db.rollback()
        assert discovery_errors(db)['scanned_rows'] == 0 and not grouped(db)
        initialize_discovery_errors(db)
        finish(db)
        assert grouped(db) == expected(db)


def test_missing_generation_or_trigger_rebuilds_derived_data_only():
    with database() as db:
        insert(db, 1, 'a', error='invalid_feed')
        db.commit()
        initialize_discovery_errors(db)
        finish(db)
        original = list(db.execute('SELECT * FROM discovery'))
        db.execute('DROP TRIGGER discovery_error_update')
        db.commit()
        initialize_discovery_errors(db)
        assert discovery_errors(db)['state'] == 'building'
        finish(db)
        assert grouped(db) == expected(db)
        assert list(db.execute('SELECT * FROM discovery')) == original


def test_missing_summary_or_crosscheck_mismatch_is_unknown_not_zero():
    with database() as db:
        assert discovery_errors(db)['state'] == 'unavailable'
        initialize_discovery_errors(db)
        insert(db, 1, 'a', error='invalid_feed')
        db.commit()
        finish(db)
        assert discovery_errors(db, expected_active_errors=1)['state'] == 'ready'
        mismatch = discovery_errors(db, expected_active_errors=2)
        assert mismatch['state'] == 'unavailable' and mismatch['providers'] is None
        advance_discovery_errors(db, limit=1, max_seconds=10, expected_active_errors=2)
        assert discovery_errors(db)['state'] == 'building'
        finish(db)
        assert grouped(db) == expected(db)


def test_missing_group_restarts_rollup_without_blocking_source_update():
    with database() as db:
        initialize_discovery_errors(db)
        insert(db, 1, 'a', error='invalid_feed')
        db.commit()
        finish(db)
        db.execute('DELETE FROM discovery_error_groups')  # Simulate damaged derived state.
        db.execute("UPDATE discovery SET error_code='transport_error' WHERE url='a'")
        db.commit()
        assert db.execute("SELECT error_code FROM discovery WHERE url='a'").fetchone()[0] == 'transport_error'
        assert discovery_errors(db)['state'] == 'building'
        finish(db)
        assert grouped(db) == expected(db)


def test_rollup_publishes_only_fixed_error_classes_not_private_error_strings():
    with database() as db:
        initialize_discovery_errors(db)
        insert(db, 1, 'secret-url', error='PRIVATE_TOKEN_ORIGIN')
        insert(db, 2, 'another-url', error='discovery_http_404')
        db.commit()
        finish(db)
        result = discovery_errors(db)
        assert result['providers']['one']['reasons'] == {'http_404': 1, 'other': 1}
        assert 'PRIVATE_TOKEN_ORIGIN' not in str(result) and 'secret-url' not in str(result)


def test_canonical_owner_resumes_build_and_updates_status_without_source_calls(config, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner

    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.db.executemany('INSERT INTO discovery(url,provider,kind,error_code,due) VALUES (?,?,?,?,?)',
                ((f'https://publisher.test/feed/{i}', 'publisher', 'rss_fulltext',
                  'discovery_http_404' if i in {1, 2199} else None, time.time() + 7200)
                 for i in range(2200)))
        initial = run_cycle(queue, http, config, max_articles=0, max_discovery_requests=0)
        assert initial['discovery_error_rollup']['state'] == 'building'
        assert initial['discovery_error_rollup']['providers'] is None
        visited = initial['discovery_error_rollup']['scanned_rows']
        assert visited > 0
        queue.close()
        queue = NewsQueue(tmp_path, http.registry)
        assert discovery_errors(queue.db)['scanned_rows'] == visited
        for _ in range(20):
            result = run_cycle(queue, http, config, max_articles=0, max_discovery_requests=0)
            if result['discovery_error_rollup']['state'] == 'ready':
                break
        summary = result['discovery_error_rollup']
        assert summary['state'] == 'ready' and summary['active_errors'] == result['discovery_errors'] == 2
        assert summary['providers']['publisher']['reasons'] == {'http_404': 2}
        with queue.db:
            queue.db.execute("UPDATE discovery SET error_code='transport_error' WHERE url=?",
                             ('https://publisher.test/feed/2',))
        result = queue.status(publish=False)
        assert result['discovery_error_rollup']['active_errors'] == result['discovery_errors'] == 3
        assert result['discovery_error_rollup']['providers']['publisher']['reasons']['transport'] == 1
        assert not session.calls
    finally:
        queue.close()
        http.close()


def test_idle_owner_continues_slices_without_extra_status_writes_or_source_calls(config, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner

    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    config['service_idle_max_seconds'] = 60
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.db.executemany('INSERT INTO discovery(url,provider,kind,error_code,due) VALUES (?,?,?,?,?)',
                ((f'https://publisher.test/idle/{i}', 'publisher', 'rss_fulltext',
                  'discovery_http_404' if i == 5000 else None, time.time() + 7200)
                 for i in range(6000)))
        published = []
        original_publish = queue.publish_status

        def track_publish(result):
            published.append(result['activity'])
            return original_publish(result)

        monkeypatch.setattr(queue, 'publish_status', track_publish)
        result = owner.run_service(queue, http, config, max_articles=0,
                                   max_discovery_requests=0, duration_seconds=5)
        summary = result['discovery_error_rollup']
        assert summary['state'] == 'ready'
        assert summary['active_errors'] == result['discovery_errors'] == 1
        assert summary['providers']['publisher']['reasons'] == {'http_404': 1}
        assert len(published) <= 3  # normal cycle, idle heartbeat, final status
        assert not session.calls
    finally:
        queue.close()
        http.close()


def test_owner_repairs_a_lost_trigger_even_if_the_total_did_not_change(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,provider,kind,error_code,due) VALUES (?,?,?,?,?)',
                             ('https://publisher.test/repair', 'publisher', 'rss_fulltext', 'invalid_feed', time.time() + 7200))
        queue.advance_discovery_error_rollup()
        assert queue.status(publish=False)['discovery_error_rollup']['state'] == 'ready'
        with queue.db:
            queue.db.execute('DROP TRIGGER discovery_error_update')
            queue.db.execute("UPDATE discovery SET error_code='transport_error' WHERE url=?", ('https://publisher.test/repair',))
        # The global total is still one, but the reason is stale. Do not publish it.
        assert queue.status(publish=False)['discovery_error_rollup']['state'] == 'unavailable'
        queue.advance_discovery_error_rollup()
        result = queue.status(publish=False)['discovery_error_rollup']
        assert result['state'] == 'ready'
        assert result['providers']['publisher']['reasons'] == {'transport': 1}
        assert not session.calls
    finally:
        queue.close()
        http.close()


def test_owner_rebuilds_corrupt_zero_group_without_touching_source(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,provider,kind,error_code,due) VALUES (?,?,?,?,?)',
                             ('https://publisher.test/repair', 'publisher', 'rss_fulltext', 'invalid_feed', time.time() + 7200))
        queue.advance_discovery_error_rollup()
        with queue.db:
            queue.db.execute("INSERT INTO discovery_error_groups VALUES ('publisher','feed','invalid_feed',0)")
        assert queue.status(publish=False)['discovery_error_rollup']['state'] == 'unavailable'
        queue.advance_discovery_error_rollup()
        queue.advance_discovery_error_rollup()
        result = queue.status(publish=False)['discovery_error_rollup']
        assert result['state'] == 'ready'
        assert result['providers']['publisher']['reasons'] == {'invalid_feed': 1}
        assert not session.calls
    finally:
        queue.close()
        http.close()

