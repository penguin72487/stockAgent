"""Source transactions, not stale caches, own endpoint progress totals."""
import json
import random
import sqlite3
from types import SimpleNamespace

import pytest

from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_status import (
    bulk_coverage_rows, initialize_bulk_coverage_summary, scan_bulk_coverage,
)


def database(path=':memory:', **kwargs):
    db = sqlite3.connect(path, **kwargs)
    db.row_factory = sqlite3.Row
    db.executescript('''CREATE TABLE bulk_api_coverage(base_url TEXT PRIMARY KEY,
        provider TEXT NOT NULL,total_articles INTEGER NOT NULL,total_pages INTEGER NOT NULL,
        observed_at_utc TEXT NOT NULL);
        CREATE TABLE bulk_window_splits(scope_url TEXT PRIMARY KEY,provider TEXT NOT NULL,
        child_urls_json TEXT NOT NULL,observed_at_utc TEXT NOT NULL);
        CREATE TABLE queue_settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);''')
    return db


@pytest.fixture
def db():
    connection = database()
    initialize_bulk_coverage_summary(connection)
    yield connection
    connection.close()


def observe(db, scope='https://a.test/api?day=1', provider='a', articles=12, pages=2, clock='2026-01-01'):
    NewsQueue.observe_bulk_coverage(SimpleNamespace(db=db), scope, provider, articles, pages, clock)


def split(db, scope='https://a.test/api?day=1', children='["child"]'):
    db.execute('INSERT INTO bulk_window_splits VALUES (?,\'unrelated-provider\',?,\'split-clock\')',
               (scope, children))


def assert_exact(db):
    splits = {r['scope_url']: json.loads(r['child_urls_json'])
              for r in db.execute('SELECT * FROM bulk_window_splits')}
    groups = {}
    for row in db.execute('SELECT * FROM bulk_api_coverage'):
        key = row['provider'], row['base_url'].split('?', 1)[0]
        item = groups.setdefault(key, [*key, 0, 0, 0, 0, 0, ''])
        children = splits.get(row['base_url'])
        superseded = isinstance(children, list) and bool(children)
        item[2] += 1
        item[3] += int(superseded)
        item[4] += int(not superseded)
        if not superseded:
            item[5] += row['total_articles']
            item[6] += row['total_pages']
        item[7] = max(item[7], row['observed_at_utc'])
    expected = [tuple(groups[key]) for key in sorted(groups)]
    assert [tuple(row) for row in scan_bulk_coverage(db)] == expected
    assert [tuple(row) for row in bulk_coverage_rows(db)] == expected


@pytest.mark.parametrize('children', ['[]', '["x"]', '[null]', 'null', 'false', '{}', '"x"'])
@pytest.mark.parametrize('split_first', [True, False])
def test_json_shape_and_split_insertion_order(db, children, split_first):
    if split_first:
        split(db, children=children)
    observe(db)
    if not split_first:
        split(db, children=children)
    assert_exact(db)


def test_canonical_upsert_moves_maximum_and_last_group(db):
    observe(db, clock='2026-03-01')
    rowid = db.execute('SELECT rowid FROM bulk_api_coverage').fetchone()[0]
    observe(db, clock='2026-02-01', articles=5)
    assert db.execute('SELECT rowid FROM bulk_api_coverage').fetchone()[0] == rowid
    observe(db, scope='https://a.test/api?day=2', clock='2026-04-01')
    assert_exact(db)
    db.execute("UPDATE bulk_api_coverage SET provider='b',base_url='https://b.test/api?day=3' "
               "WHERE base_url='https://a.test/api?day=2'")
    assert_exact(db)
    db.execute("DELETE FROM bulk_api_coverage WHERE provider='a'")
    assert_exact(db)
    db.execute('DELETE FROM bulk_api_coverage')
    assert_exact(db)
    assert list(db.execute('SELECT * FROM bulk_endpoint_metrics')) == []


def test_split_moves_deletes_and_irrelevant_provider_clock(db):
    observe(db)
    observe(db, scope='https://b.test/api?day=2', provider='b')
    split(db)
    assert_exact(db)
    db.execute("UPDATE bulk_window_splits SET provider='ignored',observed_at_utc='ignored'")
    assert_exact(db)
    db.execute("UPDATE bulk_window_splits SET scope_url='https://b.test/api?day=2'")
    assert_exact(db)
    db.execute("UPDATE bulk_window_splits SET child_urls_json='[]'")
    assert_exact(db)
    db.execute('DELETE FROM bulk_window_splits')
    assert_exact(db)


def test_rollback_and_savepoint_do_not_publish_transient_totals(db):
    observe(db)
    db.commit()
    original = [tuple(r) for r in bulk_coverage_rows(db)]
    db.execute('SAVEPOINT temporary')
    observe(db, articles=900)
    split(db)
    assert_exact(db)
    db.execute('ROLLBACK TO temporary')
    assert [tuple(r) for r in bulk_coverage_rows(db)] == original
    db.execute('RELEASE temporary')
    observe(db, articles=3)
    db.rollback()
    assert [tuple(r) for r in bulk_coverage_rows(db)] == original


def test_wal_readers_keep_source_and_projection_in_same_snapshot(tmp_path):
    path = tmp_path / 'queue.sqlite3'
    writer = database(path)
    writer.execute('PRAGMA journal_mode=WAL')
    initialize_bulk_coverage_summary(writer)
    observe(writer)
    writer.commit()
    reader = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    reader.row_factory = sqlite3.Row
    try:
        reader.execute('BEGIN')
        assert_exact(reader)
        old = [tuple(r) for r in bulk_coverage_rows(reader)]
        observe(writer, articles=700)
        writer.commit()
        assert [tuple(r) for r in bulk_coverage_rows(reader)] == old
        assert_exact(reader)
        reader.rollback()
        assert_exact(reader)
        assert [tuple(r) for r in bulk_coverage_rows(reader)] != old
    finally:
        reader.close()
        writer.close()


def test_legacy_reader_fallback_and_idempotent_initialization():
    db = database()
    try:
        observe(db)
        before = db.total_changes
        assert_exact(db)
        assert db.total_changes == before
        initialize_bulk_coverage_summary(db)
        assert_exact(db)
        before = db.total_changes
        initialize_bulk_coverage_summary(db)
        assert db.total_changes == before
    finally:
        db.close()


def test_interrupted_autocommit_migration_rolls_back_schema_and_seed():
    class Interrupt(sqlite3.Connection):
        fail = True

        def execute(self, sql, *args, **kwargs):
            if self.fail and sql.startswith('INSERT INTO bulk_endpoint_metrics '):
                raise sqlite3.OperationalError('test seed interruption')
            return super().execute(sql, *args, **kwargs)

    db = database(factory=Interrupt, isolation_level=None)
    try:
        observe(db)
        before = [tuple(r) for r in db.execute('SELECT type,name,sql FROM sqlite_master ORDER BY name')]
        with pytest.raises(sqlite3.OperationalError, match='test seed interruption'):
            initialize_bulk_coverage_summary(db)
        assert [tuple(r) for r in db.execute('SELECT type,name,sql FROM sqlite_master ORDER BY name')] == before
        db.fail = False
        initialize_bulk_coverage_summary(db)
        assert_exact(db)
    finally:
        db.close()


@pytest.mark.parametrize('missing', ['marker', 'trigger', 'index', 'table'])
def test_incomplete_generation_is_rebuilt_from_source(db, missing):
    observe(db)
    db.commit()
    if missing == 'marker':
        db.execute("DELETE FROM queue_settings WHERE key='bulk_coverage_summary_version'")
    else:
        name = {'trigger': 'bulk_split_summary_update', 'index': 'bulk_coverage_endpoint_observed',
                'table': 'bulk_endpoint_metrics'}[missing]
        db.execute(f'DROP {missing.upper()} {name}')
    initialize_bulk_coverage_summary(db)
    assert_exact(db)


def test_mixed_source_transactions_follow_independent_oracle(db):
    rng = random.Random(4319)
    for step in range(180):
        scope = f'https://{rng.choice("ab")}.test/api?day={rng.randrange(7)}'
        operation = rng.randrange(5)
        if operation < 2:
            observe(db, scope=scope, provider=rng.choice('ab'), articles=rng.randrange(200),
                    pages=rng.randrange(20), clock=f'2026-01-{rng.randrange(1,29):02}')
        elif operation == 2:
            db.execute('INSERT INTO bulk_window_splits VALUES (?,\'a\',?,\'clock\') '
                       'ON CONFLICT(scope_url) DO UPDATE SET child_urls_json=excluded.child_urls_json',
                       (scope, rng.choice(['[]', '["x"]', 'null', '{}'])))
        elif operation == 3:
            db.execute('DELETE FROM bulk_api_coverage WHERE base_url=?', (scope,))
        else:
            db.execute('DELETE FROM bulk_window_splits WHERE scope_url=?', (scope,))
        assert_exact(db)
        if step % 9 == 0:
            db.rollback()
            assert_exact(db)
        elif step % 7 == 0:
            db.commit()


def test_status_read_is_bounded_and_latest_clock_uses_index(db):
    observe(db)
    seen = []
    db.set_trace_callback(seen.append)
    bulk_coverage_rows(db)
    db.set_trace_callback(None)
    assert len(seen) == 1 and 'bulk_api_coverage' not in seen[0]
    plan = db.execute("EXPLAIN QUERY PLAN SELECT max(observed_at_utc) FROM bulk_api_coverage "
        "WHERE provider=? AND (CASE WHEN instr(base_url,'?')=0 THEN base_url "
        "ELSE substr(base_url,1,instr(base_url,'?')-1) END)=?", ('a', 'https://a.test/api'))
    assert 'bulk_coverage_endpoint_observed' in ' '.join(str(tuple(row)) for row in plan)
