"""Budget SQL materialization, not just admission after an eager join."""
from types import SimpleNamespace
import json
import sqlite3
import threading

import pytest

from downloader import news_fulltext_urls as routing
from downloader.news_fulltext_routing import GEOMETRY, TABLE
from test_news_original_queue import alias, queue
from test_news_fulltext import config


KEY = 'google_literal_url_routing_v1'


def prepared(queue, geometry, count=16):
    with queue.db:
        queue.add_articles([alias(f'https://publisher.test/{i}') for i in range(count)])
    while routing.backfill_original_urls(queue)['examined']:
        pass
    routing.initialize_original_urls(queue)
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (GEOMETRY, geometry))
        queue.db.execute('UPDATE original_url_resolutions SET target_queued=0')


class ProjectionCost:
    """Deterministic cold-row cost without slow sleeps or timing assertions."""
    def __init__(self, *, stop=None):
        self.now, self.materialized, self.stop = 100., [], stop

    def monotonic(self):
        return self.now

    def row(self, cursor, values):
        row = sqlite3.Row(cursor, values)
        if {'alias_id', 'metadata_date_raw'}.issubset(row.keys()):
            self.now += .002
            self.materialized.append(row['alias_id'])
            if self.stop is not None:
                self.stop.set()
        return row


@pytest.mark.parametrize('geometry', ['hosts', 'global'])
def test_budget_materializes_only_proofs_that_can_be_consumed(queue, monkeypatch, geometry):
    prepared(queue, geometry)
    cost = ProjectionCost()
    queue.db.row_factory = cost.row
    monkeypatch.setattr(routing, 'time', SimpleNamespace(monotonic=cost.monotonic))
    examined = routing.sync_original_url_routes(queue, limit=16, max_seconds=.004)
    assert 1 <= examined <= 3
    assert len(cost.materialized) == examined  # Old fetchall materialized all16 then consumed1.
    expected = cost.materialized[-1]
    if geometry == 'hosts':
        assert queue.db.execute('SELECT after FROM ' + TABLE).fetchone()[0] == expected
    else:
        assert json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0])['after'] == expected.hex()
    assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == examined


@pytest.mark.parametrize('geometry', ['hosts', 'global'])
def test_stop_during_projection_does_not_drain_or_advance_unconsumed_rows(queue, geometry):
    prepared(queue, geometry)
    before = queue.db.execute('SELECT after,complete FROM ' + TABLE).fetchone()[:]
    stop = threading.Event()
    cost = ProjectionCost(stop=stop)
    queue.db.row_factory = cost.row
    assert routing.sync_original_url_routes(queue, limit=16, max_seconds=.1, stop=stop) == 0
    assert len(cost.materialized) == 1
    assert queue.db.execute('SELECT after,complete FROM ' + TABLE).fetchone()[:] == before
    assert json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0])['after'] != 'done'
    assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 0


class CursorWatch:
    def __init__(self, cursor):
        self.cursor, self.closed = cursor, False

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def close(self):
        self.closed = True
        self.cursor.close()


class DatabaseWatch:
    def __init__(self, db):
        self.db, self.projections = db, []

    def __getattr__(self, name):
        return getattr(self.db, name)

    def execute(self, sql, *args):
        cursor = self.db.execute(sql, *args)
        if sql.startswith('SELECT r.*,a.title,a.metadata_date_raw'):
            cursor = CursorWatch(cursor)
            self.projections.append(cursor)
        return cursor

    def __enter__(self):
        self.db.__enter__()
        return self

    def __exit__(self, *args):
        return self.db.__exit__(*args)


@pytest.mark.parametrize('geometry', ['hosts', 'global'])
@pytest.mark.parametrize('failure', [False, True])
def test_projection_cursor_closes_before_commit_or_rollback(queue, monkeypatch, geometry, failure):
    prepared(queue, geometry)
    watch = DatabaseWatch(queue.db)
    queue.db = watch
    before = [tuple(r) for r in queue.db.execute('SELECT * FROM original_url_resolutions ORDER BY alias_id')]
    before_hosts = [tuple(r) for r in queue.db.execute('SELECT * FROM ' + TABLE)]
    if failure:
        original = queue.add_articles
        def broken(rows, **kwargs):
            original(rows, **kwargs)
            raise OSError('admission storage failure')
        monkeypatch.setattr(queue, 'add_articles', broken)
        with pytest.raises(OSError, match='admission storage failure'):
            routing.sync_original_url_routes(queue, limit=16, max_seconds=.1)
        assert before == [tuple(r) for r in queue.db.execute('SELECT * FROM original_url_resolutions ORDER BY alias_id')]
        assert before_hosts == [tuple(r) for r in queue.db.execute('SELECT * FROM ' + TABLE)]
    else:
        assert routing.sync_original_url_routes(queue, limit=16, max_seconds=.1) == 16
    assert watch.projections and all(c.closed for c in watch.projections)
    assert not queue.db.in_transaction


@pytest.mark.parametrize('geometry', ['hosts', 'global'])
@pytest.mark.parametrize('limit', [1, 2, 16])
def test_exact_limit_and_empty_tail_need_exhaustion_not_the_last_materialized_row(queue, geometry, limit):
    prepared(queue, geometry, count=4)
    examined = 0
    for _ in range(20):
        examined += routing.sync_original_url_routes(queue, limit=limit, max_seconds=.1)
        if json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0])['after'] == 'done':
            break
    else:
        pytest.fail('streaming route did not finish')
    assert examined == 4
    assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 4
    assert queue.db.execute('SELECT sum(complete) FROM ' + TABLE).fetchone()[0] == 1
