"""Stored failures get bounded service inside, never ahead of, their work share."""
import time

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config


def seed(queue, provider='publisher', host='publisher.test', *, missing=0, fresh_error=None):
    base = f'https://{host}'
    with queue.db:
        queue.add_articles([{'url': base + '/fresh', 'date': '2026-10-11'},
                            {'url': base + '/old', 'date': '2014-01-01'}]
                           + [{'url': base + f'/missing/{i}', 'date': '2013-01-01'} for i in range(missing)])
        queue.db.execute("UPDATE articles SET state='retry',body_sha256=?,body_bytes=24,"
                         "error_code=?,due=? WHERE url=?", ('a' * 64, fresh_error,
                             time.time() - 100 if fresh_error else 0, base + '/fresh'))
        queue.db.execute("UPDATE articles SET state='retry',body_sha256=?,body_bytes=24,"
                         "error_code='transport_error',attempts=3,due=?,last_checked_at_utc=?,"
                         "captured_at_utc=? WHERE url=?", ('b' * 64, time.time() - 1800,
                             '2026-10-10T00:00:00+00:00', '2026-10-09T00:00:00+00:00', base + '/old'))
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (base + '/old',)).fetchone())


def ready_again(queue, row):
    # Fixture for another ready ordinary refresh, never a production reset.
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry',lease_owner=NULL,lease_until=NULL WHERE id=?", (row['id'],))


@pytest.mark.parametrize('missing_burst', [1, 2, 8])
@pytest.mark.parametrize('stored_burst', [1, 2, 8])
def test_continuous_fresh_refresh_cannot_starve_old_stored_failure_or_borrow_missing_share(config, tmp_path, missing_burst, stored_burst):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        original = seed(queue, missing=200)
        bound = (missing_burst + 1) * (stored_burst + 1)
        selected = []
        for _ in range(bound):
            row = queue.claim('publisher', 300, missing_body_burst=missing_burst,
                              stored_body_retry_burst=stored_burst)
            selected.append(row)
            if row['url'].endswith('/fresh'):
                ready_again(queue, row)
        assert selected[-1]['url'].endswith('/old')
        assert all(row['body_sha256'] is None for i, row in enumerate(selected) if (i + 1) % (missing_burst + 1))
        assert sum(row['body_sha256'] is not None for row in selected) == stored_burst + 1
        current = dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (original['id'],)).fetchone())
        for key in ('due', 'error_code', 'body_sha256', 'captured_at_utc', 'last_checked_at_utc', 'metadata_date_raw'):
            assert current[key] == original[key]
        assert current['attempts'] == 4 and current['lease_owner'] == queue.owner
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['frontier_oldest_due_stored_retry_dispatches'] == 1
        assert counts['frontier_missing_body_dispatches'] == bound - stored_burst - 1
    finally:
        queue.close()


@pytest.mark.parametrize('fresh_error', [None, 'transport_error', 'body_not_found'])
def test_regular_stored_only_work_and_failures_do_not_reset_oldest_credit(config, tmp_path, fresh_error):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        seed(queue, fresh_error=fresh_error)
        rows = []
        for _ in range(9):
            row = queue.claim('publisher', 300)
            rows.append(row['url'])
            if row['url'].endswith('/fresh'):
                ready_again(queue, row)
        assert all(url.endswith('/fresh') for url in rows[:8])
        assert rows[8].endswith('/old')
    finally:
        queue.close()


@pytest.mark.parametrize('excluded', ['future', 'lease', 'host', 'origin'])
def test_ineligible_oldest_failure_never_consumes_credit_or_idles_ordinary_refresh(config, tmp_path, excluded):
    config['providers'][0]['domains'].append('paused.test')
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        old = seed(queue)
        options = {}
        with queue.db:
            if excluded == 'future':
                queue.db.execute('UPDATE articles SET due=? WHERE id=?', (time.time() + 3600, old['id']))
            elif excluded == 'lease':
                queue.db.execute("UPDATE articles SET state='inflight',lease_owner='other',lease_until=? WHERE id=?", (time.time() + 3600, old['id']))
            else:
                queue.db.execute("UPDATE articles SET url='https://paused.test/old',host='paused.test' WHERE id=?", (old['id'],))
                options = {'excluded_hosts': ['paused.test']} if excluded == 'host' else {'excluded_origins': ['https://paused.test']}
        before = dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (old['id'],)).fetchone())
        for _ in range(12):
            row = queue.claim('publisher', 300, **options)
            assert row['url'].endswith('/fresh')
            ready_again(queue, row)
        assert dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (old['id'],)).fetchone()) == before
        assert queue._stored_retry_dispatch_bursts['publisher'] == 8
        with queue.db:
            if excluded == 'future':
                queue.db.execute('UPDATE articles SET due=0 WHERE id=?', (old['id'],))
            elif excluded == 'lease':
                queue.db.execute("UPDATE articles SET state='retry',lease_owner=NULL,lease_until=NULL WHERE id=?", (old['id'],))
        assert queue.claim('publisher', 300)['id'] == old['id']
    finally:
        queue.close()


def test_stored_retry_fairness_is_provider_local_and_sparse_index_ordered(config, tmp_path):
    config['providers'].append({**config['providers'][0], 'id': 'peer', 'domains': ['peer.test']})
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        old = seed(queue)
        seed(queue, 'peer', 'peer.test')
        for _ in range(8):
            row = queue.claim('publisher', 300)
            ready_again(queue, row)
        assert queue.claim('peer', 300)['url'].endswith('/fresh')
        assert queue.claim('publisher', 300)['id'] == old['id']
        plan = queue.db.execute('EXPLAIN QUERY PLAN ' + queue.stored_retry_dispatch_statement(), ('publisher', time.time())).fetchall()
        assert any('dispatch_stored_retry' in row[3] for row in plan)
        assert all('TEMP B-TREE' not in row[3] for row in plan)
        assert queue.db.execute("SELECT count(*) FROM articles INDEXED BY dispatch_stored_retry "
            "WHERE body_sha256 IS NOT NULL AND error_code IS NOT NULL AND state IN ('retry','retry_cooldown')").fetchone()[0] == 1
    finally:
        queue.close()


@pytest.mark.parametrize('state', ['retry', 'retry_cooldown'])
def test_oldest_due_order_is_not_newest_publication_order(config, tmp_path, state):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        old = seed(queue)
        with queue.db:
            queue.db.execute('UPDATE articles SET state=? WHERE id=?', (state, old['id']))
            queue.db.execute("UPDATE articles SET error_code='transport_error',due=? WHERE url LIKE '%/fresh'", (time.time() - 100,))
        queue._stored_retry_dispatch_bursts = {'publisher': 8}
        assert queue.claim('publisher', 300)['id'] == old['id']
    finally:
        queue.close()


@pytest.mark.parametrize('value', [True, False, 0, -1, 1.5, 129, '8'])
def test_config_and_direct_queue_reject_unsafe_stored_retry_bursts(config, tmp_path, value):
    queue = NewsQueue(tmp_path / 'queue', PolicyRegistry(config, base=tmp_path))
    try:
        with pytest.raises(ValueError, match='invalid stored-body retry burst'):
            queue.claim('publisher', 300, stored_body_retry_burst=value)
    finally:
        queue.close()
    config['stored_body_retry_burst'] = value
    path = tmp_path / 'config.json'
    atomic_write_json(path, config)
    with pytest.raises(ValueError):
        load_config(path, repo=tmp_path)


def test_fairness_knob_changes_capacity_identity_not_source_capture_or_policy(config, tmp_path):
    from downloader.news_fulltext_capacity import PublisherCapacity
    from downloader.news_fulltext_http import NewsHTTP
    config['providers'][0]['interval_seconds'] = 0.01
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        before = dict(queue.db.execute('SELECT key,value FROM queue_settings'))
        http = NewsHTTP(queue.registry, config, tmp_path)
        try:
            first = PublisherCapacity(http, config)
        finally:
            http.close()
        changed = {**config, 'stored_body_retry_burst': 2}
        http = NewsHTTP(queue.registry, changed, tmp_path)
        try:
            second = PublisherCapacity(http, changed)
        finally:
            http.close()
        assert first.fingerprint != second.fingerprint
        assert first.provider_fingerprints != second.provider_fingerprints
        assert dict(queue.db.execute('SELECT key,value FROM queue_settings')) == before
    finally:
        queue.close()
