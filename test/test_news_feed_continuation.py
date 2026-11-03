"""A per-item refusal cannot strand permitted RSS/WordPress history.

No mixed raw/body acceptance, no HTTP deadline reset, and no invented totals.
"""
import json
import time
from datetime import datetime, timezone

import pytest

from downloader.news_fulltext import accept_discovery, discovery_fingerprint, fail_discovery, fetch_discovery, seed_discovery
from downloader.news_fulltext_feed import feed_records, feed_url
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import bulk_fixture, bulk_posts, config
from test_news_provider_efficiency import feed_fixture, feed_item, rss


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
@pytest.mark.parametrize('observation', ['older', 'newer', 'missing', 'in_flight'])
def test_current_bulk_page_cannot_overwrite_newer_article_metadata(config, tmp_path, kind, observation):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        raw = Response(task['url'], 200, {}, rss([feed_item(1), feed_item(2)]))
        records, errors = feed_records(raw, http, http.registry.sources['publisher'])
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=2)
        raw, _, records, errors = fetch_discovery(http, task)
    try:
        article_url = records[0]['requested_url']
        with queue.db:
            queue.add_articles([{'url': article_url, 'title': 'independent article title'}])
            queue.db.execute('UPDATE articles SET last_seen_utc=? WHERE url=?',
                             ('2000-01-01T00:00:00+00:00' if observation == 'newer'
                              else '2026-01-02T00:00:00+00:00' if observation == 'in_flight'
                              else '2099-01-01T00:00:00+00:00', article_url))
        if observation != 'missing':
            task['_discovery_request_started_at_utc'] = '2026-01-01T00:00:00+00:00'
            task['_discovery_response_observed_at_utc'] = '2026-01-03T00:00:00+00:00'
        else:
            task.pop('_discovery_request_started_at_utc', None)
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        accept_discovery(queue, task, raw, links, records, errors)
        article = queue.db.execute('SELECT title,last_seen_utc FROM articles WHERE url=?', (article_url,)).fetchone()
        assert article['title'] == (records[0]['title'] if observation == 'newer' else 'independent article title')
        if observation != 'newer':
            assert article['last_seen_utc'] == ('2026-01-02T00:00:00+00:00' if observation == 'in_flight'
                                                else '2099-01-01T00:00:00+00:00')
        assert queue.db.execute('SELECT 1 FROM articles WHERE url=?', (records[1]['requested_url'],)).fetchone()
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) != before
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()
        http.close()


def test_live_rss_dispatch_records_observation_before_queue_commit(config, tmp_path, monkeypatch):
    from contextlib import closing
    import downloader.news_fulltext as module
    from test_news_fulltext import FakeResponse, http_fixture

    url = 'https://publisher.test/feed/'
    config['providers'][0]['fulltext_feeds'] = [url]
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(rss([feed_item(1), feed_item(2)])),
    })
    monkeypatch.setattr(module, 'seed_discovery', lambda _: None)
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'rss_fulltext','publisher')", (url,))
        original = module.accept_discovery
        observed = {}
        def article_advances_after_fetch(queue, task, response, links, records, errors):
            observed['start'] = task.get('_discovery_request_started_at_utc')
            observed['end'] = task.get('_discovery_response_observed_at_utc')
            article_url = records[0]['requested_url']
            with queue.db:
                queue.add_articles([{'url': article_url, 'title': 'independent article title'}])
                queue.db.execute("UPDATE articles SET last_seen_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (article_url,))
            return original(queue, task, response, links, records, errors)
        monkeypatch.setattr(module, 'accept_discovery', article_advances_after_fetch)
        module.run_cycle(queue, http, config, max_articles=0, max_discovery_requests=1)
        assert datetime.fromisoformat(observed['start']).utcoffset() is not None
        assert datetime.fromisoformat(observed['start']) <= datetime.fromisoformat(observed['end'])
        assert [called for called, _ in session.calls] == ['https://publisher.test/robots.txt', url]
        assert queue.db.execute("SELECT title FROM articles WHERE url='https://publisher.test/story/1'").fetchone()[0] == 'independent article title'
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert verify_bodies(tmp_path)['accepted']


def test_backwards_local_clock_cannot_authorize_article_overwrite(config, tmp_path, monkeypatch):
    from contextlib import closing
    import downloader.news_fulltext as module
    from test_news_fulltext import FakeResponse, http_fixture

    url = 'https://publisher.test/feed/'
    config['providers'][0]['fulltext_feeds'] = [url]
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(rss([feed_item(1)])),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'rss_fulltext','publisher')", (url,))
            queue.add_articles([{'url': 'https://publisher.test/story/1', 'title': 'existing title'}])
            queue.db.execute("UPDATE articles SET last_seen_utc='2000-01-01T00:00:00+00:00'")
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        clocks = iter(('2026-01-02T00:00:00+00:00', '2026-01-01T00:00:00+00:00'))
        monkeypatch.setattr(module, 'utc_now', lambda: next(clocks))
        result = module.fetch_discovery(http, task)
        assert task['_discovery_request_started_at_utc'] is None
        assert task['_discovery_response_observed_at_utc'] == '2026-01-01T00:00:00+00:00'
        # Restore a normal clock for acceptance; the request-start witness
        # remains absent and cannot update the existing article title.
        from downloader.news_fulltext_queue import utc_now
        monkeypatch.setattr(module, 'utc_now', utc_now)
        module.accept_discovery(queue, task, *result)
        assert queue.db.execute("SELECT title FROM articles WHERE url='https://publisher.test/story/1'").fetchone()[0] == 'existing title'


@pytest.mark.parametrize('clock', [None, 'not-a-clock', '2026-01-01T00:00:00'])
def test_unordered_discovery_clock_keeps_existing_article_but_registers_new(config, tmp_path, clock):
    from downloader.news_fulltext import _article_links_started_after_heads

    http, queue = feed_fixture(config, tmp_path)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/story/1', 'title': 'existing'}])
        with queue.db:
            queue.db.execute('BEGIN IMMEDIATE')
            result = _article_links_started_after_heads(queue, [
                ('https://publisher.test:443/story/1#fragment', 'old title', ''),
                ('https://publisher.test/story/2', 'new identity', '')], clock)
        assert result == [('https://publisher.test/story/2', 'new identity', '')]
    finally:
        queue.close()
        http.close()


def test_current_article_head_lookup_normalizes_and_batches(config, tmp_path):
    from downloader.news_fulltext import _article_links_started_after_heads

    http, queue = feed_fixture(config, tmp_path)
    try:
        with queue.db:
            queue.add_articles([{'url': f'https://publisher.test/story/{i}'} for i in (1, 501, 1101)])
            queue.db.execute("UPDATE articles SET last_seen_utc='2099-01-01T00:00:00+00:00' WHERE url IN (?,?)",
                             ('https://publisher.test/story/1', 'https://publisher.test/story/501'))
            queue.db.execute("UPDATE articles SET last_seen_utc='2000-01-01T00:00:00+00:00' WHERE url=?",
                             ('https://publisher.test/story/1101',))
        links = [(f'https://publisher.test/story/{i}', f'Story {i}', '') for i in range(1, 1202)]
        links.append(('https://publisher.test:443/story/1#fragment', 'duplicate spelling', ''))
        queries = []
        queue.db.set_trace_callback(lambda query: queries.append(query) if query.startswith('SELECT url,last_seen_utc,') else None)
        with queue.db:
            queue.db.execute('BEGIN IMMEDIATE')
            result = _article_links_started_after_heads(queue, links, '2026-01-01T00:00:00+00:00')
        queue.db.set_trace_callback(None)
        assert len(result) == 1199
        assert ('https://publisher.test/story/1101', 'Story 1101', '') in result
        assert all(link[0] not in {'https://publisher.test/story/1', 'https://publisher.test/story/501',
                                   'https://publisher.test:443/story/1#fragment'} for link in result)
        assert len(queries) == 3
    finally:
        queue.close()
        http.close()


def test_current_article_head_requires_all_clocks_to_be_orderable(config, tmp_path):
    from downloader.news_fulltext import _article_links_started_after_heads

    http, queue = feed_fixture(config, tmp_path)
    try:
        urls = [f'https://publisher.test/story/{i}' for i in range(1, 7)]
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])
            queue.db.execute("UPDATE articles SET last_seen_utc='2000-01-01T00:00:00+00:00'")
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (urls[0],))
            queue.db.execute("UPDATE articles SET last_checked_at_utc='not-a-clock' WHERE url=?", (urls[1],))
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2025-01-01T00:00:00' WHERE url=?", (urls[2],))
            queue.db.execute("UPDATE articles SET body_sha256=?,captured_at_utc=NULL WHERE url=?", ('a' * 64, urls[3]))
            queue.db.execute("UPDATE articles SET captured_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (urls[4],))
        links = [(url, 'incoming', '') for url in urls] + [('https://publisher.test/story/new', 'new', '')]
        with queue.db:
            queue.db.execute('BEGIN IMMEDIATE')
            result = _article_links_started_after_heads(queue, links, '2026-01-01T00:00:00+00:00')
        assert result == [(urls[5], 'incoming', ''), ('https://publisher.test/story/new', 'new', '')]
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('same_body', [False, True])
def test_inflight_wordpress_response_keeps_newer_body_and_retry(config, tmp_path, same_body):
    from downloader.news_fulltext_bulk import wordpress_records

    posts = bulk_posts(1)
    queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=1, posts=posts)
    try:
        first = fetch_discovery(http, task)
        assert accept_discovery(queue, task, *first) == 1
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        task['_discovery_request_started_at_utc'] = '2026-01-01T00:00:00+00:00'
        task['_discovery_response_observed_at_utc'] = '2026-01-03T00:00:00+00:00'
        article_url = posts[0]['link']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',attempts=3,due=0,"
                             "captured_at_utc='2026-01-02T00:00:00+00:00',"
                             "last_checked_at_utc='2026-01-02T00:00:00+00:00' WHERE url=?", (article_url,))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (article_url,)).fetchone())
        if not same_body:
            posts[0]['content']['rendered'] = '<p>Earlier source representation. ' + 'old ' * 50 + '</p>'
        raw = Response(task['url'], 200, {'content-type': 'application/json', 'x-wp-total': '1',
                                          'x-wp-totalpages': '1'}, json.dumps(posts).encode())
        records, errors = wordpress_records(raw, http, http.registry.sources['publisher'])
        records[0]['captured_at_utc'] = '2026-01-03T00:00:00+00:00'
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        accept_discovery(queue, task, raw, links, records, errors)
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (article_url,)).fetchone())
        for key in ('body_sha256', 'parquet_path', 'captured_at_utc', 'state', 'error_code', 'attempts', 'last_checked_at_utc'):
            assert after[key] == before[key]
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
@pytest.mark.parametrize('status', [200, 304])
def test_stale_bulk_response_keeps_newer_source_and_article_heads(config, tmp_path, kind, status):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        raw = Response(task['url'], 200, {}, rss([feed_item(1), feed_item(2)]))
        records, errors = feed_records(raw, http, http.registry.sources['publisher'])
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=2)
        raw, _, records, errors = fetch_discovery(http, task)
    try:
        article_url = records[0]['requested_url']
        links = [(record['requested_url'], record['title'], record['metadata_date_raw']) for record in records]
        with queue.db:
            queue.add_articles([{'url': article_url, 'title': 'newer title'}])
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',attempts=3,"
                             "last_checked_at_utc='2099-01-01T00:00:00+00:00',"
                             "last_seen_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (article_url,))
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=5,due=?,"
                             "last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?",
                             (time.time() + 7200, task['url']))
            queue.db.execute('INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)',
                             (task['url'], 0, 'publisher', article_url, 'newer_item_error', '2099-01-01T00:00:00+00:00'))
        before_source = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        before_article = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (article_url,)).fetchone())
        before_errors = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_item_errors WHERE request_url=?', (task['url'],))]
        if status == 304:
            raw, links, records, errors = Response(task['url'], 304, {}, b''), [], [], []
        assert accept_discovery(queue, task, raw, links, records, errors) == (2 if status == 200 else 0)
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before_source
        assert [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_item_errors WHERE request_url=?', (task['url'],))] == before_errors
        current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (article_url,)).fetchone())
        for key in ('title', 'last_seen_utc', 'state', 'error_code', 'attempts', 'last_checked_at_utc'):
            assert current[key] == before_article[key]
        assert queue.db.execute('SELECT count(*) FROM bulk_api_coverage').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_page_evidence').fetchone()[0] == 0
        if status == 200:
            assert current['body_sha256'] == records[0]['body_sha256']
            assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
def test_stale_mixed_rights_page_does_not_persist_protected_text_or_overwrite_state(config, tmp_path, kind):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        raw = Response(task['url'], 200, {}, rss([feed_item(1), feed_item(2, rights='All rights reserved'), feed_item(3)]))
        records, errors = feed_records(raw, http, http.registry.sources['publisher'])
    else:
        posts = bulk_posts(3)
        posts[1]['content']['protected'] = True
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=3, posts=posts)
        raw, _, records, errors = fetch_discovery(http, task)
    try:
        links = [(record['requested_url'], record['title'], record['metadata_date_raw']) for record in records]
        article_url = records[0]['requested_url']
        with queue.db:
            queue.add_articles([{'url': article_url, 'title': 'newer title'}])
            queue.db.execute("UPDATE articles SET last_seen_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (article_url,))
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=5,due=?,"
                             "last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?",
                             (time.time() + 7200, task['url']))
        before_source = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        before_article = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (article_url,)).fetchone())
        assert accept_discovery(queue, task, raw, links, records, errors) == 0
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before_source
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (article_url,)).fetchone()) == before_article
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0] == 0
        assert not list(tmp_path.glob('**/*.parquet'))
    finally:
        queue.close()
        http.close()


def test_live_rss_failure_does_not_overwrite_newer_source_state(config, tmp_path, monkeypatch):
    from contextlib import closing
    import downloader.news_fulltext as module
    from test_news_fulltext import FakeResponse, http_fixture

    url = 'https://publisher.test/feed/'
    config['providers'][0]['fulltext_feeds'] = [url]
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(status=500),
    })
    monkeypatch.setattr(module, 'seed_discovery', lambda _: None)
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'rss_fulltext','publisher')", (url,))
        original = module.fail_discovery
        observed = {}
        def newer_head_before_failure(queue, task, error, **kwargs):
            with queue.db:
                queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=5,due=?,"
                                 "last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?",
                                 (time.time() + 7200, url))
                observed['head'] = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
            return original(queue, task, error, **kwargs)
        monkeypatch.setattr(module, 'fail_discovery', newer_head_before_failure)
        module.run_cycle(queue, http, config, max_articles=0, max_discovery_requests=1)
        assert [called for called, _ in session.calls] == ['https://publisher.test/robots.txt', url]
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == observed['head']
        assert queue.db.execute("SELECT value FROM counters WHERE key='discovery_failure_superseded'").fetchone()[0] == 1


def test_stale_discovery_missing_links_are_normalized_and_batched(config, tmp_path):
    from downloader.news_fulltext import _missing_article_links

    http, queue = feed_fixture(config, tmp_path)
    try:
        with queue.db:
            queue.add_articles([{'url': f'https://publisher.test/story/{i}'} for i in (1, 501, 1101)])
        links = [(f'https://publisher.test/story/{i}', f'Story {i}', '') for i in range(1, 1202)]
        links.append(('https://publisher.test:443/story/1#fragment', 'duplicate spelling', ''))
        queries = []
        queue.db.set_trace_callback(lambda query: queries.append(query) if query.startswith('SELECT url FROM articles WHERE url IN') else None)
        with queue.db:
            queue.db.execute('BEGIN IMMEDIATE')
            pending = _missing_article_links(queue, links)
        queue.db.set_trace_callback(None)
        assert len(pending) == 1198
        assert all(link[0] not in {'https://publisher.test/story/1', 'https://publisher.test/story/501',
                                   'https://publisher.test/story/1101', 'https://publisher.test:443/story/1#fragment'}
                   for link in pending)
        assert len(queries) == 3
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('all_restricted', [False, True])
def test_rss_mixed_page_continues_without_saving_any_envelope_or_body(config, tmp_path, all_restricted):
    http, queue = feed_fixture(config, tmp_path)
    source = http.registry.sources['publisher']
    url = feed_url(source.fulltext_feeds[0], 46)
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'rss_fulltext','publisher')", (url,))
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        response = Response(url, 200, {}, rss([
            feed_item(1, rights='All rights reserved' if all_restricted else ''),
            feed_item(2, rights='All rights reserved'),
        ]))
        records, errors = feed_records(response, http, source)
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        assert accept_discovery(queue, task, response, links, records, errors) == 0
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == int(not all_restricted)
        assert not list(tmp_path.glob('**/*.parquet'))
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_interrupted_page_payloads').fetchone()[0] == 0
        assert queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed_url(source.fulltext_feeds[0], 47),)).fetchone()
        checkpoint = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        assert checkpoint['error_code'] == 'bulk_response_contains_restricted_text'
        assert checkpoint['last_checked_at_utc'] and checkpoint['parser_fingerprint'] == discovery_fingerprint(source)
        assert checkpoint['response_sha256'] and 86390 < checkpoint['due'] - time.time() <= 86400
        assert queue.db.execute('SELECT total FROM bulk_page_evidence').fetchone()[0] == -1
        assert queue.db.execute('SELECT count(*) FROM bulk_api_coverage').fetchone()[0] == 0
        # A gate while this page waits for revalidation is not a new source
        # observation. In particular it must not erase the mixed-rights
        # classification used by a subsequent conditional 304.
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (url,))
        ready = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        fail_discovery(queue, ready, NewsError('stopping', retry=True, delay=1, request_deferred=True))
        deferred = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        assert deferred['error_code'] == checkpoint['error_code']
        assert deferred['last_checked_at_utc'] == checkpoint['last_checked_at_utc']
        assert accept_discovery(queue, deferred, Response(url, 304, {}, b''), [], [], []) == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0] == (2 if all_restricted else 1)
        assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == checkpoint['error_code']
        # The next genuinely different page commits its own free raw response,
        # not the previous mixed one, and recovers idempotently after restart.
        next_url = feed_url(source.fulltext_feeds[0], 47)
        next_task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (next_url,)).fetchone())
        free_response = Response(next_url, 200, {}, rss([feed_item(3), feed_item(4)]))
        free_records, free_errors = feed_records(free_response, http, source)
        free_links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in free_records]
        assert accept_discovery(queue, next_task, free_response, free_links, free_records, free_errors) == 2
        assert verify_bodies(tmp_path)['accepted']
        assert accept_discovery(queue, next_task, free_response, free_links, free_records, free_errors) == 0
    finally:
        queue.close()
        http.close()
    resumed = NewsQueue(tmp_path, http.registry)
    try:
        assert verify_bodies(tmp_path)['body_versions_verified'] == 2
        assert resumed.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed_url(source.fulltext_feeds[0], 48),)).fetchone()
    finally:
        resumed.close()


@pytest.mark.parametrize('error', ['source_access_denied', 'source_rate_limited', 'invalid_feed', 'bulk_all_items_unusable'])
def test_discovery_failures_record_actual_check_without_replacing_source_digest(config, tmp_path, error):
    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute('SELECT * FROM discovery').fetchone())
        with queue.db:
            queue.db.execute("UPDATE discovery SET last_checked_at_utc='2026-01-01T00:00:00+00:00',response_sha256=?,etag='original'",
                             ('c' * 64,))
        before = datetime.now(timezone.utc)
        fail_discovery(queue, task, NewsError(error, retry=True, delay=120))
        row = queue.db.execute('SELECT error_code,last_checked_at_utc,response_sha256,etag,due FROM discovery').fetchone()
        assert row['error_code'] == error and datetime.fromisoformat(row['last_checked_at_utc']) >= before
        assert row['response_sha256'] == 'c' * 64 and row['etag'] == 'original' and row['due'] > time.time()
        assert queue.status()['discovery_failure_observation_contract_version'] == 1
    finally:
        queue.close()
        http.close()


def test_restricted_rss_does_not_relax_repeating_page_identity_validation(config, tmp_path):
    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute('SELECT * FROM discovery').fetchone())
        body = rss([feed_item(1), feed_item(2, rights='All rights reserved')])
        first = Response(task['url'], 200, {}, body)
        records, errors = feed_records(first, http, http.registry.sources['publisher'])
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        accept_discovery(queue, task, first, links, records, errors)
        next_url = feed_url(task['url'], 2)
        next_task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (next_url,)).fetchone())
        with pytest.raises(NewsError, match='bulk_pagination_no_progress'):
            accept_discovery(queue, next_task, Response(next_url, 200, {}, body), links, records, errors)
        assert not queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed_url(task['url'], 3),)).fetchone()
        assert not list(tmp_path.glob('**/*.parquet'))
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('restriction', ['protected', 'rights'])
def test_wordpress_mixed_history_advances_and_never_retains_protected_raw(config, tmp_path, restriction):
    posts = bulk_posts(100)
    if restriction == 'protected':
        posts[0]['content']['protected'] = True
    else:
        posts[0]['meta'] = {'license': 'all-rights-reserved'}
    queue, http, _, task, _ = bulk_fixture(config, tmp_path, posts=posts, count=100, total=101)
    try:
        assert accept_discovery(queue, task, *fetch_discovery(http, task)) == 0
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 99
        assert not list(tmp_path.glob('**/*.parquet'))
        assert queue.db.execute("SELECT count(*) FROM discovery WHERE kind='wordpress_posts'").fetchone()[0] == 2
        assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (task['url'],)).fetchone()[0] == 'bulk_response_contains_restricted_text'
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_interrupted_page_payloads').fetchone()[0] == 0
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('finite', [False, True])
def test_mixed_page_recovery_is_once_scoped_and_does_not_reset_refusals(config, tmp_path, finite):
    entry = config['providers'][0]
    entry['fulltext_feeds'] = ['https://publisher.test/feed/']
    if finite:
        entry['acquisition_mode'] = 'finite'
    registry = PolicyRegistry(config, base=tmp_path)
    source_fingerprint = registry.sources['publisher'].data_fingerprint
    queue = NewsQueue(tmp_path, registry)
    future = time.time() + 86400
    jobs = [('https://publisher.test/feed/?paged=46', 'bulk_response_contains_restricted_text', None),
            ('https://publisher.test/feed/?paged=47', 'source_access_denied', None),
            ('https://publisher.test/feed/?paged=48', 'source_rate_limited', None),
            ('https://publisher.test/undeclared-feed/?paged=49', 'bulk_response_contains_restricted_text', None),
            ('https://publisher.test/feed/?paged=50', 'bulk_response_contains_restricted_text', discovery_fingerprint(registry.sources['publisher']))]
    try:
        with queue.db:
            queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'mixed_bulk_scope_continuation:%'")
            queue.db.executemany("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,parser_fingerprint) "
                "VALUES (?,'rss_fulltext','publisher',?,?,2,'old',?)", [(url, future, error, fp) for url, error, fp in jobs])
            queue.add_articles([{'url': 'https://publisher.test/story/1'}])
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login'")
        queue.reconcile_policy()
        row = queue.db.execute('SELECT due,error_code,attempts,etag FROM discovery WHERE url=?', (jobs[0][0],)).fetchone()
        assert row[:] == ((future, 'bulk_response_contains_restricted_text', 2, 'old') if finite else
                          (0, 'bulk_response_contains_restricted_text', 2, None))
        for url, _, _ in jobs[1:]:
            assert queue.db.execute('SELECT due,etag FROM discovery WHERE url=?', (url,)).fetchone()[:] == (future, 'old')
        assert registry.sources['publisher'].data_fingerprint == source_fingerprint
        assert queue.db.execute('SELECT state,error_code FROM articles').fetchone()[:] == ('paywall_or_login', 'paywall_or_login')
        if not finite:
            audit = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'mixed_bulk_scope_continuation:%'").fetchone()[0])
            assert len(audit['jobs']) == 1 and audit['jobs'][0]['due'] == future
            assert not audit['http_refusals_requeued'] and not audit['source_capture_contract_changed']
            with queue.db:
                queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (future, jobs[0][0]))
            queue.reconcile_policy()
            assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (jobs[0][0],)).fetchone()[0] == future
    finally:
        queue.close()


@pytest.mark.parametrize('page', ['bad', '0', '-1', '100001'])
def test_malformed_wordpress_checkpoints_do_not_break_owner_recovery(config, tmp_path, page):
    base = 'https://publisher.test/wp-json/wp/v2/posts'
    config['providers'][0]['wordpress_apis'] = [base]
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    future = time.time() + 86400
    try:
        with queue.db:
            queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'mixed_bulk_scope_continuation:%'")
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code) "
                "VALUES (?,'wordpress_posts','publisher',?,'bulk_response_contains_restricted_text')",
                (base + '?page=' + page, future))
        queue.reconcile_policy()
        assert queue.db.execute('SELECT due FROM discovery').fetchone()[0] == future
    finally:
        queue.close()
