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
        assert queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed_url(source.fulltext_feeds[0], 47),)).fetchone()
        checkpoint = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        assert checkpoint['error_code'] == 'bulk_response_contains_restricted_text'
        assert checkpoint['last_checked_at_utc'] and checkpoint['parser_fingerprint'] == discovery_fingerprint(source)
        assert checkpoint['response_sha256'] and 86390 < checkpoint['due'] - time.time() <= 86400
        assert queue.db.execute('SELECT total FROM bulk_page_evidence').fetchone()[0] == -1
        assert queue.db.execute('SELECT count(*) FROM bulk_api_coverage').fetchone()[0] == 0
        assert accept_discovery(queue, checkpoint, Response(url, 304, {}, b''), [], [], []) == 0
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
