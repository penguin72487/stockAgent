"""Metadata is useful without being relabelled as authorized article text."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import time

import pyarrow.parquet as pq
import pytest

from downloader.news_fulltext import accept_discovery, fetch_discovery, load_config, run_cycle, seed_discovery
from downloader.news_fulltext_capacity import PublisherCapacity
from downloader.news_fulltext_host_circuit import GatewayHostCircuit
from downloader.news_fulltext_http import NewsHTTP, Response
from downloader.news_fulltext_metadata_feeds import LTN_FEEDS, parse, require_request, progress
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import FakeLimiter, FakeResponse, FakeSession


@pytest.fixture
def config():
    cfg, registry, _ = load_config(Path('configs/news_fulltext.json'))
    cfg = deepcopy(cfg)
    cfg.pop('provider_catalog', None)
    cfg.pop('failure_diagnostic_focus', None)
    cfg['providers'] = [deepcopy(next(row for row in cfg['providers'] if row['id'] == 'ltn'))]
    cfg.update(exclusive_http_owner=False, min_free_disk_bytes=1)
    cfg['adaptive_rate']['enabled'] = False
    return cfg


def xml(*, date='Tue, 13 Oct 2026 20:00:00 +0800', extra='', link='https://news.ltn.com.tw/news/business/breakingnews/123'):
    return (f'<?xml version="1.0"?><rss version="2.0"><channel><title>自由時報</title><item>'
        f'<title>標題</title><description><![CDATA[<p>摘要，不是全文</p>]]></description>'
        f'<link>{link}</link><pubDate>{date}</pubDate>{extra}</item></channel></rss>').encode()


def setup(config, tmp_path, routes):
    registry = PolicyRegistry(config, base=tmp_path)
    source = registry.sources['ltn']
    # Test pacing without waiting for wall-clock seconds; production still uses
    # the source interval and persistent limiter.
    registry.sources['ltn'] = replace(source, interval_seconds=.01)
    queue = NewsQueue(tmp_path, registry)
    session = FakeSession(routes)
    http = NewsHTTP(registry, config, tmp_path, session_factory=lambda: session,
                    limiter_factory=FakeLimiter, address_check=lambda _: None, metrics=queue.metrics)
    seed_discovery(queue)
    return queue, http, session


def task(queue, url=LTN_FEEDS[0]):
    return dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())


def test_body_and_bulk_fingerprints_exactly_unchanged_by_metadata_scope():
    cfg, registry, _ = load_config(Path('configs/news_fulltext.json'))
    old = deepcopy(cfg)
    for entry in old['providers']:
        entry.pop('metadata_feed_policy', None)
    before = PolicyRegistry(old, base=Path.cwd())
    assert len(before.sources) == len(registry.sources)
    assert len(registry.sources['ltn'].metadata_feeds) == 14
    for provider, source in registry.sources.items():
        prior = before.sources[provider]
        assert (source.allowed, source.authorization_fingerprint, source.data_fingerprint,
                source.fulltext_feed_data_fingerprint, source.cnyes_bulk_data_fingerprint) == (
                prior.allowed, prior.authorization_fingerprint, prior.data_fingerprint,
                prior.fulltext_feed_data_fingerprint, prior.cnyes_bulk_data_fingerprint)


def test_metadata_only_restart_preserves_prior_host_pause_without_granting_body_access(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    metadata_only = registry.sources['ltn']
    assert not metadata_only.allowed and metadata_only.request_enabled

    # Model a formerly body-enabled owner and its actual negative checkpoint.
    # Both observations are synthetic: no publisher or production queue is used.
    previous_fingerprint = 'a' * 64 if metadata_only.data_fingerprint != 'a' * 64 else 'b' * 64
    registry.sources['ltn'] = replace(metadata_only, allowed=True, data_fingerprint=previous_fingerprint)
    previous = GatewayHostCircuit(tmp_path, registry)
    previous.success('ltn', 'video.ltn.com.tw')
    for _ in range(3):
        previous.gateway_failure('ltn', 'news.ltn.com.tw', 502, None)
    expected = previous.blocked('ltn')['news.ltn.com.tw']
    checkpoint = (tmp_path / 'gateway_host_cooldowns.json').read_bytes()

    registry.sources['ltn'] = metadata_only
    restored = GatewayHostCircuit(tmp_path, registry)
    assert restored.blocked('ltn') == {'news.ltn.com.tw': expected}
    assert restored.failures['ltn']['news.ltn.com.tw'] == 3
    assert restored.pause_evidence_fingerprints['ltn']['news.ltn.com.tw'] == previous_fingerprint
    assert (tmp_path / 'gateway_host_cooldowns.json').read_bytes() == checkpoint

    session = FakeSession({})
    http = NewsHTTP(registry, config, tmp_path, session_factory=lambda: session,
                    limiter_factory=FakeLimiter, address_check=lambda _: None)
    try:
        with pytest.raises(NewsError, match='host_gateway_cooldown') as deferred:
            http._raw(LTN_FEEDS[0], metadata_feed=LTN_FEEDS[0])
        assert deferred.value.raw_request_not_sent
        with pytest.raises(NewsError, match='awaiting_authorization'):
            http._raw(LTN_FEEDS[0])
        assert session.calls == []
    finally:
        http.close()

    registry.sources['ltn'] = replace(metadata_only, metadata_feeds=())
    assert not registry.sources['ltn'].request_enabled
    assert GatewayHostCircuit(tmp_path, registry).blocked('ltn') == {}


@pytest.mark.parametrize('key,value', [('enabled', 1), ('layout', 'other'), ('urls', ['https://news.ltn.com.tw/news/123']),
    ('urls', [LTN_FEEDS[0], LTN_FEEDS[0]]), ('urls', [LTN_FEEDS[0] + '?page=2']),
    ('interval_seconds', True), ('interval_seconds', 59), ('interval_seconds', 86401)])
def test_policy_requires_exact_reviewed_scope(config, tmp_path, key, value):
    config['providers'][0]['metadata_feed_policy'][key] = value
    with pytest.raises(ValueError, match='metadata feed'):
        PolicyRegistry(config, base=tmp_path)


@pytest.mark.parametrize('key,value', [('scope', 'all_news_fulltext'), ('kind', 'personal_research'),
    ('evidence_url', 'https://service.ltn.com.tw/copyright'), ('publisher_credit', ''), ('reviewed_on', '')])
def test_metadata_grant_not_inferred_from_purpose(config, tmp_path, key, value):
    config['providers'][0]['metadata_feed_policy']['authorization'][key] = value
    with pytest.raises(ValueError):
        PolicyRegistry(config, base=tmp_path)


@pytest.mark.parametrize('url', ['https://news.ltn.com.tw/news/123', 'https://news.ltn.com.tw/',
    'https://news.ltn.com.tw/rss/all.xml?page=2', 'https://news.ltn.com.tw/rss/notregistered.xml',
    'http://news.ltn.com.tw/rss/all.xml', 'https://ec.ltn.com.tw/rss/all.xml', 'https://evil.test/rss/all.xml'])
def test_scope_cannot_escape_to_articles_or_other_metadata(config, tmp_path, url):
    q, http, session = setup(config, tmp_path, {})
    try:
        with pytest.raises(NewsError):
            http.get(url, metadata_feed=LTN_FEEDS[0])
        with pytest.raises(NewsError, match='awaiting_authorization'):
            http.get(LTN_FEEDS[0])
        assert session.calls == []
        assert not q.registry.sources['ltn'].allowed
        assert q.claim('ltn', 300) is None
    finally:
        http.close(); q.close()


def test_scoped_rss_durable_acceptance_clocks_and_no_body(config, tmp_path):
    q, http, session = setup(config, tmp_path, {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        LTN_FEEDS[0]: FakeResponse(xml(), headers={'Content-Type': 'application/rss+xml', 'ETag': 'v1'})})
    try:
        before = time.time()
        result = fetch_discovery(http, task(q))
        assert accept_discovery(q, task(q), *result) == 0
        row = q.db.execute('SELECT * FROM articles').fetchone()
        assert row['state'] == 'awaiting_authorization' and row['body_sha256'] is None
        assert row['metadata_date_raw'] == 'Tue, 13 Oct 2026 20:00:00 +0800'
        head = q.db.execute('SELECT * FROM metadata_feed_heads').fetchone()
        receipt = json.loads((tmp_path / head['receipt_path']).read_bytes())
        raw = pq.read_table(tmp_path / receipt['parquet_path']).to_pylist()[0]
        assert raw['raw_response_body'] == xml()
        items = json.loads(raw['items_json'])
        assert items[0]['author'] is None and items[0]['publication_aware'] == '2026-10-13T20:00:00+08:00'
        assert items[0]['description_html'] == '<p>摘要，不是全文</p>'
        assert items[0]['publisher_credit'] == '自由時報／The Liberty Times'
        assert receipt['article_bodies_accepted'] == raw['article_bodies_accepted'] == 0
        assert receipt['sha256'] == hashlib.sha256((tmp_path / receipt['parquet_path']).read_bytes()).hexdigest()
        assert (tmp_path / receipt['parquet_path']).stat().st_mode & 0o777 == 0o600
        job = task(q)
        assert before + 300 <= job['due'] <= time.time() + 300
        assert job['parser_fingerprint'] == q.registry.sources['ltn'].metadata_feed_fingerprint
        assert len(session.calls) == 2
        metrics = q.metrics.snapshot()['providers']['ltn']['counts']
        assert metrics.get('durably_accepted_versions', 0) == 0
        assert metrics['metadata_feed_item_observations'] == 1
        status = q.status(activity='running')['providers']['ltn']
        assert status['automatic_collection_enabled'] and not status['automatic_fulltext_allowed']
        assert status['metadata_feed_progress']['verified_feeds'] == 1
        assert status['metadata_feed_progress']['finish_eta_utc'] is None
        saved = (tmp_path / receipt['parquet_path']).read_bytes()
        session.routes[LTN_FEEDS[0]] = FakeResponse(status=304)
        result = fetch_discovery(http, task(q))
        assert session.calls[-1][1]['headers']['If-None-Match'] == 'v1'
        assert accept_discovery(q, task(q), *result) == 0
        assert (tmp_path / receipt['parquet_path']).read_bytes() == saved
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 1
        # Revoking metadata retires only metadata dispatch, never deletes its
        # receipts or promotes the existing article identity to complete.
        q.registry.sources['ltn'] = replace(q.registry.sources['ltn'], metadata_feeds=())
        assert q.discovery_candidates(q.registry.sources['ltn'], 100) == []
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 1
    finally:
        http.close(); q.close()


@pytest.mark.parametrize('status', [200, 304])
def test_stale_metadata_feed_response_preserves_newer_heads_and_original(config, tmp_path, status):
    q, http, _ = setup(config, tmp_path, {})
    try:
        url = LTN_FEEDS[0]
        first = Response(url, 200, {'content-type': 'application/rss+xml', 'etag': 'old'}, xml())
        assert accept_discovery(q, task(q), first, [], [], []) == 0
        stale_task = task(q)
        article_url = 'https://news.ltn.com.tw/news/business/breakingnews/123'
        with q.db:
            q.db.execute("UPDATE discovery SET error_code='http_429',attempts=5,due=?,"
                         "last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (time.time() + 7200, url))
            q.db.execute("UPDATE articles SET title='較新的文章標題',last_seen_utc='2099-01-01T00:00:00+00:00' WHERE url=?",
                         (article_url,))
        before_job = task(q)
        before_head = dict(q.db.execute('SELECT * FROM metadata_feed_heads WHERE feed_url=?', (url,)).fetchone())
        before_article = dict(q.db.execute('SELECT * FROM articles WHERE url=?', (article_url,)).fetchone())
        if status == 200:
            new_item = (b'<item><title>Another</title><link>https://news.ltn.com.tw/news/business/breakingnews/124</link>'
                        b'<pubDate>Tue, 13 Oct 2026 20:00:00 +0800</pubDate><description>Summary</description></item>')
            body = xml().replace(b'</channel>', new_item + b'</channel>')
            response = Response(url, 200, {'content-type': 'application/rss+xml'}, body)
        else:
            response = Response(url, 304, {}, b'')
        assert accept_discovery(q, stale_task, response, [], [], []) == 0
        assert task(q) == before_job
        assert dict(q.db.execute('SELECT * FROM metadata_feed_heads WHERE feed_url=?', (url,)).fetchone()) == before_head
        assert dict(q.db.execute('SELECT * FROM articles WHERE url=?', (article_url,)).fetchone()) == before_article
        if status == 200:
            assert q.db.execute("SELECT state FROM articles WHERE url='https://news.ltn.com.tw/news/business/breakingnews/124'").fetchone()[0] == 'awaiting_authorization'
            digest = hashlib.sha256(response.body).hexdigest()
            observation = q.db.execute('SELECT receipt_path FROM metadata_feed_observations '
                                       'WHERE feed_url=? AND response_sha256=?', (url, digest)).fetchone()
            assert observation and (tmp_path / observation['receipt_path']).is_file()
    finally:
        http.close(); q.close()


@pytest.mark.parametrize('observation', ['older', 'newer', 'missing', 'in_flight'])
def test_current_metadata_feed_respects_independent_article_clock(config, tmp_path, observation):
    q, http, _ = setup(config, tmp_path, {})
    try:
        url = LTN_FEEDS[0]
        first = Response(url, 200, {'content-type': 'application/rss+xml'}, xml())
        accept_discovery(q, task(q), first, [], [], [])
        current = task(q)
        article_url = 'https://news.ltn.com.tw/news/business/breakingnews/123'
        with q.db:
            q.db.execute('UPDATE articles SET title=?,last_seen_utc=? WHERE url=?',
                         ('independent article title', '2000-01-01T00:00:00+00:00' if observation == 'newer'
                          else '2026-01-02T00:00:00+00:00' if observation == 'in_flight'
                          else '2099-01-01T00:00:00+00:00', article_url))
        if observation != 'missing':
            current['_discovery_request_started_at_utc'] = '2026-01-01T00:00:00+00:00'
            current['_discovery_response_observed_at_utc'] = '2026-01-03T00:00:00+00:00'
        new_item = (b'<item><title>Another</title><link>https://news.ltn.com.tw/news/business/breakingnews/124</link>'
                    b'<pubDate>Tue, 13 Oct 2026 20:00:00 +0800</pubDate><description>Summary</description></item>')
        raw = Response(url, 200, {'content-type': 'application/rss+xml'}, xml().replace(b'</channel>', new_item + b'</channel>'))
        accept_discovery(q, current, raw, [], [], [])
        article = q.db.execute('SELECT title,last_seen_utc FROM articles WHERE url=?', (article_url,)).fetchone()
        assert article['title'] == ('標題' if observation == 'newer' else 'independent article title')
        if observation != 'newer':
            assert article['last_seen_utc'] == ('2026-01-02T00:00:00+00:00' if observation == 'in_flight'
                                                else '2099-01-01T00:00:00+00:00')
        assert q.db.execute("SELECT 1 FROM articles WHERE url='https://news.ltn.com.tw/news/business/breakingnews/124'").fetchone()
    finally:
        http.close(); q.close()


def test_metadata_feed_head_and_schedule_rollback_together_then_reuse_original(config, tmp_path):
    import sqlite3

    q, http, _ = setup(config, tmp_path, {})
    try:
        url = LTN_FEEDS[0]
        first = Response(url, 200, {'content-type': 'application/rss+xml'}, xml())
        assert accept_discovery(q, task(q), first, [], [], []) == 0
        next_task = task(q)
        before_head = dict(q.db.execute('SELECT * FROM metadata_feed_heads WHERE feed_url=?', (url,)).fetchone())
        body = xml(link='https://news.ltn.com.tw/news/business/breakingnews/124')
        raw = Response(url, 200, {'content-type': 'application/rss+xml'}, body)
        digest = hashlib.sha256(body).hexdigest()
        with q.db:
            q.db.execute("CREATE TEMP TRIGGER reject_metadata_schedule BEFORE UPDATE ON discovery "
                         "BEGIN SELECT RAISE(ABORT,'simulated_schedule_write_failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='simulated_schedule_write_failure'):
            accept_discovery(q, next_task, raw, [], [], [])
        assert task(q) == next_task
        assert dict(q.db.execute('SELECT * FROM metadata_feed_heads WHERE feed_url=?', (url,)).fetchone()) == before_head
        assert not q.db.execute('SELECT 1 FROM metadata_feed_observations WHERE feed_url=? AND response_sha256=?', (url, digest)).fetchone()
        assert not q.db.execute("SELECT 1 FROM articles WHERE url='https://news.ltn.com.tw/news/business/breakingnews/124'").fetchone()
        receipts = list((tmp_path / 'metadata_feeds').rglob(digest + '.parquet.receipt.json'))
        assert len(receipts) == 1 and receipts[0].is_file()
        original_bytes = (tmp_path / json.loads(receipts[0].read_bytes())['parquet_path']).read_bytes()
        with q.db:
            q.db.execute('DROP TRIGGER reject_metadata_schedule')
        assert accept_discovery(q, next_task, raw, [], [], []) == 0
        observation = q.db.execute('SELECT receipt_path FROM metadata_feed_observations WHERE feed_url=? AND response_sha256=?',
                                   (url, digest)).fetchone()
        assert observation and tmp_path / observation['receipt_path'] == receipts[0]
        assert (tmp_path / json.loads(receipts[0].read_bytes())['parquet_path']).read_bytes() == original_bytes
        assert q.db.execute("SELECT state FROM articles WHERE url='https://news.ltn.com.tw/news/business/breakingnews/124'").fetchone()[0] == 'awaiting_authorization'
    finally:
        http.close(); q.close()


@pytest.mark.parametrize('body,mime,error', [(b'<html><p>login</p></html>', 'text/html', 'invalid_metadata_feed_representation'),
    (xml(extra='<content:encoded xmlns:content="x">full body</content:encoded>'), 'text/xml', 'unreviewed_metadata_feed_item_fields'),
    (xml(extra='<author>name</author>'), 'text/xml', 'unreviewed_metadata_feed_item_fields'),
    (xml(link='https://evil.test/story'), 'text/xml', 'metadata_feed_item_outside_publisher'),
    (b'<!DOCTYPE rss><rss version="2.0"><channel/></rss>', 'text/xml', 'invalid_metadata_feed_representation')])
def test_unreviewed_response_has_zero_body_and_no_raw_persistence(config, tmp_path, body, mime, error):
    q, http, session = setup(config, tmp_path, {})
    try:
        with pytest.raises(NewsError, match=error):
            accept_discovery(q, task(q), Response(LTN_FEEDS[0], 200, {'content-type': mime}, body), [], [], [])
        assert not (tmp_path / 'metadata_feeds').exists()
        assert q.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        http.close(); q.close()


@pytest.mark.parametrize('date', ['unknown', '13 Oct 2026 20:00:00'])
def test_unknown_clock_remains_raw_without_inventing_timezone(config, tmp_path, date):
    registry = PolicyRegistry(config, base=tmp_path)
    rows = parse(Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml(date=date)), registry.sources['ltn'], LTN_FEEDS[0])
    assert rows[0]['publication_raw'] == date and rows[0]['publication_aware'] is None


@pytest.mark.parametrize('layer', ['feed', 'robots'])
@pytest.mark.parametrize('status', [403, 429])
def test_source_refusal_preserves_shared_cooldown_without_reading_denied_body(config, tmp_path, layer, status):
    routes = {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        LTN_FEEDS[0]: FakeResponse(xml(), headers={'Content-Type': 'text/xml'})}
    denied = LTN_FEEDS[0] if layer == 'feed' else 'https://news.ltn.com.tw/robots.txt'
    routes[denied] = FakeResponse(b'denied secret body', status=status)
    q, http, session = setup(config, tmp_path, routes)
    try:
        with pytest.raises(NewsError):
            fetch_discovery(http, task(q))
        problems = list((tmp_path / 'access_problems/ltn').glob('*.json'))
        assert problems and all(json.loads(path.read_bytes())['body_read'] is False for path in problems)
        assert all(b'denied secret body' not in path.read_bytes() for path in problems)
        assert not (tmp_path / 'metadata_feeds').exists()
        assert http.provider_not_before.get('ltn', 0) > time.time()
    finally:
        http.close(); q.close()


def test_robots_disallow_and_redirect_scope_are_not_bypassed(config, tmp_path):
    q, http, session = setup(config, tmp_path, {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nDisallow: /rss/'),
        LTN_FEEDS[0]: FakeResponse(xml())})
    try:
        with pytest.raises(NewsError, match='robots_disallowed'):
            fetch_discovery(http, task(q))
        assert len(session.calls) == 1
    finally:
        http.close(); q.close()


@pytest.mark.parametrize('destination', ['https://news.ltn.com.tw/news/123', 'https://ec.ltn.com.tw/rss/all.xml', LTN_FEEDS[1]])
def test_redirect_does_not_expand_original_feed_capability(config, tmp_path, destination):
    q, http, session = setup(config, tmp_path, {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        LTN_FEEDS[0]: FakeResponse(status=302, headers={'Location': destination})})
    try:
        with pytest.raises(NewsError, match='request_outside_metadata_feed_scope'):
            fetch_discovery(http, task(q))
        assert len(session.calls) == 2
    finally:
        http.close(); q.close()


def test_metadata_permission_expiry_and_wrong_task_provider(config, tmp_path):
    q, http, session = setup(config, tmp_path, {})
    try:
        other = task(q); other['provider'] = 'cnyes'
        with pytest.raises(NewsError, match='provider_mismatch'):
            fetch_discovery(http, other)
        source = q.registry.sources['ltn']
        auth = {**source.metadata_feed_authorization, 'valid_until': (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}
        q.registry.sources['ltn'] = replace(source, metadata_feed_authorization=auth)
        with pytest.raises(NewsError, match='authorization_expired'):
            fetch_discovery(http, task(q))
        assert session.calls == []
    finally:
        http.close(); q.close()


def test_worker_downloads_only_metadata_jobs_and_calibration_does_not_probe(config, tmp_path):
    routes = {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')}
    routes.update({url: FakeResponse(xml(), headers={'Content-Type': 'text/xml'}) for url in LTN_FEEDS})
    q, http, session = setup(config, tmp_path, routes)
    try:
        with q.db:
            q.add_articles([{'url': 'https://news.ltn.com.tw/news/456'}])
            q.db.execute("INSERT INTO discovery(url,kind,provider) VALUES ('https://news.ltn.com.tw/','index','ltn')")
        capacity = PublisherCapacity(http, config)
        assert capacity.states['ltn']['state'] == 'metadata_polling'
        assert session.calls == []
        result = run_cycle(q, http, config, max_articles=10, max_discovery_requests=14)
        assert progress(q.db, q.registry.sources['ltn'])['verified_feeds'] == 14
        assert all(url in {*LTN_FEEDS, 'https://news.ltn.com.tw/robots.txt'} for url, _ in session.calls)
        assert len(session.calls) == 15
        assert result['cycle']['article_attempts_started'] == result['cycle']['durably_accepted_versions'] == 0
        assert q.next_wake_delay({}, maximum=60) > 0  # No busy loop on blocked article/index due=0.
    finally:
        http.close(); q.close()


def test_truncated_xml_is_not_repaired_into_a_successful_metadata_page(config, tmp_path):
    q, http, session = setup(config, tmp_path, {})
    try:
        response = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml().removesuffix(b'</channel></rss>'))
        with pytest.raises(NewsError, match='invalid_metadata_feed_representation'):
            accept_discovery(q, task(q), response, [], [], [])
        assert not (tmp_path / 'metadata_feeds').exists()
    finally:
        http.close(); q.close()


def test_crash_before_receipt_replays_without_duplicate_body_or_forged_success(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_feeds as module
    q, http, session = setup(config, tmp_path, {})
    response = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml())
    writer = module.atomic_write_json
    def failed_write(*args, **kwargs):
        raise OSError('test crash before durable receipt')
    try:
        monkeypatch.setattr(module, 'atomic_write_json', failed_write)
        with pytest.raises(OSError):
            accept_discovery(q, task(q), response, [], [], [])
        assert task(q)['due'] == 0
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 0
        monkeypatch.setattr(module, 'atomic_write_json', writer)
        assert accept_discovery(q, task(q), response, [], [], []) == 0
        assert len(list((tmp_path / 'metadata_feeds').rglob('*.parquet'))) == 1
        assert q.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1
        assert q.db.execute('SELECT state,body_sha256 FROM articles').fetchone()[:] == ('awaiting_authorization', None)
        assert session.calls == []
    finally:
        http.close(); q.close()


@pytest.mark.parametrize('crash_point', ['before_receipt', 'before_database_commit'])
def test_metadata_feed_owner_recovers_durable_original_after_restart(config, tmp_path, monkeypatch, crash_point):
    """The owner resumes local evidence without another publisher request."""
    import sqlite3
    import downloader.news_fulltext_metadata_feeds as module

    q, http, session = setup(config, tmp_path, {})
    registry = http.registry
    response = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml', 'etag': '"one"'}, xml())
    initial = task(q)
    try:
        with monkeypatch.context() as patch:
            if crash_point == 'before_receipt':
                patch.setattr(module, 'atomic_write_json',
                              lambda *a, **kw: (_ for _ in ()).throw(OSError('simulated receipt interruption')))
                expected = OSError
            else:
                with q.db:
                    q.db.execute("CREATE TEMP TRIGGER reject_metadata_schedule BEFORE UPDATE ON discovery "
                                 "BEGIN SELECT RAISE(ABORT,'simulated schedule interruption'); END")
                expected = sqlite3.IntegrityError
            with pytest.raises(expected):
                accept_discovery(q, initial, response, [], [], [])
        assert task(q) == initial
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 0
        assert len(list((tmp_path / 'metadata_feeds').rglob('*.parquet'))) == 1
    finally:
        http.close(); q.close()

    from downloader.news_fulltext_metadata_checkpoint import recover
    restored = NewsQueue(tmp_path, registry)
    try:
        result = recover(restored, limit=4, max_seconds=1)
        assert result['outcomes'] == ['recovered']
        head = restored.db.execute('SELECT * FROM metadata_feed_heads WHERE feed_url=?', (LTN_FEEDS[0],)).fetchone()
        assert head['response_sha256'] == hashlib.sha256(response.body).hexdigest()
        receipt = json.loads((tmp_path / head['receipt_path']).read_bytes())
        original = pq.read_table(tmp_path / receipt['parquet_path']).to_pylist()[0]
        assert original['raw_response_body'] == response.body
        assert receipt['article_bodies_accepted'] == 0 and receipt['publish'] is False
        assert restored.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
        assert restored.db.execute('SELECT state,body_sha256 FROM articles').fetchone()[:] == ('awaiting_authorization', None)
        assert session.calls == []
    finally:
        restored.close()


@pytest.mark.parametrize('case', ['missing_original', 'corrupt_original', 'policy_changed'])
def test_metadata_checkpoint_never_promotes_missing_corrupt_or_revoked_original(config, tmp_path, monkeypatch, case):
    import downloader.news_fulltext_metadata_feeds as module
    from downloader.news_fulltext_metadata_checkpoint import recover

    q, http, session = setup(config, tmp_path, {})
    response = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml())
    initial = task(q)
    try:
        with monkeypatch.context() as patch:
            name = 'atomic_write_parquet' if case == 'missing_original' else 'atomic_write_json'
            patch.setattr(module, name, lambda *a, **kw: (_ for _ in ()).throw(OSError('simulated interruption')))
            with pytest.raises(OSError):
                accept_discovery(q, initial, response, [], [], [])
        if case == 'corrupt_original':
            next((tmp_path / 'metadata_feeds').rglob('*.parquet')).write_bytes(b'not a parquet file')
        if case == 'policy_changed':
            source = q.registry.sources['ltn']
            q.registry.sources['ltn'] = replace(source, metadata_feed_fingerprint='f' * 64)
        expected = {'missing_original': 'original_missing', 'corrupt_original': 'invalid_original',
                    'policy_changed': 'policy_changed'}[case]
        assert recover(q, limit=4, max_seconds=1)['outcomes'] == [expected]
        assert task(q) == initial
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 0
        assert q.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 0
        assert q.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
        assert q.db.execute('SELECT state,outcome FROM metadata_feed_interrupted_checkpoints').fetchone()[:] == ('skipped', expected)
        assert session.calls == []
    finally:
        http.close(); q.close()


def test_metadata_checkpoint_preserves_newer_source_and_article_heads(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_feeds as module
    from downloader.news_fulltext_metadata_checkpoint import recover

    q, http, _ = setup(config, tmp_path, {})
    article = 'https://news.ltn.com.tw/news/business/breakingnews/123'
    second = 'https://news.ltn.com.tw/news/business/breakingnews/124'
    item = (f'<item><title>Other</title><link>{second}</link>'
            '<pubDate>Tue, 13 Oct 2026 20:00:00 +0800</pubDate><description>Summary</description></item>').encode()
    response = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'},
                        xml().replace(b'</channel>', item + b'</channel>'))
    try:
        with monkeypatch.context() as patch:
            patch.setattr(module, 'atomic_write_json', lambda *a, **kw: (_ for _ in ()).throw(OSError('interrupted')))
            with pytest.raises(OSError):
                accept_discovery(q, task(q), response, [], [], [])
        with q.db:
            q.add_articles([{'url': article, 'title': 'newer independent title'}])
            q.db.execute("UPDATE articles SET last_seen_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (article,))
            q.db.execute("UPDATE discovery SET due=1234567890,error_code='http_429',attempts=4 WHERE url=?", (LTN_FEEDS[0],))
        newer = task(q)
        assert recover(q, limit=4, max_seconds=1)['outcomes'] == ['recovered']
        assert task(q) == newer
        assert q.db.execute('SELECT title,last_seen_utc FROM articles WHERE url=?', (article,)).fetchone()[:] == (
            'newer independent title', '2099-01-01T00:00:00+00:00')
        assert q.db.execute('SELECT state FROM articles WHERE url=?', (second,)).fetchone()[0] == 'awaiting_authorization'
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 0
        assert q.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
    finally:
        http.close(); q.close()


def test_metadata_checkpoint_blocks_only_its_feed_and_owner_recovers_without_http(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_feeds as module
    from downloader.news_fulltext_metadata_checkpoint import defer_live_failure

    q, http, session = setup(config, tmp_path, {})
    response = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml())
    initial = task(q)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(module, 'atomic_write_json', lambda *a, **kw: (_ for _ in ()).throw(OSError('interrupted')))
            with pytest.raises(OSError):
                accept_discovery(q, initial, response, [], [], [])
        other = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml', 'etag': '"different"'}, response.body)
        assert not defer_live_failure(q, initial, other, OSError('local write'))
        assert defer_live_failure(q, initial, response, OSError('local write'))
        assert task(q) == initial
        clause, params = q.discovery_dispatch_filter(q.registry.sources['ltn'])
        selectable = {row[0] for row in q.db.execute('SELECT url FROM discovery WHERE ' + clause, params)}
        assert LTN_FEEDS[0] not in selectable and LTN_FEEDS[1] in selectable
        feed_progress = progress(q.db, q.registry.sources['ltn'])
        assert (feed_progress['pending_recovery_feeds'], feed_progress['pending_recovery_originals'],
                feed_progress['local_recovery_ready']) == (1, 1, True)
        with q.db:
            q.db.execute('UPDATE discovery SET due=? WHERE url!=?', (time.time() + 3600, LTN_FEEDS[0]))
        run_cycle(q, http, config, max_articles=1, max_discovery_requests=1, max_run_seconds=3)
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads WHERE feed_url=?', (LTN_FEEDS[0],)).fetchone()[0] == 1
        assert not q.db.execute("SELECT 1 FROM metadata_feed_interrupted_checkpoints WHERE state='pending'").fetchone()
        assert session.calls == []
    finally:
        http.close(); q.close()


def test_metadata_checkpoint_keeps_original_observation_clock_for_next_poll(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_feeds as module
    import downloader.news_fulltext_queue as queue_module
    from downloader.news_fulltext_metadata_checkpoint import recover

    q, http, _ = setup(config, tmp_path, {})
    response = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml())
    original_clock = '2025-01-02T03:04:05+00:00'
    try:
        with monkeypatch.context() as patch:
            patch.setattr(queue_module, 'utc_now', lambda: original_clock)
            patch.setattr(module, 'atomic_write_json', lambda *a, **kw: (_ for _ in ()).throw(OSError('interrupted')))
            with pytest.raises(OSError):
                accept_discovery(q, task(q), response, [], [], [])
        assert recover(q, limit=1, max_seconds=1)['outcomes'] == ['recovered']
        expected = datetime.fromisoformat(original_clock).timestamp() + q.registry.sources['ltn'].metadata_feed_interval_seconds
        assert task(q)['due'] == expected
        assert task(q)['last_checked_at_utc'] == original_clock
        assert q.db.execute('SELECT checked_at_utc FROM metadata_feed_heads').fetchone()[0] == original_clock
    finally:
        http.close(); q.close()


def test_live_metadata_commit_failure_wakes_local_recovery_without_second_http(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_feeds as module

    q, http, session = setup(config, tmp_path, {
        'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        LTN_FEEDS[0]: FakeResponse(xml(), headers={'Content-Type': 'application/rss+xml'}),
    })
    writer = module.atomic_write_json
    calls = []
    def fail_once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise OSError('simulated local receipt failure')
        return writer(*args, **kwargs)
    try:
        with q.db:
            q.db.execute('UPDATE discovery SET due=? WHERE url!=?', (time.time() + 3600, LTN_FEEDS[0]))
        monkeypatch.setattr(module, 'atomic_write_json', fail_once)
        run_cycle(q, http, config, max_articles=1, max_discovery_requests=1, max_run_seconds=5)
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads WHERE feed_url=?', (LTN_FEEDS[0],)).fetchone()[0] == 1
        assert not q.db.execute("SELECT 1 FROM metadata_feed_interrupted_checkpoints WHERE state='pending'").fetchone()
        assert [url for url, _ in session.calls].count(LTN_FEEDS[0]) == 1
        assert len(calls) == 2
    finally:
        http.close(); q.close()


def test_metadata_checkpoint_bounds_local_failures_then_releases_only_original_feed(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_feeds as module
    from downloader.news_fulltext_metadata_checkpoint import MAX_LOCAL_IO_ATTEMPTS, recover

    q, http, session = setup(config, tmp_path, {})
    initial = task(q)
    response = Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml())
    try:
        monkeypatch.setattr(module, 'atomic_write_json', lambda *a, **kw: (_ for _ in ()).throw(OSError('disk failure')))
        with pytest.raises(OSError):
            accept_discovery(q, initial, response, [], [], [])
        for attempt in range(1, MAX_LOCAL_IO_ATTEMPTS + 1):
            result = recover(q, limit=1, max_seconds=1)
            assert result['outcomes'] == ['local_io_retry' if attempt < MAX_LOCAL_IO_ATTEMPTS else 'local_io_exhausted']
            assert task(q) == initial
            if attempt < MAX_LOCAL_IO_ATTEMPTS:
                assert result['next_local_retry_epoch'] > time.time()
                with q.db:
                    q.db.execute('UPDATE metadata_feed_interrupted_checkpoints SET not_before=0')
        checkpoint = q.db.execute('SELECT state,outcome,attempts FROM metadata_feed_interrupted_checkpoints').fetchone()
        assert checkpoint[:] == ('skipped', 'local_io_exhausted', MAX_LOCAL_IO_ATTEMPTS)
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 0
        clause, params = q.discovery_dispatch_filter(q.registry.sources['ltn'])
        assert LTN_FEEDS[0] in {row[0] for row in q.db.execute('SELECT url FROM discovery WHERE ' + clause, params)}
        assert session.calls == []
    finally:
        http.close(); q.close()


def test_metadata_checkpoint_survives_abrupt_process_exit(config, tmp_path):
    import subprocess
    import sys
    from downloader.news_fulltext_metadata_checkpoint import recover

    config_path = tmp_path / 'synthetic_config.json'
    config_path.write_text(json.dumps(config), encoding='utf-8')
    script = '''
import json, os, sys
from pathlib import Path
sys.path.insert(0, 'test')
from test_news_metadata_feed_scope import setup, task, xml, LTN_FEEDS
from downloader.news_fulltext import accept_discovery
from downloader.news_fulltext_http import Response
import downloader.news_fulltext_metadata_feeds as module
root = Path(sys.argv[1])
config = json.loads((root / 'synthetic_config.json').read_text())
q, http, session = setup(config, root, {})
module.atomic_write_json = lambda *args, **kwargs: os._exit(73)
accept_discovery(q, task(q), Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml()), [], [], [])
os._exit(74)
'''
    child = subprocess.run([sys.executable, '-c', script, str(tmp_path)], cwd=Path.cwd(), timeout=30,
                           capture_output=True, text=True, check=False)
    assert child.returncode == 73, child.stderr
    registry = PolicyRegistry(config, base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    try:
        assert recover(q, limit=4, max_seconds=1)['outcomes'] == ['recovered']
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 1
        assert q.db.execute('SELECT state,body_sha256 FROM articles').fetchone()[:] == ('awaiting_authorization', None)
    finally:
        q.close()


def test_304_cannot_accept_missing_or_corrupt_durable_metadata(config, tmp_path):
    q, http, session = setup(config, tmp_path, {})
    try:
        with pytest.raises(NewsError, match='unverifiable_metadata_feed_not_modified'):
            accept_discovery(q, task(q), Response(LTN_FEEDS[0], 304, {}, b''), [], [], [])
        accept_discovery(q, task(q), Response(LTN_FEEDS[0], 200, {'content-type': 'text/xml'}, xml()), [], [], [])
        head = q.db.execute('SELECT * FROM metadata_feed_heads').fetchone()
        receipt = json.loads((tmp_path / head['receipt_path']).read_bytes())
        (tmp_path / receipt['parquet_path']).write_bytes(b'corrupt fixture')
        old_due = task(q)['due']
        with pytest.raises(NewsError, match='metadata_feed_receipt_mismatch'):
            accept_discovery(q, task(q), Response(LTN_FEEDS[0], 304, {}, b''), [], [], [])
        assert task(q)['due'] == old_due
    finally:
        http.close(); q.close()


@pytest.mark.parametrize('status', [200, 304])
@pytest.mark.parametrize('field', ['provider', 'feed_url', 'response_bytes', 'rows',
                                   'contract_version', 'publish', 'remote_training_export'])
def test_reused_metadata_receipt_requires_complete_identity_and_counts(config, tmp_path, status, field):
    q, http, session = setup(config, tmp_path, {})
    try:
        url = LTN_FEEDS[0]
        body = xml()
        accept_discovery(q, task(q), Response(url, 200, {'content-type': 'text/xml'}, body), [], [], [])
        head = dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone())
        original_job = task(q)
        receipt_path = tmp_path / head['receipt_path']
        receipt = json.loads(receipt_path.read_bytes())
        if field in {'provider', 'feed_url'}:
            receipt[field] = 'different_source'
        elif field in {'publish', 'remote_training_export'}:
            receipt[field] = True
        else:
            receipt[field] += 1
        receipt_path.write_text(json.dumps(receipt), encoding='utf-8')
        altered_receipt = receipt_path.read_bytes()
        response = Response(url, status, {'content-type': 'text/xml'}, body if status == 200 else b'')
        with pytest.raises(NewsError, match='metadata_feed_receipt_mismatch'):
            accept_discovery(q, original_job, response, [], [], [])
        assert task(q) == original_job
        assert dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone()) == head
        assert receipt_path.read_bytes() == altered_receipt
        assert session.calls == []
    finally:
        http.close(); q.close()


@pytest.mark.parametrize('field', ['rows', 'response_bytes', 'first_publication_aware',
                                   'last_publication_aware', 'receipt_path'])
def test_304_does_not_advance_an_inconsistent_metadata_head(config, tmp_path, field):
    q, http, session = setup(config, tmp_path, {})
    try:
        url = LTN_FEEDS[0]
        accept_discovery(q, task(q), Response(url, 200, {'content-type': 'text/xml'}, xml()), [], [], [])
        value = {'rows': 2, 'response_bytes': 1, 'first_publication_aware': None,
                 'last_publication_aware': None, 'receipt_path': 'other/receipt.json'}[field]
        with q.db:
            q.db.execute('UPDATE metadata_feed_heads SET ' + field + '=? WHERE feed_url=?', (value, url))
        head = dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone())
        original_job = task(q)
        with pytest.raises(NewsError, match='metadata_feed_receipt_mismatch'):
            accept_discovery(q, original_job, Response(url, 304, {}, b''), [], [], [])
        assert task(q) == original_job
        assert dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone()) == head
        assert session.calls == []
    finally:
        http.close(); q.close()


def test_200_does_not_replace_missing_original_under_existing_receipt(config, tmp_path):
    q, http, session = setup(config, tmp_path, {})
    try:
        url = LTN_FEEDS[0]
        response = Response(url, 200, {'content-type': 'text/xml'}, xml())
        accept_discovery(q, task(q), response, [], [], [])
        head = dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone())
        original_job = task(q)
        receipt_path = tmp_path / head['receipt_path']
        original = tmp_path / json.loads(receipt_path.read_bytes())['parquet_path']
        original.unlink()
        receipt_bytes = receipt_path.read_bytes()
        with pytest.raises(NewsError, match='metadata_feed_receipt_mismatch'):
            accept_discovery(q, original_job, response, [], [], [])
        assert not original.exists()
        assert receipt_path.read_bytes() == receipt_bytes
        assert task(q) == original_job
        assert dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone()) == head
        assert session.calls == []
    finally:
        http.close(); q.close()


def test_orphan_metadata_original_with_wrong_source_cannot_be_sealed(config, tmp_path):
    from downloader.artifact_io import atomic_write_parquet, table_from_records

    q, http, session = setup(config, tmp_path, {})
    try:
        url = LTN_FEEDS[0]
        response = Response(url, 200, {'content-type': 'text/xml'}, xml())
        accept_discovery(q, task(q), response, [], [], [])
        head = dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone())
        original_job = task(q)
        receipt_path = tmp_path / head['receipt_path']
        original = tmp_path / json.loads(receipt_path.read_bytes())['parquet_path']
        row = pq.read_table(original).to_pylist()[0]
        row['provider'] = 'different_provider'
        receipt_path.unlink()
        atomic_write_parquet(original, table_from_records([row]), durable=True)
        altered = original.read_bytes()
        with pytest.raises(NewsError, match='metadata_feed_receipt_mismatch'):
            accept_discovery(q, original_job, response, [], [], [])
        assert not receipt_path.exists()
        assert original.read_bytes() == altered
        assert task(q) == original_job
        assert dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone()) == head
        assert session.calls == []
    finally:
        http.close(); q.close()


def test_missing_metadata_receipt_recovers_after_unconditional_retry(config, tmp_path):
    from downloader.news_fulltext import _discovery_head_fields, fail_discovery

    routes = {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              LTN_FEEDS[0]: FakeResponse(xml(), headers={'Content-Type': 'text/xml', 'ETag': 'new-tag'})}
    q, http, session = setup(config, tmp_path, routes)
    try:
        url = LTN_FEEDS[0]
        response = Response(url, 200, {'content-type': 'text/xml', 'etag': 'old-tag',
                                       'last-modified': 'old-clock'}, xml())
        accept_discovery(q, task(q), response, [], [], [])
        head = dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone())
        receipt = tmp_path / head['receipt_path']
        original = tmp_path / json.loads(receipt.read_bytes())['parquet_path']
        original_bytes = original.read_bytes()
        receipt.unlink()
        failed = task(q)
        with pytest.raises(NewsError, match='metadata_feed_receipt_mismatch') as caught:
            accept_discovery(q, failed, Response(url, 304, {}, b''), [], [], [])
        fail_discovery(q, failed, caught.value, expected_head_fields=_discovery_head_fields(failed))
        retry = task(q)
        assert retry['etag'] is None and retry['last_modified'] is None
        assert retry['error_code'] == 'metadata_feed_receipt_mismatch'
        result = fetch_discovery(http, retry)
        assert session.calls[-1][0] == url
        assert 'If-None-Match' not in session.calls[-1][1]['headers']
        assert 'If-Modified-Since' not in session.calls[-1][1]['headers']
        accept_discovery(q, retry, *result)
        assert receipt.is_file() and original.read_bytes() == original_bytes
        assert task(q)['error_code'] is None
        assert len(session.calls) == 2  # Fake robots and fake feed; no network.
    finally:
        http.close(); q.close()


def test_orphan_metadata_original_cannot_be_sealed_without_validation(config, tmp_path):
    q, http, session = setup(config, tmp_path, {})
    try:
        url = LTN_FEEDS[0]
        response = Response(url, 200, {'content-type': 'text/xml'}, xml())
        accept_discovery(q, task(q), response, [], [], [])
        head = dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone())
        original_job = task(q)
        receipt_path = tmp_path / head['receipt_path']
        receipt = json.loads(receipt_path.read_bytes())
        original = tmp_path / receipt['parquet_path']
        receipt_path.unlink()
        original.write_bytes(b'not a parquet original')
        with pytest.raises(NewsError, match='metadata_feed_receipt_mismatch'):
            accept_discovery(q, original_job, response, [], [], [])
        assert not receipt_path.exists()
        assert original.read_bytes() == b'not a parquet original'
        assert task(q) == original_job
        assert dict(q.db.execute('SELECT * FROM metadata_feed_heads').fetchone()) == head
        assert session.calls == []
    finally:
        http.close(); q.close()
