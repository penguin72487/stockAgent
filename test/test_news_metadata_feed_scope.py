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
