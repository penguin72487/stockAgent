"""Gate deferral is not a failed download; source policy stays independent."""
from contextlib import closing
import time

import pytest

from downloader.news_fulltext import fail_discovery, fetch_article
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import (NewsError, PolicyRegistry, REQUEST_DEFERRAL_CODES,
                                            REQUEST_DEFERRAL_CONTRACT_VERSION)
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.benchmark_news_connection_recovery import OwnedLimiter, OwnedScopedSession, owned_config, owned_servers
from test_news_fulltext import FakeResponse, article_html, config, http_fixture


URL = 'https://publisher.test/history'


@pytest.mark.parametrize('code', sorted(REQUEST_DEFERRAL_CODES))
@pytest.mark.parametrize('state,prior_error,prior_check', [
    ('pending', None, None),
    ('retry', 'source_access_denied', '2026-01-01T00:00:00+00:00'),
    ('retry_cooldown', 'http_502', '2026-01-02T00:00:00+00:00'),
])
def test_article_gate_preserves_last_source_observation_and_lane(
        config, tmp_path, code, state, prior_error, prior_check):
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,last_checked_at_utc=?,attempts=5 '
                             'WHERE url=?', (state, prior_error, prior_check, URL))
        row = queue.claim_url(URL, 120)
        before = time.time()
        queue.failed(row, NewsError(code, retry=True, delay=3, request_deferred=True), 6)
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert (after['state'], after['error_code'], after['last_checked_at_utc']) == (
            state, prior_error, prior_check)
        assert after['attempts'] == 5 and after['lease_owner'] is None and after['lease_until'] is None
        assert before + 3 <= after['due'] <= time.time() + 3
        assert queue.claim_url(URL, 120) is None
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        assert dict(restarted.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == after


def test_article_gate_does_not_shorten_a_newer_deadline(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
        row = queue.claim_url(URL, 120)
        existing_due = time.time() + 120
        with queue.db:
            queue.db.execute('UPDATE articles SET due=? WHERE url=?', (existing_due, URL))
        queue.failed(row, NewsError('stopping', retry=True, delay=1, request_deferred=True), 6)
        assert queue.db.execute('SELECT due FROM articles WHERE url=?', (URL,)).fetchone()[0] == existing_due


def test_gate_does_not_erase_source_refusal_fairness(config, tmp_path):
    fresh = 'https://publisher.test/older-first-download'
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': URL, 'date': '2026-01-01'}, {'url': fresh, 'date': '2020-01-01'}])
            queue.db.execute("UPDATE articles SET state='retry',error_code='source_access_denied',"
                             "last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        refused = queue.claim_url(URL, 120)
        queue.failed(refused, NewsError('host_gateway_cooldown', retry=True, delay=0, request_deferred=True), 6)
        selected = queue.claim('publisher', 120)
        assert selected['url'] == fresh
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['frontier_access_refusal_fairness_deferrals'] == 1


def test_gate_on_direct_refresh_keeps_saved_body_and_resumes_conditional_304(config, tmp_path, monkeypatch):
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': '"v1"'}),
    })
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(http), closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
        queue.accept([fetch_article(http, queue.claim_url(URL, 120))])
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert before['state'] == 'complete' and before['body_sha256'] and before['etag'] == '"v1"'
        assert verify_bodies(tmp_path)['accepted']
        http.defer('publisher', 3)
        row = queue.claim_url(URL, 120, refresh=True)
        calls = len(session.calls)
        with pytest.raises(NewsError) as caught:
            fetch_article(http, row)
        assert caught.value.request_deferred and len(session.calls) == calls
        queue.failed(row, caught.value, 6)
        deferred = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert deferred['state'] == 'retry'  # Explicit refresh remains schedulable.
        for field in ('body_sha256', 'parquet_path', 'captured_at_utc', 'last_checked_at_utc',
                      'etag', 'last_modified', 'validator_url', 'error_code'):
            assert deferred[field] == before[field]
        assert queue.claim('publisher', 120) is None
        session.routes[URL] = FakeResponse(b'', status=304)
        wall_clock = time.time
        monkeypatch.setattr(time, 'time', lambda: wall_clock() + 4)
        again = queue.claim('publisher', 120)
        assert again['url'] == URL and fetch_article(http, again) is None
        assert session.calls[-1][1]['headers']['If-None-Match'] == '"v1"'
        queue.unchanged(again)
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert after['state'] == 'complete' and after['body_sha256'] == before['body_sha256']
        assert after['captured_at_utc'] == before['captured_at_utc']
        assert verify_bodies(tmp_path)['accepted']
        counters = dict(queue.db.execute('SELECT key,value FROM counters'))
        assert counters['article_gate_deferrals'] == 1 and counters['not_modified'] == 1
        assert counters['accepted_versions'] == 1


def test_article_owner_wakes_after_gate_race_and_commits_once(config, tmp_path, monkeypatch):
    import downloader.news_fulltext as news

    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(article_html()),
    })
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(http), closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
        original_fetch, original_accept = news.fetch_article, queue.accept
        dispatched = []

        def racing_gate(client, row):
            dispatched.append((row['url'], row['state'], row['last_checked_at_utc']))
            if len(dispatched) == 1:
                client.defer('publisher', .3)
            return original_fetch(client, row)

        def accept_and_stop(records):
            result = original_accept(records)
            http.stop.set()
            return result

        monkeypatch.setattr(news, 'fetch_article', racing_gate)
        monkeypatch.setattr(queue, 'accept', accept_and_stop)
        news.run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=8)
        assert dispatched == [(URL, 'pending', None), (URL, 'pending', None)]
        assert [call[0] for call in session.calls] == ['https://publisher.test/robots.txt', URL]
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert after['state'] == 'complete' and after['body_sha256']
        assert dict(queue.db.execute('SELECT key,value FROM counters'))['article_gate_deferrals'] == 1
        assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize('code', sorted(REQUEST_DEFERRAL_CODES))
@pytest.mark.parametrize('attempts', [0, 5, 12])
def test_tagged_gate_retains_exact_delay_and_prior_burst(config, tmp_path, code, attempts):
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET attempts=? WHERE url=?', (attempts, URL))
        row = queue.claim_url(URL, 120)
        before = time.time()
        queue.failed(row, NewsError(code, retry=True, delay=3, request_deferred=True), 6)
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert after['state'] == 'pending' and after['error_code'] is None
        assert before + 3 <= after['due'] <= time.time() + 3
        assert after['attempts'] == attempts
        assert after['body_sha256'] is None and after['lease_owner'] is None
        assert queue.claim_url(URL, 120) is None
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['frontier_attempts_failed'] == counts['frontier_source_gate_deferrals'] == 1
        assert counts['frontier_deferred_' + code] == 1
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        restored = dict(restarted.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert (restored['due'], restored['attempts'], restored['error_code']) == (after['due'], attempts, None)
        setting = restarted.db.execute("SELECT value FROM queue_settings WHERE key='article_request_deferral_contract_version'").fetchone()[0]
        assert setting == str(REQUEST_DEFERRAL_CONTRACT_VERSION)


@pytest.mark.parametrize('code', ['transport_error', 'response_deadline', 'http_502', 'http_429',
                                  'source_access_denied', 'body_not_found', 'provider_cooldown'])
def test_untagged_real_or_legacy_errors_keep_original_cooldown(config, tmp_path, code):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET attempts=5 WHERE url=?', (URL,))
        row = queue.claim_url(URL, 120)
        now = time.time()
        queue.failed(row, NewsError(code, retry=True, delay=5000), 6)
        current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert current['state'] == 'retry_cooldown' and current['due'] >= now + 5000
        assert current['attempts'] == 0 and current['error_code'] == code
        assert queue.metrics.snapshot()['providers']['publisher']['counts'].get('frontier_source_gate_deferrals', 0) == 0


@pytest.mark.parametrize('kwargs', [{'code': 'transport_error', 'retry': True, 'request_deferred': True},
    {'code': 'http_403', 'retry': True, 'request_deferred': True},
    {'code': 'provider_cooldown', 'request_deferred': True},
    {'code': 'stopping', 'retry': True, 'request_deferred': 1}])
def test_invalid_deferral_marker_is_rejected(kwargs):
    with pytest.raises(ValueError, match='verified_request_deferral_required'):
        NewsError(**kwargs)


@pytest.mark.parametrize('code', sorted(REQUEST_DEFERRAL_CODES))
@pytest.mark.parametrize('attempts', [0, 5, 12])
def test_discovery_gate_keeps_failure_burst_and_prior_source_check(config, tmp_path, code, attempts):
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider,attempts,error_code,last_checked_at_utc,etag) "
                "VALUES (?,'sitemap','publisher',?,'http_502','2026-01-01T00:00:00+00:00','source-etag')",
                (URL, attempts))
        row = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (URL,)).fetchone())
        before = time.time()
        fail_discovery(queue, row, NewsError(code, retry=True, delay=3, request_deferred=True))
        after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (URL,)).fetchone())
        assert after['attempts'] == attempts
        assert before + 3 <= after['due'] <= time.time() + 3
        assert after['error_code'] == row['error_code']
        assert after['last_checked_at_utc'] == row['last_checked_at_utc']
        assert after['etag'] == row['etag']
        assert not queue.discovery_candidates(registry.sources['publisher'], 1)
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['discovery_source_gate_deferrals'] == counts['discovery_deferred_' + code] == 1
        assert queue.status()['discovery_request_deferral_contract_version'] == 1
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        restored = dict(restarted.db.execute('SELECT * FROM discovery WHERE url=?', (URL,)).fetchone())
        assert restored == after


def test_discovery_deferral_does_not_shorten_a_newer_deadline(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'sitemap','publisher')", (URL,))
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (URL,)).fetchone())
        existing_due = time.time() + 120
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (existing_due, URL))
        fail_discovery(queue, task, NewsError('stopping', retry=True, delay=1, request_deferred=True))
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (URL,)).fetchone()[0] == existing_due


@pytest.mark.parametrize('gate', ['provider', 'robots', 'host', 'stop'])
def test_real_http_gate_never_issues_request_or_invents_body_then_captures(gate, tmp_path):
    config = owned_config()
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue, owned_servers() as servers:
        http = NewsHTTP(registry, config, tmp_path,
            session_factory=lambda: OwnedScopedSession(servers, observe_connections=True),
            limiter_factory=OwnedLimiter, address_check=lambda url: None, metrics=queue.metrics)
        stop = http.stop
        try:
            with queue.db:
                queue.add_articles([{'url': URL}])
                queue.db.execute('UPDATE articles SET attempts=5 WHERE url=?', (URL,))
            if gate == 'provider':
                http.defer('publisher', 3)
            elif gate == 'robots':
                http.robots_policy_pauses['publisher'] = {'https://publisher.test': time.time() + 3}
            elif gate == 'host':
                # Existing verified host gate interface, no real refusal reset.
                http.host_circuits.remaining = lambda provider, host: 3
            else:
                stop.set()
            row = queue.claim_url(URL, 120)
            with pytest.raises(NewsError) as caught:
                fetch_article(http, row)
            assert caught.value.request_deferred is True
            assert http.requests_by_provider.get('publisher', 0) == 0
            queue.failed(row, caught.value, 6)
            head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            assert head['attempts'] == 5 and head['state'] == 'pending' and head['body_sha256'] is None
            # Controlled owned fixture, not production policy/deadline mutation.
            http.provider_not_before.clear()
            http.robots_policy_pauses.clear()
            http.host_circuits.remaining = lambda provider, host: 0
            stop.clear()
            with queue.db:
                queue.db.execute('UPDATE articles SET due=0 WHERE url=?', (URL,))
            record = fetch_article(http, queue.claim_url(URL, 120))
            queue.accept([record])
            assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
        finally:
            http.close()
