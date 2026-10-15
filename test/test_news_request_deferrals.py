"""Gate deferral is not a failed download; source policy stays independent."""
from contextlib import closing
import time

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import (NewsError, PolicyRegistry, REQUEST_DEFERRAL_CODES,
                                            REQUEST_DEFERRAL_CONTRACT_VERSION)
from downloader.news_fulltext_queue import NewsQueue
from scripts.benchmark_news_connection_recovery import OwnedLimiter, OwnedScopedSession, owned_config, owned_servers
from test_news_fulltext import config


URL = 'https://publisher.test/history'


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
        assert after['state'] == 'retry' and after['error_code'] == code
        assert before + 3 <= after['due'] <= time.time() + 3
        assert after['attempts'] == attempts
        assert after['body_sha256'] is None and after['lease_owner'] is None
        assert queue.claim_url(URL, 120) is None
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['frontier_attempts_failed'] == counts['frontier_source_gate_deferrals'] == 1
        assert counts['frontier_deferred_' + code] == 1
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        restored = dict(restarted.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert (restored['due'], restored['attempts'], restored['error_code']) == (after['due'], attempts, code)
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
            assert head['attempts'] == 5 and head['state'] == 'retry' and head['body_sha256'] is None
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
