"""Retire an idle socket before GET, without hiding genuine source failures."""
import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_connection_observation import (
    ConnectionObservingAdapter, MAX_IDLE_CONNECTION_SECONDS, valid_connection_observation,
)
from downloader.news_fulltext_connections import OriginConnectionSession
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.benchmark_news_connection_recovery import (
    ObservedLoopbackAdapter, OwnedLimiter, OwnedScopedSession, owned_config, owned_servers,
)


class ExpiringSession(OwnedScopedSession):
    def _new_origin_adapter(self):
        adapter = ObservedLoopbackAdapter(self.servers)
        adapter.max_idle_seconds = MAX_IDLE_CONNECTION_SECONDS
        return adapter


@pytest.mark.parametrize('value', [True, 0, -1, float('inf'), float('nan'), '45'])
def test_invalid_idle_limit_is_rejected(value):
    with pytest.raises(ValueError, match='invalid_idle_connection_lifetime'):
        ConnectionObservingAdapter(max_idle_seconds=value)


def test_native_session_has_one_bounded_origin_pool_and_no_implicit_retry():
    with OriginConnectionSession() as session:
        adapter = session.get_adapter('https://publisher.test/known')
        assert adapter.max_idle_seconds == 45
        assert adapter.max_retries.total == 0 and adapter._pool_maxsize == 1
        assert session.get_adapter('https://publisher.test/other') is adapter
        assert session.idle_connection_expirations == 0


@pytest.mark.parametrize('idle,expired', [(0, False), (20, False), (44, False), (45, True), (59, True), (300, True)])
def test_real_tcp_idle_expiry_keeps_exact_request_and_durable_body_receipts(tmp_path, idle, expired):
    config = owned_config()
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    with owned_servers() as servers:
        http = NewsHTTP(registry, config, tmp_path,
            session_factory=lambda: ExpiringSession(servers), address_check=lambda _: None,
            limiter_factory=OwnedLimiter, metrics=queue.metrics)
        try:
            urls = ['https://publisher.test/warm', 'https://publisher.test/target']
            with queue.db:
                queue.add_articles([{'url': url} for url in urls])
            first = fetch_article(http, queue.claim_url(urls[0], 120))
            queue.accept([first])
            session = http._session()
            adapter = session.origin_adapters['https://publisher.test']
            # Age the local fixture clock, not a production socket/deadline.
            adapter._news_checkout.connection._news_terminal_at -= idle
            second = fetch_article(http, queue.claim_url(urls[1], 120))
            queue.accept([second])
            observation = session.finish_connection_observation(urls[1])
            assert valid_connection_observation(observation)
            assert observation['connect_attempts'] == int(expired)
            assert observation['socket_open_at_checkout'] is not expired
            assert session.idle_connection_expirations == int(expired)
            assert session.finish_connection_observation(urls[1]) == observation
            assert session.idle_connection_expirations == int(expired)
            counts = http.metrics.snapshot()['providers']['publisher']['counts']
            assert counts.get('expired_idle_connections', 0) == int(expired)
            assert counts['http_requests_started'] == counts['http_terminal_requests'] == 3
            assert counts.get('transport_errors', 0) == 0
            assert servers['publisher.test'].paths == ['/robots.txt', '/warm', '/target']
            assert servers['publisher.test'].connections == 1 + int(expired)
            assert verify_bodies(tmp_path)['accepted']
            assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 2
            assert first['published_at_raw'] == second['published_at_raw'] == '2026-10-01T00:00:00+00:00'
            assert first['body_sha256'] == second['body_sha256']
        finally:
            http.close()
            queue.close()


def test_expiry_is_origin_local_and_does_not_mask_retryable_source_failure(tmp_path):
    config = owned_config()
    registry = PolicyRegistry(config, base=tmp_path)
    with owned_servers() as servers:
        http = NewsHTTP(registry, config, tmp_path,
            session_factory=lambda: ExpiringSession(servers), address_check=lambda _: None,
            limiter_factory=OwnedLimiter)
        try:
            http._raw('https://other.test/healthy')
            http._raw('https://publisher.test/warm')
            session = http._session()
            healthy = session.origin_adapters['https://other.test']
            session.origin_adapters['https://publisher.test']._news_checkout.connection._news_terminal_at -= 59
            with pytest.raises(NewsError, match='transport_error') as failure:
                http._raw('https://publisher.test/fail/native')
            assert failure.value.retry and failure.value.delay == 30
            assert session.origin_adapters['https://other.test'] is healthy
            assert http._raw('https://other.test/still-healthy').status == 200
            counts = http.metrics.snapshot()['providers']['publisher']['counts']
            assert counts['expired_idle_connections'] == counts['transport_errors'] == counts['failed_origin_pools_reset'] == 1
            assert counts['http_requests_started'] == 2
            assert servers['publisher.test'].paths == ['/warm', '/fail/native']
            assert servers['other.test'].connections == 1
        finally:
            http.close()
