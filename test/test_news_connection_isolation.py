from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import errno
import threading

import pytest
import requests

from downloader.news_fulltext_connections import (MAX_CONNECTION_ORIGINS_PER_WORKER,
    OriginConnectionSession)
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from test_news_fulltext import FakeLimiter, config


class ControlledAdapter(requests.adapters.BaseAdapter):
    def __init__(self, routes):
        self.routes = routes
        self.calls = []
        self.closed = 0
        self.close_error = None

    def send(self, request, **kwargs):
        self.calls.append((request, kwargs))
        result = self.routes.get(request.url, b'native bytes')
        if isinstance(result, BaseException):
            raise result
        response = requests.Response()
        response.status_code = 200
        response.headers = {'Content-Type': 'text/html'}
        response._content = result
        response._content_consumed = True
        response.request = request
        response.url = request.url
        return response

    def close(self):
        self.closed += 1
        if self.close_error:
            raise self.close_error


class ControlledSession(OriginConnectionSession):
    def __init__(self, routes=None):
        super().__init__()
        self.routes = routes or {}
        self.created = []

    def _new_origin_adapter(self):
        adapter = ControlledAdapter(self.routes)
        self.created.append(adapter)
        return adapter


def fixture(config, tmp_path, routes=None):
    config = deepcopy(config)
    config['providers'][0]['domains'] = ['publisher.test', 'm.publisher.test']
    other = deepcopy(config['providers'][0])
    other.update(id='other', domains=['other.test'])
    config['providers'].append(other)
    sessions = []

    def factory():
        session = ControlledSession(routes)
        sessions.append(session)
        return session

    http = NewsHTTP(PolicyRegistry(config, base=tmp_path), config, tmp_path,
                    session_factory=factory, address_check=lambda _: None,
                    limiter_factory=FakeLimiter)
    return http, sessions


@pytest.mark.parametrize('phase', ['headers', 'body'])
def test_failed_origin_preserves_other_host_and_provider_pools(config, tmp_path, phase):
    error = requests.ReadTimeout('bounded original source error')
    http, sessions = fixture(config, tmp_path, {'https://m.publisher.test/bad': error})
    try:
        for url in ('https://publisher.test/a', 'https://other.test/b', 'https://m.publisher.test/ok'):
            assert http._raw(url).body == b'native bytes'
        session = sessions[0]
        good = session.get_adapter('https://publisher.test/a')
        other = session.get_adapter('https://other.test/b')
        bad = session.get_adapter('https://m.publisher.test/bad')
        if phase == 'body':
            send = bad.send

            def streaming_error(request, **kwargs):
                response = send(requests.Request('GET', 'https://m.publisher.test/ok').prepare(), **kwargs)
                def chunks(**unused):
                    yield b'partial native bytes'
                    raise error
                response.iter_content = chunks
                return response

            bad.send = streaming_error
        with pytest.raises(NewsError, match='transport_error') as caught:
            http._raw('https://m.publisher.test/bad')
        assert caught.value.__cause__ is error
        assert caught.value.retry and caught.value.delay == 30
        assert len(sessions) == len(http._sessions) == 1
        assert session.get_adapter('https://publisher.test/c') is good
        assert session.get_adapter('https://other.test/d') is other
        assert (good.closed, other.closed, bad.closed) == (0, 0, 1)
        assert 'publisher' not in http.provider_not_before
        for url in ('https://publisher.test/c', 'https://other.test/d', 'https://m.publisher.test/recovered'):
            assert http._raw(url).body == b'native bytes'
        assert session.get_adapter('https://m.publisher.test/recovered') is not bad
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['transport_errors'] == counts['failed_origin_pools_reset'] == 1
        assert counts.get('failed_sessions_reset', 0) == 0
        assert len(bad.calls) == 2  # Warm-up and exactly one failed request; no hidden retry.
    finally:
        http.close()
    assert good.closed == other.closed == 1


@pytest.mark.parametrize('kind', ['route', 'tls'])
def test_origin_pool_scope_does_not_weaken_provider_circuit(config, tmp_path, kind):
    error = requests.exceptions.SSLError('tls') if kind == 'tls' else requests.ConnectionError('route')
    if kind == 'route':
        error.__cause__ = OSError(errno.ENETUNREACH, 'unreachable')
    http, _ = fixture(config, tmp_path, {'https://m.publisher.test/bad': error})
    try:
        with pytest.raises(NewsError, match='transport_error'):
            http._raw('https://m.publisher.test/bad')
        with pytest.raises(NewsError, match='provider_cooldown'):
            http._raw('https://publisher.test/other')
        assert http._raw('https://other.test/good').body == b'native bytes'
    finally:
        http.close()


def test_reset_cleanup_error_cannot_mask_original_source_failure(config, tmp_path):
    error = requests.ReadTimeout('source')
    http, sessions = fixture(config, tmp_path, {'https://m.publisher.test/bad': error})
    try:
        http._raw('https://other.test/good')
        good = sessions[0].get_adapter('https://other.test/good')
        bad = sessions[0].get_adapter('https://m.publisher.test/bad')
        bad.close_error = OSError('pool cleanup')
        with pytest.raises(NewsError, match='transport_error') as caught:
            http._raw('https://m.publisher.test/bad')
        assert caught.value.__cause__ is error
        assert 'https://m.publisher.test' not in sessions[0].origin_adapters
        assert good.closed == 0
        assert http.metrics.snapshot()['providers']['publisher']['counts']['connection_pool_reset_errors'] == 1
    finally:
        http.close()


def test_origin_lru_is_bounded_and_does_not_change_path_query_identity():
    session = ControlledSession()
    try:
        first = session.get_adapter('https://origin0.test/article?id=1')
        assert session.get_adapter('https://origin0.test/other?id=2') is first
        for number in range(1, MAX_CONNECTION_ORIGINS_PER_WORKER + 1):
            session.get_adapter(f'https://origin{number}.test/article')
        assert len(session.origin_adapters) == MAX_CONNECTION_ORIGINS_PER_WORKER
        assert first.closed == 1 and session.origin_pool_evictions == 1
        live = session.get_adapter('https://origin1.test/article')
        session.get_adapter('https://another.test/article')
        assert session.get_adapter('https://origin1.test/article') is live
        assert not session.reset_origin('https://unknown.test/article')
        assert session.get_adapter('http://origin1.test/article') is not live
    finally:
        session.close()
    assert all(adapter.closed == 1 for adapter in session.created)
    session.close()  # Idempotent, no cached closed origin adapters.
    assert all(adapter.closed == 1 for adapter in session.created)


def test_session_preserves_cookie_request_and_tls_contract():
    session = ControlledSession()
    session.trust_env = False
    session.cookies.set('source', 'original', domain='publisher.test', path='/')
    try:
        session.get('https://publisher.test/a', stream=True, allow_redirects=False, timeout=(10, 25),
                    headers={'User-Agent': 'canonical-agent'})
        adapter = session.get_adapter('https://publisher.test/a')
        request, settings = adapter.calls[-1]
        assert request.headers['Cookie'] == 'source=original'
        assert settings['verify'] is True and settings['timeout'] == (10, 25)
        assert settings['stream'] and not settings['proxies']
        session.reset_origin('https://publisher.test/a')
        session.get('https://publisher.test/b', stream=True, allow_redirects=False, timeout=(10, 25))
        request = session.get_adapter('https://publisher.test/b').calls[-1][0]
        assert request.headers['Cookie'] == 'source=original'
        session.get('https://other.test/a', allow_redirects=False)
        assert 'Cookie' not in session.get_adapter('https://other.test/a').calls[-1][0].headers
    finally:
        session.close()


def test_default_real_adapter_has_no_hidden_retries_and_one_connection():
    session = OriginConnectionSession()
    try:
        adapter = session.get_adapter('https://publisher.test/a')
        assert adapter.max_retries.total == 0
        assert adapter.poolmanager.connection_pool_kw['maxsize'] == 1
        assert len(session.origin_adapters) == 1
    finally:
        session.close()


def test_canonical_session_is_thread_local_and_closed_once(config, tmp_path):
    http, sessions = fixture(config, tmp_path)
    barrier = threading.Barrier(3)

    def call(number):
        http._raw(f'https://publisher.test/{number}')
        barrier.wait()
        return id(http.local.session)

    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            assert len(set(executor.map(call, range(3)))) == 3
        assert len(sessions) == len(http._sessions) == 3
    finally:
        http.close()
    assert all(adapter.closed == 1 for session in sessions for adapter in session.created)


def test_owned_real_tcp_full_chain_keeps_healthy_connections_and_raw_parity():
    from scripts.benchmark_news_connection_recovery import benchmark

    result = benchmark(rounds=1, cycles=1)
    assert result['accepted'] and len(result['phases']) == 4
    assert result['external_source_requests'] == 0
    assert not result['production_queue_writes'] and not result['production_rate_state_writes']
    assert result['summary']['reference']['tcp_connections'][0]['other.test'] == 2
    assert result['summary']['candidate']['tcp_connections'][0]['other.test'] == 1


@pytest.mark.parametrize('rounds,cycles', [(0, 1), (13, 1), (1, 0), (1, 4), (True, 1), (1, False)])
def test_owned_benchmark_never_expands_beyond_bounded_workload(rounds, cycles):
    from scripts.benchmark_news_connection_recovery import benchmark

    with pytest.raises(ValueError, match='bounded_owned'):
        benchmark(rounds=rounds, cycles=cycles)
