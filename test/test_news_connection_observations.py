from __future__ import annotations

from http.server import ThreadingHTTPServer
import json
import ssl
import subprocess
import threading

import pytest
import requests
from urllib3.poolmanager import pool_classes_by_scheme

from downloader.news_fulltext_connection_observation import (
    CONNECTION_BUCKETS, ConnectionObservingAdapter, connection_observation_bucket,
    valid_connection_observation,
)
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_transport_observations import audit
from scripts.benchmark_news_connection_recovery import (
    Handler, OwnedLimiter, OwnedScopedSession, owned_config, owned_servers,
    observation_benchmark,
)
from test_news_transport_observations_audit import publish_status


def snapshot(**changes):
    return {'contract_version': 1, 'socket_open_at_checkout': True, 'previous_request_count': 2,
            'idle_since_previous_terminal_seconds': .5, 'connect_attempts': 0,
            'connect_completed': 0, 'connect_wall_seconds': 0.0, **changes}


@pytest.mark.parametrize('changes', [
    {'contract_version': True}, {'contract_version': 2}, {'socket_open_at_checkout': 1},
    {'previous_request_count': -1}, {'previous_request_count': True}, {'previous_request_count': 2 ** 63},
    {'idle_since_previous_terminal_seconds': None}, {'idle_since_previous_terminal_seconds': -1},
    {'idle_since_previous_terminal_seconds': float('inf')}, {'idle_since_previous_terminal_seconds': True},
    {'connect_attempts': True}, {'connect_attempts': -1}, {'connect_completed': True},
    {'connect_completed': 1}, {'connect_wall_seconds': 1}, {'connect_wall_seconds': -1},
    {'connect_wall_seconds': float('nan')}, {'unexpected_private_url': 'SECRET'},
])
def test_invalid_connection_observation_does_not_become_known_reuse(changes):
    assert not valid_connection_observation(snapshot(**changes))


def test_absence_unknown_and_real_zero_connect_hooks_are_distinct():
    assert not valid_connection_observation(None)
    assert valid_connection_observation(snapshot())
    assert connection_observation_bucket(None) == 'unobserved'
    assert connection_observation_bucket(snapshot()) == 'open_reused_idle_lt_1s'
    fresh = snapshot(socket_open_at_checkout=False, previous_request_count=0,
                     idle_since_previous_terminal_seconds=None,
                     connect_attempts=1, connect_completed=1, connect_wall_seconds=.25)
    assert valid_connection_observation(fresh, request_wall=.3)
    assert not valid_connection_observation(fresh, request_wall=.2)
    assert connection_observation_bucket(fresh) == 'newly_connected'
    assert connection_observation_bucket({**fresh, 'connect_completed': 0}) == 'failed_connect'


@pytest.mark.parametrize('idle,bucket', [(0, 'lt_1s'), (2, 'lt_5s'), (8, 'lt_15s'),
                                        (20, 'lt_60s'), (90, 'lt_300s'), (360, 'ge_300s')])
def test_real_tcp_checkout_records_reuse_and_idle_without_extra_requests(idle, bucket):
    shared_map = dict(pool_classes_by_scheme)
    with owned_servers() as servers:
        session = OwnedScopedSession(servers, observe_connections=True)
        session.trust_env = False
        url = 'https://publisher.test/owned'
        try:
            with session.get(url, stream=True, allow_redirects=False, timeout=(1, 1)) as response:
                assert response.status_code == 200
                first_body = response.content
            first = session.finish_connection_observation(url)
            assert valid_connection_observation(first)
            assert first['connect_attempts'] == first['connect_completed'] == 1
            assert not first['socket_open_at_checkout'] and first['previous_request_count'] == 0
            adapter = session.origin_adapters['https://publisher.test']
            # Fixture time only: no network wait, production state or deadline mutation.
            adapter._news_checkout.connection._news_terminal_at -= idle
            with session.get(url, stream=True, allow_redirects=False, timeout=(1, 1)) as response:
                assert response.content == first_body
            second = session.finish_connection_observation(url)
            assert valid_connection_observation(second)
            assert second['socket_open_at_checkout'] and second['previous_request_count'] == 1
            assert second['connect_attempts'] == second['connect_completed'] == 0
            assert second['connect_wall_seconds'] == 0
            assert second['idle_since_previous_terminal_seconds'] >= idle
            assert connection_observation_bucket(second) == 'open_reused_idle_' + bucket
            assert session.finish_connection_observation(url) == second  # Idempotent terminal accounting.
            assert servers['publisher.test'].connections == 1
            assert servers['publisher.test'].paths == ['/owned', '/owned']
            assert pool_classes_by_scheme == shared_map
            assert all(not adapter.poolmanager.pool_classes_by_scheme[scheme] is shared_map[scheme]
                       for scheme in ('http', 'https'))
        finally:
            session.close()


def test_real_full_chain_failed_warm_socket_has_recoverable_private_evidence(tmp_path):
    config = owned_config()
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    with owned_servers() as servers:
        http = NewsHTTP(registry, config, tmp_path,
                        session_factory=lambda: OwnedScopedSession(servers, observe_connections=True),
                        address_check=lambda _: None, limiter_factory=OwnedLimiter, metrics=queue.metrics)
        try:
            assert http._raw('https://m.publisher.test/ok').status == 200
            with pytest.raises(NewsError, match='transport_error'):
                http._raw('https://m.publisher.test/fail/original')
            paths = list((tmp_path / 'transport_problems/publisher').glob('*/*.json'))
            assert len(paths) == 1
            event = json.loads(paths[0].read_text())
            observation = event['connection_observation']
            assert valid_connection_observation(observation, request_wall=event['request_elapsed_seconds'])
            assert observation['socket_open_at_checkout'] and observation['connect_attempts'] == 0
            assert 'PRIVATE' not in json.dumps(observation)
            assert event['phase'] == 'awaiting_headers' and event['http_status'] is None
            assert not event['accepted_as_article'] and not event['body_stored']
            assert http._raw('https://m.publisher.test/recovered').status == 200
            publish_status(queue, http)
            result = audit(tmp_path, registry, ['publisher'])
            assert result['accepted'] and result['connection_reuse_observation_contract_matches']
            row = result['providers']['publisher']
            assert row['connection_checkout_counts_reconciled']
            assert row['failure_connection_buckets'] == {'open_reused_idle_lt_1s': 1}
            assert row['verified_failure_events'][0]['connection_observation'] == observation
            assert servers['m.publisher.test'].paths == ['/ok', '/fail/original', '/recovered']
            assert servers['m.publisher.test'].connections == 2
            assert http.metrics.snapshot()['providers']['publisher']['counts']['failed_origin_pools_reset'] == 1
        finally:
            http.close()
            queue.close()


@pytest.mark.parametrize('marker', [None, False, True, 0, 2, '1'])
def test_present_invalid_contract_is_not_silently_legacy(tmp_path, marker):
    config = owned_config()
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        status = queue.status(activity='running')
        status['performance']['connection_reuse_observation_contract_version'] = marker
        from downloader.artifact_io import atomic_write_json
        atomic_write_json(tmp_path / 'status.json', status)
        result = audit(tmp_path, registry, ['publisher'])
        assert not result['accepted'] and not result['connection_reuse_observation_contract_absent_legacy']
    finally:
        queue.close()


def test_known_buckets_are_bounded_and_host_provider_totals_reconcile():
    metrics = NewsMetrics()
    for idle in (0, 2, 8, 20, 90, 360):
        metrics.request_started('publisher', 'article_html')
        metrics.request_finished('publisher', 'article_html', 200, 'complete', .5, .01,
            host='publisher.test', phase='complete', headers_wall=.4,
            connection_observation=snapshot(idle_since_previous_terminal_seconds=idle))
    counts = metrics.snapshot()['providers']['publisher']['counts']
    keys = [key.removeprefix('connection_checkout_') for key in counts if key.startswith('connection_checkout_')]
    assert set(keys) <= CONNECTION_BUCKETS and sum(counts['connection_checkout_' + key] for key in keys) == 6


@pytest.mark.parametrize('wall', [True, -1, float('nan'), float('inf'), '1'])
def test_invalid_outer_clock_cannot_validate_connect_hook(wall):
    assert not valid_connection_observation(snapshot(), request_wall=wall)


@pytest.mark.parametrize('corruption', ['missing', 'duplicate', 'unknown_bucket', 'negative', 'host_changed'])
def test_connection_terminal_accounting_corruption_rejects_readonly_acceptance(tmp_path, corruption):
    from downloader.artifact_io import atomic_write_json
    config = owned_config()
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.metrics.request_started('publisher', 'article_html')
        queue.metrics.request_finished('publisher', 'article_html', 200, 'complete', .5, .01,
            host='publisher.test', phase='complete', headers_wall=.4, connection_observation=snapshot())
        status = queue.status(activity='running')
        provider = status['performance']['providers']['publisher']
        counts = provider['counts']
        key = 'connection_checkout_open_reused_idle_lt_1s'
        if corruption == 'missing':
            counts.pop(key)
        elif corruption == 'duplicate':
            counts[key] += 1
        elif corruption == 'unknown_bucket':
            counts['connection_checkout_guessed'] = counts.pop(key)
        elif corruption == 'negative':
            counts[key] = -1
        else:
            host = provider['request_hosts']['publisher.test']['counts']
            host['connection_checkout_newly_connected'] = host.pop(key)
        atomic_write_json(tmp_path / 'status.json', status)
        result = audit(tmp_path, registry, ['publisher'])
        assert not result['accepted']
        assert result['providers']['publisher']['connection_checkout_counts_reconciled'] is False
    finally:
        queue.close()


def test_real_https_observation_preserves_certificate_and_hostname_verification(tmp_path):
    certificate, key = tmp_path / 'certificate.pem', tmp_path / 'key.pem'
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt',
        'ec_paramgen_curve:prime256v1', '-sha256', '-days', '1', '-nodes',
        '-keyout', str(key), '-out', str(certificate), '-subj', '/CN=localhost',
        '-addext', 'subjectAltName=DNS:localhost'], check=True, capture_output=True)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    server.observation_lock, server.connections, server.paths = threading.Lock(), 0, []
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(certificate), str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
    thread.start()
    session = requests.Session()
    adapter = ConnectionObservingAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
    session.mount('https://', adapter)
    session.trust_env = False
    url = f'https://localhost:{server.server_port}/owned'
    try:
        for index in range(2):
            with session.get(url, verify=str(certificate), stream=True, timeout=(1, 1)) as response:
                assert response.status_code == 200 and response.content
            observation = adapter.finish_connection_observation()
            assert valid_connection_observation(observation)
            assert observation['connect_attempts'] == (1 if index == 0 else 0)
            assert observation['socket_open_at_checkout'] is (index == 1)
        assert server.connections == 1 and server.paths == ['/owned', '/owned']
        with pytest.raises(requests.exceptions.SSLError):
            session.get(url, verify=True, timeout=(1, 1))
        observation = adapter.finish_connection_observation()
        assert observation['connect_attempts'] == 1 and observation['connect_completed'] == 0
        assert connection_observation_bucket(observation) == 'failed_connect'
        with pytest.raises(requests.exceptions.SSLError):
            session.get(f'https://127.0.0.1:{server.server_port}/owned', verify=str(certificate), timeout=(1, 1))
        assert server.paths == ['/owned', '/owned']  # Rejected handshakes sent no HTTP request.
    finally:
        session.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_complete_owned_capture_pair_preserves_sources_receipts_recovery_and_tcp_count():
    result = observation_benchmark(rounds=1, cycles=1)
    assert result['accepted'] and len(result['phases']) == 4
    assert all(row['body_receipt_proof']['accepted'] for row in result['phases'])
    assert result['publisher_requests'] == 0 and not result['production_queue_writes']
