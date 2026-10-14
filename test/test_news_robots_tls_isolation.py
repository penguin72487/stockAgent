"""REP certificate failures deny that origin; other-host proof is not permission."""
from contextlib import closing
from copy import deepcopy
import errno
import hashlib
import json
import ssl
import time

import pytest
import requests

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_capacity import _completed_resume_pressure
from downloader.news_fulltext_policy import NewsError
from test_news_fulltext import FakeResponse, config, http_fixture
from test_news_fulltext_performance import FailedResponse, UnreadErrorResponse
from test_news_capacity import controller_fixture
from scripts.audit_news_transport_observations import _verify_event


GOOD = 'https://good.publisher.test/article'
BAD = 'https://bad.publisher.test/article'
REP = 'https://bad.publisher.test/robots.txt'
ORIGIN = 'https://bad.publisher.test'


def certificate_error(*, route=False, certificate=True):
    error = requests.exceptions.SSLError('private source error never exported')
    if certificate:
        error.__cause__ = ssl.SSLCertVerificationError(1, 'certificate does not match host')
        if route:
            error.__cause__.__cause__ = OSError(errno.ENETUNREACH, 'route failure')
    return error


def frozen_clock(monkeypatch):
    clock = [1900000000.0, 0.0]
    # Wall advances at the explicit recovery boundaries. Small monotonic
    # ticks let the REAL synthetic request pacer make progress; a fixed
    # monotonic clock would wait forever rather than test the failure logic.
    def monotonic():
        clock[1] += 0.01
        return clock[0] + clock[1]
    monkeypatch.setattr('downloader.news_fulltext_http.time.time', lambda: clock[0])
    monkeypatch.setattr('downloader.news_fulltext_http.time.monotonic', monotonic)
    return clock


def test_real_completed_other_host_proof_isolates_rep_certificate_failure_without_disabling_tls(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {GOOD: FakeResponse(), REP: FailedResponse(certificate_error())})
    with closing(http):
        http._raw(GOOD)
        with pytest.raises(NewsError, match='transport_error') as failure:
            http.get(BAD)
        assert failure.value.robots_origin_isolation
        assert failure.value.robots_pressure_corroboration['isolated']
        assert http.provider_not_before.get('publisher', 0) == 0
        assert http.blocked_robots_origins('publisher')[ORIGIN] > time.time()
        assert len(session.calls) == 2 and all(call[1].get('verify', True) is True for call in session.calls)
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['http_terminal_requests'] == 2 and counts['http_terminal_responses'] == 1
        assert counts['http_terminal_transport_errors'] == counts['http_terminal_isolated_robots_origin_transport_errors'] == 1
        before = len(session.calls)
        with pytest.raises(NewsError, match='robots_origin_policy_cooldown') as deferred:
            http.get(BAD)
        assert deferred.value.request_deferred and len(session.calls) == before
        assert http._raw(GOOD).status == 200
        events = list(tmp_path.glob('transport_problems/publisher/*/*.json'))
        assert len(events) == 1
        event = json.loads(events[0].read_bytes())
        assert event['http_status'] is None and event['response_bytes_seen'] == 0
        assert not event['body_stored'] and not event['accepted_as_article']
        proof = event['robots_origin_policy']['capacity_pressure']
        assert proof['corroborating_hosts'] == ['good.publisher.test'] and proof['isolated']
        assert _verify_event(events[0], event, 'publisher', http.registry.sources['publisher'], http.registry)


@pytest.mark.parametrize('health', ['unknown', 'stale', 'later_failed'])
def test_no_current_other_host_proof_keeps_conservative_provider_pressure(config, tmp_path, monkeypatch, health):
    clock = frozen_clock(monkeypatch)
    http, session = http_fixture(config, tmp_path, {GOOD: FakeResponse(), REP: FailedResponse(certificate_error())})
    with closing(http):
        if health != 'unknown':
            http._raw(GOOD)
            if health == 'stale':
                clock[0] += 121
            else:
                http.host_circuits.failure('publisher', 'good.publisher.test')
        with pytest.raises(NewsError, match='transport_error') as failure:
            http.get(BAD)
        assert failure.value.robots_origin_isolation
        assert not failure.value.robots_pressure_corroboration['isolated']
        assert http.provider_not_before['publisher'] > clock[0]
        assert not http.metrics.snapshot()['providers']['publisher']['counts'].get('http_terminal_isolated_robots_origin_transport_errors', 0)
        assert http.blocked_robots_origins('publisher')[ORIGIN] > clock[0]


@pytest.mark.parametrize('route,certificate,robots', [(True, True, True), (False, False, True), (False, True, False)])
def test_unknown_tls_routes_and_non_rep_certificate_failure_still_trip_provider(config, tmp_path, route, certificate, robots):
    http, _ = http_fixture(config, tmp_path, {GOOD: FakeResponse(), (REP if robots else BAD): FailedResponse(
        certificate_error(route=route, certificate=certificate))})
    with closing(http):
        http._raw(GOOD)
        with pytest.raises(NewsError, match='transport_error'):
            http._raw(REP if robots else BAD, **({'robots_origin': ORIGIN} if robots else {}))
        assert http.provider_not_before['publisher'] > time.time()
        assert not http.metrics.snapshot()['providers']['publisher']['counts'].get('http_terminal_isolated_robots_origin_transport_errors', 0)


def test_negative_rep_checkpoint_survives_restart_and_backoff_until_real_rep_recovery(config, tmp_path, monkeypatch):
    clock = frozen_clock(monkeypatch)
    routes = {GOOD: FakeResponse(), REP: FailedResponse(certificate_error()), BAD: FakeResponse(b'actual article')}
    http, session = http_fixture(config, tmp_path, routes)
    path = tmp_path / 'robots' / (hashlib.sha256(ORIGIN.encode()).hexdigest() + '.json')
    try:
        for generation, seconds in enumerate((30, 60, 120, 240, 300, 300), 1):
            http._raw(GOOD)
            with pytest.raises(NewsError, match='transport_error'):
                http.get(BAD)
            saved = json.loads(path.read_bytes())['policy_retry']
            assert saved['generation'] == min(generation, 5) and saved['delay_seconds'] == seconds
            assert BAD not in [call[0] for call in session.calls]
            original_bytes = path.read_bytes()
            http.close()
            http, session = http_fixture(config, tmp_path, routes)
            with pytest.raises(NewsError, match='robots_origin_policy_cooldown'):
                http.get(BAD)
            assert not session.calls and path.read_bytes() == original_bytes
            clock[0] = saved['not_before_epoch'] + 0.01
        routes[REP] = FakeResponse(b'User-agent: *\nAllow: /', headers={'Content-Type': 'text/plain'})
        assert http.get(BAD).body == b'actual article'
        assert [c[0] for c in session.calls] == [REP, BAD]
        assert not http.blocked_robots_origins('publisher')
        assert 'policy_retry' not in json.loads(path.read_bytes())
    finally:
        http.close()


def test_failed_probe_cannot_reuse_cached_allow_or_conditional_etag_as_permission(config, tmp_path, monkeypatch):
    clock = frozen_clock(monkeypatch)
    path = tmp_path / 'robots' / (hashlib.sha256(ORIGIN.encode()).hexdigest() + '.json')
    atomic_write_json(path, {'origin': ORIGIN, 'checked_at': clock[0] - config['robots_ttl_seconds'] - 1,
                            'status': 200, 'body': 'User-agent: *\nAllow: /', 'etag': 'previous-rep'})
    http, session = http_fixture(config, tmp_path, {GOOD: FakeResponse(), REP: FailedResponse(certificate_error())})
    with closing(http):
        http._raw(GOOD)
        with pytest.raises(NewsError):
            http.get(BAD)
        saved = json.loads(path.read_bytes())
        assert saved['body'] == 'User-agent: *\nAllow: /' and saved['policy_retry']['error_code'] == 'transport_error'
        assert ORIGIN not in http.robots_parsers and len(session.calls) == 2
        assert session.calls[-1][1]['headers']['If-None-Match'] == 'previous-rep'


@pytest.mark.parametrize('status,headers', [(403, {}), (429, {'Retry-After': '90'}),
    (503, {'Retry-After': '60'}), (502, {'cf-mitigated': 'challenge'}), (502, {'Retry-After': '80'})])
def test_explicit_source_signals_never_become_isolated_certificates(config, tmp_path, status, headers):
    http, _ = http_fixture(config, tmp_path, {GOOD: FakeResponse(), REP: UnreadErrorResponse(status=status, headers=headers)})
    with closing(http):
        http._raw(GOOD)
        with pytest.raises(NewsError):
            http.get(BAD)
        assert http.provider_not_before['publisher'] > time.time()
        assert not http.metrics.snapshot()['providers']['publisher']['counts'].get('http_terminal_isolated_robots_origin_transport_errors', 0)


def test_pressure_preserves_raw_failure_and_exact_non_http_denominators(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, maximum=1)
    with closing(http):
        for _ in range(100):
            http.metrics.request_started('publisher', 'article_html')
            http.metrics.request_finished('publisher', 'article_html', 200, 'complete', 1, 0)
        for _ in range(40):
            http.metrics.request_started('publisher', 'robots')
            http.metrics.request_finished('publisher', 'robots', None, 'transport_error', 1, 0,
                                          error_type='SSLError', isolated_robots_origin=True)
        http.metrics.count('publisher', 'durably_accepted_new_url_heads', 100)
        http.metrics.count('publisher', 'durably_accepted_versions', 100)
        clock.value += 10
        controller.tick()
        sample = controller.states['publisher']['last_window']
        assert sample['requests'] == 140 and sample['responses'] == 100
        assert sample['transport_or_server_errors'] == sample['isolated_robots_origin_transport_errors'] == 40
        assert sample['pressure_request_results'] == sample['pressure_response_results'] == 100
        assert sample['pressure_transport_or_server_errors'] == 0
        assert sample['healthy'] and not sample['all_observed_hosts_healthy'] and sample['new_stored_url_heads'] == 100
        assert _completed_resume_pressure(sample) == (100, 100, 0)


@pytest.mark.parametrize('patch', [
    {'isolated_robots_origin_transport_errors': 41}, {'isolated_robots_origin_transport_errors': True},
    {'pressure_request_results': 99}, {'pressure_response_results': 60},
    {'pressure_transport_or_server_errors': 1}, {'provisional_gateway_host_errors': 1},
])
def test_pressure_resume_cannot_forge_missing_http_results_or_remove_unproved_errors(patch):
    sample = {'requests': 150, 'responses': 110, 'transport_or_server_errors': 50,
        'isolated_gateway_host_errors': 5, 'isolated_robots_origin_errors': 5,
        'isolated_robots_origin_transport_errors': 40, 'provisional_gateway_host_errors': 0,
        'pressure_request_results': 100, 'pressure_response_results': 100, 'pressure_transport_or_server_errors': 0}
    assert _completed_resume_pressure(sample) == (100, 100, 0)
    assert _completed_resume_pressure({**sample, **patch}) is None
    incomplete = deepcopy(sample)
    incomplete.pop('pressure_response_results')
    assert _completed_resume_pressure(incomplete) is None
