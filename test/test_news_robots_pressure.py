"""REP denial, pressure corroboration and durable-body acceptance are distinct."""
from contextlib import closing
import time

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_transport_observations import audit, _verify_robots_context
from test_news_fulltext import FakeResponse, config, http_fixture
from test_news_fulltext_performance import UnreadErrorResponse
from test_news_capacity import controller_fixture
from test_news_transport_observations_audit import publish_status
from test_news_robots_origin_recovery import ALLOW, BAD, GOOD, cache_path


@pytest.mark.parametrize('status', [502, 504])
def test_real_rep_failure_after_completed_other_host_keeps_denial_and_raw_error(config, tmp_path, status):
    http, session = http_fixture(config, tmp_path, {
        GOOD + '/robots.txt': FakeResponse(ALLOW), GOOD + '/article': FakeResponse(b'body'),
        BAD + '/robots.txt': UnreadErrorResponse(status=status)})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        assert http.get(GOOD + '/article').body == b'body'
        with pytest.raises(NewsError, match='http_' + str(status)) as failure:
            http.get(BAD + '/article')
        proof = failure.value.robots_pressure_corroboration
        assert proof['isolated'] and proof['corroborating_hosts'] == ['good.publisher.test']
        checkpoint = sha256_file(cache_path(tmp_path))
        deadline = http.blocked_robots_origins('publisher')[BAD]
        with pytest.raises(NewsError, match='robots_origin_policy_cooldown'):
            http.get(BAD + '/different')
        assert sha256_file(cache_path(tmp_path)) == checkpoint
        assert http.blocked_robots_origins('publisher')[BAD] == deadline
        assert http.get(GOOD + '/article').body == b'body'
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['http_terminal_isolated_robots_origin_errors'] == counts['http_terminal_server_errors'] == 1
        assert counts['http_endpoint_robots_outcome_robots_origin_policy_unreachable'] == 1
        assert counts['http_terminal_requests'] == counts['http_terminal_responses'] == 4
        assert not http.provider_not_before
        assert not any(url == BAD + '/article' for url, _ in session.calls)
        publish_status(queue, http)
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['accepted'] and not result['publisher_requests']
        event = result['providers']['publisher']['verified_robots_origin_decisions'][0]
        assert event['capacity_pressure'] == proof and not event['permission_granted']
        assert not event['body_accepted'] and result['providers']['publisher']['recovery_counts'][
            'http_terminal_isolated_robots_origin_errors'] == 1


@pytest.mark.parametrize('health', ['none', 'stale', 'future', 'failed', 'paused', 'headers_only'])
def test_unknown_or_failed_other_host_never_isolates_pressure(config, tmp_path, health):
    http, _ = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http):
        host = 'good.publisher.test'
        if health not in {'none', 'headers_only'}:
            http.host_circuits.success('publisher', host)
        if health == 'stale':
            http.host_circuits.successes['publisher'][host] -= 121
        elif health == 'future':
            http.host_circuits.successes['publisher'][host] += 10
        elif health == 'failed':
            http.host_circuits.failure('publisher', host)
        elif health == 'paused':
            http.host_circuits.paused['publisher'] = {host: time.time() + 30}
        elif health == 'headers_only':
            http.metrics.response_received('publisher', 'article_html', 200)
        with pytest.raises(NewsError) as failure:
            http.get(BAD + '/article')
        assert not failure.value.robots_pressure_corroboration['isolated']
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['http_terminal_server_errors'] == 1
        assert not counts.get('http_terminal_isolated_robots_origin_errors', 0)
        assert BAD in http.blocked_robots_origins('publisher')


def test_redirect_cannot_use_initial_or_failed_response_host_as_corroboration(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        BAD + '/robots.txt': FakeResponse(status=302, headers={'Location': GOOD + '/policy'}),
        GOOD + '/policy': UnreadErrorResponse(status=502)})
    with closing(http):
        http.host_circuits.success('publisher', 'bad.publisher.test')
        http.host_circuits.success('publisher', 'good.publisher.test')
        with pytest.raises(NewsError) as failure:
            http.get(BAD + '/article')
        assert not failure.value.robots_pressure_corroboration['isolated']
        assert set(http.blocked_robots_origins('publisher')) == {BAD}


def terminal(http, status=200, endpoint='article_html', outcome='complete', *, isolated=False, n=1):
    for _ in range(n):
        http.metrics.request_started('publisher', endpoint)
        http.metrics.request_finished('publisher', endpoint, status, outcome, 1, 0,
                                      isolated_robots_origin=isolated)


@pytest.mark.parametrize('body_heads', [0, 100])
def test_capacity_does_not_slow_healthy_hosts_for_corroborated_rep_failure(config, tmp_path, body_heads):
    http, controller, clock = controller_fixture(config, tmp_path, slots=3)
    with closing(http):
        terminal(http, n=100)
        terminal(http, 502, 'robots', 'robots_origin_policy_unreachable', isolated=True, n=10)
        http.metrics.count('publisher', 'durably_accepted_new_url_heads', body_heads)
        http.metrics.count('publisher', 'durably_accepted_versions', body_heads)
        clock.value += 10
        controller.tick()
        sample = controller.states['publisher']['last_window']
        assert sample['transport_or_server_errors'] == sample['isolated_robots_origin_errors'] == 10
        assert sample['requests'] == sample['responses'] == 110
        assert sample['pressure_request_results'] == sample['pressure_response_results'] == 100
        assert sample['pressure_transport_or_server_errors'] == 0
        assert sample['healthy'] and not sample['all_observed_hosts_healthy']
        assert sample['new_stored_url_heads'] == body_heads
        assert sample['durable_versions_per_request'] == body_heads / 110
        assert controller.states['publisher']['state'] != 'recovering_transport'
        if not body_heads:
            assert controller.states['publisher']['state'] == 'insufficient_useful_data'
            assert (controller.states['publisher']['interval_seconds'], http.parallelism['publisher']) == (1, 3)


def test_only_rep_errors_cannot_supply_healthy_denominator_or_capacity(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    with closing(http):
        terminal(http, 502, 'robots', 'robots_origin_policy_unreachable', isolated=True, n=100)
        clock.value += 10
        controller.tick()
        assert 'last_window' not in controller.states['publisher']
        assert http.client_intervals['publisher'] == 1


def test_uncorroborated_rep_and_correlated_other_errors_retain_pressure(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, slots=3)
    with closing(http):
        terminal(http, n=100)
        terminal(http, 502, 'robots', 'robots_origin_policy_unreachable', isolated=True)
        terminal(http, 504, 'robots', 'robots_origin_policy_unreachable', n=2)
        terminal(http, 503, outcome='source_status_rejected', n=2)
        clock.value += 10
        controller.tick()
        sample = controller.states['publisher']['last_window']
        assert sample['isolated_robots_origin_errors'] == 1
        assert sample['transport_or_server_errors'] == 5 and sample['pressure_transport_or_server_errors'] == 4
        assert not sample['healthy'] and controller.states['publisher']['state'] == 'recovering_transport'
        assert http.parallelism['publisher'] == 2


@pytest.mark.parametrize('endpoint,status,outcome', [
    ('article_html', 502, 'robots_origin_policy_unreachable'),
    ('robots', 503, 'robots_origin_policy_unreachable'),
    ('robots', 429, 'robots_origin_policy_unreachable'),
    ('robots', 502, 'source_status_rejected'),
    ('robots', 200, 'complete')])
def test_metric_isolation_flag_cannot_reclassify_a_different_result(endpoint, status, outcome):
    metrics = NewsMetrics()
    metrics.request_started('publisher', endpoint)
    metrics.request_finished('publisher', endpoint, status, outcome, 1, 0, isolated_robots_origin=True)
    assert not metrics.snapshot()['providers']['publisher']['counts'].get('http_terminal_isolated_robots_origin_errors')


@pytest.mark.parametrize('mutation', ['version', 'basis', 'bool', 'same_origin', 'failed_target',
    'unregistered', 'port', 'duplicates', 'stale', 'negative', 'nan', 'extra', 'mismatch', 'over_bound'])
def test_private_receipt_pressure_corroboration_rejects_invalid_evidence(config, tmp_path, mutation):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        source = http.registry.sources['publisher']
        proof = {'contract_version': 1, 'isolated': True,
            'basis': 'recent_completed_other_registered_host; not_permission_or_body_acceptance',
            'corroborating_hosts': ['good.publisher.test'], 'success_ages_seconds': {'good.publisher.test': 1.0}}
        if mutation in {'same_origin', 'failed_target', 'unregistered', 'port'}:
            host = {'same_origin': 'bad.publisher.test', 'failed_target': 'redirect.publisher.test',
                    'unregistered': 'evil.test', 'port': 'good.publisher.test:443'}[mutation]
            proof.update(corroborating_hosts=[host], success_ages_seconds={host: 1.0})
        elif mutation == 'duplicates':
            proof['corroborating_hosts'] *= 2
        elif mutation in {'stale', 'negative', 'nan'}:
            proof['success_ages_seconds']['good.publisher.test'] = {'stale': 121, 'negative': -1, 'nan': float('nan')}[mutation]
        elif mutation == 'over_bound':
            hosts = sorted('h' + str(i) + '.publisher.test' for i in range(33))
            proof.update(corroborating_hosts=hosts, success_ages_seconds=dict.fromkeys(hosts, 1.0))
        else:
            proof[{'version': 'contract_version', 'basis': 'basis', 'bool': 'isolated',
                   'extra': 'extra', 'mismatch': 'success_ages_seconds'}[mutation]] = {
                'version': True, 'basis': 'guessed', 'bool': 1, 'extra': False, 'mismatch': {}}[mutation]
        event = {'requested_url': 'https://redirect.publisher.test/policy', 'robots_origin_policy': {
            'contract_version': 1, 'initial_origin': BAD, 'scope': 'initial_origin_policy', 'permission_granted': False,
            'source_data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
            'capacity_pressure': proof}}
        with pytest.raises((ValueError, NewsError)):
            _verify_robots_context(event, 'publisher', http.registry)


def test_capacity11_forecast_uses_same_complete_window_math():
    from test_news_completion_forecast import project
    legacy = project(measurement_contract_version=10)
    assert project(measurement_contract_version=11) == legacy
