"""Transport recovery must survive valid body-workload changes."""
from types import SimpleNamespace
import time

import pytest
import requests

from downloader.news_fulltext_capacity import PublisherCapacity, MEASUREMENT_CONTRACT_VERSION
from downloader.news_fulltext_policy import NewsError
from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_capacity_feedback import audit
from test_news_capacity import controller_fixture, observe, config
from test_news_fulltext import FakeResponse, http_fixture
from test_news_fulltext_performance import FailedResponse, UnreadErrorResponse


def workload_window(http, controller, clock, endpoints, *, errors=0, heads=10):
    for endpoint, count in endpoints.items():
        http.metrics.count('publisher', 'endpoint_' + endpoint + '_requests', count)
    return observe(http, controller, clock, requests=sum(endpoints.values()), heads=heads, errors=errors)


def test_feedback_cli_defaults_to_all_enabled_sources_without_network(config, tmp_path, monkeypatch):
    from scripts import audit_news_capacity_feedback as module
    root = tmp_path / config['root']
    http, session = http_fixture(config, root, {})
    queue = NewsQueue(root, http.registry)
    try:
        atomic_write_json(root / 'status.json', {'owner_id': queue.owner, 'updated_at_utc': '2026-10-10T00:00:00+00:00',
            'http_capacity': {}, 'performance': http.metrics.snapshot()})
        configured = tmp_path / 'config.json'
        atomic_write_json(configured, config)
        monkeypatch.setattr(module, 'load_config', lambda path: (config, http.registry, tmp_path))
        output = tmp_path / 'feedback.json'
        before = queue.db.total_changes
        assert module.main(['--config', str(configured), '--output', str(output)]) == 0
        import json
        result = json.loads(output.read_text())
        assert result['contract_version'] == 3 and set(result['providers']) == {'publisher'}
        assert result['publisher_requests'] == 0 and not result['source_states_mutated']
        assert not session.calls and queue.db.total_changes == before
    finally:
        queue.close()
        http.close()


def test_healthy_mixed_window_does_not_starve_transport_recovery(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        item = workload_window(http, controller, clock, {'article_html': 100}, errors=2)
        assert item['state'] == 'recovering_transport'
        assert workload_window(http, controller, clock, {'article_html': 100})['state'] == 'recovering_transport'
        item = workload_window(http, controller, clock, {'article_html': 60, 'cnyes_headline': 40})
        # Two healthy transport windows at the same pressure suffice to
        # restore only the reviewed baseline, not a different family's peak.
        assert item['state'] == 'warming'
        assert item['interval_seconds'] == 1 and item['parallelism'] == 1
        assert http.metrics.snapshot()['providers']['publisher']['counts'].get(
            'transient_recovery_prior_capacity_restored', 0) == 0
    finally:
        http.close()


def test_unhealthy_window_resets_cross_workload_recovery(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        workload_window(http, controller, clock, {'article_html': 100}, errors=2)
        assert workload_window(http, controller, clock, {'article_html': 100})['transport_stable_samples'] == 1
        assert workload_window(http, controller, clock, {'rss_fulltext': 100}, errors=2)['transport_stable_samples'] == 0
        assert workload_window(http, controller, clock, {'article_html': 100})['state'] == 'recovering_transport'
        assert workload_window(http, controller, clock, {'rss_fulltext': 100})['state'] == 'warming'
    finally:
        http.close()


def test_owner_restart_does_not_reuse_partial_transport_streak(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        workload_window(http, controller, clock, {'article_html': 100}, errors=2)
        assert workload_window(http, controller, clock, {'article_html': 100})['transport_stable_samples'] == 1
        resumed = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert resumed.snapshot()['measurement_contract_version'] == MEASUREMENT_CONTRACT_VERSION == 11
        assert resumed.states['publisher']['transport_stable_samples'] == 0
        assert workload_window(http, resumed, clock, {'rss_fulltext': 100})['state'] == 'recovering_transport'
    finally:
        http.close()


def test_repeated_errors_hold_one_recovery_step_across_workload_and_owner_changes(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, slots=3)
    try:
        item = workload_window(http, controller, clock, {'article_html': 100}, errors=2)
        pressure = (item['interval_seconds'], item['parallelism'])
        assert pressure == (2, 2) and item['transport_recovery_active']
        for endpoints in ({'article_html': 100}, {'article_html': 60, 'cnyes_headline': 40},
                          {'cnyes_headline': 100}):
            item = workload_window(http, controller, clock, endpoints, errors=2)
            assert (item['interval_seconds'], item['parallelism']) == pressure
            assert item['state'] == 'recovering_transport' and item['transport_stable_samples'] == 0
        resumed = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert resumed.states['publisher']['transport_recovery_active']
        item = workload_window(http, resumed, clock, {'article_html': 100}, errors=2)
        assert (item['interval_seconds'], item['parallelism']) == pressure
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['transient_recovery_pressure_reduced_once'] == 1
        assert counts['transient_recovery_pressure_held'] == 4
        assert resumed.snapshot()['official_maximum_claimed'] is False
    finally:
        http.close()


def test_recovery_restores_only_after_health_and_starts_a_new_single_step_episode(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, slots=3)
    try:
        workload_window(http, controller, clock, {'article_html': 100}, errors=2)
        # A representation error is not a new load experiment or permission
        # to forget the already-reduced transport pressure.
        http.metrics.count('publisher', 'http_status_406', 2)
        item = workload_window(http, controller, clock, {'article_html': 100})
        assert item['state'] == 'endpoint_validation_failed' and item['transport_recovery_active']
        assert workload_window(http, controller, clock, {'article_html': 100})['state'] == 'recovering_transport'
        item = workload_window(http, controller, clock, {'article_html': 100})
        assert item['state'] == 'warming' and not item['transport_recovery_active']
        assert (item['interval_seconds'], item['parallelism']) == (1, 3)
        item = workload_window(http, controller, clock, {'article_html': 100}, errors=2)
        assert (item['interval_seconds'], item['parallelism']) == (2, 2)
        assert http.metrics.snapshot()['providers']['publisher']['counts']['transient_recovery_pressure_reduced_once'] == 2
    finally:
        http.close()


@pytest.mark.parametrize('invalid', [None, 1, 'true', {}])
def test_corrupt_persisted_recovery_marker_is_not_an_active_episode(config, tmp_path, invalid):
    http, controller, clock = controller_fixture(config, tmp_path, slots=3)
    try:
        workload_window(http, controller, clock, {'article_html': 100}, errors=2)
        payload = controller.snapshot()
        payload['providers']['publisher']['transport_recovery_active'] = invalid
        atomic_write_json(controller.path, payload)
        resumed = PublisherCapacity(http, config, clock=clock, epoch=clock)
        item = resumed.states['publisher']
        assert not item['transport_recovery_active'] and item['transport_stable_samples'] == 0
        assert (item['interval_seconds'], item['parallelism']) == (1, 3)
        assert item['state'] == 'warming'
    finally:
        http.close()


@pytest.mark.parametrize('status', [403, 429, 503])
def test_recovery_pressure_never_bypasses_refusals_overload_or_retry_after(config, tmp_path, status):
    http, controller, clock = controller_fixture(config, tmp_path, slots=3)
    try:
        workload_window(http, controller, clock, {'article_html': 100}, errors=2)
        http.metrics.count('publisher', 'http_status_' + str(status), 1)
        with pytest.raises(NewsError):
            http._response_status('publisher', status, {'retry-after': '90'})
        deadline = http.provider_not_before['publisher']
        assert deadline > time.time()
        item = workload_window(http, controller, clock, {'article_html': 100})
        assert item['state'] == 'waiting_source'
        assert http.provider_not_before['publisher'] == deadline
        if status in {403, 429}:
            assert not item['transport_recovery_active']
        if status == 429:
            assert http.effective_interval('publisher') >= 4
    finally:
        http.close()


def test_terminal_denominator_and_endpoint_keep_late_errors_with_actual_workload(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        # Starts are mostly a NEW bulk workload, but completions belong to an
        # earlier HTML batch. Early headers/inflight starts are not outcomes.
        for key, value in {'http_terminal_requests': 10, 'http_terminal_responses': 8,
                           'http_terminal_transport_errors': 2, 'http_terminal_status_200': 8,
                           'http_endpoint_article_html_terminal_requests': 10}.items():
            http.metrics.count('publisher', key, value)
        item = workload_window(http, controller, clock, {'cnyes_headline': 200})
        sample = item['last_window']
        assert sample['request_measurement_basis'] == 'terminal_results'
        assert sample['requests'] == 10 and sample['responses'] == 8 and sample['started_requests'] == 200
        assert sample['actual_requests_per_second'] == 20
        assert sample['workload_family'] == 'article_html'
        assert sample['transport_or_server_errors'] == 2 and not sample['healthy']
        assert item['state'] == 'recovering_transport'
    finally:
        http.close()


def test_terminal_deadline_errors_prevent_false_http_200_health(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        for key, value in {'http_terminal_requests': 100, 'http_terminal_responses': 100,
                           'http_terminal_deadline_errors': 2, 'http_terminal_status_200': 100,
                           'http_endpoint_article_html_terminal_requests': 100}.items():
            http.metrics.count('publisher', key, value)
        item = workload_window(http, controller, clock, {'article_html': 100})
        assert item['state'] == 'recovering_transport'
        assert item['last_window']['transport_or_server_errors'] == 2
        assert item['last_window']['http_status_counts'] == {'200': 100}
        assert not item['last_window']['healthy']
    finally:
        http.close()


@pytest.mark.parametrize('status', [200, 304, 403, 429, 502])
def test_http_terminal_status_and_bounded_endpoint_exactly_once(config, tmp_path, status):
    config['providers'][0]['article_path_pattern'] = r'/news/id/\d+'
    url = 'https://publisher.test/news/id/123?private_not_a_metric=secret'
    response = FakeResponse(b'body', status=status) if status < 400 else UnreadErrorResponse(status=status)
    http, session = http_fixture(config, tmp_path, {url: response})
    try:
        if status < 400:
            assert http._raw(url).status == status
        else:
            with pytest.raises(NewsError):
                http._raw(url)
        snapshot = http.metrics.snapshot()
        counts = snapshot['providers']['publisher']['counts']
        assert snapshot['http_terminal_observation_contract_version'] == 1
        assert snapshot['request_dispatch_clock_contract_version'] == 2
        assert counts['http_requests_started'] == counts['http_terminal_requests'] == 1
        assert counts['http_responses_received'] == counts['http_terminal_responses'] == 1
        assert counts['http_inflight'] == 0
        assert counts['http_endpoint_article_html_terminal_status_' + str(status)] == 1
        assert counts.get('http_terminal_server_errors', 0) == int(status == 502)
        assert all('123' not in key and 'secret' not in key and 'private_not_a_metric' not in key for key in counts)
        assert [key for key in counts if key.startswith('endpoint_')] == ['endpoint_article_html_requests']
        assert len(session.calls) == 1
        stages = snapshot['providers']['publisher']['stages']
        assert stages['transport']['count'] == stages['transport_endpoint_article_html']['count'] == 1
        assert stages['transport']['wall_seconds'] == stages['transport_endpoint_article_html']['wall_seconds']
    finally:
        http.close()


@pytest.mark.parametrize('headers_received', [False, True])
def test_http_transport_failure_records_real_endpoint_before_or_after_headers(config, tmp_path, headers_received):
    config['providers'][0]['article_path_pattern'] = r'/news/id/\d+'
    class BodyFailure(FakeResponse):
        def iter_content(self, **kwargs):
            raise requests.exceptions.ReadTimeout()
    response = BodyFailure() if headers_received else FailedResponse(requests.exceptions.ReadTimeout())
    http, _ = http_fixture(config, tmp_path, {'https://publisher.test/news/id/1': response})
    try:
        with pytest.raises(NewsError, match='transport_error'):
            http._raw('https://publisher.test/news/id/1')
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['http_terminal_requests'] == counts['http_terminal_transport_errors'] == 1
        assert counts['http_endpoint_article_html_transport_error_ReadTimeout'] == 1
        assert counts.get('http_terminal_responses', 0) == int(headers_received)
        assert counts['http_inflight'] == 0
        assert 'publisher' not in http.provider_not_before
    finally:
        http.close()


def test_incomplete_body_deadline_is_terminal_failure_not_complete(monkeypatch, config, tmp_path):
    config['providers'][0]['article_path_pattern'] = r'/news/id/\d+'
    clock = SimpleNamespace(now=10000.)
    class DeadlineResponse(FakeResponse):
        def iter_content(self, **kwargs):
            clock.now += config['response_deadline_seconds'] + 1
            yield b'partial body is not accepted'
    http, _ = http_fixture(config, tmp_path, {'https://publisher.test/news/id/1': DeadlineResponse()})
    monkeypatch.setattr('downloader.news_fulltext_http.time', SimpleNamespace(
        monotonic=lambda: clock.now, thread_time=time.thread_time, time=time.time))
    try:
        with pytest.raises(NewsError, match='response_deadline'):
            http._raw('https://publisher.test/news/id/1')
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['http_terminal_requests'] == counts['response_deadline_errors'] == 1
        assert counts['http_terminal_deadline_errors'] == counts['http_terminal_status_200'] == 1
        assert counts['http_endpoint_article_html_outcome_response_deadline'] == 1
        assert counts.get('http_endpoint_article_html_outcome_complete', 0) == 0
        assert counts.get('http_response_bytes', 0) == 0 and counts['http_inflight'] == 0
    finally:
        http.close()


@pytest.mark.parametrize('calibration', [None, {'measurement_contract_version': 5, 'providers': {
    'publisher': {'state': 'recovering_transport', 'transport_stable_samples': 1, 'transport_recovery_active': True, 'history': [{
        'healthy': True, 'endpoint_request_counts': {'cnyes_headline': 100},
        'endpoint_completed_request_counts': {'article_html': 100},
        'request_measurement_basis': 'terminal_results'}]}}}])
def test_feedback_audit_is_read_only_and_distinguishes_starts_from_terminal_work(config, tmp_path, calibration):
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        atomic_write_json(tmp_path / 'status.json', {'owner_id': queue.owner, 'http_capacity': {'calibration': calibration}})
        before = queue.db.total_changes
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['contract_version'] == 3
        assert result['status_database_owner_coherent']
        assert result['publisher_requests'] == 0 and not session.calls
        assert result['source_states_mutated'] is False and queue.db.total_changes == before
        assert result['history_complete'] is False and result['body_receipts_verified'] is False
        item = result['providers']['publisher']
        if calibration:
            assert item['measurement_contract_version'] == 5
            assert item['calibration']['transport_recovery_active'] is True
            assert item['bounded_windows'][0]['request_workload_counts'] == {'cnyes_bulk': 100}
            assert item['bounded_windows'][0]['terminal_workload_counts'] == {'article_html': 100}
        else:
            assert item['bounded_windows'] == []
    finally:
        queue.close()
        http.close()
