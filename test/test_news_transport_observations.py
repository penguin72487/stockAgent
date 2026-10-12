import hashlib
import json
import stat
import time
from datetime import datetime, timezone

import pytest
import requests
from urllib3.exceptions import ReadTimeoutError

from downloader.news_fulltext_metrics import MAX_REQUEST_HOSTS_PER_PROVIDER, NewsMetrics, metrics_snapshot_clock
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import FakeResponse, http_fixture, config
from test_news_fulltext_performance import FailedResponse, UnreadErrorResponse


def observations(root):
    return list(root.glob('transport_problems/publisher/*/*.json'))


def test_counter_snapshot_clock_is_captured_under_same_lock_and_never_reused(monkeypatch):
    metrics = NewsMetrics()
    captured = []
    class SnapshotClock:
        @staticmethod
        def now(tz):
            assert metrics.lock.locked() and tz is timezone.utc
            stamp = datetime(2026, 10, 10, 9, 0, len(captured), tzinfo=timezone.utc)
            captured.append(stamp)
            return stamp
    monkeypatch.setattr('downloader.news_fulltext_metrics.datetime', SnapshotClock)
    metrics.count('publisher', 'accepted', 1)
    first = metrics.snapshot()
    metrics.count('publisher', 'accepted', 2)
    second = metrics.snapshot()
    assert first['snapshot_clock_contract_version'] == second['snapshot_clock_contract_version'] == 1
    assert first['snapshot_at_utc'] == captured[0].isoformat()
    assert second['snapshot_at_utc'] == captured[1].isoformat()
    assert first['providers']['publisher']['counts']['accepted'] == 1
    assert second['providers']['publisher']['counts']['accepted'] == 3


def test_counter_clock_legacy_is_explicitly_unverified_and_new_clock_precedes_publication():
    publication = '2026-10-10T09:00:03+00:00'
    status = {'updated_at_utc': publication, 'performance': {}}
    assert metrics_snapshot_clock(status) == (datetime.fromisoformat(publication), False)
    status['performance'] = {'snapshot_clock_contract_version': 1, 'snapshot_at_utc': '2026-10-10T09:00:00+00:00'}
    assert metrics_snapshot_clock(status) == (datetime(2026, 10, 10, 9, tzinfo=timezone.utc), True)


@pytest.mark.parametrize('performance', [
    None,
    {'snapshot_clock_contract_version': None, 'snapshot_at_utc': None},
    {'snapshot_clock_contract_version': True, 'snapshot_at_utc': '2026-10-10T09:00:00+00:00'},
    {'snapshot_clock_contract_version': 2, 'snapshot_at_utc': '2026-10-10T09:00:00+00:00'},
    {'snapshot_clock_contract_version': 1},
    {'snapshot_at_utc': '2026-10-10T09:00:00+00:00'},
    {'snapshot_clock_contract_version': 1, 'snapshot_at_utc': '2026-10-10T09:00:00'},
    {'snapshot_clock_contract_version': 1, 'snapshot_at_utc': '2026-10-10T17:00:00+08:00'},
    {'snapshot_clock_contract_version': 1, 'snapshot_at_utc': '2026-10-10T09:00:04+00:00'},
    {'snapshot_clock_contract_version': 1, 'snapshot_at_utc': 'not-a-clock'},
])
def test_present_malformed_counter_clock_is_not_silently_treated_as_legacy(performance):
    with pytest.raises(ValueError):
        metrics_snapshot_clock({'updated_at_utc': '2026-10-10T09:00:03+00:00', 'performance': performance})


def test_header_timeout_has_private_durable_owner_bound_evidence(config, tmp_path):
    url = 'https://publisher.test/news?private_query=SECRET_TOKEN'
    http, session = http_fixture(config, tmp_path, {url: FailedResponse(requests.ReadTimeout('SECRET_EXCEPTION'))})
    queue = NewsQueue(tmp_path, http.registry)
    http.metrics = queue.metrics
    try:
        with pytest.raises(NewsError, match='transport_error'):
            http._raw(url)
        paths = observations(tmp_path)
        assert len(paths) == 1
        event = json.loads(paths[0].read_text())
        assert event['owner_id'] == queue.owner
        assert event['requested_url'] == url and event['host'] == 'publisher.test'
        assert event['phase'] == 'awaiting_headers' and event['http_status'] is None
        assert event['request_to_headers_seconds'] is None and event['response_bytes_seen'] == 0
        assert event['error_types'] == ['ReadTimeout']
        assert not event['error_message_retained'] and 'SECRET_EXCEPTION' not in paths[0].read_text()
        assert not event['accepted_as_article'] and not event['body_stored'] and not event['publish']
        digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
        assert paths[0].name.endswith('-' + digest + '.json')
        assert stat.S_IMODE(paths[0].stat().st_mode) == 0o600
        assert stat.S_IMODE(paths[0].parent.stat().st_mode) == 0o700
        snapshot = http.metrics.snapshot()
        assert snapshot['http_host_phase_observation_contract_version'] == 1
        assert snapshot['transport_failure_diagnostic_contract_version'] == 1
        host = snapshot['providers']['publisher']['request_hosts']['publisher.test']
        assert host['counts']['transport_error_awaiting_headers'] == 1
        assert host['stages']['failed_before_headers']['count'] == 1
        assert snapshot['providers']['publisher']['counts']['transport_failure_observations_retained'] == 1
        assert 'SECRET_TOKEN' not in json.dumps(snapshot)
        assert len(session.calls) == 1
    finally:
        queue.close()
        http.close()


def test_partial_body_timeout_is_not_a_successful_200_and_does_not_store_text(config, tmp_path):
    class PartialResponse(FakeResponse):
        def iter_content(self, **kwargs):
            yield b'PRIVATE_UNFINISHED_BODY'
            try:
                raise ReadTimeoutError(None, 'private_url', 'PRIVATE_INNER_ERROR')
            except ReadTimeoutError as cause:
                raise requests.ConnectionError('PRIVATE_ERROR_MESSAGE') from cause

    http, _ = http_fixture(config, tmp_path, {'https://publisher.test/a': PartialResponse()})
    try:
        with pytest.raises(NewsError, match='transport_error'):
            http._raw('https://publisher.test/a')
        path = observations(tmp_path)[0]
        event = json.loads(path.read_text())
        assert event['phase'] == 'reading_body' and event['http_status'] == 200
        assert event['response_bytes_seen'] == len(b'PRIVATE_UNFINISHED_BODY')
        assert event['error_types'][:2] == ['ConnectionError', 'ReadTimeoutError']
        assert 'PRIVATE_' not in path.read_text()
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['http_terminal_status_200'] == counts['http_terminal_transport_errors'] == 1
        assert not counts.get('http_endpoint_article_html_outcome_complete', 0)
        host = http.metrics.snapshot()['providers']['publisher']['request_hosts']['publisher.test']
        assert host['counts']['transport_error_reading_body'] == 1
        assert host['stages']['request_to_headers']['count'] == 1
    finally:
        http.close()


def test_success_has_no_failure_io_and_records_separate_header_and_body_time(config, tmp_path, monkeypatch):
    monotonic = time.monotonic
    offset = [0.0]
    monkeypatch.setattr(time, 'monotonic', lambda: monotonic() + offset[0])

    class TimedResponse(FakeResponse):
        def __enter__(self):
            offset[0] += 2
            return self

        def iter_content(self, **kwargs):
            offset[0] += 3
            yield b'complete body'

    http, _ = http_fixture(config, tmp_path, {'https://publisher.test/a': TimedResponse()})
    monkeypatch.setattr(http, '_persist_problem', lambda *a, **k: pytest.fail('successful HTTP wrote diagnostic'))
    try:
        assert http._raw('https://publisher.test/a').body == b'complete body'
        assert observations(tmp_path) == []
        host = http.metrics.snapshot()['providers']['publisher']['request_hosts']['publisher.test']
        assert 2 <= host['stages']['request_to_headers']['wall_seconds'] < 2.1
        assert 3 <= host['stages']['headers_to_terminal']['wall_seconds'] < 3.1
        assert 'cpu_seconds' not in host['stages']['request_to_headers']
        assert 'cpu_seconds' not in host['stages']['headers_to_terminal']
        assert host['counts']['outcome_complete'] == 1
    finally:
        http.close()


def test_diagnostic_storage_error_preserves_original_retry_and_session_recovery(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {'https://publisher.test/a': FailedResponse(requests.ReadTimeout())})
    def fail_write(*args, **kwargs):
        raise OSError('controlled diagnostic filesystem failure')
    monkeypatch.setattr('downloader.news_fulltext_http.atomic_write_json', fail_write)
    try:
        with pytest.raises(NewsError, match='transport_error') as caught:
            http._raw('https://publisher.test/a')
        assert caught.value.retry and caught.value.delay == 30
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['transport_diagnostic_write_errors'] == 1
        assert counts['failed_sessions_reset'] == 1
        assert counts['http_terminal_requests'] == 1
        assert not counts.get('transport_failure_observations_retained', 0)
    finally:
        http.close()


@pytest.mark.parametrize('status', [403, 429, 502, 503])
def test_source_refusals_remain_access_not_transport_and_do_not_read_error_body(config, tmp_path, status):
    http, _ = http_fixture(config, tmp_path, {'https://publisher.test/a': UnreadErrorResponse(status=status)})
    try:
        with pytest.raises(NewsError):
            http._raw('https://publisher.test/a')
        assert observations(tmp_path) == []
        assert len(list(tmp_path.glob('access_problems/publisher/*.json'))) == 1
        host = http.metrics.snapshot()['providers']['publisher']['request_hosts']['publisher.test']
        assert host['counts']['outcome_source_status_rejected'] == 1
        assert host['counts']['response_bytes_seen'] == 0
        if status in {403, 429, 503}:
            assert http.provider_not_before['publisher'] > time.time()
    finally:
        http.close()


def test_host_telemetry_is_bounded_without_losing_terminal_totals():
    metrics = NewsMetrics()
    for number in range(100):
        metrics.request_started('publisher', 'article_html')
        metrics.request_finished('publisher', 'article_html', 200, 'complete', 1, 0,
                                 host=f'host{number}.test', phase='complete', headers_wall=0.5)
    snapshot = metrics.snapshot()['providers']['publisher']
    hosts = snapshot['request_hosts']
    assert len(hosts) <= MAX_REQUEST_HOSTS_PER_PROVIDER + 1
    assert '(other_hosts)' in hosts
    assert sum(h['counts']['terminal_requests'] for h in hosts.values()) == snapshot['counts']['http_terminal_requests'] == 100
