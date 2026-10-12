"""Private failures remain inspectable after retry; auditors never own a queue."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import time

import pytest
import requests

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_transport_observations import audit
from test_news_fulltext import FakeResponse, config, http_fixture
from test_news_fulltext_performance import FailedResponse, UnreadErrorResponse


def publish_status(queue, http):
    performance = http.metrics.snapshot()
    atomic_write_json(queue.root / 'status.json', {'owner_id': queue.owner, 'activity': 'running',
        'updated_at_utc': datetime.now(timezone.utc).isoformat(), 'performance': performance})


@pytest.mark.parametrize('marker,accepted,legacy', [(4, True, False), (None, True, True),
    (True, False, False), (3, False, False)])
def test_host_checkpoint_marker_distinguishes_loaded_repair_from_legacy(config, tmp_path, marker, accepted, legacy):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        performance = http.metrics.snapshot()
        if marker is None:
            performance.pop('gateway_host_checkpoint_contract_version')
        else:
            performance['gateway_host_checkpoint_contract_version'] = marker
        atomic_write_json(queue.root / 'status.json', {'owner_id': queue.owner, 'activity': 'running',
            'updated_at_utc': datetime.now(timezone.utc).isoformat(), 'performance': performance})
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['accepted'] is accepted
        assert result['gateway_host_checkpoint_contract_absent_legacy'] is legacy
        assert result['gateway_host_checkpoint_contract_matches'] is (marker == 4 and type(marker) is int)
        assert result['publisher_requests'] == 0 and not result['source_states_mutated']


def test_gateway_observation_after_counter_snapshot_not_misattributed_to_old_counter_generation(config, tmp_path):
    good, bad = 'https://good.publisher.test/a', 'https://bad.publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {good: FakeResponse(), bad: UnreadErrorResponse(status=502)})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        http._raw(good)
        before = http.metrics.snapshot()
        with pytest.raises(NewsError, match='http_502'):
            http._raw(bad)
        atomic_write_json(queue.root / 'status.json', {'owner_id': queue.owner, 'activity': 'running',
            'updated_at_utc': datetime.now(timezone.utc).isoformat(), 'performance': before})
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['accepted'] and result['metrics_snapshot_clock_verified']
        assert result['providers']['publisher']['observed_gateway_decision_count'] == 0
        assert result['providers']['publisher']['after_snapshot_access_files_excluded'] == 1
        assert before['providers']['publisher']['counts'].get('isolated_gateway_host_errors', 0) == 0
        # The next real counter generation contains both the event and count.
        publish_status(queue, http)
        later = audit(tmp_path, http.registry, ['publisher'])
        assert later['accepted'] and later['metrics_snapshot_clock_verified']
        assert later['providers']['publisher']['observed_gateway_decision_count'] == 1
        assert later['providers']['publisher']['recovery_counts']['isolated_gateway_host_errors'] == 1


def fail_then_inspect(config, tmp_path):
    url = 'https://publisher.test/news?secret=PRIVATE_TOKEN'
    http, session = http_fixture(config, tmp_path, {url: FailedResponse(requests.ReadTimeout('PRIVATE_ERROR'))})
    return http, session, url


def test_readonly_audit_preserves_timeout_after_success_without_sensitive_export(config, tmp_path):
    http, session, url = fail_then_inspect(config, tmp_path)
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        with pytest.raises(NewsError):
            http._raw(url)
        session.routes[url] = FakeResponse(body=b'successful public body')
        http._raw(url)
        publish_status(queue, http)
        before = queue.db.total_changes
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['accepted'] and queue.db.total_changes == before
        assert result['publisher_requests'] == 0 and not result['source_states_mutated']
        provider = result['providers']['publisher']
        assert provider['terminal_requests'] == 2 and provider['observed_failure_count'] == 1
        assert provider['failure_phases'] == {'awaiting_headers': 1}
        assert provider['recovery_counts']['failed_sessions_reset'] == 1
        assert len(provider['verified_failure_events'][0]['file_sha256']) == 64
        assert 'PRIVATE_' not in json.dumps(result) and 'requested_url' not in json.dumps(result)
        assert len(session.calls) == 2


def test_current_owner_corrupt_event_is_reported_not_ignored(config, tmp_path):
    http, _, url = fail_then_inspect(config, tmp_path)
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        with pytest.raises(NewsError):
            http._raw(url)
        path = next(tmp_path.glob('transport_problems/publisher/*/*.json'))
        event = json.loads(path.read_text())
        event['response_bytes_seen'] = -1
        atomic_write_json(path, event)
        publish_status(queue, http)
        result = audit(tmp_path, http.registry, ['publisher'])
        assert not result['accepted']
        assert result['errors'] == [{'provider': 'publisher', 'code': 'invalid_private_failure_observation'}]
        assert 'PRIVATE_' not in json.dumps(result)


def test_old_owner_failure_is_not_attributed_to_new_owner(config, tmp_path):
    http, _, url = fail_then_inspect(config, tmp_path)
    with closing(http):
        with closing(NewsQueue(tmp_path, http.registry)) as previous:
            http.metrics = previous.metrics
            with pytest.raises(NewsError):
                http._raw(url)
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            http.metrics = queue.metrics
            publish_status(queue, http)
            result = audit(tmp_path, http.registry, ['publisher'])
            assert result['accepted']
            assert result['providers']['publisher']['observed_failure_count'] == 0
            assert result['providers']['publisher']['older_owner_files_excluded'] == 1


@pytest.mark.parametrize('maximum', [0, 257, True, 1.5])
def test_audit_event_sample_must_be_bounded(config, tmp_path, maximum):
    http, _, _ = fail_then_inspect(config, tmp_path)
    with closing(http):
        with pytest.raises(ValueError, match='bounded_event_sample_required'):
            audit(tmp_path, http.registry, ['publisher'], maximum_events=maximum)


def test_gateway_decision_receipts_verify_provisional_scope_without_url_headers_or_body_export(config, tmp_path, monkeypatch):
    good, bad = 'https://good.publisher.test/a', 'https://bad.publisher.test/a?private=PRIVATE_TOKEN'
    http, session = http_fixture(config, tmp_path, {good: FakeResponse(),
        bad: UnreadErrorResponse(status=502, headers={'Server': 'PRIVATE_HEADER'})})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        http._raw(good)
        monotonic = time.monotonic
        monkeypatch.setattr(time, 'monotonic', lambda: monotonic() + 121)
        for _ in range(3):
            with pytest.raises(NewsError):
                http._raw(bad)
        publish_status(queue, http)
        before = queue.db.total_changes
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['accepted'] and result['gateway_scope_contract_matches']
        assert queue.db.total_changes == before and len(session.calls) == 4
        assert not result['body_receipts_verified'] and result['publisher_requests'] == 0
        provider = result['providers']['publisher']
        assert provider['gateway_scope_counts'] == {'provisional_host': 3}
        assert provider['gateway_basis_counts'] == {'older_success_no_observed_other_gateway_failure': 3}
        assert provider['recovery_counts']['provisional_gateway_host_errors'] == 3
        assert provider['recovery_counts']['provisional_gateway_host_circuit_opened'] == 1
        assert all(not d['body_accepted'] for d in provider['verified_gateway_decisions'])
        assert 'PRIVATE_' not in json.dumps(result) and 'requested_url' not in json.dumps(result)


def test_corrupt_gateway_decision_is_not_promoted_to_success(config, tmp_path):
    url = 'https://publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {url: UnreadErrorResponse(status=502)})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        with pytest.raises(NewsError):
            http._raw(url)
        path = next(tmp_path.glob('access_problems/publisher/*.json'))
        event = json.loads(path.read_text())
        event['gateway_recovery']['scope'] = 'host'
        atomic_write_json(path, event)
        publish_status(queue, http)
        result = audit(tmp_path, http.registry, ['publisher'])
        assert not result['accepted']
        assert result['errors'] == [{'provider': 'publisher', 'code': 'invalid_private_gateway_decision'}]


@pytest.mark.parametrize('corruption', [
    {'host_pause_generation': True}, {'host_pause_generation': 6}, {'host_pause_seconds': 900},
    {'host_pause_started_at_epoch': None}, {'host_pause_not_before_epoch': 1},
])
def test_progressive_gateway_receipt_rejects_wrong_delay_even_with_valid_sha(config, tmp_path, corruption):
    good, bad = 'https://good.publisher.test/a', 'https://bad.publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {good: FakeResponse(), bad: UnreadErrorResponse(status=502)})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        http._raw(good)
        for _ in range(3):
            with pytest.raises(NewsError):
                http._raw(bad)
        path = sorted(tmp_path.glob('access_problems/publisher/*.json'))[-1]
        event = json.loads(path.read_text())
        assert event['gateway_recovery']['pause_open']
        event['gateway_recovery'].update(corruption)
        # Rewrite its exact content hash too: an intact file cannot make a
        # malformed clock/duration policy valid. No raw body or URL is exported.
        digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
        atomic_write_json(path.parent / ('9999999999999999999-' + digest + '.json'), event)
        publish_status(queue, http)
        result = audit(tmp_path, http.registry, ['publisher'])
        assert not result['accepted']
        assert result['errors'] == [{'provider': 'publisher', 'code': 'invalid_private_gateway_decision'}]


def test_slow_checkpoint_is_not_a_false_negative_gateway_clock(config, tmp_path, monkeypatch):
    from types import SimpleNamespace
    import downloader.news_fulltext_host_circuit as host_module
    good, bad = 'https://good.publisher.test/a', 'https://bad.publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {good: FakeResponse(), bad: UnreadErrorResponse(status=502)})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        http._raw(good)
        # Model a checkpoint completing after the original 30-second deadline.
        # The receipt must bind the actual pause start, not assume fsync is fast.
        monkeypatch.setattr(host_module, 'time', SimpleNamespace(time=lambda: time.time() - 31,
                                                                monotonic=time.monotonic))
        for _ in range(3):
            with pytest.raises(NewsError):
                http._raw(bad)
        publish_status(queue, http)
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['accepted']
        decisions = result['providers']['publisher']['verified_gateway_decisions']
        assert any(d['pause_open'] and d['host_pause_seconds'] == 30 for d in decisions)


@pytest.mark.parametrize('corruption', ['extra_body', 'false_health'])
def test_valid_sha_does_not_accept_wrong_gateway_privacy_or_health_contract(config, tmp_path, corruption):
    url = 'https://publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {url: UnreadErrorResponse(status=502)})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        with pytest.raises(NewsError):
            http._raw(url)
        path = next(tmp_path.glob('access_problems/publisher/*.json'))
        event = json.loads(path.read_text())
        if corruption == 'extra_body':
            event['article_body'] = 'PRIVATE_TEXT_SHOULD_NOT_BE_HERE'
        else:
            event['gateway_recovery'].update(scope='host', basis='different_recent_successful_host')
        digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
        rewritten = path.parent / ('9999999999999999999-' + digest + '.json')
        atomic_write_json(rewritten, event)
        rewritten.chmod(0o600)
        publish_status(queue, http)
        result = audit(tmp_path, http.registry, ['publisher'])
        assert not result['accepted']
        assert result['errors'] == [{'provider': 'publisher', 'code': 'invalid_private_gateway_decision'}]
        assert 'PRIVATE_TEXT' not in json.dumps(result)


def test_old_gateway_owner_is_not_misattributed_to_new_owner(config, tmp_path):
    url = 'https://publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {url: UnreadErrorResponse(status=502)})
    with closing(http):
        with closing(NewsQueue(tmp_path, http.registry)) as previous:
            http.metrics = previous.metrics
            with pytest.raises(NewsError):
                http._raw(url)
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            http.metrics = queue.metrics
            publish_status(queue, http)
            result = audit(tmp_path, http.registry, ['publisher'])
            assert result['accepted']
            assert result['providers']['publisher']['observed_gateway_decision_count'] == 0
            assert result['providers']['publisher']['legacy_or_other_owner_access_files_excluded'] == 1


def test_gateway_required_cli_does_not_accept_empty_failure_sample(config, tmp_path, monkeypatch):
    from scripts import audit_news_transport_observations as module
    root = tmp_path / config['root']
    http, session = http_fixture(config, root, {})
    with closing(NewsQueue(root, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        publish_status(queue, http)
        configured = tmp_path / 'config.json'
        atomic_write_json(configured, config)
        monkeypatch.setattr(module, 'load_config', lambda path: (config, http.registry, tmp_path))
        output = tmp_path / 'gateway.json'
        assert module.main(['--config', str(configured), '--output', str(output),
                            '--require-gateway-failure-provider', 'publisher']) == 1
        result = json.loads(output.read_text())
        assert not result['accepted'] and not result['required_gateway_failures_observed']
        assert not session.calls
