"""Gateway isolation is not permission, rate-limit bypass or body acceptance."""
from contextlib import closing
import json
import time

import pytest
import requests

from downloader.news_fulltext_host_circuit import (GatewayHostCircuit, host_filtered_query,
    MAX_PAUSE_SECONDS, MAX_PAUSE_GENERATION, pause_seconds)
from downloader.news_fulltext_metrics import (GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION,
    GATEWAY_HOST_ISOLATION_CONTRACT_VERSION)
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import FakeResponse, config, http_fixture
from test_news_fulltext_performance import FailedResponse, UnreadErrorResponse
from test_news_capacity import controller_fixture
from test_cnyes_news_bulk import cfg as cnyes_config
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_cnyes_work import statement as cnyes_statement


HEALTHY = 'https://good.publisher.test/news'
BROKEN = 'https://bad.publisher.test/news'


def test_three_local_gateway_errors_do_not_pause_healthy_publisher_host(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(status=502)})
    with closing(http):
        http._raw(HEALTHY)
        for _ in range(3):
            with pytest.raises(NewsError, match='http_502') as failure:
                http._raw(BROKEN)
            assert failure.value.isolated_gateway_host
        assert not http.provider_not_before.get('publisher', 0)
        assert http.blocked_gateway_hosts('publisher') == ('bad.publisher.test',)
        with pytest.raises(NewsError, match='host_gateway_cooldown'):
            http._raw(BROKEN)
        assert http._raw(HEALTHY).status == 200
        assert len(session.calls) == 5  # Rejected locally BEFORE another request.
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['http_terminal_server_errors'] == counts['http_terminal_isolated_gateway_host_errors'] == 3
        assert counts['http_terminal_requests'] == 5
        assert not counts.get('http_circuit_opened', 0)
        assert len(list(tmp_path.glob('access_problems/publisher/*.json'))) == 3


def test_host_pause_survives_owner_restart_and_expires_automatically(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(status=504)})
    with closing(http):
        http._raw(HEALTHY)
        for _ in range(3):
            with pytest.raises(NewsError):
                http._raw(BROKEN)
        resumed = GatewayHostCircuit(tmp_path, http.registry)
        assert resumed.remaining('publisher', 'bad.publisher.test') > 0
        now = time.time()
        monkeypatch.setattr(time, 'time', lambda: now + 31)
        assert resumed.blocked('publisher') == {}
        assert resumed.remaining('publisher', 'bad.publisher.test') == 0


@pytest.mark.parametrize('status,headers', [(403, {}), (429, {}), (503, {}), (502, {'Retry-After': '60'}),
    (502, {'Retry-After': 'not-a-delay'}), (504, {'Retry-After': ''}), (502, {'Retry-After': '0'})])
def test_source_refusal_overload_and_retry_after_always_pause_publisher(config, tmp_path, status, headers):
    http, _ = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(status=status, headers=headers)})
    with closing(http):
        http._raw(HEALTHY)
        with pytest.raises(NewsError) as failure:
            http._raw(BROKEN)
        assert not getattr(failure.value, 'isolated_gateway_host', False)
        assert http.provider_not_before['publisher'] > time.time()
        assert not http.blocked_gateway_hosts('publisher')
        if status == 429:
            assert http.rate_refusal_intervals['publisher'] > 0


@pytest.mark.parametrize('status', [200, 302, 429, 502, 503, 504])
def test_explicit_challenge_header_never_becomes_gateway_isolation_or_body_acceptance(config, tmp_path, status):
    http, _ = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(
        status=status, headers={'cf-mitigated': 'challenge', 'Retry-After': '60'})})
    with closing(http):
        http._raw(HEALTHY)
        with pytest.raises(NewsError) as failure:
            http._raw(BROKEN)
        assert failure.value.code == ('http_429' if status == 429 else 'source_access_denied')
        assert not getattr(failure.value, 'isolated_gateway_host', False)
        assert not getattr(failure.value, 'provisional_gateway_host', False)
        assert http.provider_not_before['publisher'] >= time.time() + 899
        assert not http.blocked_gateway_hosts('publisher')
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['content_access_refusals'] == counts['header_access_refusals'] == 1
        if status == 429:
            assert http.rate_refusal_intervals['publisher'] > 0


def test_unknown_other_host_health_keeps_conservative_publisher_circuit(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(status=502)})
    with closing(http):
        for _ in range(3):
            with pytest.raises(NewsError) as failure:
                http._raw(BROKEN)
            assert not getattr(failure.value, 'isolated_gateway_host', False)
        assert http.provider_not_before['publisher'] > time.time()


def test_quiet_previously_successful_host_is_not_frozen_but_cannot_corroborate_health(config, tmp_path, monkeypatch):
    http, session = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(status=502)})
    with closing(http):
        http._raw(HEALTHY)
        monotonic = time.monotonic
        monkeypatch.setattr(time, 'monotonic', lambda: monotonic() + 121)
        for _ in range(3):
            with pytest.raises(NewsError, match='http_502') as failure:
                http._raw(BROKEN)
            assert not failure.value.isolated_gateway_host
            assert failure.value.provisional_gateway_host
        assert not http.provider_not_before.get('publisher', 0)
        assert http.blocked_gateway_hosts('publisher') == ('bad.publisher.test',)
        with pytest.raises(NewsError, match='host_gateway_cooldown'):
            http._raw(BROKEN)
        assert len(session.calls) == 4
        assert http._raw(HEALTHY).status == 200
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['http_terminal_provisional_gateway_host_errors'] == counts['http_terminal_server_errors'] == 3
        assert not counts.get('http_terminal_isolated_gateway_host_errors', 0)
        events = [json.loads(p.read_text()) for p in (tmp_path / 'access_problems' / 'publisher').glob('*.json')]
        assert len(events) == 3
        for event in events:
            decision = event['gateway_recovery']
            assert event['contract_version'] == 2
            assert decision['contract_version'] == GATEWAY_HOST_ISOLATION_CONTRACT_VERSION
            assert event['body_read'] is False and event['accepted_as_article'] is False
            assert decision['scope'] == 'provisional_host' and decision['corroborating_hosts'] == []
            assert decision['other_success_ages_seconds']['good.publisher.test'] >= 121
            assert decision['unrecovered_other_failed_hosts'] == []


def test_failed_host_old_success_cannot_corroborate_another_failed_host(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: FakeResponse()})
    with closing(http):
        http._raw(HEALTHY)
        http._raw(BROKEN)
        session.routes[HEALTHY] = UnreadErrorResponse(status=502)
        session.routes[BROKEN] = UnreadErrorResponse(status=502)
        with pytest.raises(NewsError) as first:
            http._raw(BROKEN)
        assert first.value.isolated_gateway_host
        for _ in range(3):
            with pytest.raises(NewsError) as failure:
                http._raw(HEALTHY)
            assert not getattr(failure.value, 'isolated_gateway_host', False)
            assert not getattr(failure.value, 'provisional_gateway_host', False)
            assert failure.value.gateway_recovery_observation['scope'] == 'publisher'
            assert failure.value.gateway_recovery_observation['unrecovered_other_failed_hosts'] == ['bad.publisher.test']
        assert http.provider_not_before['publisher'] > time.time()


def test_stale_success_plus_another_unrecovered_failure_keeps_publisher_protection(config, tmp_path, monkeypatch):
    third = 'https://third.publisher.test/news'
    http, _ = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(status=502),
                                            third: UnreadErrorResponse(status=502)})
    with closing(http):
        http._raw(HEALTHY)
        with pytest.raises(NewsError):
            http._raw(third)
        monotonic = time.monotonic
        monkeypatch.setattr(time, 'monotonic', lambda: monotonic() + 121)
        for _ in range(3):
            with pytest.raises(NewsError) as failure:
                http._raw(BROKEN)
            assert not getattr(failure.value, 'provisional_gateway_host', False)
            assert failure.value.gateway_recovery_observation['scope'] == 'publisher'
        assert http.provider_not_before['publisher'] > time.time()


def test_later_transport_timeout_invalidates_old_success_for_gateway_corroboration(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: FakeResponse()})
    with closing(http):
        http._raw(HEALTHY)
        http._raw(BROKEN)
        session.routes[HEALTHY] = FailedResponse(requests.ReadTimeout())
        with pytest.raises(NewsError, match='transport_error'):
            http._raw(HEALTHY)
        session.routes[BROKEN] = UnreadErrorResponse(status=502)
        with pytest.raises(NewsError, match='http_502') as failure:
            http._raw(BROKEN)
        assert not getattr(failure.value, 'isolated_gateway_host', False)
        assert not getattr(failure.value, 'provisional_gateway_host', False)
        assert failure.value.gateway_recovery_observation['unrecovered_other_failed_hosts'] == ['good.publisher.test']
        # A REAL later success restores corroboration; not expiry or a restart.
        session.routes[HEALTHY] = FakeResponse()
        http._raw(HEALTHY)
        with pytest.raises(NewsError, match='http_502') as restored:
            http._raw(BROKEN)
        assert restored.value.isolated_gateway_host


def test_existing_v1_host_pause_survives_recovery_contract_upgrade(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(status=504)})
    with closing(http):
        http._raw(HEALTHY)
        for _ in range(3):
            with pytest.raises(NewsError):
                http._raw(BROKEN)
        path = tmp_path / 'gateway_host_cooldowns.json'
        saved = json.loads(path.read_text())
        assert saved['contract_version'] == GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION
        saved['contract_version'] = 1
        path.write_text(json.dumps(saved))
        assert GatewayHostCircuit(tmp_path, http.registry).remaining('publisher', 'bad.publisher.test') > 0


def test_provisional_gateway_failure_remains_pressure_even_below_normal_error_tolerance(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, maximum=1)
    with closing(http):
        for status, outcome, count in [(200, 'complete', 100), (502, 'provisional_gateway_host_error', 1)]:
            for _ in range(count):
                http.metrics.request_started('publisher', 'article_html')
                http.metrics.request_finished('publisher', 'article_html', status, outcome, 1, 0)
        http.metrics.count('publisher', 'durably_accepted_new_url_heads', 100)
        http.metrics.count('publisher', 'durably_accepted_versions', 100)
        clock.value += 10
        controller.tick()
        sample = controller.states['publisher']['last_window']
        assert sample['transport_or_server_errors'] == sample['provisional_gateway_host_errors'] == 1
        assert sample['pressure_transport_or_server_errors'] == 1
        assert sample['isolated_gateway_host_errors'] == 0
        assert sample['pressure_request_results'] == sample['pressure_response_results'] == 101
        assert not sample['healthy'] and not sample['all_observed_hosts_healthy']
        assert controller.states['publisher']['state'] == 'recovering_transport'


def test_checkpoint_write_error_preserves_gateway_error_not_internal_failure(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {HEALTHY: FakeResponse(), BROKEN: UnreadErrorResponse(status=502)})
    def unavailable(*args, **kwargs):
        raise OSError('controlled filesystem error')
    with closing(http):
        http._raw(HEALTHY)
        monkeypatch.setattr('downloader.news_fulltext_host_circuit.atomic_write_json', unavailable)
        for _ in range(3):
            with pytest.raises(NewsError, match='http_502'):
                http._raw(BROKEN)
        assert http.metrics.snapshot()['providers']['publisher']['counts']['gateway_host_checkpoint_write_errors'] == 1


@pytest.mark.parametrize('lane', ['regular', 'undated', 'retry', 'stored'])
def test_every_article_lane_filters_host_before_limit_without_mutating_paused_observations(config, tmp_path, lane):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': BROKEN, 'date': '2026-10-10'}, {'url': HEALTHY, 'date': '2014-01-01'}])
            if lane == 'undated':
                queue.db.execute("UPDATE articles SET metadata_order_day=NULL")
                queue._dated_body_dispatch_bursts = {'publisher': 8}
            elif lane == 'retry':
                queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error'")
                queue._pending_body_dispatch_bursts = {'publisher': 8}
            elif lane == 'stored':
                queue.db.execute("UPDATE articles SET body_sha256=?", ('a' * 64,))
                queue._missing_body_dispatch_bursts = {'publisher': 8}
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (BROKEN,)).fetchone())
        row = queue.claim('publisher', 300, excluded_hosts=('bad.publisher.test',))
        assert row['url'] == HEALTHY
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (BROKEN,)).fetchone())
        assert before == after
        assert queue.claim('publisher', 300, excluded_hosts=('bad.publisher.test',)) is None
        # The source retry is eligible again after the caller's short pause expires.
        assert queue.claim('publisher', 300)['url'] == BROKEN


def test_empty_host_filter_keeps_existing_hot_sql_and_parameter_contract():
    statement = NewsQueue.article_dispatch_statement()
    parameters = ('publisher', 'pending', 0)
    assert host_filtered_query(statement, parameters, ()) == (statement, parameters)


def test_cnyes_host_isolation_keeps_indexed_candidate_dispatch_and_original_evidence(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': 'https://amp-news.cnyes.com/news/id/1', 'date': '2026-10-10'},
                                {'url': 'https://news.cnyes.com/news/id/2', 'date': '2014-01-01'}])
        excluded = ('amp-news.cnyes.com',)
        query, parameters = host_filtered_query(cnyes_statement(), ('cnyes', 'pending', time.time(), 0), excluded)
        plans = [r[3] for r in queue.db.execute('EXPLAIN QUERY PLAN ' + query, parameters)]
        assert any('SEARCH articles USING INDEX dispatch_cnyes_body_value' in plan for plan in plans)
        assert not any('TEMP B-TREE' in plan or 'SCAN articles' in plan for plan in plans)
        assert queue.claim('cnyes', 300, excluded_hosts=excluded)['host'] == 'news.cnyes.com'
        row = queue.db.execute("SELECT state,attempts,body_sha256 FROM articles WHERE host='amp-news.cnyes.com'").fetchone()
        assert tuple(row) == ('pending', 0, None)


def test_isolated_failures_alone_cannot_estimate_healthy_capacity(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, maximum=1)
    with closing(http):
        for _ in range(100):
            http.metrics.request_started('publisher', 'article_html')
            http.metrics.request_finished('publisher', 'article_html', 502, 'isolated_gateway_host_error', 1, 0)
        clock.value += 10
        controller.tick()
        assert 'last_window' not in controller.states['publisher']
        assert http.client_intervals['publisher'] == 1


def test_capacity_retains_raw_gateway_errors_without_mislabeling_healthy_hosts_as_overloaded(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, maximum=1)
    with closing(http):
        for status, outcome, count in [(200, 'complete', 100), (502, 'isolated_gateway_host_error', 10)]:
            for _ in range(count):
                http.metrics.request_started('publisher', 'article_html')
                http.metrics.request_finished('publisher', 'article_html', status, outcome, 1, 0)
        http.metrics.count('publisher', 'durably_accepted_new_url_heads', 100)
        http.metrics.count('publisher', 'durably_accepted_versions', 100)
        clock.value += 10
        controller.tick()
        sample = controller.states['publisher']['last_window']
        assert sample['transport_or_server_errors'] == sample['isolated_gateway_host_errors'] == 10
        assert sample['pressure_transport_or_server_errors'] == 0
        assert sample['requests'] == sample['responses'] == 110
        assert sample['pressure_request_results'] == sample['pressure_response_results'] == 100
        assert sample['healthy'] and not sample['all_observed_hosts_healthy']
        assert sample['new_stored_url_heads'] == 100
        assert sample['durable_versions_per_request'] == pytest.approx(100 / 110)
        assert controller.states['publisher']['state'] != 'recovering_transport'


def gateway_clock(monkeypatch):
    """Virtualize only the circuit's clocks, not sleep/pytest/HTTP pacing."""
    from types import SimpleNamespace
    clock = SimpleNamespace(value=time.time())
    monkeypatch.setattr('downloader.news_fulltext_host_circuit.time', SimpleNamespace(
        time=lambda: clock.value, monotonic=lambda: clock.value))
    return clock


def test_repeated_real_failed_probes_back_off_only_the_failed_host(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        clock = gateway_clock(monkeypatch)
        circuit = http.host_circuits
        for _ in range(3):
            circuit.success('publisher', 'good.publisher.test')
            opened = circuit.gateway_failure('publisher', 'bad.publisher.test', 502, None)
        assert opened['pause_open'] and opened['host_pause_seconds'] == 30
        for generation, duration in [(2, 60), (3, 120), (4, 240), (5, 300), (5, 300)]:
            deadline = circuit.paused['publisher']['bad.publisher.test']
            # A late failure from the existing in-flight cohort must neither
            # extend the deadline nor pretend another probe was sent.
            clock.value += 1
            late = circuit.gateway_failure('publisher', 'bad.publisher.test', 502, None)
            assert not late['pause_open'] and late['host_pause_not_before_epoch'] == deadline
            clock.value = deadline + 0.01
            circuit.success('publisher', 'good.publisher.test')
            opened = circuit.gateway_failure('publisher', 'bad.publisher.test', 504, None)
            assert opened['host_pause_generation'] == generation
            assert opened['host_pause_seconds'] == duration
            assert opened['pause_open'] and circuit.remaining('publisher', 'bad.publisher.test') == duration
            assert circuit.remaining('publisher', 'good.publisher.test') == 0
            assert not http.provider_not_before
        assert MAX_PAUSE_SECONDS == 300 and MAX_PAUSE_GENERATION == 5


def test_progressive_pause_and_failure_proof_survive_restart_even_after_expiry(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        clock = gateway_clock(monkeypatch)
        circuit = http.host_circuits
        circuit.success('publisher', 'good.publisher.test')
        for _ in range(3):
            circuit.gateway_failure('publisher', 'bad.publisher.test', 502, None)
        for expected in (60, 120):
            deadline = circuit.paused['publisher']['bad.publisher.test']
            # Expired state still retains retry evidence. A restart is not a
            # reason to repeat the original three-error warmup or reset quota.
            clock.value = deadline + 1
            circuit = GatewayHostCircuit(tmp_path, http.registry)
            assert circuit.blocked('publisher') == {}
            assert circuit.failures['publisher']['bad.publisher.test'] >= 3
            # Healthy-host evidence is NEVER restored from a wall-clock file.
            unknown = circuit.gateway_failure('publisher', 'bad.publisher.test', 502, None)
            assert unknown['scope'] == 'publisher' and not unknown['pause_open']
            circuit.success('publisher', 'good.publisher.test')
            observation = circuit.gateway_failure('publisher', 'bad.publisher.test', 502, None)
            assert observation['scope'] == 'host' and observation['host_pause_seconds'] == expected
            resumed = GatewayHostCircuit(tmp_path, http.registry)
            assert resumed.remaining('publisher', 'bad.publisher.test') == expected


def test_completed_success_immediately_clears_pause_generation_and_checkpoint(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        gateway_clock(monkeypatch)
        circuit = http.host_circuits
        circuit.success('publisher', 'good.publisher.test')
        for _ in range(3):
            circuit.gateway_failure('publisher', 'bad.publisher.test', 502, None)
        assert circuit.remaining('publisher', 'bad.publisher.test') == 30
        assert circuit.success('publisher', 'bad.publisher.test')
        assert circuit.remaining('publisher', 'bad.publisher.test') == 0
        assert not circuit.failures['publisher'] and not circuit.pause_generations['publisher']
        assert GatewayHostCircuit(tmp_path, http.registry).blocked('publisher') == {}
        for _ in range(3):
            reopened = circuit.gateway_failure('publisher', 'bad.publisher.test', 502, None)
        assert reopened['host_pause_seconds'] == 30 and reopened['host_pause_generation'] == 1


@pytest.mark.parametrize('checkpoint_error', [False, True])
def test_later_completed_inflight_body_recovers_host_without_duplicate_request(config, tmp_path, monkeypatch, checkpoint_error):
    http, session = http_fixture(config, tmp_path, {HEALTHY: FakeResponse()})
    class InflightRecovery(FakeResponse):
        def iter_content(self, chunk_size):
            # Controlled concurrent responses open a pause after this successful
            # request has already started. Its completed body is real evidence,
            # not an extra health-check HTTP or an accepted synthetic body.
            for _ in range(3):
                with pytest.raises(NewsError, match='http_502'):
                    http._response_status('publisher', 502, {}, host='bad.publisher.test')
            assert http.blocked_gateway_hosts('publisher') == ('bad.publisher.test',)
            if checkpoint_error:
                def unavailable(*args, **kwargs):
                    raise OSError('controlled recovery checkpoint error')
                monkeypatch.setattr('downloader.news_fulltext_host_circuit.atomic_write_json', unavailable)
            yield b'exact completed in-flight bytes'
    session.routes[BROKEN] = InflightRecovery()
    with closing(http):
        http._raw(HEALTHY)
        result = http._raw(BROKEN)
        assert result.body == b'exact completed in-flight bytes'
        assert http.blocked_gateway_hosts('publisher') == () and not http.provider_not_before
        assert len(session.calls) == 2
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts.get('gateway_host_checkpoint_write_errors', 0) == int(checkpoint_error)
        assert counts.get('gateway_host_recoveries', 0) == int(not checkpoint_error)
        if checkpoint_error:
            assert GatewayHostCircuit(tmp_path, http.registry).remaining('publisher', 'bad.publisher.test') > 0


@pytest.mark.parametrize('generation', [0, -1, 6, True, 1.5, None])
def test_gateway_pause_generation_is_not_an_unbounded_or_bool_duration(generation):
    with pytest.raises(ValueError, match='invalid_gateway_pause_generation'):
        pause_seconds(generation)


@pytest.mark.parametrize('corruption', [
    {'pause_generation': True}, {'pause_generation': 6}, {'failure_count': True}, {'failure_count': 2},
    {'not_before_epoch': float('inf')}, {'not_before_epoch': time.time() + 3600},
    {'source_data_fingerprint': 'wrong'}])
def test_invalid_progressive_checkpoint_cannot_expand_a_host_pause(config, tmp_path, corruption):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        circuit = http.host_circuits
        circuit.success('publisher', 'good.publisher.test')
        for _ in range(3):
            circuit.gateway_failure('publisher', 'bad.publisher.test', 502, None)
        saved = json.loads(circuit.path.read_text())
        saved['providers']['publisher']['bad.publisher.test'].update(corruption)
        circuit.path.write_text(json.dumps(saved))
        resumed = GatewayHostCircuit(tmp_path, http.registry)
        assert resumed.blocked('publisher') == {} and not resumed.pause_generations and not resumed.failures


def test_healthy_success_path_has_no_new_checkpoint_io(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        def unexpected(*args, **kwargs):
            raise AssertionError('ordinary successful requests must not checkpoint')
        monkeypatch.setattr('downloader.news_fulltext_host_circuit.atomic_write_json', unexpected)
        for _ in range(100):
            assert not http.host_circuits.success('publisher', 'good.publisher.test')


def test_fixed_failure_window_reduces_bad_probes_without_blocking_healthy_host(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        clock = gateway_clock(monkeypatch)
        start, probes, healthy = clock.value, 0, 0
        for second in range(660):
            clock.value = start + second
            http.host_circuits.success('publisher', 'good.publisher.test')
            healthy += int(not http.host_circuits.remaining('publisher', 'good.publisher.test'))
            if not http.host_circuits.remaining('publisher', 'bad.publisher.test'):
                http.host_circuits.gateway_failure('publisher', 'bad.publisher.test', 502, None)
                probes += 1
        assert healthy == 660 and probes == 7
        # The previous fixed-30s policy would send the first three errors at
        # t=0,1,2 and probe at t=32,62,...,632: 24, all returning the same 502.
        fixed_30s_probes = 3 + len(range(32, 660, 30))
        assert fixed_30s_probes == 24 and not http.provider_not_before
