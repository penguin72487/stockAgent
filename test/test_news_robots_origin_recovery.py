"""REP-origin failure isolation never grants access or clears source refusals."""
from contextlib import closing
from copy import deepcopy
import hashlib
import json
import time
from types import SimpleNamespace

import pytest
import requests

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_host_circuit import host_filtered_query
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_robots import policy_origin, robots_policy_retry
from scripts.audit_news_transport_observations import audit
from test_news_fulltext import FakeResponse, config, http_fixture
from test_news_fulltext_performance import FailedResponse, UnreadErrorResponse
from test_news_transport_observations_audit import publish_status
from test_cnyes_news_bulk import cfg as cnyes_config
from downloader.news_fulltext_cnyes_work import statement as cnyes_statement


BAD = 'https://bad.publisher.test'
GOOD = 'https://good.publisher.test'
ALLOW = b'User-agent: *\nAllow: /\n'


def cache_path(root, origin=BAD):
    return root / 'robots' / (hashlib.sha256(origin.encode()).hexdigest() + '.json')


def clock_fixture(monkeypatch):
    """Virtualize negative-cache wall time, not HTTP pacing or elapsed clocks."""
    clock = SimpleNamespace(value=time.time())
    monkeypatch.setattr('downloader.news_fulltext_http.time', SimpleNamespace(
        time=lambda: clock.value, time_ns=time.time_ns, monotonic=time.monotonic,
        thread_time=time.thread_time))
    return clock


@pytest.mark.parametrize('status', [502, 504])
def test_unreachable_policy_denies_only_its_initial_origin_before_body_read(config, tmp_path, status):
    routes = {BAD + '/robots.txt': UnreadErrorResponse(status=status),
              GOOD + '/robots.txt': FakeResponse(ALLOW), GOOD + '/article': FakeResponse(b'body')}
    http, session = http_fixture(config, tmp_path, routes)
    with closing(http):
        with pytest.raises(NewsError, match='http_' + str(status)) as failure:
            http.get(BAD + '/article?private=do-not-export')
        assert failure.value.robots_origin_isolation and failure.value.delay == 30
        before = sha256_file(cache_path(tmp_path))
        deadline = http.blocked_robots_origins('publisher')[BAD]
        for _ in range(3):
            with pytest.raises(NewsError, match='robots_origin_policy_cooldown'):
                http.get(BAD + '/different-article')
        assert sha256_file(cache_path(tmp_path)) == before
        assert http.blocked_robots_origins('publisher')[BAD] == deadline
        assert http.get(GOOD + '/article').body == b'body'
        assert [url for url, _ in session.calls] == [BAD + '/robots.txt', GOOD + '/robots.txt', GOOD + '/article']
        assert not http.provider_not_before and not http.transport_failures.get('publisher')
        assert not http.blocked_gateway_hosts('publisher')
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['robots_origin_unreachable_headers'] == counts['http_transient_errors'] == 1
        assert counts['http_endpoint_robots_outcome_robots_origin_policy_unreachable'] == 1
        assert counts['http_terminal_requests'] == 3
        assert not counts.get('isolated_gateway_host_errors')


@pytest.mark.parametrize('status,headers', [
    (401, {}), (403, {}), (451, {}), (429, {}), (503, {}),
    (502, {'Retry-After': '60'}), (502, {'Retry-After': 'bad'}),
    (504, {'Retry-After': ''}), (502, {'Retry-After': '0'}),
    (502, {'cf-mitigated': 'challenge'}), (200, {'cf-mitigated': 'Challenge'}),
])
def test_refusal_overload_and_any_retry_after_keep_shared_provider_protection(config, tmp_path, status, headers):
    http, session = http_fixture(config, tmp_path, {
        BAD + '/robots.txt': UnreadErrorResponse(status=status, headers=headers)})
    with closing(http):
        with pytest.raises(NewsError) as failure:
            http.get(BAD + '/article')
        assert not getattr(failure.value, 'robots_origin_isolation', False)
        assert http.provider_not_before['publisher'] > time.time()
        assert not http.blocked_robots_origins('publisher')
        assert not cache_path(tmp_path).exists()
        with pytest.raises(NewsError, match='provider_cooldown'):
            http.get(GOOD + '/article')
        assert len(session.calls) == 1
        if status == 429:
            assert http.rate_refusal_intervals['publisher'] > 0


def test_failed_redirected_policy_belongs_to_initial_origin_not_redirect_target(config, tmp_path):
    config['providers'][0]['article_path_pattern'] = '^/article$'
    http, session = http_fixture(config, tmp_path, {
        BAD + '/robots.txt': FakeResponse(status=302, headers={'Location': GOOD + '/policy'}),
        GOOD + '/policy': UnreadErrorResponse(status=502),
        GOOD + '/robots.txt': FakeResponse(ALLOW), GOOD + '/article': FakeResponse(b'body')})
    with closing(http):
        with pytest.raises(NewsError, match='http_502'):
            http.get(BAD + '/article')
        assert set(http.blocked_robots_origins('publisher')) == {BAD}
        assert http.get(GOOD + '/article').body == b'body'
        counts = http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['endpoint_robots_requests'] == counts['http_endpoint_robots_responses_received'] == 3
        assert counts['http_endpoint_robots_terminal_requests'] == 3
        assert counts['endpoint_article_html_requests'] == 1
        event = json.loads(next(tmp_path.glob('access_problems/publisher/*.json')).read_bytes())
        assert event['requested_url'] == GOOD + '/policy'
        assert event['robots_origin_policy']['initial_origin'] == BAD
        assert event['body_read'] is False and event['accepted_as_article'] is False
        assert len(session.calls) == 4


def test_negative_cache_survives_restart_and_config_change_then_actual_recovery(config, tmp_path, monkeypatch):
    clock = clock_fixture(monkeypatch)
    http, _ = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http), pytest.raises(NewsError):
        http.get(BAD + '/article')
    original = json.loads(cache_path(tmp_path).read_bytes())
    revised = deepcopy(config)
    revised['providers'][0]['article_selectors'] = ['.different']
    http, session = http_fixture(revised, tmp_path, {
        BAD + '/robots.txt': [UnreadErrorResponse(status=504), FakeResponse(ALLOW)],
        BAD + '/article': FakeResponse(b'actual body')})
    with closing(http):
        assert http.registry.sources['publisher'].data_fingerprint != original['policy_retry']['source_data_fingerprint']
        with pytest.raises(NewsError, match='robots_origin_policy_cooldown'):
            http.get(BAD + '/article')
        assert not session.calls and json.loads(cache_path(tmp_path).read_bytes()) == original
        clock.value += 31
        with pytest.raises(NewsError, match='http_504'):
            http.get(BAD + '/article')
        second = json.loads(cache_path(tmp_path).read_bytes())['policy_retry']
        assert second['generation'] == 2 and second['delay_seconds'] == 60
        clock.value += 61
        assert http.get(BAD + '/article').body == b'actual body'
        assert not http.blocked_robots_origins('publisher')
        assert 'policy_retry' not in json.loads(cache_path(tmp_path).read_bytes())
        assert http.metrics.snapshot()['providers']['publisher']['counts']['robots_origin_policy_recoveries'] == 1
        assert len(session.calls) == 3


def test_expired_failure_never_falls_back_to_old_allow_and_304_revalidates_trusted_policy(config, tmp_path, monkeypatch):
    clock = clock_fixture(monkeypatch)
    config['robots_ttl_seconds'] = 1
    http, session = http_fixture(config, tmp_path, {
        BAD + '/robots.txt': [FakeResponse(ALLOW, headers={'ETag': 'trusted'}),
                             UnreadErrorResponse(status=502), FakeResponse(status=304)],
        BAD + '/article': FakeResponse(b'actual body')})
    with closing(http):
        http.get(BAD + '/article')
        clock.value += 2
        with pytest.raises(NewsError, match='http_502'):
            http.get(BAD + '/article')
        stored = json.loads(cache_path(tmp_path).read_bytes())
        assert stored['body'] == ALLOW.decode() and stored['etag'] == 'trusted'
        assert stored['checked_at'] < clock.value and 'policy_retry' in stored
        with pytest.raises(NewsError, match='robots_origin_policy_cooldown'):
            http.get(BAD + '/article')
        clock.value += 31
        http.get(BAD + '/article')
        assert len(session.calls) == 5  # Two article bodies; one failed policy; actual 304.
        assert session.calls[-2][1]['headers']['If-None-Match'] == 'trusted'
        assert 'policy_retry' not in json.loads(cache_path(tmp_path).read_bytes())


def test_invalid_representation_etag_is_not_a_trusted_empty_policy(config, tmp_path, monkeypatch):
    clock = clock_fixture(monkeypatch)
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': [
        FakeResponse(b'<html><title>unavailable</title></html>', headers={'ETag': 'untrusted'}),
        FakeResponse(status=304)]})
    with closing(http):
        with pytest.raises(NewsError, match='robots_not_plain_rules'):
            http.get(BAD + '/article')
        checkpoint = json.loads(cache_path(tmp_path).read_bytes())
        assert set(checkpoint) == {'origin', 'policy_retry'}
        assert not http.provider_not_before
        clock.value += 301
        with pytest.raises(NewsError, match='robots_unavailable'):
            http.get(BAD + '/article')
        assert 'If-None-Match' not in session.calls[-1][1]['headers']
        assert len(session.calls) == 2 and not http.robots_parsers


def test_invalid_negative_state_requires_actual_policy_not_stale_allow(config, tmp_path):
    cache_path(tmp_path).parent.mkdir(parents=True)
    atomic_write_json(cache_path(tmp_path), {'origin': BAD, 'body': ALLOW.decode(), 'status': 200,
        'checked_at': time.time(), 'policy_retry': {'permission': True}})
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http), pytest.raises(NewsError, match='http_502'):
        http.get(BAD + '/article')
    assert len(session.calls) == 1


def test_missing_policy_redirect_location_uses_one_request_not_six_identical_calls(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': FakeResponse(status=302)})
    with closing(http), pytest.raises(NewsError, match='robots_redirect_missing_location'):
        http.get(BAD + '/article')
    assert len(session.calls) == 1 and not http.provider_not_before
    assert json.loads(cache_path(tmp_path).read_bytes())['policy_retry']['delay_seconds'] == 300


def test_repeated_policy_failures_back_off_to_bounded_five_minute_cap(config, tmp_path, monkeypatch):
    clock = clock_fixture(monkeypatch)
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http):
        for generation, delay in [(1, 30), (2, 60), (3, 120), (4, 240), (5, 300), (5, 300)]:
            with pytest.raises(NewsError, match='http_502'):
                http.get(BAD + '/article')
            state = json.loads(cache_path(tmp_path).read_bytes())['policy_retry']
            assert state['generation'] == generation and state['delay_seconds'] == delay
            with pytest.raises(NewsError, match='robots_origin_policy_cooldown'):
                http.get(BAD + '/article')
            clock.value += delay + 1
        assert len(session.calls) == 6 and not http.provider_not_before


def test_body_challenge_preserves_provider_wide_pause_not_origin_isolation(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': FakeResponse(
        b'<html><title>Just a moment</title><form id="challenge-form"></form></html>')})
    with closing(http), pytest.raises(NewsError, match='robots_access_challenge') as failure:
        http.get(BAD + '/article')
    assert not getattr(failure.value, 'robots_origin_isolation', False)
    assert http.provider_not_before['publisher'] > time.time() + 899
    assert not cache_path(tmp_path).exists() and len(session.calls) == 1


def test_policy_checkpoint_io_failure_conservatively_pauses_provider(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    def reject_checkpoint(path, *args, **kwargs):
        if path.parent.name == 'robots':
            raise OSError('full device')
        return atomic_write_json(path, *args, **kwargs)
    monkeypatch.setattr('downloader.news_fulltext_http.atomic_write_json', reject_checkpoint)
    with closing(http), pytest.raises(NewsError, match='robots_policy_checkpoint_failure'):
        http.get(BAD + '/article')
    assert http.provider_not_before['publisher'] > time.time()
    assert http.metrics.snapshot()['providers']['publisher']['counts']['robots_policy_checkpoint_write_errors'] == 1


def test_readonly_audit_verifies_origin_decisions_without_article_or_header_export(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502),
        GOOD + '/robots.txt': FakeResponse(ALLOW), GOOD + '/article': FakeResponse(b'body')})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        with pytest.raises(NewsError):
            http.get(BAD + '/article?private=SECRET')
        http.get(GOOD + '/article')
        publish_status(queue, http)
        before = queue.db.total_changes
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['accepted'] and result['robots_origin_policy_contract_matches']
        provider = result['providers']['publisher']
        assert provider['observed_robots_origin_decision_count'] == 1
        assert provider['observed_gateway_decision_count'] == 0
        assert provider['verified_robots_origin_decisions'][0]['initial_origin'] == BAD
        assert provider['recovery_counts']['robots_origin_unreachable_headers'] == 1
        assert 'SECRET' not in json.dumps(result) and 'requested_url' not in json.dumps(result)
        assert not result['publisher_requests'] and queue.db.total_changes == before and len(session.calls) == 3


def test_redirected_policy_timeout_keeps_role_proof_and_provider_failure_detection(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        BAD + '/robots.txt': FakeResponse(status=302, headers={'Location': GOOD + '/home'}),
        GOOD + '/home': FailedResponse(requests.ReadTimeout('PRIVATE_ERROR'))})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        with pytest.raises(NewsError, match='transport_error'):
            http.get(BAD + '/article')
        publish_status(queue, http)
        result = audit(tmp_path, http.registry, ['publisher'])
        assert result['accepted']
        assert result['providers']['publisher']['verified_failure_events'][0]['endpoint'] == 'robots'
        assert result['providers']['publisher']['recovery_counts']['failed_sessions_reset'] == 1
        assert 'PRIVATE_ERROR' not in json.dumps(result) and not http.blocked_robots_origins('publisher')


@pytest.mark.parametrize('version', [True, '1', 0, 2, {}])
def test_present_invalid_runtime_origin_contract_never_passes_readonly_audit(config, tmp_path, version):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        publish_status(queue, http)
        snapshot = json.loads((tmp_path / 'status.json').read_bytes())
        snapshot['performance']['robots_origin_policy_contract_version'] = version
        atomic_write_json(tmp_path / 'status.json', snapshot)
        result = audit(tmp_path, http.registry, ['publisher'])
        assert not result['accepted'] and not result['robots_origin_policy_contract_matches']


def test_policy_deadline_cannot_clear_a_later_shared_source_refusal(config, tmp_path, monkeypatch):
    clock = clock_fixture(monkeypatch)
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502),
        GOOD + '/robots.txt': UnreadErrorResponse(status=429)})
    with closing(http):
        with pytest.raises(NewsError, match='http_502'):
            http.get(BAD + '/article')
        with pytest.raises(NewsError, match='http_429'):
            http.get(GOOD + '/article')
        until = http.provider_not_before['publisher']
        clock.value += 31
        # The bounded 429 wait may have expired, but its learned rate survives.
        assert http.rate_refusal_intervals['publisher'] > 0
        http.defer('publisher', 900)
        with pytest.raises(NewsError, match='provider_cooldown'):
            http.get(BAD + '/article')
        assert http.provider_not_before['publisher'] >= until and len(session.calls) == 2


@pytest.mark.parametrize('state,dated', [('pending', True), ('pending', False), ('retry', True), ('retry_cooldown', False)])
def test_exact_origin_filtered_before_article_limit_without_changing_blocked_row(config, tmp_path, state, dated):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        rows = [{'url': BAD + '/newest', **({'date': '2026-10-11'} if dated else {})},
                {'url': GOOD + '/older', 'date': '2014-01-01'}]
        with queue.db:
            queue.add_articles(rows)
            queue.db.execute('UPDATE articles SET state=?,due=0', (state,))
        before = tuple(queue.db.execute('SELECT state,attempts,due,lease_until,body_sha256 FROM articles WHERE url=?',
            (BAD + '/newest',)).fetchone())
        assert queue.claim('publisher', 300, excluded_origins=(BAD,))['url'] == GOOD + '/older'
        assert tuple(queue.db.execute('SELECT state,attempts,due,lease_until,body_sha256 FROM articles WHERE url=?',
            (BAD + '/newest',)).fetchone()) == before
        assert queue.claim('publisher', 300, excluded_origins=(BAD,)) is None


def test_cnyes_union_and_index_survive_origin_filter_and_http_https_stay_distinct(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': 'https://amp-news.cnyes.com/news/id/1', 'date': '2026-10-11'},
                {'url': 'http://amp-news.cnyes.com/news/id/2', 'date': '2014-01-01'},
                {'url': 'https://news.cnyes.com/news/id/3'}])
        blocked = ('https://amp-news.cnyes.com',)
        query, params = host_filtered_query(cnyes_statement(), ('cnyes', 'pending', time.time(), 0), (), excluded_origins=blocked)
        plans = [row[3] for row in queue.db.execute('EXPLAIN QUERY PLAN ' + query, params)]
        assert any('SEARCH articles USING INDEX dispatch_cnyes_body_value' in plan for plan in plans)
        assert not any('TEMP B-TREE' in plan or 'SCAN articles' in plan for plan in plans)
        assert queue.claim('cnyes', 300, excluded_origins=blocked)['url'] == 'http://amp-news.cnyes.com/news/id/2'
        assert queue.claim('cnyes', 300, excluded_origins=blocked)['url'] == 'https://news.cnyes.com/news/id/3'
        assert tuple(queue.db.execute("SELECT state,attempts FROM articles WHERE url='https://amp-news.cnyes.com/news/id/1'").fetchone()) == ('pending', 0)


def test_origin_bind_filter_never_matches_sibling_host_port_or_scheme():
    import sqlite3
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE articles (url TEXT, provider TEXT)')
        urls = [BAD + '/article', 'http://bad.publisher.test/article', BAD + ':8443/article',
                'https://bad.publisher.test.other/article']
        db.executemany('INSERT INTO articles VALUES (?,?)', [(url, 'publisher') for url in urls])
        statement = 'SELECT url FROM articles WHERE provider=? ORDER BY url LIMIT ?'
        assert host_filtered_query(statement, ('publisher', 4), ()) == (statement, ('publisher', 4))
        filtered, params = host_filtered_query(statement, ('publisher', 4), (), excluded_origins=(BAD,))
        assert {r[0] for r in db.execute(filtered, params)} == set(urls[1:])
    assert policy_origin(BAD + '/article') == BAD


def test_paused_due_zero_article_and_discovery_sleep_until_origin_deadline_not_busy_loop(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': BAD + '/article'}])
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'feed','publisher',0)", (BAD + '/feed',))
        deadline = time.time() + 25
        scopes = {'publisher': {'origins': {BAD: deadline}, 'hosts': {}}}
        assert 24 < queue.next_wake_delay({}, request_scope_cooldowns=scopes) <= 25
        assert not queue.discovery_candidates(queue.registry.sources['publisher'], 1, excluded_origins=(BAD,))
        assert 49 < queue.next_wake_delay({'publisher': time.time() + 50}, request_scope_cooldowns=scopes) <= 50
        with queue.db:
            queue.add_articles([{'url': GOOD + '/article'}])
        assert queue.next_wake_delay({}, request_scope_cooldowns=scopes) == 0
        assert queue.next_wake_delay({}, request_scope_cooldowns={
            'publisher': {'origins': {BAD: time.time() - 1}}}) == 0


def test_paused_gateway_host_alone_also_cannot_cause_empty_cycle_spin(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': BAD + '/article'}])
        delay = queue.next_wake_delay({}, request_scope_cooldowns={
            'publisher': {'hosts': {'bad.publisher.test': time.time() + 20}}})
        assert 19 < delay <= 20


def test_discovery_origin_filter_precedes_limit_and_keeps_correlated_endpoint_cooldowns(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        source = queue.registry.sources['cnyes']
        api = source.cnyes_apis[0]
        blocked = 'https://amp-news.cnyes.com'
        with queue.db:
            queue.db.executemany('INSERT INTO discovery(url,kind,provider,due) VALUES (?,?,?,0)',
                [(blocked + '/feed/' + str(n), 'feed', 'cnyes') for n in range(100)] +
                [(api + '?page=1', 'cnyes_news', 'cnyes'), ('https://news.cnyes.com/feed', 'feed', 'cnyes')])
            queue.db.execute('INSERT INTO bulk_endpoint_cooldowns(provider,endpoint,data_fingerprint,not_before,error_code,request_url) '
                'VALUES (?,?,?,?,?,?)', ('cnyes', api, source.data_fingerprint, time.time() + 100, 'http_503', api + '?page=1'))
        assert queue.discovery_candidates(source, 1, excluded_origins=(blocked,))[0]['url'] == 'https://news.cnyes.com/feed'
        with queue.db:
            queue.db.execute("DELETE FROM discovery WHERE url='https://news.cnyes.com/feed'")
        assert not queue.discovery_candidates(source, 1, excluded_origins=(blocked,))
        with queue.db:
            queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=0')
        assert queue.discovery_candidates(source, 1, excluded_origins=(blocked,))[0]['url'] == api + '?page=1'
        # Filtering never consumes a blocked job or falsifies its prior result.
        assert queue.db.execute('SELECT sum(attempts) FROM discovery').fetchone()[0] == 0


@pytest.mark.parametrize('mutate', [
    lambda state: state.update(generation=True), lambda state: state.update(not_before_epoch=float('inf')),
    lambda state: state.update(observed_at_epoch=True), lambda state: state.update(error_code='HTML<script>'),
    lambda state: state.update(source_data_fingerprint='bad'), lambda state: state.update(permission_granted=True),
])
def test_negative_cache_rejects_corrupt_contract_without_granting_access(config, tmp_path, mutate):
    http, _ = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http), pytest.raises(NewsError):
        http.get(BAD + '/article')
    cache = json.loads(cache_path(tmp_path).read_bytes())
    assert robots_policy_retry(cache, BAD, time.time()) is not None
    mutate(cache['policy_retry'])
    assert robots_policy_retry(cache, BAD, time.time()) is None
