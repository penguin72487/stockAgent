"""REP-origin failure isolation never grants access or clears source refusals."""
from contextlib import closing
from copy import deepcopy
import hashlib
import json
import threading
import time
from types import SimpleNamespace

import pytest
import requests

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_host_circuit import host_filtered_query
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, public_address_check
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


@pytest.mark.parametrize('addresses', [[], ['10.0.0.1'], ['93.184.216.34', '10.0.0.1']])
def test_non_public_rep_addresses_fail_closed_once_per_origin_not_per_article(config, tmp_path, monkeypatch, addresses):
    lookups = []
    def resolve(host, port, **kwargs):
        lookups.append(host)
        return [(2, 1, 6, '', (address, port)) for address in (
            addresses if host == 'bad.publisher.test' else ['93.184.216.34'])]
    monkeypatch.setattr('downloader.news_fulltext_policy.socket.getaddrinfo', resolve)
    http, session = http_fixture(config, tmp_path, {
        GOOD + '/robots.txt': FakeResponse(ALLOW), GOOD + '/article': FakeResponse(b'body')})
    http.address_check = public_address_check
    with closing(http):
        with pytest.raises(NewsError, match='^robots_non_public_address$') as failure:
            http.get(BAD + '/article')
        assert failure.value.retry and failure.value.robots_origin_isolation
        checkpoint = json.loads(cache_path(tmp_path).read_bytes())
        assert checkpoint['policy_retry']['error_code'] == 'robots_non_public_address'
        assert checkpoint['policy_retry']['delay_seconds'] == 30
        for index in range(20):
            with pytest.raises(NewsError, match='robots_origin_policy_cooldown') as deferred:
                http.get(BAD + '/article/' + str(index))
            assert deferred.value.request_deferred
        assert lookups == ['bad.publisher.test'] and not session.calls
        assert http.get(GOOD + '/article').body == b'body'
        assert [url for url, _ in session.calls] == [GOOD + '/robots.txt', GOOD + '/article']
        assert not http.provider_not_before and not http.blocked_gateway_hosts('publisher')
        assert http.metrics.snapshot()['providers']['publisher']['counts']['robots_pretransport_address_rejections'] == 1
        assert json.loads(cache_path(tmp_path).read_bytes()) == checkpoint


def test_rep_address_negative_cache_survives_restart_and_requires_real_policy(config, tmp_path, monkeypatch):
    clock, public, lookups = clock_fixture(monkeypatch), [False], []
    def resolve(host, port, **kwargs):
        lookups.append(host)
        return [(2, 1, 6, '', ('93.184.216.34' if public[0] else '10.0.0.1', port))]
    monkeypatch.setattr('downloader.news_fulltext_policy.socket.getaddrinfo', resolve)
    http, session = http_fixture(config, tmp_path, {})
    http.address_check = public_address_check
    with closing(http), pytest.raises(NewsError, match='robots_non_public_address'):
        http.get(BAD + '/article')
    assert not session.calls and len(lookups) == 1
    http, session = http_fixture(config, tmp_path, {
        BAD + '/robots.txt': FakeResponse(b'User-agent: *\nDisallow: /\n')})
    http.address_check = public_address_check
    with closing(http):
        # The scheduler and status must see the durable pause immediately,
        # before leasing an article or making another policy request.
        assert http.blocked_robots_origins('publisher') == {BAD: clock.value + 30}
        assert http.request_scope_cooldown_snapshot()['publisher']['origins'] == {BAD: clock.value + 30}
        assert not session.calls
        with pytest.raises(NewsError, match='robots_origin_policy_cooldown'):
            http.get(BAD + '/article')
        assert len(lookups) == 1 and not session.calls
        clock.value += 31
        with pytest.raises(NewsError, match='robots_non_public_address'):
            http.get(BAD + '/article')
        retry = json.loads(cache_path(tmp_path).read_bytes())['policy_retry']
        assert retry['generation'] == 2 and retry['delay_seconds'] == 60
        assert len(lookups) == 2 and not session.calls
        public[0] = True
        clock.value += 61
        with pytest.raises(NewsError, match='^robots_disallowed$') as refusal:
            http.get(BAD + '/article')
        assert not refusal.value.retry
        assert [url for url, _ in session.calls] == [BAD + '/robots.txt']
        assert not http.blocked_robots_origins('publisher')
        assert 'policy_retry' not in json.loads(cache_path(tmp_path).read_bytes())


def test_expired_rep_failure_survives_restart_for_status_until_real_recheck(config, tmp_path, monkeypatch):
    clock = clock_fixture(monkeypatch)
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http), pytest.raises(NewsError, match='http_502'):
        http.get(BAD + '/article')
    deadline = clock.value + 30
    clock.value = deadline + 1
    http, session = http_fixture(config, tmp_path, {
        BAD + '/robots.txt': FakeResponse(ALLOW), BAD + '/article': FakeResponse(b'body')})
    with closing(http):
        before = http.capacity_snapshot()['providers']['publisher']
        assert not before['temporarily_paused_robots_origins']
        assert before['unverified_robots_origins'] == {BAD: deadline}
        assert before['unverified_robots_origins_overflow'] is False
        assert not session.calls
        assert http.get(BAD + '/article').body == b'body'
        after = http.capacity_snapshot()['providers']['publisher']
        assert not after['unverified_robots_origins']
        assert [url for url, _ in session.calls] == [BAD + '/robots.txt', BAD + '/article']


@pytest.mark.parametrize('variant', ['expired', 'wrong_file', 'bad_retry', 'foreign_origin', 'disabled'])
def test_restart_ignores_nonapplicable_negative_policy_state(config, tmp_path, monkeypatch, variant):
    clock = clock_fixture(monkeypatch)
    http, _ = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http), pytest.raises(NewsError, match='http_502'):
        http.get(BAD + '/article')
    checkpoint_path = cache_path(tmp_path)
    checkpoint = json.loads(checkpoint_path.read_bytes())
    if variant == 'expired':
        clock.value += 31
    elif variant == 'wrong_file':
        checkpoint['origin'] = GOOD
        atomic_write_json(checkpoint_path, checkpoint)
    elif variant == 'bad_retry':
        checkpoint['policy_retry']['not_before_epoch'] = float('inf')
        atomic_write_json(checkpoint_path, checkpoint)
    elif variant == 'foreign_origin':
        checkpoint_path.unlink()
        checkpoint['origin'] = 'https://elsewhere.test'
        atomic_write_json(cache_path(tmp_path, checkpoint['origin']), checkpoint)
    else:
        config['providers'][0]['enabled'] = False
    http, session = http_fixture(config, tmp_path, {})
    with closing(http):
        assert not http.blocked_robots_origins('publisher')
        assert not session.calls


def test_restart_bounds_restored_origins_with_existing_provider_fallback(config, tmp_path, monkeypatch):
    clock_fixture(monkeypatch)
    http, _ = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http), pytest.raises(NewsError, match='http_502'):
        http.get(BAD + '/article')
    checkpoint = json.loads(cache_path(tmp_path).read_bytes())
    for index in range(32):
        origin = f'https://bad{index}.publisher.test'
        atomic_write_json(cache_path(tmp_path, origin), {**checkpoint, 'origin': origin})
    http, session = http_fixture(config, tmp_path, {})
    with closing(http):
        assert len(http.blocked_robots_origins('publisher')) == 32
        control = http.capacity_snapshot()['providers']['publisher']
        assert len(control['unverified_robots_origins']) == 32
        assert control['unverified_robots_origins_overflow'] is True
        assert http.provider_not_before['publisher'] > time.time()
        assert not session.calls


def expired_overflow_fixture(config, tmp_path, monkeypatch, count=33):
    clock = clock_fixture(monkeypatch)
    http, _ = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http), pytest.raises(NewsError, match='http_502'):
        http.get(BAD + '/article')
    checkpoint = json.loads(cache_path(tmp_path).read_bytes())
    origins = [BAD] + [f'https://bad{index}.publisher.test' for index in range(count - 1)]
    for origin in origins[1:]:
        atomic_write_json(cache_path(tmp_path, origin), {**checkpoint, 'origin': origin})
    clock.value += 31
    return clock, origins


def wait_for_recheck_scan(http):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        http.capacity_snapshot()
        with http.lock:
            done = ('publisher' not in http.robots_recheck_rescan_needed
                    and not http.robots_recheck_scan_lock.locked())
        if done:
            return http.capacity_snapshot()['providers']['publisher']
        time.sleep(0.01)
    raise AssertionError('status-only recheck scan did not converge')


@pytest.mark.parametrize('recover_tracked', [True, False])
def test_overflow_reconciles_after_actual_recovery_without_restart(config, tmp_path, monkeypatch, recover_tracked):
    _, origins = expired_overflow_fixture(config, tmp_path, monkeypatch)
    routes = {origin + '/robots.txt': FakeResponse(ALLOW) for origin in origins}
    http, session = http_fixture(config, tmp_path, routes)
    with closing(http):
        before = http.capacity_snapshot()['providers']['publisher']
        assert before['unverified_robots_origins_overflow'] is True
        tracked = set(before['unverified_robots_origins'])
        origin = next(iter(tracked if recover_tracked else set(origins) - tracked))
        http._robots(origin + '/article')
        calls = len(session.calls)
        after = wait_for_recheck_scan(http)
        assert after['unverified_robots_origins_overflow'] is False
        assert set(after['unverified_robots_origins']) == set(origins) - {origin}
        assert not after['temporarily_paused_robots_origins']
        assert len(session.calls) == calls == 1  # The status repair sends no requests.
        assert 'policy_retry' not in json.loads(cache_path(tmp_path, origin).read_bytes())


def test_overflow_stays_unknown_until_all_untracked_failures_fit(config, tmp_path, monkeypatch):
    _, origins = expired_overflow_fixture(config, tmp_path, monkeypatch, count=35)
    http, session = http_fixture(config, tmp_path, {origin + '/robots.txt': FakeResponse(ALLOW) for origin in origins})
    with closing(http):
        for index, origin in enumerate(origins[:3], start=1):
            http._robots(origin + '/article')
            control = wait_for_recheck_scan(http)
            assert control['unverified_robots_origins_overflow'] is (index < 3)
            assert len(control['unverified_robots_origins']) == 32
        assert set(control['unverified_robots_origins']) == set(origins[3:])
        assert len(session.calls) == 3


def test_overflow_rescan_does_not_lose_concurrent_uncommitted_failure(config, tmp_path, monkeypatch):
    _, origins = expired_overflow_fixture(config, tmp_path, monkeypatch)
    new_origin = 'https://new.publisher.test'
    routes = {origins[0] + '/robots.txt': FakeResponse(ALLOW),
              origins[1] + '/robots.txt': FakeResponse(ALLOW),
              new_origin + '/robots.txt': UnreadErrorResponse(status=502)}
    http, _ = http_fixture(config, tmp_path, routes)
    started, release, failures = threading.Event(), threading.Event(), []

    def delayed_checkpoint(path, *args, **kwargs):
        if path == cache_path(tmp_path, new_origin):
            started.set()
            assert release.wait(10)
        return atomic_write_json(path, *args, **kwargs)

    def fail_new_origin():
        try:
            http.get(new_origin + '/article')
        except NewsError as error:
            failures.append(error.code)

    monkeypatch.setattr('downloader.news_fulltext_http.atomic_write_json', delayed_checkpoint)
    worker = threading.Thread(target=fail_new_origin)
    with closing(http):
        try:
            worker.start()
            assert started.wait(10)
            http._robots(origins[0] + '/article')
            assert http.capacity_snapshot()['providers']['publisher']['unverified_robots_origins_overflow'] is True
        finally:
            release.set()
            worker.join(10)
        assert not worker.is_alive() and failures == ['http_502']
        assert wait_for_recheck_scan(http)['unverified_robots_origins_overflow'] is True
        http._robots(origins[1] + '/article')
        final = wait_for_recheck_scan(http)
        assert final['unverified_robots_origins_overflow'] is False
        assert set(final['unverified_robots_origins']) == (set(origins[2:]) | {new_origin})


def test_capacity_fallback_still_persists_the_new_failed_origin(config, tmp_path, monkeypatch):
    clock = clock_fixture(monkeypatch)
    http, _ = http_fixture(config, tmp_path, {BAD + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http), pytest.raises(NewsError, match='http_502'):
        http.get(BAD + '/article')
    checkpoint = json.loads(cache_path(tmp_path).read_bytes())
    for index in range(31):
        origin = f'https://bad{index}.publisher.test'
        atomic_write_json(cache_path(tmp_path, origin), {**checkpoint, 'origin': origin})
    new_origin = 'https://new.publisher.test'
    http, _ = http_fixture(config, tmp_path, {new_origin + '/robots.txt': UnreadErrorResponse(status=502)})
    with closing(http):
        assert len(http.blocked_robots_origins('publisher')) == 32
        with pytest.raises(NewsError, match='robots_origin_policy_capacity_exhausted'):
            http.get(new_origin + '/article')
        assert http.provider_not_before['publisher'] > clock.value
        stored = json.loads(cache_path(tmp_path, new_origin).read_bytes())
        assert stored['origin'] == new_origin and stored['policy_retry']['error_code'] == 'http_502'
        assert http.capacity_snapshot()['providers']['publisher']['unverified_robots_origins_overflow'] is True


def test_overflow_scan_does_not_block_status_or_healthy_origin(config, tmp_path, monkeypatch):
    _, origins = expired_overflow_fixture(config, tmp_path, monkeypatch)
    http, session = http_fixture(config, tmp_path, {
        origins[0] + '/robots.txt': FakeResponse(ALLOW), GOOD + '/robots.txt': FakeResponse(ALLOW)})
    http._robots(origins[0] + '/article')
    started, release = threading.Event(), threading.Event()
    original = type(tmp_path).read_bytes

    def delayed_read(path):
        if path == cache_path(tmp_path, origins[1]) and not release.is_set():
            started.set()
            assert release.wait(10)
        return original(path)

    monkeypatch.setattr(type(tmp_path), 'read_bytes', delayed_read)
    with closing(http):
        try:
            assert http.capacity_snapshot()['providers']['publisher']['unverified_robots_origins_overflow'] is True
            assert started.wait(10)
            # The blocked reader owns no HTTP or scheduler lock.
            assert http.capacity_snapshot()['providers']['publisher']['unverified_robots_origins_overflow'] is True
            http._robots(GOOD + '/article')
        finally:
            release.set()
        result = wait_for_recheck_scan(http)
        assert result['unverified_robots_origins_overflow'] is False
        assert set(result['unverified_robots_origins']) == set(origins[1:])
        assert len(session.calls) == 2  # Recovery and healthy policy checks only.


def test_failed_checkpoint_blocks_rescan_until_same_origin_recovered(config, tmp_path, monkeypatch):
    clock, origins = expired_overflow_fixture(config, tmp_path, monkeypatch)
    new_origin = 'https://new.publisher.test'
    http, _ = http_fixture(config, tmp_path, {
        new_origin + '/robots.txt': [UnreadErrorResponse(status=502), FakeResponse(ALLOW)],
        origins[0] + '/robots.txt': FakeResponse(ALLOW)})
    reject_once = [True]

    def failed_write(path, *args, **kwargs):
        if path == cache_path(tmp_path, new_origin) and reject_once[0]:
            reject_once[0] = False
            raise OSError('temporary storage failure')
        return atomic_write_json(path, *args, **kwargs)

    monkeypatch.setattr('downloader.news_fulltext_http.atomic_write_json', failed_write)
    with closing(http):
        with pytest.raises(NewsError, match='robots_policy_checkpoint_failure'):
            http._robots(new_origin + '/article')
        clock.value += 301
        http._robots(origins[0] + '/article')
        assert http.capacity_snapshot()['providers']['publisher']['unverified_robots_origins_overflow'] is True
        assert new_origin in http.robots_recheck_unpersisted['publisher']
        http._robots(new_origin + '/article')
        result = wait_for_recheck_scan(http)
        assert result['unverified_robots_origins_overflow'] is False
        assert set(result['unverified_robots_origins']) == set(origins[1:])


def test_overflow_scan_limit_keeps_unknown_and_a_later_recovery_retries(config, tmp_path, monkeypatch):
    _, origins = expired_overflow_fixture(config, tmp_path, monkeypatch)
    http, session = http_fixture(config, tmp_path, {
        origins[0] + '/robots.txt': FakeResponse(ALLOW), origins[1] + '/robots.txt': FakeResponse(ALLOW)})
    with closing(http):
        monkeypatch.setattr('downloader.news_fulltext_http.MAX_ROBOTS_RECHECK_SCAN_FILES', 1)
        http._robots(origins[0] + '/article')
        http.capacity_snapshot()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with http.lock:
                failed = http.robots_recheck_rescan_not_before.get('publisher', 0) > time.monotonic()
            if failed and not http.robots_recheck_scan_lock.locked():
                break
            time.sleep(0.01)
        else:
            raise AssertionError('bounded scan did not record its retry deadline')
        control = http.capacity_snapshot()['providers']['publisher']
        assert control['unverified_robots_origins_overflow'] is True
        assert not http.robots_recheck_scan_lock.locked()
        monkeypatch.setattr('downloader.news_fulltext_http.MAX_ROBOTS_RECHECK_SCAN_FILES', 65536)
        http._robots(origins[1] + '/article')
        result = wait_for_recheck_scan(http)
        assert result['unverified_robots_origins_overflow'] is False
        assert set(result['unverified_robots_origins']) == set(origins[2:])
        assert len(session.calls) == 2


def test_overflow_scan_retries_if_checkpoint_changes_in_place(config, tmp_path, monkeypatch):
    _, origins = expired_overflow_fixture(config, tmp_path, monkeypatch)
    http, session = http_fixture(config, tmp_path, {origins[0] + '/robots.txt': FakeResponse(ALLOW)})
    with closing(http):
        http._robots(origins[0] + '/article')
        target = cache_path(tmp_path, origins[1])
        read_bytes = type(tmp_path).read_bytes
        mutated = threading.Event()

        def concurrent_write(path):
            payload = read_bytes(path)
            if path == target and not mutated.is_set():
                path.write_bytes(payload + b' ')
                mutated.set()
            return payload

        monkeypatch.setattr(type(tmp_path), 'read_bytes', concurrent_write)
        assert http.capacity_snapshot()['providers']['publisher']['unverified_robots_origins_overflow'] is True
        assert mutated.wait(10)
        deadline = time.monotonic() + 10
        while http.robots_recheck_scan_lock.locked() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not http.robots_recheck_scan_lock.locked()
        assert 'publisher' in http.robots_policy_rechecks_overflow
        assert 'publisher' in http.robots_recheck_rescan_needed
        result = wait_for_recheck_scan(http)
        assert result['unverified_robots_origins_overflow'] is False
        assert set(result['unverified_robots_origins']) == set(origins[1:])
        assert len(session.calls) == 1


def test_corrupt_checkpoint_origin_cannot_clear_overflow(config, tmp_path, monkeypatch):
    _, origins = expired_overflow_fixture(config, tmp_path, monkeypatch)
    http, session = http_fixture(config, tmp_path, {
        origins[0] + '/robots.txt': FakeResponse(ALLOW), origins[1] + '/robots.txt': FakeResponse(ALLOW)})
    with closing(http):
        target = cache_path(tmp_path, origins[2])
        original = target.read_bytes()
        corrupt = json.loads(original)
        corrupt['origin'] = 'https://other.publisher.test'
        target.write_text(json.dumps(corrupt))
        http._robots(origins[0] + '/article')
        assert http.capacity_snapshot()['providers']['publisher']['unverified_robots_origins_overflow'] is True
        deadline = time.monotonic() + 10
        while not http.robots_recheck_rescan_not_before.get('publisher') and time.monotonic() < deadline:
            time.sleep(0.01)
        assert http.robots_recheck_rescan_not_before.get('publisher')
        assert 'publisher' in http.robots_policy_rechecks_overflow
        target.write_bytes(original)
        http._robots(origins[1] + '/article')
        result = wait_for_recheck_scan(http)
        assert result['unverified_robots_origins_overflow'] is False
        assert set(result['unverified_robots_origins']) == set(origins[2:])
        assert len(session.calls) == 2


def test_non_public_rep_redirect_is_never_requested_and_keeps_initial_scope(config, tmp_path, monkeypatch):
    def resolve(host, port, **kwargs):
        return [(2, 1, 6, '', ('10.0.0.1' if host == 'good.publisher.test' else '93.184.216.34', port))]
    monkeypatch.setattr('downloader.news_fulltext_policy.socket.getaddrinfo', resolve)
    http, session = http_fixture(config, tmp_path, {
        BAD + '/robots.txt': FakeResponse(status=302, headers={'Location': GOOD + '/policy'})})
    http.address_check = public_address_check
    with closing(http), pytest.raises(NewsError, match='robots_non_public_address'):
        http.get(BAD + '/article')
    assert [url for url, _ in session.calls] == [BAD + '/robots.txt']
    assert set(http.blocked_robots_origins('publisher')) == {BAD}
    assert not http.provider_not_before


def test_private_article_address_after_public_rep_is_still_terminal(config, tmp_path, monkeypatch):
    lookups = []
    def resolve(host, port, **kwargs):
        lookups.append(host)
        return [(2, 1, 6, '', ('93.184.216.34' if len(lookups) == 1 else '10.0.0.1', port))]
    monkeypatch.setattr('downloader.news_fulltext_policy.socket.getaddrinfo', resolve)
    http, session = http_fixture(config, tmp_path, {BAD + '/robots.txt': FakeResponse(ALLOW)})
    http.address_check = public_address_check
    with closing(http), pytest.raises(NewsError, match='^non_public_address$') as failure:
        http.get(BAD + '/article')
    assert not failure.value.retry and not getattr(failure.value, 'robots_origin_isolation', False)
    assert len(lookups) == 2 and [url for url, _ in session.calls] == [BAD + '/robots.txt']
    assert not http.blocked_robots_origins('publisher')


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
