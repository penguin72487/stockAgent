"""Long client pacing is scheduled work, not an occupied download worker."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime
import sqlite3
import threading
import time

import pytest
from protego import Protego

from downloader import news_fulltext as main
from downloader.common import SharedRateLimiter
from downloader.news_fulltext import run_cycle, run_service
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.benchmark_news_connection_recovery import OwnedLimiter, OwnedScopedSession, owned_config, owned_servers
from test_news_fulltext import FakeLimiter, FakeResponse, FakeSession, article_html, config


URL = 'https://publisher.test/history'


def transport(config, root, *, limiter=FakeLimiter, session=None, addresses=None):
    session = session or FakeSession({})
    return NewsHTTP(PolicyRegistry(config, base=root), config, root, session_factory=lambda: session,
                    limiter_factory=limiter, address_check=lambda url: addresses.append(url) if addresses is not None else None)


def wait_for(predicate):
    deadline = time.monotonic() + 1
    while not predicate():
        assert time.monotonic() < deadline, 'owned test worker did not enter slot wait'
        time.sleep(.005)


def test_runtime_source_cooldown_expiry_keeps_real_shared_slot_before_lease_and_restart(config, tmp_path, monkeypatch):
    monkeypatch.setenv('STOCKAGENT_RATE_LIMIT_DIR', str(tmp_path / 'slots'))
    addresses, session = [], FakeSession({})
    http = transport(config, tmp_path, limiter=SharedRateLimiter, session=session, addresses=addresses)
    try:
        # Existing reserved slot outlasts a later, shorter source refusal.
        # This happens after construction, not merely on startup.
        http.limiters['publisher'].defer(120)
        http.defer('publisher', .02)
        state_path = http.limiters['publisher']._state_path
        slot_before = state_path.read_bytes()
        checkpoint_before = (tmp_path / 'cooldowns/publisher.json').read_bytes()
        time.sleep(.03)
        assert http.provider_not_before['publisher'] < time.time()
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            with queue.db:
                queue.add_articles([{'url': URL}])
                queue.db.execute('UPDATE articles SET attempts=5 WHERE url=?', (URL,))
            status = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
            row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            assert row['state'] == 'pending' and row['lease_owner'] is None and row['attempts'] == 5
            actual_due = datetime.fromisoformat(status['provider_next_attempt_utc']['publisher']).timestamp()
            assert actual_due > time.time() + 110
            assert status['http_capacity']['request_pacing_wait_contract_version'] == 1
            assert not session.calls and not addresses and not http.requests_by_provider
        assert state_path.read_bytes() == slot_before
        assert (tmp_path / 'cooldowns/publisher.json').read_bytes() == checkpoint_before
    finally:
        http.close()
    with closing(transport(config, tmp_path, limiter=SharedRateLimiter, addresses=addresses)) as restarted:
        assert 'publisher' not in restarted.provider_not_before  # Not a new source refusal.
        assert restarted.cooldown_snapshot()['publisher'] == pytest.approx(actual_due, abs=.01)
        assert state_path.read_bytes() == slot_before
        assert (tmp_path / 'cooldowns/publisher.json').read_bytes() == checkpoint_before
        assert not addresses


def test_local_capacity_change_schedules_before_dns_without_shared_file_reads(config, tmp_path):
    with closing(transport(config, tmp_path)) as http, closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        http._reserve_request(http.registry.sources['publisher'], URL)
        http.set_client_capacity('publisher', 120, 1)
        with queue.db:
            queue.add_articles([{'url': URL}])
        for _ in range(3):
            status = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
            assert status['states'] == {'pending': 1}
            assert datetime.fromisoformat(status['provider_next_attempt_utc']['publisher']).timestamp() > time.time() + 110
        with pytest.raises(NewsError, match='provider_pacing_cooldown') as caught:
            http._raw(URL)
        assert caught.value.request_deferred and not http.provider_not_before


def test_slow_grant_sends_current_request_but_never_refunds_next_shared_slot(config, tmp_path):
    class GrantedLimiter(FakeLimiter):
        deadline = 0
        reads = 0

        def wait(self, **kwargs):
            self.deadline = time.monotonic() + 120

        def remaining_delay_seconds(self):
            self.reads += 1
            return max(0, self.deadline - time.monotonic())

    session = FakeSession({URL: FakeResponse(article_html())})
    with closing(transport(config, tmp_path, limiter=GrantedLimiter, session=session)) as http:
        http.set_client_capacity('publisher', 120, 1)
        assert http._raw(URL).status == 200  # Its own grant is not a future-work gate.
        assert len(session.calls) == 1 and http.requests_by_provider == {'publisher': 1}
        due = http.cooldown_snapshot()['publisher']
        reserved_due = http.pacing_not_before['publisher']
        http.set_client_capacity('publisher', .01, 1)
        for _ in range(3):
            assert http.pacing_not_before['publisher'] == reserved_due
            assert http.cooldown_snapshot()['publisher'] == pytest.approx(due, abs=.01)
        assert http.limiters['publisher'].reads == 3  # Startup, changed slow interval, slow grant.
        with pytest.raises(NewsError, match='provider_pacing_cooldown'):
            http._raw(URL)
        assert len(session.calls) == 1 and not http.provider_not_before


def test_restored_slow_capacity_rechecks_shared_restart_hint_without_claiming(config, tmp_path, monkeypatch):
    monkeypatch.setenv('STOCKAGENT_RATE_LIMIT_DIR', str(tmp_path / 'slots'))
    previous = SharedRateLimiter(1200, name='news-fulltext:publisher')
    previous.defer(1200)
    checkpoint = previous._state_path.read_bytes()
    with closing(transport(config, tmp_path, limiter=SharedRateLimiter)) as http:
        # The short initial interval cannot distinguish this old token from a
        # host reboot. The real restored slow interval can; no token is reset.
        assert not http.cooldown_snapshot()
        http.set_client_capacity('publisher', 1200, 1)
        assert http.cooldown_snapshot()['publisher'] > time.time() + 1100
        assert previous._state_path.read_bytes() == checkpoint
        assert not http.requests_by_provider and not http.provider_not_before


def test_wall_clock_correction_cannot_refund_or_extend_elapsed_pacing(config, tmp_path, monkeypatch):
    with closing(transport(config, tmp_path)) as http:
        http.pacing_not_before['publisher'] = time.monotonic() + 120
        before = http.cooldown_snapshot()['publisher']
        wall_clock = time.time
        monkeypatch.setattr(time, 'time', lambda: wall_clock() + 600)
        assert http.cooldown_snapshot()['publisher'] - before == pytest.approx(600, abs=.01)
        with pytest.raises(NewsError, match='provider_pacing_cooldown') as caught:
            http._raw(URL)
        assert 119 < caught.value.delay <= 120
        assert not http.requests_by_provider and not http.provider_not_before


def test_unseen_shared_slot_race_is_bounded_and_preserves_attempts_and_token(config, tmp_path, monkeypatch):
    monkeypatch.setenv('STOCKAGENT_RATE_LIMIT_DIR', str(tmp_path / 'slots'))
    config['response_deadline_seconds'] = .08
    with closing(transport(config, tmp_path, limiter=SharedRateLimiter)) as http, closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        http.limiters['publisher'].defer(1)
        slot_before = http.limiters['publisher']._state_path.read_bytes()
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET attempts=5 WHERE url=?', (URL,))
        row = queue.claim_url(URL, 120)
        started = time.monotonic()
        with pytest.raises(NewsError, match='provider_pacing_cooldown') as caught:
            http._raw(URL)
        assert time.monotonic() - started < .5
        assert caught.value.request_deferred and caught.value.delay > .5
        queue.failed(row, caught.value, 6)
        restored = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert restored['attempts'] == 5 and restored['state'] == 'pending' and restored['lease_owner'] is None
        assert restored['body_sha256'] is None and restored['due'] > time.time() + .5
        assert http.limiters['publisher'].pending_waiters() == 0
        assert http.limiters['publisher']._state_path.read_bytes() == slot_before
        assert not http.requests_by_provider and not http.provider_not_before


@pytest.mark.parametrize('gate', ['refusal', 'stop'])
def test_late_gate_interrupts_shared_slot_fifo_without_network_or_lease_damage(config, tmp_path, monkeypatch, gate):
    monkeypatch.setenv('STOCKAGENT_RATE_LIMIT_DIR', str(tmp_path / 'slots'))
    with closing(transport(config, tmp_path, limiter=SharedRateLimiter)) as http:
        http.limiters['publisher'].defer(120)  # Unknown to the scheduler until it rechecks.
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(http._raw, URL)
            wait_for(lambda: http.limiters['publisher'].pending_waiters() == 1)
            if gate == 'stop':
                http.stop.set()
            else:
                http.defer('publisher', 120)
            with pytest.raises(NewsError, match='stopping' if gate == 'stop' else 'provider_cooldown') as caught:
                pending.result(timeout=1)
            assert caught.value.request_deferred
        assert http.limiters['publisher'].pending_waiters() == 0
        assert not http.requests_by_provider


@pytest.mark.parametrize('stop', [False, True])
def test_send_lock_contention_is_bounded_and_interruptible(config, tmp_path, stop):
    config['response_deadline_seconds'] = .08
    with closing(transport(config, tmp_path)) as http:
        lock = http.send_locks['publisher']
        lock.acquire()
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(http._raw, URL)
                if stop:
                    http.stop.set()
                with pytest.raises(NewsError, match='stopping' if stop else 'provider_pacing_cooldown') as caught:
                    future.result(timeout=1)
                assert caught.value.request_deferred
        finally:
            lock.release()
        assert not http.requests_by_provider


def test_long_paced_provider_cannot_block_owned_other_provider_full_capture(tmp_path):
    config = owned_config()
    config['workers'] = 1
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue, owned_servers() as servers:
        with closing(NewsHTTP(registry, config, tmp_path,
                session_factory=lambda: OwnedScopedSession(servers), limiter_factory=OwnedLimiter,
                address_check=lambda url: None, metrics=queue.metrics)) as http:
            http.last_calls['publisher'] = time.monotonic()
            http.set_client_capacity('publisher', 120, 1)
            with queue.db:
                queue.add_articles([{'url': URL}, {'url': 'https://other.test/history'}])
            status = run_cycle(queue, http, config, max_articles=2, max_discovery_requests=0)
            assert status['states'] == {'pending': 1, 'complete': 1}
            assert servers['publisher.test'].paths == []
            assert servers['other.test'].paths == ['/robots.txt', '/history']
            head = dict(queue.db.execute("SELECT * FROM articles WHERE provider='other'").fetchone())
            assert head['state'] == 'complete' and head['body_sha256']
            assert verify_bodies(tmp_path)['errors'] == []


def test_short_paced_provider_yields_worker_and_resumes_with_real_receipts(tmp_path, monkeypatch):
    """An independent ready publisher must not wait in another one's slot."""
    config = owned_config()
    config.update(workers=1, batch_articles=1, response_deadline_seconds=3)
    config['providers'][0].update(interval_seconds=2, parallelism=1)
    config['providers'][1].update(interval_seconds=.001, parallelism=1)
    other_url = 'https://other.test/history'
    session = FakeSession({URL: FakeResponse(article_html()), other_url: FakeResponse(article_html())})
    calls = []
    original_get = session.get

    def observe_get(url, **kwargs):
        calls.append((url, time.monotonic()))
        return original_get(url, **kwargs)

    session.get = observe_get
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    with closing(NewsQueue(tmp_path, registry)) as queue, closing(NewsHTTP(
            registry, config, tmp_path, session_factory=lambda: session,
            limiter_factory=FakeLimiter, address_check=lambda url: None)) as http:
        parser = Protego.parse('User-agent: *\nAllow: /')
        http.robots_parsers.update({origin: (time.time(), parser)
                                    for origin in ('https://publisher.test', 'https://other.test')})
        with queue.db:
            queue.add_articles([{'url': URL}, {'url': other_url}])
        accepted = []
        original_accept = queue.accept

        def stop_after_accept(records, **kwargs):
            result = original_accept(records, **kwargs)
            accepted.extend(records)
            if len(accepted) == 2:
                http.stop.set()
            return result

        monkeypatch.setattr(queue, 'accept', stop_after_accept)
        previous_send = time.monotonic()
        http.last_calls['publisher'] = previous_send
        assert 'publisher' not in http.cooldown_snapshot()
        assert http.scheduling_cooldown_snapshot()['publisher'] > time.time() + 1
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=6)
        assert [url for url, _ in calls] == [other_url, URL]
        assert calls[1][1] - previous_send >= 1.99
        rows = queue.db.execute('SELECT state,attempts,body_sha256 FROM articles ORDER BY url').fetchall()
        assert len(rows) == 2 and all(row['state'] == 'complete' and row['body_sha256'] for row in rows)
        assert not http.provider_not_before and not http.rate_refusal_intervals
        assert verify_bodies(tmp_path)['errors'] == []


def test_short_scheduler_pacing_expires_and_rechecks_current_capacity(config, tmp_path):
    config['providers'][0]['interval_seconds'] = 1
    config['response_deadline_seconds'] = 3
    with closing(transport(config, tmp_path)) as http:
        http.last_calls['publisher'] = time.monotonic()
        assert 'publisher' not in http.cooldown_snapshot()
        assert http.scheduling_cooldown_snapshot()['publisher'] > time.time()
        http.last_calls['publisher'] = time.monotonic() - 2
        assert 'publisher' not in http.scheduling_cooldown_snapshot()
        assert not http.provider_not_before and not http.rate_refusal_intervals


def test_prefetch_never_advances_a_source_or_shared_slot(config, tmp_path):
    config['providers'][0]['interval_seconds'] = 1
    config['response_deadline_seconds'] = 3
    with closing(transport(config, tmp_path)) as http:
        http.last_calls['publisher'] = time.monotonic() - .5
        assert not http.may_prefetch_short_local_pacing('publisher', .01)
        assert http.may_prefetch_short_local_pacing('publisher', .6)
        http.provider_not_before['publisher'] = time.time() + 10
        assert not http.may_prefetch_short_local_pacing('publisher', .6)
        http.provider_not_before.clear()
        http.pacing_not_before['publisher'] = time.monotonic() + 10
        assert not http.may_prefetch_short_local_pacing('publisher', .6)
        assert not http.requests_by_provider


def test_short_paced_backlog_keeps_one_receipt_batch(tmp_path, monkeypatch):
    """Yielding a worker must not turn each paced response into a disk batch."""
    config = owned_config()
    config['providers'] = config['providers'][:1]
    config['providers'][0].update(interval_seconds=.2, parallelism=1)
    config.update(workers=1, batch_articles=100, batch_flush_seconds=30,
                  response_deadline_seconds=3)
    urls = [f'https://publisher.test/history/{index}' for index in range(5)]
    session = FakeSession({url: FakeResponse(article_html()) for url in urls})
    starts, original_get = [], session.get

    def observe_get(url, **kwargs):
        starts.append(time.monotonic())
        return original_get(url, **kwargs)

    session.get = observe_get
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    with closing(NewsQueue(tmp_path, registry)) as queue, closing(NewsHTTP(
            registry, config, tmp_path, session_factory=lambda: session,
            limiter_factory=FakeLimiter, address_check=lambda url: None)) as http:
        http.robots_parsers['https://publisher.test'] = (
            time.time(), Protego.parse('User-agent: *\nAllow: /'))
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])
        sizes = []
        original_accept = queue.accept
        original_next_wake, wake_queries = queue.next_wake_delay, []

        def observe_next_wake(*args, **kwargs):
            wake_queries.append(1)
            return original_next_wake(*args, **kwargs)

        monkeypatch.setattr(queue, 'next_wake_delay', observe_next_wake)

        def stop_after_accept(records, **kwargs):
            result = original_accept(records, **kwargs)
            sizes.append(len(records))
            if sum(sizes) == len(urls):
                http.stop.set()
            return result

        monkeypatch.setattr(queue, 'accept', stop_after_accept)
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=5)
        assert sizes == [5]
        assert len(session.calls) == 5
        assert all(later - earlier >= .195 for earlier, later in zip(starts, starts[1:]))
        assert not wake_queries  # No competing publisher means no readiness scans per request.
        assert verify_bodies(tmp_path)['errors'] == []


def test_prefetch_never_takes_the_last_worker_from_a_ready_provider(tmp_path, monkeypatch):
    config = owned_config()
    config.update(workers=1, batch_articles=1, response_deadline_seconds=3)
    config['providers'][0].update(interval_seconds=1, parallelism=1)
    config['providers'][1].update(interval_seconds=.001, parallelism=1)
    other_url = 'https://other.test/history'
    session = FakeSession({URL: FakeResponse(article_html()), other_url: FakeResponse(article_html())})
    base, base_wall, offset, starts = time.monotonic(), time.time(), [0.], []
    monkeypatch.setattr(time, 'monotonic', lambda: base + offset[0])
    monkeypatch.setattr(time, 'time', lambda: base_wall + offset[0])
    original_get = session.get

    def observe_get(url, **kwargs):
        starts.append(url)
        if url == other_url:
            offset[0] += 1  # The other publisher runs before this local gap expires.
        return original_get(url, **kwargs)

    session.get = observe_get
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    with closing(NewsQueue(tmp_path, registry)) as queue, closing(NewsHTTP(
            registry, config, tmp_path, session_factory=lambda: session,
            limiter_factory=FakeLimiter, address_check=lambda url: None)) as http:
        parser = Protego.parse('User-agent: *\nAllow: /')
        http.robots_parsers.update({origin: (time.time(), parser)
                                    for origin in ('https://publisher.test', 'https://other.test')})
        with queue.db:
            queue.add_articles([{'url': URL}, {'url': other_url}])
        original_accept, count = queue.accept, [0]

        def stop_after_accept(records, **kwargs):
            result = original_accept(records, **kwargs)
            count[0] += len(records)
            if count[0] == 2:
                http.stop.set()
            return result

        monkeypatch.setattr(queue, 'accept', stop_after_accept)
        http.last_calls['publisher'] = base - .995  # Only five ms remain.
        watchdog = threading.Timer(5, http.stop.set)
        watchdog.start()
        try:
            run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=4)
        finally:
            watchdog.cancel()
        assert starts == [other_url, URL]
        assert count == [2] and verify_bodies(tmp_path)['errors'] == []


def test_ready_paced_source_starts_while_another_response_is_in_flight(tmp_path, monkeypatch):
    config = owned_config()
    config.update(workers=2, batch_articles=1, response_deadline_seconds=3)
    config['providers'][0].update(interval_seconds=.3, parallelism=1)
    config['providers'][1].update(interval_seconds=.001, parallelism=1)
    other_url = 'https://other.test/history'
    session = FakeSession({URL: FakeResponse(article_html()), other_url: FakeResponse(article_html())})
    publisher_started, calls, other_saw_publisher = threading.Event(), [], []
    original_get = session.get

    def observe_get(url, **kwargs):
        calls.append((url, time.monotonic()))
        if url == URL:
            publisher_started.set()
        else:
            other_saw_publisher.append(publisher_started.wait(.8))
        return original_get(url, **kwargs)

    session.get = observe_get
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    with closing(NewsQueue(tmp_path, registry)) as queue, closing(NewsHTTP(
            registry, config, tmp_path, session_factory=lambda: session,
            limiter_factory=FakeLimiter, address_check=lambda url: None)) as http:
        parser = Protego.parse('User-agent: *\nAllow: /')
        http.robots_parsers.update({origin: (time.time(), parser)
                                    for origin in ('https://publisher.test', 'https://other.test')})
        with queue.db:
            queue.add_articles([{'url': URL}, {'url': other_url}])
        original_accept, count = queue.accept, [0]

        def stop_after_accept(records, **kwargs):
            result = original_accept(records, **kwargs)
            count[0] += len(records)
            if count[0] == 2:
                http.stop.set()
            return result

        monkeypatch.setattr(queue, 'accept', stop_after_accept)
        previous_send = time.monotonic()
        http.last_calls['publisher'] = previous_send
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=4)
        assert [url for url, _ in calls] == [other_url, URL]
        assert calls[1][1] - previous_send >= .29
        assert other_saw_publisher == [True]
        assert count == [2] and verify_bodies(tmp_path)['errors'] == []


@pytest.mark.parametrize('wake_scan_fault', [False, True])
def test_future_dated_source_starts_while_another_response_is_in_flight(tmp_path, monkeypatch, wake_scan_fault):
    """A future article deadline must wake a free worker during a slow response."""
    config = owned_config()
    config.update(workers=2, batch_articles=1, response_deadline_seconds=3)
    for provider in config['providers']:
        provider.update(interval_seconds=.001, parallelism=1)
    other_url = 'https://other.test/history'
    session = FakeSession({URL: FakeResponse(article_html()), other_url: FakeResponse(article_html())})
    other_started, calls, slow_saw_other = threading.Event(), [], []
    original_get = session.get

    def observe_get(url, **kwargs):
        calls.append((url, time.monotonic()))
        if url == URL:
            slow_saw_other.append(other_started.wait(.9))
        else:
            other_started.set()
        return original_get(url, **kwargs)

    session.get = observe_get
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    with closing(NewsQueue(tmp_path, registry)) as queue, closing(NewsHTTP(
            registry, config, tmp_path, session_factory=lambda: session,
            limiter_factory=FakeLimiter, address_check=lambda url: None)) as http:
        parser = Protego.parse('User-agent: *\nAllow: /')
        http.robots_parsers.update({origin: (time.time(), parser)
                                    for origin in ('https://publisher.test', 'https://other.test')})
        with queue.db:
            queue.add_articles([{'url': URL}, {'url': other_url}])
            queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + 60, other_url))
        original_claim, due_monotonic = queue.claim, []

        def set_other_deadline_after_slow_claim(provider, *args, **kwargs):
            row = original_claim(provider, *args, **kwargs)
            if provider == 'publisher' and row and not due_monotonic:
                due_monotonic.append(time.monotonic() + .25)
                with queue.db:
                    queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + .25, other_url))
            return row

        monkeypatch.setattr(queue, 'claim', set_other_deadline_after_slow_claim)
        original_wake, wake_scans = queue.next_wake_delay, []

        def observe_wake(*args, **kwargs):
            if kwargs.get('dispatch_after') is not None:
                wake_scans.append(time.monotonic())
                if wake_scan_fault and len(wake_scans) == 1:
                    raise sqlite3.OperationalError('synthetic transient wake scan failure')
            return original_wake(*args, **kwargs)

        monkeypatch.setattr(queue, 'next_wake_delay', observe_wake)
        original_accept, count = queue.accept, [0]

        def stop_after_accept(records, **kwargs):
            result = original_accept(records, **kwargs)
            count[0] += len(records)
            if count[0] == 2:
                http.stop.set()
            return result

        monkeypatch.setattr(queue, 'accept', stop_after_accept)
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=4)
        assert [url for url, _ in calls] == [URL, other_url]
        assert calls[1][1] >= due_monotonic[0] - .01
        if not wake_scan_fault:
            assert slow_saw_other == [True]
        else:
            assert queue.metrics.providers['local_repairs']['counts']['dispatch_wake_scan_deferred'] == 1
        assert 1 <= len(wake_scans) < 20
        assert count == [2] and verify_bodies(tmp_path)['errors'] == []


def test_interrupting_short_pacing_commits_buffer_and_preserves_unsent_work(tmp_path, monkeypatch):
    config = owned_config()
    config['providers'][0].update(interval_seconds=1, parallelism=1)
    config.update(workers=1, batch_articles=100, batch_flush_seconds=30,
                  response_deadline_seconds=3)
    urls = [f'https://publisher.test/history/{index}' for index in range(2)]
    session = FakeSession({url: FakeResponse(article_html()) for url in urls})
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    with closing(NewsQueue(tmp_path, registry)) as queue, closing(NewsHTTP(
            registry, config, tmp_path, session_factory=lambda: session,
            limiter_factory=FakeLimiter, address_check=lambda url: None)) as http:
        http.robots_parsers['https://publisher.test'] = (
            time.time(), Protego.parse('User-agent: *\nAllow: /'))
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])

        class StopWake:
            calls = 0

            def wait(self, seconds):
                assert 0 < seconds <= 1
                self.calls += 1
                http.stop.set()
                return True

        wake = StopWake()
        run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                  continuous_budgets=True, max_run_seconds=3, idle_wake=wake)
        rows = [dict(row) for row in queue.db.execute('SELECT state,attempts,body_sha256 FROM articles')]
        assert wake.calls == 1 and len(session.calls) == 1
        assert sum(row['state'] == 'complete' and bool(row['body_sha256']) for row in rows) == 1
        assert sum(row['state'] == 'pending' and row['attempts'] == 0 for row in rows) == 1
        assert verify_bodies(tmp_path)['errors'] == []


@pytest.mark.parametrize('enabled_lane', ['article', 'discovery'])
def test_disabled_lane_cannot_make_resident_idle_owner_spin(tmp_path, monkeypatch, enabled_lane):
    config = owned_config()
    config['providers'] = config['providers'][:1]
    config.update(workers=1, batch_articles=1, response_deadline_seconds=3)
    feed_url = 'https://publisher.test/feed'
    session = FakeSession({URL: FakeResponse(article_html()),
                           feed_url: FakeResponse(b'<rss><channel></channel></rss>')})
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    with closing(NewsQueue(tmp_path, registry)) as queue, closing(NewsHTTP(
            registry, config, tmp_path, session_factory=lambda: session,
            limiter_factory=FakeLimiter, address_check=lambda url: None)) as http:
        http.robots_parsers['https://publisher.test'] = (time.time(), Protego.parse('User-agent: *\nAllow: /'))
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'feed','publisher',0)",
                             (feed_url,))
        original_cycle, cycles = main.run_cycle, []
        original_accept, original_discovery = queue.accept, main.accept_discovery

        def guarded_cycle(*args, **kwargs):
            if not cycles:
                with queue.db:
                    if enabled_lane == 'article':
                        queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + .35, URL))
                    else:
                        queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (time.time() + .35, feed_url))
            cycles.append(1)
            if len(cycles) > 6:
                http.stop.set()  # A broken idle wake must terminate the test.
            return original_cycle(*args, **kwargs)

        def accept(records, **kwargs):
            result = original_accept(records, **kwargs)
            http.stop.set()
            return result

        def accept_discovery(*args, **kwargs):
            result = original_discovery(*args, **kwargs)
            http.stop.set()
            return result

        monkeypatch.setattr(main, 'run_cycle', guarded_cycle)
        monkeypatch.setattr(queue, 'accept', accept)
        monkeypatch.setattr(main, 'accept_discovery', accept_discovery)
        run_service(queue, http, config, max_articles=int(enabled_lane == 'article'),
                    max_discovery_requests=int(enabled_lane == 'discovery'), duration_seconds=4)
        assert len(cycles) <= 3
        assert [call[0] for call in session.calls] == [URL if enabled_lane == 'article' else feed_url]
        if enabled_lane == 'article':
            assert verify_bodies(tmp_path)['errors'] == []
        else:
            assert queue.db.execute('SELECT last_checked_at_utc FROM discovery WHERE url=?', (feed_url,)).fetchone()[0]


def test_long_local_wait_keeps_the_owner_status_heartbeat(tmp_path, monkeypatch):
    config = owned_config()
    config['providers'][0].update(interval_seconds=20, parallelism=1)
    config.update(workers=1, batch_articles=100, batch_flush_seconds=30,
                  response_deadline_seconds=30)
    urls = [f'https://publisher.test/history/{index}' for index in range(2)]
    session = FakeSession({url: FakeResponse(article_html()) for url in urls})
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    real_monotonic, offset = time.monotonic, [0.]
    monkeypatch.setattr(time, 'monotonic', lambda: real_monotonic() + offset[0])
    with closing(NewsQueue(tmp_path, registry)) as queue, closing(NewsHTTP(
            registry, config, tmp_path, session_factory=lambda: session,
            limiter_factory=FakeLimiter, address_check=lambda url: None)) as http:
        http.robots_parsers['https://publisher.test'] = (
            time.time(), Protego.parse('User-agent: *\nAllow: /'))
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])
        observed, original_status = [], queue.status

        def observe_status(**kwargs):
            observed.append(time.monotonic())
            result = original_status(**kwargs)
            if len(observed) == 2:
                # Simulate a slow dashboard write that crosses the send time.
                offset[0] += 6
            return result

        monkeypatch.setattr(queue, 'status', observe_status)

        class AdvanceTime:
            def wait(self, seconds):
                assert 0 < seconds <= 1
                assert offset[0] < 20  # Never wait again after the send is due.
                offset[0] += seconds
                return False

        run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                  continuous_budgets=True, max_run_seconds=25, idle_wake=AdvanceTime())
        assert len(session.calls) == 2
        assert any(13 <= stamp - observed[0] < 19 for stamp in observed[1:])
        assert verify_bodies(tmp_path)['errors'] == []


def test_client_only_pacing_does_not_reteach_capacity_a_source_refusal(config, tmp_path):
    from test_news_capacity import controller_fixture
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        before = controller.states['publisher']['interval_seconds']
        http.pacing_not_before['publisher'] = time.monotonic() + 120
        for key in ('frontier_attempts_failed', 'frontier_source_gate_deferrals', 'frontier_deferred_provider_pacing_cooldown'):
            http.metrics.count('publisher', key)
        clock.value += 10
        controller.tick()
        assert controller.states['publisher']['interval_seconds'] == before
        assert not http.provider_not_before and not http.rate_refusal_intervals
        assert controller.states['publisher'].get('reason') not in {'source_circuit', 'http_access_refusal', 'http_429'}
    finally:
        http.close()
