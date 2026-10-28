"""Long client pacing is scheduled work, not an occupied download worker."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime
import time

import pytest

from downloader.common import SharedRateLimiter
from downloader.news_fulltext import run_cycle
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
