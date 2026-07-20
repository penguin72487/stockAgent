import threading
import time

import pytest

import downloader.common as downloader_common
from downloader.common import (
    DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
    SharedRateLimiter,
    provider_rate_limit,
    resolve_request_interval,
)


def test_documented_provider_defaults_use_exact_average_limit() -> None:
    okx = provider_rate_limit("okx_history_candles")
    bybit = provider_rate_limit("bybit_public_rest")
    alpaca = provider_rate_limit("alpaca_market_data_basic")

    assert round(okx.requests_per_second, 2) == 10.00
    assert round(bybit.requests_per_second, 2) == 120.00
    assert alpaca.requests_per_second == 200 / 60
    assert resolve_request_interval("okx_history_candles", None) == okx.interval_seconds
    assert resolve_request_interval("bybit_public_rest", None) == bybit.interval_seconds
    assert (
        resolve_request_interval("alpaca_market_data_basic", None)
        == alpaca.interval_seconds
    )


def test_requested_interval_is_clamped_to_configured_limit() -> None:
    okx = provider_rate_limit("okx_history_candles")
    assert (
        resolve_request_interval("okx_history_candles", 0.001) == okx.interval_seconds
    )
    assert resolve_request_interval("okx_history_candles", 1.0) == 1.0


def test_unpublished_and_unknown_providers_default_to_eight_rps() -> None:
    providers = (
        "frankfurter_public",
        "tw_public",
        "yahoo_finance",
        "future_provider_without_profile",
    )

    for provider in providers:
        profile = provider_rate_limit(provider)
        assert profile.requests_per_second == DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND
        assert resolve_request_interval(provider, None) == 0.125


def test_named_limiters_share_host_global_schedule_and_cooldown(tmp_path) -> None:
    first = SharedRateLimiter(0.03, name="shared-test", state_dir=tmp_path)
    second = SharedRateLimiter(0.03, name="shared-test", state_dir=tmp_path)

    first.wait()
    started = time.monotonic()
    second.wait()
    assert time.monotonic() - started >= 0.02

    first.defer(0.04)
    started = time.monotonic()
    second.wait()
    assert time.monotonic() - started >= 0.03


def test_shared_rate_limiter_reports_granted_slots(tmp_path) -> None:
    claims: list[float] = []
    limiter = SharedRateLimiter(
        0.0,
        name="observed-test",
        state_dir=tmp_path,
        on_claim=lambda: claims.append(time.monotonic()),
    )

    limiter.wait()
    limiter.wait()

    assert len(claims) == 2


def test_same_limiter_waiters_use_one_local_schedule_leader(
    tmp_path, monkeypatch
) -> None:
    limiter = SharedRateLimiter(
        0.02,
        name="single-local-leader-test",
        state_dir=tmp_path,
    )
    limiter.wait()
    real_sleep = time.sleep
    sleeping = 0
    max_sleeping = 0
    sleeping_lock = threading.Lock()

    def observed_sleep(seconds: float) -> None:
        nonlocal sleeping, max_sleeping
        with sleeping_lock:
            sleeping += 1
            max_sleeping = max(max_sleeping, sleeping)
        try:
            real_sleep(seconds)
        finally:
            with sleeping_lock:
                sleeping -= 1

    monkeypatch.setattr(downloader_common.time, "sleep", observed_sleep)
    threads = [threading.Thread(target=limiter.wait) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2.0)
        assert not thread.is_alive()

    assert max_sleeping == 1


def test_same_limiter_grants_all_slots_from_stable_dispatch_thread(tmp_path) -> None:
    claim_threads: list[int] = []
    worker_threads: set[int] = set()
    worker_lock = threading.Lock()
    limiter = SharedRateLimiter(
        0.001,
        name="stable-dispatch-test",
        state_dir=tmp_path,
        on_claim=lambda: claim_threads.append(threading.get_ident()),
    )

    def worker() -> None:
        with worker_lock:
            worker_threads.add(threading.get_ident())
        limiter.wait()

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2.0)
        assert not thread.is_alive()

    deadline = time.monotonic() + 2.0
    while len(claim_threads) < len(threads) and time.monotonic() < deadline:
        time.sleep(0.005)
    assert len(claim_threads) == len(threads)
    assert len(set(claim_threads)) == 1
    assert claim_threads[0] not in worker_threads


def test_slow_claim_observer_cannot_throttle_dispatch_cadence(tmp_path) -> None:
    observed: list[float] = []
    observer_lock = threading.Lock()

    def slow_observer() -> None:
        time.sleep(0.04)
        with observer_lock:
            observed.append(time.monotonic())

    limiter = SharedRateLimiter(
        0.01,
        name="nonblocking-observer-test",
        state_dir=tmp_path,
        on_claim=slow_observer,
    )
    started = time.monotonic()
    threads = [threading.Thread(target=limiter.wait) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1.0)
        assert not thread.is_alive()
    dispatch_elapsed = time.monotonic() - started

    # A synchronous observer would require at least 6 * 40 ms. The dispatcher
    # must remain governed only by the 10 ms request cadence.
    assert dispatch_elapsed < 0.16
    grant_activity = limiter.grant_activity()
    assert grant_activity["grants_total"] == len(threads)
    assert grant_activity["grants_last_60s"] == len(threads)
    # Grant-boundary telemetry is complete even while the intentionally slow
    # durable observer is still draining its diagnostic queue.
    assert grant_activity["pending_claim_observations"] > 0
    deadline = time.monotonic() + 1.0
    while len(observed) < len(threads) and time.monotonic() < deadline:
        time.sleep(0.005)
    assert len(observed) == len(threads)


def test_shared_rate_limiter_does_not_accumulate_sub_interval_wakeup_jitter(
    tmp_path, monkeypatch
) -> None:
    limiter = SharedRateLimiter(0.1, name="absolute-cadence-test", state_dir=tmp_path)
    clock = iter((100.0, 100.12, 100.35))
    monkeypatch.setattr(downloader_common.time, "monotonic", lambda: next(clock))

    assert limiter._claim_process_shared() == (True, 0.0)
    assert float(limiter._state_path.read_text().strip()) == pytest.approx(100.1)

    # A 20 ms scheduler delay is absorbed by the absolute cadence instead of
    # permanently turning 10 req/s into 8.33 req/s.
    assert limiter._claim_process_shared() == (True, 0.0)
    assert float(limiter._state_path.read_text().strip()) == pytest.approx(100.2)

    # A process that missed a complete interval resets from now and never
    # releases a burst of accumulated tickets.
    assert limiter._claim_process_shared() == (True, 0.0)
    assert float(limiter._state_path.read_text().strip()) == pytest.approx(100.45)


def test_defer_reblocks_threads_that_are_already_sleeping(
    tmp_path, monkeypatch
) -> None:
    interval = 0.05
    cooldown = 0.18
    worker_count = 3
    limiters = [
        SharedRateLimiter(interval, name="defer-race-test", state_dir=tmp_path)
        for _ in range(worker_count)
    ]

    # Occupy the current slot so every worker has to enter the wait/recheck path.
    limiters[0].wait()

    real_sleep = time.sleep
    sleeping_threads: set[int] = set()
    sleeping_lock = threading.Lock()
    all_sleeping = threading.Event()
    release_initial_sleeps = threading.Event()

    def observed_sleep(seconds: float) -> None:
        thread_id = threading.get_ident()
        first_sleep = False
        with sleeping_lock:
            if thread_id not in sleeping_threads:
                sleeping_threads.add(thread_id)
                first_sleep = True
                if len(sleeping_threads) == worker_count:
                    all_sleeping.set()
        if first_sleep:
            assert release_initial_sleeps.wait(timeout=1.0)
        real_sleep(seconds)

    monkeypatch.setattr(downloader_common.time, "sleep", observed_sleep)

    completed_at: list[float] = []
    completed_lock = threading.Lock()

    def worker(limiter: SharedRateLimiter) -> None:
        limiter.wait()
        with completed_lock:
            completed_at.append(time.monotonic())

    threads = [threading.Thread(target=worker, args=(limiter,)) for limiter in limiters]
    for thread in threads:
        thread.start()

    assert all_sleeping.wait(timeout=1.0)
    deferred_at = time.monotonic()
    limiters[0].defer(cooldown)
    release_initial_sleeps.set()

    for thread in threads:
        thread.join(timeout=2.0)
        assert not thread.is_alive()

    assert len(completed_at) == worker_count
    # Every not-yet-sent request must observe the later provider-wide cooldown.
    assert min(completed_at) - deferred_at >= cooldown - 0.02
