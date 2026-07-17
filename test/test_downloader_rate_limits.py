import threading
import time

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
    assert resolve_request_interval("alpaca_market_data_basic", None) == alpaca.interval_seconds


def test_requested_interval_is_clamped_to_configured_limit() -> None:
    okx = provider_rate_limit("okx_history_candles")
    assert resolve_request_interval("okx_history_candles", 0.001) == okx.interval_seconds
    assert resolve_request_interval("okx_history_candles", 1.0) == 1.0


def test_unpublished_and_unknown_providers_default_to_ten_rps() -> None:
    providers = (
        "frankfurter_public",
        "tw_public",
        "yahoo_finance",
        "future_provider_without_profile",
    )

    for provider in providers:
        profile = provider_rate_limit(provider)
        assert profile.requests_per_second == DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND
        assert resolve_request_interval(provider, None) == 0.1


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


def test_defer_reblocks_threads_that_are_already_sleeping(tmp_path, monkeypatch) -> None:
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
