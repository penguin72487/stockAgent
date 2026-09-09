from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import threading
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from services.discord_bot import bot as discord_bot
from stockagent.live import quote_provider, signal_engine


def test_overlapping_commands_share_work_cancel_independently_and_next_call_is_fresh(monkeypatch):
    calls = []
    release = asyncio.Event()

    async def generate(**kwargs):
        calls.append(kwargs)
        await release.wait()
        return SimpleNamespace(summary={"signal_id": str(len(calls))}, message="ready", output_dir="proof")

    monkeypatch.setattr(discord_bot, "_run_market_signal", generate)
    monkeypatch.setattr(discord_bot, "_intraday_waits_for_opening", lambda: False)
    monkeypatch.setattr(discord_bot, "_enrich_signal_performance_for_discord", lambda _cfg, result, **_kw: result)
    monkeypatch.setattr(discord_bot.bot, "_intraday_signal_inflight", {})
    cfg = SimpleNamespace(market="tw_day_trade_test")
    status = SimpleNamespace(data=SimpleNamespace(fresh=True))
    kwargs = dict(price_source="tw", top_n=20, min_abs_delta=0.001,
                  include_unconstrained_raw_scores=False, debug=False)

    async def exercise():
        first = asyncio.create_task(discord_bot._intraday_signal_now_result(cfg, status, **kwargs))
        second = asyncio.create_task(discord_bot._intraday_signal_now_result(cfg, status, **kwargs))
        while not calls:
            await asyncio.sleep(0.001)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        shared = await second
        fresh = await discord_bot._intraday_signal_now_result(cfg, status, **kwargs)
        return shared, fresh

    shared, fresh = asyncio.run(exercise())
    assert shared.summary["signal_now_singleflight_joined"] is True
    assert shared.summary["signal_id"] == "1"
    assert fresh.summary["signal_id"] == "2"
    assert len(calls) == 2
    assert calls[0]["publish_latest"] is False
    assert calls[0]["prepared_status"] is status
    assert calls[0]["_prefetch_prices"] is True
    assert not discord_bot.bot._intraday_signal_inflight


def test_quote_io_can_overlap_but_model_runtime_stays_serialized(monkeypatch):
    quote_barrier = threading.Barrier(2)
    running = 0
    peak = 0
    mutex = threading.Lock()

    def prefetch(**kwargs):
        # This barrier cannot complete if either worker holds the model lock
        # during network I/O. Use events, not latency thresholds, for proof.
        quote_barrier.wait(timeout=2)
        return kwargs["market"]

    def infer(**kwargs):
        nonlocal running, peak
        with mutex:
            running += 1
            peak = max(peak, running)
        time.sleep(0.01)
        with mutex:
            running -= 1
        return kwargs["_prefetched_quote"]

    monkeypatch.setenv("STOCKAGENT_BOT_PROGRESS", "0")
    monkeypatch.setattr(discord_bot, "_signal_kwargs", lambda **kwargs: kwargs)
    monkeypatch.setattr(discord_bot, "prefetch_live_signal_prices", prefetch)
    monkeypatch.setattr(discord_bot, "generate_live_signal", infer)
    with ThreadPoolExecutor(max_workers=2) as executor:
        jobs = [executor.submit(discord_bot._run_market_signal_sync, market=name, _prefetch_prices=True)
                for name in ("a", "b")]
        assert [job.result(timeout=3) for job in jobs] == ["a", "b"]
    assert peak == 1


def test_alignment_cache_requires_same_panel_and_checkpoint_and_skips_legacy(monkeypatch):
    monkeypatch.setattr(signal_engine, "_LIVE_ALIGNED_PANEL_CACHE", signal_engine.OrderedDict())
    calls = []
    monkeypatch.setattr(signal_engine, "checkpoint_manifest_symbols", lambda payload: payload.get("symbols"))

    def align(panel, *_args, **_kwargs):
        calls.append(panel)
        return SimpleNamespace(source=panel)

    monkeypatch.setattr(signal_engine, "align_panel_to_checkpoint_universe", align)
    panel = SimpleNamespace(features=np.zeros((2, 2, 1)))
    kwargs = dict(checkpoint_key="v1", checkpoint_payload={"symbols": ["B", "A"]},
                  state_dict={}, fold_dir=signal_engine.Path("unused"), context="test")
    first, hit = signal_engine._cached_aligned_panel(panel, **kwargs)
    assert not hit
    repeated, hit = signal_engine._cached_aligned_panel(panel, **kwargs)
    assert hit and repeated is first
    signal_engine._cached_aligned_panel(panel, **{**kwargs, "checkpoint_key": "v2"})
    signal_engine._cached_aligned_panel(SimpleNamespace(features=panel.features.copy()), **kwargs)
    for _ in range(2):
        signal_engine._cached_aligned_panel(panel, **{**kwargs, "checkpoint_payload": {}})
    assert len(calls) == 5


@pytest.mark.parametrize("change", [
    {"symbols": ["B", "A"]}, {"request_mask": np.array([True, False])},
    {"fallback_prices": np.array([101., 200.])}, {"source": "shioaji"},
    {"require_official_tw_session_open": True},
])
def test_prefetch_identity_invalidates_changed_price_contract(change):
    request = dict(symbols=["A", "B"], request_mask=np.array([True, True]),
                   fallback_prices=np.array([100., 200.]), source="tw",
                   require_official_tw_session_open=False)
    assert signal_engine._live_quote_request_key(request) != signal_engine._live_quote_request_key({**request, **change})


def test_mis_coverage_quorum_reuses_connections_but_fetches_new_prices(monkeypatch, tmp_path):
    missing = {"1009"}
    calls = []
    sessions = []
    price = 101.0
    symbols = [str(1000 + index) for index in range(10)]

    class Response:
        def __init__(self, rows):
            self.rows = rows

        def raise_for_status(self):
            pass

        def json(self):
            return {"msgArray": self.rows}

    class Session:
        def __init__(self):
            self.headers = {}
            sessions.append(self)

        def get(self, _url, *, params, **_kwargs):
            code = params["ex_ch"].split("_", 1)[1].split(".", 1)[0]
            calls.append(code)
            rows = [] if code in missing else [{"c": code, "z": str(price), "o": "100.0"}]
            return Response(rows)

    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "warm_tw_mis_quote_client", lambda: {})
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda _: {s: f"{s}.TW" for s in symbols})
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_COOKIES", {})
    monkeypatch.setattr(quote_provider, "_TW_MIS_HTTP_LOCAL", threading.local())
    kwargs = dict(parquet_root=tmp_path, chunk_size=1, max_parallel_requests=1,
                  empty_chunk_retry_attempts=1, empty_chunk_retry_delay_seconds=0,
                  minimum_response_coverage=0.9)
    with ThreadPoolExecutor(max_workers=1) as pool:
        monkeypatch.setattr(quote_provider, "_TW_MIS_HTTP_POOL", pool)
        first = quote_provider.fetch_tw_mis_last_prices(symbols, np.ones(10), **kwargs)
        assert len(calls) == 10 and first.available_count == 9
        assert first.transport_timing["mis_retry_rounds"] == 0
        price = 102.0
        second = quote_provider.fetch_tw_mis_last_prices(symbols, np.ones(10), **kwargs)
        assert second.prices[0] == 102.0 and first.prices[0] == 101.0
        assert len(sessions) == 1
        missing.add("1008")
        third = quote_provider.fetch_tw_mis_last_prices(symbols, np.ones(10), **kwargs)
        assert third.available_count == 8
        assert third.transport_timing["mis_retry_rounds"] == 1
        assert len(calls) == 32


def test_opening_priority_releases_when_scheduled_batch_is_published(monkeypatch):
    cfg = SimpleNamespace(day_trade_simulation_enabled=True)
    state = "retry"
    monkeypatch.setattr(discord_bot, "_scheduled_markets", lambda: ["mode"])
    monkeypatch.setattr(discord_bot, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(discord_bot, "_scheduled_market_session_day", lambda *_: (True, "session"))
    monkeypatch.setattr(discord_bot, "_day_trade_schedule_state", lambda *_: state)
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_started_monotonic", None)
    at_open = datetime(2026, 9, 9, 9, 0, 0, tzinfo=ZoneInfo("Asia/Taipei"))
    assert discord_bot._intraday_waits_for_opening(at_open)
    state = "pending_confirmation"
    assert not discord_bot._intraday_waits_for_opening(at_open)
    state = "retry"
    assert not discord_bot._intraday_waits_for_opening(at_open.replace(second=15))


def test_different_model_universes_share_pending_quotes_and_keep_own_missing_fallback(monkeypatch, tmp_path):
    started = threading.Event()
    release = threading.Event()
    calls = []

    def fetch(symbols, fallback_prices, **_kwargs):
        calls.append(list(symbols))
        if symbols == ["A", "B"]:
            started.set()
            assert release.wait(timeout=2)
        available = np.array([symbol != "B" for symbol in symbols])
        return quote_provider.PriceSnapshot(
            prices=np.where(available, 105.0, fallback_prices), source="twse_tpex:mis",
            available_count=int(available.sum()), requested_count=len(symbols), available_mask=available,
            timestamps_ms=np.ones(len(symbols), dtype=np.int64),
        )

    monkeypatch.setattr(quote_provider, "_fetch_tw_mis_last_prices", fetch)
    monkeypatch.setattr(quote_provider, "_TW_MIS_INFLIGHT", {})
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(quote_provider.fetch_tw_mis_last_prices, ["A", "B"], np.array([1., 2.]), parquet_root=tmp_path)
        assert started.wait(timeout=2)
        second = pool.submit(quote_provider.fetch_tw_mis_last_prices, ["C", "B", "A"], np.array([3., 4., 5.]), parquet_root=tmp_path)
        deadline = time.monotonic() + 2
        while ["C"] not in calls and time.monotonic() < deadline:
            time.sleep(0.001)
        release.set()
        a, b = first.result(timeout=2), second.result(timeout=2)
    assert calls == [["A", "B"], ["C"]]
    np.testing.assert_equal(a.prices, [105., 2.])
    np.testing.assert_equal(b.prices, [105., 4., 105.])
    assert b.available_mask.tolist() == [True, False, True]
    assert b.transport_timing["mis_inflight_shared_symbols"] == 2
    assert not quote_provider._TW_MIS_INFLIGHT
