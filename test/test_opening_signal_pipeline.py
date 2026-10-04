from __future__ import annotations

import asyncio
import json
from datetime import datetime
import threading
from types import SimpleNamespace

import numpy as np
import pytest
import yaml

from stockagent import config as config_module
from services.discord_bot import bot as discord_bot
from stockagent.live import signal_engine
from stockagent.live.quote_provider import PriceSnapshot


@pytest.mark.parametrize("text", [
    "flag: true\nlist: [1, 2.3, null, 'false']\ndate: 2026-09-23\n",
    "unicode: 開盤\na: &A {x: 1}\nb: *A\n",
])
def test_fast_safe_config_parser_retains_values(text):
    assert yaml.load(text, Loader=config_module._UniqueKeySafeLoader) == yaml.safe_load(text)


@pytest.mark.parametrize("text,error", [
    ("x: 1\nx: 2", ValueError),
    ("!!python/object/apply:os.system ['echo unsafe']", yaml.constructor.ConstructorError),
    ("base: &B {x: 1}\nchild: {<<: *B, x: 2}", ValueError),
])
def test_fast_safe_config_parser_preserves_rejection(text, error):
    with pytest.raises(error):
        yaml.load(text, Loader=config_module._UniqueKeySafeLoader)


@pytest.mark.parametrize("value", [None, "bad", "1.25", np.float32(3.5), 0, -3., float("nan"), float("inf"), -float("inf")])
def test_scalar_finite_contract(value):
    try:
        converted = float(value)
        expected = converted if np.isfinite(converted) else None
    except Exception:
        expected = None
    assert signal_engine._finite_float_or_none(value) == expected


def test_opening_cache_hit_is_read_only(monkeypatch, tmp_path):
    snapshot = PriceSnapshot(
        prices=np.array([100.]), open_prices=np.array([99.]),
        source="shioaji:stock_snapshot+shared_opening_snapshot+cache_hit",
        available_mask=np.array([True]), available_count=1, requested_count=1,
    )
    monkeypatch.setattr(signal_engine, "fetch_tw_mis_opening_snapshot", lambda *a, **k: snapshot)
    for name in ("seed_tw_opening_snapshot_cache", "fetch_shared_day_trade_stock_snapshots",
                 "fetch_shioaji_stock_snapshots"):
        monkeypatch.setattr(signal_engine, name, lambda *a, **k: pytest.fail("cache hit must not seed/write/fetch"))
    result = signal_engine._price_snapshot(
        symbols=["2330"], fallback_prices=np.array([98.]), source="tw",
        parquet_root=tmp_path, require_official_tw_session_open=True,
        prices_csv=None, yahoo_chunk_size=80,
    )
    assert result is snapshot


def test_deferred_completion_is_idempotent_and_failure_is_retryable():
    attempts = []

    def complete():
        attempts.append(True)
        if len(attempts) == 1:
            raise OSError("temporary disk failure")

    result = signal_engine.LiveSignalResult({}, [], [], [], "", _complete_artifacts=complete)
    with pytest.raises(OSError):
        signal_engine.finalize_live_signal_artifacts(result)
    assert result._complete_artifacts is complete
    signal_engine.finalize_live_signal_artifacts(result)
    signal_engine.finalize_live_signal_artifacts(result)
    assert len(attempts) == 2
    assert result._complete_artifacts is None


def test_report_completion_cannot_overwrite_newer_or_deleted_pointer(tmp_path):
    path = tmp_path / "latest_signal.json"
    old = {"signal_id": "old", "artifact_complete": False}
    newer = {"signal_id": "new", "artifact_complete": False}
    assert signal_engine._complete_latest_signal_pointer(path, old) is False
    path.write_text(json.dumps(newer))
    assert signal_engine._complete_latest_signal_pointer(path, old) is False
    assert json.loads(path.read_text()) == newer
    assert signal_engine._complete_latest_signal_pointer(path, newer) is True
    assert json.loads(path.read_text())["artifact_complete"] is True


@pytest.mark.parametrize("failed_market", [None, "b"])
def test_scheduler_prepares_together_publishes_before_reports_and_isolates_failure(monkeypatch, failed_market):
    events, receipts = [], []
    barrier = threading.Barrier(3, timeout=5)
    configs = {m: SimpleNamespace(market=m, timezone="Asia/Taipei",
                                 day_trade_simulation_enabled=True,
                                 overnight_simulation_enabled=False) for m in "abc"}
    observed = datetime.fromisoformat("2026-09-23T09:00:00.010000+08:00")

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return observed

    monkeypatch.setattr(discord_bot, "datetime", Clock)
    monkeypatch.setattr(discord_bot, "_scheduled_markets", lambda: list(configs))
    monkeypatch.setattr(discord_bot, "_resolve_market", configs.__getitem__)
    monkeypatch.setattr(discord_bot, "_preopen_market_symbol_count", lambda cfg: 10)
    monkeypatch.setattr(discord_bot, "_scheduled_market_session_day", lambda *a: (True, "test"))
    monkeypatch.setattr(discord_bot, "_scheduled_signal_key", lambda cfg, now: f"{now.date()}:{cfg.market}")
    monkeypatch.setattr(discord_bot, "_day_trade_schedule_state", lambda *a: "retry")
    monkeypatch.setattr(discord_bot, "_scheduled_signal_requires_preopen_catch_up", lambda *a: False)
    monkeypatch.setattr(discord_bot, "_preopen_market_final_armed_for_session", lambda *a: True)
    monkeypatch.setattr(discord_bot, "_opening_gate_at", lambda *a: observed.replace(microsecond=0))
    monkeypatch.setattr(discord_bot, "notify_systemd", lambda *a: None)
    monkeypatch.setattr(discord_bot, "_log_exception", lambda *a: None)
    monkeypatch.setattr(discord_bot, "_record_opening_signal_latency", receipts.append)
    for name in ("_last_scheduled_keys", "_scheduled_error_notice_keys"):
        monkeypatch.setattr(discord_bot.bot, name, set())
    for name in ("_scheduled_retry_after", "_scheduled_failure_counts"):
        monkeypatch.setattr(discord_bot.bot, name, {})

    def prepare(cfg, **kwargs):
        events.append(("prepare", cfg.market))
        barrier.wait()
        if cfg.market == failed_market:
            raise RuntimeError("source unavailable")
        return "tw", SimpleNamespace(), False

    async def generate(**kwargs):
        market = kwargs["market"]
        assert kwargs["_defer_rich_artifacts"] is True
        events.append(("publish", market))
        return signal_engine.LiveSignalResult(
            {"signal_id": market, "market": market}, [], [], [], "ready",
            _complete_artifacts=lambda: events.append(("report", market)),
        )

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(discord_bot, "_prepare_realtime_signal_sync", prepare)
    monkeypatch.setattr(discord_bot, "_run_market_signal", generate)
    monkeypatch.setattr(discord_bot, "_scheduled_broadcast_channel", noop)
    monkeypatch.setattr(discord_bot, "_send_subscription_notifications", noop)
    monkeypatch.setattr(discord_bot, "_enrich_signal_performance_for_discord", lambda cfg, result, **kw: result)
    monkeypatch.setattr(discord_bot, "_signal_sanity_issues", lambda *a: [])
    asyncio.run(discord_bot.scheduled_signal.coro())
    published = [m for phase, m in events if phase == "publish"]
    assert published == [m for m in "abc" if m != failed_market]
    assert max(i for i, e in enumerate(events) if e[0] == "publish") < min(
        i for i, e in enumerate(events) if e[0] == "report")
    assert len(receipts) == 3
    assert all("mode_dispatch_queue_ms" in r["stages"] for r in receipts)
    assert discord_bot.bot._opening_attempt_started_monotonic is None
    if failed_market:
        assert f"2026-09-23:{failed_market}" in discord_bot.bot._scheduled_error_notice_keys
