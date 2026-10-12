from __future__ import annotations

import asyncio
from datetime import datetime
import fcntl
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from stockagent.live import signal_engine
from stockagent.live.tw_public_source_guard import (
    TWPublicSourceUpdatePending,
    live_tw_public_source_read_guard,
)


def _tree(tmp_path):
    root = tmp_path / "live" / "data_tw_public"
    stocks = root / "stocks"
    stocks.mkdir(parents=True)
    (root / "tw_corporate_action_reference.parquet").write_bytes(b"reference")
    lock = root.parent / ".locks" / "tw-public-refresh.lock"
    lock.parent.mkdir()
    lock.touch()
    return root, stocks, lock


def test_unmanaged_sources_do_not_create_lock_or_change_paths(tmp_path):
    stocks = tmp_path / "crypto"
    with live_tw_public_source_read_guard(stocks):
        pass
    assert not (tmp_path / ".locks").exists()


def test_exclusive_writer_defers_before_any_panel_or_cache_read(monkeypatch, tmp_path):
    root, stocks, lock = _tree(tmp_path)
    calls = []
    monkeypatch.setattr(signal_engine, "_build_panel_from_stable_source", lambda *a, **k: calls.append(True))
    config = SimpleNamespace(data=SimpleNamespace(parquet_root=str(stocks)))
    with lock.open("a+") as producer:
        fcntl.flock(producer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(TWPublicSourceUpdatePending) as caught:
            signal_engine._build_panel(config, live_tail=True)
    assert calls == []
    assert caught.value.source_root == root
    assert caught.value.details["producer_lock_held"] is True


def test_symlink_alias_cannot_read_past_same_writer(tmp_path):
    _root, stocks, lock = _tree(tmp_path)
    alias = tmp_path / "data_tw_public"
    alias.symlink_to(stocks.parent, target_is_directory=True)
    with lock.open("a+") as producer:
        fcntl.flock(producer, fcntl.LOCK_EX)
        with pytest.raises(TWPublicSourceUpdatePending):
            with live_tw_public_source_read_guard(alias / "stocks"):
                pytest.fail("source must not be read")


def test_parallel_readers_hold_one_coherent_generation_and_release_on_error(tmp_path):
    _root, stocks, lock = _tree(tmp_path)
    with lock.open("a+") as producer:
        with pytest.raises(ValueError, match="real source defect"):
            with live_tw_public_source_read_guard(stocks):
                with live_tw_public_source_read_guard(stocks):
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(producer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    raise ValueError("real source defect")
        fcntl.flock(producer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(producer, fcntl.LOCK_UN)


def test_producer_crash_releases_wait_and_does_not_mask_remaining_corruption(monkeypatch, tmp_path):
    _root, stocks, lock = _tree(tmp_path)
    script = "import fcntl,sys,time; f=open(sys.argv[1], 'a+'); fcntl.flock(f,fcntl.LOCK_EX); print('locked',flush=True); time.sleep(30)"
    process = subprocess.Popen([sys.executable, "-c", script, str(lock)], stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "locked"
        with pytest.raises(TWPublicSourceUpdatePending):
            with live_tw_public_source_read_guard(stocks):
                pytest.fail("producer still owns this source")
    finally:
        process.terminate()
        process.wait(timeout=5)
        process.stdout.close()
    def corrupt(*a, **k):
        raise ValueError("TW exact entitlement was built from another reference size")
    monkeypatch.setattr(signal_engine, "_build_panel_from_stable_source", corrupt)
    with pytest.raises(ValueError, match="another reference size"):
        signal_engine._build_panel(SimpleNamespace(data=SimpleNamespace(parquet_root=stocks)))


@pytest.mark.parametrize("phase", ["arm", "cache"])
def test_postclose_waits_for_owner_without_failure_traceback_and_resumes(monkeypatch, tmp_path, phase):
    from services.discord_bot import bot as module
    root, _stocks, lock = _tree(tmp_path)
    cfg = SimpleNamespace(market="test", timezone="Asia/Taipei")
    now = datetime.fromisoformat("2026-10-08T14:00:00+08:00")
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(module, "_postclose_fast_markets", lambda: ["test"])
    monkeypatch.setattr(module, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(module, "_postclose_fast_clock_key", lambda *a, **k: "key")
    monkeypatch.setattr(module, "_scheduled_market_session_day", lambda *a: (True, "fixture"))
    monkeypatch.setattr(module.bot, "_postclose_fast_retry_after", {})
    monkeypatch.setattr(module.bot, "_postclose_fast_completed_keys", set())
    monkeypatch.setattr(module.bot, "_postclose_fast_arm_keys", set())
    observed = []
    monkeypatch.setattr(module, "_write_postclose_fast_cache_status_best_effort", lambda _cfg, **k: observed.append(k))
    monkeypatch.setattr(module, "_log_exception", lambda *a: pytest.fail("a producer lease is not a source failure"))
    def pending(*a, **k):
        raise TWPublicSourceUpdatePending(root, lock)
    monkeypatch.setattr(module, "_ensure_signal_ready", pending)
    coroutine = module.postclose_fast_arm if phase == "arm" else module.postclose_fast_cache
    asyncio.run(coroutine.coro())
    assert observed[-1]["status"] == "waiting_source"
    assert observed[-1]["details"]["producer_lock_held"]
    module.bot._postclose_fast_retry_after.clear()
    monkeypatch.setattr(module, "_ensure_signal_ready", lambda *a, **k: object())
    if phase == "arm":
        monkeypatch.setattr(module, "_postclose_fast_arm_market_sync", lambda *a: SimpleNamespace(summary={"panel_date":"2026-10-08"}))
    else:
        monkeypatch.setattr(module, "_postclose_fast_signal_context", lambda *a, **k: None)
    asyncio.run(coroutine.coro())
    assert observed[-1]["status"] == "ready"


def test_postclose_status_separates_wait_and_real_failure_and_does_not_inherit_old_owner(monkeypatch, tmp_path):
    from services.discord_bot import bot as module
    cfg = SimpleNamespace(market="test", day_trade_simulation_enabled=True)
    path = tmp_path / "postclose.json"
    monkeypatch.setattr(module, "_postclose_fast_cache_status_path", lambda: path)
    monkeypatch.setattr(module, "_scheduled_markets", lambda: ["test"])
    monkeypatch.setattr(module, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(module, "_BOT_RUN_ID", "current-owner")
    path.write_text(json.dumps({"session_date":"2026-10-08","run_id":"old-owner","markets":{"test":{"cache":{"status":"ready"}}}}))
    loaded = module._load_postclose_fast_cache_status()
    assert loaded["status"] == "pending" and loaded["previous_run_id"] == "old-owner"
    assert loaded["markets"] == {}
    payload = module._write_postclose_fast_cache_status(cfg, session_date="2026-10-08",phase="arm",status="running")
    assert payload["run_id"] == "current-owner"
    assert payload["cache_ready_count"] == 0
    payload = module._write_postclose_fast_cache_status(cfg, session_date="2026-10-08",phase="cache",status="waiting_source")
    assert payload["status"] == "waiting_source" and payload["cache_waiting_source_count"] == 1
    payload = module._write_postclose_fast_cache_status(cfg, session_date="2026-10-08",phase="cache",status="failed",error=ValueError("actual corruption"))
    assert payload["status"] == "degraded" and payload["cache_failed_count"] == 1
    assert module._load_postclose_fast_cache_status()["status"] == "degraded"


@pytest.mark.parametrize("cached", [True, False])
@pytest.mark.parametrize("price_source", ["panel", "panel_close"])
def test_official_cache_after_fast_window_reconciles_status_without_redundant_inference(
    monkeypatch, tmp_path, cached, price_source,
):
    from services.discord_bot import bot as module
    cfg = SimpleNamespace(
        market="test", timezone="Asia/Taipei", day_trade_simulation_enabled=True,
        signal_kwargs=lambda **kwargs: kwargs,
    )
    status = SimpleNamespace(market_open=False, data=SimpleNamespace(
        fresh=True, expected_latest_date="2026-10-08", last_data_date="2026-10-08",
        panel_date="2026-10-08",
    ))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat("2026-10-08T15:30:00+08:00")
    weights = tmp_path / "weights.parquet"
    weights.touch()
    summary = {
        "panel_date": "2026-10-08", "signal_id": "official-close",
        "price_source": price_source, "execution_preview_only": True,
    }
    result = SimpleNamespace(summary=summary)
    observed, inference = [], []
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(module, "_effective_market_config", lambda cfg: cfg)
    monkeypatch.setattr(module, "_ensure_signal_ready", lambda _: status)
    monkeypatch.setattr(module, "_completed_session_receipt_ready", lambda _: True)
    monkeypatch.setattr(module, "_market_has_panel_close_signal_for_date", lambda *_: cached)
    monkeypatch.setattr(module, "_latest_market_signal", lambda _: (tmp_path / "summary.json", summary))
    monkeypatch.setattr(module, "_summary_artifact_path", lambda *_: weights)
    monkeypatch.setattr(module, "_summary_matches_market_deployment", lambda *_: (True, None))
    monkeypatch.setattr(module, "_market_notice", lambda _: None)
    monkeypatch.setattr(module, "_env_bool", lambda *_: False)
    monkeypatch.setattr(module, "generate_live_signal", lambda **_: inference.append(True) or result)
    monkeypatch.setattr(module, "_enrich_signal_performance_for_discord", lambda _, result, **__: result)
    monkeypatch.setattr(module, "_sync_latest_live_weights_to_market_artifact", lambda _: None)
    monkeypatch.setattr(module, "_write_postclose_fast_cache_status_best_effort", lambda _, **k: observed.append(k))
    assert module._run_completed_session_signal_cache_sync(
        cfg, reconcile_gateway_cache_status=True,
    ) is (None if cached else result)
    assert len(inference) == (0 if cached else 1)
    assert observed[-1]["status"] == "ready"
    assert observed[-1]["details"]["reused_verified_cache"] is cached
    assert observed[-1]["details"]["official_reconciliation"] == "completed"
    assert not observed[-1]["details"]["formal_history_eligible"]
    # A stale/unaccepted source cannot clear the former failure from a cache hit.
    observed.clear()
    status.data.fresh = False
    assert module._run_completed_session_signal_cache_sync(
        cfg, reconcile_gateway_cache_status=True,
    ) is None
    assert observed == []
    # The canonical isolated worker may reuse/generate persistent artifacts,
    # but it cannot overwrite the Gateway's run-owned provisional cache state.
    status.data.fresh = True
    module._run_completed_session_signal_cache_sync(cfg)
    assert observed == []


def test_official_cache_status_does_not_mark_yesterdays_cache_as_today(monkeypatch):
    from services.discord_bot import bot as module
    cfg = SimpleNamespace(market="test", timezone="Asia/Taipei", day_trade_simulation_enabled=True)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat("2026-10-08T15:30:00+08:00")
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(module, "_write_postclose_fast_cache_status_best_effort", lambda *a, **k: pytest.fail("not today's cache"))
    module._record_official_postclose_cache_ready(cfg, {"panel_date": "2026-10-07"}, reused=True)
