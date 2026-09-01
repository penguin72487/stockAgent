from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pytest

import stockagent.live.tw_day_trade_dashboard as dashboard_module
from stockagent.backtest.tw_execution import TaiwanFeeSchedule
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.live.tw_day_trade_dashboard import (
    _line_count,
    _rebase_live_benchmark,
    _session_progress,
    _tail,
    _tail_for_session,
    build_dashboard_event_page,
    build_dashboard_history_snapshot,
    build_dashboard_position_page,
    build_dashboard_revision,
    build_dashboard_signal_page,
    build_dashboard_snapshot,
    build_dashboard_summary,
)
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_VWAP,
    ENTRY_FILL_POLICY_CAUSAL_BOOK,
    ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK,
    ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK,
    ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
    ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK,
    LiveEligibility,
    ModeSpec,
    TwDayTradeSimulationEngine,
    load_live_eligibility,
    quote_map_from_snapshot,
    require_exact_session_eligibility,
)
from stockagent.live.tw_day_trade_service_sync import load_service_sync


TAIPEI = ZoneInfo("Asia/Taipei")


def _now(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 8, 13, hour, minute, second, tzinfo=TAIPEI)


def _spec(tmp_path: Path) -> ModeSpec:
    checkpoint = tmp_path / "checkpoint_best.pt"
    checkpoint.write_bytes(b"checkpoint")
    parquet_root = tmp_path / "stocks"
    parquet_root.mkdir()
    (parquet_root / "symbols.csv").write_text(
        "code,name,market,security_type,source\n2330,台積電,twse,stock,official\n",
        encoding="utf-8",
    )
    return ModeSpec(
        market="tw_day_trade",
        label="10m",
        initial_capital_twd=10_000_000.0,
        config_path="config.yaml",
        checkpoint_path=str(checkpoint),
        parquet_root=parquet_root,
        live_output_dir=tmp_path / "signals",
        fee_schedule=TaiwanFeeSchedule(commission_discount=0.2),
        lot_size=1_000,
    )


def _summary(signal_id: str = "signal-1") -> dict[str, object]:
    return {
        "signal_id": signal_id,
        "generated_at": _now(9, 0, 5).isoformat(),
        "execution_mode": "tw_day_trade",
        "live_session_open_feature_applied": True,
        "feature_cutoff_date": "2026-08-12 13:30:00",
        "checkpoint_fingerprint": "abc",
        "config_fingerprint": "def",
        "weights_path": "artifacts/live_signals/unit/target_weights.parquet",
        "positions_markdown_path": "artifacts/live_signals/unit/target_positions.md",
        "symbol_count": 1,
        "target_risk": {"gross": 0.1, "long_gross": 0.1, "short_gross": 0.0},
    }


def test_process_quotes_can_isolate_one_replay_market(tmp_path: Path) -> None:
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = _spec(tmp_path)
    second = replace(first, market="tw_day_trade_second", label="second")
    for spec, signal_id in ((first, "signal-first"), (second, "signal-second")):
        assert engine.register_signal(
            spec=spec,
            summary=_summary(signal_id),
            signal_rows=[_row()],
            quotes={"2330": _quote()},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 1, 6),
        ) == "registered"

    exit_quote = _quote(bid=1_001.0, ask=1_002.0)
    exit_quote["minute_volume_lots"] = 10_000.0
    exit_quote["bid_volume"] = 10_000.0
    engine.process_quotes(
        quotes={"2330": exit_quote},
        now=_now(13, 24),
        markets=[first.market],
    )

    assert engine.state["modes"][first.market]["open_position_count"] == 0
    assert engine.state["modes"][second.market]["open_position_count"] == 1


def test_process_quotes_can_defer_state_persistence_for_historical_replay(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    engine = TwDayTradeSimulationEngine(state_dir)
    engine.state["modes"] = {
        "tw_day_trade_100m": {
            "market": "tw_day_trade_100m",
            "initial_capital_twd": 100_000_000.0,
            "positions": {},
        }
    }

    engine.process_quotes(quotes={}, now=_now(9, 2), persist=False)

    assert engine.state["modes"]["tw_day_trade_100m"]["last_mark_at"].startswith(
        "2026-08-13T09:02"
    )
    assert not (state_dir / "state.json").exists()


def test_historical_replay_ledgers_flush_once_at_session_boundary(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    engine = TwDayTradeSimulationEngine(state_dir)
    engine.begin_deferred_ledger_writes()
    engine._event("historical_minute", recorded_at=_now(9, 2))

    assert not (state_dir / "events.jsonl").exists()

    engine.flush_deferred_ledger_writes()

    rows = [json.loads(line) for line in (state_dir / "events.jsonl").read_text().splitlines()]
    assert [row["event"] for row in rows] == ["historical_minute"]


def test_dashboard_session_progress_exposes_market_retry_and_closing_auction() -> None:
    retry = _session_progress(
        observed=_now(13, 24, 30), mode_count=0, modes=[], marks=[]
    )
    auction = _session_progress(
        observed=_now(13, 25, 30), mode_count=0, modes=[], marks=[]
    )
    assert retry["phase"] == "force_exit"
    assert retry["next_milestone_label"] == "13:25 收盤集合競價"
    assert auction["phase"] == "closing_auction"
    assert auction["next_milestone_label"] == "13:30 撮合／帳務完成"


def test_dashboard_mark_progress_is_complete_after_all_modes_are_durably_flat() -> None:
    modes = [
        {
            "market": f"mode-{idx}",
            "session_date": "2026-08-13",
            "open_position_count": 0,
            "residual_conversion_completed_at": "2026-08-13T13:30:00+08:00",
        }
        for idx in range(3)
    ]
    marks = [
        {"market": f"mode-{idx}", "minute": "2026-08-13T10:00+08:00"}
        for idx in range(3)
    ]

    progress = _session_progress(
        observed=_now(13, 31), mode_count=3, modes=modes, marks=marks
    )

    assert progress["mark_tracking_complete"] is True
    assert progress["mark_tracking_completed_modes"] == 3
    assert progress["observed_mode_minutes"] == 3
    assert progress["expected_mode_minutes"] == 3
    assert progress["mark_progress_ratio"] == 1.0


def test_engine_publishes_one_compact_revision_after_related_state_files(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    engine.update_readiness([spec], now=_now(8, 59))

    receipt = load_service_sync(engine.state_dir)
    assert receipt is not None
    revision = receipt["state_revision"]
    content_revision = receipt["content_revision"]
    assert receipt["enabled_markets"] == [spec.market]
    assert receipt["ledger_integrity_ready"] is True
    assert receipt["modes"][spec.market]["checkpoint_ready"] is True
    for filename in ("state.json", "status.json", "positions.json"):
        payload = json.loads((engine.state_dir / filename).read_text(encoding="utf-8"))
        assert payload["state_revision"] == revision

    engine.update_readiness([spec], now=_now(8, 59, 30))
    heartbeat = load_service_sync(engine.state_dir)
    assert heartbeat is not None
    assert heartbeat["state_revision"] == revision + 1
    assert heartbeat["content_revision"] == content_revision


def test_engine_quarantines_interrupted_signal_commit_after_restart(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(8, 59))
    engine._event(
        "signal_commit_started",
        recorded_at=_now(9, 0, 1),
        market=spec.market,
        session_date=_now(9, 0, 1).date().isoformat(),
        signal_id="interrupted-signal",
    )

    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    restarted.update_readiness([spec], now=_now(9, 0, 2))
    mode = restarted.state["modes"][spec.market]
    status = json.loads(restarted.status_path.read_text(encoding="utf-8"))
    sync = json.loads(restarted.service_sync_path.read_text(encoding="utf-8"))

    assert mode["engine_status"] == "critical_ledger_state_divergence"
    assert mode["ledger_state_divergence"] == {
        "kind": "signal_commit_started_without_registration",
        "signal_id": "interrupted-signal",
        "session_date": "2026-08-13",
        "detected_at": mode["ledger_state_divergence"]["detected_at"],
    }
    assert status["health"] == "critical"
    assert status["ledger_integrity"]["ready"] is False
    assert sync["ledger_integrity_ready"] is False


def test_dashboard_revision_proves_discord_ack_and_hides_disabled_mode(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.state.setdefault("modes", {})["retired_mode"] = {
        "market": "retired_mode",
        "label": "retired",
        "initial_capital_twd": 1_000_000.0,
        "total_equity_twd": 1_000_000.0,
        "positions": {},
    }
    engine.update_readiness([spec], now=_now(8, 59))
    receipt = load_service_sync(engine.state_dir)
    assert receipt is not None
    bot_status = tmp_path / "service_status.json"
    bot_status.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "updated_at": _now(8, 59).isoformat(),
                "discord_connected": True,
                "engine_state_revision": receipt["state_revision"],
                "day_trade_markets": [spec.market],
            }
        ),
        encoding="utf-8",
    )

    sync = build_dashboard_revision(
        state_dir=engine.state_dir,
        discord_service_status_path=bot_status,
        now=_now(8, 59).astimezone(ZoneInfo("UTC")),
    )
    snapshot = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=_now(8, 59).astimezone(ZoneInfo("UTC")),
    )

    assert sync["synchronized"] is True
    assert sync["status"] == "synchronized"
    assert sync["revision_lag"] == 0
    assert [row["market"] for row in snapshot["modes"]] == [spec.market]
    assert snapshot["ledger_integrity"]["ready"] is True


def test_dashboard_reports_measured_input_to_ledger_latency(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(8, 59))
    for index, elapsed in enumerate((100, 300), start=1):
        started = _now(9, 0, index)
        engine.record_latency_sample(
            market=spec.market,
            signal_id=f"signal-{index}",
            result="registered",
            summary={
                "signal_started_at": started.isoformat(timespec="microseconds"),
                "signal_ready_at": started.isoformat(timespec="microseconds"),
                "artifact_published_at": started.isoformat(timespec="microseconds"),
                "live_latency": {
                    "quote_fetch_ms": 20.0,
                    "model_inference_ms": 30.0,
                    "compute_before_publish_ms": 40.0,
                    "artifact_publish_ms": 5.0,
                },
            },
            consumer_detected_at=started,
            ledger_persisted_at=started.replace(microsecond=elapsed * 1_000),
            executor_quote_fetch_ms=50.0,
            eligibility_load_ms=10.0,
            ledger_compute_persist_ms=15.0,
            opening_signal_batch_wait_ms=40.0,
            opening_signal_batch_mode_count=3,
            opening_signal_batch_expected_mode_count=3,
            opening_signal_batch_complete=True,
        )

    payload = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=_now(10, 0).astimezone(ZoneInfo("UTC")),
    )
    latency = payload["latency"]
    assert latency["sample_count"] == 2
    assert latency["latest_ms"] == pytest.approx(300.0)
    assert latency["p50_ms"] == pytest.approx(200.0)
    assert latency["p95_ms"] == pytest.approx(290.0)
    assert latency["max_ms"] == pytest.approx(300.0)
    assert latency["latest_bottleneck_stage"] == "executor_quote_fetch_ms"
    assert latency["not_external_order_or_venue_rtt"] is True
    latest_receipt = json.loads(
        (engine.state_dir / "latency.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()[-1]
    )
    assert latest_receipt["opening_signal_batch"] == {
        "observed_mode_count": 3,
        "expected_mode_count": 3,
        "complete": True,
        "single_causal_quote_request": True,
    }
    assert latest_receipt["stages"]["opening_signal_batch_wait_ms"] == 40.0


def test_runner_reads_atomic_latest_signal_pointer(tmp_path: Path) -> None:
    from scripts.run_tw_day_trade_simulation import _LATEST_SIGNAL_CACHE, _latest_signal

    spec = _spec(tmp_path)
    output = spec.live_output_dir / "2026-08-13" / "signal-fast"
    output.mkdir(parents=True)
    summary_path = output / "summary.json"
    weights_path = output / "target_weights.parquet"
    summary_path.write_text(
        json.dumps(
            {
                **_summary("signal-fast"),
                "market": spec.market,
                "asof_date": "2026-08-13 09:00:01",
            }
        ),
        encoding="utf-8",
    )
    pl.DataFrame([_row()]).write_parquet(weights_path)
    spec.live_output_dir.mkdir(parents=True, exist_ok=True)
    (spec.live_output_dir / "latest_signal.json").write_text(
        json.dumps(
            {
                "summary_path": str(summary_path),
                "weights_path": str(weights_path),
            }
        ),
        encoding="utf-8",
    )
    _LATEST_SIGNAL_CACHE.clear()

    summary, rows = _latest_signal(spec, _now(9, 0, 1)) or ({}, [])

    assert summary["signal_id"] == "signal-fast"
    assert rows[0]["symbol"] == "2330"
    assert _LATEST_SIGNAL_CACHE


def test_opening_signal_batch_waits_for_all_modes_then_returns_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from scripts import run_tw_day_trade_simulation as runner

    base = _spec(tmp_path)
    specs = [
        replace(
            base,
            market=f"mode-{index}",
            live_output_dir=tmp_path / f"signals-{index}",
        )
        for index in range(3)
    ]
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.state["modes"] = {
        spec.market: {"market": spec.market, "processed_signal_ids": []}
        for spec in specs
    }
    available_at = {"mode-0": 0.0, "mode-1": 0.25, "mode-2": 0.5}

    class ClockWatcher:
        def __init__(self) -> None:
            self.elapsed = 0.0
            self.wait_calls = 0

        def monotonic(self) -> float:
            return self.elapsed

        def now(self) -> datetime:
            return _now(9, 0) + runner.timedelta(seconds=self.elapsed)

        def wait(self, seconds: float) -> bool:
            self.elapsed += seconds
            self.wait_calls += 1
            return True

    clock = ClockWatcher()

    def fake_latest(spec: ModeSpec, _observed: datetime):
        if clock.elapsed + 1e-9 < available_at[spec.market]:
            return None
        return ({"signal_id": f"signal-{spec.market}"}, [{"symbol": "2330"}])

    monkeypatch.setattr(runner, "_latest_signal", fake_latest)

    found, metadata = runner._collect_opening_signal_batch(
        specs,
        engine,
        _now(9, 0),
        signal_watcher=clock,
        max_wait_seconds=2.0,
        cutoff_seconds=12.0,
        poll_seconds=0.25,
        now_fn=clock.now,
        monotonic_fn=clock.monotonic,
    )

    assert set(found) == {spec.market for spec in specs}
    assert metadata["complete"] is True
    assert metadata["timed_out"] is False
    assert metadata["observed_mode_count"] == 3
    assert metadata["expected_mode_count"] == 3
    assert metadata["wait_ms"] == pytest.approx(500.0)
    assert metadata["wait_by_market_ms"]["mode-0"] == pytest.approx(500.0)
    assert metadata["wait_by_market_ms"]["mode-2"] == pytest.approx(0.0)
    assert clock.wait_calls == 2


def test_opening_signal_batch_times_out_without_blocking_ready_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from scripts import run_tw_day_trade_simulation as runner

    base = _spec(tmp_path)
    specs = [
        replace(
            base,
            market=f"mode-{index}",
            live_output_dir=tmp_path / f"signals-{index}",
        )
        for index in range(2)
    ]
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.state["modes"] = {
        spec.market: {"market": spec.market, "processed_signal_ids": []}
        for spec in specs
    }

    class ClockWatcher:
        def __init__(self) -> None:
            self.elapsed = 0.0

        def monotonic(self) -> float:
            return self.elapsed

        def now(self) -> datetime:
            return _now(9, 0) + runner.timedelta(seconds=self.elapsed)

        def wait(self, seconds: float) -> bool:
            self.elapsed += seconds
            return False

    clock = ClockWatcher()
    monkeypatch.setattr(
        runner,
        "_latest_signal",
        lambda spec, _observed: (
            ({"signal_id": "signal-mode-0"}, [{"symbol": "2330"}])
            if spec.market == "mode-0"
            else None
        ),
    )

    found, metadata = runner._collect_opening_signal_batch(
        specs,
        engine,
        _now(9, 0),
        signal_watcher=clock,
        max_wait_seconds=0.5,
        cutoff_seconds=12.0,
        poll_seconds=0.25,
        now_fn=clock.now,
        monotonic_fn=clock.monotonic,
    )

    assert set(found) == {"mode-0"}
    assert metadata["complete"] is False
    assert metadata["timed_out"] is True
    assert metadata["wait_ms"] == pytest.approx(500.0)


def test_runner_engine_lock_rejects_a_second_state_writer(tmp_path: Path) -> None:
    from scripts.run_tw_day_trade_simulation import _acquire_engine_lock

    first = _acquire_engine_lock(tmp_path / "state")
    try:
        with pytest.raises(RuntimeError, match="live writer"):
            _acquire_engine_lock(tmp_path / "state")
    finally:
        first.close()
    second = _acquire_engine_lock(tmp_path / "state")
    second.close()


def test_runner_prefers_compact_execution_payload_over_parquet(tmp_path: Path) -> None:
    from scripts.run_tw_day_trade_simulation import _LATEST_SIGNAL_CACHE, _latest_signal

    spec = _spec(tmp_path)
    output = spec.live_output_dir / "2026-08-13" / "signal-compact"
    output.mkdir(parents=True)
    summary_path = output / "summary.json"
    weights_path = output / "target_weights.parquet"
    execution_weights_path = output / "execution_weights.json"
    summary_path.write_text(
        json.dumps(
            {
                **_summary("signal-compact"),
                "market": spec.market,
                "asof_date": "2026-08-13 09:00:01",
            }
        ),
        encoding="utf-8",
    )
    pl.DataFrame([_row()]).write_parquet(weights_path)
    execution_weights_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "market": spec.market,
                "signal_id": "signal-compact",
                "rows": [{**_row(), "symbol": "0050", "name": "元大台灣50"}],
            }
        ),
        encoding="utf-8",
    )
    spec.live_output_dir.mkdir(parents=True, exist_ok=True)
    (spec.live_output_dir / "latest_signal.json").write_text(
        json.dumps(
            {
                "summary_path": str(summary_path),
                "weights_path": str(weights_path),
                "execution_weights_path": str(execution_weights_path),
            }
        ),
        encoding="utf-8",
    )
    _LATEST_SIGNAL_CACHE.clear()

    summary, rows = _latest_signal(spec, _now(9, 0, 1)) or ({}, [])

    assert summary["signal_id"] == "signal-compact"
    assert rows[0]["symbol"] == "0050"
    assert summary["execution_weights_path"] == str(execution_weights_path)


def test_signal_pointer_watcher_wakes_on_atomic_publish(tmp_path: Path) -> None:
    from scripts.run_tw_day_trade_simulation import _SignalPointerWatcher

    watcher = _SignalPointerWatcher()
    watcher.configure([tmp_path])
    if not watcher.enabled:
        pytest.skip("Linux inotify is unavailable")
    temporary = tmp_path / "latest_signal.json.tmp"
    pointer = tmp_path / "latest_signal.json"
    temporary.write_text("{}", encoding="utf-8")
    os.replace(temporary, pointer)
    try:
        assert watcher.wait(0.5) is True
    finally:
        watcher.close()


def test_jsonl_batch_uses_one_durability_sync(tmp_path: Path, monkeypatch) -> None:
    from stockagent.live.tw_day_trade_simulation import _append_jsonl_many

    calls = 0
    real_fsync = os.fsync

    def counted_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", counted_fsync)
    path = tmp_path / "signals.jsonl"
    _append_jsonl_many(path, [{"row": idx} for idx in range(2_744)])

    assert calls == 1
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2_744


def _row(weight: float = 0.1) -> dict[str, object]:
    return {
        "symbol": "2330",
        "name": "台積電",
        "target_weight": weight,
        "tradable": True,
        "can_buy": True,
        "can_sell": True,
        "score": 1.0,
        "raw_score": 1.1,
    }


def test_runner_loads_all_three_configured_day_trade_modes() -> None:
    from scripts.run_tw_day_trade_simulation import _mode_specs

    repo_root = Path(__file__).resolve().parents[1]
    specs, live_configs, errors = _mode_specs(
        repo_root / "services/discord_bot/markets"
    )
    by_market = {spec.market: spec for spec in specs}
    active_expected = {
        "tw_day_trade_100m",
        "tw_day_trade_multi_basis",
        "tw_day_trade_multi_basis_projection_l1_gelu",
    }

    assert errors == {}
    assert set(by_market) == active_expected
    assert set(live_configs) == active_expected
    assert by_market["tw_day_trade_100m"].initial_capital_twd == 100_000_000.0
    assert by_market["tw_day_trade_multi_basis"].initial_capital_twd == 10_000_000.0
    assert (
        by_market["tw_day_trade_multi_basis_projection_l1_gelu"].initial_capital_twd
        == 10_000_000.0
    )
    specs, live_configs, errors = _mode_specs(
        repo_root / "services/discord_bot/markets",
        include_disabled=True,
    )
    assert errors == {}
    assert {spec.market for spec in specs} == active_expected
    assert set(live_configs) == active_expected
    assert all(spec.signal_market is None for spec in specs)
    assert all(spec.price_limit_offset_ticks == 1 for spec in specs)
    assert all(
        spec.entry_fill_policy == ENTRY_FILL_POLICY_CAUSAL_BOOK for spec in specs
    )
    assert all(spec.entry_price_offset_ticks == 0 for spec in specs)


def test_runner_blocks_weekend_before_quote_or_eligibility_work(tmp_path: Path) -> None:
    from scripts.run_tw_day_trade_simulation import _mode_specs, _verified_stock_session

    repo_root = Path(__file__).resolve().parents[1]
    specs, live_configs, errors = _mode_specs(
        repo_root / "services/discord_bot/markets"
    )
    assert errors == {}
    public_root = tmp_path / "data_tw_public"
    public_root.mkdir()
    pl.DataFrame(
        {
            "Name": ["中華民國開國紀念日"],
            "Date": ["1150101"],
            "_dataset": ["twse_api_holidayschedule_holidayschedule"],
            "_source": ["TWSE OpenAPI"],
            "_as_of_date": ["2026-08-14"],
        }
    ).write_parquet(public_root / "twse_api_holidayschedule_holidayschedule.parquet")
    live_configs = {
        market: replace(config, day_trade_rule_data_dir=str(public_root))
        for market, config in live_configs.items()
    }

    friday_open, friday_errors = _verified_stock_session(
        specs,
        live_configs,
        observed=datetime(2026, 8, 14, 9, 0, tzinfo=TAIPEI),
    )
    saturday_open, saturday_errors = _verified_stock_session(
        specs,
        live_configs,
        observed=datetime(2026, 8, 15, 9, 0, tzinfo=TAIPEI),
    )

    assert friday_open is True and friday_errors == {}
    assert saturday_open is False
    assert set(saturday_errors) == {spec.market for spec in specs}
    assert all("weekend" in reason for reason in saturday_errors.values())


def test_runner_keeps_quote_polling_for_post_close_terminal_catch_up() -> None:
    from scripts.run_tw_day_trade_simulation import (
        _active_quote_due,
        _loop_sleep_seconds,
        _missed_opening_recovery_required,
        _pending_signal_retry_delay_seconds,
    )

    observed = _now(13, 35)
    minute_key = observed.replace(second=0, microsecond=0).isoformat(timespec="minutes")
    assert _active_quote_due(["2330"], observed=observed, last_quote_minute=None)
    assert not _active_quote_due(
        ["2330"], observed=observed, last_quote_minute=minute_key
    )
    assert not _active_quote_due([], observed=observed, last_quote_minute=None)

    force_exit = _now(13, 24, 20)
    force_minute = force_exit.replace(second=0, microsecond=0).isoformat(
        timespec="minutes"
    )
    assert _active_quote_due(
        ["2330"], observed=force_exit, last_quote_minute=force_minute
    )
    auction = _now(13, 25, 20)
    auction_minute = auction.replace(second=0, microsecond=0).isoformat(
        timespec="minutes"
    )
    assert not _active_quote_due(
        ["2330"], observed=auction, last_quote_minute=auction_minute
    )
    assert _loop_sleep_seconds(
        _now(9, 0),
        fast_seconds=0.1,
        has_pending_signal=False,
        has_open_position=False,
    ) == pytest.approx(0.1)
    assert _loop_sleep_seconds(
        _now(10, 0),
        fast_seconds=0.1,
        has_pending_signal=False,
        has_open_position=True,
    ) == pytest.approx(1.0)
    assert _loop_sleep_seconds(
        _now(10, 0),
        fast_seconds=0.1,
        has_pending_signal=True,
        has_open_position=False,
    ) == pytest.approx(0.1)
    assert _pending_signal_retry_delay_seconds(
        "waiting_quote", _now(9, 18, 7)
    ) == pytest.approx(0.5)
    assert _pending_signal_retry_delay_seconds(
        "waiting_first_minute", _now(9, 18, 7)
    ) == pytest.approx(53.05)
    assert not _missed_opening_recovery_required(_now(9, 0, 15))
    assert _missed_opening_recovery_required(_now(9, 0, 16))


def test_runner_recovers_missing_daily_limits_without_replacing_shioaji_book(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import run_tw_day_trade_simulation as runner

    without_limits = PriceSnapshot(
        prices=np.asarray([999.0]),
        source="shioaji:stock_snapshot",
        available_count=1,
        # Price-limit recovery must not depend on Shioaji quote availability;
        # the official daily limits have their own source contract.
        available_mask=np.asarray([False]),
        open_prices=np.asarray([995.0]),
        bid_prices=np.asarray([998.0]),
        ask_prices=np.asarray([1_000.0]),
        bid_volumes=np.asarray([20.0]),
        ask_volumes=np.asarray([10.0]),
        upper_limit_prices=np.asarray([np.nan]),
        lower_limit_prices=np.asarray([np.nan]),
        reference_prices=np.asarray([np.nan]),
        timestamps_ms=np.asarray([int(_now(9, 0, 6).timestamp() * 1_000)]),
    )
    with_limits = PriceSnapshot(
        prices=without_limits.prices,
        source="shioaji:stock_snapshot+prepared_limits",
        available_count=1,
        available_mask=without_limits.available_mask,
        open_prices=without_limits.open_prices,
        bid_prices=without_limits.bid_prices,
        ask_prices=without_limits.ask_prices,
        bid_volumes=without_limits.bid_volumes,
        ask_volumes=without_limits.ask_volumes,
        upper_limit_prices=np.asarray([1_100.0]),
        lower_limit_prices=np.asarray([900.0]),
        reference_prices=np.asarray([1_000.0]),
        timestamps_ms=without_limits.timestamps_ms,
    )
    snapshots = iter((without_limits, with_limits))
    quote_ttls: list[float] = []
    prepared: list[dict[str, object]] = []

    def fake_quotes(
        symbols: list[str], fallback: np.ndarray, *, cache_ttl_seconds: float
    ) -> PriceSnapshot:
        assert symbols == ["2330"]
        assert fallback.tolist() == [995.0]
        quote_ttls.append(cache_ttl_seconds)
        return next(snapshots)

    def fake_prepare(
        symbols: list[str],
        fallback: np.ndarray,
        *,
        parquet_root: str | Path,
        trading_date: str,
    ) -> dict[str, object]:
        prepared.append(
            {
                "symbols": symbols,
                "fallback": fallback.tolist(),
                "parquet_root": Path(parquet_root),
                "trading_date": trading_date,
            }
        )
        return {"prepared_count": 1}

    monkeypatch.setattr(runner, "fetch_shioaji_stock_snapshots", fake_quotes)
    monkeypatch.setattr(runner, "prepare_tw_price_limit_snapshot", fake_prepare)

    quotes = runner._fetch_quotes(
        symbols=["2330"],
        fallback_by_symbol={"2330": 995.0},
        parquet_root=tmp_path,
        trading_date=_now(9, 0, 7),
    )

    assert quote_ttls == [0.0, 60.0]
    assert prepared == [
        {
            "symbols": ["2330"],
            "fallback": [995.0],
            "parquet_root": tmp_path,
            "trading_date": "2026-08-13",
        }
    ]
    assert quotes["2330"]["bid"] == 998.0
    assert quotes["2330"]["ask"] == 1_000.0
    assert quotes["2330"]["upper_limit"] == 1_100.0
    assert quotes["2330"]["lower_limit"] == 900.0


def test_benchmark_previous_close_context_uses_last_completed_official_row(
    tmp_path: Path,
) -> None:
    from scripts import run_tw_day_trade_simulation as runner

    pl.DataFrame(
        {
            "date": [date(2026, 8, 13), date(2026, 8, 14)],
            "close": [990.0, 1_000.0],
            "data_source": ["twse_official", "twse_official"],
        }
    ).write_parquet(tmp_path / "2330_features.parquet")
    quotes = {
        "2330": {
            "reference_price": 995.0,
            "source": "shioaji:stock_snapshot+prepared_limits",
        }
    }

    runner._attach_benchmark_previous_close_context(
        quotes,
        symbols={"2330"},
        parquet_root=tmp_path,
        trading_date=datetime(2026, 8, 17, 9, 0, tzinfo=TAIPEI),
    )

    assert quotes["2330"]["previous_close"] == 1_000.0
    assert quotes["2330"]["previous_close_date"] == "2026-08-14"
    assert "twse_official" in quotes["2330"]["previous_close_source"]
    assert quotes["2330"]["reference_price_source"].startswith("shioaji:")


def _eligibility() -> dict[str, LiveEligibility]:
    return {
        "2330": LiveEligibility(
            symbol="2330",
            venue="twse",
            security_type="stock",
            eligible=True,
            short_open=True,
            covered=True,
            source_date="2026-08-13",
        )
    }


def _quote(
    *,
    open_price: float = 1_000.0,
    bid: float = 998.0,
    ask: float = 1000.0,
    last: float = 999.0,
    bid_volume: float = 20.0,
    ask_volume: float = 20.0,
    minute_volume_lots: float = 100.0,
) -> dict[str, object]:
    return {
        "symbol": "2330",
        "open": open_price,
        "last": last,
        "bid": bid,
        "ask": ask,
        "bid_volume": bid_volume,
        "ask_volume": ask_volume,
        "upper_limit": 1_100.0,
        "lower_limit": 900.0,
        "minute_volume_lots": minute_volume_lots,
        "quote_at": _now(9, 1, 6).isoformat(),
        "source": "fixture",
    }


def test_entry_waits_for_quote_strictly_after_signal(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote()
    quote["quote_at"] = _now(9, 0, 5).isoformat()

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 6),
    )

    assert result == "waiting_quote"
    assert not engine.state["modes"][spec.market]["positions"]


@pytest.mark.parametrize(
    ("weight", "expected_entry"),
    ((0.1, 1_000.0), (-0.1, 998.0)),
)
def test_live_entry_starts_at_0900_and_uses_first_causally_later_best_quote(
    tmp_path: Path,
    weight: float,
    expected_entry: float,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote()
    quote["quote_at"] = _now(9, 0, 6).isoformat()

    assert (
        engine.register_signal(
            spec=spec,
            summary=_summary(),
            signal_rows=[_row(weight)],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 0, 7),
        )
        == "registered"
    )

    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert mode["entry_fill_policy"] == ENTRY_FILL_POLICY_CAUSAL_BOOK
    assert mode["entry_price_offset_ticks"] == 0
    assert mode["counterfactual_open_replay"] is False
    assert position["entry_price"] == expected_entry
    assert position["entry_quote_at"] == _now(9, 0, 6).isoformat()
    assert position["entry_at"] == _now(9, 0, 7).isoformat()
    assert position["synthetic_fallback_fill"] is False
    assert position["counterfactual_open_price_fill"] is False


def test_legacy_causal_entry_executes_after_0901_with_completed_kbar(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote(minute_volume_lots=100.0)
    quote["quote_at"] = _now(9, 1, 6).isoformat()

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 59),
    )

    assert result == "registered"
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["filled_shares"] == 1_000


def test_late_entry_waits_for_adjacent_completed_minute_after_restart(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote()
    quote["minute_volume_lots"] = None
    quote["quote_at"] = _now(9, 18, 6).isoformat()

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 18, 7),
    )

    assert result == "waiting_first_minute"
    mode = engine.state["modes"][spec.market]
    assert mode.get("entry_completed_at") is None
    assert mode["processed_signal_ids"] == []
    assert mode["engine_status"] == "waiting_completed_minute_liquidity"

    next_quote = _quote(minute_volume_lots=100.0)
    next_quote["quote_at"] = _now(9, 19, 6).isoformat()
    assert (
        engine.register_signal(
            spec=spec,
            summary=_summary(),
            signal_rows=[_row()],
            quotes={"2330": next_quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 19, 7),
        )
        == "registered"
    )
    assert next(iter(mode["positions"].values()))["filled_shares"] == 1_000


@pytest.mark.parametrize(
    ("weight", "expected_entry", "expected_take_profit", "expected_stop"),
    (
        (0.1, 1_000.0, 1_100.0, 900.0),
        (-0.1, 998.0, 900.0, 1_100.0),
    ),
)
def test_market_entry_uses_causal_best_quote_and_records_board_lot(
    tmp_path: Path,
    weight: float,
    expected_entry: float,
    expected_take_profit: float,
    expected_stop: float,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(weight)],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    assert result == "registered"
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["entry_price"] == expected_entry
    assert position["filled_shares"] == 1_000
    order = next(
        json.loads(line)
        for line in engine.orders_path.read_text().splitlines()
        if json.loads(line)["purpose"] == "entry"
    )
    assert order["quantity"] == 1_000
    assert order["status"] == "filled"
    assert order["unfilled_quantity"] == 0
    assert order["model_requested_quantity"] == 1_000
    assert order["target_unsubmitted_quantity"] == 0
    assert order["order_type"] == "MKT"
    assert order["synthetic_fill"] is False
    assert position["take_profit_price"] == expected_take_profit
    assert position["stop_trigger_price"] == expected_stop


@pytest.mark.parametrize("weight", (0.1, -0.1))
def test_official_open_policy_executes_at_0901_open_without_tick_or_book(
    tmp_path: Path,
    weight: float,
) -> None:
    spec = replace(
        _spec(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
        entry_price_offset_ticks=0,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote(open_price=999.0, bid=900.0, ask=1_100.0)
    quote["quote_at"] = _now(9, 1, 0).isoformat()

    assert (
        engine.register_signal(
            spec=spec,
            summary=_summary(),
            signal_rows=[_row(weight)],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 1, 1),
        )
        == "registered"
    )

    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    order = next(
        json.loads(line)
        for line in engine.orders_path.read_text().splitlines()
        if json.loads(line)["purpose"] == "entry"
    )
    fill = next(
        json.loads(line)
        for line in engine.fills_path.read_text().splitlines()
        if json.loads(line)["purpose"] == "entry"
    )
    assert position["entry_price"] == 999.0
    assert position["sizing_open_price"] == 999.0
    assert position["entry_price_offset_ticks"] == 0
    assert position["counterfactual_open_price_fill"] is True
    assert position["synthetic_fallback_fill"] is False
    assert position["paper_market_fill"] is False
    assert order["order_type"] == "PAPER_OPEN_PRICE_0901"
    assert order["entry_fill_policy"] == ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
    assert order["entry_price_offset_ticks"] == 0
    assert fill["entry_fill_policy"] == ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
    assert fill["entry_price_offset_ticks"] == 0
    assert fill["price"] == 999.0
    assert mode["entry_official_open_fill_count"] == 1
    assert mode["entry_synthetic_fallback_fill_count"] == 0


def test_official_open_policy_does_not_execute_before_0901(tmp_path: Path) -> None:
    spec = replace(
        _spec(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
        entry_price_offset_ticks=0,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 0, 59),
    )

    assert result == "blocked"
    assert not engine.fills_path.exists()


@pytest.mark.parametrize("weight", (0.1, -0.1))
def test_missed_opening_replay_sizes_at_open_and_executes_at_0901_vwap(
    tmp_path: Path,
    weight: float,
) -> None:
    spec = replace(
        _spec(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_VWAP,
        entry_price_offset_ticks=0,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    summary = {
        **_summary(),
        "simulation_replay": True,
        "entry_fill_contract": (
            "retrospective_official_open_signal_at_09_00_observed_09_01_minute_vwap_counterfactual"
        ),
    }
    quote = _quote(open_price=1_000.0, bid=900.0, ask=1_100.0)
    quote.update(
        {
            "execution_price_0901": 1_007.5,
            "quote_at": _now(9, 1).isoformat(),
            "historical_source_quote_at": _now(9, 0, 59).isoformat(),
            "entry_price_source": (
                "shioaji:historical_ticks_0900_090059_vwap_right_label_0901"
            ),
        }
    )

    assert (
        engine.register_signal(
            spec=spec,
            summary=summary,
            signal_rows=[{**_row(weight), "open_price": 1_000.0}],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 1),
            counterfactual_open_replay=True,
        )
        == "registered"
    )

    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    order = next(
        json.loads(line)
        for line in engine.orders_path.read_text().splitlines()
        if json.loads(line)["purpose"] == "entry"
    )
    assert position["sizing_open_price"] == 1_000.0
    assert position["entry_price"] == 1_007.5
    assert position["counterfactual_0901_price_fill"] is True
    assert position["counterfactual_open_price_fill"] is False
    assert position["fill_guaranteed"] is False
    assert order["order_type"] == "PAPER_0901_MINUTE_VWAP"
    assert mode["entry_0901_vwap_fill_count"] == 1
    assert mode["entry_official_open_fill_count"] == 0

    # Reproduce the former restart bug: the live default had overwritten the
    # policy label even though the committed position/fill contract was 09:01.
    mode["entry_fill_policy"] = ENTRY_FILL_POLICY_CAUSAL_BOOK
    engine._persist(_now(9, 1, 1))
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    restarted.update_readiness(
        [replace(spec, entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_BOOK)],
        now=_now(9, 2),
    )
    restarted_mode = restarted.state["modes"][spec.market]
    assert (
        restarted_mode["entry_fill_policy"]
        == ENTRY_FILL_POLICY_0901_MINUTE_VWAP
    )
    assert (
        restarted_mode["configured_entry_fill_policy"]
        == ENTRY_FILL_POLICY_CAUSAL_BOOK
    )
    assert restarted_mode["counterfactual_0901_price_fill"] is True


def test_missed_opening_replay_never_falls_back_when_0901_vwap_is_missing(
    tmp_path: Path,
) -> None:
    spec = replace(
        _spec(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_VWAP,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    summary = {
        **_summary(),
        "simulation_replay": True,
        "entry_fill_contract": (
            "retrospective_official_open_signal_at_09_00_observed_09_01_minute_vwap_counterfactual"
        ),
    }
    quote = _quote(open_price=1_000.0, bid=999.0, ask=1_001.0)
    quote["execution_price_0901"] = None
    quote["quote_at"] = _now(9, 1).isoformat()

    assert (
        engine.register_signal(
            spec=spec,
            summary=summary,
            signal_rows=[{**_row(0.1), "open_price": 1_000.0}],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 1),
            counterfactual_open_replay=True,
        )
        == "registered"
    )
    signal = json.loads(engine.signals_path.read_text().splitlines()[0])
    assert signal["status"] == "blocked"
    assert signal["reason"] == "observed_09_01_minute_vwap_unavailable"
    assert signal["execution_price"] is None
    assert not engine.fills_path.exists()


def test_paper_market_order_fills_full_request_at_best_quote_without_depth_cap(
    tmp_path: Path,
) -> None:
    spec = replace(
        _spec(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK,
        entry_price_offset_ticks=1,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote(
        open_price=500.0,
        ask=1_000.0,
        ask_volume=1.0,
        minute_volume_lots=None,
    )
    quote["quote_at"] = _now(9, 2, 6).isoformat()

    assert (
        engine.register_signal(
            spec=spec,
            summary=_summary(),
            signal_rows=[_row(0.5)],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 2, 7),
        )
        == "registered"
    )

    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert position["requested_shares"] == 10_000
    assert position["filled_shares"] == 10_000
    assert position["entry_price"] == 1_000.0
    assert position["paper_market_fill"] is True
    assert mode["entry_fill_outcome"] == "filled"
    assert mode["entry_unfilled_shares"] == 0
    assert mode["entry_paper_market_fill_count"] == 1
    signal = json.loads(engine.signals_path.read_text().splitlines()[-1])
    assert signal["top_book_capacity_shares"] == 1_000
    assert signal["filled_shares"] == 10_000
    assert signal["synthetic_fill"] is False


def test_paper_market_order_uses_adverse_open_tick_only_without_causal_quote(
    tmp_path: Path,
) -> None:
    spec = replace(
        _spec(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK,
        entry_price_offset_ticks=1,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote(open_price=1_000.0)
    quote.update({"ask": None, "ask_volume": None, "quote_at": None})

    assert (
        engine.register_signal(
            spec=spec,
            summary=_summary(),
            signal_rows=[_row(0.5)],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 2, 7),
        )
        == "registered"
    )

    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert position["requested_shares"] == 5_000
    assert position["filled_shares"] == 5_000
    assert position["entry_price"] == 1_005.0
    assert position["synthetic_fallback_fill"] is True
    assert mode["entry_fill_outcome"] == "filled"
    assert mode["entry_synthetic_fallback_fill_count"] == 1
    assert mode["entry_fill_is_synthetic"] is True


@pytest.mark.parametrize(
    ("weight", "expected_entry"),
    ((0.5, 1_005.0), (-0.5, 999.0)),
)
def test_synthetic_open_tick_policy_fills_all_legal_board_lots_at_adverse_tick(
    tmp_path: Path,
    weight: float,
    expected_entry: float,
) -> None:
    spec = replace(
        _spec(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK,
        entry_price_offset_ticks=1,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote(
        open_price=1_000.0,
        bid_volume=0.0,
        ask_volume=0.0,
        minute_volume_lots=0.0,
    )
    quote["bid"] = None
    quote["ask"] = None
    quote["quote_at"] = None

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(weight)],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 10),
    )

    assert result == "registered"
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert position["entry_price"] == expected_entry
    assert position["requested_shares"] == 5_000
    assert position["filled_shares"] == 5_000
    assert position["entry_fill_is_synthetic"] is True
    assert position["fill_guaranteed"] is True
    assert mode["entry_fill_outcome"] == "filled"
    assert mode["entry_unfilled_shares"] == 0
    assert mode["signal_reason_counts"] == {"synthetic_open_tick_fill": 1}
    entry_order = next(
        json.loads(line)
        for line in engine.orders_path.read_text().splitlines()
        if json.loads(line)["purpose"] == "entry"
    )
    assert entry_order["order_type"] == "SYNTHETIC_OPEN_TICK"
    assert entry_order["synthetic_fill"] is True


@pytest.mark.parametrize(
    ("with_best_ask", "expected_entry", "expected_exact", "expected_fallback"),
    ((True, 1_000.0, 1, 0), (False, 1_005.0, 0, 1)),
)
def test_historical_hybrid_entry_records_exact_quote_or_adverse_tick_fallback(
    tmp_path: Path,
    with_best_ask: bool,
    expected_entry: float,
    expected_exact: int,
    expected_fallback: int,
) -> None:
    spec = replace(
        _spec(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK,
        entry_price_offset_ticks=1,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    replay = _summary()
    replay.update(
        {
            "simulation_replay": True,
            "entry_fill_contract": (
                "retrospective_historical_best_quote_else_adverse_open_tick_counterfactual"
            ),
        }
    )
    quote = _quote(open_price=1_000.0, ask=1_000.0, ask_volume=1.0)
    quote["quote_at"] = _now(9, 1).isoformat()
    quote["historical_source_quote_at"] = _now(9, 0, 7).isoformat()
    quote["entry_price_source"] = "shioaji:historical_stock_tick_best_ask"
    if not with_best_ask:
        quote["ask"] = None
        quote["ask_volume"] = None
        quote["entry_price_is_synthetic_fallback"] = True
        quote["entry_price_source"] = (
            "official_daily_session_open:adverse_one_legal_tick_fallback"
        )

    assert (
        engine.register_signal(
            spec=spec,
            summary=replay,
            signal_rows=[_row(0.5)],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 1),
            counterfactual_open_replay=True,
        )
        == "registered"
    )

    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert position["entry_price"] == expected_entry
    assert position["historical_entry_quote_at"] == _now(9, 0, 7).isoformat()
    assert position["entry_fill_is_synthetic"] is (not with_best_ask)
    assert mode["entry_best_quote_fill_count"] == expected_exact
    assert mode["entry_synthetic_fallback_fill_count"] == expected_fallback
    assert mode["entry_fill_is_synthetic"] is (not with_best_ask)


def test_sub_board_lot_signal_finishes_without_requesting_a_quote(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    row = {**_row(0.00001), "open_price": 1_000.0}

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[row],
        quotes={},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 10),
    )

    assert result == "registered"
    mode = engine.state["modes"][spec.market]
    assert mode["positions"] == {}
    assert mode["signal_counts"] == {"skipped": 1}
    signal = json.loads(engine.signals_path.read_text().splitlines()[-1])
    assert signal["reason"] == "below_one_board_lot"


def test_entry_sizes_at_open_and_caps_fill_at_half_minute_kbar(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(0.5)],
        quotes={
            "2330": _quote(
                open_price=500.0,
                ask=1_000.0,
                ask_volume=20.0,
                minute_volume_lots=2.0,
            )
        },
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["requested_shares"] == 10_000
    assert position["sizing_open_price"] == 500.0
    assert position["entry_price"] == 1_000.0
    assert position["filled_shares"] == 1_000
    entry_order = next(
        json.loads(line)
        for line in engine.orders_path.read_text().splitlines()
        if json.loads(line)["purpose"] == "entry"
    )
    assert entry_order["quantity"] == 1_000
    assert entry_order["status"] == "filled"
    assert entry_order["unfilled_quantity"] == 0
    assert entry_order["model_requested_quantity"] == 10_000
    assert entry_order["target_unsubmitted_quantity"] == 9_000


def test_cumulative_snapshots_only_create_adjacent_minute_volume(
    tmp_path: Path,
) -> None:
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    at_open = engine.prepare_minute_quotes(
        {"2330": {"cumulative_volume_lots": 3.0}}, now=_now(9, 0)
    )
    first = engine.prepare_minute_quotes(
        {"2330": {"cumulative_volume_lots": 10.0}}, now=_now(9, 1)
    )
    second = engine.prepare_minute_quotes(
        {"2330": {"cumulative_volume_lots": 14.0}}, now=_now(9, 2)
    )
    gap = engine.prepare_minute_quotes(
        {"2330": {"cumulative_volume_lots": 30.0}}, now=_now(9, 4)
    )

    assert at_open["2330"]["minute_volume_lots"] is None
    assert first["2330"]["minute_volume_lots"] == 10.0
    assert second["2330"]["minute_volume_lots"] == 4.0
    assert gap["2330"]["minute_volume_lots"] is None


def test_schema2_open_position_migrates_fail_closed(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "modes": {
                    "tw_day_trade": {"positions": {"legacy": {"signed_shares": 1_000}}}
                },
                "benchmarks": {},
            }
        ),
        encoding="utf-8",
    )

    engine = TwDayTradeSimulationEngine(state_dir)

    mode = engine.state["modes"]["tw_day_trade"]
    assert engine.state["schema_version"] == 4
    assert mode["legacy_execution_contract"] is True
    assert mode["engine_status"] == ("critical_legacy_position_requires_reconciliation")


def test_schema2_legacy_position_reconciles_only_with_executable_close(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    original = TwDayTradeSimulationEngine(tmp_path / "state")
    original.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    persisted = json.loads(original.state_path.read_text(encoding="utf-8"))
    persisted["schema_version"] = 2
    original.state_path.write_text(json.dumps(persisted), encoding="utf-8")
    engine = TwDayTradeSimulationEngine(original.state_dir)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert mode["legacy_execution_contract"] is True

    no_depth = _quote(last=899.0, bid=898.0, bid_volume=0.0)
    engine.process_quotes(quotes={"2330": no_depth}, now=_now(9, 2))
    assert position["signed_shares"] == 1_000
    assert mode["legacy_execution_contract"] is True

    executable = _quote(last=899.0, bid=898.0, bid_volume=2.0)
    engine.process_quotes(quotes={"2330": executable}, now=_now(9, 3))
    assert position["signed_shares"] == 0
    assert mode["legacy_execution_contract"] is False
    events = [json.loads(line) for line in engine.events_path.read_text().splitlines()]
    assert events[-1]["event"] == "legacy_positions_reconciled"


def test_inside_limit_mode_offsets_long_bracket_one_legal_tick(
    tmp_path: Path,
) -> None:
    spec = replace(
        _spec(tmp_path),
        label="inside one tick",
        price_limit_offset_ticks=1,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(0.1)],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert mode["signal_market"] == spec.market
    assert mode["price_limit_offset_ticks"] == 1
    assert mode["fill_guaranteed"] is False
    assert position["take_profit_price"] == 1_095.0
    assert position["stop_trigger_price"] == 901.0

    engine.process_quotes(
        quotes={"2330": _quote(bid=1_095.0, ask=1_100.0, last=1_095.0)},
        now=_now(9, 10),
    )

    assert position["signed_shares"] == 0
    assert position["exit_reason"] == "take_profit_inside_daily_limit_1_tick"


def test_inside_limit_mode_offsets_short_bracket_and_uses_market_stop(
    tmp_path: Path,
) -> None:
    spec = replace(
        _spec(tmp_path),
        price_limit_offset_ticks=1,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(-0.1)],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["take_profit_price"] == 901.0
    assert position["stop_trigger_price"] == 1_095.0

    engine.process_quotes(
        quotes={"2330": _quote(bid=1_095.0, ask=1_096.0, last=1_095.0)},
        now=_now(9, 10),
    )

    assert position["signed_shares"] == 0
    assert position["exit_price"] == 1_096.0
    assert position["exit_reason"] == "stop_loss_inside_daily_limit_1_tick_trigger"


def test_entry_only_fills_displayed_level_one_volume(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(0.3)],
        quotes={"2330": _quote(ask_volume=1.0)},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["requested_shares"] == 3_000
    assert position["filled_shares"] == 1_000
    signal = json.loads(engine.signals_path.read_text().splitlines()[-1])
    assert signal["status"] == "partial_depth"
    assert signal["top_book_capacity_shares"] == 1_000


def test_two_sided_live_signal_keeps_each_symbols_independent_fill(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    long_row = {**_row(0.5), "symbol": "2330"}
    short_row = {
        **_row(-0.5),
        "symbol": "2317",
        "name": "鴻海",
    }
    eligibility = {
        **_eligibility(),
        "2317": LiveEligibility(
            symbol="2317",
            venue="twse",
            security_type="stock",
            eligible=True,
            short_open=True,
            covered=True,
            source_date="2026-08-13",
        ),
    }

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[long_row, short_row],
        quotes={
            "2330": _quote(open_price=100.0, bid=99.0, ask=100.0, ask_volume=1.0),
            "2317": _quote(open_price=100.0, bid=100.0, ask=101.0, bid_volume=20.0),
        },
        eligibility=eligibility,
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    mode = engine.state["modes"][spec.market]
    by_symbol = {row["symbol"]: row for row in mode["positions"].values()}
    assert by_symbol["2330"]["filled_shares"] == 1_000
    assert by_symbol["2317"]["filled_shares"] == 20_000
    assert "execution_projection" not in mode


def test_two_sided_live_signal_keeps_executable_side_when_other_side_has_no_depth(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    eligibility = {
        **_eligibility(),
        "2317": LiveEligibility(
            symbol="2317",
            venue="twse",
            security_type="stock",
            eligible=True,
            short_open=True,
            covered=True,
            source_date="2026-08-13",
        ),
    }

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[
            {**_row(0.5), "symbol": "2330"},
            {**_row(-0.5), "symbol": "2317"},
        ],
        quotes={
            "2330": _quote(bid=99.0, ask=100.0, ask_volume=0.0),
            "2317": _quote(bid=100.0, ask=101.0, bid_volume=20.0),
        },
        eligibility=eligibility,
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    mode = engine.state["modes"][spec.market]
    by_symbol = {row["symbol"]: row for row in mode["positions"].values()}
    assert set(by_symbol) == {"2317"}
    assert by_symbol["2317"]["filled_shares"] == 5_000
    assert by_symbol["2317"]["signed_shares"] == -5_000
    assert "execution_projection" not in mode
    assert mode["engine_status"] == "active"


def test_entry_without_displayed_volume_fails_closed(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote()
    quote.pop("ask_volume")

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    assert not engine.state["modes"][spec.market]["positions"]
    signal = json.loads(engine.signals_path.read_text().splitlines()[-1])
    assert signal["reason"] == "marketable_depth_unavailable"


def test_second_same_day_signal_cannot_overwrite_an_open_position(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    result = engine.register_signal(
        spec=spec,
        summary=_summary("signal-2"),
        signal_rows=[_row(-0.1)],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1),
    )

    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert result == "blocked"
    assert position["signed_shares"] == 1_000
    assert position["signal_id"] == "signal-1"
    mode = engine.state["modes"][spec.market]
    assert mode["signal_id"] == "signal-1"
    assert mode["last_duplicate_signal_id"] == "signal-2"
    assert mode["last_duplicate_signal_reason"] == "daily_signal_already_consumed"
    assert mode["engine_status"] == "active"


def test_next_day_signal_blocks_when_prior_position_is_unflattened(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    next_day = datetime(2026, 8, 14, 9, 1, 7, tzinfo=TAIPEI)
    summary = _summary("signal-2")
    summary["generated_at"] = datetime(2026, 8, 14, 9, 0, 5, tzinfo=TAIPEI).isoformat()
    quote = _quote()
    quote["quote_at"] = datetime(2026, 8, 14, 9, 1, 6, tzinfo=TAIPEI).isoformat()

    result = engine.register_signal(
        spec=spec,
        summary=summary,
        signal_rows=[_row(-0.1)],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=next_day,
    )

    assert result == "blocked"
    assert (
        engine.state["modes"][spec.market]["engine_status"]
        == "critical_prior_position_unflattened"
    )


def test_only_one_daily_entry_is_allowed_after_the_first_is_closed(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    engine.process_quotes(
        quotes={"2330": _quote(bid=899.0, ask=900.0, last=900.0)},
        now=_now(10, 0),
    )

    result = engine.register_signal(
        spec=spec,
        summary=_summary("signal-2"),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(10, 1),
    )

    assert result == "blocked"
    mode = engine.state["modes"][spec.market]
    assert mode["signal_id"] == "signal-1"
    assert mode["last_duplicate_signal_id"] == "signal-2"
    assert mode["last_duplicate_signal_reason"] == "daily_signal_already_consumed"
    assert mode["engine_status"] == "session_flat_after_exit"


def test_positions_output_keeps_target_and_executed_layers_explicit(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    output = json.loads(engine.positions_path.read_text(encoding="utf-8"))
    mode = output["modes"][spec.market]
    assert mode["signal_id"] == "signal-1"
    assert mode["target_weights_path"].endswith("target_weights.parquet")
    assert mode["target_positions_path"].endswith("target_positions.md")
    assert mode["target_symbol_count"] == 1
    assert mode["target_risk"]["gross"] == 0.1
    assert len(mode["positions"]) == 1
    assert mode["positions"][0]["signed_shares"] == 1_000


def test_load_repairs_duplicate_signal_identity_from_event_ledger(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    engine.process_quotes(
        quotes={"2330": _quote(bid=899.0, ask=900.0, last=900.0)},
        now=_now(10, 0),
    )
    mode = engine.state["modes"][spec.market]
    mode["signal_id"] = "signal-2"
    engine._event(
        "signal_blocked",
        market=spec.market,
        signal_id="signal-2",
        reason="daily_signal_already_consumed",
    )
    engine._persist(_now(10, 1))

    restored = TwDayTradeSimulationEngine(engine.state_dir)

    restored_mode = restored.state["modes"][spec.market]
    assert restored_mode["signal_id"] == "signal-1"
    assert restored_mode["last_duplicate_signal_id"] == "signal-2"


def test_rearm_flat_session_preserves_audit_and_allows_replacement_signal(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    missing = LiveEligibility(
        symbol="2330",
        venue="twse",
        security_type="stock",
        eligible=False,
        short_open=False,
        covered=False,
        source_date="2026-08-12",
        reason="exact_session_eligibility_missing",
    )
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility={"2330": missing},
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    assert (
        engine.rearm_flat_session(
            spec.market,
            now=_now(9, 30),
            reason="official rules refreshed",
        )
        == "rearmed"
    )
    mode = engine.state["modes"][spec.market]
    assert mode["entry_completed_at"] is None
    assert mode["processed_signal_ids"] == ["signal-1"]

    replacement = _summary("signal-2")
    replacement["generated_at"] = _now(9, 30, 1).isoformat()
    quote = _quote()
    quote["quote_at"] = _now(9, 30, 2).isoformat()
    assert (
        engine.register_signal(
            spec=spec,
            summary=replacement,
            signal_rows=[_row()],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 30, 3),
        )
        == "registered"
    )
    assert next(iter(mode["positions"].values()))["entry_price"] == 1_000.0
    events = [json.loads(line) for line in engine.events_path.read_text().splitlines()]
    assert any(row["event"] == "flat_session_rearmed" for row in events)


def test_rearm_flat_session_refuses_any_position_history(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    engine.process_quotes(
        quotes={"2330": _quote(bid=899.0, ask=900.0, last=900.0)},
        now=_now(9, 10),
    )

    with pytest.raises(RuntimeError, match="position history"):
        engine.rearm_flat_session(
            spec.market,
            now=_now(9, 30),
            reason="must not double enter",
        )


def test_non_session_invalidation_voids_only_zero_fill_signal(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    missing = LiveEligibility(
        symbol="2330",
        venue="twse",
        security_type="stock",
        eligible=False,
        short_open=False,
        covered=False,
        source_date="2026-08-12",
        reason="exact_session_eligibility_missing",
    )
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility={"2330": missing},
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    assert (
        engine.invalidate_non_session_flat_signal(
            spec.market,
            now=_now(9, 30),
            reason="official calendar says closed",
        )
        == "invalidated"
    )
    mode = engine.state["modes"][spec.market]
    assert mode["entry_completed_at"] is None
    assert mode["session_valid"] is False
    assert mode["engine_status"] == "invalid_non_trading_session"
    assert mode["processed_signal_ids"] == ["signal-1"]
    engine.update_readiness([spec], now=_now(10, 0))
    assert mode["engine_status"] == "invalid_non_trading_session"
    events = [json.loads(line) for line in engine.events_path.read_text().splitlines()]
    assert events[-1]["event"] == "non_session_signal_invalidated"
    assert events[-1]["positions"] == 0
    assert events[-1]["fills"] == 0

    next_session = datetime(2026, 8, 14, 9, 1, 7, tzinfo=TAIPEI)
    replacement = _summary("signal-2")
    replacement["generated_at"] = datetime(
        2026, 8, 14, 9, 0, 5, tzinfo=TAIPEI
    ).isoformat()
    quote = _quote()
    quote["quote_at"] = datetime(2026, 8, 14, 9, 1, 6, tzinfo=TAIPEI).isoformat()
    assert (
        engine.register_signal(
            spec=spec,
            summary=replacement,
            signal_rows=[_row()],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=next_session,
        )
        == "registered"
    )
    assert mode["session_valid"] is True
    assert "non_session_invalidated_at" not in mode
    assert mode["blocked_reason"] is None


def test_non_session_invalidation_refuses_position_history(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    with pytest.raises(RuntimeError, match="has positions"):
        engine.invalidate_non_session_flat_signal(
            spec.market,
            now=_now(9, 30),
            reason="official calendar says closed",
        )


def test_retire_flat_mode_removes_only_state_and_keeps_audit_log(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(8, 30))

    assert (
        engine.retire_flat_mode(
            spec.market,
            now=_now(8, 31),
            reason="mistaken extra strategy",
        )
        == "retired"
    )
    assert spec.market not in engine.state["modes"]
    events = [json.loads(line) for line in engine.events_path.read_text().splitlines()]
    assert events[-1]["event"] == "simulation_mode_retired"
    assert events[-1]["market"] == spec.market


def test_lower_limit_is_local_stop_not_immediate_sell_limit(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    engine.process_quotes(
        quotes={"2330": _quote(bid=995.0, ask=996.0, last=995.0)},
        now=_now(9, 1),
    )

    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["signed_shares"] == 1_000
    assert position["stop_order_status"] == "armed_local_trigger"


def test_stop_trigger_is_idempotent_and_uses_executable_bid(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    stop_quote = _quote(bid=899.0, ask=900.0, last=900.0)
    engine.process_quotes(quotes={"2330": stop_quote}, now=_now(10, 0))
    engine.process_quotes(quotes={"2330": stop_quote}, now=_now(10, 1))

    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["signed_shares"] == 0
    assert position["exit_price"] == 899.0
    assert position["exit_reason"] == "stop_loss_full_price_limit_trigger"
    fills = [json.loads(line) for line in engine.fills_path.read_text().splitlines()]
    assert (
        sum(row["purpose"] == "stop_loss_full_price_limit_trigger" for row in fills)
        == 1
    )


def test_1320_passive_limit_then_1324_market_force_exit(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    engine.process_quotes(
        quotes={"2330": _quote(bid=1005.0, ask=1007.0, last=1006.0)},
        now=_now(13, 20),
    )
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["eod_limit_price"] == 1007.0
    assert position["signed_shares"] == 1_000

    engine.process_quotes(
        quotes={"2330": _quote(bid=1004.0, ask=1006.0, last=1005.0)},
        now=_now(13, 21),
    )
    assert position["eod_limit_price"] == 1006.0
    assert position["eod_limit_reprice_count"] == 1
    reprice_order = [
        json.loads(line)
        for line in engine.orders_path.read_text().splitlines()
        if json.loads(line)["purpose"] == "13_20_exit_limit_reprice"
    ][-1]
    assert reprice_order["previous_price"] == 1007.0
    assert reprice_order["price"] == 1006.0
    assert reprice_order["pricing_rule"] == "follow_passive_best_until_13_24"

    engine.process_quotes(
        quotes={"2330": _quote(bid=1004.0, ask=1006.0, last=1005.0)},
        now=_now(13, 24),
    )
    assert position["signed_shares"] == 0
    assert position["exit_price"] == 1004.0
    assert position["exit_reason"] == "13_24_market_force_exit"


def test_1320_missing_quote_places_limit_on_first_later_quote(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))

    engine.process_quotes(quotes={}, now=_now(13, 20))
    assert position["eod_limit_order_status"] == "not_submitted_no_quote"
    engine.process_quotes(
        quotes={"2330": _quote(bid=1004.0, ask=1006.0)},
        now=_now(13, 21),
    )

    assert position["eod_limit_order_status"] == "working"
    assert position["eod_limit_price"] == 1006.0
    assert position["eod_limit_submitted_at"] == _now(13, 21).isoformat(
        timespec="seconds"
    )


def test_1324_market_then_1325_limit_rod_and_1330_auction_fill(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(0.3)],
        quotes={"2330": _quote(ask_volume=3.0)},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))

    engine.process_quotes(
        quotes={"2330": _quote(bid=1004.0, ask=1006.0, bid_volume=1.0)},
        now=_now(13, 24),
    )
    assert position["signed_shares"] == 2_000
    assert position["status"] == "force_exit_partially_filled"
    mode = engine.state["modes"][spec.market]
    assert mode["force_exit_failures"] == 1
    assert mode["total_equity_twd"] - mode["initial_capital_twd"] == pytest.approx(
        mode["cumulative_realized_net_pnl_twd"] + mode["open_net_liquidation_pnl_twd"]
    )
    assert position["total_net_pnl_twd"] == pytest.approx(
        position["realized_net_pnl_twd"] + position["last_complete_net_pnl_twd"]
    )

    engine.update_readiness([spec], now=_now(14, 0))
    assert (
        engine.state["modes"][spec.market]["engine_status"]
        == "critical_unflattened_after_13_24"
    )

    engine.process_quotes(
        quotes={"2330": _quote(bid=1003.0, ask=1005.0, bid_volume=2.0)},
        now=_now(13, 25),
    )
    assert position["signed_shares"] == 2_000
    assert position["closing_auction_order_status"] == "working"

    engine.process_quotes(
        quotes={
            "2330": _quote(
                bid=1003.0,
                ask=1005.0,
                last=1004.0,
                bid_volume=2.0,
                minute_volume_lots=4.0,
            )
        },
        now=_now(13, 30),
    )
    assert position["signed_shares"] == 0
    assert position["status"] == "closed"
    assert position["total_net_pnl_twd"] == pytest.approx(
        position["realized_net_pnl_twd"]
    )
    assert mode["open_net_liquidation_pnl_twd"] == 0.0
    assert mode["total_equity_twd"] - mode["initial_capital_twd"] == pytest.approx(
        mode["cumulative_realized_net_pnl_twd"]
    )
    dashboard = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=_now(13, 30).astimezone(ZoneInfo("UTC")),
    )
    dashboard_position = dashboard["positions"][0]
    assert dashboard_position["unrealized_net_pnl_twd"] == 0.0
    assert dashboard_position["reconciled_total_net_pnl_twd"] == pytest.approx(
        dashboard_position["realized_net_pnl_twd"]
    )
    assert dashboard_position["pnl_reconciliation_difference_twd"] == pytest.approx(0.0)
    exit_fills = [
        row
        for row in (
            json.loads(line) for line in engine.fills_path.read_text().splitlines()
        )
        if row["purpose"] in {"13_24_market_force_exit", "13_30_closing_auction_fill"}
    ]
    assert [row["quantity"] for row in exit_fills] == [1_000, 2_000]


def test_1324_retries_market_until_1325_without_reusing_minute_capacity(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(0.3)],
        quotes={"2330": _quote(ask_volume=3.0)},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    marks_before = len(engine.marks_path.read_text().splitlines())

    force_quote = _quote(
        bid=1_004.0,
        ask=1_006.0,
        bid_volume=1.0,
        minute_volume_lots=4.0,
    )
    engine.process_quotes(quotes={"2330": force_quote}, now=_now(13, 24, 1))
    assert position["signed_shares"] == 2_000
    engine.process_quotes(
        quotes={"2330": force_quote},
        now=_now(13, 24, 20),
        append_mark_history=False,
    )
    assert position["signed_shares"] == 1_000
    engine.process_quotes(
        quotes={"2330": force_quote},
        now=_now(13, 24, 40),
        append_mark_history=False,
    )
    assert position["signed_shares"] == 1_000
    assert position["force_exit_minute_capacity_shares"] == 2_000
    assert position["force_exit_minute_consumed_shares"] == 2_000
    assert len(engine.marks_path.read_text().splitlines()) == marks_before + 1

    engine.process_quotes(quotes={"2330": force_quote}, now=_now(13, 25))
    assert position["closing_auction_order_status"] == "working"
    assert position["closing_auction_limit_price"] == 900.0
    auction_order = next(
        row
        for row in (
            json.loads(line) for line in engine.orders_path.read_text().splitlines()
        )
        if row["purpose"] == "13_25_closing_auction_force_exit"
    )
    assert auction_order["side"] == "sell"
    assert auction_order["price"] == 900.0
    assert auction_order["quantity"] == 1_000


def test_1325_short_cover_uses_upper_limit_in_call_auction(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(-0.1)],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))

    engine.process_quotes(
        quotes={
            "2330": _quote(
                bid=998.0,
                ask=1_000.0,
                bid_volume=0.0,
                ask_volume=0.0,
                minute_volume_lots=0.0,
            )
        },
        now=_now(13, 25),
    )

    assert position["signed_shares"] == -1_000
    assert position["closing_auction_order_status"] == "working"
    assert position["closing_auction_limit_price"] == 1_100.0
    auction_order = next(
        row
        for row in (
            json.loads(line) for line in engine.orders_path.read_text().splitlines()
        )
        if row["purpose"] == "13_25_closing_auction_force_exit"
    )
    assert auction_order["side"] == "buy_to_cover"
    assert auction_order["price"] == 1_100.0


@pytest.mark.parametrize("target_weight", [0.1, -0.1])
def test_1330_residual_is_terminally_flattened_without_exchange_fill_claim(
    tmp_path: Path,
    target_weight: float,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(target_weight)],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))

    engine.process_quotes(
        quotes={
            "2330": _quote(
                bid_volume=0.0,
                ask_volume=0.0,
                minute_volume_lots=0.0,
                last=1_000.0,
            )
        },
        now=_now(13, 30),
    )

    mode = engine.state["modes"][spec.market]
    assert position["signed_shares"] == 0
    assert position["exit_reason"] == "13_30_terminal_ledger_flatten"
    assert position["terminal_flatten_price_source"] == "observed_session_close"
    assert position["terminal_flatten_simulation_only"] is True
    assert position["closing_auction_order_status"] == ("terminal_ledger_flattened")
    assert mode["open_position_count"] == 0
    assert mode["force_exit_failures"] == 0
    assert mode["terminal_flatten_count"] == 1
    assert mode["engine_status"] == "session_flat_after_exit"
    terminal_fill = next(
        row
        for row in (
            json.loads(line) for line in engine.fills_path.read_text().splitlines()
        )
        if row["purpose"] == "13_30_terminal_ledger_flatten"
    )
    assert terminal_fill["synthetic_terminal_ledger"] is True
    assert terminal_fill["fill_contract"] == (
        "simulation_terminal_ledger_not_exchange_fill"
    )


def test_new_signal_resets_prior_session_closing_markers(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(8, 0))
    mode = engine.state["modes"][spec.market]
    prior = "2026-08-12T13:30:00+08:00"
    mode["closing_auction_submitted_at"] = prior
    mode["closing_auction_settled_at"] = prior
    mode["residual_conversion_completed_at"] = prior
    mode["force_exit_failures"] = 99

    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    mode = engine.state["modes"][spec.market]
    assert mode["closing_auction_submitted_at"] is None
    assert mode["closing_auction_settled_at"] is None
    assert mode["residual_conversion_completed_at"] is None
    assert mode["force_exit_failures"] == 0

    # Persisted stale markers must also be harmless for a process that loads
    # the current session after it has already accepted the signal.
    mode["closing_auction_submitted_at"] = prior
    mode["closing_auction_settled_at"] = prior
    mode["residual_conversion_completed_at"] = prior
    engine.process_quotes(
        quotes={
            "2330": _quote(
                bid_volume=0.0,
                ask_volume=0.0,
                minute_volume_lots=0.0,
            )
        },
        now=_now(13, 30),
    )
    assert mode["open_position_count"] == 0
    assert str(mode["closing_auction_submitted_at"]).startswith("2026-08-13")
    assert str(mode["closing_auction_settled_at"]).startswith("2026-08-13")
    assert str(mode["residual_conversion_completed_at"]).startswith("2026-08-13")


@pytest.mark.parametrize(
    ("target_weight", "expected_price"),
    [(0.1, 900.0), (-0.1, 1_100.0)],
)
def test_terminal_flatten_uses_adverse_limit_when_close_is_missing(
    tmp_path: Path,
    target_weight: float,
    expected_price: float,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(target_weight)],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))

    engine.process_quotes(
        quotes={
            "2330": _quote(
                bid_volume=0.0,
                ask_volume=0.0,
                minute_volume_lots=0.0,
                last=None,
            )
        },
        now=_now(13, 30),
    )

    mode = engine.state["modes"][spec.market]
    assert position["signed_shares"] == 0
    assert position["exit_price"] == expected_price
    assert position["terminal_flatten_price_source"] == ("adverse_daily_limit_fallback")
    assert mode["terminal_flatten_degraded_count"] == 1


def test_missing_mark_carries_same_position_equity_and_flags_stale(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )
    engine.process_quotes(
        quotes={"2330": _quote(bid=1005.0, ask=1006.0, last=1005.0)},
        now=_now(9, 1),
    )
    first = engine.state["modes"][spec.market]["total_equity_twd"]
    engine.process_quotes(quotes={}, now=_now(9, 2))
    mode = engine.state["modes"][spec.market]
    assert mode["total_equity_twd"] == first
    assert mode["stale_position_count"] == 1


def test_exact_session_eligibility_missing_fails_closed(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    rules = tmp_path / "rules"
    rules.mkdir()
    for name in ("twse", "tpex"):
        pl.DataFrame(
            {
                "證券代號": ["2330" if name == "twse" else "8069"],
                "暫停現股賣出後現款買進當沖註記": [""],
                "date": ["2026-08-12"],
            }
        ).write_parquet(rules / f"{name}_day_trade_eligibility.parquet")

    resolved, coverage = load_live_eligibility(
        rule_data_dir=rules,
        parquet_root=spec.parquet_root,
        symbols=["2330"],
        trading_date=date(2026, 8, 13),
    )

    assert not resolved["2330"].covered
    assert not resolved["2330"].eligible
    assert coverage["twse"]["latest_date"] == "2026-08-12"
    with pytest.raises(RuntimeError, match="target=2026-08-13 latest=2026-08-12"):
        require_exact_session_eligibility(
            rule_data_dir=rules,
            parquet_root=spec.parquet_root,
            trading_date=date(2026, 8, 13),
        )

    for path in rules.glob("*_day_trade_eligibility.parquet"):
        frame = pl.read_parquet(path).with_columns(pl.lit("2026-08-13").alias("date"))
        frame.write_parquet(path)
    coverage = require_exact_session_eligibility(
        rule_data_dir=rules,
        parquet_root=spec.parquet_root,
        trading_date=date(2026, 8, 13),
    )
    assert all(row["covered"] for row in coverage.values())


def test_quote_snapshot_exposes_bid_ask_and_computes_limits(tmp_path: Path) -> None:
    snapshot = PriceSnapshot(
        prices=np.asarray([100.0]),
        source="fixture",
        open_prices=np.asarray([98.0]),
        volumes=np.asarray([123.0]),
        bid_prices=np.asarray([99.9]),
        ask_prices=np.asarray([100.0]),
        bid_volumes=np.asarray([5.0]),
        ask_volumes=np.asarray([4.0]),
        reference_prices=np.asarray([100.0]),
        timestamps_ms=np.asarray([int(_now(9, 0, 1).timestamp() * 1000)]),
    )
    quotes = quote_map_from_snapshot(["2330"], snapshot, trading_date=date(2026, 8, 13))
    assert quotes["2330"]["bid"] == 99.9
    assert quotes["2330"]["ask"] == 100.0
    assert quotes["2330"]["open"] == 98.0
    assert quotes["2330"]["cumulative_volume_lots"] == 123.0
    assert quotes["2330"]["bid_volume"] == 5.0
    assert quotes["2330"]["ask_volume"] == 4.0
    assert quotes["2330"]["upper_limit"] == 110.0
    assert quotes["2330"]["lower_limit"] == 90.0


def test_percent_benchmarks_use_executable_books_costs_and_atomic_tx_roll(
    tmp_path: Path,
) -> None:
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    schedule = TaiwanFeeSchedule(
        commission_discount=0.2,
        minimum_commission=20.0,
        commission_rounding="half_up",
        tax_rounding="floor",
    )
    stock_quotes = {
        "0050": {
            "bid": 49.9,
            "ask": 50.0,
            "quote_at": _now(10, 0).isoformat(),
            "source": "fixture",
        },
        "2330": {
            "bid": 999.0,
            "ask": 1_000.0,
            "quote_at": _now(10, 0).isoformat(),
            "source": "fixture",
        },
    }
    engine.process_benchmarks(
        stock_quotes=stock_quotes,
        stock_fee_schedule=schedule,
        current_future_contract_code="TXFH6",
        current_future_quote={
            "bid": 45_000.0,
            "ask": 45_001.0,
            "quote_at": _now(10, 0).isoformat(),
            "source": "fixture",
        },
        now=_now(10, 0),
    )

    benchmarks = engine.state["benchmarks"]
    assert benchmarks["benchmark_0050"]["initial_capital_twd"] == 50_000.0
    assert benchmarks["benchmark_0050"]["fixed_fees_twd"] == 20.0
    assert benchmarks["benchmark_2330"]["initial_capital_twd"] == 1_000_000.0
    tx = benchmarks["benchmark_tx_continuous"]
    assert tx["initial_capital_twd"] == 701_000.0
    assert tx["contract_code"] == "TXFH6"
    assert tx["fixed_fees_twd"] == 60.0
    assert tx["return_pct"] == pytest.approx(
        tx["net_pnl_twd"] / tx["initial_capital_twd"] * 100.0
    )

    # A changed Contract V2 target is not spliced until both executable legs
    # coexist: old contract bid to close and new contract ask to reopen.
    engine.process_benchmarks(
        stock_quotes=stock_quotes,
        stock_fee_schedule=schedule,
        current_future_contract_code="TXFI6",
        current_future_quote={
            "bid": 45_199.0,
            "ask": 45_200.0,
            "quote_at": _now(10, 1).isoformat(),
            "source": "fixture",
        },
        previous_future_quote={},
        now=_now(10, 1),
    )
    assert tx["contract_code"] == "TXFH6"
    assert tx["roll_count"] == 0
    assert tx["valuation_source"] == "roll_waiting_for_old_bid_and_new_ask"

    engine.process_benchmarks(
        stock_quotes=stock_quotes,
        stock_fee_schedule=schedule,
        current_future_contract_code="TXFI6",
        current_future_quote={
            "bid": 45_199.0,
            "ask": 45_200.0,
            "quote_at": _now(10, 2).isoformat(),
            "source": "fixture",
        },
        previous_future_quote={"bid": 45_150.0},
        now=_now(10, 2),
    )
    assert tx["contract_code"] == "TXFI6"
    assert tx["roll_count"] == 1
    assert tx["fixed_fees_twd"] == 180.0
    assert tx["last_roll_old_bid"] == 45_150.0
    assert tx["last_roll_new_ask"] == 45_200.0
    assert len(tx["roll_history"]) == 1
    assert tx["roll_history"][0] == {
        "rolled_at": _now(10, 2).isoformat(),
        "from_contract": "TXFH6",
        "to_contract": "TXFI6",
        "old_bid": 45_150.0,
        "old_exit_price": 45_150.0,
        "old_exit_price_source": "executable_old_contract_bid",
        "official_final_settlement": None,
        "new_ask": 45_200.0,
        "old_exit_fee_twd": 60.0,
        "new_entry_fee_twd": 60.0,
    }
    payload = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=_now(10, 2).astimezone(ZoneInfo("UTC")),
    )
    assert len(payload["benchmarks"]) == 3
    assert len(payload["benchmark_marks"]) == 9
    assert all(
        row["return_pct"]
        == pytest.approx(
            (row["total_equity_twd"] / row["initial_capital_twd"] - 1.0) * 100.0
        )
        for row in payload["benchmark_marks"]
        if row["total_equity_twd"] is not None
    )
    assert "own capital basis" in payload["source_contract"]["comparison"]


def test_expired_tx_continuous_roll_uses_official_settlement_not_zero(
    tmp_path: Path,
) -> None:
    settlement_path = tmp_path / "tx_final_settlement.parquet"
    pl.DataFrame(
        {
            "settlement_date": [date(2026, 8, 19)],
            "option_series": ["202608"],
            "final_settlement_price": [45_123],
            "source_file": ["official-fsp.html"],
            "source_sha256": ["a" * 64],
            "source_url": ["https://www.taifex.com.tw/cht/5/futIndxFSP"],
        }
    ).write_parquet(settlement_path)
    engine = TwDayTradeSimulationEngine(
        tmp_path / "state",
        final_settlement_path=settlement_path,
    )
    schedule = TaiwanFeeSchedule()
    entry_time = datetime(2026, 8, 19, 10, 0, tzinfo=TAIPEI)
    engine.process_benchmarks(
        stock_quotes={},
        stock_fee_schedule=schedule,
        current_future_contract_code="TXFH6",
        current_future_quote={
            "bid": 45_000.0,
            "ask": 45_001.0,
            "delivery_month": "202608",
            "last_trading_date": "2026-08-19",
            "quote_at": entry_time.isoformat(),
            "source": "fixture",
        },
        now=entry_time,
    )

    # The expiring TX month stops trading at 13:30 on its last trading day.
    # Its same-calendar-day night session already belongs to the next month,
    # so the old leg must use the official FSP without waiting for midnight.
    roll_time = datetime(2026, 8, 19, 15, 0, tzinfo=TAIPEI)
    engine.process_benchmarks(
        stock_quotes={},
        stock_fee_schedule=schedule,
        current_future_contract_code="TXFI6",
        current_future_quote={
            "bid": 45_199.0,
            "ask": 45_200.0,
            "delivery_month": "202609",
            "last_trading_date": "2026-09-16",
            "quote_at": roll_time.isoformat(),
            "source": "fixture",
        },
        previous_future_quote={
            "bid": 0.0,
            "delivery_month": "202608",
            "last_trading_date": "2026-08-19",
        },
        now=roll_time,
    )

    row = engine.state["benchmarks"]["benchmark_tx_continuous"]
    assert row["contract_code"] == "TXFI6"
    assert row["entry_price"] == 45_200.0
    assert row["roll_count"] == 1
    assert row["last_roll_old_bid"] is None
    assert row["last_roll_old_price"] == 45_123.0
    assert row["last_roll_official_final_settlement"] == 45_123.0
    assert row["last_roll_old_price_source"] == (
        "official_taifex_index_final_settlement"
    )
    assert row["realized_gross_pnl_twd"] == pytest.approx((45_123.0 - 45_001.0) * 200.0)
    # The expiry event has no fabricated sell fee; only the new-month entry
    # adds one side after the original entry fee.
    assert row["fixed_fees_twd"] == 120.0
    assert row["roll_history"][0]["official_final_settlement"]["price"] == 45_123.0
    assert row["net_pnl_twd"] > -100_000.0


def test_expired_tx_roll_fails_closed_when_official_settlement_is_missing(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "missing.parquet"
    engine = TwDayTradeSimulationEngine(
        tmp_path / "state",
        final_settlement_path=missing_path,
    )
    schedule = TaiwanFeeSchedule()
    entry_time = datetime(2026, 8, 19, 10, 0, tzinfo=TAIPEI)
    engine.process_benchmarks(
        stock_quotes={},
        stock_fee_schedule=schedule,
        current_future_contract_code="TXFH6",
        current_future_quote={
            "bid": 45_000.0,
            "ask": 45_001.0,
            "delivery_month": "202608",
            "last_trading_date": "2026-08-19",
        },
        now=entry_time,
    )
    before = dict(engine.state["benchmarks"]["benchmark_tx_continuous"])

    engine.process_benchmarks(
        stock_quotes={},
        stock_fee_schedule=schedule,
        current_future_contract_code="TXFI6",
        current_future_quote={"bid": 45_199.0, "ask": 45_200.0},
        previous_future_quote={"bid": 0.0},
        now=datetime(2026, 8, 20, 9, 0, tzinfo=TAIPEI),
    )

    row = engine.state["benchmarks"]["benchmark_tx_continuous"]
    assert row["contract_code"] == "TXFH6"
    assert row["roll_count"] == 0
    assert row["realized_gross_pnl_twd"] == 0.0
    assert row["total_equity_twd"] == before["total_equity_twd"]
    assert row["valuation_stale"] is True
    assert row["valuation_source"] == (
        "roll_waiting_for_official_final_settlement_and_new_ask"
    )
    assert str(row["roll_blocked_reason"]).startswith(
        "missing_official_final_settlement_file:"
    )


def test_tx_benchmark_rebase_keeps_immutable_origin_across_contract_roll() -> None:
    source = {
        "benchmark_id": "benchmark_tx_continuous",
        "instrument_type": "continuous_long_future",
        "entry_at": "2026-08-13T10:00:00+08:00",
        "entry_price": 45_200.0,
        "origin_entry_at": "2026-08-13T10:00:00+08:00",
        "origin_entry_price": 45_000.0,
        "current_contract_entry_price": 45_200.0,
        "roll_count": 1,
        "net_pnl_twd": 1_000.0,
        "fixed_fees_twd": 180.0,
        "transaction_tax_twd": 540.0,
        "last_mark_price": 45_210.0,
        "valuation_source": "official_settlement_roll",
    }
    origin = {
        "session_date": "2026-08-13",
        "entry_at": "2026-08-13T08:45:00+08:00",
        "entry_price": 44_900.0,
        "initial_capital_twd": 701_000.0,
        "initial_fixed_fees_twd": 60.0,
        "initial_transaction_tax_twd": 180.0,
        "gross_pnl_multiplier": 200.0,
        "live_origin": {
            "entry_at": "2026-08-13T10:00:00+08:00",
            "entry_price": 45_000.0,
            "initial_fixed_fees_twd": 60.0,
            "initial_transaction_tax_twd": 180.0,
        },
    }

    row = _rebase_live_benchmark(source, origin)

    assert row["benchmark_origin_rebased"] is True
    assert row["entry_price"] == 44_900.0
    assert row["current_contract_entry_price"] == 45_200.0
    assert row["net_pnl_twd"] == 21_000.0
    assert row["total_equity_twd"] == 722_000.0
    assert row.get("benchmark_origin_error") is None


def test_tx_benchmark_rebase_uses_audited_roll_offset_without_calendar_gap() -> None:
    source = {
        "benchmark_id": "benchmark_tx_continuous",
        "instrument_type": "continuous_long_future",
        "entry_at": "2026-08-20T12:18:43+08:00",
        "entry_price": 44_806.0,
        "origin_entry_at": "2026-08-20T12:18:43+08:00",
        "origin_entry_price": 44_806.0,
        "net_pnl_twd": 1_000.0,
        "fixed_fees_twd": 60.0,
        "transaction_tax_twd": 180.0,
        "last_mark_price": 44_810.0,
        "valuation_source": "live_book",
    }
    origin = {
        "session_date": "2026-08-13",
        "entry_at": "2026-08-13T08:45:00+08:00",
        "entry_price": 45_350.0,
        "initial_capital_twd": 701_000.0,
        "initial_fixed_fees_twd": 60.0,
        "initial_transaction_tax_twd": 180.0,
        "gross_pnl_multiplier": 200.0,
        "live_net_pnl_offset_twd": -2_500.0,
        "fixed_fees_twd_to_live_origin": 120.0,
        "transaction_tax_twd_to_live_origin": 520.0,
        "live_origin": {
            "entry_at": "2026-08-20T12:18:43+08:00",
            "entry_price": 44_806.0,
            "initial_fixed_fees_twd": 60.0,
            "initial_transaction_tax_twd": 180.0,
        },
    }

    row = _rebase_live_benchmark(source, origin)

    assert row["net_pnl_twd"] == -1_500.0
    assert row["total_equity_twd"] == 699_500.0
    assert row["fixed_fees_twd"] == 120.0
    assert row["transaction_tax_twd"] == 520.0


def test_stock_benchmark_rebase_hides_incomplete_total_return_marks() -> None:
    row = _rebase_live_benchmark(
        {
            "benchmark_id": "benchmark_0050",
            "instrument_type": "stock_buy_and_hold",
            "total_return_contract": "official_ex_date_reference_reinvestment_v1",
            "valuation_source": ("corporate_action_reference_unavailable_fail_closed"),
            "valuation_stale": True,
            "initial_capital_twd": 100_000.0,
            "total_equity_twd": 101_000.0,
            "net_pnl_twd": 1_000.0,
            "return_fraction": 0.01,
            "return_pct": 1.0,
        },
        None,
    )

    assert row["total_equity_twd"] is None
    assert row["net_pnl_twd"] is None
    assert row["return_fraction"] is None
    assert row["return_pct"] is None
    assert row["benchmark_origin_error"] == "total_return_source_incomplete"


def test_stock_benchmark_reinvests_official_actions_once(tmp_path: Path) -> None:
    reference = tmp_path / "tw_corporate_action_reference.parquet"
    pl.DataFrame(
        {
            "date": [date(2026, 8, 14), date(2026, 8, 15)],
            "symbol": ["0050", "0050"],
            "previous_close": [100.0, 95.0],
            "reference_price": [95.0, 47.5],
            "event_type": ["息", "權"],
        }
    ).write_parquet(reference)
    reference.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "failure_count": 0,
                "end_date": "2026-08-15",
            }
        ),
        encoding="utf-8",
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    schedule = TaiwanFeeSchedule(
        commission_discount=0.2,
        minimum_commission=20.0,
        commission_rounding="half_up",
        tax_rounding="floor",
    )

    def process(observed: datetime, stock_price: float) -> None:
        engine.process_benchmarks(
            stock_quotes={
                "0050": {
                    "bid": stock_price,
                    "ask": stock_price,
                    "quote_at": observed.isoformat(),
                    "source": "fixture",
                },
                "2330": {
                    "bid": 1_000.0,
                    "ask": 1_000.0,
                    "quote_at": observed.isoformat(),
                    "source": "fixture",
                },
            },
            stock_fee_schedule=schedule,
            current_future_contract_code="TXFH6",
            current_future_quote={
                "bid": 45_000.0,
                "ask": 45_000.0,
                "quote_at": observed.isoformat(),
                "source": "fixture",
            },
            corporate_action_reference_path=reference,
            now=observed,
        )

    process(datetime(2026, 8, 13, 10, 0, tzinfo=TAIPEI), 100.0)
    process(datetime(2026, 8, 15, 10, 0, tzinfo=TAIPEI), 47.5)
    row = engine.state["benchmarks"]["benchmark_0050"]
    assert row["corporate_action_factor"] == pytest.approx(100.0 / 47.5)
    assert row["adjusted_quantity"] == pytest.approx(1_000 * 100.0 / 47.5)
    assert row["corporate_action_count"] == 2
    assert row["last_corporate_action_date"] == "2026-08-15"
    assert row["corporate_action_coverage"] is True
    assert row["total_return_contract"] == (
        "official_ex_date_reference_reinvestment_v1"
    )
    first_total = row["total_equity_twd"]

    process(datetime(2026, 8, 15, 10, 1, tzinfo=TAIPEI), 47.5)
    assert row["corporate_action_count"] == 2
    assert row["corporate_action_factor"] == pytest.approx(100.0 / 47.5)
    assert row["total_equity_twd"] == pytest.approx(first_total)


def test_stock_benchmark_bridges_one_live_session_with_official_reference(
    tmp_path: Path,
) -> None:
    reference = tmp_path / "tw_corporate_action_reference.parquet"
    pl.DataFrame(
        {
            "date": [date(2026, 8, 14)],
            "symbol": ["0050"],
            "previous_close": [100.0],
            "reference_price": [95.0],
            "event_type": ["息"],
        }
    ).write_parquet(reference)
    reference.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "failure_count": 0,
                "end_date": "2026-08-14",
            }
        ),
        encoding="utf-8",
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    schedule = TaiwanFeeSchedule()
    entry_time = datetime(2026, 8, 13, 10, 0, tzinfo=TAIPEI)
    mark_time = datetime(2026, 8, 17, 10, 0, tzinfo=TAIPEI)

    engine.process_benchmarks(
        stock_quotes={
            "0050": {
                "bid": 100.0,
                "ask": 100.0,
                "quote_at": entry_time.isoformat(),
                "source": "fixture",
            }
        },
        stock_fee_schedule=schedule,
        current_future_contract_code=None,
        current_future_quote={},
        corporate_action_reference_path=reference,
        now=entry_time,
    )
    live_quote = {
        "bid": 47.5,
        "ask": 47.6,
        "reference_price": 47.5,
        "reference_price_source": "twse_tpex:mis_static_limits",
        "previous_close": 95.0,
        "previous_close_date": "2026-08-14",
        "previous_close_source": "twse_official:0050_features.parquet",
        "quote_at": mark_time.isoformat(),
        "source": "fixture",
    }
    engine.process_benchmarks(
        stock_quotes={"0050": live_quote},
        stock_fee_schedule=schedule,
        current_future_contract_code=None,
        current_future_quote={},
        corporate_action_reference_path=reference,
        now=mark_time,
    )

    row = engine.state["benchmarks"]["benchmark_0050"]
    assert row["label"] == "0050 元大台灣50（含息）"
    assert row["return_type"] == "total_return"
    assert row["corporate_action_factor"] == pytest.approx(100.0 / 47.5)
    assert row["adjusted_quantity"] == pytest.approx(1_000 * 100.0 / 47.5)
    assert row["corporate_action_count"] == 2
    assert row["corporate_action_coverage"] is True
    assert row["corporate_action_coverage_end"] == "2026-08-17"
    assert row["corporate_action_status"] == (
        "official_reference_complete_with_current_session_reference"
    )
    assert row["valuation_stale"] is False
    assert row["current_session_reference_price"] == 47.5
    assert row["previous_official_close"] == 95.0
    first_total = row["total_equity_twd"]

    engine.process_benchmarks(
        stock_quotes={"0050": live_quote},
        stock_fee_schedule=schedule,
        current_future_contract_code=None,
        current_future_quote={},
        corporate_action_reference_path=reference,
        now=mark_time.replace(minute=1),
    )
    assert row["corporate_action_count"] == 2
    assert row["corporate_action_factor"] == pytest.approx(100.0 / 47.5)
    assert row["total_equity_twd"] == pytest.approx(first_total)


def test_stock_benchmark_rejects_noncontiguous_live_reference_bridge(
    tmp_path: Path,
) -> None:
    reference = tmp_path / "tw_corporate_action_reference.parquet"
    pl.DataFrame(
        {
            "date": [date(2026, 8, 13)],
            "symbol": ["0050"],
            "previous_close": [100.0],
            "reference_price": [95.0],
            "event_type": ["息"],
        }
    ).write_parquet(reference)
    reference.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "failure_count": 0,
                "end_date": "2026-08-13",
            }
        ),
        encoding="utf-8",
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine._load_corporate_actions(reference)

    factor, actions, status = engine._stock_total_return_adjustment(
        symbol="0050",
        entry_at="2026-08-12T09:00:00+08:00",
        mark_date=date(2026, 8, 17),
        current_reference_price=95.0,
        previous_close=100.0,
        previous_close_date="2026-08-14",
    )

    assert factor is None
    assert actions == []
    assert status == "current_session_previous_close_not_contiguous"


def test_same_session_stock_benchmark_does_not_require_future_close_receipt(
    tmp_path: Path,
) -> None:
    reference = tmp_path / "tw_corporate_action_reference.parquet"
    pl.DataFrame(
        {
            "date": [date(2026, 8, 14)],
            "symbol": ["0050"],
            "previous_close": [100.0],
            "reference_price": [95.0],
            "event_type": ["息"],
        }
    ).write_parquet(reference)
    reference.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "failure_count": 0,
                "end_date": "2026-08-14",
            }
        ),
        encoding="utf-8",
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    observed = datetime(2026, 8, 17, 10, 0, tzinfo=TAIPEI)
    engine.process_benchmarks(
        stock_quotes={
            "0050": {
                "bid": 100.0,
                "ask": 100.1,
                "quote_at": observed.isoformat(),
                "source": "fixture",
            },
            "2330": {
                "bid": 1_000.0,
                "ask": 1_001.0,
                "quote_at": observed.isoformat(),
                "source": "fixture",
            },
        },
        stock_fee_schedule=TaiwanFeeSchedule(),
        current_future_contract_code="TXFH6",
        current_future_quote={
            "bid": 45_000.0,
            "ask": 45_001.0,
            "quote_at": observed.isoformat(),
            "source": "fixture",
        },
        corporate_action_reference_path=reference,
        now=observed,
    )

    for benchmark_id in ("benchmark_0050", "benchmark_2330"):
        row = engine.state["benchmarks"][benchmark_id]
        assert row["corporate_action_status"] == "same_session_no_action_boundary"
        assert row["corporate_action_factor"] == 1.0
        assert row["last_mark_price"] is not None
        assert row["return_pct"] is not None
        assert row["valuation_stale"] is False


def test_dashboard_history_ranges_anchor_to_latest_retained_mark(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    root.mkdir()
    rows = [
        {
            "market": "tw_day_trade",
            "minute": "2026-08-13T10:00+08:00",
            "initial_capital_twd": 100.0,
            "total_equity_twd": 101.0,
        },
        {
            "market": "tw_day_trade",
            "minute": "2026-08-14T09:30+08:00",
            "initial_capital_twd": 100.0,
            "total_equity_twd": 102.0,
        },
        {
            "market": "tw_day_trade",
            "minute": "2026-08-14T10:00+08:00",
            "initial_capital_twd": 100.0,
            "total_equity_twd": 103.0,
        },
    ]
    (root / "marks.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )

    one_hour = build_dashboard_history_snapshot(state_dir=root, range_key="1h")
    all_time = build_dashboard_history_snapshot(state_dir=root, range_key="all")
    selected_day = build_dashboard_history_snapshot(
        state_dir=root,
        range_key="all",
        start_date="2026-08-14",
        end_date="2026-08-14",
    )

    assert one_hour["range"] == "1h"
    assert one_hour["raw_points_in_range"] == 2
    assert [row["return_pct"] for row in one_hour["history"]] == pytest.approx(
        [2.0, 3.0]
    )
    assert all_time["raw_points_in_range"] == 3
    assert selected_day["start_date"] == "2026-08-14"
    assert selected_day["end_date"] == "2026-08-14"
    assert selected_day["available_start_date"] == "2026-08-13"
    assert selected_day["available_end_date"] == "2026-08-14"
    assert selected_day["raw_points_in_range"] == 2
    assert [row["return_pct"] for row in selected_day["history"]] == pytest.approx(
        [102.0 / 101.0 * 100.0 - 100.0, 103.0 / 101.0 * 100.0 - 100.0]
    )
    assert selected_day["curve_granularity"] == "1m"
    assert selected_day["return_basis"] == (
        "previous_retained_mark_before_start_else_initial_capital"
    )
    assert len(selected_day["range_summary"]) == 1
    summary = selected_day["range_summary"][0]
    assert summary["series_id"] == "tw_day_trade"
    assert summary["baseline_kind"] == "previous_retained_mark"
    assert summary["baseline_equity_twd"] == 101.0
    assert summary["end_equity_twd"] == 103.0
    assert summary["range_net_pnl_twd"] == 2.0
    assert summary["point_count"] == 2
    assert summary["expected_minute_points"] == 270
    assert selected_day["expected_strategy_session_points_from_09_01"] == 270


def test_dashboard_history_keeps_leveraged_reference_below_zero(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "benchmark_history.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "origins": {},
                "marks": [
                    {
                        "benchmark_id": "benchmark_tx_continuous",
                        "session_date": "2026-03-09",
                        "minute": "2026-03-09T08:45+08:00",
                        "initial_capital_twd": 100.0,
                        "total_equity_twd": -10.0,
                    },
                    {
                        "benchmark_id": "benchmark_tx_continuous",
                        "session_date": "2026-03-09",
                        "minute": "2026-03-09T08:46+08:00",
                        "initial_capital_twd": 100.0,
                        "total_equity_twd": -5.0,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    result = build_dashboard_history_snapshot(
        state_dir=root,
        range_key="all",
        start_date="2026-03-09",
        end_date="2026-03-09",
    )

    assert result["raw_points_in_range"] == 2
    assert [row["return_pct"] for row in result["history"]] == pytest.approx(
        [-110.0, -105.0]
    )


def test_dashboard_merges_actual_open_benchmark_history_and_rebases_live_marks(
    tmp_path: Path,
) -> None:
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    schedule = TaiwanFeeSchedule(
        commission_discount=0.2,
        minimum_commission=20.0,
        commission_rounding="half_up",
        tax_rounding="floor",
    )
    now_14 = datetime(2026, 8, 14, 10, 0, tzinfo=TAIPEI)
    engine.process_benchmarks(
        stock_quotes={
            "0050": {
                "bid": 49.9,
                "ask": 50.0,
                "quote_at": now_14.isoformat(),
                "source": "fixture",
            },
            "2330": {
                "bid": 999.0,
                "ask": 1_000.0,
                "quote_at": now_14.isoformat(),
                "source": "fixture",
            },
        },
        stock_fee_schedule=schedule,
        current_future_contract_code="TXFH6",
        current_future_quote={
            "bid": 45_000.0,
            "ask": 45_001.0,
            "quote_at": now_14.isoformat(),
            "source": "fixture",
        },
        now=now_14,
    )
    live = engine.state["benchmarks"]
    day_13_marks = []
    canonical_entries = {
        "benchmark_0050": (49.0, 49_000.0, 20.0, 0.0, 1_000.0),
        "benchmark_2330": (990.0, 990_000.0, 20.0, 0.0, 1_000.0),
        "benchmark_tx_continuous": (
            44_900.0,
            701_000.0,
            60.0,
            live["benchmark_tx_continuous"]["transaction_tax_twd"],
            200.0,
        ),
    }
    origins = {}
    for benchmark_id, row in live.items():
        entry, capital, entry_fee, entry_tax, multiplier = canonical_entries[
            benchmark_id
        ]
        origin_at = datetime(
            2026,
            8,
            13,
            8 if benchmark_id == "benchmark_tx_continuous" else 9,
            45 if benchmark_id == "benchmark_tx_continuous" else 0,
            tzinfo=TAIPEI,
        )
        origins[benchmark_id] = {
            "session_date": "2026-08-13",
            "entry_at": origin_at.isoformat(),
            "entry_price": entry,
            "initial_capital_twd": capital,
            "initial_fixed_fees_twd": entry_fee,
            "initial_transaction_tax_twd": entry_tax,
            "gross_pnl_multiplier": multiplier,
            "live_origin": {
                "entry_at": row["entry_at"],
                "entry_price": row["entry_price"],
                "initial_fixed_fees_twd": row["fixed_fees_twd"],
                "initial_transaction_tax_twd": row.get("transaction_tax_twd", 0.0),
            },
        }
        day_13_marks.append(
            {
                "session_date": "2026-08-13",
                "recorded_at": origin_at.isoformat(),
                "minute": origin_at.isoformat(timespec="minutes"),
                "benchmark_id": benchmark_id,
                "label": row["label"],
                "entry_at": origin_at.isoformat(),
                "entry_price": entry,
                "initial_capital_twd": capital,
                "last_mark_price": entry,
                "net_pnl_twd": -100.0,
                "total_equity_twd": capital - 100.0,
                "valuation_stale": False,
                "benchmark_origin_rebased": True,
                "counterfactual_open_replay": True,
            }
        )
    (engine.state_dir / "benchmark_history.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "origins": origins,
                "marks": day_13_marks,
            }
        ),
        encoding="utf-8",
    )

    historical = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        session_date="2026-08-13",
        now=now_14.astimezone(ZoneInfo("UTC")),
    )
    assert len(historical["benchmark_marks"]) == 3
    assert all(
        row["entry_at"].startswith("2026-08-13") for row in historical["benchmarks"]
    )

    current = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        session_date="2026-08-14",
        now=now_14.astimezone(ZoneInfo("UTC")),
    )
    by_id = {row["benchmark_id"]: row for row in current["benchmarks"]}
    assert by_id["benchmark_0050"]["entry_price"] == 49.0
    assert by_id["benchmark_0050"]["net_pnl_twd"] == pytest.approx(
        live["benchmark_0050"]["net_pnl_twd"] + 1_000.0
    )
    assert by_id["benchmark_tx_continuous"]["entry_price"] == 44_900.0
    assert by_id["benchmark_tx_continuous"]["net_pnl_twd"] == pytest.approx(
        live["benchmark_tx_continuous"]["net_pnl_twd"] + 20_200.0
    )
    assert all(row["benchmark_origin_rebased"] for row in current["benchmark_marks"])
    assert current["record_counts"]["benchmark_history_marks"] == 3


def test_dashboard_contains_all_sources_without_broker_secrets(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(8, 59))
    payload = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=_now(8, 59).astimezone(ZoneInfo("UTC")),
    )
    assert payload["simulation_only"] is True
    assert payload["production_order_possible"] is False
    assert "live execution starts at 09:00" in payload["source_contract"]["entry_fill"]
    assert (
        "observed 09:01 minute VWAP"
        in payload["source_contract"]["entry_fill"]
    )
    assert (
        "without open-price fill"
        in payload["source_contract"]["entry_fill"]
    )
    assert "live entry quantity is bounded" in payload["source_contract"]["depth_limit"]
    assert (
        "Missed-opening replay uses the official open only for sizing"
        in payload["source_contract"]["depth_limit"]
    )
    assert payload["signals"] == []
    assert payload["payload_window"]["signals"] == 0
    assert "account" not in json.dumps(payload).casefold()
    assert "broker" not in json.dumps(payload).casefold()


def test_dashboard_exposes_same_day_preopen_progress_and_measured_speed(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(8, 29))
    readiness_path = tmp_path / "preopen_readiness.json"
    readiness_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_id": "discord-process-1",
                "updated_at": _now(8, 31).isoformat(),
                "markets": {
                    "tw_day_trade": {
                        "status": "ready",
                        "started_at": _now(8, 30).isoformat(),
                        "completed_at": _now(8, 30, 30).isoformat(),
                        "elapsed_seconds": 30.0,
                        "panel_date": "2026-08-12 13:30:00",
                        "symbol_count": 3_000,
                        "live_latency": {
                            "model_inference_ms": 500.0,
                            "compute_before_publish_ms": 12_000.0,
                        },
                        "preopen_price_limits": {
                            "prepared_count": 2_400,
                            "requested_count": 3_000,
                            "missing_count": 600,
                        },
                        "same_session_eligibility": {
                            "target_date": "2026-08-13",
                            "venues": {
                                "twse": {
                                    "covered": True,
                                    "target_date": "2026-08-13",
                                    "latest_date": "2026-08-13",
                                },
                                "tpex": {
                                    "covered": True,
                                    "target_date": "2026-08-13",
                                    "latest_date": "2026-08-13",
                                },
                            },
                        },
                        "final_arm": {
                            "status": "ready",
                            "run_id": "discord-process-1",
                            "completed_at": _now(8, 55).isoformat(),
                            "live_latency": {
                                "panel_cache_hit": True,
                                "checkpoint_cache_hit": True,
                                "model_cache_hit": True,
                            },
                            "quote_prewarm": {
                                "ready": True,
                                "run_id": "discord-process-1",
                                "connection_scope": "process",
                                "requested_count": 3_000,
                                "primed_count": 3_000,
                                "resolved_count": 3_000,
                                "missing_count": 0,
                            },
                        },
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (engine.state_dir / "preopen_readiness.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "session_date": "2026-08-13",
                "status": "ready",
                "updated_at": _now(8, 55).isoformat(),
                "components": {
                    "eligibility": {
                        "status": "ready",
                        "checked_at": _now(8, 30).isoformat(),
                        "elapsed_ms": 30.0,
                        "details": {
                            "proof": "exact_session_twse_tpex_coverage",
                            "symbol_count": 3_000,
                        },
                    },
                    "shioaji_quote": {
                        "status": "ready",
                        "checked_at": _now(8, 55).isoformat(),
                        "elapsed_ms": 50.0,
                        "details": {"proof": "simulation_client_usage_probe"},
                    },
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        preopen_readiness_path=readiness_path,
        now=_now(8, 59).astimezone(ZoneInfo("UTC")),
    )

    assert payload["schema_version"] == 5
    assert payload["preopen"]["status"] == "ready"
    assert payload["preopen"]["ready_count"] == 1
    assert payload["preopen"]["progress_ratio"] == 1.0
    assert payload["preopen"]["wall_elapsed_seconds"] == 30.0
    mode = payload["preopen"]["markets"][0]
    assert mode["symbols_per_second"] == 100.0
    assert mode["model_symbols_per_second"] == 6_000.0
    assert mode["price_limit_coverage_ratio"] == 0.8
    assert mode["eligibility_ready"] is True
    assert mode["final_arm_hot_ready"] is True
    assert mode["eligibility_target_date"] == "2026-08-13"
    assert payload["preopen"]["simulation"]["ready"] is True
    assert (
        payload["preopen"]["simulation"]["components"]["shioaji_quote"]["proof"]
        == "simulation_client_usage_probe"
    )
    assert payload["session_progress"]["phase"] == "preopen"


def test_dashboard_projects_unattended_guardian_without_internal_actions(
    tmp_path: Path,
) -> None:
    receipt = tmp_path / "guardian.json"
    receipt.write_text(
        json.dumps(
            {
                "status": "ready",
                "ready": True,
                "observed_at_taipei": _now(8, 45).isoformat(),
                "simulation_only": True,
                "production_order_possible": False,
                "failures": [],
                "warnings": [],
                "actions": [{"command": ["systemctl", "enable", "secret.service"]}],
                "components": {
                    "time_sync": {"ready": True},
                    "source_events": {"ready": True},
                    "runtime_sync": {"ready": True},
                    "public_dashboard": {"ready": True},
                    "post_close_flat": {"ready": True},
                    "disks": {"repository": {"ready": True}},
                },
            }
        ),
        encoding="utf-8",
    )

    projected = dashboard_module._unattended_guardian_status(
        path=receipt, observed=_now(8, 45)
    )

    assert projected["ready"] is True
    assert projected["action_count"] == 1
    assert projected["components"] == {
        "time_sync": True,
        "source_events": True,
        "runtime_sync": True,
        "public_dashboard": True,
        "post_close_flat": True,
        "disk": True,
    }
    assert "actions" not in projected
    assert "secret.service" not in json.dumps(projected)


def test_dashboard_marks_failed_preopen_as_late_recovery_after_engine_commit(
    tmp_path: Path,
) -> None:
    readiness_path = tmp_path / "preopen_readiness.json"
    readiness_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "updated_at": _now(9, 1).isoformat(),
                "markets": {
                    "tw_day_trade": {
                        "status": "failed",
                        "started_at": _now(8, 30).isoformat(),
                        "completed_at": _now(9, 1).isoformat(),
                        "elapsed_seconds": 1_860.0,
                        "error": "BotUserError: timeout",
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    modes = [
        {
            "market": "tw_day_trade",
            "signal_market": "tw_day_trade",
            "engine_status": "active",
            "session_date": "2026-08-13",
            "signal_id": "signal-recovered",
            "entry_completed_at": _now(9, 2).isoformat(),
        }
    ]

    progress = dashboard_module._preopen_progress(
        path=readiness_path,
        modes=modes,
        observed=_now(9, 3),
    )

    assert progress["status"] == "recovered_late"
    assert progress["ready_count"] == 1
    assert progress["failed_count"] == 0
    assert progress["recovered_count"] == 1
    row = progress["markets"][0]
    assert row["status"] == "recovered_late"
    assert row["preparation_status"] == "failed"
    assert row["preparation_error"] == "BotUserError: timeout"
    assert row["recovered_signal_id"] == "signal-recovered"
    assert row["public_error_code"] == "preopen_recovered_late"


def test_dashboard_default_view_exposes_today_prewarm_before_first_signal(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(8, 29))
    state = json.loads(engine.state_path.read_text(encoding="utf-8"))
    state["modes"][spec.market]["session_date"] = "2026-08-12"
    engine.state_path.write_text(
        json.dumps(state) + "\n",
        encoding="utf-8",
    )
    readiness_path = tmp_path / "preopen_readiness.json"
    readiness_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "updated_at": _now(8, 45).isoformat(),
                "markets": {
                    spec.market: {
                        "status": "ready",
                        "started_at": _now(8, 30).isoformat(),
                        "completed_at": _now(8, 45).isoformat(),
                        "elapsed_seconds": 900.0,
                        "step": 23,
                        "total": 23,
                        "message": "ready",
                        "panel_date": "2026-08-12 13:30:00",
                        "symbol_count": 3_000,
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        preopen_readiness_path=readiness_path,
        now=_now(8, 46).astimezone(ZoneInfo("UTC")),
    )

    assert payload["session_date"] == "2026-08-12"
    assert payload["preopen"]["ready_count"] == 1
    assert payload["preopen"]["updated_at"] == _now(8, 45).isoformat()
    assert payload["preopen"]["simulation"]["session_date"] == "2026-08-13"
    assert payload["preopen"]["status"] == "pending"


def test_simulation_executor_preopen_receipt_requires_both_components(
    tmp_path: Path,
) -> None:
    from scripts.run_tw_day_trade_simulation import _write_preopen_readiness

    state_dir = tmp_path / "state"
    first = _write_preopen_readiness(
        state_dir,
        session_date="2026-08-13",
        component="eligibility",
        status="ready",
        observed=_now(8, 30),
        details={"proof": "exact_session_twse_tpex_coverage"},
    )
    assert first["status"] == "warming"
    ready = _write_preopen_readiness(
        state_dir,
        session_date="2026-08-13",
        component="shioaji_quote",
        status="ready",
        observed=_now(8, 55),
        details={"proof": "simulation_client_usage_probe"},
    )
    assert ready["status"] == "ready"
    assert (
        json.loads((state_dir / "preopen_readiness.json").read_text())["status"]
        == "ready"
    )


def test_dashboard_html_is_local_and_refreshes_api() -> None:
    root = Path(__file__).resolve().parents[1] / "services" / "tw_day_trade_dashboard"
    html = (root / "index.html").read_text(encoding="utf-8")
    javascript = (root / "app.js").read_text(encoding="utf-8")
    assert "http://" not in html and "https://" not in html
    assert 'id="workflow-progress"' in html
    assert 'id="preopen-progress"' in html
    assert "fetchWithTimeout(`api/status" in javascript
    assert 'fetchWithTimeout("api/revision"' in javascript
    assert "const SERVICE_REVISION_REFRESH_MS = 1000" in javascript
    assert "Dashboard.scheduleRefresh(refreshServiceRevision" in javascript
    assert "服務同步" in javascript
    assert "fetchWithTimeout(`api/signals?${params.toString()}`" in javascript
    assert "fetchWithTimeout(`api/positions?${params.toString()}`" in javascript
    assert "fetchWithTimeout(`api/events?${params.toString()}`" in javascript
    assert "function renderOperations(data)" in javascript
    assert "模擬執行器盤前守門" in javascript
    assert "Shioaji usage 探測" in javascript
    assert "function compareByAbsoluteWeight(a, b)" in javascript
    assert javascript.count(".sort(compareByAbsoluteWeight)") == 1
    assert "function beginSilentTableUpdate" in javascript
    assert javascript.count("signalRows = [];") == 1
    assert javascript.count("positionRows = [];") == 1
    assert javascript.count("eventRows = [];") == 1
    assert "location.reload" not in javascript
    assert "window.location" not in javascript
    assert (
        'beginSilentTableUpdate("signal-body", "load-more-signals", append)'
        in javascript
    )
    assert (
        'beginSilentTableUpdate("position-body", "load-more-positions", append)'
        in javascript
    )
    assert (
        'beginSilentTableUpdate("event-body", "load-more-events", append)' in javascript
    )
    assert "refreshInFlight" in javascript
    assert "SIGNAL_PAGE_SIZE" in javascript
    assert "const SIGNAL_PAGE_SIZE = 100" in javascript
    assert "const POSITION_PAGE_SIZE = 100" in javascript
    assert "function hydrateDefaultPositions(data)" in javascript
    assert "const detailLoads = [];" in javascript
    assert "if (shouldReloadPositions) detailLoads.push(loadPositions());" in javascript
    assert "}, 80);" in javascript
    assert "const sourceNumber" in javascript
    assert "maximumSignificantDigits" not in javascript
    assert javascript.count("maximumFractionDigits: 2") >= 5
    assert "const monetaryNumber" in javascript
    assert "maximumFractionDigits: 8" not in javascript
    assert "number(row.bid,2)" not in javascript
    assert "number(row.raw_score ?? row.score,5)" not in javascript
    assert "(pnl / initial * 100).toFixed(3)" not in javascript
    assert 'id="load-more-signals"' in html
    assert 'id="load-more-events"' in html
    assert 'id="load-more-positions"' in html
    assert 'id="signal-direction-summary"' in html
    assert "signalOpeningExecutionAudit" in javascript
    assert "開盤計價 ${money(openingPrice)}" in javascript
    assert "configured_entry_fill_policy" in javascript
    assert "市價買進／回補取第一筆較晚最佳 Ask" in javascript
    assert "市價賣出／放空取第一筆較晚最佳 Bid" in javascript
    assert "不回寫成最佳報價" in javascript
    assert 'class="compact-table position-table"' in html
    assert 'class="compact-table signal-table"' in html
    assert "<th>損益拆分／估值</th>" in html
    assert "<th>開盤計價／市價成交</th>" in html
    assert "<th>現在市價／該檔盈虧</th>" in html
    assert "<th>損益／模式總權益</th>" in html
    assert 'class="skip-link"' in html
    assert 'aria-label="公開面板導覽"' in html
    assert 'id="overview-kpis"' in html
    assert 'id="reset-filters"' in html
    assert "function renderOverview(data)" in javascript
    assert "13:24 市價重試後有殘餘，已轉 13:25 集合競價" in javascript
    assert "13:20/13:24/13:25 退出" in javascript
    assert "區間訊號目標" in javascript
    assert "資格／整張／深度後實際成交" in javascript
    assert "方向平衡後" not in javascript
    assert "雙向整張不足・保持空倉" not in javascript
    assert "各模式已實現" in javascript
    assert "各模式未實現" in javascript
    assert "已實現＋未實現，已與總權益對帳" in javascript
    assert "未實現淨清算損益" in javascript
    assert "已實現 ${money(realizedNet)}" in javascript
    assert "未實現 ${money(unrealizedNet)}" in javascript
    assert "reconciled_total_net_pnl_twd" in javascript
    assert "function resolvedPositionPnl(row = {})" in javascript
    assert "名目 ${money(entryNotional)}" in javascript
    assert "佔該模式總權益" in javascript
    assert (
        '模式總權益 ${Number.isFinite(modeTotalEquity) ? summaryMoney(modeTotalEquity) : "—"}'
        in javascript
    )
    assert "Number(positionPnl.total) / modeTotalEquity * 100" in javascript
    assert "Number(positionPnl.total) / totalPortfolioPnl * 100" not in javascript
    assert '未成交・${esc(row.reason || row.status || "受限")}' in javascript
    assert "所選交易日當沖資格未完整覆蓋" in javascript
    assert "較晚補齊的資料不會回填成假成交" in javascript
    assert "原子指標由 inotify 事件即時喚醒" in javascript
    assert 'id="latency-kpis"' in html
    assert "今日尚無開盤樣本" in javascript
    assert "這不是券商回報或交易所往返時間" in html
    assert javascript.count("const requestRange = detailRangeKey();") == 3
    assert "start_date: selectedDetailStartDate()" in javascript
    assert "end_date: selectedDetailEndDate()" in javascript
    assert 'id="detail-start-date" type="date"' in html
    assert 'id="detail-end-date" type="date"' in html
    assert (
        '$("detail-start-date").addEventListener("change", detailDateChanged)'
        in javascript
    )
    assert (
        '$("detail-end-date").addEventListener("change", detailDateChanged)'
        in javascript
    )
    assert "啟用模式盤前預熱測速（不等同該日執行完成）" in html
    assert "依 |持倉目標 %| 由大到小" in html
    assert "const PRICE_REFRESH_MS = 60000" in javascript
    assert "Dashboard.scheduleRefresh(() => {" in javascript
    assert "}, {intervalMs: PRICE_REFRESH_MS});" in javascript
    assert 'aria-label="權益曲線日期來源"' in html
    assert 'id="equity-time-range"' not in html
    assert 'id="equity-start-date"' not in html
    assert "rangeSummaryFor" in javascript
    assert "selectedDetailStartDate()" in javascript
    assert "起始日前最後一筆權益" in html
    assert 'id="chart-legend"' in html
    assert 'aria-label="曲線顯示開關"' in html
    assert 'data-range="1y"' not in html
    assert 'data-range="all"' not in html
    assert "HIDDEN_EQUITY_SERIES_STORAGE_KEY" in javascript
    assert "HISTORY_CLIENT_CACHE_MS" in javascript
    assert "chartHistoryCache" in javascript
    assert "response.status === 429" not in javascript
    assert "秒後自動重試" not in javascript
    assert "button[data-series-id]" in javascript
    assert 'aria-pressed="${String(!hidden)}"' in javascript
    assert "refreshSummary" not in javascript


def test_dashboard_counts_only_same_day_execution_events(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(9, 5))
    mode = engine.state["modes"][spec.market]
    mode["signal_at"] = "2026-08-12T09:00:00+08:00"
    mode["entry_completed_at"] = "2026-08-12T09:00:01+08:00"
    engine._persist(_now(9, 5))

    missing = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=_now(9, 5).astimezone(ZoneInfo("UTC")),
    )
    assert missing["execution_records"]["executed_count"] == 0
    assert missing["modes"][0]["today_execution_status"] == "starting"
    assert missing["session_progress"]["signal_completed_modes"] == 0

    engine.events_path.write_text(
        json.dumps(
            {
                "event": "signal_blocked",
                "market": spec.market,
                "reason": "prior_position_unflattened",
                "recorded_at": _now(9, 6).isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    recorded = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=_now(9, 6).astimezone(ZoneInfo("UTC")),
    )
    assert recorded["execution_records"]["executed_count"] == 0
    assert recorded["execution_records"]["attempted_count"] == 1
    assert recorded["execution_records"]["blocked_count"] == 1
    assert recorded["execution_records"]["all_executed"] is False
    assert recorded["modes"][0]["today_execution_status"] == "blocked"
    assert recorded["modes"][0]["today_execution_outcome"] == "blocked"
    assert recorded["operational_issues"]
    assert recorded["health"] in {"degraded", "stale"}
    assert recorded["modes"][0]["today_execution_reason"] == (
        "prior_position_unflattened"
    )
    assert recorded["session_progress"]["signal_completed_modes"] == 0

    with engine.events_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "event": "signal_registered",
                    "market": spec.market,
                    "signal_id": "consumed",
                    "recorded_at": _now(9, 7).isoformat(),
                }
            )
            + "\n"
        )
        handle.write(
            json.dumps(
                {
                    "event": "signal_blocked",
                    "market": spec.market,
                    "signal_id": "duplicate",
                    "reason": "daily_signal_already_consumed",
                    "recorded_at": _now(9, 8).isoformat(),
                }
            )
            + "\n"
        )
    duplicate = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=_now(9, 8).astimezone(ZoneInfo("UTC")),
    )
    assert duplicate["execution_records"]["executed_count"] == 1
    assert duplicate["execution_records"]["blocked_count"] == 0
    assert duplicate["modes"][0]["today_execution_status"] == "completed"
    assert duplicate["modes"][0]["today_execution_outcome"] == "no_order"

    summary = build_dashboard_summary(
        state_dir=engine.state_dir,
        now=_now(9, 8).astimezone(ZoneInfo("UTC")),
    )
    assert summary["execution_records"]["executed_count"] == 1
    assert summary["modes"][0]["today_execution_status"] == "completed"
    assert "signals" not in summary
    assert "events" not in summary


def test_dashboard_date_filter_switches_all_session_ledgers(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_now(8, 59))
    rows = [
        {
            "event": "signal_registered",
            "market": spec.market,
            "signal_id": "day-13",
            "recorded_at": "2026-08-13T09:01:00+08:00",
            "entry_fill_policy": ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK,
            "entry_price_offset_ticks": 1,
            "entry_fill_is_synthetic": True,
        },
        {
            "event": "signal_blocked",
            "market": spec.market,
            "signal_id": "day-14",
            "reason": "prior_position_unflattened",
            "recorded_at": "2026-08-14T09:01:00+08:00",
        },
    ]
    engine.events_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    engine.marks_path.write_text(
        "".join(
            json.dumps(row) + "\n"
            for row in (
                {
                    "session_date": "2026-08-13",
                    "market": spec.market,
                    "minute": "2026-08-13T13:30+08:00",
                    "initial_capital_twd": 10_000_000.0,
                    "total_equity_twd": 10_010_000.0,
                    "open_position_count": 0,
                    "stale_position_count": 0,
                },
                {
                    "session_date": "2026-08-14",
                    "market": spec.market,
                    "minute": "2026-08-14T09:01+08:00",
                    "initial_capital_twd": 10_000_000.0,
                    "total_equity_twd": 10_020_000.0,
                    "open_position_count": 0,
                    "stale_position_count": 0,
                },
            )
        ),
        encoding="utf-8",
    )

    day_13 = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        session_date="2026-08-13",
        now=datetime(2026, 8, 14, 2, 0, tzinfo=ZoneInfo("UTC")),
    )
    assert day_13["available_session_dates"] == ["2026-08-14", "2026-08-13"]
    assert day_13["session_date"] == "2026-08-13"
    assert day_13["execution_records"]["executed_count"] == 1
    assert day_13["marks"][0]["total_equity_twd"] == 10_010_000.0
    assert {event["signal_id"] for event in day_13["events"]} == {"day-13"}
    historical_mode = day_13["modes"][0]
    assert historical_mode["configured_entry_fill_policy"] == (
        ENTRY_FILL_POLICY_CAUSAL_BOOK
    )
    assert historical_mode["configured_entry_price_offset_ticks"] == 0
    assert historical_mode["configured_entry_fill_is_synthetic"] is False
    assert historical_mode["entry_fill_policy"] == (
        ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK
    )
    assert historical_mode["entry_price_offset_ticks"] == 1
    assert historical_mode["entry_fill_is_synthetic"] is True

    day_14 = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        session_date="2026-08-14",
        now=datetime(2026, 8, 14, 2, 0, tzinfo=ZoneInfo("UTC")),
    )
    assert day_14["execution_records"]["executed_count"] == 0
    assert day_14["execution_records"]["blocked_count"] == 1
    assert day_14["marks"][0]["total_equity_twd"] == 10_020_000.0
    assert {event["signal_id"] for event in day_14["events"]} == {"day-14"}

    with pytest.raises(ValueError, match="unavailable"):
        build_dashboard_snapshot(
            state_dir=engine.state_dir,
            session_date="2026-08-12",
            now=datetime(2026, 8, 14, 2, 0, tzinfo=ZoneInfo("UTC")),
        )


def test_next_session_archives_closed_positions_for_dashboard_history(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    replay = _summary()
    replay.update(
        {
            "simulation_replay": True,
            "replay_basis": "recorded_open_to_official_close",
            "replay_source": "fixture",
            "entry_fill_contract": "retrospective_open_price",
            "entry_liquidity_assumption": "counterfactual_unbounded",
        }
    )
    assert (
        engine.register_signal(
            spec=spec,
            summary=replay,
            signal_rows=[_row()],
            quotes={"2330": _quote()},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 1, 7),
        )
        == "registered"
    )
    engine.process_quotes(
        quotes={"2330": _quote(bid=1_010.0, ask=1_010.0, last=1_010.0)},
        now=_now(13, 30),
    )

    next_day = datetime(2026, 8, 14, 9, 1, 7, tzinfo=TAIPEI)
    next_summary = dict(replay)
    next_summary["signal_id"] = "signal-2"
    next_summary["generated_at"] = datetime(
        2026, 8, 14, 9, 0, 5, tzinfo=TAIPEI
    ).isoformat()
    next_quote = _quote()
    next_quote["quote_at"] = datetime(2026, 8, 14, 9, 1, 6, tzinfo=TAIPEI).isoformat()
    assert (
        engine.register_signal(
            spec=spec,
            summary=next_summary,
            signal_rows=[_row()],
            quotes={"2330": next_quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=next_day,
        )
        == "registered"
    )

    archive = engine.position_history_dir / "2026-08-13" / "tw_day_trade.json"
    archived = json.loads(archive.read_text(encoding="utf-8"))
    assert archived["positions"][0]["status"] == "closed"
    snapshot = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        session_date="2026-08-13",
        now=datetime(2026, 8, 14, 2, 0, tzinfo=ZoneInfo("UTC")),
    )
    assert len(snapshot["positions"]) == 1
    assert snapshot["positions"][0]["session_date"] == "2026-08-13"
    assert snapshot["positions"][0]["simulation_replay"] is True
    assert snapshot["modes"][0]["simulation_replay"] is True
    registered = [
        event
        for event in snapshot["events"]
        if event.get("event") == "signal_registered"
    ]
    assert registered[0]["recorded_at"] == _now(9, 1, 7).isoformat()


def test_counterfactual_best_quote_replay_records_every_entry_at_session_open(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    replay = _summary()
    replay.update(
        {
            "generated_at": _now(9, 43, 52).isoformat(),
            "simulation_replay": True,
            "replay_basis": "actual_session_open_to_official_close",
            "replay_source": "fixture",
            "entry_fill_contract": "retrospective_observed_best_quote_counterfactual",
            "entry_liquidity_assumption": "recorded_level_one_displayed_depth",
        }
    )
    open_at = _now(9, 1)
    quote = _quote()
    quote["quote_at"] = open_at.isoformat()

    assert (
        engine.register_signal(
            spec=spec,
            summary=replay,
            signal_rows=[_row()],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=open_at,
            counterfactual_open_replay=True,
        )
        == "registered"
    )

    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert mode["signal_at"] == open_at.isoformat()
    assert mode["source_signal_at"] == _now(9, 43, 52).isoformat()
    assert mode["entry_completed_at"] == open_at.isoformat()
    assert position["entry_at"] == open_at.isoformat()
    assert position["entry_quote_at"] == open_at.isoformat()
    assert position["source_signal_at"] == _now(9, 43, 52).isoformat()

    for path in (engine.signals_path, engine.orders_path, engine.fills_path):
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        assert rows
        assert {row["recorded_at"] for row in rows} == {open_at.isoformat()}
    fills = [json.loads(line) for line in engine.fills_path.read_text().splitlines()]
    assert {row["fill_at"] for row in fills} == {open_at.isoformat()}
    events = [json.loads(line) for line in engine.events_path.read_text().splitlines()]
    assert {row["recorded_at"] for row in events} == {open_at.isoformat()}
    assert events[-1]["source_signal_at"] == _now(9, 43, 52).isoformat()
    snapshot = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        now=datetime(2026, 8, 13, 2, 0, tzinfo=ZoneInfo("UTC")),
    )
    assert snapshot["modes"][0]["signal_at"] == open_at.isoformat()
    assert snapshot["modes"][0]["source_signal_at"] == _now(9, 43, 52).isoformat()
    assert snapshot["modes"][0]["counterfactual_open_replay"] is True
    assert snapshot["positions"][0]["source_signal_at"] == _now(9, 43, 52).isoformat()


def test_counterfactual_open_replay_cannot_relax_normal_live_signal_gate(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote()
    quote["quote_at"] = _now(9, 0, 5).isoformat()

    assert (
        engine.register_signal(
            spec=spec,
            summary=_summary(),
            signal_rows=[_row()],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 1),
        )
        == "waiting_quote"
    )

    with pytest.raises(ValueError, match="simulation_replay=true"):
        engine.register_signal(
            spec=spec,
            summary={**_summary(), "signal_id": "unsafe-replay"},
            signal_rows=[_row()],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 1),
            counterfactual_open_replay=True,
        )


def test_dashboard_jsonl_tail_and_incremental_count(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        "".join(json.dumps({"index": index}) + "\n" for index in range(5)),
        encoding="utf-8",
    )
    assert [row["index"] for row in _tail(path, 2)] == [3, 4]
    assert _line_count(path) == 5

    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"index": 5}) + "\n")
    assert _line_count(path) == 6
    assert [row["index"] for row in _tail(path, 2)] == [4, 5]

    path.write_text(json.dumps({"index": 9}) + "\n", encoding="utf-8")
    assert _line_count(path) == 1
    assert [row["index"] for row in _tail(path, 2)] == [9]


def test_session_tail_is_not_crowded_out_by_a_later_date(tmp_path: Path) -> None:
    path = tmp_path / "fills.jsonl"
    rows = [{"session_date": "2026-08-13", "index": index} for index in range(5)] + [
        {"session_date": "2026-08-14", "index": index} for index in range(500)
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    selected = _tail_for_session(path, 3, "2026-08-13")

    assert [row["index"] for row in selected] == [2, 3, 4]


def test_available_session_date_cache_invalidates_when_ledger_grows(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "state.json").write_text(
        json.dumps({"modes": {"mode_a": {"session_date": "2026-08-13"}}}) + "\n",
        encoding="utf-8",
    )
    signals_path = state_dir / "signals.jsonl"
    signals_path.write_text(
        json.dumps(
            {
                "session_date": "2026-08-13",
                "market": "mode_a",
                "symbol": "2330",
                "status": "ready",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    first = build_dashboard_signal_page(state_dir=state_dir, limit=10)
    assert first["available_session_dates"] == ["2026-08-13"]

    with signals_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "session_date": "2026-08-14",
                    "market": "mode_a",
                    "symbol": "2317",
                    "status": "ready",
                }
            )
            + "\n"
        )

    second = build_dashboard_signal_page(
        state_dir=state_dir,
        start_date="2026-08-13",
        end_date="2026-08-14",
        limit=10,
    )
    assert second["available_session_dates"] == ["2026-08-14", "2026-08-13"]
    assert second["total"] == 2


def test_signal_page_cache_ignores_unrelated_live_state_marks(
    tmp_path: Path, monkeypatch
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    state_path = state_dir / "state.json"
    state = {
        "modes": {
            "mode_a": {
                "session_date": "2026-08-13",
                "initial_capital_twd": 10_000_000,
                "last_mark_at": "2026-08-13T09:01:00+08:00",
            }
        }
    }
    state_path.write_text(json.dumps(state) + "\n", encoding="utf-8")
    (state_dir / "signals.jsonl").write_text(
        json.dumps(
            {
                "session_date": "2026-08-13",
                "market": "mode_a",
                "symbol": "2330",
                "target_weight": 0.1,
                "status": "ready",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    first = build_dashboard_signal_page(state_dir=state_dir, limit=10)
    assert first["total"] == 1

    state["modes"]["mode_a"]["last_mark_at"] = "2026-08-13T09:02:00+08:00"
    state_path.write_text(json.dumps(state) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        dashboard_module,
        "_rows_for_sessions",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unrelated mark update must not reparse signal rows")
        ),
    )

    second = build_dashboard_signal_page(state_dir=state_dir, limit=10)
    assert second["rows"] == first["rows"]


def test_dashboard_signal_page_filters_sorts_and_bounds_payload(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "state.json").write_text(
        json.dumps(
            {
                "modes": {
                    "mode_a": {"session_date": "2026-08-13"},
                    "mode_b": {"session_date": "2026-08-13"},
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    rows = [
        {
            "session_date": "2026-08-12",
            "market": "mode_a",
            "symbol": "OLD",
            "target_weight": 9.0,
            "status": "ready",
        },
        {
            "session_date": "2026-08-13",
            "market": "mode_a",
            "symbol": "2330",
            "name": "台積電",
            "target_weight": 0.1,
            "status": "ready",
            "sizing_open_price": 100.0,
            "execution_price": 100.5,
            "requested_shares": 1_000,
            "filled_shares": 1_000,
        },
        {
            "session_date": "2026-08-13",
            "market": "mode_a",
            "symbol": "2317",
            "name": "鴻海",
            "target_weight": -0.3,
            "status": "missing_quote",
            "reason": "missing_quote",
        },
        {
            "session_date": "2026-08-13",
            "market": "mode_a",
            "symbol": "2454",
            "name": "聯發科",
            "target_weight": 0.2,
            "status": "partial_depth",
            "sizing_open_price": 900.0,
            "execution_price": 901.0,
            "requested_shares": 1_000,
            "filled_shares": 500,
        },
        {
            "session_date": "2026-08-13",
            "market": "mode_b",
            "symbol": "2330",
            "name": "台積電",
            "target_weight": 0.8,
            "status": "ready",
        },
    ]
    (state_dir / "signals.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )

    first = build_dashboard_signal_page(
        state_dir=state_dir,
        mode="mode_a",
        offset=0,
        limit=2,
    )
    assert first["simulation_only"] is True
    assert first["production_order_possible"] is False
    assert first["total"] == 3
    assert first["has_more"] is True
    assert [row["symbol"] for row in first["rows"]] == ["2317", "2454"]
    assert first["source_rows_scanned"] == 4
    assert first["record_count"] == 5
    assert first["direction_summary"]["target"] == {
        "long_count": 2,
        "short_count": 1,
        "long_gross": pytest.approx(0.3),
        "short_gross": pytest.approx(0.3),
    }
    assert first["opening_execution_audit"]["mode_a"] == {
        "nonzero_signal_count": 3,
        "opening_price_covered_count": 2,
        "opening_price_missing_count": 1,
        "execution_price_covered_count": 2,
        "requested_signal_count": 2,
        "filled_signal_count": 2,
        "unfilled_signal_count": 1,
        "missing_open_symbols": ["2317"],
        "unfilled_reason_counts": {"missing_quote": 1},
    }

    filtered = build_dashboard_signal_page(
        state_dir=state_dir,
        mode="mode_a",
        symbol="鴻海",
        status="blocked",
        limit=10,
    )
    assert filtered["total"] == 1
    assert filtered["rows"][0]["symbol"] == "2317"

    prior = build_dashboard_signal_page(
        state_dir=state_dir,
        session_date="2026-08-12",
        limit=10,
    )
    assert prior["session_date"] == "2026-08-12"
    assert [row["symbol"] for row in prior["rows"]] == ["OLD"]

    ranged = build_dashboard_signal_page(
        state_dir=state_dir,
        start_date="2026-08-12",
        end_date="2026-08-16",
        limit=10,
    )
    assert ranged["start_date"] == "2026-08-12"
    assert ranged["end_date"] == "2026-08-16"
    assert ranged["session_dates"] == ["2026-08-12", "2026-08-13"]
    assert ranged["total"] == 5
    assert [row["symbol"] for row in ranged["rows"]] == [
        "OLD",
        "2330",
        "2317",
        "2454",
        "2330",
    ]

    ranged_first_page = build_dashboard_signal_page(
        state_dir=state_dir,
        start_date="2026-08-12",
        end_date="2026-08-16",
        limit=2,
    )
    assert [row["target_weight"] for row in ranged_first_page["rows"]] == [
        9.0,
        0.8,
    ]

    closed_weekend = build_dashboard_signal_page(
        state_dir=state_dir,
        start_date="2026-08-15",
        end_date="2026-08-16",
        limit=10,
    )
    assert closed_weekend["session_dates"] == []
    assert closed_weekend["rows"] == []


def test_dashboard_position_page_filters_an_inclusive_calendar_range(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    history_dir = state_dir / "position_history" / "2026-08-13"
    history_dir.mkdir(parents=True)
    current_position = {
        "position_id": "current-private-id",
        "session_date": "2026-08-14",
        "market": "mode_a",
        "symbol": "2330",
        "name": "台積電",
        "side": "long",
        "signed_shares": 1_000,
        "filled_shares": 1_000,
        "requested_shares": 1_000,
        "entry_price": 1_000.0,
        "target_weight": 0.1,
        "status": "open",
    }
    historical_position = {
        "position_id": "historical-private-id",
        "session_date": "2026-08-13",
        "market": "mode_b",
        "symbol": "2317",
        "name": "鴻海",
        "side": "short",
        "signed_shares": 0,
        "filled_shares": 2_000,
        "requested_shares": 2_000,
        "entry_price": 200.0,
        "target_weight": -0.2,
        "status": "closed",
    }
    (state_dir / "state.json").write_text(
        json.dumps(
            {
                "modes": {
                    "mode_a": {
                        "session_date": "2026-08-14",
                        "positions": {"current-private-id": current_position},
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (history_dir / "mode_b.json").write_text(
        json.dumps(
            {
                "session_date": "2026-08-13",
                "market": "mode_b",
                "positions": [historical_position],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    ranged = build_dashboard_position_page(
        state_dir=state_dir,
        start_date="2026-08-13",
        end_date="2026-08-16",
        limit=10,
    )
    assert ranged["start_date"] == "2026-08-13"
    assert ranged["end_date"] == "2026-08-16"
    assert ranged["session_dates"] == ["2026-08-13", "2026-08-14"]
    assert ranged["total"] == 2
    assert [row["symbol"] for row in ranged["rows"]] == ["2330", "2317"]

    closed = build_dashboard_position_page(
        state_dir=state_dir,
        start_date="2026-08-13",
        end_date="2026-08-16",
        status="closed",
        limit=10,
    )
    assert closed["total"] == 1
    assert closed["rows"][0]["symbol"] == "2317"


def test_dashboard_event_page_returns_all_selected_day_rows_safely(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "state.json").write_text(
        json.dumps(
            {
                "modes": {
                    "mode_a": {"session_date": "2026-08-14"},
                    "mode_b": {"session_date": "2026-08-14"},
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    orders = [
        {
            "session_date": "2026-08-13",
            "recorded_at": "2026-08-13T09:00:00+08:00",
            "market": "mode_a",
            "symbol": "2330",
            "order_id": "private-order-1",
            "position_id": "private-position-1",
            "purpose": "entry",
            "order_type": "MKT",
            "price": 900.0,
            "quantity": 1_000,
            "status": "filled",
        },
        {
            "session_date": "2026-08-13",
            "recorded_at": "2026-08-13T09:01:00+08:00",
            "market": "mode_b",
            "symbol": "2317",
            "order_id": "private-order-2",
            "position_id": "private-position-2",
            "purpose": "entry",
            "order_type": "MKT",
            "price": 200.0,
            "quantity": 2_000,
            "status": "filled",
        },
        {
            "session_date": "2026-08-14",
            "recorded_at": "2026-08-14T09:00:00+08:00",
            "market": "mode_a",
            "symbol": "2330",
            "order_id": "later-day",
        },
    ]
    fills = [
        {
            "session_date": "2026-08-13",
            "recorded_at": "2026-08-13T09:00:00+08:00",
            "fill_at": "2026-08-13T09:00:01+08:00",
            "market": "mode_a",
            "symbol": "2330",
            "order_id": "private-order-1",
            "position_id": "private-position-1",
            "purpose": "entry",
            "price": 900.0,
            "quantity": 1_000,
        }
    ]
    for filename, rows in (("orders.jsonl", orders), ("fills.jsonl", fills)):
        (state_dir / filename).write_text(
            "".join(json.dumps(row) + "\n" for row in rows),
            encoding="utf-8",
        )

    first = build_dashboard_event_page(
        state_dir=state_dir,
        session_date="2026-08-13",
        offset=0,
        limit=2,
    )
    assert first["simulation_only"] is True
    assert first["production_order_possible"] is False
    assert first["total"] == 3
    assert first["order_total"] == 2
    assert first["fill_total"] == 1
    assert first["has_more"] is True
    assert [row["event_kind"] for row in first["rows"]] == ["order", "fill"]
    keys = set().union(*(row.keys() for row in first["rows"]))
    assert not ({"order_id", "position_id"} & keys)

    second = build_dashboard_event_page(
        state_dir=state_dir,
        session_date="2026-08-13",
        offset=2,
        limit=2,
    )
    assert second["returned"] == 1
    assert second["has_more"] is False
    filtered = build_dashboard_event_page(
        state_dir=state_dir,
        session_date="2026-08-13",
        mode="mode_a",
        symbol="2330",
        limit=10,
    )
    assert filtered["total"] == 2
    assert {row["event_kind"] for row in filtered["rows"]} == {"order", "fill"}

    ranged = build_dashboard_event_page(
        state_dir=state_dir,
        start_date="2026-08-13",
        end_date="2026-08-16",
        limit=10,
    )
    assert ranged["session_dates"] == ["2026-08-13", "2026-08-14"]
    assert ranged["total"] == 4
    assert {row["session_date"] for row in ranged["rows"]} == {
        "2026-08-13",
        "2026-08-14",
    }
