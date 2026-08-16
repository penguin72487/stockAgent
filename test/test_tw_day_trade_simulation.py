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
    build_dashboard_signal_page,
    build_dashboard_snapshot,
    build_dashboard_summary,
)
from stockagent.live.tw_day_trade_simulation import (
    LiveEligibility,
    ModeSpec,
    TwDayTradeSimulationEngine,
    load_live_eligibility,
    quote_map_from_snapshot,
    require_exact_session_eligibility,
)


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


def test_runner_offsets_all_four_existing_modes_without_adding_a_fifth() -> None:
    from scripts.run_tw_day_trade_simulation import _mode_specs

    repo_root = Path(__file__).resolve().parents[1]
    specs, live_configs, errors = _mode_specs(
        repo_root / "services/discord_bot/markets"
    )
    by_market = {spec.market: spec for spec in specs}
    expected = {
        "tw_day_trade_1m",
        "tw_day_trade",
        "tw_day_trade_multi_basis",
        "tw_day_trade_100m",
    }

    assert errors == {}
    assert set(by_market) == expected
    assert set(live_configs) == expected
    assert all(spec.signal_market is None for spec in specs)
    assert all(spec.price_limit_offset_ticks == 1 for spec in specs)


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
    ).write_parquet(
        public_root / "twse_api_holidayschedule_holidayschedule.parquet"
    )
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


def test_entry_executes_during_opening_minute_without_waiting_for_kbar(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = _quote(minute_volume_lots=0.0)
    quote["quote_at"] = _now(9, 0, 6).isoformat()

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 0, 59),
    )

    assert result == "registered"
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["filled_shares"] == 1_000


def test_market_entry_uses_best_ask_and_records_board_lot(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")

    result = engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(0.1)],
        quotes={"2330": _quote()},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 1, 7),
    )

    assert result == "registered"
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["entry_price"] == 1_000.0
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
    assert position["take_profit_price"] == 1_100.0
    assert position["stop_trigger_price"] == 900.0


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
        now=_now(9, 0, 10),
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
    assert engine.state["schema_version"] == 3
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


def test_two_sided_live_signal_reduces_only_the_better_filled_side(
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
    assert by_symbol["2317"]["pre_balance_filled_shares"] == 20_000
    assert by_symbol["2317"]["filled_shares"] == 1_000
    assert mode["execution_projection"]["pre_balance_short_gross"] == 0.2
    assert mode["execution_projection"]["post_balance_long_gross"] == 0.01
    assert mode["execution_projection"]["post_balance_short_gross"] == 0.01


def test_two_sided_live_signal_fails_complete_portfolio_flat_when_one_side_has_no_lot(
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
    assert mode["positions"] == {}
    assert mode["execution_projection"]["collapsed_to_flat"] is True
    assert mode["engine_status"] == "flat_directional_mix_unexecutable"


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

    assert engine.invalidate_non_session_flat_signal(
        spec.market,
        now=_now(9, 30),
        reason="official calendar says closed",
    ) == "invalidated"
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
    quote["quote_at"] = datetime(
        2026, 8, 14, 9, 1, 6, tzinfo=TAIPEI
    ).isoformat()
    assert engine.register_signal(
        spec=spec,
        summary=replacement,
        signal_rows=[_row()],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=next_session,
    ) == "registered"
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
    assert row["realized_gross_pnl_twd"] == pytest.approx(
        (45_123.0 - 45_001.0) * 200.0
    )
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

    assert one_hour["range"] == "1h"
    assert one_hour["raw_points_in_range"] == 2
    assert [row["return_pct"] for row in one_hour["history"]] == pytest.approx(
        [2.0, 3.0]
    )
    assert all_time["raw_points_in_range"] == 3

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
    assert payload["source_contract"]["entry_fill"].startswith("causally later")
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
        now=_now(8, 59).astimezone(ZoneInfo("UTC")),
    )

    assert payload["schema_version"] == 4
    assert payload["preopen"]["status"] == "ready"
    assert payload["preopen"]["ready_count"] == 1
    assert payload["preopen"]["progress_ratio"] == 1.0
    assert payload["preopen"]["wall_elapsed_seconds"] == 30.0
    mode = payload["preopen"]["markets"][0]
    assert mode["symbols_per_second"] == 100.0
    assert mode["model_symbols_per_second"] == 6_000.0
    assert mode["price_limit_coverage_ratio"] == 0.8
    assert mode["eligibility_ready"] is True
    assert mode["eligibility_target_date"] == "2026-08-13"
    assert payload["session_progress"]["phase"] == "preopen"


def test_dashboard_html_is_local_and_refreshes_api() -> None:
    root = Path(__file__).resolve().parents[1] / "services" / "tw_day_trade_dashboard"
    html = (root / "index.html").read_text(encoding="utf-8")
    javascript = (root / "app.js").read_text(encoding="utf-8")
    assert "http://" not in html and "https://" not in html
    assert 'id="workflow-progress"' in html
    assert 'id="preopen-progress"' in html
    assert "fetchWithTimeout(`api/status" in javascript
    assert "fetchWithTimeout(`api/signals?${params.toString()}`" in javascript
    assert "fetchWithTimeout(`api/events?${params.toString()}`" in javascript
    assert "function renderOperations(data)" in javascript
    assert "function compareByAbsoluteWeight(a, b)" in javascript
    assert javascript.count(".sort(compareByAbsoluteWeight)") == 1
    assert "refreshInFlight" in javascript
    assert "SIGNAL_PAGE_SIZE" in javascript
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
    assert 'class="compact-table position-table"' in html
    assert 'class="compact-table signal-table"' in html
    assert "<th>損益拆分／估值</th>" in html
    assert "<th>進場成本／成交</th>" in html
    assert "<th>現在市價／該檔盈虧</th>" in html
    assert "<th>損益／模式總權益</th>" in html
    assert 'class="skip-link"' in html
    assert 'aria-label="公開面板導覽"' in html
    assert 'id="overview-kpis"' in html
    assert 'id="reset-filters"' in html
    assert "function renderOverview(data)" in javascript
    assert "13:24 市價重試後有殘餘，已轉 13:25 集合競價" in javascript
    assert "13:20/13:24/13:25 退出" in javascript
    assert "目前訊號目標" in javascript
    assert "方向平衡後" in javascript
    assert "雙向整張不足・保持空倉" in javascript
    assert "四模式已實現" in javascript
    assert "四模式未實現" in javascript
    assert "已實現＋未實現，已與總權益對帳" in javascript
    assert "未實現淨清算損益" in javascript
    assert "已實現 ${money(realizedNet)}" in javascript
    assert "未實現 ${money(unrealizedNet)}" in javascript
    assert "reconciled_total_net_pnl_twd" in javascript
    assert "function resolvedPositionPnl(row = {})" in javascript
    assert "進場名目 ${money(entryNotional)}" in javascript
    assert "佔該模式總權益" in javascript
    assert (
        '模式總權益 ${Number.isFinite(modeTotalEquity) ? summaryMoney(modeTotalEquity) : "—"}'
        in javascript
    )
    assert "Number(positionPnl.total) / modeTotalEquity * 100" in javascript
    assert "Number(positionPnl.total) / totalPortfolioPnl * 100" not in javascript
    assert "未成交・無進場成本" in javascript
    assert "所選交易日當沖資格未完整覆蓋" in javascript
    assert "較晚補齊的資料不會回填成假成交" in javascript
    assert "原子指標由 inotify 事件即時喚醒" in javascript
    assert 'id="latency-kpis"' in html
    assert "今日尚無開盤樣本" in javascript
    assert "這不是券商回報或交易所往返時間" in html
    assert javascript.count("const requestDate = selectedDate();") == 2
    assert "params = new URLSearchParams({\n    date: requestDate," in javascript
    assert '$("date-filter").addEventListener("change"' in javascript
    assert "四模式盤前預熱測速（不等同該日執行完成）" in html
    assert "依 |目標權重| 由大到小" in html
    assert "const PRICE_REFRESH_MS = 60000" in javascript
    assert "window.setInterval(() => { void refresh(); void loadChartHistory(); }, PRICE_REFRESH_MS)" in javascript
    assert 'id="equity-time-range"' in html
    assert 'id="chart-legend"' in html
    assert 'aria-label="曲線顯示開關"' in html
    assert 'data-range="1y"' in html
    assert 'data-range="all"' in html
    assert "HIDDEN_EQUITY_SERIES_STORAGE_KEY" in javascript
    assert "HISTORY_CLIENT_CACHE_MS" in javascript
    assert "chartHistoryCache" in javascript
    assert "response.status === 429" not in javascript
    assert "秒後自動重試" not in javascript
    assert 'button[data-series-id]' in javascript
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


def test_counterfactual_open_replay_records_every_entry_at_session_open(
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
            "entry_fill_contract": (
                "retrospective_actual_session_open_price_counterfactual"
            ),
            "entry_liquidity_assumption": "counterfactual_unbounded",
        }
    )
    open_at = _now(9, 0)
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
    quote["quote_at"] = _now(9, 0).isoformat()

    assert (
        engine.register_signal(
            spec=spec,
            summary=_summary(),
            signal_rows=[_row()],
            quotes={"2330": quote},
            eligibility=_eligibility(),
            eligibility_coverage={},
            now=_now(9, 0),
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
            now=_now(9, 0),
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
        },
        {
            "session_date": "2026-08-13",
            "market": "mode_a",
            "symbol": "2317",
            "name": "鴻海",
            "target_weight": -0.3,
            "status": "missing_quote",
        },
        {
            "session_date": "2026-08-13",
            "market": "mode_a",
            "symbol": "2454",
            "name": "聯發科",
            "target_weight": 0.2,
            "status": "partial_depth",
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
