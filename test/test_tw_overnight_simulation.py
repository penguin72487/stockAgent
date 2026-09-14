from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from stockagent.backtest.tw_execution import TaiwanFeeSchedule
from stockagent.live.tw_day_trade_dashboard import build_dashboard_snapshot
from stockagent.live.tw_day_trade_service_sync import load_service_sync
from stockagent.live.tw_day_trade_simulation import ModeSpec
from stockagent.live.tw_overnight_simulation import (
    DELAYED_CLOSE_DEADLINE,
    OPEN_OBSERVATION_DEADLINE,
    TwOvernightSimulationEngine,
    _auction_print,
)


TAIPEI = ZoneInfo("Asia/Taipei")


def _at(day: int, hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, day, hour, minute, second, tzinfo=TAIPEI)


def _spec(tmp_path: Path) -> ModeSpec:
    checkpoint = tmp_path / "checkpoint_best.pt"
    checkpoint.write_bytes(b"checkpoint")
    parquet_root = tmp_path / "stocks"
    parquet_root.mkdir()
    return ModeSpec(
        market="tw_overnight_unit",
        label="隔日沖測試",
        initial_capital_twd=10_000_000.0,
        config_path="config.yaml",
        checkpoint_path=str(checkpoint),
        parquet_root=parquet_root,
        live_output_dir=tmp_path / "signals",
        fee_schedule=TaiwanFeeSchedule(commission_discount=0.2),
        lot_size=1_000,
    )


def _summary() -> dict[str, object]:
    return {
        "signal_id": "close-signal-1",
        "generated_at": _at(9, 13, 20, 1).isoformat(),
        "signal_ready_at": _at(9, 13, 20, 1).isoformat(),
        "signal_started_at": _at(9, 13, 20, 0).isoformat(),
        "execution_mode": "tw_day_trade",
        "live_session_latest_quote_feature_applied": True,
        "feature_cutoff_date": "2026-09-09 13:20:00",
        "checkpoint_fingerprint": "checkpoint-sha",
        "config_fingerprint": "config-sha",
        "weights_path": "artifacts/live_signals/unit/target_weights.parquet",
        "symbol_count": 1,
        "target_risk": {"gross": 0.02, "long_gross": 0.02, "short_gross": 0.0},
    }


def _row() -> dict[str, object]:
    return {
        "symbol": "2330",
        "name": "台積電",
        "target_weight": 0.02,
        "score": 1.0,
        "raw_score": 2.0,
        "current_price": 100.0,
        "tradable": True,
        "can_buy": True,
        "overnight_can_short_open": False,
    }


def _quote(
    *,
    day: int,
    hour: int,
    minute: int,
    last: float = 101.0,
    open_price: float = 103.0,
    simtrade: bool = False,
    lower: float = 90.0,
    upper: float = 110.0,
) -> dict[str, object]:
    observed = _at(day, hour, minute)
    return {
        "last": last,
        "open": open_price,
        "bid": last - 0.5,
        "ask": last + 0.5,
        "lower_limit": lower,
        "upper_limit": upper,
        "quote_at": observed.isoformat(),
        "exchange_quote_at": observed.isoformat(),
        "simtrade": simtrade,
    }


def _jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_close_to_next_open_lifecycle_rejects_both_trial_matches(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwOvernightSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_at(9, 13, 15))

    assert engine.register_close_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote(day=9, hour=13, minute=25, simtrade=True)},
        security_types={"2330": "stock"},
        now=_at(9, 13, 25, 2),
    ) == "registered"
    mode = engine.state["modes"][spec.market]
    order = next(iter(mode["pending_entry_orders"].values()))
    assert order["price"] == 110.0
    assert order["order_type"] == "LMT_ROD"
    assert mode["positions"] == {}

    engine.process_quotes(
        quotes={"2330": _quote(day=9, hour=13, minute=29, simtrade=True)},
        now=_at(9, 13, 30),
    )
    assert mode["positions"] == {}
    assert order["status"] == "working"

    engine.process_quotes(
        quotes={"2330": _quote(day=9, hour=13, minute=30, last=101.0)},
        now=_at(9, 13, 30, 1),
    )
    position = next(iter(mode["positions"].values()))
    assert position["entry_price"] == 101.0
    assert position["entry_price_source"] == "actual_close_auction_print"
    assert position["signed_shares"] == 2_000
    assert mode["engine_status"] == "carrying_to_next_open"

    engine.process_quotes(
        quotes={"2330": _quote(day=10, hour=8, minute=40, simtrade=True)},
        now=_at(10, 8, 40),
    )
    assert position["opening_exit_order_status"] == "working"
    assert position["opening_exit_limit_price"] == 90.0
    assert position["signed_shares"] == 2_000

    engine.process_quotes(
        quotes={"2330": _quote(day=10, hour=9, minute=0, open_price=103.0)},
        now=_at(10, 9, 0, 1),
    )
    assert position["opening_exit_order_status"] == "filled"
    assert position["exit_price"] == 103.0
    assert position["exit_session_date"] == "2026-09-10"
    assert position["signed_shares"] == 0
    assert mode["engine_status"] == "flat_after_next_open"

    fills = _jsonl(engine.fills_path)
    assert [row["purpose"] for row in fills] == [
        "close_auction_entry",
        "next_session_open_auction_fill",
    ]
    assert fills[1]["session_date"] == "2026-09-10"
    assert fills[1]["exchange_match_at"].startswith("2026-09-10T09:00")
    # The ordinary overnight stock sell tax is 0.3%, not the day-trade 0.15%.
    assert position["sell_fee_rate"] > 0.003


def test_1300_switch_1320_decision_and_1330_order_deadline(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwOvernightSimulationEngine(tmp_path / "state")

    engine.update_readiness([spec], now=_at(9, 12, 59, 59))
    assert engine.state["modes"][spec.market]["engine_status"] == (
        "waiting_13_00_switch"
    )
    engine.update_readiness([spec], now=_at(9, 13, 0))
    assert engine.state["modes"][spec.market]["engine_status"] == (
        "armed_waiting_13_20_calculation"
    )

    early = {**_summary(), "signal_id": "early"}
    early["signal_ready_at"] = _at(9, 13, 19, 59).isoformat()
    assert engine.register_close_signal(
        spec=spec,
        summary=early,
        signal_rows=[_row()],
        quotes={"2330": _quote(day=9, hour=13, minute=20)},
        now=_at(9, 13, 20),
    ) == "blocked"
    assert engine.state["modes"][spec.market]["blocked_reason"] == (
        "signal_before_13_20_decision_gate"
    )

    late = {**_summary(), "signal_id": "late"}
    assert engine.register_close_signal(
        spec=spec,
        summary=late,
        signal_rows=[_row()],
        quotes={"2330": _quote(day=9, hour=13, minute=29)},
        now=_at(9, 13, 30),
    ) == "blocked"
    assert engine.state["modes"][spec.market]["blocked_reason"] == (
        "outside_13_20_close_order_window"
    )

    on_time = {**_summary(), "signal_id": "on-time"}
    assert engine.register_close_signal(
        spec=spec,
        summary=on_time,
        signal_rows=[_row()],
        quotes={"2330": _quote(day=9, hour=13, minute=29)},
        now=_at(9, 13, 29, 59),
    ) == "registered"
    signal = _jsonl(engine.signals_path)[-1]
    assert signal["decision_clock"] == "13:20 Asia/Taipei"
    assert signal["sizing_price_at_decision"] == 100.0
    assert "sizing_price_at_13_25" not in signal


def test_missing_full_short_inventory_blocks_instead_of_shrinking(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwOvernightSimulationEngine(tmp_path / "state")
    row = {
        **_row(),
        "target_weight": -0.02,
        "overnight_can_short_open": True,
        "overnight_short_capacity_shares": 1_000,
    }

    assert engine.register_close_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[row],
        quotes={"2330": _quote(day=9, hour=13, minute=25)},
        now=_at(9, 13, 25, 2),
    ) == "registered"

    mode = engine.state["modes"][spec.market]
    assert mode["pending_entry_orders"] == {}
    signal = _jsonl(engine.signals_path)[0]
    assert signal["requested_shares"] == 2_000
    assert signal["status"] == "blocked"
    assert signal["reason"] == "short_inventory_missing_or_below_full_model_target"


def test_auction_print_requires_non_simulated_same_session_exchange_time() -> None:
    regular = _quote(day=9, hour=13, minute=30)
    assert _auction_print(
        regular,
        session_date="2026-09-09",
        not_before=_at(9, 13, 30).time(),
        price_field="last",
    ) is not None
    assert _auction_print(
        {**regular, "simtrade": True},
        session_date="2026-09-09",
        not_before=_at(9, 13, 30).time(),
        price_field="last",
    ) is None
    assert _auction_print(
        {key: value for key, value in regular.items() if key != "simtrade"},
        session_date="2026-09-09",
        not_before=_at(9, 13, 30).time(),
        price_field="last",
    ) is None
    assert _auction_print(
        {**regular, "exchange_quote_at": _at(10, 13, 30).isoformat()},
        session_date="2026-09-09",
        not_before=_at(9, 13, 30).time(),
        price_field="last",
    ) is None
    assert _auction_print(
        {**regular, "exchange_quote_at": _at(9, 13, 34).isoformat()},
        session_date="2026-09-09",
        not_before=_at(9, 13, 30).time(),
        not_after=DELAYED_CLOSE_DEADLINE,
        price_field="last",
    ) is None


def test_close_print_after_delayed_close_deadline_is_not_a_fill(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwOvernightSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_at(9, 13, 15))
    assert engine.register_close_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote(day=9, hour=13, minute=25)},
        now=_at(9, 13, 25, 2),
    ) == "registered"

    engine.process_quotes(
        quotes={"2330": _quote(day=9, hour=13, minute=34)},
        now=_at(9, 13, 34),
    )

    mode = engine.state["modes"][spec.market]
    order = next(iter(mode["pending_entry_orders"].values()))
    assert order["status"] == "expired_without_actual_close_print"
    assert mode["positions"] == {}
    assert mode["engine_status"] == "critical_actual_close_print_missing"
    engine.update_readiness([spec], now=_at(9, 13, 35))
    assert mode["engine_status"] == "critical_actual_close_print_missing"


def test_open_order_requires_current_session_quote_and_missed_open_is_visible(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwOvernightSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=_at(9, 13, 15))
    assert engine.register_close_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row()],
        quotes={"2330": _quote(day=9, hour=13, minute=25)},
        now=_at(9, 13, 25, 2),
    ) == "registered"
    engine.process_quotes(
        quotes={"2330": _quote(day=9, hour=13, minute=30)},
        now=_at(9, 13, 30),
    )
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))

    engine.process_quotes(
        quotes={"2330": _quote(day=9, hour=8, minute=40, simtrade=True)},
        now=_at(10, 8, 40),
    )
    assert position["opening_exit_order_status"] == "waiting_current_session_limits"
    assert position.get("opening_exit_order_id") is None

    engine.process_quotes(
        quotes={"2330": _quote(day=10, hour=8, minute=41, simtrade=True)},
        now=_at(10, 8, 41),
    )
    assert position["opening_exit_order_status"] == "working"
    assert position["opening_exit_order_session_date"] == "2026-09-10"

    engine.process_quotes(
        quotes={"2330": _quote(day=10, hour=9, minute=11)},
        now=_at(10, 9, 11),
    )
    assert _at(10, 9, 11).time() > OPEN_OBSERVATION_DEADLINE
    assert position["opening_exit_order_status"] == "missed_actual_opening_print"
    assert position["signed_shares"] == 2_000
    assert engine.state["modes"][spec.market]["engine_status"] == (
        "critical_actual_opening_print_missing"
    )

    engine.process_quotes(
        quotes={"2330": _quote(day=11, hour=8, minute=40, simtrade=True)},
        now=_at(11, 8, 40),
    )
    assert position["opening_exit_order_status"] == "working"
    assert position["opening_exit_order_session_date"] == "2026-09-11"


def test_dashboard_uses_explicit_discord_status_for_overnight_ack(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    engine = TwOvernightSimulationEngine(tmp_path / "state")
    observed = _at(9, 13, 15)
    engine.update_readiness([spec], now=observed)
    receipt = load_service_sync(engine.state_dir)
    assert receipt is not None
    bot_status = tmp_path / "discord" / "service_status.json"
    bot_status.parent.mkdir()
    bot_status.write_text(
        json.dumps(
            {
                "updated_at": observed.isoformat(),
                "discord_connected": True,
                "overnight_engine_state_revision": receipt["state_revision"],
                "overnight_markets": [spec.market],
            }
        ),
        encoding="utf-8",
    )

    snapshot = build_dashboard_snapshot(
        state_dir=engine.state_dir,
        discord_service_status_path=bot_status,
        discord_markets_field="overnight_markets",
        discord_engine_revision_field="overnight_engine_state_revision",
        now=observed.astimezone(ZoneInfo("UTC")),
    )

    assert snapshot["service_sync"]["synchronized"] is True
    assert snapshot["service_sync"]["discord"]["markets"] == [spec.market]
    assert snapshot["service_sync"]["session_clock"]["rollover_local_time"] == (
        "13:00"
    )
    assert snapshot["session_progress"]["phase"] == "armed_waiting_calculation"


def test_close_signal_latency_uses_1320_gate_and_separate_stages(
    tmp_path: Path,
) -> None:
    engine = TwOvernightSimulationEngine(tmp_path / "state")
    summary = {
        **_summary(),
        "artifact_published_at": _at(9, 13, 20, 1).isoformat(),
        "live_latency": {
            "quote_fetch_ms": 20.0,
            "model_inference_ms": 30.0,
            "artifact_publish_ms": 4.0,
        },
    }
    engine.record_close_signal_latency_sample(
        market="tw_overnight_unit",
        signal_id="close-signal-1",
        result="registered",
        summary=summary,
        consumer_detected_at=_at(9, 13, 20, 1),
        ledger_persisted_at=_at(9, 13, 20, 2),
        executor_quote_fetch_ms=12.0,
        security_metadata_load_ms=2.0,
        ledger_compute_persist_ms=5.0,
        shared_quote_batch_mode_count=4,
    )

    row = _jsonl(engine.latency_path)[0]
    assert row["decision_clock"] == "13:20 close-auction target"
    assert row["decision_gate_to_ledger_ms"] == 2_000.0
    assert row["input_to_ledger_ms"] == 2_000.0
    assert row["ready_to_ledger_ms"] == 1_000.0
    assert row["shared_quote_batch_mode_count"] == 4
    assert row["stages"]["executor_shared_quote_fetch_ms"] == 12.0
