from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pytest

from stockagent.backtest.tw_execution import TaiwanFeeSchedule
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.live.tw_day_trade_dashboard import (
    _line_count,
    _tail,
    build_dashboard_signal_page,
    build_dashboard_snapshot,
)
from stockagent.live.tw_day_trade_simulation import (
    DAY_TRADE_SHORTFALL_BORROW_FEE_RATE,
    DAY_TRADE_SHORTFALL_HANDLING_FEE_FRACTION,
    LiveEligibility,
    MARGIN_FINANCING_ANNUAL_RATE,
    MARGIN_FINANCING_RATIO,
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
    }


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


def test_entry_waits_until_first_minute_is_complete(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
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

    assert result == "waiting_first_minute"
    assert not engine.state["modes"][spec.market]["positions"]


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
    assert position["take_profit_price"] == 1_100.0
    assert position["stop_trigger_price"] == 900.0


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


def test_cumulative_snapshots_only_create_adjacent_minute_volume(tmp_path: Path) -> None:
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
                    "tw_day_trade": {
                        "positions": {"legacy": {"signed_shares": 1_000}}
                    }
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
    assert mode["engine_status"] == (
        "critical_legacy_position_requires_reconciliation"
    )


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
            "2330": _quote(
                open_price=100.0, bid=99.0, ask=100.0, ask_volume=1.0
            ),
            "2317": _quote(
                open_price=100.0, bid=100.0, ask=101.0, bid_volume=20.0
            ),
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
    assert signal["reason"] == "minute_or_top_book_volume_unavailable"


def test_second_signal_cannot_overwrite_an_open_position(tmp_path: Path) -> None:
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
    assert (
        engine.state["modes"][spec.market]["blocked_reason"]
        == "daily_signal_already_consumed"
    )


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
    assert engine.state["modes"][spec.market]["force_exit_failures"] == 1

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
    exit_fills = [
        row
        for row in (
            json.loads(line) for line in engine.fills_path.read_text().splitlines()
        )
        if row["purpose"]
        in {"13_24_market_force_exit", "13_30_closing_auction_fill"}
    ]
    assert [row["quantity"] for row in exit_fills] == [1_000, 2_000]


@pytest.mark.parametrize("target_weight", [0.1, -0.1])
def test_1330_residual_is_reclassified_with_conservative_carry_cost(
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

    assert abs(position["signed_shares"]) == 1_000
    if target_weight > 0:
        assert position["carry_type"] == "margin_financing_long"
        expected_principal = 1_000 * 1_000.0 * MARGIN_FINANCING_RATIO
        assert position["financing_principal_twd"] == expected_principal
        assert position["carry_cost_twd"] == pytest.approx(
            expected_principal * MARGIN_FINANCING_ANNUAL_RATE / 365.0
        )
    else:
        assert position["carry_type"] == "day_trade_securities_shortfall"
        expected_borrow = (
            1_000 * 1_000.0 * DAY_TRADE_SHORTFALL_BORROW_FEE_RATE
        )
        assert position["shortfall_borrow_fee_twd"] == expected_borrow
        assert position["shortfall_handling_fee_twd"] == pytest.approx(
            expected_borrow * DAY_TRADE_SHORTFALL_HANDLING_FEE_FRACTION
        )
        assert position["normal_sell_tax_adjustment_twd"] > 0.0
    assert engine.state["modes"][spec.market]["engine_status"] == (
        "critical_residual_carried_after_13_30"
    )


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

    assert payload["schema_version"] == 3
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
    assert 'fetch("api/status"' in javascript
    assert "fetch(`api/signals?${params.toString()}`" in javascript
    assert "function renderOperations(data)" in javascript
    assert "function compareByAbsoluteWeight(a, b)" in javascript
    assert javascript.count(".sort(compareByAbsoluteWeight)") == 1
    assert "refreshInFlight" in javascript
    assert "SIGNAL_PAGE_SIZE" in javascript
    assert "const sourceNumber" in javascript
    assert "maximumSignificantDigits: 15" in javascript
    assert "const monetaryNumber" in javascript
    assert "maximumFractionDigits: 8" in javascript
    assert "number(row.bid,2)" not in javascript
    assert "number(row.raw_score ?? row.score,5)" not in javascript
    assert "(pnl / initial * 100).toFixed(3)" not in javascript
    assert 'id="load-more-signals"' in html
    assert 'id="load-more-positions"' in html
    assert 'id="signal-direction-summary"' in html
    assert 'class="skip-link"' in html
    assert 'aria-label="公開面板導覽"' in html
    assert 'id="overview-kpis"' in html
    assert 'id="reset-filters"' in html
    assert "function renderOverview(data)" in javascript
    assert "13:24 強平後仍有未平部位" in javascript
    assert "目前訊號目標" in javascript
    assert "方向平衡後" in javascript
    assert "今日當沖資格未完整覆蓋" in javascript
    assert "較晚補齊的資料不會回填成假成交" in javascript
    assert "依 |目標權重| 由大到小" in html
    assert "window.setInterval(refresh, REFRESH_MS)" in javascript


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
