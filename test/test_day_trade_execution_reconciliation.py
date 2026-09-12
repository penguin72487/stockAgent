"""Executable-policy regressions for the 2026-09-10 YTD reconciliation."""
from dataclasses import replace
from datetime import date, datetime
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from stockagent.live.signal_engine import _day_trade_model_eligibility
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
    EXECUTION_REALISM_CONTRACT,
    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
    TwDayTradeSimulationEngine,
    load_live_eligibility,
)
from test_tw_day_trade_simulation import _spec, _row, _quote, _eligibility, _summary, _now, TAIPEI
from stockagent.data.panel import PanelData


def _panel() -> PanelData:
    return PanelData(
        dates=np.array(["2026-08-12", "2026-08-13"], dtype="datetime64[D]"),
        symbols=["2330", "6733"], feature_names=["base"],
        features=np.zeros((2, 2, 1), dtype=np.float32),
        returns_1d=np.zeros((2, 2), dtype=np.float32),
        tradable_mask=np.zeros((2, 2), dtype=bool),
        alive_mask=np.zeros((2, 2), dtype=bool),
        benchmark_returns=np.zeros(2, dtype=np.float32),
        close_prices=np.ones((2, 2), dtype=np.float32),
        day_trade_eligible_mask=np.array([[False, True], [True, False]]),
        day_trade_can_sell_open_mask=np.array([[False, True], [False, False]]),
    )


def test_policy_uses_exact_session_not_alive_or_future_fill_masks(tmp_path):
    panel = _panel()
    mask, short, proof = _day_trade_model_eligibility(
        panel, session_date="2026-08-13", parquet_root=tmp_path,
    )
    np.testing.assert_array_equal(mask, panel.day_trade_eligible_mask[1])
    assert mask.tolist() == [True, False]  # old live alive/quote mask was different
    assert short.tolist() == [False, False]  # side veto must not remove policy token
    assert proof["session_date"] == "2026-08-13"
    mask[:] = False
    assert panel.day_trade_eligible_mask[1, 0]  # no mutation of cached input


def _rules(root: Path, venue: str, symbol: str):
    pl.DataFrame({
        "date": ["2026-08-13", "2026-08-14"], "證券代號": [symbol, symbol],
        "暫停現股賣出後現款買進當沖註記": ["", ""],
    }).write_parquet(root / f"{venue}_day_trade_eligibility.parquet")


def test_historical_exact_rule_rows_are_not_replaced_by_latest(tmp_path):
    spec = _spec(tmp_path)
    for venue in ("twse", "tpex"):
        _rules(tmp_path, venue, "2330")
    kwargs = dict(rule_data_dir=tmp_path, parquet_root=spec.parquet_root,
                  symbols=["2330"], trading_date=date(2026, 8, 13))
    rules, _ = load_live_eligibility(**kwargs, require_latest=False)
    assert rules["2330"].eligible
    assert rules["2330"].source_date == "2026-08-13"
    strict, _ = load_live_eligibility(**kwargs)  # existing readiness contract retained
    assert not strict["2330"].covered


def test_rule_cache_invalidates_on_atomic_source_repair(tmp_path, monkeypatch):
    spec = _spec(tmp_path)
    monkeypatch.setenv("STOCKAGENT_TW_DAY_TRADE_RULE_DATA_DIR", str(tmp_path))
    panel = _panel()
    panel.day_trade_eligible_mask = None
    for venue in ("twse", "tpex"):
        _rules(tmp_path, venue, "2330")
    kwargs = dict(session_date="2026-08-13", parquet_root=spec.parquet_root)
    first, _, _ = _day_trade_model_eligibility(panel, **kwargs)
    assert first.tolist() == [True, False]
    _rules(tmp_path, "twse", "9999")
    second, _, _ = _day_trade_model_eligibility(panel, **kwargs)
    assert not second.any()
    with pytest.raises(RuntimeError, match="eligibility unavailable"):
        _day_trade_model_eligibility(panel, session_date="2026-08-15", parquet_root=spec.parquet_root)


@pytest.mark.parametrize("pnl, expected", [(2_000_000.0, 6000), (-2_000_000.0, 4000)])
def test_sizing_inherits_profit_and_loss_without_changing_initial_capital(tmp_path, pnl, expected):
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    mode = engine._mode(spec)
    mode["cumulative_realized_net_pnl_twd"] = pnl
    result = engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(.5)],
        quotes={"2330": _quote(ask_volume=100, minute_volume_lots=100)},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7))
    assert result == "registered"
    assert mode["initial_capital_twd"] == 10_000_000
    assert mode["session_sizing_nav_twd"] == 10_000_000 + pnl
    position = next(iter(mode["positions"].values()))
    assert position["requested_shares"] == expected
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    assert restarted.state["modes"][spec.market]["session_sizing_nav_twd"] == 10_000_000 + pnl
    record = json.loads(engine.signals_path.read_text().splitlines()[0])
    assert record["filled_weight"] == pytest.approx(position["filled_shares"] * position["entry_price"] / (10_000_000 + pnl))


def test_ruined_account_never_resets_to_initial_capital(tmp_path):
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine._mode(spec)["cumulative_realized_net_pnl_twd"] = -10_000_000
    result = engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row()],
        quotes={"2330": _quote()}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7))
    assert result == "blocked"
    assert engine.state["modes"][spec.market]["blocked_reason"] == "nonpositive_account_nav"


@pytest.mark.parametrize("volume, filled", [(5.0, 2000), (None, 0), (0.0, 0)])
def test_0901_price_is_not_unlimited_liquidity(tmp_path, volume, filled):
    spec = replace(_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    summary = _summary() | {"simulation_replay": True, "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE}
    quote = _quote(ask_volume=999999, minute_volume_lots=volume) | {
        "execution_price_0901": 1000.0, "execution_price_0901_method": "minute_vwap",
        "quote_at": _now(9, 1).isoformat(),
    }
    assert engine.register_signal(spec=spec, summary=summary, signal_rows=[_row(.5)],
        quotes={"2330": quote}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1),
        counterfactual_open_replay=True) == "registered"
    record = json.loads(engine.signals_path.read_text().splitlines()[0])
    assert record["requested_shares"] == 5000
    assert record["filled_shares"] == filled
    assert record["minute_kbar_capacity_shares"] == filled
    assert record["execution_realism_contract"] == EXECUTION_REALISM_CONTRACT


def test_budget_includes_fees_and_does_not_credit_short_proceeds(tmp_path):
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    assert engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(-1.0)],
        quotes={"2330": _quote(bid=1000, bid_volume=100, minute_volume_lots=100)},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7)) == "registered"
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["requested_shares"] == 10000
    assert position["filled_shares"] == 9000
    assert position["filled_shares"] * position["entry_price"] + position["entry_gross_fee_and_tax_twd"] <= 10_000_000


@pytest.mark.parametrize("weight", [.1, -.1])
def test_no_liquidity_preserves_delivery_obligation_instead_of_fake_flat(tmp_path, weight):
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(weight)],
        quotes={"2330": _quote()}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7))
    engine.process_quotes(quotes={"2330": _quote(minute_volume_lots=0) | {"quote_at": _now(13, 30).isoformat()}}, now=_now(13, 30))
    mode = engine.state["modes"][spec.market]
    assert mode["open_position_count"] == 1
    assert mode["force_exit_failures"] == 1
    assert not mode["execution_evidence_complete"]
    fills = [json.loads(line) for line in engine.fills_path.read_text().splitlines()]
    assert len(fills) == 1
    assert not any(f.get("synthetic_terminal_ledger") for f in fills)
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    assert restarted.state["modes"][spec.market]["open_position_count"] == 1
    following = datetime(2026, 8, 14, 9, 1, 7, tzinfo=TAIPEI)
    assert restarted.register_signal(spec=spec, summary=_summary("next") | {"generated_at": following.isoformat()},
        signal_rows=[_row()], quotes={"2330": _quote()}, eligibility=_eligibility(), eligibility_coverage={}, now=following) == "blocked"


def test_zero_fill_archive_invalidates_old_strategy_rows(tmp_path):
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    mode = {"market": "tw_day_trade", "session_date": "2026-08-13", "signal_id": "new", "positions": {}}
    path = engine.position_history_dir / "2026-08-13" / "tw_day_trade.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"positions": [{"symbol": "OLD"}]}))
    engine._archive_mode_positions(mode, archived_at=_now(13, 30))
    assert json.loads(path.read_text())["positions"] == []
    assert json.loads(path.read_text())["signal_id"] == "new"


def test_replay_promotion_rejects_excess_capacity(tmp_path):
    from scripts.promote_tw_day_trade_replay import _validate_0901_minute_price_signal_ledger
    (tmp_path / "signals.jsonl").write_text(json.dumps({
        "entry_fill_policy": ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        "filled_shares": 3000, "minute_kbar_volume_lots": 4.0,
        "sizing_nav_twd": 10000000, "filled_weight_basis": "session_start_account_nav",
        "recorded_at": _now(9, 1).isoformat(), "execution_price": 1000,
        "sizing_open_price": 1000, "counterfactual_0901_price_fill": True,
        "counterfactual_open_price_fill": False, "synthetic_fill": False,
        "synthetic_fallback_fill": False, "paper_market_fill": False,
        "entry_price_source": "fixture_0901_minute_vwap",
    }) + "\n")
    failures = []
    _validate_0901_minute_price_signal_ledger(tmp_path, expected_fills=1, failures=failures, require_capacity=True)
    assert any("exceeded v3 NAV/liquidity proof" in x for x in failures)


def test_duplicate_signal_does_not_double_consume_liquidity(tmp_path):
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    result = engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(), _row()],
        quotes={"2330": _quote()}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7))
    assert result == "blocked"
    assert engine.state["modes"][spec.market]["blocked_reason"] == "duplicate_signal_symbols"
    assert not engine.fills_path.exists()


def test_1324_submission_uses_following_right_labelled_minute(tmp_path):
    from scripts.rebuild_tw_day_trade_open_price_replay import _replay_historical_intraday
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(.1)],
        quotes={"2330": _quote()}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7))
    bars = {"2330": {}}
    for h, m, price in [(13, 20, 1000.), (13, 24, 1000.), (13, 25, 990.), (13, 30, 980.)]:
        bars["2330"][_now(h, m).isoformat(timespec="minutes")] = {
            "open": price, "close": price, "high": price, "low": price,
            "vwap": price, "volume_shares": 10000.,
        }
    _replay_historical_intraday(engine, markets=[spec.market], bars=bars, trading_date=date(2026, 8, 13))
    fills = [json.loads(line) for line in engine.fills_path.read_text().splitlines()]
    exits = [f for f in fills if f.get("purpose") != "entry"]
    assert len(exits) == 1
    assert exits[0]["price"] == 990.0
    assert exits[0]["fill_at"] == _now(13, 25).isoformat()
    marks = [json.loads(line) for line in engine.marks_path.read_text().splitlines()]
    assert len(marks) == 270
