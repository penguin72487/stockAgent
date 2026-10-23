"""Entry-only TP/SL admission; raw decisions and committed fills remain facts."""

from dataclasses import replace
from datetime import date
import json
import math

import pytest

from scripts.complete_tw_day_trade_paper_entry import make_plan
from stockagent.live.tw_day_trade_dashboard import build_dashboard_signal_page
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
    ENTRY_FILL_POLICY_CAUSAL_BOOK,
    ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET,
    ENTRY_PRICE_GUARD_CONTRACT,
    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
    TwDayTradeSimulationEngine,
    _prepare_entry_plan,
    entry_bracket_prices,
    entry_price_guard_reason,
)
from test_tw_day_trade_simulation import _eligibility, _now, _quote, _row, _spec, _summary


def _rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


@pytest.mark.parametrize("side,tp,sl", [("long", 1100., 900.), ("short", 900., 1100.)])
@pytest.mark.parametrize("boundary", ["tp", "sl"])
@pytest.mark.parametrize("distance", [0., 1., -1.])
def test_guard_uses_strict_inequalities(side, tp, sl, boundary, distance):
    edge = tp if boundary == "tp" else sl
    # Positive distance is outside; negative distance is inside.
    direction = 1 if edge == 1100. else -1
    reason = entry_price_guard_reason(
        side=side, price=edge + direction * distance,
        take_profit_price=tp, stop_trigger_price=sl,
    )
    expected = "take_profit" if boundary == "tp" else "stop_loss"
    assert reason == (f"entry_price_at_or_beyond_{expected}" if distance >= 0 else None)


@pytest.mark.parametrize("tp,sl,reason", [
    (None, 900., "entry_bracket_unavailable"),
    (1100., float("nan"), "entry_bracket_unavailable"),
    (float("inf"), 900., "entry_bracket_unavailable"),
    (900., 1100., "entry_bracket_invalid"),
    (1000., 1000., "entry_bracket_invalid"),
])
def test_invalid_brackets_fail_closed(tp, sl, reason):
    assert entry_price_guard_reason(
        side="long", price=1000., take_profit_price=tp, stop_trigger_price=sl,
    ) == reason


def test_guard_never_rounds_an_outside_float_into_the_interval():
    assert entry_price_guard_reason(
        side="long", price=math.nextafter(1100., math.inf),
        take_profit_price=1100., stop_trigger_price=900.,
    ) == "entry_price_at_or_beyond_take_profit"
    assert entry_price_guard_reason(
        side="long", price=math.nextafter(1100., 0.),
        take_profit_price=1100., stop_trigger_price=900.,
    ) is None


@pytest.mark.parametrize("policy", [ENTRY_FILL_POLICY_CAUSAL_BOOK, ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET])
@pytest.mark.parametrize("weight", [.3, -.3])
@pytest.mark.parametrize("price", [900., 901., 1095., 1100.])
def test_all_live_modes_skip_entry_at_or_outside_original_bracket(tmp_path, policy, weight, price):
    spec = replace(_spec(tmp_path), entry_fill_policy=policy, price_limit_offset_ticks=1)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = {**_quote(bid=price, ask=price, last=1000.), "simtrade": False}
    assert engine.register_signal(
        spec=spec, summary=_summary(), signal_rows=[_row(weight)],
        quotes={"2330": quote}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7),
    ) == "registered"
    mode = engine.state["modes"][spec.market]
    assert mode["positions"] == mode["pending_entry_orders"] == {}
    assert not _rows(engine.fills_path)
    signal = _rows(engine.signals_path)[0]
    assert signal["target_weight"] == weight and signal["requested_shares"] == 3000
    assert signal["status"] == "skipped" and signal["filled_shares"] == 0
    assert signal["raw_score"] == _row()["raw_score"]
    assert signal["entry_price_guard_contract"] == ENTRY_PRICE_GUARD_CONTRACT
    assert signal["reason"] in mode["entry_price_guard_rejection_counts"]
    assert mode["engine_status"] == "flat_entry_price_guard"
    # A later inside quote may not silently resurrect the skipped signal.
    engine.process_quotes(quotes={"2330": {**_quote(), "quote_at": _now(9, 2).isoformat()}}, now=_now(9, 2))
    assert not _rows(engine.fills_path)


@pytest.mark.parametrize("weight", [.3, -.3])
def test_inside_entry_uses_executable_side_not_last_or_midpoint(tmp_path, weight):
    spec = replace(_spec(tmp_path), price_limit_offset_ticks=1)
    price_key = "ask" if weight > 0 else "bid"
    quote = {**_quote(last=900. if weight > 0 else 1100.), price_key: 1000.}
    plan = _prepare_entry_plan(_row(weight), quote=quote, evidence=_eligibility()["2330"],
        signal_at=_now(9, 0, 5), observation_at=_now(9, 1, 7), spec=spec)
    assert plan["filled_shares"] == 3000
    assert plan["entry_price"] == 1000.
    # A harmless last/mid does not excuse a crossing executable-side price.
    quote.update(last=1000., **{price_key: 1100. if weight > 0 else 900.})
    plan = _prepare_entry_plan(_row(weight), quote=quote, evidence=_eligibility()["2330"],
        signal_at=_now(9, 0, 5), observation_at=_now(9, 1, 7), spec=spec)
    assert plan["filled_shares"] == 0
    assert plan["reason"] == "entry_price_at_or_beyond_take_profit"


@pytest.mark.parametrize("weight", [.3, -.3])
@pytest.mark.parametrize("full_target_retry", [False, True])
def test_partial_retry_cancels_only_remainder_and_survives_restart(tmp_path, weight, full_target_retry):
    spec = replace(_spec(tmp_path), price_limit_offset_ticks=1)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = {**_quote(bid=1000., ask_volume=1., bid_volume=1.),
             "quote_at": _now(9, 0, 6).isoformat(), "simtrade": False}
    engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(weight)],
        quotes={"2330": quote}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 0, 7))
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert position["filled_shares"] == 1000
    before = engine.fills_path.read_bytes()
    if full_target_retry:
        # A restored old depth-capped target may now be consumed by the shared
        # full-target path. It must obey the same gate, not bypass it.
        mode["configured_entry_fill_policy"] = ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET
    bad = {**quote, "bid": 901. if weight < 0 else 1090.,
           "ask": 902. if weight < 0 else 1095., "quote_at": _now(9, 0, 8).isoformat()}
    engine.process_quotes(quotes={"2330": bad}, now=_now(9, 0, 8))
    order = mode["pending_entry_orders"]["2330"]
    assert order["status"] == "cancelled_entry_price_guard"
    assert order["remaining_shares"] == 2000 and mode["pending_entry_shares"] == 0
    assert position["filled_shares"] == 1000 and abs(position["signed_shares"]) == 1000
    assert engine.fills_path.read_bytes() == before
    assert len([r for r in _rows(engine.events_path) if r["event"] == "entry_price_guard_rejected"]) == 1
    displayed = build_dashboard_signal_page(state_dir=engine.state_dir, session_date="2026-08-13",
        mode=spec.market, symbol="2330")["rows"][0]
    assert displayed["reason"] == "entry_price_at_or_beyond_take_profit"
    assert displayed["filled_shares"] == 1000
    assert displayed["entry_remainder_rejection"]["cancelled_shares"] == 2000
    recovered = TwDayTradeSimulationEngine(engine.state_dir)
    recovered.process_quotes(quotes={"2330": {**quote, "quote_at": _now(9, 0, 9).isoformat()}}, now=_now(9, 0, 9))
    assert recovered.fills_path.read_bytes() == before
    # Entry rejection does not suppress an ordinary liquidation at the TP.
    closing = {**quote, "bid": 1095. if weight > 0 else 900.,
               "ask": 1100. if weight > 0 else 901., "last": 1095. if weight > 0 else 901.,
               "quote_at": _now(9, 2).isoformat(), "minute_volume_lots": 100.}
    recovered.process_quotes(quotes={"2330": closing}, now=_now(9, 2))
    assert recovered.state["modes"][spec.market]["positions"][position["position_id"]]["signed_shares"] == 0
    assert recovered.fills_path.read_bytes().startswith(before)


@pytest.mark.parametrize("weight", [.3, -.3])
def test_unsized_opening_recovery_also_rejects_crossed_entry(tmp_path, weight):
    spec = replace(_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET, price_limit_offset_ticks=1)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    row = {**_row(weight), "open_price": None, "opening_price_wait_reason": "awaiting_first_trade_print"}
    quote = {**_quote(), "quote_at": _now(9, 0, 6).isoformat(), "open": None, "simtrade": False}
    engine.register_signal(spec=spec, summary=_summary(), signal_rows=[row], quotes={"2330": quote},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 0, 7))
    mode = engine.state["modes"][spec.market]
    assert mode["pending_entry_source_count"] == 1
    bad = {**quote, "open": 1000., "bid": 901., "ask": 1095., "quote_at": _now(9, 0, 8).isoformat()}
    engine.process_quotes(quotes={"2330": bad}, now=_now(9, 0, 8))
    assert mode["pending_entry_source_count"] == 0
    assert mode["pending_entry_orders"]["2330"]["status"] == "cancelled_entry_price_guard"
    assert not mode["positions"] and not _rows(engine.fills_path)


@pytest.mark.parametrize("policy", [ENTRY_FILL_POLICY_CAUSAL_BOOK, ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET])
@pytest.mark.parametrize("weight", [.3, -.3])
def test_zero_fill_retry_at_stop_is_cancelled_without_inventing_a_position(tmp_path, policy, weight):
    spec = replace(_spec(tmp_path), entry_fill_policy=policy, price_limit_offset_ticks=1)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = {**_quote(bid=None, ask=None), "quote_at": _now(9, 0, 6).isoformat(), "simtrade": False}
    engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(weight)], quotes={"2330": quote},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 0, 7))
    mode = engine.state["modes"][spec.market]
    assert mode["pending_entry_orders"]["2330"]["status"] == "working"
    bad = {**quote, "bid": 900. if weight > 0 else 1095., "ask": 901. if weight > 0 else 1100.,
           "quote_at": _now(9, 0, 8).isoformat()}
    engine.process_quotes(quotes={"2330": bad}, now=_now(9, 0, 8))
    assert mode["positions"] == {} and not _rows(engine.fills_path)
    displayed = build_dashboard_signal_page(state_dir=engine.state_dir, session_date="2026-08-13",
        mode=spec.market, symbol="2330")["rows"][0]
    assert displayed["reason"] == "entry_price_at_or_beyond_stop_loss"
    assert displayed["filled_shares"] == 0 and displayed.get("position") is None
    assert displayed["status"] == "skipped"
    assert "rejected_entry_price" not in displayed  # Private quote is not republished.


def test_reading_old_committed_mode_never_relabels_its_fills(tmp_path):
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row()], quotes={"2330": _quote()},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7))
    mode = engine.state["modes"][spec.market]
    mode.pop("entry_price_guard_contract")
    before = engine.fills_path.read_bytes()
    engine._mode(spec)
    assert "entry_price_guard_contract" not in mode
    assert mode["configured_entry_price_guard_contract"] == ENTRY_PRICE_GUARD_CONTRACT
    assert engine.fills_path.read_bytes() == before


@pytest.mark.parametrize("weight", [.3, -.3])
@pytest.mark.parametrize("price", [901., 1095.])
@pytest.mark.parametrize("full_fill", [False, True])
def test_new_counterfactual_replay_checks_its_actual_execution_price(tmp_path, weight, price, full_fill):
    spec = replace(_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        price_limit_offset_ticks=1, historical_full_fill_at_0901=full_fill)
    plan = _prepare_entry_plan(_row(weight),
        quote={**_quote(), "execution_price_0901": price, "quote_at": _now(9, 1).isoformat()},
        evidence=_eligibility()["2330"], signal_at=_now(9, 0), observation_at=_now(9, 1),
        spec=spec, allow_quote_at_signal=True)
    assert plan["filled_shares"] == 0 and plan["status"] == "skipped"


@pytest.mark.parametrize("weight", [.3, -.3])
def test_historical_minute_sweep_does_not_add_at_crossed_bracket(tmp_path, weight):
    spec = replace(_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        price_limit_offset_ticks=1, entry_sweep_funding_policy="proportional_net_reservation_v1")
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    quote = {**_quote(minute_volume_lots=2.), "quote_at": _now(9, 1).isoformat(), "execution_price_0901": 1000.}
    engine.register_signal(spec=spec,
        summary={**_summary(), "simulation_replay": True, "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE},
        signal_rows=[_row(weight)], quotes={"2330": quote}, eligibility=_eligibility(), eligibility_coverage={},
        now=_now(9, 1), counterfactual_open_replay=True)
    mode = engine.state["modes"][spec.market]
    before = engine.fills_path.read_bytes()
    assert mode["pending_entry_orders"]["2330"]["remaining_shares"] == 2000
    price = 1095. if weight > 0 else 901.
    engine._retry_historical_minute_entry_orders(mode,
        {"2330": {**quote, "execution_price_minute": price, "last": price,
                  "quote_at": _now(9, 2).isoformat(), "historical_minute_valuation": True}}, _now(9, 2))
    assert mode["pending_entry_orders"]["2330"]["status"] == "cancelled_entry_price_guard"
    assert mode["pending_entry_shares"] == 0 and engine.fills_path.read_bytes() == before


@pytest.mark.parametrize("weight,price", [(.3, 1100.), (-.3, 900.)])
def test_reductions_do_not_inherit_entry_guard(tmp_path, weight, price):
    spec = replace(_spec(tmp_path), price_limit_offset_ticks=1)
    plan = _prepare_entry_plan(_row(weight), quote=_quote(bid=price, ask=price), evidence=None,
        signal_at=_now(9, 0, 5), observation_at=_now(9, 1, 7), spec=spec,
        reduce_only=True, requested_shares_override=1000)
    assert plan["filled_shares"] == 1000 and plan["entry_price_guard_contract"] is None


def test_etf_guard_reuses_the_original_etf_tick_brackets():
    tp, sl = entry_bracket_prices(side="long", upper_limit=50., lower_limit=45.,
        offset_ticks=1, trading_date=date(2026, 8, 13), security_type="etf")
    assert (tp, sl) == (49.99, 45.01)
    assert entry_price_guard_reason(side="long", price=49.99, take_profit_price=tp, stop_trigger_price=sl) == "entry_price_at_or_beyond_take_profit"


def test_manual_paper_completion_cannot_bypass_guard(tmp_path):
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(.3)],
        quotes={"2330": _quote(ask_volume=1.)}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1, 7))
    state = json.loads(engine.state_path.read_text())
    position = next(iter(state["modes"][spec.market]["positions"].values()))
    position["take_profit_price"] = position["entry_price"]
    engine.state_path.write_text(json.dumps(state))
    before = engine.fills_path.read_bytes()
    with pytest.raises(ValueError, match="entry_price_at_or_beyond_take_profit"):
        make_plan(engine.state_dir, market=spec.market, session_date=date(2026, 8, 13), symbol="2330")
    assert engine.fills_path.read_bytes() == before

