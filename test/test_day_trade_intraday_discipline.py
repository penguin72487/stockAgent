"""Same-day flatten is an obligation, not a fabricated fill guarantee."""
from dataclasses import replace
from datetime import datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_CAUSAL_BOOK, STRICT_INTRADAY_CONTRACT, TwDayTradeSimulationEngine,
)
from test_day_trade_margin_carry import action_reference
from test_tw_day_trade_simulation import _spec, _row, _quote, _eligibility, _summary, _now


def quote(at, **updates):
    q = _quote() | {"quote_at": at.isoformat(), "exchange_quote_at": at.isoformat(),
                    "simtrade": False, "cumulative_volume_lots": 100.}
    q.update(updates)
    return q


def account(tmp_path, *, weight=.3, depth=20):
    spec = replace(_spec(tmp_path), strict_intraday=True, residual_margin_conversion=True,
                   margin_corporate_action_reference_path=action_reference(tmp_path),
                   entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_BOOK)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    assert engine.register_signal(spec=spec, summary=_summary(), signal_rows=[_row(weight)],
        quotes={"2330": quote(_now(9, 0, 6), ask_volume=depth, bid_volume=depth)},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 0, 6)) == "registered"
    return engine, spec, engine.state["modes"][spec.market]


def step(engine, at, **updates):
    engine.process_quotes(quotes={"2330": quote(at, **updates)}, now=at)


def test_partial_entry_continues_across_restart_without_reusing_depth(tmp_path):
    engine, spec, mode = account(tmp_path, depth=1)
    p = next(iter(mode["positions"].values()))
    assert p["signed_shares"] == 1000
    step(engine, _now(9, 0, 7), ask_volume=1)
    assert p["signed_shares"] == 1000
    step(engine, _now(9, 0, 8), ask_volume=1, cumulative_volume_lots=102)
    assert p["signed_shares"] == 2000
    engine = TwDayTradeSimulationEngine(engine.state_dir)
    mode = engine.state["modes"][spec.market]
    step(engine, _now(9, 0, 9), ask_volume=1, cumulative_volume_lots=102)
    assert next(iter(mode["positions"].values()))["signed_shares"] == 2000
    step(engine, _now(9, 0, 10), ask_volume=1, cumulative_volume_lots=104)
    assert mode["entry_filled_shares"] == mode["entry_requested_shares"] == 3000
    assert mode["pending_entry_shares"] == 0
    assert mode["entry_unfilled_shares"] == 0
    assert len([json.loads(s) for s in engine.fills_path.read_text().splitlines()]) == 3


@pytest.mark.parametrize("hour,minute,second", [(13,30,6), (13,33,1)])
def test_auction_waits_for_late_real_print_and_flattens(tmp_path, hour, minute, second):
    engine, spec, mode = account(tmp_path)
    step(engine, _now(13,25), minute_volume_lots=0)
    step(engine, _now(13,30,1), minute_volume_lots=0, auction_volume_lots=0)
    assert not mode["closing_auction_settled_at"]
    assert mode["open_position_count"] == 1
    assert mode["engine_status"] == "waiting_closing_auction_evidence"
    step(engine, _now(hour,minute,second), last=1000, auction_volume_lots=10)
    assert mode["open_position_count"] == 0
    assert mode["closing_auction_settled_at"]
    assert not any(p.get("margin_carry_contract") for p in mode["positions"].values())


@pytest.mark.parametrize("updates", [
    {"simtrade": True}, {"simtrade": None}, {"quote_at": _now(13,29).isoformat()},
    {"exchange_quote_at": _now(13,24).isoformat()},
    {"exchange_quote_at": _now(13,33).isoformat()}, {"last": 1001.},
])
def test_invalid_auction_evidence_never_fills_or_authorizes_carry(tmp_path, updates):
    engine, spec, mode = account(tmp_path)
    q = quote(_now(13,30), last=1000, auction_volume_lots=100) | updates
    engine.process_quotes(quotes={"2330": q}, now=_now(13,30))
    assert mode["open_position_count"] == 1
    engine.process_quotes(quotes={}, now=_now(13,35))
    assert mode["unresolved_exit_count"] == 1
    assert mode["margin_exception_count"] == 0
    assert mode["engine_status"] == "critical_day_trade_exit_unresolved"
    assert next(iter(mode["positions"].values()))["carry_type"] == "unresolved_day_trade_delivery_obligation"


@pytest.mark.parametrize("weight,last,depth_key", [(.3,900.,"bid_volume"), (-.3,1100.,"ask_volume")])
def test_only_adverse_locked_limit_is_exception_not_normal_carry(tmp_path, weight, last, depth_key):
    engine, spec, mode = account(tmp_path, weight=weight)
    step(engine, _now(13,24), last=last, bid=last, ask=last, **{depth_key:0})
    step(engine, _now(13,30), last=last, auction_volume_lots=0, **{depth_key:0})
    step(engine, _now(13,35), last=last, auction_volume_lots=0, **{depth_key:0})
    p = next(iter(mode["positions"].values()))
    assert p["margin_exception_evidence"]["reason"] == "adverse_limit_locked_no_counterparty"
    assert p["mandatory_exit_pending"] is True
    assert mode["margin_exception_count"] == 1
    assert mode["unresolved_exit_count"] == 0
    assert mode["engine_status"] == "critical_adverse_limit_exception"


@pytest.mark.parametrize("last,depth", [(1000.,0), (1100.,0), (900.,None), (900.,1)])
def test_non_limit_missing_depth_or_capacity_never_becomes_exception(tmp_path,last,depth):
    engine,spec,mode=account(tmp_path)
    step(engine,_now(13,35),last=last,bid_volume=depth,minute_volume_lots=0)
    assert mode["margin_exception_count"] == 0
    assert mode["unresolved_exit_count"] == 1


def test_stop_latches_through_price_recovery_and_passive_phase(tmp_path):
    engine,spec,mode=account(tmp_path,depth=1)
    p=next(iter(mode["positions"].values()))
    step(engine,_now(13,19),last=900,bid=900,bid_volume=0)
    assert p["stop_triggered_at"]
    step(engine,_now(13,20),last=1000,bid=1000,bid_volume=20,cumulative_volume_lots=120)
    assert p["signed_shares"] == 0
    assert mode["pending_entry_shares"] == 0
    assert all(o["status"] == "cancelled_exit_priority" for o in mode["pending_entry_orders"].values())


def test_minute_volume_updates_late_but_rejects_regression(tmp_path):
    engine=TwDayTradeSimulationEngine(tmp_path/"state")
    def prepare(at, cumulative):
        return engine.prepare_minute_quotes({"2330":quote(at,cumulative_volume_lots=cumulative)},now=at)["2330"]
    prepare(_now(13,29,59),100)
    assert prepare(_now(13,30),100)["minute_volume_lots"] == 0
    assert prepare(_now(13,30,5),130)["minute_volume_lots"] == 30
    assert prepare(_now(13,30,6),110)["minute_volume_lots"] is None
    assert prepare(_now(13,30,7),130)["minute_volume_lots"] == 30


def test_stream_callbacks_keep_zero_depth_trial_and_original_event_time(monkeypatch):
    import stockagent.live.quote_provider as provider
    monkeypatch.setattr(provider,"_SHIOAJI_STREAM_ROWS",{})
    t=_now(13,30)
    event=SimpleNamespace(code="2330",datetime=t,simtrade=False,intraday_odd=False,
                          bid_price=[900],ask_price=[901],bid_volume=[0],ask_volume=[2])
    provider._record_stock_stream_event("book",event,received=t)
    event.datetime=t-timedelta(seconds=1)
    event.bid_volume=[10]
    provider._record_stock_stream_event("book",event,received=t)
    book=provider._SHIOAJI_STREAM_ROWS["2330"]["book"]
    assert book["bid_volume"] == 0
    assert book["simtrade"] is False
    assert book["exchange_at"] == t.isoformat()


def test_prior_inventory_is_exited_without_waiting_for_another_signal(tmp_path):
    engine,spec,mode=account(tmp_path)
    step(engine,_now(13,35),last=900,bid_volume=0,minute_volume_lots=0)
    next_open=_now(9,0,10)+timedelta(days=1)
    step(engine,next_open,bid=1000,bid_volume=20)
    assert mode["open_position_count"] == 0
    p=next(iter(mode["positions"].values()))
    assert p["exit_reason"] == "prior_day_mandatory_liquidation"
    assert p["session_date"] == "2026-08-13"
    fill=json.loads(engine.fills_path.read_text().splitlines()[-1])
    assert fill["session_date"] == "2026-08-14"


def test_prior_inventory_also_participates_in_next_closing_auction(tmp_path):
    engine,spec,mode=account(tmp_path)
    step(engine,_now(13,35),last=900,bid_volume=0,minute_volume_lots=0)
    step(engine,_now(13,25)+timedelta(days=1),last=900,bid_volume=0,minute_volume_lots=0)
    at=_now(13,30,2)+timedelta(days=1)
    step(engine,at,last=1000,auction_volume_lots=20,minute_volume_lots=0)
    assert mode["open_position_count"] == 0


def test_repeated_auction_print_cannot_multiply_liquidity_after_restart(tmp_path):
    engine,spec,mode=account(tmp_path)
    step(engine,_now(13,25),minute_volume_lots=0)
    step(engine,_now(13,30),last=1000,auction_volume_lots=2)
    assert next(iter(mode["positions"].values()))["signed_shares"] == 2000
    engine=TwDayTradeSimulationEngine(engine.state_dir)
    mode=engine.state["modes"][spec.market]
    step(engine,_now(13,30,1),last=1000,auction_volume_lots=2)
    assert next(iter(mode["positions"].values()))["signed_shares"] == 2000


def test_strict_policy_is_not_applied_retroactively_to_committed_session(tmp_path):
    spec=replace(_spec(tmp_path),entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_BOOK)
    engine=TwDayTradeSimulationEngine(tmp_path/"state")
    engine.register_signal(spec=spec,summary=_summary(),signal_rows=[_row()],
        quotes={"2330":quote(_now(9,0,6))},eligibility=_eligibility(),eligibility_coverage={},now=_now(9,0,6))
    strict=replace(spec,strict_intraday=True)
    engine.update_readiness([strict],now=_now(13,35))
    mode=engine.state["modes"][spec.market]
    assert mode["intraday_contract"] is None
    assert mode["configured_intraday_contract"] == STRICT_INTRADAY_CONTRACT
    assert mode["engine_status"] == "critical_legacy_carry_requires_review"


def test_auction_trade_does_not_need_fresh_or_non_trial_book(tmp_path):
    engine,spec,mode=account(tmp_path)
    step(engine,_now(13,25),minute_volume_lots=0)
    step(engine,_now(13,30,2),last=1000,auction_volume_lots=20,
         auction_volume_source="exchange_non_trial_tick",simtrade=True,
         quote_at=_now(13,25).isoformat(),trade_simtrade=False,trade_quote_at=_now(13,30,1).isoformat())
    assert mode["open_position_count"] == 0


def test_stream_subscriptions_are_shared_and_not_snapshot_polling(monkeypatch):
    import stockagent.live.quote_provider as provider
    calls=[]
    class API:
        contracts=SimpleNamespace(get=lambda code: SimpleNamespace(code=code,reference=1000,limit_up=1100,limit_down=900))
        def set_on_quote_stk_v1_callback(self, callback): self.quote=callback
        def subscribe(self, contract, **kw): calls.append(("subscribe",contract.code))
        def unsubscribe(self, contract, **kw): calls.append(("unsubscribe",contract.code))
    api=API()
    monkeypatch.setattr(provider,"_shioaji_stock_api",lambda:api)
    monkeypatch.setattr(provider,"_SHIOAJI_STREAM_API",None)
    monkeypatch.setattr(provider,"_SHIOAJI_STREAM_ROWS",{})
    monkeypatch.setattr(provider,"_SHIOAJI_STREAM_SUBSCRIPTIONS",set())
    monkeypatch.setattr(provider,"_SHIOAJI_STREAM_CURSOR",0)
    monkeypatch.setattr(provider,"_SHIOAJI_STREAM_ROTATION_AT",0.0)
    monkeypatch.setattr(provider,"_load_prepared_tw_price_limits",lambda day:({},None))
    provider.fetch_shioaji_stock_live_quotes(["2330"],trading_date=_now(9,0).date())
    provider.fetch_shioaji_stock_live_quotes(["2330"],trading_date=_now(9,0).date())
    assert calls == [("subscribe","2330")]
    provider.fetch_shioaji_stock_live_quotes([],trading_date=_now(9,0).date())
    assert calls[-1:] == [("unsubscribe","2330")]


def test_quote_stream_carries_book_trial_and_auction_evidence(monkeypatch):
    import stockagent.live.quote_provider as provider
    callbacks = {}
    class API:
        contracts = SimpleNamespace(get=lambda code: SimpleNamespace(code=code, reference=1000, limit_up=1100, limit_down=900))
        def set_on_quote_stk_v1_callback(self, callback): callbacks["quote"] = callback
        def subscribe(self, contract, **kw): pass
        def unsubscribe(self, contract, **kw): pass
    monkeypatch.setattr(provider, "_shioaji_stock_api", lambda: API_INSTANCE)
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_API", None)
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_ROWS", {})
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_SUBSCRIPTIONS", set())
    monkeypatch.setattr(provider, "_load_prepared_tw_price_limits", lambda day: ({}, None))
    API_INSTANCE = API()
    observed = datetime.now(_now(9, 0).tzinfo)
    day = observed.date()
    provider.fetch_shioaji_stock_live_quotes(["2330"], trading_date=day)
    callbacks["quote"](SimpleNamespace(
        code="2330", datetime=observed, simtrade=False, suspend=False,
        intraday_odd=False, open=1000, close=1001, high=1002, low=999,
        volume=2, total_volume=123, bid_price=[1000], ask_price=[1001],
        bid_volume=[5], ask_volume=[4],
    ))
    quote = provider.fetch_shioaji_stock_live_quotes(["2330"], trading_date=day)["2330"]
    assert quote["source"] == "shioaji_stock_quote_stream"
    assert quote["simtrade"] is False
    assert quote["bid"] == 1000
    assert quote["ask"] == 1001
    assert quote["cumulative_volume_lots"] == 123
    assert quote["quote_at"] is not None
    provider._SHIOAJI_STREAM_ROWS.clear()
    auction_at = observed.replace(hour=13, minute=30, second=2, microsecond=0)
    callbacks["quote"](SimpleNamespace(
        code="2330", datetime=auction_at, simtrade=False, suspend=False,
        intraday_odd=False, open=1000, close=1005, high=1005, low=999,
        volume=10, total_volume=133, bid_price=[1004], ask_price=[1005],
        bid_volume=[0], ask_volume=[0],
    ))
    callbacks["quote"](SimpleNamespace(
        code="2330", datetime=auction_at + timedelta(seconds=1),
        simtrade=False, suspend=False, intraday_odd=False,
        open=1000, close=1005, high=1005, low=999,
        volume=0, total_volume=133, bid_price=[1004], ask_price=[1005],
        bid_volume=[0], ask_volume=[0],
    ))
    quote = provider.fetch_shioaji_stock_live_quotes(["2330"], trading_date=day)["2330"]
    assert quote["auction_volume_lots"] == 10
    assert quote["auction_volume_source"] == "exchange_non_trial_quote_trade"
    assert quote["last"] == 1005
    assert quote["trade_quote_at"] == provider._SHIOAJI_STREAM_ROWS["2330"]["auction"]["received_at"]


def test_quote_stream_rotates_without_exceeding_subscription_limit(monkeypatch):
    import stockagent.live.quote_provider as provider
    calls = []
    callbacks = {}
    class API:
        contracts = SimpleNamespace(get=lambda code: SimpleNamespace(code=code, reference=10, limit_up=11, limit_down=9))
        def set_on_quote_stk_v1_callback(self, callback): callbacks["quote"] = callback
        def subscribe(self, contract, **kw): calls.append(("subscribe", contract.code))
        def unsubscribe(self, contract, **kw): calls.append(("unsubscribe", contract.code))
    api = API()
    monkeypatch.setattr(provider, "_shioaji_stock_api", lambda: api)
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_API", None)
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_ROWS", {})
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_SUBSCRIPTIONS", set())
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_CURSOR", 0)
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_ROTATION_AT", 0.0)
    monkeypatch.setattr(provider, "_load_prepared_tw_price_limits", lambda day: ({}, None))
    symbols = [str(1000 + idx) for idx in range(258)]
    observed = datetime.now(_now(9, 0).tzinfo)
    for _ in range(8):
        first = provider.fetch_shioaji_stock_live_quotes(symbols, trading_date=observed.date())
    assert len(provider._SHIOAJI_STREAM_SUBSCRIPTIONS) == provider._SHIOAJI_STOCK_STREAM_LIMIT
    assert sum(bool(row["stream_capacity_limited"]) for row in first.values()) == 58
    cached_code = symbols[142]
    callbacks["quote"](SimpleNamespace(
        code=cached_code, datetime=observed, simtrade=False, suspend=False,
        intraday_odd=False, open=10, close=10, high=10, low=10,
        volume=1, total_volume=10, bid_price=[10], ask_price=[10],
        bid_volume=[1], ask_volume=[1],
    ))
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_ROTATION_AT", 0.0)
    for _ in range(5):
        second = provider.fetch_shioaji_stock_live_quotes(symbols, trading_date=observed.date())
    assert len(provider._SHIOAJI_STREAM_SUBSCRIPTIONS) == provider._SHIOAJI_STOCK_STREAM_LIMIT
    assert all(first[code]["stream_subscribed"] or second[code]["stream_subscribed"] for code in symbols)
    assert second[cached_code]["stream_capacity_limited"] is True
    assert second[cached_code]["stream_subscribed"] is False
    assert second[cached_code]["available"] is True
    assert len([action for action, _ in calls if action == "subscribe"]) > provider._SHIOAJI_STOCK_STREAM_LIMIT
