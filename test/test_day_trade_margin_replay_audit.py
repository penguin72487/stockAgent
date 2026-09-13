from datetime import date, datetime, timedelta
import json
from pathlib import Path

import pytest
import numpy as np
from types import SimpleNamespace

from scripts.audit_tw_day_trade_margin_replay import (
    audit, _entry_source_path, _verify_calendar_coverage, _claim_matches_source, _verified_action_sources,
)
from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT


def test_claim_arithmetic_cannot_substitute_for_official_cash_terms():
    reference = SimpleNamespace(exact_cash_terms_by_symbol={"2330": (
        np.array(["2026-08-14"], dtype="datetime64[D]"), np.array([10.]),
        np.array(["2026-08-18"], dtype="datetime64[D]"))})
    claim = dict(symbol="2330", ex_date="2026-08-14", cash_per_share=10., payment_date="2026-08-18")
    assert _claim_matches_source(claim, reference)
    for patch in (dict(cash_per_share=20.), dict(payment_date="2026-08-19"),
                  dict(symbol="0050"), dict(ex_date="2026-08-13")):
        assert not _claim_matches_source(claim | patch, reference)


def test_audit_action_sources_cannot_omit_the_requested_horizon(tmp_path, monkeypatch):
    import scripts.audit_tw_day_trade_margin_replay as module
    from test_day_trade_margin_carry import cash_entitlement
    cash_entitlement(tmp_path)
    for suffix in (".parquet", ".summary.json"):
        (tmp_path / f"tw_share_replacement_reference{suffix}").write_text("fixture")
    observed = []
    monkeypatch.setattr(module, "load_share_replacements", lambda *a, **kw: observed.append(kw) or ())
    path = tmp_path / "tw_corporate_action_reference.parquet"
    _, _, hashes = _verified_action_sources(path, "2026-02-25", "2026-09-09")
    assert len(hashes) == 6
    assert observed == [dict(required_start=date(2026, 2, 25), required_end=date(2026, 9, 9))]
    with pytest.raises(ValueError, match="cover"):
        _verified_action_sources(path, "2026-02-25", "2026-09-11")


def test_fill_inventory_and_all_minute_nav_reconcile(tmp_path):
    start = datetime.fromisoformat("2026-08-13T09:01:00+08:00")
    fills = [dict(market="a", position_id="p", symbol="2330", session_date="2026-08-13",
                  recorded_at=start.isoformat(), purpose="entry", side="buy", quantity=1000, price=10.),
             dict(market="a", position_id="p", symbol="2330", session_date="2026-08-13",
                  recorded_at=(start + timedelta(minutes=1)).isoformat(), purpose="take_profit", side="sell",
                  quantity=1000, price=10.1, gross_pnl_twd=100., net_pnl_twd=98., entry_fee_allocated_twd=1., fee_and_tax_twd=1.)]
    rows = []
    for i in range(270):
        realized = 98. if i else 0.
        rows.append(dict(session_date="2026-08-13", market="a", minute=(start + timedelta(minutes=i)).isoformat(timespec="minutes"),
            margin_carry_contract=MARGIN_CARRY_CONTRACT, initial_capital_twd=10000., cumulative_realized_net_pnl_twd=realized,
            open_net_liquidation_pnl_twd=0., total_equity_twd=10000.+realized, open_position_count=0 if i else 1,
            historical_minute_replay=True, minute_valuation_contract="right_labelled_historical_last_trade_mark_v1",
            valuation_source="fixture_kbar", valuation_executable=False, fresh_trade_notional_coverage_ratio=1.,
            fresh_trade_position_count=0 if i else 1, last_trade_carried_position_count=0, missing_price_position_count=0))
    state = {"modes": {"a": {"margin_carry_contract": MARGIN_CARRY_CONTRACT, "session_date": "2026-08-13",
                           "total_equity_twd": 10098., "positions": {"p": {"position_id": "p", "signed_shares": 0}}}}}
    (tmp_path / "state.json").write_text(json.dumps(state))
    (tmp_path / "rebuild_receipt.json").write_text(json.dumps({"sessions": [{"session_date": "2026-08-13"}]}))
    for i, fill in enumerate(fills):
        fill["order_id"] = f"order-{i}"
    (tmp_path / "orders.jsonl").write_text(''.join(json.dumps(x)+'\n' for x in fills))
    for fill in fills:
        fill.pop("side")  # actual engine fills bind side through their order
    (tmp_path / "fills.jsonl").write_text(''.join(json.dumps(x)+'\n' for x in fills))
    (tmp_path / "marks.jsonl").write_text(''.join(json.dumps(x)+'\n' for x in rows))
    assert audit(tmp_path, verify_sources=False)["passed"]
    fills[1]["quantity"] = 2000
    (tmp_path / "fills.jsonl").write_text(''.join(json.dumps(x)+'\n' for x in fills))
    result = audit(tmp_path, verify_sources=False)
    assert not result["passed"]
    assert any("exit exceeds inventory" in e for e in result["errors"])


@pytest.mark.parametrize("method", ["minute_vwap", "minute_close"])
def test_entry_source_tag_is_not_part_of_file_path(method):
    path = "/source/minute_chunks/2330/2026-08-01_2026-08-31.parquet"
    assert _entry_source_path(f"local_minute_parquet_0901_{method}:{path}") == Path(path)


@pytest.mark.parametrize("source", ["/source/file.parquet", "unknown:/source/file.parquet",
                                    "local_minute_parquet_0901_minute_vwap:relative.parquet"])
def test_unknown_or_relative_entry_sources_are_rejected(source):
    with pytest.raises(ValueError, match="identity"):
        _entry_source_path(source)


def test_whole_missing_sessions_cannot_be_a_full_range_pass(monkeypatch):
    dates = ["2026-08-13", "2026-08-14", "2026-08-17"]
    proof = {"official_session_calendar": {"path": "/source/twse_taiex_ohlc.parquet",
        "sha256": "exact", "session_count": 3, "start_date": dates[0], "end_date": dates[-1]}}
    monkeypatch.setattr("scripts.audit_tw_day_trade_margin_replay._validated_taiex_session_dates",
                        lambda *_: ({date.fromisoformat(day) for day in dates}, "exact"))
    assert _verify_calendar_coverage(proof, dates, prefix=False)["verified_sessions"] == 3
    assert _verify_calendar_coverage(proof, dates[:2], prefix=True)["requested_sessions"] == 3
    for observed, prefix in [(dates[:2], False), ([dates[0], dates[2]], True)]:
        with pytest.raises(ValueError, match="omitted"):
            _verify_calendar_coverage(proof, observed, prefix=prefix)
    proof["official_session_calendar"]["sha256"] = "different"
    with pytest.raises(ValueError, match="identity"):
        _verify_calendar_coverage(proof, dates, prefix=False)


def test_auditor_reconciles_share_conversion_then_odd_lot_exit(tmp_path):
    from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
    day1, day2 = "2026-08-13", "2026-08-14"
    action = dict(action_id="p:replacement", position_id="p", symbol="2330", effective_date=day2,
                  recorded_at=f"{day2}T09:00:00+08:00", old_signed_shares=1000, new_signed_shares=750,
                  old_entry_price=100., new_entry_price=100./.75, ratio=.75, cash_per_old_share=2.5)
    claim = dict(claim_id="p:refund", position_id="p", share_action_id=action["action_id"],
                 ex_date=day2, payment_date="2026-08-15", entitled_signed_shares=1000,
                 cash_per_share=2.5, amount_twd=2500.)
    fills = [dict(market="a", order_id="entry", position_id="p", symbol="2330", session_date=day1,
                  recorded_at=f"{day1}T09:01:00+08:00", purpose="entry", quantity=1000, price=100.),
             dict(market="a", order_id="exit", position_id="p", symbol="2330", session_date=day2,
                  recorded_at=f"{day2}T09:01:00+08:00", purpose="next_signal_inventory_delta", quantity=750,
                  price=130., gross_pnl_twd=-2500., net_pnl_twd=-2502., entry_fee_allocated_twd=1.,
                  fee_and_tax_twd=1., odd_lot_execution_policy=ODD_LOT_BOARD_PRICE)]
    rows = []
    for day in (day1, day2):
        realized, earned = (-2502., 2500.) if day == day2 else (0., 0.)
        start = datetime.fromisoformat(f"{day}T09:01:00+08:00")
        for i in range(270):
            rows.append(dict(session_date=day, market="a", minute=(start+timedelta(minutes=i)).isoformat(timespec="minutes"),
                margin_carry_contract=MARGIN_CARRY_CONTRACT, initial_capital_twd=10000.,
                cumulative_realized_net_pnl_twd=realized, cumulative_corporate_action_net_twd=earned,
                open_net_liquidation_pnl_twd=0., total_equity_twd=10000.+realized+earned,
                open_position_count=int(day == day1), historical_minute_replay=True,
                minute_valuation_contract="right_labelled_historical_last_trade_mark_v1",
                valuation_source="fixture_kbar", valuation_executable=False,
                fresh_trade_notional_coverage_ratio=1., fresh_trade_position_count=int(day == day1),
                last_trade_carried_position_count=0, missing_price_position_count=0))
    state = {"modes": {"a": dict(margin_carry_contract=MARGIN_CARRY_CONTRACT, session_date=day2,
        odd_lot_execution_policy=ODD_LOT_BOARD_PRICE, total_equity_twd=9998.,
        positions={"p": {"position_id": "p", "signed_shares": 0}},
        share_replacement_ledger=[action], corporate_action_ledger=[claim])}}
    (tmp_path/"state.json").write_text(json.dumps(state))
    (tmp_path/"rebuild_receipt.json").write_text(json.dumps({"sessions": [{"session_date": day1}, {"session_date": day2}]}))
    (tmp_path/"fills.jsonl").write_text(''.join(json.dumps(row)+'\n' for row in fills))
    (tmp_path/"marks.jsonl").write_text(''.join(json.dumps(row)+'\n' for row in rows))
    orders = [dict(market="a", order_id="entry", side="buy"), dict(market="a", order_id="exit", side="sell")]
    (tmp_path/"orders.jsonl").write_text(''.join(json.dumps(row)+'\n' for row in orders))
    result = audit(tmp_path, verify_sources=False)
    assert result["passed"], result["errors"]
    assert result["accounts"]["a"]["share_replacements"] == 1
    action["new_signed_shares"] = 700
    (tmp_path/"state.json").write_text(json.dumps(state))
    assert not audit(tmp_path, verify_sources=False)["passed"]
