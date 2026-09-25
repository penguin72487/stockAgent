from datetime import date, datetime, timedelta
import json
from pathlib import Path

import pytest
import numpy as np
import polars as pl
from types import SimpleNamespace

from scripts.audit_tw_day_trade_margin_replay import (
    audit, _entry_source_path, _retained_entry_book_source_days,
    _verify_calendar_coverage, _claim_matches_source, _verified_action_sources,
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


def test_audit_disclosed_paper_entry_completion_is_inventory_not_exit(tmp_path):
    from scripts.complete_tw_day_trade_paper_entry import CONTRACT

    start = datetime.fromisoformat("2026-08-13T09:01:00+08:00")
    common = dict(market="a", position_id="p", symbol="2330", session_date="2026-08-13")
    disclosure = dict(contract=CONTRACT, full_quantity_is_user_assumption=True,
                      same_price_as_original_entry=True, broker_fill=False)
    fills = [
        dict(**common, order_id="entry", purpose="entry", side="buy", quantity=1000,
             recorded_at=start.isoformat(), price=10.),
        dict(**common, order_id="completion", purpose="entry_completion", side="buy",
             quantity=1000, recorded_at=start.isoformat(), price=10.,
             fill_contract=CONTRACT, counterfactual_paper_completion=disclosure,
             broker_fill=False),
        dict(**common, order_id="exit", purpose="take_profit", side="sell", quantity=2000,
             recorded_at=(start + timedelta(minutes=1)).isoformat(), price=10.1,
             gross_pnl_twd=200., net_pnl_twd=196., entry_fee_allocated_twd=2.,
             fee_and_tax_twd=2.),
    ]
    rows = [
        dict(session_date="2026-08-13", market="a",
             minute=(start + timedelta(minutes=i)).isoformat(timespec="minutes"),
             margin_carry_contract=MARGIN_CARRY_CONTRACT, initial_capital_twd=10000.,
             cumulative_realized_net_pnl_twd=196. if i else 0.,
             open_net_liquidation_pnl_twd=0., total_equity_twd=10196. if i else 10000.,
             open_position_count=0 if i else 1, historical_minute_replay=True,
             minute_valuation_contract="right_labelled_historical_last_trade_mark_v1",
             valuation_source="fixture_kbar", valuation_executable=False,
             fresh_trade_notional_coverage_ratio=1.,
             fresh_trade_position_count=0 if i else 1,
             last_trade_carried_position_count=0, missing_price_position_count=0)
        for i in range(270)
    ]
    state = {"modes": {"a": {"margin_carry_contract": MARGIN_CARRY_CONTRACT,
                            "session_date": "2026-08-13", "total_equity_twd": 10196.,
                            "positions": {"p": {"position_id": "p", "signed_shares": 0}}}}}
    (tmp_path / "state.json").write_text(json.dumps(state), encoding="utf-8")
    (tmp_path / "rebuild_receipt.json").write_text(
        json.dumps({"sessions": [{"session_date": "2026-08-13"}]}), encoding="utf-8"
    )
    (tmp_path / "orders.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in fills), encoding="utf-8"
    )
    (tmp_path / "fills.jsonl").write_text(
        "".join(json.dumps({k: v for k, v in row.items() if k != "side"}) + "\n" for row in fills),
        encoding="utf-8",
    )
    (tmp_path / "marks.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    assert audit(tmp_path, verify_sources=False)["passed"]

    fills[1]["counterfactual_paper_completion"]["broker_fill"] = True
    (tmp_path / "fills.jsonl").write_text(
        "".join(json.dumps({k: v for k, v in row.items() if k != "side"}) + "\n" for row in fills),
        encoding="utf-8",
    )
    result = audit(tmp_path, verify_sources=False)
    assert not result["passed"]
    assert any("unverified paper entry completion" in error for error in result["errors"])


def test_audit_minute_sweep_completion_updates_weighted_entry_basis(tmp_path):
    from stockagent.live.tw_day_trade_simulation import (
        ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
    )

    start = datetime.fromisoformat("2026-08-13T09:01:00+08:00")
    common = dict(
        market="a",
        position_id="p",
        symbol="2330",
        session_date="2026-08-13",
    )
    fills = [
        dict(
            **common,
            order_id="entry",
            purpose="entry",
            side="buy",
            quantity=1000,
            recorded_at=start.isoformat(),
            fill_at=start.isoformat(),
            price=10.0,
        ),
        dict(
            **common,
            order_id="completion",
            purpose="entry_completion",
            side="buy",
            quantity=1000,
            recorded_at=(start + timedelta(minutes=1)).isoformat(),
            fill_at=(start + timedelta(minutes=1)).isoformat(),
            price=12.0,
            fill_contract=REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
            entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
            counterfactual_minute_sweep_fill=True,
            simulation_replay=True,
            observed_minute_volume_lots=2.0,
        ),
        dict(
            **common,
            order_id="exit",
            purpose="take_profit",
            side="sell",
            quantity=2000,
            recorded_at=(start + timedelta(minutes=2)).isoformat(),
            price=13.0,
            gross_pnl_twd=4000.0,
            net_pnl_twd=3996.0,
            entry_fee_allocated_twd=2.0,
            fee_and_tax_twd=2.0,
        ),
    ]
    rows = [
        dict(
            session_date="2026-08-13",
            market="a",
            minute=(start + timedelta(minutes=i)).isoformat(timespec="minutes"),
            margin_carry_contract=MARGIN_CARRY_CONTRACT,
            initial_capital_twd=10000.0,
            cumulative_realized_net_pnl_twd=3996.0 if i >= 2 else 0.0,
            open_net_liquidation_pnl_twd=0.0,
            total_equity_twd=13996.0 if i >= 2 else 10000.0,
            open_position_count=0 if i >= 2 else 1,
            historical_minute_replay=True,
            minute_valuation_contract="right_labelled_historical_last_trade_mark_v1",
            valuation_source="fixture_kbar",
            valuation_executable=False,
            fresh_trade_notional_coverage_ratio=1.0,
            fresh_trade_position_count=0 if i >= 2 else 1,
            last_trade_carried_position_count=0,
            missing_price_position_count=0,
        )
        for i in range(270)
    ]
    state = {
        "modes": {
            "a": {
                "margin_carry_contract": MARGIN_CARRY_CONTRACT,
                "session_date": "2026-08-13",
                "total_equity_twd": 13996.0,
                "positions": {"p": {"position_id": "p", "signed_shares": 0}},
            }
        }
    }
    (tmp_path / "state.json").write_text(json.dumps(state), encoding="utf-8")
    (tmp_path / "rebuild_receipt.json").write_text(
        json.dumps({"sessions": [{"session_date": "2026-08-13"}]}),
        encoding="utf-8",
    )
    (tmp_path / "orders.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in fills), encoding="utf-8"
    )
    (tmp_path / "fills.jsonl").write_text(
        "".join(
            json.dumps({k: v for k, v in row.items() if k != "side"}) + "\n"
            for row in fills
        ),
        encoding="utf-8",
    )
    (tmp_path / "marks.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )

    assert audit(tmp_path, verify_sources=False)["passed"]


@pytest.mark.parametrize("method", ["minute_vwap", "minute_close"])
def test_entry_source_tag_is_not_part_of_file_path(method):
    path = "/source/minute_chunks/2330/2026-08-01_2026-08-31.parquet"
    assert _entry_source_path(f"local_minute_parquet_0901_{method}:{path}") == Path(path)


@pytest.mark.parametrize("source", ["/source/file.parquet", "unknown:/source/file.parquet",
                                    "local_minute_parquet_0901_minute_vwap:relative.parquet"])
def test_unknown_or_relative_entry_sources_are_rejected(source):
    with pytest.raises(ValueError, match="identity"):
        _entry_source_path(source)


def test_hash_pinned_entry_book_retains_0901_only_fill_source(tmp_path):
    from scripts.audit_tw_day_trade_margin_replay import _sha256

    book_dir = tmp_path / "replay_entry_books"
    book_dir.mkdir()
    source = tmp_path / "minute_chunks" / "2330" / "2026-08-01_2026-08-31.parquet"
    daily_source = (
        tmp_path / "research_dataset" / "trade_date=2026-08-13" / "data.parquet"
    )
    source.parent.mkdir(parents=True)
    book = book_dir / "2026-08-13.parquet"
    pl.DataFrame({
        "symbol": ["2330", "0050"],
        "quote_at": ["2026-08-13T09:01:00+08:00", "2026-08-13T09:01:00+08:00"],
        "source": [
            f"local_minute_parquet_0901_minute_vwap:{source}",
            f"local_minute_parquet_0901_minute_close:{daily_source}",
        ],
    }).write_parquet(book)
    session = {
        "session_date": "2026-08-13",
        "historical_entry_books": {"path": str(book), "sha256": _sha256(book)},
    }
    signatures = {}
    assert _retained_entry_book_source_days(
        tmp_path, session, {"2330", "0050"}, signatures
    ) == {
        str(source): {date(2026, 8, 13)},
        str(daily_source): {date(2026, 8, 13)},
    }
    assert str(book) in signatures

    session["historical_entry_books"]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="hash mismatch"):
        _retained_entry_book_source_days(tmp_path, session, {"2330"}, {})


def test_hash_pinned_entry_book_accepts_canonical_shioaji_0901_evidence(tmp_path):
    from scripts.audit_tw_day_trade_margin_replay import _sha256

    day = "2026-08-13"
    book_dir = tmp_path / "replay_entry_books"
    book_dir.mkdir()
    book = book_dir / f"{day}.parquet"
    pl.DataFrame(
        {
            "symbol": ["2330"],
            "quote_at": [f"{day}T09:01:00+08:00"],
            "source": [
                "shioaji:historical_ticks_0900_090059_vwap_right_label_0901"
            ],
            "execution_price_0901": [100.25],
            "tick_volume_units_0901": [12.0],
            "source_window_start": [f"{day}T09:00:01+08:00"],
            "source_window_end": [f"{day}T09:00:59+08:00"],
        }
    ).write_parquet(book)
    session = {
        "session_date": day,
        "historical_entry_books": {
            "path": str(book),
            "sha256": _sha256(book),
        },
    }
    retained = []

    assert _retained_entry_book_source_days(
        tmp_path, session, {"2330"}, {}, retained
    ) == {}
    assert retained == [
        {
            "symbol": "2330",
            "minute_key": f"{day}T09:01",
            "high": 100.25,
            "low": 100.25,
            "volume_shares": 12_000.0,
        }
    ]


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
    appended = _verify_calendar_coverage(proof, dates, prefix=False)
    assert appended["source_file_unchanged"] is False
    assert appended["identity_contract"] == (
        "receipt_validated_exact_requested_session_set_after_append"
    )


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
