from datetime import date, datetime, timedelta
from copy import deepcopy
import hashlib
import json

import pytest
import polars as pl

from scripts.rebuild_tw_day_trade_minute_curves import rebuild_carried_strategy_marks
from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT


def fixture():
    days = ["2026-08-13", "2026-08-14"]
    p = dict(position_id="p", symbol="2330", side="long", entry_price=100.,
             buy_fee_rate=0., sell_fee_rate=0., cash_buy_fee_rate=0., cash_sell_fee_rate=0.,
             margin_carry_contract=MARGIN_CARRY_CONTRACT, margin_converted_at=days[0]+"T13:30:00+08:00")
    fills = [dict(position_id="p", market="a", symbol="2330", purpose="entry", quantity=1000,
                  recorded_at=days[0]+"T09:01:00+08:00", session_date=days[0], price=100., fee_and_tax_twd=10.),
             dict(position_id="p", market="a", symbol="2330", purpose="exit", quantity=750,
                  recorded_at=days[1]+"T09:02:00+08:00", session_date=days[1], price=130.,
                  net_pnl_twd=-2510., entry_fee_allocated_twd=10.)]
    mode = dict(initial_capital_twd=100000., margin_carry_contract=MARGIN_CARRY_CONTRACT,
                share_replacement_ledger=[dict(position_id="p", recorded_at=days[1]+"T09:00:00+08:00",
                    old_signed_shares=1000, new_signed_shares=750, new_entry_price=100/.75, ratio=.75, cash_per_old_share=2.5)],
                corporate_action_ledger=[dict(ex_date=days[1], payment_date="2026-08-15", amount_twd=2500.)],
                carry_cost_ledger=[dict(date=days[1], amount_twd=5., kind="financing_interest")])
    rows = []
    for day in days:
        for clock in ["09:01", "13:30"]:
            realized = -2510. if day == days[1] and clock == "13:30" else 0.
            net = -10. if day == days[0] else (-2510. if clock == "09:01" else 0.)
            earned, cost = (2500., 5.) if day == days[1] else (0., 0.)
            rows.append(dict(market="a", minute=f"{day}T{clock}+08:00", session_date=day,
                initial_capital_twd=100000., cumulative_realized_net_pnl_twd=realized,
                open_net_liquidation_pnl_twd=net, total_equity_twd=100000+realized+net+earned-cost,
                cumulative_corporate_action_net_twd=earned, cumulative_carry_cost_twd=cost,
                margin_carry_contract=MARGIN_CARRY_CONTRACT, open_position_count=int(not realized)))
    class Store:
        def prices(self, symbol, day):
            # No 09:01 print on day 2: carry the prior observed price through
            # the physical action, never use day 2's future close as its open.
            start = datetime.fromisoformat(day+"T09:01:00+08:00")
            return {(start+timedelta(minutes=i)).isoformat(timespec="minutes"): (100. if day == days[0] else 130.)
                    for i in range(270) if day == days[0] or i > 0}
    return rows, {day: {"a": [p]} for day in days}, Store(), {"modes": {"a": mode}}, fills


def test_carried_revaluation_preserves_fills_basis_cash_interest_and_endpoints():
    rows, positions, store, state, fills = fixture()
    unchanged = deepcopy((rows, positions, state, fills))
    rebuilt, stats = rebuild_carried_strategy_marks(rows, positions, store, state=state, fill_rows=fills,
                                                    start=date(2026, 8, 13), end=date(2026, 8, 14))
    assert len(rebuilt) == 540 and stats["carried_fill_book_revalued_without_execution"]
    assert rebuilt[1]["total_equity_twd"] == pytest.approx(99990.)
    assert rebuilt[271]["total_equity_twd"] == pytest.approx(99985.)
    assert rebuilt[271]["corporate_action_receivable_twd"] == 2500.
    assert rebuilt[271]["open_position_count"] == 0
    assert (rows, positions, state, fills) == unchanged
    for endpoint in rows:
        actual = next(r for r in rebuilt if r["minute"] == endpoint["minute"])
        assert actual["total_equity_twd"] == endpoint["total_equity_twd"]


def test_new_paper_entry_does_not_reprice_older_cohort_without_a_minute_trade():
    days = ["2026-08-13", "2026-08-14"]
    base = dict(symbol="2330", side="long", buy_fee_rate=0., sell_fee_rate=0.,
                cash_buy_fee_rate=0., cash_sell_fee_rate=0.,
                margin_carry_contract=MARGIN_CARRY_CONTRACT)
    old = {**base, "position_id": "old", "entry_price": 9.51,
           "margin_converted_at": days[0] + "T13:30:00+08:00"}
    new = {**base, "position_id": "new", "entry_price": 9.51,
           "margin_converted_at": days[1] + "T13:30:00+08:00"}
    fills = [dict(position_id="old", market="a", symbol="2330", purpose="entry",
                  quantity=1000, fill_at=days[0]+"T09:01:00+08:00",
                  recorded_at=days[0]+"T09:01:00+08:00", session_date=days[0],
                  price=9.51, fee_and_tax_twd=0.),
             dict(position_id="new", market="a", symbol="2330", purpose="entry",
                  quantity=1000, fill_at=days[1]+"T09:01:00+08:00",
                  recorded_at=days[1]+"T09:01:00+08:00", session_date=days[1],
                  price=9.51, fee_and_tax_twd=0.)]
    rows = [dict(market="a", minute=f"{day}T{clock}+08:00", session_date=day,
                 initial_capital_twd=100000., cumulative_realized_net_pnl_twd=0.,
                 open_net_liquidation_pnl_twd=net,
                 total_equity_twd=100000. + net,
                 cumulative_corporate_action_net_twd=0., cumulative_carry_cost_twd=0.,
                 margin_carry_contract=MARGIN_CARRY_CONTRACT, open_position_count=count)
            for day, count, opening, closing in ((days[0], 1, 40., 40.),
                                                  (days[1], 2, 40., 100.))
            for clock, net in (("09:01", opening), ("13:30", closing))]

    class Store:
        def prices(self, symbol, day):
            first = datetime.fromisoformat(day + "T09:01:00+08:00")
            return {(first + timedelta(minutes=i)).isoformat(timespec="minutes"):
                    (9.55 if day == days[0] else 9.56)
                    for i in range(270) if day == days[0] or i >= 44}

    state = {"modes": {"a": dict(initial_capital_twd=100000.,
                                 margin_carry_contract=MARGIN_CARRY_CONTRACT,
                                 share_replacement_ledger=[], corporate_action_ledger=[],
                                 carry_cost_ledger=[])}}
    positions = {days[0]: {"a": [old]}, days[1]: {"a": [old, new]}}
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows, positions, Store(), state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14))
    by_minute = {row["minute"]: row for row in rebuilt}
    assert by_minute[days[1]+"T09:02+08:00"]["total_equity_twd"] == pytest.approx(100040.)
    assert by_minute[days[1]+"T09:44+08:00"]["total_equity_twd"] == pytest.approx(100040.)
    assert by_minute[days[1]+"T09:45+08:00"]["total_equity_twd"] == pytest.approx(100100.)
    assert stats["differing_original_equity_points"] == 0


def test_carried_revaluation_rejects_a_changed_endpoint_or_impossible_exit():
    rows, positions, store, state, fills = fixture()
    rows[-1]["cumulative_realized_net_pnl_twd"] = 99.
    with pytest.raises(RuntimeError, match="endpoint accounting"):
        rebuild_carried_strategy_marks(rows, positions, store, state=state, fill_rows=fills,
                                       start=date(2026, 8, 13), end=date(2026, 8, 14))


def test_carried_opening_revaluation_requires_completed_bar_and_preserves_fills():
    rows, positions, store, state, fills = fixture()
    original_fills = deepcopy(fills)
    original_prices = store.prices

    class ShiftedStore:
        def prices(self, symbol, day):
            prices = original_prices(symbol, day)
            if day == "2026-08-13":
                prices["2026-08-13T09:01+08:00"] = 101.
            return prices

    rebuilt, _ = rebuild_carried_strategy_marks(
        rows, positions, ShiftedStore(), state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14),
        revalue_opening_marks=True,
    )
    opening = next(row for row in rebuilt if row["minute"] == "2026-08-13T09:01+08:00")
    closing = next(row for row in rebuilt if row["minute"] == "2026-08-13T13:30+08:00")
    assert opening["opening_mark_revalued_from_completed_bar"] is True
    assert opening["total_equity_twd"] == pytest.approx(100990.)
    assert closing["total_equity_twd"] == rows[1]["total_equity_twd"]
    assert fills == original_fills

    class MissingOpeningStore:
        def prices(self, symbol, day):
            prices = original_prices(symbol, day)
            if day == "2026-08-13":
                prices.pop("2026-08-13T09:01+08:00")
            return prices

    with pytest.raises(RuntimeError, match="missing completed 09:01 valuation bar"):
        rebuild_carried_strategy_marks(
            rows, positions, MissingOpeningStore(), state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14),
            revalue_opening_marks=True,
        )


def test_carried_prices_require_exact_source_receipt_and_positive_trade_volume(tmp_path):
    from scripts.rebuild_tw_day_trade_minute_curves import MinutePriceStore
    from downloader.download_shioaji_tw_minute_kbars import RECEIPT_SCHEMA_VERSION, SOURCE_NAME, STORAGE_FREQUENCY
    path = tmp_path / "minute_chunks/2330/2026-08-13_2026-08-13.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({"date": [date(2026, 8, 13)]*2,
                  "ts": [datetime(2026, 8, 13, 9, 1), datetime(2026, 8, 13, 9, 2)],
                  "Close": [100., 200.], "Volume": [1., 0.]}).write_parquet(path)
    proof = dict(schema_version=RECEIPT_SCHEMA_VERSION, source=SOURCE_NAME, storage_frequency=STORAGE_FREQUENCY,
                 symbol="2330", start_date="2026-08-13", end_date="2026-08-13", status="ok", rows=2,
                 returned_dates=["2026-08-13"], output_receipt=dict(path=str(path), size=path.stat().st_size,
                     sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    store = MinutePriceStore([tmp_path], [], require_receipts=True)
    store.prepare({"2330": {"2026-08-13"}})
    assert not store.prices("2330", "2026-08-13")
    path.with_suffix(".receipt.json").write_text(json.dumps(proof))
    store.invalidate("2330", "2026-08-13")
    store.prepare({"2330": {"2026-08-13"}})
    assert store.prices("2330", "2026-08-13") == {"2026-08-13T09:01+08:00": 100.}
    proof["output_receipt"]["sha256"] = "changed"
    path.with_suffix(".receipt.json").write_text(json.dumps(proof))
    store.invalidate("2330", "2026-08-13")
    assert not store.prices("2330", "2026-08-13")


def test_live_writer_prevents_late_publication_during_opening_window(tmp_path):
    import fcntl
    from scripts.rebuild_tw_day_trade_minute_curves import _assert_minute_publication_window
    with (tmp_path / ".engine.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="protected window"):
            _assert_minute_publication_window(tmp_path, datetime(2026, 9, 10, 9, 0))
        _assert_minute_publication_window(tmp_path, datetime(2026, 9, 10, 3, 0))
