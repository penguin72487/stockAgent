from datetime import date, datetime, timedelta
import json

import pytest

from scripts.audit_shioaji_opening_simulation_probe import (
    DEFAULT_MARKET, audit, simulation_deals, source_observations,
)
from stockagent.live.opening_price_journal import pack_observation_details
from stockagent.live.tw_stock_simulation_execution import (
    TAIPEI, StockSimulationIntent, StockSimulationJournal, account_fingerprint,
)


SESSION = date(2026, 10, 30)
GATE = datetime(2026, 10, 30, 9, 0, tzinfo=TAIPEI)


def source_row(symbol="2330", **changes):
    return {"symbol": symbol, "state": "valid_open", "source": "twse_tpex:mis",
        "open_price": 100, "available": True,
        "market_timestamp_ms": int((GATE + timedelta(milliseconds=100)).timestamp() * 1000),
        "received_timestamp_ms": int((GATE + timedelta(milliseconds=200)).timestamp() * 1000),
        "simtrade": False} | changes


def event(rows, *, provider="mis", at=None):
    return {"session_date": SESSION.isoformat(), "provider": provider,
        "event": "observations_delta", "observed_at_taipei": (at or GATE + timedelta(seconds=1)).isoformat(),
        **pack_observation_details({"observations": rows})}


def make_fill(path, *, with_event=True, market=DEFAULT_MARKET):
    account = account_fingerprint("sim-broker", "sim-account")
    journal = StockSimulationJournal(path, account_key=account)
    intent = StockSimulationIntent("probe:2330", market, "2330", GATE, 1000, "Buy")
    row = journal.prepare_and_claim(intent)
    filled = GATE + timedelta(milliseconds=300)
    callback = GATE + timedelta(milliseconds=700)
    deal = {"custom_field": row["tag"], "trade_id": "trade-1", "exchange_seq": "1",
        "broker_id": "sim-broker", "account_id": "sim-account", "code": "2330",
        "action": "Buy", "order_cond": "Cash", "order_lot": "Common",
        "price": 100.5, "quantity": 1, "ts": filled.timestamp()}
    if with_event:
        journal.record_event("StockDeal", deal, callback)
    journal.stock_deal(deal, received_at=callback)
    journal.close()


def write_events(path, *events):
    path.write_text("".join(json.dumps(row) + "\n" for row in events), encoding="utf-8")


def test_missing_journal_is_waiting_and_does_not_read_or_create_files(tmp_path):
    journal, source = tmp_path / "not-created.sqlite", tmp_path / "not-created.jsonl"
    result = audit(SESSION, journal, source)
    assert result["simulation_state"] == "journal_missing"
    assert result["comparison_state"] == "waiting_simulation_deal"
    assert result["source_state"] == "not_read_without_simulation_deal"
    assert result["simulation_deal_count"] == 0
    assert result["orders_submitted"] is False and result["ledger_modified"] is False
    assert not journal.exists() and not source.exists()


def test_compares_source_observation_with_deal_not_an_order_ack(tmp_path):
    journal, source = tmp_path / "sim.sqlite", tmp_path / "source.jsonl"
    make_fill(journal)
    write_events(source, event([source_row()]))
    result = audit(SESSION, journal, source)
    row = result["comparisons"][0]
    assert result["simulation_state"] == "observed" and result["source_state"] == "observed"
    assert result["comparison_state"] == "observed_comparison_not_exchange_price_verified"
    assert row["simulation_fill_after_open_ms"] == 300
    assert row["callback_received_after_open_ms"] == 700
    assert row["sources"][0]["journal_event_after_open_ms"] == 1000
    assert row["sources"][0]["callback_minus_source_journal_event_ms"] == -300
    assert row["sources"][0]["simulation_minus_source_price"] == "0.5"
    assert not row["sources"][0]["same_price_as_simulation_deal"]
    assert row["shares"] == 1000
    assert "sim-account" not in json.dumps(result)


def test_fill_clock_never_substitutes_for_missing_callback_clock(tmp_path):
    journal, source = tmp_path / "sim.sqlite", tmp_path / "source.jsonl"
    make_fill(journal, with_event=False)
    write_events(source, event([source_row()]))
    row = audit(SESSION, journal, source)["comparisons"][0]
    assert row["callback_received_at"] is None
    assert row["sources"][0]["callback_minus_source_journal_event_ms"] is None
    assert audit(SESSION, journal, source)["comparison_state"] == "incomplete_or_conflicting_evidence"


def test_wrong_account_callback_cannot_prove_arrival(tmp_path):
    journal, source = tmp_path / "sim.sqlite", tmp_path / "source.jsonl"
    make_fill(journal, with_event=False)
    owner = StockSimulationJournal(journal, account_key=account_fingerprint("sim-broker", "sim-account"))
    owner.record_event("StockDeal", {"custom_field": "S00001", "trade_id": "trade-1", "exchange_seq": "1",
        "broker_id": "sim-broker", "account_id": "different-account", "code": "2330", "action": "Buy",
        "order_lot": "Common", "price": 100.5, "quantity": 1}, GATE + timedelta(milliseconds=700))
    owner.close()
    write_events(source, event([source_row()]))
    row = audit(SESSION, journal, source)["comparisons"][0]
    assert row["callback_received_at"] is None


def test_unrelated_strategy_fills_are_excluded(tmp_path):
    journal, source = tmp_path / "sim.sqlite", tmp_path / "source.jsonl"
    make_fill(journal, market="actual-other-paper-mode")
    result = audit(SESSION, journal, source)
    assert result["simulation_state"] == "no_simulation_deals"
    assert result["simulation_deal_count"] == 0


def test_source_conflict_is_not_silenced_or_treated_as_official_truth(tmp_path):
    journal, source = tmp_path / "sim.sqlite", tmp_path / "source.jsonl"
    make_fill(journal)
    write_events(source, event([source_row()]),
                 event([source_row(open_price=101)], at=GATE + timedelta(seconds=2)))
    row = audit(SESSION, journal, source)["comparisons"][0]
    assert row["source_price_conflict"]
    assert row["sources"][0]["different_later_prices"] == ["101"]
    assert audit(SESSION, journal, source)["comparison_state"] == "incomplete_or_conflicting_evidence"


def test_invalid_bulk_event_never_commits_a_partial_prefix(tmp_path):
    source = tmp_path / "source.jsonl"
    write_events(source, event([source_row(), source_row("2317", simtrade=True)]))
    result = source_observations(source, SESSION, {"2330", "2317"})
    assert result["state"] == "degraded" and result["invalid_lines"] == 1
    assert not result["rows"]["2330"] and not result["rows"]["2317"]


def test_packed_source_rows_and_partial_tail_are_reported(tmp_path):
    source = tmp_path / "source.jsonl"
    bulk = [source_row()] + [source_row(str(3000 + index)) for index in range(127)]
    write_events(source, event(bulk))
    with source.open("ab") as handle:
        handle.write(b'{"session_date":')
    result = source_observations(source, SESSION, {"2330"})
    assert result["state"] == "degraded" and result["invalid_lines"] == 1
    assert result["rows"]["2330"]["mis"]["open_price"] == "100"


@pytest.mark.parametrize("bad", ["2026-10-30T09:00:00", "2026-10-30T08:59:59+08:00"])
def test_source_clock_must_be_aware_and_after_open(tmp_path, bad):
    source = tmp_path / "source.jsonl"
    value = event([source_row()])
    value["observed_at_taipei"] = bad
    write_events(source, value)
    result = source_observations(source, SESSION, {"2330"})
    assert result["state"] == "degraded" and not result["rows"]["2330"]


def test_read_only_rejects_noncanonical_database(tmp_path):
    import sqlite3
    path = tmp_path / "unrelated.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE meta (key TEXT, value TEXT)")
    with pytest.raises(ValueError, match="canonical"):
        simulation_deals(path, SESSION, DEFAULT_MARKET)


def test_cli_refuses_to_replace_any_input(tmp_path, monkeypatch):
    import sys
    from scripts.audit_shioaji_opening_simulation_probe import main
    journal = tmp_path / "journal.sqlite"
    make_fill(journal)
    before = journal.read_bytes()
    monkeypatch.setattr(sys, "argv", ["audit", "--session-date", SESSION.isoformat(),
        "--journal", str(journal), "--source-journal", str(tmp_path / "source.jsonl"),
        "--output", str(journal)])
    with pytest.raises(SystemExit) as failure:
        main()
    assert failure.value.code == 2 and journal.read_bytes() == before


def test_optional_daily_aggregate_has_price_comparison_and_file_identity(tmp_path):
    import polars as pl
    journal, source = tmp_path / "sim.sqlite", tmp_path / "source.jsonl"
    twse, tpex = tmp_path / "twse.parquet", tmp_path / "tpex.parquet"
    make_fill(journal)
    write_events(source, event([source_row()]))
    pl.DataFrame({"date": [SESSION.isoformat()], "證券代號": ["2330"],
        "開盤價": ["100"], "最高價": ["102"], "最低價": ["99"],
        "收盤價": ["101"], "成交股數": ["10000"]}).write_parquet(twse)
    pl.DataFrame({"date": [SESSION.isoformat()], "代號": ["9999"],
        "開盤": ["10"], "最高": ["11"], "最低": ["9"],
        "收盤": ["10"], "成交股數": ["5000"]}).write_parquet(tpex)
    result = audit(SESSION, journal, source, daily_paths=(twse, tpex))
    reference = result["comparisons"][0]["postclose_daily_reference"]
    assert result["postclose_daily_state"] == "loaded_source_file_origin_and_receipts_not_verified"
    assert reference["source"] == "twse_daily_ohlcv"
    assert reference["simulation_minus_daily_open_price"] == "0.5"
    assert reference["same_price_as_simulation_deal"] is False
    assert len(result["postclose_daily_files"]) == 2
    assert all(len(item["sha256"]) == 64 for item in result["postclose_daily_files"])


def test_zero_volume_daily_reference_never_becomes_an_open_price(tmp_path):
    import polars as pl
    from scripts.audit_shioaji_opening_simulation_probe import official_daily_reference
    twse, tpex = tmp_path / "twse.parquet", tmp_path / "tpex.parquet"
    pl.DataFrame({"date": [SESSION.isoformat()], "證券代號": ["2330"],
        "開盤價": [None], "最高價": [None], "最低價": [None],
        "收盤價": [None], "成交股數": ["0"]}).write_parquet(twse)
    pl.DataFrame({"date": [SESSION.isoformat()], "代號": ["9999"],
        "開盤": ["10"], "最高": ["11"], "最低": ["9"],
        "收盤": ["10"], "成交股數": ["5000"]}).write_parquet(tpex)
    result = official_daily_reference(SESSION, twse, tpex, {"2330"})
    assert result["rows"]["2330"]["state"] == "zero_volume_no_daily_trade"
    assert "open_price" not in result["rows"]["2330"]
