from datetime import date, datetime
import hashlib
import json

import polars as pl
import pytest

from scripts import rebuild_tw_day_trade_open_price_replay as replay
from stockagent.live.tw_day_trade_nontrade import select_pinned_last_minute_observations


def _source(tmp_path, *, duplicate=False):
    path = tmp_path / "minute.parquet"
    rows = [("2330", datetime(2026, 8, 13, 12, 58), 995.0, 2000.0)]
    if duplicate:
        rows.append(rows[0])
    pl.DataFrame(rows, schema={"symbol": pl.String, "ts": pl.Datetime,
                              "Close": pl.Float64, "volume_shares": pl.Float64},
                 orient="row").write_parquet(path)
    source = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
              "size_bytes": path.stat().st_size, "symbols": ["2330"]}
    bars = {"2330": {"2026-08-13T12:58+08:00": {"close": 995.0, "volume_shares": 2000.0}}}
    return path, bars, {"source_files": [source]}


def test_minute_reference_is_source_pinned_and_never_claims_an_exact_trade_time(tmp_path):
    path, bars, coverage = _source(tmp_path)
    observations, pins = select_pinned_last_minute_observations(
        day=date(2026, 8, 13), symbols={"2330"}, bars=bars, coverage=coverage)
    item = observations["2330"]
    assert item["reference_price"] == 995.0
    assert item["reference_interval_start_exclusive"] == "2026-08-13T12:57:00+08:00"
    assert item["reference_interval_end_inclusive"] == "2026-08-13T12:58:00+08:00"
    assert item["exact_trade_time_known"] is False
    assert "fill_at" not in item
    assert pins == [{"path": str(path), "sha256": coverage["source_files"][0]["sha256"]}]


def test_nontrade_minute_refuses_duplicate_keys_and_changed_source(tmp_path):
    path, bars, coverage = _source(tmp_path, duplicate=True)
    with pytest.raises(ValueError, match="not unique"):
        select_pinned_last_minute_observations(
            day=date(2026, 8, 13), symbols={"2330"}, bars=bars, coverage=coverage)
    coverage["source_files"][0]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="pinned evidence"):
        select_pinned_last_minute_observations(
            day=date(2026, 8, 13), symbols={"2330"}, bars=bars, coverage=coverage)


def test_provisional_nontrade_records_assumption_without_order_or_fill(tmp_path):
    from test_day_trade_terminal_close import setup

    (tmp_path / "engine").mkdir()
    engine, spec = setup(tmp_path / "engine")
    mode = engine.state["modes"][spec.market]
    mode["provisional_minute_source_coverage"] = True
    position = next(iter(mode["positions"].values()))
    path, bars, coverage = _source(tmp_path)
    official = tmp_path / "official.json"
    official.write_text("pinned official proof")
    proof = {"session_date": "2026-08-13", "symbol": "2330", "price": 995.,
             "source": "twse_official_daily_close", "source_sha256": hashlib.sha256(official.read_bytes()).hexdigest(),
             "source_path": str(official), "price_basis": "official_session_close"}
    fills, orders = engine.fills_path.read_bytes(), engine.orders_path.read_bytes()
    result = replay._settle_provisional_unobserved_terminal(engine, markets=[spec.market],
        trading_date=date(2026, 8, 13), official_quotes={"2330": {"session_close_evidence": proof}},
        accepted_quotes={}, bars=bars, coverage=coverage, limit_path=path, limit_sha="unused")
    assert result["position_count"] == 1 and result["simulated_fill"] is False
    assert position["signed_shares"] == 0
    assert position["nontrade_settlement"]["reference"]["exact_trade_time_known"] is False
    assert position["nontrade_settlement"]["simulated_fill"] is False
    assert engine.fills_path.read_bytes() == fills and engine.orders_path.read_bytes() == orders
    assert mode["execution_evidence_complete"] is False


def test_research_short_at_limit_with_no_auction_capacity_stops_before_mutation(tmp_path):
    from test_day_trade_terminal_close import setup

    (tmp_path / "engine").mkdir()
    engine, spec = setup(tmp_path / "engine")
    mode = engine.state["modes"][spec.market]
    mode.update(provisional_minute_source_coverage=True, research_short_limit_up_close_penalty_rate=0.07)
    position = next(iter(mode["positions"].values()))
    position.update(signed_shares=-1000, side="short", upper_limit=995.0)
    path, bars, coverage = _source(tmp_path)
    proof = {"session_date": "2026-08-13", "symbol": "2330", "price": 995.0}
    with pytest.raises(ValueError, match="capacity unknown"):
        replay._settle_provisional_unobserved_terminal(engine, markets=[spec.market],
            trading_date=date(2026, 8, 13), official_quotes={"2330": {"session_close_evidence": proof}},
            accepted_quotes={}, bars=bars, coverage=coverage, limit_path=path, limit_sha="unused")
    assert position["signed_shares"] == -1000
    assert "nontrade_settlement" not in position


def test_missing_minutes_can_use_pinned_daily_close_without_inventing_time(tmp_path):
    from test_day_trade_terminal_close import setup
    from scripts.settle_tw_day_trade_official_close import official_closes
    from scripts.audit_tw_day_trade_margin_replay import _verify_provisional_nontrade_event
    from stockagent.live.tw_day_trade_dashboard import _safe_position

    (tmp_path / "engine").mkdir()
    engine, spec = setup(tmp_path / "engine")
    mode = engine.state["modes"][spec.market]
    mode["provisional_minute_source_coverage"] = True
    position = next(iter(mode["positions"].values()))
    raw_root = tmp_path / "raw"
    for exchange, symbol in (("twse", "2330"), ("tpex", "6488")):
        folder = raw_root / f"{exchange}_daily_ohlcv"
        folder.mkdir(parents=True)
        (folder / "2026-08-13.json").write_text(json.dumps({"date": "20260813", "stat": "OK",
            "tables": [{"fields": ["證券代號", "開盤價", "收盤價", "成交股數"],
                        "data": [[symbol, "1000", "995", "2,000"]]}]}))
    rows, sources = official_closes(raw_root, date(2026, 8, 13), pin_sources=True)
    row = rows["2330"]
    proof = {"session_date": "2026-08-13", "symbol": "2330", "price": row["price"],
             "source": row["source"], "source_sha256": row["source_sha256"],
             "source_path": row["source_path"], "price_basis": row["price_basis"]}
    kwargs = dict(markets=[spec.market], trading_date=date(2026, 8, 13),
        official_quotes={"2330": {"session_close_evidence": proof}}, accepted_quotes={},
        bars={}, coverage={"source_files": []}, limit_path=raw_root, limit_sha="unused")
    with pytest.raises(ValueError, match="no verified positive-volume minute"):
        replay._settle_provisional_unobserved_terminal(engine, **kwargs)
    fills, orders = engine.fills_path.read_bytes(), engine.orders_path.read_bytes()
    result = replay._settle_provisional_unobserved_terminal(engine, **kwargs, allow_official_close_reference=True)
    assert result["official_daily_close_time_unknown_symbols"] == ["2330"]
    settlement = position["nontrade_settlement"]
    assert settlement["reference"]["reference_observed_at"] is None
    assert settlement["reference_before_simulated_entry"] is None
    assert settlement["simulated_fill"] is False
    assert _safe_position(position)["nontrade_reference_time_unknown"] is True
    assert engine.fills_path.read_bytes() == fills and engine.orders_path.read_bytes() == orders
    event = [json.loads(line) for line in engine.events_path.read_text().splitlines()
             if json.loads(line).get("event") == "nontrade_last_observed_accounting_settlement"][-1]
    session = {"close": {"nontrade_settlement": result, "terminal_close_sources": sources},
               "intraday_replay": {"source_files": []}}
    entry = next(json.loads(line) for line in engine.fills_path.read_text().splitlines()
                 if json.loads(line).get("purpose") == "entry")
    with pytest.raises(ValueError, match="disclosure"):
        _verify_provisional_nontrade_event(event, session, mode=mode, entry=entry, verify_sources=True)
    verified = _verify_provisional_nontrade_event(event, session, mode=mode, entry=entry,
                                                  verify_sources=True, allow_daily_close_reference=True)
    assert verified["_nontrade_accounting_assumption"] is True
    assert verified["price"] == 995
