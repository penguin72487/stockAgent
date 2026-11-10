from datetime import date
import hashlib
import json

import polars as pl
import pytest

from stockagent.live.tw_day_trade_nontrade import select_last_observations


def _ticks(rows):
    return pl.DataFrame(rows, schema={"date": pl.String, "Time": pl.String,
        "stock_id": pl.String, "deal_price": pl.Float64, "volume": pl.Int64}, orient="row")


def test_last_observation_is_not_a_1330_execution_or_a_later_auction():
    frame = _ticks([
        ("2026-11-04", "09:15:12.100000", "2330", 100., 2),
        ("2026-11-04", "13:24:33.123456", "2330", 101., 1),
        ("2026-11-04", "13:25:10.000000", "2330", 999., 0),
        ("2026-11-04", "13:33:00.000000", "2330", 102., 1),
    ])
    actual = select_last_observations(frame, day=date(2026, 11, 4), symbols={"2330"})["2330"]
    assert actual["reference_price"] == 101.
    assert actual["reference_observed_at"] == "2026-11-04T13:24:33.123456+08:00"
    assert actual["source_row_index"] == 1
    assert "fill_at" not in actual


@pytest.mark.parametrize("rows, message", [
    ([("2026-11-04", "10:00:00", "2330", 100., 0)], "no positive"),
    ([("2026-11-04", "10:00:00", "2330", 100., 1),
      ("2026-11-04", "10:00:00", "2330", 101., 1)], "ambiguous"),
    ([("2026-11-03", "10:00:00", "2330", 100., 1)], "date"),
    ([("2026-11-04", "invalid", "2330", 100., 1)], "clock"),
])
def test_last_observation_refuses_unknown_or_ambiguous_evidence(rows, message):
    with pytest.raises(ValueError, match=message):
        select_last_observations(_ticks(rows), day=date(2026, 11, 4), symbols={"2330"})


def test_same_timestamp_same_price_is_safe_without_claiming_execution():
    rows = [("2026-11-04", "13:00:00", "2330", 100., 1),
            ("2026-11-04", "13:00:00", "2330", 100., 2)]
    result = select_last_observations(_ticks(rows), day=date(2026, 11, 4), symbols={"2330"})["2330"]
    assert result["source_row_index"] == 1


def test_tied_price_can_only_be_selected_by_matching_official_close():
    rows = [("2026-11-04", "13:00:00", "2330", 100., 1),
            ("2026-11-04", "13:00:00", "2330", 101., 2)]
    actual = select_last_observations(_ticks(rows), day=date(2026, 11, 4), symbols={"2330"},
                                      official_closes={"2330": 100.})["2330"]
    assert actual["reference_price"] == 100.
    assert actual["same_clock_price_tie_resolved_by_official_close"] is True
    with pytest.raises(ValueError, match="does not resolve"):
        select_last_observations(_ticks(rows), day=date(2026, 11, 4), symbols={"2330"},
                                 official_closes={"2330": 99.})


def test_nontrade_settlement_retains_prior_exit_and_appends_no_fill(tmp_path, monkeypatch):
    from scripts import repair_tw_day_trade_flat_opening as repair
    from test_day_trade_terminal_close import setup
    from test_day_trade_margin_carry import register
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    position.update(last_exit_at="2026-08-13T12:00:00+08:00", last_exit_price=996.)
    before_fills = engine.fills_path.read_bytes()
    before_orders = engine.orders_path.read_bytes()
    official = tmp_path / "official.json"
    official.write_text("pinned official proof")
    sha = hashlib.sha256(official.read_bytes()).hexdigest()
    proof = {"session_date": "2026-08-13", "symbol": "2330", "price": 995.,
             "source": "twse_official_daily_close", "source_sha256": sha,
             "source_path": str(official), "price_basis": "official_session_close"}
    reference = {"symbol": "2330", "reference_price": 995.,
                 "reference_observed_at": "2026-08-13T12:58:20.000000+08:00"}
    monkeypatch.setattr(repair, "load_pinned_finmind_ticks",
        lambda *args, **kwargs: ({"2330": reference}, [{"path": "source", "sha256": "digest"}]))
    quotes = {"2330": {"session_close_evidence": proof}}
    item = {"day": date(2026, 8, 13), "bars": {}, "nontrade_tick_receipt": "receipt"}
    result = repair._settle_missing_close_nontrade(engine, mode, item, quotes)
    assert result["settled_shares"] > 0 and result["exchange_fill"] is False
    assert position["signed_shares"] == 0 and position["status"] == "nontrade_book_settled"
    assert position["last_exit_at"] == "2026-08-13T12:00:00+08:00"
    assert position["last_exit_price"] == 996. and "exit_at" not in position
    assert position["nontrade_settlement"]["accounting_at"] == "2026-08-13T13:30:00+08:00"
    assert position["nontrade_settlement"]["reference"]["reference_observed_at"] == reference["reference_observed_at"]
    assert position["nontrade_settlement"]["simulated_fill"] is False
    assert mode["execution_evidence_complete"] is False
    assert engine.fills_path.read_bytes() == before_fills
    assert engine.orders_path.read_bytes() == before_orders
    event = json.loads(engine.events_path.read_text().splitlines()[-1])
    assert event["event"] == "nontrade_last_observed_accounting_settlement"
    assert event["exchange_fill"] is False and event["broker_fill"] is False
    engine._mark_mode(spec.market, _now(13, 30), {}, append_history=False)
    engine._persist(_now(13, 30))
    committed_mode = json.loads(engine.status_path.read_text())["modes"][spec.market]
    assert committed_mode["nontrade_settlement_count"] == 1
    assert committed_mode["nontrade_settlement_shares"] == result["settled_shares"]
    cumulative = mode["cumulative_nontrade_settlement_net_pnl_twd"]
    assert register(engine, spec, 1, -.2) == "registered"
    next_mode = engine.state["modes"][spec.market]
    assert not next_mode.get("nontrade_settlement_count")
    assert next_mode["cumulative_nontrade_settlement_net_pnl_twd"] == cumulative
