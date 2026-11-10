from dataclasses import replace
from datetime import date
import hashlib
import json

import polars as pl
import pytest

from scripts import audit_tw_day_trade_margin_replay as audit
from scripts import rebuild_tw_day_trade_open_price_replay as replay
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
    HISTORICAL_OPEN_FALLBACK_CONTRACT,
    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
    TwDayTradeSimulationEngine,
)
from test_tw_day_trade_simulation import _eligibility, _now, _row, _spec, _summary


def _fixture(tmp_path):
    day = date(2026, 8, 13)
    raw = tmp_path / "raw"
    for exchange, symbol, price, volume in (("twse", "2330", "1,000", "10,000"),
                                             ("tpex", "6488", "535", "0")):
        folder = raw / f"{exchange}_daily_ohlcv"
        folder.mkdir(parents=True)
        (folder / f"{day}.json").write_text(json.dumps({"date": "20260813", "stat": "OK",
            "tables": [{"fields": ["證券代號", "開盤價", "收盤價", "成交股數"],
                        "data": [[symbol, price, price, volume]]}]}))
    spec = replace(_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                   historical_counterfactual_provisional=True, historical_open_price_fallback=True,
                   strict_intraday=True)
    proof, sources = replay._verified_official_open_fallbacks(raw_root=raw, trading_date=day,
        canonical_opens={"2330": 1000, "6488": 535}, symbols={"2330", "6488"})
    return spec, proof, sources


def _quotes(spec, proof, **kwargs):
    return replay._entry_quotes([_row(.1) | {"open_price": 1000}],
        {"2330": {"upper_limit_price": 1100, "lower_limit_price": 900}},
        quote_at=_now(9, 1), spec=spec, canonical_open_by_symbol={"2330": 1000},
        canonical_open_source="official", official_open_fallback_evidence=proof, **kwargs)


def _register(tmp_path, spec, quotes):
    engine = TwDayTradeSimulationEngine(tmp_path / "isolated")
    summary = {**_summary(), "simulation_replay": True, "historical_minute_valuation": True,
        "provisional_minute_source_coverage": True,
        "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE}
    result = engine.register_signal(spec=spec, summary=summary, signal_rows=[_row(.1) | {"open_price": 1000}],
        quotes=quotes, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1),
        counterfactual_open_replay=True)
    return engine, result


def test_missing_minute_uses_pinned_open_only_as_labelled_paper_assumption(tmp_path):
    spec, proof, sources = _fixture(tmp_path)
    assert set(proof) == {"2330"} and len(sources) == 2
    assert proof["2330"]["opening_time_known"] is False
    quotes, quality = _quotes(spec, proof)
    assert quality["missing_0901_price_symbols"] == ["2330"]
    assert quality["official_open_fallback_symbols"] == ["2330"]
    assert quotes["2330"]["execution_price_0901"] is None
    assert quotes["2330"]["quote_at"] is None
    assert quotes["2330"]["historical_source_quote_at"] is None
    engine, result = _register(tmp_path, spec, quotes)
    assert result == "registered"
    signal = json.loads(engine.signals_path.read_text().splitlines()[0])
    fill = json.loads(engine.fills_path.read_text().splitlines()[0])
    assert signal["filled_shares"] == 1000 and signal["execution_price"] == 1000
    assert signal["historical_open_fallback_fill"] is True
    assert signal["counterfactual_0901_price_fill"] is False
    assert fill["fill_contract"] == HISTORICAL_OPEN_FALLBACK_CONTRACT
    assert fill["quote_at"] is None
    assert fill["recorded_at_basis"] == "counterfactual_effective_clock_not_original_publication"
    assert fill["replay_generated_at"] == proof["2330"]["proof_created_at"]
    assert fill["historical_open_fallback_evidence"]["broker_fill"] is False
    assert engine.state["modes"][spec.market]["historical_open_fallback_fill_count"] == 1
    from stockagent.live.tw_day_trade_dashboard import _signal_valuation_position
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    projected = _signal_valuation_position(position)
    assert projected["historical_open_fallback_fill"] is True
    assert projected["replay_generated_at"] == proof["2330"]["proof_created_at"]


@pytest.mark.parametrize("kwargs", [
    {"observed_no_trade_through_0901_symbols": {"2330"}},
    {"official_no_trade_symbols": {"2330"}},
    {"historical_books": {"2330": {"execution_price_0901": 1000, "tick_volume_units_0901": 0}}},
])
def test_confirmed_no_trade_or_explicit_zero_volume_is_not_a_fallback(tmp_path, kwargs):
    spec, proof, _ = _fixture(tmp_path)
    quotes, quality = _quotes(spec, proof, **kwargs)
    assert not quality["official_open_fallback_symbols"]
    assert quotes["2330"]["historical_open_fallback_evidence"] is None


def test_observed_minute_price_and_volume_take_precedence(tmp_path):
    spec, proof, _ = _fixture(tmp_path)
    quotes, quality = _quotes(spec, proof, historical_books={"2330": {
        "execution_price_0901": 1005, "tick_volume_units_0901": 2,
        "observed_volume_unit_0901": "board_lots", "source": "test"}})
    assert not quality["official_open_fallback_symbols"]
    engine, _ = _register(tmp_path, spec, quotes)
    fill = json.loads(engine.fills_path.read_text().splitlines()[0])
    assert fill["price"] == 1005 and fill["historical_open_fallback_fill"] is False
    assert fill["fill_contract"] == REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE


def test_no_live_opt_in_or_mismatched_open(tmp_path):
    spec, proof, _ = _fixture(tmp_path)
    with pytest.raises(ValueError, match="requires a labelled isolated"):
        replace(spec, historical_counterfactual_provisional=False)
    quotes, quality = _quotes(spec, {"2330": proof["2330"] | {"price": 999}})
    assert not quality["official_open_fallback_symbols"]
    engine, _ = _register(tmp_path, spec, quotes)
    assert not engine.fills_path.exists()


def test_independent_audit_rechecks_exact_daily_source_and_disclosure(tmp_path):
    spec, proof, sources = _fixture(tmp_path)
    quotes, _ = _quotes(spec, proof)
    engine, _ = _register(tmp_path, spec, quotes)
    fill = json.loads(engine.fills_path.read_text().splitlines()[0])
    book = tmp_path / "isolated" / "replay_entry_books" / "2026-08-13.parquet"
    book.parent.mkdir()
    pl.DataFrame(schema={"symbol": pl.String}).write_parquet(book)
    digest = hashlib.sha256(book.read_bytes()).hexdigest()
    session = {"session_date": "2026-08-13", "canonical_open": {
        "official_open_fallback_sources": sources, "official_no_trade_print_symbols": []},
        "historical_entry_books": {"sha256": digest, "verified_no_trade_through_0901_symbols": []},
        "modes": [{"market": spec.market, "entry_price_quality": {"official_open_fallback_symbols": ["2330"]}}]}
    rebuild = {"provisional_minute_source_coverage": True,
               "historical_open_fallback_assumption": HISTORICAL_OPEN_FALLBACK_CONTRACT}
    hashes = audit._verify_official_open_fallback_fill(fill, session, rebuild, state_dir=tmp_path / "isolated")
    assert hashes[str(book)] == digest
    with pytest.raises(ValueError, match="disclosure"):
        audit._verify_official_open_fallback_fill(fill | {"simulation_only": False}, session, rebuild,
                                                  state_dir=tmp_path / "isolated")
    source = proof["2330"]["source_path"]
    with open(source, "ab") as stream:
        stream.write(b" ")
    with pytest.raises(ValueError, match="namespace or hash"):
        audit._verify_official_open_fallback_fill(fill, session, rebuild, state_dir=tmp_path / "isolated")


def test_source_free_assumption_session_never_becomes_trade_completeness():
    day = "2026-11-09"
    session = {"session_date": day, "intraday_replay": {"source_files": [], "resolved_symbols": 0,
               "provisional_minute_source_coverage": True, "missing_symbols": ["2330"]},
               "modes": [{"after_close": {"open_position_rows": 0}}]}
    fill = {"session_date": day, "market": "m", "position_id": "p", "symbol": "2330", "quantity": 1000,
            "purpose": "entry", "historical_open_fallback_fill": True}
    nontrade = {"session_date": day, "market": "m", "position_id": "p", "quantity": 1000,
                "_nontrade_accounting_assumption": True,
                "reference": {"source_clock_basis": "official_daily_close_last_trade_time_unknown"}}
    marks = [{"session_date": day, "provisional_minute_source_coverage": True,
              "open_position_count": 1, "valuation_stale": True, "fresh_trade_position_count": 0}
             for _ in range(270)]
    rebuild = {"provisional_minute_source_coverage": True,
               "official_daily_close_reference_when_minute_missing": True,
               "historical_open_fallback_assumption": HISTORICAL_OPEN_FALLBACK_CONTRACT}
    assert audit._source_free_provisional_sessions([session], [fill], marks, [nontrade], rebuild) == frozenset({day})
    assert not audit._source_free_provisional_sessions([session], [fill], marks, [], rebuild)
    assert not audit._source_free_provisional_sessions([session], [fill], marks[:-1], [nontrade], rebuild)
    assert not audit._source_free_provisional_sessions([session], [fill],
        marks[:-1] + [marks[-1] | {"valuation_stale": False}], [nontrade], rebuild)
