from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from scripts import audit_tw_day_trade_opening_replay_inputs as audit


def _fixture(tmp_path: Path):
    day = date(2026, 10, 28)
    root = tmp_path / "signals"
    source = root / "2026-10-28T09:00:01+08:00" / "first" / "summary.json"
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps({"signal_id": "first", "market": "m", "execution_mode": "tw_day_trade",
                                  "opening_coverage_gate_basis": "observed_or_known_prior_trade_model_only"}))
    pl.DataFrame({"symbol": ["2330", "0051", "0050"], "target_weight": [0.1, 0.2, 0.0],
                  "open_price": [100.0, None, None]}).write_parquet(source.with_name("target_weights.parquet"))
    opening = tmp_path / "opening.json"
    opening.write_text(json.dumps({"contract": "daily_opening_original_clock_deadline_v1",
        "session_date": str(day), "expected_markets": ["m"], "publication": {"modes": {
            "m": {"state": "published", "signal_id": "first", "artifact_published_at": "2026-10-28T09:00:02+08:00"}}}}))
    state = tmp_path / "ledger"
    state.mkdir()
    (state / "state.json").write_text(json.dumps({"simulation_only": True, "production_order_possible": False,
        "modes": {"m": {"session_date": str(day), "engine_status": "session_flat_after_exit", "positions": {}}}}))
    events = [
        {"event": "signal_registered", "market": "m", "session_date": str(day),
         "signal_id": "later", "recorded_at": "2026-10-28T09:01:00+08:00"},
        {"event": "signal_registered", "market": "m", "session_date": str(day),
         "signal_id": "revision", "recorded_at": "2026-10-28T09:02:00+08:00"},
    ]
    (state / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    return day, opening, state, SimpleNamespace(market="m", signal_market=None, live_output_dir=root), source


def test_first_publication_pinned_separately_from_all_registered_revisions(tmp_path, monkeypatch):
    day, opening, state, spec, source = _fixture(tmp_path)
    monkeypatch.setattr(audit, "load_local_stock_0901_vwaps", lambda *a, **k: ({}, {"error_counts": {}}))
    report, missing = audit.inspect_sources(day=day, audit_path=opening, state_dir=state,
                                            specs=[spec], minute_roots=(tmp_path / "minutes",))
    mode = report["modes"]["m"]
    assert report["status"] == "source_inventory_only_not_a_replay_or_deployment_acceptance"
    assert mode["first_published_signal_id"] == "first"
    assert mode["first_registered_matches_first_publication"] is False
    assert [event["signal_id"] for event in mode["registrations"]] == ["later", "revision"]
    assert mode["summary_path"] == str(source.resolve()) and mode["summary_sha256"]
    assert mode["current_state_nonflat_positions"] == 0
    assert missing == ["0051"]
    assert report["ledger_modified"] is False


def test_native_daily_inventory_keeps_original_publication_and_evidence_errors(tmp_path, monkeypatch):
    day, opening, state, spec, _source = _fixture(tmp_path)
    payload = json.loads(opening.read_text())
    payload["contract"] = "daily_native_opening_latency_audit_v1"
    payload["opening_publication_slo"] = payload.pop("publication")
    payload["opening_publication_slo"].update(
        contract="all_enabled_mode_immutable_publication_stages_v1", expected_mode_count=1,
    )
    payload["evidence_errors"] = ["malformed_attempt_rows:1"]
    opening.write_text(json.dumps(payload))
    monkeypatch.setattr(audit, "load_local_stock_0901_vwaps", lambda *a, **k: ({}, {"error_counts": {}}))
    report, missing = audit.inspect_sources(day=day, audit_path=opening, state_dir=state,
                                            specs=[spec], minute_roots=())
    assert missing == ["0051"]
    assert report["modes"]["m"]["first_published_signal_id"] == "first"
    assert report["opening_audit_contract"] == "daily_native_opening_latency_audit_v1"
    assert report["opening_audit_evidence_errors"] == ["malformed_attempt_rows:1"]
    assert report["ledger_modified"] is False
    payload["opening_publication_slo"]["expected_mode_count"] = 2
    opening.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="publication contract or roster differs"):
        audit.inspect_sources(day=day, audit_path=opening, state_dir=state, specs=[spec], minute_roots=())


def test_first_publication_rejects_duplicate_symbol_or_wrong_roster(tmp_path):
    day, opening, state, spec, source = _fixture(tmp_path)
    pl.DataFrame({"symbol": ["0051", "0051"], "target_weight": [0.1, 0.2],
                  "open_price": [None, None]}).write_parquet(source.with_name("target_weights.parquet"))
    with pytest.raises(ValueError, match="duplicate or empty signal symbol"):
        audit.inspect_sources(day=day, audit_path=opening, state_dir=state, specs=[spec], minute_roots=())
    with pytest.raises(ValueError, match="enabled modes differ"):
        audit.inspect_sources(day=day, audit_path=opening, state_dir=state, specs=[], minute_roots=())


def test_reference_inventory_does_not_turn_ambiguous_ohlc_into_no_trade(tmp_path, monkeypatch):
    from scripts import rebuild_tw_day_trade_open_price_replay as replay

    twse = tmp_path / "twse.parquet"
    tpex = tmp_path / "tpex.parquet"
    pl.DataFrame({
        "date": ["2026-10-27", "2026-10-27", "2026-10-28", "2026-10-28", "2026-10-28"],
        "證券代號": ["0051", "00647L", "2330", "0051", "00647L"],
        "開盤價": ["9", "5", "100", "--", "--"],
        "最高價": ["10", "5", "101", "--", "--"],
        "最低價": ["9", "5", "99", "--", "--"],
        "收盤價": ["10", "5", "100", "--", "--"],
        "成交股數": ["1000", "1000", "1000", "0", "464"],
    }).write_parquet(twse)
    pl.DataFrame(schema={"date": pl.String, "代號": pl.String, "開盤": pl.String,
                         "最高": pl.String, "最低": pl.String, "收盤": pl.String,
                         "成交股數": pl.String}).write_parquet(tpex)
    monkeypatch.setattr(replay, "_verified_local_0901_observations",
                        lambda **kw: ({"2330": 100.0}, {"0051"}))
    proof = audit._verify_causal_references(
        day=date(2026, 10, 28), symbols={"2330", "0051", "00647L"}, books={},
        twse_path=twse, tpex_path=tpex,
    )
    assert proof["references"]["2330"]["basis"] == "first_minute_trade"
    assert proof["references"]["0051"]["previous_trade"]["price"] == 10
    assert proof["references"]["0051"]["basis"] == "previous_real_trade_model_reference_only"
    assert proof["unresolved_symbols"] == ["00647L"]
    assert proof["not_a_fill_or_replay_acceptance"] is True
