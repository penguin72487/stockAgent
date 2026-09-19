from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from scripts.rebuild_tw_day_trade_open_price_replay import (
    _bar_quote,
    _retained_prior_paper_fills,
)
from scripts import rebuild_tw_day_trade_open_price_replay as replay
from scripts.audit_tw_day_trade_capacity_recovery import audit
from scripts.stage_tw_day_trade_prior_paper_source import stage, verify
from stockagent.live import tw_share_replacement
from stockagent.live.tw_day_trade_simulation import REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL


def test_retained_paper_fills_are_pinned_and_volume_weighted(tmp_path):
    rows = [
        {"session_date": "2026-09-17", "market": "mode_a", "symbol": "2330",
         "purpose": "entry", "simulation_only": True,
         "fill_at": "2026-09-17T09:00:11+08:00", "price": 100.0,
         "quantity": 1000, "order_id": "first", "entry_price_source": "paper"},
        {"session_date": "2026-09-17", "market": "mode_a", "symbol": "2330",
         "purpose": "entry", "simulation_only": True,
         "fill_at": "2026-09-17T09:00:20+08:00", "price": 102.0,
         "quantity": 1000, "order_id": "second", "entry_price_source": "paper"},
        {"session_date": "2026-09-17", "market": "unrelated", "symbol": "2317",
         "purpose": "entry", "simulation_only": True,
         "fill_at": "2026-09-17T09:00:11+08:00", "price": 100.0,
         "quantity": 1000},
        {"session_date": "2026-09-17", "market": "mode_a", "symbol": "2454",
         "purpose": "entry", "simulation_only": False,
         "fill_at": "2026-09-17T09:00:11+08:00", "price": 100.0,
         "quantity": 1000},
    ]
    (tmp_path / "fills.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    fills, proof = _retained_prior_paper_fills(
        tmp_path,
        pinned_signal_keys={("2026-09-17", "mode_a")},
        end_date=date(2026, 9, 17),
    )
    assert list(fills) == [("2026-09-17", "mode_a", "2330")]
    assert fills[("2026-09-17", "mode_a", "2330")]["price"] == 101.0
    assert fills[("2026-09-17", "mode_a", "2330")]["fill_at"] == "2026-09-17T09:00:11+08:00"
    assert proof["prior_0900_symbol_sessions"] == 1


def test_retained_paper_fills_reject_invalid_clock(tmp_path):
    (tmp_path / "fills.jsonl").write_text(json.dumps({
        "session_date": "2026-09-17", "market": "mode_a", "symbol": "2330",
        "purpose": "entry", "simulation_only": True,
        "fill_at": "2026-09-17T13:31:00+08:00", "price": 100.0,
        "quantity": 1000,
    }) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="clock"):
        _retained_prior_paper_fills(
            tmp_path,
            pinned_signal_keys={("2026-09-17", "mode_a")},
            end_date=date(2026, 9, 17),
        )


def test_prior_paper_source_snapshot_survives_source_removal_and_detects_tamper(tmp_path):
    import hashlib

    source = tmp_path / "original.jsonl"
    line = b'{"purpose":"entry","quantity":1000}\n'
    source.write_bytes(line)
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    manifest = {"source": str(source), "fills": {
        "2026-09-17|mode_a|2330": {"source_record_sha256": [hashlib.sha256(line).hexdigest()]},
    }}
    replay._atomic_json(candidate / "prior_paper_fills.json", manifest)
    receipt = {"replay_contract": {"entry": REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL},
               "prior_paper_fill_provenance": {
                   "candidate_manifest_sha256": replay._sha256(candidate / "prior_paper_fills.json")}}
    replay._atomic_json(candidate / "rebuild_receipt.json", receipt)
    with pytest.raises(ValueError, match="snapshot not staged"):
        verify(candidate, receipt)
    assert stage(candidate)["records"] == 1
    staged_receipt = json.loads((candidate / "rebuild_receipt.json").read_text())
    assert len(verify(candidate, staged_receipt)) == 1
    source.unlink()
    assert len(verify(candidate, staged_receipt)) == 1
    (candidate / "prior_paper_source_records.jsonl").write_bytes(b"tampered\n")
    with pytest.raises(ValueError, match="snapshot invalid"):
        verify(candidate, staged_receipt)


def test_full_target_exit_quote_keeps_observed_volume_separate():
    observed = datetime(2026, 9, 17, 13, 25, tzinfo=ZoneInfo("Asia/Taipei"))
    bar = {"open": 100.0, "high": 101.0, "low": 99.0,
           "close": 100.5, "volume_shares": 1_000.0}
    quote = _bar_quote({"symbol": "2330"}, bar, observed=observed,
                       bid=100.5, ask=None, full_target=True)
    assert quote["observed_minute_volume_lots"] == 1.0
    assert quote["minute_volume_lots"] > 1_000_000
    assert "no_liquidity_claim" in quote["fill_contract"]
    no_trade = _bar_quote({"symbol": "2330"}, {**bar, "volume_shares": 0.0},
                          observed=observed, bid=100.5, ask=None, full_target=True)
    assert no_trade["minute_volume_lots"] == 0.0


def test_replacement_without_suspension_does_not_block_historical_session(monkeypatch, tmp_path):
    monkeypatch.setattr(tw_share_replacement, "load_share_replacements", lambda root: (
        {"symbol": "2330", "suspension_date": None, "resume_date": date(2026, 9, 18)},
        {"symbol": "2317", "suspension_date": date(2026, 9, 16),
         "resume_date": date(2026, 9, 18)},
    ))
    assert tw_share_replacement.halted_symbols(tmp_path, date(2026, 9, 17)) == {"2317"}


def test_capacity_audit_separates_prior_fill_from_missing_price(tmp_path):
    (tmp_path / "fills.jsonl").write_text(json.dumps({
        "session_date": "2026-09-17", "market": "mode_a", "symbol": "2330",
        "purpose": "entry", "simulation_only": True, "quantity": 1_000,
    }) + "\n", encoding="utf-8")
    signals = [
        {"session_date": "2026-09-17", "market": "mode_a", "symbol": "2330",
         "requested_shares": 3_000, "filled_shares": 1_000,
         "reason": "observed_09_01_minute_capacity_exhausted"},
        {"session_date": "2026-09-17", "market": "mode_a", "symbol": "2317",
         "requested_shares": 2_000, "filled_shares": 0,
         "reason": "observed_09_01_minute_price_unavailable"},
    ]
    (tmp_path / "signals.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in signals), encoding="utf-8"
    )
    market = audit(tmp_path, date(2026, 9, 17))["by_market"]["mode_a"]
    assert market["capacity_gap_with_prior_paper_fill_shares"] == 2_000
    assert market["missing_0901_price_rows"] == 1
    assert market.get("missing_0901_price_with_prior_paper_fill_rows", 0) == 0


def test_full_replay_rejects_current_session_and_daily_only_path(monkeypatch, tmp_path):
    today = datetime.now(ZoneInfo("Asia/Taipei")).date()
    monkeypatch.setattr("sys.argv", ["replay", "--state-dir", str(tmp_path / "candidate"),
                                    "--start-date", today.isoformat(),
                                    "--end-date", today.isoformat(),
                                    "--historical-full-fill-0901", "--replay-intraday-kbars"])
    with pytest.raises(ValueError, match="completed past sessions only"):
        replay.main()
    yesterday = today - timedelta(days=1)
    monkeypatch.setattr("sys.argv", ["replay", "--state-dir", str(tmp_path / "candidate"),
                                    "--start-date", yesterday.isoformat(),
                                    "--end-date", yesterday.isoformat(),
                                    "--historical-full-fill-0901"])
    with pytest.raises(ValueError, match="requires --replay-intraday-kbars"):
        replay.main()
