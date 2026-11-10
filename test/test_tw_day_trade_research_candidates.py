import hashlib
import gzip
import json
import math
from datetime import datetime
from http.client import HTTPConnection
from pathlib import Path
import threading
from zoneinfo import ZoneInfo

import polars as pl
import pytest
import yaml

from stockagent.live.tw_day_trade_research_candidates import (
    build_research_candidates, build_research_minute_replay,
)


@pytest.fixture
def research(tmp_path):
    fold = tmp_path / "research/fold_10"
    fold.mkdir(parents=True)
    (fold / "checkpoint_best.pt").write_bytes(b"selected-checkpoint")
    pl.DataFrame({"date": ["2026-01-02", "2026-01-05"],
                  "portfolio_return": [math.log(1.1), math.log(0.8)]}).write_parquet(
                      fold / "daily_portfolio_returns.parquet")
    (fold / "metrics.json").write_text(json.dumps({"test_metrics": {"cumulative_return": -0.12}}))
    (fold / "fold_complete.json").write_text(json.dumps({"status": "complete", "fold_id": 10,
        "test_scope": "full_horizon", "test_rows": 2, "test_date_start": "2026-01-02",
        "test_date_end": "2026-01-05"}))
    (fold / "mode_artifact_contract.json").write_text(json.dumps({"execution_mode": "tw_day_trade",
        "frequency": "daily_policy_exact_minute_execution", "mode_details": {"daily_policy_decisions_per_session": 1}}))
    candidate_dir = tmp_path / "configs/deployments/candidates"
    candidate_dir.mkdir(parents=True)
    (candidate_dir / "candidate.yaml").write_text(yaml.safe_dump({"market": "2m", "label": "研究候選",
        "enabled": False, "day_trade_simulation_enabled": True, "initial_capital": 2_000_000,
        "fold_id": 10, "checkpoint_path": str(fold / "checkpoint_best.pt")}))
    pins = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in fold.iterdir()}
    registry = {"schema_version": 1, "candidates": [{"market_config": "configs/deployments/candidates/candidate.yaml",
        "expected_market": "2m", "expected_initial_capital_twd": 2_000_000, "artifact_sha256": pins}]}
    (candidate_dir.parent / "public_research_candidates.yaml").write_text(yaml.safe_dump(registry))
    (tmp_path / "services/discord_bot/markets").mkdir(parents=True)
    return tmp_path, fold


def observed(year=2026, month=11, day=9):
    return datetime(year, month, day, 10, tzinfo=ZoneInfo("Asia/Taipei"))


def test_source_pinned_log_returns_are_research_not_paper_execution(research):
    root, _ = research
    result = build_research_candidates(repo_root=root, observed_at=observed())
    row = result["candidates"][0]
    assert row["cumulative_return_pct"] == pytest.approx(-12)
    assert row["daily"][0]["daily_return_pct"] == pytest.approx(10)
    assert row["daily"][1]["cumulative_return_pct"] == pytest.approx(-12)
    assert (row["start_date"], row["end_date"], row["session_count"]) == ("2026-01-02", "2026-01-05", 2)
    assert row["status"] == "research_only_not_enabled"
    assert row["paper_execution_enabled"] is row["live_signal_available"] is row["broker_fill"] is False
    assert str(root) not in json.dumps(result)


def test_changed_artifact_fails_closed_without_private_path(research):
    root, fold = research
    (fold / "checkpoint_best.pt").write_bytes(b"different-checkpoint")
    result = build_research_candidates(repo_root=root, observed_at=observed())
    assert result["candidates"] == [{"status": "research_source_unavailable", "research_status": "unavailable"}]
    assert str(root) not in json.dumps(result)


def test_prior_year_is_not_shown_and_future_days_are_not_included(research):
    root, _ = research
    next_year = build_research_candidates(repo_root=root, observed_at=observed(2027, 1, 4))["candidates"][0]
    assert next_year["research_status"] == "no_current_year_data" and next_year["daily"] == []
    in_progress = build_research_candidates(repo_root=root, observed_at=observed(2026, 1, 3))["candidates"][0]
    assert in_progress["session_count"] == 1
    assert in_progress["end_date"] == "2026-01-02"
    assert in_progress["cumulative_return_pct"] == pytest.approx(10)


def test_configured_account_without_committed_state_is_not_called_live(research, monkeypatch):
    root, _ = research
    (root / "services/discord_bot/markets/2m.yaml").write_text(yaml.safe_dump(
        {"market": "2m", "enabled": True, "day_trade_simulation_enabled": True}))
    candidate = build_research_candidates(repo_root=root, observed_at=observed())["candidates"][0]
    assert candidate["status"] == "research_only_pending_paper"
    assert candidate["paper_execution_enabled"] is None
    from stockagent.live import tw_day_trade_research_candidates as module
    monkeypatch.setattr(module, "_confirmed_active_market", lambda *_: True)
    assert build_research_candidates(repo_root=root, observed_at=observed())["candidates"] == []


def test_provisional_summary_is_separate_pinned_and_never_exposes_private_fields(research):
    root, _ = research
    destination = root / "artifacts/operations/public_research"
    destination.mkdir(parents=True)
    summary = destination / "candidate.json"
    payload = {"contract": "public_audited_provisional_daily_replay_v1", "market": "2m", "year": 2026,
        "generated_at": "2026-01-02T15:00:00+08:00",
        "simulation_only": True, "broker_fill": False, "exchange_fill": False, "paper_execution_enabled": False,
        "provisional": True, "source_completeness_verified": False,
        "all_rejected_entry_prices_independently_verified": False,
        "historical_decision_policy": "single_counterfactual_open_not_published_revision_replay_v1",
        "historical_bracket_cross_assumption": "historical_bracket_full_on_strict_observed_cross_paper_assumption_v1",
        "start_date": "2026-01-02", "end_date": "2026-01-02", "session_count": 1, "minute_count": 270,
        "carried_mark_minutes": 10, "full_cross_assumptions": 1, "nontrade_assumptions": 1,
        "end_equity_twd": 2_100_000, "cumulative_return_pct": 5,
        "daily": [{"date": "2026-01-02", "end_equity_twd": 2_100_000, "daily_return_pct": 5,
                   "cumulative_return_pct": 5, "carried_mark_minutes": 10, "private_path": "/secret"}]}
    summary.write_text(json.dumps(payload))
    path = root / "configs/deployments/public_research_candidates.yaml"
    registry = yaml.safe_load(path.read_text())
    registry["candidates"][0]["provisional_replay"] = {
        "path": str(summary.relative_to(root)), "sha256": hashlib.sha256(summary.read_bytes()).hexdigest()}
    path.write_text(yaml.safe_dump(registry))
    item = build_research_candidates(repo_root=root, observed_at=observed())["candidates"][0]
    replay = item["provisional_replay"]
    assert replay["status"] == "available" and replay["session_count"] == 1
    assert replay["nontrade_assumptions"] == 1 and replay["carried_mark_minutes"] == 10
    assert "/secret" not in json.dumps(item) and str(root) not in json.dumps(item)
    summary.write_text(json.dumps(payload | {"broker_fill": True}))
    item = build_research_candidates(repo_root=root, observed_at=observed())["candidates"][0]
    assert item["provisional_replay"] == {"status": "unavailable"}
    assert item["research_status"] == "available"  # distinct source stays available


def test_counterfactual_signal_is_pinned_labelled_and_not_a_live_account(research):
    root, fold = research
    destination = root / "artifacts/operations/public_research"
    destination.mkdir(parents=True)
    path = destination / "signal.json"
    payload = {"contract": "public_source_checked_counterfactual_open_signal_v1", "market": "2m",
        "session_date": "2026-11-09", "generated_at": "2026-11-09T14:35:00+08:00",
        "published_at": "2026-11-09T14:40:00+08:00", "checkpoint_sha256": hashlib.sha256(
            (fold / "checkpoint_best.pt").read_bytes()).hexdigest(),
        "simulation_only": True, "counterfactual": True, "live_signal": False,
        "paper_execution_enabled": False, "broker_fill": False, "exchange_fill": False,
        "execution_and_minute_coverage_verified": False, "signal_count": 2,
        "signals": [{"symbol": "6488", "name": "乙", "target_weight_pct": -20,
                     "tradable": True, "can_buy": True, "can_sell": True, "private": "/secret"},
                    {"symbol": "2330", "name": "甲", "target_weight_pct": 10,
                     "tradable": True, "can_buy": True, "can_sell": True}]}
    path.write_text(json.dumps(payload))
    registry_path = root / "configs/deployments/public_research_candidates.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["candidates"][0]["counterfactual_signal"] = {
        "path": str(path.relative_to(root)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    registry_path.write_text(yaml.safe_dump(registry))
    assert build_research_candidates(repo_root=root, observed_at=observed())["candidates"][0]["counterfactual_signal"] == {"status": "unavailable"}
    after = datetime(2026, 11, 9, 15, tzinfo=ZoneInfo("Asia/Taipei"))
    signal = build_research_candidates(repo_root=root, observed_at=after)["candidates"][0]["counterfactual_signal"]
    assert signal["status"] == "available" and signal["signal_count"] == 2
    assert signal["counterfactual"] is True and signal["live_signal"] is False and signal["broker_fill"] is False
    assert "/secret" not in json.dumps(signal) and str(root) not in json.dumps(signal)
    path.write_text(json.dumps(payload | {"broker_fill": True}))
    assert build_research_candidates(repo_root=root, observed_at=after)["candidates"][0]["counterfactual_signal"] == {"status": "unavailable"}


def test_public_http_separate_from_signal_and_paper_accounts(research, monkeypatch):
    from scripts import serve_public_dashboards as gateway
    root, _ = research
    monkeypatch.setattr(gateway, "committed_paper_read_guard", lambda *_: pytest.fail("research must not read ledger"))
    code = Path(__file__).resolve().parents[1]
    server = gateway.PublicDashboardServer(("127.0.0.1", 0), repo_root=root, audience="public",
        public_static_root=code / "services/public_dashboards", taifex_static_root=code / "services/taifex_dashboard",
        tw_static_root=code / "services/tw_day_trade_dashboard", shioaji_static_root=code / "services/shioaji_api_dashboard",
        openbb_static_root=code / "services/openbb_archive_dashboard", data_monitor_static_root=code / "services/data_monitor_dashboard",
        traffic_static_root=code / "services/traffic_dashboard", taifex_upstream="http://127.0.0.1:1", tw_upstream="http://127.0.0.1:1")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    conn = HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
    try:
        conn.request("GET", "/tw-day-trade/api/research-candidates")
        response = conn.getresponse()
        assert response.status == 200
        assert json.loads(response.read())["candidates"][0]["status"] == "research_only_not_enabled"
        conn.request("GET", "/tw-day-trade/research-candidates.js?v=1")
        response = conn.getresponse()
        assert response.status == 200 and b"api/research-candidates" in response.read()
        conn.request("GET", "/tw-day-trade/api/research-candidates?year=2025")
        response = conn.getresponse()
        assert response.status == 400
        response.read()
        conn.request("GET", "/tw-day-trade/api/research-minute?market=2m&date=2026-01-02")
        response = conn.getresponse()
        assert response.status == 200 and json.loads(response.read())["status"] == "unavailable"
        for query in ("date=2026-01-02", "market=2m&date=20260102", "market=2m&date=2026-01-02&x=1",
                      "market=2m&market=other&date=2026-01-02", "market=../private&date=2026-01-02"):
            conn.request("GET", f"/tw-day-trade/api/research-minute?{query}")
            response = conn.getresponse()
            assert response.status == 400
            response.read()
    finally:
        conn.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_research_minute_archive_is_source_pinned_scoped_and_never_live(research):
    root, _ = research
    destination = root / "artifacts/operations/public_research"
    destination.mkdir(parents=True)
    summary = destination / "candidate.json"
    payload = {"contract": "public_audited_provisional_daily_replay_v1", "market": "2m", "year": 2026,
        "generated_at": "2026-01-02T15:00:00+08:00",
        "simulation_only": True, "broker_fill": False, "exchange_fill": False, "paper_execution_enabled": False,
        "provisional": True, "source_completeness_verified": False,
        "all_rejected_entry_prices_independently_verified": False,
        "historical_decision_policy": "single_counterfactual_open_not_published_revision_replay_v1",
        "historical_bracket_cross_assumption": "historical_bracket_full_on_strict_observed_cross_paper_assumption_v1",
        "start_date": "2026-01-02", "end_date": "2026-01-02", "session_count": 1, "minute_count": 270,
        "carried_mark_minutes": 10, "full_cross_assumptions": 1, "nontrade_assumptions": 0,
        "end_equity_twd": 2_100_000, "cumulative_return_pct": 5,
        "daily": [{"date": "2026-01-02", "end_equity_twd": 2_100_000, "daily_return_pct": 5,
                   "cumulative_return_pct": 5, "carried_mark_minutes": 10}]}
    summary.write_text(json.dumps(payload))
    digest = hashlib.sha256(summary.read_bytes()).hexdigest()
    archive = destination / "minute.json.gz"
    minute = {"contract": "public_audited_provisional_minute_equity_v1", "market": "2m", "year": 2026,
        "summary_sha256": digest, "simulation_only": True, "provisional": True, "broker_fill": False,
        "exchange_fill": False, "source_completeness_verified": False, "session_count": 1, "minute_count": 270,
        "sessions": [{"date": "2026-01-02", "equity_twd": [2_000_000] * 269 + [2_100_000],
                      "valuation_stale": [True] * 10 + [False] * 260, "private_path": "/secret"}]}
    archive.write_bytes(gzip.compress(json.dumps(minute).encode(), mtime=0))
    registry_path = root / "configs/deployments/public_research_candidates.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["candidates"][0]["provisional_replay"] = {"path": str(summary.relative_to(root)), "sha256": digest}
    registry["candidates"][0]["provisional_minutes"] = {"path": str(archive.relative_to(root)),
        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
    registry_path.write_text(yaml.safe_dump(registry))
    candidate = build_research_candidates(repo_root=root, observed_at=observed())["candidates"][0]
    assert candidate["provisional_replay"]["minute_series_available"] is True
    result = build_research_minute_replay(repo_root=root, market="2m", session_date="2026-01-02", observed_at=observed())
    assert result["status"] == "available" and len(result["equity_twd"]) == 270
    assert result["first_minute"] == "2026-01-02T09:01:00+08:00" and sum(result["valuation_stale"]) == 10
    assert result["simulation_only"] is True and result["broker_fill"] is False
    assert "/secret" not in json.dumps(result) and str(root) not in json.dumps(result)
    assert build_research_minute_replay(repo_root=root, market="2m", session_date="2026-11-09", observed_at=observed())["status"] == "unavailable"
    assert build_research_minute_replay(repo_root=root, market="other", session_date="2026-01-02", observed_at=observed())["status"] == "unavailable"

    # A source-pinned complete session can be shown on the same date after
    # settlement; neither a future publication clock nor a date alone is proof.
    payload.update(start_date="2026-11-09", end_date="2026-11-09",
                   generated_at="2026-11-09T14:40:00+08:00")
    payload["daily"][0]["date"] = "2026-11-09"
    summary.write_text(json.dumps(payload))
    digest = hashlib.sha256(summary.read_bytes()).hexdigest()
    minute["summary_sha256"] = digest
    minute["sessions"][0]["date"] = "2026-11-09"
    archive.write_bytes(gzip.compress(json.dumps(minute).encode(), mtime=0))
    registry["candidates"][0]["provisional_replay"]["sha256"] = digest
    registry["candidates"][0]["provisional_minutes"]["sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
    registry_path.write_text(yaml.safe_dump(registry))
    pre_close = datetime(2026, 11, 9, 14, 30, 59, tzinfo=ZoneInfo("Asia/Taipei"))
    post_close = datetime(2026, 11, 9, 14, 40, tzinfo=ZoneInfo("Asia/Taipei"))
    assert build_research_minute_replay(repo_root=root, market="2m", session_date="2026-11-09", observed_at=pre_close)["status"] == "unavailable"
    assert build_research_candidates(repo_root=root, observed_at=pre_close)["candidates"][0]["provisional_replay"] == {"status": "unavailable"}
    result = build_research_minute_replay(repo_root=root, market="2m", session_date="2026-11-09", observed_at=post_close)
    assert result["status"] == "available" and result["first_minute"] == "2026-11-09T09:01:00+08:00"
    assert result["provisional"] is True and result["broker_fill"] is False

    archive.write_bytes(b"tampered")
    assert build_research_minute_replay(repo_root=root, market="2m", session_date="2026-11-09", observed_at=post_close)["status"] == "unavailable"
