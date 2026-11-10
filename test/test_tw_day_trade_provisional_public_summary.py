from datetime import date, datetime, time, timedelta
import hashlib
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts import build_tw_day_trade_provisional_public_summary as public
from scripts import build_tw_day_trade_provisional_minute_archive as minute_public


def _write(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path, monkeypatch, *, day=date(2026, 9, 11), now=None):
    now = now or datetime(2026, 11, 9, 11, tzinfo=ZoneInfo("Asia/Taipei"))
    checkpoint = tmp_path / "checkpoint_best.pt"
    checkpoint.write_bytes(b"candidate checkpoint")
    config = tmp_path / "candidate.yaml"
    config.write_text("candidate")
    cfg = SimpleNamespace(enabled=False, day_trade_simulation_enabled=True, checkpoint_path=str(checkpoint),
                          market="2m", initial_capital=2_000_000.0)
    monkeypatch.setattr(public, "load_market_config", lambda _path: cfg)
    opening, weights, summary = (tmp_path / name for name in ("open.csv", "weights.parquet", "summary.json"))
    opening.write_text("source")
    weights.write_bytes(b"weights")
    _write(summary, {"checkpoint_path": str(checkpoint)})
    artifact = {"market": "2m", "session_date": str(day)}
    for field, path in (("open_input", opening), ("summary", summary), ("weights", weights)):
        artifact[f"{field}_path"] = str(path)
        artifact[f"{field}_sha256"] = _digest(path)
    preflight = tmp_path / "signal_preflight.json"
    _write(preflight, {"status": "accepted_sources_only", "failures": [], "expected_signals": 1,
                       "accepted_signals": 1, "artifacts": [artifact]})
    _write(tmp_path / "state.json", {"modes": {"2m": {"initial_capital_twd": 2_000_000.0,
                                                          "total_equity_twd": 2_002_690.0}}})
    _write(tmp_path / "rebuild_receipt.json", {"simulation_only": True, "production_order_possible": False,
        "publication_authorized": False, "historical_bracket_cross_assumption": public.HISTORICAL_BRACKET_FULL_CROSS_CONTRACT,
        "historical_decision_policy": "single_counterfactual_open_not_published_revision_replay_v1",
        "sessions": [{"session_date": str(day), "close": {"status": "session_flat_no_broker_fill_claim"}}],
        "signal_preflight": {"path": str(preflight), "sha256": _digest(preflight)}})
    start = datetime.combine(day, time(9, 1), tzinfo=ZoneInfo("Asia/Taipei"))
    with (tmp_path / "marks.jsonl").open("w") as stream:
        for index in range(270):
            stamp = start + timedelta(minutes=index)
            stream.write(json.dumps({"market": "2m", "session_date": str(day), "minute": stamp.isoformat(timespec="minutes"),
                "total_equity_twd": 2_000_000.0 + index * 10, "valuation_stale": index == 10,
                "historical_minute_replay": True, "provisional_minute_source_coverage": True,
                "recorded_at_basis": "counterfactual_effective_clock_not_original_publication",
                "replay_generated_at": now.isoformat()}) + "\n")
    for name in ("fills.jsonl", "orders.jsonl", "events.jsonl"):
        (tmp_path / name).write_text("")
    minute_source = tmp_path / "minute.parquet"
    minute_source.write_bytes(b"verified source fixture")
    st = minute_source.stat()
    names = ("state.json", "rebuild_receipt.json", "fills.jsonl", "marks.jsonl", "orders.jsonl", "events.jsonl")
    audit = tmp_path / "audit.json"
    _write(audit, {"contract": "strict_intraday_fill_cash_cost_minute_reconciliation_v1", "passed": True,
        "full_requested_range_passed": True, "sources_verified": True, "error_count": 0,
        "provisional_minute_source_coverage": True, "complete_minute_source_coverage_verified": False,
        "source_hashes": {name: _digest(tmp_path / name) for name in names},
        "minute_source_signatures": {str(minute_source): [st.st_size, st.st_mtime_ns, st.st_ctime_ns]},
        "session_count": 1, "start_date": str(day), "end_date": str(day),
        "provisional_full_cross_bracket_assumptions": 1, "provisional_nontrade_accounting_assumptions": 0})
    return dict(state_dir=tmp_path, audit_path=audit, candidate_config=config,
                checkpoint_sha256=_digest(checkpoint), now=now), minute_source


def test_summary_is_year_scoped_and_explicitly_not_a_live_account(tmp_path, monkeypatch):
    args, _ = _fixture(tmp_path, monkeypatch)
    result = public.build(**args)
    assert result["provisional"] is True and result["paper_execution_enabled"] is False
    assert result["broker_fill"] is False and result["source_completeness_verified"] is False
    assert result["minute_count"] == 270 and result["carried_mark_minutes"] == 1
    assert result["end_equity_twd"] == 2_002_690.0
    assert result["cumulative_return_pct"] == pytest.approx(0.1345)
    assert str(tmp_path) not in json.dumps(result)


def test_summary_rejects_changed_source_or_unverified_audit(tmp_path, monkeypatch):
    args, minute_source = _fixture(tmp_path, monkeypatch)
    minute_source.write_bytes(b"changed")
    with pytest.raises(ValueError, match="source changed"):
        public.build(**args)


def test_summary_rejects_mutated_mark_and_wrong_checkpoint(tmp_path, monkeypatch):
    args, _ = _fixture(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="checkpoint"):
        public.build(**(args | {"checkpoint_sha256": "0" * 64}))
    with (tmp_path / "marks.jsonl").open("a") as stream:
        stream.write("{}\n")
    with pytest.raises(ValueError, match="changed after its independent audit"):
        public.build(**args)


@pytest.mark.parametrize("hour,minute,second,accepted", [
    (14, 30, 59, False), (14, 31, 0, True), (15, 0, 0, True),
])
def test_today_replay_requires_post_auction_deadline_and_full_audit(
        tmp_path, monkeypatch, hour, minute, second, accepted):
    now = datetime(2026, 11, 9, hour, minute, second, tzinfo=ZoneInfo("Asia/Taipei"))
    args, _ = _fixture(tmp_path, monkeypatch, day=now.date(), now=now)
    if accepted:
        result = public.build(**args)
        assert result["end_date"] == "2026-11-09"
        assert result["paper_execution_enabled"] is False
    else:
        with pytest.raises(ValueError, match="completed current-year"):
            public.build(**args)


def test_replay_cannot_publish_a_future_generation(tmp_path, monkeypatch):
    now = datetime(2026, 11, 9, 15, tzinfo=ZoneInfo("Asia/Taipei"))
    args, _ = _fixture(tmp_path, monkeypatch, day=now.date(), now=now + timedelta(minutes=1))
    with pytest.raises(ValueError, match="minute clock or equity"):
        public.build(**(args | {"now": now}))


def test_minute_projection_keeps_all_audited_values_and_flags(tmp_path, monkeypatch):
    args, _ = _fixture(tmp_path, monkeypatch)
    summary = tmp_path / "summary-public.json"
    _write(summary, public.build(**args))
    projected = minute_public.project(**args, summary_path=summary, summary_sha256=_digest(summary))
    assert projected["simulation_only"] is True and projected["broker_fill"] is False
    assert projected["provisional"] is True and projected["source_completeness_verified"] is False
    assert projected["minute_count"] == 270 and len(projected["sessions"]) == 1
    session = projected["sessions"][0]
    assert len(session["equity_twd"]) == len(session["valuation_stale"]) == 270
    assert session["equity_twd"][0] == 2_000_000.0
    assert session["equity_twd"][-1] == 2_002_690.0
    assert sum(session["valuation_stale"]) == 1 and session["valuation_stale"][10]
    assert str(tmp_path) not in json.dumps(projected)


def test_minute_projection_rejects_other_summary_or_account(tmp_path, monkeypatch):
    args, _ = _fixture(tmp_path, monkeypatch)
    summary = tmp_path / "summary-public.json"
    original = public.build(**args)
    _write(summary, original)
    with pytest.raises(ValueError, match="published daily summary changed"):
        minute_public.project(**args, summary_path=summary, summary_sha256="0" * 64)
    _write(summary, original | {"market": "different"})
    with pytest.raises(ValueError, match="differs"):
        minute_public.project(**args, summary_path=summary, summary_sha256=_digest(summary))
