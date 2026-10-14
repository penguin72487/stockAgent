"""A backfilled account may append old dates after other accounts' newer rows."""

from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from stockagent.live import tw_day_trade_dashboard as dashboard


DAY = "2026-09-24"
TAIPEI = ZoneInfo("Asia/Taipei")


def _write_rows(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


@pytest.fixture
def fragmented_accounts(tmp_path, monkeypatch):
    now = datetime(2026, 9, 27, 8, tzinfo=TAIPEI)
    active = ["tw_day_trade_a", "tw_day_trade_b"]
    modes = {
        market: {
            "market": market,
            "label": market,
            "session_date": DAY,
            "checkpoint_ready": True,
            "initial_capital_twd": 10_000_000,
            "total_equity_twd": 10_000_000 + ordinal * 100_000,
            "target_symbol_count": 1,
            "positions": {},
        }
        for ordinal, market in enumerate([*active, "retired"], 1)
    }
    (tmp_path / "state.json").write_text(json.dumps({
        "enabled_markets": active, "modes": modes,
    }))
    (tmp_path / "status.json").write_text(json.dumps({
        "updated_at": now.isoformat(), "health": "active",
    }))
    signals, events, marks = [], [], []
    opening = datetime.fromisoformat(f"{DAY}T09:00:00+08:00")
    for market, mode in modes.items():
        # Each imported account has a complete date block; the same date can
        # therefore occur in several disjoint spans in every shared ledger.
        separator = {"market": market, "session_date": "2026-09-23"}
        signals.append(separator)
        events.append(separator)
        marks.append(separator)
        signals.append({
            "market": market, "session_date": DAY, "symbol": "2330",
            "signal_id": market, "target_weight": 0.1, "status": "ready",
        })
        events.append({
            "market": market, "session_date": DAY,
            "recorded_at": opening.isoformat(), "event": "signal_registered",
            "signal_id": market, "counts": {"ready": 1},
            "entry_fill_count": 1, "entry_requested_shares": 1000,
            "entry_filled_shares": 1000, "entry_unfilled_shares": 0,
            "entry_fill_outcome": "filled",
        })
        for minute in range(1, 271):
            marks.append({
                "market": market, "session_date": DAY,
                "minute": (opening + timedelta(minutes=minute)).isoformat(),
                "initial_capital_twd": mode["initial_capital_twd"],
                "total_equity_twd": mode["total_equity_twd"],
                "open_position_count": 0, "stale_position_count": 0,
            })
    for filename, rows in [("signals", signals), ("events", events), ("marks", marks)]:
        _write_rows(tmp_path / f"{filename}.jsonl", rows)
    monkeypatch.setattr(dashboard, "dashboard_session_clock", lambda *_a, **_k: {
        "display_session_date": DAY,
    })
    return tmp_path, now, active


def test_latest_signal_page_reads_all_imported_accounts(fragmented_accounts):
    root, _, active = fragmented_accounts
    page = dashboard.build_dashboard_signal_page(
        state_dir=root, start_date=DAY, end_date=DAY, limit=100,
    )
    assert page["total"] == len(active)
    assert {row["market"] for row in page["rows"]} == set(active)


@pytest.mark.parametrize("latest,private_features", [(True, True), (False, True), (True, False)])
def test_committed_scoped_repair_view_keeps_complete_new_signal_and_original_audit(
    fragmented_accounts, latest, private_features, monkeypatch,
):
    root, _, active = fragmented_accounts
    market = active[0]
    original_id = market
    repaired_id = original_id + ":minute-repair:approved"
    state = json.loads((root / "state.json").read_text())
    mode = state["modes"][market]
    mode.update(signal_id=repaired_id, processed_signal_ids=[original_id, repaired_id], simulation_replay=True,
                opening_repair={"contract": "never_filled_original_signal_append_only_minute_repair_v2",
                                "original_signal_id": original_id, "simulation_only": True,
                                "original_weights_preserved": True})
    (root / "state.json").write_text(json.dumps(state))
    summary = root / "summary.json"
    summary.write_text(json.dumps({"source_publication_contract": {
        "feature_proxy_publication_allowed": not private_features, "research_only": True,
        "historical_point_in_time_proven": False}, "model_explanation": {"top_feature_drivers": []}}))
    rows = [json.loads(line) for line in (root / "signals.jsonl").read_text().splitlines()]
    old = next(row for row in rows if row.get("signal_id") == original_id)
    old.update(ask=10, bid=9, execution_price=10, filled_shares=0, filled_weight=0,
               requested_shares=1000, reason="quote_not_after_signal", status="blocked", sizing_open_price=10)
    rows.append({**old, "signal_id": repaired_id, "status": "ready", "filled_shares": 1000,
                 "reason": "counterfactual_observed_09_01_minute_price_fill", "signal_source_path": str(summary)})
    _write_rows(root / "signals.jsonl", rows)
    original_bytes = (root / "signals.jsonl").read_bytes()
    monkeypatch.setattr(dashboard, "_COLUMNAR_LEDGER_MIN_BYTES", 1)
    page = dashboard.build_dashboard_signal_page(state_dir=root, start_date=DAY if latest else "2026-09-23",
                                                 end_date=DAY, mode=market, limit=100)
    current = [row for row in page["rows"] if row["session_date"] == DAY]
    assert [row["signal_id"] for row in current] == [repaired_id]
    assert current[0]["filled_shares"] == 1000
    if private_features:
        assert page["feature_drivers_by_signal"][repaired_id]["feature_proxy_publication_allowed"] is False
    assert (root / "signals.jsonl").read_bytes() == original_bytes
    other = dashboard.build_dashboard_signal_page(state_dir=root, session_date=DAY, mode=active[1], limit=100)
    assert other["total"] == 1 and other["rows"][0]["signal_id"] == active[1]


@pytest.mark.parametrize("invalid", ["not_processed", "unrelated_id", "unpreserved", "nonpaper", "wrong_contract"])
def test_uncommitted_or_unrelated_repair_cannot_hide_original_signal(invalid):
    mode = {"session_date": DAY, "signal_id": "original:minute-repair:accepted",
            "processed_signal_ids": ["original:minute-repair:accepted"], "simulation_replay": True,
            "opening_repair": {"contract": "never_filled_original_signal_append_only_minute_repair_v2",
                               "original_signal_id": "original", "simulation_only": True,
                               "original_weights_preserved": True}}
    if invalid == "not_processed":
        mode["processed_signal_ids"] = []
    elif invalid == "unrelated_id":
        mode["signal_id"] = "unrelated"
    elif invalid == "unpreserved":
        mode["opening_repair"]["original_weights_preserved"] = False
    elif invalid == "nonpaper":
        mode["opening_repair"]["simulation_only"] = False
    else:
        mode["opening_repair"]["contract"] = "candidate_not_applied"
    assert not dashboard._committed_scoped_repair_superseded_signals({"modes": {"m": mode}})


def test_signal_page_cache_invalidates_committed_repair_view(fragmented_accounts):
    root, _, active = fragmented_accounts
    market = active[0]
    state = json.loads((root / "state.json").read_text())
    mode = state["modes"][market]
    repaired_id = market + ":minute-repair:accepted"
    mode.update(signal_id=repaired_id, simulation_replay=True, processed_signal_ids=[market, repaired_id])
    rows = [json.loads(line) for line in (root / "signals.jsonl").read_text().splitlines()]
    old = next(row for row in rows if row.get("signal_id") == market)
    rows.append({**old, "signal_id": repaired_id})
    _write_rows(root / "signals.jsonl", rows)
    (root / "state.json").write_text(json.dumps(state))
    args = dict(state_dir=root, session_date=DAY, mode=market)
    assert dashboard.build_dashboard_signal_page(**args)["total"] == 2
    mode["opening_repair"] = {"contract": "never_filled_original_signal_append_only_minute_repair_v2",
                              "original_signal_id": market, "simulation_only": True,
                              "original_weights_preserved": True}
    (root / "state.json").write_text(json.dumps(state))
    accepted = dashboard.build_dashboard_signal_page(**args)
    assert accepted["total"] == 1 and accepted["rows"][0]["signal_id"] == repaired_id


@pytest.mark.parametrize("compact", [True, False])
def test_status_aggregates_complete_session_before_limiting_response(
    fragmented_accounts, compact,
):
    root, now, active = fragmented_accounts
    snapshot = dashboard.build_dashboard_snapshot(
        state_dir=root, now=now, session_date=DAY,
        maximum_event_rows=1, maximum_mark_rows=1,
        include_ledger_session_dates=not compact,
    )
    assert snapshot["execution_records"]["executed_count"] == len(active)
    assert snapshot["execution_records"]["all_modes_filled"] is True
    for mode in snapshot["modes"]:
        assert mode["engine_status"] == "historical_session_complete"
        assert mode["total_equity_twd"] > mode["initial_capital_twd"]
        assert mode["last_mark_at"][:16] == f"{DAY}T13:30"
    progress = snapshot["session_progress"]
    assert progress["observed_mode_minutes"] == 270 * len(active)
    assert progress["expected_mode_minutes"] == 270 * len(active)
    assert progress["mark_progress_ratio"] == 1.0
    assert len(snapshot["marks"]) == len(snapshot["events"]) == 1
    assert snapshot["payload_window"]["marks"] == 1
    assert snapshot["payload_window"]["events"] == 1


def test_latest_session_reader_extends_index_after_backfill(tmp_path):
    path = tmp_path / "signals.jsonl"
    _write_rows(path, [{"session_date": DAY, "symbol": "2330"}])
    assert [r["symbol"] for r in dashboard._latest_contiguous_session_rows(
        path, DAY, 100,
    )] == ["2330"]
    with path.open("a") as handle:
        handle.write(json.dumps({"session_date": "2026-09-23"}) + "\n")
        handle.write(json.dumps({"session_date": DAY, "symbol": "2317"}) + "\n")
    assert [r["symbol"] for r in dashboard._latest_contiguous_session_rows(
        path, DAY, 100,
    )] == ["2330", "2317"]


@pytest.mark.parametrize("state_day,signal_id,use_committed", [
    (DAY, "tw_day_trade_a", True),
    ("2026-09-23", "tw_day_trade_a", False),
    (DAY, "other-signal", False),
])
def test_historical_view_keeps_only_matching_committed_completion(
    fragmented_accounts, state_day, signal_id, use_committed,
):
    root, now, _ = fragmented_accounts
    state = json.loads((root / "state.json").read_text())
    state["modes"]["tw_day_trade_a"].update({
        "session_date": state_day, "signal_id": signal_id,
        "entry_completed_at": f"{DAY}T09:02:00+08:00",
        "entry_fill_count": 2, "entry_fill_outcome": "partial",
        "entry_requested_shares": 3000, "entry_filled_shares": 2000,
        "entry_unfilled_shares": 1000,
        "capital_sizing_basis": "session_start_account_nav",
    })
    (root / "state.json").write_text(json.dumps(state))
    snapshot = dashboard.build_dashboard_snapshot(
        state_dir=root, now=now, session_date=DAY, include_ledger_session_dates=False,
    )
    mode = next(row for row in snapshot["modes"] if row["market"] == "tw_day_trade_a")
    assert mode["entry_filled_shares"] == (2000 if use_committed else 1000)
    assert mode["today_execution_outcome"] == ("partial" if use_committed else "filled")
    if use_committed:
        assert mode["account_performance"]["capital_sizing_basis"] == "session_start_account_nav"


def test_historical_costs_come_from_the_selected_mark(fragmented_accounts):
    root, now, active = fragmented_accounts
    path = root / "marks.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    for row in rows:
        if row.get("session_date") == DAY and row["market"] == active[0]:
            row.update(cumulative_carry_cost_twd=300, cumulative_corporate_action_net_twd=40)
    _write_rows(path, rows)
    snapshot = dashboard.build_dashboard_snapshot(state_dir=root, now=now, session_date=DAY)
    account = snapshot["modes"][0]["account_performance"]
    assert account["cumulative_carry_cost_twd"] == 300
    assert account["cumulative_corporate_action_net_twd"] == 40


@pytest.mark.parametrize("strict", [False, True])
def test_research_carry_presentation_never_weakens_strict_intraday(strict):
    mode = {
        "market": "tw_day_trade_a", "session_date": DAY, "open_position_count": 3,
        "account_performance": {"margin_carry_contract": dashboard.MARGIN_CARRY_CONTRACT},
        "intraday_contract": "flatten_same_day_adverse_limit_exception_only_v1" if strict else None,
    }
    issues = dashboard._operational_issues(
        modes=[mode], preopen={}, observed=datetime(2026, 9, 24, 14, tzinfo=TAIPEI),
    )
    assert issues[0]["code"] == ("intraday_residual_open" if strict else "research_margin_carry")
    assert issues[0]["severity"] == ("error" if strict else "warning")
