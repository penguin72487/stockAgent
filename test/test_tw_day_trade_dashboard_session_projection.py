"""A backfilled account may append old dates after other accounts' newer rows."""

from datetime import datetime, timedelta
import json
import os
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


@pytest.mark.parametrize("narrow", [False, True])
@pytest.mark.parametrize("has_book", [False, True])
def test_fragmented_signal_history_uses_complete_session_projections(
    fragmented_accounts, monkeypatch, narrow, has_book,
):
    root, _, active = fragmented_accounts
    cache = root / "cache"
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))
    rows = [json.loads(line) for line in (root / "signals.jsonl").read_text().splitlines()]
    # Same-date/same-mode revisions must keep their original append order.
    for row in rows:
        row.update({"ask": 101, "bid": 100, "sizing_open_price": 100,
                    "filled_weight": 0.1, "reason": "paper_fill"})
        if row.get("signal_id"):
            row.update(ask=101, bid=100, sizing_open_price=100,
                       filled_shares=1000, requested_shares=1000, execution_price=101)
    rows.append({**next(row for row in rows if row.get("signal_id") == active[0]),
                 "signal_id": "later-revision", "target_weight": -0.2})
    if not has_book:
        for row in rows:
            row.pop("ask", None)
            row.pop("bid", None)
    _write_rows(root / "signals.jsonl", rows)
    before = (root / "signals.jsonl").read_bytes()
    request = dict(state_dir=root, start_date="2026-09-23", end_date=DAY, limit=100)
    expected = dashboard.build_dashboard_signal_page(**request, maximum_scan_rows=1000)
    dashboard._SIGNAL_PAGE_CACHE.clear()
    dashboard._SIGNAL_RANGE_SUMMARY_CACHE.clear()
    actual = dashboard.build_dashboard_signal_page(
        **request, maximum_scan_rows=1, _allow_narrow_projection=False,
    )
    assert actual["scan_limit_reached"] is False
    for key in ("total", "rows", "direction_summary", "opening_execution_audit"):
        assert actual[key] == expected[key]
    assert actual["source_rows_scanned"] == len(rows)
    # Once the immutable per-day shards exist, both readers must reuse them,
    # even if an imported account split every day into multiple file spans.
    dashboard._SIGNAL_PAGE_CACHE.clear()
    dashboard._SIGNAL_RANGE_SUMMARY_CACHE.clear()
    monkeypatch.setattr(dashboard, "_columnar_ledger_frame",
                        lambda *_a, **_k: pytest.fail("must reuse complete session shards"))
    monkeypatch.setattr(dashboard, "_rows_for_sessions",
                        lambda *_a, **_k: pytest.fail("must not use capped raw history"))
    reused = dashboard.build_dashboard_signal_page(
        **request, maximum_scan_rows=1, _allow_narrow_projection=narrow,
    )
    assert reused == actual
    assert (root / "signals.jsonl").read_bytes() == before


@pytest.mark.parametrize("change", ["enabled_markets", "committed_repair"])
def test_range_summary_cache_tracks_the_actual_signal_filter_scope(fragmented_accounts, monkeypatch, change):
    root, _, active = fragmented_accounts
    cache = root / "cache"
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))
    rows = [json.loads(line) for line in (root / "signals.jsonl").read_text().splitlines()]
    for row in rows:
        row.update(filled_shares=0, requested_shares=1000, execution_price=100,
                   sizing_open_price=100, filled_weight=0, reason="test")
    _write_rows(root / "signals.jsonl", rows)
    request = dict(state_dir=root, start_date="2026-09-23", end_date=DAY, limit=100)
    initial = dashboard.build_dashboard_signal_page(**request)
    assert dashboard._SIGNAL_RANGE_SUMMARY_CACHE
    state_path = root / "state.json"
    state = json.loads(state_path.read_text())
    if change == "enabled_markets":
        state["enabled_markets"] = active[:1]
    else:
        original = active[0]
        repaired = original + ":minute-repair:approved"
        state["modes"][original].update(
            signal_id=repaired, processed_signal_ids=[original, repaired], simulation_replay=True,
            opening_repair={"contract": "never_filled_original_signal_append_only_minute_repair_v2",
                            "original_signal_id": original, "simulation_only": True,
                            "original_weights_preserved": True})
    state_path.write_text(json.dumps(state))
    # Neither case changed the signal source, range, capitals or filters passed
    # as arguments. The effective state-owned filter is nevertheless different.
    actual = dashboard.build_dashboard_signal_page(**request)
    dashboard._SIGNAL_PAGE_CACHE.clear()
    dashboard._SIGNAL_RANGE_SUMMARY_CACHE.clear()
    independent = dashboard.build_dashboard_signal_page(**request)
    assert independent["total"] < initial["total"]
    assert actual == independent


@pytest.mark.parametrize("latest", [False, True])
def test_signal_page_rejects_ctime_only_history_rewrites(fragmented_accounts, monkeypatch, latest):
    root, _, _ = fragmented_accounts
    cache = root / "cache"
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))
    source = root / "signals.jsonl"
    request = dict(state_dir=root, start_date=DAY if latest else "2026-09-23", end_date=DAY, limit=100)
    initial = dashboard.build_dashboard_signal_page(**request)
    assert all(row["target_weight"] == 0.1 for row in initial["rows"] if row.get("signal_id"))
    before = source.stat()
    source.write_bytes(source.read_bytes().replace(b'"target_weight": 0.1', b'"target_weight": 0.2'))
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert source.stat().st_ctime_ns != before.st_ctime_ns
    actual = dashboard.build_dashboard_signal_page(**request)
    assert all(row["target_weight"] == 0.2 for row in actual["rows"] if row.get("signal_id"))
    assert actual["direction_summary"]["target"]["long_gross"] == pytest.approx(0.4)


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("filename", ["orders.jsonl", "fills.jsonl"])
def test_order_fill_page_rejects_ctime_only_history_rewrites(fragmented_accounts, monkeypatch, persistent, filename):
    root, _, active = fragmented_accounts
    if persistent:
        cache = root / "cache"
        cache.mkdir()
        monkeypatch.setenv(dashboard._LEDGER_SESSION_INDEX_CACHE_ENV, str(cache))
    else:
        monkeypatch.delenv(dashboard._LEDGER_SESSION_INDEX_CACHE_ENV, raising=False)
    source = root / filename
    _write_rows(source, [{"session_date": DAY, "market": active[0], "symbol": "2330",
                          "recorded_at": f"{DAY}T09:00:00+08:00", "price": 100.0,
                          "quantity": 1000, "status": "filled"}])
    request = dict(state_dir=root, session_date=DAY, limit=100)
    assert dashboard.build_dashboard_event_page(**request)["rows"][0]["price"] == 100.0
    before = source.stat()
    source.write_bytes(source.read_bytes().replace(b'"price": 100.0', b'"price": 200.0'))
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
    page = dashboard.build_dashboard_event_page(**request)
    assert page["total"] == 1 and page["rows"][0]["price"] == 200.0


@pytest.mark.parametrize("filename", ["orders.jsonl", "fills.jsonl"])
def test_persistent_detail_reuses_verified_append_but_not_rewrite_plus_growth(tmp_path, monkeypatch, filename):
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv(dashboard._LEDGER_SESSION_INDEX_CACHE_ENV, str(cache))
    path = tmp_path / filename
    first_row = {"session_date": DAY, "market": "one", "symbol": "2330", "price": 100.0}
    _write_rows(path, [first_row])
    schema = dashboard._EVENT_PAGE_COLUMN_TYPES
    first_index = dashboard._ledger_session_index(path, recorded_at_fallback=False)
    first = dashboard._projected_ledger_session_frame(path, index=first_index, session_date=DAY, projected_schema=schema)
    assert first["price"].to_list() == [100.0]
    shard = dashboard._detail_session_projection_root(path, schema) / f"{DAY}.parquet"
    original = dashboard.metadata_signature(shard.stat())
    with path.open("a") as stream:
        stream.write(json.dumps({**first_row, "session_date": "2026-09-25"}) + "\n")
    appended = dashboard._ledger_session_index(path, recorded_at_fallback=False)
    assert appended.lineage == first_index.lineage
    assert dashboard._projected_ledger_session_frame(path, index=appended, session_date=DAY, projected_schema=schema)["price"].to_list() == [100.0]
    assert dashboard.metadata_signature(shard.stat()) == original
    dashboard._LEDGER_SESSION_INDEX_CACHE.pop((path.resolve(), False), None)
    cold_index = dashboard._ledger_session_index(path, recorded_at_fallback=False)
    assert cold_index.lineage == appended.lineage
    assert dashboard._projected_ledger_session_frame(path, index=cold_index, session_date=DAY, projected_schema=schema)["price"].to_list() == [100.0]
    assert dashboard.metadata_signature(shard.stat()) == original
    path.write_bytes(path.read_bytes().replace(b'"price": 100.0', b'"price": 200.0') +
                     (json.dumps({**first_row, "session_date": "2026-09-28"}) + "\n").encode())
    changed = dashboard._ledger_session_index(path, recorded_at_fallback=False)
    assert changed.lineage != appended.lineage
    assert dashboard._projected_ledger_session_frame(path, index=changed, session_date=DAY, projected_schema=schema)["price"].to_list() == [200.0]


def test_detail_projection_rejects_rewrite_during_build(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv(dashboard._LEDGER_SESSION_INDEX_CACHE_ENV, str(cache))
    path = tmp_path / "orders.jsonl"
    _write_rows(path, [{"session_date": DAY, "price": 100.0}])
    index = dashboard._ledger_session_index(path, recorded_at_fallback=False)
    original = dashboard._columnar_ledger_frame
    def race(*args, **kwargs):
        result = original(*args, **kwargs)
        path.write_bytes(path.read_bytes().replace(b'"price": 100.0', b'"price": 200.0'))
        return result
    monkeypatch.setattr(dashboard, "_columnar_ledger_frame", race)
    assert dashboard._projected_ledger_session_frame(path, index=index, session_date=DAY,
                                                    projected_schema=dashboard._EVENT_PAGE_COLUMN_TYPES) is None


@pytest.mark.parametrize("reader", [dashboard._tail_for_session, dashboard._latest_contiguous_session_rows])
def test_bounded_session_caches_reject_ctime_only_rewrites(tmp_path, reader):
    source = tmp_path / "signals.jsonl"
    _write_rows(source, [{"session_date": DAY, "i": 1}])

    def read():
        if reader is dashboard._tail_for_session:
            return reader(source, 10, DAY)
        return reader(source, DAY, 10)

    assert read()[0]["i"] == 1
    before = source.stat()
    source.write_bytes(source.read_bytes().replace(b'"i": 1', b'"i": 2'))
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert read()[0]["i"] == 2


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


@pytest.mark.parametrize("open_positions", [0, 12])
def test_latest_no_fill_warning_does_not_erase_prior_positions(open_positions):
    mode = {"market": "tw_day_trade_unit_mgda", "label": "MGDA",
            "session_date": DAY, "open_position_count": open_positions,
            "today_execution_outcome": "no_fill", "entry_unfilled_shares": 88000}
    issues = dashboard._operational_issues(
        modes=[mode], preopen={}, observed=datetime(2026, 9, 24, 11, tzinfo=TAIPEI),
    )
    issue = next(row for row in issues if row["code"] == "entry_no_fill")
    assert issue["severity"] == "error"
    assert "本次訊號尚無新增成交" in issue["detail"]
    assert "88,000 股" in issue["detail"]
    assert ("12 筆先前建立的未平模擬持倉" in issue["detail"]) is bool(open_positions)
    assert "沒有建立任何成交部位" not in issue["detail"]


@pytest.mark.parametrize("source_time,warning", [("09:00:12", False), ("09:01:00", False), ("09:10:03", True)])
def test_counterfactual_later_signal_is_not_silently_presented_as_causal(source_time, warning):
    mode = {"market": "tw_day_trade_100m", "label": "100m", "session_date": DAY,
            "counterfactual_open_replay": True, "signal_at": f"{DAY}T09:01:00+08:00",
            "source_signal_at": f"{DAY}T{source_time}+08:00"}
    issues = dashboard._operational_issues(
        modes=[mode], preopen={}, observed=datetime(2026, 9, 24, 11, tzinfo=TAIPEI),
    )
    assert any(row["code"] == "replay_signal_after_simulated_time" for row in issues) is warning
