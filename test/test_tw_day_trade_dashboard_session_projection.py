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


def test_nontrade_settlement_public_projection_has_no_source_path_or_fake_exit():
    from stockagent.live.public_dashboards import sanitize_tw_positions

    position = {"market": "m", "symbol": "2330", "session_date": DAY, "signed_shares": 0,
        "status": "nontrade_book_settled", "realized_net_pnl_twd": 120.,
        "nontrade_settlement": {"contract": "last_observed_trade_nonexecution_book_settlement_v1",
            "quantity": 1000, "accounting_at": f"{DAY}T13:30:00+08:00",
            "reference": {"reference_observed_at": f"{DAY}T12:59:20+08:00", "source_path": "/private/source",
                          "reference_price": 999.},
            "assumed_exit_fee_and_tax_twd": 5., "net_pnl_twd": 80.,
            "reference_before_simulated_entry": False, "simulated_fill": False,
            "broker_fill": False, "exchange_fill": False}}
    safe = dashboard._safe_position(position)
    assert safe["execution_realized_net_pnl_twd"] == 40.
    assert safe["nontrade_settlement_net_pnl_twd"] == 80.
    assert "exit_at" not in safe and "exit_price" not in safe
    public = sanitize_tw_positions({"simulation_only": True, "production_order_possible": False, "rows": [safe]})
    row = public["rows"][0]
    assert row["nontrade_settlement_shares"] == 1000
    assert row["nontrade_reference_observed_at"] == f"{DAY}T12:59:20+08:00"
    assert "/private/source" not in str(public) and "999.0" not in str(public)


def test_history_uses_pinned_header_without_rereading_replaced_state(tmp_path, monkeypatch):
    original = dashboard._object

    def read(path, **kwargs):
        if path == tmp_path / "state.json":
            pytest.fail("the committed history header must not race a later state rename")
        return original(path, **kwargs)

    monkeypatch.setattr(dashboard, "_object", read)
    result = dashboard.build_dashboard_history_snapshot(state_dir=tmp_path,
        _committed_history_state={"product": "tw_day_trade", "enabled_markets": ["one"]},
        use_memory_cache=False, use_persistent_cache=False, use_session_projection=False)
    assert result["returned_points"] == 0


def test_feature_summary_fences_racing_cache(tmp_path, monkeypatch):
    source = tmp_path / "summary.json"
    source.write_text(json.dumps({
        "weights": list(range(1000)),
        "source_publication_contract": {"research_only": True},
        "model_explanation": {"all_feature_drivers": [{"feature": "open", "weighted_abs_value": 1}]},
    }))
    monkeypatch.setattr(dashboard, "_SIGNAL_FEATURE_SUMMARY_CACHE", {})
    original = dashboard._object

    def race(path):
        result = original(path)
        source.write_text(source.read_text().replace('"weighted_abs_value": 1', '"weighted_abs_value": 2'))
        return result

    monkeypatch.setattr(dashboard, "_object", race)
    first = dashboard._read_signal_feature_record(source)
    assert first == {"drivers": [{"feature": "open", "weighted_abs_value": 1}], "research_only": True}
    assert not dashboard._SIGNAL_FEATURE_SUMMARY_CACHE
    monkeypatch.setattr(dashboard, "_object", original)
    assert dashboard._read_signal_feature_record(source)["drivers"][0]["weighted_abs_value"] == 2
    assert len(dashboard._SIGNAL_FEATURE_SUMMARY_CACHE) == 1


def test_feature_summary_retains_canonical_manifest_validation_and_no_parent_cache(tmp_path, monkeypatch):
    source = tmp_path / "summary.json"
    source.write_text('{"paper_mode_\\u0073napshot":null}')
    monkeypatch.setattr(dashboard, "_SIGNAL_FEATURE_SUMMARY_CACHE", {})
    calls = []

    def canonical(path):
        calls.append(path)
        return {"model_explanation": {"top_feature_drivers": [{"feature": "open"}]}}

    monkeypatch.setattr(dashboard, "_object", canonical)
    assert dashboard._read_signal_feature_record(source)["drivers"] == [{"feature": "open", "weighted_abs_value": 0}]
    dashboard._read_signal_feature_record(source)
    assert calls == [source, source]
    assert not dashboard._SIGNAL_FEATURE_SUMMARY_CACHE


@pytest.mark.parametrize("hour", [2, 8, 12])
def test_preopen_uses_selected_session_without_backdating_observation(tmp_path, hour):
    session = "2026-10-30"
    observed = datetime(2026, 10, 31, hour, tzinfo=TAIPEI)
    stamp = f"{session}T08:30:00+08:00"
    preopen = tmp_path / "discord.json"
    preopen.write_text(json.dumps({"updated_at": stamp, "markets": {
        "m": {"status": "ready", "completed_at": stamp},
    }}))
    result = dashboard._preopen_progress(
        path=preopen, modes=[{"market": "m"}], observed=observed, session_date=session,
    )
    assert result["status"] == "ready"
    assert result["session_date"] == session
    assert result["source_age_seconds"] == (observed - datetime.fromisoformat(stamp)).total_seconds()
    assert result["markets"][0]["final_arm_current_process_required"] is False

    simulation = tmp_path / "simulation.json"
    simulation.write_text(json.dumps({"session_date": session, "status": "ready", "components": {
        "eligibility": {"status": "ready"}, "shioaji_quote": {"status": "ready"},
    }}))
    assert dashboard._simulation_preopen_progress(
        path=simulation, observed=observed, session_date=session,
    )["ready"] is True
    # Previous-session readiness must never admit the next trading session.
    assert dashboard._simulation_preopen_progress(
        path=simulation, observed=observed, session_date="2026-11-02",
    )["ready"] is False
    assert dashboard._preopen_progress(
        path=preopen, modes=[{"market": "m"}], observed=observed, session_date="2026-11-02",
    )["status"] == "pending"


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
@pytest.mark.parametrize("latest", [False, True])
@pytest.mark.parametrize("stream_full", [False, True])
def test_fragmented_signal_history_uses_complete_session_projections(
    fragmented_accounts, monkeypatch, narrow, has_book, latest, stream_full,
):
    root, _, active = fragmented_accounts
    cache = root / "cache"
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))
    if stream_full:
        monkeypatch.setattr(dashboard, "_COLUMNAR_LEDGER_MAX_BYTES", 128)
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
    request = dict(state_dir=root, start_date=DAY if latest else "2026-09-23", end_date=DAY, limit=100)
    expected = dashboard.build_dashboard_signal_page(**request, maximum_scan_rows=1000)
    dashboard._SIGNAL_PAGE_CACHE.clear()
    dashboard._SIGNAL_RANGE_SUMMARY_CACHE.clear()
    actual = dashboard.build_dashboard_signal_page(
        **request, maximum_scan_rows=1, _allow_narrow_projection=False,
    )
    assert actual["scan_limit_reached"] is False
    for key in ("total", "rows", "direction_summary", "opening_execution_audit"):
        assert actual[key] == expected[key]
    assert actual["source_rows_scanned"] == sum(
        not latest or row["session_date"] == DAY for row in rows
    )
    # Once the immutable per-day shards exist, both readers must reuse them,
    # even if an imported account split every day into multiple file spans.
    dashboard._SIGNAL_PAGE_CACHE.clear()
    dashboard._SIGNAL_RANGE_SUMMARY_CACHE.clear()
    monkeypatch.setattr(dashboard, "_columnar_ledger_frame",
                        lambda *_a, **_k: pytest.fail("must reuse complete session shards"))
    monkeypatch.setattr(dashboard, "_rows_for_sessions",
                        lambda *_a, **_k: pytest.fail("must not use capped raw history"))
    monkeypatch.setattr(dashboard, "_latest_contiguous_session_rows",
                        lambda *_a, **_k: pytest.fail("latest signals must not use capped raw rows"))
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


@pytest.mark.parametrize("damage", [b"\x00\x00{\"event\":\"revision\"}\n", b"\xff\n", b"{broken}\n"])
def test_corrupt_opening_diagnostic_degrades_only_the_telemetry_view(
    fragmented_accounts, damage,
):
    root, now, active = fragmented_accounts
    path = root / "opening_signal_latency.jsonl"
    valid = json.dumps({
        "source": "discord_scheduled_signal", "status": "ready",
        "session_date": DAY, "market": active[0], "signal_id": "a",
        "gate_at": f"{DAY}T09:00:00+08:00",
        "signal_started_at": f"{DAY}T09:00:00+08:00",
        "signal_ready_at": f"{DAY}T09:00:00.1+08:00",
    }).encode() + b"\n"
    raw = valid + damage
    path.write_bytes(raw)
    snapshot = dashboard.build_dashboard_snapshot(
        state_dir=root, now=now, session_date=DAY, include_ledger_session_dates=False,
    )
    latency = snapshot["opening_signal_latency"]
    assert latency["integrity"] == {
        "state": "partial_corrupt", "scope": "last_physical_lines",
        "maximum_lines": 10000, "malformed_lines": 1,
    }
    assert latency["observed_mode_count"] == 1
    assert latency["first_signal_goal_met"] is None
    assert latency["all_modes_goal_met"] is None
    assert latency["final_ledger_ms"] is None
    assert latency["replay_without_live_measurement"] is None
    assert all(not row["complete"] and row["all_modes_goal_met"] is None
               for row in latency["trend"])
    assert snapshot["health"] == "degraded"
    assert any(issue["code"] == "opening_signal_telemetry_corrupt"
               for issue in snapshot["operational_issues"])
    assert snapshot["execution_records"]["all_modes_filled"] is True
    assert path.read_bytes() == raw
    with pytest.raises((ValueError, UnicodeDecodeError)):
        dashboard._tail(path, 10000, row_decoder=dashboard._decode_opening_latency_row)


def test_corrupt_telemetry_never_masks_corrupt_financial_state(fragmented_accounts):
    root, now, _ = fragmented_accounts
    (root / "opening_signal_latency.jsonl").write_bytes(b"\x00{}\n")
    (root / "state.json").write_bytes(b"{broken}")
    with pytest.raises(json.JSONDecodeError):
        dashboard.build_dashboard_snapshot(state_dir=root, now=now, session_date=DAY)


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


@pytest.mark.parametrize("has_historical_value", [False, True])
def test_historical_nontrade_component_is_not_backdated_from_latest_account(fragmented_accounts, has_historical_value):
    root, now, active = fragmented_accounts
    state_path = root / "state.json"
    state = json.loads(state_path.read_text())
    state["modes"][active[0]].update(session_date="2026-09-25", cumulative_nontrade_settlement_net_pnl_twd=999.)
    state_path.write_text(json.dumps(state))
    mark_path = root / "marks.jsonl"
    rows = [json.loads(line) for line in mark_path.read_text().splitlines()]
    for row in rows:
        if row.get("session_date") == DAY and row["market"] == active[0] and has_historical_value:
            row["cumulative_nontrade_settlement_net_pnl_twd"] = 12.
    _write_rows(mark_path, rows)
    snapshot = dashboard.build_dashboard_snapshot(state_dir=root, now=now, session_date=DAY)
    mode = next(mode for mode in snapshot["modes"] if mode["market"] == active[0])
    assert mode["cumulative_nontrade_settlement_net_pnl_twd"] == (12. if has_historical_value else None)


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


def test_unavailable_price_band_does_not_claim_every_security_has_limits():
    mode = {"market": "m", "label": "模型", "session_date": DAY,
            "signal_reason_counts": {"price_limit_unavailable": 1}}
    issues = dashboard._operational_issues(
        modes=[mode], preopen={}, observed=datetime(2026, 9, 24, 11, tzinfo=TAIPEI),
    )
    issue = next(row for row in issues if row["code"] == "price_limit_unavailable")
    assert issue["severity"] == "error" and issue["count"] == 1
    assert "未提供或不適用" in issue["title"]
    assert "未取得可驗證" in issue["detail"] and "不適用漲跌幅限制" in issue["detail"]
    assert "TP／SL" in issue["detail"] and "未建立或增加該筆新持倉" in issue["detail"]
    assert "既有持倉不因此被移除" in issue["detail"]


@pytest.mark.parametrize("open_positions", [0, 9])
def test_divergent_ledger_warning_explains_exit_processing_is_suspended(open_positions):
    mode = {
        "market": "tw_day_trade_multi_basis_22", "label": "多基底22",
        "session_date": DAY, "open_position_count": open_positions,
        "engine_status": "critical_ledger_state_divergence",
    }
    issues = dashboard._operational_issues(
        modes=[mode], preopen={}, observed=datetime(2026, 9, 24, 11, tzinfo=TAIPEI),
    )
    issue = next(row for row in issues if row["code"] == "critical_ledger_state_divergence")
    assert issue["severity"] == "error"
    assert "行情處理" in issue["detail"]
    assert "停損及收盤沖銷不會自動處理" in issue["detail"]
    assert "不能直接解除隔離" in issue["detail"]


@pytest.mark.parametrize("source_time,warning", [("09:00:12", False), ("09:01:00", False), ("09:10:03", True)])
def test_counterfactual_later_signal_is_not_silently_presented_as_causal(source_time, warning):
    mode = {"market": "tw_day_trade_100m", "label": "100m", "session_date": DAY,
            "counterfactual_open_replay": True, "signal_at": f"{DAY}T09:01:00+08:00",
            "source_signal_at": f"{DAY}T{source_time}+08:00"}
    issues = dashboard._operational_issues(
        modes=[mode], preopen={}, observed=datetime(2026, 9, 24, 11, tzinfo=TAIPEI),
    )
    assert any(row["code"] == "replay_signal_after_simulated_time" for row in issues) is warning


@pytest.mark.parametrize("provisional", [False, True])
def test_provisional_replay_discloses_unknown_source_gaps(provisional):
    mode = {"market": "tw_day_trade_100m", "label": "100m", "session_date": DAY,
            "simulation_replay": True, "provisional_minute_source_coverage": provisional}
    issues = dashboard._operational_issues(
        modes=[mode], preopen={}, observed=datetime(2026, 9, 24, 14, tzinfo=TAIPEI))
    matches = [row for row in issues if row["code"] == "provisional_minute_source_coverage"]
    assert bool(matches) is provisional
    if provisional:
        assert matches[0]["severity"] == "warning"
        assert "結果可能改變" in matches[0]["detail"]
        assert "不是券商成交" in matches[0]["detail"]


def test_later_live_session_still_discloses_provisional_cumulative_history():
    mode = {"market": "tw_day_trade_unit_mgda_2m_20261103", "label": "200 萬",
            "session_date": DAY, "simulation_replay": False,
            "provisional_minute_source_coverage": False,
            "cumulative_provisional_minute_source_coverage": True,
            "provisional_history_from": "2026-01-02"}
    issues = dashboard._operational_issues(
        modes=[mode], preopen={}, observed=datetime(2026, 9, 24, 14, tzinfo=TAIPEI))
    match = next(row for row in issues if row["code"] == "cumulative_provisional_minute_source_coverage")
    assert match["severity"] == "warning"
    assert "累積權益" in match["detail"]
    assert "不是券商成交" in match["detail"]


def test_public_status_retains_cumulative_history_warning_after_session_projection(fragmented_accounts):
    from stockagent.live.public_dashboards import sanitize_tw_status

    root, now, active = fragmented_accounts
    path = root / "state.json"
    state = json.loads(path.read_text())
    state["modes"][active[0]].update(
        simulation_replay=False, provisional_minute_source_coverage=False,
        cumulative_provisional_minute_source_coverage=True,
        provisional_history_from="2026-01-02",
    )
    path.write_text(json.dumps(state))
    snapshot = dashboard.build_dashboard_snapshot(state_dir=root, now=now, session_date=DAY,
        include_ledger_session_dates=False)
    public = sanitize_tw_status(snapshot)
    selected = next(row for row in public["modes"] if row["market"] == active[0])
    assert selected["simulation_replay"] is False
    assert selected["cumulative_provisional_minute_source_coverage"] is True
    assert selected["provisional_history_from"] == "2026-01-02"
    assert any(issue["code"] == "cumulative_provisional_minute_source_coverage"
               and issue["market"] == active[0] for issue in public["operational_issues"])
