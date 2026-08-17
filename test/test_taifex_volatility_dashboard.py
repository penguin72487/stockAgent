from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re

import pytest

from stockagent.live.taifex_volatility_dashboard import (
    _performance_metrics,
    build_dashboard_history_snapshot,
    build_dashboard_snapshot,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _fixture(tmp_path: Path, *, age_seconds: float = 2.0) -> tuple[Path, Path]:
    state_dir = tmp_path / "state"
    receipts = tmp_path / "receipts"
    now = datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc)
    strategy_ids = ["classic_opening_straddle", "daily_vol_model_gamma__black_scholes"]
    marks = {
        strategy_id: {
            "strategy_id": strategy_id,
            "net_equity_twd": 125.0,
            "cumulative_pnl_twd": 125.0,
            "initial_capital_twd": 1_000.0,
            "total_equity_twd": 1_125.0,
            "gross_cash_twd": -10_000.0,
            "open_liquidation_value_twd": 10_200.0,
            "fixed_fees_twd": 44.0,
            "transaction_tax_twd": 31.0,
            "futures_position": 0,
            "option_positions": {"TXO-C": 2, "TXO-P": -1},
            "option_books_valid": True,
            "future_book_valid": True,
        }
        for strategy_id in strategy_ids
    }
    _write_json(
        state_dir / "status.json",
        {
            "updated_at_utc": (now - timedelta(seconds=age_seconds)).isoformat(),
            "engine_status": "cycle_open",
            "blocked_reason": None,
            "simulation_only": True,
            "production_order_possible": False,
            "current_session": "night",
            "current_trading_date": "2026-08-13",
            "intraday_entry_cutoff": "13:20:00",
            "intraday_flatten_time": "13:35:00",
            "night_entry_cutoff": "04:40:00",
            "night_flatten_time": "04:55:00",
            "bootstrap_after_date": "2026-08-11",
            "broker_orders_enabled": True,
            "broker_order_failures": 0,
            "inflight_order_count": 0,
            "underlying_contract": "TXFH6",
            "underlying_product": "TX",
            "underlying_multiplier_twd_per_point": 200.0,
            "underlying_fee_per_side_twd": 60.0,
            "underlying_initial_margin_per_contract_twd": 470_000.0,
            "hedge_contract": "MXFH6",
            "option_contract_count": 98,
            "latest_book_count": 100,
            "held_option_contract_count": 2,
            "held_option_subscribed_count": 2,
            "held_option_book_count": 2,
            "missing_held_option_subscription_codes": [],
            "active_cycle": {
                "cycle_id": "safe-cycle",
                "expiry_date": "2026-08-14",
                "strike": 45_000,
                "account_id": "must-not-leak",
            },
            "pending_targets": {},
            "put_call_parity_tx": {
                "pending_signal": {
                    "signal_decision_ts_ns": 123,
                    "direction": "sell_rich_synthetic_buy_tx",
                    "account_id": "must-not-leak",
                },
                "open_position": None,
                "last_settled_expiry": None,
                "blocked_expiry": None,
                "monitor": {
                    "state": "signal_pending_next_books",
                    "direction": "sell_rich_synthetic_buy_tx",
                    "expiry_date": "2026-08-19",
                    "series": "202608",
                    "strike": 45_000.0,
                    "gross_locked_edge_twd": 24_000.0,
                    "total_estimated_cost_twd": 840.0,
                    "net_after_estimated_cost_twd": 23_160.0,
                    "minimum_net_edge_twd": 0.0,
                    "broker_submission": False,
                    "private_path": "/must/not/leak",
                },
            },
            "strategies": marks,
        },
    )
    _write_json(
        state_dir / "state.json",
        {
            "schema_version": 1,
            "strategy_ids": strategy_ids,
            "strategies": {
                strategy_id: {"initial_capital_twd": 1_000.0}
                for strategy_id in strategy_ids
            },
            "account_id": "secret",
        },
    )
    mark_rows = []
    for minute in range(12):
        for strategy_id in strategy_ids:
            mark_rows.append(
                {
                    "recorded_at_utc": now.isoformat(),
                    "decision_ts_ns": int(
                        (now - timedelta(minutes=11 - minute)).timestamp() * 1e9
                    ),
                    "strategy_id": strategy_id,
                    "net_equity_twd": float(minute),
                    "gross_cash_twd": 0.0,
                    "open_liquidation_value_twd": float(minute),
                    "fixed_fees_twd": 0.0,
                    "transaction_tax_twd": 0.0,
                    "futures_position": 0,
                    "option_books_valid": True,
                    "future_book_valid": True,
                }
            )
    (state_dir / "marks.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in mark_rows), encoding="utf-8"
    )
    (state_dir / "events.jsonl").write_text("{}\n", encoding="utf-8")
    _write_json(
        receipts / "20260812-futures-simulation-lifecycle.json",
        {
            "account": {"account_id": "must-not-leak"},
            "result": "ok",
            "simulation": True,
            "production_order_possible": False,
            "logical_contract": "TXFR1",
            "resolved_contract": "TXFH6",
            "baseline_position": 0,
            "final_position": 0,
            "finished_at_utc": now.isoformat(),
            "steps": [{"order_id": "must-not-leak"}],
        },
    )
    return state_dir, receipts


def test_dashboard_snapshot_is_bounded_fresh_and_account_safe(tmp_path: Path) -> None:
    state_dir, receipts = _fixture(tmp_path)
    now = datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc)
    payload = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=now,
        mark_limit_per_strategy=8,
    )
    assert payload["health"] == "active"
    assert payload["dashboard_schema_version"] == 7
    assert payload["source_age_seconds"] == 2.0
    assert payload["market"]["book_coverage_ratio"] == 1.0
    assert payload["market"]["strategy_fresh_valuation_coverage_ratio"] == 1.0
    assert payload["market"]["strategy_timely_valuation_coverage_ratio"] == 1.0
    assert payload["market"]["strategy_fresh_valuation_count"] == 2
    assert payload["market"]["held_option_subscription_coverage_ratio"] == 1.0
    assert len(payload["strategies"]) == 2
    assert payload["strategy_counts"]["live_ideal"] == 58
    assert payload["strategy_counts"]["blocked_contract"] >= 1
    assert (
        len(payload["strategy_catalog"]) == payload["strategy_counts"]["catalog_total"]
    )
    assert any(
        row["strategy_id"] == "short_atm_straddle"
        and row["availability"] == "live_ideal"
        and row["directional_exposure"] == "neutral"
        and row["volatility_exposure"] == "short_volatility"
        and row["design_option_short_ratio"] == 1.0
        for row in payload["strategy_catalog"]
    )
    assert any(
        row["strategy_id"] == "conversion"
        and row["directional_exposure"] == "hedged_neutral"
        and row["volatility_exposure"] == "volatility_neutral"
        and row["hedge_type"] == "parity_locked"
        and row["design_option_long_ratio"] == 0.5
        for row in payload["strategy_catalog"]
    )
    assert len(payload["history"]) == 16
    assert payload["record_counts"]["marks"] == 24
    assert payload["api_round_trip"]["result"] == "ok"
    assert payload["current_session"] == "night"
    assert payload["current_market_phase"] == "day_continuous"
    assert payload["runner_mode"] == "always_on_scheduled_capture"
    assert len(payload["trading_schedule"]) == 6
    assert payload["trading_schedule"][0]["ideal_fill_allowed"] is False
    assert payload["trading_schedule"][1]["ideal_fill_allowed"] is True
    assert payload["catalog_expansion_entry_policy"] is None
    assert payload["current_trading_date"] == "2026-08-13"
    assert payload["night_flatten_time"] == "04:55:00"
    assert payload["market"]["underlying_product"] == "TX"
    assert payload["put_call_parity_tx"]["state"] == ("signal_pending_next_books")
    assert payload["put_call_parity_tx"]["net_after_estimated_cost_twd"] == (23_160.0)
    assert payload["put_call_parity_tx"]["pending_signal"] == {
        "direction": "sell_rich_synthetic_buy_tx",
        "signal_decision_ts_ns": 123,
    }
    assert "private_path" not in payload["put_call_parity_tx"]
    first = payload["strategies"][0]
    assert first["reserved_capital_twd"] == 1_000.0
    assert first["one_unit_net_pnl_twd"] == 125.0
    assert first["one_unit_net_pnl_abs_twd"] == 125.0
    assert first["fixed_capital_return"] == 0.125
    assert first["explicit_cost_twd"] == 75.0
    assert first["net_pnl_to_explicit_cost_ratio"] == pytest.approx(5 / 3)
    assert first["observed_trading_day_count"] == 2
    assert first["compounded_return_to_live_mark"] == pytest.approx(0.126254)
    assert first["directional_exposure"] == "neutral"
    assert first["volatility_exposure"] == "long_volatility"
    assert first["design_option_ratio_label"] == "多 100% / 空 0% (2:0 口)"
    assert first["live_option_long_ratio"] == pytest.approx(2 / 3)
    assert first["live_option_short_ratio"] == pytest.approx(1 / 3)
    assert first["live_option_ratio_label"] == "多 67% / 空 33% (2:1 口)"
    assert payload["exposure_summary"]["ratio_basis"]
    assert payload["exposure_summary"]["live"]["directional_exposure"]
    assert payload["exposure_taxonomy"]["hedge_type"]["dynamic_delta"]["label"]
    summary = payload["portfolio_summary"]
    assert summary["independent_strategy_reserved_capital_twd"] == 2_000.0
    assert summary["independent_strategy_explicit_cost_twd"] == 150.0
    assert summary["median_fixed_capital_return"] == 0.125
    assert payload["metric_definitions"]["fixed_capital_return"]
    assert "account" not in json.dumps(payload).casefold()
    assert "order_id" not in json.dumps(payload).casefold()


def test_dashboard_snapshot_fails_visible_when_source_is_stale(tmp_path: Path) -> None:
    state_dir, receipts = _fixture(tmp_path, age_seconds=20.0)
    payload = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc),
    )
    assert payload["health"] == "stale"


def test_dedicated_history_snapshot_is_range_aware_and_bounded(tmp_path: Path) -> None:
    state_dir, receipts = _fixture(tmp_path)
    now = datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc)
    full = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=now,
        mark_limit_per_strategy=8,
    )
    history = build_dashboard_history_snapshot(
        state_dir=state_dir,
        now=now,
        mark_limit_per_strategy=8,
    )
    assert history["range"] == "1d"
    assert {row["strategy_id"] for row in history["history"]} == {
        row["strategy_id"] for row in full["history"]
    }
    assert history["history"][-1] == full["history"][-1]
    assert history["record_counts"] == {"history_rows_returned": 16}


def test_history_snapshot_merges_receipted_backfill_and_prefers_live_on_collision(
    tmp_path: Path,
) -> None:
    state_dir, _receipts = _fixture(tmp_path)
    strategy_id = "classic_opening_straddle"
    backfill_dir = state_dir / "backfills" / "rolling_straddles_bidask_v1"
    backfill_dir.mkdir(parents=True)
    older_ns = int(datetime(2026, 8, 10, 1, 0, tzinfo=timezone.utc).timestamp() * 1e9)
    live_first = json.loads((state_dir / "marks.jsonl").read_text().splitlines()[0])
    rows = [
        {
            "decision_ts_ns": older_ns,
            "strategy_id": strategy_id,
            "cumulative_pnl_twd": 200.0,
            "initial_capital_twd": 2_000.0,
            "gross_cash_twd": 0.0,
            "open_liquidation_value_twd": 200.0,
            "fixed_fees_twd": 0.0,
            "transaction_tax_twd": 0.0,
            "futures_position": 0,
            "option_books_valid": True,
            "future_book_valid": True,
            "valuation_available": True,
            "history_source": "shioaji_worker0_completed_second_bidask",
            "replay_id": "receipt-test",
        },
        {
            **live_first,
            "strategy_id": strategy_id,
            "cumulative_pnl_twd": 999.0,
            "net_equity_twd": 999.0,
            "history_source": "must_be_replaced_by_live",
        },
    ]
    backfill_mark_path = backfill_dir / "marks.jsonl"
    backfill_mark_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    _write_json(
        backfill_dir / "receipt.json",
        {
            "status": "partial_receipt_backfill",
            "replay_id": "receipt-test",
            "source": "shioaji_worker0_completed_second_bidask",
            "strategy_ids": [strategy_id],
            "requested_start_date": "2026-08-10",
            "requested_end_date": "2026-08-12",
            "source_coverage": [],
            "record_counts": {"marks": 2},
            "output_sha256": {
                "marks.jsonl": hashlib.sha256(
                    backfill_mark_path.read_bytes()
                ).hexdigest()
            },
        },
    )

    history = build_dashboard_history_snapshot(
        state_dir=state_dir,
        now=datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc),
        mark_limit_per_strategy=100,
        range_key="all",
    )
    older = next(
        row for row in history["history"] if row["decision_ts_ns"] == older_ns
    )
    collision = next(
        row
        for row in history["history"]
        if row["decision_ts_ns"] == live_first["decision_ts_ns"]
        and row["strategy_id"] == strategy_id
    )
    assert older["initial_capital_twd"] == 2_000.0
    assert older["fixed_capital_return"] == pytest.approx(0.1)
    assert older["history_source"] == "shioaji_worker0_completed_second_bidask"
    assert collision["history_source"] == "live_forward_ledger"
    assert collision["cumulative_pnl_twd"] != 999.0
    assert history["record_counts"]["backfill_mark_rows"] == 2
    assert history["backfills"][0]["replay_id"] == "receipt-test"


def test_dashboard_marks_fresh_but_incomplete_strategy_valuation_as_degraded(
    tmp_path: Path,
) -> None:
    state_dir, receipts = _fixture(tmp_path)
    status_path = state_dir / "status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    broken = status["strategies"]["classic_opening_straddle"]
    broken["valuation_available"] = False
    broken["option_books_valid"] = False
    broken["cumulative_pnl_twd"] = None
    broken["net_equity_twd"] = None
    broken["total_equity_twd"] = None
    _write_json(status_path, status)

    payload = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc),
    )

    assert payload["health"] == "degraded"
    assert payload["market"]["strategy_fresh_valuation_count"] == 1
    assert payload["market"]["strategy_fresh_valuation_coverage_ratio"] == 0.5
    assert payload["market"]["strategy_timely_valuation_coverage_ratio"] == 0.5


def test_dashboard_accepts_only_recent_explicit_carried_valuation_for_health(
    tmp_path: Path,
) -> None:
    state_dir, receipts = _fixture(tmp_path)
    status_path = state_dir / "status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    carried = status["strategies"]["classic_opening_straddle"]
    carried["valuation_available"] = True
    carried["valuation_stale"] = True
    carried["valuation_carried_forward"] = True
    carried["valuation_age_seconds"] = 5.0
    carried["option_books_valid"] = False
    _write_json(status_path, status)

    recent = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc),
    )
    assert recent["health"] == "active"
    assert recent["market"]["strategy_fresh_valuation_count"] == 1
    assert recent["market"]["strategy_recent_carried_valuation_count"] == 1
    assert recent["market"]["strategy_timely_valuation_count"] == 2

    status["strategies"]["classic_opening_straddle"]["valuation_age_seconds"] = 20.0
    _write_json(status_path, status)
    old = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc),
    )
    assert old["health"] == "degraded"
    assert old["market"]["strategy_timely_valuation_count"] == 1


def test_dashboard_expected_closed_gap_is_waiting_not_stale(tmp_path: Path) -> None:
    state_dir, receipts = _fixture(tmp_path, age_seconds=300.0)
    payload = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=datetime(2026, 8, 12, 6, 0, tzinfo=timezone.utc),
    )
    assert payload["current_market_phase"] == "day_close_to_night_preopen"
    assert payload["source_fresh_expected"] is False
    assert payload["health"] == "waiting"


def test_dashboard_append_only_counts_and_tail_follow_new_rows(tmp_path: Path) -> None:
    state_dir, receipts = _fixture(tmp_path)
    now = datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc)
    first = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=now,
        mark_limit_per_strategy=2,
    )
    new_row = {
        "recorded_at_utc": now.isoformat(),
        "decision_ts_ns": int(now.timestamp() * 1e9),
        "strategy_id": "classic_opening_straddle",
        "net_equity_twd": 99.0,
        "open_liquidation_value_twd": 99.0,
        "option_books_valid": True,
        "future_book_valid": True,
    }
    with (state_dir / "marks.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(new_row) + "\n")

    second = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=now,
        mark_limit_per_strategy=2,
    )
    assert first["record_counts"]["marks"] == 24
    assert second["record_counts"]["marks"] == 25
    classic = [
        row
        for row in second["history"]
        if row["strategy_id"] == "classic_opening_straddle"
    ]
    assert len(classic) == 2
    assert classic[-1]["net_equity_twd"] == 99.0
    assert classic[-1]["total_equity_twd"] == 1_099.0


def test_dashboard_carries_previous_complete_mark_instead_of_partial_jump(
    tmp_path: Path,
) -> None:
    state_dir, receipts = _fixture(tmp_path)
    now = datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc)
    invalid = {
        "recorded_at_utc": now.isoformat(),
        "decision_ts_ns": int(now.timestamp() * 1e9),
        "strategy_id": "classic_opening_straddle",
        "active_cycle_id": None,
        "net_equity_twd": 450_000.0,
        "gross_cash_twd": 0.0,
        "open_liquidation_value_twd": 450_000.0,
        "fixed_fees_twd": 0.0,
        "transaction_tax_twd": 0.0,
        "futures_position": 0,
        "option_books_valid": False,
        "future_book_valid": True,
    }
    with (state_dir / "marks.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(invalid) + "\n")

    payload = build_dashboard_snapshot(
        state_dir=state_dir,
        api_receipt_dir=receipts,
        now=now,
    )
    classic = [
        row
        for row in payload["history"]
        if row["strategy_id"] == "classic_opening_straddle"
    ]
    assert classic[-1]["valuation_carried_forward"] is True
    assert classic[-1]["cumulative_pnl_twd"] == classic[-2]["cumulative_pnl_twd"]
    assert classic[-1]["total_equity_twd"] == classic[-2]["total_equity_twd"]
    assert classic[-1]["total_equity_twd"] != 451_000.0


def test_dashboard_compounds_daily_pnl_changes_not_minute_marks() -> None:
    metrics = _performance_metrics(
        daily_endpoints={
            "2026-08-12": (1, 100.0),
            "2026-08-13": (2, 210.0),
        },
        current_trading_date="2026-08-13",
        current_ts_ns=2,
        current_pnl_twd=210.0,
        reserved_capital_twd=1_000.0,
        explicit_cost_twd=25.0,
        margin_required_twd=400.0,
    )
    assert metrics["fixed_capital_return"] == 0.21
    assert metrics["compounded_return_to_live_mark"] == pytest.approx(0.221)
    assert metrics["margin_utilization"] == 0.4
    assert metrics["observed_trading_day_count"] == 2
    assert metrics["compound_includes_partial_trading_day"] is True


def test_dashboard_html_is_local_and_refreshes_the_read_only_api() -> None:
    root = Path(__file__).resolve().parents[1] / "services" / "taifex_dashboard"
    html = (root / "index.html").read_text(encoding="utf-8")
    javascript = (root / "app.js").read_text(encoding="utf-8")
    stylesheet = (root / "styles.css").read_text(encoding="utf-8")
    assert "http://" not in html
    external_links = re.findall(r'href="(https://[^"]+)"', html)
    assert external_links == [
        "https://www.taifex.com.tw/cht/11/newsDetail?idx=17259&amp;newsType=1"
    ]
    assert 'fetchWithTimeout("api/status"' in javascript
    assert (
        "fetchWithTimeout(`api/history?range=${encodeURIComponent(requestedRange)}`"
        in javascript
    )
    assert "HISTORY_CLIENT_CACHE_MS" in javascript
    assert "historyPayloadCache" in javascript
    assert "response.status === 429" not in javascript
    assert "秒後自動重試" not in javascript
    assert "const PRICE_REFRESH_MS = 60000" in javascript
    assert "function refreshMinuteSnapshot()" in javascript
    assert "window.setInterval(refreshMinuteSnapshot, PRICE_REFRESH_MS)" in javascript
    assert javascript.count("window.setInterval(") == 1
    assert "const REFRESH_MS = 5000" not in javascript
    assert "row.total_equity_twd" in javascript
    assert "row.total_equity_twd != null" in javascript
    assert "CARRIED" in javascript
    assert 'id="night-session-times"' in html
    assert 'id="strategy-guide-grid"' in html
    assert 'id="curve-wall-grid"' in html
    assert 'id="strategy-sort"' in html
    assert 'id="exposure-direction-filter"' in html
    assert 'id="exposure-volatility-filter"' in html
    assert 'id="exposure-hedge-filter"' in html
    assert 'id="exposure-summary"' in html
    assert 'id="performance-reserve"' in html
    assert 'class="skip-link"' in html
    assert 'aria-label="公開面板導覽"' in html
    assert 'id="curve-load-more"' in html
    assert 'id="strategy-search"' in html
    assert 'id="guide-load-more"' in html
    assert 'id="runner-mode"' in html
    assert 'id="night-official-session"' in html
    assert "所有策略怎麼做" in html
    assert 'setText("current-session"' in javascript
    assert "renderStrategyGuide" in javascript
    assert "renderCurveWall" in javascript
    assert "renderExposureSummary" in javascript
    assert "matchesExposureFilters" in javascript
    assert "row.design_option_ratio_label" in javascript
    assert "compounded_return_to_live_mark" in javascript
    assert 'return {label: "資料逾時", state: "blocked"}' in javascript
    assert "if (document.hidden || refreshInFlight) return" in javascript
    assert "if (document.hidden) return" in javascript
    assert "if (historyInFlight) return" in javascript
    assert "curveVisibleCount" in javascript
    assert "guideVisibleCount" in javascript
    assert 'snapshot.health === "degraded"' in javascript
    assert 'href="styles.css?v=14"' in html
    assert 'src="../time-axis.js?v=4"' in html
    assert 'src="app.js?v=20"' in html
    assert "collapseEmptyIntervals: true" in javascript
    assert "全策略皆無資料的區段已略過、不補 0" in javascript
    assert 'id="equity-time-range"' in html
    assert 'data-range="1y"' in html
    assert 'data-range="all"' in html
    assert 'id="parity-net-edge"' in html
    assert "function renderParity" in javascript
    assert "row.underlying_futures_position" in javascript
    assert "之後每 1 分鐘同步刷新" in html
    assert 'class="strategy-table"' in html
    assert (
        "<th>策略／狀態</th><th>曝險／口數比</th><th>報酬</th><th>損益／成本</th><th>資金／保證金</th><th>部位／估值</th>"
        in html
    )
    assert "function appendTableMetric" in javascript
    assert "function strategyStatusPill" in javascript
    assert 'capital.dataset.label = "資金／保證金"' in javascript
    assert "可左右滑動" not in html
    assert ".strategy-table-wrap { overflow-x: visible; }" in stylesheet
    assert ".strategy-table { table-layout: fixed;" in stylesheet
    assert "@media (max-width: 900px)" in stylesheet
