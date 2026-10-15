"""History baseline, view priorities and candidate-scoped progress contracts."""
from datetime import UTC, date, datetime, timedelta
import json

from scripts.download_finlab_market_intraday import _coverage_phases, _open_index, _record, default_market_end, session_days
from stockagent.data.finlab_progress import annotate_inventory, estimate_attempt_batch, feature_group
from stockagent.live.finlab_dashboard import build_finlab_public_status


def test_baselines_become_incrementals_and_source_null_is_not_complete():
    rows = [
        {"key": "price:收盤價", "state": "downloaded", "workload": {"downloaded": True, "needs_refresh": True}},
        {"key": "financial_statement:EPS", "state": "downloaded", "workload": {"downloaded": True}},
        {"key": "unknown:new", "state": "pending", "workload": {"downloaded": False}},
        {"key": "management_change_events:date", "state": "provider_empty", "workload": {"blocked_reason": "provider_empty"}},
        {"key": "tw_tick:2330", "state": "partial_windowed"},
    ]
    info = annotate_inventory(rows, workload_current=True)
    assert info["general_total"] == 4
    assert info["mode_counts"] == {"incremental": 2, "history_backfill": 1, "source_wait": 1}
    assert rows[0]["collection_mode"] == "incremental"  # due != missing history
    assert rows[-1]["collection_mode"] == "windowed_backfill"
    assert sum(group["total_keys"] for group in info["groups"]) == 4
    assert sum(group["due_keys"] for group in info["groups"]) == 1


def test_stale_or_regressed_scope_cannot_claim_unqualified_history_baseline():
    rows = [{"key": "financial_statement:EPS", "state": "downloaded",
             "workload": {"downloaded": True}, "history_scope_warning": {"non_null_count_decline": 10}}]
    assert annotate_inventory(rows, workload_current=True)["mode_counts"] == {"incremental_with_history_review": 1}
    assert annotate_inventory(rows, workload_current=False)["mode_counts"] == {"unverified": 1}


def test_every_feature_is_classified_and_tick_is_lowest_view_priority():
    assert feature_group("price:成交股數")["importance"] == 1
    assert feature_group("financial_statement_revised:EPS")["importance"] == 2
    assert feature_group("tw_taifex_stock_margin_ratio:rate")["feature_group"] == "derivatives"
    assert feature_group("after_market_fixed_price:市場別", record_unit="metadata_values")["feature_group"] == "labels"
    assert feature_group("new_unknown:field")["feature_group"] == "other"
    assert feature_group("tw_minute:2330")["importance"] == 10


def test_batch_eta_adds_full_cycle_cost_and_does_not_count_inflight_as_completed():
    estimate = estimate_attempt_batch([1, 1, 10], limit=6, since_completion_seconds=1)
    assert estimate["eta_measurement_contract_version"] == 2
    assert estimate["seconds_per_attempt_estimate"] == 4  # median=1 would underestimate additive work
    assert estimate["remaining_batch_seconds_estimate"] == 11
    assert estimate["completed_attempts"] == 3
    assert estimate_attempt_batch([10, 10], limit=6, since_completion_seconds=2)[
        "remaining_batch_seconds_estimate"] is None
    assert estimate_attempt_batch([10000]*10 + [2]*64, limit=80, since_completion_seconds=0)[
        "seconds_per_attempt_estimate"] == 2
    assert estimate_attempt_batch([1, 1, 10], limit=3, since_completion_seconds=1)[
        "remaining_batch_seconds_estimate"] == 0


def test_latest_candidate_bound_is_taipei_today_but_unobserved_dates_are_not_sessions(tmp_path):
    import polars as pl

    now = datetime(2026, 10, 7, 8, tzinfo=UTC)  # 16:00 in Taipei
    bound = default_market_end(now)
    assert bound == date(2026, 10, 7)
    assert default_market_end(datetime(2026, 10, 6, 17, tzinfo=UTC)) == date(2026, 10, 6)
    assert default_market_end(datetime(2026, 10, 7, 5, 29, tzinfo=UTC)) == date(2026, 10, 6)
    assert default_market_end(datetime(2026, 10, 7, 5, 30, tzinfo=UTC)) == bound
    pl.DataFrame({"date": ["2026-10-06"]}).write_parquet(tmp_path / "twse_market_index.parquet")
    assert session_days(tmp_path, start=date(2026, 10, 6), end=bound) == [date(2026, 10, 6)]
    pl.DataFrame({"date": ["2026-10-06", "2026-10-07", "2026-10-08"]}).write_parquet(
        tmp_path / "twse_market_index.parquet")
    assert session_days(tmp_path, start=date(2026, 10, 6), end=bound) == [
        date(2026, 10, 6), date(2026, 10, 7)]


def test_candidate_phases_are_disjoint_and_never_treat_not_ready_as_acquired(tmp_path):
    days = [date(2026, 5, 19), date(2026, 5, 20), date(2026, 5, 21)]
    universe = [
        {"symbol": "2330", "current": True, "first_candidate_date": "2026-05-19"},
        {"symbol": "dead", "current": False, "first_candidate_date": "2026-05-19",
         "delisting_date": "2026-05-20", "last_local_daily_date": "2026-05-20"},
    ]
    with _open_index(tmp_path / "index.sqlite3") as db:
        _record(db, "tw_tick", "2330", days[1], "downloaded_unverified_for_pit", 5, 100)
        _record(db, "tw_tick", "2330", days[-1], "partition_not_ready")
        _record(db, "tw_tick", "dead", days[1], "verified_no_trade")
        _record(db, "tw_tick", "dead", days[-1], "downloaded_unverified_for_pit", 100, 1000)  # outside lifecycle
        _record(db, "tw_tick", "other", days[1], "downloaded_unverified_for_pit", 100, 1000)  # outside universe
        _record(db, "tw_minute", "2330", days[1], "downloaded_unverified_for_pit", 100, 1000)  # legacy
        phases = _coverage_phases(db, universe, days, first_observed="2026-05-20")
    assert sum(row["candidate_partitions"] for row in phases) == 5
    assert sum(row["receipted_partitions"] for row in phases) == 2
    assert sum(row["rows"] for row in phases) == 5
    assert phases[0]["candidate_check_ratio"] == 1
    assert phases[0]["candidate_coverage_ratio"] == 0
    assert phases[0]["state"] == "source_range_unverified"
    assert phases[1]["state"] == "candidate_window_acquired"
    assert phases[2]["untested_candidate_partitions"] == 2
    assert all(row["finish_at_utc"] is None for row in phases)


def test_exemplar_reconciles_with_same_market_source_not_old_generic_sample(tmp_path):
    now = datetime(2026, 10, 7, 8, tzinfo=UTC)
    def write(relative, value):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
    write("artifacts/live/data_monitor/public_status.json", {
        "read_only": True, "production_control_possible": False, "generated_at_utc": now.isoformat(),
        "sources": [{"id": "finlab:tw_tick:2330", "finlab_acquisition_state": "partial_windowed",
                     "record_stats": {"count": 2, "first": "2026-06-01", "last": "2026-06-01"}}],
    })
    market = {"schema_version": 3, "observed_at_utc": now.isoformat(),
              "symbols": [{"key": "tw_tick:2330", "rows": 999, "first_data_date": "2026-05-20",
                           "last_data_date": "2026-10-06", "receipted_partitions": 20,
                           "target_partitions": 100, "parquet_bytes": 42}],
              "coverage_phases": []}
    write("data_finlab/intraday/market_status.json", market)
    value = build_finlab_public_status(tmp_path, now=now)
    assert value["datasets"][0]["rows"] == 999
    assert value["datasets"][0]["first"] == "2026-05-20"
    assert value["datasets"][0]["partition_receipts"] == 20
    assert value["datasets"][0]["collection_mode"] == "windowed_backfill"
    market["observed_at_utc"] = (now - timedelta(minutes=5)).isoformat()
    write("data_finlab/intraday/market_status.json", market)
    stale = build_finlab_public_status(tmp_path, now=now)
    assert stale["intraday_market"]["fresh"] is False
    assert stale["datasets"][0]["rows"] == 999  # retain historical evidence, never regress to the old sample
    assert stale["datasets"][0]["partition_snapshot_fresh"] is False
    assert stale["datasets"][0]["partition_observed_at_utc"] == market["observed_at_utc"]
