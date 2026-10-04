from datetime import UTC, datetime, timedelta
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.finlab_workload import MIB, build_workload, record_measure, schedule_jobs
from scripts.download_finlab_history import refresh_due, safe_stem
from stockagent.live.finlab_dashboard import _public_workload

NOW = datetime(2026, 9, 28, 2, tzinfo=UTC)


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def quota(remaining=30, limit=5000):
    return {"observed_at_utc": NOW.isoformat(), "remaining_mb": remaining,
            "limit_mb": limit, "reset_at_utc": "2026-09-29T00:00:00+00:00"}


def test_schedule_uses_bytes_not_key_counts_and_waits_for_reset():
    jobs = [{"key": "a", "transfer_bytes": 100 * MIB, "fetch_seconds": 60},
            {"key": "b", "transfer_bytes": 4000 * MIB, "fetch_seconds": 10}]
    result = schedule_jobs(jobs, now=NOW, quota=quota(), reserve_mb=50, next_run=None)
    assert result["quota_resets"] == 1
    assert result["finish_at_utc"] == "2026-09-29T00:01:15+00:00"
    assert result["processing_seconds"] == 70


def test_whole_object_does_not_fit_tail_budget_even_if_total_average_would():
    jobs = [{"key": str(i), "transfer_bytes": 3000*MIB, "fetch_seconds": 10} for i in range(2)]
    result = schedule_jobs(jobs, now=NOW, quota=quota(5000), reserve_mb=50, next_run=None)
    assert result["quota_resets"] == 1
    assert result["key_finish_at_utc"]["1"] == "2026-09-29T00:00:15+00:00"


@pytest.mark.parametrize("changes,state", [
    ({"remaining_mb": float("nan")}, "quota_unverified"),
    ({"observed_at_utc": "2026-09-28T01:00:00Z"}, "quota_unverified"),
    ({"observed_at_utc": "2026-09-28T03:00:00Z"}, "quota_unverified"),
    ({"remaining_mb": 9000}, "quota_unverified"),
    ({"limit_mb": 50}, "no_daily_budget"),
])
def test_unverified_budget_has_no_finish(changes, state):
    result = schedule_jobs([], now=NOW, quota={**quota(), **changes}, reserve_mb=50, next_run=None)
    assert result["state"] == state
    assert result["finish_at_utc"] is None


def test_large_object_and_unknown_sample_have_no_fake_eta():
    for size, seconds, state in [(6000*MIB, 1, "object_exceeds_daily_budget"),
                                 (100*MIB, None, "insufficient_samples")]:
        result = schedule_jobs([{"key": "a", "transfer_bytes": size, "fetch_seconds": seconds}],
                               now=NOW, quota=quota(), reserve_mb=50, next_run=None)
        assert result["state"] == state
        assert result["finish_at_utc"] is None


def test_opening_window_wait_is_counted_without_changing_download_work():
    result = schedule_jobs([{"key": "a", "transfer_bytes": MIB, "fetch_seconds": 10}],
                           now=NOW, quota=quota(), reserve_mb=50, next_run=None,
                           opening_policy=lambda t: t + timedelta(hours=1))
    assert result["finish_at_utc"] == "2026-09-29T01:00:15+00:00"
    assert result["processing_seconds"] == 10


def test_initial_queue_and_retry_wait_reconcile_to_remaining_wall_time():
    result = schedule_jobs(
        [{"key": "a", "transfer_bytes": MIB, "fetch_seconds": 10,
          "retry_at_utc": (NOW + timedelta(minutes=5)).isoformat()}],
        now=NOW, quota=quota(5000), reserve_mb=50,
        next_run=NOW + timedelta(minutes=2))
    assert result["queue_wait_seconds"] == 300
    assert result["quota_opening_wait_seconds"] == 0
    assert result["total_wait_seconds"] == 300
    assert result["remaining_seconds"] == result["processing_seconds"] + result["total_wait_seconds"]


def test_empty_queue_does_not_wait_for_next_timer():
    result = schedule_jobs([], now=NOW, quota=quota(), reserve_mb=50,
                           next_run=NOW + timedelta(hours=1))
    assert result["state"] == "complete"
    assert result["finish_at_utc"] == NOW.isoformat()
    assert result["remaining_seconds"] == 0


def test_rows_cells_and_events_do_not_share_a_denominator(tmp_path):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"source_index": ["a", "b"], "2330": [1, None], "2317": [0, 3]}), path)
    result = record_measure(path, {})
    assert result["unit"] == "wide_values"
    assert result["count"] == 3
    assert result["stored_rows"] == 2
    assert record_measure(path, {"storage_layout": "provider_long_arrow"})["count"] == 2
    pq.write_table(pa.table({"value": [1, None]}), path, write_statistics=False)
    assert record_measure(path, {})["count"] is None


def setup_root(root):
    keys = ["price:a", "price:b", "price:empty", "tw_tick:2330"]
    write(root / "configs/finlab_history_candidates.json", {"datasets": []})
    write(root / "data_finlab/catalog/discovery.json", {"keys": keys, "observed_at_utc": NOW.isoformat()})
    write(root / "artifacts/live/data_monitor/public_status.json", {
        "generated_at_utc": NOW.isoformat(), "read_only": True, "production_control_possible": False,
        "finlab_acquisition": {"timer_active": True, "next_run_at_utc": NOW.isoformat()}})
    for i, key in enumerate(keys[:2]):
        relative = f"datasets/{safe_stem(key)}.parquet"
        path = root / "data_finlab" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"source_index": ["x", "y"], "v": [1, None]}), path)
        write(root / "data_finlab/receipts" / f"{safe_stem(key)}.json", {
            "dataset": key, "status": "downloaded_unverified_for_pit",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "rows": 2, "rows_with_values": 1, "parquet_size_bytes": path.stat().st_size,
            "parquet_path": relative, "source_checked_at_utc": (NOW - timedelta(days=i)).isoformat(),
            "source_check_mode": "upstream_forced", "last_fetch_elapsed_seconds": 10,
            "source_index_columns": ["source_index"]})
        cache = root / "sdk"; cache.mkdir(exist_ok=True)
        (cache / (key.replace(":", "#") + ".feather")).write_bytes(b"x"*(100 if i == 0 else 900))
    write(root / "data_finlab/attempts" / f"{safe_stem('price:empty')}.json", {"status": "provider_empty"})
    return keys


def test_active_sample_overrun_cannot_slide_a_fake_finish_forward(tmp_path):
    setup_root(tmp_path)
    write(tmp_path / "data_finlab/runs/latest.json", {
        "state": "running", "active_key": "price:b", "active_started_at_utc": (NOW-timedelta(minutes=10)).isoformat()})
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path / "sdk", now=NOW,
                               quota=quota(5000), footer_budget_seconds=10)
    assert result["scenarios"]["reference"]["state"] == "insufficient_samples"
    assert next(r for r in result["datasets"] if r["key"] == "price:b")["estimated_finish_at_utc"] is None


def test_snapshot_weights_bytes_includes_refresh_not_just_missing_keys(tmp_path):
    setup_root(tmp_path)
    result, cache = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                                  quota=quota(), footer_budget_seconds=10)
    assert result["general_keys"] == 3
    assert result["blocked_keys"] == ["price:empty"]
    assert result["transfer"]["ratio"] == .1  # not 2 downloaded / 3 keys
    assert result["transfer"]["remaining_bytes_estimate"] == 900
    assert result["work_time"]["ratio"] == .5  # time weights are independent of bytes
    assert result["work_time"]["completed_seconds"] == 10
    assert result["work_time"]["remaining_seconds_estimate"] == 10
    assert result["records"][0]["completed_count"] == 1
    assert result["records"][0]["remaining_estimate"] == 1
    assert result["all_data_finish_at_utc"] is None
    second, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                               quota=quota(), cache=cache, footer_budget_seconds=0)
    assert second["measurement"]["footer_reads"] == 0
    assert second["records"] == result["records"]


def test_no_footer_budget_keeps_counts_unknown_not_zero(tmp_path):
    setup_root(tmp_path)
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                               quota=quota(), footer_budget_seconds=0)
    assert result["unknown_record_keys"] == 2
    assert all(row["record_count"] is None for row in result["datasets"])


def test_live_incremental_quota_policy_and_v2_footer_cache_are_shared_with_projection(tmp_path):
    keys = setup_root(tmp_path)
    _, cache = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                              quota=quota(), footer_budget_seconds=10)
    cache["contract_version"] = 2
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                               quota=quota(.001), cache=cache, footer_budget_seconds=0)
    assert result["contract_version"] == 4 and result["quota_policy_version"] == 1
    assert result["measurement"]["footer_reads"] == 0
    row = next(r for r in result["datasets"] if r["key"] == "price:b")
    assert row["incremental_quota_exempt"] is True and row["record_count"] == 1
    assert result["scenarios"]["reference"]["quota_resets"] == 0
    write(tmp_path/"artifacts/live/finlab/workload_latest.json", result)
    public = _public_workload(tmp_path, [{"key": key} for key in keys], NOW)
    assert public["incremental_quota_policy"] == "provider_enforced_no_local_reserve"
    assert next(r for r in public["datasets"] if r["key"] == "price:b")["incremental_quota_exempt"] is True


@pytest.mark.parametrize("status", ["vip_only", "authentication_failed", "provider_empty"])
def test_old_verified_data_does_not_hide_a_new_source_refresh_denial(tmp_path, status):
    setup_root(tmp_path)
    key = "price:b"
    write(tmp_path/"data_finlab/attempts"/(safe_stem(key)+".json"), {
        "dataset": key, "status": status, "attempted_at_utc": (NOW-timedelta(minutes=1)).isoformat()})
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                               quota=quota(), footer_budget_seconds=10)
    row = next(r for r in result["datasets"] if r["key"] == key)
    assert row["downloaded"] and row["incremental_quota_exempt"] and row["needs_refresh"]
    assert row["blocked_reason"] == status and row["queue_role"] == "source_issues"
    assert row["estimated_finish_at_utc"] is None
    assert result["scenarios"]["reference"]["finish_at_utc"] is None


def test_later_success_supersedes_a_previous_source_permission_failure(tmp_path):
    setup_root(tmp_path)
    key = "price:b"
    write(tmp_path/"data_finlab/attempts"/(safe_stem(key)+".json"), {
        "dataset": key, "status": "vip_only", "attempted_at_utc": (NOW-timedelta(days=2)).isoformat()})
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                               quota=quota(), footer_budget_seconds=10)
    row = next(r for r in result["datasets"] if r["key"] == key)
    assert row["blocked_reason"] is None and row["queue_role"] == "priority_updates"


def test_forecast_retry_clock_matches_collectors_computed_failure_backoff(tmp_path):
    setup_root(tmp_path)
    key = "price:b"
    attempted = NOW-timedelta(minutes=1)
    write(tmp_path/"data_finlab/attempts"/(safe_stem(key)+".json"), {
        "dataset": key, "status": "timed_out", "attempted_at_utc": attempted.isoformat(), "failure_streak": 1})
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                               quota=quota(5000), footer_budget_seconds=10)
    row = next(r for r in result["datasets"] if r["key"] == key)
    assert row["blocked_reason"] is None
    assert row["retry_at_utc"] == (attempted+timedelta(minutes=15)).isoformat()
    assert row["estimated_finish_at_utc"] > row["retry_at_utc"]


def test_optional_metadata_in_backoff_is_scheduled_not_a_missing_work_plan(tmp_path):
    keys = setup_root(tmp_path)
    key = "after_market_fixed_price:市場別"
    write(tmp_path/"data_finlab/catalog/discovery.json", {"keys": [*keys, key], "observed_at_utc": NOW.isoformat()})
    receipt = json.loads((tmp_path/"data_finlab/receipts"/(safe_stem("price:b")+".json")).read_text())
    write(tmp_path/"data_finlab/receipts"/(safe_stem(key)+".json"), {**receipt, "dataset": key})
    (tmp_path/"sdk"/(key.replace(":", "#")+".feather")).write_bytes(b"x"*100)
    attempted = NOW-timedelta(minutes=1)
    write(tmp_path/"data_finlab/attempts"/(safe_stem(key)+".json"), {
        "dataset": key, "status": "resource_deferred", "attempted_at_utc": attempted.isoformat()})
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                               quota=quota(5000), footer_budget_seconds=10)
    row = next(r for r in result["datasets"] if r["key"] == key)
    assert row["scheduled"] and row["queue_role"] == "metadata" and not row["incremental_quota_exempt"]
    assert row["retry_at_utc"] == (attempted+timedelta(hours=1)).isoformat()
    assert result["scenarios"]["reference"]["state"] != "unscheduled_work"
    assert row["estimated_finish_at_utc"] > row["retry_at_utc"]


def test_changed_file_cannot_remain_current_through_workload_cache(tmp_path):
    setup_root(tmp_path)
    _, cache = build_workload(tmp_path, sdk_cache_root=tmp_path / 'sdk', now=NOW,
                              quota=quota(), footer_budget_seconds=10)
    path = tmp_path / 'data_finlab/datasets' / f'{safe_stem("price:a")}.parquet'
    path.write_bytes(b'')
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path / 'sdk', now=NOW,
                               quota=quota(), cache=cache, footer_budget_seconds=10)
    row = next(r for r in result['datasets'] if r['key'] == 'price:a')
    assert not row['downloaded'] and row['needs_refresh']
    assert row['record_count'] is None
    assert result['transfer']['completed_bytes'] == 0


@pytest.mark.parametrize('checked', [
    (NOW + timedelta(milliseconds=8)).isoformat(),
    (NOW + timedelta(days=1)).isoformat(),
    NOW.replace(tzinfo=None).isoformat(),
])
def test_receipt_after_snapshot_cutoff_is_pending_not_unscheduled(tmp_path, checked):
    setup_root(tmp_path)
    root = tmp_path / 'data_finlab'
    receipt_path = root / 'receipts' / f'{safe_stem("price:a")}.json'
    receipt = json.loads(receipt_path.read_text())
    receipt['source_checked_at_utc'] = checked
    write(receipt_path, receipt)
    assert refresh_due('price:a', root, now=NOW, days=1)
    assert refresh_due('price:a', root, now=NOW, days=7)
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path / 'sdk', now=NOW,
                               quota=quota(5000), footer_budget_seconds=10)
    row = next(r for r in result['datasets'] if r['key'] == 'price:a')
    assert row['needs_refresh'] and row['scheduled']
    assert result['transfer']['completed_bytes'] == 0
    assert result['scenarios']['reference']['state'] == 'conditional'


def test_stale_scheduler_or_catalog_cannot_generate_finish(tmp_path):
    setup_root(tmp_path)
    path = tmp_path / "artifacts/live/data_monitor/public_status.json"
    value = json.loads(path.read_text()); value["generated_at_utc"] = (NOW-timedelta(hours=1)).isoformat()
    write(path, value)
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW, quota=quota())
    assert result["scenarios"]["reference"]["finish_at_utc"] is None
    assert result["scenarios"]["reference"]["state"] == "scheduler_unverified"


def test_arrow_null_type_is_known_empty_even_without_statistics(tmp_path):
    path = tmp_path / "nulls.parquet"
    pq.write_table(pa.table({"source_index": ["a", "b"], "empty": [None, None], "value": [0, 1]}), path)
    result = record_measure(path, {})
    assert result["count"] == 2


def test_public_projection_redacts_and_invalidates_stale_eta(tmp_path):
    keys = setup_root(tmp_path)
    result, _ = build_workload(tmp_path, sdk_cache_root=tmp_path/"sdk", now=NOW,
                               quota=quota(), footer_budget_seconds=10)
    result["token"] = "SECRET"
    result["datasets"][0]["raw_path"] = "SECRET"
    write(tmp_path / "artifacts/live/finlab/workload_latest.json", result)
    projected = _public_workload(tmp_path, [{"key": key} for key in keys], NOW)
    assert projected["state"] == "available"
    assert "SECRET" not in json.dumps(projected)
    old = _public_workload(tmp_path, [{"key": key} for key in keys], NOW+timedelta(minutes=4))
    assert old["state"] == "stale"
    assert old["scenarios"] == {}
    assert old["transfer"]["ratio"] is None
    assert old["work_time"]["ratio"] is None
    assert all(row["ratio"] is None for row in old["records"])
