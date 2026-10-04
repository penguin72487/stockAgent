from datetime import UTC, datetime, timedelta
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.download_finlab_history import fetch_one, general_work_status, local_plan_reads, read_key_state
from scripts.download_finlab_history import sync_selection
from scripts.finlab_incremental_refresh import checked_get
from scripts.finlab_stage_schedule import MIB, forecast, stage_summary, next_release_waves
from stockagent.data.finlab_acquisition_contract import (
    next_source_check, proven_source_empty, safe_stem, source_check_due,
)

NOW = datetime(2026, 10, 1, 10, tzinfo=UTC)


def test_future_wave_cannot_start_before_its_observed_expiry():
    due = NOW + timedelta(hours=1)
    rows = [job("price:a", size=1, stage="priority_updates", pending=False, next_check=due.isoformat())]
    wave = next_release_waves(rows, now=NOW, quota=account(1000), reserve_mb=50, next_run=NOW,
                              dispatch_interval_seconds=0)["priority_updates"]
    assert wave["keys"] == 1 and wave["check_at_utc"] == due.isoformat()
    assert wave["scenarios"]["reference"]["start_at_utc"] == due.isoformat()
    assert wave["scenarios"]["reference"]["finish_at_utc"] == (due + timedelta(seconds=10)).isoformat()
    assert forecast(rows, now=NOW, quota=account(1000), reserve_mb=50, next_run=NOW)["state"] == "complete"


def test_stage_retains_retry_clock_without_promising_source_recovery():
    retry = NOW + timedelta(minutes=15)
    rows = [{**job("empty", stage="source_issues"), "blocked_reason": "provider_empty", "fetch_seconds": None,
             "retry_at_utc": retry.isoformat()}]
    stage = next(s for s in stage_summary(rows, {"reference": {"state": "blocked"}}, now=NOW, tick={})
                 if s["id"] == "source_issues")
    assert stage["next_retry_at_utc"] == retry.isoformat() and stage["cooldown_keys"] == 1
    assert stage["remaining_work_seconds_estimate"] is None
    assert stage["scenarios"]["reference"]["finish_at_utc"] is None


def account(remaining=0, limit=5000):
    return {"observed_at_utc": NOW.isoformat(), "remaining_mb": remaining, "limit_mb": limit,
            "reset_at_utc": "2026-10-02T00:00:00+00:00"}


def job(key, size=100, seconds=10, stage="updates", pending=True, next_check=None):
    return {"key": key, "transfer_bytes": size*MIB, "admission_bytes": size*MIB,
            "fetch_seconds": seconds, "queue_role": stage, "blocked_reason": None,
            "needs_refresh": pending, "source_checked_at_utc": (NOW-timedelta(days=1)).isoformat(),
            "next_source_check_at_utc": next_check, "record_count": 42, "record_unit": "wide_values"}


def sdk_mock(*, publication=True, marker=True, fail=False):
    storage = SimpleNamespace(_publications={}, expiry=NOW, created=NOW)
    storage.get_time_created = lambda key: storage.created
    storage.get_time_expired = lambda key: storage.expiry
    storage.set_time_expired = lambda key, expiry, save=False: setattr(storage, "expiry", expiry)
    ctx = SimpleNamespace(_storage=storage, _loaded_datasets={"price:a"},
                          prefer_local_if_exists=True, use_local_data_only=True, force_cloud_download=True)
    def get(key, *, force_download, progress, progress_callback):
        assert all(getattr(ctx, n) is False for n in ("prefer_local_if_exists", "use_local_data_only", "force_cloud_download"))
        assert key not in ctx._loaded_datasets
        if fail:
            raise RuntimeError("safe test failure")
        progress_callback({"dataset": key, "phase": "download", "download_bytes": 50})
        progress_callback({"dataset": key, "phase": "download", "download_bytes": 100})
        progress_callback({"dataset": key, "phase": "complete", "download_bytes": 100})
        if marker:
            storage._publications[key] = (storage.created, {"content_hash": "abc12345"} if publication else None)
        storage.expiry = datetime.now(UTC) + timedelta(hours=2)
        return pd.DataFrame({"v": [0, -3]}, index=pd.to_datetime(["2020-01-01", "2026-01-01"]))
    sdk = SimpleNamespace(_default_context=ctx, get=Mock(side_effect=get))
    return sdk, ctx, storage


def test_sdk_incremental_must_observe_source_marker_and_restore_flags():
    sdk, ctx, storage = sdk_mock()
    frame, evidence = checked_get("price:a", sdk=sdk)
    assert sdk.get.call_args.kwargs["force_download"] is False
    assert evidence["source_check_mode"] == "upstream_incremental"
    assert evidence["provider_content_hash"] == "abc12345"
    assert evidence["last_payload_bytes"] == 100  # cumulative progress is not summed
    assert next_source_check(evidence) > datetime.now(UTC)
    assert frame.iloc[:, 0].tolist() == [0, -3]
    assert ctx.prefer_local_if_exists and ctx.use_local_data_only and ctx.force_cloud_download


def test_cache_only_read_falls_back_to_forced_source_check():
    sdk, _, _ = sdk_mock(marker=False)
    _, evidence = checked_get("price:a", sdk=sdk)
    assert [c.kwargs["force_download"] for c in sdk.get.call_args_list] == [False, True]
    assert evidence["source_check_mode"] == "upstream_forced"
    assert evidence["provider_content_hash"] is None
    assert evidence["last_payload_bytes"] == 200


def test_legacy_metadata_check_without_publication_is_not_hash_proof():
    sdk, _, _ = sdk_mock(publication=False)
    _, evidence = checked_get("price:a", sdk=sdk)
    assert evidence["source_check_mode"] == "upstream_incremental"
    assert evidence["provider_content_hash"] is None


def test_failure_restores_expiry_and_context():
    sdk, ctx, storage = sdk_mock(fail=True)
    with pytest.raises(RuntimeError):
        checked_get("price:a", sdk=sdk)
    assert storage.expiry == NOW
    assert ctx.prefer_local_if_exists and ctx.use_local_data_only and ctx.force_cloud_download


@pytest.mark.parametrize("name,value", [("truncate_start", "2024"), ("truncate_end", "2026"),
                                        ("_projected_datasets", {"price:a"})])
def test_projected_context_cannot_certify_full_history(name, value):
    sdk, ctx, _ = sdk_mock()
    setattr(ctx, name, value)
    with pytest.raises(ValueError, match="unprojected"):
        checked_get("price:a", sdk=sdk)
    sdk.get.assert_not_called()


def test_bound_sdk_expiry_prevents_daily_blind_rescan():
    receipt = {"source_check_mode": "upstream_incremental", "source_checked_at_utc": NOW.isoformat(),
               "next_source_check_at_utc": (NOW+timedelta(days=7)).isoformat()}
    assert not source_check_due(receipt, now=NOW+timedelta(days=2))
    assert source_check_due(receipt, now=NOW+timedelta(days=7))
    assert source_check_due(receipt, now=NOW-timedelta(seconds=1))
    receipt["source_check_mode"] = "sdk_cache_allowed"
    assert source_check_due(receipt, now=NOW)


@pytest.mark.parametrize("expiry", [None, "broken", "2026-09-01T00:00:00Z", "2030-01-01T00:00:00Z"])
def test_invalid_expiry_retains_legacy_reset_policy(expiry):
    receipt = {"source_check_mode": "upstream_forced", "source_checked_at_utc": NOW.isoformat(),
               "next_source_check_at_utc": expiry}
    assert next_source_check(receipt).isoformat() == "2026-10-02T00:00:00+00:00"


def test_cross_day_arrivals_preempt_metadata_instead_of_frozen_queue():
    rows = [job("metadata", stage="metadata"), job("close", stage="priority_updates", pending=False)]
    result = forecast(rows, now=NOW, quota=account(), reserve_mb=50, next_run=NOW)
    assert result["newly_due_checks"] == 1
    assert result["simulated_checks"] == 2
    assert result["finish_at_utc"] == "2026-10-02T00:00:25+00:00"
    assert result["stage_start_at_utc"]["metadata"] == "2026-10-02T00:00:15+00:00"
    assert result["remaining_seconds"] == result["processing_seconds"] + result["total_wait_seconds"]


def test_incremental_eta_does_not_wait_for_reset_just_because_room_is_under_reserve():
    row = dict(job("close", size=1, stage="priority_updates"), incremental_quota_exempt=True)
    result = forecast([row], now=NOW, quota=account(30), reserve_mb=50, next_run=NOW)
    assert result["finish_at_utc"] == (NOW+timedelta(seconds=10)).isoformat()
    assert result["quota_resets"] == 0 and result["quota_opening_wait_seconds"] == 0


def test_quota_blocked_history_prefix_cannot_delay_known_incremental_stage_eta():
    rows = [job("history", size=100, stage="history"),
            dict(job("feature", size=1), incremental_quota_exempt=True)]
    result = forecast(rows, now=NOW, quota=account(30), reserve_mb=50, next_run=NOW)
    assert result["key_finish_at_utc"]["feature"] == (NOW+timedelta(seconds=10)).isoformat()
    assert result["key_finish_at_utc"]["history"] > result["key_finish_at_utc"]["feature"]
    stages = stage_summary(rows, {"reference": result}, now=NOW, tick={})
    assert next(s for s in stages if s["id"] == "updates")["incremental_quota_exempt_keys"] == 1


def test_incremental_eta_still_accounts_for_actual_daily_source_capacity():
    row = dict(job("close", size=100, stage="priority_updates"), incremental_quota_exempt=True)
    result = forecast([row], now=NOW, quota=account(30), reserve_mb=50, next_run=NOW)
    assert result["quota_resets"] == 1
    assert result["finish_at_utc"] == "2026-10-02T00:00:15+00:00"


def test_quota_deferred_history_waits_for_earlier_incremental_retry_not_just_reset():
    due = NOW + timedelta(hours=1)
    rows = [job("history", size=100, stage="history"),
            dict(job("feature", size=1), incremental_quota_exempt=True, retry_at_utc=due.isoformat())]
    result = forecast(rows, now=NOW, quota=account(30), reserve_mb=50, next_run=NOW)
    assert result["key_finish_at_utc"]["feature"] == (due+timedelta(seconds=10)).isoformat()
    assert result["remaining_seconds"] == result["processing_seconds"] + result["total_wait_seconds"]


def test_future_incremental_release_uses_remaining_room_before_reset():
    rows = [job("history", size=100, stage="history"),
            dict(job("close", size=1, stage="priority_updates", pending=False,
                     next_check=(NOW+timedelta(hours=1)).isoformat()), incremental_quota_exempt=True)]
    result = forecast(rows, now=NOW, quota=account(30), reserve_mb=50, next_run=NOW)
    # One source check before reset, another after reset, then history.
    assert result["newly_due_checks"] == 2 and result["simulated_checks"] == 3
    assert result["finish_at_utc"] == "2026-10-02T00:00:25+00:00"


def test_local_reserve_cannot_erase_an_entire_small_incremental_daily_capacity():
    row = dict(job("close", size=1, stage="priority_updates"), incremental_quota_exempt=True)
    result = forecast([row], now=NOW, quota=account(remaining=30, limit=30), reserve_mb=50, next_run=NOW)
    assert result["finish_at_utc"] == (NOW+timedelta(seconds=10)).isoformat()


def test_expiry_after_target_completion_does_not_get_charged_early():
    rows = [job("metadata", stage="metadata"), job("close", stage="priority_updates", pending=False,
                                                   next_check="2026-10-02T08:00:00Z")]
    result = forecast(rows, now=NOW, quota=account(), reserve_mb=50, next_run=NOW)
    assert result["simulated_checks"] == 1
    assert result["finish_at_utc"] == "2026-10-02T00:00:15+00:00"


def test_capacity_constraint_retains_earlier_phase_completion_not_fake_all_eta():
    rows = [job("close", size=600, stage="priority_updates"), job("meta", size=450, stage="metadata")]
    result = forecast(rows, now=NOW, quota=account(remaining=1000, limit=1000), reserve_mb=50,
                      next_run=NOW, horizon_days=3)
    assert result["state"] == "capacity_constrained"
    assert result["finish_at_utc"] is None
    assert result["stage_finish_at_utc"]["priority_updates"] == (NOW+timedelta(seconds=10)).isoformat()
    assert "metadata" not in result["stage_finish_at_utc"]
    stages = stage_summary(rows, {"reference": result}, now=NOW, tick={})
    assert next(s for s in stages if s["id"] == "metadata")["scenarios"]["reference"]["finish_at_utc"] is None


@pytest.mark.parametrize("change,state", [({"limit_mb": 50}, "no_daily_budget"),
    ({"remaining_mb": float("nan")}, "quota_unverified"),
    ({"observed_at_utc": (NOW+timedelta(seconds=1)).isoformat()}, "quota_unverified")])
def test_stages_never_forecast_against_invalid_quota(change, state):
    result = forecast([job("a")], now=NOW, quota={**account(), **change}, reserve_mb=50, next_run=NOW)
    assert result["state"] == state and result["finish_at_utc"] is None


def test_unknown_new_arrival_and_oversized_object_have_no_fake_finish():
    for row, state in [(job("a", size=6000), "object_exceeds_daily_budget"),
                       ({**job("a"), "fetch_seconds": None}, "insufficient_samples")]:
        result = forecast([row], now=NOW, quota=account(), reserve_mb=50, next_run=NOW)
        assert result["state"] == state and result["finish_at_utc"] is None


def test_source_issue_and_tick_cannot_be_marked_complete():
    rows = [{**job("empty", stage="source_issues"), "blocked_reason": "provider_empty"}]
    stages = stage_summary(rows, {"reference": {"state": "blocked"}}, now=NOW,
                           tick={"reason": "unmeasured"})
    issue = next(s for s in stages if s["id"] == "source_issues")
    assert issue["state"] == "blocked" and issue["progress_ratio"] is None
    assert issue["scenarios"]["reference"]["finish_at_utc"] is None
    assert stages[-1]["state"] == "unmeasured" and stages[-1]["remaining_bytes_estimate"] is None


def write(root, kind, key, data):
    path = root / kind / (safe_stem(key)+".json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def test_ephemeral_read_cache_is_cleared_for_the_next_plan(tmp_path):
    write(tmp_path, "receipts", "a", {"value": 1})
    with local_plan_reads():
        assert read_key_state("a", tmp_path)["value"] == 1
        write(tmp_path, "receipts", "a", {"value": 2})
        assert read_key_state("a", tmp_path)["value"] == 1
    assert read_key_state("a", tmp_path)["value"] == 2


def test_unchanged_sdk_hash_skips_serialization_but_not_integrity(tmp_path):
    from importlib.metadata import version
    path = tmp_path / "datasets/a.parquet"
    path.parent.mkdir()
    pq.write_table(pa.table({"source_index": ["2020"], "v": [-3]}), path)
    old = {"dataset": "price:a", "status": "downloaded_unverified_for_pit", "rows": 1,
           "rows_with_values": 1, "field_columns": 1, "parquet_path": "datasets/a.parquet",
           "parquet_size_bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
           "fetched_at_utc": "2020-01-01T00:00:00Z", "provider_content_hash": "abc12345",
           "provider_hash_basis": "sdk_validated_publication_v1", "incremental_adapter_version": 1,
           "normalization_contract_version": 1,
           "finlab_sdk_version": version("finlab")}
    write(tmp_path, "receipts", "price:a", old)
    sdk, _, _ = sdk_mock()
    frame, evidence = checked_get("price:a", sdk=sdk)
    with patch("scripts.finlab_incremental_refresh.checked_get", return_value=(frame, evidence)), \
            patch("scripts.download_finlab_history.serialize_provider_frame", side_effect=AssertionError("should skip")):
        receipt = fetch_one("price:a", tmp_path, refresh=True, incremental=True)
    assert receipt["last_check_result"] == "unchanged"
    assert receipt["fetched_at_utc"] == old["fetched_at_utc"]
    assert receipt["sha256"] == old["sha256"]
    assert len(list((tmp_path / "datasets").glob("*.parquet"))) == 1


def test_source_empty_exception_does_not_release_global_completeness(tmp_path):
    attempt = {"dataset": "empty:a", "status": "provider_empty", "raw_non_null_values": 0,
               "empty_evidence_path": "empty_evidence/proof.json", "attempted_at_utc": NOW.isoformat()}
    write(tmp_path, "attempts", "empty:a", attempt)
    discovery = {"keys": ["empty:a"], "observed_at_utc": NOW.isoformat()}
    assert not general_work_status(discovery, {}, tmp_path, now=NOW, refresh_days=1)["supplemental_allowed"]
    proof_root = tmp_path / "empty_evidence"
    proof_root.mkdir()
    raw = proof_root / "raw.feather"
    raw.write_bytes(b"all-null source test bytes")
    (proof_root / "proof.json").write_text(json.dumps({
        "dataset": "empty:a", "status": "source_all_null", "usable_observations": False,
        "raw_non_null_values": 0, "raw_path": "empty_evidence/raw.feather",
        "raw_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
    }))
    status = general_work_status(discovery, {}, tmp_path, now=NOW, refresh_days=1)
    assert not status["required_complete"]
    assert status["supplemental_allowed"]
    assert status["scheduled_reserve_mb"] == 50
    raw.write_bytes(b"changed source bytes")
    assert not general_work_status(discovery, {}, tmp_path, now=NOW, refresh_days=1)["supplemental_allowed"]
    attempt.pop("empty_evidence_path")
    write(tmp_path, "attempts", "empty:a", attempt)
    assert not general_work_status(discovery, {}, tmp_path, now=NOW, refresh_days=1)["supplemental_allowed"]
    assert not proven_source_empty("empty:a", {"status": "provider_empty"})


def stored(root, key, checked, *, next_check=None, raw_bytes=None):
    path = root / "datasets" / (safe_stem(key)+".parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"source_index": ["2020"], "v": [0]}), path)
    receipt = {"dataset": key, "status": "downloaded_unverified_for_pit", "rows": 1,
               "rows_with_values": 1, "parquet_path": str(path.relative_to(root)),
               "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "parquet_size_bytes": path.stat().st_size,
               "source_check_mode": "upstream_incremental", "source_checked_at_utc": checked.isoformat(),
               "next_source_check_at_utc": next_check, "last_full_source_bytes": raw_bytes}
    write(root, "receipts", key, receipt)


def test_aged_metadata_is_not_starved_by_new_daily_numeric_checks(tmp_path):
    meta = "after_market_fixed_price:市場別"
    numeric = "financial_statement:eps"
    stored(tmp_path, meta, NOW-timedelta(days=4))
    stored(tmp_path, numeric, NOW-timedelta(days=1))
    selected = sync_selection([numeric, meta, "missing:numeric"], {}, tmp_path,
                              now=NOW, refresh_days=1, retry_unavailable=False)
    assert selected == ["missing:numeric", meta, numeric]
    rows = [job(numeric), job(meta, stage="metadata")]
    rows[1]["source_checked_at_utc"] = (NOW-timedelta(days=4)).isoformat()
    result = forecast(rows, now=NOW, quota=account(5000), reserve_mb=50, next_run=NOW)
    assert result["key_finish_at_utc"][meta] < result["key_finish_at_utc"][numeric]


def test_dynamic_reserve_only_protects_checks_before_this_reset(tmp_path):
    stored(tmp_path, "price:a", NOW, next_check=(NOW+timedelta(hours=1)).isoformat(), raw_bytes=250*MIB)
    stored(tmp_path, "price:b", NOW, next_check=(NOW+timedelta(days=2)).isoformat(), raw_bytes=2000*MIB)
    discovery = {"keys": ["price:a", "price:b"], "observed_at_utc": NOW.isoformat()}
    status = general_work_status(discovery, {}, tmp_path, now=NOW, refresh_days=1)
    assert status["required_complete"] and status["supplemental_allowed"]
    assert status["scheduled_reserve_mb"] == 300
    assert status["scheduled_reserve_unknown_keys"] == 0


def test_unmeasured_scheduled_release_keeps_conservative_tick_reserve(tmp_path):
    stored(tmp_path, "price:a", NOW, next_check=(NOW+timedelta(hours=1)).isoformat())
    discovery = {"keys": ["price:a"], "observed_at_utc": NOW.isoformat()}
    status = general_work_status(discovery, {}, tmp_path, now=NOW, refresh_days=1)
    assert status["scheduled_reserve_mb"] == 500 and status["scheduled_reserve_unknown_keys"] == 1


def test_capacity_heuristic_does_not_skip_a_feasible_current_tail_window():
    rows = [job("meta", size=450, stage="metadata"),
            job("close", size=600, stage="priority_updates", pending=False)]
    result = forecast(rows, now=NOW, quota=account(remaining=500, limit=1000), reserve_mb=50,
                      next_run=NOW)
    assert result["state"] == "conditional"
    assert result["finish_at_utc"] == (NOW+timedelta(seconds=10)).isoformat()
