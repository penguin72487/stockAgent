from datetime import UTC, datetime, timedelta
import json

import pytest

from scripts.calibrate_finlab_tick_eta import interpolate_quota, reconstruct
from stockagent.data.finlab_tick_eta import (
    _finish_work, estimate_tick_progress, general_quota_cycles, load_batches, measured_capacity,
)


NOW = datetime(2026, 10, 7, 9, tzinfo=UTC)


def batch(start, *, acquired=64, seconds=100, attempted=256, state="batch_limit", quota=64000):
    return {"contract_version": 1, "started_at_utc": start.isoformat(),
            "finished_at_utc": (start + timedelta(seconds=seconds)).isoformat(),
            "state": state, "elapsed_seconds": seconds, "attempt_limit": 256,
            "attempted": attempted, "completed_attempts": attempted,
            "successful": acquired, "new_receipted_partitions": acquired,
            "account_delta_bytes": quota, "quota_sample_successes": acquired}


def samples():
    return [{"observed_at_utc": datetime(2026, 10, 6, h, m, tzinfo=UTC).isoformat(),
             "used_mb": used, "limit_mb": 5000}
            for h, m, used in ((0, 0, 0), (23, 59, 1000))]


def inputs():
    start = NOW - timedelta(hours=2)
    batches = [batch(start), batch(start + timedelta(seconds=200)),
               batch(start + timedelta(seconds=400), acquired=0, quota=0),
               batch(start + timedelta(seconds=600), acquired=0, quota=0)]
    phases = []
    for identity, got, missing in (("latest_session", 0, 5), ("observed_history", 10, 0),
                                   ("older_discovery", 0, 30)):
        phases.append({"id": identity, "candidate_partitions": 100, "receipted_partitions": got,
                       "provider_not_ready_partitions": missing, "untested_candidate_partitions": 100-got-missing,
                       "rows": got * 100, "local_parquet_bytes": got * 50})
    market = {"observed_at_utc": NOW.isoformat(), "target_end_date": "2026-10-07",
              "by_kind": {"tw_tick": {"receipted_partitions": 10, "rows": 1000, "parquet_bytes": 500}},
              "coverage_phases": phases}
    quota = {"observed_at_utc": NOW.isoformat(), "limit_mb": 5000, "remaining_mb": 1000,
             "reset_at_utc": "2026-10-08T00:00:00+00:00"}
    return market, {"batches": batches, "quota": quota, "workload": {}, "quota_history": samples(),
                    "now": NOW, "timer_active": True}


def test_minute_estimates_never_use_legacy_tick_batch_throughput_or_billing():
    market, kwargs = inputs()
    market["by_kind"]["tw_minute"] = dict(market["by_kind"]["tw_tick"])
    market["minute_coverage_phases"] = market["coverage_phases"]
    result = estimate_tick_progress(market, kind="tw_minute", **kwargs)
    assert result["measurement"]["data_batches"] == 0
    assert result["phases"][1]["finish_at_utc"] is None
    assert result["phases"][1]["remaining_transfer_bytes_estimate"] is None
    assert result["phases"][1]["scenarios"]["reference"]["state"] == "insufficient_samples"
    assert result["phases"][2]["scenarios"]["reference"]["state"] == "bounded_source_discovery"
    for row in kwargs["batches"]:
        row["kind"] = "tw_minute"
    assert estimate_tick_progress(market, kind="tw_minute", **kwargs)["measurement"]["data_batches"] == 2
    assert measured_capacity(kwargs["batches"], now=NOW)["data_batches"] == 0


def test_tick_eta_is_withheld_while_higher_priority_minute_work_is_unmeasured():
    market, kwargs = inputs()
    market.update(acquisition_mode="minute_first_tick_residual", minute_priority={"tick_residual_allowed": False})
    result = estimate_tick_progress(market, **kwargs)
    assert result["state"] == "waiting_higher_priority_minutes"
    assert all(row["finish_at_utc"] is None for row in result["phases"])
    assert result["phases"][1]["scenarios"]["reference"]["state"] == "waiting_higher_priority_minutes"


def test_new_engine_earns_its_own_capacity_without_serial_or_other_worker_samples():
    start = NOW - timedelta(hours=2)
    serial = {**batch(start), "kind": "tw_minute", "acquisition_policy_version": 3}
    candidate = {**batch(start + timedelta(seconds=200)), "kind": "tw_minute",
                 "acquisition_policy_version": 4, "processing_engine": "bounded_range_process_pool",
                 "pipeline_workers": 1, "range_calendar_days": 31}
    selected = {**batch(start + timedelta(seconds=400)), **{key: candidate[key] for key in (
        "kind", "acquisition_policy_version", "processing_engine", "range_calendar_days")}, "pipeline_workers": 4}
    result = measured_capacity([serial, candidate, selected], kind="tw_minute", now=NOW)
    assert result["data_batches"] == 1 and result["seconds_per_acquired_partition"] is None
    assert result["pipeline_workers"] == 4 and result["acquisition_policy_version"] == 4
    second = {**selected, **batch(start + timedelta(seconds=600))}
    result = measured_capacity([serial, candidate, selected, second], kind="tw_minute", now=NOW)
    assert result["data_batches"] == 2 and result["data_partitions"] == 128
    assert result["seconds_per_acquired_partition"] == 300 / 128
    # Config changes must withhold the old engine before the first new batch.
    selected_identity = {key: selected[key] for key in (
        "acquisition_policy_version", "processing_engine", "pipeline_workers", "range_calendar_days")}
    missing = measured_capacity([serial, candidate, selected, second], kind="tw_minute", now=NOW,
                                configuration={**selected_identity, "pipeline_workers": 8})
    assert missing["data_batches"] == 0 and missing["seconds_per_acquired_partition"] is None


def test_minute_eta_does_not_mix_authorization_or_queue_or_prefetch_settings():
    start = NOW - timedelta(hours=2)
    selected = {**batch(start), "kind": "tw_minute", "acquisition_policy_version": 6,
                "processing_engine": "rolling_range_process_pool", "pipeline_workers": 32,
                "range_calendar_days": 31, "http_keepalive": True, "download_threads": 1,
                "queue_depth": 4, "authorization_slots": 16,
                "quota_policy": {"configuration_fingerprint": "fixture-current"}}
    following = {**selected, **batch(start+timedelta(seconds=200))}
    identity = {key: selected[key] for key in (
        "acquisition_policy_version", "processing_engine", "pipeline_workers", "range_calendar_days",
        "http_keepalive", "download_threads", "queue_depth", "authorization_slots")}
    identity["configuration_fingerprint"] = "fixture-current"
    assert measured_capacity([selected, following], kind="tw_minute", now=NOW,
                             configuration=identity)["data_batches"] == 2
    for key, different in (("download_threads", 5), ("queue_depth", 1), ("authorization_slots", 8),
                           ("configuration_fingerprint", "fixture-older-policy")):
        result = measured_capacity([selected, following], kind="tw_minute", now=NOW,
                                   configuration={**identity, key: different})
        assert result["data_batches"] == 0 and result["seconds_per_acquired_partition"] is None


def test_tick_eta_waits_for_general_debt_even_after_minutes_are_cleared():
    market, kwargs = inputs()
    market.update(acquisition_mode="minute_first_tick_residual", minute_priority={"tick_residual_allowed": True},
                  general_acquisition_admission={"supplemental_allowed": False})
    result = estimate_tick_progress(market, **kwargs)
    assert result["state"] == "waiting_required_general_data"
    assert all(row["finish_at_utc"] is None for row in result["phases"])


def test_general_budget_subtracts_both_intraday_families_without_counting_minute_as_general():
    prior = datetime(2026, 10, 6, 8, tzinfo=UTC)
    rows = [batch(prior, quota=100000), {**batch(prior+timedelta(minutes=10), quota=50000), "kind": "tw_minute"}]
    result = general_quota_cycles(samples(), rows, now=NOW)[0]
    assert result["general_bytes_proxy"] == 1000 * 1024**2 - 150000
    assert result["tick_worker_bytes_proxy"] == 100000
    assert result["intraday_worker_bytes_proxy"] == 150000


def test_capacity_uses_aggregate_whole_batches_and_empty_queries_never_accelerate_downloads():
    start = NOW - timedelta(hours=2)
    rows = [batch(start, acquired=64, seconds=100),
            batch(start + timedelta(seconds=200), acquired=128, seconds=400),
            batch(start + timedelta(seconds=700), acquired=0, seconds=1),
            batch(start + timedelta(seconds=800), acquired=0, seconds=1)]
    info = measured_capacity(rows, now=NOW)
    assert info["data_batches"] == 2
    assert info["seconds_per_acquired_partition"] == (100+60+400+100) / 192
    assert info["discovery_batches"] == 2
    assert info["account_bytes_per_acquired_partition_proxy"] == 128000 / 192
    # A requery of old receipts or interrupted batch provides no new capacity.
    rows.append({**batch(start + timedelta(seconds=900)), "new_receipted_partitions": 0})
    rows.append(batch(start + timedelta(seconds=1100), state="interrupted"))
    assert measured_capacity(rows, now=NOW)["data_partitions"] == 192


def test_estimated_rows_local_bytes_and_billed_bytes_have_distinct_grains_and_old_scope_is_queries():
    market, kwargs = inputs()
    info = estimate_tick_progress(market, **kwargs)
    latest, recent, older = info["phases"]
    assert latest["remaining_rows_estimate"] == 10000
    assert recent["remaining_rows_estimate"] == 9000
    assert recent["estimated_total_rows"] == 10000
    assert recent["remaining_local_bytes_estimate"] == 4500
    assert recent["remaining_transfer_bytes_estimate"] == 90000
    assert older["remaining_work_items"] == 70  # not 100 repeated empty checks
    assert older["remaining_rows_estimate"] is None
    assert older["remaining_transfer_bytes_estimate"] is None
    assert older["estimate_scope"] == "first_pass_candidate_queries"
    assert info["all_published_history_finish_at_utc"] is None
    assert latest["source_wait_seconds"] is None
    for name in ("fast", "reference", "slow"):
        assert recent["scenarios"][name]["finish_at_utc"] > latest["scenarios"][name]["finish_at_utc"]
        assert older["scenarios"][name]["finish_at_utc"] > recent["scenarios"][name]["finish_at_utc"]
    assert recent["scenarios"]["fast"]["finish_at_utc"] <= recent["finish_at_utc"]
    assert recent["finish_at_utc"] <= recent["scenarios"]["slow"]["finish_at_utc"]


@pytest.mark.parametrize("kind, expected", [("quota", "quota_unverified"), ("timer", "unscheduled"),
                                           ("samples", "insufficient_samples"), ("billing", "transfer_unmeasured")])
def test_unverified_or_stopped_estimates_do_not_show_a_false_completion_countdown(kind, expected):
    market, kwargs = inputs()
    if kind == "quota":
        kwargs["quota"]["observed_at_utc"] = (NOW - timedelta(minutes=4)).isoformat()
    elif kind == "timer":
        kwargs["timer_active"] = False
    elif kind == "samples":
        kwargs["batches"] = kwargs["batches"][:1]
    else:
        for row in kwargs["batches"]:
            row["account_delta_bytes"] = None
    recent = estimate_tick_progress(market, **kwargs)["phases"][1]
    assert recent["finish_at_utc"] is None
    assert recent["scenarios"]["reference"]["state"] == expected


def test_complete_candidate_scope_is_distinct_from_full_published_history():
    market, kwargs = inputs()
    for row in market["coverage_phases"]:
        row.update(candidate_partitions=0, receipted_partitions=0, untested_candidate_partitions=0,
                   provider_not_ready_partitions=0)
    info = estimate_tick_progress(market, **kwargs)
    assert all(row["scenarios"]["reference"]["state"] == "complete" for row in info["phases"])
    assert info["all_published_history_finish_at_utc"] is None


def test_daily_account_cycles_do_not_extrapolate_partial_days_or_add_overlapping_billing():
    prior = datetime(2026, 10, 6, 8, tzinfo=UTC)
    rows = [batch(prior, quota=100000)]
    partial = [{"observed_at_utc": NOW.isoformat(), "used_mb": 200, "limit_mb": 5000}]
    cycles = general_quota_cycles(samples() + partial, rows, now=NOW)
    assert len(cycles) == 1
    assert cycles[0]["general_bytes_proxy"] == 1000 * 1024**2 - 100000
    assert general_quota_cycles(samples()[1:], rows, now=NOW) == []


def test_quota_wait_uses_remaining_today_then_exact_daily_reset_and_no_double_counting():
    reset = NOW + timedelta(hours=1)
    result = _finish_work(start=NOW, now=NOW, work_seconds=100, transfer_bytes=1000,
                          reset=reset, first_room=100, daily_room=500, quota_cycles=2)
    assert result["finish_at_utc"] == (reset + timedelta(days=1, seconds=40)).isoformat()
    assert result["quota_resets"] == 2
    assert result["processing_seconds"] == 100
    assert result["remaining_seconds"] == 3600+86400+40


def test_opening_boundary_is_not_crossed_by_a_single_large_fluid_chunk():
    start = datetime(2026, 10, 8, 0, tzinfo=UTC)
    def opening(instant):
        left = instant.replace(hour=0, minute=7, second=30)
        right = instant.replace(hour=1, minute=10, second=0)
        return right if left <= instant < right else instant
    result = _finish_work(start=start, now=start, work_seconds=3600, transfer_bytes=0,
                          reset=start+timedelta(days=1), first_room=0, daily_room=1,
                          quota_cycles=1, opening_policy=opening)
    assert result["finish_at_utc"] == (start + timedelta(seconds=3600+3750)).isoformat()


def test_zero_future_quota_does_not_erase_usable_remaining_quota_today():
    args = dict(start=NOW, now=NOW, work_seconds=100, transfer_bytes=1000,
                reset=NOW+timedelta(hours=1), daily_room=0, quota_cycles=2)
    result = _finish_work(**args, first_room=1000)
    assert result["finish_at_utc"] == (NOW+timedelta(seconds=100)).isoformat()
    assert _finish_work(**args, first_room=500)["state"] == "capacity_constrained"


def test_empty_prior_stages_do_not_require_unused_download_or_billing_samples():
    market, kwargs = inputs()
    for row in market["coverage_phases"][:2]:
        row.update(candidate_partitions=0, receipted_partitions=0, untested_candidate_partitions=0)
    kwargs["batches"] = kwargs["batches"][2:]
    older = estimate_tick_progress(market, **kwargs)["phases"][2]
    assert older["scenarios"]["reference"]["state"] == "conditional"
    assert older["finish_at_utc"] is not None


def test_journal_calibration_requires_reconciled_new_receipts_and_same_cycle_quota():
    start = NOW - timedelta(minutes=5)
    snapshots = [(start-timedelta(seconds=20), 10), (start+timedelta(seconds=40), 20),
                 (start+timedelta(seconds=90), 40), (start+timedelta(seconds=150), 50)]
    def message(got, attempted, successful, end):
        return json.dumps({"acquisition_mode": "tick_api_only_minute_derived_locally",
                           "run_started_at_utc": start.isoformat(), "observed_at_utc": end.isoformat(),
                           "attempted_this_run": attempted, "successes_this_run": successful,
                           "state": "batch_limit", "by_kind": {"tw_tick": {"receipted_partitions": got}}})
    rows = reconstruct([message(100, 0, 0, start), message(164, 256, 64, start+timedelta(seconds=100))], snapshots)
    assert len(rows) == 1
    assert rows[0]["new_receipted_partitions"] == 64
    assert rows[0]["account_delta_bytes"] > 0
    assert interpolate_quota([(start, 100), (start+timedelta(seconds=60), 0)], start+timedelta(seconds=30)) is None


def test_bounded_reader_ignores_interrupted_and_overlapping_batches_and_new_owner_wins(tmp_path):
    folder = tmp_path / "intraday"
    folder.mkdir()
    start = NOW-timedelta(hours=2)
    seed = batch(start)
    (folder / "batch_metrics_seed.json").write_text(json.dumps({"contract_version": 1, "batches": [seed]}))
    owner = {**seed, "elapsed_seconds": 150}
    interrupted = batch(start+timedelta(minutes=5), state="interrupted")
    (folder / "batch_metrics.json").write_text(json.dumps({"contract_version": 1, "batches": [owner, interrupted]}))
    rows = load_batches(tmp_path)
    assert len(rows) == 1 and rows[0]["elapsed_seconds"] == 150
