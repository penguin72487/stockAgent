"""Conditional Tick workload estimates, never a promise of provider supply.

Standard-library only. Readers use bounded batch receipts, not raw Tick files,
the partition writer database, credentials or SDK calls. Acquisition, discovery,
local storage and account billing deliberately retain separate units.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
from statistics import fmean


TICK_ETA_VERSION = 1
BATCH_METRICS_VERSION = 1
MIB = 1024**2
PHASES = ("latest_session", "observed_history", "older_discovery")
ACCEPTED = {"downloaded_unverified_for_pit", "verified_closed_date", "verified_no_trade"}


def timestamp(value) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.astimezone(UTC) if result.tzinfo else None
    except (ValueError, TypeError):
        return None


def number(value) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
        return float(value)
    return None


def read_json(path: Path) -> dict:
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
        return result if isinstance(result, dict) else {}
    except (OSError, ValueError):
        return {}


def phase_for_day(day: str, *, latest: str, first_observed: str | None) -> str:
    return ("latest_session" if day == latest else
            "older_discovery" if first_observed is None or day < first_observed else "observed_history")


def load_batches(root: Path) -> list[dict]:
    """Bounded index plus one-time journal calibration; new owner records win."""
    by_id = {}
    for name in ("batch_metrics_seed.json", "batch_metrics.json"):
        payload = read_json(root / "intraday" / name)
        if payload.get("contract_version") != BATCH_METRICS_VERSION:
            continue
        for row in payload.get("batches", [])[-256:]:
            if not isinstance(row, dict) or row.get("contract_version") != BATCH_METRICS_VERSION:
                continue
            started, finished = timestamp(row.get("started_at_utc")), timestamp(row.get("finished_at_utc"))
            attempts, completed, seconds = (number(row.get(k)) for k in
                                            ("attempted", "completed_attempts", "elapsed_seconds"))
            if (started and finished and finished >= started and seconds is not None and seconds > 0
                    and completed is not None and attempts == completed and completed > 0
                    and row.get("state") not in {"running", "interrupted"}):
                by_id[started] = row
    return [by_id[key] for key in sorted(by_id)[-256:]]


def measured_capacity(batches: list[dict], *, now: datetime, kind: str = "tw_tick",
                      configuration: dict | None = None) -> dict:
    """Aggregate whole batches, including dispatch/ordinary-work gaps.

    Empty-response batches cannot accelerate the data-acquisition estimate.
    Same-partition rechecks cannot masquerade as newly acquired partitions.
    Recent data-bearing full batches calibrate acquisition; empty full batches
    calibrate only candidate queries. No average of per-batch rates.
    """
    valid = []
    previous_end = None
    # A new acquisition engine must earn its own whole-batch calibration.
    # Do not mix sequential day requests with parallel official range queries.
    family = [row for row in batches if row.get("kind", "tw_tick") == kind]
    versions = [row.get("acquisition_policy_version") for row in family
                if type(row.get("acquisition_policy_version")) is int]
    current_version = (configuration.get("acquisition_policy_version") if configuration is not None
                       else max(versions) if versions else None)
    current_family = [row for row in family if current_version is None
                      or row.get("acquisition_policy_version") == current_version]
    engine_fields = ("processing_engine", "pipeline_workers", "range_calendar_days", "http_keepalive",
                     "download_threads", "queue_depth", "authorization_slots", "configuration_fingerprint")
    latest = max(current_family, key=lambda row: timestamp(row.get("finished_at_utc"))
                 or datetime.min.replace(tzinfo=UTC), default={})
    def identity(row):
        policy = row.get("quota_policy")
        policy = policy if isinstance(policy, dict) else {}
        return tuple((row.get(key, policy.get(key)) if key == "configuration_fingerprint"
                      else row.get(key, row.get("pipeline_workers", 1) if key == "authorization_slots"
                                   else 1 if key in {"download_threads", "queue_depth"} else None))
                     for key in engine_fields)
    engine = identity(configuration if configuration is not None else latest)
    parallel = engine[0] in {"bounded_range_process_pool", "rolling_range_process_pool"}
    for row in batches:
        # Legacy receipts were Tick-only. Direct minute batches must never
        # inherit the much larger Tick object's throughput or billing cost.
        if row.get("kind", "tw_tick") != kind:
            continue
        if current_version is not None and row.get("acquisition_policy_version") != current_version:
            continue
        if identity(row) != engine:
            continue
        start, end = timestamp(row.get("started_at_utc")), timestamp(row.get("finished_at_utc"))
        if not start or not end or not timedelta(0) <= now - end <= timedelta(days=7):
            continue
        if previous_end and start < previous_end:  # overlapping/duplicate work cannot add capacity
            continue
        elapsed = number(row.get("elapsed_seconds"))
        if not elapsed or row.get("completed_attempts") != row.get("attempted"):
            continue
        if parallel:
            # Consecutive passes share one account owner; a minute timer gap
            # after every pass would invent time that was never waited.
            gap = max(0.0, (start - previous_end).total_seconds()) if previous_end else 0.0
        else:
            gap = max(60.0, (start - previous_end).total_seconds()) if previous_end else 60.0
        previous_end = end
        # Quota/long offline waits belong to the daily model, not per-batch cost.
        if gap > 3600:
            gap = 0.0 if parallel else 60.0
        if row.get("state") != "batch_limit" or row.get("attempted", 0) < row.get("attempt_limit", 256):
            continue
        valid.append({**row, "wall_seconds": elapsed + gap})
    data = [row for row in valid if row.get("new_receipted_partitions", 0) >= row["attempted"] / 4]
    empty = [row for row in valid if row.get("successful", 0) == 0]
    received = sum(row.get("new_receipted_partitions", 0) for row in data)
    attempts = sum(row["attempted"] for row in empty)
    quota_samples = [row for row in data if (number(row.get("account_delta_bytes")) is not None
                                             and row.get("quota_sample_successes", 0) > 0)]
    billed_received = sum(row["quota_sample_successes"] for row in quota_samples)
    billed_bytes = sum(row["account_delta_bytes"] for row in quota_samples)
    return {
        "data_batches": len(data), "data_partitions": received,
        "data_wall_seconds": sum(row["wall_seconds"] for row in data),
        "seconds_per_acquired_partition": sum(row["wall_seconds"] for row in data) / received
            if len(data) >= 2 and received else None,
        "discovery_batches": len(empty), "discovery_attempts": attempts,
        "discovery_wall_seconds": sum(row["wall_seconds"] for row in empty),
        "seconds_per_candidate_query": sum(row["wall_seconds"] for row in empty) / attempts
            if len(empty) >= 2 and attempts else None,
        "quota_sample_partitions": billed_received,
        "account_bytes_per_acquired_partition_proxy": billed_bytes / billed_received
            if len(quota_samples) >= 2 and billed_received and billed_bytes > 0 else None,
        "latest_batch_at_utc": valid[-1]["finished_at_utc"] if valid else None,
        "basis": "sum_completed_full_batch_wall_and_dispatch_gaps_over_new_receipts_or_queries",
        "kind": kind,
        "acquisition_policy_version": current_version,
        **dict(zip(engine_fields, engine)),
        "billing_basis": "shared_account_delta_during_exclusive_intraday_worker_proxy_not_local_parquet_bytes",
    }


def general_quota_cycles(samples: list[dict], batches: list[dict], *, now: datetime) -> list[dict]:
    """Observed prior complete 08:00 Taipei cycles, less Tick-worker consumption.

    Minute interpolation/other account clients can affect attribution: this is
    a residual-budget scenario, not provider per-dataset billing verification.
    Partial cycles are never extrapolated to 24 hours.
    """
    cycles = {}
    for item in samples:
        at, used, limit = timestamp(item.get("observed_at_utc")), number(item.get("used_mb")), number(item.get("limit_mb"))
        if not at or used is None or not limit or at >= now or used > limit:
            continue
        cycles.setdefault(at.date(), []).append((at, used, limit))
    result = []
    for day, rows in sorted(cycles.items()):
        start = datetime.combine(day, datetime.min.time(), UTC)
        rows.sort()
        if (day >= now.date() or (rows[0][0] - start).total_seconds() > 120
                or (start + timedelta(days=1) - rows[-1][0]).total_seconds() > 300):
            continue
        intraday = sum(number(row.get("account_delta_bytes")) or 0 for row in batches
                   if timestamp(row.get("started_at_utc")) and
                   start <= timestamp(row["started_at_utc"]) < start + timedelta(days=1)
                   and timestamp(row.get("finished_at_utc")) and
                   timestamp(row["finished_at_utc"]) < start + timedelta(days=1))
        used = max(row[1] for row in rows) * MIB
        result.append({"cycle_date": day.isoformat(), "account_used_bytes": round(used),
                       "tick_worker_bytes_proxy": round(sum(number(row.get("account_delta_bytes")) or 0 for row in batches
                           if row.get("kind", "tw_tick") == "tw_tick" and timestamp(row.get("started_at_utc"))
                           and start <= timestamp(row["started_at_utc"]) < start + timedelta(days=1)
                           and timestamp(row.get("finished_at_utc")) and timestamp(row["finished_at_utc"]) < start + timedelta(days=1))),
                       "intraday_worker_bytes_proxy": round(intraday),
                       "general_bytes_proxy": round(max(0, used - intraday))})
    return result[-3:]


def _finish_work(*, start: datetime, work_seconds: float, transfer_bytes: float,
                 now: datetime, reset: datetime, first_room: float, daily_room: float,
                 quota_cycles: int, opening_policy=None) -> dict:
    """Fluid workload, with daily byte bottleneck and optional opening calendar.

    O(elapsed days), not millions of candidate requests. Per-batch costs already
    contain normal timer/general-work gaps. Bytes and seconds progress together;
    quota waits are not added twice to CPU/network work.
    """
    cursor, remaining, resets, wait = start, 1.0, 0, 0.0
    work = max(0.0, work_seconds)
    total = max(0.0, transfer_bytes)
    for _ in range(3660):
        while cursor >= reset:
            reset += timedelta(days=1)
            first_room = daily_room
            resets += 1
        if total > 0 and first_room <= 0 and daily_room <= 0:
            return {"state": "capacity_constrained", "finish_at_utc": None,
                    "processing_seconds": round(work_seconds, 1)}
        if opening_policy:
            shifted = opening_policy(cursor)
            if shifted > cursor:
                wait += (shifted - cursor).total_seconds()
                cursor = shifted
                continue
        # Stop at the start of a calendar-protected window, not only when a
        # new day begins. The policy returns that day's verified/fallback gate.
        boundary = reset
        if opening_policy:
            opening = cursor.replace(hour=0, minute=7, second=30, microsecond=0)
            gate = opening_policy(opening)
            if gate > opening and cursor < opening < boundary:
                boundary = opening
        available_time = max(0.0, (boundary - cursor).total_seconds())
        time_fraction = available_time / work if work > 0 else remaining
        byte_fraction = first_room / total if total > 0 else remaining
        take = max(0.0, min(remaining, time_fraction, byte_fraction))
        cursor += timedelta(seconds=take * work)
        first_room = max(0.0, first_room - take * total)
        remaining = max(0.0, remaining - take)
        if remaining < 1e-10:
            return {"state": "conditional", "finish_at_utc": cursor.isoformat(),
                    "remaining_seconds": round((cursor - now).total_seconds(), 1),
                    "processing_seconds": round(work_seconds, 1),
                    "quota_wait_seconds": round(wait, 1), "quota_resets": resets,
                    "general_quota_cycle_samples": quota_cycles}
        if byte_fraction <= take + 1e-12:
            wait += max(0.0, (reset - cursor).total_seconds())
            cursor = reset
        else:
            cursor = boundary
    return {"state": "horizon_exceeded", "finish_at_utc": None,
            "processing_seconds": round(work_seconds, 1), "horizon_days": 3660}


def estimate_tick_progress(market: dict, *, batches: list[dict], quota: dict, workload: dict,
                           quota_history: list[dict], now: datetime, timer_active: bool,
                           reserve_mb: float = 50.0, opening_policy=None,
                           kind: str = "tw_tick", measurement_configuration: dict | None = None) -> dict:
    if kind not in {"tw_tick", "tw_minute"}:
        raise ValueError("unsupported intraday ETA family")
    measured = measured_capacity(batches, now=now, kind=kind, configuration=measurement_configuration)
    cycles = general_quota_cycles(quota_history, batches, now=now)
    tick = market.get("by_kind", {}).get(kind, {})
    got = number(tick.get("receipted_partitions")) or 0
    rows_per = (number(tick.get("rows")) or 0) / got if got else None
    local_per = (number(tick.get("parquet_bytes")) or 0) / got if got else None
    quota_at, reset = timestamp(quota.get("observed_at_utc")), timestamp(quota.get("reset_at_utc"))
    limit, room = number(quota.get("limit_mb")), number(quota.get("remaining_mb"))
    quota_valid = (quota_at is not None and timedelta(0) <= now - quota_at <= timedelta(minutes=3)
                   and reset is not None and reset > now and limit is not None and limit > 0
                   and room is not None and room <= limit)
    generals = [row["general_bytes_proxy"] for row in cycles]
    general_pending = workload.get("scenarios", {}).get("reference", {})
    general_finish = timestamp(general_pending.get("finish_at_utc"))
    start = max(now + timedelta(seconds=60), general_finish or now)
    result = {
        "contract_version": TICK_ETA_VERSION, "generated_at_utc": now.isoformat(),
        "kind": kind, "acquisition_policy_version": market.get("acquisition_policy_version", 1),
        "inventory_at_utc": market.get("observed_at_utc"), "state": "conditional",
        "measurement": measured, "general_quota_cycles": cycles,
        "candidate_scope_fixed_through": market.get("target_end_date"),
        "source_wait_seconds": None, "all_published_history_finish_at_utc": None,
        "source_wait_reason": "來源仍補歷史；未發布等待不可由下載速度預測。候選不是已發布清冊。",
        "rows_per_received_partition_proxy": rows_per,
        "local_bytes_per_received_partition_proxy": local_per,
        "assumptions": {
            "fixed_candidate_scope": True, "future_new_sessions_included": False,
            "recent_candidates_assumed_available": True, "older_estimate_is_first_pass_queries_only": True,
            "minute_derivation_included_in_batch_wall": kind == "tw_tick",
            "scenario_factors": {"fast": 0.75, "reference": 1.0, "slow": 1.5},
            "range_is_sensitivity_not_confidence_interval": True,
            "acquisition_cost_includes_current_discovery_mix": True,
            "older_full_query_capacity_after_recent_stages": True,
            "general_budget_basis": "prior_complete_account_cycles_less_tick_worker_proxy",
            "inventory_sample_bias": "已收到的股票日均值外推；未抓股票與年代的成交密度可能不同",
        },
        "phases": [],
    }
    phase_rows = {row.get("id"): row for row in market.get(
        "minute_coverage_phases" if kind == "tw_minute" else "coverage_phases", []) if isinstance(row, dict)}
    priority_wait = (kind == "tw_tick" and market.get("acquisition_mode") == "minute_first_tick_residual"
                     and market.get("minute_priority", {}).get("tick_residual_allowed") is not True)
    general_wait = (kind == "tw_tick" and market.get("acquisition_mode") == "minute_first_tick_residual"
                    and market.get("general_acquisition_admission", {}).get("supplemental_allowed") is False)
    if priority_wait:
        result["state"] = "waiting_higher_priority_minutes"
    elif general_wait:
        result["state"] = "waiting_required_general_data"
    cumulative_work = cumulative_bytes = 0.0
    for identity in PHASES:
        row = phase_rows.get(identity, {})
        remaining = max(0, (number(row.get("candidate_partitions")) or 0) -
                        (number(row.get("receipted_partitions")) or 0))
        discovery = identity == "older_discovery"
        work_items = (number(row.get("untested_candidate_partitions")) or 0) if discovery else remaining
        cost = measured.get("seconds_per_candidate_query" if discovery else "seconds_per_acquired_partition")
        transfer_per = None if discovery else measured.get("account_bytes_per_acquired_partition_proxy")
        seconds = 0.0 if work_items == 0 else work_items * cost if cost is not None else None
        traffic = 0.0 if work_items == 0 else work_items * transfer_per if transfer_per is not None else None
        public = {**row, "estimate_scope": "first_pass_candidate_queries" if discovery else "acquisition_if_published",
                  "remaining_work_items": round(work_items), "remaining_work_seconds_estimate": seconds,
                  "remaining_rows_estimate": round(work_items * rows_per) if not discovery and rows_per is not None else None,
                  "remaining_local_bytes_estimate": round(work_items * local_per) if not discovery and local_per is not None else None,
                  "remaining_transfer_bytes_estimate": round(traffic) if traffic is not None else None,
                  "estimated_total_rows": round((number(row.get("rows")) or 0) + work_items * rows_per)
                      if not discovery and rows_per is not None else None,
                  "scenarios": {}}
        previous_known = cumulative_work is not None
        cumulative_work = cumulative_work + seconds if cumulative_work is not None and seconds is not None else None
        cumulative_bytes = cumulative_bytes + (traffic or 0) if cumulative_bytes is not None and (discovery or traffic is not None) else None
        for name, factor in result["assumptions"]["scenario_factors"].items():
            budget_general = (min(generals) if name == "fast" else max(generals) if name == "slow"
                              else fmean(generals)) if generals else None
            daily = max(0.0, limit * MIB - budget_general - 50 * MIB) if limit and budget_general is not None else None
            first_room = max(0.0, (room - reserve_mb) * MIB) if room is not None else None
            assumptions = {"duration_and_size_factor": factor, "general_daily_bytes_proxy": budget_general,
                           "tick_daily_budget_bytes_estimate": daily, "tick_remaining_today_bytes_estimate": first_room}
            state = ("unscheduled" if not timer_active else "waiting_higher_priority_minutes" if priority_wait
                     else "waiting_required_general_data" if general_wait
                     else "bounded_source_discovery" if kind == "tw_minute" and discovery
                     else "insufficient_samples" if cumulative_work is None
                     else "quota_unverified" if not quota_valid else
                     "general_budget_unmeasured" if daily is None else
                     "transfer_unmeasured" if cumulative_bytes is None else None)
            if work_items == 0 and (discovery or remaining == 0):
                scenario = {"state": "complete", "finish_at_utc": None, "processing_seconds": 0}
            elif state:
                scenario = {"state": state, "finish_at_utc": None,
                            "processing_seconds": seconds * factor if seconds is not None else None}
            else:
                scenario = _finish_work(start=start, work_seconds=cumulative_work * factor,
                                        transfer_bytes=cumulative_bytes * factor, now=now, reset=reset,
                                        first_room=first_room, daily_room=daily,
                                        quota_cycles=len(cycles), opening_policy=opening_policy)
            public["scenarios"][name] = {**scenario, **assumptions}
        public["finish_at_utc"] = public["scenarios"]["reference"].get("finish_at_utc")
        public["source_wait_seconds"] = None if row.get("provider_not_ready_partitions", 0) or work_items else 0
        public["scenario_sequence_includes_previous_phases"] = previous_known and identity != "latest_session"
        result["phases"].append(public)
    return result
