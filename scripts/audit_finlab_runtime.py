#!/usr/bin/env python3
"""Bounded, local-only FinLab audit; never authenticates or downloads.

Reconcile the canonical workload with its dataset rows, retain current service
identities, and time the existing public projection. Receipts remain read-only.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import sys
import tempfile
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.download_finlab_history import _atomic_json  # noqa: E402
from scripts.finlab_workload import read_json  # noqa: E402
from stockagent.data.finlab_quota_policy import load_policy  # noqa: E402
from stockagent.live.finlab_dashboard import build_finlab_public_status  # noqa: E402

UNITS = (
    "stockagent-finlab-local-refresh.service", "stockagent-finlab-local-refresh.timer",
    "stockagent-finlab-quota-snapshot.timer", "stockagent-public-dashboards.service",
    "stockagent-finmind-free.service", "stockagent-finmind-complement.service",
    "stockagent-finmind-sponsor.service", "stockagent-shioaji-taifex-bidask.service",
    "stockagent-tw-day-trade-simulation.service", "stockagent-tw-overnight-simulation.service",
)


def _interval_seconds(intervals: list[tuple[datetime, datetime]]) -> float:
    """Union owner intervals; overlapping workers/runs never add wall time."""
    total = 0.0
    frontier = None
    for start, finish in sorted(intervals):
        if frontier is None or start >= frontier:
            total += (finish - start).total_seconds()
        elif finish > frontier:
            total += (finish - frontier).total_seconds()
        frontier = max(finish, frontier) if frontier else finish
    return total


def efficiency_history(root: Path, day: date, *, through: datetime,
                       include_service_journal: bool = True) -> dict:
    """Read all immutable batches, not the 256-row dashboard calibration tail.

    A local calendar day and a provider quota cycle are deliberately separate.
    No SDK call, arbitrary error text, credential or signed URL is retained.
    """
    local_zone = ZoneInfo("Asia/Taipei")
    if through.tzinfo is None:
        raise ValueError("efficiency history requires an aware observation time")
    through = through.astimezone(UTC)
    start = datetime.combine(day, datetime.min.time(), local_zone).astimezone(UTC)
    stop = min(start + timedelta(days=1), through)
    if stop <= start:
        raise ValueError("efficiency history requires an elapsed Taipei date")
    scalar_fields = ("kind", "state", "acquisition_policy_version", "attempt_limit", "attempted",
        "completed_attempts", "new_receipted_partitions", "elapsed_seconds", "sdk_calls", "window_requests",
        "http_gets", "http_posts", "http_errors", "http_429s", "http_503s", "http_conflicts",
        "http_conflict_retries", "http_download_bytes", "source_seconds", "persist_seconds",
        "worker_slot_seconds", "pipeline_workers", "range_calendar_days", "processing_engine")
    batches, invalid, crossing = [], [], []
    for path in sorted((root / "data_finlab/intraday/batch_receipts").glob("*.json")):
        raw = read_json(path)
        try:
            left, right = (datetime.fromisoformat(raw[name]) for name in ("started_at_utc", "finished_at_utc"))
            if left.tzinfo is None or right.tzinfo is None:
                raise ValueError("naive source clock")
            left, right = left.astimezone(UTC), right.astimezone(UTC)
            if right < left:
                raise ValueError("negative duration")
        except (ValueError, KeyError, TypeError):
            invalid.append(path.name)
            continue
        if right <= start or left >= stop:
            continue
        if left < start or right > stop:
            crossing.append({"receipt": str(path.relative_to(root)), "started_at_utc": left.isoformat(),
                             "finished_at_utc": right.isoformat(), "counts_excluded": True})
            continue  # Counts cannot be proportionally assigned to a partial run.
        row = {name: raw.get(name) for name in scalar_fields}
        row.update(receipt=str(path.relative_to(root)), started_at_utc=left.isoformat(),
                   finished_at_utc=right.isoformat(), measured_owner_seconds=(right-left).total_seconds())
        batches.append(row)
    states = []
    for state in sorted({row["state"] for row in batches}):
        group = [row for row in batches if row["state"] == state]
        seconds = sum(row["measured_owner_seconds"] for row in group)
        acquired = sum(row.get("new_receipted_partitions") or 0 for row in group)
        states.append({"state": state, "batches": len(group), "new_receipted_partitions": acquired,
                       "attempted": sum(row.get("attempted") or 0 for row in group),
                       "owner_seconds": seconds,
                       "acquired_stock_days_per_active_hour": acquired / seconds * 3600 if seconds else None})
    intervals = [(datetime.fromisoformat(row["started_at_utc"]), datetime.fromisoformat(row["finished_at_utc"]))
                 for row in batches]
    active = _interval_seconds(intervals)
    duration = (stop-start).total_seconds()
    quota_path = root / "artifacts/live/finlab/quota_history.sqlite3"
    quota = []
    if quota_path.is_file():
        # Include both complete quota cycles intersecting this calendar day.
        from stockagent.data.finlab_acquisition_contract import quota_cycle_start
        cycle_cutoff = min(through, quota_cycle_start(stop-timedelta(microseconds=1))+timedelta(days=1))
        with sqlite3.connect(quota_path.resolve().as_uri()+"?mode=ro", uri=True, timeout=5) as db:
            samples = db.execute("SELECT observed_at_utc,used_mb,limit_mb FROM quota_samples "
                "WHERE observed_at_utc>=? AND observed_at_utc<? ORDER BY observed_at_utc",
                (quota_cycle_start(start).isoformat(), cycle_cutoff.isoformat())).fetchall()
        grouped = {}
        for stamp, used, limit in samples:
            instant = datetime.fromisoformat(stamp)
            cycle = quota_cycle_start(instant).isoformat()
            grouped.setdefault(cycle, []).append((stamp, used, limit))
        for cycle, group in sorted(grouped.items()):
            finish = datetime.fromisoformat(cycle) + timedelta(days=1)
            quota.append({"cycle_start_utc": cycle, "reset_at_utc": finish.isoformat(),
                "samples": len(group), "first_observed_at_utc": group[0][0],
                "last_observed_at_utc": group[-1][0], "last_used_mb": group[-1][1],
                "last_limit_mb": group[-1][2], "quota_used_ratio": group[-1][1]/group[-1][2] if group[-1][2] > 0 else None,
                "closed_cycle": finish <= through, "sample_tail_seconds": (finish-datetime.fromisoformat(group[-1][0])).total_seconds(),
                "basis": "shared_account_native_status; not_exclusive_minute_transfer"})
    general = []
    journal_available = False
    if include_service_journal:
        result = subprocess.run(["journalctl", "-u", UNITS[0], "--since", start.isoformat(),
            "--until", stop.isoformat(), "--no-pager", "-o", "cat"],
            capture_output=True, text=True, timeout=30)
        journal_available = result.returncode == 0
        for line in result.stdout.splitlines() if journal_available else []:
            if not line.startswith("finlab_sync="):
                continue
            try:
                row = json.loads(line.split("=", 1)[1])
                left, right = (datetime.fromisoformat(row[name]) for name in ("started_at_utc", "finished_at_utc"))
                if left < start or right > stop or right < left:
                    continue
            except (ValueError, KeyError, TypeError):
                continue
            general.append({name: row.get(name) for name in ("started_at_utc", "finished_at_utc", "state",
                "attempted", "downloaded", "unchanged", "provider_errors", "pending_before", "pending_after_estimate")}
                | {"owner_seconds": (right-left).total_seconds()})
    acquired = sum(row.get("new_receipted_partitions") or 0 for row in batches)
    general_attempts = sum(row.get("attempted") or 0 for row in general)
    return {"contract_version": 1, "timezone": "Asia/Taipei", "calendar_date": day.isoformat(),
        "window_start_utc": start.isoformat(), "window_end_utc": stop.isoformat(), "provider_calls": 0,
        "batch_receipts": batches, "invalid_receipts": invalid, "boundary_crossing_receipts": crossing,
        "intraday": {"batches": len(batches), "new_receipted_partitions": acquired,
            "active_owner_union_seconds": active, "window_seconds": duration,
            "outside_recorded_intraday_seconds": max(0, duration-active),
            "calendar_hour_acquired_stock_days": acquired/duration*3600,
            "active_hour_acquired_stock_days": acquired/active*3600 if active else None,
            "states": states,
            "outside_time_reason": "general_work/catalog/overlay/timer/diagnostics_or_unknown; not_all_idle"},
        "quota_cycles": quota, "general_journal_available": journal_available,
        "general": {"owner_batches": len(general), "attempted": general_attempts,
            "unchanged": sum(row.get("unchanged") or 0 for row in general),
            "downloaded": sum(row.get("downloaded") or 0 for row in general),
            "provider_errors": sum(row.get("provider_errors") or 0 for row in general),
            "owner_seconds": sum(row["owner_seconds"] for row in general), "runs": general},
        "overlapping_worker_times_are_not_wall_time": True, "all_history_complete_claim": False}


def measure_pipeline(baseline: list[dict], current: list[dict]) -> dict:
    """Whole accepted owner work, not summed overlapping process timings.

    The cohorts differ: this is an operational before/after observation, not
    a matched-source speedup benchmark. SDK calls are not HTTP object counts.
    """
    def summarize(rows):
        accepted = [row for row in rows
                    if row.get("kind") == "tw_minute"
                    and row.get("state") == "batch_limit"
                    and row.get("attempted", 0) > 0
                    and row["attempted"] >= row.get("attempt_limit", row["attempted"])
                    and row.get("completed_attempts") == row.get("attempted")
                    and row.get("new_receipted_partitions", 0) >= row["attempted"] / 4
                    and row.get("elapsed_seconds", 0) > 0][-2:]
        seconds = sum(row["elapsed_seconds"] for row in accepted)
        acquired = sum(row["new_receipted_partitions"] for row in accepted)
        attempts = sum(row["attempted"] for row in accepted)
        sdk_calls = sum(row.get("sdk_calls") if type(row.get("sdk_calls")) is int else row["attempted"]
                        for row in accepted)
        return {"batches": len(accepted), "new_receipted_partitions": acquired,
                "completed_stock_day_checks": attempts, "sdk_calls": sdk_calls,
                "whole_owner_seconds": seconds,
                "acquired_stock_days_per_hour": acquired / seconds * 3600 if seconds else None,
                "sdk_calls_per_acquired_stock_day": sdk_calls / acquired if acquired else None,
                "started_at_utc": accepted[0]["started_at_utc"] if accepted else None,
                "finished_at_utc": accepted[-1]["finished_at_utc"] if accepted else None}

    old, new = summarize(baseline), summarize(current)
    return {"contract_version": 2, "before": old, "after": new,
            "observed_rate_ratio": new["acquired_stock_days_per_hour"] / old["acquired_stock_days_per_hour"]
                if old["acquired_stock_days_per_hour"] and new["acquired_stock_days_per_hour"] else None,
            "scope": "last_two_completed_full_data_bearing_owner_batches_per_cohort",
            "matched_source_benchmark": False, "official_maximum_throughput_proven": False,
            "sdk_call_basis": "data.get_invocations_not_manifest_signing_and_object_HTTP_total"}


def write_inventory(path: Path, public: dict, workload: dict) -> None:
    """Export receipt-backed grains; a source date is not a release timestamp."""
    catalog = {row["key"]: row for row in public["datasets"]}
    fields = ("key", "category", "feature_group", "feature_group_label", "importance", "collection_mode",
              "stage", "state", "downloaded", "needs_refresh", "blocked_reason",
              "first_observed", "last_observed", "stored_rows", "record_unit", "record_count",
              "local_parquet_bytes", "source_weight_bytes", "expected_next_payload_bytes",
              "source_checked_at_utc", "next_source_check_at_utc", "retry_at_utc", "time_basis")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8-sig", newline="", dir=path.parent,
                                     prefix=".finlab-inventory-", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in sorted(workload["datasets"], key=lambda item: item["key"]):
            source = catalog.get(row["key"], {})
            writer.writerow({**{key: row.get(key) for key in fields},
                             **{key: source.get(key) for key in (
                                 "feature_group", "feature_group_label", "importance", "collection_mode")},
                             "stage": row.get("queue_role"),
                             "state": source.get("state"), "first_observed": source.get("first"),
                             "last_observed": source.get("last"), "source_weight_bytes": row.get("transfer_bytes"),
                             "expected_next_payload_bytes": row.get("expected_payload_bytes")})
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def verify_recent_partitions(root: Path, count: int) -> list[dict]:
    """Check bounded immutable source objects, not the entire history."""
    source = root / "data_finlab"
    index = source / "intraday/market_index.sqlite3"
    with sqlite3.connect(index.resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as db:
        rows = db.execute("SELECT symbol,trade_date,rows,parquet_bytes,checked_at_utc FROM partitions "
                          "WHERE kind='tw_minute' AND status='downloaded_unverified_for_pit' "
                          "ORDER BY checked_at_utc DESC LIMIT ?", (count,)).fetchall()
    return _verify_partition_rows(source, rows)


def verify_partition_windows(root: Path, windows: list[tuple[str, str, str]]) -> list[dict]:
    """Read-only, bounded verification of already inventoried error ranges."""
    source = root / "data_finlab"
    index = source / "intraday/market_index.sqlite3"
    rows = {}
    with sqlite3.connect(index.resolve().as_uri()+"?mode=ro", uri=True, timeout=5) as db:
        for symbol, begin, end in windows:
            if date.fromisoformat(end) < date.fromisoformat(begin):
                raise ValueError("invalid FinLab verification window")
            found = db.execute("SELECT symbol,trade_date,rows,parquet_bytes,checked_at_utc FROM partitions "
                "WHERE kind='tw_minute' AND symbol=? AND trade_date BETWEEN ? AND ? ORDER BY trade_date",
                (symbol, begin, end)).fetchall()
            if not found or len(found) > 366:
                raise ValueError("verification window absent or exceeds bounded scope")
            for row in found:
                rows[(row[0], row[1])] = row
    return _verify_partition_rows(source, list(rows.values()))


def _verify_partition_rows(source: Path, rows: list) -> list[dict]:
    import polars as pl
    from scripts.download_finlab_intraday import (
        _file_sha256, _partition_paths, _stored_receipt, _validate_frame,
    )

    result = []
    for symbol, day_text, expected_rows, expected_bytes, checked in rows:
        key, day = f"tw_minute:{symbol}", date.fromisoformat(day_text)
        receipt = _stored_receipt(_partition_paths(source, key, day)[0], source, key, day)
        checks = {"valid_receipt": receipt is not None}
        if receipt:
            path = source / receipt["parquet_path"]
            frame = pl.read_parquet(path)
            checks.update(
                sha256=_file_sha256(path) == receipt["sha256"],
                rows=len(frame) == expected_rows == receipt["rows"],
                bytes=path.stat().st_size == expected_bytes == receipt["parquet_size_bytes"],
                source_semantics=_validate_frame(frame, key, day) == receipt["status"],
            )
        result.append({"dataset": key, "trade_date": day_text, "checked_at_utc": checked,
                       "checks": checks, "accepted": all(checks.values())})
    return result


def audit(root: Path, *, baseline: Path | None = None, inventory: Path | None = None,
          recent_partitions: int = 0, source_windows: list | None = None) -> dict:
    now = datetime.now(UTC)
    quota = read_json(root / "artifacts/live/finlab/quota_latest.json")
    workload = read_json(root / "artifacts/live/finlab/workload_latest.json")
    rows = workload.get("datasets", [])
    started = time.perf_counter()
    public = build_finlab_public_status(root, now=now)
    elapsed = time.perf_counter() - started
    if inventory:
        write_inventory(inventory, public, workload)
    checks = {
        "unique_workload_keys": len({r["key"] for r in rows}) == len(rows),
        "general_keys_reconciled": workload.get("general_keys") == len(rows),
        "pending_reconciled": workload.get("refresh_pending") == sum(
            r["needs_refresh"] and not r["blocked_reason"] for r in rows),
        "blocked_reconciled": set(workload.get("blocked_keys", [])) == {
            r["key"] for r in rows if r["blocked_reason"]},
        "stages_reconciled": all(s["total_keys"] == sum(r["queue_role"] == s["id"] for r in rows)
                                 for s in workload.get("stages", []) if s["id"] != "tick"),
        "public_read_only": public.get("read_only") is True and public.get("production_control_possible") is False,
        "history_mode_counts_reconciled": public["history_inventory"]["general_total"] == len(rows),
        "history_groups_reconciled": sum(item["total_keys"] for item in public["history_inventory"]["groups"]) == len(rows),
    }
    market_view = public.get("intraday_market", {})
    phases = market_view.get("coverage_phases", [])
    tick_counts = market_view.get("by_kind", {}).get("tw_tick", {})
    checks["intraday_progress_contract_current"] = market_view.get("progress_contract_version") == 1
    checks["intraday_candidate_windows_reconciled"] = sum(
        phase.get("candidate_partitions", 0) for phase in phases
    ) == tick_counts.get("target_partitions")
    checks["intraday_phase_counts_reconciled"] = all(
        sum(phase.get(field, 0) for phase in phases) == tick_counts.get(target)
        for field, target in (("receipted_partitions", "receipted_partitions"),
                              ("provider_not_ready_partitions", "provider_not_ready_partitions"),
                              ("other_failed_partitions", "other_failed_partitions"),
                              ("rows", "rows"), ("local_parquet_bytes", "parquet_bytes"))
    )
    if market_view.get("acquisition_mode") == "minute_first_tick_residual":
        minute_counts = market_view.get("by_kind", {}).get("tw_minute", {})
        minute_phases = market_view.get("minute_coverage_phases", [])
        checks["minute_phases_reconciled"] = all(
            sum(row.get(field, 0) for row in minute_phases) == minute_counts.get(target)
            for field, target in (("candidate_partitions", "target_partitions"),
                ("receipted_partitions", "receipted_partitions"), ("rows", "rows"),
                ("local_parquet_bytes", "parquet_bytes"),
                ("provider_not_ready_partitions", "provider_not_ready_partitions")))
        checks["minute_origins_deduplicated"] = (minute_counts.get("direct_api_partitions", 0)
            + minute_counts.get("derived_partitions", 0) == minute_counts.get("receipted_partitions"))
        checks["minute_and_tick_measurement_isolated"] = (
            public.get("minute_progress", {}).get("measurement", {}).get("kind") == "tw_minute"
            and public.get("tick_progress", {}).get("measurement", {}).get("kind") == "tw_tick")
        if market_view.get("minute_priority", {}).get("tick_residual_allowed") is False:
            checks["tick_eta_respects_minute_priority"] = all(row.get("finish_at_utc") is None
                for row in public.get("tick_progress", {}).get("phases", []))
    tick_progress = public.get("tick_progress", {})
    quota_policy = public.get("intraday_quota_policy", {})
    checks["intraday_quota_policy_available"] = quota_policy.get("state") == "available"
    if quota_policy.get("state") == "available":
        checks["minute_quota_floor_is_independent"] = (
            quota_policy["minute"]["normal_reserve_mb"]
                == load_policy(root / "configs/finlab_quota_policy.json").minute_reserve_mb
            and quota_policy["minute"]["kind"] == "tw_minute")
        checks["intraday_worker_policy_current"] = quota_policy.get("worker_policy_current") is True
    estimates = tick_progress.get("phases", [])
    checks["tick_eta_contract_current"] = tick_progress.get("contract_version") == 1
    checks["tick_eta_scopes_reconciled"] = [item.get("id") for item in estimates] == [
        "latest_session", "observed_history", "older_discovery"]
    checks["tick_eta_never_claims_source_completion"] = (
        tick_progress.get("all_published_history_finish_at_utc") is None
        and all(item.get("estimate_scope") == (
            "first_pass_candidate_queries" if item.get("id") == "older_discovery" else "acquisition_if_published")
                for item in estimates))
    checks["tick_eta_three_scenarios"] = all(
        set(item.get("scenarios", {})) == {"fast", "reference", "slow"} for item in estimates)
    checks["tick_eta_scenario_order"] = all(
        not all(item.get("scenarios", {}).get(name, {}).get("finish_at_utc") for name in ("fast", "reference", "slow"))
        or item["scenarios"]["fast"]["finish_at_utc"] <= item["scenarios"]["reference"]["finish_at_utc"]
            <= item["scenarios"]["slow"]["finish_at_utc"] for item in estimates)
    services = {}
    for unit in UNITS:
        result = subprocess.run(["systemctl", "show", unit, "--property=ActiveState,SubState,MainPID,"
                                 "InvocationID,NRestarts,Result,NextElapseUSecRealtime,LastTriggerUSec,"
                                 "TimersMonotonic,TimersCalendar,ExecCondition,MemoryCurrent,MemoryPeak,"
                                 "MemoryHigh,MemoryMax,MemorySwapMax,MemorySwapCurrent,CPUUsageNSec"],
                                capture_output=True, text=True, timeout=5)
        services[unit] = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    protected = None
    if baseline:
        before = read_json(baseline).get("services", {})
        protected = {unit: all(services[unit].get(field) == before.get(unit, {}).get(field)
                              for field in ("InvocationID", "MainPID", "NRestarts"))
                     for unit in UNITS[4:]}
        checks["protected_runtime_identities_unchanged"] = all(protected.values())
        checks["minute_timer_deployed"] = "OnUnitInactiveUSec=1min" in services[
            "stockagent-finlab-local-refresh.timer"].get("TimersMonotonic", "")
        checks["local_dispatch_guard_deployed"] = "run_finlab_refresh_due.sh" in services[
            "stockagent-finlab-local-refresh.service"].get("ExecCondition", "")
        checks["pre_reset_timer_deployed"] = "07:00:05 Asia/Taipei" in services[
            "stockagent-finlab-local-refresh.timer"].get("TimersCalendar", "")
    market = read_json(root / "data_finlab/intraday/market_status.json")
    verified = verify_recent_partitions(root, recent_partitions) if recent_partitions else []
    if recent_partitions:
        checks["recent_source_objects_verified"] = (
            len(verified) == recent_partitions and all(item["accepted"] for item in verified))
    repaired = verify_partition_windows(root, source_windows) if source_windows else []
    if source_windows:
        checks["recorded_error_window_objects_verified"] = bool(repaired) and all(item["accepted"] for item in repaired)
    fingerprint = quota_policy.get("minute", {}).get("configuration_fingerprint")
    recent_batches = [batch for batch in read_json(root / "data_finlab/intraday/batch_metrics.json").get("batches", [])
                      if batch.get("quota_policy", {}).get("configuration_fingerprint") == fingerprint
                      and batch.get("quota_policy", {}).get("normal_reserve_mb")
                          == quota_policy.get("minute", {}).get("normal_reserve_mb")
                      and batch.get("kind") == "tw_minute"][-8:]
    processing = measure_pipeline(read_json(baseline).get("recent_policy_batches", []) if baseline else [],
                                  recent_batches)
    baseline_at = read_json(baseline).get("observed_at_utc") if baseline else None
    candidate_fields = ("started_at_utc", "finished_at_utc", "kind", "state", "attempt_limit", "attempted",
        "new_receipted_partitions", "elapsed_seconds", "processing_engine", "pipeline_workers",
        "range_calendar_days", "download_threads", "queue_depth", "authorization_slots", "sdk_calls",
        "window_requests", "http_gets", "http_posts", "http_errors", "http_download_bytes",
        "http_429s", "http_503s", "http_conflicts", "http_conflict_retries", "source_seconds", "persist_seconds")
    candidate_batches = [{key: row.get(key) for key in candidate_fields}
        for row in read_json(root / "data_finlab/intraday/batch_metrics.json").get("batches", [])[-256:]
        if isinstance(row, dict) and baseline_at and row.get("started_at_utc", "") >= baseline_at
        and row.get("kind") == "tw_minute"]
    return {
        "schema_version": 2, "observed_at_utc": now.isoformat(), "provider_calls": 0,
        "checks": checks, "accepted": all(checks.values()), "services": services,
        "public_build_seconds": round(elapsed, 4), "public_health": public["health"],
        "protected_units": protected, "inventory_path": str(inventory) if inventory else None,
        "quota": {k: quota.get(k) for k in ("observed_at_utc", "used_mb", "limit_mb", "remaining_mb", "reset_at_utc")},
        "workload": {k: workload.get(k) for k in ("contract_version", "generated_at_utc", "state",
                     "catalog_keys", "general_keys", "refresh_pending", "blocked_keys", "transfer",
                     "records", "stages", "scenarios", "tick", "measurement")},
        "public_execution": public.get("execution"),
        "history_inventory": public.get("history_inventory"),
        "tick_progress": tick_progress,
        "minute_progress": public.get("minute_progress"),
        "intraday_quota_policy": quota_policy,
        "intraday_worker": read_json(root / "data_finlab/intraday/active_run.json"),
        "recent_source_objects": verified,
        "repaired_source_objects": repaired,
        "source_validation_scope": "bounded_latest_direct_minute_objects_not_all_history",
        "recent_policy_batches": recent_batches,
        "pipeline_measurement": processing,
        "throughput_candidate_batches": candidate_batches,
        "pre_reset_drain_live_observed": any(batch.get("quota_policy", {}).get("mode") == "pre_reset_drain"
            and batch.get("new_receipted_partitions", 0) > 0 for batch in recent_batches),
        "all_history_complete_claim": False,
        "run": read_json(root / "data_finlab/runs/latest.json"),
        "intraday": {k: v for k, v in market.items() if k != "symbols"},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--inventory", type=Path)
    parser.add_argument("--efficiency-day", type=date.fromisoformat,
                        help="Audit all receipt/journal work for one Taipei calendar date, without API calls")
    parser.add_argument("--recent-partitions", type=int, default=0, choices=range(0, 33),
                        metavar="0..32", help="Bounded read-only source SHA/schema verification")
    parser.add_argument("--verify-window", nargs=3, action="append", metavar=("SYMBOL", "START", "END"),
                        help="Verify receipted minute objects in an inventoried error window without API calls")
    args = parser.parse_args()
    if args.efficiency_day:
        result = efficiency_history(args.root, args.efficiency_day, through=datetime.now(UTC))
        _atomic_json(args.output, result)
        print(json.dumps({"provider_calls": 0, "output": str(args.output),
                          "date": result["calendar_date"], "intraday": result["intraday"],
                          "general": {k: v for k, v in result["general"].items() if k != "runs"},
                          "quota_cycles": result["quota_cycles"]}, ensure_ascii=False))
        return 0
    result = audit(args.root, baseline=args.baseline, inventory=args.inventory,
                   recent_partitions=args.recent_partitions, source_windows=args.verify_window)
    _atomic_json(args.output, result)
    print(json.dumps({"accepted": result["accepted"], "checks": result["checks"],
                      "public_build_seconds": result["public_build_seconds"],
                      "provider_calls": 0, "output": str(args.output)}, ensure_ascii=False))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
