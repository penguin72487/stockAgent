"""Read-only, bounded measurements for conditional FinMind ETA scenarios.

The traffic ledger records request *starts*, not responses. Queue outcomes are
latest partition states, not an append-only network completion ledger. Keep the
two populations separate: batch fan-out and local derivation make their ratio
unsuitable as a success rate. No API call, credential read or writer migration
is performed here.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
import sqlite3
from typing import Any, Iterator

from downloader.finmind_account import backfill_budget
from downloader.finmind_scheduling import fixed_incremental_demand


WINDOWS = {"15m": 900, "1h": 3600, "24h": 86400, "7d": 604800}
BIN_SECONDS = 900
ACCOUNT_MAX_AGE_SECONDS = 300
WORKER_MAX_AGE_SECONDS = 900


def _stamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _age(value: object, now: datetime) -> float | None:
    parsed = _stamp(value)
    return (now - parsed).total_seconds() if parsed else None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if path.stat().st_size > 2 * 1024 * 1024:
            return {}
        payload = json.loads(path.read_bytes())
        return payload if isinstance(payload, dict) else {}
    except (OSError, ValueError):
        return {}


@contextmanager
def _connect(path: Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=2)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        yield connection
    finally:
        connection.close()


def _quantile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low, high = math.floor(position), math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def _distribution(bins: list[dict[str, Any]]) -> dict[str, Any]:
    complete = [item for item in bins if item["complete"]]
    active = [float(item["requests_per_hour"]) for item in complete if item["requests"] > 0]
    return {
        "complete_bin_count": len(complete),
        "active_bin_count": len(active),
        "idle_bin_count": sum(item["requests"] == 0 for item in complete),
        "partial_bin_count": len(bins) - len(complete),
        "p10": _quantile(active, 0.1), "p50": _quantile(active, 0.5),
        "p90": _quantile(active, 0.9),
        "minimum": min(active) if active else None,
        "maximum": max(active) if active else None,
        "units": "request_starts_per_hour",
        "basis": "positive_complete_15_minute_bins_no_low_rate_filter",
        "statistical_completion_probability": False,
    }


def _traffic(root: Path, now: datetime) -> dict[str, Any]:
    path = root / "request_traffic.sqlite3"
    base: dict[str, Any] = {"state": "missing", "windows": {}, "bins_15m": [],
                            "rate_distributions": {}, "by_dataset": []}
    if not path.is_file():
        return base
    try:
        with _connect(path) as connection:
            # SQLite cannot use its single MIN/MAX shortcut for both aggregates
            # together. Two indexed endpoint seeks avoid revisiting the full
            # lifetime ledger on every minute sample. Both share this snapshot.
            first_row = connection.execute(
                "SELECT started_at_utc FROM requests WHERE started_at_utc<=? "
                "ORDER BY started_at_utc ASC LIMIT 1", (now.isoformat(),),
            ).fetchone()
            last_row = connection.execute(
                "SELECT started_at_utc FROM requests WHERE started_at_utc<=? "
                "ORDER BY started_at_utc DESC LIMIT 1", (now.isoformat(),),
            ).fetchone()
            first_raw = first_row[0] if first_row else None
            last_raw = last_row[0] if last_row else None
            first, last = _stamp(first_raw), _stamp(last_raw)
            windows = {}
            for name, seconds in WINDOWS.items():
                lower = now - timedelta(seconds=seconds)
                count = connection.execute(
                    "SELECT count(*) FROM requests WHERE started_at_utc>? AND started_at_utc<=?",
                    (lower.isoformat(), now.isoformat()),
                ).fetchone()[0]
                observed = max(0.0, (now - max(lower, first)).total_seconds()) if first else 0.0
                windows[name] = {
                    "attempts": count, "observed_seconds": observed,
                    "complete_window": bool(first and first <= lower),
                    "wall_requests_per_hour": count * 3600 / observed if observed else None,
                }
            bin_end = int(now.timestamp()) // BIN_SECONDS * BIN_SECONDS
            bin_start = bin_end - WINDOWS["24h"]
            sql_bins = dict(connection.execute(
                "SELECT CAST(strftime('%s',started_at_utc) AS INTEGER)/?, count(*) "
                "FROM requests WHERE started_at_utc>=? AND started_at_utc<? GROUP BY 1",
                (BIN_SECONDS, datetime.fromtimestamp(bin_start, UTC).isoformat(),
                 datetime.fromtimestamp(bin_end, UTC).isoformat()),
            ).fetchall())
            bins = []
            for epoch in range(bin_start, bin_end, BIN_SECONDS):
                stamp = datetime.fromtimestamp(epoch, UTC)
                count = int(sql_bins.get(epoch // BIN_SECONDS, 0))
                bins.append({"started_at_utc": stamp.isoformat(),
                             "ended_at_utc": (stamp + timedelta(seconds=BIN_SECONDS)).isoformat(),
                             "requests": count, "requests_per_hour": count * 4,
                             "complete": bool(first and first <= stamp)})
            by_dataset = [
                {"dataset": dataset, "attempts_24h": count, "last_attempt_at_utc": last_at}
                for dataset, count, last_at in connection.execute(
                    "SELECT dataset,count(*),max(started_at_utc) FROM requests "
                    "WHERE started_at_utc>? AND started_at_utc<=? GROUP BY dataset ORDER BY dataset",
                    ((now - timedelta(hours=24)).isoformat(), now.isoformat()),
                )
            ]
            future = int(connection.execute(
                "SELECT count(*) FROM requests WHERE started_at_utc>?", (now.isoformat(),),
            ).fetchone()[0])
        return {"state": "observed" if first else "empty", "tracking_started_at_utc": first_raw,
                "last_attempt_at_utc": last_raw,
                "last_attempt_age_seconds": (now - last).total_seconds() if last else None,
                "observed_span_seconds": (last - first).total_seconds() if first and last else None,
                "windows": windows, "bins_15m": bins,
                "complete_bins_end_at_utc": datetime.fromtimestamp(bin_end, UTC).isoformat(),
                "rate_distributions": {"1h": _distribution(bins[-4:]), "24h": _distribution(bins)},
                "by_dataset": by_dataset, "future_timestamp_count": future,
                "basis": "shared_account_local_request_starts_not_successful_responses",
                "external_applications_observed": False,
                "success_rate": None, "retry_rate": None, "request_latency_seconds": None}
    except (sqlite3.Error, OSError):
        return {**base, "state": "unavailable", "error_code": "traffic_read_failed"}


def _queue_worker(root: Path, owner: str, now: datetime) -> dict[str, Any]:
    status = _read_json(root / "status.json")
    age = _age(status.get("observed_at_utc"), now)
    result: dict[str, Any] = {
        "state": "missing", "worker_state": status.get("state"),
        "status_observed_at_utc": status.get("observed_at_utc"), "status_age_seconds": age,
        "status_fresh": age is not None and -60 <= age <= WORKER_MAX_AGE_SECONDS,
        "queue_states": {}, "recent_outcomes_by_window": {}, "batch_failure_audit": {},
    }
    path = root / "queue.sqlite3"
    if not path.is_file():
        return result
    try:
        with _connect(path) as connection:
            result["queue_states"] = dict(connection.execute(
                "SELECT state,count(*) FROM tasks GROUP BY state"))
            # Four windows in one bounded scan. Queues can have hundreds of
            # thousands of rows and are not indexed by last_attempt_at_utc.
            measures = ",".join(
                "sum(CASE WHEN last_attempt_at_utc>? THEN 1 ELSE 0 END),"
                "sum(CASE WHEN last_attempt_at_utc>? THEN rows ELSE 0 END),"
                "sum(CASE WHEN last_attempt_at_utc>? THEN bytes ELSE 0 END)"
                for _ in WINDOWS
            )
            parameters = [
                (now - timedelta(seconds=seconds)).isoformat()
                for seconds in WINDOWS.values() for _ in range(3)
            ] + [(now - timedelta(days=7)).isoformat(), now.isoformat()]
            all_windows = list(connection.execute(
                f"SELECT state,kind,{measures} FROM tasks "
                "WHERE last_attempt_at_utc>? AND last_attempt_at_utc<=? GROUP BY state,kind",
                parameters,
            ))
            for index, window in enumerate(WINDOWS):
                grouped = [tuple(row[:2]) + tuple(row[2 + index * 3:5 + index * 3])
                           for row in all_windows]
                accepted = [row for row in grouped if row[0] in {"complete", "observed_empty"}]
                result["recent_outcomes_by_window"][window] = {
                    "successful_latest_partitions": sum(row[2] for row in accepted),
                    "nonempty_latest_partitions": sum(row[2] for row in grouped if row[0] == "complete"),
                    "empty_latest_partitions": sum(row[2] for row in grouped if row[0] == "observed_empty"),
                    "derived_latest_partitions": sum(row[2] for row in accepted if row[1] == "derived"),
                    "failed_latest_partitions": sum(row[2] for row in grouped if row[0] == "failed"),
                    "rows_in_successful_latest_partitions": sum(row[3] or 0 for row in accepted),
                    "bytes_in_successful_latest_partitions": sum(row[4] or 0 for row in accepted),
                    "not_a_network_completion_count": True,
                }
            result["last_attempt_at_utc"] = connection.execute(
                "SELECT max(last_attempt_at_utc) FROM tasks WHERE last_attempt_at_utc<=?",
                (now.isoformat(),),
            ).fetchone()[0]
            table = {"sponsor": "request_batch_failure_audit",
                     "complement": "complement_year_batch_failures"}[owner]
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,),
            ).fetchone()
            if exists:
                # The table identifier is a fixed mapping, never user input.
                result["batch_failure_audit"] = {
                    "events_7d_by_code": dict(connection.execute(
                        f"SELECT error_code,count(*) FROM {table} "
                        "WHERE failed_at_utc>? AND failed_at_utc<=? GROUP BY error_code",
                        ((now - timedelta(days=7)).isoformat(), now.isoformat()),
                    )), "scope": "recorded_batch_failures_only_not_all_request_failures",
                }
        result["state"] = "observed"
        result["basis"] = "latest_partition_state_snapshot_not_append_only_completion_ledger"
    except (sqlite3.Error, OSError):
        result.update(state="unavailable", error_code="queue_read_failed")
    return result


def build_finmind_eta_telemetry(root: Path, now: datetime) -> dict[str, Any]:
    """Inspect ``data_finmind`` without invoking provider APIs or mutating state."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone aware")
    now = now.astimezone(UTC)
    root = Path(root)
    account = _read_json(root / "account_status.json")
    age = _age(account.get("observed_at_utc"), now)
    fresh = age is not None and -60 <= age <= ACCOUNT_MAX_AGE_SECONDS
    limit = account.get("official_requests_per_hour")
    limit = limit if type(limit) is int and 0 < limit <= 100_000 else None
    demand = fixed_incremental_demand(root, now)
    reserve = min(limit, demand + 2) if limit is not None else None
    budget = backfill_budget(account, root, fixed_incremental_requests=demand, now=now) if limit else None
    quota = {
        "official_requests_per_hour": limit,
        "account_tier": account.get("tier") if account.get("tier") in {"Free", "Backer", "Sponsor", "SponsorPro"} else None,
        "account_observed_at_utc": account.get("observed_at_utc"), "account_age_seconds": age,
        "account_fresh": fresh,
        "provider_used_in_hour": account.get("provider_used_in_hour")
        if type(account.get("provider_used_in_hour")) is int and account["provider_used_in_hour"] >= 0 else None,
        "paced_requests_per_hour": 3600 / (3600 / limit + 0.01) if limit else None,
        "fixed_incremental_demand": demand, "reserved_requests_per_hour": reserve,
        "backfill_capacity_requests_per_hour": max(0, limit - reserve) if limit and fresh else None,
        "current_budget": budget, "scope": "one_shared_account_not_per_worker",
        "account_history_available": False,
        "reserve_basis": "next_hour_named_incremental_demand_plus_two_inflight_lanes",
        "reserve_is_consumption_measurement": False,
    }
    traffic = _traffic(root, now)
    workers = {owner: _queue_worker(root / owner, owner, now) for owner in ("sponsor", "complement")}
    free = _read_json(root / "status.json")
    free_age = _age(free.get("observed_at_utc"), now)
    workers["free"] = {
        "worker_state": free.get("state"), "status_observed_at_utc": free.get("observed_at_utc"),
        "status_age_seconds": free_age,
        "status_fresh": free_age is not None and -60 <= free_age <= WORKER_MAX_AGE_SECONDS,
        "pending_session_day_tasks": free.get("pending_session_day_tasks"),
        "basis": "latest_worker_status_no_completion_event_ledger",
    }
    return {
        "schema_version": 1, "observed_at_utc": now.isoformat(),
        "state": "observed" if fresh and limit and traffic["state"] == "observed" else "insufficient_evidence",
        "quota": quota, "traffic": traffic, "workers": workers,
        "limitations": [
            "request_starts_are_not_successful_completions",
            "queue_latest_outcomes_are_not_an_append_only_retry_history",
            "batch_fanout_and_local_derivation_prevent_completion_to_start_ratios",
            "active_bin_rates_exclude_reported_idle_bins_not_low_rate_bins",
            "historical_rates_do_not_predict_unbounded_failures_or_external_consumers",
            "no_request_latency_or_full_account_usage_history_is_recorded",
            "separate_read_only_snapshots_not_one_atomic_cross_worker_transaction",
        ],
    }
