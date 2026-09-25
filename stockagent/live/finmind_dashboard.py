"""Read-only, credential-free projection for the FinMind download page."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import shutil
import sqlite3
from typing import Any, Mapping

from downloader.download_finmind_free import (
    CALENDAR_DATASET,
    MASTER_DATASET,
    SESSION_DATASETS,
)
from downloader.download_finmind_complement import (
    ALL_DATASETS as COMPLEMENT_DATASETS,
    SNAPSHOTS as COMPLEMENT_SNAPSHOTS,
    GLOBAL_EQUITY_HISTORY,
    GLOBAL_HISTORY,
    FIXED_ID_HISTORY,
    WIDE_INSTITUTIONAL,
)
from downloader.download_finmind_sponsor import SOURCES as SPONSOR_SOURCES, UNSCHEDULED as SPONSOR_UNSCHEDULED


LABELS = {
    CALENDAR_DATASET: "台股交易日曆",
    SESSION_DATASETS[0]: "全市場委託與成交統計",
    SESSION_DATASETS[1]: "全市場盤中指標",
    MASTER_DATASET: "權證主檔全表快照",
}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _stamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else None


def _nonnegative_int(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _receipt_file_size(root: Path, receipt: Mapping[str, Any]) -> int | None:
    relative = receipt.get("parquet_path")
    expected = _nonnegative_int(receipt.get("parquet_size_bytes"))
    if not isinstance(relative, str) or expected is None:
        return None
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        return None
    try:
        size = path.stat().st_size
    except OSError:
        return None
    return size if size == expected else None


def _traffic(root: Path, now: datetime, limit: int) -> dict[str, Any]:
    """All local workers' observed requests, not the provider account balance."""
    path = root / "request_traffic.sqlite3"
    if not path.is_file():
        return {"state": "not_started", "observed_requests_60m": None,
                "history": [], "tracking_started_at_utc": None}
    cutoff = now - timedelta(hours=25)
    try:
        with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=1.0) as conn:
            first_row = conn.execute("SELECT MIN(started_at_utc) FROM requests").fetchone()
            raw = conn.execute(
                "SELECT started_at_utc FROM requests WHERE started_at_utc >= ? "
                "AND started_at_utc <= ? ORDER BY started_at_utc",
                (cutoff.isoformat(), now.isoformat()),
            ).fetchall()
    except (sqlite3.Error, OSError):
        return {"state": "unavailable", "observed_requests_60m": None,
                "history": [], "tracking_started_at_utc": None}
    first = _stamp(first_row[0]) if first_row else None
    events = [stamp for (value,) in raw if (stamp := _stamp(value)) is not None]
    recent_cutoff = now - timedelta(hours=1)
    used = sum(stamp > recent_cutoff for stamp in events)
    # The first hour after instrumentation is a measured lower bound, not a
    # complete account quota window. External applications remain unobserved.
    state = "complete_worker_window" if first and first <= recent_cutoff else "partial_worker_window"
    history: list[dict[str, Any]] = []
    if first:
        cursor = max(first, now - timedelta(hours=24)).replace(second=0, microsecond=0)
        end = now.replace(second=0, microsecond=0)
        window: deque[datetime] = deque()
        index = 0
        while cursor <= end:
            while index < len(events) and events[index] <= cursor:
                window.append(events[index])
                index += 1
            boundary = cursor - timedelta(hours=1)
            while window and window[0] <= boundary:
                window.popleft()
            history.append({"at_utc": cursor.isoformat(), "observed_requests_60m": len(window)})
            cursor += timedelta(minutes=1 if cursor >= now - timedelta(hours=3) else 10)
    return {
        "state": state,
        "official_requests_per_hour": limit,
        "observed_requests_60m": used,
        "worker_headroom_60m": max(0, limit - used) if state == "complete_worker_window" else None,
        "tracking_started_at_utc": first.isoformat() if first else None,
        "history": history,
        "basis": "Only requests started by StockAgent FinMind workers; official account samples are reported separately.",
    }


def build_finmind_public_status(repo_root: Path, *, now: datetime | None = None) -> dict[str, Any]:
    """Project local receipts without fetching FinMind or exposing raw rows."""
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    root = Path(repo_root) / "data_finmind"
    status = _read_json(root / "status.json")
    status_time = _stamp(status.get("observed_at_utc"))
    age = max(0, (observed - status_time).total_seconds()) if status_time else None
    account = _read_json(root / "account_status.json")
    account_stamp = _stamp(account.get("observed_at_utc"))
    account_fresh = bool(account_stamp and observed - account_stamp <= timedelta(minutes=30))
    official_limit = account.get("official_requests_per_hour") if account_fresh else status.get("official_requests_per_hour")
    limit = official_limit if type(official_limit) is int and 1 <= official_limit <= 100_000 else 300
    series = status.get("series") if isinstance(status.get("series"), Mapping) else {}
    datasets: list[dict[str, Any]] = []
    for dataset in SESSION_DATASETS:
        item = series.get(dataset) if isinstance(series.get(dataset), Mapping) else {}
        total = _nonnegative_int(item.get("total"))
        complete = _nonnegative_int(item.get("complete"))
        pending = max(0, total - complete) if total is not None and complete is not None else None
        datasets.append({
            "id": dataset,
            "label": LABELS[dataset],
            "kind": "session_history",
            "state": "complete" if pending == 0 and total else "backfilling" if complete else "pending",
            "target_partitions": total,
            "complete_partitions": complete,
            "pending_partitions": pending,
            "deferred_partitions": _nonnegative_int(item.get("deferred")),
            "rows": _nonnegative_int(item.get("rows")),
            "local_bytes": _nonnegative_int(item.get("bytes")),
            "first_data_date": item.get("first_complete_date"),
            "last_data_date": item.get("last_complete_date"),
            "last_receipt_at_utc": item.get("last_receipt_at_utc"),
            "observed_grains": {
                key: value for key, value in (item.get("observed_grains") or {}).items()
                if key in {"1m", "5s"} and _nonnegative_int(value) is not None
            } if isinstance(item.get("observed_grains"), Mapping) else {},
            "minimum_network_seconds_remaining": round(pending * 3600 / limit) if pending is not None else None,
        })

    calendar = _read_json(root / "calendar.json")
    dates = calendar.get("dates") if isinstance(calendar.get("dates"), list) else []
    dates = [value for value in dates if isinstance(value, str) and len(value) == 10]
    calendar_complete = bool(dates and calendar.get("source_dataset") == CALENDAR_DATASET)
    try:
        calendar_size = (root / "calendar.json").stat().st_size if calendar_complete else None
    except OSError:
        calendar_size = None
        calendar_complete = False
    datasets.append({
        "id": CALENDAR_DATASET, "label": LABELS[CALENDAR_DATASET], "kind": "reference",
        "state": "complete" if calendar_complete else "pending",
        "target_partitions": 1, "complete_partitions": int(calendar_complete),
        "pending_partitions": int(not calendar_complete), "deferred_partitions": 0,
        "rows": len(dates) if calendar_complete else None, "local_bytes": calendar_size,
        "first_data_date": min(dates) if dates else None,
        "last_data_date": max(dates) if dates else None,
        "last_receipt_at_utc": calendar.get("observed_at_utc"),
        "minimum_network_seconds_remaining": None,
    })

    master_receipts = sorted((root / "receipts" / MASTER_DATASET).glob("*.json"), reverse=True)
    master: dict[str, Any] = {}
    for path in master_receipts:
        candidate = _read_json(path)
        if candidate.get("status") == "complete" and candidate.get("query_scope") == "full_table_snapshot" and _receipt_file_size(root, candidate) is not None:
            master = candidate
            break
    master_complete = bool(master)
    datasets.append({
        "id": MASTER_DATASET, "label": LABELS[MASTER_DATASET], "kind": "snapshot",
        "state": "complete" if master_complete else "pending",
        "target_partitions": 1, "complete_partitions": int(master_complete),
        "pending_partitions": int(not master_complete), "deferred_partitions": 0,
        "rows": _nonnegative_int(master.get("rows")),
        "local_bytes": _receipt_file_size(root, master),
        "first_data_date": master.get("source_first_date"),
        "last_data_date": master.get("source_last_date"),
        "snapshot_date_taipei": master.get("snapshot_date_taipei"),
        "last_receipt_at_utc": master.get("fetched_at_utc"),
        "minimum_network_seconds_remaining": None,
        "point_in_time_history_available": False,
    })

    complement = _read_json(root / "complement" / "status.json")
    complement_stamp = _stamp(complement.get("observed_at_utc"))
    complement_age = max(0, (observed - complement_stamp).total_seconds()) if complement_stamp else None
    companion_series = complement.get("series") if isinstance(complement.get("series"), Mapping) else {}
    delegated = set(complement.get("delegated_to_sponsor", [])) if isinstance(complement.get("delegated_to_sponsor"), list) else set()
    companion_total = companion_complete = companion_empty = companion_failed = companion_blocked = companion_invalid = 0
    for dataset in COMPLEMENT_DATASETS:
        item = companion_series.get(dataset) if isinstance(companion_series.get(dataset), Mapping) else {}
        total = _nonnegative_int(item.get("target")) or 0
        complete = _nonnegative_int(item.get("complete")) or 0
        empty = _nonnegative_int(item.get("observed_empty")) or 0
        failed = _nonnegative_int(item.get("failed")) or 0
        blocked = _nonnegative_int(item.get("not_entitled")) or 0
        invalid = _nonnegative_int(item.get("invalid_request")) or 0
        if dataset not in delegated:
            companion_total += total
            companion_complete += complete
            companion_empty += empty
            companion_failed += failed
            companion_blocked += blocked
            companion_invalid += invalid
        kind = ("snapshot" if dataset in COMPLEMENT_SNAPSHOTS else
                "global_history" if dataset in GLOBAL_HISTORY or dataset in FIXED_ID_HISTORY else
                "global_equity_history" if dataset in GLOBAL_EQUITY_HISTORY else
                "symbol_history")
        pending = max(0, total - complete - empty - blocked - invalid) if total else None
        checked = complete + empty
        state = ("delegated" if dataset in delegated else
                 "pending" if not total else
                 "unavailable" if blocked + invalid == total else
                 "complete" if checked == total else
                 "backfilling" if complete or empty or failed or blocked else "pending")
        datasets.append({
            "id": dataset, "label": dataset, "kind": kind, "state": state,
            "target_partitions": total or None,
            "complete_partitions": complete,
            "checked_partitions": checked,
            "pending_partitions": pending,
            "deferred_partitions": failed,
            "observed_empty_partitions": empty,
            "not_entitled_partitions": blocked,
            "invalid_request_partitions": invalid,
            "rows": _nonnegative_int(item.get("rows")),
            "local_bytes": _nonnegative_int(item.get("bytes")),
            "first_data_date": item.get("first_data_date"),
            "last_data_date": item.get("last_data_date"),
            "last_receipt_at_utc": item.get("last_attempt_at_utc"),
            "minimum_network_seconds_remaining": round(pending * 3600 / limit) if pending is not None and not blocked and dataset not in delegated else None,
            "source_status": ("locally_derived_from_institutional_long"
                              if dataset == WIDE_INSTITUTIONAL else
                              "observed_response_only_not_provider_completeness"),
        })

    sponsor = _read_json(root / "sponsor" / "status.json")
    sponsor_stamp = _stamp(sponsor.get("observed_at_utc"))
    sponsor_age = max(0, (observed - sponsor_stamp).total_seconds()) if sponsor_stamp else None
    sponsor_series = sponsor.get("series") if isinstance(sponsor.get("series"), Mapping) else {}
    sponsor_total = sponsor_complete = sponsor_empty = sponsor_failed = sponsor_blocked = 0
    for spec in SPONSOR_SOURCES:
        item = sponsor_series.get(spec.dataset) if isinstance(sponsor_series.get(spec.dataset), Mapping) else {}
        total = _nonnegative_int(item.get("target")) or 0
        complete = _nonnegative_int(item.get("complete")) or 0
        empty = _nonnegative_int(item.get("observed_empty")) or 0
        failed = _nonnegative_int(item.get("failed")) or 0
        blocked = _nonnegative_int(item.get("blocked")) or 0
        sponsor_total += total
        sponsor_complete += complete
        sponsor_empty += empty
        sponsor_failed += failed
        sponsor_blocked += blocked
        remaining = max(0, total - complete - empty - blocked)
        datasets.append({
            "id": f"{spec.dataset}:all_market", "label": f"{spec.dataset}（Sponsor 全市場）",
            "kind": "sponsor_" + spec.grain,
            "state": "unavailable" if blocked and blocked == total else
                     "complete" if total and remaining == 0 else
                     "backfilling" if complete or empty or failed else "pending",
            "target_partitions": total or None,
            "complete_partitions": complete,
            "checked_partitions": complete + empty,
            "pending_partitions": remaining if total else None,
            "deferred_partitions": failed,
            "observed_empty_partitions": empty,
            "not_entitled_partitions": blocked,
            "rows": _nonnegative_int(item.get("rows")),
            "local_bytes": _nonnegative_int(item.get("bytes")),
            "first_data_date": item.get("first_data_date"),
            "last_data_date": item.get("last_data_date"),
            "last_receipt_at_utc": item.get("last_attempt_at_utc"),
            "minimum_network_seconds_remaining": round(remaining * 3600 / limit) if total and not blocked else None,
            "source_status": "observed_sponsor_market_response_not_provider_completeness",
        })
    for dataset, reason in SPONSOR_UNSCHEDULED.items():
        datasets.append({
            "id": f"{dataset}:sponsor_unallocated", "label": f"{dataset}（未排程）",
            "kind": "sponsor_unscheduled", "state": "unavailable",
            "target_partitions": None, "complete_partitions": 0,
            "checked_partitions": 0, "pending_partitions": None,
            "rows": None, "local_bytes": None,
            "first_data_date": None, "last_data_date": None,
            "minimum_network_seconds_remaining": None,
            "source_status": reason,
        })

    session_total = _nonnegative_int(status.get("total_session_day_tasks"))
    session_complete = _nonnegative_int(status.get("complete_session_day_tasks"))
    pending = max(0, session_total - session_complete) if session_total is not None and session_complete is not None else None
    all_total = session_total + 2 + companion_total + sponsor_total if session_total is not None else None
    all_complete = session_complete + int(calendar_complete) + int(master_complete) + companion_complete + sponsor_complete if session_complete is not None else None
    all_checked = all_complete + companion_empty + sponsor_empty if all_complete is not None else None
    all_pending = max(0, all_total - all_complete - companion_empty - sponsor_empty - companion_blocked - companion_invalid - sponsor_blocked) if all_total is not None and all_complete is not None else None
    state = str(status.get("state") or "not_started")
    known_states = {"running", "backfilling", "current", "protected_opening", "waiting_retry",
                    "rate_limited", "not_entitled", "not_started"}
    if state not in known_states and not state.startswith("calendar_"):
        state = "unknown"
    fresh_base = age is not None and age <= 180
    fresh_complement = complement_age is not None and complement_age <= 180
    fresh_sponsor = sponsor_age is not None and sponsor_age <= 180
    health = (
        "unavailable" if age is None and complement_age is None and sponsor_age is None else
        "stale" if not fresh_base and not fresh_complement and not fresh_sponsor else
        "degraded" if (fresh_base and (state.startswith("calendar_") or state in {"not_entitled", "rate_limited"})) or
                      (fresh_complement and complement.get("state") in {"rate_limited", "disk_guard", "invalid_token", "ip_banned"}) or
                      (fresh_sponsor and sponsor.get("state") in {"rate_limited", "disk_guard", "invalid_token", "ip_banned", "account_unverified", "not_entitled"}) else
        "updating" if (fresh_base and state in {"running", "backfilling"}) or
                      (fresh_complement and complement.get("state") == "running") or
                      (fresh_sponsor and sponsor.get("state") == "running") else
        "waiting"
    )
    traffic = _traffic(root, observed, limit)
    local_bytes = sum(item["local_bytes"] or 0 for item in datasets)
    try:
        filesystem_free = shutil.disk_usage(root).free
    except OSError:
        filesystem_free = None
    return {
        "schema_version": 1,
        "generated_at_utc": observed.isoformat(),
        "read_only": True,
        "production_control_possible": False,
        "health": health,
        "status_age_seconds": round(min(value for value in (age, complement_age, sponsor_age) if value is not None))
            if age is not None or complement_age is not None or sponsor_age is not None else None,
        "acquisition": {
            "state": state,
            "observed_at_utc": status.get("observed_at_utc"),
            "total_session_day_tasks": session_total,
            "complete_session_day_tasks": session_complete,
            "pending_session_day_tasks": pending,
            "total_tasks": all_total,
            "complete_tasks": all_complete,
            "checked_tasks": all_checked,
            "pending_tasks": all_pending,
            "observed_empty_tasks": companion_empty + sponsor_empty,
            "failed_tasks": companion_failed + sponsor_failed,
            "not_entitled_tasks": companion_blocked + sponsor_blocked,
            "invalid_request_tasks": companion_invalid,
            "unknown_universe_datasets": sum(1 for row in datasets if row["target_partitions"] is None),
            "candidate_universe": {
                key: _nonnegative_int(complement.get("candidate_universe", {}).get(key))
                for key in ("finmind_current_master_stock_ids", "official_delisted_stock_ids",
                            "additional_official_delisted_ids")
            } if isinstance(complement.get("candidate_universe"), Mapping) else {},
            "complement_state": complement.get("state") if complement else "not_started",
            "sponsor_state": sponsor.get("state") if sponsor else "not_started",
            "sponsor_observed_at_utc": sponsor.get("observed_at_utc"),
            "sponsor_active_tasks": sponsor.get("active_tasks", [])[:8] if isinstance(sponsor.get("active_tasks"), list) else [],
            "sponsor_last_task": sponsor.get("last_task") if isinstance(sponsor.get("last_task"), Mapping) else None,
            "complement_observed_at_utc": complement.get("observed_at_utc"),
            "complement_status_age_seconds": round(complement_age) if complement_age is not None else None,
            "complement_active_task": {
                key: complement.get("active_task", {}).get(key)
                for key in ("dataset", "data_id", "partition")
            } if isinstance(complement.get("active_task"), Mapping) else None,
            "complement_next_task": {
                key: complement.get("next_task", {}).get(key)
                for key in ("dataset", "data_id", "partition")
            } if isinstance(complement.get("next_task"), Mapping) else None,
            "complement_last_task": {
                key: complement.get("last_task", {}).get(key)
                for key in ("dataset", "data_id", "partition", "status", "rows", "error_code")
            } if isinstance(complement.get("last_task"), Mapping) else None,
            "retry_deferred_tasks": (_nonnegative_int(status.get("retry_deferred_tasks")) or 0) + companion_failed + sponsor_failed,
            "minimum_network_seconds_remaining": round(all_pending * 3600 / limit) if all_pending is not None and not companion_blocked and not sponsor_blocked else None,
            "last_task": {key: status.get("last_task", {}).get(key) for key in ("dataset", "date", "status", "rows")}
                if isinstance(status.get("last_task"), Mapping) else None,
            "active_task": {key: status.get("active_task", {}).get(key) for key in ("dataset", "date", "started_at_utc")}
                if isinstance(status.get("active_task"), Mapping) else None,
            "queue_preview": [
                {key: row.get(key) for key in ("dataset", "date")}
                for row in status.get("queue_preview", [])[:12]
                if isinstance(row, Mapping)
            ] if isinstance(status.get("queue_preview"), list) else [],
            "news": "disabled_by_user",
            "training": "raw_downloaded_not_pit_validated",
        },
        "quota": {**traffic,
                  "account_tier": account.get("tier") if account_fresh else None,
                  "provider_used_in_hour": _nonnegative_int(account.get("provider_used_in_hour")) if account_fresh else None,
                  "provider_observed_at_utc": account.get("observed_at_utc") if account_fresh else None},
        "storage": {"local_bytes": local_bytes, "filesystem_free_bytes": filesystem_free,
                    "unit": "receipt_backed_local_bytes", "estimated_total_bytes": None},
        "datasets": datasets,
        "scope": {
            "scheduled_datasets": [*SESSION_DATASETS, CALENDAR_DATASET, MASTER_DATASET,
                                   *COMPLEMENT_DATASETS, *[f"{spec.dataset}:all_market" for spec in SPONSOR_SOURCES]],
            "excluded": ["TaiwanStockNews", *SPONSOR_UNSCHEDULED],
            "public_raw_rows_exposed": False,
            "cold_published": False,
        },
    }
