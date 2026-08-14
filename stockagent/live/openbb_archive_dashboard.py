"""Sanitized, read-only projections for the long-running OpenBB archive.

The full archive monitor is intentionally expensive and can be stale while a
new audit is running.  Public health therefore comes from two independent
facts: live process/activity evidence and the latest complete manifest scan.
"""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
import json
import math
import os
from pathlib import Path
import re
import shutil
from typing import Any, Mapping


OPENBB_PUBLIC_SCHEMA_VERSION = 1
FULL_SNAPSHOT_STALE_SECONDS = 30 * 60
PROCESS_ACTIVITY_STALE_SECONDS = 10 * 60
MAX_JSON_BYTES = 8 * 1024 * 1024
MAX_HISTORY_ROWS = 50_000
_SAFE_NAME = re.compile(r"^[a-z0-9_][a-z0-9_.-]{0,63}$")

_ALERT_MESSAGES = {
    "accepted_progress_stalled": "近期沒有新增已接受任務，下載可能正受配額或上游限制。",
    "disk_space_low": "可用磁碟空間低於安全門檻，封存程序會停止以保護資料。",
    "downloader_inactive": "下載程序目前未執行。",
    "endpoints_without_accepted_data": "仍有端點尚未產生成功資料或權威空結果。",
    "high_attempt_tasks": "部分任務重試次數偏高，仍會保留在可排程集合。",
    "non_authoritative_terminal_unavailable": "部分終止任務缺少足夠證據，不能計為完整封存。",
    "permanent_provider_outcomes": "部分供應商回應被分類為永久結果，需要持續稽核。",
    "provider_progress_stalled": "仍有待辦的供應商近期沒有新增已接受任務。",
    "provider_quota_completion_floor": "供應商配額形成完成時間下限，提高並行度無法消除。",
    "provider_quota_deferred": "部分任務正等待供應商配額或速率限制解除。",
    "systematic_retryable_failures": "多個可重試任務呈現相同失敗型態。",
    "unproven_terminal_unavailable": "部分不可用任務缺少可驗證的權限或能力證據。",
}


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _datetime(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _datetime(value)
    if parsed is None:
        return None
    return round(max(0.0, (now - parsed).total_seconds()), 3)


def _finite(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _integer(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return default


def _name(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized if _SAFE_NAME.fullmatch(normalized) else None


def _safe_phase(value: Any) -> str:
    normalized = str(value or "unknown").strip().lower()
    return normalized if _SAFE_NAME.fullmatch(normalized) else "unknown"


def _pid_alive(pid_path: Path, expected_fragments: tuple[str, ...]) -> bool:
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
        if pid <= 1:
            return False
        command = (
            (Path("/proc") / str(pid) / "cmdline")
            .read_bytes()
            .replace(b"\0", b" ")
            .decode("utf-8", errors="replace")
        )
        os.kill(pid, 0)
    except (
        FileNotFoundError,
        PermissionError,
        ProcessLookupError,
        OSError,
        ValueError,
    ):
        return False
    return any(fragment in command for fragment in expected_fragments)


def _activity_timestamp(
    state_dir: Path,
    scheduler: Mapping[str, Any],
    downloader_phase: Mapping[str, Any],
) -> datetime | None:
    candidates: list[datetime] = []
    for source in (scheduler, downloader_phase):
        updated_at = _datetime(source.get("updated_at"))
        if updated_at is not None:
            candidates.append(updated_at)
    for path in (
        state_dir / "provider_scheduler.json",
        state_dir.parent / "logs" / "supervisor.log",
    ):
        try:
            candidates.append(datetime.fromtimestamp(path.stat().st_mtime, tz=UTC))
        except OSError:
            continue
    return max(candidates) if candidates else None


def project_openbb_history_row(status: Mapping[str, Any]) -> dict[str, Any]:
    """Project one expensive monitor result into a compact public trend row."""

    checked_at = _datetime(status.get("checked_at"))
    if checked_at is None:
        return {}
    total = _integer(status.get("total_tasks"))
    accepted = _integer(status.get("accepted_tasks"))
    resolved = _integer(status.get("resolved_tasks"))
    unresolved = _integer(status.get("unresolved_tasks"))
    unavailable = _integer(status.get("unavailable_tasks"))
    return {
        "checked_at": checked_at.isoformat(),
        "completion_percent": round(
            _finite(status.get("completion_percent"), 0.0) or 0.0, 6
        ),
        "accepted_tasks": accepted,
        "total_tasks": total,
        "resolved_tasks": resolved,
        "unresolved_tasks": unresolved,
        "retryable_tasks": _integer(status.get("retryable_tasks")),
        "unavailable_tasks": unavailable,
        "accepted_percent": round(100.0 * accepted / total, 6) if total else 0.0,
        "resolved_percent": round(100.0 * resolved / total, 6) if total else 0.0,
        "unresolved_percent": round(100.0 * unresolved / total, 6) if total else 0.0,
        "success_rows": _integer(status.get("success_rows")),
        "accepted_tasks_last_15m": _integer(status.get("accepted_tasks_last_15m")),
        "tasks_per_minute_last_15m": round(
            _finite(status.get("tasks_per_minute_last_15m"), 0.0) or 0.0, 6
        ),
        "health": _safe_phase(status.get("health")),
        "complete": bool(status.get("complete")),
    }


def _category_rows(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    source = snapshot.get("category_progress")
    if not isinstance(source, list):
        return result
    for raw in source:
        if not isinstance(raw, Mapping):
            continue
        category = _name(raw.get("category"))
        if category is None:
            continue
        result.append(
            {
                "category": category,
                "completion_percent": round(
                    _finite(raw.get("completion_percent"), 0.0) or 0.0, 6
                ),
                "accepted_tasks": _integer(raw.get("accepted_tasks")),
                "total_tasks": _integer(raw.get("total_tasks")),
                "unresolved_tasks": _integer(raw.get("unresolved_tasks")),
                "unavailable_tasks": _integer(raw.get("unavailable_tasks")),
                "success_rows": _integer(raw.get("success_rows")),
            }
        )
    return sorted(result, key=lambda row: (-row["unresolved_tasks"], row["category"]))


def _provider_rows(
    snapshot: Mapping[str, Any], scheduler: Mapping[str, Any], now: datetime
) -> list[dict[str, Any]]:
    progress_by_name: dict[str, Mapping[str, Any]] = {}
    for raw in snapshot.get("provider_progress", []):
        if isinstance(raw, Mapping) and (provider := _name(raw.get("provider"))):
            progress_by_name[provider] = raw
    eta_by_name: dict[str, Mapping[str, Any]] = {}
    for raw in snapshot.get("provider_eta_projections", []):
        if isinstance(raw, Mapping) and (provider := _name(raw.get("provider"))):
            eta_by_name[provider] = raw
    scheduler_providers = scheduler.get("providers")
    if not isinstance(scheduler_providers, Mapping):
        scheduler_providers = {}
    cooldowns = snapshot.get("active_provider_cooldowns")
    if not isinstance(cooldowns, Mapping):
        cooldowns = {}

    names = sorted(set(progress_by_name) | set(eta_by_name))
    result: list[dict[str, Any]] = []
    for provider in names:
        progress = progress_by_name.get(provider, {})
        eta = eta_by_name.get(provider, {})
        runtime = scheduler_providers.get(provider, {})
        runtime = runtime if isinstance(runtime, Mapping) else {}
        cooldown = cooldowns.get(provider, {})
        cooldown = cooldown if isinstance(cooldown, Mapping) else {}
        cooldown_until = _datetime(cooldown.get("until"))
        cooldown_active = bool(
            runtime.get("cooldown")
            or (cooldown_until is not None and cooldown_until > now)
        )
        result.append(
            {
                "provider": provider,
                "accepted_tasks": _integer(progress.get("accepted_tasks")),
                "success_rows": _integer(progress.get("rows")),
                "eligible_backlog_tasks": _integer(eta.get("eligible_backlog_tasks")),
                "exclusive_backlog_tasks": _integer(eta.get("exclusive_backlog_tasks")),
                "recent_tasks_per_minute": round(
                    _finite(eta.get("recent_tasks_per_minute"), 0.0) or 0.0, 6
                ),
                "requests_per_second": round(
                    _finite(
                        runtime.get("requests_per_second"),
                        _finite(eta.get("requests_per_second"), 0.0),
                    )
                    or 0.0,
                    6,
                ),
                "configured_concurrency": _integer(
                    runtime.get("execution_limit", eta.get("configured_concurrency"))
                ),
                "active": _integer(runtime.get("active")),
                "cooldown": cooldown_active,
                "cooldown_until": (
                    cooldown_until.isoformat()
                    if cooldown_active and cooldown_until is not None
                    else None
                ),
                "cooldown_kind": _safe_phase(cooldown.get("kind"))
                if cooldown_active
                else None,
            }
        )
    return sorted(
        result,
        key=lambda row: (
            -row["eligible_backlog_tasks"],
            -row["accepted_tasks"],
            row["provider"],
        ),
    )


def _public_alerts(snapshot: Mapping[str, Any]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    raw_alerts = snapshot.get("alerts")
    if not isinstance(raw_alerts, list):
        return result
    for raw in raw_alerts[:50]:
        if not isinstance(raw, Mapping):
            continue
        code = _name(raw.get("code"))
        if code is None:
            continue
        severity = str(raw.get("severity") or "warning").lower()
        if severity not in {"info", "warning", "critical"}:
            severity = "warning"
        result.append(
            {
                "severity": severity,
                "code": code,
                "message": _ALERT_MESSAGES.get(
                    code, "偵測到需要檢查的資料完整性條件。"
                ),
            }
        )
    return result


def build_openbb_public_status(
    repo_root: Path, *, now: datetime | None = None
) -> dict[str, Any]:
    """Build a fail-closed public snapshot without exposing raw errors or IDs."""

    current = (now or datetime.now(UTC)).astimezone(UTC)
    output_dir = Path(repo_root) / "data_openBB"
    state_dir = output_dir / "_state"
    snapshot = _read_json_object(state_dir / "monitor_latest.json")
    scheduler = _read_json_object(state_dir / "provider_scheduler.json")
    downloader_phase = _read_json_object(state_dir / "downloader_phase.json")
    snapshot_age = _age_seconds(snapshot.get("checked_at"), current)
    scheduler_age = _age_seconds(scheduler.get("updated_at"), current)
    phase_age = _age_seconds(downloader_phase.get("updated_at"), current)
    scheduler_current = (
        scheduler_age is not None and scheduler_age <= PROCESS_ACTIVITY_STALE_SECONDS
    )
    phase_current = (
        phase_age is not None and phase_age <= PROCESS_ACTIVITY_STALE_SECONDS
    )
    snapshot_state = (
        "missing"
        if snapshot_age is None
        else "current"
        if snapshot_age <= FULL_SNAPSHOT_STALE_SECONDS
        else "stale"
    )

    supervisor_alive = _pid_alive(
        state_dir / "supervisor.pid", ("run_openbb_archive_supervisor.sh",)
    )
    downloader_alive = _pid_alive(
        state_dir / "downloader.pid",
        (
            "downloader.run_openbb_archive",
            "downloader/download_openbb_archive.py",
            "run_openbb_archive.py",
        ),
    )
    activity_at = _activity_timestamp(state_dir, scheduler, downloader_phase)
    activity_age = _age_seconds(
        activity_at.isoformat() if activity_at else None, current
    )
    complete = bool(snapshot.get("complete"))
    if complete:
        health = "complete"
    elif supervisor_alive and downloader_alive:
        health = (
            "active"
            if activity_age is not None
            and activity_age <= PROCESS_ACTIVITY_STALE_SECONDS
            else "degraded"
        )
    elif supervisor_alive:
        health = (
            "starting"
            if activity_age is not None
            and activity_age <= PROCESS_ACTIVITY_STALE_SECONDS
            else "degraded"
        )
    else:
        health = "stopped"
    phase = (
        _safe_phase(downloader_phase.get("phase"))
        if phase_current
        else _safe_phase(scheduler.get("phase"))
        if scheduler_current
        else "initializing"
        if supervisor_alive or downloader_alive
        else "unknown"
    )

    total = _integer(snapshot.get("total_tasks"))
    accepted = _integer(snapshot.get("accepted_tasks"))
    archive_end = snapshot.get("archive_end_date")
    try:
        archive_end = datetime.fromisoformat(str(archive_end)).date().isoformat()
    except (TypeError, ValueError):
        archive_end = None
    status_counts = snapshot.get("status_counts")
    if not isinstance(status_counts, Mapping):
        status_counts = {}
    disk_free = None
    try:
        disk_free = shutil.disk_usage(output_dir).free
    except OSError:
        pass
    min_free = _integer(snapshot.get("min_free_bytes"), 100 * 1024**3)

    return {
        "schema_version": OPENBB_PUBLIC_SCHEMA_VERSION,
        "generated_at_utc": current.isoformat(),
        "read_only": True,
        "production_control_possible": False,
        "health": health,
        "audit_health": _safe_phase(snapshot.get("health")),
        "complete": complete,
        "snapshot_state": snapshot_state,
        "source_updated_at": (
            _datetime(snapshot.get("checked_at")).isoformat()
            if _datetime(snapshot.get("checked_at")) is not None
            else None
        ),
        "source_age_seconds": snapshot_age,
        "process": {
            "supervisor_alive": supervisor_alive,
            "downloader_alive": downloader_alive,
            "phase": phase,
            "activity_updated_at": activity_at.isoformat() if activity_at else None,
            "activity_age_seconds": activity_age,
            "scheduler_age_seconds": scheduler_age,
            "phase_age_seconds": phase_age,
            "phase_completed": _integer(downloader_phase.get("endpoints_completed"))
            if phase_current
            else 0,
            "phase_total": _integer(downloader_phase.get("endpoints_total"))
            if phase_current
            else 0,
            "generated_tasks": _integer(downloader_phase.get("generated_tasks"))
            if phase_current
            else 0,
            "auto_start_service": "stockagent-openbb-archive.service",
        },
        "archive": {
            "start_date": "2000-01-01",
            "end_date": archive_end,
            "completion_percent": round(
                _finite(snapshot.get("completion_percent"), 0.0) or 0.0, 6
            ),
            "accepted_tasks": accepted,
            "total_tasks": total,
            "resolved_tasks": _integer(snapshot.get("resolved_tasks")),
            "unresolved_tasks": _integer(snapshot.get("unresolved_tasks")),
            "actionable_unresolved_tasks": _integer(
                snapshot.get("actionable_unresolved_tasks")
            ),
            "retryable_tasks": _integer(snapshot.get("retryable_tasks")),
            "unavailable_tasks": _integer(snapshot.get("unavailable_tasks")),
            "success_rows": _integer(snapshot.get("success_rows")),
            "endpoint_count": _integer(snapshot.get("endpoint_count")),
            "status_counts": {
                key: _integer(status_counts.get(key))
                for key in (
                    "success",
                    "empty",
                    "pending",
                    "running",
                    "failed",
                    "unavailable",
                )
            },
        },
        "recent": {
            "accepted_tasks_last_15m": _integer(
                snapshot.get("accepted_tasks_last_15m")
            ),
            "success_rows_last_15m": _integer(snapshot.get("success_rows_last_15m")),
            "tasks_per_minute_last_15m": round(
                _finite(snapshot.get("tasks_per_minute_last_15m"), 0.0) or 0.0,
                6,
            ),
            "minutes_since_last_accepted": _finite(
                snapshot.get("minutes_since_last_accepted")
            ),
        },
        "storage": {
            "free_bytes": disk_free,
            "minimum_free_bytes": min_free,
            "above_safety_floor": disk_free is not None and disk_free >= min_free,
        },
        "categories": _category_rows(snapshot),
        "providers": _provider_rows(
            snapshot, scheduler if scheduler_current else {}, current
        ),
        "alerts": _public_alerts(snapshot),
    }


_RANGE_DELTAS = {
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
    "1w": timedelta(days=7),
    "1mo": timedelta(days=31),
    "1q": timedelta(days=92),
    "1y": timedelta(days=366),
}


def build_openbb_public_history(
    repo_root: Path, range_key: str, *, now: datetime | None = None
) -> dict[str, Any]:
    """Read the compact trend ledger and return a bounded time range."""

    if range_key not in {*_RANGE_DELTAS, "all"}:
        raise ValueError("unsupported OpenBB history range")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    cutoff = current - _RANGE_DELTAS[range_key] if range_key != "all" else None
    state_dir = Path(repo_root) / "data_openBB" / "_state"
    rows: deque[dict[str, Any]] = deque(maxlen=MAX_HISTORY_ROWS)
    history_path = state_dir / "monitor_dashboard_history.jsonl"
    try:
        handle = history_path.open("r", encoding="utf-8")
    except OSError:
        handle = None
    if handle is not None:
        with handle:
            for line in handle:
                if len(line) > 32_768:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(raw, Mapping):
                    continue
                row = project_openbb_history_row(raw)
                checked_at = _datetime(row.get("checked_at"))
                if (
                    row
                    and checked_at is not None
                    and (cutoff is None or checked_at >= cutoff)
                ):
                    rows.append(row)
    if not rows:
        latest = project_openbb_history_row(
            _read_json_object(state_dir / "monitor_latest.json")
        )
        checked_at = _datetime(latest.get("checked_at"))
        if (
            latest
            and checked_at is not None
            and (cutoff is None or checked_at >= cutoff)
        ):
            rows.append(latest)
    return {
        "schema_version": OPENBB_PUBLIC_SCHEMA_VERSION,
        "generated_at_utc": current.isoformat(),
        "read_only": True,
        "range": range_key,
        "history": list(rows),
    }
