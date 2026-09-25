#!/usr/bin/env python3
"""Persist and repair the unattended TW day-trade operational contract.

This guardian does not download data, infer a model, create a signal, or own a
broker connection.  It checks the receipts produced by those canonical
components and only re-arms their existing systemd units.  Active Shioaji and
Discord processes are never restarted by this script because their clients are
process-local and a restart would discard warm contracts and subscriptions.
"""

from __future__ import annotations

import argparse
from datetime import datetime, time as datetime_time
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any
from urllib.request import urlopen
import uuid
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import DEFAULT_DATASETS  # noqa: E402
from scripts.check_tw_day_trade_preopen_readiness import _session_contract  # noqa: E402
from stockagent.live.market_config import enabled_day_trade_markets  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
EXPECTED_MARKETS = enabled_day_trade_markets(
    REPO_ROOT / "services/discord_bot/markets"
)
REQUIRED_SERVICES = (
    "stockagent-tw-day-trade-simulation.service",
    "stockagent-discord-bot.service",
    "stockagent-tw-public-source-events.service",
    "stockagent-public-dashboards.service",
)
REQUIRED_TIMERS = (
    "stockagent-time-sync-check.timer",
    "stockagent-tw-day-trade-eligibility.timer",
    "stockagent-tw-public-publication-sweep.timer",
    "stockagent-tw-public-0830-check.timer",
    "stockagent-tw-day-trade-preopen-gate.timer",
    "stockagent-discord-artifact-maintenance.timer",
    "stockagent-tw-day-trade-unattended-guardian.timer",
    "stockagent-tw-day-trade-minute-curves.timer",
    "stockagent-tw-day-trade-margin-actions.timer",
)
BEST_EFFORT_MAINTENANCE_UNITS = {
    "stockagent-openbb-archive.service": (
        "stockagent-openbb-archive.timer"
    ),
    "stockagent-registered-data-backfill.service": (
        "stockagent-registered-data-backfill.timer"
    ),
    "stockagent-registered-data-daily.service": (
        "stockagent-registered-data-daily.timer"
    ),
    "stockagent-registered-data-intraday.service": (
        "stockagent-registered-data-intraday.timer"
    ),
}
BEST_EFFORT_MAINTENANCE_SERVICES = tuple(BEST_EFFORT_MAINTENANCE_UNITS)
OPENING_RESOURCE_GUARD_START = datetime_time(8, 20)
OPENING_RESOURCE_GUARD_END = datetime_time(9, 10)
CAUSAL_LIVE_ENTRY_FILL_POLICIES = frozenset(
    {
        "causal_best_quote",
        "causal_market_full_target_at_best_quote",
    }
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-root",
        type=Path,
        default=Path("artifacts/operations/tw_day_trade_guardian"),
    )
    parser.add_argument("--no-repair", action="store_true")
    parser.add_argument("--action-cooldown-seconds", type=float, default=300.0)
    parser.add_argument("--minimum-free-gib", type=float, default=5.0)
    parser.add_argument("--minimum-free-percent", type=float, default=5.0)
    parser.add_argument("--warning-free-percent", type=float, default=15.0)
    parser.add_argument("--observed-at", default=None)
    return parser.parse_args()


def _accepted_margin_residual(mode: dict[str, Any], session_date: str) -> bool:
    from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT, STRICT_INTRADAY_CONTRACT
    from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
    count = int(mode.get("open_position_count") or 0)
    if mode.get("configured_intraday_contract") == STRICT_INTRADAY_CONTRACT or mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT:
        # Exceptional risk is still degraded, never an unattended-health pass.
        return False
    receipt = mode.get("margin_corporate_action_receipt") or {}
    return bool(count > 0 and mode.get("session_date") == session_date
                and mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                and mode.get("odd_lot_execution_policy") == ODD_LOT_BOARD_PRICE
                and int(mode.get("margin_carry_position_count") or 0) == count
                and mode.get("valuation_complete") is True
                and str(mode.get("closing_auction_settled_at") or "").startswith(session_date)
                and str(mode.get("residual_conversion_completed_at") or "").startswith(session_date)
                and receipt.get("status") != "blocked")


def _repo_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _prune_managed_runs(
    directory: Path, *, max_files: int = 4096, max_age_days: int = 90
) -> None:
    """Bound only guardian-generated observation receipts."""

    try:
        rows = sorted(
            (path for path in directory.glob("*.json") if path.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return
    cutoff = time.time() - max_age_days * 86400
    for index, path in enumerate(rows):
        try:
            if index >= max_files or path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except OSError:
            continue


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=TAIPEI)


def _age_seconds(value: Any, observed: datetime) -> float | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return max(0.0, (observed - parsed.astimezone(TAIPEI)).total_seconds())


def _classify_session_signals(
    modes: dict[str, Any], *, session_date: str
) -> tuple[list[str], list[str]]:
    """Separate a missing commit from a completed non-live recovery.

    A retrospective official-open/09:01 replay is a durable current-session
    signal, but it did not meet the causal 09:00 live-execution contract.  The
    guardian must preserve both facts instead of reporting the replay as if no
    signal had been committed at all.
    """

    missing_markets: list[str] = []
    noncausal_recovery_markets: list[str] = []
    for market in EXPECTED_MARKETS:
        row = modes.get(market) or {}
        if (
            row.get("session_date") != session_date
            or not row.get("signal_id")
            or not row.get("entry_completed_at")
        ):
            missing_markets.append(market)
            continue
        if (
            row.get("entry_fill_policy") not in CAUSAL_LIVE_ENTRY_FILL_POLICIES
            or int(row.get("entry_price_offset_ticks") or 0) != 0
        ):
            noncausal_recovery_markets.append(market)
    return missing_markets, noncausal_recovery_markets


def _systemctl_show(unit: str) -> dict[str, str]:
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property="
            "LoadState,ActiveState,SubState,UnitFileState,Result,NRestarts,"
            "MemoryCurrent,MemoryHigh,MemoryMax,TasksCurrent,TasksMax",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    rows: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            rows[key] = value
    rows["show_returncode"] = str(completed.returncode)
    return rows


def _unit_resource_pressure(row: dict[str, str]) -> dict[str, Any]:
    def integer(name: str) -> int | None:
        try:
            return int(row.get(name, ""))
        except (TypeError, ValueError):
            return None

    memory_current = integer("MemoryCurrent")
    memory_high = integer("MemoryHigh")
    memory_max = integer("MemoryMax")
    tasks_current = integer("TasksCurrent")
    tasks_max = integer("TasksMax")
    memory_high_ratio = (
        memory_current / memory_high
        if memory_current is not None and memory_high not in {None, 0}
        else None
    )
    memory_max_ratio = (
        memory_current / memory_max
        if memory_current is not None and memory_max not in {None, 0}
        else None
    )
    tasks_ratio = (
        tasks_current / tasks_max
        if tasks_current is not None and tasks_max not in {None, 0}
        else None
    )
    return {
        "memory_current_bytes": memory_current,
        "memory_high_bytes": memory_high,
        "memory_max_bytes": memory_max,
        "memory_high_ratio": (
            round(memory_high_ratio, 4) if memory_high_ratio is not None else None
        ),
        "memory_max_ratio": (
            round(memory_max_ratio, 4) if memory_max_ratio is not None else None
        ),
        "tasks_current": tasks_current,
        "tasks_max": tasks_max,
        "tasks_ratio": round(tasks_ratio, 4) if tasks_ratio is not None else None,
        "warning": bool(
            (memory_high_ratio is not None and memory_high_ratio >= 0.8)
            or (memory_max_ratio is not None and memory_max_ratio >= 0.9)
            or (tasks_ratio is not None and tasks_ratio >= 0.8)
        ),
    }


def _run_systemctl(*arguments: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["systemctl", *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return {
        "command": ["systemctl", *arguments],
        "returncode": int(completed.returncode),
        "stdout": completed.stdout[-1000:],
        "stderr": completed.stderr[-1000:],
    }


def _cooldown_ready(
    action_key: str,
    *,
    action_state: dict[str, Any],
    now_monotonic_wall: float,
    cooldown_seconds: float,
) -> bool:
    try:
        prior = float((action_state.get(action_key) or {}).get("attempted_at_epoch"))
    except (AttributeError, TypeError, ValueError):
        return True
    return now_monotonic_wall - prior >= cooldown_seconds


def _record_action(
    action_key: str,
    result: dict[str, Any],
    *,
    action_state: dict[str, Any],
    observed: datetime,
) -> None:
    action_state[action_key] = {
        "attempted_at_epoch": time.time(),
        "attempted_at_taipei": observed.isoformat(timespec="seconds"),
        "returncode": result.get("returncode"),
    }


def _repair_unit(
    unit: str,
    *,
    timer: bool,
    repair: bool,
    action_state: dict[str, Any],
    observed: datetime,
    cooldown_seconds: float,
) -> list[dict[str, Any]]:
    if not repair:
        return []
    key = f"enable-start:{unit}"
    if not _cooldown_ready(
        key,
        action_state=action_state,
        now_monotonic_wall=time.time(),
        cooldown_seconds=cooldown_seconds,
    ):
        return []
    actions: list[dict[str, Any]] = []
    reset = _run_systemctl("reset-failed", unit)
    reset["action_key"] = key
    actions.append(reset)
    start_args = ("enable", "--now", unit) if timer else (
        "enable",
        "--now",
        unit,
    )
    started = _run_systemctl(*start_args)
    started["action_key"] = key
    actions.append(started)
    _record_action(key, started, action_state=action_state, observed=observed)
    return actions


def _trigger_oneshot(
    unit: str,
    *,
    repair: bool,
    action_state: dict[str, Any],
    observed: datetime,
    cooldown_seconds: float,
) -> list[dict[str, Any]]:
    if not repair:
        return []
    key = f"start:{unit}"
    if not _cooldown_ready(
        key,
        action_state=action_state,
        now_monotonic_wall=time.time(),
        cooldown_seconds=cooldown_seconds,
    ):
        return []
    result = _run_systemctl("start", "--no-block", unit)
    result["action_key"] = key
    _record_action(key, result, action_state=action_state, observed=observed)
    return [result]


def _clear_receipted_oneshot_failure(
    unit: str,
    *,
    repair: bool,
    action_state: dict[str, Any],
    observed: datetime,
    cooldown_seconds: float,
) -> list[dict[str, Any]]:
    """Clear only systemd's latch; the immutable incident receipt is retained."""

    if not repair:
        return []
    key = f"reset-receipted:{unit}"
    if not _cooldown_ready(
        key,
        action_state=action_state,
        now_monotonic_wall=time.time(),
        cooldown_seconds=cooldown_seconds,
    ):
        return []
    result = _run_systemctl("reset-failed", unit)
    result["action_key"] = key
    result["incident_receipt_retained"] = True
    _record_action(key, result, action_state=action_state, observed=observed)
    return [result]


def _protect_opening_resources(
    *,
    observed: datetime,
    repair: bool,
    action_state: dict[str, Any],
    action_path: Path | None = None,
    session_open: bool | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    """Pause only resumable bulk jobs across the opening critical path."""

    def persist_action_state() -> None:
        if action_path is not None:
            _atomic_json(action_path, action_state)

    def is_running(row: dict[str, Any]) -> bool:
        # Type=oneshot remains ``activating/start`` for its entire long run.
        # Looking only for ``active`` misses exactly the bulk jobs we need to
        # drain before the opening critical path.
        return row.get("ActiveState") in {"active", "activating", "reloading"}

    wall = observed.timetz().replace(tzinfo=None)
    protected = bool(
        (observed.weekday() < 5 if session_open is None else session_open)
        and OPENING_RESOURCE_GUARD_START <= wall < OPENING_RESOURCE_GUARD_END
    )
    session_date = observed.date().isoformat()
    actions: list[dict[str, Any]] = []
    failures: list[str] = []
    units: dict[str, Any] = {}
    for service_unit, timer_unit in BEST_EFFORT_MAINTENANCE_UNITS.items():
        service_row = _systemctl_show(service_unit)
        timer_row = _systemctl_show(timer_unit)
        key = f"opening-resource-pause:{service_unit}"
        pause = action_state.get(key)
        pause = dict(pause) if isinstance(pause, dict) else {}
        pending = bool(pause.get("resume_pending"))
        service_was_active = is_running(service_row)
        timer_was_active = is_running(timer_row)
        if protected and (service_was_active or timer_was_active) and not pending:
            if repair:
                targets = [
                    unit
                    for unit in (
                        timer_unit if timer_was_active else None,
                        service_unit if service_was_active else None,
                    )
                    if unit is not None
                ]
                # Write the recovery intent before mutating systemd.  If this
                # guardian is killed after stopping a timer, the next minute or
                # next boot can still resume exactly what used to be active.
                action_state[key] = {
                    "paused_at_taipei": observed.isoformat(timespec="seconds"),
                    "session_date": session_date,
                    "resume_pending": True,
                    "service_was_active": service_was_active,
                    "timer_was_active": timer_was_active,
                    "stop_requested_units": targets,
                    "stopped_units": [],
                }
                persist_action_state()
                requested: list[str] = []
                succeeded = True
                # Stop the controller first.  In particular, the intraday
                # timer otherwise starts its service again one minute later.
                for unit in targets:
                    result = _run_systemctl("stop", "--no-block", unit)
                    result.update(
                        {
                            "action_key": key,
                            "reason": "protect_0820_0910_tw_opening_resources",
                        }
                    )
                    actions.append(result)
                    requested.append(unit)
                    succeeded = succeeded and result.get("returncode") == 0
                action_state[key]["stopped_units"] = requested
                action_state[key]["stop_succeeded"] = succeeded
                persist_action_state()
                pending = True
                if not succeeded:
                    failures.append(
                        f"failed to pause opening competitor: {service_unit}"
                    )
        elif protected and pending and repair:
            # A no-block stop can still be draining at the next heartbeat.
            # Keep the controller down and retry the bounded stop instead of
            # assuming the first request completed.
            for unit, row in (
                (timer_unit, timer_row),
                (service_unit, service_row),
            ):
                if not is_running(row):
                    continue
                result = _run_systemctl("stop", "--no-block", unit)
                result.update(
                    {
                        "action_key": key,
                        "reason": "continue_opening_resource_pause",
                    }
                )
                actions.append(result)
                if result.get("returncode") != 0:
                    failures.append(
                        f"failed to keep opening competitor stopped: {unit}"
                    )
        elif not protected and pending:
            if repair:
                requested = []
                succeeded = True
                # Resume the interrupted work before its scheduler.  Starting
                # the timer first can race OnUnitInactiveSec and double-trigger.
                for unit, was_active, row in (
                    (
                        service_unit,
                        bool(pause.get("service_was_active")),
                        service_row,
                    ),
                    (
                        timer_unit,
                        bool(pause.get("timer_was_active")),
                        timer_row,
                    ),
                ):
                    if not was_active or is_running(row):
                        continue
                    result = _run_systemctl("start", "--no-block", unit)
                    result.update(
                        {
                            "action_key": key,
                            "reason": "resume_after_tw_opening_resource_guard",
                        }
                    )
                    actions.append(result)
                    requested.append(unit)
                    succeeded = succeeded and result.get("returncode") == 0
                if succeeded:
                    action_state[key] = {
                        **pause,
                        "resumed_at_taipei": observed.isoformat(timespec="seconds"),
                        "resume_pending": False,
                        "started_units": requested,
                    }
                    persist_action_state()
                    pending = False
                else:
                    persist_action_state()
                    failures.append(
                        f"failed to resume opening competitor: {service_unit}"
                    )
        units[service_unit] = {
            "active_state": service_row.get("ActiveState"),
            "sub_state": service_row.get("SubState"),
            "result": service_row.get("Result"),
            "restart_count": service_row.get("NRestarts"),
            "timer": timer_unit,
            "timer_active_state": timer_row.get("ActiveState"),
            "timer_sub_state": timer_row.get("SubState"),
            "resume_pending": pending,
        }
    return (
        {
            "protected": protected,
            "window": "08:20:00..09:10:00 Asia/Taipei",
            "policy": "pause_and_resume_only_resumable_best_effort_maintenance",
            "units": units,
        },
        actions,
        failures,
    )


def _run_time_check(*, repair: bool) -> dict[str, Any]:
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts/check_stockagent_time_sync.py"),
    ]
    if repair:
        command.append("--repair")
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    receipt = _json(REPO_ROOT / "artifacts/operations/time_sync/latest.json")
    return {
        "returncode": int(completed.returncode),
        "ready": completed.returncode == 0 and receipt.get("ready") is True,
        "receipt": receipt,
        "stderr": completed.stderr[-1000:],
    }


def _public_endpoint(path: str) -> dict[str, Any]:
    port = int(os.getenv("STOCKAGENT_PUBLIC_DASHBOARD_PORT", "8770"))
    url = f"http://127.0.0.1:{port}{path}"
    try:
        with urlopen(url, timeout=3) as response:  # noqa: S310 - localhost only
            value = json.loads(response.read())
        return {
            "ready": isinstance(value, dict),
            "status_code": 200,
            "url": url,
            "payload": value if isinstance(value, dict) else {},
        }
    except Exception as exc:
        return {
            "ready": False,
            "url": url,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _disk_health(
    path: Path,
    *,
    minimum_gib: float,
    minimum_percent: float,
    warning_percent: float,
) -> dict[str, Any]:
    try:
        usage = shutil.disk_usage(path)
    except OSError as exc:
        return {
            "path": str(path),
            "ready": False,
            "error": f"{type(exc).__name__}: {exc}",
            "policy": {
                "minimum_free_gib": minimum_gib,
                "minimum_free_percent": minimum_percent,
                "warning_free_percent": warning_percent,
                "automatic_deletion": False,
            },
        }
    free_gib = usage.free / (1024**3)
    free_percent = (usage.free / usage.total * 100.0) if usage.total else 0.0
    return {
        "path": str(path),
        "free_gib": round(free_gib, 3),
        "free_percent": round(free_percent, 3),
        "ready": free_gib >= minimum_gib and free_percent >= minimum_percent,
        "warning": free_percent < warning_percent,
        "policy": {
            "minimum_free_gib": minimum_gib,
            "minimum_free_percent": minimum_percent,
            "warning_free_percent": warning_percent,
            "automatic_deletion": False,
        },
    }


def main() -> int:
    args = parse_args()
    observed = (
        datetime.fromisoformat(args.observed_at).astimezone(TAIPEI)
        if args.observed_at
        else datetime.now(TAIPEI)
    )
    state_root = _repo_path(args.state_root)
    state_root.mkdir(parents=True, exist_ok=True)
    lock_path = state_root / "guardian.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0

        repair = not bool(args.no_repair)
        action_path = state_root / "action_state.json"
        action_state = _json(action_path)
        actions: list[dict[str, Any]] = []
        failures: list[str] = []
        warnings: list[str] = []
        try:
            session_state, session_reason, session_markets = _session_contract(observed)
        except (OSError, RuntimeError, ValueError) as exc:
            session_state = "unknown"
            session_reason = (
                f"market session contract failed: {type(exc).__name__}: {exc}"
            )
            session_markets = ()
        session_open = session_state == "open"
        if session_state == "unknown":
            failures.append("TWSE session calendar is not verified: " + session_reason)

        time_health = _run_time_check(repair=repair)
        if not time_health["ready"]:
            failures.append("schedule clock is not verified")

        opening_resource_guard, resource_actions, resource_failures = (
            _protect_opening_resources(
                observed=observed,
                repair=repair,
                action_state=action_state,
                action_path=action_path,
                session_open=session_state != "closed",
            )
        )
        actions.extend(resource_actions)
        failures.extend(resource_failures)
        failed_maintenance = [
            unit
            for unit, row in opening_resource_guard["units"].items()
            if row.get("active_state") == "failed"
        ]
        if failed_maintenance:
            warnings.append(
                "best-effort data maintenance is failed but isolated from the "
                "opening path: " + ",".join(failed_maintenance)
            )

        services: dict[str, Any] = {}
        for unit in REQUIRED_SERVICES:
            row = _systemctl_show(unit)
            services[unit] = row
            if row.get("ActiveState") != "active":
                failures.append(f"required service is inactive: {unit}")
                actions.extend(
                    _repair_unit(
                        unit,
                        timer=False,
                        repair=repair,
                        action_state=action_state,
                        observed=observed,
                        cooldown_seconds=float(args.action_cooldown_seconds),
                    )
                )

        restarted_services = [
            unit
            for unit, row in services.items()
            if str(row.get("NRestarts") or "0").isdigit()
            and int(row.get("NRestarts") or 0) > 0
        ]
        if restarted_services:
            warnings.append(
                "required services have auto-restarted in the current activation: "
                + ",".join(restarted_services)
            )
        service_resource_pressure = {
            unit: _unit_resource_pressure(row) for unit, row in services.items()
        }
        pressured_services = [
            unit
            for unit, row in service_resource_pressure.items()
            if row.get("warning")
        ]
        if pressured_services:
            warnings.append(
                "required services are approaching cgroup resource bounds: "
                + ",".join(pressured_services)
            )

        timers: dict[str, Any] = {}
        for unit in REQUIRED_TIMERS:
            row = _systemctl_show(unit)
            timers[unit] = row
            if row.get("ActiveState") != "active" or row.get("UnitFileState") not in {
                "enabled",
                "static",
            }:
                failures.append(f"required weekly timer is not armed: {unit}")
                actions.extend(
                    _repair_unit(
                        unit,
                        timer=True,
                        repair=repair,
                        action_state=action_state,
                        observed=observed,
                        cooldown_seconds=float(args.action_cooldown_seconds),
                    )
                )

        event_receipt = _json(
            REPO_ROOT / "artifacts/data_refresh/tw_public/events/latest.json"
        )
        event_age = _age_seconds(event_receipt.get("updated_at_taipei"), observed)
        event_blocking = int(
            (
                event_receipt.get("blocking_unapplied_event_count")
                if "blocking_unapplied_event_count" in event_receipt
                else event_receipt.get("unapplied_event_count")
            )
            or 0
        )
        event_ready = bool(
            event_receipt.get("coverage_complete") is True
            and int(event_receipt.get("registered_dataset_count") or -1)
            == len(DEFAULT_DATASETS)
            and int(event_receipt.get("observed_dataset_count") or -1)
            == len(DEFAULT_DATASETS)
            and int(event_receipt.get("failed_probe_count") or 0) == 0
            and event_blocking == 0
            and event_age is not None
            and event_age <= 180.0
        )
        if not event_ready:
            warnings.append("TW public source-event receipt is stale or degraded")

        wall = observed.timetz().replace(tzinfo=None)
        session_date = observed.date().isoformat()
        eligibility = _json(
            REPO_ROOT / "artifacts/data_refresh/tw_day_trade_eligibility/latest.json"
        )
        eligibility_ready = bool(
            eligibility.get("status") == "ok"
            and eligibility.get("trading_date") == session_date
        )
        if session_open and datetime_time(5, 30) <= wall <= datetime_time(10, 0):
            if not eligibility_ready:
                warnings.append("same-session TWSE/TPEx eligibility is not accepted")
                actions.extend(
                    _trigger_oneshot(
                        "stockagent-tw-day-trade-eligibility.service",
                        repair=repair,
                        action_state=action_state,
                        observed=observed,
                        cooldown_seconds=float(args.action_cooldown_seconds),
                    )
                )

        public_acceptance = _json(
            REPO_ROOT / "artifacts/data_refresh/tw_public/0830/latest.json"
        )
        public_started = str(public_acceptance.get("started_at_taipei") or "")[:10]
        public_ready = bool(
            public_acceptance.get("status") == "ok"
            and public_started == session_date
            and (public_acceptance.get("acceptance") or {}).get(
                "live_root_receipt_fresh"
            )
            is True
        )
        if session_open and datetime_time(8, 0) <= wall <= datetime_time(10, 0):
            if not public_ready:
                warnings.append("08:30 TW public-data acceptance is not ready")
                actions.extend(
                    _trigger_oneshot(
                        "stockagent-tw-public-0830-check.service",
                        repair=repair,
                        action_state=action_state,
                        observed=observed,
                        cooldown_seconds=float(args.action_cooldown_seconds),
                    )
                )

        # The gate intentionally exits nonzero when the 09:00 SLO was missed.
        # After its last catch-up point, retain that truth in the gate receipt
        # and dashboard but clear systemd's failed latch so boot-health checks
        # do not confuse a receipted historical incident with a dead daemon.
        preopen_unit = "stockagent-tw-day-trade-preopen-gate.service"
        preopen_systemd = _systemctl_show(preopen_unit)
        if wall >= datetime_time(10, 5) and preopen_systemd.get("ActiveState") == "failed":
            actions.extend(
                _clear_receipted_oneshot_failure(
                    preopen_unit,
                    repair=repair,
                    action_state=action_state,
                    observed=observed,
                    cooldown_seconds=float(args.action_cooldown_seconds),
                )
            )

        engine_sync_path = (
            REPO_ROOT / "artifacts/live/tw_day_trade_simulation/service_sync.json"
        )
        discord_path = REPO_ROOT / "artifacts/discord_bot/service_status.json"
        engine_sync: dict[str, Any] = {}
        discord_status: dict[str, Any] = {}
        revision_lag: int | None = None
        for attempt in range(3):
            engine_sync = _json(engine_sync_path)
            discord_status = _json(discord_path)
            revision_lag = int(engine_sync.get("state_revision") or 0) - int(
                discord_status.get("engine_state_revision") or 0
            )
            if revision_lag == 0:
                break
            if attempt < 2:
                time.sleep(1.0)
        engine_age = _age_seconds(
            engine_sync.get("heartbeat_at") or engine_sync.get("published_at"),
            observed,
        )
        discord_age = _age_seconds(discord_status.get("updated_at"), observed)
        modes = engine_sync.get("modes")
        modes = dict(modes) if isinstance(modes, dict) else {}
        runtime_ready = bool(
            engine_sync.get("simulation_only") is True
            and engine_sync.get("production_order_possible") is False
            and engine_sync.get("ledger_integrity_ready") is True
            and set(engine_sync.get("enabled_markets") or ()) == set(EXPECTED_MARKETS)
            and discord_status.get("simulation_only") is True
            and discord_status.get("production_order_possible") is False
            and discord_status.get("discord_connected") is True
            and set(discord_status.get("day_trade_markets") or ())
            == set(EXPECTED_MARKETS)
            and set(discord_status.get("scheduled_day_trade_markets") or ())
            == set(EXPECTED_MARKETS)
            and engine_age is not None
            and engine_age <= 20.0
            and discord_age is not None
            and discord_age <= 20.0
            and revision_lag == 0
        )
        if not runtime_ready:
            failures.append("paper engine and Discord revisions are not synchronized")
        maintenance = discord_status.get("background_maintenance")
        maintenance = dict(maintenance) if isinstance(maintenance, dict) else {}
        maintenance_ready = bool(
            maintenance.get("status") == "ready"
            and int(maintenance.get("failed_count") or 0) == 0
            and int(maintenance.get("running_count") or 0) == 0
        )
        maintenance_degraded = not maintenance_ready
        if maintenance_degraded:
            # Formal-history maintenance is intentionally a separate health
            # domain: make failure visible without declaring the Gateway or
            # the independent opening execution engine disconnected.
            warnings.append("post-close Discord artifact maintenance is not ready")

        if session_open and wall >= datetime_time(9, 0, 15):
            missing_signals, noncausal_recovery_markets = (
                _classify_session_signals(modes, session_date=session_date)
            )
            if missing_signals:
                failures.append(
                    "current-session signal commit missing: " + ",".join(missing_signals)
                )
            if noncausal_recovery_markets:
                failures.append(
                    "09:00 causal live execution SLA missed; current-session "
                    "signals recovered by non-live replay: "
                    + ",".join(noncausal_recovery_markets)
                )
        else:
            missing_signals = []
            noncausal_recovery_markets = []

        if session_open and wall >= datetime_time(13, 30):
            open_markets = [
                market
                for market in EXPECTED_MARKETS
                if int((modes.get(market) or {}).get("open_position_count") or 0) != 0
            ]
            accepted_carry_markets = [m for m in open_markets if _accepted_margin_residual(modes[m], session_date)]
            unaccepted_open_markets = [m for m in open_markets if m not in accepted_carry_markets]
            if unaccepted_open_markets:
                failures.append(
                    "post-close residuals violate the configured accounting contract: " + ",".join(unaccepted_open_markets)
                )
        else:
            open_markets = []
            accepted_carry_markets, unaccepted_open_markets = [], []

        dashboard_status = _public_endpoint("/tw-day-trade/api/status")
        dashboard_revision = _public_endpoint("/tw-day-trade/api/revision")
        public_surface_ready = bool(
            dashboard_status.get("ready")
            and dashboard_revision.get("ready")
            and (dashboard_status.get("payload") or {}).get("simulation_only") is True
            and (dashboard_status.get("payload") or {}).get(
                "production_order_possible"
            )
            is False
            and (dashboard_status.get("payload") or {})
            .get("ledger_integrity", {})
            .get("ready")
            is True
        )
        if not public_surface_ready:
            failures.append("read-only TW day-trade dashboard endpoint is unavailable")

        disks = {
            "repository": _disk_health(
                REPO_ROOT,
                minimum_gib=float(args.minimum_free_gib),
                minimum_percent=float(args.minimum_free_percent),
                warning_percent=float(args.warning_free_percent),
            ),
            "tw_public_live": _disk_health(
                Path("/srv/stockagent-live/data_tw_public"),
                minimum_gib=float(args.minimum_free_gib),
                minimum_percent=float(args.minimum_free_percent),
                warning_percent=float(args.warning_free_percent),
            ),
        }
        if any(not row["ready"] for row in disks.values()):
            failures.append("disk free-space guard is below threshold; no data was deleted")
        elif any(row.get("warning") for row in disks.values()):
            warnings.append(
                "disk free space is inside the early-warning band; no data was deleted"
            )

        if actions:
            status = "repairing"
        elif failures:
            status = "failed"
        elif warnings:
            status = "degraded"
        else:
            status = "ready"
        payload = {
            "schema_version": 1,
            "status": status,
            "ready": not failures and not warnings,
            "repair_enabled": repair,
            "simulation_only": True,
            "production_order_possible": False,
            "observed_at_taipei": observed.isoformat(timespec="milliseconds"),
            "session_date": session_date,
            "session_state": session_state,
            "session_reason": session_reason,
            "session_markets": list(session_markets),
            "expected_markets": list(EXPECTED_MARKETS),
            "failures": failures,
            "warnings": warnings,
            "actions": actions,
            "components": {
                "time_sync": time_health,
                "opening_resource_guard": opening_resource_guard,
                "services": services,
                "service_resource_pressure": service_resource_pressure,
                "weekly_timers": timers,
                "source_events": {
                    "ready": event_ready,
                    "age_seconds": event_age,
                    "blocking_unapplied_event_count": event_blocking,
                    "registered_dataset_count": event_receipt.get(
                        "registered_dataset_count"
                    ),
                    "observed_dataset_count": event_receipt.get(
                        "observed_dataset_count"
                    ),
                    "failed_probe_count": event_receipt.get("failed_probe_count"),
                },
                "eligibility": {
                    "ready": eligibility_ready,
                    "status": eligibility.get("status"),
                    "trading_date": eligibility.get("trading_date"),
                },
                "public_0830": {
                    "ready": public_ready,
                    "status": public_acceptance.get("status"),
                    "started_at_taipei": public_acceptance.get(
                        "started_at_taipei"
                    ),
                },
                "runtime_sync": {
                    "ready": runtime_ready,
                    "engine_age_seconds": engine_age,
                    "discord_age_seconds": discord_age,
                    "revision_lag": revision_lag,
                    "engine_run_id": engine_sync.get("engine_run_id"),
                    "enabled_markets": engine_sync.get("enabled_markets"),
                    "scheduled_day_trade_markets": discord_status.get(
                        "scheduled_day_trade_markets"
                    ),
                },
                "post_close_artifact_maintenance": {
                    "ready": not maintenance_degraded,
                    **maintenance,
                },
                "session_signals": {
                    "ready": not missing_signals and not noncausal_recovery_markets,
                    "committed": not missing_signals,
                    "live_sla_met": (
                        not missing_signals and not noncausal_recovery_markets
                    ),
                    "missing_markets": missing_signals,
                    "noncausal_recovery_markets": noncausal_recovery_markets,
                    "modes": modes,
                },
                "post_close_flat": {
                    "ready": not open_markets,
                    "open_markets": open_markets,
                },
                "post_close_accounting": {
                    "ready": not unaccepted_open_markets,
                    "accepted_assumed_margin_markets": accepted_carry_markets,
                    "unaccepted_open_markets": unaccepted_open_markets,
                    "exchange_fill_or_margin_approval_proven": False,
                },
                "public_dashboard": {
                    "ready": public_surface_ready,
                    "status_endpoint": {
                        key: value
                        for key, value in dashboard_status.items()
                        if key != "payload"
                    },
                    "revision_endpoint": {
                        key: value
                        for key, value in dashboard_revision.items()
                        if key != "payload"
                    },
                },
                "disks": disks,
            },
        }
        _atomic_json(action_path, action_state)
        _atomic_json(state_root / "latest.json", payload)
        # A minute heartbeat belongs in ``latest``.  Preserve an immutable run
        # only on the hourly boundary or when it contains an incident/action.
        if observed.minute == 0 or status != "ready" or actions:
            run_id = observed.strftime("%Y%m%dT%H%M%S%f")
            runs_dir = state_root / "runs"
            _atomic_json(runs_dir / f"{run_id}.json", payload)
            _prune_managed_runs(runs_dir)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
