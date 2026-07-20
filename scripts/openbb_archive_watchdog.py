#!/usr/bin/env python3
"""Read the small atomic state used by the OpenBB archive supervisor."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WatchdogState:
    scheduler_phase: str = "missing"
    scheduler_attempted: int = 0
    scheduler_active: int = 0
    scheduler_completed_pending: int = 0
    scheduler_backpressure: bool = False
    scheduler_updated_at: str = ""
    has_cooldown: bool = False
    pending_eligible: int = 1
    running_tasks: int = 0
    manifest_last_task_update: str = ""
    monitor_checked_at: str = ""
    next_cooldown_until_epoch: int = 0


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _nonnegative_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return default


def _next_cooldown_epoch(cooldowns: Any) -> int:
    if not isinstance(cooldowns, dict):
        return 0
    deadlines: list[int] = []
    for value in cooldowns.values():
        if not isinstance(value, dict):
            continue
        raw = value.get("until")
        try:
            if isinstance(raw, (int, float)):
                deadline = int(raw)
            else:
                deadline = int(datetime.fromisoformat(str(raw)).timestamp())
        except (TypeError, ValueError, OverflowError):
            continue
        if deadline > 0:
            deadlines.append(deadline)
    return min(deadlines, default=0)


def build_watchdog_state(state_dir: Path) -> WatchdogState:
    """Return a fail-open snapshot without scanning the manifest database."""

    scheduler = _read_json_object(state_dir / "provider_scheduler.json")
    monitor = _read_json_object(state_dir / "monitor_latest.json")
    cooldowns = monitor.get("active_provider_cooldowns")
    cooldown_pending = _nonnegative_int(monitor.get("pending_cooldown"))
    status_counts = monitor.get("status_counts")
    if not isinstance(status_counts, dict):
        status_counts = {}
    return WatchdogState(
        scheduler_phase=str(scheduler.get("phase") or "missing"),
        scheduler_attempted=_nonnegative_int(scheduler.get("attempted_this_run")),
        scheduler_active=_nonnegative_int(scheduler.get("active_total")),
        scheduler_completed_pending=_nonnegative_int(
            scheduler.get("completed_pending_total")
        ),
        scheduler_backpressure=bool(
            scheduler.get("completion_backpressure_active")
        ),
        scheduler_updated_at=str(scheduler.get("updated_at") or ""),
        has_cooldown=bool(cooldowns and cooldown_pending > 0),
        pending_eligible=_nonnegative_int(monitor.get("pending_eligible"), default=1),
        running_tasks=_nonnegative_int(status_counts.get("running")),
        manifest_last_task_update=str(monitor.get("last_task_update") or ""),
        monitor_checked_at=str(monitor.get("checked_at") or ""),
        next_cooldown_until_epoch=_next_cooldown_epoch(cooldowns),
    )


def _shell_lines(state: WatchdogState) -> tuple[str, ...]:
    """Emit fixed-position newline fields; '-' preserves empty array slots."""

    return (
        state.scheduler_phase,
        str(state.scheduler_attempted),
        str(state.scheduler_active),
        str(state.scheduler_completed_pending),
        str(int(state.scheduler_backpressure)),
        state.scheduler_updated_at or "-",
        str(int(state.has_cooldown)),
        str(state.pending_eligible),
        str(state.running_tasks),
        state.manifest_last_task_update or "-",
        state.monitor_checked_at or "-",
        str(state.next_cooldown_until_epoch),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read lightweight OpenBB archive supervisor state."
    )
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--shell-fields", action="store_true")
    args = parser.parse_args()
    state = build_watchdog_state(args.state_dir)
    if args.shell_fields:
        print("\n".join(_shell_lines(state)))
    else:
        print(json.dumps(asdict(state), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
