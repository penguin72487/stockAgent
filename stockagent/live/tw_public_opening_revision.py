"""Bounded revision freeze for deterministic Taiwan opening inference.

The public-source monitor continues probing while the freeze is active, but it
must not mutate the canonical live root between the accepted 08:30 snapshot and
the opening recovery boundary.  The lease expires automatically so a crashed
acceptance job cannot stop ingestion indefinitely.
"""

from __future__ import annotations

from datetime import datetime, time as datetime_time
import json
import os
from pathlib import Path
from typing import Any, Mapping
import uuid
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
OPENING_REVISION_FREEZE_START = datetime_time(8, 20)
OPENING_REVISION_FREEZE_UNTIL = datetime_time(9, 5)
OPENING_REVISION_GATE_NAME = "tw-public-opening-revision.lock"
OPENING_REVISION_FREEZE_NAME = "tw-public-opening-revision.json"


def opening_revision_gate_path(live_root: Path) -> Path:
    return live_root.parent / ".locks" / OPENING_REVISION_GATE_NAME


def opening_revision_freeze_path(live_root: Path) -> Path:
    return live_root.parent / ".locks" / OPENING_REVISION_FREEZE_NAME


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_timestamp(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TAIPEI)
    return parsed.astimezone(TAIPEI)


def active_opening_revision_freeze(
    live_root: Path, *, observed: datetime
) -> dict[str, Any]:
    """Return the active, same-session freeze lease or an empty mapping."""

    current = observed.astimezone(TAIPEI)
    payload = _read_json(opening_revision_freeze_path(live_root))
    until = _parse_timestamp(payload.get("defer_apply_until_taipei"))
    if (
        payload.get("status") != "active"
        or str(payload.get("session_date") or "") != current.date().isoformat()
        or until is None
        or current >= until
    ):
        return {}
    return payload


def create_opening_revision_freeze(
    live_root: Path,
    *,
    observed: datetime,
    owner: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Atomically publish a weekday lease ending at the 09:05 recovery edge."""

    current = observed.astimezone(TAIPEI)
    local_time = current.timetz().replace(tzinfo=None)
    until = current.replace(
        hour=OPENING_REVISION_FREEZE_UNTIL.hour,
        minute=OPENING_REVISION_FREEZE_UNTIL.minute,
        second=0,
        microsecond=0,
    )
    if (
        current.weekday() >= 5
        or local_time < OPENING_REVISION_FREEZE_START
        or current >= until
    ):
        return {}
    payload: dict[str, Any] = {
        "schema_version": 1,
        "status": "active",
        "session_date": current.date().isoformat(),
        "frozen_at_taipei": current.isoformat(timespec="seconds"),
        "defer_apply_until_taipei": until.isoformat(timespec="seconds"),
        "policy": "probe_and_queue_then_apply_after_opening_boundary",
        "reason": "stable accepted public-data revision for opening inference",
    }
    if owner:
        payload["owner"] = dict(owner)
    path = opening_revision_freeze_path(live_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return payload


__all__ = [
    "OPENING_REVISION_FREEZE_START",
    "OPENING_REVISION_FREEZE_UNTIL",
    "active_opening_revision_freeze",
    "create_opening_revision_freeze",
    "opening_revision_freeze_path",
    "opening_revision_gate_path",
]
