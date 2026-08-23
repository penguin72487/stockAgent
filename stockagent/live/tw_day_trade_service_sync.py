"""Compact receipts for the Discord -> paper engine -> dashboard pipeline.

The large simulation state is intentionally not a synchronization primitive.
The paper engine publishes this small receipt only after every related state
file has been atomically replaced.  Readers can therefore acknowledge one
committed revision without repeatedly parsing multi-megabyte ledgers.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Final, Mapping


SERVICE_SYNC_FILENAME: Final[str] = "service_sync.json"
DISCORD_SERVICE_STATUS_FILENAME: Final[str] = "service_status.json"
SERVICE_SYNC_SCHEMA_VERSION: Final[int] = 1


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    return payload


def load_service_sync(state_dir: Path) -> dict[str, Any] | None:
    """Load the paper engine's compact commit receipt, if one exists."""

    path = Path(state_dir) / SERVICE_SYNC_FILENAME
    try:
        payload = read_json_object(path)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if int(payload.get("schema_version") or 0) != SERVICE_SYNC_SCHEMA_VERSION:
        return None
    return payload


def mode_from_service_sync(
    receipt: Mapping[str, Any] | None,
    market: str,
) -> dict[str, Any] | None:
    if not isinstance(receipt, Mapping):
        return None
    row = (receipt.get("modes") or {}).get(str(market))
    return dict(row) if isinstance(row, Mapping) else None


def parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def age_seconds(value: object, *, now: datetime | None = None) -> float | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return max(0.0, (observed.astimezone(timezone.utc) - parsed).total_seconds())


__all__ = [
    "DISCORD_SERVICE_STATUS_FILENAME",
    "SERVICE_SYNC_FILENAME",
    "SERVICE_SYNC_SCHEMA_VERSION",
    "age_seconds",
    "load_service_sync",
    "mode_from_service_sync",
    "parse_timestamp",
    "read_json_object",
]
