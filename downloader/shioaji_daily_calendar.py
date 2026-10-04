"""Receipt-backed calendar admission for local Shioaji daily views."""
from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
from typing import Any


DAILY_CALENDAR_CONTRACT = "receipt_verified_taiex_sessions_quarantine_v1"


def session_dates_sha256(sessions: set[date], start: date, end: date) -> str:
    body = "\n".join(day.isoformat() for day in sorted(sessions) if start <= day <= end)
    return hashlib.sha256(body.encode("ascii")).hexdigest()


def load_daily_calendar(
    root: Path, start: date, end: date
) -> tuple[set[date], dict[str, Any]]:
    from downloader.download_tw_public_data import _validated_taiex_session_dates

    sessions, parquet_sha256 = _validated_taiex_session_dates(root, start, end)
    if not sessions:
        raise RuntimeError("receipt-backed daily calendar has no selected sessions")
    return sessions, {
        "contract": DAILY_CALENDAR_CONTRACT,
        "root": str(root.resolve()),
        "sha256": parquet_sha256,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "session_dates_sha256": session_dates_sha256(sessions, start, end),
    }


def calendar_prefix_matches(
    recorded: Any, sessions: set[date], current: dict[str, Any], end: date
) -> bool:
    """Allow extension only when the exact previously admitted dates still agree."""
    if not isinstance(recorded, dict):
        return False
    try:
        start = date.fromisoformat(str(current["start_date"]))
        return (
            recorded.get("contract") == DAILY_CALENDAR_CONTRACT
            and recorded.get("start_date") == start.isoformat()
            and recorded.get("end_date") == end.isoformat()
            and end <= date.fromisoformat(str(current["end_date"]))
            and len(str(recorded.get("sha256", ""))) == 64
            and recorded.get("session_dates_sha256")
            == session_dates_sha256(sessions, start, end)
        )
    except (KeyError, ValueError, TypeError):
        return False
