from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


DEFAULT_DISPLAY_TIMEZONE = "Asia/Taipei"
TAIPEI_DISPLAY_LABEL = "UTC+8 台北"


def display_timezone_label(timezone_name: str | None = None) -> str:
    name = str(timezone_name or DEFAULT_DISPLAY_TIMEZONE).strip() or DEFAULT_DISPLAY_TIMEZONE
    if name == DEFAULT_DISPLAY_TIMEZONE:
        return TAIPEI_DISPLAY_LABEL
    try:
        tz = ZoneInfo(name)
        now = datetime.now(tz)
        offset = now.utcoffset()
        if offset is None:
            return name
        total_minutes = int(offset.total_seconds() // 60)
        sign = "+" if total_minutes >= 0 else "-"
        total_minutes = abs(total_minutes)
        hours, minutes = divmod(total_minutes, 60)
        return f"UTC{sign}{hours}" + (f":{minutes:02d}" if minutes else f" {name}")
    except Exception:
        return name


def _zoneinfo_or_default(timezone_name: str | None, default: str = DEFAULT_DISPLAY_TIMEZONE) -> ZoneInfo:
    try:
        return ZoneInfo(str(timezone_name or default))
    except Exception:
        return ZoneInfo(default)


def _parse_datetime(value: Any, source_timezone: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null", "nat", "n/a"}:
        return None
    iso_text = text.replace(" ", "T")
    if iso_text.endswith("Z"):
        iso_text = iso_text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(iso_text)
    except Exception:
        parsed = None
        normalized = text.replace("T", " ")
        for fmt, size in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d %H:%M", 16)):
            try:
                parsed = datetime.strptime(normalized[:size], fmt)
                break
            except Exception:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_zoneinfo_or_default(source_timezone))
    return parsed


def is_datetime_text(value: Any) -> bool:
    text = str(value or "").strip()
    return len(text) >= 16 and ":" in text


def format_display_time(
    value: Any,
    *,
    source_timezone: str | None = None,
    display_timezone: str | None = DEFAULT_DISPLAY_TIMEZONE,
    include_timezone: bool = False,
) -> str:
    text = str(value or "").strip()
    if not text:
        return "n/a"
    if not is_datetime_text(text):
        return text
    parsed = _parse_datetime(text, source_timezone)
    if parsed is None:
        return text
    display_tz_name = str(display_timezone or DEFAULT_DISPLAY_TIMEZONE)
    converted = parsed.astimezone(_zoneinfo_or_default(display_tz_name))
    formatted = converted.strftime("%Y-%m-%d %H:%M:%S")
    if include_timezone:
        return f"{formatted} {display_timezone_label(display_tz_name)}"
    return formatted
