"""Explicit source fixtures for tests that need a verified presentation clock."""
from datetime import date

import pytest


@pytest.fixture
def verified_dashboard_calendar(tmp_path, monkeypatch):
    import stockagent.live.tw_day_trade_dashboard as dashboard

    sessions = {date(2026, 8, day) for day in (12, 13, 14)} | {
        date(2026, 9, 9), date(2026, 9, 10),
    }
    monkeypatch.setattr(dashboard, "DEFAULT_CALENDAR_PARQUET_ROOT", tmp_path / "calendar")
    monkeypatch.setattr(
        dashboard, "verified_tw_stock_session_day",
        lambda day, **_kwargs: (day in sessions, "fixture verified session" if day in sessions else "fixture missing calendar"),
    )
    dashboard._session_clock_cached.cache_clear()
    yield
    dashboard._session_clock_cached.cache_clear()
