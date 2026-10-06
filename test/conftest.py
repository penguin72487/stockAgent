"""Explicit source and node-role fixtures, independent of live enrollment."""
from datetime import date

import pytest


@pytest.fixture(autouse=True)
def isolated_node_role(tmp_path_factory, monkeypatch):
    """Default to an unenrolled test node; explicit role fixtures keep their gates."""
    from stockagent.data_sync import node_roles

    observe_role = node_roles.training_only_node
    absent_role_file = tmp_path_factory.getbasetemp() / "unregistered-test-node-role.json"

    def test_role(**kwargs):
        kwargs.setdefault("role_file", absent_role_file)
        return observe_role(**kwargs)

    monkeypatch.setattr(node_roles, "training_only_node", test_role)


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
