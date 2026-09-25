"""FinLab uses the verified stock-session calendar only for opening protection."""

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from scripts.check_outside_tw_opening_resource_window import evaluate


def test_holiday_does_not_hold_finlab_after_quota_reset():
    observed = datetime.fromisoformat("2026-09-25T08:30:00+08:00")
    with patch(
        "stockagent.live.market_status.verified_tw_stock_session_day",
        return_value=(False, "official TWSE schedule as-of 2026-09-16: 中秋節"),
    ):
        result = evaluate(observed, official_calendar_root=Path("data_tw_public"))
    assert result["stock_session"] is False
    assert result["allowed"] is True


def test_verified_session_and_unknown_calendar_remain_protected():
    observed = datetime.fromisoformat("2026-09-24T08:30:00+08:00")
    for response in (
        (True, "official TWSE schedule as-of 2026-09-16: ordinary weekday session"),
        (False, "official TWSE holiday schedule is missing"),
        (False, "official TWSE holiday schedule is unverified: malformed"),
    ):
        with patch(
            "stockagent.live.market_status.verified_tw_stock_session_day",
            return_value=response,
        ):
            result = evaluate(observed, official_calendar_root=Path("data_tw_public"))
        assert result["stock_session"] is True
        assert result["allowed"] is False
