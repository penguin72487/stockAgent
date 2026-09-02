from __future__ import annotations

from pathlib import Path

from scripts.deploy_tw_day_trade_multi_basis_22_history import (
    DEPLOYMENT_SESSION_DATE,
    MARKETS,
    _terminal_mode,
    _visible_history_dates,
)


def test_terminal_mode_requires_same_day_flat_and_settled() -> None:
    settled = {
        "session_date": "2026-09-02",
        "engine_status": "active",
        "closing_auction_settled_at": "2026-09-02T13:30:00+08:00",
        "open_position_count": 0,
        "positions": {"position": {"signed_shares": 0}},
    }
    assert _terminal_mode(settled, session_date="2026-09-02")
    assert not _terminal_mode(settled, session_date="2026-09-01")
    assert not _terminal_mode(
        {**settled, "positions": {"position": {"signed_shares": 1_000}}},
        session_date="2026-09-02",
    )
    assert _terminal_mode(
        {
            **settled,
            "engine_status": "terminal",
            "closing_auction_settled_at": None,
        },
        session_date="2026-09-02",
    )


def test_history_deploy_timer_is_week_calendar_based() -> None:
    root = Path(__file__).resolve().parents[1]
    timer = (
        root
        / "deploy/systemd/stockagent-tw-day-trade-multi-basis-22-history.timer.in"
    ).read_text(encoding="utf-8")

    assert len(MARKETS) == 4
    assert DEPLOYMENT_SESSION_DATE == "2026-09-02"
    assert "OnCalendar=Mon..Fri" in timer
    assert "OnUnitActiveSec" not in timer
    assert "OnBootSec" not in timer


def test_public_history_dates_are_derived_from_utc_minutes() -> None:
    payload = {
        "history": [
            {
                "series_id": "tw_day_trade_multi_basis_22",
                "minute": "2026-02-25T01:01+00:00",
            },
            {
                "series_id": "tw_day_trade_multi_basis_22",
                "minute": "2026-09-02T05:30+00:00",
            },
            {"series_id": "another", "minute": "2026-01-01T01:00+00:00"},
        ]
    }

    assert _visible_history_dates(payload, "tw_day_trade_multi_basis_22") == [
        "2026-02-25",
        "2026-09-02",
    ]
