#!/usr/bin/env python3
"""Fail a systemd ExecCondition during the Taiwan opening critical window."""

from __future__ import annotations

import argparse
from datetime import datetime, time as datetime_time, timedelta
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
WINDOW_START = datetime_time(8, 20)
WINDOW_END = datetime_time(9, 10)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observed-at", default=None)
    parser.add_argument("--minimum-runway-minutes", type=int, default=0)
    parser.add_argument("--protected-until", default="09:10")
    parser.add_argument("--official-calendar-root", type=Path, default=None)
    parser.add_argument("--calendar-refresh", action="store_true",
                        help="Defer redundant calendar writes during opening; allow required calendar bootstrap")
    parser.add_argument("--session-status", action="store_true",
                        help="Exit zero for a verified TW stock session; unknown weekdays are protected")
    return parser.parse_args()


def evaluate(
    observed: datetime,
    *,
    minimum_runway_minutes: int = 0,
    protected_until: datetime_time = WINDOW_END,
    official_calendar_root: Path | None = None,
) -> dict[str, object]:
    if not 0 <= minimum_runway_minutes <= 500:
        raise ValueError("minimum_runway_minutes must be 0..500")
    if protected_until <= WINDOW_START:
        raise ValueError("protected_until must be after opening window start")
    local = observed.astimezone(TAIPEI)
    session = local.weekday() < 5
    calendar_evidence = "weekday_only"
    if official_calendar_root is not None:
        repo_root = Path(__file__).resolve().parents[1]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        from stockagent.live.market_status import verified_tw_stock_session_day

        verified_session, calendar_evidence = verified_tw_stock_session_day(
            local.date(), parquet_root=official_calendar_root,
        )
        # A scheduled makeup session can fall on a weekend. A missing or
        # corrupt official calendar is not proof of a closure on any day.
        session = verified_session or not calendar_evidence.startswith(
            "official TWSE schedule as-of"
        )
    wall = local.timetz().replace(tzinfo=None)
    protected_start = (
        datetime.combine(local.date(), WINDOW_START)
        - timedelta(minutes=minimum_runway_minutes)
    ).time()
    protected = bool(
        session and protected_start <= wall < protected_until
    )
    return {
        "allowed": not protected,
        "protected": protected,
        "observed_at_taipei": local.isoformat(timespec="seconds"),
        "window": f"08:20:00..{protected_until.isoformat(timespec='seconds')} Asia/Taipei",
        "minimum_runway_minutes": minimum_runway_minutes,
        "protected_start": protected_start.isoformat(timespec="seconds"),
        "stock_session": session,
        "calendar_evidence": calendar_evidence,
        "policy": (
            "best_effort_maintenance_must_not_start_during_tw_session"
            if protected_until > WINDOW_END
            else "best_effort_maintenance_must_not_start_during_tw_opening"
        ),
    }


def main() -> int:
    args = parse_args()
    observed = (
        datetime.fromisoformat(args.observed_at)
        if args.observed_at
        else datetime.now(TAIPEI)
    )
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=TAIPEI)
    def canonical(minimum_runway_minutes=args.minimum_runway_minutes):
        return evaluate(
            observed,
            minimum_runway_minutes=minimum_runway_minutes,
            protected_until=datetime_time.fromisoformat(args.protected_until),
            official_calendar_root=args.official_calendar_root,
        )
    repo_root = Path(__file__).resolve().parents[1]
    if (args.observed_at is None and args.minimum_runway_minutes in (120, 20)
            and args.protected_until == '13:30' and args.official_calendar_root is not None
            and args.official_calendar_root.resolve() == (repo_root/'data_tw_public').resolve()
            and not args.calendar_refresh and not args.session_status):
        # Both storage runways share only the verified session decision.
        # Retain the original cache's 120-minute identity; recompute the
        # installer's 08:00 boundary instead of inheriting its 06:20 block.
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        try:
            from scripts.storage_priority_cache import evaluate_cached
        except ImportError:
            result = canonical()
        else:
            result = evaluate_cached(observed.astimezone(TAIPEI), args.official_calendar_root,
                                     lambda: canonical(120))
            if args.minimum_runway_minutes == 20:
                local = observed.astimezone(TAIPEI)
                protected = bool(result['stock_session'] and
                    datetime_time(8, 0) <= local.timetz().replace(tzinfo=None) < datetime_time(13, 30))
                result = {**result, 'minimum_runway_minutes': 20, 'protected_start': '08:00:00',
                          'protected': protected, 'allowed': not protected}
    else:
        result = canonical()
    if args.calendar_refresh:
        if args.official_calendar_root is None or args.session_status:
            raise ValueError("calendar-refresh requires its official calendar root and not session-status")
        result = calendar_refresh_admission(result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    if args.session_status:
        return 0 if result["stock_session"] else 1
    # ExecCondition exit 1 skips the service without marking the unit failed.
    return 0 if result["allowed"] else 1


def calendar_refresh_admission(window: dict[str, object]) -> dict[str, object]:
    """Calendar maintenance yields to opening, not missing-calendar recovery.

    Keep the canonical writer lock and the official session verifier. This
    admission is checked before AND after waiting for that lock, so a timer
    queued before the critical window cannot become its writer at 09:00.
    Unknown/corrupt calendars still need their original collector to bootstrap;
    ordinary maintenance must not delay a source reader with a verified calendar.
    """
    verified = str(window["calendar_evidence"]).startswith("official TWSE schedule as-of")
    bootstrap = bool(window["protected"] and not verified)
    return {**window, "allowed": bool(window["allowed"] or bootstrap),
            "calendar_bootstrap_required": bootstrap,
            "policy": "verified_calendar_refresh_yields_to_opening_v1"}


if __name__ == "__main__":
    raise SystemExit(main())
