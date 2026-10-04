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
    if session and official_calendar_root is not None:
        repo_root = Path(__file__).resolve().parents[1]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        from stockagent.live.market_status import verified_tw_stock_session_day

        verified_session, calendar_evidence = verified_tw_stock_session_day(
            local.date(), parquet_root=official_calendar_root,
        )
        # A missing or corrupt calendar is not proof of a holiday. Keep the
        # critical opening window protected until official evidence exists.
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
    result = evaluate(
        observed,
        minimum_runway_minutes=args.minimum_runway_minutes,
        protected_until=datetime_time.fromisoformat(args.protected_until),
        official_calendar_root=args.official_calendar_root,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    if args.session_status:
        return 0 if result["stock_session"] else 1
    # ExecCondition exit 1 skips the service without marking the unit failed.
    return 0 if result["allowed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
