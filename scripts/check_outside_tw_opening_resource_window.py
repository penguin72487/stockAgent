#!/usr/bin/env python3
"""Fail a systemd ExecCondition during the Taiwan opening critical window."""

from __future__ import annotations

import argparse
from datetime import datetime, time as datetime_time
import json
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
WINDOW_START = datetime_time(8, 20)
WINDOW_END = datetime_time(9, 10)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observed-at", default=None)
    return parser.parse_args()


def evaluate(observed: datetime) -> dict[str, object]:
    local = observed.astimezone(TAIPEI)
    wall = local.timetz().replace(tzinfo=None)
    protected = bool(
        local.weekday() < 5 and WINDOW_START <= wall < WINDOW_END
    )
    return {
        "allowed": not protected,
        "protected": protected,
        "observed_at_taipei": local.isoformat(timespec="seconds"),
        "window": "08:20:00..09:10:00 Asia/Taipei",
        "policy": "best_effort_maintenance_must_not_start_during_tw_opening",
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
    result = evaluate(observed)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    # ExecCondition exit 1 skips the service without marking the unit failed.
    return 0 if result["allowed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
