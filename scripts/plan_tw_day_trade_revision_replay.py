#!/usr/bin/env python3
"""Build an evidence-bound chronological *plan*, not a trading replay.

Every retained signal remains a distinct event.  Without a timestamped quote
tape, minute OHLC cannot establish which trade occurred after an intraminute
signal, nor prove the fill of any version.  This plan finds the first whole
minute after the conservative pointer-visibility bound and reports whether a
later revision could have superseded it.  It produces no orders or ledger.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TAIPEI = ZoneInfo("Asia/Taipei")
ROOT = Path(__file__).resolve().parents[1]


def _clock(value: str, day: date) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None or parsed.astimezone(TAIPEI).date() != day:
        raise ValueError("a timezone-aware clock in the requested session is required")
    return parsed.astimezone(TAIPEI)


def _first_full_minute_after(visible_no_later_than: datetime) -> tuple[datetime, datetime]:
    start = visible_no_later_than.replace(second=0, microsecond=0)
    if visible_no_later_than != start:
        start += timedelta(minutes=1)
    return start, start + timedelta(minutes=1)


def build_plan(report: dict[str, Any]) -> dict[str, Any]:
    if (report.get("contract") != "paper_signal_published_revision_clock_audit_v1"
            or report.get("simulation_only") is not True
            or report.get("production_order_possible") is not False
            or report.get("ledger_modified") is not False):
        raise ValueError("expected a published-signal audit with no production orders")
    day = date.fromisoformat(report["session_date"])
    modes = report.get("modes")
    if not isinstance(modes, dict) or not modes:
        raise ValueError("audit has no modes")
    output: dict[str, Any] = {}
    total = blocked = superseded = overlaps = 0
    for market, mode in sorted(modes.items()):
        versions = mode.get("versions")
        if not isinstance(versions, list) or len(versions) != mode.get("publication_count"):
            raise ValueError(f"{market}: incomplete publication list")
        if not versions or versions[0].get("signal_id") != mode.get("first_published_signal_id"):
            raise ValueError(f"{market}: missing first published signal")
        events: list[dict[str, Any]] = []
        seen: set[str] = set()
        previous: datetime | None = None
        for index, version in enumerate(versions):
            signal_id = str(version.get("signal_id") or "")
            if not signal_id or signal_id in seen:
                raise ValueError(f"{market}: missing or duplicate signal identity")
            seen.add(signal_id)
            upper = _clock(version["execution_pointer_written_at"], day)
            lower = _clock(version["artifact_published_at"], day)
            if lower > upper or previous is not None and upper < previous:
                raise ValueError(f"{market}: publication bounds or order are invalid")
            previous = upper
            start, end = _first_full_minute_after(upper)
            next_version = versions[index + 1] if index + 1 < len(versions) else None
            next_lower = _clock(next_version["artifact_published_at"], day) if next_version else None
            next_upper = _clock(next_version["execution_pointer_written_at"], day) if next_version else None
            if next_upper is not None and next_upper <= start:
                next_relation = "newer_revision_certainly_visible_before_full_bar"
                superseded += 1
            elif next_lower is not None and next_lower < end:
                next_relation = "newer_revision_may_be_visible_before_full_bar_ends"
                overlaps += 1
            else:
                next_relation = "full_bar_precedes_known_next_revision_lower_bound"
            errors = list(version.get("errors") or [])
            if errors:
                blocked += 1
            event = {
                "signal_id": signal_id,
                "revision": version.get("opening_signal_revision"),
                "summary_path": version.get("summary_path"),
                "summary_sha256": version.get("summary_sha256"),
                "weights_path": version.get("weights_path"),
                "weights_sha256": version.get("weights_sha256"),
                "publication_visibility_lower_bound": lower.isoformat(),
                "publication_visibility_upper_bound": upper.isoformat(),
                "first_fully_post_visibility_minute_start": start.isoformat(),
                "first_fully_post_visibility_minute_end": end.isoformat(),
                "minute_end_within_entry_window": end.timetz().replace(tzinfo=None) < time(13, 20),
                "next_revision_relation": next_relation,
                "was_registered_in_original_ledger": version.get("was_registered") is True,
                "source_errors_blocking_replay": errors,
                "executable_quote_or_fill_verified": False,
            }
            events.append(event)
        total += len(events)
        output[market] = {"publication_count": len(events), "events": events}
    return {
        "schema_version": 1,
        "contract": "published_revision_causal_minute_replay_plan_v1",
        "session_date": day.isoformat(),
        "status": "plan_only_no_replay_or_price_acceptance",
        "simulation_only": True,
        "production_order_possible": False,
        "ledger_modified": False,
        "event_count": total,
        "events_blocked_by_source_clock_errors": blocked,
        "superseded_before_first_full_minute": superseded,
        "first_full_minute_may_overlap_next_revision": overlaps,
        "publication_completeness_proven": False,
        "minute_ohlc_proves_intraminute_fills": False,
        "fills_fees_curves_and_restart_accepted": False,
        "modes": output,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    source = args.audit.read_bytes()
    report = build_plan(json.loads(source))
    report["published_revision_audit_sha256"] = hashlib.sha256(source).hexdigest()
    output = args.output.resolve()
    if output.is_relative_to((ROOT / "artifacts/live/tw_day_trade_simulation").resolve()):
        raise ValueError("replay plan cannot write into the live ledger")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps({key: report[key] for key in (
        "status", "event_count", "events_blocked_by_source_clock_errors",
        "superseded_before_first_full_minute", "first_full_minute_may_overlap_next_revision")},
        ensure_ascii=False))


if __name__ == "__main__":
    main()
