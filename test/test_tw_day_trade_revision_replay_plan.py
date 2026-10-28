from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from scripts.plan_tw_day_trade_revision_replay import _first_full_minute_after, build_plan


def _version(signal_id, lower, upper, errors=None):
    return {"signal_id": signal_id, "artifact_published_at": lower,
            "execution_pointer_written_at": upper, "errors": errors or [], "was_registered": True}


def _report(versions):
    return {"contract": "paper_signal_published_revision_clock_audit_v1", "simulation_only": True,
            "production_order_possible": False, "ledger_modified": False, "session_date": "2026-10-28",
            "modes": {"m": {"first_published_signal_id": "a", "publication_count": len(versions),
                            "versions": versions}}}


def test_first_full_minute_keeps_exact_boundary_and_ceil():
    tz = ZoneInfo("Asia/Taipei")
    start, end = _first_full_minute_after(datetime(2026, 10, 28, 9, 0, tzinfo=tz))
    assert (start.minute, end.minute) == (0, 1)
    start, end = _first_full_minute_after(datetime(2026, 10, 28, 9, 0, 1, tzinfo=tz))
    assert (start.minute, end.minute) == (1, 2)


def test_keeps_all_revisions_and_never_calls_bar_a_fill():
    versions = [_version("a", "2026-10-28T09:00:01+08:00", "2026-10-28T09:00:02+08:00"),
                _version("b", "2026-10-28T09:00:03+08:00", "2026-10-28T09:00:04+08:00"),
                _version("c", "2026-10-28T09:02:01+08:00", "2026-10-28T09:02:02+08:00",
                         ["artifact_completed_before_pointer_write"])]
    plan = build_plan(_report(versions))
    events = plan["modes"]["m"]["events"]
    assert [e["signal_id"] for e in events] == ["a", "b", "c"]
    assert events[0]["next_revision_relation"] == "newer_revision_certainly_visible_before_full_bar"
    assert events[1]["next_revision_relation"] == "full_bar_precedes_known_next_revision_lower_bound"
    assert plan["events_blocked_by_source_clock_errors"] == 1
    assert plan["fills_fees_curves_and_restart_accepted"] is False
    assert all(not e["executable_quote_or_fill_verified"] for e in events)


def test_plan_rejects_bad_order_or_non_paper_source():
    report = _report([_version("a", "2026-10-28T09:00:03+08:00", "2026-10-28T09:00:02+08:00")])
    with pytest.raises(ValueError, match="bounds"):
        build_plan(report)
    report["production_order_possible"] = True
    with pytest.raises(ValueError, match="no production orders"):
        build_plan(report)
