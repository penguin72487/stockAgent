from __future__ import annotations

import json

from scripts.openbb_archive_watchdog import build_watchdog_state


def test_watchdog_reads_atomic_scheduler_and_monitor_state(tmp_path) -> None:
    (tmp_path / "provider_scheduler.json").write_text(
        json.dumps(
            {
                "phase": "waiting",
                "wait_reason": "provider_cooldown",
                "wait_until": "2026-07-19T20:05:00+00:00",
                "attempted_this_run": 123,
                "active_total": 40,
                "completed_pending_total": 7,
                "completion_backpressure_active": True,
                "updated_at": "2026-07-19T06:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "monitor_latest.json").write_text(
        json.dumps(
            {
                "active_provider_cooldowns": {
                    "bls": {
                        "kind": "quota",
                        "until": "2026-07-20T04:05:00+00:00",
                    },
                    "fmp": {
                        "kind": "quota",
                        "until": "2026-07-19T20:05:00+00:00",
                    },
                },
                "pending_cooldown": 9,
                "pending_eligible": 0,
                "status_counts": {"running": 0},
                "last_task_update": "2026-07-19T06:00:01+00:00",
                "checked_at": "2026-07-19T06:00:02+00:00",
            }
        ),
        encoding="utf-8",
    )

    state = build_watchdog_state(tmp_path)

    assert state.scheduler_phase == "waiting"
    assert state.scheduler_attempted == 123
    assert state.scheduler_active == 40
    assert state.scheduler_completed_pending == 7
    assert state.scheduler_backpressure is True
    assert state.has_cooldown is True
    assert state.pending_eligible == 0
    assert state.running_tasks == 0
    assert state.manifest_last_task_update == "2026-07-19T06:00:01+00:00"
    assert state.next_cooldown_until_epoch == 1784491500
    assert state.scheduler_wait_reason == "provider_cooldown"
    assert state.scheduler_wait_until_epoch == 1784491500


def test_watchdog_corrupt_or_missing_state_fails_open_without_false_cooldown(
    tmp_path,
) -> None:
    (tmp_path / "provider_scheduler.json").write_text("{", encoding="utf-8")
    (tmp_path / "monitor_latest.json").write_text("[]", encoding="utf-8")

    state = build_watchdog_state(tmp_path)

    assert state.scheduler_phase == "missing"
    assert state.scheduler_attempted == 0
    assert state.scheduler_active == 0
    assert state.scheduler_backpressure is False
    assert state.has_cooldown is False
    assert state.pending_eligible == 1
    assert state.running_tasks == 0
    assert state.next_cooldown_until_epoch == 0
    assert state.scheduler_wait_reason == ""
    assert state.scheduler_wait_until_epoch == 0


def test_watchdog_requires_pending_cooldown_evidence(tmp_path) -> None:
    (tmp_path / "monitor_latest.json").write_text(
        json.dumps(
            {
                "active_provider_cooldowns": {"fmp": {"kind": "quota"}},
                "pending_cooldown": 0,
                "pending_eligible": 0,
                "status_counts": {"running": 0},
            }
        ),
        encoding="utf-8",
    )

    state = build_watchdog_state(tmp_path)

    assert state.has_cooldown is False
