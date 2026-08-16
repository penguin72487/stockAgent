from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from stockagent.live import openbb_archive_dashboard as dashboard


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _snapshot(checked_at: datetime) -> dict[str, object]:
    return {
        "checked_at": checked_at.isoformat(),
        "health": "critical",
        "complete": False,
        "archive_end_date": "2026-07-18",
        "total_tasks": 100,
        "accepted_tasks": 72,
        "resolved_tasks": 90,
        "unresolved_tasks": 10,
        "actionable_unresolved_tasks": 9,
        "completion_percent": 72.0,
        "success_rows": 123456,
        "retryable_tasks": 8,
        "pending_retry_deferred": 3,
        "next_task_retry_at": (checked_at + timedelta(hours=2)).isoformat(),
        "unavailable_tasks": 18,
        "endpoint_count": 4,
        "min_free_bytes": 1,
        "status_counts": {"success": 70, "empty": 2, "pending": 8, "failed": 2},
        "category_progress": [
            {
                "category": "equity",
                "completion_percent": 50,
                "accepted_tasks": 5,
                "total_tasks": 10,
                "unresolved_tasks": 3,
                "unavailable_tasks": 2,
                "success_rows": 100,
                "private_path": "/secret/category",
            }
        ],
        "provider_progress": [{"provider": "fmp", "accepted_tasks": 7, "rows": 99}],
        "provider_eta_projections": [
            {
                "provider": "fmp",
                "eligible_backlog_tasks": 11,
                "exclusive_backlog_tasks": 9,
                "recent_tasks_per_minute": 0.5,
                "requests_per_second": 2,
                "configured_concurrency": 3,
                "observed_quota_limit": {"reason": "secret provider response"},
            }
        ],
        "active_provider_cooldowns": {
            "fmp": {
                "until": (checked_at + timedelta(hours=1)).isoformat(),
                "kind": "quota",
                "reason": "HTTP 429 with a private API token",
            }
        },
        "alerts": [
            {
                "severity": "critical",
                "code": "provider_quota_completion_floor",
                "message": "raw upstream message containing a key",
            }
        ],
    }


def test_public_status_separates_live_process_truth_from_stale_audit(
    tmp_path: Path, monkeypatch
) -> None:
    now = datetime(2026, 8, 15, 4, 0, tzinfo=UTC)
    state = tmp_path / "data_openBB" / "_state"
    _write_json(state / "monitor_latest.json", _snapshot(now - timedelta(days=2)))
    _write_json(
        state / "provider_scheduler.json",
        {
            "updated_at": (now - timedelta(seconds=20)).isoformat(),
            "phase": "running",
            "providers": {
                "fmp": {
                    "requests_per_second": 4,
                    "execution_limit": 6,
                    "active": 1,
                    "cooldown": True,
                }
            },
            "pid": 999,
            "plan_token": "private-plan",
        },
    )
    monkeypatch.setattr(
        dashboard, "_pid_alive", lambda path, fragments: path.name == "supervisor.pid"
    )

    public = dashboard.build_openbb_public_status(tmp_path, now=now)

    assert public["health"] == "starting"
    assert public["audit_health"] == "critical"
    assert public["snapshot_state"] == "stale"
    assert public["process"]["supervisor_alive"] is True
    assert public["process"]["downloader_alive"] is False
    assert public["archive"]["completion_percent"] == 72.0
    assert public["archive"]["actionable_unresolved_tasks"] == 9
    assert public["archive"]["retry_deferred_tasks"] == 3
    assert public["archive"]["next_task_retry_at"] == (
        now - timedelta(days=2) + timedelta(hours=2)
    ).isoformat()
    assert public["categories"][0]["missing_tasks"] == 5
    assert public["categories"][0]["unavailable_tasks"] == 2
    assert public["providers"][0]["requests_per_second"] == 4.0
    assert public["alerts"][0]["message"].startswith("供應商配額形成")
    encoded = json.dumps(public, ensure_ascii=False)
    for forbidden in (
        "private",
        "raw upstream",
        "API token",
        "pid",
        "plan_token",
        "/secret",
    ):
        assert forbidden not in encoded


def test_openbb_page_uses_actionable_backlog_without_double_counting_unavailable() -> None:
    root = Path(__file__).resolve().parents[1] / "services/openbb_archive_dashboard"
    html = (root / "index.html").read_text(encoding="utf-8")
    javascript = (root / "app.js").read_text(encoding="utf-8")
    assert "可處理待辦" in html
    assert "尚未接受且非永久不可用" in html
    assert "archive.actionable_unresolved_tasks" in javascript
    assert 'label: "尚未接受"' in javascript
    assert 'let range = "1d"' in javascript
    assert 'data-range="1d" class="active" aria-pressed="true"' in html


def test_public_status_fails_closed_when_incomplete_processes_are_dead(
    tmp_path: Path, monkeypatch
) -> None:
    now = datetime(2026, 8, 15, 4, 0, tzinfo=UTC)
    state = tmp_path / "data_openBB" / "_state"
    _write_json(state / "monitor_latest.json", _snapshot(now))
    monkeypatch.setattr(dashboard, "_pid_alive", lambda *_args: False)
    public = dashboard.build_openbb_public_status(tmp_path, now=now)
    assert public["health"] == "stopped"
    assert public["snapshot_state"] == "current"


def test_compact_history_projection_and_range_filter(tmp_path: Path) -> None:
    now = datetime(2026, 8, 15, 4, 0, tzinfo=UTC)
    state = tmp_path / "data_openBB" / "_state"
    state.mkdir(parents=True)
    rows = [
        dashboard.project_openbb_history_row(_snapshot(now - timedelta(days=2))),
        dashboard.project_openbb_history_row(_snapshot(now - timedelta(minutes=30))),
    ]
    (state / "monitor_dashboard_history.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    recent = dashboard.build_openbb_public_history(tmp_path, "1h", now=now)
    all_rows = dashboard.build_openbb_public_history(tmp_path, "all", now=now)
    assert len(recent["history"]) == 1
    assert len(all_rows["history"]) == 2
    assert all_rows["history"][0]["accepted_percent"] == 72.0
    assert "output_dir" not in json.dumps(all_rows)
