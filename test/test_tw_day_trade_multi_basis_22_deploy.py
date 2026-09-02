from __future__ import annotations

from pathlib import Path

from scripts.deploy_tw_day_trade_multi_basis_22_history import (
    DEPLOYMENT_SESSION_DATE,
    MARKETS,
    _resumable_replay_candidate,
    _stage_succeeded,
    _terminal_mode,
    _visible_history_dates,
    _wait_discord_ready,
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


def test_history_deploy_wrapper_executes_resolved_python_not_shell_function() -> None:
    root = Path(__file__).resolve().parents[1]
    wrapper = (
        root / "scripts/run_tw_day_trade_multi_basis_22_history_deploy.sh"
    ).read_text(encoding="utf-8")

    assert 'FINTECH_PYTHON="$(resolve_fintech_python)"' in wrapper
    assert 'exec "$FINTECH_PYTHON"' in wrapper
    assert "exec run_fintech_python" not in wrapper


def test_history_deploy_resumes_only_a_complete_replay_candidate(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    for name in ("state.json", "positions.json", "rebuild_receipt.json"):
        (candidate / name).write_text("{}", encoding="utf-8")
    for name in ("fills.jsonl", "marks.jsonl"):
        (candidate / name).write_text("", encoding="utf-8")
    (candidate / "rebuild_receipt.json").write_text(
        '{"sessions": ['
        '{"session_date": "2026-02-25"},'
        '{"session_date": "2026-09-02"}'
        "]}",
        encoding="utf-8",
    )
    status = {
        "status": "failed_retryable",
        "session_date": "2026-09-02",
        "candidate_dir": str(candidate),
        "stages": [{"stage": "replay", "returncode": 0}],
    }

    assert _resumable_replay_candidate(
        status, session_date="2026-09-02"
    ) == candidate
    assert (
        _resumable_replay_candidate(
            {**status, "stages": []}, session_date="2026-09-02"
        )
        is None
    )
    assert (
        _resumable_replay_candidate(status, session_date="2026-09-03") is None
    )
    assert _stage_succeeded(status, "replay")
    assert not _stage_succeeded(status, "promote")


def test_discord_acceptance_requires_a_new_ready_run(monkeypatch) -> None:
    base = {
        "discord_connected": True,
        "core_health": "ready",
        "scheduled_day_trade_markets": list(MARKETS),
        "engine_run_id": "engine-new",
        "startup_inference_warmup": {
            "status": "ready",
            "ready_count": len(MARKETS),
        },
    }
    statuses = iter(
        [
            {**base, "run_id": "discord-old"},
            {**base, "run_id": "discord-new"},
        ]
    )
    monkeypatch.setattr(
        "scripts.deploy_tw_day_trade_multi_basis_22_history._object",
        lambda _path: next(statuses),
    )
    monkeypatch.setattr(
        "scripts.deploy_tw_day_trade_multi_basis_22_history.wall_time.sleep",
        lambda _seconds: None,
    )

    ready = _wait_discord_ready(
        expected_engine_run_id="engine-new",
        previous_run_id="discord-old",
        timeout_seconds=1.0,
    )

    assert ready["run_id"] == "discord-new"


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
