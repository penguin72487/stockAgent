from pathlib import Path

from scripts import check_stockagent_time_sync as time_sync
from scripts import check_tw_day_trade_unattended_health as guardian


ROOT = Path(__file__).resolve().parents[1]


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_wsl_host_clock_is_authoritative_even_when_chrony_is_unsynchronised(
    monkeypatch,
) -> None:
    monkeypatch.setattr(time_sync, "_is_wsl", lambda: True)
    monkeypatch.setattr(
        time_sync,
        "_chrony_tracking",
        lambda: {"stratum": 0, "leap_status": "Not synchronised"},
    )
    monkeypatch.setattr(
        time_sync,
        "_sample_windows_host_offset",
        lambda **_kwargs: {
            "source": "wsl_windows_host_clock",
            "offset_seconds": 0.012,
            "round_trip_seconds": 0.03,
            "sample_count": 3,
        },
    )

    payload = time_sync.evaluate_time_sync(
        repair=False,
        max_offset_ms=500.0,
        max_repair_offset_seconds=300.0,
        samples=3,
    )

    assert payload["ready"] is True
    assert payload["status"] == "ok"
    assert payload["simulation_schedule_authority"]["source"] == (
        "wsl_windows_host_clock"
    )
    assert payload["chrony_observation"]["leap_status"] == "Not synchronised"


def test_wsl_large_clock_offset_fails_closed_without_repair(monkeypatch) -> None:
    monkeypatch.setattr(time_sync, "_is_wsl", lambda: True)
    monkeypatch.setattr(time_sync, "_chrony_tracking", lambda: {})
    monkeypatch.setattr(
        time_sync,
        "_sample_windows_host_offset",
        lambda **_kwargs: {
            "source": "wsl_windows_host_clock",
            "offset_seconds": 2.0,
            "round_trip_seconds": 0.03,
            "sample_count": 3,
        },
    )

    payload = time_sync.evaluate_time_sync(
        repair=False,
        max_offset_ms=500.0,
        max_repair_offset_seconds=300.0,
        samples=3,
    )

    assert payload["ready"] is False
    assert payload["status"] == "failed"
    assert any("exceeds" in row for row in payload["failures"])


def test_weekly_guardian_uses_existing_authoritative_units_without_active_restart() -> None:
    guardian = _read("scripts/check_tw_day_trade_unattended_health.py")
    timer = _read(
        "deploy/systemd/stockagent-tw-day-trade-unattended-guardian.timer.in"
    )
    time_timer = _read("deploy/systemd/stockagent-time-sync-check.timer.in")

    assert "OnCalendar=Mon..Fri" in timer
    assert "OnCalendar=Mon..Fri" in time_timer
    assert "stockagent-tw-day-trade-eligibility.service" in guardian
    assert "stockagent-tw-public-0830-check.service" in guardian
    assert "stockagent-discord-artifact-maintenance.timer" in guardian
    assert "post_close_artifact_maintenance" in guardian
    assert "systemctl\", \"restart" not in guardian
    assert "production_order_possible\": False" in guardian
    for market in (
        "tw_day_trade_100m",
        "tw_day_trade_multi_basis",
        "tw_day_trade_multi_basis_22",
        "tw_day_trade_multi_basis_projection_l1_gelu",
    ):
        assert market in guardian


def test_acceptance_wrappers_verify_clock_before_canonical_gate() -> None:
    for relative, command in (
        ("scripts/run_tw_public_0830_check.sh", "run_tw_public_0830_check.py"),
        (
            "scripts/run_tw_day_trade_preopen_gate.sh",
            "check_tw_day_trade_preopen_readiness.py",
        ),
    ):
        wrapper = _read(relative)
        assert wrapper.index("check_stockagent_time_sync.py --repair") < wrapper.index(
            command
        )


def test_source_event_watchdog_tracks_probe_lock_and_download_progress() -> None:
    source = _read("scripts/watch_tw_public_source_events.py")

    assert "pending_hosts={len(pending)}" in source
    assert "waiting for canonical TW public refresh lock" in source
    assert "applying TW public source event pid={process.pid}" in source
    assert "subprocess.Popen(command, cwd=REPO_ROOT)" in source


def test_guardian_distinguishes_missing_signal_from_nonlive_recovery() -> None:
    session_date = "2026-09-02"
    modes = {
        market: {
            "session_date": session_date,
            "signal_id": f"{market}-signal",
            "entry_completed_at": f"{session_date}T09:01:00+08:00",
            "entry_fill_policy": "causal_best_quote",
            "entry_price_offset_ticks": 0,
        }
        for market in guardian.EXPECTED_MARKETS
    }
    modes["tw_day_trade_100m"]["entry_fill_policy"] = (
        "official_open_signal_0900_execute_0901_vwap"
    )
    modes["tw_day_trade_multi_basis"].pop("entry_completed_at")

    missing, recovered = guardian._classify_session_signals(
        modes, session_date=session_date
    )

    assert missing == ["tw_day_trade_multi_basis"]
    assert recovered == ["tw_day_trade_100m"]


def test_guardian_rearms_failed_unit_without_restart(monkeypatch) -> None:
    commands: list[tuple[str, ...]] = []

    def fake_systemctl(*arguments: str) -> dict[str, object]:
        commands.append(arguments)
        return {"command": ["systemctl", *arguments], "returncode": 0}

    monkeypatch.setattr(guardian, "_run_systemctl", fake_systemctl)
    actions = guardian._repair_unit(
        "stockagent-discord-bot.service",
        timer=False,
        repair=True,
        action_state={},
        observed=time_sync.datetime.now(time_sync.TAIPEI),
        cooldown_seconds=300.0,
    )

    assert commands == [
        ("reset-failed", "stockagent-discord-bot.service"),
        ("enable", "--now", "stockagent-discord-bot.service"),
    ]
    assert all("restart" not in row["command"] for row in actions)


def test_guardian_clears_only_latch_for_receipted_oneshot(monkeypatch) -> None:
    commands: list[tuple[str, ...]] = []

    def fake_systemctl(*arguments: str) -> dict[str, object]:
        commands.append(arguments)
        return {"command": ["systemctl", *arguments], "returncode": 0}

    monkeypatch.setattr(guardian, "_run_systemctl", fake_systemctl)
    actions = guardian._clear_receipted_oneshot_failure(
        "stockagent-tw-day-trade-preopen-gate.service",
        repair=True,
        action_state={},
        observed=time_sync.datetime.now(time_sync.TAIPEI),
        cooldown_seconds=300.0,
    )

    assert commands == [
        ("reset-failed", "stockagent-tw-day-trade-preopen-gate.service")
    ]
    assert actions[0]["incident_receipt_retained"] is True
