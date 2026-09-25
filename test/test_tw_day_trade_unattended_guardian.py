import json
from datetime import time as datetime_time
from pathlib import Path

from scripts import check_outside_tw_opening_resource_window as opening_window
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
    assert "stockagent-tw-day-trade-margin-actions.timer" in guardian
    assert "post_close_artifact_maintenance" in guardian
    assert "scheduled_day_trade_markets" in guardian
    assert "systemctl\", \"restart" not in guardian
    assert "production_order_possible\": False" in guardian
    assert "enabled_day_trade_markets" in guardian


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
    assert "waiting for stable TW public opening revision gate" in source
    assert "fcntl.LOCK_EX | fcntl.LOCK_NB" in source
    assert "applying TW public source event pid={process.pid}" in source
    assert "subprocess.Popen(command, cwd=REPO_ROOT)" in source


def test_paper_executor_is_protected_from_bulk_refresh_oom_pressure() -> None:
    service = (
        ROOT
        / "deploy/systemd/stockagent-tw-day-trade-simulation.service.in"
    ).read_text(encoding="utf-8")

    assert "OOMScoreAdjust=-500" in service
    assert "MemoryHigh=8G" in service
    assert "MemoryMax=16G" in service
    assert "MemorySwapMax=2G" in service
    assert "TasksMax=2048" in service

    for name in (
        "stockagent-registered-data-backfill.service.in",
        "stockagent-registered-data-daily.service.in",
        "stockagent-registered-data-intraday.service.in",
    ):
        bulk_service = (ROOT / "deploy/systemd" / name).read_text(
            encoding="utf-8"
        )
        assert "OOMScoreAdjust=750" in bulk_service
        assert "Slice=stockagent-heavy-data.slice" in bulk_service

    aggregate = (
        ROOT / "deploy/systemd/stockagent-heavy-data.slice.in"
    ).read_text(encoding="utf-8")
    assert "MemoryHigh=64G" in aggregate
    assert "MemoryMax=76G" in aggregate
    assert "MemorySwapMax=12G" in aggregate
    assert "TasksMax=8192" in aggregate


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


def test_guardian_accepts_both_causal_best_quote_paper_contracts() -> None:
    session_date = "2026-09-24"
    modes = {
        market: {
            "session_date": session_date,
            "signal_id": f"{market}-signal",
            "entry_completed_at": f"{session_date}T09:00:05+08:00",
            "entry_fill_policy": "causal_market_full_target_at_best_quote",
            "entry_price_offset_ticks": 0,
        }
        for market in guardian.EXPECTED_MARKETS
    }
    modes["tw_day_trade_100m"]["entry_fill_policy"] = "causal_best_quote"

    assert guardian._classify_session_signals(modes, session_date=session_date) == (
        [],
        [],
    )

    modes["tw_day_trade_multi_basis"]["entry_fill_policy"] = (
        "official_open_signal_0900_execute_0901_vwap"
    )
    modes["tw_day_trade_multi_basis_22"]["entry_price_offset_ticks"] = 1
    assert guardian._classify_session_signals(modes, session_date=session_date) == (
        [],
        ["tw_day_trade_multi_basis", "tw_day_trade_multi_basis_22"],
    )


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


def test_guardian_pauses_and_resumes_bulk_jobs_around_open(monkeypatch) -> None:
    commands: list[tuple[str, ...]] = []
    managed_units = {
        unit
        for pair in guardian.BEST_EFFORT_MAINTENANCE_UNITS.items()
        for unit in pair
    }
    active = {unit: "active" for unit in managed_units}

    monkeypatch.setattr(
        guardian,
        "_systemctl_show",
        lambda unit: {"ActiveState": active[unit], "SubState": "running"},
    )

    def fake_systemctl(*arguments: str) -> dict[str, object]:
        commands.append(arguments)
        unit = arguments[-1]
        active[unit] = "inactive" if arguments[0] == "stop" else "active"
        return {"command": ["systemctl", *arguments], "returncode": 0}

    monkeypatch.setattr(guardian, "_run_systemctl", fake_systemctl)
    action_state: dict[str, object] = {}
    guard, pause_actions, failures = guardian._protect_opening_resources(
        observed=time_sync.datetime(2026, 9, 14, 8, 30, tzinfo=guardian.TAIPEI),
        repair=True,
        action_state=action_state,
    )

    assert guard["protected"] is True
    assert failures == []
    assert len(pause_actions) == len(managed_units)
    assert all(command[:2] == ("stop", "--no-block") for command in commands)
    assert all(row["timer_active_state"] == "active" for row in guard["units"].values())

    commands.clear()
    guard, resume_actions, failures = guardian._protect_opening_resources(
        observed=time_sync.datetime(2026, 9, 14, 9, 10, tzinfo=guardian.TAIPEI),
        repair=True,
        action_state=action_state,
    )

    assert guard["protected"] is False
    assert failures == []
    assert len(resume_actions) == len(managed_units)
    assert all(command[:2] == ("start", "--no-block") for command in commands)


def test_guardian_does_not_pause_bulk_jobs_on_verified_closed_weekday(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        guardian,
        "_systemctl_show",
        lambda _unit: {"ActiveState": "active", "SubState": "running"},
    )
    monkeypatch.setattr(
        guardian,
        "_run_systemctl",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("closed session must not pause maintenance")
        ),
    )
    guard, actions, failures = guardian._protect_opening_resources(
        observed=time_sync.datetime(2026, 9, 25, 8, 30, tzinfo=guardian.TAIPEI),
        repair=True,
        action_state={},
        session_open=False,
    )
    assert guard["protected"] is False
    assert actions == []
    assert failures == []


def test_opening_resource_guard_never_manages_critical_services() -> None:
    managed = set(guardian.BEST_EFFORT_MAINTENANCE_UNITS)

    assert "stockagent-openbb-archive.service" in managed
    assert (
        guardian.BEST_EFFORT_MAINTENANCE_UNITS[
            "stockagent-openbb-archive.service"
        ]
        == "stockagent-openbb-archive.timer"
    )
    assert managed.isdisjoint(guardian.REQUIRED_SERVICES)
    assert "stockagent-discord-bot.service" not in managed
    assert "stockagent-tw-day-trade-simulation.service" not in managed
    assert "stockagent-public-dashboards.service" not in managed


def test_guardian_pauses_active_timer_without_starting_idle_service(
    monkeypatch,
) -> None:
    commands: list[tuple[str, ...]] = []
    active = {
        service: "inactive"
        for service in guardian.BEST_EFFORT_MAINTENANCE_UNITS
    }
    active.update(
        {
            timer: "active"
            for timer in guardian.BEST_EFFORT_MAINTENANCE_UNITS.values()
        }
    )
    monkeypatch.setattr(
        guardian,
        "_systemctl_show",
        lambda unit: {
            "ActiveState": active[unit],
            "SubState": "waiting" if active[unit] == "active" else "dead",
        },
    )

    def fake_systemctl(*arguments: str) -> dict[str, object]:
        commands.append(arguments)
        unit = arguments[-1]
        active[unit] = "inactive" if arguments[0] == "stop" else "active"
        return {"command": ["systemctl", *arguments], "returncode": 0}

    monkeypatch.setattr(guardian, "_run_systemctl", fake_systemctl)
    action_state: dict[str, object] = {}
    guardian._protect_opening_resources(
        observed=time_sync.datetime(2026, 9, 14, 8, 20, tzinfo=guardian.TAIPEI),
        repair=True,
        action_state=action_state,
    )

    assert {command[-1] for command in commands} == set(
        guardian.BEST_EFFORT_MAINTENANCE_UNITS.values()
    )

    commands.clear()
    guardian._protect_opening_resources(
        observed=time_sync.datetime(2026, 9, 14, 9, 10, tzinfo=guardian.TAIPEI),
        repair=True,
        action_state=action_state,
    )

    assert {command[-1] for command in commands} == set(
        guardian.BEST_EFFORT_MAINTENANCE_UNITS.values()
    )
    assert all(not command[-1].endswith(".service") for command in commands)


def test_guardian_treats_long_oneshot_activating_state_as_running(
    monkeypatch,
) -> None:
    service = "bulk.service"
    timer = "bulk.timer"
    states = {service: "activating", timer: "active"}
    commands: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        guardian, "BEST_EFFORT_MAINTENANCE_UNITS", {service: timer}
    )
    monkeypatch.setattr(
        guardian,
        "_systemctl_show",
        lambda unit: {"ActiveState": states[unit], "SubState": "start"},
    )

    def fake_systemctl(*arguments: str) -> dict[str, object]:
        commands.append(arguments)
        return {"command": ["systemctl", *arguments], "returncode": 0}

    monkeypatch.setattr(guardian, "_run_systemctl", fake_systemctl)
    guardian._protect_opening_resources(
        observed=time_sync.datetime(2026, 9, 14, 8, 30, tzinfo=guardian.TAIPEI),
        repair=True,
        action_state={},
    )

    assert ("stop", "--no-block", service) in commands
    assert ("stop", "--no-block", timer) in commands


def test_opening_resource_guard_write_ahead_journals_before_systemd_mutation(
    monkeypatch, tmp_path: Path
) -> None:
    service = "bulk.service"
    timer = "bulk.timer"
    active = {service: "active", timer: "active"}
    monkeypatch.setattr(
        guardian, "BEST_EFFORT_MAINTENANCE_UNITS", {service: timer}
    )
    monkeypatch.setattr(
        guardian,
        "_systemctl_show",
        lambda unit: {"ActiveState": active[unit], "SubState": "running"},
    )
    action_path = tmp_path / "action_state.json"

    def fake_systemctl(*arguments: str) -> dict[str, object]:
        persisted = json.loads(action_path.read_text(encoding="utf-8"))
        assert persisted[f"opening-resource-pause:{service}"]["resume_pending"]
        unit = arguments[-1]
        active[unit] = "inactive" if arguments[0] == "stop" else "active"
        return {"command": ["systemctl", *arguments], "returncode": 0}

    monkeypatch.setattr(guardian, "_run_systemctl", fake_systemctl)
    state: dict[str, object] = {}
    guardian._protect_opening_resources(
        observed=time_sync.datetime(
            2026, 9, 14, 8, 20, tzinfo=guardian.TAIPEI
        ),
        repair=True,
        action_state=state,
        action_path=action_path,
    )

    persisted = json.loads(action_path.read_text(encoding="utf-8"))
    assert persisted[f"opening-resource-pause:{service}"]["stop_succeeded"]

    guardian._protect_opening_resources(
        observed=time_sync.datetime(
            2026, 9, 14, 9, 10, tzinfo=guardian.TAIPEI
        ),
        repair=True,
        action_state=state,
        action_path=action_path,
    )
    persisted = json.loads(action_path.read_text(encoding="utf-8"))
    assert not persisted[f"opening-resource-pause:{service}"]["resume_pending"]


def test_bulk_services_fail_safe_before_guardian_can_stop_them() -> None:
    root = Path(__file__).resolve().parents[1]
    wrapper = (root / "scripts/run_outside_tw_opening_resource_window.sh").read_text(
        encoding="utf-8"
    )
    assert "resolve_fintech_python" in wrapper
    for service in guardian.BEST_EFFORT_MAINTENANCE_SERVICES:
        template = (
            root / "deploy/systemd" / f"{service}.in"
        ).read_text(encoding="utf-8")
        assert "ExecCondition=" in template
        assert "run_outside_tw_opening_resource_window.sh" in template

    for timestamp, allowed in (
        ("2026-09-14T08:19:59+08:00", True),
        ("2026-09-14T08:20:00+08:00", False),
        ("2026-09-14T09:09:59+08:00", False),
        ("2026-09-14T09:10:00+08:00", True),
        ("2026-09-13T08:30:00+08:00", True),
    ):
        result = opening_window.evaluate(time_sync.datetime.fromisoformat(timestamp))
        assert result["allowed"] is allowed


def test_openbb_compaction_reserves_full_runway_before_opening() -> None:
    template = _read("deploy/systemd/stockagent-openbb-l1-compaction.service.in")
    assert "run_outside_tw_opening_resource_window.sh" in template
    assert "--minimum-runway-minutes 45" in template
    assert "--protected-until 13:35" in template
    for timestamp, allowed in (
        ("2026-09-14T07:34:59+08:00", True),
        ("2026-09-14T07:35:00+08:00", False),
        ("2026-09-14T08:18:00+08:00", False),
        ("2026-09-14T09:09:59+08:00", False),
        ("2026-09-14T09:10:00+08:00", False),
        ("2026-09-14T13:34:59+08:00", False),
        ("2026-09-14T13:35:00+08:00", True),
        ("2026-09-13T08:30:00+08:00", True),
    ):
        result = opening_window.evaluate(
            time_sync.datetime.fromisoformat(timestamp),
            minimum_runway_minutes=45,
            protected_until=datetime_time(13, 35),
        )
        assert result["allowed"] is allowed
        assert result["protected_start"] == "07:35:00"


def test_disk_guard_warns_before_it_reaches_the_fail_closed_floor(
    monkeypatch, tmp_path: Path
) -> None:
    usage = type("Usage", (), {"total": 100, "used": 90, "free": 10})()
    monkeypatch.setattr(guardian.shutil, "disk_usage", lambda _path: usage)

    result = guardian._disk_health(
        tmp_path,
        minimum_gib=0.0,
        minimum_percent=5.0,
        warning_percent=15.0,
    )

    assert result["ready"] is True
    assert result["warning"] is True
    assert result["policy"]["automatic_deletion"] is False


def test_required_service_resource_pressure_warns_before_cgroup_exhaustion() -> None:
    result = guardian._unit_resource_pressure(
        {
            "MemoryCurrent": "900",
            "MemoryHigh": "1000",
            "MemoryMax": "2000",
            "TasksCurrent": "10",
            "TasksMax": "100",
        }
    )

    assert result["memory_high_ratio"] == 0.9
    assert result["memory_max_ratio"] == 0.45
    assert result["warning"] is True


def test_opening_resource_guard_preserves_maintenance_failure_evidence(
    monkeypatch,
) -> None:
    service = "bulk.service"
    timer = "bulk.timer"
    monkeypatch.setattr(
        guardian, "BEST_EFFORT_MAINTENANCE_UNITS", {service: timer}
    )

    def fake_show(unit: str) -> dict[str, str]:
        if unit == service:
            return {
                "ActiveState": "failed",
                "SubState": "failed",
                "Result": "oom-kill",
                "NRestarts": "0",
            }
        return {"ActiveState": "active", "SubState": "waiting"}

    monkeypatch.setattr(guardian, "_systemctl_show", fake_show)
    result, actions, failures = guardian._protect_opening_resources(
        observed=time_sync.datetime(
            2026, 9, 14, 10, 0, tzinfo=guardian.TAIPEI
        ),
        repair=False,
        action_state={},
    )

    assert actions == []
    assert failures == []
    assert result["units"][service]["result"] == "oom-kill"
    assert result["units"][service]["restart_count"] == "0"
