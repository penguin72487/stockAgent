from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_windows_public_gateway_does_not_block_or_kill_wsl_bootstrap() -> None:
    launcher = _read("scripts/start_windows_public_caddy.ps1")
    installer = _read("scripts/install_windows_public_caddy.ps1")

    assert "systemctl $verb --no-block stockagent-public-dashboards.service" in launcher
    assert 'Request-WslGateway "backend_unhealthy"' in launcher
    assert "Test-GatewayBackend" in launcher
    assert "Test-GatewayListener" in launcher
    assert "$consecutiveBackendFailures -ge $restartAfterFailures" in launcher
    assert '"backend_sustained_unresponsive" $true' in launcher
    assert '$verb = if ($RestartService) { "restart" } else { "start" }' in launcher
    assert "WSL gateway dispatch failed" in launcher
    assert "$wslBootstrapProcess.HasExited" in launcher
    assert "function Record-WslGatewayCompletion" in launcher
    assert "exit_code=$($wslBootstrapProcess.ExitCode)" in launcher
    assert "elapsed_seconds=$elapsed" in launcher
    assert "$wslBootstrapProcess.ExitTime" in launcher
    assert "observed_lag_seconds=$observedLag" in launcher
    assert "--distribution $DistroName --exec" in launcher
    assert "--distribution `\"$DistroName`\" --exec" not in launcher
    assert "WSL distribution name cannot be passed safely" in launcher
    assert "Record-WslGatewayCompletion\n    try" in launcher
    assert "Get-CaddyProcesses" in launcher
    assert "WaitForExit" not in launcher
    assert ".Kill(" not in launcher
    assert "while ($true)" in launcher
    assert "Start-CaddyIfNeeded" in launcher
    assert "-DistroName" in installer
    assert "-CaddyPath" in installer
    assert "New-ScheduledTaskTrigger -AtStartup" in installer
    assert '-LogonType S4U' in installer
    assert '-Principal $principal' in installer
    assert "pre_login_recovery=$preLoginRecovery" in installer
    assert "at-logon self-healing fallback" in installer
    assert "-User $currentUser" in installer
    assert "-RepetitionInterval (New-TimeSpan -Minutes 1)" in installer
    assert "-MultipleInstances IgnoreNew" in installer
    assert "-StartWhenAvailable" in installer


def test_expensive_recovery_jobs_are_timer_only_and_staggered() -> None:
    service_paths = (
        "deploy/systemd/stockagent-taifex-futures-daily.service.in",
        "deploy/systemd/stockagent-openbb-archive.service.in",
        "deploy/systemd/stockagent-shioaji-minute-backfill.service.in",
        "deploy/systemd/stockagent-shioaji-tx-history-backfill.service.in",
    )
    for service_path in service_paths:
        service = _read(service_path)
        assert "WantedBy=multi-user.target" not in service

    expected_delays = {
        "deploy/systemd/stockagent-openbb-archive.timer.in": "OnBootSec=5min",
        "deploy/systemd/stockagent-shioaji-tx-history-backfill.timer.in": "OnBootSec=10min",
        "deploy/systemd/stockagent-openbb-l1-compaction.timer.in": "OnActiveSec=20min",
        "deploy/systemd/stockagent-shioaji-minute-backfill.timer.in": "OnActiveSec=30min",
    }
    for timer_path, expected_delay in expected_delays.items():
        timer = _read(timer_path)
        assert expected_delay in timer
        assert "WantedBy=timers.target" in timer

    archive_service = _read(
        "deploy/systemd/stockagent-openbb-archive.service.in"
    )
    archive_timer = _read("deploy/systemd/stockagent-openbb-archive.timer.in")
    assert "run_outside_tw_opening_resource_window.sh" in archive_service
    assert "OnCalendar=Mon..Fri *-*-* 09:12:00 Asia/Taipei" in archive_timer
    assert "OnCalendar=*-*-* 15:15:00 Asia/Taipei" in archive_timer
    assert "Persistent=false" in archive_timer

    storage_timer = _read(
        "deploy/systemd/stockagent-shioaji-storage-monitor.timer.in"
    )
    assert "OnCalendar=*-*-* 00..07,14..23:09:00 Asia/Taipei" in storage_timer
    assert "OnBootSec=" not in storage_timer
    assert "OnUnitActiveSec=" not in storage_timer
    assert "Persistent=false" in storage_timer

    crypto_timer = _read(
        "deploy/systemd/stockagent-crypto-training-refresh.timer.in"
    )
    assert "OnCalendar=*-*-* 02:30:00 Asia/Taipei" in crypto_timer
    assert "OnCalendar=*-*-* 16:30:00 Asia/Taipei" in crypto_timer
    assert "OnCalendar=*-*-* 22:30:00 Asia/Taipei" in crypto_timer
    assert "10:30:00" not in crypto_timer
    assert "Persistent=false" in crypto_timer

    intraday_timer = _read(
        "deploy/systemd/stockagent-registered-data-intraday.timer.in"
    )
    assert "OnActiveSec=1min" in intraday_timer
    assert "OnUnitInactiveSec=1min" in intraday_timer
    assert "OnBootSec=" not in intraday_timer

    compaction_timer = _read(
        "deploy/systemd/stockagent-openbb-l1-compaction.timer.in"
    )
    assert "Persistent=true" not in compaction_timer
    assert "OnUnitInactiveSec=30min" in compaction_timer
    minute_timer = _read(
        "deploy/systemd/stockagent-shioaji-minute-backfill.timer.in"
    )
    assert "Persistent=true" not in minute_timer
    assert "OnCalendar=Mon..Fri *-*-* 14:31:00 Asia/Taipei" in minute_timer
    for service_path in (
        "deploy/systemd/stockagent-shioaji-minute-backfill.service.in",
        "deploy/systemd/stockagent-shioaji-historical-market-data.service.in",
        "deploy/systemd/stockagent-shioaji-tx-history-backfill.service.in",
    ):
        service = _read(service_path)
        assert "run_outside_tw_opening_resource_window.sh" in service
        assert "--minimum-runway-minutes 35 --protected-until 14:31" in service


def test_completion_relative_timers_have_independent_restart_anchor() -> None:
    """A completed service may have no new inactive event after timer restart."""
    for timer_path in sorted((ROOT / "deploy/systemd").glob("*.timer.in")):
        directives = {
            line.split("=", 1)[0].strip()
            for line in timer_path.read_text(encoding="utf-8").splitlines()
            if "=" in line and not line.lstrip().startswith(("#", ";"))
        }
        if not directives.intersection({"OnUnitInactiveSec", "OnUnitActiveSec"}):
            continue
        assert directives.intersection({"OnActiveSec", "OnCalendar"}), timer_path.name


def test_intraday_timer_has_isolated_rearm_install_mode() -> None:
    installer = _read("scripts/install_registered_data_refresh_services.sh")
    assert '"${1:-}" == "intraday-timer-only"' in installer
    assert 'units=(stockagent-registered-data-intraday.timer)' in installer
    assert 'systemctl restart stockagent-registered-data-intraday.timer' in installer
    assert 'if [[ "$timer_substate" == "elapsed" ]]' in installer
    assert 'verify_units=("$temporary_dir"/*)' in installer


def test_expensive_job_installers_remove_legacy_boot_symlinks() -> None:
    installers = (
        "scripts/install_taifex_futures_daily_service.sh",
        "scripts/install_openbb_archive_service.sh",
        "scripts/install_shioaji_minute_backfill_service.sh",
        "scripts/install_shioaji_tx_history_backfill_service.sh",
    )
    for installer_path in installers:
        installer = _read(installer_path)
        assert "--run-now" in installer
        assert "systemctl disable" in installer
        assert "enable --now" in installer


def test_postclose_futures_refresh_targets_the_completed_same_day_session() -> None:
    runner = _read("scripts/run_taifex_all_futures_daily.sh")

    assert "TAIFEX_FUTURES_TARGET_SESSION" in runner
    assert "TZ=Asia/Taipei date +%F" in runner
    assert '--end-date "$target_session"' in runner


def test_stock_minute_download_is_independent_but_curve_consumer_requires_exact_tx() -> None:
    runner = _read("scripts/run_shioaji_minute_full_backfill.sh")
    curves = _read("scripts/maintain_tw_day_trade_minute_curves.py")

    assert "latest_completed_tw_stock_session" in runner
    assert "latest_completed_futures_session" not in runner
    assert "futures_history=independent_downstream_gate" in runner
    assert "_valid_receipt(tx_history_root, trading_date)" in curves
    assert '"status": "waiting_source"' in curves


def test_artifact_dedup_does_not_depend_on_retired_hot_sync_service() -> None:
    service = _read("deploy/systemd/stockagent-artifact-dedup.service.in")
    assert "stockagent-hot-artifact-sync.service" not in service
    assert "stockagent-live-artifact-sync.service" not in service


def test_cold_boot_probe_requires_new_boot_and_all_public_surfaces() -> None:
    probe = _read("scripts/test_wsl_cold_boot_recovery.ps1")
    assert '"stockagent-hot-artifact-sync.service"' not in probe
    assert "& $wsl --shutdown" in probe
    assert "DefaultDistribution" in probe
    assert "DistributionName" in probe
    assert '--distribution $DistroName --exec' in probe
    assert '$output = $output -replace "`0", ""' in probe
    assert "$DistroName -notin $runningDistributions" in probe
    assert "wsl_restart_observed_at" in probe
    assert "Read boot_id only after" in probe
    assert "target_distribution = $DistroName" in probe
    assert "$postBootId -ne $preBootId" in probe
    assert "systemctl --failed" in probe
    assert "systemctl is-system-running" in probe
    assert "Stop-Process -Id $_.ProcessId" in probe
    assert "Get-Command caddy.exe" in probe
    assert "function Stop-CaddyViaAdmin" in probe
    assert "& $caddy stop --address 127.0.0.1:2019" in probe
    assert '$ErrorActionPreference = "SilentlyContinue"' in probe
    assert "Get-NetTCPConnection -State Listen" in probe
    assert "$_.LocalPort -in 80, 443" in probe
    assert "[int]$_.ProcessId -in $listenerPids" in probe
    assert "ForEach-Object { $_.Trim() }" in probe
    assert "--list --running --quiet" in probe
    assert "wsl_stopped_observed" in probe
    for path in (
        "/taifex/api/status",
        "/tw-day-trade/api/status",
        "/shioaji/api/status",
        "/openbb/api/status",
        "/data-monitor/api/status",
        "/traffic/api/status",
    ):
        assert f'"{path}"' in probe
