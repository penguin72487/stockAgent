from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_windows_public_gateway_does_not_block_or_kill_wsl_bootstrap() -> None:
    launcher = _read("scripts/start_windows_public_caddy.ps1")
    installer = _read("scripts/install_windows_public_caddy.ps1")

    assert "systemctl start --no-block stockagent-public-dashboards.service" in launcher
    assert 'Request-WslGateway "backend_unhealthy"' in launcher
    assert "Test-GatewayBackend" in launcher
    assert "WSL gateway dispatch failed" in launcher
    assert "$wslBootstrapProcess.HasExited" in launcher
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
        "deploy/systemd/stockagent-shioaji-storage-monitor.timer.in": "OnBootSec=15min",
        "deploy/systemd/stockagent-openbb-l1-compaction.timer.in": "OnActiveSec=20min",
        "deploy/systemd/stockagent-shioaji-minute-backfill.timer.in": "OnActiveSec=30min",
    }
    for timer_path, expected_delay in expected_delays.items():
        timer = _read(timer_path)
        assert expected_delay in timer
        assert "WantedBy=timers.target" in timer

    compaction_timer = _read(
        "deploy/systemd/stockagent-openbb-l1-compaction.timer.in"
    )
    assert "Persistent=true" not in compaction_timer
    assert "OnUnitInactiveSec=30min" in compaction_timer
    minute_timer = _read(
        "deploy/systemd/stockagent-shioaji-minute-backfill.timer.in"
    )
    assert "Persistent=true" not in minute_timer
    assert "OnCalendar=Mon..Fri *-*-* 14:45:00 Asia/Taipei" in minute_timer


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


def test_artifact_dedup_waits_for_the_current_hot_sync_service() -> None:
    service = _read("deploy/systemd/stockagent-artifact-dedup.service.in")
    assert "stockagent-hot-artifact-sync.service" in service
    assert "stockagent-live-artifact-sync.service" not in service


def test_cold_boot_probe_requires_new_boot_and_all_public_surfaces() -> None:
    probe = _read("scripts/test_wsl_cold_boot_recovery.ps1")
    assert "& $wsl --shutdown" in probe
    assert "DefaultDistribution" in probe
    assert "DistributionName" in probe
    assert '--distribution $DistroName --exec' in probe
    assert '$output = $output -replace "`0", ""' in probe
    assert "$DistroName -notin $runningDistributions" in probe
    assert "target_distribution = $DistroName" in probe
    assert "$postBootId -ne $preBootId" in probe
    assert "systemctl --failed" in probe
    assert "systemctl is-system-running" in probe
    assert "Stop-Process -Id $_.ProcessId" in probe
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
