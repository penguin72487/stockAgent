"""Safety boundaries for the Windows-only offline VHD maintenance worker."""
from pathlib import Path


SCRIPT = (Path(__file__).resolve().parents[1] /
          "scripts/maintain_windows_wsl_storage.ps1").read_text(encoding="utf-8")


def test_no_unsafe_sparse_conversion_or_zero_fill():
    for forbidden in ("--allow-unsafe", "--set-sparse", "fsutil sparse", "dd if=", "Clear-Disk", "Resize-VHD"):
        assert forbidden not in SCRIPT


def test_default_is_plan_and_mutation_requires_apply():
    assert "[string]$Mode = 'Plan'" in SCRIPT
    assert "if ($Mode -eq 'Plan' -or -not $Apply)" in SCRIPT
    assert SCRIPT.index("if ($Mode -eq 'Plan' -or -not $Apply)") < SCRIPT.index("$mutex =")


def test_explicit_three_tib_cap_alignment_and_no_shrink():
    assert "$TargetSizeBytes -gt 3298534883328" in SCRIPT
    assert "$TargetSizeBytes % 1MB -ne 0" in SCRIPT
    assert "throw 'Refusing virtual-disk shrink'" in SCRIPT
    assert "($TargetSizeBytes.ToString() + 'B')" in SCRIPT


def test_identity_and_native_worker_guard():
    assert "Registered VHD does not match the pinned native Windows path" in SCRIPT
    assert "ReparsePoint" in SCRIPT
    assert "$before.ParentPath" in SCRIPT
    assert "$PSCommandPath.StartsWith('\\\\')" in SCRIPT
    assert "WindowsBuiltInRole]::Administrator" in SCRIPT


def test_shared_single_owner_and_read_only_compaction():
    assert "'Local\\StockAgent.WslStorage.'" in SCRIPT
    assert "Mount-VHD -Path $vhdPath -ReadOnly -NoDriveLetter" in SCRIPT
    assert "Optimize-VHD -Path $vhdPath -Mode Quick" in SCRIPT
    assert "finally {\n        if ($mountedByUs) { Dismount-VHD" in SCRIPT


def test_automatic_branch_never_stops_or_starts_wsl():
    branch = SCRIPT.split("if ($Mode -eq 'CompactIfStopped') {", 1)[1].split("    } else {\n        $running", 1)[0]
    assert "skipped-running" in branch
    for forbidden in ("Invoke-WslChecked", "Start-ScheduledTask", "Stop-ScheduledTask", "--shutdown", "--resize"):
        assert forbidden not in branch


def test_recovery_intent_precedes_task_disable_and_is_finally_restored():
    assert SCRIPT.index("$restoreRequired = $true") < SCRIPT.index("Disable-ScheduledTask")
    assert SCRIPT.index("Export-ScheduledTask") < SCRIPT.index("$restoreRequired = $true")
    restoration = SCRIPT.split("} finally {\n    if ($restoreRequired)", 1)[1]
    assert "if ($task.enabled)" in restoration
    assert "if ($task.running)" in restoration
    assert "Start-ScheduledTask" in restoration
    assert "canonical-boot-audit-passed" in restoration


def test_graceful_shutdown_does_not_force_after_timeout():
    assert "'/bin/systemctl', 'poweroff'" in SCRIPT
    assert "Graceful poweroff timed out; no forced VM termination" in SCRIPT
    assert "another distribution is running" in SCRIPT
    assert "Stop-Process" not in SCRIPT
    assert "--terminate" not in SCRIPT


def test_no_data_cleanup_or_other_task_control():
    assert "Remove-Item" not in SCRIPT
    assert "OfflineRepair" not in SCRIPT
    assert "Preserve Crash Dumps" not in SCRIPT
    assert "One-Time WSL Filesystem Repair" not in SCRIPT


def test_capacity_admission_keeps_full_growth_reserve_after_measured_compaction():
    branch = SCRIPT.split("    } else {\n        $running", 1)[1]
    assert branch.index("Compact-StoppedVhd") < branch.index("$growth =")
    assert "host_free_bytes -ge ($growth + 32GB)" in branch
    assert branch.index("resize_admission.admitted") < branch.index("'offline-resize-started'")
    assert "$expectedBootBytes = [long](Get-VHD -Path $vhdPath).Size" in branch
    assert ".Replace('__TARGET__', $expectedBootBytes.ToString())" in branch
    assert 'test "$fs_bytes" -eq __TARGET__' in branch
    assert 'LC_ALL=C tune2fs -l' in branch


def test_other_running_distros_require_explicit_scope_and_restore_after_failure():
    assert "[string[]]$CompanionDistroNames = @()" in SCRIPT
    assert "$_ -ne $DistroName -and $_ -notin $CompanionDistroNames" in SCRIPT
    assert "Graceful companion poweroff timed out; no forced VM termination" in SCRIPT
    restoration = SCRIPT.split("} finally {\n    if ($restoreRequired)", 1)[1]
    assert "foreach ($companion in $companionShutdownRequested)" in restoration
    assert "'/usr/bin/timeout', '180'" in restoration
    assert "$WslArguments = @('--cd', '/') + $WslArguments" in SCRIPT


def test_hourly_trim_uses_existing_owner_and_admits_wsl_root_only():
    root = Path(__file__).resolve().parents[1]
    timer = (root / "deploy/systemd/fstrim.timer.d/stockagent-hourly.conf").read_text()
    service = (root / "deploy/systemd/fstrim.service.d/stockagent-wsl-root.conf").read_text()
    assert "OnCalendar=\nOnCalendar=hourly" in timer
    for text in (timer, service):
        assert "ConditionVirtualization=\nConditionVirtualization=wsl" in text
    assert "ExecStart=\nExecStart=/sbin/fstrim --verbose /\n" in service
