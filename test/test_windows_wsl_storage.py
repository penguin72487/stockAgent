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


def test_compaction_only_reuses_graceful_owner_without_resizing_or_growth_reserve():
    assert "'Expand', 'Compact', 'CompactIfStopped'" in SCRIPT
    branch = SCRIPT.split("        if ($Mode -eq 'Compact') {", 1)[1].split("        } else {", 1)[0]
    assert "offline-compaction-verified-without-resize" in branch
    assert "virtual_bytes -ne $receipt.before.virtual_bytes" in branch
    assert "--resize" not in branch
    assert "$Mode -eq 'Compact' -and (Get-VhdObservation).host_free_bytes -lt 1GB" in SCRIPT
    assert "$Mode -eq 'Expand' -and (Get-VhdObservation).host_free_bytes -lt 32GB" in SCRIPT
    assert "Save-Receipt 'compacted-and-boot-verified'" in SCRIPT


def test_automatic_branch_never_stops_or_starts_wsl():
    branch = SCRIPT.split("if ($Mode -eq 'CompactIfStopped') {", 1)[1].split("    } else {\n        $running", 1)[0]
    assert "skipped-running" in branch
    for forbidden in ("Invoke-WslChecked", "Start-ScheduledTask", "Stop-ScheduledTask", "--shutdown", "--resize"):
        assert forbidden not in branch


def test_recovery_intent_precedes_task_disable_and_is_finally_restored():
    assert SCRIPT.index("$restoreRequired = $true") < SCRIPT.index("Disable-ScheduledTask")
    assert SCRIPT.index("Export-ScheduledTask") < SCRIPT.index("$restoreRequired = $true")
    restoration = SCRIPT.rsplit("} finally {", 1)[1]
    assert "if ($restoreRequired)" in restoration
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


def test_running_wsl_vm_requires_fresh_systemd_shutdown_evidence():
    assert "Test-ShutdownJournalEvent" in SCRIPT
    assert "--lines=0" in SCRIPT and "'_PID=1'" in SCRIPT
    assert "'stockagent-wsl-maint-watch'" in SCRIPT
    assert "systemd-poweroff-target-observed" in SCRIPT
    gate = SCRIPT.index("A still-running distribution has no fresh systemd poweroff evidence")
    shutdown = SCRIPT.index("$null = Invoke-WslChecked @('--shutdown')", gate)
    assert gate < shutdown
    assert "Another distribution started before VM shutdown; preserve it" in SCRIPT
    assert "Only the read-only observer created by this invocation is stopped" in SCRIPT


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


def test_expansion_reserves_whole_target_after_physical_allocation_not_virtual_delta():
    assert 'Get-FullAllocationGrowth $TargetSizeBytes $capacity.allocated_bytes' in SCRIPT
    assert 'Get-FullAllocationGrowth $TargetSizeBytes $receipt.before.allocated_bytes' in SCRIPT
    assert '$TargetSizeBytes - (Get-VHD -Path $vhdPath).Size' not in SCRIPT
    assert 'GetCompressedFileSizeW' in SCRIPT
    assert 'Get-FileAllocationBytes $vhdPath' in SCRIPT


def test_optional_data_expansion_is_pinned_and_does_not_replace_service_disk():
    assert '[string]$DataDistroName' in SCRIPT
    assert "$Mode -notin @('Plan', 'Compact')" in SCRIPT
    assert '$DataTargetSizeBytes -ne 3298534883328' in SCRIPT
    assert '$DataDistroName -eq $DistroName' in SCRIPT
    assert '$DataDistroName -in $CompanionDistroNames' in SCRIPT
    assert 'Data VHD physical E volume identity changed' in SCRIPT
    assert 'Refusing redirected data VHD ancestor' in SCRIPT
    assert 'Data ext4 UUID changed before shutdown' in SCRIPT
    assert 'Refusing data virtual-disk shrink' in SCRIPT
    assert 'filesystem_shrunk = $false' in SCRIPT


def test_data_resize_is_detached_and_fully_admitted_before_any_growth():
    branch = SCRIPT.split("        $null = Invoke-WslChecked @('--shutdown')", 1)[1]
    assert branch.index('$dataCheck.attached') < branch.index("'--manage', $DataDistroName, '--resize'")
    assert '$dataGrowth + 64GB' in branch
    assert 'Get-FullAllocationGrowth $DataTargetSizeBytes $dataCheck.allocated_bytes' in branch
    assert 'Data VHD target readback mismatch' in branch
    assert 'test "$fs_bytes" -eq __TARGET__' in branch
    assert 'expanded_and_guest_filesystem_verified' in branch


def test_other_running_distros_require_explicit_scope_and_restore_after_failure():
    assert "[string[]]$CompanionDistroNames = @()" in SCRIPT
    assert "$_ -ne $DistroName -and $_ -notin $CompanionDistroNames" in SCRIPT
    assert "Graceful companion poweroff timed out; no forced VM termination" in SCRIPT
    restoration = SCRIPT.rsplit("} finally {", 1)[1]
    assert "if ($restoreRequired)" in restoration
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
