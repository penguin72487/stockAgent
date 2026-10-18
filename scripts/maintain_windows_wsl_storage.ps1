# Run from a Windows-native copy: shutting WSL stops scripts on its UNC share.
[CmdletBinding()]
param(
    [ValidateSet('Plan', 'Expand', 'CompactIfStopped')]
    [string]$Mode = 'Plan',
    [Parameter(Mandatory = $true)][string]$DistroName,
    [Parameter(Mandatory = $true)][string]$ExpectedVhdPath,
    [long]$TargetSizeBytes = 2499999170560,
    [string]$RepoRoot = '/root/stockAgent',
    [string]$ReceiptRoot = (Join-Path $env:LOCALAPPDATA 'StockAgent\WslStorage\receipts'),
    [ValidateRange(0, 300)][int]$DelaySeconds = 0,
    [switch]$Apply
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false
$wsl = Join-Path $env:WINDIR 'System32\wsl.exe'
if ($DistroName -notmatch '^[A-Za-z0-9_.-]+$' -or
    $RepoRoot -notmatch '^/[A-Za-z0-9_./-]+$') {
    throw 'Unsafe distribution or repository argument'
}
if ($TargetSizeBytes -le 0 -or $TargetSizeBytes -gt 2500000000000 -or
    $TargetSizeBytes % 1MB -ne 0) {
    throw 'Target must be positive, MiB-aligned and no greater than decimal 2.5 TB'
}
$registration = @(Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss' |
    Get-ItemProperty | Where-Object DistributionName -eq $DistroName)
if ($registration.Count -ne 1 -or $registration[0].Version -ne 2) {
    throw 'Exactly one registered WSL2 distribution is required'
}
$vhdName = 'ext4.vhdx'
if ($registration[0].PSObject.Properties['VhdFileName']) {
    $vhdName = [string]$registration[0].VhdFileName
}
$vhdPath = [IO.Path]::GetFullPath((Join-Path $registration[0].BasePath $vhdName))
if (-not $vhdPath.Equals([IO.Path]::GetFullPath($ExpectedVhdPath),
        [StringComparison]::OrdinalIgnoreCase) -or $vhdPath.StartsWith('\\')) {
    throw 'Registered VHD does not match the pinned native Windows path'
}
$vhdFile = Get-Item -LiteralPath $vhdPath
if ($vhdFile.Extension -ne '.vhdx' -or
    ($vhdFile.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw 'Refusing non-VHDX or reparse-point target'
}
$before = Get-VHD -Path $vhdPath
if ([string]$before.VhdType -ne 'Dynamic' -or $before.ParentPath) {
    throw 'Only an independent dynamic VHD is supported'
}
$taskNames = @('StockAgent Public Caddy', 'StockAgent TEJ Desktop Recovery',
    'StockAgent TEJ WSL Startup')
$tasks = @($taskNames | ForEach-Object {
    $task = Get-ScheduledTask -TaskName $_ -TaskPath '\'
    [pscustomobject]@{ name = $_; enabled = [bool]$task.Settings.Enabled;
        running = ([string]$task.State -eq 'Running') }
})

function Get-RunningDistributions {
    $lines = ((& $wsl --list --running --quiet 2>&1) | Out-String) -replace "`0", ''
    if ($LASTEXITCODE -ne 0) { throw "Cannot query running WSL distributions: $lines" }
    return @($lines -split '\r?\n' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
}
function Get-VhdObservation {
    $disk = Get-VHD -Path $vhdPath
    return [ordered]@{ virtual_bytes = [long]$disk.Size; file_bytes = [long]$disk.FileSize;
        attached = [bool]$disk.Attached;
        host_free_bytes = [long]([IO.DriveInfo]::new([IO.Path]::GetPathRoot($vhdPath))).AvailableFreeSpace }
}
$receipt = [ordered]@{
    contract = 'stockagent-wsl-storage-maintenance-v1'; mode = $Mode; state = 'planned';
    distro = $DistroName; registration = $registration[0].PSChildName; vhd_path = $vhdPath;
    target_bytes = $TargetSizeBytes; before = Get-VhdObservation;
    running_distributions = @(Get-RunningDistributions); tasks = $tasks;
    sparse_enabled_by_worker = $false; instantaneous_reclaim = $false;
    script_sha256 = (Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash;
    errors = @(); events = @(); recovery = $null
}
if ($Mode -eq 'Plan' -or -not $Apply) {
    $receipt | ConvertTo-Json -Depth 12
    return
}
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
if (-not ([Security.Principal.WindowsPrincipal]::new($identity)).IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Apply requires an elevated Windows administrator token'
}
if ($PSCommandPath.StartsWith('\\') -or $ReceiptRoot.StartsWith('\\')) {
    throw 'Worker and receipts must survive WSL shutdown on a native Windows volume'
}
$mutex = [Threading.Mutex]::new($false, 'Local\StockAgent.WslStorage.' + $registration[0].PSChildName)
$locked = $false
try { $locked = $mutex.WaitOne(0) } catch [Threading.AbandonedMutexException] { $locked = $true }
if (-not $locked) { $mutex.Dispose(); throw 'Another WSL storage maintenance owner is running' }
New-Item -ItemType Directory -Force -Path $ReceiptRoot | Out-Null
$runRoot = Join-Path $ReceiptRoot ((Get-Date -Format 'yyyyMMddTHHmmssfff') + '-' + $Mode)
New-Item -ItemType Directory -Path $runRoot | Out-Null
$receiptPath = Join-Path $runRoot 'status.json'

function Save-Receipt([string]$State) {
    $receipt.state = $State
    $receipt['observed_at_utc'] = [DateTime]::UtcNow.ToString('o')
    $temporary = $receiptPath + '.tmp'
    [IO.File]::WriteAllText($temporary, ($receipt | ConvertTo-Json -Depth 16), [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temporary -Destination $receiptPath -Force
}
function Add-Event([string]$Name) {
    $receipt.events += [ordered]@{ at_utc = [DateTime]::UtcNow.ToString('o'); name = $Name }
    Save-Receipt 'running'
}
function Invoke-WslChecked([string[]]$WslArguments) {
    $output = (& $wsl @WslArguments 2>&1 | Out-String)
    $code = $LASTEXITCODE
    [IO.File]::AppendAllText((Join-Path $runRoot 'wsl.log'), $output)
    if ($code -ne 0) { throw "wsl.exe exited $code; see wsl.log" }
    return $output
}
function Compact-StoppedVhd {
    if ($DistroName -in @(Get-RunningDistributions) -or (Get-VHD -Path $vhdPath).Attached) {
        return $false
    }
    $mountedByUs = $false
    try {
        # The read-only attachment is also the race guard against a concurrent boot.
        Mount-VHD -Path $vhdPath -ReadOnly -NoDriveLetter | Out-Null
        $mountedByUs = $true
        Add-Event 'read-only-vhd-mounted'
        # Root ext4 already supports discard. Do not zero-fill or scan 2 TB of live data.
        Optimize-VHD -Path $vhdPath -Mode Quick
        Add-Event 'quick-compaction-completed'
    } finally {
        if ($mountedByUs) { Dismount-VHD -Path $vhdPath }
    }
    return $true
}

$restoreRequired = $false
$shutdownRequested = $false
$exitCode = 0
try {
    Save-Receipt 'admitted'
    if ($DelaySeconds) { Start-Sleep -Seconds $DelaySeconds }
    if ($Mode -eq 'CompactIfStopped') {
        # This branch NEVER launches or stops a distro, changes tasks, or resizes it.
        if ($DistroName -in @(Get-RunningDistributions) -or (Get-VHD -Path $vhdPath).Attached) {
            Save-Receipt 'skipped-running'
        } else {
            $compacted = Compact-StoppedVhd
            $receipt['after'] = Get-VhdObservation
            if ($receipt.after.virtual_bytes -ne $receipt.before.virtual_bytes) {
                throw 'Compaction unexpectedly changed virtual capacity'
            }
            if ($compacted) { Save-Receipt 'compacted' } else { Save-Receipt 'skipped-running' }
        }
    } else {
        $running = @(Get-RunningDistributions)
        if (@($running | Where-Object { $_ -ne $DistroName }).Count) {
            throw 'Refusing global WSL shutdown while another distribution is running'
        }
        if ($DistroName -notin $running) {
            throw 'Expansion requires a running distro for a fresh filesystem identity baseline'
        }
        if ((Get-VHD -Path $vhdPath).Size -gt $TargetSizeBytes) { throw 'Refusing virtual-disk shrink' }
        $growth = $TargetSizeBytes - (Get-VHD -Path $vhdPath).Size
        if ((Get-VhdObservation).host_free_bytes -lt ($growth + 32GB)) {
            throw 'Host cannot cover future growth plus a 32 GiB reserve'
        }
        $tasks = @($taskNames | ForEach-Object {
            $task = Get-ScheduledTask -TaskName $_ -TaskPath '\'
            [pscustomobject]@{ name = $_; enabled = [bool]$task.Settings.Enabled;
                running = ([string]$task.State -eq 'Running') }
        })
        $receipt.tasks = $tasks
        $receipt['root_uuid'] = (Invoke-WslChecked @('--distribution', $DistroName, '--user', 'root',
            '--exec', '/usr/bin/findmnt', '-no', 'UUID', '/')).Trim()
        if ($receipt.root_uuid -notmatch '^[a-fA-F0-9-]{36}$') { throw 'Cannot pin ext4 root UUID' }
        foreach ($task in $tasks) {
            Export-ScheduledTask -TaskName $task.name -TaskPath '\' |
                Set-Content -LiteralPath (Join-Path $runRoot ($task.name + '.xml')) -Encoding UTF8
        }
        # Set restoration intent BEFORE the first task mutation.
        $restoreRequired = $true
        Add-Event 'task-backups-saved-restoration-required'
        foreach ($task in $tasks) {
            Disable-ScheduledTask -TaskName $task.name -TaskPath '\' | Out-Null
            if ($task.running) { Stop-ScheduledTask -TaskName $task.name -TaskPath '\' }
        }
        if ($DistroName -in $running) {
            Add-Event 'sync-and-trim-before-shutdown'
            $null = Invoke-WslChecked @('--distribution', $DistroName, '--user', 'root', '--exec',
                '/bin/bash', '-c', 'sync && fstrim -v /')
            Add-Event 'graceful-systemd-poweroff-requested'
            $shutdownRequested = $true
            $null = Invoke-WslChecked @('--distribution', $DistroName, '--user', 'root',
                '--exec', '/bin/systemctl', 'poweroff')
            $deadline = (Get-Date).AddSeconds(180)
            while ($DistroName -in @(Get-RunningDistributions)) {
                if ((Get-Date) -gt $deadline) { throw 'Graceful poweroff timed out; no forced VM termination' }
                Start-Sleep -Seconds 2
            }
        }
        $null = Invoke-WslChecked @('--shutdown')
        if ((Get-VHD -Path $vhdPath).Attached) { throw 'VHD is still attached after shutdown' }
        Add-Event 'offline-resize-started'
        $null = Invoke-WslChecked @('--manage', $DistroName, '--resize', ($TargetSizeBytes.ToString() + 'B'))
        if ((Get-VHD -Path $vhdPath).Size -ne $TargetSizeBytes) { throw 'VHD capacity readback mismatch' }
        Add-Event 'offline-resize-verified'
        # --manage uses a utility VM. Shut it down before offline compaction.
        $null = Invoke-WslChecked @('--shutdown')
        if (-not (Compact-StoppedVhd)) { throw 'VHD unexpectedly restarted before compaction' }
        $receipt['after'] = Get-VhdObservation
    }
} catch {
    $receipt.errors += $_.Exception.Message
    $exitCode = 1
} finally {
    if ($restoreRequired) {
        foreach ($task in $tasks) {
            try {
                if ($task.enabled) {
                    Enable-ScheduledTask -TaskName $task.name -TaskPath '\' | Out-Null
                    if ($task.running) { Start-ScheduledTask -TaskName $task.name -TaskPath '\' }
                } else {
                    Disable-ScheduledTask -TaskName $task.name -TaskPath '\' | Out-Null
                }
            } catch {
                $receipt.errors += "Task restoration $($task.name): $($_.Exception.Message)"
                $exitCode = 1
            }
        }
        if ($shutdownRequested) {
            try {
                Add-Event 'boot-readback-started'
                $capacityProbe = 'set -e; test "$(findmnt -no UUID /)" = "__UUID__"; test "$(blockdev --getsize64 "$(findmnt -no SOURCE /)")" -eq __TARGET__; findmnt -no SOURCE,UUID,OPTIONS /; df -B1 /; cat /proc/sys/kernel/random/boot_id; '
                $capacityProbe = $capacityProbe.Replace('__UUID__', $receipt.root_uuid).Replace('__TARGET__', $TargetSizeBytes.ToString())
                $probe = $capacityProbe + "cd $RepoRoot && timeout 180 bash scripts/run_boot_recovery_audit.sh"
                $receipt.recovery = Invoke-WslChecked @('--distribution', $DistroName, '--user', 'root',
                    '--exec', '/bin/bash', '-lc', $probe)
                Add-Event 'canonical-boot-audit-passed'
            } catch {
                $receipt.errors += "Boot readback: $($_.Exception.Message)"
                $exitCode = 1
            }
        }
        $receipt['after'] = Get-VhdObservation
        if ($exitCode) { Save-Receipt 'needs-attention' } else { Save-Receipt 'expanded-and-boot-verified' }
    } elseif ($exitCode) {
        Save-Receipt 'failed'
    }
    if ($locked) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
Write-Output $receiptPath
exit $exitCode
