# Run from a Windows-native copy: shutting WSL stops scripts on its UNC share.
[CmdletBinding()]
param(
    [ValidateSet('Plan', 'Expand', 'Compact', 'CompactIfStopped')]
    [string]$Mode = 'Plan',
    [Parameter(Mandatory = $true)][string]$DistroName,
    [Parameter(Mandatory = $true)][string]$ExpectedVhdPath,
    [long]$TargetSizeBytes = 2499999170560,
    [string[]]$CompanionDistroNames = @(),
    [string]$DataDistroName = '',
    [string]$ExpectedDataVhdPath = '',
    [string]$ExpectedDataVolumeId = '',
    [string]$ExpectedDataFilesystemUuid = '',
    [long]$DataTargetSizeBytes = 3298534883328,
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
if ($TargetSizeBytes -le 0 -or $TargetSizeBytes -gt 3298534883328 -or
    $TargetSizeBytes % 1MB -ne 0) {
    throw 'Target must be positive, MiB-aligned and no greater than 3 TiB'
}
if (@($CompanionDistroNames | Where-Object {
    $_ -notmatch '^[A-Za-z0-9_.-]+$' -or $_ -eq $DistroName
}).Count -or @($CompanionDistroNames | Select-Object -Unique).Count -ne $CompanionDistroNames.Count) {
    throw 'Companion distributions must be distinct explicitly named safe identifiers'
}
$resizeData = [bool]($DataDistroName -or $ExpectedDataVhdPath -or
    $ExpectedDataVolumeId -or $ExpectedDataFilesystemUuid -or
    $PSBoundParameters.ContainsKey('DataTargetSizeBytes'))
if ($resizeData -and ($Mode -notin @('Plan', 'Compact') -or
    $DataDistroName -notmatch '^[A-Za-z0-9_.-]+$' -or $DataDistroName -eq $DistroName -or
    $DataDistroName -in $CompanionDistroNames -or
    $ExpectedDataVhdPath -notmatch '^E:\\' -or
    $ExpectedDataVolumeId -notmatch '^[a-fA-F0-9-]{36}$' -or
    $ExpectedDataFilesystemUuid -notmatch '^[a-fA-F0-9-]{36}$' -or
    $DataTargetSizeBytes -ne 3298534883328)) {
    throw 'Data expansion requires Compact/Plan, distinct enrolled E identity and exactly 3 TiB'
}

function Get-FullAllocationGrowth([long]$Target, [long]$Allocated) {
    if ($Target -le 0 -or $Allocated -lt 0) { throw 'Invalid physical allocation estimate' }
    # Existing virtual free blocks can still need host allocation. Reserve for
    # the WHOLE target, not just the difference between two virtual ceilings.
    return [Math]::Max(0L, $Target - $Allocated)
}

function Get-FileAllocationBytes([string]$Path) {
    if (-not ('StockAgentMaintenanceFileAllocation' -as [type])) {
        Add-Type -TypeDefinition @'
using System;using System.Runtime.InteropServices;
public static class StockAgentMaintenanceFileAllocation {
 [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]
 public static extern uint GetCompressedFileSizeW(string path,out uint high);
}
'@
    }
    [uint32]$high = 0
    $low = [StockAgentMaintenanceFileAllocation]::GetCompressedFileSizeW($Path, [ref]$high)
    $errorCode = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
    if ($low -eq [uint32]::MaxValue -and $errorCode -ne 0) {
        throw "Cannot measure VHD physical allocation: $errorCode"
    }
    return [long](([uint64]$high * 4294967296) + [uint64]$low)
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
        allocated_bytes = Get-FileAllocationBytes $vhdPath;
        attached = [bool]$disk.Attached;
        host_free_bytes = [long]([IO.DriveInfo]::new([IO.Path]::GetPathRoot($vhdPath))).AvailableFreeSpace }
}

function Get-DataVhdObservation {
    $entries = @(Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss' |
        Get-ItemProperty | Where-Object DistributionName -eq $DataDistroName)
    if ($entries.Count -ne 1 -or $entries[0].Version -ne 2) {
        throw 'Exactly one enrolled WSL2 data distribution is required'
    }
    $name = 'ext4.vhdx'
    if ($entries[0].PSObject.Properties['VhdFileName']) { $name = [string]$entries[0].VhdFileName }
    $path = [IO.Path]::GetFullPath((Join-Path $entries[0].BasePath $name))
    if (-not $path.Equals([IO.Path]::GetFullPath($ExpectedDataVhdPath),
        [StringComparison]::OrdinalIgnoreCase) -or $path -notmatch '^E:\\') {
        throw 'Data VHD does not match its enrolled native E path'
    }
    $item = Get-Item -LiteralPath $path
    if ($item.Extension -ne '.vhdx' -or
        ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw 'Refusing redirected data VHD'
    }
    $parent = $item.Directory
    while ($parent) {
        if (($parent.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'Refusing redirected data VHD ancestor'
        }
        $parent = $parent.Parent
    }
    $volume = Get-Volume -DriveLetter E
    if ([string]$volume.FileSystemType -ne 'NTFS' -or
        ([string]$volume.UniqueId).IndexOf($ExpectedDataVolumeId,
            [StringComparison]::OrdinalIgnoreCase) -lt 0) {
        throw 'Data VHD physical E volume identity changed'
    }
    $disk = Get-VHD -Path $path
    if ([string]$disk.VhdType -ne 'Dynamic' -or $disk.ParentPath) {
        throw 'Data VHD must be independent and dynamic'
    }
    return [ordered]@{ path = $path; virtual_bytes = [long]$disk.Size;
        file_bytes = [long]$disk.FileSize; allocated_bytes = Get-FileAllocationBytes $path;
        host_free_bytes = [long]([IO.DriveInfo]::new('E:\')).AvailableFreeSpace;
        attached = [bool]$disk.Attached }
}
$receipt = [ordered]@{
    contract = 'stockagent-wsl-storage-maintenance-v1'; mode = $Mode; state = 'planned';
    distro = $DistroName; registration = $registration[0].PSChildName; vhd_path = $vhdPath;
    target_bytes = $TargetSizeBytes; before = Get-VhdObservation;
    running_distributions = @(Get-RunningDistributions); tasks = $tasks;
    companion_distributions = @($CompanionDistroNames);
    sparse_enabled_by_worker = $false; instantaneous_reclaim = $false;
    script_sha256 = (Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash;
    errors = @(); events = @(); recovery = $null
}
$plannedGrowth = if ($Mode -in @('Compact', 'CompactIfStopped')) { 0L } else {
    Get-FullAllocationGrowth $TargetSizeBytes $receipt.before.allocated_bytes
}
$maintenanceReserve = if ($Mode -eq 'Compact') { 1GB } else { 32GB }
$receipt['capacity_plan'] = [ordered]@{
    future_growth_bytes=$plannedGrowth; reserve_bytes=$maintenanceReserve;
    required_host_free_bytes=($plannedGrowth + $maintenanceReserve);
    host_free_bytes=$receipt.before.host_free_bytes;
    full_growth_covered_before_compaction=($receipt.before.host_free_bytes -ge ($plannedGrowth + $maintenanceReserve));
    final_admission='remeasure_after_offline_read_only_compaction';
    virtual_limit_is_not_host_physical_space=$true }
if ($resizeData) {
    $dataBefore = Get-DataVhdObservation
    if ($DataTargetSizeBytes -lt $dataBefore.virtual_bytes) { throw 'Refusing data virtual-disk shrink' }
    $dataGrowth = Get-FullAllocationGrowth $DataTargetSizeBytes $dataBefore.allocated_bytes
    $receipt['data_expansion'] = [ordered]@{ distribution = $DataDistroName;
        expected_filesystem_uuid = $ExpectedDataFilesystemUuid; before = $dataBefore;
        target_bytes = $DataTargetSizeBytes; reserve_bytes = 64GB;
        full_future_allocation_bytes = $dataGrowth;
        admitted_before_shutdown = ($dataBefore.host_free_bytes -ge ($dataGrowth + 64GB));
        state = 'planned'; after = $null; readback = $null }
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
    if ($resizeData) {
        # One original-owner pointer avoids re-statting months of hourly
        # receipts on every minute's read-only Linux observation.
        $latest = Join-Path $ReceiptRoot 'latest-c-e-maintenance.json'
        if ((Test-Path -LiteralPath $latest) -and
            ((Get-Item -LiteralPath $latest).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'Refusing redirected maintenance receipt pointer'
        }
        $latestTemporary = $latest + '.tmp'
        if ((Test-Path -LiteralPath $latestTemporary) -and
            ((Get-Item -LiteralPath $latestTemporary).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'Refusing redirected maintenance temporary pointer'
        }
        [IO.File]::WriteAllText($latestTemporary, ($receipt | ConvertTo-Json -Depth 16), [Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $latestTemporary -Destination $latest -Force
    }
}
function Add-Event([string]$Name) {
    $receipt.events += [ordered]@{ at_utc = [DateTime]::UtcNow.ToString('o'); name = $Name }
    Save-Receipt 'running'
}
function Invoke-WslChecked([string[]]$WslArguments) {
    if ($WslArguments.Count -and $WslArguments[0] -eq '--distribution') {
        # A companion need not contain this checkout's UNC working directory.
        # Pin Linux cwd explicitly instead of letting WSL translate that path.
        $WslArguments = @('--cd', '/') + $WslArguments
    }
    # Windows PowerShell 5 wraps native stderr as ErrorRecord. Tools such as
    # e2fsck may use it even on exit 0: preserve it, then check the actual exit.
    $previousErrorPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = (& $wsl @WslArguments 2>&1 | Out-String)
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorPreference
    }
    [IO.File]::AppendAllText((Join-Path $runRoot 'wsl.log'), $output)
    if ($code -ne 0) { throw "wsl.exe exited $code; see wsl.log" }
    return $output
}

function Test-ShutdownJournalEvent([string]$Path, [string]$Token, [string]$Event) {
    # A new, per-run journal stream plus a unique logger record prevents an
    # old poweroff entry from authorizing shutdown. Only PID 1 can establish
    # the actual systemd target, and the readiness record must precede it.
    if ($Event -notin @('ready', 'poweroff')) { throw 'Unknown shutdown journal event' }
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    $item = Get-Item -LiteralPath $Path
    if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or $item.Length -gt 16MB) {
        throw 'Shutdown journal observation is redirected or exceeded its bound'
    }
    $stream = [IO.File]::Open($Path, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
    $reader = [IO.StreamReader]::new($stream, [Text.Encoding]::UTF8, $true)
    try {
        $ready = $false
        while ($null -ne ($line = $reader.ReadLine())) {
            try { $record = $line | ConvertFrom-Json -ErrorAction Stop } catch { continue }
            if (-not $record -or -not $record.PSObject.Properties['MESSAGE']) { continue }
            $message = [string]$record.MESSAGE
            if ($record.PSObject.Properties['SYSLOG_IDENTIFIER'] -and
                [string]$record.SYSLOG_IDENTIFIER -eq 'stockagent-wsl-maint-watch' -and
                $message -ceq $Token) {
                $ready = $true
                if ($Event -eq 'ready') { return $true }
                continue
            }
            if ($Event -eq 'poweroff' -and $ready -and $record.PSObject.Properties['_PID'] -and
                [string]$record._PID -eq '1' -and
                $message -match '^Reached target poweroff\.target(?:\s|$)') { return $true }
        }
    } finally {
        $reader.Dispose()
        $stream.Dispose()
    }
    return $false
}

function Stop-ShutdownJournalWatch($Watch) {
    if (-not $Watch) { return }
    # Only the read-only observer created by this invocation is stopped.
    $Watch.process.Refresh()
    if (-not $Watch.process.HasExited) {
        $Watch.process.Kill()
        $null = $Watch.process.WaitForExit(5000)
    }
    $Watch.process.Dispose()
}

function Start-ShutdownJournalWatch([string]$Distribution) {
    $token = 'stockagent-wsl-watch-' + [guid]::NewGuid().ToString('N')
    $outputPath = Join-Path $runRoot ($token + '.jsonl')
    $errorPath = Join-Path $runRoot ($token + '.stderr')
    $arguments = @('--cd', '/', '--distribution', $Distribution, '--user', 'root', '--exec',
        '/usr/bin/stdbuf', '-oL', '/usr/bin/journalctl', '--follow', '--boot',
        '--lines=0', '--no-pager', '--output=json', '_PID=1', '+',
        'SYSLOG_IDENTIFIER=stockagent-wsl-maint-watch')
    $process = Start-Process -FilePath $wsl -ArgumentList $arguments -PassThru -NoNewWindow `
        -RedirectStandardOutput $outputPath -RedirectStandardError $errorPath
    $watch = [pscustomobject]@{ process=$process; path=$outputPath; token=$token; distribution=$Distribution }
    try {
        $deadline = (Get-Date).AddSeconds(15)
        while ((Get-Date) -le $deadline) {
            $process.Refresh()
            if ($process.HasExited) { throw 'Shutdown journal observer exited before readiness' }
            $null = Invoke-WslChecked @('--distribution', $Distribution, '--user', 'root',
                '--exec', '/usr/bin/logger', '-t', 'stockagent-wsl-maint-watch', $token)
            Start-Sleep -Milliseconds 250
            if (Test-ShutdownJournalEvent $outputPath $token 'ready') { return $watch }
        }
        throw 'Shutdown journal observer was not independently read back'
    } catch {
        Stop-ShutdownJournalWatch $watch
        throw
    }
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
$companionShutdownRequested = @()
$shutdownWatches = @{}
$observedPoweroff = @{}
$runtimePlanName = 'wsl-' + [IO.Path]::GetFileName($runRoot) + '.json'
$runtimePlanIdentity = $null
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
        if (@($running | Where-Object { $_ -ne $DistroName -and $_ -notin $CompanionDistroNames -and
            (-not $resizeData -or $_ -ne $DataDistroName) }).Count) {
            throw 'Refusing global WSL shutdown while another distribution is running'
        }
        if ($DistroName -notin $running) {
            throw 'Maintenance requires a running distro for a fresh filesystem identity baseline'
        }
        if ($resizeData -and $DataDistroName -in $running) {
            throw 'The data distro was already running; its original user processes need an explicit drain'
        }
        if ($Mode -eq 'Expand' -and (Get-VHD -Path $vhdPath).Size -gt $TargetSizeBytes) { throw 'Refusing virtual-disk shrink' }
        if ($Mode -eq 'Expand' -and (Get-VhdObservation).host_free_bytes -lt 32GB) {
            throw 'Host lacks the unchanged 32 GiB physical reserve before maintenance'
        }
        if ($Mode -eq 'Compact' -and (Get-VhdObservation).host_free_bytes -lt 1GB) {
            throw 'Host lacks 1 GiB for bounded maintenance metadata; application reserves are unchanged'
        }
        if ($resizeData) {
            $dataCheck = Get-DataVhdObservation
            $dataGrowth = Get-FullAllocationGrowth $DataTargetSizeBytes $dataCheck.allocated_bytes
            if ($dataCheck.host_free_bytes -lt ($dataGrowth + 64GB)) {
                throw 'E cannot cover the complete target allocation plus 64 GiB reserve'
            }
            $dataUuid = (Invoke-WslChecked @('--distribution', $DataDistroName, '--user', 'root',
                '--exec', '/usr/bin/findmnt', '-no', 'UUID', '-T', '/workspace')).Trim()
            if ($dataUuid -ne $ExpectedDataFilesystemUuid) { throw 'Data ext4 UUID changed before shutdown' }
            $null = Invoke-WslChecked @('--distribution', $DataDistroName, '--user', 'root',
                '--exec', '/usr/bin/sync')
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
        # Establish both observers while every distro is healthy. They are
        # read-only and see a unique record before any service is stopped.
        $shutdownWatches[$DistroName] = Start-ShutdownJournalWatch $DistroName
        foreach ($companion in @($running | Where-Object { $_ -in $CompanionDistroNames })) {
            $shutdownWatches[$companion] = Start-ShutdownJournalWatch $companion
        }
        $receipt['shutdown_observer_ready'] = @($shutdownWatches.Keys)
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
        # Only explicitly named companions may stop. All running unknown
        # distributions still block maintenance before any task mutation.
        foreach ($companion in @($running | Where-Object { $_ -in $CompanionDistroNames })) {
            $companionShutdownRequested += $companion
            Add-Event ('graceful-companion-poweroff-' + $companion)
            $null = Invoke-WslChecked @('--distribution', $companion, '--user', 'root',
                '--exec', '/bin/systemctl', 'poweroff')
            $deadline = (Get-Date).AddSeconds(180)
            while ($companion -in @(Get-RunningDistributions)) {
                if (Test-ShutdownJournalEvent $shutdownWatches[$companion].path `
                        $shutdownWatches[$companion].token 'poweroff') {
                    $observedPoweroff[$companion] = $true
                    Add-Event ('companion-systemd-poweroff-observed-' + $companion)
                    break
                }
                if ((Get-Date) -gt $deadline) { throw 'Graceful companion poweroff timed out; no forced VM termination' }
                Start-Sleep -Seconds 2
            }
        }
        if ($DistroName -in $running) {
            Add-Event 'sync-and-trim-before-shutdown'
            $null = Invoke-WslChecked @('--distribution', $DistroName, '--user', 'root', '--exec',
                '/bin/bash', '-c', 'sync && fstrim -v /')
            # Capture only known finite agent owners immediately before the
            # shutdown request. Unknown work refuses shutdown rather than
            # being silently abandoned on the other side of the reboot.
            $capture = "cd $RepoRoot && bash scripts/run_authority_storage_operation.sh scripts/wsl_maintenance_resume.py capture --name $runtimePlanName"
            $plan = (Invoke-WslChecked @('--distribution', $DistroName, '--user', 'root',
                '--exec', '/bin/bash', '-c', $capture) | ConvertFrom-Json)
            if ($plan.state -ne 'captured' -or $plan.identity_sha256 -notmatch '^[0-9a-f]{64}$') {
                throw 'Finite owner recovery plan was not independently captured'
            }
            $runtimePlanIdentity = [string]$plan.identity_sha256
            $receipt['runtime_resume_plan'] = $plan
            Add-Event 'finite-owner-resume-plan-captured'
            Add-Event 'graceful-systemd-poweroff-requested'
            $shutdownRequested = $true
            $null = Invoke-WslChecked @('--distribution', $DistroName, '--user', 'root',
                '--exec', '/bin/systemctl', 'poweroff')
            $deadline = (Get-Date).AddSeconds(180)
            while ($DistroName -in @(Get-RunningDistributions)) {
                if (Test-ShutdownJournalEvent $shutdownWatches[$DistroName].path `
                        $shutdownWatches[$DistroName].token 'poweroff') {
                    $observedPoweroff[$DistroName] = $true
                    Add-Event 'systemd-poweroff-target-observed'
                    break
                }
                if ((Get-Date) -gt $deadline) { throw 'Graceful poweroff timed out; no forced VM termination' }
                Start-Sleep -Seconds 2
            }
        }
        # systemd can finish powering off while WSL still lists the distro as
        # running because a native client remains. Only a fresh PID-1 target
        # observation permits the user's explicitly requested VM shutdown.
        $beforeVmShutdown = @(Get-RunningDistributions)
        if (@($beforeVmShutdown | Where-Object { $_ -ne $DistroName -and $_ -notin $CompanionDistroNames -and
            (-not $resizeData -or $_ -ne $DataDistroName) }).Count) {
            throw 'Another distribution started before VM shutdown; preserve it'
        }
        foreach ($name in @($beforeVmShutdown | Where-Object { $_ -eq $DistroName -or $_ -in $CompanionDistroNames })) {
            if (-not $shutdownWatches.ContainsKey($name)) {
                throw 'A running distribution has no prepared shutdown observer'
            }
            if (-not $observedPoweroff[$name] -and -not (Test-ShutdownJournalEvent `
                    $shutdownWatches[$name].path $shutdownWatches[$name].token 'poweroff')) {
                throw 'A still-running distribution has no fresh systemd poweroff evidence'
            }
        }
        Add-Event 'all-running-systemd-distros-powered-off'
        $null = Invoke-WslChecked @('--shutdown')
        if ((Get-VHD -Path $vhdPath).Attached) { throw 'VHD is still attached after shutdown' }
        if ($resizeData) {
            $dataCheck = Get-DataVhdObservation
            $dataGrowth = Get-FullAllocationGrowth $DataTargetSizeBytes $dataCheck.allocated_bytes
            if ($dataCheck.attached -or $dataCheck.virtual_bytes -gt $DataTargetSizeBytes -or
                $dataCheck.host_free_bytes -lt ($dataGrowth + 64GB)) {
                throw 'Offline E identity, detached state or complete allocation admission changed'
            }
            $receipt.data_expansion.state = 'running'
            Add-Event 'offline-data-expansion-admitted'
            if ($dataCheck.virtual_bytes -lt $DataTargetSizeBytes) {
                try {
                    $null = Invoke-WslChecked @('--manage', $DataDistroName, '--resize',
                        ($DataTargetSizeBytes.ToString() + 'B'))
                } finally {
                    # --manage uses a utility VM. Keep both volumes detached
                    # before compaction; no filesystem is truncated here.
                    $null = Invoke-WslChecked @('--shutdown')
                }
            }
            $dataAfter = Get-DataVhdObservation
            if ($dataAfter.attached -or $dataAfter.virtual_bytes -ne $DataTargetSizeBytes) {
                throw 'Data VHD target readback mismatch'
            }
            $receipt.data_expansion.after = $dataAfter
            $receipt.data_expansion.state = 'virtual_target_verified_guest_readback_pending'
            Add-Event 'offline-data-virtual-target-verified'
        }
        # Retired ext4 blocks are not proof of reclaimed NTFS allocation.
        # Compact via the existing read-only owner, then measure admission
        # for ALL future growth plus the unchanged 32 GiB physical reserve.
        if (-not (Compact-StoppedVhd)) { throw 'VHD unexpectedly restarted before capacity admission' }
        if ($Mode -eq 'Compact') {
            $receipt['after'] = Get-VhdObservation
            if ($receipt.after.virtual_bytes -ne $receipt.before.virtual_bytes) {
                throw 'Compaction unexpectedly changed virtual capacity'
            }
            # Preserve the actual offline result before post-boot writes can
            # grow the VHD again or an unrelated service readiness gate fails.
            $receipt['compaction_readback'] = $receipt.after
            Add-Event 'offline-compaction-verified-without-resize'
        } else {
            $capacity = Get-VhdObservation
            $growth = Get-FullAllocationGrowth $TargetSizeBytes $capacity.allocated_bytes
            $receipt['resize_admission'] = [ordered]@{
                growth_bytes=$growth; reserve_bytes=32GB;
                required_host_free_bytes=($growth + 32GB); host_free_bytes=$capacity.host_free_bytes;
                admitted=($capacity.host_free_bytes -ge ($growth + 32GB)) }
            Save-Receipt 'offline-capacity-observed'
            if (-not $receipt.resize_admission.admitted) {
                throw 'Host cannot cover future growth plus a 32 GiB reserve after compaction'
            }
            Add-Event 'offline-resize-started'
            $null = Invoke-WslChecked @('--manage', $DistroName, '--resize', ($TargetSizeBytes.ToString() + 'B'))
            if ((Get-VHD -Path $vhdPath).Size -ne $TargetSizeBytes) { throw 'VHD capacity readback mismatch' }
            Add-Event 'offline-resize-verified'
            # --manage uses a utility VM. Shut it down before offline compaction.
            $null = Invoke-WslChecked @('--shutdown')
            if (-not (Compact-StoppedVhd)) { throw 'VHD unexpectedly restarted before compaction' }
            $receipt['after'] = Get-VhdObservation
        }
    }
} catch {
    $receipt.errors += $_.Exception.Message
    $exitCode = 1
} finally {
    foreach ($watch in @($shutdownWatches.Values)) {
        try { Stop-ShutdownJournalWatch $watch } catch {
            $receipt.errors += "Shutdown observer cleanup: $($_.Exception.Message)"
            $exitCode = 1
        }
    }
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
                # Admission can reject after shutdown. Recover that unchanged
                # disk too; do not compare its old size with the new target.
                $expectedBootBytes = [long](Get-VHD -Path $vhdPath).Size
                $capacityProbe = 'set -e; test "$(findmnt -no UUID /)" = "__UUID__"; test "$(blockdev --getsize64 "$(findmnt -no SOURCE /)")" -eq __TARGET__; fs_bytes=$(LC_ALL=C tune2fs -l "$(findmnt -no SOURCE /)" | awk -F: ''/^Block count:/ {n=$2} /^Block size:/ {s=$2} END {printf "%.0f", n*s}''); test "$fs_bytes" -eq __TARGET__; findmnt -no SOURCE,UUID,OPTIONS /; df -B1 /; cat /proc/sys/kernel/random/boot_id; '
                $capacityProbe = $capacityProbe.Replace('__UUID__', $receipt.root_uuid).Replace('__TARGET__', $expectedBootBytes.ToString())
                $probe = $capacityProbe + "cd $RepoRoot && timeout 180 bash scripts/run_boot_recovery_audit.sh"
                try {
                    $receipt.recovery = Invoke-WslChecked @('--distribution', $DistroName, '--user', 'root',
                        '--exec', '/bin/bash', '-lc', $probe)
                    Add-Event 'canonical-boot-audit-passed'
                } catch {
                    # A separate source/service readiness failure must remain
                    # a failure, while E's exact filesystem readback still runs.
                    $receipt.errors += "Boot audit: $($_.Exception.Message)"
                    $exitCode = 1
                }
                if ($resizeData) {
                    $dataProbe = 'set -e; target=/srv/stockagent-workspace; test "$(findmnt -no UUID -T "$target")" = "__UUID__"; dev=$(findmnt -no SOURCE -T "$target"); dev=${dev%%\[*}; test "$(blockdev --getsize64 "$dev")" -eq __TARGET__; fs_bytes=$(LC_ALL=C tune2fs -l "$dev" | awk -F: ''/^Block count:/ {n=$2} /^Block size:/ {s=$2} END {printf "%.0f", n*s}''); test "$fs_bytes" -eq __TARGET__; df -B1 "$target"'
                    $dataProbe = $dataProbe.Replace('__UUID__', $ExpectedDataFilesystemUuid).Replace('__TARGET__', $DataTargetSizeBytes.ToString())
                    $receipt.data_expansion.readback = Invoke-WslChecked @('--distribution', $DistroName,
                        '--user', 'root', '--exec', '/bin/bash', '-c', $dataProbe)
                    $receipt.data_expansion.state = 'expanded_and_guest_filesystem_verified'
                    $receipt.data_expansion['observed_at_utc'] = [DateTime]::UtcNow.ToString('o')
                    Add-Event 'data-native-filesystem-readback-passed'
                }
            } catch {
                $receipt.errors += "Boot readback: $($_.Exception.Message)"
                $exitCode = 1
            }
            if ($runtimePlanIdentity) {
                try {
                    $resume = "cd $RepoRoot && bash scripts/run_authority_storage_operation.sh scripts/wsl_maintenance_resume.py restore --name $runtimePlanName --identity $runtimePlanIdentity"
                    for ($attempt = 1; $attempt -le 6; $attempt++) {
                        try {
                            $receipt['runtime_resume'] = (Invoke-WslChecked @('--distribution', $DistroName,
                                '--user', 'root', '--exec', '/bin/bash', '-c', $resume) | ConvertFrom-Json)
                            break
                        } catch {
                            if ($attempt -eq 6 -or $_.Exception.Message -notmatch 'wsl.exe exited 75') { throw }
                            Start-Sleep -Seconds 10
                        }
                    }
                    Add-Event 'finite-owner-resume-readback-completed'
                } catch {
                    $receipt.errors += "Finite owner recovery: $($_.Exception.Message)"
                    $exitCode = 1
                }
            }
        }
        foreach ($companion in $companionShutdownRequested) {
            try {
                # Bound boot observation even if a companion service fails.
                $companionBoot = Invoke-WslChecked @('--distribution', $companion, '--user', 'root',
                    '--exec', '/usr/bin/timeout', '180', '/bin/systemctl', 'is-system-running', '--wait')
                Add-Event ('companion-boot-verified-' + $companion)
            } catch {
                $receipt.errors += "Companion restoration $($companion): $($_.Exception.Message)"
                $exitCode = 1
            }
        }
        $receipt['after'] = Get-VhdObservation
        $receipt['backing_capacity'] = [ordered]@{
            measured_allocation_bytes = $receipt.after.allocated_bytes;
            host_free_bytes = $receipt.after.host_free_bytes;
            reserve_bytes = 32GB;
            ownership_ceiling_bytes = [Math]::Max(0L,
                ($receipt.after.allocated_bytes + $receipt.after.host_free_bytes - 32GB));
            virtual_limit_unchanged = ($receipt.after.virtual_bytes -eq $receipt.before.virtual_bytes);
            filesystem_shrunk = $false }
        if ($exitCode) {
            Save-Receipt 'needs-attention'
        } elseif ($Mode -eq 'Compact') {
            Save-Receipt 'compacted-and-boot-verified'
        } else {
            Save-Receipt 'expanded-and-boot-verified'
        }
    } elseif ($exitCode) {
        Save-Receipt 'failed'
    }
    if ($locked) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
Write-Output $receiptPath
exit $exitCode
