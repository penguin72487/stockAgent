param(
    [string]$PublicOrigin = "https://penguin72487.ddnsgeek.com",
    [string]$TaskName = "StockAgent Public Caddy",
    [string]$ReportRoot = "$env:LOCALAPPDATA\StockAgentPublic\logs",
    [string]$DistroName = "",
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
$wsl = Join-Path $env:WINDIR "System32\wsl.exe"
if (-not $DistroName) {
    $lxssRoot = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss"
    $defaultDistributionId = [string](Get-ItemProperty $lxssRoot).DefaultDistribution
    if (-not $defaultDistributionId) {
        throw "Cannot resolve the default WSL distribution ID."
    }
    $distributionKey = Join-Path $lxssRoot $defaultDistributionId
    $DistroName = [string](Get-ItemProperty $distributionKey).DistributionName
    if (-not $DistroName) {
        throw "Cannot resolve the default WSL distribution name."
    }
}
$coreServices = @(
    "chrony.service",
    "syncthing@root.service",
    "stockagent-discord-bot.service",
    "stockagent-hot-artifact-sync.service",
    "stockagent-public-dashboards.service",
    "stockagent-shioaji-taifex-bidask.service",
    "stockagent-shioaji-taifex-dashboard.service",
    "stockagent-shioaji-top200.service",
    "stockagent-tw-day-trade-simulation.service",
    "stockagent-tw-public-source-events.service"
)
$publicPaths = @(
    "/",
    "/healthz",
    "/taifex/",
    "/taifex/api/status",
    "/tw-day-trade/",
    "/tw-day-trade/api/status",
    "/shioaji/",
    "/shioaji/api/status",
    "/openbb/",
    "/openbb/api/status",
    "/data-monitor/",
    "/data-monitor/api/status",
    "/traffic/",
    "/traffic/api/status"
)

function Get-WslBootId {
    try {
        return ((& $wsl --distribution $DistroName --exec /bin/cat /proc/sys/kernel/random/boot_id 2>$null) |
            Out-String).Trim()
    } catch {
        return ""
    }
}

function Get-RunningWslDistributions {
    try {
        $output = ((& $wsl --list --running --quiet 2>$null) | Out-String)
        $output = $output -replace "`0", ""
        return @($output -split "`r?`n" |
            ForEach-Object { $_.Trim() } | Where-Object { $_ })
    } catch {
        return @()
    }
}

function Get-PublicProbe([string]$Path) {
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 15 -Uri ($PublicOrigin + $Path)
        $watch.Stop()
        return [ordered]@{
            path = $Path
            status_code = [int]$response.StatusCode
            elapsed_ms = [math]::Round($watch.Elapsed.TotalMilliseconds, 1)
            error = $null
        }
    } catch {
        $watch.Stop()
        $statusCode = $null
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        return [ordered]@{
            path = $Path
            status_code = $statusCode
            elapsed_ms = [math]::Round($watch.Elapsed.TotalMilliseconds, 1)
            error = $_.Exception.Message
        }
    }
}

function Get-WslLine([string]$Command) {
    return ((& $wsl --distribution $DistroName --exec /bin/bash -lc $Command 2>$null) |
        Out-String).Trim()
}

New-Item -ItemType Directory -Force -Path $ReportRoot | Out-Null
$stamp = Get-Date -Format "yyyyMMddTHHmmss"
$reportPath = Join-Path $ReportRoot "cold-boot-$stamp.json"
$startedAt = Get-Date
$preBootId = Get-WslBootId
$installRoot = Split-Path -Parent $ReportRoot
$caddyConfig = Join-Path $installRoot "Caddyfile"
$escapedCaddyConfig = [Regex]::Escape($caddyConfig)

function Get-CaddyProcesses {
    return @(Get-CimInstance Win32_Process -Filter "Name = 'caddy.exe'" |
        Where-Object { $_.CommandLine -match $escapedCaddyConfig })
}

# Exercise the Windows reverse proxy's own recovery path before removing WSL.
$oldCaddyPids = @(Get-CaddyProcesses | ForEach-Object { [int]$_.ProcessId })
Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
Get-CaddyProcesses | ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}
$caddyStoppedProbe = Get-PublicProbe "/healthz"
Start-ScheduledTask -TaskName $TaskName
$caddyReadyAt = $null
$newCaddyPids = @()
$caddyDeadline = (Get-Date).AddSeconds($TimeoutSeconds)
do {
    $probe = Get-PublicProbe "/healthz"
    $newCaddyPids = @(Get-CaddyProcesses | ForEach-Object { [int]$_.ProcessId })
    if ($probe.status_code -eq 200 -and $newCaddyPids.Count -gt 0 -and
        @($newCaddyPids | Where-Object { $_ -in $oldCaddyPids }).Count -eq 0) {
        $caddyReadyAt = Get-Date
        break
    }
    Start-Sleep -Milliseconds 250
} while ((Get-Date) -lt $caddyDeadline)

$shutdownAt = Get-Date
& $wsl --shutdown
$stoppedAt = $null
$runningDistributionsAtStop = @()
$stopDeadline = (Get-Date).AddSeconds($TimeoutSeconds)
do {
    $runningDistributions = @(Get-RunningWslDistributions)
    if ($DistroName -notin $runningDistributions) {
        $runningDistributionsAtStop = $runningDistributions
        $stoppedAt = Get-Date
        break
    }
    Start-Sleep -Milliseconds 250
} while ((Get-Date) -lt $stopDeadline)

$postBootId = ""
$serviceStates = @()
$timerStates = @()
$failedUnits = ""
$systemState = ""
$publicProbes = @()
$readyAt = $null
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)

do {
    if (-not $stoppedAt) {
        break
    }
    $postBootId = Get-WslBootId
    if (-not $postBootId -or $postBootId -eq $preBootId) {
        Start-Sleep -Milliseconds 250
        continue
    }

    $serviceStates = @()
    foreach ($unit in $coreServices) {
        $serviceStates += [ordered]@{
            unit = $unit
            state = Get-WslLine "systemctl is-active '$unit' 2>/dev/null || true"
        }
    }
    $timerNames = Get-WslLine (
        "systemctl list-unit-files --state=enabled --type=timer --no-legend " +
        "| awk '/^stockagent-/{print `$1}'"
    )
    $timerStates = @()
    foreach ($unit in @($timerNames -split "`n" |
        ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
        $timerStates += [ordered]@{
            unit = $unit
            state = Get-WslLine "systemctl is-active '$unit' 2>/dev/null || true"
        }
    }
    $failedUnits = Get-WslLine "systemctl --failed --no-legend --plain"
    $systemState = Get-WslLine "systemctl is-system-running 2>/dev/null || true"
    $publicProbes = @($publicPaths | ForEach-Object { Get-PublicProbe $_ })

    $servicesReady = @($serviceStates | Where-Object { $_.state -ne "active" }).Count -eq 0
    $timersReady = @($timerStates | Where-Object { $_.state -ne "active" }).Count -eq 0
    $publicReady = @($publicProbes | Where-Object { $_.status_code -ne 200 }).Count -eq 0
    if ($servicesReady -and $timersReady -and $publicReady -and
        -not $failedUnits -and $systemState -eq "running") {
        $readyAt = Get-Date
        break
    }
    Start-Sleep -Milliseconds 500
} while ((Get-Date) -lt $deadline)

$success = (
    $caddyReadyAt -and
    $caddyStoppedProbe.status_code -ne 200 -and
    $stoppedAt -and
    $readyAt -and
    $postBootId -and
    $postBootId -ne $preBootId
)
$report = [ordered]@{
    schema_version = 1
    test = "windows_caddy_and_wsl_cold_boot_recovery"
    started_at = $startedAt.ToString("o")
    caddy_ready_at = if ($caddyReadyAt) { $caddyReadyAt.ToString("o") } else { $null }
    caddy_outage_observed = [bool]($caddyStoppedProbe.status_code -ne 200)
    old_caddy_pids = $oldCaddyPids
    new_caddy_pids = $newCaddyPids
    caddy_restart_seconds = if ($caddyReadyAt) {
        [math]::Round(($caddyReadyAt - $startedAt).TotalSeconds, 3)
    } else { $null }
    wsl_shutdown_at = $shutdownAt.ToString("o")
    target_distribution = $DistroName
    wsl_stopped_at = if ($stoppedAt) { $stoppedAt.ToString("o") } else { $null }
    wsl_stopped_observed = [bool]$stoppedAt
    running_distributions_at_stop = $runningDistributionsAtStop
    wsl_shutdown_seconds = if ($stoppedAt) {
        [math]::Round(($stoppedAt - $shutdownAt).TotalSeconds, 3)
    } else { $null }
    ready_at = if ($readyAt) { $readyAt.ToString("o") } else { $null }
    wsl_recovery_seconds = if ($readyAt) {
        [math]::Round(($readyAt - $shutdownAt).TotalSeconds, 3)
    } else { $null }
    pre_boot_id = $preBootId
    post_boot_id = $postBootId
    boot_id_changed = [bool]($postBootId -and $postBootId -ne $preBootId)
    system_state = $systemState
    core_services = $serviceStates
    enabled_stockagent_timers = $timerStates
    failed_units = @($failedUnits -split "`n" | Where-Object { $_ })
    public_probes = $publicProbes
    success = [bool]$success
}
$temporaryPath = "$reportPath.tmp"
$report | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $temporaryPath
Move-Item -Force -Path $temporaryPath -Destination $reportPath
Write-Output $reportPath
if (-not $success) {
    exit 1
}
