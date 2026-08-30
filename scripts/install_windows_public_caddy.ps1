param(
    [Parameter(Mandatory = $true)]
    [string]$SourceCaddyfile,
    [Parameter(Mandatory = $true)]
    [string]$SourceLauncher
)

$ErrorActionPreference = "Stop"
if (-not (Get-Command caddy.exe -ErrorAction SilentlyContinue)) {
    if (-not (Get-Command scoop.ps1 -ErrorAction SilentlyContinue)) {
        throw "Caddy is not installed and Scoop is unavailable"
    }
    & scoop.ps1 install caddy
}

$installRoot = Join-Path $env:LOCALAPPDATA "StockAgentPublic"
$logDir = Join-Path $installRoot "logs"
New-Item -ItemType Directory -Force -Path $installRoot, $logDir | Out-Null
Copy-Item -Force $SourceCaddyfile (Join-Path $installRoot "Caddyfile")
Copy-Item -Force $SourceLauncher (Join-Path $installRoot "start-caddy.ps1")
$caddyPath = (Get-Command caddy.exe -ErrorAction Stop).Source
$wslPath = Join-Path $env:WINDIR "System32\wsl.exe"
$lxssRoot = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss"
$defaultDistributionId = [string](Get-ItemProperty $lxssRoot).DefaultDistribution
if (-not $defaultDistributionId) {
    throw "Cannot resolve the default WSL distribution ID"
}
$distributionKey = Join-Path $lxssRoot $defaultDistributionId
$distroName = [string](Get-ItemProperty $distributionKey).DistributionName
if (-not $distroName) {
    throw "Cannot resolve the default WSL distribution name"
}

$env:STOCKAGENT_PUBLIC_LOG = (Join-Path $logDir "access.json").Replace("\", "/")
$env:STOCKAGENT_PUBLIC_RUNTIME_LOG = (Join-Path $logDir "runtime.json").Replace("\", "/")
& caddy.exe validate --config (Join-Path $installRoot "Caddyfile") --adapter caddyfile

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument (
        "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden " +
        "-ExecutionPolicy Bypass -File `"$installRoot\start-caddy.ps1`" " +
        "-InstallRoot `"$installRoot`" -DistroName `"$distroName`" " +
        "-CaddyPath `"$caddyPath`" -WslPath `"$wslPath`""
    )
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal `
    -UserId $currentUser `
    -LogonType S4U `
    -RunLevel Limited
$startupTrigger = New-ScheduledTaskTrigger -AtStartup
$startupTrigger.Delay = "PT15S"
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $currentUser
# A task that is externally terminated can finish with 0xC000013A, which Task
# Scheduler treats as a cancellation instead of a retryable crash.  Keep a
# one-minute recurring trigger as an independent liveness source.  IgnoreNew
# means it is free while the long-running gateway is healthy and starts a new
# instance within one minute only when the prior task is no longer running.
$livenessTrigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 1)
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -RestartCount 5 `
    -RestartInterval (New-TimeSpan -Minutes 1)
$description = "HTTPS gateway for sanitized StockAgent public dashboards"
$preLoginRecovery = $true
try {
    Register-ScheduledTask `
        -TaskName "StockAgent Public Caddy" `
        -Action $action `
        -Trigger @($startupTrigger, $logonTrigger, $livenessTrigger) `
        -Principal $principal `
        -Settings $settings `
        -Description $description `
        -Force | Out-Null
} catch {
    # Creating an S4U/startup principal requires an elevated Windows token on
    # some hosts.  Keep the self-healing supervisor deployable without silently
    # claiming pre-login recovery: the interactive fallback starts at logon and
    # then continuously retries both WSL and Caddy.
    $preLoginRecovery = $false
    Write-Warning (
        "Pre-login S4U task registration was denied; installing explicit " +
        "at-logon self-healing fallback. Run this installer once from an " +
        "elevated Windows PowerShell to enable pre-login recovery. " +
        "Error: $($_.Exception.Message)"
    )
    Register-ScheduledTask `
        -TaskName "StockAgent Public Caddy" `
        -Action $action `
        -Trigger @($logonTrigger, $livenessTrigger) `
        -User $currentUser `
        -Settings $settings `
        -Description $description `
        -Force | Out-Null
}

Start-ScheduledTask -TaskName "StockAgent Public Caddy"
Write-Output (
    "Caddy scheduled task installed under $installRoot " +
    "distro=$distroName pre_login_recovery=$preLoginRecovery"
)
