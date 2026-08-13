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

$env:STOCKAGENT_PUBLIC_LOG = (Join-Path $logDir "access.json").Replace("\", "/")
$env:STOCKAGENT_PUBLIC_RUNTIME_LOG = (Join-Path $logDir "runtime.json").Replace("\", "/")
& caddy.exe validate --config (Join-Path $installRoot "Caddyfile") --adapter caddyfile

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$installRoot\start-caddy.ps1`""
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 5 `
    -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask `
    -TaskName "StockAgent Public Caddy" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "HTTPS gateway for sanitized StockAgent public dashboards" `
    -Force | Out-Null

Start-ScheduledTask -TaskName "StockAgent Public Caddy"
Write-Output "Caddy scheduled task installed under $installRoot"
