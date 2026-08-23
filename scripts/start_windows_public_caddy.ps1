param(
    [string]$InstallRoot = "$env:LOCALAPPDATA\StockAgentPublic"
)

$ErrorActionPreference = "Stop"
$caddy = (Get-Command caddy.exe -ErrorAction Stop).Source
$config = Join-Path $InstallRoot "Caddyfile"
$logDir = Join-Path $InstallRoot "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$startupLog = Join-Path $logDir "startup.log"
$env:STOCKAGENT_PUBLIC_LOG = (Join-Path $logDir "access.json").Replace("\", "/")
$env:STOCKAGENT_PUBLIC_RUNTIME_LOG = (Join-Path $logDir "runtime.json").Replace("\", "/")

# Starting the default WSL distribution also restores the systemd-supervised
# localhost gateway after a Windows login or reboot. Dispatch it without
# waiting: Caddy can bind HTTPS while WSL starts, and its active health check
# will admit the upstream as soon as /healthz becomes ready. Never kill the WSL
# bootstrap process because it may own the distro's first-start transaction.
try {
    $wslStartInfo = New-Object System.Diagnostics.ProcessStartInfo
    $wslStartInfo.FileName = (Join-Path $env:WINDIR "System32\wsl.exe")
    $wslStartInfo.Arguments = '--exec /bin/sh -lc "systemctl start --no-block stockagent-public-dashboards.service"'
    $wslStartInfo.UseShellExecute = $false
    $wslStartInfo.CreateNoWindow = $true
    $wslProcess = [System.Diagnostics.Process]::Start($wslStartInfo)
    Add-Content -Path $startupLog -Value "$(Get-Date -Format o) WSL gateway start dispatched pid=$($wslProcess.Id)"
} catch {
    Add-Content -Path $startupLog -Value "$(Get-Date -Format o) WSL gateway dispatch failed: $($_.Exception.Message); continuing with Caddy"
}

$escapedConfig = [Regex]::Escape($config)
$existing = Get-CimInstance Win32_Process -Filter "Name = 'caddy.exe'" |
    Where-Object { $_.CommandLine -match $escapedConfig }
if ($existing) {
    exit 0
}

Set-Location $InstallRoot
Add-Content -Path $startupLog -Value "$(Get-Date -Format o) Caddy starting config=$config"
try {
    & $caddy run --config $config --adapter caddyfile
    $caddyExitCode = $LASTEXITCODE
    Add-Content -Path $startupLog -Value "$(Get-Date -Format o) Caddy exited code=$caddyExitCode"
    exit $caddyExitCode
} catch {
    Add-Content -Path $startupLog -Value "$(Get-Date -Format o) Caddy failed: $($_.Exception.Message)"
    throw
}
