param(
    [string]$InstallRoot = "$env:LOCALAPPDATA\StockAgentPublic"
)

$ErrorActionPreference = "Stop"
$caddy = (Get-Command caddy.exe -ErrorAction Stop).Source
$config = Join-Path $InstallRoot "Caddyfile"
$logDir = Join-Path $InstallRoot "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$env:STOCKAGENT_PUBLIC_LOG = (Join-Path $logDir "access.json").Replace("\", "/")
$env:STOCKAGENT_PUBLIC_RUNTIME_LOG = (Join-Path $logDir "runtime.json").Replace("\", "/")

# Starting the default WSL distribution also restores the systemd-supervised
# localhost gateway after a Windows login or reboot.
& wsl.exe --exec /bin/sh -lc "systemctl start stockagent-public-dashboards.service" | Out-Null

$escapedConfig = [Regex]::Escape($config)
$existing = Get-CimInstance Win32_Process -Filter "Name = 'caddy.exe'" |
    Where-Object { $_.CommandLine -match $escapedConfig }
if ($existing) {
    exit 0
}

Set-Location $InstallRoot
& $caddy run --config $config --adapter caddyfile
