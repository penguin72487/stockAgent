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
# localhost gateway after a Windows login or reboot.  This is a best-effort
# prerequisite: WSL interop can occasionally hang even when the gateway is
# already healthy, and it must not prevent Caddy from binding public HTTPS.
try {
    $wslStartInfo = New-Object System.Diagnostics.ProcessStartInfo
    $wslStartInfo.FileName = (Join-Path $env:WINDIR "System32\wsl.exe")
    $wslStartInfo.Arguments = '--exec /bin/sh -lc "systemctl start stockagent-public-dashboards.service"'
    $wslStartInfo.UseShellExecute = $false
    $wslStartInfo.CreateNoWindow = $true
    $wslProcess = [System.Diagnostics.Process]::Start($wslStartInfo)
    if (-not $wslProcess.WaitForExit(15000)) {
        $wslProcess.Kill()
        Add-Content -Path $startupLog -Value "$(Get-Date -Format o) WSL gateway start timed out; continuing with Caddy"
    } elseif ($wslProcess.ExitCode -ne 0) {
        Add-Content -Path $startupLog -Value "$(Get-Date -Format o) WSL gateway start exited $($wslProcess.ExitCode); continuing with Caddy"
    }
} catch {
    Add-Content -Path $startupLog -Value "$(Get-Date -Format o) WSL gateway start failed; continuing with Caddy"
}

$escapedConfig = [Regex]::Escape($config)
$existing = Get-CimInstance Win32_Process -Filter "Name = 'caddy.exe'" |
    Where-Object { $_.CommandLine -match $escapedConfig }
if ($existing) {
    exit 0
}

Set-Location $InstallRoot
& $caddy run --config $config --adapter caddyfile
