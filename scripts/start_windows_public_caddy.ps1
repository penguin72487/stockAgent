param(
    [string]$InstallRoot = "$env:LOCALAPPDATA\StockAgentPublic",
    [string]$DistroName = "",
    [string]$CaddyPath = "",
    [string]$WslPath = "$env:WINDIR\System32\wsl.exe",
    [int]$ProbeIntervalSeconds = 5,
    [int]$WslRetrySeconds = 10
)

$ErrorActionPreference = "Stop"
$caddy = if ($CaddyPath) {
    $CaddyPath
} else {
    (Get-Command caddy.exe -ErrorAction Stop).Source
}
$config = Join-Path $InstallRoot "Caddyfile"
$logDir = Join-Path $InstallRoot "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$startupLog = Join-Path $logDir "startup.log"
$env:STOCKAGENT_PUBLIC_LOG = (Join-Path $logDir "access.json").Replace("\", "/")
$env:STOCKAGENT_PUBLIC_RUNTIME_LOG = (Join-Path $logDir "runtime.json").Replace("\", "/")
$probeInterval = [math]::Max(1, $ProbeIntervalSeconds)
$wslRetry = [math]::Max(5, $WslRetrySeconds)
$gatewayHealthUri = "http://127.0.0.1:8770/healthz"
$escapedConfig = [Regex]::Escape($config)
$wslBootstrapProcess = $null
$lastWslAttempt = [DateTime]::MinValue
$lastBackendHealthy = $null

function Write-StartupLog([string]$Message) {
    Add-Content -Path $startupLog -Value "$(Get-Date -Format o) $Message"
}

if (-not (Test-Path -LiteralPath $caddy -PathType Leaf)) {
    throw "Caddy executable does not exist: $caddy"
}
if (-not (Test-Path -LiteralPath $WslPath -PathType Leaf)) {
    throw "WSL executable does not exist: $WslPath"
}
if (-not (Test-Path -LiteralPath $config -PathType Leaf)) {
    throw "Caddy config does not exist: $config"
}

function Get-CaddyProcesses {
    return @(Get-CimInstance Win32_Process -Filter "Name = 'caddy.exe'" |
        Where-Object { $_.CommandLine -match $escapedConfig })
}

function Start-CaddyIfNeeded {
    if (@(Get-CaddyProcesses).Count -gt 0) {
        return
    }
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $caddy
    $startInfo.Arguments = "run --config `"$config`" --adapter caddyfile"
    $startInfo.WorkingDirectory = $InstallRoot
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $process = [System.Diagnostics.Process]::Start($startInfo)
    Write-StartupLog "Caddy start dispatched pid=$($process.Id) config=$config"
}

function Test-GatewayBackend {
    try {
        $request = [System.Net.HttpWebRequest]::Create($gatewayHealthUri)
        $request.Method = "GET"
        $request.Timeout = 2000
        $request.ReadWriteTimeout = 2000
        $request.Proxy = $null
        $response = $request.GetResponse()
        try {
            return [int]$response.StatusCode -eq 200
        } finally {
            $response.Dispose()
        }
    } catch {
        return $false
    }
}

function Request-WslGateway([string]$Reason) {
    $now = Get-Date
    if ($wslBootstrapProcess -and -not $wslBootstrapProcess.HasExited) {
        return
    }
    if (($now - $lastWslAttempt).TotalSeconds -lt $wslRetry) {
        return
    }
    $arguments = if ($DistroName) {
        "--distribution `"$DistroName`" --exec /bin/sh -lc `"systemctl start --no-block stockagent-public-dashboards.service`""
    } else {
        "--exec /bin/sh -lc `"systemctl start --no-block stockagent-public-dashboards.service`""
    }
    try {
        $startInfo = New-Object System.Diagnostics.ProcessStartInfo
        $startInfo.FileName = $WslPath
        $startInfo.Arguments = $arguments
        $startInfo.UseShellExecute = $false
        $startInfo.CreateNoWindow = $true
        $script:wslBootstrapProcess = [System.Diagnostics.Process]::Start($startInfo)
        $script:lastWslAttempt = $now
        Write-StartupLog "WSL gateway start dispatched pid=$($wslBootstrapProcess.Id) distro=$DistroName reason=$Reason"
    } catch {
        $script:lastWslAttempt = $now
        Write-StartupLog "WSL gateway dispatch failed distro=$DistroName reason=$Reason error=$($_.Exception.Message)"
    }
}

Set-Location $InstallRoot
Request-WslGateway "supervisor_start"
while ($true) {
    try {
        Start-CaddyIfNeeded
    } catch {
        Write-StartupLog "Caddy start failed error=$($_.Exception.Message); retrying"
    }
    $backendHealthy = Test-GatewayBackend
    if ($lastBackendHealthy -eq $null -or $backendHealthy -ne $lastBackendHealthy) {
        Write-StartupLog "gateway backend healthy=$backendHealthy uri=$gatewayHealthUri"
        $lastBackendHealthy = $backendHealthy
    }
    if (-not $backendHealthy) {
        Request-WslGateway "backend_unhealthy"
    }
    Start-Sleep -Seconds $probeInterval
}
