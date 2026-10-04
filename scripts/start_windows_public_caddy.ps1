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
$lastWslVerb = ""
$lastWslReason = ""
$lastGatewayRestartAttempt = [DateTime]::MinValue
$lastBackendHealthy = $null
$consecutiveBackendFailures = 0
$restartAfterFailures = 6
$restartCooldownSeconds = 120

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
if ($DistroName -and $DistroName -notmatch '^[A-Za-z0-9._-]+$') {
    # wsl.exe invoked through ProcessStartInfo.Arguments on this host treats
    # quoted distro names literally and returns WSL_E_DISTRO_NOT_FOUND.
    # Do not silently fall back to another/default distribution.
    throw "WSL distribution name cannot be passed safely: $DistroName"
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

function Test-GatewayListener {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $connect = $client.ConnectAsync("127.0.0.1", 8770)
        if (-not $connect.Wait(500)) {
            return $false
        }
        return $client.Connected
    } catch {
        return $false
    } finally {
        $client.Dispose()
    }
}

function Record-WslGatewayCompletion {
    if (-not $wslBootstrapProcess) {
        return
    }
    try {
        if (-not $wslBootstrapProcess.HasExited) {
            return
        }
    } catch {
        Write-StartupLog (
            "WSL gateway completion unavailable " +
            "error=$($_.Exception.GetType().Name)"
        )
        return
    }
    try {
        $exitAt = $wslBootstrapProcess.ExitTime
        $elapsed = [math]::Round(
            ($exitAt - $wslBootstrapProcess.StartTime).TotalSeconds, 3
        )
        $observedLag = [math]::Round(((Get-Date) - $exitAt).TotalSeconds, 3)
        Write-StartupLog (
            "WSL gateway dispatch completed pid=$($wslBootstrapProcess.Id) " +
            "verb=$lastWslVerb reason=$lastWslReason " +
            "exit_code=$($wslBootstrapProcess.ExitCode) " +
            "elapsed_seconds=$elapsed observed_lag_seconds=$observedLag"
        )
    } catch {
        Write-StartupLog (
            "WSL gateway completion unavailable " +
            "error=$($_.Exception.GetType().Name)"
        )
    } finally {
        $wslBootstrapProcess.Dispose()
        $script:wslBootstrapProcess = $null
    }
}

function Request-WslGateway(
    [string]$Reason,
    [bool]$RestartService = $false
) {
    $now = Get-Date
    if ($wslBootstrapProcess -and -not $wslBootstrapProcess.HasExited) {
        return $false
    }
    if (($now - $lastWslAttempt).TotalSeconds -lt $wslRetry) {
        return $false
    }
    $verb = if ($RestartService) { "restart" } else { "start" }
    $arguments = if ($DistroName) {
        "--distribution $DistroName --exec /bin/sh -lc `"systemctl $verb --no-block stockagent-public-dashboards.service`""
    } else {
        "--exec /bin/sh -lc `"systemctl $verb --no-block stockagent-public-dashboards.service`""
    }
    try {
        $startInfo = New-Object System.Diagnostics.ProcessStartInfo
        $startInfo.FileName = $WslPath
        $startInfo.Arguments = $arguments
        $startInfo.UseShellExecute = $false
        $startInfo.CreateNoWindow = $true
        $script:wslBootstrapProcess = [System.Diagnostics.Process]::Start($startInfo)
        $script:lastWslAttempt = $now
        $script:lastWslVerb = $verb
        $script:lastWslReason = $Reason
        Write-StartupLog "WSL gateway $verb dispatched pid=$($wslBootstrapProcess.Id) distro=$DistroName reason=$Reason"
        return $true
    } catch {
        $script:lastWslAttempt = $now
        Write-StartupLog "WSL gateway dispatch failed distro=$DistroName reason=$Reason error=$($_.Exception.Message)"
        return $false
    }
}

Set-Location $InstallRoot
$null = Request-WslGateway "supervisor_start"
while ($true) {
    Record-WslGatewayCompletion
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
    if ($backendHealthy) {
        $consecutiveBackendFailures = 0
    } else {
        $consecutiveBackendFailures += 1
        $listenerAlive = Test-GatewayListener
        if (-not $listenerAlive) {
            $null = Request-WslGateway "backend_unhealthy"
        } elseif (
            $consecutiveBackendFailures -ge $restartAfterFailures -and
            ((Get-Date) - $lastGatewayRestartAttempt).TotalSeconds -ge
                $restartCooldownSeconds
        ) {
            # A listener that accepts TCP but cannot answer /healthz is a
            # different failure from an inactive WSL service.  Allow brief
            # CPU/GIL stalls to recover, then restart exactly the read-only
            # gateway; never restart the trading engine or Discord here.
            $dispatched = Request-WslGateway `
                "backend_sustained_unresponsive" $true
            if ($dispatched) {
                $lastGatewayRestartAttempt = Get-Date
                $consecutiveBackendFailures = 0
            }
        }
    }
    Start-Sleep -Seconds $probeInterval
}
