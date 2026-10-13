#Requires -RunAsAdministrator
param(
    [string]$CaddyPath = "",
    [string]$ReceiptPath = ""
)

$ErrorActionPreference = "Stop"

function Resolve-CaddyFirewallPath([string]$Path) {
    if (-not $Path -or -not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Caddy executable does not exist: $Path"
    }
    $file = Get-Item -LiteralPath $Path
    if ($file.Name -ne "caddy.exe" -or $file.FullName -match '[\\/]shims[\\/]') {
        throw "Firewall must target the Caddy server, not a command shim: $Path"
    }
    # WFP matches the executable image, not the Scoop command or its moving
    # 'current' junction. Resolve the junction when Caddy has not started yet.
    $parent = Get-Item -LiteralPath $file.DirectoryName
    if ($parent.LinkType -eq "Junction" -and @($parent.Target).Count -eq 1) {
        $file = Get-Item -LiteralPath (Join-Path $parent.Target[0] $file.Name)
    }
    return $file.FullName
}

if (-not $CaddyPath) {
    $serverPaths = @(
        Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
            Where-Object { $_.LocalPort -in @(80, 443) } |
            Select-Object -ExpandProperty OwningProcess -Unique |
            ForEach-Object {
                $server = Get-Process -Id $_ -ErrorAction Stop
                if ($server.ProcessName -eq "caddy") {
                    Resolve-CaddyFirewallPath $server.Path
                }
            } | Select-Object -Unique
    )
    if ($serverPaths.Count -gt 1) {
        throw "More than one Caddy executable owns the public ports"
    }
    if ($serverPaths.Count -eq 1) {
        $CaddyPath = $serverPaths[0]
    } else {
        $CaddyPath = Join-Path (& scoop.ps1 prefix caddy) "caddy.exe"
    }
}
$CaddyPath = Resolve-CaddyFirewallPath $CaddyPath
$ruleName = "StockAgent Public HTTPS Gateway"
$existingRules = @(Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue)
$beforePrograms = @($existingRules | Get-NetFirewallApplicationFilter |
    Select-Object -ExpandProperty Program)
$ruleArguments = @{
    Direction = "Inbound"
    Action = "Allow"
    Enabled = "True"
    Profile = "Any"
    Protocol = "TCP"
    LocalPort = @(80, 443)
    LocalAddress = "Any"
    RemoteAddress = "Any"
    Program = $CaddyPath
}
if ($existingRules.Count -gt 0) {
    # Update in place: a failed validation/update must not remove the last rule.
    foreach ($rule in $existingRules) {
        Set-NetFirewallRule -Name $rule.Name @ruleArguments | Out-Null
    }
} else {
    New-NetFirewallRule -Name "StockAgentPublicHTTPS" `
        -DisplayName $ruleName @ruleArguments | Out-Null
}
$effectiveRules = @(Get-NetFirewallRule -PolicyStore ActiveStore -DisplayName $ruleName)
$afterPrograms = @($effectiveRules | Get-NetFirewallApplicationFilter |
    Select-Object -ExpandProperty Program)
if (-not $afterPrograms.Count -or @($afterPrograms | Where-Object { $_ -ne $CaddyPath }).Count) {
    throw "The active firewall policy does not match the Caddy executable"
}
if ($ReceiptPath) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $ReceiptPath) | Out-Null
    [ordered]@{
        schema_version = 1
        applied_at_utc = [DateTime]::UtcNow.ToString("o")
        caddy_path = $CaddyPath
        before_programs = $beforePrograms
        after_programs = $afterPrograms
        local_ports = @(80, 443)
        success = $true
    } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $ReceiptPath -Encoding UTF8
}
Write-Output "Allowed Caddy TCP 80/443 for public HTTP/HTTPS: $CaddyPath"
