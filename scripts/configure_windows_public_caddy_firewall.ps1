#Requires -RunAsAdministrator
param(
    [string]$CaddyPath = "",
    [string]$ReceiptPath = "",
    [string]$AllowedCaddyRoot = "",
    [switch]$Maintenance
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

function Test-CaddyFirewallRule($Rule, [string]$Program) {
    $application = $Rule | Get-NetFirewallApplicationFilter
    $port = $Rule | Get-NetFirewallPortFilter
    $address = $Rule | Get-NetFirewallAddressFilter
    $ports = @($port.LocalPort | ForEach-Object { [string]$_ } | Sort-Object)
    return (
        [string]$Rule.Enabled -eq "True" -and
        [string]$Rule.Direction -eq "Inbound" -and
        [string]$Rule.Action -eq "Allow" -and
        [string]$Rule.Profile -eq "Any" -and
        $application.Program -eq $Program -and
        [string]$port.Protocol -in @("TCP", "6") -and
        ($ports -join ",") -eq "443,80" -and
        (@($port.RemotePort) -join ",") -eq "Any" -and
        (@($address.LocalAddress) -join ",") -eq "Any" -and
        (@($address.RemoteAddress) -join ",") -eq "Any"
    )
}

function Write-FirewallReceipt($Receipt) {
    if (-not $ReceiptPath) { return }
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $ReceiptPath) | Out-Null
    $temporary = "$ReceiptPath.$PID.tmp"
    $Receipt | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $temporary -Encoding UTF8
    Move-Item -LiteralPath $temporary -Destination $ReceiptPath -Force
}

$receipt = [ordered]@{
    schema_version = 2
    checked_at_utc = [DateTime]::UtcNow.ToString("o")
    state = "failed"
    caddy_path = $null
    before_programs = @()
    after_programs = @()
    local_ports = @(80, 443)
    changed = $false
    success = $false
    error = $null
}
try {
    # A maintenance task follows the running image, never the installer's hint
    # or the SYSTEM account's PATH. No listener means waiting, not "healthy".
    if ($Maintenance) {
        if (-not $AllowedCaddyRoot) { throw "Maintenance requires an approved Caddy installation root" }
        $CaddyPath = ""
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
        } elseif ($Maintenance) {
            $receipt.state = "waiting_for_caddy"
            throw "No Caddy listener on TCP 80/443; keep the existing rule and retry"
        } else {
            $prefix = & scoop.ps1 prefix caddy
            if ($LASTEXITCODE -ne 0 -or -not $prefix) {
                throw "Cannot resolve the installed Caddy executable"
            }
            $CaddyPath = Join-Path $prefix "caddy.exe"
        }
    }
    $CaddyPath = Resolve-CaddyFirewallPath $CaddyPath
    if ($AllowedCaddyRoot) {
        $approvedRoot = [IO.Path]::GetFullPath($AllowedCaddyRoot).TrimEnd('\') + '\'
        if (-not $CaddyPath.StartsWith($approvedRoot, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Caddy listener executable is outside the approved installation root"
        }
    }
    $receipt.caddy_path = $CaddyPath
    $ruleName = "StockAgent Public HTTPS Gateway"
    $existingRules = @(Get-NetFirewallRule -PolicyStore PersistentStore -DisplayName $ruleName -ErrorAction SilentlyContinue)
    $beforePrograms = @($existingRules | Get-NetFirewallApplicationFilter |
        Select-Object -ExpandProperty Program)
    $receipt.before_programs = $beforePrograms
    $ruleArguments = @{
        Direction = "Inbound"
        Action = "Allow"
        Enabled = "True"
        Profile = "Any"
        Protocol = "TCP"
        LocalPort = @(80, 443)
        RemotePort = "Any"
        LocalAddress = "Any"
        RemoteAddress = "Any"
        Program = $CaddyPath
    }
    if ($existingRules.Count -gt 0) {
        # Update in place: a failed validation/update must not remove the last rule.
        foreach ($rule in $existingRules) {
            if (-not (Test-CaddyFirewallRule $rule $CaddyPath)) {
                Set-NetFirewallRule -Name $rule.Name @ruleArguments | Out-Null
                $receipt.changed = $true
            }
        }
    } else {
        New-NetFirewallRule -Name "StockAgentPublicHTTPS" `
            -DisplayName $ruleName @ruleArguments | Out-Null
        $receipt.changed = $true
    }
    $effectiveRules = @(Get-NetFirewallRule -PolicyStore ActiveStore -DisplayName $ruleName)
    $afterPrograms = @($effectiveRules | Get-NetFirewallApplicationFilter |
        Select-Object -ExpandProperty Program)
    $receipt.after_programs = $afterPrograms
    if (-not $effectiveRules.Count -or @($effectiveRules | Where-Object {
        -not (Test-CaddyFirewallRule $_ $CaddyPath)
    }).Count) {
        throw "The active firewall policy does not match Caddy and TCP 80/443"
    }
    $receipt.state = if ($receipt.changed) { "repaired" } else { "unchanged" }
    $receipt.success = $true
    Write-Output "Caddy TCP 80/443 firewall $($receipt.state): $CaddyPath"
} catch {
    $receipt.error = $_.Exception.Message
    throw
} finally {
    $receipt.completed_at_utc = [DateTime]::UtcNow.ToString("o")
    Write-FirewallReceipt $receipt
}
