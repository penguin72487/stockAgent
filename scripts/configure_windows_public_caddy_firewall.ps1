#Requires -RunAsAdministrator
param(
    [string]$CaddyPath = ""
)

$ErrorActionPreference = "Stop"
if (-not $CaddyPath) {
    $runningCaddy = Get-Process caddy -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($runningCaddy) {
        $CaddyPath = $runningCaddy.Path
    } else {
        $CaddyPath = Join-Path (& scoop.ps1 prefix caddy) "caddy.exe"
    }
}
$ruleName = "StockAgent Public HTTPS Gateway"
Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue |
    Remove-NetFirewallRule
New-NetFirewallRule `
    -DisplayName $ruleName `
    -Direction Inbound `
    -Action Allow `
    -Enabled True `
    -Profile Any `
    -Protocol TCP `
    -LocalPort 80,443 `
    -Program $CaddyPath | Out-Null
Write-Output "Allowed Caddy TCP 80/443 for public HTTP/HTTPS"
