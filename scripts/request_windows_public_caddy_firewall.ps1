param([switch]$InstallMaintenance)

$ErrorActionPreference = "Stop"
$source = Join-Path $PSScriptRoot "configure_windows_public_caddy_firewall.ps1"
$installRoot = Join-Path $env:LOCALAPPDATA "StockAgentPublic"
$target = Join-Path $installRoot "configure-firewall.ps1"
New-Item -ItemType Directory -Force -Path $installRoot | Out-Null
Copy-Item -Force $source $target
$receipt = Join-Path $installRoot "logs\firewall-configure.json"
$elevatedScript = $target
$extraArguments = @("-ReceiptPath", "`"$receipt`"")
if ($InstallMaintenance) {
    $prefix = & scoop.ps1 prefix caddy
    if ($LASTEXITCODE -ne 0 -or -not $prefix) { throw "Cannot locate the Caddy installation root" }
    $approvedRoot = Split-Path -Parent $prefix
    $installer = Join-Path $PSScriptRoot "install_windows_public_caddy_firewall_maintenance.ps1"
    $elevatedScript = Join-Path $installRoot "install-firewall-maintenance.ps1"
    Copy-Item -LiteralPath $installer -Destination $elevatedScript -Force
    $extraArguments = @("-SourceScript", "`"$target`"", "-AllowedCaddyRoot", "`"$approvedRoot`"")
}

$arguments = @("-NoLogo", "-NoProfile", "-NonInteractive", `
    "-ExecutionPolicy", "Bypass", "-File", "`"$elevatedScript`"") + $extraArguments
$process = Start-Process `
    -FilePath "powershell.exe" `
    -Verb RunAs `
    -ArgumentList $arguments `
    -Wait -PassThru
if ($process.ExitCode -ne 0) {
    throw "Caddy firewall configuration failed (exit $($process.ExitCode))"
}
if ($InstallMaintenance) {
    Write-Output "Caddy firewall maintenance installed; inspect C:\ProgramData\StockAgentPublicFirewallMaintenance\logs\firewall-maintenance.json"
} else {
    Write-Output "Caddy firewall configuration completed; receipt=$receipt"
}
