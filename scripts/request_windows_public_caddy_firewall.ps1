$ErrorActionPreference = "Stop"
$source = Join-Path $PSScriptRoot "configure_windows_public_caddy_firewall.ps1"
$installRoot = Join-Path $env:LOCALAPPDATA "StockAgentPublic"
$target = Join-Path $installRoot "configure-firewall.ps1"
New-Item -ItemType Directory -Force -Path $installRoot | Out-Null
Copy-Item -Force $source $target
$receipt = Join-Path $installRoot "logs\firewall-configure.json"

$process = Start-Process `
    -FilePath "powershell.exe" `
    -Verb RunAs `
    -ArgumentList "-NoLogo", "-NoProfile", "-NonInteractive", `
        "-ExecutionPolicy", "Bypass", "-File", "`"$target`"", `
        "-ReceiptPath", "`"$receipt`"" `
    -Wait -PassThru
if ($process.ExitCode -ne 0) {
    throw "Caddy firewall configuration failed (exit $($process.ExitCode))"
}
Write-Output "Caddy firewall configuration completed; receipt=$receipt"
