$ErrorActionPreference = "Stop"
$source = Join-Path $PSScriptRoot "configure_windows_public_caddy_firewall.ps1"
$installRoot = Join-Path $env:LOCALAPPDATA "StockAgentPublic"
$target = Join-Path $installRoot "configure-firewall.ps1"
New-Item -ItemType Directory -Force -Path $installRoot | Out-Null
Copy-Item -Force $source $target

Start-Process `
    -FilePath "powershell.exe" `
    -Verb RunAs `
    -ArgumentList "-NoLogo", "-NoProfile", "-NonInteractive", `
        "-ExecutionPolicy", "Bypass", "-File", $target `
    -Wait
