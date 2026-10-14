"""Isolated native Windows tests: every firewall/process command is mocked."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
POWERSHELL = shutil.which("powershell.exe")


def test_privileged_maintenance_is_protected_and_narrow() -> None:
    installer = (ROOT / "scripts/install_windows_public_caddy_firewall_maintenance.ps1").read_text()
    assert '#Requires -RunAsAdministrator' in installer
    assert '$env:ProgramData "StockAgentPublicFirewallMaintenance"' in installer
    assert 'SetAccessRuleProtection($true, $false)' in installer
    assert '"ReadAndExecute"' in installer
    assert 'ReparsePoint' in installer
    assert '-UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest' in installer
    assert '-RepetitionInterval (New-TimeSpan -Minutes 1)' in installer
    assert '-MultipleInstances IgnoreNew' in installer
    assert '-Maintenance -AllowedCaddyRoot' in installer
    assert 'Copy-Item -LiteralPath $SourceScript -Destination $staged' in installer
    assert '[IO.File]::Replace($staged, $target, [NullString]::Value)' in installer
    assert 'Start-ScheduledTask -TaskName $taskName' in installer
    for forbidden in ('Restart-Service', 'Stop-Process', 'wsl.exe', 'Dynu', 'Clear-NetFirewall', 'Remove-NetFirewall'):
        assert forbidden not in installer


def test_native_request_uses_existing_elevation_and_explicit_opt_in() -> None:
    request = (ROOT / "scripts/request_windows_public_caddy_firewall.ps1").read_text()
    assert 'param([switch]$InstallMaintenance)' in request
    assert '-Verb RunAs' in request
    assert '-Wait -PassThru' in request
    assert '"-AllowedCaddyRoot"' in request
    assert 'if ($InstallMaintenance)' in request


@pytest.mark.skipif(not POWERSHELL, reason="native Windows interop required")
def test_native_atomic_install_replaces_existing_file_without_empty_backup_path() -> None:
    import base64

    command = r"""
    $ErrorActionPreference='Stop'
    $staged=[IO.Path]::GetTempFileName()
    $target=[IO.Path]::GetTempFileName()
    try {
        [IO.File]::WriteAllText($staged, 'new-script')
        [IO.File]::WriteAllText($target, 'old-script')
        $wrongArgumentError=$null
        try { [IO.File]::Replace($staged, $target, $null) } catch { $wrongArgumentError=$_.Exception.Message }
        # This is the exact production operation; recreate the fixtures on
        # runtimes that accept the old overload as well.
        [IO.File]::WriteAllText($staged, 'new-script')
        [IO.File]::Replace($staged, $target, [NullString]::Value)
        [pscustomobject]@{replaced=([IO.File]::ReadAllText($target) -eq 'new-script'); old_argument_error=$wrongArgumentError} |
            ConvertTo-Json -Compress
    } finally {
        foreach($path in @($staged,$target)) { if([IO.File]::Exists($path)){[IO.File]::Delete($path)} }
    }
    """
    result = subprocess.run(
        [POWERSHELL, "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand",
         base64.b64encode(command.encode("utf-16le")).decode("ascii")],
        capture_output=True, text=True, timeout=15, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["replaced"] is True


NATIVE_FIXTURES = r"""
$ErrorActionPreference = 'Stop'
$script:case = '__CASE__'
$script:program = 'C:\approved\caddy\2.11.7\caddy.exe'
$script:rule = [pscustomobject]@{
    Name='owned-rule'; Enabled='True'; Direction='Inbound'; Action='Allow'; Profile='Any';
    Program=$script:program; Protocol='TCP'; LocalPort=@('80','443'); RemotePort='Any';
    LocalAddress='Any'; RemoteAddress='Any'
}
$script:writes = 0
$script:receiptJson = $null
if ($script:case -eq 'version_changed') { $script:rule.Program='C:\approved\caddy\2.10.0\caddy.exe' }
if ($script:case -eq 'disabled') { $script:rule.Enabled='False' }
if ($script:case -eq 'broad_ports') { $script:rule.LocalPort=@('Any') }
if ($script:case -eq 'outside_root') { $script:program='C:\unapproved\caddy.exe' }
if ($script:case -eq 'shim') { $script:program='C:\approved\caddy\shims\caddy.exe' }
if ($script:case -eq 'set_failure') { $script:rule.Program='C:\approved\caddy\old\caddy.exe' }

function Get-NetTCPConnection {
    param($State, $ErrorAction)
    if ($script:case -eq 'no_listener') { return }
    [pscustomobject]@{LocalPort=443; OwningProcess=100}
    if ($script:case -eq 'ambiguous') { [pscustomobject]@{LocalPort=80; OwningProcess=101} }
}
function Get-Process {
    param($Id, $ErrorAction)
    $path=$script:program
    if ($Id -eq 101) { $path='C:\approved\caddy\other\caddy.exe' }
    [pscustomobject]@{ProcessName='caddy'; Path=$path}
}
function Test-Path { param($LiteralPath, $PathType) return ($script:case -ne 'missing_image') }
function Get-Item {
    param($LiteralPath)
    if ($LiteralPath.EndsWith('caddy.exe')) {
        [pscustomobject]@{Name='caddy.exe'; FullName=$LiteralPath; DirectoryName=(Split-Path -Parent $LiteralPath)}
    } else { [pscustomobject]@{LinkType=$null; Target=@(); FullName=$LiteralPath} }
}
function Get-NetFirewallRule {
    param($PolicyStore, $DisplayName, $ErrorAction)
    if ($script:case -eq 'create_rule' -and $script:writes -eq 0) { return }
    if ($PolicyStore -eq 'ActiveStore' -and $script:case -eq 'bad_active_policy') {
        $bad = $script:rule.PSObject.Copy(); $bad.LocalPort=@('Any'); return $bad
    }
    $script:rule
}
function Get-NetFirewallApplicationFilter {
    param([Parameter(ValueFromPipeline=$true)]$Rule)
    process { [pscustomobject]@{Program=$Rule.Program} }
}
function Get-NetFirewallPortFilter {
    param([Parameter(ValueFromPipeline=$true)]$Rule)
    process { [pscustomobject]@{Protocol=$Rule.Protocol; LocalPort=$Rule.LocalPort; RemotePort=$Rule.RemotePort} }
}
function Get-NetFirewallAddressFilter {
    param([Parameter(ValueFromPipeline=$true)]$Rule)
    process { [pscustomobject]@{LocalAddress=$Rule.LocalAddress; RemoteAddress=$Rule.RemoteAddress} }
}
function Set-NetFirewallRule {
    param($Name,$Direction,$Action,$Enabled,$Profile,$Protocol,$LocalPort,$RemotePort,$LocalAddress,$RemoteAddress,$Program)
    if ($script:case -eq 'set_failure') { throw 'injected write failure' }
    if (($LocalPort -join ',') -ne '80,443' -or $Direction -ne 'Inbound' -or $Action -ne 'Allow') {
        throw 'unexpected scope expansion'
    }
    $script:writes++
    foreach($key in @('Direction','Action','Enabled','Profile','Protocol','LocalPort','RemotePort','LocalAddress','RemoteAddress','Program')) {
        $script:rule.$key = Get-Variable -Name $key -ValueOnly
    }
}
function New-NetFirewallRule {
    param($Name,$DisplayName,$Direction,$Action,$Enabled,$Profile,$Protocol,$LocalPort,$RemotePort,$LocalAddress,$RemoteAddress,$Program)
    Set-NetFirewallRule -Name $Name -Direction $Direction -Action $Action -Enabled $Enabled -Profile $Profile `
        -Protocol $Protocol -LocalPort $LocalPort -RemotePort $RemotePort -LocalAddress $LocalAddress -RemoteAddress $RemoteAddress -Program $Program
}
function New-Item { param($ItemType,[switch]$Force,$Path) }
function Set-Content { param([Parameter(ValueFromPipeline=$true)]$Value,$LiteralPath,$Encoding) process {$script:receiptJson=$Value} }
function Move-Item { param($LiteralPath,$Destination,[switch]$Force) }
function scoop.ps1 { throw 'maintenance must not execute scoop as SYSTEM' }
function Remove-NetFirewallRule { throw 'must not delete the previous rule' }

# Only the administrator marker is removed. Production command bodies execute
# unchanged against the mocks above, with no elevated token or real mutation.
$body = [IO.File]::ReadAllText('__SOURCE__') -replace '(?m)^#Requires -RunAsAdministrator\r?\n', ''
$block = [ScriptBlock]::Create($body)
$failed = $false
try {
    & $block -Maintenance -AllowedCaddyRoot 'C:\approved\caddy' -ReceiptPath 'C:\mock\receipt.json' | Out-Null
} catch { $failed=$true; $failureMessage=$_.Exception.Message }
if (-not $script:receiptJson) { throw "missing outcome receipt: $failureMessage" }
$receipt = $script:receiptJson | ConvertFrom-Json
[pscustomobject]@{failed=$failed; writes=$script:writes; receipt=$receipt} | ConvertTo-Json -Depth 6 -Compress
"""


@pytest.mark.skipif(not POWERSHELL or not shutil.which("wslpath"), reason="native Windows interop required")
@pytest.mark.parametrize(
    ("case", "state", "writes", "success"),
    [
        ("healthy", "unchanged", 0, True),
        ("version_changed", "repaired", 1, True),
        ("disabled", "repaired", 1, True),
        ("broad_ports", "repaired", 1, True),
        ("create_rule", "repaired", 1, True),
        ("no_listener", "waiting_for_caddy", 0, False),
        ("ambiguous", "failed", 0, False),
        ("outside_root", "failed", 0, False),
        ("shim", "failed", 0, False),
        ("missing_image", "failed", 0, False),
        ("set_failure", "failed", 0, False),
        ("bad_active_policy", "failed", 0, False),
    ],
)
def test_native_firewall_reconciliation(case: str, state: str, writes: int, success: bool) -> None:
    import base64

    source = subprocess.check_output(
        ["wslpath", "-w", str(ROOT / "scripts/configure_windows_public_caddy_firewall.ps1")], text=True
    ).strip()
    command = NATIVE_FIXTURES.replace("__CASE__", case).replace("__SOURCE__", source.replace("'", "''"))
    encoded = base64.b64encode(command.encode("utf-16le")).decode("ascii")
    result = subprocess.run(
        [POWERSHELL, "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
        capture_output=True, text=True, timeout=25, check=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip())
    assert payload["failed"] is not success
    assert payload["writes"] == writes
    assert payload["receipt"]["state"] == state
    assert payload["receipt"]["success"] is success
    assert payload["receipt"]["local_ports"] == [80, 443]
