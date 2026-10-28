[CmdletBinding()]
param([string]$WorkerPath = '')
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
if (-not $WorkerPath) { $WorkerPath = Join-Path $PSScriptRoot 'maintain_windows_wsl_storage.ps1' }
$tokens=$null; $parseErrors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($WorkerPath,[ref]$tokens,[ref]$parseErrors)
if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
$function=@($ast.FindAll({param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Invoke-WslChecked'
},$true))
if ($function.Count -ne 1) {throw 'Expected exactly one maintained native command wrapper'}
# Load only this reviewed function. Never execute the worker's mutation branch.
. ([scriptblock]::Create($function[0].Extent.Text))
$runRoot=Join-Path $env:LOCALAPPDATA ('StockAgent\maintenance\verification-' + [Guid]::NewGuid())
New-Item -ItemType Directory -Path $runRoot | Out-Null
$wsl=Join-Path $env:WINDIR 'System32\cmd.exe'
$output=Invoke-WslChecked @('/c','echo native-stderr-preserved 1>&2 & exit /b 0')
if (-not $output.Contains('native-stderr-preserved') -or $ErrorActionPreference -ne 'Stop') {
    throw 'Native stderr success or caller error policy was not preserved'
}
$failed=$false
try { $null=Invoke-WslChecked @('/c','exit /b 7') }
catch { $failed=$_.Exception.Message.Contains('wsl.exe exited 7') }
if (-not $failed -or $ErrorActionPreference -ne 'Stop') {throw 'Nonzero native exit was accepted'}
$rejections=@()
foreach ($target in @(0L,-1048576L,3298534883329L,2999997956097L)) {
    $rejected=$false
    try { & $WorkerPath -Mode Plan -DistroName 'Ubuntu-26.04' -ExpectedVhdPath 'C:\unused.vhdx' -TargetSizeBytes $target }
    catch { $rejected=$_.Exception.Message.Contains('Target must be positive, MiB-aligned') }
    if (-not $rejected) {throw "Invalid target reached a native disk operation: $target"}
    $rejections+= $target
}
foreach ($names in @(@('Ubuntu-26.04'),@('Wazuh-Security','Wazuh-Security'),@('unsafe distro'))) {
    $rejected=$false
    try { & $WorkerPath -Mode Plan -DistroName 'Ubuntu-26.04' -ExpectedVhdPath 'C:\unused.vhdx' -CompanionDistroNames $names }
    catch { $rejected=$_.Exception.Message.Contains('Companion distributions must be distinct') }
    if (-not $rejected) {throw 'Invalid companion scope was admitted'}
}
$proof=[ordered]@{contract='stockagent_windows_storage_native_control_verification_v1';
    worker_sha256=(Get-FileHash -LiteralPath $WorkerPath -Algorithm SHA256).Hash.ToLowerInvariant();
    native_stderr_exit_zero_preserved=$true; native_nonzero_rejected=$true;
    caller_error_policy_restored=$true; invalid_target_rejections=$rejections;
    invalid_companion_scope_rejections=3; disks_modified=$false;
    observed_at_utc=[DateTime]::UtcNow.ToString('o')}
[IO.File]::WriteAllText((Join-Path $runRoot 'acceptance.json'),($proof | ConvertTo-Json -Depth 8),[Text.UTF8Encoding]::new($false))
$proof | ConvertTo-Json -Depth 8
