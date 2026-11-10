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
$growthFunction=@($ast.FindAll({param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Get-FullAllocationGrowth'
},$true))
if ($growthFunction.Count -ne 1) {throw 'Expected one whole-target physical allocation planner'}
. ([scriptblock]::Create($growthFunction[0].Extent.Text))
foreach ($case in @(@(3TB,2TB,1TB),@(3TB,3TB,0L),@(3TB,(3TB+1MB),0L),@(3TB,0L,3TB))) {
    if ((Get-FullAllocationGrowth $case[0] $case[1]) -ne $case[2]) {
        throw 'Whole-target admission failed; virtual remaining space was omitted'
    }
}
foreach ($case in @(@(0L,0L),@(3TB,-1L))) {
    $rejected=$false
    try { $null=Get-FullAllocationGrowth $case[0] $case[1] }
    catch { $rejected=$_.Exception.Message.Contains('Invalid physical allocation estimate') }
    if (-not $rejected) {throw 'Invalid full-allocation estimate was admitted'}
}
$journalFunction=@($ast.FindAll({param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Test-ShutdownJournalEvent'
},$true))
if ($journalFunction.Count -ne 1) {throw 'Expected one bounded shutdown journal gate'}
. ([scriptblock]::Create($journalFunction[0].Extent.Text))
$journalPath=Join-Path $runRoot 'synthetic-shutdown-journal.jsonl'
$token='verification-fixed-token'
$target='{"_PID":"1","MESSAGE":"Reached target poweroff.target - System Power Off."}'
$wrongPid='{"_PID":"44","MESSAGE":"Reached target poweroff.target - System Power Off."}'
$ready='{"SYSLOG_IDENTIFIER":"stockagent-wsl-maint-watch","MESSAGE":"verification-fixed-token"}'
[IO.File]::WriteAllText($journalPath, ($target + "`n" + $ready + "`n" + $wrongPid + "`n"), [Text.UTF8Encoding]::new($false))
if (-not (Test-ShutdownJournalEvent $journalPath $token 'ready') -or
        (Test-ShutdownJournalEvent $journalPath $token 'poweroff') -or
        (Test-ShutdownJournalEvent $journalPath 'wrong-token' 'ready')) {
    throw 'A stale target, wrong PID or wrong observer token was accepted'
}
[IO.File]::AppendAllText($journalPath, ($target + "`n"), [Text.UTF8Encoding]::new($false))
if (-not (Test-ShutdownJournalEvent $journalPath $token 'poweroff')) {
    throw 'A fresh PID-1 target following the observer token was rejected'
}
$rejectedEvent=$false
try { $null=Test-ShutdownJournalEvent $journalPath $token 'unknown' } catch { $rejectedEvent=$true }
if (-not $rejectedEvent) {throw 'Unknown shutdown event was admitted'}
$dataRejections=0
foreach ($options in @(
    @{DataDistroName='Ubuntu-26.04'},
    @{DataDistroName='unsafe distro'},
    @{DataDistroName='StockAgentData';ExpectedDataVhdPath='C:\wrong.vhdx'},
    @{DataTargetSizeBytes=1TB},
    @{DataDistroName='StockAgentData';ExpectedDataVhdPath='E:\data.vhdx';
      ExpectedDataVolumeId='8d21b58a-e583-4941-9d0e-cd0a15dc18c8';
      ExpectedDataFilesystemUuid='a4d071d9-941f-446a-91b0-6ac36b08275a';
      DataTargetSizeBytes=4TB}
)) {
    $rejected=$false
    try { & $WorkerPath -Mode Plan -DistroName 'Ubuntu-26.04' -ExpectedVhdPath 'C:\unused.vhdx' @options }
    catch { $rejected=$_.Exception.Message.Contains('Data expansion requires Compact/Plan') }
    if (-not $rejected) {throw 'Incomplete or unsafe E expansion scope reached native disk queries'}
    $dataRejections++
}
$proof=[ordered]@{contract='stockagent_windows_storage_native_control_verification_v1';
    worker_sha256=(Get-FileHash -LiteralPath $WorkerPath -Algorithm SHA256).Hash.ToLowerInvariant();
    native_stderr_exit_zero_preserved=$true; native_nonzero_rejected=$true;
    caller_error_policy_restored=$true; invalid_target_rejections=$rejections;
    invalid_companion_scope_rejections=3; disks_modified=$false;
    journal_target_order_and_pid_verified=$true; wrong_token_and_event_rejected=$true;
    whole_target_growth_cases=4; invalid_growth_cases=2; invalid_data_scope_rejections=$dataRejections;
    observed_at_utc=[DateTime]::UtcNow.ToString('o')}
[IO.File]::WriteAllText((Join-Path $runRoot 'acceptance.json'),($proof | ConvertTo-Json -Depth 8),[Text.UTF8Encoding]::new($false))
$proof | ConvertTo-Json -Depth 8
