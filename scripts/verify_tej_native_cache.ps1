<# Cache CODE in an owned fixture only; no TEJ/Excel/source connection. #>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$Output,
      [string]$CacheDirectory,[string]$ChildOutput,[switch]$Child)
$ErrorActionPreference='Stop'
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
if($Child) {
    $tokens=$null;$errors=$null
    $ast=[Management.Automation.Language.Parser]::ParseFile($BridgeScript,[ref]$tokens,[ref]$errors)
    if($errors.Count -ne 0){throw 'Canonical bridge syntax invalid'}
    foreach($name in @('Compile-TejNativeHelper','Initialize-TejNativeHelper')) {
        $definitions=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name},$true))
        if($definitions.Count -ne 1){throw 'Unique current native initializer required'}
        Invoke-Expression $definitions[0].Extent.Text
    }
    $clock=[Diagnostics.Stopwatch]::StartNew()
    $cache=Initialize-TejNativeHelper $BridgeScript $CacheDirectory
    $seconds=$clock.Elapsed.TotalSeconds
    if([TejBridgeNative]::SignedControlResult(4294967295) -ne -1 -or
       -not [TejBridgeNative]::SameItems([string[]]@('First','Second'),[string[]]@('Second','First')) -or
       [TejBridgeNative]::SameItems([string[]]@('First','First'),[string[]]@('First','Second')) -or
       [TejBridgeNative]::PreviewActionState() -ne 0 -or [TejBridgeNative]::LastSelectionMethod -cne 'none'){
        throw 'Loaded native semantics/static worker isolation differ'
    }
    [IO.File]::WriteAllText($ChildOutput,(@{cache=$cache;seconds=$seconds;native_semantics_equal=$true;
        fresh_mutable_state=$true;provider_queries_sent=0}|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
    exit
}
if(Test-Path -LiteralPath $Output){throw 'Do not overwrite native cache acceptance'}
$directory=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) ('StockAgent\TEJSmartWizard\cache-fixture-'+[Guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($directory)
$samples=[Collections.Generic.List[object]]::new();$checks=[Collections.Generic.List[object]]::new()
function Check([string]$Name,[bool]$Value) {
    $checks.Add(@{name=$Name;accepted=$Value});if(-not $Value){throw ('Cache fixture failed: '+$Name)}
}
function Run-Child([string]$Label,[string]$Bridge) {
    $result=Join-Path $directory ($Label+'.json')
    $powershell=Join-Path $PSHOME 'powershell.exe'
    & $powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $PSCommandPath `
        -BridgeScript $Bridge -Output $Output -Child -CacheDirectory $directory -ChildOutput $result
    if($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $result)){throw 'Owned isolated cache child failed'}
    $sample=Get-Content -LiteralPath $result -Raw -Encoding UTF8|ConvertFrom-Json
    $samples.Add(@{name=$Label;state=$sample.cache.state;seconds=$sample.seconds;
        fingerprint=$sample.cache.fingerprint;dll_sha256=$sample.cache.dll_sha256;
        timings=$sample.cache.timings;
        native_semantics_equal=$sample.native_semantics_equal;fresh_mutable_state=$sample.fresh_mutable_state})
    return $sample
}
$cold=Run-Child 'cold' $BridgeScript
Check 'cold_exact_code_compiled' ($cold.cache.state -ceq 'built')
$warm=Run-Child 'warm' $BridgeScript
Check 'fresh_process_loads_exact_same_dll' ($warm.cache.state -ceq 'hit' -and $warm.cache.dll_sha256 -ceq $cold.cache.dll_sha256)
Check 'worker_static_state_not_reused' ($warm.fresh_mutable_state -eq $true)
$active=Join-Path (Join-Path $directory $cold.cache.fingerprint) 'active.json'
$original=Get-Content -LiteralPath $active -Raw -Encoding UTF8|ConvertFrom-Json
$original|Add-Member -NotePropertyName source_sha256 -NotePropertyValue ('0'*64) -Force
[IO.File]::WriteAllText($active,($original|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
$identity=Run-Child 'wrong_source' $BridgeScript
Check 'wrong_source_identity_recompiled_not_loaded' ($identity.cache.state -ceq 'rebuilt' -and $identity.cache.rejection -ceq 'cache_identity_or_bytes_rejected')
$pointer=Get-Content -LiteralPath $active -Raw -Encoding UTF8|ConvertFrom-Json
$binary=Join-Path (Split-Path $active) $pointer.dll
[IO.File]::WriteAllBytes($binary,[byte[]]@(0,1,2,3))
$damaged=Run-Child 'damaged_dll' $BridgeScript
Check 'corrupt_dll_recompiled_before_loading' ($damaged.cache.state -ceq 'rebuilt' -and $damaged.cache.rejection -ceq 'cache_identity_or_bytes_rejected')
$repaired=Run-Child 'repaired_hit' $BridgeScript
Check 'repaired_generation_reusable' ($repaired.cache.state -ceq 'hit' -and $repaired.cache.dll_sha256 -ceq $damaged.cache.dll_sha256)
$pointer=Get-Content -LiteralPath $active -Raw -Encoding UTF8|ConvertFrom-Json
$pointer.environment.powershell='wrong-compiler-environment'
[IO.File]::WriteAllText($active,($pointer|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
$environment=Run-Child 'wrong_environment' $BridgeScript
Check 'compiler_environment_metadata_rejected' ($environment.cache.state -ceq 'rebuilt')
$pointer=Get-Content -LiteralPath $active -Raw -Encoding UTF8|ConvertFrom-Json
$pointer|Add-Member -NotePropertyName dll -NotePropertyValue '..\foreign.dll' -Force
[IO.File]::WriteAllText($active,($pointer|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
$redirect=Run-Child 'redirect' $BridgeScript
Check 'foreign_path_refused_and_rebuilt' ($redirect.cache.state -ceq 'rebuilt')
$changed=Join-Path $directory 'changed-bridge.ps1'
[IO.File]::WriteAllText($changed,([IO.File]::ReadAllText($BridgeScript)+"`r`n# owned cache fingerprint fixture`r`n"),[Text.UTF8Encoding]::new($false))
$other=Run-Child 'changed_source' $changed
Check 'bridge_change_uses_new_fingerprint' ($other.cache.state -ceq 'built' -and $other.cache.fingerprint -cne $cold.cache.fingerprint)
$unchanged=Run-Child 'original_still_hit' $BridgeScript
Check 'new_source_never_replaces_original_generation' ($unchanged.cache.state -ceq 'hit' -and $unchanged.cache.fingerprint -ceq $cold.cache.fingerprint)
Check 'rejected_manifest_and_binary_generations_retained' (@(Get-ChildItem -LiteralPath (Split-Path $active) -Filter '*.retained.json').Count -eq 4 -and
    @(Get-ChildItem -LiteralPath (Split-Path $active) -Filter '*.dll').Count -eq 5)
$receipt=@{contract='owned_exact_native_compilation_cache_fixture_v1';accepted=$true;
    observed_at_utc=[DateTime]::UtcNow.ToString('o');checks=$checks.ToArray();samples=$samples.ToArray();
    provider_queries_sent=0;tej_or_excel_connected=$false;credentials_read=$false;
    bridge_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant()}
[IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 6 -Compress),[Text.UTF8Encoding]::new($false))
Write-Output ('Native cache fixture passed '+$checks.Count+' checks; no provider query')
