<# Cache CODE in an owned fixture only; no TEJ/Excel/source connection. #>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$Output,
      [string]$CacheDirectory,[string]$ChildOutput,[switch]$Child,
      [switch]$FingerprintProfile,[string]$BaselineBridgeScript,
      [switch]$PreparationClassification)
$ErrorActionPreference='Stop'
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
if($PreparationClassification) {
    # Execute the exact pure production classifier without loading TEJ/Excel
    # or any native helper. The script's catch still owns the Preview boundary.
    if($Child -or $FingerprintProfile -or (Test-Path -LiteralPath $Output)){throw 'New isolated classifier receipt required'}
    $tokens=$null;$errors=$null
    $ast=[Management.Automation.Language.Parser]::ParseFile($BridgeScript,[ref]$tokens,[ref]$errors)
    if($errors.Count -ne 0){throw 'Canonical bridge syntax invalid'}
    $definitions=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -ceq 'Get-TejPreparationFailureCode'},$true))
    if($definitions.Count -ne 1){throw 'Unique production preparation classifier required'}
    Invoke-Expression $definitions[0].Extent.Text
    $cases=@(
        @('SOURCE_BINDING_BEFORE_QUERY: unavailable',$false,'local_source_binding_failed_before_preview'),
        @('DESKTOP_CONTEXT_BEFORE_QUERY: unavailable',$false,'desktop_context_unavailable_before_preview'),
        @('DATE_INPUT_BEFORE_QUERY: Foreground unavailable; no input sent',$true,'desktop_foreground_unavailable_before_preview'),
        @('DATE_INPUT_BEFORE_QUERY: DATE_FOCUS_AVAILABILITY_BEFORE_INPUT:',$true,'desktop_foreground_unavailable_before_preview'),
        @('DATE_INPUT_BEFORE_QUERY: digit acknowledgement failed',$true,'local_date_input_failed_before_preview'),
        @('LOCAL_SELECTION_BEFORE_QUERY: requested source label absent; complete batch verified',$true,'local_list_selection_failed_before_preview'),
        @('LOCAL_SELECTION_BEFORE_QUERY: requested source label absent; complete batch verified',$false,'local_query_preparation_failed_before_preview'),
        @('QUERY_ACTIVATION_BEFORE_QUERY: refused',$true,'local_query_activation_failed_before_preview'),
        @('QUERY_ACTIVATION_BEFORE_QUERY: refused',$false,'local_query_preparation_failed_before_preview'),
        @('Vendor notice/disabled root blocks source selection',$false,'local_query_preparation_failed_before_preview'),
        @('Unrecognized exception',$true,'local_query_preparation_failed_before_preview'),
        @('',$false,'local_query_preparation_failed_before_preview'))
    foreach($case in $cases) {
        if((Get-TejPreparationFailureCode $case[0] $case[1]) -cne $case[2]){throw 'Preparation failure classification differs'}
    }
    $receipt=@{contract='tej_native_preparation_failure_classification_v1';accepted=$true;checks=$cases.Count;
        observed_at_utc=[DateTime]::UtcNow.ToString('o');provider_queries_sent=0;desktop_actions_sent=0;
        tej_or_excel_connected=$false;bridge_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant()}
    [IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 4 -Compress),[Text.UTF8Encoding]::new($false))
    Write-Output 'Exact native preparation classifier passed; no provider query';exit
}
if($FingerprintProfile) {
    # Run the EXACT nested file digest from each retained bridge, not a copied
    # benchmark implementation. ABBA order reduces warm-cache/order bias. No
    # TEJ object, window, source query or production helper cache is touched.
    if($Child -or -not $BaselineBridgeScript -or (Test-Path -LiteralPath $Output)) {
        throw 'A retained comparator and new fingerprint profile output required'
    }
    $functions=@{}
    foreach($entry in @(@{name='baseline';path=$BaselineBridgeScript},@{name='candidate';path=$BridgeScript})) {
        $tokens=$null;$errors=$null
        $ast=[Management.Automation.Language.Parser]::ParseFile($entry.path,[ref]$tokens,[ref]$errors)
        if($errors.Count -ne 0){throw 'Fingerprint comparator syntax invalid'}
        $definitions=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Digest-File'},$true))
        if($definitions.Count -ne 1){throw 'Unique exact file fingerprint function required'}
        $functions[$entry.name]=[scriptblock]::Create($definitions[0].Extent.Text+"`n"+'Digest-File -path $args[0]')
    }
    $files=@((Join-Path ([Runtime.InteropServices.RuntimeEnvironment]::GetRuntimeDirectory()) 'csc.exe'),
        [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location,
        [string].Assembly.Location,[Uri].Assembly.Location,[Linq.Enumerable].Assembly.Location)
    $samples=[Collections.Generic.List[object]]::new();$digests=@{}
    foreach($variant in @('baseline','candidate','candidate','baseline')) {
        foreach($path in $files) {
            $info=Get-Item -LiteralPath $path
            $clock=[Diagnostics.Stopwatch]::StartNew()
            $sha=& $functions[$variant] $path
            $seconds=$clock.Elapsed.TotalSeconds
            if($sha -cnotmatch '^[0-9a-f]{64}$'){throw 'Exact byte SHA256 required'}
            if($digests.ContainsKey($path) -and $digests[$path] -cne $sha){throw 'Comparator file digest differs'}
            $digests[$path]=$sha
            $samples.Add(@{variant=$variant;file=$info.Name;bytes=$info.Length;sha256=$sha;seconds=$seconds})
        }
    }
    $receipt=@{contract='tej_exact_runtime_fingerprint_abba_profile_v1';accepted=$true;
        observed_at_utc=[DateTime]::UtcNow.ToString('o');samples=$samples.ToArray();order=@('baseline','candidate','candidate','baseline');
        all_file_digests_equal=$true;provider_queries_sent=0;tej_or_excel_connected=$false;credentials_read=$false;
        baseline_bridge_sha256=(Get-FileHash -LiteralPath $BaselineBridgeScript).Hash.ToLowerInvariant();
        candidate_bridge_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant();
        interpretation='Same runtime bytes, same process, balanced warm-cache comparison; not full-chain speed or a cold boot'}
    [IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 6 -Compress),[Text.UTF8Encoding]::new($false))
    Write-Output 'Exact runtime fingerprint ABBA profile passed; no provider query'
    exit
}
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
    # The real metadata-only progress writer: fixed phases, exact attempt,
    # transitions and failed publication, without invoking any desktop action.
    $phasePath=Join-Path $CacheDirectory ('scope-'+[Guid]::NewGuid().ToString('N')+'.json.progress.json')
    $task='a'*24;$attempt=$task+'-'+'b'*32
    $phases=@('context_and_source_binding','field_selection','company_universe_and_lookup','date_model_and_lookup',
        'company_selection_and_verification','date_selection_and_verification','final_source_and_preview_guards')
    $phaseClock=[Diagnostics.Stopwatch]::StartNew()
    foreach($phase in $phases) {
        [TejBridgeNative]::WriteScopeProgress($phasePath,$task,$attempt,$phase)
        $progress=Get-Content -LiteralPath $phasePath -Raw -Encoding UTF8|ConvertFrom-Json
        if($progress.scope_phase -cne $phase -or $progress.stage -cne 'preparing_scope' -or
           $progress.task_id -cne $task -or $progress.attempt_id -cne $attempt -or
           $null -ne $progress.scanned_row_slots){throw 'Bounded scope progress differs'}
    }
    $phaseSeconds=$phaseClock.Elapsed.TotalSeconds
    $originalPhase=[IO.File]::ReadAllText($phasePath)
    [TejBridgeNative]::WriteScopeProgress($phasePath,$task,$attempt,'SECRET')
    [TejBridgeNative]::WriteScopeProgress($phasePath,$task,'foreign','field_selection')
    if([IO.File]::ReadAllText($phasePath) -cne $originalPhase){throw 'Unreviewed scope progress was published'}
    [TejBridgeNative]::WriteProgress($phasePath,$task,$attempt,'awaiting_preview',-1,-1)
    $waiting=Get-Content -LiteralPath $phasePath -Raw -Encoding UTF8|ConvertFrom-Json
    if($waiting.stage -cne 'awaiting_preview' -or $null -ne $waiting.scope_phase){throw 'Old scope phase survived source wait'}
    $heldPhase=[IO.File]::Open($phasePath,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::None)
    try{[TejBridgeNative]::WriteScopeProgress($phasePath,$task,$attempt,'field_selection')}finally{$heldPhase.Dispose()}
    if(([IO.File]::ReadAllText($phasePath)|ConvertFrom-Json).stage -cne 'awaiting_preview'){
        throw 'Optional progress failure replaced a locked original'
    }
    # Exercise the exact production writer on NTFS AND the WSL UNC output.
    # Exclusively lock the final file to inject a publication failure without
    # interacting with any TEJ window or sending a provider request.
    $publication=[Collections.Generic.List[object]]::new()
    foreach($target in @((Join-Path $CacheDirectory ('publication-'+[Guid]::NewGuid().ToString('N')+'.json')),
                         ($Output+'.publication-'+[Guid]::NewGuid().ToString('N')+'.json'))) {
        $original='{"state":"original","value":"台積電"}'
        $next='{"state":"replacement","value":"台灣/α"}'
        [TejBridgeNative]::WriteArtifact($target,$original)
        $held=[IO.File]::Open($target,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::None)
        $refused=$false;$failureClock=[Diagnostics.Stopwatch]::StartNew()
        try{[TejBridgeNative]::WriteArtifact($target,$next)}catch{$refused=$true}finally{$held.Dispose()}
        $failureSeconds=$failureClock.Elapsed.TotalSeconds
        if(-not $refused -or [IO.File]::ReadAllText($target) -cne $original -or $failureSeconds -gt 5){
            throw 'Atomic publication did not preserve locked original with bounded failure'
        }
        $writeClock=[Diagnostics.Stopwatch]::StartNew()
        [TejBridgeNative]::WriteArtifact($target,$next)
        $writeSeconds=$writeClock.Elapsed.TotalSeconds
        if([IO.File]::ReadAllText($target) -cne $next -or
           @(Get-ChildItem -LiteralPath (Split-Path $target) -Filter ((Split-Path $target -Leaf)+'.*.tmp')).Count -ne 0){
            throw 'Atomic UTF8 publication or own-temporary cleanup differs'
        }
        $publication.Add(@{filesystem=$(if($target.StartsWith('\\')){'wsl_unc'}else{'ntfs'});
            locked_original_preserved=$true;bounded_failure_seconds=$failureSeconds;
            successful_write_seconds=$writeSeconds;utf8_equal=$true;temporary_files_retained=0})
    }
    [IO.File]::WriteAllText($ChildOutput,(@{cache=$cache;seconds=$seconds;native_semantics_equal=$true;
        fresh_mutable_state=$true;artifact_publication=$publication.ToArray();
        scope_progress=@{phases=$phases.Count;accepted=$true;publication_failure_did_not_fail_query=$true;
            phase_writes_and_readback_seconds=$phaseSeconds};
        provider_queries_sent=0}|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
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
        artifact_publication=$sample.artifact_publication;scope_progress=$sample.scope_progress;
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
