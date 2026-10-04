<# Same-owned-source AB/BA control resolution benchmark, strictly no selection.
The functions come from the baseline/current canonical bridge. Only the final
BM_CLICK is removed; all current per-click ownership/state/geometry checks run.
No data query, focus, keys, clipboard, source adoption or workbook write.
#>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$BaselineScript,
      [Parameter(Mandatory=$true)][string]$Session,
      [Parameter(Mandatory=$true)][string]$Prepared,
      [Parameter(Mandatory=$true)][string]$Output,
      [int]$Iterations=24)
$ErrorActionPreference='Stop'
if(Test-Path -LiteralPath $Output){throw 'Retain original benchmark evidence'}
if($Iterations -lt 1 -or $Iterations -gt 32){throw 'Bounded operation benchmark required'}
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
$bridgeHash=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant()
$baselineHash=(Get-FileHash -LiteralPath $BaselineScript).Hash.ToLowerInvariant()
function Parse([string]$path) {
    $tokens=$null;$errors=$null
    $ast=[Management.Automation.Language.Parser]::ParseFile($path,[ref]$tokens,[ref]$errors)
    if($errors.Count -ne 0){throw 'Canonical script syntax error'};return $ast
}
$current=Parse $BridgeScript;$baseline=Parse $BaselineScript
function Definition($ast,[string]$name) {
    $definitions=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name},$true))
    if($definitions.Count -ne 1){throw 'Unique canonical function required'};return $definitions[0].Extent.Text
}
function GuardDefinition([string]$definition,[string]$action) {
    if($definition.Split(@($action),[StringSplitOptions]::None).Count -ne 2){throw 'Exactly one reviewed selection action required'}
    $definition=$definition.Replace($action,'')
    if($definition -match '0xF5|SelectListItem|PostMessage|Select-Combo|Activate\('){throw 'Read-only guard contains an action'}
    return $definition
}
$native=[regex]::Match([IO.File]::ReadAllText($BridgeScript),"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $native.Success){throw 'Canonical native helper declaration required'}
Add-Type -TypeDefinition $native.Groups[1].Value -ReferencedAssemblies @(
    [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)
$oldNative=[regex]::Match([IO.File]::ReadAllText($BaselineScript),"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $oldNative.Success){throw 'Original native declaration required'}
Add-Type -TypeDefinition ([regex]::Replace($oldNative.Groups[1].Value,'\bTejBridgeNative\b','TejBridgeBaseline')) -ReferencedAssemblies @(
    [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)
$pin=Get-Content -LiteralPath $Session -Raw -Encoding UTF8|ConvertFrom-Json
$request=Get-Content -LiteralPath $Prepared -Raw -Encoding UTF8|ConvertFrom-Json
$ExpectedWindow=[long]$pin.ExpectedWindow;$TejProcessId=[int]$pin.TejProcessId;$ExpectedTitle=[string]$pin.ExpectedTitle
$root=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$ExpectedWindow)
$mutex=[Threading.Mutex]::new($false,'Local\StockAgentTEJSmartWizardOwner');$owns=$false
$samples=[Collections.Generic.List[object]]::new();$checks=[Collections.Generic.List[object]]::new();$failure=$null
function Check([string]$name,[bool]$ok) {$checks.Add(@{name=$name;accepted=$ok});if(-not $ok){throw ('Operation benchmark failed: '+$name)}}
try {
    $owns=$mutex.WaitOne(0);if(-not $owns){throw 'Another desktop owner prevents benchmarking'}
    foreach($name in @('Assert-Scope','Find-QueryGroup','Native-Controls','Control','Message','Items','Lists','Company-SelectButton')) {
        Invoke-Expression (Definition $current $name)
    }
    Invoke-Expression (GuardDefinition (Definition $current 'Click-ResolvedButton') '[void](Message $h 0xF5)')
    Invoke-Expression (Definition $current 'Select-Company')
    $old=GuardDefinition (Definition $baseline 'Select-Company') '[void](Message $buttons[0].Current.NativeWindowHandle 0xF5)'
    Invoke-Expression ($old.Replace('function Select-Company','function Resolve-CompanyBaseline'))
    Assert-Scope $ExpectedWindow
    Check 'original_owned_root_enabled' ($root.Current.IsEnabled -and [TejBridgeNative]::Dialogs($ExpectedWindow).Count -eq 0)
    $source=Find-QueryGroup 'Data Source';$company=Find-QueryGroup 'Company Setting';$dates=Find-QueryGroup 'Date Setting'
    $companyLists=Lists $company
    if($companyLists.Count -ne 4){throw 'Original four-list company layout required'}
    foreach($pair in @(@('Type',$request.type),@('SmartID',$request.smart_id),@('Data',$request.table))) {
        $h=Control $source $pair[0] '*COMBOBOX*';$labels=Items $h $true
        Check ('exact_'+$pair[0]+'_binding') ($labels[(Message $h 0x147)] -ceq $pair[1])
    }
    function Fingerprint {
        $state=@(foreach($group in @($source,$company,$dates)) {
            foreach($list in (Lists $group)){@($list.Current.NativeWindowHandle,@(Items $list.Current.NativeWindowHandle))}
        })
        $bytes=[Text.Encoding]::UTF8.GetBytes(($state|ConvertTo-Json -Depth 6 -Compress))
        $sha=[Security.Cryptography.SHA256]::Create()
        try {return [BitConverter]::ToString($sha.ComputeHash($bytes)).Replace('-','').ToLowerInvariant()}finally{$sha.Dispose()}
    }
    $before=Fingerprint;$foreground=[TejBridgeNative]::GetForegroundWindow()
    # Real simultaneous Windows processes, checking admission before any UI.
    $contention=$Output+'.contention.json'
    $child='$m=[Threading.Mutex]::new($false,''Local\StockAgentTEJSmartWizardOwner'');$a=$m.WaitOne(0);try{[IO.File]::WriteAllText('''+
        $contention.Replace("'","''")+''',(@{owner_acquired=$a;provider_queries_sent=0;ui_operations_sent=0}|ConvertTo-Json -Compress))}finally{if($a){$m.ReleaseMutex()};$m.Dispose()}'
    $encoded=[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($child))
    $contender=Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -ArgumentList @('-NoProfile','-NonInteractive','-EncodedCommand',$encoded) -PassThru -WindowStyle Hidden
    if(-not $contender.WaitForExit(10000)){throw 'Parallel owner probe deadline exceeded'}
    $competing=Get-Content -LiteralPath $contention -Raw|ConvertFrom-Json
    Check 'simultaneous_second_windows_worker_refused_before_ui' ($competing.owner_acquired -eq $false -and $competing.ui_operations_sent -eq 0)
    foreach($order in @(@('baseline','resolved'),@('resolved','baseline'))) {
        foreach($mode in $order) {
            # Baseline/current native control enumeration is identical; use
            # each exact root-identity implementation for the full comparison.
            if($mode -eq 'baseline'){Invoke-Expression (Definition $baseline 'Assert-Scope')}
            else{Invoke-Expression (Definition $current 'Assert-Scope')}
            $clock=[Diagnostics.Stopwatch]::StartNew()
            if($mode -eq 'resolved') {
                $companyButton=Company-SelectButton;$fieldButton=Control $source 'Select' '*BUTTON*';$dateButton=Control $dates 'Select' '*BUTTON*'
            }
            for($i=0;$i -lt $Iterations;$i++) {
                if($mode -eq 'baseline') {
                    Resolve-CompanyBaseline
                    [void](Control $source 'Select' '*BUTTON*');[void](Control $dates 'Select' '*BUTTON*')
                }else {
                    Select-Company $companyButton
                    Click-ResolvedButton $source $fieldButton 'Select';Click-ResolvedButton $dates $dateButton 'Select'
                }
            }
            if($mode -eq 'resolved') {
                Check 'company_button_stable' ((Company-SelectButton) -eq $companyButton)
                Check 'field_button_stable' ((Control $source 'Select' '*BUTTON*') -eq $fieldButton)
                Check 'date_button_stable' ((Control $dates 'Select' '*BUTTON*') -eq $dateButton)
            }
            $clock.Stop();$samples.Add(@{mode=$mode;seconds=$clock.Elapsed.TotalSeconds;iterations=$Iterations;controls_per_iteration=3})
        }
    }
    $wrongGroupRejected=$false;try{Click-ResolvedButton $dates $fieldButton 'Select'}catch{$wrongGroupRejected=$true}
    Check 'resolved_button_wrong_parent_refused' $wrongGroupRejected
    $wrongNameRejected=$false;try{Click-ResolvedButton $source $fieldButton 'Different'}catch{$wrongNameRejected=$true}
    Check 'resolved_button_wrong_name_refused' $wrongNameRejected
    $comboHandles=@(foreach($name in @('Type','SmartID','Data')){Control $source $name '*COMBOBOX*'})
    $counts=@(foreach($h in $comboHandles){Message $h 0x146})
    foreach($order in @(@('catalog_baseline','selected_exact'),@('selected_exact','catalog_baseline'))) {
        foreach($mode in $order) {
            $clock=[Diagnostics.Stopwatch]::StartNew()
            foreach($h in $comboHandles) {
                $index=[TejBridgeNative]::Message($h,0x147,0,0)
                if($mode -eq 'catalog_baseline'){
                    $labels=[TejBridgeBaseline]::Items($h,$true);$value=$labels[$index]
                    $found=[Array]::IndexOf($labels,$value)
                }else{
                    $value=[TejBridgeNative]::SelectedComboText($ExpectedWindow,$h)
                    $found=[TejBridgeNative]::ExactComboIndex($ExpectedWindow,$h,$value)
                }
                Check 'selected_combo_and_unique_lookup_same_index' ($found -eq $index)
            }
            $clock.Stop();$samples.Add(@{mode=$mode;seconds=$clock.Elapsed.TotalSeconds;iterations=1;
                controls_per_iteration=3;catalog_items_total=($counts|Measure-Object -Sum).Sum})
        }
    }
    $dateLists=Lists $dates
    $allCompanies=$request.company_labels.Count -eq (Message $companyLists[2].Current.NativeWindowHandle 0x18B)
    $allDates=$request.date_labels.Count -eq (Message $dateLists[0].Current.NativeWindowHandle 0x18B)
    if($allCompanies -and $allDates) {
        foreach($order in @(@('redundant_all_lookup','verified_all_shortcut'),@('verified_all_shortcut','redundant_all_lookup'))) {
            foreach($mode in $order) {
                $clock=[Diagnostics.Stopwatch]::StartNew()
                if($mode -eq 'redundant_all_lookup') {
                    foreach($label in $request.company_labels){[void][TejBridgeBaseline]::ExactListIndex($ExpectedWindow,$companyLists[2].Current.NativeWindowHandle,$label)}
                    foreach($label in $request.date_labels){[void][TejBridgeBaseline]::ExactListIndex($ExpectedWindow,$dateLists[0].Current.NativeWindowHandle,$label)}
                }
                # BOTH paths retain full destination label verification. No
                # action/query is sent for this comparison of existing state.
                Check 'all_company_names_still_exact' ([TejBridgeNative]::SameItems(
                    [string[]](Items $companyLists[3].Current.NativeWindowHandle),[string[]]$request.company_labels))
                Check 'all_date_names_still_exact' ([TejBridgeNative]::SameItems(
                    [string[]](Items $dateLists[1].Current.NativeWindowHandle),[string[]]$request.date_labels))
                $clock.Stop();$samples.Add(@{mode=$mode;seconds=$clock.Elapsed.TotalSeconds;iterations=1;
                    requested_companies=$request.company_labels.Count;requested_dates=$request.date_labels.Count})
            }
        }
    }
    Check 'source_schema_and_axes_unchanged' ((Fingerprint) -ceq $before)
    Check 'foreground_unchanged' ([TejBridgeNative]::GetForegroundWindow() -eq $foreground)
    Check 'canonical_script_unchanged_during_comparison' ((Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant() -ceq $bridgeHash)
    Check 'baseline_script_unchanged_during_comparison' ((Get-FileHash -LiteralPath $BaselineScript).Hash.ToLowerInvariant() -ceq $baselineHash)
}catch{$failure=$_.Exception.Message}finally{if($owns){$mutex.ReleaseMutex()};$mutex.Dispose()}
$receipt=@{contract='same_owned_controls_readonly_ab_ba_v1';accepted=($null -eq $failure);failure=$failure;
    observed_at_utc=[DateTime]::UtcNow.ToString('o');checks=$checks.ToArray();samples=$samples.ToArray();
    provider_queries_sent=0;ui_operations_sent=0;source_values_exposed=$false;queue_modified=$false;
    bridge_sha256=$bridgeHash;baseline_sha256=$baselineHash}
[IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 6 -Compress),[Text.UTF8Encoding]::new($false))
if($failure){throw $failure}
