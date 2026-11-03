<# Isolated Windows Forms fixture for the canonical large-axis read-only sample.
No TEJ/Excel/source connection, source selection or provider query. #>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$Output,
      [switch]$FullCapture,
      [switch]$IntentCapture,
      [switch]$FiniteLookup,
      [switch]$FiniteAxisCapture,
      [ValidateRange(2,2000)][int]$FiniteAxisLabels=2,
      [ValidateRange(150001,2000000)][int]$Count=150001)
$ErrorActionPreference='Stop'
if(Test-Path -LiteralPath $Output){throw 'New fixture receipt required'}
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
$native=[regex]::Match([IO.File]::ReadAllText($BridgeScript),"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $native.Success){throw 'Canonical native helper declaration required'}
Add-Type -TypeDefinition $native.Groups[1].Value -ReferencedAssemblies @(
    [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Windows.Forms;
public static class StockAgentLargeAxisFixture {
    [DllImport("user32.dll", CharSet=CharSet.Unicode, EntryPoint="SendMessageW")]
    static extern IntPtr SendText(IntPtr h, uint message, IntPtr w, string value);
    [DllImport("user32.dll", EntryPoint="SendMessageW")]
    static extern IntPtr SendValue(IntPtr h, uint message, IntPtr w, IntPtr value);
    class PassiveForm:Form {
        protected override bool ShowWithoutActivation {get{return true;}}
        protected override CreateParams CreateParams {get{var p=base.CreateParams;p.ExStyle|=0x08000000;return p;}}
    }
    static Form form;static ListBox list;static Thread thread;
    public static string Label(int i) {return "\u671f\ud83d\udcc8_"+i.ToString("D6");}
    public static long[] Open(int count) {
        var ready=new ManualResetEvent(false);Exception error=null;long[] handles=null;
        thread=new Thread(()=>{try {
            form=new PassiveForm {Text="StockAgent isolated large-axis fixture",Width=320,Height=180,ShowInTaskbar=false};
            list=new ListBox {Dock=DockStyle.Fill,Sorted=false};form.Controls.Add(list);
            // This fixture exercises native messages, not WinForms Items.
            // Seed the actual listbox directly so constructing the large test
            // does not repeatedly rebuild an unused managed collection.
            list.BeginUpdate();var handle=list.Handle;
            SendValue(handle,0x1A8u,new IntPtr(count),new IntPtr(count*32)); // LB_INITSTORAGE
            for(int i=0;i<count;i++)
                if(SendText(handle,0x180u,IntPtr.Zero,Label(i)).ToInt64()!=i)
                    throw new Exception("Native fixture insertion differs");
            list.EndUpdate();
            form.Shown+=(s,e)=>{handles=new long[]{form.Handle.ToInt64(),list.Handle.ToInt64()};ready.Set();};
            Application.Run(form);
        }catch(Exception e){error=e;ready.Set();}});
        thread.IsBackground=true;thread.SetApartmentState(ApartmentState.STA);thread.Start();
        if(!ready.WaitOne(90000))throw new Exception("Large-axis fixture startup timeout");
        ready.Dispose();if(error!=null)throw error;return handles;
    }
    public static void SetState(string state) {
        form.Invoke(new Action(()=>{
            list.Enabled=state!="disabled";list.Visible=state!="hidden";
            if(state=="small")SendValue(list.Handle,0x184u,IntPtr.Zero,IntPtr.Zero); // LB_RESETCONTENT
        }));
    }
    public static void Duplicate(int index) {form.Invoke(new Action(()=>{
        SendText(list.Handle,0x180u,IntPtr.Zero,Label(index));
    }));}
    public static void RemoveLast() {form.Invoke(new Action(()=>{
        int n=(int)SendValue(list.Handle,0x18Bu,IntPtr.Zero,IntPtr.Zero);
        SendValue(list.Handle,0x182u,new IntPtr(n-1),IntPtr.Zero); // LB_DELETESTRING
    }));}
    public static void Close() {
        if(form!=null&&!form.IsDisposed)form.Invoke(new Action(()=>form.Close()));
        if(thread!=null&&!thread.Join(10000))throw new Exception("Fixture did not close");
    }
}
'@ -ReferencedAssemblies @([Windows.Forms.SendKeys].Assembly.Location)
$checks=[Collections.Generic.List[object]]::new()
function Check([string]$name,[bool]$ok) {$checks.Add(@{name=$name;accepted=$ok});if(-not $ok){throw ('Fixture rejected: '+$name)}}
$clock=[Diagnostics.Stopwatch]::StartNew()
try {
    $handles=[StockAgentLargeAxisFixture]::Open($Count);$root=$handles[0];$list=$handles[1]
    $title='StockAgent isolated large-axis fixture';$count=$Count
    Check 'owned_list_and_exact_count' ([TejBridgeNative]::Message($list,0x18B,0,0) -eq $count)
    if($FiniteLookup) {
        $lookupClock=[Diagnostics.Stopwatch]::StartNew()
        $middle=[int][Math]::Floor($count/2);$last=$count-1
        $labels=[string[]]@([StockAgentLargeAxisFixture]::Label(0),
            [StockAgentLargeAxisFixture]::Label($middle),[StockAgentLargeAxisFixture]::Label($last))
        $found=[TejBridgeNative]::ExactListIndices($root,$list,$labels,$PID,$title)
        Check 'large_axis_finite_lookup_preserves_exact_unicode_and_order' (($found -join ',') -ceq ('0,'+$middle+','+$last))
        $rejected=$false
        try {[void][TejBridgeNative]::ExactListIndices($root,$list,
            [string[]]@('not-in-list',$labels[1]),$PID,$title)} catch {$rejected=$true}
        Check 'large_axis_absence_proof_is_bounded_and_complete' ($rejected -and
            ([TejBridgeNative]::LastAbsentLabelPositions -join ',') -ceq '0' -and
            [TejBridgeNative]::LastLookupListCount -eq $count)
        $rejected=$false
        try {[void][TejBridgeNative]::ExactListIndices($root,$list,$labels,($PID+1),$title)} catch {$rejected=$true}
        Check 'large_axis_foreign_owner_is_rejected' ($rejected -and
            [TejBridgeNative]::LastAbsentLabelPositions.Length -eq 0 -and
            [TejBridgeNative]::LastLookupListCount -eq -1)
        [StockAgentLargeAxisFixture]::Duplicate($middle)
        $rejected=$false
        try {[void][TejBridgeNative]::ExactListIndices($root,$list,[string[]]@($labels[1]),$PID,$title)} catch {$rejected=$true}
        Check 'large_axis_duplicate_exact_label_is_refused' $rejected
        [StockAgentLargeAxisFixture]::RemoveLast()
        Check 'large_axis_duplicate_fixture_restored' ([TejBridgeNative]::Message($list,0x18B,0,0) -eq $count)
        $rejected=$false
        try {[void][TejBridgeNative]::Items($list,$false)} catch {$rejected=$true}
        Check 'large_axis_full_catalog_guard_is_unchanged' $rejected
        $lookupClock.Stop();$finiteLookupSeconds=$lookupClock.Elapsed.TotalSeconds
    }
    $sampleClock=[Diagnostics.Stopwatch]::StartNew()
    $actual=@([TejBridgeNative]::SampleLargeCompanyAxis($root,$list,$PID,$title))
    $sampleSeconds=$sampleClock.Elapsed.TotalSeconds
    $anchors=@(0,([long][Math]::Floor($count/4)),([long][Math]::Floor($count/2)),([long][Math]::Floor(3*$count/4)),($count-4))
    $expected=@(foreach($anchor in $anchors){foreach($offset in 0..3){[StockAgentLargeAxisFixture]::Label($anchor+$offset)}})
    Check 'all_twenty_unicode_labels_match_native_list_order' (($actual -join "`n") -ceq ($expected -join "`n"))
    $pageClock=[Diagnostics.Stopwatch]::StartNew()
    $page=@([TejBridgeNative]::ReadLargeCompanyAxisPage($root,$list,$PID,$title,$count,0,2048))
    $pageSeconds=$pageClock.Elapsed.TotalSeconds
    $tail=@([TejBridgeNative]::ReadLargeCompanyAxisPage($root,$list,$PID,$title,$count,($count-2048),2048))
    $expectedPage=@(foreach($index in 0..2047){[StockAgentLargeAxisFixture]::Label($index)})
    $expectedTail=@(foreach($index in ($count-2048)..($count-1)){[StockAgentLargeAxisFixture]::Label($index)})
    Check 'first_and_last_full_pages_preserve_unicode_and_order' (
        ($page -join "`n") -ceq ($expectedPage -join "`n") -and ($tail -join "`n") -ceq ($expectedTail -join "`n"))
    if($FullCapture) {
        $snapshotPath=$Output+'.snapshot.bin'
        if(Test-Path -LiteralPath $snapshotPath){throw 'New isolated snapshot required'}
        $captureClock=[Diagnostics.Stopwatch]::StartNew()
        [TejBridgeNative]::CaptureLargeCompanyAxis($root,$list,$PID,$title,$count,$snapshotPath)
        $captureSeconds=$captureClock.Elapsed.TotalSeconds
        Check 'full_capture_nonempty_and_bounded' ((Get-Item -LiteralPath $snapshotPath).Length -gt 0 -and
            (Get-Item -LiteralPath $snapshotPath).Length -lt 256MB)
    }
    if($IntentCapture) {
        # Exercise the exact gated block from the canonical bridge against an
        # owned synthetic list. No source session or Preview is involved.
        $fixtureRoot=Join-Path ([IO.Path]::GetDirectoryName($Output)) 'intent-fixture'
        $intentResult=& {
            param($fixtureRoot,$root,$list,$title,$count,$BridgeScript)
            foreach($name in @('raw','requests','progress','large_axis_capture_intents/claimed')) {
                [void][IO.Directory]::CreateDirectory((Join-Path $fixtureRoot $name))
            }
            $task='a'*24;$attempt=$task+'-'+('b'*32);$priorAttempt=$task+'-'+('c'*32)
            $ownerRoot=$fixtureRoot;$intentDir=Join-Path $ownerRoot 'large_axis_capture_intents'
            $intentPath=Join-Path $intentDir ($task+'.json')
            $Output=Join-Path $ownerRoot ('raw/'+$attempt+'.json')
            $Request=Join-Path $ownerRoot ('requests/'+$attempt+'.json')
            $progressPath=Join-Path $ownerRoot ('progress/'+$attempt+'.json.progress.json')
            $ExpectedWindow=$root;$TejProcessId=$PID;$ExpectedTitle=$title;$availableCompanyCount=$count
            $companyLists=@($null,$null,[pscustomobject]@{Current=[pscustomobject]@{NativeWindowHandle=$list}})
            $requestDoc=[pscustomobject]@{task_id=$task;action='plan';type='Option (ALL)';smart_id='TEJ Derivatives';table='Fixture'}
            [IO.File]::WriteAllText($Request,($requestDoc|ConvertTo-Json -Compress),[Text.UTF8Encoding]::new($false))
            $requestSha=(Get-FileHash -LiteralPath $Request).Hash.ToLowerInvariant()
            $scope=@{binding_readback_verified=$true;vendor_notices_absent=$true}
            $priorName=$priorAttempt+'.json.company-axis-sample.json';$priorPath=Join-Path $ownerRoot ('raw/'+$priorName)
            $prior=@{contract='large_company_axis_diagnostic_only_v1';task_id=$task;table='Fixture';
                prepared_request_sha256=$requestSha;source_scope_proof=$scope;market_data_query_submitted=$false;full_axis_admitted=$false}
            [IO.File]::WriteAllText($priorPath,($prior|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
            $created=[DateTimeOffset]::UtcNow
            $intent=@{contract='tej_large_company_axis_capture_once_v1';task_id=$task;priority_request_id=('d'*64);
                type='Option (ALL)';smart_id='TEJ Derivatives';table='Fixture';prepared_request_sha256=$requestSha;
                bridge_script_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant();
                prior_sample_basename=$priorName;prior_sample_sha256=(Get-FileHash -LiteralPath $priorPath).Hash.ToLowerInvariant();
                expected_count=($count+1);created_at_utc=$created.ToString('o');expires_at_utc=$created.AddMinutes(12).ToString('o');
                capacity_checked_at_utc=$created.ToString('o');maximum_capture_bytes=268435456;
                capacity=@{state='admitted';available_bytes=1000000000;required_bytes=100000000;
                    backing_volume=@{physical_free_bytes=1000000000;required_bytes=100000000}};
                provider_queries_by_registration=0;market_data_query_authorized=$false}
            function Verify-EditableScope([switch]$BindingReadback) {
                if(-not $BindingReadback){throw 'Binding readback required'}
                return @{binding_readback_verified=$true;vendor_notices_absent=$true}
            }
            $tokens=$null;$errors=$null
            $ast=[Management.Automation.Language.Parser]::ParseFile($BridgeScript,[ref]$tokens,[ref]$errors)
            if($errors.Count){throw 'Canonical bridge parse failed'}
            $nodes=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.IfStatementAst] -and
                $n.Extent.Text.StartsWith('if(Test-Path -LiteralPath $intentPath)')},$true))
            if($nodes.Count -ne 1){throw 'One exact gated capture block required'}
            $probe=[scriptblock]::Create('$PSCommandPath=$BridgeScript;'+$nodes[0].Extent.Text)
            [IO.File]::WriteAllText($intentPath,($intent|ConvertTo-Json -Depth 8 -Compress),[Text.UTF8Encoding]::new($false))
            . $probe
            Check 'gated_capture_rejects_changed_count_without_claim' ((Test-Path -LiteralPath $intentPath) -and
                -not (Test-Path -LiteralPath (Join-Path $intentDir ('claimed/'+$attempt+'.json'))) -and
                -not (Test-Path -LiteralPath ($Output+'.company-axis.snapshot.bin')))
            $intent.expected_count=$count
            [IO.File]::WriteAllText($intentPath,($intent|ConvertTo-Json -Depth 8 -Compress),[Text.UTF8Encoding]::new($false))
            . $probe
            $binary=$Output+'.company-axis.snapshot.bin';$manifest=$Output+'.company-axis.snapshot.json'
            Check 'gated_capture_claims_once_and_publishes_full_source_proof' (
                -not (Test-Path -LiteralPath $intentPath) -and (Test-Path -LiteralPath $binary) -and
                (Test-Path -LiteralPath $manifest) -and (Test-Path -LiteralPath (Join-Path $intentDir ('claimed/'+$attempt+'.json'))))
            $proof=Get-Content -LiteralPath $manifest -Raw -Encoding UTF8|ConvertFrom-Json
            Check 'gated_capture_preserves_research_only_and_two_pass_boundary' (
                $proof.full_axis_admitted -eq $false -and $proof.market_data_query_submitted -eq $false -and
                $proof.complete_ordered_live_reads -eq 2 -and $proof.labels -eq $count -and
                $proof.binary_sha256 -ceq (Get-FileHash -LiteralPath $binary).Hash.ToLowerInvariant())
            return @{binary_path=$binary;manifest_path=$manifest;capture_seconds=$proof.capture_seconds}
        } $fixtureRoot $root $list $title $count $BridgeScript
    }
    if($FiniteAxisCapture) {
        # Run both exact one-shot blocks against the owned native list.  Date
        # menu readback is a local stub; the vendor date Search is not tested.
        $fixtureRoot=Join-Path ([IO.Path]::GetDirectoryName($Output)) 'finite-intent-fixture'
        $finiteResult=& {
            param($fixtureRoot,$root,$list,$title,$count,$BridgeScript,$FiniteAxisLabels)
            foreach($name in @('raw','requests','finite_axis_capture_intents/claimed')) {
                [void][IO.Directory]::CreateDirectory((Join-Path $fixtureRoot $name))
            }
            $task='e'*24;$attempt=$task+'-'+('f'*32)
            $finiteRoot=$fixtureRoot;$finiteIntentDir=Join-Path $fixtureRoot 'finite_axis_capture_intents'
            $finiteIntentPath=Join-Path $finiteIntentDir ($task+'.json')
            $Output=Join-Path $fixtureRoot ('raw/'+$attempt+'.json')
            $Request=Join-Path $fixtureRoot ('requests/'+$attempt+'.json')
            $ExpectedWindow=$root;$TejProcessId=$PID;$ExpectedTitle=$title;$availableCompanyCount=$count
            $companyLists=@($null,$null,[pscustomobject]@{Current=[pscustomobject]@{NativeWindowHandle=$list}})
            $requestDoc=[pscustomobject]@{task_id=$task;action='plan';type='Option (ALL)';
                smart_id='TEJ Derivatives';table='Fixture';frequency='daily';start='2011-01-01';end='2011-12-31'}
            $sourceKeyMode=2
            [IO.File]::WriteAllText($Request,($requestDoc|ConvertTo-Json -Compress),[Text.UTF8Encoding]::new($false))
            $created=[DateTimeOffset]::UtcNow
            $expectedIndices=@(for($i=0;$i -lt $FiniteAxisLabels;$i++) {
                [int][Math]::Floor($i*($count-1)/($FiniteAxisLabels-1))
            })
            $requestedLabels=@(foreach($index in $expectedIndices){[StockAgentLargeAxisFixture]::Label($index)})
            $intent=@{contract='tej_finite_company_date_axis_capture_once_v1';task_id=$task;
                priority_request_id=('d'*64);type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
                prepared_request_sha256=(Get-FileHash -LiteralPath $Request).Hash.ToLowerInvariant();
                bridge_script_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant();
                prior_snapshot_sha256=('a'*64);candidate_manifest_sha256=('b'*64);expected_count=($count+1);
                company_labels=$requestedLabels;
                date='2011-02-16';created_at_utc=$created.ToString('o');expires_at_utc=$created.AddMinutes(12).ToString('o');
                capacity_checked_at_utc=$created.ToString('o');capacity=@{state='admitted';available_bytes=5000000;
                    required_bytes=1000000;backing_volume=@{physical_free_bytes=5000000;required_bytes=1000000}};
                provider_queries_by_registration=0;market_data_query_authorized=$false}
            function Verify-EditableScope([switch]$BindingReadback) {
                if(-not $BindingReadback){throw 'Binding readback required'}
                return @{binding_readback_verified=$true;vendor_notices_absent=$true}
            }
            function Message([long]$handle,[int]$message) {return [TejBridgeNative]::Message($handle,$message,0,0)}
            function Items([long]$handle) {return ,[string[]]@('2011/02/16')}
            $tokens=$null;$errors=$null
            $ast=[Management.Automation.Language.Parser]::ParseFile($BridgeScript,[ref]$tokens,[ref]$errors)
            if($errors.Count){throw 'Candidate bridge parse failed'}
            $gates=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.IfStatementAst] -and
                $n.Extent.Text.StartsWith("if(`$requestDoc.action -ceq 'plan'") -and
                $n.Extent.Text.Contains('tej_finite_company_date_axis_capture_once_v1')},$true))
            $reads=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.IfStatementAst] -and
                $n.Extent.Text.StartsWith('if($null -ne $finiteAxisContext)') -and
                $n.Extent.Text.Contains('tej_finite_company_date_axis_observation_v1')},$true))
            if($gates.Count -ne 1 -or $reads.Count -ne 1){throw 'Exact finite capture blocks required'}
            $gate=[scriptblock]::Create('$PSCommandPath=$BridgeScript;'+$gates[0].Extent.Text)
            $read=[scriptblock]::Create('$PSCommandPath=$BridgeScript;'+$reads[0].Extent.Text)
            [IO.File]::WriteAllText($finiteIntentPath,($intent|ConvertTo-Json -Depth 8 -Compress),[Text.UTF8Encoding]::new($false))
            $finiteAxisContext=$null;$rejected=$false;try {. $gate} catch {$rejected=$true}
            Check 'finite_capture_rejects_changed_count_without_claim' ($rejected -and (Test-Path -LiteralPath $finiteIntentPath) -and
                -not (Test-Path -LiteralPath (Join-Path $finiteIntentDir ('claimed/'+$attempt+'.json'))))
            $intent.expected_count=$count;$intent.capacity.available_bytes=1000000
            [IO.File]::WriteAllText($finiteIntentPath,($intent|ConvertTo-Json -Depth 8 -Compress),[Text.UTF8Encoding]::new($false))
            $rejected=$false;try {. $gate} catch {$rejected=$true}
            Check 'finite_capture_rejects_insufficient_headroom' ($rejected -and (Test-Path -LiteralPath $finiteIntentPath))
            $intent.capacity.available_bytes=5000000;$intent.expires_at_utc=$created.AddSeconds(-1).ToString('o')
            [IO.File]::WriteAllText($finiteIntentPath,($intent|ConvertTo-Json -Depth 8 -Compress),[Text.UTF8Encoding]::new($false))
            $rejected=$false;try {. $gate} catch {$rejected=$true}
            Check 'finite_capture_rejects_expired_intent' ($rejected -and (Test-Path -LiteralPath $finiteIntentPath))
            $intent.expires_at_utc=$created.AddMinutes(12).ToString('o')
            [IO.File]::WriteAllText($finiteIntentPath,($intent|ConvertTo-Json -Depth 8 -Compress),[Text.UTF8Encoding]::new($false))
            $sourceKeyMode=1;$rejected=$false;try {. $gate} catch {$rejected=$true}
            Check 'finite_capture_rejects_non_dated_source_key' ($rejected -and (Test-Path -LiteralPath $finiteIntentPath))
            $sourceKeyMode=2
            . $gate
            Check 'finite_capture_claims_once_and_reads_exact_native_indices' (
                -not (Test-Path -LiteralPath $finiteIntentPath) -and
                (Test-Path -LiteralPath (Join-Path $finiteIntentDir ('claimed/'+$attempt+'.json'))) -and
                (($finiteAxisContext.indices -join ',') -ceq ($expectedIndices -join ',')))
            $dateStart='2011-02-16';$dateEnd='2011-02-16';$availableDates=@('2011/02/15')
            $dateLists=@([pscustomobject]@{Current=[pscustomobject]@{NativeWindowHandle=123}})
            $selectedFields=@('field one');$rejected=$false;try {. $read} catch {$rejected=$true}
            Check 'finite_capture_rejects_changed_date_readback' ($rejected -and
                -not (Test-Path -LiteralPath ($Output+'.finite-axis.json')))
            $availableDates=@('2011/02/16');$stopped=$false
            try {. $read} catch {$stopped=$_.Exception.Message.StartsWith('FINITE_AXIS_CAPTURE_COMPLETE_BEFORE_QUERY:')}
            $witness=$Output+'.finite-axis.json';$proof=Get-Content -LiteralPath $witness -Raw -Encoding UTF8|ConvertFrom-Json
            Check 'finite_capture_stops_before_query_and_keeps_partial_scope' ($stopped -and
                $proof.market_data_query_submitted -eq $false -and $proof.full_axis_admitted -eq $false -and
                $proof.complete_live_company_reads -eq 2 -and $proof.complete_date_reads -eq 2 -and
                $proof.source_key_mode -eq 2 -and
                ($proof.date_labels -join ',') -ceq '2011/02/16')
            return @{witness_path=$witness;date_search_ui_exercised=$false}
        } $fixtureRoot $root $list $title $count $BridgeScript $FiniteAxisLabels
    }
    foreach($bad in @(@($count,0,0),@($count,0,4097),@($count,-1,1),@($count,$count,1),@(($count-1),0,1))) {
        $rejected=$false;try{[void][TejBridgeNative]::ReadLargeCompanyAxisPage($root,$list,$PID,$title,$bad[0],$bad[1],$bad[2])}catch{$rejected=$true}
        Check 'reject_invalid_page_range_or_source_count' $rejected
    }
    foreach($fault in @('wrong_pid','wrong_title','wrong_control','disabled','hidden','small')) {
        [StockAgentLargeAxisFixture]::SetState($fault)
        $target=$list;$pidArg=$PID;$titleArg=$title
        if($fault -eq 'wrong_pid'){$pidArg++};if($fault -eq 'wrong_title'){$titleArg+=' other'}
        if($fault -eq 'wrong_control'){$target=$root}
        $rejected=$false;try{[void][TejBridgeNative]::SampleLargeCompanyAxis($root,$target,$pidArg,$titleArg)}catch{$rejected=$true}
        Check ("reject_"+$fault) $rejected
        if($fault -ne 'small'){[StockAgentLargeAxisFixture]::SetState('normal')}
    }
    $receipt=@{contract='large_company_axis_native_owned_fixture_v1';accepted=$true;checks=$checks;
        native_list_count=$count;sample_labels=20;sample_seconds=$sampleSeconds;
        page_labels=2048;page_seconds=$pageSeconds;total_seconds=$clock.Elapsed.TotalSeconds;
        full_capture_requested=[bool]$FullCapture;full_capture_seconds=$captureSeconds;
        finite_lookup_requested=[bool]$FiniteLookup;finite_lookup_fixture_seconds=$finiteLookupSeconds;
        finite_axis_capture_requested=[bool]$FiniteAxisCapture;finite_axis_capture=$finiteResult;
        finite_axis_requested_labels=$FiniteAxisLabels;
        full_capture_path=$snapshotPath;
        intent_capture_requested=[bool]$IntentCapture;intent_capture=$intentResult;
        bridge_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant();
        provider_queries_sent=0;tej_or_excel_connected=$false;full_axis_admitted=$false;
        observed_at_utc=[DateTime]::UtcNow.ToString('o')}
    [IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
} finally {[StockAgentLargeAxisFixture]::Close()}
