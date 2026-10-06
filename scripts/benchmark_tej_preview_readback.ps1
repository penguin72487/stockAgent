<#
Compare complete bounded readbacks of the SAME stable grid. No query, clipboard,
keyboard, focus change, Excel write, queue adoption or permission acknowledgement.
Fixture mode owns a new non-activating software form; source mode requires the
canonical collector lock to be held by the Python launcher and exact session pins.
Only hashes, counts and timings leave this process, never source financial values.
#>
param(
    [Parameter(Mandatory=$true)][string]$BridgeScript,
    [Parameter(Mandatory=$true)][string]$BaselineScript,
    [Parameter(Mandatory=$true)][string]$Output,
    [string]$Session,
    [string]$Prepared,
    [string]$SourceResult,
    [switch]$Fixture,
    [int]$FixtureRows=1000,
    [int]$FixtureColumns=30,
    [int]$Rounds=2,
    [switch]$CheckStability
)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if(Test-Path -LiteralPath $Output){throw 'Retain the original benchmark evidence'}
if($Rounds -lt 1 -or $Rounds -gt 3){throw 'Unreviewed repetition bound'}
if($CheckStability -and -not $Fixture){throw 'Mutation checks may only modify the owned fixture'}
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
foreach($entry in @(@($BaselineScript,'TejBridgeBaseline'),@($BridgeScript,'TejBridgeNative'))) {
    $text=[IO.File]::ReadAllText($entry[0])
    $match=[regex]::Match($text,"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
    if(-not $match.Success){throw 'Canonical native declaration missing'}
    $code=[regex]::Replace($match.Groups[1].Value,'\bTejBridgeNative\b',$entry[1])
    Add-Type -TypeDefinition $code -ReferencedAssemblies @(
        [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)
}
Add-Type -TypeDefinition @'
using System;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using System.Security.Cryptography;
public static class TejReadbackBenchmark {
    private class PassiveForm:Form {
        protected override bool ShowWithoutActivation {get{return true;}}
        protected override CreateParams CreateParams {get{var p=base.CreateParams;p.ExStyle|=0x08000000;return p;}}
    }
    private static Form form;
    private static DataGridView grid;
    private static Thread thread;
    public static long Open(int rows,int columns) {
        var ready=new ManualResetEvent(false);Exception error=null;long handle=0;
        thread=new Thread(()=> {
            try {
                form=new PassiveForm {Text="StockAgent owned readback benchmark",Width=540,Height=350};
                grid=new DataGridView {Dock=DockStyle.Fill,AllowUserToAddRows=true};
                for(int c=0;c<columns;c++)grid.Columns.Add("c"+c,"Raw_"+c);
                for(int r=0;r<rows;r++) {
                    var cells=new object[columns];
                    for(int c=0;c<columns;c++) {
                        // Keep zero, signs, null, unicode, quoted strings and
                        // more-than-double precision distinct, without math.
                        string[] values={"0","-0.0001",null,"台灣 café 😀","1,234.50","\"literal\"","1234567890123456789.001"};
                        cells[c]=c==0?"ID"+r:values[(r+c)%values.Length];
                    }
                    grid.Rows.Add(cells);
                }
                form.Controls.Add(grid);form.Shown+=(s,e)=>{handle=grid.Handle.ToInt64();ready.Set();};
                Application.Run(form);
            }catch(Exception e){error=e;ready.Set();}
        });
        thread.IsBackground=true;thread.SetApartmentState(ApartmentState.STA);thread.Start();
        if(!ready.WaitOne(30000))throw new Exception("Owned fixture startup timed out");
        ready.Dispose();if(error!=null)throw error;return handle;
    }
    public static void Close() {
        if(form!=null&&!form.IsDisposed)form.Invoke(new Action(()=>form.Close()));
        if(thread!=null&&!thread.Join(5000))throw new Exception("Owned fixture did not close");
    }
    public static void ChangeFixtureRows(bool add) {
        if(form==null||form.IsDisposed)throw new Exception("Owned fixture is not open");
        form.Invoke(new Action(()=> {
            if(add)grid.Rows.Add(new object[]{"OWNED_EXTRA_ROW"});
            else grid.Rows.RemoveAt(grid.Rows.Count-2);
        }));
    }
    public static void ChangeFixtureColumns(bool add) {
        if(form==null||form.IsDisposed)throw new Exception("Owned fixture is not open");
        form.Invoke(new Action(()=> {
            if(add) {
                grid.Columns.Add("owned_extra1","OWNED_EXTRA_1");
                grid.Columns.Add("owned_extra2","OWNED_EXTRA_2");
            }else {
                grid.Columns.Remove("owned_extra2");grid.Columns.Remove("owned_extra1");
            }
        }));
    }
    public static string Digest(object[][] rows) {
        using(var sha=SHA256.Create()) {
            Action<string> append=(s)=>{var bytes=Encoding.UTF8.GetBytes(s);sha.TransformBlock(bytes,0,bytes.Length,bytes,0);};
            append(rows.Length+":");
            foreach(var row in rows) {
                append(row.Length+":");
                foreach(var value in row) {
                    if(value==null){append("null;");continue;}
                    if(!(value is string))throw new Exception("Non-string source value");
                    string text=(string)value;append("s"+text.Length+":"+text+";");
                }
            }
            sha.TransformFinalBlock(new byte[0],0,0);
            return BitConverter.ToString(sha.Hash).Replace("-","").ToLowerInvariant();
        }
    }
    public static bool Same(object[][] a,object[][] b) {
        if(a.Length!=b.Length)return false;
        for(int r=0;r<a.Length;r++) {
            if(a[r].Length!=b[r].Length)return false;
            for(int c=0;c<a[r].Length;c++)if(!Object.Equals(a[r][c],b[r][c]))return false;
        }
        return true;
    }
}
'@ -ReferencedAssemblies @([Windows.Forms.Form].Assembly.Location)

$opened=$false;$records=[Collections.Generic.List[object]]::new();$reference=$null
$beforeForeground=[TejBridgeNative]::GetForegroundWindow().ToInt64()
$mutex=[Threading.Mutex]::new($false,'Local\StockAgentTEJSmartWizardOwner');$owns=$false
try {
    if($Fixture) {
        if($Session -or $Prepared -or $FixtureRows -lt 1 -or $FixtureRows -gt 3000 -or $FixtureColumns -lt 3 -or $FixtureColumns -gt 30){throw 'Unreviewed fixture scope'}
        $grid=[TejReadbackBenchmark]::Open($FixtureRows,$FixtureColumns);$opened=$true
        $maxRows=$FixtureRows+1;$columns=$FixtureColumns;$basis='owned_software_fixture_not_TEJ_source'
    } else {
        if(-not $Session -or -not $Prepared -or -not $SourceResult){throw 'Exact session, prepared request and saved result required'}
        $owns=$mutex.WaitOne(0);if(-not $owns){throw 'Canonical desktop owner is active'}
        $sessionDoc=[IO.File]::ReadAllText($Session)|ConvertFrom-Json
        $request=[IO.File]::ReadAllText($Prepared)|ConvertFrom-Json
        $ownerPid=0;$root=[long]$sessionDoc.ExpectedWindow
        [void][TejBridgeNative]::GetWindowThreadProcessId([IntPtr]$root,[ref]$ownerPid)
        if($ownerPid -ne $sessionDoc.TejProcessId -or
           [TejBridgeNative]::WindowTitle($root) -cne $sessionDoc.ExpectedTitle -or
           -not [TejBridgeNative]::IsWindowEnabled([IntPtr]$root) -or
           [TejBridgeNative]::Dialogs($root).Count -ne 0){throw 'Exact idle query scope unavailable'}
        $tabs=[TejBridgeNative]::Children($root,'WindowsForms10.SysTabControl32.')
        if($tabs.Count -ne 1 -or [TejBridgeNative]::Message($tabs[0],0x130B,0,0) -ne 3){throw 'Only an already visible completed Preview can be benchmarked'}
        $keysCount=if($request.source_key_mode -eq 1){1}elseif($request.source_key_mode -eq 3){3}else{2}
        $maxRows=[int]$request.max_rows;$columns=$request.fields.Count+$keysCount
        if($maxRows -lt 1 -or $maxRows -gt 10000 -or $columns -lt 2 -or $columns -gt 30){throw 'Unreviewed source capture bound'}
        $matches=@(foreach($h in [TejBridgeNative]::Children($root,'WindowsForms10.Window.')) {
            if(-not [TejBridgeNative]::IsWindowVisible([IntPtr]$h)){continue}
            $candidate=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$h)
            if($candidate.Current.Name -cne 'DataGridView'){continue}
            $header=[TejBridgeNative]::PreviewHeader($h,$columns)
            if($header.Count -eq $columns+1 -and $header[0] -cin @('','Top Left Header Cell')){$header=$header[1..($header.Count-1)]}
            if($header.Count -ne $columns){continue}
            $ok=$true
            for($i=0;$i -lt $request.fields.Count;$i++) {
                $actual=$header[$i+$keysCount];$expected=$request.fields[$i]
                if($actual -cne $expected -and $actual -cne $expected.Replace(',',' ')){$ok=$false;break}
            }
            if($ok){$h}
        })
        if($matches.Count -ne 1){throw 'No unambiguous exact-schema source grid'}
        $grid=[long]$matches[0];$basis='same_existing_completed_TEJ_preview_no_query'
        $savedResult=[IO.File]::ReadAllText($SourceResult)|ConvertFrom-Json
        if($savedResult.task_id -cne $request.task_id -or $savedResult.query_attempt_id -cne $request.query_attempt_id){throw 'Exact attempt-bound result required'}
    }
    $signatureBefore=[TejBridgeNative]::PreviewSignature($grid)
    $shapeBefore=[TejBridgeNative]::PreviewShape($grid)
    $referenceSample=$null
    $baselineCapture=[TejBridgeBaseline].GetMethod('CaptureFullPreview')
    for($round=0;$round -lt $Rounds;$round++) {
        # Balanced AB/BA ordering. Full-array parity, including null and all
        # strings, is checked after each complete read, outside its timing.
        $order=if($round%2 -eq 0){@('baseline','candidate')}else{@('candidate','baseline')}
        foreach($method in $order) {
            $clock=[Diagnostics.Stopwatch]::StartNew();$fallback=0;$sample=$null;$timing=$null
            if($method -eq 'baseline') {
                if($null -ne $baselineCapture) {
                    $matrix=[TejBridgeBaseline]::CaptureFullPreview($grid,$maxRows,$columns,[ref]$fallback,
                        [ref]$sample,[ref]$timing,$null,$null,$null)
                } else {
                    $sample=[TejBridgeBaseline]::Preview($grid,$maxRows,$columns)
                    $matrix=[TejBridgeBaseline]::FullPreview($grid,$maxRows,$columns,[ref]$fallback)
                }
            } else {
                $matrix=[TejBridgeNative]::CaptureFullPreview($grid,$maxRows,$columns,[ref]$fallback,
                    [ref]$sample,[ref]$timing,$null,$null,$null)
            }
            $seconds=$clock.Elapsed.TotalSeconds
            if($null -eq $reference){
                if(-not $Fixture -and -not [TejReadbackBenchmark]::Same($savedResult.cells,$matrix)){throw 'Current Preview differs from the exact saved source result'}
                $reference=$matrix;$referenceSample=($sample|ConvertTo-Json -Depth 5 -Compress)
            }
            if(-not [TejReadbackBenchmark]::Same($reference,$matrix) -or
               ($sample|ConvertTo-Json -Depth 5 -Compress) -cne $referenceSample){throw 'Exact full-grid or object-path sample parity failed'}
            $records.Add(@{method=$method;round=$round;seconds=$seconds;rows=$matrix.Count;
                columns=$columns;fallback_cells=$fallback;sha256=[TejReadbackBenchmark]::Digest($matrix);
                all_cells_identical=$true;sample_identical=$true;component_seconds=$timing})
        }
    }
    if([TejBridgeNative]::PreviewSignature($grid) -cne $signatureBefore -or
       ([TejBridgeNative]::PreviewShape($grid) -join ',') -cne ($shapeBefore -join ',')){throw 'Grid changed during benchmark'}
    $stabilityChecks=@{}
    if($CheckStability) {
        # Reflection only calls our reviewed client helper, not vendor internals.
        # No source form/query can reach these mutations: fixture-only guard
        # above precedes all UI interactions.
        $verify=[TejBridgeNative].GetMethod('VerifyPreviewRowStability',
            ([Reflection.BindingFlags]::NonPublic -bor [Reflection.BindingFlags]::Static))
        if($null -eq $verify){throw 'Candidate stability helper missing'}
        $count=[int]$sample[0]
        [void]$verify.Invoke($null,@([long]$grid,[int]$maxRows,$count))
        $stabilityChecks.unchanged_all_roles_accepted=$true
        [TejReadbackBenchmark]::ChangeFixtureRows($true)
        try {
            $rejected=$false
            try {[void]$verify.Invoke($null,@([long]$grid,[int]($maxRows+1),$count))}
            catch {if($_.Exception.ToString() -notmatch 'Source grid changed during readback'){throw};$rejected=$true}
            if(-not $rejected){throw 'Changed row count was incorrectly accepted'}
            $stabilityChecks.changed_row_count_rejected=$true
        }finally {[TejReadbackBenchmark]::ChangeFixtureRows($false)}
        [void]$verify.Invoke($null,@([long]$grid,[int]$maxRows,$count))
        $stabilityChecks.restored_grid_accepted=$true
        $rejected=$false
        try {[void]$verify.Invoke($null,@([long]$grid,[int]0,$count))}
        catch {if($_.Exception.ToString() -notmatch 'Preview row bound exceeded'){throw};$rejected=$true}
        if(-not $rejected){throw 'Over-bound native grid was incorrectly accepted'}
        $stabilityChecks.row_bound_rejected=$true
        [TejReadbackBenchmark]::ChangeFixtureColumns($true)
        try {
            $rejected=$false;$discardSample=$null;$discardTiming=$null;$discardFallback=0
            try {
                [void][TejBridgeNative]::CaptureFullPreview($grid,$maxRows,$columns,[ref]$discardFallback,
                    [ref]$discardSample,[ref]$discardTiming,$null,$null,$null)
            }catch {
                if($_.Exception.ToString() -notmatch 'Preview column bound exceeded|Unexpected full Preview row schema'){throw}
                $rejected=$true
            }
            if(-not $rejected){throw 'Changed full row schema was incorrectly adopted'}
            $stabilityChecks.changed_schema_full_capture_rejected=$true
        }finally {[TejReadbackBenchmark]::ChangeFixtureColumns($false)}
        [TejReadbackBenchmark]::Close();$opened=$false
        $rejected=$false
        try {[void]$verify.Invoke($null,@([long]$grid,[int]$maxRows,$count))}
        catch {$rejected=$true}
        if(-not $rejected){throw 'Destroyed source window was incorrectly accepted'}
        $stabilityChecks.destroyed_window_rejected=$true
        $rejected=$false;$discardSample=$null;$discardTiming=$null;$discardFallback=0
        try {
            [void][TejBridgeNative]::CaptureFullPreview($grid,$maxRows,$columns,[ref]$discardFallback,
                [ref]$discardSample,[ref]$discardTiming,$null,$null,$null)
        }catch {$rejected=$true}
        if(-not $rejected){throw 'Destroyed grid complete capture was incorrectly adopted'}
        $stabilityChecks.destroyed_window_full_capture_rejected=$true
    }
    $receipt=@{contract='tej_same_grid_complete_readback_benchmark_v1';observed_at_utc=[DateTime]::UtcNow.ToString('o');
        basis=$basis;readback_contract=[TejBridgeNative]::ReadbackContract;measurements=$records.ToArray();
        baseline_script_sha256=(Get-FileHash -LiteralPath $BaselineScript).Hash.ToLowerInvariant();
        candidate_script_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant();
        provider_queries_sent=0;source_values_exposed=$false;queue_modified=$false;accepted=$true;
        source_signature_unchanged=$true;balanced_order=$true;
        exact_saved_source_result_matches=$(-not $Fixture);
        stability_checks=$stabilityChecks;
        interpretation='Complete readback component comparison, not end-to-end query speed, official throughput, API quota or all-table equivalence'}
    [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($Output))|Out-Null
    [IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 6 -Compress),[Text.UTF8Encoding]::new($false))
    Write-Output 'Same-grid complete readback comparison passed; source values were not emitted'
} finally {
    if($opened){[TejReadbackBenchmark]::Close()}
    if($owns){$mutex.ReleaseMutex()};$mutex.Dispose()
}
