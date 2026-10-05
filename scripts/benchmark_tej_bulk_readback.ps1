<# Experimental fixture-only bulk UIA candidate versus canonical full MSAA.
No source queries, selection, input, activation, clipboard or source adoption.
Unsupported properties/rows/null representations are a rejected candidate,
never permission to substitute missing values or deploy a lossy fast path.
Run only under an external bounded launcher. The cache candidate timed out
in the owned fixture; production/source execution is deliberately prohibited.
#>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$FixtureScript,
      [Parameter(Mandatory=$true)][string]$Output,
      [switch]$Fixture,
      [ValidateRange(1,1000)][int]$FixtureRows=128)
$ErrorActionPreference='Stop'
if(-not $Fixture){throw 'Unaccepted cache candidate may only access its owned fixture'}
if(Test-Path -LiteralPath $Output){throw 'Retain original bulk comparison'}
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
$code=[regex]::Match([IO.File]::ReadAllText($BridgeScript),"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $code.Success){throw 'Canonical native declaration required'}
Add-Type -TypeDefinition $code.Groups[1].Value -ReferencedAssemblies @(
    [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)
$fixtureCode=[regex]::Match([IO.File]::ReadAllText($FixtureScript),"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $fixtureCode.Success -or $fixtureCode.Groups[1].Value -notmatch 'public static class TejReadbackBenchmark'){throw 'Canonical owned fixture required'}
Add-Type -TypeDefinition $fixtureCode.Groups[1].Value -ReferencedAssemblies @([Windows.Forms.Form].Assembly.Location)
Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.Windows.Automation;
using System.Threading;
public static class TejBulkReadbackCandidate {
    private static object Property(AutomationElement e,AutomationProperty p) {
        object v=e.GetCachedPropertyValue(p,true);
        if(v==AutomationElement.NotSupported)throw new Exception("bulk_property_not_supported");
        return v;
    }
    public static object[][] Read(long grid,int maxRows,int columns) {
        // UIA owns no windows and creates/uses/releases its elements in ONE
        // MTA. Only plain strings/arrays cross back to the STA caller.
        object[][] result=null;Exception error=null;
        var worker=new Thread(()=> {
            try {result=ReadCore(grid,maxRows,columns);}catch(Exception e){error=e;}
        });
        worker.IsBackground=true;worker.SetApartmentState(ApartmentState.MTA);worker.Start();
        if(!worker.Join(10000))throw new TimeoutException("bulk_MTA_provider_cache_timeout");
        if(error!=null)throw error;
        return result;
    }
    private static object[][] ReadCore(long grid,int maxRows,int columns) {
        var request=new CacheRequest();
        request.AutomationElementMode=AutomationElementMode.None;
        request.TreeScope=TreeScope.Element|TreeScope.Descendants;
        request.TreeFilter=Condition.TrueCondition;
        request.Add(AutomationElement.ControlTypeProperty);
        request.Add(AutomationElement.NameProperty);
        request.Add(ValuePattern.ValueProperty);
        var root=AutomationElement.FromHandle(new IntPtr(grid)).GetUpdatedCache(request);
        var rows=new List<object[]>();long characters=0;
        var rowNodes=root.CachedChildren;
        for(int ordinal=0;ordinal<rowNodes.Count;ordinal++) {
            var row=rowNodes[ordinal];
            var role=(ControlType)Property(row,AutomationElement.ControlTypeProperty);
            if(role==ControlType.ScrollBar)continue;
            if(role!=ControlType.DataItem&&role!=ControlType.Header&&role!=ControlType.Custom)
                throw new Exception("bulk_row_role_differs");
            var cells=row.CachedChildren;int skip=cells.Count-columns;
            if(skip<0||skip>1||rows.Count>maxRows+1)throw new Exception("bulk_grid_shape_differs");
            var values=new object[columns];bool blank=true;
            for(int c=skip;c<cells.Count;c++) {
                object value=Property(cells[c],rows.Count==0?AutomationElement.NameProperty:ValuePattern.ValueProperty);
                if(value!=null&&!(value is string))throw new Exception("bulk_value_type_differs");
                string text=value as string;
                if(text!=null)characters+=text.Length;
                if(characters>64000000)throw new Exception("bulk_text_bound_exceeded");
                values[c-skip]=value;if(!String.IsNullOrEmpty(text))blank=false;
            }
            // Exactly the same final-new-row affordance as canonical MSAA.
            if(blank&&rows.Count>0) {
                if(ordinal==rowNodes.Count-1)continue;
            }
            rows.Add(values);
        }
        if(rows.Count<2)throw new Exception("bulk_empty_matrix");
        return rows.ToArray();
    }
}
'@ -ReferencedAssemblies @([Windows.Automation.AutomationElement].Assembly.Location,[Windows.Automation.AutomationProperty].Assembly.Location)
$opened=$false
$records=[Collections.Generic.List[object]]::new()
try {
    $grid=[TejReadbackBenchmark]::Open($FixtureRows,30);$opened=$true;$maxRows=$FixtureRows+1;$columns=30
    $signature=[TejBridgeNative]::PreviewSignature($grid)
    $reference=$null;$candidateAccepted=$true
    foreach($order in @(@('msaa','bulk_uia'),@('bulk_uia','msaa'))) {
        foreach($mode in $order) {
            $clock=[Diagnostics.Stopwatch]::StartNew();$reason=$null;$matrix=$null
            # The launcher can retain this exact no-query stage if an optional
            # UIA provider cache hangs. Never relaunch a source query on timeout.
            [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($Output))|Out-Null
            [IO.File]::WriteAllText(($Output+'.progress.json'),(@{
                contract='tej_readonly_bulk_uia_candidate_progress_v1';
                method=$mode;provider_queries_sent=0;fixture=[bool]$Fixture;
                completed_measurements=$records.ToArray();observed_at_utc=[DateTime]::UtcNow.ToString('o')
            }|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
            try {
                if($mode -ceq 'msaa') {
                    $fallback=0;$sample=$null;$timing=$null
                    $matrix=[TejBridgeNative]::CaptureFullPreview($grid,$maxRows,$columns,[ref]$fallback,
                        [ref]$sample,[ref]$timing,$null,$null,$null)
                } else {$matrix=[TejBulkReadbackCandidate]::Read($grid,$maxRows,$columns)}
            }catch {
                if($mode -ceq 'msaa'){throw}
                $reason='bulk_cache_unsupported_or_shape_incompatible';$candidateAccepted=$false
            }
            $seconds=$clock.Elapsed.TotalSeconds;$parity=$false
            if($null -ne $matrix) {
                if($null -eq $reference){$reference=$matrix}
                $parity=[TejReadbackBenchmark]::Same($reference,$matrix)
                if(-not $parity){$reason='exact_null_string_or_matrix_parity_failed';$candidateAccepted=$false}
            }
            $records.Add(@{method=$mode;seconds=$seconds;accepted=$parity;reason=$reason;
                rows=$(if($null -ne $matrix){$matrix.Count}else{$null});columns=$columns})
            if(-not $candidateAccepted){break}
        }
        if(-not $candidateAccepted){break}
    }
    if([TejBridgeNative]::PreviewSignature($grid) -cne $signature){throw 'Existing source changed during candidate evaluation'}
    $receipt=@{contract='tej_readonly_bulk_uia_candidate_v1';accepted=$true;candidate_accepted=$candidateAccepted;
        provider_queries_sent=0;source_values_exposed=$false;source_adopted=$false;queue_modified=$false;
        existing_grid_signature_unchanged=$true;fixture=[bool]$Fixture;measurements=$records.ToArray();
        uia_threading_contract='owned_non_UI_MTA_properties_only_v1';
        observed_at_utc=[DateTime]::UtcNow.ToString('o')}
    [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($Output))|Out-Null
    [IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
    Write-Output 'Read-only bulk candidate measured; no source request or values emitted'
}finally {
    if($opened){[TejReadbackBenchmark]::Close()}
}
