<# Same-control ABBA, using the exact production functions. No input or query.
Source mode requires the canonical Linux .download.lock and exact session pins.
Only the isolated, nonactivating fixture may be mutated for rejection tests. #>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$BaselineScript,
      [Parameter(Mandatory=$true)][string]$Output,
      [string]$Session,[switch]$Fixture,[int]$Iterations=10)
$ErrorActionPreference='Stop'
if((Test-Path -LiteralPath $Output) -or $Iterations -lt 1 -or $Iterations -gt 30 -or
   ($Fixture -and $Session) -or (-not $Fixture -and -not $Session)) {
    throw 'New bounded fixture or exact pinned source comparison required'
}
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
function Definition([string]$path,[string]$name) {
    $tokens=$null;$errors=$null
    $ast=[Management.Automation.Language.Parser]::ParseFile($path,[ref]$tokens,[ref]$errors)
    if($errors.Count -ne 0){throw 'Comparator syntax invalid'}
    $matches=@($ast.FindAll({param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name
    },$true))
    if($matches.Count -ne 1){throw 'Unique exact production function required'}
    return $matches[0].Extent.Text
}
foreach($name in @('Compile-TejNativeHelper','Initialize-TejNativeHelper')) {
    Invoke-Expression (Definition $BridgeScript $name)
}
$cache=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) ('StockAgent\TEJSmartWizard\control-benchmark-'+[Guid]::NewGuid().ToString('N'))
$native=Initialize-TejNativeHelper $BridgeScript $cache
$resolvers=@{}
foreach($entry in @(@('baseline',$BaselineScript),@('candidate',$BridgeScript))) {
    $resolvers[$entry[0]]=[scriptblock]::Create('param($group,$name)'+"`n"+
        (Definition $entry[1] 'Control')+"`n"+'Control $group $name ''*COMBOBOX*''')
}
# Baseline's own native enumeration and fresh owner checks, not a proxy loop.
Invoke-Expression (Definition $BaselineScript 'Native-Controls')
Invoke-Expression (Definition $BaselineScript 'Assert-Scope')
Add-Type -TypeDefinition @'
using System;
using System.Threading;
using System.Windows.Forms;
public static class TejControlLookupFixture {
    class PassiveForm:Form {
        protected override bool ShowWithoutActivation {get{return true;}}
        protected override CreateParams CreateParams {get{var p=base.CreateParams;p.ExStyle|=0x08000000;return p;}}
    }
    static Form form;static GroupBox group;static ComboBox duplicate;static ComboBox first;static Thread thread;
    public static long[] Open() {
        var ready=new ManualResetEvent(false);Exception error=null;long[] handles=null;
        thread=new Thread(()=>{try {
            form=new PassiveForm {Text="StockAgent owned combo lookup fixture",Width=400,Height=220};
            group=new GroupBox {Text="Data Source",Dock=DockStyle.Fill};form.Controls.Add(group);
            string[] names={"Type","SmartID","Data"};
            for(int i=0;i<names.Length;i++) {
                var combo=new ComboBox {AccessibleName="Different MSAA "+names[i],Left=15,Top=30+i*35,Width=200};
                combo.Items.AddRange(new object[]{names[i],"Second"});combo.SelectedIndex=0;
                group.Controls.Add(combo);if(i==0)first=combo;
            }
            form.Shown+=(s,e)=>{handles=new long[]{form.Handle.ToInt64(),group.Handle.ToInt64()};ready.Set();};
            Application.Run(form);
        }catch(Exception e){error=e;ready.Set();}});
        thread.IsBackground=true;thread.SetApartmentState(ApartmentState.STA);thread.Start();
        if(!ready.WaitOne(15000))throw new Exception("Owned fixture startup timeout");
        ready.Dispose();if(error!=null)throw error;return handles;
    }
    public static void Change(string state) {
        form.Invoke(new Action(()=>{
            if(duplicate!=null){group.Controls.Remove(duplicate);duplicate.Dispose();duplicate=null;}
            // Recreate only this fixture's widget. A legacy UIA provider can
            // retain its old Name even after native text changed; a failed
            // mutation acknowledgement must not masquerade as a lookup bug.
            group.Controls.Remove(first);first.Dispose();
            first=new ComboBox {AccessibleName="Different MSAA Type",Left=15,Top=30,Width=200};
            first.Items.AddRange(new object[]{state=="case"?"type":"Type","Second"});first.SelectedIndex=0;
            group.Controls.Add(first);first.Visible=true;first.Enabled=true;
            if(state=="duplicate") {duplicate=new ComboBox {AccessibleName="Different MSAA",Text="Type",Left=230,Top=30};group.Controls.Add(duplicate);}
            if(state=="hidden")first.Visible=false;
            if(state=="disabled")first.Enabled=false;
        }));
    }
    public static void Close() {
        if(form!=null&&!form.IsDisposed)form.Invoke(new Action(()=>form.Close()));
        if(thread!=null&&!thread.Join(5000))throw new Exception("Owned fixture did not close");
    }
}
'@ -ReferencedAssemblies @([Windows.Forms.SendKeys].Assembly.Location)
$mutexName='Local\StockAgentTEJSmartWizardOwner'
if($Fixture){$mutexName+='-owned-fixture-'+[Guid]::NewGuid().ToString('N')}
$mutex=[Threading.Mutex]::new($false,$mutexName);$owns=$false
$samples=[Collections.Generic.List[object]]::new();$checks=[Collections.Generic.List[object]]::new()
$receipt=@{contract='tej_exact_control_lookup_abba_v1';accepted=$false;provider_queries_sent=0;
    desktop_actions_sent=0;source_values_exposed=$false;queue_modified=$false;fixture=[bool]$Fixture;
    baseline_sha256=(Get-FileHash -LiteralPath $BaselineScript).Hash.ToLowerInvariant();
    candidate_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant()}
try {
    $owns=$mutex.WaitOne(0);if(-not $owns){throw 'Canonical desktop owner is busy'}
    if($Fixture) {
        $handles=[TejControlLookupFixture]::Open();$ExpectedWindow=$handles[0]
        $TejProcessId=$PID;$ExpectedTitle='StockAgent owned combo lookup fixture';$parent=$handles[1]
    }else {
        $pins=Get-Content -LiteralPath $Session -Raw -Encoding UTF8|ConvertFrom-Json
        $ExpectedWindow=[long]$pins.ExpectedWindow;$TejProcessId=[int]$pins.TejProcessId;$ExpectedTitle=[string]$pins.ExpectedTitle
        Assert-Scope $ExpectedWindow
        if([TejBridgeNative]::Dialogs($ExpectedWindow).Count -ne 0){throw 'Vendor notice blocks read-only comparison'}
        $parent=[TejBridgeNative]::ExactQueryGroup($ExpectedWindow,'Data Source',$TejProcessId,$ExpectedTitle)
    }
    $group=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$parent)
    if($Fixture) {
        $receipt.fixture_controls=@(foreach($h in [TejBridgeNative]::Children($parent,'WindowsForms10.')) {
            $node=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$h)
            @{class=[TejBridgeNative]::WindowClass($h);uia_name=$node.Current.Name;
              msaa_name=[TejBridgeNative]::AccessibleName($h);visible=[TejBridgeNative]::IsWindowVisible([IntPtr]$h)}
        })
    }
    function Snapshot {
        $result=@(foreach($name in @('Type','SmartID','Data')) {
            $h=& $resolvers.baseline $group $name
            @($h,[TejBridgeNative]::SelectedComboText($ExpectedWindow,$h))
        })
        return ($result|ConvertTo-Json -Depth 4 -Compress)
    }
    $before=Snapshot;$expected=@{}
    foreach($name in @('Type','SmartID','Data')){$expected[$name]=& $resolvers.baseline $group $name}
    foreach($variant in @('baseline','candidate','candidate','baseline')) {
        $clock=[Diagnostics.Stopwatch]::StartNew()
        for($i=0;$i -lt $Iterations;$i++) {
            foreach($name in @('Type','SmartID','Data')) {
                if((& $resolvers[$variant] $group $name) -ne $expected[$name]){throw 'Exact control handle parity differs'}
            }
        }
        $samples.Add(@{variant=$variant;lookups=3*$Iterations;seconds=$clock.Elapsed.TotalSeconds})
    }
    if((Snapshot) -cne $before){throw 'Source selection changed during comparison'}
    if($Fixture) {
        foreach($fault in @('duplicate','case','hidden')) {
            [TejControlLookupFixture]::Change($fault)
            foreach($variant in @('baseline','candidate')) {
                $rejected=$false;try{[void](& $resolvers[$variant] $group 'Type')}catch{$rejected=$true}
                $checks.Add(@{fault=$fault;variant=$variant;rejected=$rejected})
                if(-not $rejected){throw 'Ambiguous or missing control accepted'}
            }
        }
        [TejControlLookupFixture]::Change('disabled')
        if((& $resolvers.candidate $group 'Type') -ne (& $resolvers.baseline $group 'Type')){throw 'Disabled read-only lookup differs'}
        [TejControlLookupFixture]::Change('normal')
        foreach($fault in @('wrong_pid','wrong_title','foreign_parent')) {
            $badPid=$TejProcessId;$badTitle=$ExpectedTitle;$badParent=$parent
            if($fault -eq 'wrong_pid'){$badPid++};if($fault -eq 'wrong_title'){$badTitle+=' foreign'}
            if($fault -eq 'foreign_parent'){$badParent=[TejBridgeNative]::GetParent([IntPtr]$ExpectedWindow).ToInt64()}
            $rejected=$false;try{[TejBridgeNative]::AssertControlScope($ExpectedWindow,$badParent,$badPid,$badTitle)}catch{$rejected=$true}
            $checks.Add(@{fault=$fault;variant='candidate';rejected=$rejected})
            if(-not $rejected){throw 'Foreign control owner accepted'}
        }
        foreach($name in @('Type','SmartID','Data')) {
            $h=& $resolvers.baseline $group $name
            if((& $resolvers.candidate $group $name) -ne $h -or
               [TejBridgeNative]::SelectedComboText($ExpectedWindow,$h) -cne $name){throw 'Restored fixture selection differs'}
        }
    }
    $receipt.accepted=$true;$receipt.source_selection_unchanged=$true;$receipt.exact_handle_parity=$true
}finally {
    if($Fixture){[TejControlLookupFixture]::Close()}
    if($owns){$mutex.ReleaseMutex()};$mutex.Dispose()
    $receipt.samples=$samples.ToArray();$receipt.checks=$checks.ToArray();$receipt.observed_at_utc=[DateTime]::UtcNow.ToString('o')
    [IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 6 -Compress),[Text.UTF8Encoding]::new($false))
}
Write-Output 'Exact control lookup ABBA passed; no input or source query'
