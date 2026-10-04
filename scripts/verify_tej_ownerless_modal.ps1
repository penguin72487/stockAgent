<#
Validate the canonical native modal helpers on a newly owned Windows fixture.
No TEJ/Excel connection, provider query, credentials or policy changes.
Closes only its own windows; preserves the original foreground where possible.
#>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$Output)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if(Test-Path -LiteralPath $Output){throw 'Do not overwrite modal acceptance'}
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
$source=[IO.File]::ReadAllText($BridgeScript)
$tokens=$null;$parseErrors=$null
[void][Management.Automation.Language.Parser]::ParseInput($source,[ref]$tokens,[ref]$parseErrors)
if($parseErrors.Count -ne 0){throw 'Canonical bridge PowerShell parse failed'}
$match=[regex]::Match($source,"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $match.Success){throw 'Canonical native helpers missing'}
Add-Type -TypeDefinition $match.Groups[1].Value -ReferencedAssemblies @([Accessibility.IAccessible].Assembly.Location)
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Windows.Forms;
public static class TejModalFixture {
    public static Form Root,Other;
    private static Thread thread;
    [DllImport("user32.dll",CharSet=CharSet.Unicode)] static extern int MessageBoxW(IntPtr owner,string text,string caption,uint flags);
    public static void Start() {
        var ready=new ManualResetEvent(false);
        thread=new Thread(()=> {
            Root=new Form {Text="StockAgent owned modal fixture",Width=320,Height=150};
            Root.Shown+=(s,e)=>ready.Set();Application.Run(Root);
        });
        thread.IsBackground=true;thread.SetApartmentState(ApartmentState.STA);thread.Start();
        if(!ready.WaitOne(10000))throw new Exception("Fixture startup timed out");ready.Dispose();
    }
    public static void Modal(string message,bool owned) {
        Root.BeginInvoke(new Action(()=>MessageBoxW(owned?Root.Handle:IntPtr.Zero,message,"Owned fixture notice",owned?0u:0x2000u)));
    }
    public static void ShowOther() {
        Root.Invoke(new Action(()=>{Other=new Form {Text="Owned ambiguity fixture",Width=200,Height=100};Other.Show();}));
    }
    public static void HideOther() {if(Other!=null)Root.Invoke(new Action(()=>{Other.Close();Other.Dispose();Other=null;}));}
    public static void Stop() {
        if(Root!=null&&!Root.IsDisposed)Root.BeginInvoke(new Action(()=>Root.Close()));
        if(thread!=null&&!thread.Join(5000))throw new Exception("Owned fixture did not stop");
    }
}
'@ -ReferencedAssemblies @([Windows.Forms.Form].Assembly.Location)
$checks=[Collections.Generic.List[object]]::new()
$prior=[TejBridgeNative]::GetForegroundWindow().ToInt64();$modal=0
function Check([string]$name,[bool]$condition) {
    if(-not $condition){throw ('Modal fixture failed: '+$name)}
    $checks.Add(@{name=$name;accepted=$true})
}
function Await-Modal {
    $deadline=[DateTime]::UtcNow.AddSeconds(5)
    do {
        Start-Sleep -Milliseconds 50
        $found=@([TejBridgeNative]::VisibleProcessWindows($rootHandle)|Where-Object {$_ -ne $rootHandle})
    }while($found.Count -ne 1 -and [DateTime]::UtcNow -lt $deadline)
    if($found.Count -ne 1){throw 'One exact owned fixture modal required'}
    return $found[0]
}
function Await-Closed([long]$handle) {
    $deadline=[DateTime]::UtcNow.AddSeconds(5)
    while([TejBridgeNative]::IsWindow([IntPtr]$handle) -and [DateTime]::UtcNow -lt $deadline){Start-Sleep -Milliseconds 50}
    Check 'normal_acknowledgement_closed_fixture' (-not [TejBridgeNative]::IsWindow([IntPtr]$handle))
}
function Acknowledge-Modal([long]$handle,[long]$button) {
    [TejBridgeNative]::AcknowledgeNormalEmptyDialog($rootHandle,$handle,$button)
}
try {
    [TejModalFixture]::Start();$rootHandle=[TejModalFixture]::Root.Handle.ToInt64()
    [TejModalFixture]::Modal('ERROR1:No data !!(fixture)',$false);$modal=Await-Modal
    Check 'ownerless_is_really_zero_owner' ([TejBridgeNative]::GetWindow([IntPtr]$modal,4) -eq [IntPtr]::Zero)
    Check 'source_root_disabled' (-not [TejBridgeNative]::IsWindowEnabled([IntPtr]$rootHandle))
    Check 'unique_same_thread_modal_recognized' ([TejBridgeNative]::IsUniqueOwnerlessModal($rootHandle,$modal))
    Check 'canonical_notice_guard_includes_ownerless' ($modal -in [TejBridgeNative]::Dialogs($rootHandle))
    $button=@([TejBridgeNative]::Children($modal,'Button'))[0]
    if($prior -ne 0 -and [TejBridgeNative]::IsWindow([IntPtr]$prior)){[TejBridgeNative]::Activate($prior)}
    $before=[TejBridgeNative]::GetForegroundWindow()
    Acknowledge-Modal $modal $button
    Await-Closed $modal;$modal=0
    Check 'acknowledgement_no_foreground_activation' ([TejBridgeNative]::GetForegroundWindow() -eq $before)
    [TejModalFixture]::Modal('Quota exceeded',$false);$modal=Await-Modal
    $button=@([TejBridgeNative]::Children($modal,'Button'))[0];$rejected=$false
    try {Acknowledge-Modal $modal $button}catch{$rejected=$true}
    Check 'quota_message_not_acknowledged' ($rejected -and [TejBridgeNative]::IsWindow([IntPtr]$modal))
    [void][TejBridgeNative]::PostMessageW([IntPtr]$modal,0x10,[IntPtr]::Zero,[IntPtr]::Zero);Await-Closed $modal;$modal=0
    [TejModalFixture]::Modal('ERROR1:No data !!(fixture)',$false);$modal=Await-Modal
    [TejModalFixture]::ShowOther()
    Check 'second_visible_process_window_is_ambiguous' (-not [TejBridgeNative]::IsUniqueOwnerlessModal($rootHandle,$modal))
    $button=@([TejBridgeNative]::Children($modal,'Button'))[0];$rejected=$false
    try {Acknowledge-Modal $modal $button}catch{$rejected=$true}
    Check 'ambiguous_ownerless_modal_not_acknowledged' ($rejected -and [TejBridgeNative]::IsWindow([IntPtr]$modal))
    [TejModalFixture]::HideOther()
    Acknowledge-Modal $modal $button;Await-Closed $modal;$modal=0
    [TejModalFixture]::Modal('ERROR1:No data !!(fixture)',$true);$modal=Await-Modal
    Check 'direct_owned_dialog_still_recognized' ($modal -in [TejBridgeNative]::Dialogs($rootHandle))
    $button=@([TejBridgeNative]::Children($modal,'Button'))[0]
    Acknowledge-Modal $modal $button;Await-Closed $modal;$modal=0
}finally {
    if($modal -ne 0 -and [TejBridgeNative]::IsWindow([IntPtr]$modal)) {
        # Cleanup ONLY the exact modal created by this owned fixture.
        [void][TejBridgeNative]::PostMessageW([IntPtr]$modal,0x10,[IntPtr]::Zero,[IntPtr]::Zero)
        Start-Sleep -Milliseconds 150
    }
    [TejModalFixture]::HideOther();[TejModalFixture]::Stop()
    if($prior -ne 0 -and [TejBridgeNative]::IsWindow([IntPtr]$prior)){[TejBridgeNative]::Activate($prior)}
}
$receipt=@{contract='owned_fixture_ownerless_modal_acceptance_v1';accepted=$true;
    observed_at_utc=[DateTime]::UtcNow.ToString('o');checks=$checks.ToArray();
    source_queries_sent=0;tej_or_excel_connected=$false;credentials_read=$false;
    bridge_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant()}
[IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 4 -Compress),[Text.UTF8Encoding]::new($false))
Write-Output ('Owned modal fixture passed '+$checks.Count+' checks; no provider query')
