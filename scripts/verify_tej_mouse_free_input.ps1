<#
Engineering acceptance against ONLY a newly owned WinForms fixture. Compiles
the real acquisition helpers; never connects to TEJ, Excel or a provider API.
Closes only its own fixture and restores the previous foreground when possible.
Use a new output path; does not relax Windows/Office execution policy.
#>
param(
    [Parameter(Mandatory=$true)][string]$BridgeScript,
    [Parameter(Mandatory=$true)][string]$CatalogScript,
    [Parameter(Mandatory=$true)][string]$Output
)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if(Test-Path -LiteralPath $Output){throw 'Refusing to overwrite input acceptance evidence'}
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
Add-Type -AssemblyName System.Drawing
foreach($path in @($BridgeScript,$CatalogScript)) {
    $text=[IO.File]::ReadAllText($path)
    $match=[regex]::Match($text,"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
    if(-not $match.Success){throw 'Canonical native helper declaration missing'}
    Add-Type -TypeDefinition $match.Groups[1].Value -ReferencedAssemblies @(
        [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location
    )
}
Add-Type -TypeDefinition @'
using System;
using System.Threading;
using System.Windows.Forms;
using System.Drawing;
public static class TejInputFixture {
    public static Form Root;
    public static TabControl Tabs;
    public static GroupBox Dates;
    public static MaskedTextBox Start,End;
    public static TextBox Foreign;
    public static Button Preview;
    public static DataGridView Grid;
    public static ListBox Labels;
    public static Form ForegroundFixture;
    public static int PreviewClicks;
    public static bool StealFocus;
    public static int TabChanges;
    private static Thread thread;
    private static Exception startupError;
    public static void Open() {
        var ready=new ManualResetEvent(false);
        thread=new Thread(()=> {
            try {
                Root=new Form {Text="StockAgent owned input fixture",Size=new Size(620,300),StartPosition=FormStartPosition.CenterScreen};
                Tabs=new TabControl {Dock=DockStyle.Fill};
                foreach(string name in new[]{"Main Page","Transformation","Style Format","Preview"})Tabs.TabPages.Add(name);
                Tabs.SelectedIndexChanged+=(s,e)=>TabChanges++;
                Dates=new GroupBox {Text="Date Setting",Location=new Point(16,16),Size=new Size(350,95)};
                Start=new MaskedTextBox("0000/00/00") {Text="2020/03/02",Location=new Point(12,28),Width=135,TabIndex=0};
                End=new MaskedTextBox("0000/00/00") {Text="2026/10/01",Location=new Point(170,28),Width=135,TabIndex=1};
                Foreign=new TextBox {Text="UNRELATED_INPUT_UNCHANGED",Location=new Point(16,135),Width=350,TabIndex=2};
                Preview=new Button {Text="Preview",Location=new Point(430,135),Size=new Size(100,30)};
                Grid=new DataGridView {Dock=DockStyle.Fill,AllowUserToAddRows=false};
                Grid.Columns.Add("id","CO_ID");Grid.Columns.Add("date","Date");Grid.Columns.Add("value","Raw");
                Grid.Rows.Add("2330","2014/01/02","10");Grid.Rows.Add("2330","2014/01/03","20");
                Tabs.TabPages[3].Controls.Add(Grid);
                Preview.Click+=(s,e)=>{PreviewClicks++;Tabs.SelectedIndex=3;};
                Dates.Controls.Add(Start);Dates.Controls.Add(End);
                Tabs.TabPages[0].Controls.Add(Dates);Tabs.TabPages[0].Controls.Add(Foreign);Root.Controls.Add(Tabs);
                Root.Controls.Add(Preview);Preview.BringToFront();
                Labels=new ListBox {Location=new Point(380,25),Size=new Size(190,90)};
                Labels.Items.AddRange(new object[]{"2330 Foo","20261001","prefix","dupe","dupe"});
                Tabs.TabPages[0].Controls.Add(Labels);
                Start.TextChanged+=(s,e)=>{if(StealFocus){StealFocus=false;Foreign.Focus();}};
                Root.Shown+=(s,e)=>ready.Set();
                Application.Run(Root);
            }catch(Exception e){startupError=e;ready.Set();}
        });
        thread.IsBackground=true;thread.SetApartmentState(ApartmentState.STA);thread.Start();
        if(!ready.WaitOne(10000))throw new Exception("Owned fixture startup timed out");
        ready.Dispose();if(startupError!=null)throw startupError;
    }
    public static long Handle(string name) {
        return (long)Root.Invoke(new Func<long>(()=> {
            Control c=name=="root"?(Control)Root:name=="tabs"?(Control)Tabs:name=="group"?(Control)Dates:
                      name=="start"?(Control)Start:name=="end"?(Control)End:name=="preview"?(Control)Preview:
                      name=="grid"?(Control)Grid:name=="labels"?(Control)Labels:(Control)Foreign;
            return c.Handle.ToInt64();
        }));
    }
    public static void SetMode(string mode) {
        Root.Invoke(new Action(()=> {
            StealFocus=false;Dates.Enabled=true;Start.ReadOnly=false;Start.Text="2020/03/02";
            Preview.Enabled=true;Preview.Visible=true;
            End.Text="2026/10/01";Foreign.Text="UNRELATED_INPUT_UNCHANGED";
            if(mode=="disabled")Dates.Enabled=false;
            if(mode=="readonly")Start.ReadOnly=true;
            if(mode=="blank")Start.Text="";
            if(mode=="steal")StealFocus=true;
            if(mode=="preview_disabled")Preview.Enabled=false;
            if(mode=="preview_hidden")Preview.Visible=false;
        }));
    }
    public static void ChangeGridValue() {Root.Invoke(new Action(()=>Grid.Rows[1].Cells[2].Value="21"));}
    public static long ShowForegroundFixture() {
        return (long)Root.Invoke(new Func<long>(()=> {
            ForegroundFixture=new Form {Text="StockAgent unrelated owned foreground fixture",Size=new Size(320,180)};
            ForegroundFixture.Show();ForegroundFixture.Activate();return ForegroundFixture.Handle.ToInt64();
        }));
    }
    public static void Close() {
        if(ForegroundFixture!=null&&!ForegroundFixture.IsDisposed)ForegroundFixture.Invoke(new Action(()=>ForegroundFixture.Close()));
        if(Root!=null&&!Root.IsDisposed)Root.Invoke(new Action(()=>Root.Close()));
        if(thread!=null&&!thread.Join(5000))throw new Exception("Owned fixture did not close");
    }
}
'@ -ReferencedAssemblies @([Windows.Forms.Form].Assembly.Location,[Drawing.Point].Assembly.Location)

$checks=[Collections.Generic.List[object]]::new()
$previous=[TejBridgeNative]::GetForegroundWindow().ToInt64()
$clock=[Diagnostics.Stopwatch]::StartNew()
$opened=$false;$failure=$null;$fixtureDiagnostic=$null
function Check([string]$name,[bool]$ok) {
    $checks.Add(@{name=$name;passed=$ok})
    if(-not $ok){throw ('Fixture check failed: '+$name)}
}
function Expect-Rejection([string]$name,[scriptblock]$operation) {
    $rejected=$false
    try {& $operation}catch {$rejected=$true}
    Check $name $rejected
}
try {
    [TejInputFixture]::Open();$opened=$true
    $r=[TejInputFixture]::Handle('root');$t=[TejInputFixture]::Handle('tabs')
    $g=[TejInputFixture]::Handle('group');$s=[TejInputFixture]::Handle('start');$e=[TejInputFixture]::Handle('end')
    $f=[TejInputFixture]::Handle('foreign')
    [TejBridgeNative]::SelectTab($r,$t,3)
    Check 'preview_tab_native_readback' ([TejBridgeNative]::Message($t,0x130B,0,0) -eq 3)
    $pidOfRoot=[uint32]0;[void][TejBridgeNative]::GetWindowThreadProcessId([IntPtr]$r,[ref]$pidOfRoot)
    [TejCatalogNative]::MainPage($r,$t,$pidOfRoot)
    Check 'catalog_main_page_native_readback' ([TejBridgeNative]::Message($t,0x130B,0,0) -eq 0)
    Check 'normal_tab_notifications_observed' ([TejInputFixture]::TabChanges -ge 2)
    $list=[TejInputFixture]::Handle('labels')
    Check 'exact_native_company_lookup' ([TejBridgeNative]::ExactListIndex($r,$list,'2330 Foo') -eq 0)
    Check 'exact_native_date_lookup' ([TejBridgeNative]::ExactListIndex($r,$list,'20261001') -eq 1)
    Expect-Rejection 'absent_exact_label_rejected' {[TejBridgeNative]::ExactListIndex($r,$list,'prefix suffix')}
    Expect-Rejection 'duplicate_exact_label_rejected' {[TejBridgeNative]::ExactListIndex($r,$list,'dupe')}
    [TejBridgeNative]::DateText($r,$s,$g,'20210715')
    Check 'start_changed_date_real_keyboard_readback' ([TejBridgeNative]::Text($s) -ceq '2021/07/15')
    Check 'start_did_not_change_end' ([TejBridgeNative]::Text($e) -ceq '2026/10/01')
    [TejBridgeNative]::DateText($r,$s,$g,'20200302')
    [TejBridgeNative]::DateText($r,$e,$g,'20260930')
    Check 'end_changed_date_real_keyboard_readback' ([TejBridgeNative]::Text($e) -ceq '2026/09/30')
    Check 'end_did_not_change_start' ([TejBridgeNative]::Text($s) -ceq '2020/03/02')
    [TejBridgeNative]::DateText($r,$e,$g,'20261001')
    [TejInputFixture]::SetMode('blank')
    Check 'blank_source_mask_observed' ([TejBridgeNative]::Text($s) -ceq '____/__/__')
    [TejBridgeNative]::DateText($r,$s,$g,'20210715')
    Check 'blank_mask_real_digits_and_model_commit' ([TejBridgeNative]::Text($s) -ceq '2021/07/15')
    Check 'blank_start_never_modified_end' ([TejBridgeNative]::Text($e) -ceq '2026/10/01')
    [TejInputFixture]::SetMode('disabled')
    Expect-Rejection 'disabled_group_no_input' {[TejBridgeNative]::WriteDateText($r,$s,$g,'20210715')}
    Check 'disabled_group_values_unchanged' ([TejBridgeNative]::Text($s) -ceq '2020/03/02')
    [TejInputFixture]::SetMode('normal')
    Expect-Rejection 'foreign_edit_scope_rejected' {[TejBridgeNative]::WriteDateText($r,$f,$g,'20210715')}
    Expect-Rejection 'invalid_calendar_date_rejected' {[TejBridgeNative]::WriteDateText($r,$s,$g,'20210230')}
    Check 'invalid_date_values_unchanged' ([TejBridgeNative]::Text($s) -ceq '2020/03/02')
    [TejInputFixture]::SetMode('readonly')
    Expect-Rejection 'readonly_date_no_write' {[TejBridgeNative]::WriteDateText($r,$s,$g,'20210715')}
    Check 'readonly_values_unchanged' ([TejBridgeNative]::Text($s) -ceq '2020/03/02')
    [TejInputFixture]::SetMode('steal')
    Expect-Rejection 'focus_loss_stops_real_input' {[TejBridgeNative]::WriteDateText($r,$s,$g,'20210715')}
    Check 'focus_loss_never_typed_into_unrelated_edit' ([TejBridgeNative]::Text($f) -ceq 'UNRELATED_INPUT_UNCHANGED')
    [TejInputFixture]::SetMode('normal');[TejBridgeNative]::Activate($r)
    $b=[TejInputFixture]::Handle('preview');$grid=[TejInputFixture]::Handle('grid')
    Check 'preview_actual_msaa_press_capability' ([TejBridgeNative]::AccessibleDefaultAction($b) -ceq 'Press')
    $before=[TejBridgeNative]::PreviewSignature($grid)
    Check 'bounded_preview_signature_stable' ($before -ceq [TejBridgeNative]::PreviewSignature($grid))
    [TejInputFixture]::ChangeGridValue()
    Check 'bounded_preview_signature_detects_changed_value' ($before -cne [TejBridgeNative]::PreviewSignature($grid))
    [TejInputFixture]::SetMode('preview_disabled')
    Expect-Rejection 'disabled_preview_never_invoked' {[TejBridgeNative]::BeginPreviewDefaultAction($r,$b)}
    [TejInputFixture]::SetMode('preview_hidden')
    Expect-Rejection 'hidden_preview_never_invoked' {[TejBridgeNative]::BeginPreviewDefaultAction($r,$b)}
    [TejInputFixture]::SetMode('normal')
    Expect-Rejection 'nonbutton_preview_never_invoked' {[TejBridgeNative]::BeginPreviewDefaultAction($r,$f)}
    Check 'preview_no_calls_before_owned_action' ([TejInputFixture]::PreviewClicks -eq 0)
    $backgroundForeground=[TejInputFixture]::ShowForegroundFixture()
    [TejBridgeNative]::Activate($backgroundForeground)
    Check 'preview_owner_really_in_background' ([TejBridgeNative]::GetForegroundWindow().ToInt64() -ne $r)
    [TejBridgeNative]::BeginPreviewDefaultAction($r,$b)
    $by=[DateTime]::UtcNow.AddSeconds(5)
    while([TejBridgeNative]::PreviewActionState() -eq 0 -and [DateTime]::UtcNow -lt $by){Start-Sleep -Milliseconds 25}
    Check 'single_owned_msaa_action_completed' ([TejBridgeNative]::PreviewActionState() -eq 1)
    Check 'single_owned_action_changed_tab' ([TejBridgeNative]::Message($t,0x130B,0,0) -eq 3)
    Expect-Rejection 'second_preview_action_rejected' {[TejBridgeNative]::BeginPreviewDefaultAction($r,$b)}
    Check 'single_owned_msaa_click_exactly_once' ([TejInputFixture]::PreviewClicks -eq 1)
    # MSAA can activate its own UI as part of the normal default action.
    # Record that observed side effect; do not promise a headless desktop.
    $previewForegroundAfter=[TejBridgeNative]::GetForegroundWindow().ToInt64()
    Check 'background_preview_single_action_verified' ([TejInputFixture]::PreviewClicks -eq 1)
} catch {$failure='owned_fixture_acceptance_failed';$fixtureDiagnostic=$_.Exception.Message} finally {
    if($opened){[TejInputFixture]::Close()}
    if($previous -gt 0 -and [TejBridgeNative]::IsWindow([IntPtr]$previous)){
        try {[TejBridgeNative]::Activate($previous)}catch {} # No retry or input.
    }
    $clock.Stop()
}
$receipt=@{contract_version=1;desktop_input_contract=[TejBridgeNative]::InputContract;
    preview_submission_contract=[TejBridgeNative]::SubmissionContract;
    observed_at_utc=[DateTime]::UtcNow.ToString('o');basis='owned_winforms_fixture_not_vendor_download';
    source_bridge_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant();
    source_catalog_sha256=(Get-FileHash -LiteralPath $CatalogScript).Hash.ToLowerInvariant();
    seconds=$clock.Elapsed.TotalSeconds;checks=$checks.ToArray();failure=$failure;fixture_diagnostic=$fixtureDiagnostic;
    preview_foreground_before=$backgroundForeground;preview_foreground_after=$previewForegroundAfter;
    passed=($null -eq $failure);fixture_closed=$true;vendor_connected=$false;market_data_query_submitted=$false}
[IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
if($failure){throw $failure}
Write-Output 'Owned Windows input fixture passed; no vendor connection or query'
