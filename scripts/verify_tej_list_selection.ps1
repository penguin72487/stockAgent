<#
Exercise the real TEJ native list helper on owned WinForms lists only.
No TEJ/Excel connection, source query, credential read or policy changes.
Run with a new output path; failure evidence is retained as well as success.
#>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$Output)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if(Test-Path -LiteralPath $Output){throw 'Do not overwrite list acceptance evidence'}
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
$source=[IO.File]::ReadAllText($BridgeScript)
$tokens=$null;$parseErrors=$null
$syntax=[Management.Automation.Language.Parser]::ParseInput($source,[ref]$tokens,[ref]$parseErrors)
if($parseErrors.Count -ne 0){throw 'Canonical bridge PowerShell parse failed'}
$match=[regex]::Match($source,"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $match.Success){throw 'Canonical native helpers missing'}
Add-Type -TypeDefinition $match.Groups[1].Value -ReferencedAssemblies @([Accessibility.IAccessible].Assembly.Location)
function Select-FixtureItem([long]$OwnerHandle,[long]$Handle,[int]$Item,[string]$Label) {
    [TejBridgeNative]::SelectListItem($OwnerHandle,$Handle,$Item,$Label)
}
Add-Type -TypeDefinition @'
using System;
using System.Threading;
using System.Windows.Forms;
public static class TejListFixture {
    private class PassiveForm:Form {
        protected override bool ShowWithoutActivation {get{return true;}}
        protected override CreateParams CreateParams {get{var p=base.CreateParams;p.ExStyle|=0x08000000;return p;}}
    }
    public static Form Root,Other;
    public static ListBox Labels,Target;
    public static ComboBox Names;
    public static Button Select;
    public static int Changes,SelectClicks;
    public static bool DisableAfterSelect;
    private static Thread thread;
    public static void Start() {
        var ready=new ManualResetEvent(false);
        thread=new Thread(()=> {
            Root=new PassiveForm {Text="StockAgent owned list fixture",Width=320,Height=200};
            var group=new GroupBox {Text="Owned source list",Dock=DockStyle.Fill};
            Labels=new ListBox {Dock=DockStyle.Top,Height=80};
            Labels.Items.AddRange(new object[]{"First","Second","Third"});
            Names=new ComboBox {Dock=DockStyle.Top,DropDownStyle=ComboBoxStyle.DropDownList};
            Names.Items.AddRange(new object[]{"First","first","\u53f0\u7063 caf\u00e9"});Names.SelectedIndex=0;
            Target=new ListBox {Dock=DockStyle.Fill};
            Labels.SelectedIndexChanged+=(s,e)=>Changes++;
            Select=new Button {Text="Select",Dock=DockStyle.Bottom};
            Select.Click+=(s,e)=> {
                SelectClicks++;
                foreach(object item in Labels.SelectedItems)Target.Items.Add(item);
                if(DisableAfterSelect)Select.Enabled=false;
            };
            group.Controls.Add(Target);group.Controls.Add(Labels);group.Controls.Add(Names);group.Controls.Add(Select);Root.Controls.Add(group);
            Root.Shown+=(s,e)=>ready.Set();Application.Run(Root);
        });
        thread.IsBackground=true;thread.SetApartmentState(ApartmentState.STA);thread.Start();
        if(!ready.WaitOne(10000))throw new Exception("Owned fixture startup timed out");ready.Dispose();
    }
    public static void Mode(string mode,bool bound) {
        Root.Invoke(new Action(()=> {
            Labels.DataSource=null;Labels.Items.Clear();
            if(bound)Labels.DataSource=new string[]{"First","Second","Third"};
            else Labels.Items.AddRange(new object[]{"First","Second","Third"});
            Labels.Enabled=true;Labels.SelectionMode=(SelectionMode)Enum.Parse(typeof(SelectionMode),mode);
            Labels.ClearSelected();Labels.SetSelected(0,true);
            // Force the managed collection to observe the old selection before
            // cross-process native events, as a source event handler may do.
            int ignored=Labels.SelectedItems.Count;Changes=0;SelectClicks=0;
        }));
    }
    public static int[] Selected() {
        return (int[])Root.Invoke(new Func<int[]>(()=> {
            var items=new int[Labels.SelectedIndices.Count];Labels.SelectedIndices.CopyTo(items,0);return items;
        }));
    }
    public static void Disable() {Root.Invoke(new Action(()=>Labels.Enabled=false));}
    public static void DuplicateCombo() {Root.Invoke(new Action(()=>Names.Items.Add("First")));}
    public static void LookupLabels(string[] labels) {
        Root.Invoke(new Action(()=> {Labels.DataSource=null;Labels.Items.Clear();Labels.Items.AddRange(labels);Labels.ClearSelected();}));
    }
    public static void SelectCombo(int index) {Root.Invoke(new Action(()=>Names.SelectedIndex=index));}
    public static void BatchReset(bool disableAfterSelect) {
        Root.Invoke(new Action(()=> {Target.Items.Clear();Select.Dock=DockStyle.Bottom;Select.Enabled=true;SelectClicks=0;DisableAfterSelect=disableAfterSelect;}));
    }
    public static void WrongGeometry() {Root.Invoke(new Action(()=>{Select.Dock=DockStyle.None;Select.SetBounds(0,0,80,20);}));}
    public static void EnableRoot(bool enabled) {Root.Invoke(new Action(()=>Root.Enabled=enabled));}
    public static long ShowOther() {
        return (long)Root.Invoke(new Func<long>(()=> {
            Other=new PassiveForm {Text="StockAgent owned foreground fixture",Width=250,Height=140};
            Other.Show();return Other.Handle.ToInt64();
        }));
    }
    public static void Stop() {
        if(Root!=null&&!Root.IsDisposed)Root.Invoke(new Action(()=>{if(Other!=null)Other.Close();Root.Close();}));
        if(thread!=null&&!thread.Join(5000))throw new Exception("Owned fixture did not stop");
    }
}
'@ -ReferencedAssemblies @([Windows.Forms.Form].Assembly.Location)
$checks=[Collections.Generic.List[object]]::new();$failure=$null;$rootHandle=0;$listHandle=0
$before=[TejBridgeNative]::GetForegroundWindow().ToInt64()
function Check([string]$name,[bool]$ok) {
    $checks.Add(@{name=$name;accepted=$ok});if(-not $ok){throw ('List fixture failed: '+$name)}
}
try {
    [TejListFixture]::Start();$rootHandle=[TejListFixture]::Root.Handle.ToInt64()
    $listHandle=[TejListFixture]::Labels.Handle.ToInt64();[void][TejListFixture]::ShowOther()
    $foreground=[TejBridgeNative]::GetForegroundWindow()
    [TejBridgeNative]::AssertControlScope($rootHandle,$listHandle,$PID,'StockAgent owned list fixture')
    Check 'fresh_native_root_and_control_owner_verified' $true
    Check 'native_group_caption_resolves_exact_owned_groupbox' ([TejBridgeNative]::ExactQueryGroup(
        $rootHandle,'Owned source list',$PID,'StockAgent owned list fixture') -eq [TejBridgeNative]::GetParent([IntPtr]$listHandle).ToInt64())
    $rejected=$false;try{[void][TejBridgeNative]::ExactQueryGroup($rootHandle,'Missing',$PID,'StockAgent owned list fixture')}catch{$rejected=$true}
    Check 'native_group_missing_name_refused' $rejected
    foreach($case in @(@($rootHandle,$listHandle,$PID+1,'StockAgent owned list fixture'),
                      @($rootHandle,$listHandle,$PID,'Wrong owner title'),
                      @($rootHandle,[TejListFixture]::Other.Handle.ToInt64(),$PID,'StockAgent owned list fixture'))) {
        $rejected=$false;try{[TejBridgeNative]::AssertControlScope($case[0],$case[1],$case[2],$case[3])}catch{$rejected=$true}
        Check 'wrong_pid_caption_or_child_scope_refused' $rejected
    }
    $combo=[TejListFixture]::Names.Handle.ToInt64()
    Check 'single_selected_combo_text_matches_full_catalog' (
        [TejBridgeNative]::SelectedComboText($rootHandle,$combo) -ceq ([TejBridgeNative]::Items($combo,$true))[0])
    Check 'native_combo_lookup_preserves_exact_uppercase' ([TejBridgeNative]::ExactComboIndex($rootHandle,$combo,'First') -eq 0)
    Check 'native_combo_lookup_preserves_exact_lowercase' ([TejBridgeNative]::ExactComboIndex($rootHandle,$combo,'first') -eq 1)
    $unicode=[string][char]0x53f0+[char]0x7063+' caf'+[char]0x00e9
    Check 'native_combo_lookup_preserves_unicode' ([TejBridgeNative]::ExactComboIndex($rootHandle,$combo,$unicode) -eq 2)
    Check 'native_combo_lookup_missing_label_is_not_selected' ([TejBridgeNative]::ExactComboIndex($rootHandle,$combo,'Missing') -eq -1)
    Check 'native_combo_lookup_never_mutates_selection' ([TejBridgeNative]::Message($combo,0x147,0,0) -eq 0)
    Check 'zero_extended_32bit_cb_err_is_negative' ([TejBridgeNative]::SignedControlResult(4294967295) -eq -1)
    Check 'native_negative_cb_err_preserved' ([TejBridgeNative]::SignedControlResult(-1) -eq -1)
    Check 'valid_index_and_large_handle_not_truncated' (
        [TejBridgeNative]::SignedControlResult(23) -eq 23 -and [TejBridgeNative]::SignedControlResult(4294967296) -eq 4294967296)
    [TejListFixture]::SelectCombo(-1)
    Check 'no_selection_is_null_not_out_of_scope_exception' ($null -eq [TejBridgeNative]::SelectedComboText($rootHandle,$combo))
    [TejListFixture]::SelectCombo(0)
    [TejListFixture]::DuplicateCombo();$rejected=$false
    try{[void][TejBridgeNative]::ExactComboIndex($rootHandle,$combo,'First')}catch{$rejected=$true}
    Check 'duplicate_exact_combo_labels_refused' $rejected
    [TejListFixture]::EnableRoot($false)
    Check 'disabled_original_source_can_be_read_without_enabling' (
        [TejBridgeNative]::SelectedComboText($rootHandle,$combo) -ceq 'First' -and -not [TejBridgeNative]::IsWindowEnabled([IntPtr]$rootHandle))
    $rejected=$false;try{[void][TejBridgeNative]::ExactComboIndex($rootHandle,$combo,'First')}catch{$rejected=$true}
    Check 'disabled_original_source_is_not_mutation_permission' $rejected
    [TejListFixture]::EnableRoot($true)
    Check 'all_axis_equal_count_different_names_refused' (-not [TejBridgeNative]::SameItems(
        [string[]]@('First','Second'),[string[]]@('First','Other')))
    Check 'all_axis_duplicate_requested_names_refused' (-not [TejBridgeNative]::SameItems(
        [string[]]@('First','Second'),[string[]]@('First','First')))
    Check 'all_axis_duplicate_source_names_refused' (-not [TejBridgeNative]::SameItems(
        [string[]]@('First','First'),[string[]]@('First','Second')))
    foreach($bound in @($false,$true)) {
    foreach($mode in @('One','MultiSimple','MultiExtended')) {
        $case=$mode+$(if($bound){'_bound'}else{'_unbound'})
        [TejListFixture]::Mode($mode,$bound)
        # WinForms can recreate its native handle when SelectionMode changes.
        $listHandle=[TejListFixture]::Labels.Handle.ToInt64()
        Select-FixtureItem $rootHandle $listHandle 1 'Second'
        $indices=[TejListFixture]::Selected()
        Check ($case+'_managed_and_native_exact_selection') ($indices.Count -eq 1 -and $indices[0] -eq 1)
        $checks.Add(@{name=$case+'_observed_normal_notifications';accepted=$true;
            observed_notifications=[TejListFixture]::Changes})
        Check ($case+'_no_select_button_action') ([TejListFixture]::SelectClicks -eq 0)
        Check ($case+'_no_foreground_activation') ([TejBridgeNative]::GetForegroundWindow() -eq $foreground)
        Select-FixtureItem $rootHandle $listHandle 2 'Third'
        $indices=[TejListFixture]::Selected()
        Check ($case+'_subsequent_exact_selection') ($indices.Count -eq 1 -and $indices[0] -eq 2)
        $rejected=$false
        try {Select-FixtureItem $rootHandle $listHandle 1 'Wrong label'}catch {$rejected=$true}
        Check ($case+'_wrong_label_rejected_before_action') ($rejected -and [TejListFixture]::SelectClicks -eq 0)
    }
    }
    [TejListFixture]::LookupLabels([string[]]@('first','First','台灣 café'))
    $listHandle=[TejListFixture]::Labels.Handle.ToInt64()
    Check 'exact_list_case_variant_uses_ordinal_name' ([TejBridgeNative]::ExactListIndex($rootHandle,$listHandle,'First') -eq 1)
    Check 'exact_list_unicode_name_is_not_a_prefix' ([TejBridgeNative]::ExactListIndex($rootHandle,$listHandle,'台灣 café') -eq 2)
    $rejected=$false;try{[void][TejBridgeNative]::ExactListIndex($rootHandle,$listHandle,'Fir')}catch{$rejected=$true}
    Check 'list_prefix_is_not_an_exact_source_name' $rejected
    [TejListFixture]::LookupLabels([string[]]@('First','Other','First'))
    $rejected=$false;try{[void][TejBridgeNative]::ExactListIndex($rootHandle,$listHandle,'First')}catch{$rejected=$true}
    Check 'duplicate_exact_list_source_name_is_refused' $rejected
    [TejListFixture]::LookupLabels([string[]]@('first','First','台灣 café'))
    $listHandle=[TejListFixture]::Labels.Handle.ToInt64()
    $indices=[TejBridgeNative]::ExactListIndices($rootHandle,$listHandle,[string[]]@('First','first','台灣 café'),$PID,'StockAgent owned list fixture')
    Check 'batch_search_keeps_ordinal_unicode_and_request_order' (($indices -join ',') -ceq '1,0,2')
    $rejected=$false;try{[void][TejBridgeNative]::ExactListIndices($rootHandle,$listHandle,[string[]]@('First','First'),$PID,'StockAgent owned list fixture')}catch{$rejected=$true}
    Check 'batch_search_duplicate_request_refused_without_selection' ($rejected -and [TejListFixture]::SelectClicks -eq 0)
    [TejListFixture]::BatchReset($false)
    $target=[TejListFixture]::Target.Handle.ToInt64();$batchButton=[TejListFixture]::Select.Handle.ToInt64()
    $parent=[TejBridgeNative]::GetParent([IntPtr]$batchButton).ToInt64()
    [TejBridgeNative]::SelectListBatch($rootHandle,$parent,$listHandle,$target,$batchButton,
        [int[]]$indices,[string[]]@('First','first','台灣 café'),$PID,'StockAgent owned list fixture',$false)
    Check 'batch_selection_reads_complete_normal_model_destination' ([TejBridgeNative]::SameItems(
        [TejBridgeNative]::Items($target,$false),[string[]]@('First','first','台灣 café')) -and [TejListFixture]::SelectClicks -eq 3)
    Check 'batch_selection_does_not_activate_foreground' ([TejBridgeNative]::GetForegroundWindow() -eq $foreground)
    foreach($bad in @('wrong_parent','wrong_pid','wrong_label','wrong_geometry','duplicate_request')) {
        [TejListFixture]::BatchReset($false);$rejected=$false
        try {
            $batchParent=$(if($bad -ceq 'wrong_parent'){$target}else{$parent})
            $owner=$(if($bad -ceq 'wrong_pid'){$PID+1}else{$PID})
            $labels=[string[]]$(if($bad -ceq 'wrong_label'){@('Wrong')}elseif($bad -ceq 'duplicate_request'){@('First','First')}else{@('First')})
            $positions=[int[]]$(if($bad -ceq 'duplicate_request'){@(1,1)}else{@(1)})
            # Swap source/target for geometry refusal while both lists remain
            # real owned children. No click may be sent on a wrong layout.
            if($bad -ceq 'wrong_geometry'){[TejListFixture]::WrongGeometry()}
            [TejBridgeNative]::SelectListBatch($rootHandle,$batchParent,$listHandle,$target,$batchButton,
                $positions,$labels,$owner,'StockAgent owned list fixture',($bad -ceq 'wrong_geometry'))
        }catch{$rejected=$true}
        Check ('batch_'+$bad+'_refused_before_click') ($rejected -and [TejListFixture]::SelectClicks -eq 0)
    }
    [TejListFixture]::BatchReset($true);$rejected=$false
    try{[TejBridgeNative]::SelectListBatch($rootHandle,$parent,$listHandle,$target,$batchButton,
        [int[]]$indices,[string[]]@('First','first','台灣 café'),$PID,'StockAgent owned list fixture',$false)}catch{$rejected=$true}
    Check 'batch_midstream_disabled_button_stops_without_repeating' ($rejected -and [TejListFixture]::SelectClicks -eq 1 -and
        ([TejBridgeNative]::Items($target,$false)).Count -eq 1)
    [TejListFixture]::BatchReset($false)
    Check 'native_button_named_lookup_keeps_owned_identity' ([TejBridgeNative]::ExactButton($rootHandle,$rootHandle,'Select',[int]::MinValue,43) -eq [TejListFixture]::Select.Handle.ToInt64())
    $rejected=$false;try{[void][TejBridgeNative]::ExactButton($rootHandle,$rootHandle,'Select',[int]::MinValue,44)}catch{$rejected=$true}
    Check 'same_caption_wrong_control_role_is_refused' $rejected
    $rejected=$false;try{[void][TejBridgeNative]::ExactButton($rootHandle,$rootHandle,'Select',[int]::MaxValue)}catch{$rejected=$true}
    Check 'named_button_still_requires_correct_geometry' $rejected
    $button=[TejListFixture]::Select.Handle.ToInt64()
    [TejBridgeNative]::VerifyResolvedButton($rootHandle,$rootHandle,$button,$PID,'StockAgent owned list fixture','Select')
    Check 'batched_button_guard_does_not_send_an_action' ([TejListFixture]::SelectClicks -eq 0)
    foreach($case in @('wrong_name','wrong_pid','wrong_parent')){
        $rejected=$false
        try {
            $parent=$(if($case -ceq 'wrong_parent'){$listHandle}else{$rootHandle})
            $owner=$(if($case -ceq 'wrong_pid'){$PID+1}else{$PID})
            $caption=$(if($case -ceq 'wrong_name'){'Wrong'}else{'Select'})
            [TejBridgeNative]::VerifyResolvedButton($rootHandle,$parent,$button,$owner,'StockAgent owned list fixture',$caption)
        }catch{$rejected=$true}
        Check ('batched_button_guard_'+$case+'_refused') ($rejected -and [TejListFixture]::SelectClicks -eq 0)
    }
    Check 'direct_search_keeps_foreground_unchanged' ([TejBridgeNative]::GetForegroundWindow() -eq $foreground)
    [TejListFixture]::Disable();$listHandle=[TejListFixture]::Labels.Handle.ToInt64()
    $rejected=$false
    try {Select-FixtureItem $rootHandle $listHandle 0 'First'}catch {$rejected=$true}
    Check 'disabled_list_rejected' $rejected
}catch {
    $failure=$_.Exception.Message
    if($listHandle -ne 0 -and [TejBridgeNative]::IsWindow([IntPtr]$listHandle)) {
        $checks.Add(@{name='failure_selection_readback';accepted=$false;
            native_style=[TejBridgeNative]::GetWindowLong([IntPtr]$listHandle,-16);
            native_current=[TejBridgeNative]::Message($listHandle,0x188,0,0);
            native_selected_count=[TejBridgeNative]::Message($listHandle,0x190,0,0);
            managed_selected_indices=@([TejListFixture]::Selected())})
    }
}finally {
    [TejListFixture]::Stop()
}
$receipt=@{contract='owned_list_selection_fixture_v1';accepted=($null -eq $failure);
    observed_at_utc=[DateTime]::UtcNow.ToString('o');checks=$checks.ToArray();failure=$failure;
    provider_queries_sent=0;tej_or_excel_connected=$false;credentials_read=$false;
    bridge_sha256=(Get-FileHash -LiteralPath $BridgeScript).Hash.ToLowerInvariant()}
[IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
if($null -ne $failure){throw $failure}
Write-Output ('Owned list fixture passed '+$checks.Count+' checks; no provider query')
