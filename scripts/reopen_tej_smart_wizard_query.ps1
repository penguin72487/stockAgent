<#
Explicit operator recovery of ONLY the named scratch Smart Wizard form.
Uses normal window-close and the existing Excel Add-ins Database Settings
command. Never enables a disabled control, closes/saves a workbook, reads
credentials/comments, or submits Search/Preview. Restarting the exact add-in
process is a separately authorized, image/start-time-pinned operator action.
Every phase has its own immutable private receipt. An unresolved normal
launcher action is not repeated; a proven pre-action failure can be resumed.
#>
param(
    [Parameter(Mandatory=$true)][ValidateSet('close','open','inspect-open','inspect-addin','stop-addin')][string]$Action,
    [Parameter(Mandatory=$true)][string]$BridgeScript,
    [Parameter(Mandatory=$true)][string]$Output,
    [Parameter(Mandatory=$true)][int]$TejProcessId,
    [Parameter(Mandatory=$true)][long]$ExpectedWindow,
    [Parameter(Mandatory=$true)][string]$ExpectedTitle,
    [Parameter(Mandatory=$true)][string]$ExpectedWorkbook,
    [Parameter(Mandatory=$true)][long]$ExpectedExcelWindow,
    [switch]$AllowDiscardQuerySettings,
    [switch]$AllowRestartAddin,
    [string]$ExpectedProcessStartUtc='',
    [string]$ExpectedImageSha256='',
    [string]$ExpectedLaunchAtUtc=''
)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if(Test-Path -LiteralPath $Output){throw 'Refusing to overwrite operator recovery evidence'}
if(Test-Path -LiteralPath ($Output+'.submission.json')){throw 'Existing launcher intent; no action repeated'}
if(-not $AllowDiscardQuerySettings){throw 'Explicit scratch-query reset authorization required'}
if($Action -in @('inspect-addin','stop-addin') -and -not $AllowRestartAddin){throw 'Separate exact-addin restart authorization required'}
if($ExpectedTitle -cne ('TEJ Smart Wizard (Version 4.1.1.7) -- '+$ExpectedWorkbook)){throw 'Unreviewed wizard version/workbook'}
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
$source=[IO.File]::ReadAllText($BridgeScript)
$match=[regex]::Match($source,"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $match.Success){throw 'Canonical native helper declaration missing'}
Add-Type -TypeDefinition $match.Groups[1].Value -ReferencedAssemblies @(
    [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)
$mutex=[Threading.Mutex]::new($false,'Local\StockAgentTEJSmartWizardOwner')
$owns=$false;$excel=$null;$book=$null
try {
    $owns=$mutex.WaitOne(0)
    if(-not $owns){throw 'Another acquisition owns this desktop session'}
    $excel=[Runtime.InteropServices.Marshal]::GetActiveObject('Excel.Application')
    $book=$excel.Workbooks.Item($ExpectedWorkbook)
    if($null -eq $book -or $book.Windows.Count -ne 1 -or $book.Windows.Item(1).Hwnd -ne $ExpectedExcelWindow){throw 'Original workbook/window unavailable; no rebind'}
    # Excel may activate its other workbook when the independent add-in exits.
    # Restore ONLY the inspected original workbook view; no Save/Close/new file.
    $activatedOriginalWorkbook=$false
    if($excel.Hwnd -ne $ExpectedExcelWindow -or $excel.ActiveWorkbook.Name -cne $ExpectedWorkbook){
        $book.Activate();$activatedOriginalWorkbook=$true
    }
    function Assert-Workbook {
        if($excel.Hwnd -ne $ExpectedExcelWindow -or $null -eq $excel.ActiveWorkbook -or
           $excel.ActiveWorkbook.Name -cne $ExpectedWorkbook){throw 'Exact active Excel/workbook identity changed'}
    }
    Assert-Workbook
    $payload=@{contract=$(if($AllowRestartAddin){'explicit_exact_addin_restart_v1'}else{'explicit_scratch_query_lifecycle_v1'});action=$Action;
        observed_at_utc=[DateTime]::UtcNow.ToString('o');old_query_window=$ExpectedWindow;
        workbook_closed=$false;workbook_saved=$false;credentials_read=$false;
        market_data_query_submitted=$false;discard_scratch_query_settings_authorized=$true;
        original_workbook_activated=$activatedOriginalWorkbook}
    if($AllowRestartAddin){$payload.addin_process_restart_authorized=$true}
    if($Action -in @('inspect-addin','stop-addin')) {
        $root=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$ExpectedWindow)
        if($root.Current.Name -cne $ExpectedTitle -or $root.Current.ProcessId -ne $TejProcessId){throw 'Original query identity changed'}
        $process=Get-Process -Id $TejProcessId
        if($process.ProcessName -cne 'TEJAddin'){throw 'Only the exact independent TEJAddin may be restarted'}
        $processStart=$process.StartTime.ToUniversalTime().ToString('o')
        $image=$process.MainModule.FileName
        $imageHash=(Get-FileHash -LiteralPath $image -Algorithm SHA256).Hash
        $windows=@([TejBridgeNative]::VisibleProcessWindows($ExpectedWindow))
        $connectors=@(foreach($handle in $windows) {
            $candidate=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
            if($candidate.Current.Name -ceq 'TEJProConnector' -and $candidate.Current.ClassName -ceq '#32770' -and
               $candidate.Current.ProcessId -eq $TejProcessId){$handle}
        })
        if($connectors.Count -ne 1){throw 'Exact independent add-in connector identity unavailable'}
        $emptyConnectorDialogs=0;$scratchCloseConfirmations=0
        foreach($handle in $windows) {
            $window=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
            if($window.Current.ProcessId -ne $TejProcessId){throw 'Add-in window owner changed'}
            if($handle -eq $ExpectedWindow -or $handle -eq $connectors[0]){continue}
            # Legacy connector UIA can report unnamed Pane controls despite
            # populated native Static/Button captions. Inspect exact native
            # children instead of inferring a genuinely empty notice.
            if($window.Current.Name -cne '' -or $window.Current.ClassName -cne '#32770' -or
               [TejBridgeNative]::GetWindow([IntPtr]$handle,4).ToInt64() -ne $connectors[0]){throw 'Another query or unreviewed notice shares the add-in; restart refused'}
            $nativeChildren=@([TejBridgeNative]::Children($handle,''))
            if($nativeChildren.Count -eq 0){$emptyConnectorDialogs++;continue}
            $nativeTexts=@([TejBridgeNative]::Children($handle,'Static'))
            $nativeButtons=@([TejBridgeNative]::Children($handle,'Button'))
            if($nativeChildren.Count -ne 3 -or $nativeTexts.Count -ne 1 -or $nativeButtons.Count -ne 2){throw 'Unreviewed connector dialog layout; restart refused'}
            $question=([TejBridgeNative]::Text($nativeTexts[0])).Replace("`r`n",' ').Replace("`n",' ')
            $buttonCaptions=@($nativeButtons|ForEach-Object {[TejBridgeNative]::Text($_)})
            if($question -cne 'Had been setting conditions. Do you close form?' -or
               @($buttonCaptions|Where-Object {$_ -in @('Yes','&Yes')}).Count -ne 1 -or
               @($buttonCaptions|Where-Object {$_ -in @('No','&No')}).Count -ne 1){throw 'Unreviewed connector dialog captions; restart refused'}
            # No notice acknowledgement or auth/quota override. The operator
            # explicitly authorized restarting the exact inspected add-in.
            $scratchCloseConfirmations++
        }
        if($emptyConnectorDialogs+$scratchCloseConfirmations -gt 1){throw 'Unreviewed connector-dialog multiplicity; restart refused'}
        $payload.process_name=$process.ProcessName;$payload.process_id=$TejProcessId
        $payload.process_start_utc=$processStart;$payload.image_sha256=$imageHash
        $payload.root_enabled=$root.Current.IsEnabled
        $payload.empty_connector_owned_dialogs=$emptyConnectorDialogs
        $payload.connector_owned_scratch_close_confirmations=$scratchCloseConfirmations
        if($Action -eq 'stop-addin') {
            if($ExpectedProcessStartUtc -cne $processStart -or $ExpectedImageSha256 -cne $imageHash){throw 'Pinned add-in process identity changed; stop refused'}
            Assert-Workbook
            # The operator explicitly authorized this additional scope after
            # normal query-close failed. Stop the inspected process object,
            # never all processes by name and never Excel or TEJ Pro.
            Stop-Process -InputObject $process -Force
            if(-not $process.WaitForExit(5000)){throw 'Exact add-in stop unresolved; no repeat'}
            if([TejBridgeNative]::IsWindow([IntPtr]$ExpectedWindow)){throw 'Original query still exists after exact add-in stop'}
            Assert-Workbook
            $payload.addin_stopped_verified=$true
        }
    } elseif($Action -eq 'close') {
        $root=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$ExpectedWindow)
        if($root.Current.Name -cne $ExpectedTitle -or $root.Current.ProcessId -ne $TejProcessId){throw 'Original query identity changed'}
        $pendingCloseNotices=@([TejBridgeNative]::Dialogs($ExpectedWindow))
        if($pendingCloseNotices.Count -gt 1){throw 'Unresolved owned notices; do not close the query'}
        # WM_CLOSE is the normal window close request, retaining the vendor's
        # FormClosing/confirmation handling. The disabled form ignored the
        # system-menu SC_CLOSE in the first operator attempt. Never destroy,
        # force-enable or kill the form/process; no repeated close here.
        $payload.close_method='owned_normal_wm_close_once_v1'
        if($pendingCloseNotices.Count -eq 0) {
            if(-not [TejBridgeNative]::PostMessageW([IntPtr]$ExpectedWindow,0x10,[IntPtr]::Zero,[IntPtr]::Zero)){throw 'Normal close not accepted; no repeat'}
        } else {$payload.close_method='adopt_exact_owned_close_confirmation_without_close_replay_v1'}
        $acknowledged=$false;$deadline=[DateTime]::UtcNow.AddSeconds(12)
        while([TejBridgeNative]::IsWindow([IntPtr]$ExpectedWindow) -and [DateTime]::UtcNow -lt $deadline) {
            $dialogs=@([TejBridgeNative]::Dialogs($ExpectedWindow))
            if($dialogs.Count -gt 0) {
                if($acknowledged -or $dialogs.Count -ne 1){throw 'Unreviewed close notice; nothing acknowledged'}
                $dialog=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$dialogs[0])
                $nodes=$dialog.FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)
                if($nodes.Count -gt 16 -or $dialog.Current.ProcessId -ne $TejProcessId -or
                   [TejBridgeNative]::GetWindow([IntPtr]$dialogs[0],4).ToInt64() -ne $ExpectedWindow){throw 'Close confirmation scope changed'}
                $texts=@($nodes|Where-Object {$_.Current.ClassName -ceq 'Static' -and $_.Current.Name})
                $buttons=@($nodes|Where-Object {$_.Current.ClassName -ceq 'Button'})
                $yes=@($buttons|Where-Object {$_.Current.Name -in @('Yes','&Yes') -and $_.Current.IsEnabled})
                $no=@($buttons|Where-Object {$_.Current.Name -in @('No','&No') -and $_.Current.IsEnabled})
                $closeQuestion=$(if($texts.Count -eq 1){($texts[0].Current.Name).Replace("`r`n",' ').Replace("`n",' ')}else{''})
                if($texts.Count -ne 1 -or $closeQuestion -cne 'Had been setting conditions. Do you close form?' -or
                   $buttons.Count -ne 2 -or $yes.Count -ne 1 -or $no.Count -ne 1 -or
                   -not [TejBridgeNative]::IsChild([IntPtr]$dialogs[0],[IntPtr]$yes[0].Current.NativeWindowHandle)){throw 'Unreviewed close confirmation; nothing acknowledged'}
                $acknowledged=$true
                [void][TejBridgeNative]::Message($yes[0].Current.NativeWindowHandle,0xF5,0,0)
            }
            Start-Sleep -Milliseconds 100
        }
        if([TejBridgeNative]::IsWindow([IntPtr]$ExpectedWindow)){throw 'Original query did not close; normal close is not repeated'}
        Assert-Workbook
        $payload.query_closed_verified=$true;$payload.scratch_settings_confirmation_acknowledged=$acknowledged
    } else {
        if([TejBridgeNative]::IsWindow([IntPtr]$ExpectedWindow)){throw 'Original query still exists; do not create another query'}
        $excelRoot=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$ExpectedExcelWindow)
        if(-not $excelRoot.Current.IsEnabled){throw 'Excel interface unavailable'}
        if($Action -ne 'inspect-open') {
        $tabs=$excelRoot.FindAll([Windows.Automation.TreeScope]::Descendants,
            [Windows.Automation.PropertyCondition]::new([Windows.Automation.AutomationElement]::AutomationIdProperty,'TabAddIns'))
        if($tabs.Count -ne 1 -or $tabs[0].Current.Name -cne 'Add-ins' -or
           $tabs[0].Current.ClassName -cne 'NetUIRibbonTab' -or $tabs[0].Current.ProcessId -ne $excelRoot.Current.ProcessId -or
           -not $tabs[0].Current.IsEnabled){throw 'No unique normal Excel Add-ins tab'}
        $selection=$tabs[0].GetCurrentPattern([Windows.Automation.SelectionItemPattern]::Pattern)
        if(-not $selection.Current.IsSelected){$selection.Select()}
        $commands=$excelRoot.FindAll([Windows.Automation.TreeScope]::Descendants,
            [Windows.Automation.PropertyCondition]::new([Windows.Automation.AutomationElement]::NameProperty,'Database Settings'))
        if($commands.Count -ne 1 -or $commands[0].Current.ProcessId -ne $excelRoot.Current.ProcessId -or
           -not $commands[0].Current.IsEnabled -or $commands[0].Current.ControlType -ne [Windows.Automation.ControlType]::Button){throw 'No unique enabled Database Settings command'}
        Assert-Workbook
        $patterns=@($commands[0].GetSupportedPatterns())
        if([Windows.Automation.TogglePattern]::Pattern -in $patterns) {
            # The live Excel TEJ toolbar exposes Toggle, not Invoke. Select
            # the supported normal action before submitting it once; never
            # try another action after an exception/unknown outcome.
            $toggle=$commands[0].GetCurrentPattern([Windows.Automation.TogglePattern]::Pattern)
            if($toggle.Current.ToggleState -ne [Windows.Automation.ToggleState]::Off){throw 'Database Settings is already toggled; no action repeated'}
            $payload.open_method='owned_uia_toggle_once_v1'
            [IO.File]::WriteAllText(($Output+'.submission.json'),($payload|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
            $toggle.Toggle()
        } elseif([Windows.Automation.InvokePattern]::Pattern -in $patterns) {
            $payload.open_method='owned_uia_invoke_once_v1'
            [IO.File]::WriteAllText(($Output+'.submission.json'),($payload|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
            $commands[0].GetCurrentPattern([Windows.Automation.InvokePattern]::Pattern).Invoke()
        } else {throw 'No supported normal Database Settings action; nothing submitted'}
        } elseif(-not $ExpectedLaunchAtUtc) {throw 'Exact original launch time required; no launch repeated'}
        $deadline=[DateTime]::UtcNow.AddSeconds(12);$windows=@()
        do {
            Start-Sleep -Milliseconds 200
            $windows=@([Windows.Automation.AutomationElement]::RootElement.FindAll([Windows.Automation.TreeScope]::Children,
                [Windows.Automation.PropertyCondition]::new([Windows.Automation.AutomationElement]::NameProperty,$ExpectedTitle)))
        } while($windows.Count -eq 0 -and [DateTime]::UtcNow -lt $deadline)
        if($windows.Count -ne 1 -or
           -not $windows[0].Current.IsEnabled -or -not $windows[0].Current.ClassName.StartsWith('WindowsForms10.Window.',[StringComparison]::Ordinal)){throw 'Reopened query identity unavailable; no launch retry'}
        $newProcessId=$windows[0].Current.ProcessId
        $newProcess=Get-Process -Id $newProcessId
        $launchAt=$(if($Action -eq 'inspect-open'){[DateTime]::Parse($ExpectedLaunchAtUtc).ToUniversalTime()}else{[DateTime]::Parse($payload.observed_at_utc).ToUniversalTime()})
        $excelProcess=Get-Process -Id $excelRoot.Current.ProcessId
        if($newProcess.ProcessName -cne 'TEJAddin' -or $newProcess.SessionId -ne $excelProcess.SessionId){throw 'New query process/session not verified'}
        if($newProcessId -ne $TejProcessId -and -not $AllowRestartAddin) {
            if((Get-Process -Id $TejProcessId -ErrorAction SilentlyContinue) -or $newProcess.StartTime.ToUniversalTime() -lt $launchAt.AddSeconds(-5)) {
                throw 'Old process still exists or replacement predates this exact launch; no rebind'
            }
            $payload.process_recreated_after_query_close=$true
            $payload.process_name=$newProcess.ProcessName
            $payload.process_start_utc=$newProcess.StartTime.ToUniversalTime().ToString('o')
        }
        if($AllowRestartAddin) {
            $newProcess=Get-Process -Id $newProcessId
            if($newProcess.ProcessName -cne 'TEJAddin' -or $newProcessId -eq $TejProcessId -or
               (Get-FileHash -LiteralPath $newProcess.MainModule.FileName -Algorithm SHA256).Hash -cne $ExpectedImageSha256){throw 'New add-in image/identity not verified'}
            $payload.image_sha256=$ExpectedImageSha256
        }
        Assert-Workbook
        $payload.query_open_verified=$true
        $payload.new_session=@{TejProcessId=$newProcessId;ExpectedWindow=[long]$windows[0].Current.NativeWindowHandle;
            ExpectedTitle=$ExpectedTitle;ExpectedWorkbook=$ExpectedWorkbook;ExpectedExcelWindow=$ExpectedExcelWindow}
    }
    [IO.File]::WriteAllText($Output,($payload|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
    Write-Output 'Exact scratch-query lifecycle phase verified; workbook retained; no data query'
} finally {
    foreach($o in @($book,$excel)){if($null -ne $o -and [Runtime.InteropServices.Marshal]::IsComObject($o)){[void][Runtime.InteropServices.Marshal]::ReleaseComObject($o)}}
    if($owns){$mutex.ReleaseMutex()};$mutex.Dispose()
}
