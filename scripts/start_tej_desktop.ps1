<# Restart only the explicitly owned acquisition desktop after user logon.
No credentials, source query, global process kill, mouse or clipboard access.
Run under the logged-in user's Interactive token, never SYSTEM/S4U Session 0.
#>
param(
    [Parameter(Mandatory=$true)][string]$BridgeScript,
    [Parameter(Mandatory=$true)][string]$Session,
    [Parameter(Mandatory=$true)][string]$OwnedWorkbook,
    [Parameter(Mandatory=$true)][string]$Output,
    [Parameter(Mandatory=$true)][int]$WindowsSessionId
)
$ErrorActionPreference='Stop'
$OwnedWorkbook=[Environment]::ExpandEnvironmentVariables($OwnedWorkbook)
$ownedRoot=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'StockAgent\TEJSmartWizard'
if([IO.Path]::GetFullPath($OwnedWorkbook) -cne (Join-Path $ownedRoot 'StockAgent-TEJ-Acquisition.xlsx')){
    throw 'Only the explicitly owned empty acquisition workbook may be created'
}
if(Test-Path -LiteralPath $Output){throw 'Retain desktop startup evidence'}
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
$native=[regex]::Match([IO.File]::ReadAllText($BridgeScript),"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $native.Success){throw 'Canonical desktop helpers required'}
Add-Type -TypeDefinition $native.Groups[1].Value -ReferencedAssemblies @(
    [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)
$mutex=[Threading.Mutex]::new($false,'Local\StockAgentTEJSmartWizardOwner')
$owns=$false;$excel=$null;$book=$null
$proof=@{contract='owned_interactive_tej_desktop_startup_v1';state='desktop_startup_unverified';
    observed_at_utc=[DateTime]::UtcNow.ToString('o');windows_session_id=[Diagnostics.Process]::GetCurrentProcess().SessionId;
    windows_boot_at_utc=(Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString('o');
    provider_queries_sent=0;credentials_read=$false;mouse_used=$false;user_workbooks_closed=$false;
    retired_window_absent=$false;retired_process_absent=$false;new_session=$null}
$intent=$OwnedWorkbook+'.'+([DateTime]::Parse($proof.windows_boot_at_utc).Ticks)+'.launcher.json'
function Same-Session($a,$b) {
    if($null -eq $a -or $null -eq $b){return $false}
    foreach($key in @('TejProcessId','ExpectedWindow','ExpectedTitle','ExpectedWorkbook','ExpectedExcelWindow')){
        if($a.$key -cne $b.$key){return $false}
    }
    return $true
}
try {
    if($proof.windows_session_id -le 0 -or $proof.windows_session_id -ne $WindowsSessionId){throw 'interactive_session_required'}
    $owns=$mutex.WaitOne(0);if(-not $owns){throw 'desktop_writer_active'}
    $old=Get-Content -LiteralPath $Session -Raw -Encoding UTF8|ConvertFrom-Json
    $oldWindow=[TejBridgeNative]::IsWindow([IntPtr]$old.ExpectedWindow)
    $oldProcess=Get-Process -Id $old.TejProcessId -ErrorAction SilentlyContinue
    $proof.retired_session=$old
    $proof.retired_window_absent=-not $oldWindow
    $proof.retired_process_absent=$null -eq $oldProcess
    if($oldWindow) {
        [TejBridgeNative]::AssertControlScope($old.ExpectedWindow,$old.ExpectedWindow,$old.TejProcessId,$old.ExpectedTitle)
        if(-not [TejBridgeNative]::IsWindowEnabled([IntPtr]$old.ExpectedWindow) -or
           [TejBridgeNative]::IsHungAppWindow([IntPtr]$old.ExpectedWindow) -or
           [TejBridgeNative]::Dialogs($old.ExpectedWindow).Length -ne 0){throw 'owned_query_needs_reconciliation'}
        $proof.new_session=$old;$proof.state='desktop_ready';$proof.existing_session_preserved=$true
    } else {
        # Do not adopt an unrelated form or reuse a numeric PID after reboot.
        if($null -ne $oldProcess -and $oldProcess.ProcessName -ceq 'TEJAddin'){throw 'retired_addin_still_present'}
        $titles=@(('TEJ Smart Wizard (Version 4.1.1.7) -- '+[IO.Path]::GetFileName($OwnedWorkbook)),
                  ('TEJ Smart Wizard (Version 4.1.1.7) -- '+$OwnedWorkbook))
        function Owned-Forms {
            return @(foreach($caption in $titles){
                [Windows.Automation.AutomationElement]::RootElement.FindAll([Windows.Automation.TreeScope]::Children,
                    [Windows.Automation.PropertyCondition]::new([Windows.Automation.AutomationElement]::NameProperty,$caption))
            })
        }
        $forms=Owned-Forms
        try{$excel=[Runtime.InteropServices.Marshal]::GetActiveObject('Excel.Application')}catch{$excel=$null}
        if($null -eq $excel){$excel=New-Object -ComObject Excel.Application;$excel.Visible=$true;$proof.excel_created=$true}
        foreach($candidate in $excel.Workbooks) {
            if($candidate.FullName -ceq $OwnedWorkbook){$book=$candidate;break}
            [void][Runtime.InteropServices.Marshal]::ReleaseComObject($candidate)
        }
        if($null -eq $book) {
            if($forms.Count -gt 0){throw 'owned_workbook_context_unavailable'}
            if(Test-Path -LiteralPath $OwnedWorkbook){$book=$excel.Workbooks.Open($OwnedWorkbook)}
            else {
                [void][IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($OwnedWorkbook))
                $book=$excel.Workbooks.Add();$book.SaveAs($OwnedWorkbook,51);$proof.empty_owned_workbook_created=$true
            }
        }
        if($book.FullName -cne $OwnedWorkbook -or $book.Windows.Count -ne 1){throw 'owned_workbook_identity_changed'}
        if($forms.Count -eq 0) {
            # A durable one-shot launcher intent blocks repetition after a
            # timeout in the same Windows boot. A new boot cannot retain it.
            $history=@()
            if(Test-Path -LiteralPath $intent) {
                $previous=Get-Content -LiteralPath $intent -Raw|ConvertFrom-Json
                if($previous.windows_boot_at_utc -cne $proof.windows_boot_at_utc -or
                   $previous.workbook -cne $OwnedWorkbook -or $previous.state -cne 'confirmed' -or
                   -not (Same-Session $previous.new_session $old) -or
                   -not $proof.retired_window_absent -or -not $proof.retired_process_absent){
                    throw 'startup_launcher_outcome_unresolved'
                }
                $history=@($previous.launch_history)
                if($history.Count -eq 0){$history=@($previous.observed_at_utc)}
                $cutoff=[DateTime]::UtcNow.AddHours(-1)
                if(@($history|Where-Object{[DateTime]::Parse($_).ToUniversalTime() -gt $cutoff}).Count -ge 3){
                    throw 'desktop_restart_cooldown'
                }
            }
            $book.Activate()
            $excelRoot=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$book.Windows.Item(1).Hwnd)
            $tabs=$excelRoot.FindAll([Windows.Automation.TreeScope]::Descendants,
                [Windows.Automation.PropertyCondition]::new([Windows.Automation.AutomationElement]::AutomationIdProperty,'TabAddIns'))
            if($tabs.Count -ne 1 -or -not $tabs[0].Current.IsEnabled){throw 'tej_addin_not_ready'}
            $tab=$tabs[0].GetCurrentPattern([Windows.Automation.SelectionItemPattern]::Pattern)
            if(-not $tab.Current.IsSelected){$tab.Select()}
            $commands=$excelRoot.FindAll([Windows.Automation.TreeScope]::Descendants,
                [Windows.Automation.PropertyCondition]::new([Windows.Automation.AutomationElement]::NameProperty,'Database Settings'))
            if($commands.Count -ne 1 -or -not $commands[0].Current.IsEnabled -or
               $commands[0].Current.ProcessId -ne $excelRoot.Current.ProcessId){throw 'tej_addin_not_ready'}
            $patterns=@($commands[0].GetSupportedPatterns())
            if([Windows.Automation.TogglePattern]::Pattern -in $patterns) {
                $normal=$commands[0].GetCurrentPattern([Windows.Automation.TogglePattern]::Pattern)
                if($normal.Current.ToggleState -ne [Windows.Automation.ToggleState]::Off){throw 'startup_launcher_outcome_unresolved'}
                $method='owned_uia_toggle_once_v1'
            } elseif([Windows.Automation.InvokePattern]::Pattern -in $patterns) {
                $normal=$commands[0].GetCurrentPattern([Windows.Automation.InvokePattern]::Pattern);$method='owned_uia_invoke_once_v1'
            } else {throw 'tej_addin_not_ready'}
            $launch=@{contract=$proof.contract;windows_boot_at_utc=$proof.windows_boot_at_utc;
                workbook=$OwnedWorkbook;observed_at_utc=[DateTime]::UtcNow.ToString('o');method=$method;provider_queries_sent=0;
                state='prepared';launch_history=@($history)+@([DateTime]::UtcNow.ToString('o'))}
            if(Test-Path -LiteralPath $intent){Copy-Item -LiteralPath $intent -Destination ($intent+'.'+[Guid]::NewGuid().ToString('N')+'.retained.json')}
            [IO.File]::WriteAllText($intent,($launch|ConvertTo-Json -Compress),[Text.UTF8Encoding]::new($false))
            if($method -ceq 'owned_uia_toggle_once_v1'){$normal.Toggle()}else{$normal.Invoke()}
            $proof.query_launcher_sent_once=$true
            $deadline=[DateTime]::UtcNow.AddSeconds(40)
            do {
                $forms=Owned-Forms
                if($forms.Count -eq 1){break};Start-Sleep -Milliseconds 100
            }while([DateTime]::UtcNow -lt $deadline)
        }
        if($forms.Count -ne 1 -or -not $forms[0].Current.IsEnabled){throw 'desktop_waiting_login_or_addin'}
        $process=Get-Process -Id $forms[0].Current.ProcessId
        if($process.ProcessName -cne 'TEJAddin' -or $process.SessionId -ne $WindowsSessionId -or
           -not $forms[0].Current.ClassName.StartsWith('WindowsForms10.Window.',[StringComparison]::Ordinal)){throw 'owned_query_identity_unverified'}
        $h=[long]$forms[0].Current.NativeWindowHandle
        $title=[string]$forms[0].Current.Name
        [TejBridgeNative]::AssertControlScope($h,$h,$process.Id,$title)
        if([TejBridgeNative]::Dialogs($h).Length -ne 0){throw 'desktop_waiting_login_or_addin'}
        $proof.new_session=@{TejProcessId=$process.Id;ExpectedWindow=$h;ExpectedTitle=$title;
            ExpectedWorkbook=$book.Name;ExpectedExcelWindow=[long]$book.Windows.Item(1).Hwnd}
        $proof.owned_workbook=$OwnedWorkbook;$proof.new_process_start_utc=$process.StartTime.ToUniversalTime().ToString('o')
        $proof.state='desktop_ready'
    }
    # A confirmed launch may be recovered again after its EXACT process and
    # window disappear. An unresolved launch is never blindly repeated.
    if($proof.state -ceq 'desktop_ready' -and $proof.new_session.ExpectedWorkbook -ceq [IO.Path]::GetFileName($OwnedWorkbook) -and
       (Test-Path -LiteralPath $intent)) {
        $confirmed=Get-Content -LiteralPath $intent -Raw|ConvertFrom-Json
        if($confirmed.workbook -cne $OwnedWorkbook -or $confirmed.windows_boot_at_utc -cne $proof.windows_boot_at_utc){
            throw 'startup_launcher_outcome_unresolved'
        }
        if(-not (Same-Session $confirmed.new_session $proof.new_session) -or $confirmed.state -cne 'confirmed'){
            Copy-Item -LiteralPath $intent -Destination ($intent+'.'+[Guid]::NewGuid().ToString('N')+'.retained.json')
            $confirmed|Add-Member -NotePropertyName state -NotePropertyValue 'confirmed' -Force
            $confirmed|Add-Member -NotePropertyName new_session -NotePropertyValue $proof.new_session -Force
            $confirmed|Add-Member -NotePropertyName confirmed_at_utc -NotePropertyValue ([DateTime]::UtcNow.ToString('o')) -Force
            [IO.File]::WriteAllText($intent,($confirmed|ConvertTo-Json -Depth 6 -Compress),[Text.UTF8Encoding]::new($false))
        }
    }
} catch {
    # No vendor text, usernames or credentials in a public status/log.
    $code=$_.Exception.Message
    $allowed=@('interactive_session_required','desktop_writer_active','owned_query_needs_reconciliation',
        'retired_addin_still_present','owned_workbook_context_unavailable','owned_workbook_identity_changed',
        'startup_launcher_outcome_unresolved','desktop_restart_cooldown','tej_addin_not_ready','desktop_waiting_login_or_addin','owned_query_identity_unverified')
    $proof.state=$(if($code -in $allowed){$code}else{'desktop_startup_unverified'})
    $proof.private_error=$code
} finally {
    $proof.finished_at_utc=[DateTime]::UtcNow.ToString('o')
    [IO.File]::WriteAllText($Output,($proof|ConvertTo-Json -Depth 6 -Compress),[Text.UTF8Encoding]::new($false))
    foreach($com in @($book,$excel)){if($null -ne $com -and [Runtime.InteropServices.Marshal]::IsComObject($com)){[void][Runtime.InteropServices.Marshal]::ReleaseComObject($com)}}
    if($owns){$mutex.ReleaseMutex()};$mutex.Dispose()
}
