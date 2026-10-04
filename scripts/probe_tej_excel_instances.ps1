<# Bounded independent Excel capacity probe. Only a NEW hidden, empty Excel
instance may be closed. Preserve the exact original workbook and TEJ query.
No Wizard launcher, query, login, add-in connection or source data access.
#>
param([Parameter(Mandatory=$true)][string]$BridgeScript,
      [Parameter(Mandatory=$true)][string]$Session,
      [Parameter(Mandatory=$true)][string]$Output)
$ErrorActionPreference='Stop'
if(Test-Path -LiteralPath $Output){throw 'Retain original instance probe evidence'}
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
$source=[IO.File]::ReadAllText($BridgeScript)
$native=[regex]::Match($source,"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@")
if(-not $native.Success){throw 'Canonical native helpers required'}
Add-Type -TypeDefinition $native.Groups[1].Value -ReferencedAssemblies @(
    [Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)
$pin=Get-Content -LiteralPath $Session -Raw -Encoding UTF8|ConvertFrom-Json
$mutex=[Threading.Mutex]::new($false,'Local\StockAgentTEJSmartWizardOwner');$owns=$false
$original=$null;$book=$null;$created=$null;$createdOwned=$false;$closed=$false;$failure=$null
$receipt=@{contract='owned_empty_independent_excel_capacity_probe_v1';provider_queries_sent=0;
    source_rows_read=0;source_workbook_modified=$false;wizard_launched=$false;
    independent_tej_sessions_proven=$false;credentials_read=$false;original_instance_preserved=$false}
try {
    $owns=$mutex.WaitOne(0);if(-not $owns){throw 'Another acquisition owns the desktop'}
    [TejBridgeNative]::AssertControlScope($pin.ExpectedWindow,$pin.ExpectedWindow,$pin.TejProcessId,$pin.ExpectedTitle)
    $original=[Runtime.InteropServices.Marshal]::GetActiveObject('Excel.Application')
    $book=$original.Workbooks.Item($pin.ExpectedWorkbook)
    if($book.Windows.Count -ne 1 -or $book.Windows.Item(1).Hwnd -ne $pin.ExpectedExcelWindow){throw 'Original workbook pin changed'}
    $originalPid=[uint32]0;[void][TejBridgeNative]::GetWindowThreadProcessId([IntPtr]$pin.ExpectedExcelWindow,[ref]$originalPid)
    $beforeAddin=@(Get-Process -Name TEJAddin -ErrorAction SilentlyContinue|ForEach-Object {$_.Id})
    $created=New-Object -ComObject Excel.Application
    $createdPid=[uint32]0;[void][TejBridgeNative]::GetWindowThreadProcessId([IntPtr]$created.Hwnd,[ref]$createdPid)
    $receipt.original_excel_pid=$originalPid;$receipt.new_excel_pid=$createdPid
    $createdOwned=$createdPid -gt 0 -and $createdPid -ne $originalPid
    $receipt.independent_excel_pid=$createdOwned
    if(-not $createdOwned){throw 'COM did not create an independently owned Excel; original will not be closed'}
    if($created.Workbooks.Count -ne 0){throw 'New Excel contains a workbook; do not close unknown work'}
    $connected=0;$installed=0
    foreach($addin in $created.COMAddIns){
        if(([string]$addin.ProgId+' '+[string]$addin.Description) -match '(?i)TEJ'){$installed++;if($addin.Connect){$connected++}}
        [void][Runtime.InteropServices.Marshal]::ReleaseComObject($addin)
    }
    $receipt.new_excel_tej_com_addins=$installed;$receipt.new_excel_connected_tej_com_addins=$connected
    $afterAddin=@(Get-Process -Name TEJAddin -ErrorAction SilentlyContinue|ForEach-Object {$_.Id})
    $receipt.external_addin_processes_before=$beforeAddin.Count;$receipt.external_addin_processes_after=$afterAddin.Count
    $receipt.external_addin_process_identity_unchanged=(($beforeAddin|Sort-Object) -join ',') -ceq (($afterAddin|Sort-Object) -join ',')
    $created.Quit();$closed=$true
    [TejBridgeNative]::AssertControlScope($pin.ExpectedWindow,$pin.ExpectedWindow,$pin.TejProcessId,$pin.ExpectedTitle)
    if($book.Windows.Item(1).Hwnd -ne $pin.ExpectedExcelWindow){throw 'Original workbook changed after probe'}
    $receipt.original_instance_preserved=$true
}catch{$failure=$_.Exception.Message}finally {
    if($createdOwned -and -not $closed -and $null -ne $created){
        # Only an empty independently pinned process can be disposed here.
        if($created.Workbooks.Count -eq 0){$created.Quit();$closed=$true}
    }
    foreach($com in @($created,$book,$original)) {
        if($null -ne $com -and [Runtime.InteropServices.Marshal]::IsComObject($com)){[void][Runtime.InteropServices.Marshal]::ReleaseComObject($com)}
    }
    if($owns){$mutex.ReleaseMutex()};$mutex.Dispose()
}
$receipt.accepted=($null -eq $failure);$receipt.failure=$failure;$receipt.new_empty_instance_closed=$closed
$receipt.observed_at_utc=[DateTime]::UtcNow.ToString('o')
[IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
if($failure){throw $failure}
