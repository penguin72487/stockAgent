<#
Inventory the already-authorized Smart Wizard query's visible catalog.
Only Type/SmartID/Data selectors are changed and restored. No Preview, Excel
export, login, query-comment read, provider API-key read or market-data request
is initiated. Catalog metadata requests made internally by the vendor are not
measured. A visible field is not proof of a successful historical download.
#>
param(
    [Parameter(Mandatory=$true)][int]$TejProcessId,
    [Parameter(Mandatory=$true)][long]$ExpectedWindow,
    [Parameter(Mandatory=$true)][string]$ExpectedTitle,
    [Parameter(Mandatory=$true)][string]$Output,
    [string]$ResumeSource='',
    [int[]]$TypeIndices=@(),
    [switch]$NavigateMainPage,
    [switch]$CaptureQueryImage,
    [ValidateRange(1,5000)][int]$MaxBindings=2000
)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -TypeDefinition @'
using System;
using System.Text;
using System.Runtime.InteropServices;
public static class TejCatalogNative {
    [DllImport("user32.dll")] public static extern IntPtr SetThreadDpiAwarenessContext(IntPtr c);
    [DllImport("user32.dll")] private static extern bool IsWindowVisible(IntPtr h);
    [DllImport("user32.dll")] private static extern bool IsWindowEnabled(IntPtr h);
    [DllImport("user32.dll")] private static extern int GetWindowLong(IntPtr h,int index);
    public static void MainPage(long root,long tab,uint expectedPid) {
        // Native tab focus emits the normal page-selection notification.
        // No cursor, foreground request, coordinates or mouse fallback.
        IntPtr r=new IntPtr(root),t=new IntPtr(tab);uint owner;
        GetWindowThreadProcessId(t,out owner);
        if(owner!=expectedPid||!IsChild(r,t)||!IsWindowVisible(t)||!IsWindowEnabled(t)||
           Message(tab,0x1304,0,0)!=4||(GetWindowLong(t,-16)&0x100)!=0)throw new Exception("Unexpected tab owner/count/style");
        if(Message(tab,0x130B,0,0)==0)return;
        Message(tab,0x1330,0,0);
        for(int i=0;i<40;i++) {
            if(Message(tab,0x130B,0,0)==0)return;
            System.Threading.Thread.Sleep(50);
        }
        throw new Exception("Main Page selection unresolved; action not repeated");
    }
    [DllImport("user32.dll")] public static extern bool IsChild(IntPtr p,IntPtr c);
    [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr h,out uint p);
    [DllImport("user32.dll")] public static extern IntPtr GetParent(IntPtr h);
    [DllImport("user32.dll")] public static extern int GetDlgCtrlID(IntPtr h);
    [DllImport("user32.dll",CharSet=CharSet.Unicode)] public static extern int GetClassName(IntPtr h,StringBuilder s,int n);
    [StructLayout(LayoutKind.Sequential)] public struct Rect {public int Left,Top,Right,Bottom;}
    [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h,out Rect r);
    [DllImport("user32.dll")] public static extern bool PrintWindow(IntPtr h,IntPtr d,uint f);
    [DllImport("user32.dll",CharSet=CharSet.Unicode)] private static extern IntPtr SendMessageTimeoutW(IntPtr h,uint m,IntPtr w,IntPtr l,uint f,uint t,out IntPtr r);
    [DllImport("user32.dll",CharSet=CharSet.Unicode,EntryPoint="SendMessageTimeoutW")] private static extern IntPtr ReadText(IntPtr h,uint m,IntPtr w,StringBuilder l,uint f,uint t,out IntPtr r);
    public static long Message(long h,uint m,long w,long l) {
        IntPtr r; if(SendMessageTimeoutW(new IntPtr(h),m,new IntPtr(w),new IntPtr(l),2,30000,out r)==IntPtr.Zero)throw new TimeoutException("Catalog UI timed out");
        return r.ToInt64();
    }
    public static string[] Items(long h,bool combo) {
        long n=Message(h,combo?0x146u:0x18Bu,0,0);
        if(n<0||n>30000)throw new Exception("Unexpected catalog count");
        string[] v=new string[n];
        for(int i=0;i<n;i++) {
            long len=Message(h,combo?0x149u:0x18Au,i,0);
            if(len<0||len>4096)throw new Exception("Unexpected catalog label length");
            StringBuilder b=new StringBuilder((int)len+1); IntPtr r;
            if(ReadText(new IntPtr(h),combo?0x148u:0x189u,new IntPtr(i),b,2,10000,out r)==IntPtr.Zero)throw new TimeoutException("Catalog label timed out");
            v[i]=b.ToString();
        }
        return v;
    }
}
'@
if(Test-Path -LiteralPath $Output) {throw 'Refusing to overwrite existing catalog evidence'}
$root=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$ExpectedWindow)
if($root.Current.ProcessId -ne $TejProcessId -or $root.Current.Name -cne $ExpectedTitle -or
   $ExpectedTitle -notlike 'TEJ Smart Wizard*') {throw 'Unexpected Smart Wizard scope'}
if($CaptureQueryImage) {
    Add-Type -AssemblyName System.Drawing
    $queryRect=[TejCatalogNative+Rect]::new()
    if(-not [TejCatalogNative]::GetWindowRect([IntPtr]$ExpectedWindow,[ref]$queryRect)) {throw 'Unavailable query geometry'}
    $imageWidth=$queryRect.Right-$queryRect.Left
    $imageHeight=$queryRect.Bottom-$queryRect.Top
    if($imageWidth -lt 100 -or $imageWidth -gt 4000 -or $imageHeight -lt 100 -or $imageHeight -gt 2400) {throw 'Unexpected query geometry'}
    if([IO.Path]::GetExtension($Output) -cne '.png') {throw 'Query image requires a new PNG path'}
    $queryImage=[Drawing.Bitmap]::new($imageWidth,$imageHeight)
    $queryGraphics=[Drawing.Graphics]::FromImage($queryImage)
    try {
        $queryDc=$queryGraphics.GetHdc()
        try {if(-not [TejCatalogNative]::PrintWindow([IntPtr]$ExpectedWindow,$queryDc,0)) {throw 'Query image capture failed'}}
        finally {$queryGraphics.ReleaseHdc($queryDc)}
        $queryImage.Save($Output,[Drawing.Imaging.ImageFormat]::Png)
    } finally {$queryGraphics.Dispose();$queryImage.Dispose()}
    Write-Output 'Exact query image captured; no workbook or source request'
    exit
}
$condition=[Windows.Automation.PropertyCondition]::new([Windows.Automation.AutomationElement]::NameProperty,'Data Source')
$sources=$root.FindAll([Windows.Automation.TreeScope]::Descendants,$condition)
if($sources.Count -eq 0 -and $NavigateMainPage) {
    $queryNodes=$root.FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)
    $tabs=@($queryNodes | Where-Object {$_.Current.ClassName -like '*SysTabControl32*'})
    if($tabs.Count -ne 1 -or $tabs[0].Current.ProcessId -ne $TejProcessId -or
       -not [TejCatalogNative]::IsChild([IntPtr]$ExpectedWindow,[IntPtr]$tabs[0].Current.NativeWindowHandle)) {
        throw 'Unexpected query tab ownership'
    }
    [TejCatalogNative]::MainPage($ExpectedWindow,[long]$tabs[0].Current.NativeWindowHandle,[uint32]$TejProcessId)
    for($pageAttempt=0;$pageAttempt -lt 12;$pageAttempt++) {
        $sources=$root.FindAll([Windows.Automation.TreeScope]::Descendants,$condition)
        if($sources.Count -eq 1) {break}
        Start-Sleep -Milliseconds 250
    }
}
if($sources.Count -ne 1) {throw 'Select the Main Page; expected exactly one Data Source group'}
$source=$sources[0]
$children=$source.FindAll([Windows.Automation.TreeScope]::Children,[Windows.Automation.Condition]::TrueCondition)
function Find-Combo([string]$name) {
    $matches=@($children | Where-Object {$_.Current.ClassName -like '*COMBOBOX*' -and $_.Current.Name.Trim() -ceq $name})
    if($matches.Count -ne 1) {throw 'Unexpected catalog selector layout'}
    return [long]$matches[0].Current.NativeWindowHandle
}
$type=Find-Combo 'Type'
$smart=Find-Combo 'SmartID'
$data=Find-Combo 'Data'
$lists=@($children | Where-Object {$_.Current.ClassName -like '*LISTBOX*'})
if($lists.Count -ne 2) {throw 'Unexpected source/selected field layout'}
$positions=@(foreach($list in $lists) {
    $rect=[TejCatalogNative+Rect]::new()
    if(-not [TejCatalogNative]::GetWindowRect([IntPtr]$list.Current.NativeWindowHandle,[ref]$rect)) {throw 'Unavailable field geometry'}
    [pscustomobject]@{Left=$rect.Left;Handle=[long]$list.Current.NativeWindowHandle}
}) | Sort-Object Left
if($positions[0].Left -eq $positions[1].Left) {throw 'Ambiguous source field layout'}
$fields=$positions[0].Handle
function Assert-Scope([long]$handle) {
    $owner=[uint32]0
    [void][TejCatalogNative]::GetWindowThreadProcessId([IntPtr]$handle,[ref]$owner)
    if($owner -ne $TejProcessId -or -not [TejCatalogNative]::IsChild([IntPtr]$ExpectedWindow,[IntPtr]$handle)) {throw 'Catalog control changed owner'}
    if($root.Current.Name -cne $ExpectedTitle) {throw 'Query window changed identity'}
}
function Items([long]$handle,[bool]$combo=$true) {
    for($readAttempt=0;$readAttempt -lt 3;$readAttempt++) {
        Assert-Scope $handle
        try {return ,[TejCatalogNative]::Items($handle,$combo)} catch {
            if($_.Exception.InnerException -isnot [TimeoutException] -or $readAttempt -eq 2) {throw}
            # Retry only a read. Never resend a selector action on unknown outcome.
            Start-Sleep -Milliseconds 750
        }
    }
}
function Selected([long]$handle) {
    $labels=Items $handle
    $index=[TejCatalogNative]::Message($handle,0x147,0,0)
    if($index -lt 0 -or $index -ge $labels.Count) {return $null}
    return $labels[$index]
}
function Select-Label([long]$handle,[string]$label) {
    $labels=Items $handle
    $index=[Array]::IndexOf($labels,$label)
    if($index -lt 0) {throw 'Expected catalog item disappeared'}
    if((Selected $handle) -ceq $label) {return}
    [void][TejCatalogNative]::Message($handle,0x14E,$index,0)
    $parent=[TejCatalogNative]::GetParent([IntPtr]$handle).ToInt64()
    Assert-Scope $parent
    $command=([TejCatalogNative]::GetDlgCtrlID([IntPtr]$handle) -band 0xFFFF) -bor (1 -shl 16)
    [void][TejCatalogNative]::Message($parent,0x111,$command,$handle)
    if((Selected $handle) -cne $label) {throw 'Catalog selection did not apply'}
}
$original=@{Type=(Selected $type);Smart=(Selected $smart);Data=(Selected $data)}
$bindings=0
$types=Items $type
$selectedTypes=@($types)
if($TypeIndices.Count -gt 0) {
    if(@($TypeIndices | Where-Object {$_ -lt 0 -or $_ -ge $types.Count}).Count -gt 0) {throw 'Invalid type index'}
    $selectedTypes=@($TypeIndices | Select-Object -Unique | ForEach-Object {$types[$_]})
}
$retained=@{}
if($ResumeSource) {
    $resumeHeader=$null
    foreach($line in [IO.File]::ReadLines($ResumeSource)) {
        $old=$line | ConvertFrom-Json
        if(-not $resumeHeader) {
            if($old.record_kind -cne 'header' -or $old.provider -cne 'tej_smart_wizard' -or
               $old.contract_version -ne 1 -or (@($old.types) -join "`t") -cne (@($types) -join "`t")) {
                throw 'Unrecognized retained catalog scope'
            }
            $resumeHeader=$old
        }
        if($old.record_kind -eq 'binding') {
            if($old.field_count -ne @($old.fields).Count -or $old.type -cnotin $types -or
               -not $old.smart_id -or -not $old.table -or
               @($old.fields | Where-Object {$_ -isnot [string] -or -not $_}).Count -gt 0) {
                throw 'Inconsistent retained catalog evidence'
            }
            $retained[(@($old.type,$old.smart_id,$old.table) -join "`t")]= $old
        }
    }
    if(-not $resumeHeader) {throw 'Empty retained catalog evidence'}
}
$writer=[IO.StreamWriter]::new($Output,$false,[Text.UTF8Encoding]::new($false))
function Emit([object]$record) {
    $writer.WriteLine(($record | ConvertTo-Json -Compress -Depth 7))
    $writer.Flush()
}
$complete=$false
try {
    Emit @{contract_version=1;record_kind='header';observed_at_utc=[DateTime]::UtcNow.ToString('o');
        provider='tej_smart_wizard';extraction_method='scoped_win32_visible_catalog';types=@($types);
        resume_source_sha256=$(if($ResumeSource){(Get-FileHash -LiteralPath $ResumeSource -Algorithm SHA256).Hash}else{$null});
        selected_types=@($selectedTypes);
        market_data_requested=$false;catalog_provider_request_count=$null;login_or_credentials_read=$false}
    foreach($typeLabel in $selectedTypes) {
        $typeStart=$bindings
        Select-Label $type $typeLabel
        $smartLabels=Items $smart
        Write-Output ("catalog_type="+$typeLabel+";smart_ids="+$smartLabels.Count)
        if($smartLabels.Count -eq 0) {Emit @{record_kind='empty_type';type=$typeLabel}}
        foreach($smartLabel in $smartLabels) {
            Select-Label $smart $smartLabel
            $dataLabels=Items $data
            if($dataLabels.Count -eq 0) {Emit @{record_kind='empty_smart_id';type=$typeLabel;smart_id=$smartLabel}}
            foreach($dataLabel in $dataLabels) {
                if($bindings -ge $MaxBindings) {throw 'Catalog binding bound exceeded; evidence remains incomplete'}
                $bindingKey=@($typeLabel,$smartLabel,$dataLabel) -join "`t"
                if($retained.ContainsKey($bindingKey)) {
                    Emit $retained[$bindingKey]
                    $bindings++
                    continue
                }
                Select-Label $data $dataLabel
                $settled=$false
                $labels=Items $fields $false
                # Vendor catalog callbacks can finish after the selector message.
                # Read only stable labels owned by the exact requested selection.
                $stableReads=0
                for($attempt=0;$attempt -lt 20;$attempt++) {
                    Start-Sleep -Milliseconds 250
                    $again=Items $fields $false
                    $actual=@((Selected $type),(Selected $smart),(Selected $data))
                    if(($actual -join "`t") -cne $bindingKey) {
                        Write-Output ('catalog_selection_changed='+($actual -join '|'))
                        throw 'Catalog selection changed during read; refusing attribution'
                    }
                    if(($labels -join "`n") -ceq ($again -join "`n")) {$stableReads++}else{$stableReads=0}
                    if($stableReads -ge 2) {$settled=$true;break}
                    $labels=$again
                }
                if(-not $settled) {throw 'Catalog fields did not settle; evidence remains incomplete'}
                Emit @{record_kind='binding';type=$typeLabel;smart_id=$smartLabel;table=$dataLabel;
                    fields=@($labels);field_count=$labels.Count;observed_at_utc=[DateTime]::UtcNow.ToString('o');
                    entitlement='catalog_visible_historical_query_unverified';history_start=$null;history_end=$null}
                $bindings++
                Write-Output ("catalog_binding="+$bindings+";fields="+$labels.Count+";table="+$dataLabel)
            }
        }
        Emit @{record_kind='type_completion';type=$typeLabel;smart_id_count=$smartLabels.Count;
            bindings=($bindings-$typeStart);complete=$true}
    }
    $complete=$true
} finally {
    $restored=$false
    try {
        Select-Label $type $original.Type
        if($original.Smart) {Select-Label $smart $original.Smart}
        if($original.Data) {Select-Label $data $original.Data}
        $restored=$true
    } catch {Write-Warning 'Original catalog selection could not be restored; no workbook was closed'}
    Emit @{record_kind='completion';observed_at_utc=[DateTime]::UtcNow.ToString('o');
        complete=$complete;bindings=$bindings;type_count=$types.Count;selectors_restored=$restored;
        selected_type_count=$selectedTypes.Count;full_type_scan=($complete -and $selectedTypes.Count -eq $types.Count);
        historical_data_or_api_entitlement_proven=$false}
    $writer.Dispose()
}
