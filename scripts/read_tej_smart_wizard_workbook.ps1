<#
Read only a named, already-exported TEJ OHLCV worksheet. No login, provider
request, macro, workbook save or query-comment read is performed here.
Run from Windows PowerShell; stdout contains only market cells and provenance.
#>
param(
    [Parameter(Mandatory=$true)][string]$ExpectedWorkbook,
    [Parameter(Mandatory=$true)][long]$ExpectedExcelWindow,
    [string]$Worksheet='Sheet1',
    [ValidateRange(1,5000)][int]$MaxRows=100
)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
$OutputEncoding=[Console]::OutputEncoding
$excel=$null
$book=$null
$sheet=$null
$range=$null
try {
    $excel=[Runtime.InteropServices.Marshal]::GetActiveObject('Excel.Application')
    if($excel.Hwnd -ne $ExpectedExcelWindow) {throw 'Excel window is outside the explicitly selected scope'}
    $book=$excel.ActiveWorkbook
    if($null -eq $book -or $book.Name -ne $ExpectedWorkbook) {throw 'Active workbook does not match the explicitly selected scope'}
    $sheet=$book.Worksheets.Item($Worksheet)
    $range=$sheet.UsedRange
    $headers=@('CO_ID','Date','Open(NTD)','High(NTD)','Low(NTD)','Close(NTD)','Volume(1000S)','Amount(NTD1000)')
    if($range.Row -ne 1 -or $range.Column -ne 1 -or $range.Columns.Count -ne $headers.Count -or $range.Rows.Count -lt 2 -or $range.Rows.Count -gt $MaxRows+1) {throw 'Worksheet is outside the bounded OHLCV export contract'}
    $cells=$range.Value2
    for($c=1;$c -le $headers.Count;$c++) {
        if($cells.GetValue(1,$c) -cne $headers[$c-1]) {throw 'Unexpected schema or source units; no cells will be emitted'}
    }
    $rows=@()
    for($r=1;$r -le $range.Rows.Count;$r++) {
        $values=@()
        for($c=1;$c -le $headers.Count;$c++) {$values+=,$cells.GetValue($r,$c)}
        $rows+=,[object]$values
    }
    [pscustomobject]@{
        contract_version=1
        provider='tej_smart_wizard'
        extraction_method='excel_com_value2'
        observed_at_utc=[DateTime]::UtcNow.ToString('o')
        date_system=$(if($book.Date1904){'excel_1904'}else{'excel_1900'})
        workbook=$ExpectedWorkbook
        worksheet=$Worksheet
        cells=$rows
        query_comments_read=$false
        provider_requests_made=0
    } | ConvertTo-Json -Depth 5
} finally {
    foreach($comObject in @($range,$sheet,$book,$excel)) {
        if($null -ne $comObject -and [Runtime.InteropServices.Marshal]::IsComObject($comObject)) {[void][Runtime.InteropServices.Marshal]::ReleaseComObject($comObject)}
    }
}
