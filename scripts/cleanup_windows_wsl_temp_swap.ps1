[CmdletBinding()]
param(
    [switch]$Apply,
    [double]$MinimumAgeHours = 1.0,
    [string]$TempRoot = (Join-Path $env:LOCALAPPDATA "Temp"),
    [string]$ReceiptDirectory = (Join-Path $env:LOCALAPPDATA "StockAgent\maintenance\receipts")
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($MinimumAgeHours -lt 0) {
    throw "MinimumAgeHours must be non-negative"
}

$resolvedTempRoot = [System.IO.Path]::GetFullPath($TempRoot).TrimEnd("\")
$expectedTempRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $env:LOCALAPPDATA "Temp")
).TrimEnd("\")
if (-not $resolvedTempRoot.Equals(
    $expectedTempRoot,
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw "Refusing non-canonical Windows user Temp root: $resolvedTempRoot"
}
if (-not (Test-Path -LiteralPath $resolvedTempRoot -PathType Container)) {
    throw "Windows user Temp root does not exist: $resolvedTempRoot"
}

$activeVmIds = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
Get-CimInstance Win32_Process -Filter "Name='wslhost.exe'" |
    ForEach-Object {
        $commandLine = [string]$_.CommandLine
        foreach ($match in [regex]::Matches(
            $commandLine,
            '--vm-id\s+\{([0-9a-fA-F-]{36})\}'
        )) {
            [void]$activeVmIds.Add($match.Groups[1].Value)
        }
    }

$now = Get-Date
$drive = [System.IO.DriveInfo]::new(
    [System.IO.Path]::GetPathRoot($resolvedTempRoot)
)
$freeBefore = $drive.AvailableFreeSpace
$candidates = [System.Collections.Generic.List[object]]::new()
$skipped = [System.Collections.Generic.List[object]]::new()
$errors = [System.Collections.Generic.List[object]]::new()
$deleted = [System.Collections.Generic.List[object]]::new()

foreach ($directory in Get-ChildItem -LiteralPath $resolvedTempRoot -Directory -Force) {
    $parsedGuid = [Guid]::Empty
    if (-not [Guid]::TryParse($directory.Name, [ref]$parsedGuid)) {
        continue
    }
    $children = @(Get-ChildItem -LiteralPath $directory.FullName -Force)
    if ($children.Count -ne 1 -or $children[0].PSIsContainer -or
        $children[0].Name -ne "swap.vhdx") {
        $skipped.Add([pscustomobject]@{
            directory = $directory.FullName
            reason = "directory-is-not-a-single-swap-vhdx"
        })
        continue
    }

    $swap = $children[0]
    $ageHours = ($now - $swap.LastWriteTime).TotalHours
    if ($activeVmIds.Contains($parsedGuid.ToString())) {
        $skipped.Add([pscustomobject]@{
            directory = $directory.FullName
            bytes = $swap.Length
            reason = "active-wsl-vm-id"
        })
        continue
    }
    if ($ageHours -lt $MinimumAgeHours) {
        $skipped.Add([pscustomobject]@{
            directory = $directory.FullName
            bytes = $swap.Length
            reason = "younger-than-minimum-age"
        })
        continue
    }

    $stream = $null
    try {
        $stream = [System.IO.File]::Open(
            $swap.FullName,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
    }
    catch {
        $skipped.Add([pscustomobject]@{
            directory = $directory.FullName
            bytes = $swap.Length
            reason = "exclusive-open-blocked"
            error = $_.Exception.Message
        })
        continue
    }
    finally {
        if ($null -ne $stream) {
            $stream.Dispose()
        }
    }

    $candidate = [pscustomobject]@{
        directory = $directory.FullName
        swap_path = $swap.FullName
        bytes = [long]$swap.Length
        last_write_time = $swap.LastWriteTime.ToString("o")
        age_hours = $ageHours
    }
    $candidates.Add($candidate)
    if (-not $Apply) {
        continue
    }
    try {
        # Recheck the exact shape and exclusive lock immediately before removal.
        $latestChildren = @(Get-ChildItem -LiteralPath $directory.FullName -Force)
        if ($latestChildren.Count -ne 1 -or $latestChildren[0].PSIsContainer -or
            $latestChildren[0].Name -ne "swap.vhdx" -or
            [long]$latestChildren[0].Length -ne [long]$swap.Length -or
            $latestChildren[0].LastWriteTimeUtc -ne $swap.LastWriteTimeUtc) {
            throw "candidate changed after audit"
        }
        $recheck = [System.IO.File]::Open(
            $latestChildren[0].FullName,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
        $recheck.Dispose()
        Remove-Item -LiteralPath $directory.FullName -Recurse -Force
        $deleted.Add($candidate)
    }
    catch {
        $errors.Add([pscustomobject]@{
            directory = $directory.FullName
            bytes = $swap.Length
            error = $_.Exception.Message
        })
    }
}

$drive = [System.IO.DriveInfo]::new(
    [System.IO.Path]::GetPathRoot($resolvedTempRoot)
)
$candidateBytes = [long]0
foreach ($item in $candidates) {
    $candidateBytes += [long]$item.bytes
}
$deletedBytes = [long]0
foreach ($item in $deleted) {
    $deletedBytes += [long]$item.bytes
}
$result = [ordered]@{
    schema_version = 1
    recorded_at = (Get-Date).ToUniversalTime().ToString("o")
    apply = [bool]$Apply
    temp_root = $resolvedTempRoot
    active_vm_ids = @($activeVmIds | Sort-Object)
    minimum_age_hours = $MinimumAgeHours
    free_bytes_before = [long]$freeBefore
    free_bytes_after = [long]$drive.AvailableFreeSpace
    candidate_count = $candidates.Count
    candidate_bytes = $candidateBytes
    deleted_count = $deleted.Count
    deleted_bytes = $deletedBytes
    candidates = @($candidates)
    deleted = @($deleted)
    skipped = @($skipped)
    errors = @($errors)
}

New-Item -ItemType Directory -Path $ReceiptDirectory -Force | Out-Null
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss.fffffffZ")
$mode = if ($Apply) { "apply" } else { "audit" }
$receipt = Join-Path $ReceiptDirectory "wsl-temp-swap-$mode-$stamp.json"
$temporaryReceipt = "$receipt.tmp"
$json = $result | ConvertTo-Json -Depth 8
[System.IO.File]::WriteAllText($temporaryReceipt, $json + [Environment]::NewLine)
Move-Item -LiteralPath $temporaryReceipt -Destination $receipt -Force
$result["receipt"] = $receipt
$result | ConvertTo-Json -Depth 8

if ($errors.Count -gt 0) {
    exit 2
}
