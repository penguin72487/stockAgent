$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::InputEncoding = New-Object Text.UTF8Encoding($false)
$document = [Console]::In.ReadLine() | ConvertFrom-Json
if ($null -eq $document -or @($document.groups).Count -lt 1 -or @($document.groups).Count -gt 16) {
    throw 'Bounded source ZIP batch required'
}
Add-Type -AssemblyName System.IO.Compression
$output = [Console]::OpenStandardOutput()
$buffer = New-Object byte[] (1MB)
foreach ($group in $document.groups) {
    $path = [IO.Path]::GetFullPath([string]$group.path)
    $production = $path -match '^D:\\stockagent-cold-primary\\packed\\objects\\packs\\[0-9a-f]{2}\\[0-9a-f]{64}\.zip$'
    $fixture = $group.fixture -eq $true -and $path -match '^D:\\stockagent-completed-benchmark-scratch\\stockagent-zip-reader-probe-[a-z0-9_]{8}\\pack-[0-9]+\.zip$'
    if ((!$production -and !$fixture) -or $path -ne [string]$group.path -or
        @($group.members).Count -lt 1 -or @($group.members).Count -gt 2048) {
        throw 'Fixed D ZIP namespace and bounded members required'
    }
    $file = $null
    $archive = $null
    try {
        $file = New-Object IO.FileStream($path, [IO.FileMode]::Open, [IO.FileAccess]::Read,
                                       [IO.FileShare]::Read, 1MB, [IO.FileOptions]::RandomAccess)
        if ($file.Length -ne [long]$group.expected_bytes) { throw 'Source ZIP length changed' }
        $archive = New-Object IO.Compression.ZipArchive($file, [IO.Compression.ZipArchiveMode]::Read, $true)
        $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::Ordinal)
        foreach ($entry in $archive.Entries) {
            if (!$seen.Add([string]$entry.FullName)) { throw 'Source ZIP has duplicate entry names' }
        }
        foreach ($member in $group.members) {
            $entry = $archive.GetEntry([string]$member.name)
            if ($null -eq $entry -or $entry.Length -ne [long]$member.size) { throw 'Exact ZIP member differs' }
            $stream = $entry.Open()
            try {
                $remaining = [long]$member.size
                while ($remaining -gt 0) {
                    $take = [int][Math]::Min([long]$buffer.Length, $remaining)
                    $count = $stream.Read($buffer, 0, $take)
                    if ($count -le 0) { throw 'ZIP member is truncated' }
                    $output.Write($buffer, 0, $count)
                    $remaining -= $count
                }
                if ($stream.ReadByte() -ne -1) { throw 'ZIP member is oversized' }
            } finally {
                $stream.Dispose()
            }
        }
    } finally {
        if ($null -ne $archive) { $archive.Dispose() }
        if ($null -ne $file) { $file.Dispose() }
    }
}
$output.Flush()
