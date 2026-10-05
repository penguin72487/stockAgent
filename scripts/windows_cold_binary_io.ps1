$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$config = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($env:STOCKAGENT_BINARY_IO_CONFIG)) | ConvertFrom-Json
$path = [IO.Path]::GetFullPath([string]$config.path)
if (!$path.StartsWith('D:\stockagent-cold-primary\', [StringComparison]::OrdinalIgnoreCase) -or $path -ne [string]$config.path) {
    throw 'Fixed D cold namespace required'
}
$stream = $null
$hash = $null
$prefix = $null
try {
    $buffer = New-Object byte[] (1MB)
    if ($config.mode -eq 'read') {
        $stream = New-Object IO.FileStream($path, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read, 1MB, [IO.FileOptions]::SequentialScan)
        $output = [Console]::OpenStandardOutput()
        while (($count = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
            $output.Write($buffer, 0, $count)
        }
        $output.Flush()
    } elseif ($config.mode -eq 'write') {
        $offset = [long]$config.offset
        $fileMode = [IO.FileMode]::CreateNew
        if ($offset -gt 0) { $fileMode = [IO.FileMode]::Open }
        $stream = New-Object IO.FileStream($path, $fileMode, [IO.FileAccess]::ReadWrite, [IO.FileShare]::Read, 1MB, [IO.FileOptions]::SequentialScan)
        if ($stream.Length -ne $offset) { throw 'Partial size differs; no append permitted' }
        $hash = [Security.Cryptography.SHA256]::Create()
        if ($offset -gt 0) { $prefix = [Security.Cryptography.SHA256]::Create() }
        $total = [long]0
        while ($total -lt $offset) {
            $count = $stream.Read($buffer, 0, [int][Math]::Min([long]$buffer.Length, [long]($offset - $total)))
            if ($count -le 0) { throw 'Truncated retained prefix' }
            [void]$hash.TransformBlock($buffer, 0, $count, $buffer, 0)
            if ($prefix) { [void]$prefix.TransformBlock($buffer, 0, $count, $buffer, 0) }
            $total += $count
        }
        # Hash the same prefix once into the full-file and prefix SHA engines.
        # Python independently read it before launching the exact producer.
        if ($offset -gt 0) {
            [void]$prefix.TransformFinalBlock((New-Object byte[] 0), 0, 0)
            $prefixHex = ([BitConverter]::ToString($prefix.Hash)).Replace('-', '').ToLowerInvariant()
            if ($prefixHex -ne [string]$config.prefix_sha256) { throw 'Retained prefix SHA differs; source untouched' }
            $prefix.Dispose();$prefix=$null
        }
        $stream.Position = $offset
        @{state='native_writer_ready'; offset=$offset; io='windows-filestream-v1'} | ConvertTo-Json -Compress
        $inputBytes = [Console]::OpenStandardInput()
        $nextReserveCheck = $total
        $drive = New-Object IO.DriveInfo('D')
        while (($count = $inputBytes.Read($buffer, 0, $buffer.Length)) -gt 0) {
            if ($total -ge $nextReserveCheck) {
                if ($drive.AvailableFreeSpace -lt ([long]$config.reserve_bytes + $count)) { throw 'D reserve reached' }
                $nextReserveCheck = $total + 128MB
            }
            $stream.Write($buffer, 0, $count)
            [void]$hash.TransformBlock($buffer, 0, $count, $buffer, 0)
            $total += $count
        }
        $stream.Flush($true)
        [void]$hash.TransformFinalBlock((New-Object byte[] 0), 0, 0)
        $sha = ([BitConverter]::ToString($hash.Hash)).Replace('-', '').ToLowerInvariant()
        @{bytes=$total; sha256=$sha; flushed=$true; io='windows-filestream-v1'} | ConvertTo-Json -Compress
    } else { throw 'Unsupported binary I/O mode' }
} catch {
    [Console]::Error.WriteLine('STOCKAGENT_WINDOWS_BINARY_IO_FAILED: ' + $_.Exception.Message)
    exit 2
} finally {
    if ($stream) { $stream.Dispose() }
    if ($hash) { $hash.Dispose() }
    if ($prefix) { $prefix.Dispose() }
}
