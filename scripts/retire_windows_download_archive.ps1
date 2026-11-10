# Move one explicitly pinned, inactive C: file to a verified D: or E: copy.
# The source handle denies all sharing and is used for deletion, so the
# pathname cannot be swapped between the readback and the retirement.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$Source,
    [Parameter(Mandatory=$true)][string]$DestinationDirectory,
    [Parameter(Mandatory=$true)][long]$ExpectedSize,
    [Parameter(Mandatory=$true)][string]$ExpectedLastWriteUtc,
    [ValidateSet('D','E')][string]$DestinationDrive = 'D',
    [long]$DestinationReserveBytes = 68719476736
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if ($ExpectedSize -le 0 -or $DestinationReserveBytes -lt 0) { throw 'Invalid size or destination reserve' }
$sourcePath = [IO.Path]::GetFullPath($Source)
$destinationPath = [IO.Path]::GetFullPath($DestinationDirectory)
$destinationRoot = ('{0}:\' -f $DestinationDrive)
if (-not $sourcePath.StartsWith('C:\', [StringComparison]::OrdinalIgnoreCase) -or
    -not $destinationPath.StartsWith($destinationRoot, [StringComparison]::OrdinalIgnoreCase) -or
    ($DestinationDrive -eq 'E' -and -not $destinationPath.StartsWith('E:\StockAgent\', [StringComparison]::OrdinalIgnoreCase))) {
    throw 'This operation requires an explicit C: source and selected D: or E:\StockAgent destination'
}
function Assert-NoRedirectedAncestors([string]$Path) {
    $item = [IO.DirectoryInfo]::new($Path)
    while ($null -ne $item) {
        if (-not $item.Exists -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Missing or redirected directory: $($item.FullName)"
        }
        $item = $item.Parent
    }
}
Assert-NoRedirectedAncestors ([IO.Path]::GetDirectoryName($sourcePath))
Assert-NoRedirectedAncestors $destinationPath
$disk = [IO.DriveInfo]::new($DestinationDrive)
if (-not $disk.IsReady -or $disk.DriveType -ne [IO.DriveType]::Fixed -or
    $disk.AvailableFreeSpace -lt ($ExpectedSize + $DestinationReserveBytes)) {
    throw 'The selected destination does not have the pinned copy size plus its reserve'
}
$target = Join-Path $destinationPath ([IO.Path]::GetFileName($sourcePath))
$pending = $target + '.retirement-pending.json'
$complete = $target + '.retirement-complete.json'
if ((Test-Path -LiteralPath $target) -or (Test-Path -LiteralPath $pending) -or (Test-Path -LiteralPath $complete)) {
    throw 'Destination or a prior retirement journal already exists; reconcile it first'
}
$expectedTime = [DateTime]::Parse($ExpectedLastWriteUtc).ToUniversalTime()

Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

public sealed class StockAgentPinnedArchive : IDisposable {
    [StructLayout(LayoutKind.Sequential)]
    private struct Info {
        public uint attrs, cLow, cHigh, aLow, aHigh, wLow, wHigh, volume, sizeHigh, sizeLow,
                    links, indexHigh, indexLow;
    }
    [StructLayout(LayoutKind.Sequential)]
    private struct Disposition { public byte deleteFile; }
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    private static extern SafeFileHandle CreateFile(string path, uint access, uint share, IntPtr security,
        uint creation, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError=true)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle h, out Info result);
    [DllImport("kernel32.dll", SetLastError=true)]
    private static extern bool SetFileInformationByHandle(SafeFileHandle h, int kind,
        ref Disposition info, uint size);
    private SafeFileHandle handle;
    private FileStream source;
    private Info initial;
    public string FileIdentity { get; private set; }
    public long Size { get; private set; }
    public long LastWriteTicksUtc { get; private set; }
    private static string Hex(byte[] hash) { return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant(); }
    private Info ReadInfo() {
        Info info; if (!GetFileInformationByHandle(handle, out info)) throw new Win32Exception(Marshal.GetLastWin32Error());
        return info;
    }
    public StockAgentPinnedArchive(string path, long expectedSize, long expectedTicks) {
        // GENERIC_READ | DELETE; zero share mode excludes readers, writers and rename.
        handle = CreateFile(path, 0x80010000u, 0, IntPtr.Zero, 3, 0x08200000u, IntPtr.Zero);
        if (handle.IsInvalid) throw new Win32Exception(Marshal.GetLastWin32Error(), "Cannot exclusively open source");
        try {
            initial = ReadInfo();
            if ((initial.attrs & 0x410u) != 0 || initial.links != 1) throw new IOException("Source is redirected, a directory, or hardlinked");
            Size = ((long)initial.sizeHigh << 32) | initial.sizeLow;
            LastWriteTicksUtc = DateTime.FromFileTimeUtc(((long)initial.wHigh << 32) | initial.wLow).Ticks;
            if (Size != expectedSize || LastWriteTicksUtc != expectedTicks) throw new IOException("Pinned source size or modification time changed");
            FileIdentity = initial.volume.ToString("x8") + ":" + initial.indexHigh.ToString("x8") + initial.indexLow.ToString("x8");
            source = new FileStream(handle, FileAccess.Read, 1024*1024, false);
        } catch { handle.Dispose(); throw; }
    }
    public string CopyToNew(string stage) {
        using (var destination = new FileStream(stage, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024*1024, FileOptions.SequentialScan))
        using (var hash = SHA256.Create()) {
            var buffer = new byte[4*1024*1024]; int count; long total = 0;
            while ((count = source.Read(buffer, 0, buffer.Length)) != 0) {
                destination.Write(buffer, 0, count); hash.TransformBlock(buffer, 0, count, buffer, 0); total += count;
            }
            hash.TransformFinalBlock(new byte[0], 0, 0); destination.Flush(true);
            if (total != Size) throw new IOException("Copied size differs from pinned source");
            var current = ReadInfo();
            if (current.volume != initial.volume || current.indexHigh != initial.indexHigh || current.indexLow != initial.indexLow ||
                current.sizeHigh != initial.sizeHigh || current.sizeLow != initial.sizeLow || current.wHigh != initial.wHigh || current.wLow != initial.wLow)
                throw new IOException("Pinned source changed during copy");
            return Hex(hash.Hash);
        }
    }
    public void DeletePinnedSource() {
        var current = ReadInfo();
        if (current.volume != initial.volume || current.indexHigh != initial.indexHigh || current.indexLow != initial.indexLow ||
            current.sizeHigh != initial.sizeHigh || current.sizeLow != initial.sizeLow || current.wHigh != initial.wHigh || current.wLow != initial.wLow)
            throw new IOException("Pinned source changed before retirement");
        var disposition = new Disposition { deleteFile = 1 };
        if (!SetFileInformationByHandle(handle, 4, ref disposition, 1)) throw new Win32Exception(Marshal.GetLastWin32Error(), "Handle-based source retirement failed");
    }
    public void Dispose() { if (source != null) source.Dispose(); else if (handle != null) handle.Dispose(); }
}
// Keep the verified destination exclusively open until source retirement and
// its journal complete. This closes the hash-to-delete replacement window.
public sealed class StockAgentPinnedDestination : IDisposable {
    private FileStream file;
    public StockAgentPinnedDestination(string path, long expectedSize) {
        file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None, 1024*1024, FileOptions.SequentialScan);
        if (file.Length != expectedSize) { file.Dispose(); throw new IOException("Destination size differs"); }
    }
    public string Hash() {
        using (var hash = SHA256.Create()) {
            return BitConverter.ToString(hash.ComputeHash(file)).Replace("-", "").ToLowerInvariant();
        }
    }
    public void Dispose() { if (file != null) file.Dispose(); }
}
'@
function Write-NewReceipt([string]$Path, [object]$Value) {
    $temporary = $Path + '.tmp-' + [Guid]::NewGuid().ToString('N')
    try {
        $bytes = [Text.UTF8Encoding]::new($false).GetBytes(($Value | ConvertTo-Json -Depth 8))
        $file = [IO.FileStream]::new($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        try { $file.Write($bytes, 0, $bytes.Length); $file.Flush($true) } finally { $file.Dispose() }
        [IO.File]::Move($temporary, $Path)
    } finally { if ([IO.File]::Exists($temporary)) { [IO.File]::Delete($temporary) } }
}
$stage = Join-Path $destinationPath ('.stockagent-archive-' + [Guid]::NewGuid().ToString('N') + '.partial')
$sourceHandle = $null
$destinationHandle = $null
$markedForDeletion = $false
try {
    $sourceHandle = [StockAgentPinnedArchive]::new($sourcePath, $ExpectedSize, $expectedTime.Ticks)
    $before = [long]([IO.DriveInfo]::new('C')).AvailableFreeSpace
    $hash = $sourceHandle.CopyToNew($stage)
    [IO.File]::Move($stage, $target)
    $destinationHandle = [StockAgentPinnedDestination]::new($target, $ExpectedSize)
    if ($destinationHandle.Hash() -cne $hash) { throw 'Independent destination SHA-256 readback failed' }
    $receipt = [ordered]@{
        contract='stockagent.windows-archive-retirement.v1'; source=$sourcePath; destination=$target;
        bytes=$ExpectedSize; source_identity=$sourceHandle.FileIdentity; source_last_write_utc=$expectedTime.ToString('o');
        sha256=$hash; verification='complete_source_hash_and_independent_destination_hash';
        dataset_contents_verified=$false; state='destination_verified_reconcile_source';
        source_delete_requested_at_utc=[DateTime]::UtcNow.ToString('o'); c_free_before_copy=$before;
        c_free_before_retirement=[long]([IO.DriveInfo]::new('C')).AvailableFreeSpace
    }
    Write-NewReceipt $pending $receipt
    $sourceHandle.DeletePinnedSource()
    $markedForDeletion = $true
    $sourceHandle.Dispose(); $sourceHandle = $null
    if ([IO.File]::Exists($sourcePath)) { throw 'Source path remains after handle retirement' }
    $receipt.state='source_retired_destination_hash_verified'
    $receipt['completed_at_utc']=[DateTime]::UtcNow.ToString('o')
    $receipt['c_free_after']=[long]([IO.DriveInfo]::new('C')).AvailableFreeSpace
    Write-NewReceipt $complete $receipt
    $receipt | ConvertTo-Json -Depth 8 -Compress
} finally {
    if ($null -ne $sourceHandle) { $sourceHandle.Dispose() }
    if ($null -ne $destinationHandle) { $destinationHandle.Dispose() }
    if (-not $markedForDeletion -and [IO.File]::Exists($stage)) { [IO.File]::Delete($stage) }
}
