# Native integration test. Generates and removes only its own tiny fixtures.
[CmdletBinding()]
param([ValidateSet('D','E')][string]$DestinationDrive='D')
$ErrorActionPreference='Stop'
$worker=Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\retire_windows_download_archive.ps1'
$id=[Guid]::NewGuid().ToString('N')
$srcDir=Join-Path $env:TEMP ('stockagent-archive-selftest-'+$id)
$destinationParent=if ($DestinationDrive -eq 'E') { 'E:\StockAgent' } else { 'D:\OffloadedFromC' }
$dstDir=Join-Path $destinationParent ('stockagent-archive-selftest-'+$id)
[void][IO.Directory]::CreateDirectory($srcDir)
[void][IO.Directory]::CreateDirectory($dstDir)
function Invoke-Worker($source,$size,$stamp) {
    $output=Join-Path $srcDir ('process-'+[Guid]::NewGuid().ToString('N')+'.log')
    $errors=$output+'.err'
    $arguments=@('-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',('"'+$worker+'"'),
        '-Source',('"'+$source+'"'),'-DestinationDirectory',('"'+$dstDir+'"'),
        '-ExpectedSize',([string]$size),'-ExpectedLastWriteUtc',('"'+$stamp+'"'),
        '-DestinationReserveBytes','0')
    # Keep exercising the worker's original implicit D: behavior as well.
    if ($DestinationDrive -ne 'D') { $arguments+=@('-DestinationDrive',$DestinationDrive) }
    $p=Start-Process -FilePath powershell.exe -ArgumentList $arguments -Wait -PassThru -NoNewWindow -RedirectStandardOutput $output -RedirectStandardError $errors
    return $p.ExitCode
}
try {
    $source=Join-Path $srcDir 'success.bin'
    $bytes=[byte[]]::new(262144); [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    [IO.File]::WriteAllBytes($source,$bytes)
    $info=Get-Item -LiteralPath $source; $expectedHash=(Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash
    $stamp=$info.LastWriteTimeUtc.ToString('o')
    if ((Invoke-Worker $source $info.Length $stamp) -ne 0) { throw 'Successful copy test failed' }
    $target=Join-Path $dstDir 'success.bin'
    if ((Test-Path -LiteralPath $source) -or -not (Test-Path -LiteralPath ($target+'.retirement-complete.json')) -or
        (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash -ne $expectedHash) { throw 'Successful retirement readback failed' }
    $source=Join-Path $srcDir 'locked.bin'; [IO.File]::WriteAllBytes($source,$bytes)
    $info=Get-Item -LiteralPath $source; $stamp=$info.LastWriteTimeUtc.ToString('o')
    $block=[IO.File]::Open($source,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::None)
    try { if ((Invoke-Worker $source $info.Length $stamp) -eq 0) { throw 'Locked source incorrectly accepted' } } finally { $block.Dispose() }
    if (-not (Test-Path -LiteralPath $source) -or (Test-Path -LiteralPath (Join-Path $dstDir 'locked.bin'))) { throw 'Locked source was mutated' }
    if ((Invoke-Worker $source ($info.Length+1) $stamp) -eq 0 -or -not (Test-Path -LiteralPath $source)) { throw 'Size pin was not enforced' }
    $collision=Join-Path $dstDir 'locked.bin'; [IO.File]::WriteAllText($collision,'protected')
    if ((Invoke-Worker $source $info.Length $stamp) -eq 0 -or [IO.File]::ReadAllText($collision) -ne 'protected' -or -not (Test-Path -LiteralPath $source)) { throw 'Collision was not rejected' }
    $linked=Join-Path $srcDir 'linked.bin'; [IO.File]::WriteAllBytes($linked,$bytes)
    $alias=Join-Path $srcDir 'linked-alias.bin'; New-Item -ItemType HardLink -Path $alias -Target $linked | Out-Null
    $info=Get-Item -LiteralPath $linked; $stamp=$info.LastWriteTimeUtc.ToString('o')
    if ((Invoke-Worker $linked $info.Length $stamp) -eq 0 -or -not (Test-Path -LiteralPath $linked) -or
        -not (Test-Path -LiteralPath $alias)) { throw 'Hardlinked source was not preserved' }
    Write-Output ('{"test":"windows_archive_retirement","destination_drive":"'+$DestinationDrive+'","success":true,"cases":["verified_retirement","exclusive_lock","size_pin","destination_collision","hardlink_refusal"]}')
} finally {
    if ($srcDir -like (Join-Path $env:TEMP 'stockagent-archive-selftest-*') -and (Test-Path -LiteralPath $srcDir)) { Remove-Item -LiteralPath $srcDir -Recurse -Force }
    if ($dstDir -eq (Join-Path $destinationParent ('stockagent-archive-selftest-'+$id)) -and (Test-Path -LiteralPath $dstDir)) { Remove-Item -LiteralPath $dstDir -Recurse -Force }
}
