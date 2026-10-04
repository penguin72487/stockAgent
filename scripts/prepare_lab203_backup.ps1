# Windows backup-node inventory and bounded NAS write/read measurement.
# No project deletion, Syncthing enrollment, service stop or global VPN change.
[CmdletBinding()]
param(
    [string]$OutputDirectory = '',
    [string]$SyncthingConfigPath = '',
    [switch]$TestNas,
    [switch]$ConfigureNasCredential,
    [string]$NasCredentialFile = '',
    [ValidateRange(1, 1024)][int]$ProbeMiB = 32,
    [ValidateRange(1, 5)][int]$Repeats = 3
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2
if ($env:OS -ne 'Windows_NT') { throw 'Run this script on the Windows backup node.' }

function New-PrivateDirectory([string]$Path) {
    if (Test-Path -LiteralPath $Path) { throw 'Use a fresh receipt directory.' }
    [void][System.IO.Directory]::CreateDirectory($Path)
    $item = Get-Item -LiteralPath $Path
    if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        throw 'Receipt directory must not be a junction or symlink.'
    }
    $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
    $acl = New-Object System.Security.AccessControl.DirectorySecurity
    $acl.SetOwner($sid)
    $acl.SetAccessRuleProtection($true, $false)
    $flags = [System.Security.AccessControl.InheritanceFlags]'ContainerInherit,ObjectInherit'
    foreach ($principal in @($sid, [System.Security.Principal.SecurityIdentifier]'S-1-5-18')) {
        $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
            $principal, 'FullControl', $flags, 'None', 'Allow')
        [void]$acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}

function Tool-Path([string]$Name) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $command) { return $null }
    return $command.Source
}

if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $env:LOCALAPPDATA ('StockAgent\backup-node\' + [guid]::NewGuid().ToString('N'))
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
New-PrivateDirectory $OutputDirectory
$relative = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('5a2455Sf5LiK5YKz5Y2AL+mMouaYseWQjQ==')).Replace('/', '\')
$share = '\\140.127.208.143\Lab203'
$nasRoot = $share + '\' + $relative
$receipt = [ordered]@{
    schema_version = 1
    contract = 'windows_backup_node_preparation_v1'
    observed_at_utc = [DateTime]::UtcNow.ToString('o')
    host = $env:COMPUTERNAME
    windows_identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    state = 'read_only_inventory'
    nas_path = $nasRoot
    nas_authenticated = $false
    nas_probe = @()
    durable_off_host_backup_verified = $false
    process_references_checked = $false
    deletion_authorized = $false
    syncthing_folders = @()
    errors = @()
}
$driveCreated = $false
$probeDirectory = $null
$probeCreated = $false
$probeFiles = New-Object 'System.Collections.Generic.List[string]'
try {
    $osInfo = Get-CimInstance Win32_OperatingSystem
    $receipt.windows = @{ caption = $osInfo.Caption; version = $osInfo.Version; build = $osInfo.BuildNumber }
    $receipt.cpu_sockets = @(Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors)
    $receipt.memory_bytes = [int64]$osInfo.TotalVisibleMemorySize * 1024
    $receipt.disks = @(Get-CimInstance Win32_LogicalDisk -Filter 'DriveType=3' | Select-Object DeviceID, FileSystem, Size, FreeSpace)
    $receipt.tools = @{ mamba = (Tool-Path 'mamba'); conda = (Tool-Path 'conda'); restic = (Tool-Path 'restic'); ssh = (Tool-Path 'ssh') }
    $sshService = Get-Service sshd -ErrorAction SilentlyContinue
    $receipt.sshd = $(if ($null -eq $sshService) { 'not_installed' } else { [string]$sshService.Status })
    $receipt.processes = @(Get-Process | Select-Object Id, ProcessName)
    $receipt.project_services = @(Get-CimInstance Win32_Service | Where-Object {
        $_.Name -match 'stockagent|syncthing' -or $_.DisplayName -match 'stockagent|syncthing'
    } | Select-Object Name, State, StartMode, ProcessId)
    if (Tool-Path 'wsl.exe') {
        $receipt.wsl_distributions = @((& wsl.exe --list --quiet 2>$null) | ForEach-Object { $_.Replace([string][char]0, '').Trim() } | Where-Object { $_ })
        $receipt.wsl_list_exit_code = $LASTEXITCODE
    } else { $receipt.wsl_distributions = @() }

    $candidates = @($SyncthingConfigPath, (Join-Path $env:LOCALAPPDATA 'Syncthing\config.xml'), (Join-Path $env:APPDATA 'Syncthing\config.xml')) | Where-Object { $_ }
    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) { continue }
        [xml]$config = Get-Content -LiteralPath $candidate -Raw -Encoding UTF8
        # Select public folder metadata; never copy GUI API keys/certificates.
        $receipt.syncthing_folders += @($config.configuration.folder | ForEach-Object {
            @{ id = $_.id; path = $_.path; type = $_.type; config_path = $candidate }
        })
    }

    if ($ConfigureNasCredential) {
        if ($NasCredentialFile) { throw 'Configure a new credential or import an existing one, not both.' }
        $credential = Get-Credential -Message 'Enter the QNAP account on this computer; never send the password in chat.'
        if ($null -eq $credential) { throw 'No NAS credential supplied.' }
    } elseif ($NasCredentialFile) {
        $credential = Import-Clixml -LiteralPath $NasCredentialFile
        if ($credential -isnot [System.Management.Automation.PSCredential]) { throw 'Expected a Windows encrypted PSCredential.' }
    } else { $credential = $null }
    if ($null -ne $credential) {
        New-PSDrive -Name 'StockAgentNasProbe' -PSProvider FileSystem -Root $share -Credential $credential | Out-Null
        $driveCreated = $true
    }
    if ($TestNas -or $ConfigureNasCredential -or $NasCredentialFile) {
        if (-not (Test-Path -LiteralPath $nasRoot -PathType Container)) {
            throw 'The exact NAS user directory is inaccessible. Verify VPN, SMB share name and this account permission.'
        }
        $receipt.nas_authenticated = $true
    }
    if ($ConfigureNasCredential) {
        $savedCredential = Join-Path $OutputDirectory 'nas-credential.xml'
        $credential | Export-Clixml -LiteralPath $savedCredential
        $receipt.nas_credential_file = $savedCredential
        $receipt.credential_scope = 'DPAPI: same Windows user and same computer; validate again under the scheduled/SSH identity'
    }
    if ($TestNas) {
        $probeDirectory = $nasRoot + '\stockagent-probe-' + [guid]::NewGuid().ToString('N')
        if (Test-Path -LiteralPath $probeDirectory) { throw 'Probe namespace already exists.' }
        [void][System.IO.Directory]::CreateDirectory($probeDirectory)
        $probeCreated = $true
        $buffer = New-Object byte[] (1MB)
        $bytes = [int64]$ProbeMiB * 1MB
        for ($round = 0; $round -lt $Repeats; $round++) {
            $file = Join-Path $probeDirectory ('probe-' + $round + '.bin')
            $probeFiles.Add($file)
            $timer = [Diagnostics.Stopwatch]::StartNew()
            $writer = New-Object System.IO.FileStream($file, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None, 1MB, [IO.FileOptions]::WriteThrough)
            $expectedHash = [Security.Cryptography.SHA256]::Create()
            $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
            try {
                for ($i = 0; $i -lt $ProbeMiB; $i++) {
                    $rng.GetBytes($buffer)
                    $writer.Write($buffer, 0, $buffer.Length)
                    [void]$expectedHash.TransformBlock($buffer, 0, $buffer.Length, $buffer, 0)
                }
                [void]$expectedHash.TransformFinalBlock((New-Object byte[] 0), 0, 0)
                $expected = [BitConverter]::ToString($expectedHash.Hash).Replace('-', '').ToLowerInvariant()
                $writer.Flush($true)
            } finally { $writer.Dispose(); $expectedHash.Dispose(); $rng.Dispose() }
            $writeSeconds = $timer.Elapsed.TotalSeconds
            $timer.Restart()
            $reader = [IO.File]::OpenRead($file)
            $hasher = [Security.Cryptography.SHA256]::Create()
            try { $actual = [BitConverter]::ToString($hasher.ComputeHash($reader)).Replace('-', '').ToLowerInvariant() }
            finally { $reader.Dispose(); $hasher.Dispose() }
            $readSeconds = $timer.Elapsed.TotalSeconds
            if ($actual -ne $expected -or (Get-Item -LiteralPath $file).Length -ne $bytes) { throw 'NAS probe checksum differs.' }
            $receipt.nas_probe += @(@{ round = $round; bytes = $bytes; write_seconds = $writeSeconds; read_seconds = $readSeconds; sha256 = $actual; verified = $true })
        }
        $receipt.nas_probe_scope = 'fresh files, flushed writes, exact same-client readback; client cache is possible, not cold restore proof'
        $receipt.state = 'nas_write_read_probe_verified'
    }
    if (Get-Command Get-SmbConnection -ErrorAction SilentlyContinue) {
        $receipt.smb_connections = @(Get-SmbConnection -ErrorAction SilentlyContinue | Where-Object {
            $_.ServerName -eq '140.127.208.143' -and $_.ShareName -eq 'Lab203'
        } | Select-Object ServerName, ShareName, Dialect, Encrypted)
    }
} catch {
    $receipt.state = 'preparation_failed'
    $receipt.errors += @($_.Exception.Message)
} finally {
    # Only exact disposable files created by this invocation may be removed.
    # Directory.Delete(false) refuses unknown files; never recursive deletion.
    if ($probeCreated) {
        try {
            foreach ($file in $probeFiles) { [System.IO.File]::Delete($file) }
            [System.IO.Directory]::Delete($probeDirectory, $false)
            $receipt.probe_cleanup = 'own_exact_probe_files_removed'
        } catch { $receipt.probe_cleanup = 'incomplete_preserved'; $receipt.errors += @($_.Exception.Message) }
    }
    if ($driveCreated) { Remove-PSDrive -Name 'StockAgentNasProbe' -ErrorAction SilentlyContinue }
    $receiptPath = Join-Path $OutputDirectory 'preparation.json'
    $receipt | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
}
[ordered]@{ state = $receipt.state; receipt = $receiptPath; durable_off_host_backup_verified = $false; deletion_authorized = $false } | ConvertTo-Json -Compress
if ($receipt.state -eq 'preparation_failed' -or $receipt.errors.Count) { exit 1 }
