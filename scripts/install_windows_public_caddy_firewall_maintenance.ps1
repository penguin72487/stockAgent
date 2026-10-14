#Requires -RunAsAdministrator
param(
    [string]$SourceScript = (Join-Path $PSScriptRoot "configure_windows_public_caddy_firewall.ps1"),
    [Parameter(Mandatory = $true)][string]$AllowedCaddyRoot
)

$ErrorActionPreference = "Stop"
$taskName = "StockAgent Public Caddy Firewall Maintenance"
$installRoot = Join-Path $env:ProgramData "StockAgentPublicFirewallMaintenance"
$target = Join-Path $installRoot "configure-firewall.ps1"
$receipt = Join-Path $installRoot "logs\firewall-maintenance.json"

# A SYSTEM task must never execute a user-writable AppData/WSL script. Reject
# reparse points before securing the dedicated directory and its ancestors.
if ($AllowedCaddyRoot -match '["\r\n]' -or
    -not (Test-Path -LiteralPath $AllowedCaddyRoot -PathType Container)) {
    throw "Invalid approved Caddy installation root"
}
$AllowedCaddyRoot = [IO.Path]::GetFullPath($AllowedCaddyRoot)
foreach ($path in @($installRoot, (Join-Path $installRoot "logs"))) {
    if (Test-Path -LiteralPath $path) {
        $item = Get-Item -LiteralPath $path
        if (-not $item.PSIsContainer -or
            ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw "Refusing a non-directory or reparse-point maintenance path: $path"
        }
    } else {
        New-Item -ItemType Directory -Path $path | Out-Null
    }
    $acl = [Security.AccessControl.DirectorySecurity]::new()
    $acl.SetAccessRuleProtection($true, $false)
    $admins = [Security.Principal.SecurityIdentifier]::new("S-1-5-32-544")
    $system = [Security.Principal.SecurityIdentifier]::new("S-1-5-18")
    $users = [Security.Principal.SecurityIdentifier]::new("S-1-5-32-545")
    $acl.SetOwner($admins)
    foreach ($identity in @($admins, $system)) {
        $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
            $identity, "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow"
        ))
    }
    $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
        $users, "ReadAndExecute", "ContainerInherit,ObjectInherit", "None", "Allow"
    ))
    Set-Acl -LiteralPath $path -AclObject $acl
}

try {
    if (-not (Test-Path -LiteralPath $SourceScript -PathType Leaf)) {
        throw "Firewall source script does not exist: $SourceScript"
    }
    if (Test-Path -LiteralPath $target) {
        if ((Get-Item -LiteralPath $target).Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw "Refusing a reparse-point maintenance script: $target"
        }
    }
    $staged = Join-Path $installRoot ("configure-firewall." + [Guid]::NewGuid().ToString("N") + ".tmp")
    $sourceHash = (Get-FileHash -LiteralPath $SourceScript -Algorithm SHA256).Hash
    Copy-Item -LiteralPath $SourceScript -Destination $staged
    $tokens = $null
    $parseErrors = $null
    [Management.Automation.Language.Parser]::ParseFile($staged, [ref]$tokens, [ref]$parseErrors) | Out-Null
    if ($parseErrors.Count -or (Get-FileHash -LiteralPath $staged -Algorithm SHA256).Hash -ne $sourceHash) {
        throw "Staged maintenance script failed syntax/hash validation; previous script is unchanged"
    }
    # Reset inherited file ACLs too: upgrading an old installation must not retain
    # an explicit user write permission on the privileged entrypoint.
    $fileAcl = [Security.AccessControl.FileSecurity]::new()
    $fileAcl.SetAccessRuleProtection($false, $false)
    $fileAcl.SetOwner($admins)
    Set-Acl -LiteralPath $staged -AclObject $fileAcl
    if (Test-Path -LiteralPath $target) {
        # PowerShell coerces $null to an empty string for this .NET overload;
        # Replace then rejects the backup path. Use an actual null string.
        [IO.File]::Replace($staged, $target, [NullString]::Value)
    } else {
        [IO.File]::Move($staged, $target)
    }
    $writeRights = [Security.AccessControl.FileSystemRights]::Write -bor
        [Security.AccessControl.FileSystemRights]::Delete -bor
        [Security.AccessControl.FileSystemRights]::ChangePermissions -bor
        [Security.AccessControl.FileSystemRights]::TakeOwnership
    foreach ($path in @($installRoot, (Join-Path $installRoot "logs"), $target)) {
        $effectiveAcl = Get-Acl -LiteralPath $path
        $unsafe = @($effectiveAcl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier]) |
            Where-Object {
                $_.AccessControlType -eq "Allow" -and
                ($_.FileSystemRights -band $writeRights) -and
                $_.IdentityReference.Value -notin @("S-1-5-18", "S-1-5-32-544")
            })
        if ($unsafe.Count) { throw "Maintenance path has non-administrator write access: $path" }
    }
    if ((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash -ne $sourceHash) {
        throw "Installed maintenance script differs from the source"
    }
    
    $powershellPath = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
    $arguments = "-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$target`" -Maintenance -AllowedCaddyRoot `"$AllowedCaddyRoot`" -ReceiptPath `"$receipt`""
    $action = New-ScheduledTaskAction -Execute $powershellPath -Argument $arguments
    $boot = New-ScheduledTaskTrigger -AtStartup
    $boot.Delay = "PT20S"
    $retry = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
        -RepetitionInterval (New-TimeSpan -Minutes 1)
    $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 1)
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger @($boot, $retry) `
        -Principal $principal -Settings $settings -Force | Out-Null
    
    # Run the registered owner, not a second interactive writer.
    Start-ScheduledTask -TaskName $taskName
    $task = Get-ScheduledTask -TaskName $taskName
    [ordered]@{
        schema_version = 1
        installed_at_utc = [DateTime]::UtcNow.ToString("o")
        task_name = $taskName
        principal = $task.Principal.UserId
        task_state = [string]$task.State
        task_action = $task.Actions.Arguments
        triggers = @($task.Triggers | ForEach-Object {
            [ordered]@{kind=$_.CimClass.CimClassName; delay=$_.Delay; interval=$_.Repetition.Interval}
        })
        multiple_instances = [string]$task.Settings.MultipleInstances
        execution_time_limit = $task.Settings.ExecutionTimeLimit
        script = $target
        script_sha256 = $sourceHash
        receipt = $receipt
        interval_seconds = 60
        approved_caddy_root = $AllowedCaddyRoot
        scope = "Caddy application-scoped inbound TCP 80/443 only; no process restart or DDNS write"
        acceptance = "Task registration is not firewall readback or public reachability"
    } | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $installRoot "installation.json") -Encoding UTF8
    Write-Output "Installed $taskName; read current policy receipt at $receipt"
} catch {
    # The old registration/rule is not removed. Persist the actual installation
    # failure: a successful policy receipt from the old worker is not an upgrade
    # receipt. This directory has already been secured above.
    [ordered]@{
        schema_version = 1
        failed_at_utc = [DateTime]::UtcNow.ToString("o")
        state = "failed"
        error = $_.Exception.Message
        line = $_.InvocationInfo.ScriptLineNumber
        target_script = $target
    } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $installRoot "installation-failure.json") -Encoding UTF8
    throw
}
