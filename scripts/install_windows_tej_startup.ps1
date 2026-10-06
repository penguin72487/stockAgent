<# Register boot (WSL only) and logon (interactive GUI) recovery independently.
No Windows password, auto-login, credentials extraction or privilege changes.
#>
param([Parameter(Mandatory=$true)][string]$Launcher,
      [Parameter(Mandatory=$true)][string]$RepoRoot,
      [Parameter(Mandatory=$true)][string]$Output)
$ErrorActionPreference='Stop'
$registry='HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss'
$default=(Get-ItemProperty $registry).DefaultDistribution
$distro=(Get-ItemProperty (Join-Path $registry $default)).DistributionName
if($distro -notmatch '^[A-Za-z0-9_.-]+$' -or $RepoRoot -notmatch '^/[A-Za-z0-9_./-]+$'){throw 'Exact safe distro/repository required'}
$install=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'StockAgent\TEJStartup'
[void][IO.Directory]::CreateDirectory($install)
$digest=(Get-FileHash -LiteralPath $Launcher -Algorithm SHA256).Hash.ToLowerInvariant()
$installed=Join-Path $install ('start-'+$digest+'.ps1')
if(Test-Path -LiteralPath $installed) {
    if((Get-FileHash -LiteralPath $installed).Hash.ToLowerInvariant() -cne $digest){throw 'Pinned Windows launcher changed'}
}else{Copy-Item -LiteralPath $Launcher -Destination $installed}
$exe=Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$arguments='-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "'+$installed+'" -DistroName '+$distro+' -RepoRoot '+$RepoRoot
$currentUser=[Security.Principal.WindowsIdentity]::GetCurrent().Name
$settings=New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -StartWhenAvailable `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$logon=New-ScheduledTaskTrigger -AtLogOn -User $currentUser
$logon.Delay='PT10S'
$tick=New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
$interactive=New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Limited
$action=New-ScheduledTaskAction -Execute $exe -Argument $arguments
Register-ScheduledTask -TaskName 'StockAgent TEJ Desktop Recovery' -Action $action `
    -Trigger @($logon,$tick) -Principal $interactive -Settings $settings `
    -Description 'Owned TEJ desktop after logon; original attempts preserved; canonical WSL collector only' -Force -ErrorAction Stop|Out-Null
$installedInteractive=Get-ScheduledTask -TaskName 'StockAgent TEJ Desktop Recovery' -TaskPath '\' -ErrorAction Stop
if($installedInteractive.Principal.LogonType -ne 'Interactive' -or $installedInteractive.Actions[0].Arguments -cne $arguments){
    throw 'Installed interactive task differs; registration is not acceptance'
}
$bootRegistered=$false;$bootError=$null
try {
    $boot=New-ScheduledTaskTrigger -AtStartup;$boot.Delay='PT15S'
    $s4u=New-ScheduledTaskPrincipal -UserId $currentUser -LogonType S4U -RunLevel Limited
    $bootAction=New-ScheduledTaskAction -Execute $exe -Argument ($arguments+' -StartServiceOnly')
    Register-ScheduledTask -TaskName 'StockAgent TEJ WSL Startup' -Action $bootAction `
        -Trigger $boot -Principal $s4u -Settings $settings `
        -Description 'Start WSL TEJ systemd collector/API timer at boot; never touch desktop before user logon' -Force -ErrorAction Stop|Out-Null
    $installedBoot=Get-ScheduledTask -TaskName 'StockAgent TEJ WSL Startup' -TaskPath '\' -ErrorAction Stop
    if($installedBoot.Principal.LogonType -ne 'S4U' -or
       $installedBoot.Actions[0].Arguments -cne ($arguments+' -StartServiceOnly')){throw 'Installed boot task differs'}
    $bootRegistered=$true
}catch{
    $bootError=$(if($_.FullyQualifiedErrorId -match '0x80070005'){'administrator_permission_required'}else{'windows_boot_task_unverified'})
    Write-Warning 'Pre-logon WSL task could not be registered; at-logon desktop recovery remains installed'
}
Start-ScheduledTask -TaskName 'StockAgent TEJ Desktop Recovery'
$proof=@{contract='tej_windows_boot_logon_tasks_v1';observed_at_utc=[DateTime]::UtcNow.ToString('o');
    distro=$distro;repo_root=$RepoRoot;interactive_task_registered=$true;prelogon_wsl_task_registered=$bootRegistered;
    interactive_logon_required=$true;windows_autologin_changed=$false;provider_queries_sent=0;launcher_sha256=$digest;
    prelogon_wsl_error_code=$bootError;
    task_interval_seconds=60;task_timeout_seconds=$null;bootstrap_timeout_seconds=100;multiple_instances='IgnoreNew'}
[IO.File]::WriteAllText($Output,($proof|ConvertTo-Json -Compress),[Text.UTF8Encoding]::new($false))
$proof|ConvertTo-Json -Compress
