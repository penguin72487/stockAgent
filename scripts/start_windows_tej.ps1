<# Small, bounded Windows launcher; all downloads stay in the WSL collector. #>
param([Parameter(Mandatory=$true)][string]$DistroName,
      [Parameter(Mandatory=$true)][string]$RepoRoot,
      [switch]$StartServiceOnly)
$ErrorActionPreference='Stop'
if($DistroName -notmatch '^[A-Za-z0-9_.-]+$' -or $RepoRoot -notmatch '^/[A-Za-z0-9_./-]+$'){
    throw 'Explicit safe WSL distribution and repository required'
}
$sessionId=[Diagnostics.Process]::GetCurrentProcess().SessionId
$shell='systemctl start --no-block stockagent-tej-history.service stockagent-tej-api-trial.timer'
if(-not $StartServiceOnly) {
    if($sessionId -le 0){throw 'Interactive logged-in desktop required; no autologin'}
    $shell += '; cd '+$RepoRoot+' && source scripts/runtime_env.sh && run_fintech_python scripts/recover_tej_startup.py --watch --windows-session-id '+$sessionId
}
$start=[Diagnostics.ProcessStartInfo]::new()
$start.FileName=Join-Path $env:SystemRoot 'System32\wsl.exe'
$start.Arguments='--distribution '+$DistroName+' --exec /bin/bash -lc "'+$shell+'"'
$start.UseShellExecute=$false;$start.CreateNoWindow=$true
$start.RedirectStandardOutput=[bool]$StartServiceOnly;$start.RedirectStandardError=[bool]$StartServiceOnly
$process=[Diagnostics.Process]::Start($start)
if($StartServiceOnly){$stdout=$process.StandardOutput.ReadToEndAsync();$stderr=$process.StandardError.ReadToEndAsync()}
if(-not $StartServiceOnly) {
    # Own a durable Interactive WSL relay. Recurring IgnoreNew triggers cost
    # nothing while healthy; Task Scheduler restarts this launcher after exit.
    while(-not $process.WaitForExit(30000)){}
} elseif(-not $process.WaitForExit(150000)) {
    # Stop only this exact launcher, not WSL, Excel, TEJ or user workbooks.
    $process.Kill();throw 'Bounded WSL startup dispatch expired; original source attempts retained'
}
if($process.ExitCode -ne 0){throw 'WSL TEJ startup failed; inspect private task/collector evidence'}
if($StartServiceOnly){$stdout.Result}
