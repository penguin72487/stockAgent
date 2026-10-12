<# Pure policy acceptance; no Excel, TEJ, launcher, credentials or GUI access. #>
param([Parameter(Mandatory=$true)][string]$Script,
      [Parameter(Mandatory=$true)][string]$Output)
$ErrorActionPreference='Stop'
if(Test-Path -LiteralPath $Output){throw 'Retain original startup acceptance'}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Script,[ref]$tokens,[ref]$errors)
if($errors.Count -ne 0){throw 'Startup syntax invalid'}
foreach($name in @('Same-Session','Launcher-RetryBasis')) {
    $functions=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name},$true))
    if($functions.Count -ne 1){throw 'Unique current startup policy required'}
    Invoke-Expression $functions[0].Extent.Text
}
$now=[DateTime]::UtcNow;$book='owned-fixture.xlsx'
$old=@{TejProcessId=1;ExpectedWindow=2;ExpectedTitle='owned';ExpectedWorkbook='owned-fixture.xlsx';ExpectedExcelWindow=3}
$proof=@{contract='owned_interactive_tej_desktop_startup_v1';windows_boot_at_utc='fixture-boot';retired_window_absent=$true;retired_process_absent=$true}
$base=@{contract=$proof.contract;windows_boot_at_utc=$proof.windows_boot_at_utc;workbook=$book;
    state='prepared';method='owned_uia_toggle_once_v1';observed_at_utc=$now.AddSeconds(-61).ToString('o')}
$checks=[Collections.Generic.List[object]]::new()
foreach($case in @('prepared','confirmed','invoke','wrong_contract','wrong_boot','wrong_workbook','live_window',
                   'live_process','live_addin','excel_unavailable','too_young','future','invalid_time','foreign_method',
                   'foreign_state','foreign_confirmed_session')) {
    $previous=$base.Clone();$current=$proof.Clone();$absent=$true;$ready=$true
    switch($case) {
        'confirmed' {$previous.state='confirmed';$previous.new_session=$old}
        'invoke' {$previous.method='owned_uia_invoke_once_v1'}
        'wrong_contract' {$previous.contract='foreign'}
        'wrong_boot' {$previous.windows_boot_at_utc='other-boot'}
        'wrong_workbook' {$previous.workbook='other-book'}
        'live_window' {$current.retired_window_absent=$false}
        'live_process' {$current.retired_process_absent=$false}
        'live_addin' {$absent=$false}
        'excel_unavailable' {$ready=$false}
        'too_young' {$previous.observed_at_utc=$now.AddSeconds(-59).ToString('o')}
        'future' {$previous.observed_at_utc=$now.AddSeconds(1).ToString('o')}
        'invalid_time' {$previous.observed_at_utc='not-a-clock'}
        'foreign_method' {$previous.method='unknown'}
        'foreign_state' {$previous.state='unknown'}
        'foreign_confirmed_session' {$previous.state='confirmed';$previous.new_session=@{TejProcessId=99}}
    }
    $allowed=$false
    try{$basis=Launcher-RetryBasis $previous $old $current $book $now $absent $ready;$allowed=$true}catch{$basis=$null}
    $ok=$allowed -eq ($case -in @('prepared','confirmed','invoke'))
    $checks.Add(@{name=$case;accepted=$ok;allowed=$allowed;basis=$basis})
    if(-not $ok){throw ('Startup policy acceptance failed: '+$case)}
}
$receipt=@{contract='owned_launcher_retry_policy_fixture_v1';accepted=$true;checks=$checks.ToArray();
    observed_at_utc=[DateTime]::UtcNow.ToString('o');provider_queries_sent=0;desktop_actions_sent=0;
    startup_script_sha256=(Get-FileHash -LiteralPath $Script).Hash.ToLowerInvariant()}
[IO.File]::WriteAllText($Output,($receipt|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
Write-Output ('Startup policy passed '+$checks.Count+' cases; no desktop action')
