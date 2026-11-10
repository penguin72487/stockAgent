"""Shell orchestration must isolate source failures without permitting release."""
from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess
from zoneinfo import ZoneInfo
import pytest


ROOT = Path(__file__).resolve().parents[1]


def runner(tmp_path):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    shutil.copyfile(ROOT / 'scripts/run_taifex_public_history.sh', scripts / 'run_taifex_public_history.sh')
    # No HTTP, local dataset reads, publication or real Python environment.
    (scripts / 'runtime_env.sh').write_text('''
run_fintech_python() {
  printf '%s\\n' "$*" >> calls.txt
  case "$*" in *"--phase positioning"*) return 7;; esac
  return 0
}
''')
    return scripts / 'run_taifex_public_history.sh'


def test_independent_sources_continue_after_positioning_failure(tmp_path):
    result = subprocess.run(['bash', str(runner(tmp_path))], capture_output=True, text=True)
    calls = (tmp_path / 'calls.txt').read_text().splitlines()
    assert any(call.startswith('-c ') and 'guard_path(' in call for call in calls)
    assert any(call.startswith('-c ') and 'guard_locked_path(' in call for call in calls)
    calls = [call for call in calls if not call.startswith('-c ')]
    assert result.returncode == 1  # wrapper must NOT publish a successful release
    assert len(calls) == 5
    assert '--phase put-call' in calls[0]
    assert '--phase positioning' in calls[1]
    assert '--phase large-trader-range' in calls[2]
    assert 'download_taifex_openapi_catalog.py' in calls[3]
    assert 'download_taifex_vix_recent.py' in calls[4]
    assert '--end-date ' + str(datetime.now(ZoneInfo('Asia/Taipei')).date()) in calls[0]


def test_targeted_phase_does_not_redownload_unrelated_sources(tmp_path):
    result = subprocess.run(['bash', str(runner(tmp_path)), '--phase', 'large-trader-range',
                             '--large-trader-start', '2026-07-01'], capture_output=True, text=True)
    calls = (tmp_path / 'calls.txt').read_text().splitlines()
    assert any(call.startswith('-c ') and 'guard_path(' in call for call in calls)
    assert any(call.startswith('-c ') and 'guard_locked_path(' in call for call in calls)
    calls = [call for call in calls if not call.startswith('-c ')]
    assert result.returncode == 0
    assert len(calls) == 1
    assert '--large-trader-start 2026-07-01' in calls[0]


@pytest.mark.parametrize(('membership_rc', 'portfolio_rc', 'expected'), [
    (0, 75, 75), (7, 0, 7), (0, 0, 0),
])
def test_futures_sources_and_panel_fail_independently(
    tmp_path, membership_rc, portfolio_rc, expected,
):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    executable = scripts / 'run_taifex_all_futures_daily.sh'
    shutil.copyfile(ROOT / 'scripts/run_taifex_all_futures_daily.sh', executable)
    (scripts / 'runtime_env.sh').write_text('''
run_fintech_python() {
  printf '%s\\n' "$*" >> calls.txt
  case "$*" in
    *--stock-futures-only*) return "${MEMBERSHIP_RC}";;
    *build_taifex_futures_portfolio_daily.py*) return "${PORTFOLIO_RC}";;
  esac
  return 0
}
''')
    import os
    env = {**os.environ, 'MEMBERSHIP_RC': str(membership_rc),
           'PORTFOLIO_RC': str(portfolio_rc),
           'TAIFEX_FUTURES_TARGET_SESSION': '2026-11-09'}
    result = subprocess.run(['bash', str(executable)], env=env, capture_output=True, text=True)
    calls = (tmp_path / 'calls.txt').read_text().splitlines()
    assert result.returncode == expected
    membership = next(i for i, call in enumerate(calls) if '--stock-futures-only' in call)
    portfolio = next(i for i, call in enumerate(calls) if 'build_taifex_futures_portfolio_daily.py' in call)
    assert membership < portfolio
    assert any('download_taifex_futures_final_settlement_history.py' in call for call in calls)


def test_unverified_rule_archive_is_excluded_from_cold_publication():
    catalog = json.loads((ROOT / 'configs/data_sync/packed_datasets.json').read_text())
    # Catalog has one canonical list, separate from the public source aliases.
    datasets = catalog['datasets']
    by_id = {item['dataset']: item for item in datasets}
    assert 'rules' in by_id['taifex-public-history']['excluded_subtrees']
    assert by_id['taifex-rule-history']['source'] == 'data_taifex_public_history/rules'
    assert by_id['taifex-rule-history']['publish'] is False


def test_partial_market_archive_is_not_publishable(tmp_path):
    from scripts.publish_data_releases import _source_freshness, SnapshotError

    catalog = json.loads((ROOT / 'configs/data_sync/packed_datasets.json').read_text())
    entry = next(item for item in catalog['datasets'] if item['dataset'] == 'taifex-public-history')
    entry = {**entry, 'source': str(tmp_path)}
    receipt = {'status': 'partial', 'effective_end_date': '2026-09-24',
               'completion_claim': 'source_acquisition_not_full_pit_readiness'}
    (tmp_path / 'manifest.json').write_text(json.dumps(receipt))
    with pytest.raises(SnapshotError, match='completion gate failed'):
        _source_freshness(entry)
    receipt['status'] = 'complete'
    (tmp_path / 'manifest.json').write_text(json.dumps(receipt))
    assert _source_freshness(entry)['value'] == '2026-09-24'
