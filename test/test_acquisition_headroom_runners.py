"""A low physical-space sample must not start historical provider work."""
from pathlib import Path
import shutil
import subprocess

import pytest


REPO = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('runner', [
    'run_shioaji_historical_market_data.sh',
    'run_shioaji_tx_history_backfill.sh',
])
def test_history_runner_waits_before_login_when_acquisition_is_deferred(tmp_path, runner):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    shutil.copy(REPO / 'scripts' / runner, scripts / runner)
    (tmp_path / '.env.futures').write_text('')
    (scripts / 'runtime_env.sh').write_text('''
run_fintech_python() {
    case "$1" in
      */check_acquisition_capacity.py) return 75 ;;
      *) printf 'unexpected provider dispatch\\n'; return 97 ;;
    esac
}
''')
    (scripts / 'shioaji_history_runner_common.sh').write_text('''
history_protected_delay() { echo 0; }
history_connection_delay() { echo 'unexpected login check' >&2; return 97; }
history_wait() { printf 'waiting:%s:%s\\n' "$1" "$2"; exit 81; }
''')
    result = subprocess.run(['bash', str(scripts / runner)], cwd=tmp_path,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 81
    assert 'waiting:60:verification_storage_headroom' in result.stdout
    assert 'unexpected' not in result.stdout + result.stderr


def test_tej_owner_waits_before_launch_when_acquisition_is_deferred(tmp_path):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    shutil.copy(REPO / 'scripts' / 'run_tej_history.sh', scripts / 'run_tej_history.sh')
    (scripts / 'runtime_env.sh').write_text('''
run_fintech_python() { return 75; }
sleep() { printf 'waiting:%s\\n' "$1"; exit 81; }
resolve_fintech_python() { echo 'unexpected owner launch' >&2; return 97; }
''')
    result = subprocess.run(['bash', str(scripts / 'run_tej_history.sh')], cwd=tmp_path,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 81
    assert 'waiting:60' in result.stdout
    assert 'unexpected' not in result.stderr
