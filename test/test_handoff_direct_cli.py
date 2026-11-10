"""Direct CLI invocations must load their guard without relying on PYTHONPATH."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('program,extra', [
    ('download_crypto_historical_public.py', []),
    ('download_crypto_keyed_context.py', ['--env-file', '/dev/null']),
    ('download_dune_crypto_history.py', ['--env-file', '/dev/null']),
    ('download_coinmetrics_community.py', ['--lock-timeout-seconds', '0']),
    ('download_forex_frankfurter.py', ['--lock-timeout-seconds', '0']),
    ('download_okx_perp_1m.py', ['--lock-timeout-seconds', '0']),
    ('download_okx_perp_daily.py', ['--lock-timeout-seconds', '0']),
    ('download_bybit_perp_1m.py', ['--lock-timeout-seconds', '0']),
    ('download_bybit_perp_daily.py', ['--lock-timeout-seconds', '0']),
    ('download_bybit_funding_history.py', []),
])
def test_cli_loads_guard_and_stops_before_network(tmp_path, program, extra):
    output = tmp_path / 'not-a-directory'
    output.write_text('sentinel')
    env = {'PATH': os.environ.get('PATH', ''), 'DUNE_API_KEY': 'synthetic-test-key', 'LC_ALL': 'C.UTF-8'}
    result = subprocess.run([sys.executable, str(ROOT / 'downloader' / program),
        '--output-dir', str(output), *extra], cwd=tmp_path, env=env,
        text=True, capture_output=True, timeout=15)
    assert result.returncode != 0
    assert "No module named 'stockagent'" not in result.stderr
    assert 'NotADirectoryError' in result.stderr or 'FileExistsError' in result.stderr
    assert output.read_text() == 'sentinel'
