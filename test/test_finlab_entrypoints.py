"""Exercise the actual file-entrypoint contract, not pytest's import path."""
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('script', [
    'download_finlab_history.py', 'download_finlab_intraday.py',
    'download_finlab_market_intraday.py', 'snapshot_finlab_quota.py',
    'finlab_workload.py',
])
def test_finlab_cli_starts_without_cwd_or_pythonpath(script, tmp_path):
    # Isolated Python ignores PYTHONPATH and the caller's cwd; --help must not
    # authenticate, fetch any data or create source files.
    result = subprocess.run(
        [sys.executable, '-I', str(ROOT / 'scripts' / script), '--help'],
        cwd=tmp_path, capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert 'usage:' in result.stdout.lower()
    assert not list(tmp_path.iterdir())
