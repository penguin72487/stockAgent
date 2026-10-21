"""CLI startup must not depend on the shell's PYTHONPATH or current folder."""
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/probe_news_sources.py'


@pytest.mark.parametrize('launch', ('isolated_external_script', 'repo_script', 'repo_module'))
@pytest.mark.parametrize('script_name', ['probe_news_sources', 'diagnose_news_access'])
def test_news_probe_help_resolves_current_checkout_without_network(tmp_path, launch, script_name):
    script = ROOT / 'scripts' / (script_name + '.py')
    env = {key: value for key, value in os.environ.items() if key != 'PYTHONPATH'}
    if launch == 'isolated_external_script':
        args, cwd = [sys.executable, '-I', str(script), '--help'], tmp_path
    elif launch == 'repo_script':
        args, cwd = [sys.executable, str(script), '--help'], ROOT
    else:
        args, cwd = [sys.executable, '-m', 'scripts.' + script_name, '--help'], ROOT
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert ('--current-failure-provider' if script_name == 'probe_news_sources' else '--resolve-redirect-only') in result.stdout
    assert '--require-current-missing-body-failure' in result.stdout
    if script_name == 'probe_news_sources':
        assert '--retain-reviewed-response' in result.stdout
    assert 'Traceback' not in result.stderr
