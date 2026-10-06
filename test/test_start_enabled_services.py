"""Dependency recovery must respect disabled owners and reject unsafe names."""
import os
from pathlib import Path
import subprocess

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/start_enabled_services.sh'


def invoke(tmp_path, state, unit='syncthing@root.service'):
    trace = tmp_path / 'calls'
    executable = tmp_path / 'systemctl'
    executable.write_text(
        '#!/usr/bin/env bash\n'
        'printf "%s\\n" "$*" >> "$RECOVERY_TEST_TRACE"\n'
        'if [[ "$1" == is-enabled ]]; then\n'
        '  printf "%s\\n" "$RECOVERY_TEST_STATE"\n'
        '  [[ "$RECOVERY_TEST_STATE" == enabled ]]\n'
        'fi\n'
    )
    executable.chmod(0o755)
    env = {**os.environ, 'PATH': str(tmp_path) + ':' + os.environ['PATH'],
           'RECOVERY_TEST_TRACE': str(trace), 'RECOVERY_TEST_STATE': state}
    result = subprocess.run(['bash', str(SCRIPT), unit], env=env, capture_output=True, text=True)
    return result, trace.read_text().splitlines() if trace.exists() else []


def test_only_enabled_dependency_is_queued_without_waiting(tmp_path):
    result, calls = invoke(tmp_path, 'enabled')
    assert result.returncode == 0
    assert calls == ['is-enabled syncthing@root.service', 'start --no-block syncthing@root.service']


@pytest.mark.parametrize('state', ['disabled', 'masked', 'static', 'not-found'])
def test_disabled_or_absent_dependency_is_not_revived(tmp_path, state):
    result, calls = invoke(tmp_path, state)
    assert result.returncode == 0
    assert calls == ['is-enabled syncthing@root.service']


def test_unknown_dependency_state_is_a_visible_precondition_failure(tmp_path):
    result, calls = invoke(tmp_path, 'unexpected')
    assert result.returncode == 75
    assert calls == ['is-enabled syncthing@root.service']


@pytest.mark.parametrize('unit', ['other.service', 'stockagent-a.service; echo injected', 'stockagent-$(id).service'])
def test_unsupported_dependency_never_reaches_systemctl(tmp_path, unit):
    result, calls = invoke(tmp_path, 'enabled', unit)
    assert result.returncode == 2
    assert calls == []
