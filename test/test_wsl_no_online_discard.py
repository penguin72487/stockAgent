import os
from pathlib import Path
import subprocess

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/ensure_wsl_no_online_discard.sh'


@pytest.fixture
def fake_commands(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    state = tmp_path / 'options'
    log = tmp_path / 'mount.log'
    commands = {
        'systemd-detect-virt': '#!/bin/sh\nprintf "%s\\n" "$FAKE_VIRT"\n',
        'findmnt': '#!/bin/sh\ncase "$*" in *FSTYPE*) printf "%s\\n" "$FAKE_FSTYPE";; '
                   '*OPTIONS*) cat "$FAKE_OPTIONS";; *) exit 1;; esac\n',
        'mount': '#!/bin/sh\nprintf "%s\\n" "$*" >> "$FAKE_LOG"\n'
                 'test "$FAKE_FAIL" = 0 || exit 1\nprintf "rw,relatime\\n" > "$FAKE_OPTIONS"\n',
    }
    for name, body in commands.items():
        path = bin_dir / name
        path.write_text(body)
        path.chmod(0o755)
    state.write_text('rw,relatime,discard,errors=remount-ro\n')
    env = {**os.environ, 'PATH': f"{bin_dir}:{os.environ['PATH']}", 'FAKE_VIRT': 'wsl',
           'FAKE_FSTYPE': 'ext4', 'FAKE_OPTIONS': str(state), 'FAKE_LOG': str(log), 'FAKE_FAIL': '0'}
    return env, state, log


def test_wsl_discard_is_disabled_once_without_issuing_trim(fake_commands):
    env, _state, log = fake_commands
    result = subprocess.run(['bash', str(SCRIPT)], env=env, capture_output=True, text=True)
    assert result.returncode == 0 and 'no trim was issued' in result.stdout
    assert log.read_text().splitlines() == ['-o remount,nodiscard /']
    assert subprocess.run(['bash', str(SCRIPT)], env=env).returncode == 0
    assert log.read_text().splitlines() == ['-o remount,nodiscard /']


@pytest.mark.parametrize('changed', [{'FAKE_VIRT': 'docker'}, {'FAKE_FSTYPE': 'xfs'}, {'FAKE_FAIL': '1'}])
def test_unexpected_environment_or_failed_remount_does_not_claim_success(fake_commands, changed):
    env, state, log = fake_commands
    result = subprocess.run(['bash', str(SCRIPT)], env={**env, **changed}, capture_output=True, text=True)
    assert result.returncode != 0 and 'no trim was issued' not in result.stdout
    assert 'discard' in state.read_text()
    if changed.get('FAKE_FAIL') != '1':
        assert not log.exists()
