import os
from pathlib import Path
import stat

import pytest

from scripts.ensure_rate_limit_directory import ensure


def test_bootstrap_creates_and_reuses_the_shared_owner_directory(tmp_path):
    path = ensure(tmp_path, os.getuid(), os.getgid())
    assert path == tmp_path/f'stockagent-rate-limits-{os.getuid()}'
    assert stat.S_IMODE(path.stat().st_mode) == 0o700
    state = path/'provider.state'
    state.write_text('existing rate limit')
    before = path.stat().st_ino
    assert ensure(tmp_path, os.getuid(), os.getgid()) == path
    assert path.stat().st_ino == before and state.read_text() == 'existing rate limit'


def test_bootstrap_refuses_a_symlink_without_touching_its_target(tmp_path):
    target = tmp_path/'unrelated'
    target.mkdir(mode=0o755)
    before = stat.S_IMODE(target.stat().st_mode)
    (tmp_path/f'stockagent-rate-limits-{os.getuid()}').symlink_to(target, target_is_directory=True)
    with pytest.raises(OSError):
        ensure(tmp_path, os.getuid(), os.getgid())
    assert stat.S_IMODE(target.stat().st_mode) == before


def test_bootstrap_refuses_an_existing_directory_for_another_uid(tmp_path):
    expected_uid = os.getuid()+1
    path = tmp_path/f'stockagent-rate-limits-{expected_uid}'
    path.mkdir()
    before = path.stat().st_mode
    with pytest.raises(ValueError, match='owner or identity differs'):
        ensure(tmp_path, expected_uid, os.getgid())
    assert path.stat().st_mode == before


def test_service_waits_for_per_invocation_bootstrap_before_sandbox():
    root = Path(__file__).resolve().parents[1]
    service = (root/'deploy/systemd/stockagent-tw-trading-universe.service.in').read_text()
    helper = (root/'deploy/systemd/stockagent-rate-limit-directory@.service.in').read_text()
    installer = (root/'scripts/install_tw_public_publication_services.sh').read_text()
    assert 'Requires=stockagent-rate-limit-directory@@SERVICE_UID@.service' in service
    assert 'After=stockagent-rate-limit-directory@@SERVICE_UID@.service' in service
    assert 'RemainAfterExit=yes' not in helper
    assert 'ensure_rate_limit_directory.py' in helper and '--uid %i' in helper
    assert 'stockagent-rate-limit-directory@.service' in installer
