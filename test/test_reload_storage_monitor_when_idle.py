import fcntl
import hashlib
import os

import pytest

from scripts import reload_storage_monitor_when_idle as reload


@pytest.fixture
def scope(tmp_path):
    code = tmp_path/'storage_progress.py'
    code.write_text('fixed revision')
    lock = tmp_path/'owner.lock'
    lock.touch()
    digest = hashlib.sha256(code.read_bytes()).hexdigest()
    pids = {reload.MONITOR: 101, reload.GATEWAY: 102}
    return code, lock, digest, pids


def test_idle_reload_waits_without_interrupting_existing_census(scope):
    code, lock, digest, pids = scope
    launched = []
    fd = os.open(lock, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        value = reload.attempt(digest, pids, code=code, lock=lock, get_pid=pids.get,
                               launch=launched.append, scan_running=lambda: False)
        assert value['state'] == 'waiting_existing_census' and not launched
    finally:
        os.close(fd)
    value = reload.attempt(digest, pids, code=code, lock=lock, get_pid=pids.get,
                           launch=launched.append, scan_running=lambda: True)
    assert value['state'] == 'waiting_existing_census' and not launched


def test_idle_reload_submits_only_original_active_units(scope):
    code, lock, digest, pids = scope
    current = {reload.MONITOR: 201, reload.GATEWAY: 102}
    launched = []
    value = reload.attempt(digest, pids, code=code, lock=lock, get_pid=current.get,
                           launch=launched.append, scan_running=lambda: False)
    assert value['state'] == 'submitted' and launched == [[reload.GATEWAY]]
    assert value['readiness_confirmed'] is False
    current[reload.GATEWAY] = 0  # Never start a stopped service.
    value = reload.attempt(digest, pids, code=code, lock=lock, get_pid=current.get,
                           launch=launched.append, scan_running=lambda: False)
    assert value['state'] == 'already_restarted' and len(launched) == 1


def test_collection_only_reload_leaves_dashboard_gateway_running(scope):
    code, lock, digest, pids = scope
    launched = []
    value = reload.attempt(digest, pids, code=code, lock=lock, get_pid=pids.get,
                           launch=launched.append, scan_running=lambda: False, monitor_only=True)
    assert value['state'] == 'submitted' and launched == [[reload.MONITOR]]
    assert reload.GATEWAY not in value['submitted_units']


def test_idle_reload_rejects_changed_code_and_redirected_lock(scope, tmp_path):
    code, lock, digest, pids = scope
    launched = []
    code.write_text('newer revision')
    assert reload.attempt(digest, pids, code=code, lock=lock, get_pid=pids.get,
                          launch=launched.append, scan_running=lambda: False)['state'] == 'code_changed'
    code.write_text('fixed revision')
    redirected = tmp_path/'redirected'
    redirected.symlink_to(lock)
    with pytest.raises(OSError):
        reload.attempt(digest, pids, code=code, lock=redirected, get_pid=pids.get,
                       launch=launched.append, scan_running=lambda: False)
    assert not launched
