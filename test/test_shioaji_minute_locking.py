from __future__ import annotations

import fcntl
from pathlib import Path
import select
import subprocess
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / 'scripts/shioaji_minute_locking.sh'


def _line(process: subprocess.Popen, timeout: float = 5) -> str:
    assert process.stdout is not None
    assert select.select([process.stdout], [], [], timeout)[0], 'runner did not produce the expected state'
    return process.stdout.readline().strip()


def test_idle_releases_source_lock_but_keeps_singleton_and_reacquires(tmp_path):
    script = '''set -euo pipefail
source "$1"
minute_runner_acquire_locks "$2"
echo WAITING
minute_runner_sleep_unlocked 1
echo REACQUIRED
'''
    process = subprocess.Popen(['bash', '-c', script, 'test-runner', str(HELPER), str(tmp_path)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert _line(process) == 'WAITING'
        with (tmp_path / 'runner.lock').open('a+') as source, \
             (tmp_path / 'runner.owner.lock').open('a+') as owner:
            deadline = time.monotonic() + 5
            while True:
                try:
                    fcntl.flock(source, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    assert time.monotonic() < deadline
                    time.sleep(.01)
            with pytest.raises(BlockingIOError):
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Even after its sleep expires, the runner must not write while
            # another owner still holds the original source lock.
            assert not select.select([process.stdout], [], [], 1.2)[0]
            fcntl.flock(source, fcntl.LOCK_UN)
            assert _line(process) == 'REACQUIRED'
        _, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)


def test_start_waits_for_current_source_owner_instead_of_racing(tmp_path):
    script = '''set -euo pipefail
source "$1"
minute_runner_acquire_locks "$2"
echo ACQUIRED
'''
    with (tmp_path / 'runner.lock').open('a+') as source:
        fcntl.flock(source, fcntl.LOCK_EX | fcntl.LOCK_NB)
        process = subprocess.Popen(['bash', '-c', script, 'test-runner', str(HELPER), str(tmp_path)],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            assert not select.select([process.stdout], [], [], .2)[0]
            fcntl.flock(source, fcntl.LOCK_UN)
            assert _line(process) == 'ACQUIRED'
            _, stderr = process.communicate(timeout=5)
            assert process.returncode == 0, stderr
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
