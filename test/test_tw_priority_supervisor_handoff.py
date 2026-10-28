import os
import subprocess
import sys
import time

import pytest

from scripts import run_tw_priority_supervisor_handoff as handoff


def _spawn_waiter():
    code = (
        "import subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(0.35)']); "
        "print(p.pid,flush=True); p.wait(); print('done',flush=True)"
    )
    proc = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    child = int(proc.stdout.readline())
    return proc, child


def test_handoff_resumes_parent_after_real_child_exit():
    proc, child = _spawn_waiter()
    parent_fd, child_fd = handoff._pidfd_open(proc.pid), handoff._pidfd_open(child)
    try:
        assert handoff._hold_until_child_done(parent_fd, child_fd, time.monotonic() + 3) == "child_finished"
        assert proc.communicate(timeout=3)[0].strip() == "done"
        assert proc.returncode == 0
    finally:
        os.close(parent_fd)
        os.close(child_fd)
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_handoff_deadline_resumes_parent():
    proc, child = _spawn_waiter()
    parent_fd, child_fd = handoff._pidfd_open(proc.pid), handoff._pidfd_open(child)
    try:
        assert handoff._hold_until_child_done(parent_fd, child_fd, time.monotonic() + 0.05) == "exit_protection_deadline"
        assert proc.communicate(timeout=3)[0].strip() == "done"
    finally:
        os.close(parent_fd)
        os.close(child_fd)
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_handoff_resumes_parent_on_watch_error(monkeypatch):
    proc, child = _spawn_waiter()
    parent_fd, child_fd = handoff._pidfd_open(proc.pid), handoff._pidfd_open(child)
    try:
        monkeypatch.setattr(handoff.select, "select", lambda *_a: (_ for _ in ()).throw(OSError("test")))
        with pytest.raises(OSError, match="test"):
            handoff._hold_until_child_done(parent_fd, child_fd, time.monotonic() + 3)
        assert proc.communicate(timeout=3)[0].strip() == "done"
    finally:
        os.close(parent_fd)
        os.close(child_fd)
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_handoff_rejects_a_different_service_invocation(monkeypatch):
    monkeypatch.setattr(handoff, "_service_identity", lambda: (123, "actual", "/test"))
    with pytest.raises(RuntimeError, match="invocation changed"):
        handoff._validated_pair(123, 456, "different")
