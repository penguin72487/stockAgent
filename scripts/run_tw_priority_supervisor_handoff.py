#!/usr/bin/env python3
"""Extend an already running legacy source supervisor without restarting its work.

The legacy supervisor is paused only while an independent, systemd-supervised
handoff watches its existing child. On child exit or at 13:20 it is resumed and
its normal wait/cleanup logic runs. This is a rollout aid, not a data producer.
"""
from __future__ import annotations

import argparse
import ctypes
import errno
import os
from pathlib import Path
import select
import signal
import subprocess
import time
from datetime import date, datetime, time as wall_time
from zoneinfo import ZoneInfo

UNIT = "stockagent-tw-day-trade-priority-source.service"
SCRIPT = "run_discord_artifact_maintenance.py"
TAIPEI = ZoneInfo("Asia/Taipei")


def _pidfd_open(pid: int) -> int:
    if hasattr(os, "pidfd_open"):
        return os.pidfd_open(pid)
    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.pidfd_open(pid, 0)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "pidfd_open")
    return fd


def _pidfd_signal(fd: int, sig: int) -> None:
    if hasattr(signal, "pidfd_send_signal"):
        signal.pidfd_send_signal(fd, sig)
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.pidfd_send_signal(fd, sig, None, 0) < 0:
        raise OSError(ctypes.get_errno(), "pidfd_send_signal")


def _proc_info(pid: int) -> tuple[int, int, int, list[str], str]:
    proc = Path("/proc") / str(pid)
    stat = (proc / "stat").read_text()
    # The comm field can itself contain spaces and parentheses.
    fields = stat[stat.rfind(")") + 2:].split()
    argv = (proc / "cmdline").read_bytes().split(b"\0")
    cgroup = (proc / "cgroup").read_text().strip()
    return int(fields[1]), int(fields[2]), int(fields[19]), [os.fsdecode(x) for x in argv if x], cgroup


def _is_zombie(pid: int) -> bool:
    stat = (Path("/proc") / str(pid) / "stat").read_text()
    return stat[stat.rfind(")") + 2:].split()[0] == "Z"


def _service_identity() -> tuple[int, str, str]:
    text = subprocess.check_output(
        ["systemctl", "show", UNIT, "-p", "MainPID", "-p", "InvocationID", "-p", "ControlGroup"],
        text=True,
    )
    values = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    return int(values.get("MainPID") or 0), values.get("InvocationID", ""), values.get("ControlGroup", "")


def _resume_valid_supervisor(supervisor: int, invocation: str) -> None:
    """Fail-safe: never leave this exact service invocation stopped on errors."""
    main, current_invocation, control_group = _service_identity()
    parent = _proc_info(supervisor)
    if (not main or not invocation or current_invocation != invocation or not control_group
            or parent[0] != main or parent[4] != f"0::{control_group}"
            or not any(Path(a).name == SCRIPT for a in parent[3])
            or "--preopen-source-only" not in parent[3]):
        raise RuntimeError("refusing to resume an unrelated process")
    fd = _pidfd_open(supervisor)
    try:
        if (_proc_info(supervisor) != parent
                or _service_identity() != (main, invocation, control_group)):
            raise RuntimeError("supervisor changed during fail-safe verification")
        _pidfd_signal(fd, signal.SIGCONT)
    finally:
        os.close(fd)


def _validated_pair(supervisor: int, child: int, invocation: str) -> tuple[int, int]:
    main, current_invocation, control_group = _service_identity()
    if not main or not invocation or current_invocation != invocation or not control_group:
        raise RuntimeError("source service invocation changed")
    parent = _proc_info(supervisor)
    worker = _proc_info(child)
    if (parent[0] != main or worker[0] != supervisor or worker[1] != child
            or not any(Path(a).name == SCRIPT for a in parent[3])
            or "--preopen-source-only" not in parent[3]
            or not ((any(Path(a).name == SCRIPT for a in worker[3])
                     and "--preopen-source-child" in worker[3])
                    or (not worker[3] and _is_zombie(child)))
            or parent[4] != f"0::{control_group}" or worker[4] != parent[4]):
        raise RuntimeError("source supervisor/child identity does not match the owned service")
    supervisor_fd = _pidfd_open(supervisor)
    try:
        child_fd = _pidfd_open(child)
    except BaseException:
        os.close(supervisor_fd)
        raise
    try:
        if (_proc_info(supervisor) != parent or _proc_info(child) != worker
                or _service_identity() != (main, invocation, control_group)):
            raise RuntimeError("source process changed during identity verification")
        return supervisor_fd, child_fd
    except BaseException:
        os.close(supervisor_fd)
        os.close(child_fd)
        raise


def _hold_until_child_done(supervisor_fd: int, child_fd: int, deadline_monotonic: float) -> str:
    """Resume on *every* catchable exit; pidfds prevent PID reuse races."""
    paused = False
    try:
        paused = True
        _pidfd_signal(supervisor_fd, signal.SIGSTOP)
        while True:
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                return "exit_protection_deadline"
            ready, _, _ = select.select([supervisor_fd, child_fd], [], [], min(remaining, 30.0))
            if supervisor_fd in ready:
                return "supervisor_exited"
            if child_fd in ready:
                return "child_finished"
    finally:
        if paused:
            try:
                _pidfd_signal(supervisor_fd, signal.SIGCONT)
            except OSError as exc:
                if exc.errno != errno.ESRCH:
                    raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--supervisor", required=True, type=int)
    parser.add_argument("--child", type=int)
    parser.add_argument("--invocation", required=True)
    parser.add_argument("--session", required=True, type=date.fromisoformat)
    parser.add_argument("--resume-only", action="store_true")
    args = parser.parse_args()
    now = datetime.now(TAIPEI)
    if now.date() != args.session:
        raise RuntimeError("handoff is limited to the specified local session")
    if args.resume_only:
        try:
            _resume_valid_supervisor(args.supervisor, args.invocation)
        except (FileNotFoundError, ProcessLookupError, RuntimeError):
            print("handoff=fail_safe_no_matching_supervisor", flush=True)
        else:
            print("handoff=fail_safe_resumed", flush=True)
        return 0
    if args.child is None:
        parser.error("--child is required unless --resume-only is used")
    try:
        supervisor_fd, child_fd = _validated_pair(args.supervisor, args.child, args.invocation)
    except BaseException:
        # A restarted watcher can race a child finishing; the supervisor may
        # still be stopped by its predecessor, so recover it on any error.
        try:
            _resume_valid_supervisor(args.supervisor, args.invocation)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            pass
        raise
    try:
        remaining = (datetime.combine(args.session, wall_time(13, 20), tzinfo=TAIPEI) - now).total_seconds()
        if remaining <= 0:
            # A restarted handoff may find the legacy supervisor already paused.
            _pidfd_signal(supervisor_fd, signal.SIGCONT)
            print("handoff=not_started reason=exit_protection_deadline", flush=True)
            return 0

        def interrupted(signum, _frame):
            raise InterruptedError(f"handoff interrupted by signal {signum}")

        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        print(f"handoff=watching supervisor={args.supervisor} child={args.child}", flush=True)
        reason = _hold_until_child_done(supervisor_fd, child_fd, time.monotonic() + remaining)
        print(f"handoff=resumed reason={reason}", flush=True)
        return 0
    finally:
        os.close(child_fd)
        os.close(supervisor_fd)


if __name__ == "__main__":
    raise SystemExit(main())
