#!/usr/bin/env python3
"""Upgrade one known cohort only at its unowned transaction boundary.

Reuses the established SIGSTOP/check/SIGTERM handoff and task launcher. Never
terminates an in-flight publisher/SSH child or starts a second cohort writer.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.desync_snapshots import atomic_write_json, sha256_file, SnapshotError
from stockagent.data_sync.remote_legacy_return import real

COMMON = Path("/run/lock/stockagent-remote-cold-artifact-ingress.lock")
BULK_STATE = Path("/srv/stockagent-d-volume/stockagent-cold-primary/remote-artifact-incoming")
COHORTS = {Path("/var/lib/stockagent-vast-legacy-return"),
           Path("/var/lib/stockagent-vast-legacy-return-partitions"), BULK_STATE}


def legacy_systemd_owner(state_root: Path) -> str:
    owners = {Path("/var/lib/stockagent-vast-legacy-return"): "main",
              Path("/var/lib/stockagent-vast-legacy-return-partitions"): "partitions"}
    if state_root not in owners:
        raise SnapshotError("legacy service handoff requires an exact retained cohort")
    return "stockagent-legacy-return@" + owners[state_root] + ".service"


def validate_worker_argv(argv: list[str], state_root: Path) -> None:
    if state_root == BULK_STATE:
        positions = [i for i, a in enumerate(argv) if Path(a).name == "organize_vast_bulk_archives.py"]
        if len(positions) != 1:
            raise SnapshotError("not the known default bulk organizer")
        options = argv[positions[0] + 1:]
        flags, batches = [], []
        while options:
            word = options.pop(0)
            if word == "--batch" and options:
                batches.append(Path(options.pop(0)))
            elif word in {"--watch", "--apply", "--retire"}:
                flags.append(word)
            else:
                raise SnapshotError("bulk organizer options differ from the known owner")
        if sorted(flags) != ["--apply", "--retire", "--watch"]:
            raise SnapshotError("not the known default bulk organizer")
        known = {BULK_STATE / name for name in (
            "20261004T094634-a473273e07ba", "20261004T102138-5cc3c20ef8b2",
            "20261004T102032-b371004176fb")}
        if any(path not in known for path in batches) or len(batches) != len(set(batches)):
            raise SnapshotError("bulk organizer does not name the exact retained cohorts")
        return
    if not any(Path(a).name == "return_remote_legacy_archives.py" for a in argv):
        raise SnapshotError("known worker program changed")
    configured_root = Path("/var/lib/stockagent-vast-legacy-return")
    for index, word in enumerate(argv):
        if word == "--state-root" and index + 1 < len(argv):
            configured_root = Path(argv[index + 1])
        elif word.startswith("--state-root="):
            configured_root = Path(word.partition("=")[2])
    if configured_root != state_root:
        raise SnapshotError("known worker belongs to a different cohort")


def observation(pid: int, start_ticks: int, state_root: Path) -> dict | None:
    proc = Path("/proc") / str(pid)
    try:
        fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
        argv = [(a.decode(errors="replace")) for a in (proc / "cmdline").read_bytes().split(b"\0") if a]
        if int(fields[19]) != start_ticks:
            raise SnapshotError("known worker PID identity changed")
        validate_worker_argv(argv, state_root)
        children = []
        for task in (proc / "task").iterdir():
            children.extend((task / "children").read_text().split())
        common_fd = holds = False
        for fd in (proc / "fd").iterdir():
            if fd.resolve() == COMMON:
                common_fd = True
                holds = holds or any(line.startswith("lock:") and "FLOCK" in line and "WRITE" in line
                    for line in (proc / "fdinfo" / fd.name).read_text().splitlines())
        return {"state": fields[0], "wchan": (proc / "wchan").read_text().strip(),
                "children": children, "common_fd_present": common_fd, "holds_common_lock": holds,
                "observation_complete": True}
    except FileNotFoundError:
        # /proc task and fd entries can disappear while the worker remains
        # alive. An incomplete observation must not be treated as worker exit
        # or as permission to freeze/terminate it; wait for a complete sample.
        return {"observation_complete": False} if proc.exists() else None


def safe_boundary(value: dict | None, *, stopped: bool = False) -> bool:
    return bool(value and value.get("observation_complete", True)
                and value["common_fd_present"] and not value["holds_common_lock"]
                and not value["children"] and
                (value["state"] == "T" if stopped else value["wchan"] == "locks_lock_inode_wait"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--state-root", required=True, type=Path)
    parser.add_argument("--run-name")
    service = parser.add_mutually_exclusive_group()
    service.add_argument("--bulk-systemd", action="store_true",
                         help="Hand the known bulk coordinator to its persistent service")
    service.add_argument("--legacy-systemd", action="store_true",
                         help="Hand the known legacy cohort to its existing persistent service")
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--wait-seconds", type=int, default=3600)
    args = parser.parse_args()
    os.umask(0o077)
    if args.state_root not in COHORTS or not 1 <= args.wait_seconds <= 7200 or args.receipt.exists():
        parser.error("known cohort, bounded wait and fresh receipt required")
    if (args.bulk_systemd and args.state_root != BULK_STATE
            or args.legacy_systemd and args.state_root == BULK_STATE
            or not (args.bulk_systemd or args.legacy_systemd) and not args.run_name):
        parser.error("bulk service handoff or a supervised run name is required")
    real(args.state_root)
    if args.state_root == BULK_STATE:
        from stockagent.data_sync.bulk_archive import native_guard
        native_guard()
    proc = Path("/proc") / str(args.pid)
    start = int((proc / "stat").read_text().rsplit(")", 1)[1].split()[19])
    deadline = time.monotonic() + args.wait_seconds
    while time.monotonic() < deadline:
        before = observation(args.pid, start, args.state_root)
        if before is None:
            raise SnapshotError("worker ended before handoff; inspect existing owner instead of guessing")
        if not safe_boundary(before):
            time.sleep(1)
            continue
        os.kill(args.pid, signal.SIGSTOP)
        try:
            for _ in range(20):
                frozen = observation(args.pid, start, args.state_root)
                if frozen and frozen["state"] == "T":
                    break
                time.sleep(0.05)
            if not safe_boundary(frozen, stopped=True):
                os.kill(args.pid, signal.SIGCONT)
                time.sleep(1)
                continue
            summary = ({} if args.state_root == BULK_STATE else
                       json.loads((args.state_root / "summary.json").read_text()))
            os.kill(args.pid, signal.SIGTERM)
            os.kill(args.pid, signal.SIGCONT)
        except BaseException:
            try:
                os.kill(args.pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
            raise
        for _ in range(100):
            if not proc.exists():
                break
            time.sleep(0.1)
        if proc.exists():
            raise SnapshotError("previous cohort owner has not exited; no competing writer started")
        command = ["stockagent-agent", "run", "--backend", "tmux", "--name", args.run_name,
            "vast-all-artifacts-cold-return-20261004", "--", "bash", "scripts/run_remote_legacy_archive_return.sh",
            "archive", "--apply", "--order", "reclaim-first", "--state-root", str(args.state_root)]
        unit = None
        if args.bulk_systemd or args.legacy_systemd:
            unit = ("stockagent-vast-bulk-return.service" if args.bulk_systemd
                    else legacy_systemd_owner(args.state_root))
            # PID exit can precede systemd's transaction completion. Wait for
            # the old job to end rather than having start attach to that job.
            for _ in range(100):
                state = subprocess.check_output(
                    ["systemctl", "show", unit, "-p", "MainPID", "-p", "ActiveState"], text=True)
                properties = dict(line.split("=", 1) for line in state.splitlines())
                if (properties.get("MainPID") == "0"
                        and properties.get("ActiveState") in {"inactive", "failed"}):
                    break
                time.sleep(0.1)
            else:
                raise SnapshotError("persistent owner did not finish its prior job; no replacement started")
            command = ["systemctl", "start", "--no-block", unit]
        elif args.state_root == BULK_STATE:
            command = ["stockagent-agent", "run", "--backend", "tmux", "--name", args.run_name,
                       "vast-all-artifacts-cold-return-20261004", "--", "bash",
                       "scripts/run_authority_storage_operation.sh",
                       "scripts/organize_vast_bulk_archives.py", "--watch", "--apply", "--retire"]
            # One unchanged publisher/retirement owner. Reorder only the exact
            # received cohort; do not preempt another worker's live transaction.
            for batch in ("20261004T094634-a473273e07ba", "20261004T102138-5cc3c20ef8b2",
                          "20261004T102032-b371004176fb"):
                command += ["--batch", str(BULK_STATE / batch)]
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30, cwd=ROOT)
        run = ({"unit": unit} if unit else json.loads(result.stdout))
        policy_name = ("vastai_bulk_preservation.json" if args.state_root == BULK_STATE
                       else "vastai_legacy_archive_return.json")
        receipt = {"state": "same_cohort_handed_off_at_unowned_boundary", "old_pid": args.pid,
            "old_start_ticks": start, "state_root": str(args.state_root), "before": before,
            "frozen": frozen, "summary_before": summary, "new_owner": run,
            "policy_path": "configs/data_sync/" + policy_name,
            "policy_sha256": sha256_file(ROOT / "configs/data_sync" / policy_name),
            "source_files_changed": False, "production_processes_stopped": False,
            "observed_at_epoch": time.time()}
        if "run_id" in run:
            receipt["new_run_id"] = run["run_id"]
        atomic_write_json(args.receipt, receipt)
        print(json.dumps(receipt), flush=True)
        return
    raise SnapshotError("no unowned boundary observed; original worker preserved")


if __name__ == "__main__":
    main()
