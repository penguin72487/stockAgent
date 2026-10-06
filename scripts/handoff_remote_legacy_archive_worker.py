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
LOCAL_CLEANUP = ROOT / "artifacts/operations/wsl-storage-cleanup-20261006"
LOCAL_COHORTS = {
    LOCAL_CLEANUP / name: LOCAL_CLEANUP / inventory
    for name, inventory in (
        ("pilot", "pilot-inventory.json"),
        ("offline-reviewed", "offline-eligible-inventory.json"),
        ("offline-simulations", "offline-simulation-inventory.json"),
        ("existing-rule-retirement", "existing-rule-inventory.json"),
        ("closed-repair-retirement", "closed-repair-eligible-inventory.json"),
        ("uncaptured-cache-retirement", "uncaptured-cache-reinventory.json"),
        ("remaining-unencoded-retirement", "remaining-unencoded-reinventory-eligible.json"),
    )
}
LOCAL_COHORTS[LOCAL_CLEANUP / 'margin-derived-retirement'] = LOCAL_CLEANUP / 'margin-derived-batched-inventory.json'
COHORTS |= LOCAL_COHORTS.keys()
SCRATCH_COHORTS = {
    Path('/var/lib/stockagent-vast-legacy-return'): LOCAL_CLEANUP / 'return-main-prune',
    Path('/var/lib/stockagent-vast-legacy-return-partitions'): LOCAL_CLEANUP / 'return-partition-prune',
}


def legacy_systemd_owner(state_root: Path) -> str:
    owners = {Path("/var/lib/stockagent-vast-legacy-return"): "main",
              Path("/var/lib/stockagent-vast-legacy-return-partitions"): "partitions"}
    if state_root not in owners:
        raise SnapshotError("legacy service handoff requires an exact retained cohort")
    return "stockagent-legacy-return@" + owners[state_root] + ".service"


def validate_worker_argv(argv: list[str], state_root: Path, *, scratch_prune: bool = False) -> None:
    if scratch_prune:
        positions = [i for i, word in enumerate(argv) if Path(word).name == 'prune_verified_return_scratch.py']
        if state_root not in SCRATCH_COHORTS or len(positions) != 1:
            raise SnapshotError('not the known private scratch cleanup worker')
        options = argv[positions[0]+1:]
        if (len(options) not in (5,6) or options[0] != '--state-root' or options[2] != '--receipt-dir'
                or options[4] != '--apply' or Path(options[1]) != state_root
                or (ROOT / options[3]).absolute() != SCRATCH_COHORTS[state_root]
                or options[5:] not in ([],['--wait-for-owner'])):
            raise SnapshotError('private scratch cleanup differs from the fixed cohort')
        return
    if state_root in LOCAL_COHORTS:
        positions = [i for i, word in enumerate(argv)
                     if Path(word).name == "retire_local_offline_artifacts.py"]
        if len(positions) != 1:
            raise SnapshotError("not the known local cleanup worker")
        options = argv[positions[0] + 1:]
        if (len(options) not in (5, 7, 8) or options[0] != "apply" or options[1] != "--inventory"
                or options[3] != "--receipt-dir"
                or (ROOT / options[2]).absolute() != LOCAL_COHORTS[state_root]
                or (ROOT / options[4]).absolute() != state_root
                or (len(options) == 7 and options[5:] != ['--retry-rounds', '3'])
                or (len(options) == 8 and options[5:] != ['--wait-for-owner', '--retry-rounds', '3'])):
            raise SnapshotError("local cleanup worker differs from the fixed cohort")
        return
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


def observation(pid: int, start_ticks: int, state_root: Path, *, scratch_prune: bool = False) -> dict | None:
    proc = Path("/proc") / str(pid)
    try:
        fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
        argv = [(a.decode(errors="replace")) for a in (proc / "cmdline").read_bytes().split(b"\0") if a]
        if int(fields[19]) != start_ticks:
            raise SnapshotError("known worker PID identity changed")
        validate_worker_argv(argv, state_root, scratch_prune=scratch_prune)
        children = []
        for task in (proc / "task").iterdir():
            children.extend((task / "children").read_text().split())
        common_fd = holds = cohort_fd = holds_cohort = False
        queued = state_root in LOCAL_COHORTS and argv[-3:] == ['--wait-for-owner', '--retry-rounds', '3']
        retry_three = state_root in LOCAL_COHORTS and argv[-2:] == ['--retry-rounds', '3']
        for fd in (proc / "fd").iterdir():
            if fd.resolve() == COMMON:
                common_fd = True
                holds = holds or any(line.startswith("lock:") and "FLOCK" in line and "WRITE" in line
                    for line in (proc / "fdinfo" / fd.name).read_text().splitlines())
            if queued and fd.resolve() == state_root / 'owner.lock':
                cohort_fd = True
                holds_cohort = holds_cohort or any(line.startswith('lock:') and 'FLOCK' in line and 'WRITE' in line
                    for line in (proc / 'fdinfo' / fd.name).read_text().splitlines())
        return {"state": fields[0], "wchan": (proc / "wchan").read_text().strip(),
                "children": children, "common_fd_present": common_fd, "holds_common_lock": holds,
                'queued_owner_wait': queued, 'cohort_fd_present': cohort_fd, 'holds_cohort_lock': holds_cohort,
                'retry_rounds_three': retry_three,
                "observation_complete": True}
    except FileNotFoundError:
        # /proc task and fd entries can disappear while the worker remains
        # alive. An incomplete observation must not be treated as worker exit
        # or as permission to freeze/terminate it; wait for a complete sample.
        return {"observation_complete": False} if proc.exists() else None


def safe_boundary(value: dict | None, *, stopped: bool = False) -> bool:
    return bool(value and value.get("observation_complete", True)
                and not value["holds_common_lock"]
                and (value["common_fd_present"] or (
                    value.get('queued_owner_wait') and value.get('cohort_fd_present')
                    and not value.get('holds_cohort_lock', True)))
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
    service.add_argument('--scratch-prune-worker', action='store_true',
                         help='Resume the same explicitly enrolled local scratch cleanup')
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--wait-seconds", type=int, default=3600)
    args = parser.parse_args()
    os.umask(0o077)
    if args.state_root not in COHORTS or not 1 <= args.wait_seconds <= 7200 or args.receipt.exists():
        parser.error("known cohort, bounded wait and fresh receipt required")
    if (args.bulk_systemd and args.state_root != BULK_STATE
            or args.legacy_systemd and (args.state_root == BULK_STATE or args.state_root in LOCAL_COHORTS)
            or not (args.bulk_systemd or args.legacy_systemd) and not args.run_name):
        parser.error("bulk service handoff or a supervised run name is required")
    if args.scratch_prune_worker and (args.state_root not in SCRATCH_COHORTS or not args.run_name):
        parser.error('scratch cleanup handoff needs its retained cohort and run name')
    real(args.state_root)
    if args.state_root == BULK_STATE:
        from stockagent.data_sync.bulk_archive import native_guard
        native_guard()
    proc = Path("/proc") / str(args.pid)
    start = int((proc / "stat").read_text().rsplit(")", 1)[1].split()[19])
    deadline = time.monotonic() + args.wait_seconds
    while time.monotonic() < deadline:
        before = observation(args.pid, start, args.state_root, scratch_prune=args.scratch_prune_worker)
        if before is None:
            raise SnapshotError("worker ended before handoff; inspect existing owner instead of guessing")
        if not safe_boundary(before):
            time.sleep(1)
            continue
        os.kill(args.pid, signal.SIGSTOP)
        try:
            for _ in range(20):
                frozen = observation(args.pid, start, args.state_root, scratch_prune=args.scratch_prune_worker)
                if frozen and frozen["state"] == "T":
                    break
                time.sleep(0.05)
            if not safe_boundary(frozen, stopped=True):
                os.kill(args.pid, signal.SIGCONT)
                time.sleep(1)
                continue
            if args.state_root in LOCAL_COHORTS:
                inventory = real(LOCAL_COHORTS[args.state_root])
                summary = {"inventory_sha256": sha256_file(inventory)}
                progress = args.state_root / "progress.json"
                if progress.exists():
                    summary["progress"] = json.loads(real(progress).read_text())
            else:
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
        if args.scratch_prune_worker:
            command = ['stockagent-agent', 'run', '--backend', 'tmux', '--name', args.run_name,
                       'penguin-wsl-storage-cleanup-20261006', '--', 'bash',
                       'scripts/run_authority_storage_operation.sh', 'scripts/prune_verified_return_scratch.py',
                       '--state-root', str(args.state_root), '--receipt-dir', str(SCRATCH_COHORTS[args.state_root]),
                       '--apply', '--wait-for-owner']
        elif args.state_root in LOCAL_COHORTS:
            if sha256_file(LOCAL_COHORTS[args.state_root]) != summary["inventory_sha256"]:
                raise SnapshotError("local inventory changed during handoff; no replacement started")
            command = ["stockagent-agent", "run", "--backend", "tmux", "--name", args.run_name,
                       "penguin-wsl-storage-cleanup-20261006", "--", "bash",
                       "scripts/run_authority_storage_operation.sh",
                       "scripts/retire_local_offline_artifacts.py", "apply", "--inventory",
                       str(LOCAL_COHORTS[args.state_root]), "--receipt-dir", str(args.state_root)]
            if before.get('queued_owner_wait'):
                command += ['--wait-for-owner']
            if before.get('retry_rounds_three'):
                command += ['--retry-rounds', '3']
        elif args.bulk_systemd or args.legacy_systemd:
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
        policy_path = (LOCAL_COHORTS[args.state_root] if args.state_root in LOCAL_COHORTS else
                       ROOT / "configs/data_sync" / ("vastai_bulk_preservation.json"
                       if args.state_root == BULK_STATE else "vastai_legacy_archive_return.json"))
        receipt = {"state": "same_cohort_handed_off_at_unowned_boundary", "old_pid": args.pid,
            "old_start_ticks": start, "state_root": str(args.state_root), "before": before,
            "frozen": frozen, "summary_before": summary, "new_owner": run,
            "policy_path": str(policy_path.relative_to(ROOT)),
            "policy_sha256": sha256_file(policy_path),
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
