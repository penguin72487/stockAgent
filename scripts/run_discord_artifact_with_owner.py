#!/usr/bin/env python3
"""Run the existing maintenance worker with the verified Gateway code view.

Only the worker's entrypoint is read from the current workspace and pinned by
SHA-256. Its model, bot and other imports come from the running Gateway's
read-only namespace. The worker stays in its own systemd cgroup; it does not
connect a Gateway, restart services, bypass checkpoint checks, or place orders.
Receipt-only and pre-open source controller commands retain their usual path.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live.service_code_release import (
    configured_release, native_owner_pid, verify_owner_namespace,
)

ENTRY = ROOT / "scripts/run_discord_artifact_maintenance.py"
MAX_SOURCE_BYTES = 2 * 1024 * 1024

# Executed inside the existing Gateway mount namespace, but in the maintenance
# worker's own cgroup. The /proc/1/root path addresses exactly the workspace
# entrypoint verified by the outer controller, not the frozen older script.
CHILD = r'''
import hashlib, json, os, stat, sys
from pathlib import Path
root = Path(os.environ["STOCKAGENT_MAINTENANCE_ROOT"])
receipt = Path(os.environ["STOCKAGENT_MAINTENANCE_RELEASE"])
pid = int(os.environ["STOCKAGENT_MAINTENANCE_OWNER_PID"])
from stockagent.live.service_code_release import configured_release, native_owner_pid, verify_mounted_source, verify_owner_namespace
if configured_release(root) != receipt or native_owner_pid(root, role="discord") != pid:
    raise RuntimeError("Discord owner or configured release changed before maintenance startup")
verify_owner_namespace(receipt, root, pid, role="discord")
verify_mounted_source(receipt, root, os.getpid())
path = Path("/proc/1/root") / str(root).lstrip("/") / "scripts/run_discord_artifact_maintenance.py"
fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
with os.fdopen(fd, "rb") as source:
    before = os.fstat(source.fileno())
    if not stat.S_ISREG(before.st_mode) or before.st_size > 2 * 1024 * 1024:
        raise ValueError("maintenance source is not a bounded regular file")
    body = source.read(2 * 1024 * 1024 + 1)
    after = os.fstat(source.fileno())
    generation = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if (len(body) > 2 * 1024 * 1024 or generation(before) != generation(after)
            or generation(before) != generation(path.lstat())
            or hashlib.sha256(body).hexdigest() != os.environ["STOCKAGENT_MAINTENANCE_ENTRY_SHA256"]):
        raise RuntimeError("maintenance entrypoint changed before execution")
print(json.dumps({"status": "verified_gateway_source_for_maintenance", "owner_pid": pid,
                  "release": str(receipt), "entry_sha256": hashlib.sha256(body).hexdigest(),
                  "worker_cgroup_unchanged": True}), flush=True)
if os.environ.pop("STOCKAGENT_MAINTENANCE_VERIFY_ONLY", "0") == "1":
    raise SystemExit(0)
sys.argv[0] = str(path)
exec(compile(body, str(path), "exec"), {"__name__": "__main__", "__file__": str(path)})
'''


def _stable_entry_digest(path: Path) -> str:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_SOURCE_BYTES:
            raise ValueError("maintenance source is not a bounded regular file")
        body = source.read(MAX_SOURCE_BYTES + 1)
        after = os.fstat(source.fileno())
        generation = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if (len(body) > MAX_SOURCE_BYTES or generation(before) != generation(after)
                or generation(before) != generation(path.lstat())):
            raise RuntimeError("maintenance source changed during pinning")
    return hashlib.sha256(body).hexdigest()


def owner_command(arguments: list[str], *, verify_only: bool = False) -> tuple[list[str], dict[str, str]]:
    receipt = configured_release(ROOT)
    if receipt is None:
        raise RuntimeError("Discord Gateway has no verified pinned code release")
    pid = native_owner_pid(ROOT, role="discord")
    verify_owner_namespace(receipt, ROOT, pid, role="discord")
    if configured_release(ROOT) != receipt or native_owner_pid(ROOT, role="discord") != pid:
        raise RuntimeError("Discord owner or configured release changed during pinning")
    environment = os.environ.copy()
    environment.update({"STOCKAGENT_MAINTENANCE_ROOT": str(ROOT),
                        "STOCKAGENT_MAINTENANCE_RELEASE": str(receipt),
                        "STOCKAGENT_MAINTENANCE_OWNER_PID": str(pid),
                        "STOCKAGENT_MAINTENANCE_ENTRY_SHA256": _stable_entry_digest(ENTRY)})
    if verify_only:
        environment["STOCKAGENT_MAINTENANCE_VERIFY_ONLY"] = "1"
    command = ["nsenter", "--target", str(pid), "--mount", f"--wdns={ROOT}",
               "--", sys.executable, "-c", CHILD, *arguments]
    return command, environment


def main() -> None:
    args = sys.argv[1:]
    verify_only = args == ["--verify-code-only"]
    # Control probes and source recovery do not execute the model and must not
    # depend on Gateway liveness. Preserve their current canonical code path.
    if not verify_only and args not in ([], ["--signal-cache-only"]):
        os.execv(sys.executable, [sys.executable, str(ENTRY), *args])
    try:
        command, environment = owner_command([] if verify_only else args, verify_only=verify_only)
        os.execvpe(command[0], command, environment)
    except Exception as exc:
        print(json.dumps({"status": "gateway_source_unavailable", "error_type": type(exc).__name__,
                          "error": str(exc)[:500], "inference_started": False}), flush=True)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
