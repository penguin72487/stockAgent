#!/usr/bin/env python3
"""Enable bounded completed-run returns on the existing penguin ingress owner."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.desync_snapshots import atomic_write_bytes  # noqa: E402
from stockagent.data_sync.offhost_backup import private_file, private_json  # noqa: E402
from stockagent.data_sync.training_return import admit_workspace, load_policy  # noqa: E402


def execute(args):
    return subprocess.run(args, capture_output=True, text=True, check=True, timeout=30)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--policy", type=Path, default=ROOT / "configs/data_sync/training_return.json")
    p.add_argument("--evidence", type=Path, required=True)
    args = p.parse_args()
    os.umask(0o077)
    if os.geteuid() != 0 or socket.gethostname() != "penguin" or args.evidence.exists():
        raise ValueError("use penguin's existing root owner and fresh installation evidence")
    policy = load_policy(args.policy)
    environment = Path("/etc/stockagent/remote-cold-artifact-ingress.env")
    private_file(environment)
    state = Path("/var/lib/stockagent-cold-artifacts")
    space = admit_workspace(state, policy["reserve_bytes"])
    execute(["bash", str(ROOT / "scripts/mount_packed_d_cold.sh"), "--check"])
    execute(["systemctl", "is-enabled", "stockagent-remote-cold-artifact-ingress.timer"])
    execute(["systemctl", "is-active", "stockagent-remote-cold-artifact-ingress.timer"])
    unit = Path("/etc/systemd/system/stockagent-remote-cold-artifact-ingress.service")
    timer = unit.with_suffix(".timer")
    before = {str(x): hashlib.sha256(x.read_bytes()).hexdigest() for x in (unit, timer)}
    original = environment.read_bytes()
    entries = {"COLD_ARTIFACT_RETURN_POLICY": str(args.policy.absolute())}
    if "microsoft" in Path("/proc/sys/kernel/osrelease").read_text().lower():
        # systemd does not inherit the interactive WSL distribution identity.
        # Persist the identity whose actual VHDX capacity was verified above.
        entries["WSL_DISTRO_NAME"] = os.environ.get("WSL_DISTRO_NAME", "")
    additions = []
    for name, setting in entries.items():
        key = name + "="
        value = key + shlex.quote(setting)
        found = [line for line in original.decode().splitlines() if line.startswith(key)]
        if found and found != [value]:
            raise ValueError("inspect a different existing training return environment before replacing it")
        if not found:
            additions.append(value)
    args.evidence.mkdir(mode=0o700, parents=True)
    atomic_write_bytes(args.evidence / "original-ingress.env.private", original, mode=0o600)
    if additions:
        if environment.read_bytes() != original:
            raise ValueError("private ingress environment changed during setup")
        atomic_write_bytes(environment, original.rstrip(b"\n") + b"\n" +
                           "\n".join(additions).encode() + b"\n", mode=0o600)
    dropin = Path("/etc/systemd/system/stockagent-remote-cold-artifact-ingress.service.d/training-return.conf")
    body = b"[Service]\nMemoryHigh=1G\nMemoryMax=2G\n"
    if dropin.exists() and dropin.read_bytes() != body:
        raise ValueError("preserve a different existing training return resource policy")
    atomic_write_bytes(dropin, body, mode=0o644)
    execute(["systemd-analyze", "verify", str(unit)])
    execute(["systemctl", "daemon-reload"])
    after = {str(x): hashlib.sha256(x.read_bytes()).hexdigest() for x in (unit, timer)}
    if before != after:
        raise ValueError("original service/timer changed while enabling training returns")
    receipt = {"state": "existing_training_return_owner_enabled", "original_units_unchanged": True,
               "second_timer_created": False, "existing_source_retention_unchanged": True,
               "policy_sha256": hashlib.sha256(args.policy.read_bytes()).hexdigest(),
               "workspace_admission": space, "source_inputs_deleted": False,
               "real_training_return_acceptance_verified": False}
    private_json(args.evidence / "installation.json", receipt)
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
