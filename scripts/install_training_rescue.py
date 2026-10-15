#!/usr/bin/env python3
"""Install only the Penguin training preservation owner; never starts training."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.training_rescue import Rescue, load_policy  # noqa: E402
from stockagent.training_rescue_worker import atomic_json  # noqa: E402
from stockagent.data_sync.desync_snapshots import atomic_write_bytes  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--evidence", type=Path, required=True)
    args = p.parse_args()
    if os.geteuid() != 0 or socket.gethostname() != "penguin" or args.evidence.exists():
        raise ValueError("install in Penguin owner namespace with fresh evidence")
    if os.readlink("/proc/self/ns/mnt") != os.readlink("/proc/1/ns/mnt"):
        raise ValueError("enter PID1 mount namespace before installing the D preservation owner")
    rescue = Rescue(load_policy(ROOT / "configs/training_rescue.json")); rescue.guard()
    if os.environ.get("WSL_DISTRO_NAME") != rescue.policy["windows_distribution"]:
        raise ValueError("verify the actual WSL distribution before installing its storage owner")
    if not any((rescue.state / "runs").glob("*/spec.json")):
        raise ValueError("enroll a real formal run before enabling a preservation timer")
    observed = {}
    for name in ("service", "timer"):
        source = ROOT / f"deploy/systemd/stockagent-training-rescue.{name}.in"
        destination = Path(f"/etc/systemd/system/stockagent-training-rescue.{name}")
        body = source.read_text().replace("__REPO_ROOT__", str(ROOT)).encode()
        if destination.exists() and destination.read_bytes() != body:
            raise ValueError("inspect an existing different rescue owner before replacing it")
        atomic_write_bytes(destination, body, mode=0o644)
        observed[str(destination)] = hashlib.sha256(body).hexdigest()
    subprocess.run(["systemd-analyze", "verify", "/etc/systemd/system/stockagent-training-rescue.service",
                    "/etc/systemd/system/stockagent-training-rescue.timer"], check=True, capture_output=True)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "--now", "stockagent-training-rescue.timer"], check=True, capture_output=True)
    subprocess.run(["systemctl", "is-active", "stockagent-training-rescue.timer"], check=True, capture_output=True)
    receipt = {"state": "training_preservation_timer_enabled", "unit_sha256": observed,
               "timer_interval_seconds": 30, "training_started": False, "source_files_deleted": False,
               "existing_backup_and_completed_return_owners_modified": False,
               "automatic_destination_selection": False, "real_new_vast_resume_verified": False}
    atomic_json(args.evidence / "installation.json", receipt)
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
