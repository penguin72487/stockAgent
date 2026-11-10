#!/usr/bin/env python3
"""Install startup ordering for the single service-placement operator.

Existing service bodies and running processes are preserved. The dependency
applies to their next start; initial placement retains the original owner gate,
while reboot replay must restore the enrolled control mounts before startup.
"""
import argparse
import json
from pathlib import Path
import subprocess

from stockagent.data_sync.desync_snapshots import atomic_write_bytes

UNIT = "stockagent-service-layout.service"
ORDERING = b"[Unit]\nRequires=stockagent-service-layout.service\nAfter=stockagent-service-layout.service\n"
DATA_READERS = frozenset({"stockagent-discord-bot.service", "stockagent-tw-day-trade-simulation.service",
                         "stockagent-tw-overnight-simulation.service", "stockagent-public-dashboards.service"})
DATA_MOUNTS = ("stockagent-e-workspace-mount.service", "root-stockAgent-data_finlab.mount",
               "root-stockAgent-data_finmind.mount", "root-stockAgent-data_tej.mount")
DATA_ORDERING = ("[Unit]\nWants=" + " ".join(DATA_MOUNTS) + "\nAfter="
                 + " ".join(DATA_MOUNTS) + "\n").encode()


def install(root: Path, systemd: Path, *, apply: bool = False) -> dict:
    root, systemd = root.resolve(strict=True), systemd.resolve(strict=True)
    body = (root / "deploy/systemd" / (UNIT + ".in")).read_text().replace("@REPO_ROOT@", str(root)).encode()
    files = {systemd / UNIT: body}
    ordered_units, data_readers = 0, 0
    enrolled_data_mounts = all((systemd / name).is_file() for name in DATA_MOUNTS)
    for unit in sorted(systemd.glob("stockagent*.service")):
        if unit.name != UNIT:
            files[systemd / (unit.name + ".d") / "30-service-storage-layout.conf"] = ORDERING
            ordered_units += 1
            if enrolled_data_mounts and unit.name in DATA_READERS:
                files[systemd / (unit.name + ".d") / "35-service-data-volume-order.conf"] = DATA_ORDERING
                data_readers += 1
    # Only the exact previously managed unit may upgrade in place. Unrelated
    # service bodies/drop-ins still require review and are never overwritten.
    prior_body = body.replace(b"StartLimitIntervalSec=0\n", b"").replace(
        b"Restart=on-failure\nRestartSec=5s\n", b"")
    for path, content in files.items():
        previous = path.read_bytes() if path.exists() else None
        managed_upgrade = path == systemd / UNIT and previous == prior_body
        if path.is_symlink() or (previous is not None and previous != content and not managed_upgrade):
            raise ValueError("existing unrelated placement installation must be reviewed: " + str(path))
        if path.parent.exists() and path.parent.resolve() != path.parent:
            raise ValueError("placement unit parent is redirected")
    if apply:
        for path, content in files.items():
            path.parent.mkdir(mode=0o755, exist_ok=True)
            if not path.exists() or path == systemd / UNIT and path.read_bytes() != content:
                atomic_write_bytes(path, content)
                path.chmod(0o644)
        subprocess.run(["systemctl", "daemon-reload"], check=True, timeout=60)
        subprocess.run(["systemctl", "enable", UNIT], check=True, timeout=30)
    return {"contract": "stockagent.service_storage_startup_order.v1", "apply": apply,
            "ordered_units": ordered_units, "data_readers_ordered": data_readers,
            "running_services_restarted": 0,
            "unit": str(systemd / UNIT)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--systemd", type=Path, default=Path("/etc/systemd/system"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(install(args.root, args.systemd, apply=args.apply)))
