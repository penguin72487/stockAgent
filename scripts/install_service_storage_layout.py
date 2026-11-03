#!/usr/bin/env python3
"""Install startup ordering for the single service-placement operator.

Existing service bodies and running processes are preserved. The dependency
only applies to their next start; the operator refuses live control owners.
"""
import argparse
import json
from pathlib import Path
import subprocess

from stockagent.data_sync.desync_snapshots import atomic_write_bytes

UNIT = "stockagent-service-layout.service"
ORDERING = b"[Unit]\nRequires=stockagent-service-layout.service\nAfter=stockagent-service-layout.service\n"


def install(root: Path, systemd: Path, *, apply: bool = False) -> dict:
    root, systemd = root.resolve(strict=True), systemd.resolve(strict=True)
    body = (root / "deploy/systemd" / (UNIT + ".in")).read_text().replace("@REPO_ROOT@", str(root)).encode()
    files = {systemd / UNIT: body}
    for unit in sorted(systemd.glob("stockagent*.service")):
        if unit.name != UNIT:
            files[systemd / (unit.name + ".d") / "30-service-storage-layout.conf"] = ORDERING
    for path, content in files.items():
        if path.is_symlink() or (path.exists() and path.read_bytes() != content):
            raise ValueError("existing unrelated placement installation must be reviewed: " + str(path))
        if path.parent.exists() and path.parent.resolve() != path.parent:
            raise ValueError("placement unit parent is redirected")
    if apply:
        for path, content in files.items():
            path.parent.mkdir(mode=0o755, exist_ok=True)
            if not path.exists():
                atomic_write_bytes(path, content)
                path.chmod(0o644)
        subprocess.run(["systemctl", "daemon-reload"], check=True, timeout=60)
        subprocess.run(["systemctl", "enable", UNIT], check=True, timeout=30)
    return {"contract": "stockagent.service_storage_startup_order.v1", "apply": apply,
            "ordered_units": len(files) - 1, "running_services_restarted": 0,
            "unit": str(systemd / UNIT)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--systemd", type=Path, default=Path("/etc/systemd/system"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(install(args.root, args.systemd, apply=args.apply)))
