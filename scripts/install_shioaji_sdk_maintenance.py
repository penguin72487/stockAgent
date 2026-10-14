#!/usr/bin/env python3
"""Install the canonical SDK maintenance timer; do not restart broker owners."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_text


def main() -> int:
    if os.geteuid() != 0:
        raise PermissionError("system SDK maintenance requires root")
    paths = []
    for suffix in ("service", "timer"):
        name = f"stockagent-shioaji-sdk-maintenance.{suffix}"
        body = (ROOT / "deploy/systemd" / f"{name}.in").read_text().replace("__REPO_ROOT__", str(ROOT))
        path = Path("/etc/systemd/system") / name
        marker = ("scripts/run_shioaji_sdk_maintenance.sh" if suffix == "service"
                  else "Unit=stockagent-shioaji-sdk-maintenance.service")
        if path.exists() and marker not in path.read_text():
            raise ValueError("existing unit belongs to another installer")
        atomic_write_text(path, body)
        path.chmod(0o644)
        paths.append(str(path))
    subprocess.run(["systemd-analyze", "verify", *paths], check=True)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "--now", "stockagent-shioaji-sdk-maintenance.timer"], check=True)
    subprocess.run(["systemctl", "is-active", "--quiet", "stockagent-shioaji-sdk-maintenance.timer"], check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
