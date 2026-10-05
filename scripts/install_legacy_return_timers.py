#!/usr/bin/env python3
"""Schedule canonical retries for penguin's two retained legacy cohorts."""
from __future__ import annotations

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
from stockagent.data_sync.desync_snapshots import atomic_write_bytes
from stockagent.data_sync.offhost_backup import private_file, private_json


def execute(argv):
    return subprocess.run(argv, capture_output=True, text=True, check=True, timeout=30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0 or socket.gethostname() != "penguin" or args.evidence.exists():
        raise ValueError("use penguin's root owner and a fresh evidence file")
    os.umask(0o077)
    private_file(Path("/etc/stockagent/remote-cold-artifact-ingress.env"))
    execute(["bash", str(ROOT / "scripts/mount_packed_d_cold.sh"), "--check"])
    for cohort in ("/var/lib/stockagent-vast-legacy-return",
                   "/var/lib/stockagent-vast-legacy-return-partitions"):
        for name in ("inventory.json", "progress.json", "archive-catalog.json", "cohort-owner.lock"):
            path = Path(cohort) / name
            if not path.is_file() or path.is_symlink():
                raise ValueError("initialize and verify the retained cohort before scheduling it")
    units = []
    for kind in ("service", "timer"):
        name = "stockagent-legacy-return@." + kind
        template = ROOT / "deploy/systemd" / (name + ".in")
        body = template.read_text().replace("__REPO_ROOT__", str(ROOT)).encode()
        path = Path("/etc/systemd/system") / name
        if path.is_symlink() or (path.exists() and path.read_bytes() != body):
            raise ValueError("preserve a different existing legacy retry unit")
        atomic_write_bytes(path, body, mode=0o644)
        units.append({"path": str(path), "sha256": hashlib.sha256(body).hexdigest()})
    execute(["systemd-analyze", "verify", *[unit["path"] for unit in units]])
    execute(["systemctl", "daemon-reload"])
    timers = ["stockagent-legacy-return@main.timer", "stockagent-legacy-return@partitions.timer"]
    execute(["systemctl", "enable", "--now", *timers])
    for timer in timers:
        execute(["systemctl", "is-enabled", timer])
        execute(["systemctl", "is-active", timer])
    receipt = {"state": "retained_legacy_cohort_retry_timers_installed", "units": units,
               "timers": timers, "retry_interval_seconds": 300,
               "existing_cohort_owner_required": True, "inventories_replaced": False,
               "shared_ingress_owner_reused": True, "source_files_deleted_by_install": False,
               "complete_history_verified": False}
    private_json(args.evidence, receipt)
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
