#!/usr/bin/env python3
"""Schedule canonical retries for explicitly retained Vast legacy cohorts."""
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
    parser.add_argument("--reviewed-cleanup-20261006", action="store_true")
    args = parser.parse_args()
    if os.geteuid() != 0 or socket.gethostname() != "penguin" or args.evidence.exists():
        raise ValueError("use penguin's root owner and a fresh evidence file")
    os.umask(0o077)
    private_file(Path("/etc/stockagent/remote-cold-artifact-ingress.env"))
    execute(["bash", str(ROOT / "scripts/mount_packed_d_cold.sh"), "--check"])
    cohorts = {"main": "/var/lib/stockagent-vast-legacy-return",
               "partitions": "/var/lib/stockagent-vast-legacy-return-partitions"}
    if args.reviewed_cleanup_20261006:
        from scripts.return_remote_legacy_archives import load_cohort_selection, preservation_policy
        preservation_policy(json.loads((ROOT / "configs/data_sync/vastai_reviewed_legacy_return_20261006.json").read_text()))
        cohorts.update({"reviewed-main-20261006": "/var/lib/stockagent-vast-reviewed-return-20261006",
                        "reviewed-panel-20261006": "/var/lib/stockagent-vast-reviewed-partitions-20261006"})
        panel = Path(cohorts["reviewed-panel-20261006"])
        selected = load_cohort_selection(panel / "scope-selection.json", panel)
        if any(not root.startswith("markets/tw_day_trade_factorized_panel_20261004_v1/") for root in selected):
            raise ValueError("reviewed panel scope may not overlap the retained ablation cohort")
    for cohort in cohorts.values():
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
    if args.reviewed_cleanup_20261006:
        for cohort in ("main", "partitions"):
            path = Path("/etc/systemd/system") / ("stockagent-legacy-return@" + cohort + ".service.d") / "reviewed-20261006.conf"
            body = b'[Service]\nEnvironment="STOCKAGENT_LEGACY_REVIEWED_20261006=1"\n'
            if path.is_symlink() or (path.exists() and path.read_bytes() != body):
                raise ValueError("preserve a different existing cohort policy override")
            path.parent.mkdir(mode=0o755, exist_ok=True)
            atomic_write_bytes(path, body, mode=0o644)
            units.append({"path": str(path), "sha256": hashlib.sha256(body).hexdigest()})
    execute(["systemd-analyze", "verify", *[unit["path"] for unit in units if not unit["path"].endswith('.conf')]])
    execute(["systemctl", "daemon-reload"])
    timers = ["stockagent-legacy-return@" + name + ".timer" for name in cohorts]
    execute(["systemctl", "enable", "--now", *timers])
    for timer in timers:
        execute(["systemctl", "is-enabled", timer])
        execute(["systemctl", "is-active", timer])
    receipt = {"state": "retained_legacy_cohort_retry_timers_installed", "units": units,
               "timers": timers, "retry_interval_seconds": 300,
               "existing_cohort_owner_required": True, "inventories_replaced": False,
               "shared_ingress_owner_reused": True, "source_files_deleted_by_install": False,
               "complete_history_verified": False,
               "reviewed_cleanup_20261006": args.reviewed_cleanup_20261006}
    private_json(args.evidence, receipt)
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
