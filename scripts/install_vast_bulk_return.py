#!/usr/bin/env python3
"""Persist the existing D-backed preservation owner, without launching duplicates."""
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
from stockagent.data_sync.bulk_archive import native_guard, load_policy
from stockagent.data_sync.desync_snapshots import atomic_write_bytes
from stockagent.data_sync.offhost_backup import private_json, private_file


def upgrade_body(kind: str, before: bytes | None, body: bytes, *, upgrade: bool) -> tuple[bytes, bool]:
    if before is None:
        return body, False
    if before.rstrip(b"\n") == body.rstrip(b"\n"):
        return before, False
    anchor = b"OnActiveSec=30s\n"
    predecessor = body.replace(anchor, b"")
    if (not upgrade or kind != "timer" or body.count(anchor) != 1
            or before.rstrip(b"\n") != predecessor.rstrip(b"\n")):
        raise ValueError("preserve a different existing bulk return unit")
    # Preserve the installed unit, including its harmless final blank lines;
    # admit only the known predecessor and insert the independent restart anchor.
    return before.replace(b"OnBootSec=3min\n", b"OnBootSec=3min\n" + anchor, 1), True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--upgrade-restart-anchor", action="store_true",
                        help="upgrade only the known previous timer missing OnActiveSec; preserve every other difference")
    args = parser.parse_args()
    if os.geteuid() != 0 or socket.gethostname() != "penguin" or args.evidence.exists():
        parser.error("penguin root and a fresh evidence path required")
    native_guard()
    load_policy()
    private_file(Path("/etc/stockagent/vast-legacy-cache-capture.json"))
    os.umask(0o077)
    units = []
    planned = []
    anchor_upgraded = False
    for kind in ("service", "timer"):
        name = "stockagent-vast-bulk-return." + kind
        body = (ROOT / "deploy/systemd" / (name + ".in")).read_text().replace("__REPO_ROOT__", str(ROOT)).encode()
        path = Path("/etc/systemd/system") / name
        before = path.read_bytes() if path.exists() and not path.is_symlink() else None
        if path.is_symlink():
            raise ValueError("preserve a redirected bulk return unit")
        body, upgraded = upgrade_body(kind, before, body, upgrade=args.upgrade_restart_anchor)
        anchor_upgraded |= upgraded
        planned.append((path, before, body))
    # Check every unit before making any change.
    for path, before, body in planned:
        atomic_write_bytes(path, body, mode=0o644)
        units.append({"path": str(path), "sha256": hashlib.sha256(body).hexdigest(),
                      "before_sha256": hashlib.sha256(before).hexdigest() if before is not None else None})
    for command in (["systemd-analyze", "verify", *[r["path"] for r in units]],
                    ["systemctl", "daemon-reload"],
                    ["systemctl", "enable", "stockagent-vast-bulk-return.timer"]):
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
    if anchor_upgraded:
        # Restart the timer only. An in-flight service retains its owner, journal
        # and process tree; future attempts still obey the same cold/source gates.
        subprocess.run(["systemctl", "restart", "stockagent-vast-bulk-return.timer"],
                       check=True, capture_output=True, text=True, timeout=30)
    receipt = {"state": "restart_anchor_upgraded" if anchor_upgraded else "persistent_bulk_retry_installed_not_started", "units": units,
               "timer": "stockagent-vast-bulk-return.timer", "source_files_deleted_by_install": False,
               "restart_anchor_upgraded": anchor_upgraded, "active_service_restarted": False,
               "publication_owner_reused": True, "coordinator_journal_owner_required": True,
               "handoff_required_for_existing_unsupervised_owner": True}
    private_json(args.evidence, receipt)
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
