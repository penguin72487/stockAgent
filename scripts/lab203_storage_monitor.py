#!/usr/bin/env python3
"""Locally install the fixed read-only storage observer from a verified handoff.

No source data is copied/deleted and the backup service is never restarted.
The already paired reverse receipt folder carries only storage metadata.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import stat
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
from scripts.backup_delivery_receipt import read_json, valid_signature
from scripts.verify_backup_delivery import verify
from stockagent.data_sync.desync_snapshots import atomic_write_bytes, atomic_write_json
from stockagent.data_sync.storage_monitor import guarded_volume, monitor

PACKAGE = "storage-observer-20261025-v1"
INSTALLED = Path("/opt/lab203-backup/storage-observer-code-v1")
CONFIG = Path("/etc/lab203-backup/storage-observer-v1.json")
UNIT = Path("/etc/systemd/system/lab203-storage-monitor.service")
PAIRED = ("producer_device_id", "receiver_device_id", "repository_id")


def private(path):
    path = Path(path)
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("redirected_private_configuration")
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077:
        raise ValueError("reuse_the_existing_private_root_configuration")
    return read_json(path)


def receiver_config(pipeline):
    return {"schema_version": 1, "observer_node_id": "lab203", "capacity_interval_seconds": 60,
        "classification_interval_seconds": 21600, "classification_timeout_seconds": 1800, "classification_depth": 3,
        "pairing": {key: pipeline[key] for key in PAIRED}, "volumes": [
            {"id": "lab203-disk", "path": pipeline["ingress_root"], "roots": [
                {"path": pipeline["ingress_root"], "label": "已封閉交付批次", "kind": "control"},
                {"path": pipeline["state_root"], "label": "備份狀態與日誌", "kind": "control"},
                {"path": pipeline["scratch_root"], "label": "獨立還原暫存", "kind": "materialized"}]},
            {"id": "nas-disk", "nas_configuration": pipeline["nas_configuration"]}]}


def unit_body(receipts: Path, *, code=INSTALLED, python=None) -> str:
    python = python or sys.executable
    # systemd quoting, not a shell. Paths may contain spaces; percent is an
    # escaped systemd specifier. No arbitrary command is loaded from receipts.
    quote = lambda p: json.dumps(str(p).replace("%", "%%"))
    args = [python, code / "scripts/lab203_storage_monitor.py", "--watch", "--config", CONFIG,
            "--output", receipts / "storage-status.json"]
    return "\n".join(["[Unit]", "Description=Read-only lab203 and guarded NAS storage observations",
        "After=local-fs.target network-online.target", "[Service]", "Type=simple",
        "WorkingDirectory=" + quote(code), "ExecStart=" + " ".join(quote(arg) for arg in args),
        "Environment=PYTHONDONTWRITEBYTECODE=1", "Restart=on-failure", "RestartSec=30", "Nice=19",
        "IOSchedulingClass=idle", "CPUQuota=30%", "MemoryMax=640M", "TasksMax=32", "KillMode=control-group",
        "TimeoutStopSec=10", "ProtectSystem=strict", "PrivateTmp=true", "ReadWritePaths=" + quote(receipts),
        "[Install]", "WantedBy=multi-user.target", ""])


def install(args):
    if os.geteuid() != 0:
        raise ValueError("install_locally_as_the_existing_lab203_root_owner")
    expected_root = Path("/srv/lab203-backup/ingress/tools") / PACKAGE
    if ROOT != expected_root or any(p.is_symlink() for p in (ROOT, *ROOT.parents)):
        raise ValueError("install_only_from_the_fixed_verified_ingress_package")
    manifest_path = ROOT / "handoff-manifest.json"
    raw = manifest_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != args.manifest_sha256:
        raise ValueError("handoff_does_not_match_the_source_manifest_pin")
    manifest = json.loads(raw)
    valid_signature(manifest, "frozen_backup_receiver_handoff_v1")
    if manifest.get("automatic_execution_from_syncthing") is not False or manifest.get("private_credentials_included") is not False:
        raise ValueError("handoff_requires_local_installation_and_no_credentials")
    envelope = read_json(ROOT / "backup-envelope.json")
    verify(ROOT, envelope["identity_sha256"])
    if envelope["source_plan_identity_sha256"] != manifest["identity_sha256"]:
        raise ValueError("handoff_envelope_is_not_this_manifest")
    pipeline = private(args.pipeline_config)
    if any(pipeline[key] != manifest[key] for key in PAIRED):
        raise ValueError("handoff_belongs_to_another_paired_receiver")
    private(Path(pipeline["nas_configuration"]))
    receipts = Path(pipeline["receipt_root"])
    if (not receipts.is_dir() or any(p.is_symlink() for p in (receipts, *receipts.parents))
            or Path(pipeline["ingress_root"]) != Path("/srv/lab203-backup/ingress")):
        raise ValueError("reuse_the_existing_paired_ingress_and_receipt_directories")
    config = receiver_config(pipeline)
    guarded_volume(config["volumes"][1])
    if INSTALLED.exists() or CONFIG.exists() or UNIT.exists():
        raise ValueError("preserve_existing_installation_and_verify_it_before_upgrading")
    shutil.copytree(ROOT, INSTALLED)
    verify(INSTALLED, envelope["identity_sha256"])
    # Probe capacities only before enabling the census; no payload scan here.
    atomic_write_json(CONFIG, config)
    CONFIG.chmod(0o600)
    atomic_write_bytes(UNIT, unit_body(receipts).encode(), mode=0o644)
    subprocess.run(["systemd-analyze", "verify", str(UNIT)], check=True)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "--now", UNIT.name], check=True)
    return {"state": "read_only_observer_installed", "backup_service_modified": False,
            "source_data_modified": False, "native_nas_telemetry_requires_fresh_returned_receipt": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install", action="store_true")
    parser.add_argument("--manifest-sha256")
    parser.add_argument("--pipeline-config", type=Path, default=Path("/etc/lab203-backup/backup-pipeline-v10.json"))
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    if args.install:
        if not args.manifest_sha256 or args.watch or args.output:
            parser.error("installation needs the source manifest pin, without watch/output")
        print(json.dumps(install(args)))
    else:
        if not args.output:
            parser.error("observation needs the paired receipt output")
        monitor(args.config, args.output, ROOT, watch=args.watch)


if __name__ == "__main__":
    main()
