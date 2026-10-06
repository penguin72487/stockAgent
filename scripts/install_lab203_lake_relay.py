#!/usr/bin/env python3
"""One local installation of a pinned immutable NAS archive relay."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.data_sync.immutable_replication import verify
from stockagent.runtime_identity import runtime_identity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-identity", required=True)
    parser.add_argument("--existing-pipeline", type=Path, default=Path("/etc/lab203-backup/backup-pipeline-v10.json"))
    parser.add_argument("--pg-bin", type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise ValueError("local root installation required")
    pin = verify(ROOT)
    if pin["identity_sha256"] != args.expected_identity or pin["context"].get("kind") != "fixed_lab203_lake_relay_installation":
        raise ValueError("install only the source-pinned closed relay package")
    if args.existing_pipeline.is_symlink() or args.existing_pipeline.stat().st_mode & 0o077:
        raise ValueError("reuse the private locally installed backup pipeline configuration")
    old = json.loads(args.existing_pipeline.read_bytes())
    for key in ("producer_device_id", "receiver_device_id"):
        if old[key] != pin["context"][key]:
            raise ValueError("existing relay pairing differs from the installation pin")
    install = Path("/opt/lab203-lake-relay")
    if install.exists():
        raise ValueError("preserve an existing immutable relay installation")
    install.mkdir(mode=0o700)
    shutil.copytree(ROOT, install / "code")
    if verify(install / "code") != pin:
        raise ValueError("installed fixed package differs")
    # Existing local Miniforge manager; one separate role from the exact Linux
    # platform lock, never patch the running backup or native GPU interpreter.
    from scripts.manage_runtime_environments import create_role
    environment = install / "env"
    create_role(environment, install / "code/configs/environments/locks/lakehouse-control-linux-64-20261005.explicit.txt",
                install / "environment-receipts", explicit=True)
    pg_bin = args.pg_bin
    if pg_bin is None:
        options = sorted(Path("/usr/lib/postgresql").glob("*/bin"), key=lambda p: int(p.parent.name))
        options = [p for p in options if all((p / n).is_file() for n in ("initdb", "pg_ctl", "pg_restore"))]
        if not options:
            raise ValueError("use the already accepted private recovery role's pg_bin; do not fake catalog restore")
        pg_bin = options[-1]
    for name in ("initdb", "pg_ctl", "pg_restore"):
        if not (pg_bin / name).is_file():
            raise ValueError("PostgreSQL recovery binary set incomplete")
    code = install / "code"
    runtime = json.loads(subprocess.run([str(environment / "bin/python"), "-c",
        "import json; from stockagent.runtime_identity import runtime_identity; print(json.dumps(runtime_identity()))"],
        cwd=code, capture_output=True, text=True, check=True).stdout)
    lock = install / "runtime-lock.json"
    atomic_write_json(lock, runtime)
    extensions = install / "extensions"
    subprocess.run([str(environment / "bin/python"), "-c",
        "import duckdb,sys; c=duckdb.connect(); c.execute(\"SET extension_directory='\"+sys.argv[1]+\"'\"); c.execute('INSTALL postgres'); c.execute('INSTALL ducklake')", str(extensions)], check=True)
    rclone = code / "bin/rclone"
    rclone.chmod(0o755)
    config = {"schema_version": 1, "producer_device_id": old["producer_device_id"], "receiver_device_id": old["receiver_device_id"],
              "ingress_root": str(Path(old["ingress_root"]) / "lakehouse"), "receipt_root": str(Path(old["receipt_root"]) / "lakehouse"),
              "state_root": "/srv/lab203-backup/lake-archive-state", "owner_lock": old["owner_lock"],
              "nas_configuration": old["nas_configuration"], "runtime_lock": str(lock), "rclone": str(rclone),
              "role_prefix": str(environment), "extensions": str(extensions), "pg_bin": str(pg_bin), "maximum_jobs": 4, "workers": 2}
    private = Path("/etc/lab203-backup/lake-relay.json")
    if private.exists():
        raise ValueError("preserve an existing relay policy")
    atomic_write_json(private, config)
    private.chmod(0o600)
    unit = """[Unit]
Description=Immutable lake rclone NAS archive and independent recovery
Requires=lab203-nas.service
After=lab203-nas.service network.target
[Service]
Type=oneshot
WorkingDirectory=__CODE__
ExecStart=/bin/bash __CODE__/scripts/run_lab203_lake_relay.sh
TimeoutStartSec=3h
SuccessExitStatus=75
UMask=0077
CPUQuota=200%
MemoryHigh=1G
MemoryMax=2G
Nice=10
""".replace("__CODE__", str(code))
    timer = "[Unit]\nDescription=Resume immutable lake relay automatically\n[Timer]\nOnBootSec=30s\nOnActiveSec=10s\nOnUnitInactiveSec=30s\nAccuracySec=1s\n[Install]\nWantedBy=timers.target\n"
    for suffix, body in (("service", unit), ("timer", timer)):
        path = Path("/etc/systemd/system/lab203-lake-relay." + suffix)
        if path.exists():
            raise ValueError("preserve unknown lake relay unit")
        atomic_write_text(path, body)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "--now", "lab203-lake-relay.timer"], check=True)
    result = {"state": "installed", "installed_package_identity_sha256": pin["identity_sha256"], "runtime_sha256": runtime["sha256"],
              "existing_backup_owner_reused": True, "restic_service_preserved": True, "ssh_enabled": False,
              "service": "lab203-lake-relay.service", "timer": "lab203-lake-relay.timer"}
    atomic_write_json(install / "installation.json", result)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
