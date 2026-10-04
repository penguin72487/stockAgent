#!/usr/bin/env python3
"""Prepare private SMB access and measure only the authorized NAS user folder."""
from __future__ import annotations

import argparse
import getpass
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402
from stockagent.data_sync.nas_target import NasTarget  # noqa: E402
from stockagent.data_sync.offhost_backup import private_file, private_json  # noqa: E402


def write_credentials(target: NasTarget, username: str) -> dict:
    path = Path(target.nas["credentials_file"])
    if not path.is_absolute() or any(p.is_symlink() for p in (path, *path.parents)):
        raise SnapshotError("credential path must be absolute and not redirected")
    if path.exists():
        raise SnapshotError("preserve the existing NAS credential file")
    password = getpass.getpass("QNAP password (local terminal only): ")
    if not username or not password or any(c in username + password for c in "\r\n\0"):
        raise SnapshotError("invalid or empty SMB credentials")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(f"username={username}\npassword={password}\n")
        stream.flush()
        os.fsync(stream.fileno())
    private_file(path)
    return {"state": "private_credentials_written", "path": str(path),
            "nas_authenticated": False, "durable_off_host_backup_verified": False}


def mount(target: NasTarget) -> dict:
    binary = shutil.which("mount.cifs")
    if os.geteuid() != 0 or binary is None:
        raise SnapshotError("mount requires root and cifs-utils; no VPN or global routes are modified")
    try:
        observed = target.check()
    except SnapshotError:
        # Refuse any existing different mount, including a bind/local volume.
        from stockagent.data_sync.packed_backup import mounted_volume
        try:
            mounted_volume(target.mount_point)
        except SnapshotError:
            pass
        else:
            raise SnapshotError("an existing different mount cannot be replaced") from None
    else:
        return {"state": "configured_nas_already_mounted", **observed}
    credentials = private_file(Path(target.nas["credentials_file"]))
    if any(c in str(credentials) for c in ",\r\n\0"):
        raise SnapshotError("credential path contains an unsupported mount option separator")
    if any(p.is_symlink() for p in (target.mount_point, *target.mount_point.parents)):
        raise SnapshotError("mount point is redirected")
    target.mount_point.mkdir(parents=True, exist_ok=True, mode=0o700)
    if any(target.mount_point.iterdir()):
        raise SnapshotError("mount point contains local files; refuse to cover them")
    options = f"credentials={credentials},vers=3,seal,nosuid,nodev,noexec,cache=strict"
    result = subprocess.run([binary, target.source, str(target.mount_point), "-o", options],
                            capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise SnapshotError(f"SMB mount failed with exit {result.returncode}; check private credentials, VPN and kernel CIFS support")
    return {"state": "configured_nas_mounted", **target.check()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("credentials", "mount", "inspect", "probe"))
    parser.add_argument("--config", type=Path, default=Path("configs/data_sync/offhost_backup.json"))
    parser.add_argument("--username")
    parser.add_argument("--size-mib", type=int, default=32)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if args.output.exists():
        parser.error("use a fresh receipt path")
    target = NasTarget(args.config)
    try:
        if args.operation == "credentials":
            if not args.username:
                parser.error("credentials requires --username; password is entered only in the local terminal")
            result = write_credentials(target, args.username)
        elif args.operation == "mount":
            result = mount(target)
        elif args.operation == "probe":
            result = target.probe(size_mib=args.size_mib, repeats=args.repeats)
        else:
            result = {"state": "configured_nas_observed", **target.check()}
    except (SnapshotError, OSError, subprocess.TimeoutExpired) as error:
        result = {"state": "rejected", "error": str(error), "durable_off_host_backup_verified": False}
    private_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("state", "durable_off_host_backup_verified") if key in result}))
    if result["state"] == "rejected":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
