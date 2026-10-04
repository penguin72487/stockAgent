#!/usr/bin/env python3
"""Incremental, additive C: cold-store backup to the explicitly enrolled D: volume."""
from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.cold_primary import D_PRIMARY_MARKER
from stockagent.data_sync.packed_backup import (
    BackupConfig, PackedBackup, VolumeGuard, current_inventory, inventory, now_iso,
)
from scripts.run_live_artifact_sync import RecursiveInotify


def windows_volume_id() -> str:
    """Enrollment only; subsequent passes require the marker on that volume."""
    result = subprocess.run([
        "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
        "-NoProfile", "-NonInteractive", "-Command",
        "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; (Get-Volume -DriveLetter D).UniqueId",
    ], check=True, capture_output=True, text=True, timeout=30)
    return result.stdout.strip().lower()


def watch_timeout(cfg: BackupConfig, result: dict, *, has_watcher: bool) -> float:
    """Drain a progressing backlog, but avoid full scans while inotify is quiet."""
    if result["pending_objects"] > 0 and result["completed_objects"] > 0:
        return 0
    return cfg.idle_reconcile_seconds if has_watcher else cfg.poll_seconds


def watch(cfg: BackupConfig) -> None:
    watcher = None
    backup = None
    priority: set[str] = set()
    while True:
        try:
            if backup is None:
                backup = PackedBackup(cfg)
            if watcher is None:
                try:
                    watcher = RecursiveInotify({name: cfg.source / name for name in ("objects", "heads", "manifests")})
                except OSError as exc:
                    print(json.dumps({"warning": f"inotify unavailable; polling: {exc}"}), flush=True)
            if watcher is not None:
                changes, _overflow = watcher.wait(0)
                priority.update(f"objects/{path}" for path in changes["objects"])
            result = backup.run_once(max_objects=cfg.batch_objects, priority=priority, time_budget_seconds=30)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            priority.clear()
            # Keep draining a healthy initial backlog. A bad object must not
            # cause a busy retry loop. Polling also repairs missed/overflow events.
            timeout = watch_timeout(cfg, result, has_watcher=watcher is not None)
            if watcher is not None:
                changes, _overflow = watcher.wait(timeout)
                priority.update(f"objects/{path}" for path in changes["objects"])
                if any(changes.values()):
                    time.sleep(0.5)  # coalesce an atomic publication burst
            elif timeout:
                time.sleep(timeout)
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as exc:
            failure = {"state": "blocked", "updated_at": now_iso(), "source": str(cfg.source),
                       "destination": str(cfg.destination), "error_count": 1, "errors": [str(exc)],
                       "deletion_propagation": False, "materialization": False}
            atomic_write_json(cfg.state_dir / "status.json", failure)
            print(json.dumps(failure, ensure_ascii=False), flush=True)
            if backup is not None:
                backup.close()
                backup = None
            if watcher is not None:
                watcher.close()
                watcher = None
            time.sleep(cfg.poll_seconds)


def backup_status(cfg: BackupConfig) -> dict:
    """Never present an old C-to-D receipt as a current D-primary backup."""
    if (cfg.source / ".stockagent-d-mount-required").exists():
        return {
            "state": "unavailable",
            "reason": "D cold primary is not mounted",
            "backup_verified": False,
        }
    if (cfg.source / D_PRIMARY_MARKER).exists():
        return {
            "state": "retired_single_d_primary",
            "reason": "C-to-D independent backup is retired; D is the sole cold volume",
            "backup_verified": False,
        }
    status_path = cfg.state_dir / "status.json"
    return json.loads(status_path.read_text()) if status_path.exists() else {"state": "not_started"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "init", "install-service", "once", "watch", "status"))
    parser.add_argument("--config", type=Path, default=REPO_ROOT / "configs/data_sync/packed_backup.json")
    parser.add_argument("--verify-existing", action="store_true", help="once only: re-read checksums in the configured backup scope")
    args = parser.parse_args()
    cfg = BackupConfig.load(args.config)
    if args.verify_existing and args.command != "once":
        parser.error("--verify-existing requires once")
    if args.command == "status":
        result = backup_status(cfg)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    guard = VolumeGuard(cfg)
    if args.command == "plan":
        guard.check(require_marker=guard.marker_path.exists())
        if cfg.backup_scope == "current_heads":
            objects, errors, _, _ = current_inventory(cfg)
        else:
            objects, errors = inventory(cfg)
        result = {"dry_run": True, "source": str(cfg.source), "destination": str(cfg.destination),
                  "backup_scope": cfg.backup_scope,
                  "mount": guard.mount_identity, "objects": len(objects), "object_bytes": sum(item["bytes"] for item in objects),
                  "destination_free_bytes": shutil.disk_usage(cfg.mount_point).free, "reserve_bytes": cfg.reserve_bytes,
                  "errors": errors, "checksums_verified": False, "deletes": [], "materialization": False}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return int(bool(errors))
    if args.command == "init":
        guard.check(require_marker=False)
        if "{" + cfg.volume_id.lower() + "}" not in windows_volume_id():
            raise SnapshotError("D: Windows volume UUID does not match the configured backup disk")
        guard.initialize()
        cfg.state_dir.mkdir(parents=True, exist_ok=True)
        print(json.dumps({"initialized": True, "marker": str(guard.marker_path)}))
        return 0
    if args.command == "install-service":
        if args.config.resolve() != (REPO_ROOT / "configs/data_sync/packed_backup.json").resolve():
            raise SnapshotError("service installation requires the canonical configuration")
        guard.check()
        # This unit is penguin-specific. Render the current checkout path;
        # do not install a stale home-directory path on another machine.
        template = (REPO_ROOT / "services/systemd/stockagent-packed-backup.service").read_text()
        if any(char in str(REPO_ROOT) for char in ('"', '%', '\\', '\n')):
            raise SnapshotError("unsupported characters in systemd repository path")
        unit = Path("/etc/systemd/system/stockagent-packed-backup.service")
        rendered = template.replace("@REPO_ROOT@", str(REPO_ROOT)).encode()
        if unit.exists() and b"Penguin additive verified packed cold backup" not in unit.read_bytes():
            raise SnapshotError("refusing to replace an unrelated existing backup unit")
        from stockagent.data_sync.desync_snapshots import atomic_write_bytes
        atomic_write_bytes(unit, rendered)
        subprocess.run(["systemd-analyze", "verify", str(unit)], check=True)
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "enable", unit.name], check=True)
        subprocess.run(["systemctl", "restart", unit.name], check=True)
        print(json.dumps({"installed": str(unit), "started": True}))
        return 0
    # State is local C: operational metadata; lock never touches the cold store.
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    with (cfg.state_dir / "run.lock").open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SnapshotError("backup already running; use status (or stop the service before once)")
        if args.command == "watch":
            watch(cfg)
        else:
            backup = PackedBackup(cfg)
            try:
                result = backup.run_once(force_verify=args.verify_existing)
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return int(result["state"] != "up_to_date")
            finally:
                backup.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (SnapshotError, OSError, ValueError, subprocess.SubprocessError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
