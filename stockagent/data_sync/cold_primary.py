"""Proof boundary for penguin's explicitly accepted single-volume D cold store."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from stockagent.data_sync.desync_snapshots import ResolvedSnapshot, SnapshotError
from stockagent.data_sync.packed_backup import BackupConfig, VolumeGuard
from stockagent.data_sync.packed_snapshots import (
    resolve_packed_snapshot_id,
    verify_packed_snapshot,
)


D_PRIMARY_MARKER = ".stockagent-d-primary"
D_PRIMARY_VOLUME_ID = "9ba6ab87-3889-40e7-90e4-9597dc88abaf"
D_PRIMARY_BACKING = "drvfs_msize8192"
NATIVE_D_PRIMARY = Path("/mnt/d/stockagent-cold-primary/packed")


def _check_d_primary_mount(sync_root: Path) -> None:
    if sync_root.resolve() != Path("/srv/stockagent-packed"):
        raise SnapshotError("D cold primary must use the canonical packed root")
    checker = Path(__file__).resolve().parents[2] / "scripts/mount_packed_d_cold.sh"
    try:
        result = subprocess.run(
            ["/bin/bash", str(checker), "--check"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SnapshotError("D cold primary mount check failed") from exc
    if result.returncode != 0:
        raise SnapshotError(
            "D cold primary mount check failed: " + result.stderr.strip()
        )


def d_primary_read_alias(path: Path) -> Path:
    """Read one existing canonical object through the native D mount only.

    Locks, heads and authority remain canonical. The enrolled D volume and
    identical NTFS file ID/portable stat prove this is another namespace of the
    SAME physical object, not a second replica or a cached proof. The caller
    still hashes every byte and rechecks the canonical signature afterwards.
    """
    canonical = Path("/srv/stockagent-packed")
    _check_d_primary_mount(canonical)
    path = path.absolute()
    try:
        relative = path.relative_to(canonical)
    except ValueError as error:
        raise SnapshotError("D read alias outside canonical cold store") from error
    if not relative.parts or relative.parts[0] != "objects" or path.resolve() != path:
        raise SnapshotError("native D fast reads are restricted to existing immutable objects")
    if NATIVE_D_PRIMARY.resolve() != NATIVE_D_PRIMARY:
        raise SnapshotError("native D primary alias is redirected")
    try:
        mount = json.loads(subprocess.check_output(
            ["findmnt", "-J", "-T", str(NATIVE_D_PRIMARY), "-o", "TARGET,SOURCE,FSTYPE"], text=True))
        records = mount.get("filesystems", [])
        if len(records) != 1 or records[0].get("source") != "D:\\" or records[0].get("fstype") != "9p":
            raise SnapshotError("native D alias is not the enrolled physical D drive")
        if (NATIVE_D_PRIMARY / D_PRIMARY_MARKER).read_bytes() != (canonical / D_PRIMARY_MARKER).read_bytes():
            raise SnapshotError("native/canonical D authority markers differ")
        alias = NATIVE_D_PRIMARY / relative
        if alias.resolve() != alias:
            raise SnapshotError("native D object is redirected")
        first, second = path.lstat(), alias.lstat()
        # st_dev differs between the two 9p mounts. NTFS file IDs do not.
        fields = ("st_ino", "st_size", "st_mtime_ns", "st_mode", "st_uid", "st_gid", "st_nlink")
        if any(getattr(first, key) != getattr(second, key) for key in fields):
            raise SnapshotError("native D object identity differs from canonical inode")
        import stat
        if not stat.S_ISREG(first.st_mode):
            raise SnapshotError("D read alias is not a regular immutable object")
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        raise SnapshotError("cannot validate native D read alias") from error
    return alias


def verify_cold_resilience(
    sync_root: Path,
    resolved: ResolvedSnapshot,
    backup_config: Path,
) -> dict[str, bool | str]:
    """Verify the applicable cold proof without claiming a same-disk backup.

    The D-primary marker changes the policy only after the exact canonical
    mount check passes. Without it, the established independent-backup proof
    remains mandatory. This function never substitutes a second-copy claim.
    """

    marker = sync_root / D_PRIMARY_MARKER
    if marker.is_symlink():
        raise SnapshotError("D cold primary marker is redirected")
    if marker.exists():
        if not marker.is_file():
            raise SnapshotError("D cold primary marker is not a file")
        try:
            identity = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise SnapshotError("D cold primary marker is invalid") from exc
        if identity != {
            "schema_version": 1,
            "volume_id": D_PRIMARY_VOLUME_ID,
            "backing": D_PRIMARY_BACKING,
            "authority_node_id": "penguin",
            "resilience": "single_d_volume",
        }:
            raise SnapshotError("D cold primary marker identity mismatch")
        _check_d_primary_mount(sync_root)
        return {
            "backup_verified": False,
            "cold_primary_verified": True,
            "resilience": "single_d_volume",
        }

    cfg = BackupConfig.load(backup_config)
    if cfg.source.resolve() != sync_root.resolve():
        raise SnapshotError("backup configuration source differs from the packed root")
    VolumeGuard(cfg).check()
    backup = resolve_packed_snapshot_id(
        cfg.destination,
        str(resolved.manifest["dataset"]),
        str(resolved.manifest["snapshot_id"]),
    )
    if backup.manifest_sha256 != resolved.manifest_sha256:
        raise SnapshotError(
            "independent backup manifest differs from current cold release"
        )
    verify_packed_snapshot(cfg.destination, backup)
    return {
        "backup_verified": True,
        "cold_primary_verified": False,
        "resilience": "independent_c_and_d",
    }
