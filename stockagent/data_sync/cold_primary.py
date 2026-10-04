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
