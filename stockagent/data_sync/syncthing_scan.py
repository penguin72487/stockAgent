"""Explicit Syncthing scan after an atomic publish on penguin's D: DrvFs."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _exclusive_lock,
    atomic_write_json,
    validate_slug,
)


FOLDER_ID = "stockagent-packed"
CANONICAL_ROOT = Path("/srv/stockagent-packed")
D_PRIMARY_MARKER = ".stockagent-d-primary"
CONFIGS = (
    Path("/root/.local/state/syncthing/config.xml"),
    Path("/root/.config/syncthing/config.xml"),
)


def scan_after_publish(
    sync_root: Path,
    dataset: str,
    *,
    new_object_paths: tuple[str, ...] = (),
    retry_full: bool = False,
) -> bool:
    """Return False outside D-primary mode; otherwise require a successful scan.

    Syncthing cannot rely on Linux inotify for this Windows-backed directory.
    Scan objects before advertising the new manifest/head. A periodic rescan
    remains configured to recover from a missed or interrupted notification.
    """

    marker = sync_root / D_PRIMARY_MARKER
    if not marker.exists():
        return False
    dataset = validate_slug(dataset, "dataset")
    if marker.is_symlink() or sync_root.resolve() != CANONICAL_ROOT:
        raise SnapshotError(
            "D-primary Syncthing scan requires the canonical mounted root"
        )
    pending = sync_root / ".local-state" / "scan-pending" / f"{dataset}.json"
    with _exclusive_lock(sync_root / ".local-state" / "locks" / f"scan-{dataset}.lock"):
        has_pending = pending.is_file()
        if retry_full and not has_pending:
            return False
        previous_paths, full_objects_scan = _load_pending(pending, dataset) if has_pending else ((), False)
        scan_paths = tuple(sorted(set(previous_paths) | set(new_object_paths)))
        if not retry_full:
            atomic_write_json(
                pending,
                {
                    "schema_version": 1,
                    "dataset": dataset,
                    "new_object_paths": list(scan_paths),
                    "full_objects_scan": full_objects_scan,
                },
            )
        _scan_pending(sync_root, dataset, scan_paths, full_objects_scan=full_objects_scan)
        pending.unlink()
    return True


def _load_pending(pending: Path, dataset: str) -> tuple[tuple[str, ...], bool]:
    try:
        payload = json.loads(pending.read_text(encoding="utf-8"))
        paths = payload["new_object_paths"]
        if (
            payload.get("schema_version") != 1
            or payload.get("dataset") != dataset
            or not isinstance(paths, list)
            or not all(isinstance(path, str) for path in paths)
        ):
            raise ValueError("invalid pending scan receipt")
        return tuple(paths), bool(payload.get("full_objects_scan", False))
    except (OSError, ValueError, KeyError, TypeError):
        # A damaged receipt cannot prove which object paths were missed.
        return (), True


def _scan_pending(
    sync_root: Path,
    dataset: str,
    new_object_paths: tuple[str, ...],
    *,
    full_objects_scan: bool,
) -> None:
    checker = Path(__file__).resolve().parents[2] / "scripts/mount_packed_d_cold.sh"
    mounted = subprocess.run(
        ["/bin/bash", str(checker), "--check"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if mounted.returncode != 0:
        raise SnapshotError(
            "D-primary mount failed before Syncthing scan: " + mounted.stderr.strip()
        )
    config = next((path for path in CONFIGS if path.is_file()), None)
    if config is None:
        raise SnapshotError("Syncthing configuration is missing")
    root = ET.parse(config).getroot()
    folder = next(
        (row for row in root.findall("folder") if row.get("id") == FOLDER_ID), None
    )
    if folder is None or Path(str(folder.get("path"))).resolve() != CANONICAL_ROOT:
        raise SnapshotError("Syncthing folder is not the canonical D cold root")
    if folder.get("paused") == "true":
        raise SnapshotError("Syncthing cold folder is paused")
    key = root.findtext("./gui/apikey")
    address = root.findtext("./gui/address") or "127.0.0.1:8384"
    if not key:
        raise SnapshotError("Syncthing API key is missing")
    _, _, port = address.rpartition(":")
    base = f"http://127.0.0.1:{port or '8384'}"
    paths = (
        ["objects"]
        if full_objects_scan or len(new_object_paths) > 256
        else sorted(set(new_object_paths))
    )
    paths += [f"manifests/{dataset}", f"heads/{dataset}"]
    for relative in paths:
        if relative.startswith("/") or ".." in Path(relative).parts:
            raise SnapshotError(f"unsafe Syncthing scan path: {relative}")
        query = urllib.parse.urlencode({"folder": FOLDER_ID, "sub": relative})
        request = urllib.request.Request(
            base + "/rest/db/scan?" + query,
            headers={"X-API-Key": key},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                if response.status != 200:
                    raise SnapshotError(
                        f"Syncthing scan failed for {relative}: HTTP {response.status}"
                    )
        except (OSError, TimeoutError) as exc:
            raise SnapshotError(f"Syncthing scan failed for {relative}: {exc}") from exc
