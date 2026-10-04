"""Explicit Syncthing scan after an atomic publish on penguin's D: DrvFs."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import urllib.parse
import urllib.request
import uuid
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
# Conservative request bounds for the opt-in multi-sub API candidate. The
# transport remains sequential, including the separate manifest/head phases.
MAX_BATCH_SUB_PATHS = 64
MAX_BATCH_QUERY_BYTES = 16 * 1024
CONFIGS = (
    Path("/root/.local/state/syncthing/config.xml"),
    Path("/root/.config/syncthing/config.xml"),
)


def _pending_path(sync_root: Path, dataset: str) -> Path | None:
    marker = sync_root / D_PRIMARY_MARKER
    if not marker.exists():
        return None
    dataset = validate_slug(dataset, "dataset")
    if marker.is_symlink() or sync_root.resolve() != CANONICAL_ROOT:
        raise SnapshotError(
            "D-primary Syncthing scan requires the canonical mounted root"
        )
    directory = sync_root / ".local-state" / "scan-pending"
    pending = directory / f"{dataset}.json"
    if directory.is_symlink() or pending.is_symlink():
        raise SnapshotError("pending scan path must not be a symlink")
    if pending.exists() and not pending.is_file():
        raise SnapshotError("pending scan receipt must be a regular file")
    return pending


def queue_after_publish(
    sync_root: Path,
    dataset: str,
    *,
    new_object_paths: tuple[str, ...] = (),
) -> bool:
    """Durably merge scan intent without waiting for any network scan.

    This short metadata lock is separate from the scan-execution lock: a source
    transaction can enqueue while another scan waits on Syncthing. True proves
    only durable local intent, never a scan acknowledgement or peer delivery.
    """

    pending = _pending_path(sync_root, dataset)
    if pending is None:
        return False
    if not all(
        isinstance(path, str) and path and not path.startswith("/")
        and ".." not in Path(path).parts
        for path in new_object_paths
    ):
        raise SnapshotError("unsafe Syncthing scan queue path")
    with _exclusive_lock(sync_root / ".local-state" / "locks" / f"scan-queue-{dataset}.lock"):
        if _pending_path(sync_root, dataset) != pending:
            raise SnapshotError("D-primary disappeared before queue commit")
        previous_paths, full_objects_scan = (
            _load_pending(pending, dataset) if pending.is_file() else ((), False)
        )
        atomic_write_json(pending, {
            "schema_version": 1,
            "dataset": dataset,
            "generation": uuid.uuid4().hex,
            "new_object_paths": sorted(set(previous_paths) | set(new_object_paths)),
            "full_objects_scan": full_objects_scan,
        })
    return True


def scan_after_publish(
    sync_root: Path,
    dataset: str,
    *,
    new_object_paths: tuple[str, ...] = (),
    retry_full: bool = False,
    batch_object_paths: bool = False,
) -> bool:
    """Queue then immediately scan; a retry only drains existing intent.

    True acknowledges and clears exactly the generation scanned. False means
    non-D-primary, no pending intent, or a newer generation remains pending.
    Network errors retain pending intent and propagate as before. Requests scan
    objects before manifests/heads; acknowledgement is not peer convergence.
    Object-path batching is opt-in; existing callers keep one path per request.
    """

    if type(batch_object_paths) is not bool:
        raise SnapshotError("batch_object_paths must be a boolean")
    pending = _pending_path(sync_root, dataset)
    if pending is None:
        return False
    if not retry_full:
        queue_after_publish(sync_root, dataset, new_object_paths=new_object_paths)
    queue_lock = sync_root / ".local-state" / "locks" / f"scan-queue-{dataset}.lock"
    with _exclusive_lock(sync_root / ".local-state" / "locks" / f"scan-{dataset}.lock"):
        with _exclusive_lock(queue_lock):
            if _pending_path(sync_root, dataset) != pending:
                raise SnapshotError("D-primary disappeared before scan")
            if not pending.is_file():
                return False
            scan_paths, full_objects_scan = _load_pending(pending, dataset)
            observed = pending.read_bytes()
            observed_stat = pending.stat()
        _scan_pending(
            sync_root, dataset, scan_paths, full_objects_scan=full_objects_scan,
            **({"batch_object_paths": True} if batch_object_paths else {}),
        )
        with _exclusive_lock(queue_lock):
            if _pending_path(sync_root, dataset) != pending:
                raise SnapshotError("D-primary disappeared before scan acknowledgement")
            if not pending.is_file():
                return False
            current_stat = pending.stat()
            # UUID generations prevent an identical-path enqueue from being
            # erased; inode/ctime also protect legacy receipts without a UUID.
            if (
                (current_stat.st_dev, current_stat.st_ino, current_stat.st_ctime_ns)
                != (observed_stat.st_dev, observed_stat.st_ino, observed_stat.st_ctime_ns)
                or pending.read_bytes() != observed
            ):
                return False
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
            or not all(
                isinstance(path, str) and path and not path.startswith("/")
                and ".." not in Path(path).parts for path in paths
            )
        ):
            raise ValueError("invalid pending scan receipt")
        return tuple(paths), bool(payload.get("full_objects_scan", False))
    except (OSError, ValueError, KeyError, TypeError):
        # A damaged receipt cannot prove which object paths were missed.
        return (), True


def _scan_request_groups(
    dataset: str,
    new_object_paths: tuple[str, ...],
    *,
    full_objects_scan: bool,
    batch_object_paths: bool = False,
) -> tuple[tuple[str, ...], ...]:
    """Keep exact object coverage and strict object -> manifest -> head phases."""

    paths = (
        ["objects"]
        if full_objects_scan or len(new_object_paths) > 256
        else sorted(set(new_object_paths))
    )
    metadata = [f"manifests/{dataset}", f"heads/{dataset}"]
    for relative in [*paths, *metadata]:
        if not relative or relative.startswith("/") or ".." in Path(relative).parts:
            raise SnapshotError(f"unsafe Syncthing scan path: {relative}")
    if not batch_object_paths:
        return tuple((relative,) for relative in [*paths, *metadata])
    groups: list[tuple[str, ...]] = []
    current: list[str] = []
    prefix_bytes = len(urllib.parse.urlencode({"folder": FOLDER_ID}))
    query_bytes = prefix_bytes
    for relative in paths:
        # urlencode emits ASCII; include the separating '&' in the exact bound.
        encoded_bytes = 1 + len(urllib.parse.urlencode({"sub": relative}))
        if prefix_bytes + encoded_bytes > MAX_BATCH_QUERY_BYTES:
            raise SnapshotError("Syncthing scan path exceeds the batch query limit")
        if current and (
            len(current) >= MAX_BATCH_SUB_PATHS
            or query_bytes + encoded_bytes > MAX_BATCH_QUERY_BYTES
        ):
            groups.append(tuple(current))
            current = []
            query_bytes = prefix_bytes
        current.append(relative)
        query_bytes += encoded_bytes
    if current:
        groups.append(tuple(current))
    groups.extend((relative,) for relative in metadata)
    return tuple(groups)


def _scan_pending(
    sync_root: Path,
    dataset: str,
    new_object_paths: tuple[str, ...],
    *,
    full_objects_scan: bool,
    batch_object_paths: bool = False,
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
    groups = _scan_request_groups(
        dataset, new_object_paths, full_objects_scan=full_objects_scan,
        batch_object_paths=batch_object_paths,
    )
    for group in groups:
        # Syncthing v2.1.5 postDBScan passes qs["sub"] (all repeated values)
        # to ScanFolderSubdirs; never combine metadata with the object phase.
        query = urllib.parse.urlencode({"folder": FOLDER_ID, "sub": group}, doseq=True)
        label = group[0] if len(group) == 1 else f"object batch ({len(group)} paths)"
        request = urllib.request.Request(
            base + "/rest/db/scan?" + query,
            headers={"X-API-Key": key},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                if response.status != 200:
                    raise SnapshotError(
                        f"Syncthing scan failed for {label}: HTTP {response.status}"
                    )
        except (OSError, TimeoutError) as exc:
            raise SnapshotError(f"Syncthing scan failed for {label}: {exc}") from exc
