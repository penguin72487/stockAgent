"""Multi-writer dataset snapshots backed by desync and Syncthing.

Syncthing transports immutable chunks, indexes and manifests.  Each publisher
writes only its own head file; a deterministic hybrid logical clock selects the
latest complete snapshot without multiple machines writing the same file.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class SnapshotError(RuntimeError):
    """A snapshot cannot safely be published, resolved, or restored."""


def _name(value: str, kind: str) -> str:
    if not _NAME_RE.fullmatch(value):
        raise SnapshotError(f"invalid {kind}: {value!r}")
    return value


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _atomic_write(path: Path, data: bytes, mode: int = 0o660) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(raw_tmp)
    try:
        with os.fdopen(fd, "wb") as handle:
            os.fchmod(handle.fileno(), mode)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read valid JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SnapshotError(f"expected JSON object in {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _paths(sync_root: Path) -> dict[str, Path]:
    return {
        "local": sync_root / ".local-state",
        "heads": sync_root / "heads",
        "indices": sync_root / "indices",
        "manifests": sync_root / "manifests",
        "stores": sync_root / "stores",
    }


def init_sync_root(sync_root: Path, node_id: str, ignore_template: Path | None = None) -> dict[str, Any]:
    node_id = _name(node_id, "node ID")
    sync_root = sync_root.resolve()
    paths = _paths(sync_root)
    sync_root.mkdir(parents=True, exist_ok=True)
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    node_file = paths["local"] / "node-id"
    if node_file.exists():
        current = node_file.read_text(encoding="utf-8").strip()
        if current != node_id:
            raise SnapshotError(
                f"sync root belongs to node {current!r}, refusing to replace it with {node_id!r}"
            )
    else:
        _atomic_write(node_file, f"{node_id}\n".encode(), mode=0o600)
    if ignore_template is not None:
        _atomic_write(sync_root / ".stignore", ignore_template.read_bytes())
    return {"sync_root": str(sync_root), "node_id": node_id, "schema": SCHEMA_VERSION}


def _node_id(sync_root: Path) -> str:
    path = sync_root / ".local-state" / "node-id"
    try:
        return _name(path.read_text(encoding="utf-8").strip(), "node ID")
    except OSError as exc:
        raise SnapshotError(f"missing node ID; run init first: {path}") from exc


def _inventory(root: Path, *, portable: bool = False) -> dict[str, Any]:
    if not root.is_dir():
        raise SnapshotError(f"snapshot source is not a directory: {root}")
    digest = hashlib.sha256()
    files = directories = total_bytes = 0
    for current, dir_names, file_names in os.walk(root, topdown=True, followlinks=False):
        dir_names.sort()
        file_names.sort()
        current_path = Path(current)
        for name in [*dir_names, *file_names]:
            path = current_path / name
            rel = path.relative_to(root).as_posix()
            info = path.lstat()
            mode = stat.S_IFMT(info.st_mode)
            target = os.readlink(path) if stat.S_ISLNK(info.st_mode) else ""
            # desync v1.0.3 restores file mtimes but directory mtimes change as
            # children are created. Keep them in the publish stability check,
            # but omit them from the portable materialization fingerprint.
            is_directory = stat.S_ISDIR(info.st_mode)
            mtime_ns = 0 if portable and is_directory else info.st_mtime_ns
            size = 0 if portable and is_directory else info.st_size
            record = f"{rel}\0{mode}\0{info.st_mode & 0o7777}\0{size}\0{mtime_ns}\0{target}\n"
            digest.update(record.encode("utf-8", "surrogateescape"))
            if is_directory:
                directories += 1
            else:
                files += 1
                total_bytes += info.st_size
    return {
        "fingerprint": digest.hexdigest(),
        "files": files,
        "directories": directories,
        "bytes": total_bytes,
    }


def _run_desync(desync_bin: str, args: Iterable[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
    command = [desync_bin, *args]
    try:
        return subprocess.run(
            command,
            check=True,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
    except FileNotFoundError as exc:
        raise SnapshotError(f"desync executable not found: {desync_bin}") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or str(exc)).strip()
        raise SnapshotError(f"desync failed: {detail}") from exc


def _head_files(sync_root: Path, dataset: str) -> list[Path]:
    directory = sync_root / "heads" / dataset
    return sorted(directory.glob("*.json")) if directory.is_dir() else []


def _clock_tuple(head: dict[str, Any]) -> tuple[int, int, str]:
    clock = head.get("hlc")
    if not isinstance(clock, dict):
        raise SnapshotError("head has no HLC")
    return int(clock["physical_ms"]), int(clock["logical"]), str(head["publisher"])


def _next_clock(sync_root: Path, dataset: str, publisher: str) -> dict[str, int]:
    clocks: list[tuple[int, int, str]] = []
    for path in _head_files(sync_root, dataset):
        clocks.append(_clock_tuple(_load_json(path)))
    now = time.time_ns() // 1_000_000
    if not clocks:
        return {"physical_ms": now, "logical": 0}
    physical, logical, _ = max(clocks)
    if now > physical:
        return {"physical_ms": now, "logical": 0}
    same_physical = [item[1] for item in clocks if item[0] == physical]
    return {"physical_ms": physical, "logical": max(same_physical) + 1}


def publish_snapshot(
    dataset: str,
    source: Path,
    sync_root: Path,
    metadata: dict[str, str] | None = None,
    desync_bin: str = "desync",
) -> dict[str, Any]:
    dataset = _name(dataset, "dataset")
    sync_root = sync_root.resolve()
    source = source.resolve()
    publisher = _node_id(sync_root)
    paths = _paths(sync_root)
    lock_path = paths["local"] / "publish.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        before = _inventory(source)
        tmp_dir = paths["local"] / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        fd, raw_index = tempfile.mkstemp(prefix=f"{dataset}.", suffix=".caidx", dir=tmp_dir)
        os.close(fd)
        tmp_index = Path(raw_index)
        tmp_index.unlink()
        try:
            _run_desync(
                desync_bin,
                ["tar", "-i", "-x", "-s", str(paths["stores"]), str(tmp_index), str(source)],
            )
            after = _inventory(source)
            if before != after:
                raise SnapshotError("source changed while publishing; no head was created")
            snapshot_id = _sha256(tmp_index)
            index_path = paths["indices"] / dataset / f"{snapshot_id}.caidx"
            index_path.parent.mkdir(parents=True, exist_ok=True)
            if index_path.exists():
                if _sha256(index_path) != snapshot_id:
                    raise SnapshotError(f"existing immutable index is corrupt: {index_path}")
                tmp_index.unlink()
            else:
                os.replace(tmp_index, index_path)
            manifest = {
                "schema": SCHEMA_VERSION,
                "dataset": dataset,
                "snapshot_id": snapshot_id,
                "publisher": publisher,
                "created_unix_ns": time.time_ns(),
                "index": f"indices/{dataset}/{snapshot_id}.caidx",
                "index_sha256": snapshot_id,
                "inventory": _inventory(source, portable=True),
                "metadata": dict(sorted((metadata or {}).items())),
            }
            manifest_path = paths["manifests"] / dataset / f"{snapshot_id}.json"
            manifest_bytes = _json_bytes(manifest)
            if manifest_path.exists() and manifest_path.read_bytes() != manifest_bytes:
                raise SnapshotError(f"immutable manifest collision: {manifest_path}")
            if not manifest_path.exists():
                _atomic_write(manifest_path, manifest_bytes)
            head = {
                "schema": SCHEMA_VERSION,
                "dataset": dataset,
                "snapshot_id": snapshot_id,
                "publisher": publisher,
                "hlc": _next_clock(sync_root, dataset, publisher),
                "manifest": f"manifests/{dataset}/{snapshot_id}.json",
                "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            }
            _atomic_write(paths["heads"] / dataset / f"{publisher}.json", _json_bytes(head))
            return {**head, "inventory": manifest["inventory"], "metadata": manifest["metadata"]}
        finally:
            tmp_index.unlink(missing_ok=True)


def resolve_status(dataset: str, sync_root: Path, desync_bin: str = "desync") -> dict[str, Any]:
    dataset = _name(dataset, "dataset")
    sync_root = sync_root.resolve()
    candidates: list[dict[str, Any]] = []
    for path in _head_files(sync_root, dataset):
        head = _load_json(path)
        if head.get("dataset") != dataset:
            raise SnapshotError(f"dataset mismatch in {path}")
        candidates.append(head)
    if not candidates:
        raise SnapshotError(f"no heads found for dataset {dataset!r}")
    head = max(candidates, key=_clock_tuple)
    manifest_path = sync_root / str(head["manifest"])
    manifest_bytes = manifest_path.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != head["manifest_sha256"]:
        raise SnapshotError(f"manifest checksum mismatch: {manifest_path}")
    manifest = json.loads(manifest_bytes)
    index_path = sync_root / str(manifest["index"])
    if _sha256(index_path) != manifest["index_sha256"]:
        raise SnapshotError(f"index checksum mismatch: {index_path}")
    inspected = _run_desync(
        desync_bin, ["inspect-chunks", "-s", str(sync_root / "stores"), str(index_path)], capture=True
    )
    chunks = json.loads(inspected.stdout or "[]")
    missing = [item["id"] for item in chunks if not item.get("compressed_size")]
    if missing:
        raise SnapshotError(
            f"latest head is present but {len(missing)} chunks are missing; wait for Syncthing"
        )
    return {
        **head,
        "manifest_path": str(manifest_path),
        "index_path": str(index_path),
        "inventory": manifest["inventory"],
        "metadata": manifest.get("metadata", {}),
        "chunks": len(chunks),
        "complete": True,
    }


def fetch_snapshot(
    dataset: str,
    sync_root: Path,
    materialized_root: Path,
    pin: Path | None = None,
    desync_bin: str = "desync",
) -> dict[str, Any]:
    status_value = resolve_status(dataset, sync_root, desync_bin)
    snapshot_id = status_value["snapshot_id"]
    target_parent = materialized_root.resolve() / _name(dataset, "dataset")
    target = target_parent / snapshot_id
    target_parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        tmp = Path(tempfile.mkdtemp(prefix=f".{snapshot_id}.", dir=target_parent))
        try:
            _run_desync(
                desync_bin,
                [
                    "untar",
                    "-i",
                    "-s",
                    str(Path(sync_root).resolve() / "stores"),
                    status_value["index_path"],
                    str(tmp),
                ],
            )
            if _inventory(tmp, portable=True) != status_value["inventory"]:
                raise SnapshotError("materialized snapshot inventory does not match its manifest")
            os.replace(tmp, target)
        finally:
            if tmp.exists():
                shutil.rmtree(tmp)
    pin_value = {
        "schema": SCHEMA_VERSION,
        "dataset": dataset,
        "snapshot_id": snapshot_id,
        "publisher": status_value["publisher"],
        "path": str(target),
    }
    if pin is not None:
        _atomic_write(pin.resolve(), _json_bytes(pin_value))
    return pin_value


def verify_snapshot(dataset: str, sync_root: Path, desync_bin: str = "desync") -> dict[str, Any]:
    status_value = resolve_status(dataset, sync_root, desync_bin)
    _run_desync(desync_bin, ["verify", "-s", str(Path(sync_root).resolve() / "stores")])
    return {
        "dataset": dataset,
        "snapshot_id": status_value["snapshot_id"],
        "publisher": status_value["publisher"],
        "verified": True,
    }
