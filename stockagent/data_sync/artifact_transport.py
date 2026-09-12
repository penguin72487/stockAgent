"""Bounded completed-artifact quarantine transport, NOT a cold publisher.

Uses the canonical bucket/blob packing primitives. Only Syncthing transports
objects; this module never reads SSH/network credentials, writes release heads,
changes edge identity, activates a model, or deletes source artifacts.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import stat
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.cold_artifacts import (
    ColdArtifactSpec,
    validate_cold_artifact_source,
)
from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _fsync_directory,
    _paths_overlap,
    _safe_relative_path,
    atomic_write_bytes,
    atomic_write_json,
    sha256_file,
)
from stockagent.data_sync.packed_snapshots import (
    _bucket_for_path,
    _canonical_json_bytes,
    _collect_entries,
    _copy_and_hash,
    _copy_member_and_verify,
    _ensure_source_stat,
    _validate_hash,
    _verify_materialized,
    _write_pack,
)

ROLE = "completed-artifact-quarantine-not-cold-release-v1"


def _relative(value: str) -> str:
    path = _safe_relative_path(value, "transport path").as_posix()
    if path != value or "\\" in path or "\0" in path:
        raise SnapshotError("noncanonical transport path")
    return path


def _path(root: Path, relative: str) -> Path:
    path = root / _relative(relative)
    for component in (path, *path.parents):
        if component.is_symlink():
            raise SnapshotError(f"symlink is forbidden in transport: {component}")
    return path


def _scope(config: dict[str, Any]) -> dict[str, Any]:
    return {
        key: config[key]
        for key in (
            "schema_version",
            "folder_id",
            "origin",
            "receiver",
            "artifact",
            "checkpoint",
            "checkpoint_sha256",
            "maximum_entries",
            "maximum_logical_bytes",
        )
    }


def _setup(config: dict[str, Any]) -> tuple[ColdArtifactSpec, Path, Path]:
    if config["schema_version"] != 1:
        raise SnapshotError("unsupported transport configuration")
    spec = ColdArtifactSpec.from_dict(config["artifact"])
    if spec.maximum_file_bytes is not None:
        raise SnapshotError("transport must include the complete artifact")
    _relative(config["checkpoint"])
    _validate_hash(config["checkpoint_sha256"], "checkpoint")
    root, state = Path(config["transport_root"]), Path(config["state_root"])
    for path in (root, state):
        if not path.is_absolute() or path == Path("/"):
            raise SnapshotError("transport/state must be explicit absolute directories")
        _path(path.parent, path.name)
        for forbidden in (
            Path("/srv/stockagent-packed"),
            Path("/srv/stockagent-artifacts-hot"),
            Path("/srv/stockagent-packed-materialized"),
        ):
            if _paths_overlap(path, forbidden):
                raise SnapshotError("transport cannot overlap a canonical store")
    if _paths_overlap(root, state):
        raise SnapshotError("quarantine state must be outside Syncthing")
    if (
        not 0 < config["maximum_entries"] <= 100000
        or not 0 < config["maximum_logical_bytes"] <= 100_000_000_000
    ):
        raise SnapshotError("invalid transport resource bounds")
    root.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    return spec, root, state


@contextlib.contextmanager
def _lock(state: Path):
    with (state / "transport.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def _bounded(
    config: dict[str, Any], entries: list[Any], summary: dict[str, Any]
) -> None:
    if (
        summary["symlinks"]
        or len(entries) > config["maximum_entries"]
        or summary["logical_bytes"] > config["maximum_logical_bytes"]
    ):
        raise SnapshotError(
            "artifact exceeds the authorized transport bounds or contains symlinks"
        )


def _source_ok(
    artifact_root: Path, spec: ColdArtifactSpec, config: dict[str, Any]
) -> None:
    source = _path(artifact_root, spec.relative_root)
    if artifact_process_references(source, artifact_root):
        raise SnapshotError("artifact is referenced by an active process")
    validate_cold_artifact_source(artifact_root, spec)
    if sha256_file(_path(source, config["checkpoint"])) != config["checkpoint_sha256"]:
        raise SnapshotError("checkpoint differs from the explicitly selected model")


def build_package(config: dict[str, Any], artifact_root: Path) -> dict[str, Any]:
    spec, root, state = _setup(config)
    if any(_paths_overlap(artifact_root, path) for path in (root, state)):
        raise SnapshotError("source, transport and quarantine must be separate")
    source = _path(artifact_root, spec.relative_root)
    with (
        _lock(state),
        tempfile.TemporaryDirectory(prefix="pack-", dir=state) as scratch,
    ):
        _source_ok(artifact_root, spec, config)
        entries, before = _collect_entries(source)
        _bounded(config, entries, before)
        objects: dict[str, dict[str, Any]] = {}
        groups: dict[int, list[Any]] = {}

        def install(temporary: Path, kind: str) -> str:
            digest = sha256_file(temporary)
            relative = f"objects/{kind}/{digest[:2]}/{digest}"
            destination = _path(root, relative)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                if sha256_file(destination) != digest:
                    raise SnapshotError("existing transport object is corrupt")
            else:
                # Scratch and transport may be on different filesystems. The
                # atomic finalization happens inside the destination directory.
                with tempfile.NamedTemporaryFile(
                    prefix=".syncthing.",
                    suffix=".tmp",
                    dir=destination.parent,
                    delete=False,
                ) as staged:
                    staged_path = Path(staged.name)
                    with temporary.open("rb") as stream:
                        while block := stream.read(8 * 1024 * 1024):
                            staged.write(block)
                    staged.flush()
                    # Syncthing may be supervised as a different local user.
                    # NamedTemporaryFile defaults to 0600, which would let the
                    # manifest sync while silently stranding every object.
                    os.fchmod(staged.fileno(), 0o644)
                    os.fsync(staged.fileno())
                os.replace(staged_path, destination)
                _fsync_directory(destination.parent)
            if stat.S_IMODE(destination.stat().st_mode) != 0o644:
                destination.chmod(0o644)
            objects[digest] = {
                "kind": kind,
                "path": relative,
                "sha256": digest,
                "bytes": destination.stat().st_size,
            }
            return digest

        for index, entry in enumerate(entries):
            if entry.kind != "file":
                continue
            if entry.size >= spec.loose_file_threshold_bytes:
                temporary = Path(scratch) / f"blob-{index}"
                _ensure_source_stat(source / entry.path, entry)
                entry.sha256 = _copy_and_hash(source / entry.path, temporary)
                _ensure_source_stat(source / entry.path, entry)
                entry.storage = {"object": install(temporary, "blob")}
            else:
                groups.setdefault(
                    _bucket_for_path(entry.path, spec.pack_buckets), []
                ).append(entry)
        for bucket, members in sorted(groups.items()):
            temporary = Path(scratch) / f"pack-{bucket}"
            _write_pack(source, members, temporary, compression_level=6)
            digest = install(temporary, "pack")
            for entry in members:
                entry.storage = {"object": digest}
        _, after = _collect_entries(source)
        if (
            before["stability_fingerprint_sha256"]
            != after["stability_fingerprint_sha256"]
        ):
            raise SnapshotError("artifact changed during packing; no package committed")
        _source_ok(artifact_root, spec, config)
        manifest = {
            "role": ROLE,
            "scope": _scope(config),
            "source": {
                key: before[key]
                for key in (
                    "files",
                    "directories",
                    "symlinks",
                    "logical_bytes",
                    "portable_fingerprint_sha256",
                )
            },
            "entries": [entry.inventory_row() for entry in entries],
            "objects": [objects[key] for key in sorted(objects)],
        }
        payload = _canonical_json_bytes(manifest)
        package_id = hashlib.sha256(payload).hexdigest()
        destination = _path(root, f"packages/{package_id}.json")
        if destination.exists():
            if destination.read_bytes() != payload:
                raise SnapshotError("existing package is corrupt")
        else:
            atomic_write_bytes(destination, payload)
        receipt = {
            "package_id": package_id,
            "role": ROLE,
            "source": manifest["source"],
            "objects": len(objects),
            "transport_bytes": sum(obj["bytes"] for obj in objects.values()),
            "source_stability_fingerprint_sha256": before[
                "stability_fingerprint_sha256"
            ],
        }
        atomic_write_json(state / f"built-{package_id}.json", receipt)
        return receipt


def receive_package(config: dict[str, Any], package_id: str) -> dict[str, Any]:
    spec, root, state = _setup(config)
    _validate_hash(package_id, "pinned package")
    with _lock(state):
        manifest_path = _path(root, f"packages/{package_id}.json")
        if manifest_path.stat().st_size > 16 * 1024 * 1024:
            raise SnapshotError("package manifest is oversized")
        payload = manifest_path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != package_id:
            raise SnapshotError("package manifest checksum mismatch")
        manifest = json.loads(payload)
        if manifest["role"] != ROLE or manifest["scope"] != _scope(config):
            raise SnapshotError("package is outside the authorized scope")
        rows, summary = manifest["entries"], manifest["source"]
        _bounded(config, rows, summary)
        paths: dict[str, dict[str, Any]] = {}
        for row in rows:
            relative = _relative(row["path"])
            if relative in paths or row["kind"] not in {"file", "directory"}:
                raise SnapshotError("duplicate or forbidden entry")
            if not isinstance(row["mode"], int) or row["mode"] & ~0o777:
                raise SnapshotError("unsafe mode bits")
            paths[relative] = row
            if row["kind"] == "file":
                _validate_hash(row["sha256"], "file")
                _validate_hash(row["storage"]["object"], "storage")
                if not isinstance(row["size"], int) or row["size"] < 0:
                    raise SnapshotError("invalid file size")
        files = [row for row in rows if row["kind"] == "file"]
        if (
            len(files) != summary["files"]
            or sum(row["size"] for row in files) != summary["logical_bytes"]
        ):
            raise SnapshotError("manifest counts do not match actual entries")
        for relative in paths:
            for parent in Path(relative).parents:
                if parent == Path("."):
                    continue
                if paths.get(parent.as_posix(), {}).get("kind") != "directory":
                    raise SnapshotError("missing or non-directory entry parent")
        objects = {}
        if len(manifest["objects"]) > len(files):
            raise SnapshotError("too many transport objects")
        if any(
            not isinstance(obj["bytes"], int) or obj["bytes"] < 0
            for obj in manifest["objects"]
        ):
            raise SnapshotError("invalid object size")
        if (
            sum(obj["bytes"] for obj in manifest["objects"])
            > config["maximum_logical_bytes"] + config["maximum_entries"] * 65536
        ):
            raise SnapshotError("transport objects exceed resource bounds")
        for obj in manifest["objects"]:
            digest = _validate_hash(obj["sha256"], "object")
            if digest in objects or obj["kind"] not in {"blob", "pack"}:
                raise SnapshotError("duplicate or invalid object")
            if obj["path"] != f"objects/{obj['kind']}/{digest[:2]}/{digest}":
                raise SnapshotError("invalid object path")
            path = _path(root, obj["path"])
            if (
                not stat.S_ISREG(path.stat().st_mode)
                or path.stat().st_size != obj["bytes"]
                or sha256_file(path) != digest
            ):
                raise SnapshotError("transport object checksum/size mismatch")
            objects[digest] = obj
        if set(objects) != {row["storage"]["object"] for row in files}:
            raise SnapshotError("unreferenced or missing transport object")
        accepted = _path(state, f"accepted/{package_id}")

        def verify(destination: Path) -> None:
            source = _path(destination / "artifacts", spec.relative_root)
            _verify_materialized(source, manifest, rows)
            _source_ok(destination / "artifacts", spec, config)

        if accepted.exists():
            verify(accepted)
        else:
            with tempfile.TemporaryDirectory(prefix="receive-", dir=state) as scratch:
                staging = Path(scratch) / "candidate"
                source = _path(staging / "artifacts", spec.relative_root)
                source.mkdir(parents=True)
                for row in rows:
                    if row["kind"] == "directory":
                        (source / row["path"]).mkdir(parents=True, exist_ok=True)
                for digest, obj in objects.items():
                    members = [
                        row for row in files if row["storage"]["object"] == digest
                    ]
                    path = _path(root, obj["path"])
                    with contextlib.ExitStack() as stack:
                        archive = (
                            stack.enter_context(zipfile.ZipFile(path))
                            if obj["kind"] == "pack"
                            else None
                        )
                        if archive is not None:
                            if sorted(archive.namelist()) != sorted(
                                row["path"] for row in members
                            ):
                                raise SnapshotError("pack member list mismatch")
                        for row in members:
                            if archive is not None:
                                if (
                                    archive.getinfo(row["path"]).file_size
                                    != row["size"]
                                ):
                                    raise SnapshotError("pack member size mismatch")
                                stream = archive.open(row["path"])
                            else:
                                if (
                                    row["sha256"] != digest
                                    or row["size"] != obj["bytes"]
                                ):
                                    raise SnapshotError("blob entry mismatch")
                                stream = path.open("rb")
                            with stream:
                                _copy_member_and_verify(
                                    stream,
                                    source / row["path"],
                                    expected_size=row["size"],
                                    expected_sha256=row["sha256"],
                                    mode=row["mode"],
                                )
                verify(staging)
                accepted.parent.mkdir(parents=True, exist_ok=True)
                os.rename(staging, accepted)
                _fsync_directory(accepted.parent)
        receipt = {
            "package_id": package_id,
            "status": "verified_quarantine",
            "artifact_root": str(accepted / "artifacts"),
            "checkpoint_sha256": config["checkpoint_sha256"],
            "source": summary,
            "cold_published": False,
            "model_activated": False,
        }
        atomic_write_json(state / f"received-{package_id}.json", receipt)
        return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=Path("configs/data_sync/artifact_ingress.json")
    )
    parser.add_argument(
        "--config-json", help="control-plane configuration; never artifact bytes"
    )
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("artifacts").absolute()
    )
    parser.add_argument("action", choices=("build", "receive"))
    parser.add_argument("--package-id")
    args = parser.parse_args()
    config = json.loads(args.config_json or args.config.read_text())
    result = (
        build_package(config, args.artifact_root)
        if args.action == "build"
        else receive_package(config, args.package_id)
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
