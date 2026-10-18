"""One complete CPU forward, published by the canonical source/key owner.

This is a narrow catalog family, not a second cache, archive or registry. The
original two files and v3 decoder remain authoritative; pins use the existing
packed format and the configured retention owner's materialized namespace.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import pickle
import re
import stat
from typing import Any, Mapping

from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory, _paths_overlap, validate_slug
from stockagent.data_sync.packed_snapshots import (
    _existing_packed_publication_owner,
    _load_inventory,
    _validate_inventory,
    resolve_packed_pin,
    verify_packed_snapshot,
    write_packed_pin,
)

FAMILY = "completed_tw_panel_forward_cpu_v3"


def validate_template(entry: Mapping[str, Any]) -> None:
    if entry.get("publication_family") != FAMILY:
        raise SnapshotError("unsupported catalog publication family")
    if any(name in entry for name in (
        "included_root_files", "excluded_subtrees", "maximum_file_bytes",
        "source_coordination_lock", "source_coordination_lock_scope", "completed_forward_key",
    )):
        raise SnapshotError("completed-forward family requires explicit CLI key selection, not catalog file/lock overrides")


def validate_key(key: object) -> str:
    if not isinstance(key, str) or re.fullmatch(r"[0-9a-f]{64}", key) is None:
        raise SnapshotError("completed-forward key must be a full lowercase SHA-256")
    return key


def select_entry(entry: Mapping[str, Any], key: str) -> dict[str, Any]:
    validate_template(entry)
    validate_key(key)
    source_name = Path(str(entry["source"])).name
    dataset = validate_slug(f"{entry['dataset']}-{key}", "completed-forward publication dataset")
    return {**entry, "dataset": dataset, "completed_forward_key": key,
            "included_root_files": [f"{key}.forward", f"{key}.receipt.json"],
            "source_coordination_lock_scope": "source_parent",
            "source_coordination_lock": f"{source_name}/{key}.lock"}


def _generation(path: Path) -> tuple[int, ...]:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
        raise SnapshotError(f"completed-forward member is not an owned regular file: {path}")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns,
            info.st_mode, info.st_nlink, info.st_uid, info.st_gid)


def _root_generation(path: Path) -> tuple[int, ...]:
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or path.resolve() != path.absolute()):
        raise SnapshotError(f"completed-forward root must be real, owned and private: {path}")
    # New unrelated keys may be committed concurrently. Directory timestamps
    # do not describe this explicit pair, but replacing its owner does.
    return info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SnapshotError("duplicate completed-forward receipt field")
        result[key] = value
    return result


@dataclass(frozen=True)
class ForwardSourceProof:
    root: Path
    root_generation: tuple[int, ...]
    generations: tuple[tuple[Path, tuple[int, ...]], ...]
    metadata: dict[str, str]

    def guard(self) -> None:
        if _root_generation(self.root) != self.root_generation or any(
            _generation(path) != expected for path, expected in self.generations
        ):
            raise SnapshotError("completed-forward source changed before publication/pin commit")


def validate_source(entry: Mapping[str, Any], source: Path) -> ForwardSourceProof:
    # Lazy import: status, template discovery and unrelated publications must
    # not import Torch, construct a model, query a provider or initialize CUDA.
    from stockagent.live.completed_inference_cache import (
        CONTRACT, MAX_PAYLOAD_BYTES, _CACHE_MISS, _read, identity_digest,
    )

    key = validate_key(entry.get("completed_forward_key"))
    if CONTRACT != FAMILY:
        raise SnapshotError("completed-forward publication/reader contract mismatch")
    source = source.absolute()
    root_generation = _root_generation(source)
    payload, receipt = source / f"{key}.forward", source / f"{key}.receipt.json"
    try:
        generations = tuple((path, _generation(path)) for path in (payload, receipt))
        payload_info, receipt_info = payload.lstat(), receipt.lstat()
        if (payload_info.st_mode & 0o222 or not 0 < payload_info.st_size <= MAX_PAYLOAD_BYTES
                or receipt_info.st_nlink != 1 or receipt_info.st_size > 1024**2):
            raise SnapshotError("completed-forward source is writable, oversized or not a private receipt")
        descriptor = os.open(receipt, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as handle:
            raw = handle.read(1024**2 + 1)
            opened = os.fstat(handle.fileno())
        if (len(raw) > 1024**2 or (opened.st_dev, opened.st_ino)
                != (receipt_info.st_dev, receipt_info.st_ino)):
            raise SnapshotError("completed-forward receipt changed during bounded read")
        record = json.loads(raw, object_pairs_hook=_unique_object)
        if (not isinstance(record, dict) or record.get("contract") != FAMILY
                or record.get("key") != key or not isinstance(record.get("identity"), dict)
                or type(record.get("payload_bytes")) is not int
                or identity_digest(record["identity"]) != key):
            raise SnapshotError("completed-forward receipt is not bound to this key")
        commit: dict[str, Any] = {}
        value = _read(payload, key, record["identity"], commit=commit)
        if value is _CACHE_MISS or commit.get("receipt_sha256") != hashlib.sha256(raw).hexdigest():
            raise SnapshotError("completed-forward source was not fully decoded and accepted")
        del value  # Do not retain a publishing worker's CPU mmap after verification.
        proof = ForwardSourceProof(source, root_generation, generations, {
            "completed_forward_contract": FAMILY, "completed_forward_key": key,
            "completed_forward_payload_sha256": commit["payload_sha256"],
            "completed_forward_payload_bytes": str(commit["payload_bytes"]),
            "completed_forward_receipt_sha256": commit["receipt_sha256"],
        })
        proof.guard()
        return proof
    except (OSError, ValueError, RuntimeError, EOFError, pickle.UnpicklingError) as error:
        raise SnapshotError(f"cannot accept completed-forward source: {error}") from error


def fixed_pin_path(entry: Mapping[str, Any], *, sync_root: Path,
                   retention_config: Path, repo_root: Path) -> Path:
    from stockagent.data_sync.packed_retention import RetentionConfig

    try:
        config = RetentionConfig.load(retention_config, repo_root=repo_root)
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise SnapshotError(f"cannot read completed-forward retention owner: {error}") from error
    if not config.sync_root.is_absolute() or config.sync_root != sync_root.absolute():
        raise SnapshotError("completed-forward pin retention owner does not cover this sync root")
    root = config.materialized_root
    source = Path(str(entry["source"]))
    source = source if source.is_absolute() else repo_root / source
    if not root.is_absolute() or _paths_overlap(root, sync_root) or _paths_overlap(root, source):
        raise SnapshotError("completed-forward pins require the separate configured materialized root")
    _root_generation(root)
    key = validate_key(entry.get("completed_forward_key"))
    family = str(entry["dataset"]).removesuffix(f"-{key}")
    # Use the canonical retention pin discovery; no second pin registry.
    pin = root / "pins" / family / f"{key}.pin.json"
    _pin_parents(pin)
    return pin


def _pin_parents(pin: Path, *, create: bool = False) -> tuple[tuple[Path, tuple[int, ...]], ...]:
    generations = []
    for parent in (pin.parent.parent.parent, pin.parent.parent, pin.parent):
        if not parent.exists() and not parent.is_symlink():
            if not create:
                continue
            parent.mkdir(mode=0o700)
            _fsync_directory(parent.parent)
        generations.append((parent, _root_generation(parent)))
    return tuple(generations)


def _guard_pin_parents(generations: tuple[tuple[Path, tuple[int, ...]], ...]) -> None:
    if any(_root_generation(path) != expected for path, expected in generations):
        raise SnapshotError("completed-forward retention pin owner changed during publication")


def _check_binding(sync_root: Path, entry: Mapping[str, Any], proof: ForwardSourceProof, resolved) -> None:
    manifest = resolved.manifest
    if (manifest.get("source", {}).get("selection", {}).get("included_root_files")
            != entry["included_root_files"] or manifest["source"]["files"] != 2
            or any(manifest.get("metadata", {}).get(key) != value
                   for key, value in proof.metadata.items())):
        raise SnapshotError("fixed completed-forward publication differs from the accepted source")
    rows = _load_inventory(sync_root, manifest)
    _validate_inventory(manifest, rows)
    expected = {
        proof.generations[0][0].name: (proof.metadata["completed_forward_payload_sha256"],
                                      proof.generations[0][1][2]),
        proof.generations[1][0].name: (proof.metadata["completed_forward_receipt_sha256"],
                                      proof.generations[1][1][2]),
    }
    if len(rows) != 2 or any(
        row.get("kind") != "file" or row.get("path") not in expected
        or (row.get("sha256"), row.get("size")) != expected[row["path"]]
        for row in rows
    ):
        raise SnapshotError("fixed completed-forward inventory does not match the original file hashes")


def reuse_fixed_pin(sync_root: Path, pin: Path, entry: Mapping[str, Any], proof: ForwardSourceProof):
    if not pin.exists() and not pin.is_symlink():
        return None
    with _existing_packed_publication_owner(sync_root):
        parents = _pin_parents(pin)
        resolved = resolve_packed_pin(sync_root, pin, expected_dataset=str(entry["dataset"]))
        _check_binding(sync_root, entry, proof, resolved)
        verify_packed_snapshot(sync_root, resolved, reconstruct_paths=entry["included_root_files"])
        proof.guard()
        _guard_pin_parents(parents)
        return resolved


def protect_fixed_pin(sync_root: Path, pin: Path, entry: Mapping[str, Any],
                      proof: ForwardSourceProof, resolved) -> None:
    # The publisher has released its global lock before transport. Re-enter
    # the same existing owner and independently verify the exact release; a
    # lost/corrupt release is an error, never a replacement or a compute miss.
    with _existing_packed_publication_owner(sync_root):
        parents = _pin_parents(pin, create=True)
        _check_binding(sync_root, entry, proof, resolved)
        verify_packed_snapshot(sync_root, resolved, reconstruct_paths=entry["included_root_files"])
        proof.guard()
        write_packed_pin(pin, resolved, exclusive=True)
        accepted = resolve_packed_pin(sync_root, pin, expected_dataset=str(entry["dataset"]))
        if accepted.manifest_sha256 != resolved.manifest_sha256:
            raise SnapshotError("completed-forward fixed pin readback differs")
        proof.guard()
        _guard_pin_parents(parents)
