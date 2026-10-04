"""Encrypted backup of exact canonical releases, with independent restore proof.

Restic transports selected immutable objects. Existing packed verifiers still
establish content identity and reconstructibility. No source is deleted, no
active head is overwritten, and a local test is never called a NAS backup.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import time
from typing import Any

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes
from stockagent.data_sync.packed_backup import (
    PinnedObjectSignatures, object_descriptor, safe_path, signature,
)
from stockagent.data_sync.packed_snapshots import (
    PACKED_HEAD_SCHEMA_VERSION, _validate_manifest, resolve_packed_snapshot_id,
    verify_packed_snapshot,
)
from stockagent.runtime_identity import identity_sha256

PLAN_CONTRACT = "exact_packed_and_control_restic_backup_v1"
_HASH = re.compile(r"[0-9a-f]{64}\Z")


def private_json(path: Path, value: dict) -> None:
    old = os.umask(0o077)
    try:
        atomic_write_json(path, value)
    finally:
        os.umask(old)


def private_file(path: Path) -> Path:
    if os.name != "posix":
        raise SnapshotError("this backup producer requires Linux/WSL private-file permissions")
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise SnapshotError("private credential path must not be a symlink")
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.geteuid():
        raise SnapshotError("private credential needs owner-only permissions and ownership")
    if info.st_size == 0:
        raise SnapshotError("private credential file is empty")
    return path.resolve(strict=True)


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _regular(path: Path) -> Path:
    # Check the unresolved path too: resolve() must not bless a symlink.
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise SnapshotError("backup input must not traverse a symlink")
    if not stat.S_ISREG(path.stat().st_mode):
        raise SnapshotError("backup input must be a regular file")
    return path.absolute()


def capture_plan(cold_root: Path, *, selectors: list[tuple[str, str]] | None = None,
                 control_receipt: Path | None = None, all_history: bool = False) -> dict[str, Any]:
    """Capture head bytes now; an advancing source does not alter this backup.

    selectors=None means every current head, not all historical releases or all
    unpublished downloader workspaces. Explicit selectors are exact releases.
    """
    cold = cold_root.resolve(strict=True)
    if all_history and selectors is not None:
        raise SnapshotError("all-history capture cannot be combined with explicit release selection")
    objects: dict[str, dict] = {}
    releases: list[dict] = []
    heads: dict[str, str] = {}
    selected: list[tuple[str, str]] = []
    if selectors is None:
        for head_path in sorted((cold / "heads").glob("*/*.json")):
            relative = head_path.relative_to(cold).as_posix()
            raw = _regular(safe_path(cold, relative)).read_bytes()
            head = json.loads(raw)
            if (head.get("schema_version") != PACKED_HEAD_SCHEMA_VERSION
                    or head.get("dataset") != head_path.parent.name
                    or head.get("node_id") != head_path.stem
                    or ".sync-conflict-" in head_path.name):
                raise SnapshotError("invalid current packed head")
            selected.append((head["dataset"], head["snapshot_id"]))
            heads[relative] = raw.decode("utf-8")
        if all_history:
            selected.extend((p.parent.name, p.stem) for p in sorted((cold / "manifests").glob("*/*.json")))
    else:
        selected = list(selectors)
    if not selected:
        raise SnapshotError("no exact releases selected")
    # Reuse the canonical no-follow directory-FD scanner. Repeated path
    # resolution per file is costly on DrvFs; pinned directories retain the
    # same symlink/replacement boundary, checked before this pass is accepted.
    with PinnedObjectSignatures(cold) as pinned:
        for dataset, snapshot_id in sorted(set(selected)):
            resolved = resolve_packed_snapshot_id(cold, dataset, snapshot_id, require_objects=False)
            manifest_path = _regular(resolved.manifest_path)
            raw = manifest_path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != resolved.manifest_sha256:
                raise SnapshotError("manifest changed during capture")
            relative = manifest_path.relative_to(cold).as_posix()
            releases.append({"dataset": dataset, "snapshot_id": snapshot_id,
                             "manifest_sha256": resolved.manifest_sha256,
                             "manifest_relative": relative, "manifest_json": raw.decode("utf-8")})
            for ref in [resolved.manifest["archive"]["inventory"], *resolved.manifest["archive"]["objects"]]:
                relative = ref["relpath"]
                digest = object_descriptor(relative)
                if ref["sha256"] != digest:
                    raise SnapshotError("packed object identity differs from manifest")
                if relative in objects:
                    if (objects[relative]["sha256"], objects[relative]["bytes"]) != (digest, ref["bytes"]):
                        raise SnapshotError("conflicting object reference")
                    continue
                observed = pinned.signature(relative)
                if observed[2] != ref["bytes"]:
                    raise SnapshotError("packed object size differs from manifest")
                objects[relative] = {"path": str(cold / relative), "relative": relative, "sha256": digest,
                                     "bytes": observed[2], "signature": list(observed), "role": "packed_object"}
        pinned.recheck()
    for relative, body in heads.items():
        head = json.loads(body)
        matching = [r for r in releases if r["dataset"] == head["dataset"]
                    and r["snapshot_id"] == head["snapshot_id"]]
        if (len(matching) != 1 or matching[0]["manifest_sha256"] != head["manifest_sha256"]
                or matching[0]["manifest_relative"] != head["manifest_relpath"]):
            raise SnapshotError("current head and manifest disagree")
    files = sorted(objects.values(), key=lambda row: row["relative"])
    control = None
    if control_receipt is not None:
        receipt_path = private_file(control_receipt)
        receipt = json.loads(receipt_path.read_bytes())
        if receipt.get("state") != "backup_written" or not receipt.get("same_mvcc_snapshot_as_dump"):
            raise SnapshotError("control backup requires an exported same-snapshot receipt")
        control_files = []
        for name, hash_key in (("archive", "sha256"), ("logical_state_file", "logical_state_file_sha256")):
            path = private_file(Path(receipt[name]))
            if path.parent != receipt_path.parent or _sha(path) != receipt[hash_key]:
                raise SnapshotError("control backup differs from its private receipt")
            info = signature(path)
            row = {"path": str(path), "relative": path.name, "sha256": receipt[hash_key],
                   "bytes": info[2], "signature": list(info), "role": "control_" + name}
            files.append(row); control_files.append(row)
        control = {"logical_state_identity_sha256": receipt["logical_state_identity_sha256"],
                   "receipt_json": receipt, "files": control_files}
    body = {"contract": PLAN_CONTRACT, "cold_root": str(cold), "releases": releases,
            "captured_heads": heads, "files": files, "control": control,
            "scope": "selected immutable packed releases and optional consistent control backup",
            "selection": "all_observed_retained_manifests" if all_history else ("current_heads" if selectors is None else "explicit_releases"),
            "all_history_verified": False, "unpublished_sources_included": False}
    return {**body, "identity_sha256": identity_sha256(body),
            "observed_at_utc": datetime.now(timezone.utc).isoformat()}


def validate_plan(plan: dict) -> None:
    body = {key: value for key, value in plan.items() if key not in {"identity_sha256", "observed_at_utc"}}
    if plan.get("contract") != PLAN_CONTRACT or identity_sha256(body) != plan.get("identity_sha256"):
        raise SnapshotError("backup plan contract or fingerprint differs")
    cold = PurePosixPath(plan["cold_root"])
    if not cold.is_absolute() or ".." in cold.parts or cold == PurePosixPath("/"):
        raise SnapshotError("invalid authoritative cold root")
    if not plan["releases"] or not plan["files"]:
        raise SnapshotError("backup plan has no exact releases or objects")
    expected_objects: dict[str, tuple[str, int]] = {}
    releases: dict[tuple[str, str], dict] = {}
    for release in plan["releases"]:
        manifest = json.loads(release["manifest_json"])
        _validate_manifest(manifest)
        if (hashlib.sha256(release["manifest_json"].encode()).hexdigest() != release["manifest_sha256"]
                or manifest["dataset"] != release["dataset"]
                or manifest["snapshot_id"] != release["snapshot_id"]
                or release["manifest_relative"] != f"manifests/{release['dataset']}/{release['snapshot_id']}.json"):
            raise SnapshotError("captured release metadata differs from its identity")
        key = (release["dataset"], release["snapshot_id"])
        if key in releases:
            raise SnapshotError("duplicate selected release")
        releases[key] = release
        for ref in [manifest["archive"]["inventory"], *manifest["archive"]["objects"]]:
            expected = (ref["sha256"], ref["bytes"])
            if ref["relpath"] in expected_objects and expected_objects[ref["relpath"]] != expected:
                raise SnapshotError("conflicting immutable reference")
            expected_objects[ref["relpath"]] = expected
    for relative, raw in plan["captured_heads"].items():
        head = json.loads(raw)
        release = releases.get((head.get("dataset"), head.get("snapshot_id")))
        if (head.get("schema_version") != PACKED_HEAD_SCHEMA_VERSION or release is None
                or relative != f"heads/{head['dataset']}/{head.get('node_id')}.json"
                or head.get("manifest_sha256") != release["manifest_sha256"]
                or head.get("manifest_relpath") != release["manifest_relative"]):
            raise SnapshotError("captured head differs from its selected release")
    seen_paths: set[str] = set()
    actual_objects: dict[str, tuple[str, int]] = {}
    control_files = []
    for row in plan["files"]:
        path = PurePosixPath(row["path"])
        if (not _HASH.fullmatch(row["sha256"]) or not path.is_absolute() or ".." in path.parts
                or "\n" in row["path"] or "\r" in row["path"]
                or row["path"] in seen_paths or type(row["bytes"]) is not int or row["bytes"] < 0
                or len(row["signature"]) != 5 or any(type(v) is not int for v in row["signature"])
                or row["signature"][2] != row["bytes"]):
            raise SnapshotError("invalid backup file identity")
        seen_paths.add(row["path"])
        if row["role"] == "packed_object":
            if object_descriptor(row["relative"]) != row["sha256"]:
                raise SnapshotError("invalid packed file descriptor")
            if path != cold / row["relative"]:
                raise SnapshotError("packed file escaped the selected cold root")
            actual_objects[row["relative"]] = (row["sha256"], row["bytes"])
        elif row["role"] in {"control_archive", "control_logical_state_file"}:
            control_files.append(row)
        else:
            raise SnapshotError("unsupported backup input role")
    if expected_objects != actual_objects:
        raise SnapshotError("backup file set differs from the selected manifests")
    control = plan["control"]
    if control is None:
        if control_files:
            raise SnapshotError("control files have no consistent backup receipt")
    else:
        receipt = control["receipt_json"]
        if (receipt.get("state") != "backup_written" or not receipt.get("same_mvcc_snapshot_as_dump")
                or control_files != control["files"] or len(control_files) != 2
                or control["logical_state_identity_sha256"] != receipt["logical_state_identity_sha256"]):
            raise SnapshotError("inconsistent control backup receipt")
        for row, name, hash_key in zip(control_files, ("archive", "logical_state_file"),
                                       ("sha256", "logical_state_file_sha256")):
            if (row["path"] != receipt[name] or row["sha256"] != receipt[hash_key]
                    or row["role"] != "control_" + name):
                raise SnapshotError("control file differs from consistent receipt")


def unchanged_inputs(plan: dict) -> None:
    validate_plan(plan)
    with PinnedObjectSignatures(Path(plan["cold_root"])) as pinned:
        for row in plan["files"]:
            observed = (pinned.signature(row["relative"]) if row["role"] == "packed_object"
                        else signature(_regular(Path(row["path"]))))
            if list(observed) != row["signature"]:
                raise SnapshotError("backup input changed after capture")
        pinned.recheck()


def export_delivery(plan: dict, destination: Path) -> dict:
    """Make one closed, bounded transport with canonical packed objects.

    The receiver can verify bytes on native Windows without porting the Linux
    publisher. No raw workspaces, runtime keys or node-local state are included.
    Copies leave source inodes and their signature receipts unchanged.
    """
    from scripts.verify_backup_delivery import CONTRACT, verify
    unchanged_inputs(plan)
    destination = destination.absolute()
    source = Path(plan["cold_root"])
    if (destination.exists() or any(p.is_symlink() for p in (destination, *destination.parents))
            or destination.is_relative_to(source) or source.is_relative_to(destination)):
        raise SnapshotError("backup delivery needs a fresh independent destination")
    destination.mkdir(parents=True, mode=0o700)
    members: dict[str, dict] = {}
    copied = 0
    def metadata(relative: str, payload: bytes) -> None:
        atomic_write_bytes(safe_path(destination, relative), payload, mode=0o600)
        members[relative] = {"relative": relative, "sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}
    for row in plan["files"]:
        relative = ("cold/" + row["relative"] if row["role"] == "packed_object"
                    else "control/" + row["relative"])
        target = safe_path(destination, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        original = _regular(Path(row["path"]))
        with original.open("rb") as reader, target.open("xb") as writer:
            for chunk in iter(lambda: reader.read(4 * 1024**2), b""):
                writer.write(chunk)
        target.chmod(0o600)
        copied += 1
        members[relative] = {"relative": relative, "sha256": row["sha256"], "bytes": row["bytes"]}
    for release in plan["releases"]:
        metadata("cold/" + release["manifest_relative"], release["manifest_json"].encode())
    for relative, body in plan["captured_heads"].items():
        metadata("cold/" + relative, body.encode())
    metadata("source-plan.json", json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2).encode())
    body = {"contract": CONTRACT, "source_plan_identity_sha256": plan["identity_sha256"],
            "files": sorted(members.values(), key=lambda r: r["relative"]),
            "producer_node_id": "penguin", "source_scope": plan["scope"],
            "selection": plan.get("selection", "current_heads"),
            "all_history_verified": False, "unpublished_sources_included": False}
    envelope = {**body, "identity_sha256": identity_sha256(body)}
    private_json(destination / "backup-envelope.json", envelope)
    unchanged_inputs(plan)
    receipt = verify(destination, envelope["identity_sha256"], require_ready=False)
    verified = []
    for release in plan["releases"]:
        selected = resolve_packed_snapshot_id(destination / "cold", release["dataset"], release["snapshot_id"])
        verified.append(verify_packed_snapshot(destination / "cold", selected))
    unchanged_inputs(plan)
    # Publish READY last. A failed SHA/CRC/source check leaves an unready
    # diagnostic directory, which must never be shared as a closed batch.
    atomic_write_bytes(destination / "READY", (envelope["identity_sha256"] + "\n").encode(), mode=0o600)
    return {"state": "closed_backup_delivery_verified", "destination": str(destination),
            "envelope_identity_sha256": envelope["identity_sha256"], "transport": receipt,
            "canonical_releases_verified": len(verified), "hardlinked_objects": 0, "copied_files": copied,
            "canonical_proofs": verified,
            "durable_off_host_backup_verified": False}


def export_incremental_delivery(cold_root: Path, rows: list[dict], destination: Path, *,
                                catalog_identity: str, read_root: Path | None = None) -> dict:
    """Export bounded, deduplicated canonical objects or fixed metadata.

    A wave deliberately need not contain a whole release. Its envelope proves
    its exact file bytes; reconstruction is tested on the union of NAS restores.
    Live downloader workspaces, credentials and node-local files are excluded.
    """
    from scripts.verify_backup_delivery import CONTRACT, verify
    if not _HASH.fullmatch(catalog_identity) or not rows:
        raise SnapshotError("incremental delivery requires a pinned nonempty catalog selection")
    cold = cold_root.absolute()
    reader_root = (read_root or cold_root).absolute()
    destination = destination.absolute()
    if (destination.exists() or any(p.is_symlink() for p in (destination, *destination.parents, reader_root, *reader_root.parents))
            or destination.is_relative_to(cold) or cold.is_relative_to(destination)):
        raise SnapshotError("incremental delivery needs a fresh independent destination")
    relative_names = set()
    for row in rows:
        relative = row["relative"]
        if relative in relative_names or not _HASH.fullmatch(row["sha256"]):
            raise SnapshotError("duplicate or invalid incremental member")
        relative_names.add(relative)
        if relative.startswith("objects/"):
            if object_descriptor(relative) != row["sha256"]:
                raise SnapshotError("incremental object differs from its canonical descriptor")
        elif not (re.fullmatch(r"(?:manifests|heads)/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+\.json", relative)
                  or re.fullmatch(r"head-history/heads/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+/[0-9a-f]{64}\.json", relative)):
            raise SnapshotError("incremental selection is outside canonical cold metadata/objects")
        if type(row["bytes"]) is not int or row["bytes"] < 0:
            raise SnapshotError("invalid incremental member size")
    destination.mkdir(parents=True, mode=0o700)
    members = []
    with PinnedObjectSignatures(cold) as pinned:
        def unchanged(row: dict) -> None:
            if "captured_bytes_utf8" in row:
                raw = row["captured_bytes_utf8"].encode("utf-8")
                if (not row["relative"].startswith("heads/") or len(raw) != row["bytes"]
                        or hashlib.sha256(raw).hexdigest() != row["sha256"]):
                    raise SnapshotError("captured head bytes differ from their fixed catalog identity")
                return
            observed = (pinned.signature(row["relative"]) if row["relative"].startswith("objects/")
                        else signature(_regular(safe_path(cold, row["relative"]))))
            if list(observed) != list(row["signature"]):
                raise SnapshotError("incremental source changed after its fixed capture")

        for row in rows:
            unchanged(row)
            source = None if "captured_bytes_utf8" in row else _regular(safe_path(reader_root, row["relative"]))
            reader_before = signature(source) if source else None
            relative = "cold/" + row["relative"]
            target = safe_path(destination, relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            reader_stream = source.open("rb") if source else io.BytesIO(row["captured_bytes_utf8"].encode("utf-8"))
            with reader_stream as reader, target.open("xb") as writer:
                target.chmod(0o600)
                for chunk in iter(lambda: reader.read(8 * 1024**2), b""):
                    digest.update(chunk)
                    writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
            if (digest.hexdigest() != row["sha256"] or target.stat().st_size != row["bytes"]
                    or (source is not None and signature(source) != reader_before)):
                raise SnapshotError("incremental source bytes differ from the canonical selection")
            unchanged(row)
            members.append({"relative": relative, "sha256": row["sha256"], "bytes": row["bytes"]})
        pinned.recheck()
        body = {"contract": CONTRACT, "source_plan_identity_sha256": catalog_identity,
                "files": sorted(members, key=lambda r: r["relative"]), "producer_node_id": "penguin",
                "source_scope": "fixed canonical cold metadata and/or immutable object increment",
                "selection": "incremental_object_wave", "all_history_verified": False,
                "unpublished_sources_included": False, "complete_release_in_this_delivery": False}
        envelope = {**body, "identity_sha256": identity_sha256(body)}
        private_json(destination / "backup-envelope.json", envelope)
        transport = verify(destination, envelope["identity_sha256"], require_ready=False)
        for row in rows:
            unchanged(row)
        pinned.recheck()
        atomic_write_bytes(destination / "READY", (envelope["identity_sha256"] + "\n").encode(), mode=0o600)
    return {"state": "closed_incremental_delivery_verified", "destination": str(destination),
            "envelope_identity_sha256": envelope["identity_sha256"], "transport": transport,
            "copied_files": len(rows), "hardlinked_objects": 0,
            "alternate_read_root_used": reader_root != cold,
            "canonical_reconstruction_verified": False, "durable_off_host_backup_verified": False}


class ResticBackup:
    def __init__(self, executable: Path, repository: str, password_file: Path, *, cache: Path):
        self.executable = executable.resolve(strict=True)
        self.password_file = private_file(password_file)
        if "\n" in repository or not repository or repository.startswith("-"):
            raise SnapshotError("invalid repository")
        # Authentication stays in private files, not repository URLs.
        if "@" in repository and not repository.startswith("sftp:"):
            raise SnapshotError("credential-bearing repository URLs are unsupported")
        self.repository = repository
        self.cache = cache.absolute()

    def run(self, arguments: list[str], *, timeout: int | None = None) -> subprocess.CompletedProcess:
        environment = {**{key: value for key, value in os.environ.items() if not key.startswith("RESTIC_")},
                       "RESTIC_PASSWORD_FILE": str(self.password_file),
                       "RESTIC_REPOSITORY": self.repository, "RESTIC_CACHE_DIR": str(self.cache)}
        result = subprocess.run([str(self.executable), *arguments], env=environment,
                                capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            # Restic exit 3 means incomplete backup, even if a snapshot exists.
            raise SnapshotError(f"restic operation failed with exit {result.returncode}; acceptance withheld")
        return result

    def initialize(self) -> dict:
        result = self.run(["init", "--json"], timeout=60)
        return json.loads(result.stdout)

    def backup(self, plan: dict, plan_file: Path) -> dict:
        unchanged_inputs(plan)
        plan_file = _regular(plan_file.absolute())
        if json.loads(plan_file.read_bytes()) != plan:
            raise SnapshotError("recorded plan differs from selected plan")
        listing = plan_file.with_name("restic-files.txt")
        selected = [str(plan_file.absolute()), *(row["path"] for row in plan["files"])]
        if any("\n" in path or "\r" in path for path in selected):
            raise SnapshotError("line breaks in backup paths are unsupported")
        atomic_write_bytes(listing, ("\n".join(selected) + "\n").encode(), mode=0o600)
        started = time.perf_counter()
        result = self.run(["backup", "--json", "--files-from-verbatim", str(listing),
                           "--tag", PLAN_CONTRACT, "--tag", plan["identity_sha256"]])
        summaries = [json.loads(line) for line in result.stdout.splitlines()
                     if line.strip() and json.loads(line).get("message_type") == "summary"]
        if len(summaries) != 1 or not _HASH.fullmatch(summaries[0].get("snapshot_id", "")):
            raise SnapshotError("restic did not return one exact snapshot identity")
        unchanged_inputs(plan)
        if json.loads(plan_file.read_bytes()) != plan:
            raise SnapshotError("recorded plan changed during backup")
        return {"state": "snapshot_written", "snapshot_id": summaries[0]["snapshot_id"],
                "plan_file": str(plan_file),
                "plan_identity_sha256": plan["identity_sha256"], "summary": summaries[0],
                "complete_wall_seconds": time.perf_counter() - started,
                "durable_off_host_backup_verified": False, "restore_verified": False}

    def restore(self, snapshot_id: str, destination: Path) -> dict:
        if not _HASH.fullmatch(snapshot_id):
            raise SnapshotError("restore requires an exact full snapshot ID, never latest")
        if destination.exists() or destination.is_symlink():
            raise SnapshotError("restore requires a fresh independent directory")
        destination = destination.absolute()
        if any(p.is_symlink() for p in destination.parents):
            raise SnapshotError("restore parent must not traverse a symlink")
        self.run(["restore", snapshot_id, "--target", str(destination), "--verify"], timeout=None)
        return {"state": "restored", "snapshot_id": snapshot_id, "destination": str(destination),
                "durable_off_host_backup_verified": False}


def verify_restore(plan: dict, restored_root: Path, *, record_plan_path: Path) -> dict:
    """Verify restored bytes, then use the canonical packed verifier on that copy."""
    validate_plan(plan)
    restored = restored_root.resolve(strict=True)
    cold_source = Path(plan["cold_root"])
    if restored == cold_source or restored.is_relative_to(cold_source) or cold_source.is_relative_to(restored):
        raise SnapshotError("restore verification must be independent of authoritative source")
    recovered_plan = safe_path(restored, record_plan_path.absolute().as_posix().lstrip("/"))
    if json.loads(_regular(recovered_plan).read_bytes()) != plan:
        raise SnapshotError("restored plan differs from the exact selected plan")
    for row in plan["files"]:
        recovered = _regular(safe_path(restored, PurePosixPath(row["path"]).as_posix().lstrip("/")))
        if recovered.stat().st_size != row["bytes"] or _sha(recovered) != row["sha256"]:
            raise SnapshotError("restored file content differs from its immutable identity")
    cold_copy = safe_path(restored, cold_source.as_posix().lstrip("/"))
    for release in plan["releases"]:
        metadata = release["manifest_json"].encode()
        if hashlib.sha256(metadata).hexdigest() != release["manifest_sha256"]:
            raise SnapshotError("captured manifest identity differs")
        atomic_write_bytes(safe_path(cold_copy, release["manifest_relative"]), metadata, mode=0o600)
    for relative, metadata in plan["captured_heads"].items():
        atomic_write_bytes(safe_path(cold_copy, relative), metadata.encode(), mode=0o600)
    verified = []
    for release in plan["releases"]:
        resolved = resolve_packed_snapshot_id(cold_copy, release["dataset"], release["snapshot_id"])
        result = verify_packed_snapshot(cold_copy, resolved)
        verified.append({"dataset": release["dataset"], "snapshot_id": release["snapshot_id"],
                         "manifest_sha256": release["manifest_sha256"], "proof": result})
    return {"state": "restore_verified", "plan_identity_sha256": plan["identity_sha256"],
            "files_verified": len(plan["files"]), "bytes_verified": sum(row["bytes"] for row in plan["files"]),
            "releases": verified, "control_archive_bytes_verified": plan["control"] is not None,
            "control_database_restore_verified": False, "durable_off_host_backup_verified": False,
            "scope": plan["scope"]}
