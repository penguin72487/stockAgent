"""Exact D-backed legacy archive acknowledgements for ephemeral compute nodes.

Uses the existing legacy archive and consumer gates. This is private SSH control,
never synced executable code, a training-completion claim, or model activation.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import stat
import time
import uuid
from dataclasses import asdict

from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path, atomic_write_json, sha256_file
from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec, _archive_directories, source_plan
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids, process_references
from stockagent.data_sync.packed_snapshots import _load_inventory, _validate_inventory, resolve_packed_snapshot_id

RECOVERY_HOLDS = Path("/var/lib/stockagent-legacy-return/recovery-holds.json")


def _recovery_hold_policy() -> dict:
    path = real(RECOVERY_HOLDS)
    if not path.exists():
        return {"schema_version": 1, "origin_node_id": "vastai1T", "authority_node_id": "penguin",
                "protected_roots": [], "reason": "explicit-user-recovery", "created_at_epoch": time.time()}
    before = path.lstat()
    if (not stat.S_ISREG(before.st_mode) or before.st_uid != 0 or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) & 0o077):
        raise SnapshotError("private recovery hold is redirected, shared or not root-private")
    try:
        policy = json.loads(path.read_text())
    except (ValueError, UnicodeError) as error:
        raise SnapshotError("private recovery hold is unreadable") from error
    if (signature(path.lstat()) != signature(before) or not isinstance(policy, dict)
            or set(policy) != {"schema_version", "origin_node_id", "authority_node_id",
                               "protected_roots", "reason", "created_at_epoch"}
            or policy["schema_version"] != 1 or policy["origin_node_id"] != "vastai1T"
            or policy["authority_node_id"] != "penguin"
            or not isinstance(policy["protected_roots"], list)):
        raise SnapshotError("private recovery hold identity/schema changed")
    for name in policy["protected_roots"]:
        relative = _safe_relative_path(name, "explicit recovery root")
        if len(relative.parts) != 2 or relative.parts[0] not in {"markets", "ablations"}:
            raise SnapshotError("private recovery hold must name an exact artifact root")
    return policy


def install_recovery_hold(relative_root: str) -> dict:
    """Authenticated private control only; never read a hold from Syncthing."""
    relative = _safe_relative_path(relative_root, "explicit recovery root")
    if len(relative.parts) != 2 or relative.parts[0] not in {"markets", "ablations"}:
        raise SnapshotError("recovery protection requires an exact artifact root")
    path = real(RECOVERY_HOLDS)
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    with (path.parent / "recovery-holds.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX)
        policy = _recovery_hold_policy()
        policy["protected_roots"] = sorted(set(policy["protected_roots"]) | {relative_root})
        atomic_write_json(path, policy)
        path.chmod(0o600)
    return {"state": "explicit_recovery_hold_installed", "relative_root": relative_root,
            "protected_roots": policy["protected_roots"], "source_deleted": False, "cold_deleted": False}


def recovery_hold_references(source: Path, repo_root: Path) -> list[str]:
    policy = _recovery_hold_policy()
    current = source.absolute()
    refs = []
    for relative in policy["protected_roots"]:
        protected = (repo_root / "artifacts" / relative).absolute()
        if current == protected or current in protected.parents or protected in current.parents:
            refs.append("explicit-recovery-hold:" + relative)
    return refs


def active_configuration_references(source: Path, repo_root: Path) -> list[str]:
    """Protect independent jobs even after they have closed their input fds."""
    import yaml
    from stockagent.data_sync.artifact_consumers import _strings
    result = recovery_hold_references(source, repo_root)
    for process in Path("/proc").glob("[0-9]*"):
        try:
            argv = (process / "cmdline").read_bytes().decode(errors="replace").split("\0")
            cwd = Path(os.readlink(process / "cwd"))
        except OSError:
            continue
        configurations = [argv[i+1] for i, a in enumerate(argv[:-1]) if a in {"--config", "--config-file"}]
        configurations += [a.partition("=")[2] for a in argv if a.startswith("--config=")]
        training = any(a.endswith("/train.py") or a == "train.py" for a in argv)
        for name in configurations:
            path = Path(name)
            if path.suffix.lower() not in {".yaml", ".yml", ".json"}:
                continue
            if not path.is_absolute():
                path = cwd / path
            try:
                documents = [yaml.safe_load(path.read_text())]
                if training:
                    from stockagent.config import load_config
                    documents.append(asdict(load_config(path)))
                for document in documents:
                    for field, value in _strings(document):
                        if not value.startswith("artifacts/") and not Path(value).is_absolute():
                            continue
                        paths = [Path(value)] if Path(value).is_absolute() else [cwd / value, repo_root / value]
                        if any(p.resolve() == source.resolve() or source.resolve() in p.resolve().parents
                               or p.resolve() in source.resolve().parents for p in paths):
                            result.append(f"pid={process.name}:active-config:{path}:{field}")
            except Exception as error:
                result.append(f"pid={process.name}:active-config-unreadable:{type(error).__name__}")
    return sorted(set(result))


def identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def dataset_name(relative: str) -> str:
    path = _safe_relative_path(relative, "remote legacy root")
    if len(path.parts) < 2 or path.parts[0] not in {"markets", "ablations"}:
        raise SnapshotError("remote legacy return is outside approved artifact scopes")
    return "legacy-vast-return-" + hashlib.sha256(relative.encode()).hexdigest()[:24]


def real(path: Path) -> Path:
    path = path.absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise SnapshotError("remote archive path is redirected")
    return path


def signature(info) -> list[int]:
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_mode, info.st_nlink]


def metadata_tree(source: Path) -> dict:
    """Read-only complete inventory, including shared blocks and empty dirs."""
    source = real(source)
    device = source.stat().st_dev
    rows, inodes = [], {}
    for parent, dirs, files in os.walk(source, followlinks=False):
        for name in sorted(dirs + files):
            path = Path(parent) / name
            info = path.lstat()
            kind = "file" if stat.S_ISREG(info.st_mode) else "directory" if stat.S_ISDIR(info.st_mode) else "unsupported"
            rows.append({"path": path.relative_to(source).as_posix(), "kind": kind,
                         "signature": signature(info), "cross_filesystem": info.st_dev != device})
            if kind == "file":
                key = (info.st_dev, info.st_ino)
                value = inodes.setdefault(key, [info.st_nlink, info.st_blocks * 512, 0])
                if value[:2] != [info.st_nlink, info.st_blocks * 512]:
                    raise SnapshotError("source inode mutated during inventory")
                value[2] += 1
    rows.sort(key=lambda r: r["path"])
    return {"rows": rows, "logical_bytes": sum(r["signature"][2] for r in rows if r["kind"] == "file"),
            "files": sum(r["kind"] == "file" for r in rows),
            "allocated_unique_file_bytes": sum(v[1] for v in inodes.values()),
            "reclaimable_allocated_file_bytes": sum(v[1] for v in inodes.values() if v[0] == v[2]),
            "fingerprint": identity({"rows": rows})}


def inventory_scopes(artifact_root: Path, include_roots: list[str] | None = None) -> dict:
    artifact_root = real(artifact_root)
    results = []
    for scope in ("markets", "ablations"):
        parent = real(artifact_root / scope)
        for source in sorted(parent.iterdir()):
            relative = source.relative_to(artifact_root).as_posix()
            if include_roots and relative not in include_roots:
                continue
            if source.is_symlink() or not source.is_dir():
                info = source.lstat()
                results.append({"relative_root": relative, "state": "non-directory-protected",
                                "bytes": info.st_size, "signature": signature(info)})
                continue
            result = metadata_tree(source)
            refs = artifact_process_references(source, parent)
            newest = max((r["signature"][3] for r in result["rows"] if r["kind"] == "file"), default=0)
            results.append({"relative_root": relative, "state": "inventoried", **result,
                            "newest_mtime_ns": newest, "process_references": refs})
    return {"schema_version": 1, "origin_node_id": "vastai1T", "authority_node_id": "penguin",
            "all_scopes_inventoried": not bool(include_roots),
            "captured_at_epoch": time.time(), "artifact_root": str(artifact_root), "items": results}


def acknowledgement_age(verified_at_epoch: float, maximum_age_seconds: float) -> float:
    """Wait for a slightly slower origin clock; never accept a future proof.

    The authenticated authority proof keeps its original timestamp and expiry.
    A five-second monotonic wait bound handles small clock offsets without
    extending either proof's validity window or changing either host's clock.
    Larger offsets, stalled/rolled-back clocks and expired proofs fail closed.
    Call only after the acknowledgement identity and authority are validated.
    """
    if (isinstance(verified_at_epoch, bool) or not isinstance(verified_at_epoch, (int, float))
            or not math.isfinite(verified_at_epoch)):
        raise SnapshotError("invalid or stale D acknowledgement timestamp")
    age = time.time() - verified_at_epoch
    deadline = time.monotonic() + 5
    while -5 <= age < 0 and time.monotonic() < deadline:
        time.sleep(min(0.05, -age))
        age = time.time() - verified_at_epoch
    if not 0 <= age <= maximum_age_seconds:
        raise SnapshotError("invalid or stale D acknowledgement; clock did not meet original expiry")
    return age


def retirement_plan(artifact_root: Path, repo_root: Path, ack: dict) -> dict:
    body = {k: v for k, v in ack.items() if k != "identity_sha256"}
    if (ack.get("contract") != "d_verified_remote_legacy_return_v1"
        or identity(body) != ack.get("identity_sha256") or ack.get("cold_verified") is not True
        or ack.get("origin_node_id") != "vastai1T" or ack.get("authority_node_id") != "penguin"
        or ack.get("dataset") != dataset_name(ack.get("relative_root", ""))):
        raise SnapshotError("invalid or stale exact D archive acknowledgement")
    acknowledgement_age(ack.get("verified_at_epoch", 0), 300)
    root = real(artifact_root)
    source = real(root / ack["relative_root"])
    archive = ack["archive_manifest"]
    if (archive.get("dataset") != ack["dataset"] or archive.get("relative_root") != ack["relative_root"]
        or archive.get("deployable") is not False or archive.get("completion_claim") != "not_checked"
        or not isinstance(archive.get("directories"), list)):
        raise SnapshotError("archive does not preserve this complete original tree")
    observed = metadata_tree(source)
    expected_files = {r["path"]: r for r in archive["files"]}
    file_rows = [r for r in observed["rows"] if r["kind"] == "file"]
    if {r["path"] for r in file_rows} != set(expected_files):
        raise SnapshotError("remote source has extra or missing archived files")
    if _archive_directories(source) != archive["directories"]:
        raise SnapshotError("remote source directory metadata differs from its archive")
    blockers = []
    if any(r["kind"] == "unsupported" or r["cross_filesystem"] for r in observed["rows"]):
        blockers.append("unsupported-or-cross-filesystem-entry")
    if any(r["signature"][6] != 1 for r in file_rows):
        blockers.append("shared-inode-requires-separate-audit")
    for row in file_rows:
        path = source / row["path"]
        expected = expected_files[row["path"]]
        info = path.lstat()
        if (signature(info) != row["signature"] or info.st_size != expected["source"]["size"]
            or info.st_mtime_ns != expected["source"]["mtime_ns"]
            or stat.S_IMODE(info.st_mode) != expected["source"]["mode"]
            or sha256_file(path) != expected["original_sha256"]
            or signature(path.lstat()) != row["signature"]):
            raise SnapshotError("remote source bytes or metadata differ from the D archive")
    if metadata_tree(source) != observed:
        raise SnapshotError("remote source mutated during exact recovery comparison")
    scope = root / PurePosixPath(ack["relative_root"]).parts[0]
    refs = artifact_process_references(source, scope) + active_configuration_references(source, repo_root)
    if refs:
        blockers.append("active-process-reference")
    # Conflicted config.py or invalid live service configs MUST block deletion.
    try:
        services = artifact_service_references([source], repo_root)[str(source)]
    except Exception as error:
        services = ["consumer-gate-unavailable:" + type(error).__name__]
        blockers.append("consumer-configuration-unreadable")
    if services:
        blockers.append("service-dependency")
    if ack["snapshot_id"] in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized")):
        blockers.append("pinned-release")
    resolved = resolve_packed_snapshot_id(Path("/srv/stockagent-packed"), ack["dataset"],
                                          ack["snapshot_id"], require_objects=False)
    if resolved.manifest_sha256 != ack["manifest_sha256"]:
        raise SnapshotError("returned cold manifest was not delivered exactly to the origin")
    metadata = resolved.manifest.get("metadata", {})
    if (metadata.get("legacy_manifest_sha256") != ack["legacy_manifest_sha256"]
        or metadata.get("transport_role") != "legacy-quarantine-archive"
        or metadata.get("source_relative_root") != ack["relative_root"]
        or metadata.get("deployable") != "false"):
        raise SnapshotError("cold release is not the exact approved legacy archive")
    _validate_inventory(resolved.manifest, _load_inventory(Path("/srv/stockagent-packed"), resolved.manifest))
    from scripts.configure_artifact_ingress_syncthing import credentials
    from scripts.manage_packed_edge import _convergence
    base, key = credentials()
    convergence = _convergence(base, key, "stockagent-packed", "penguin")
    if convergence.get("ok") is not True:
        blockers.append("packed-peer-not-converged")
    result = {"source": str(source), "relative_root": ack["relative_root"],
              "rows": observed["rows"], "ack_identity_sha256": ack["identity_sha256"],
              "reclaimable_allocated_file_bytes": observed["reclaimable_allocated_file_bytes"],
              "blockers": sorted(set(blockers)), "process_references": refs, "service_references": services,
              "convergence": convergence}
    result["fingerprint"] = identity({k: v for k, v in result.items() if k != "convergence"})
    return result


def retire(artifact_root: Path, repo_root: Path, state_root: Path, ack: dict, *, apply: bool) -> dict:
    state_root = real(state_root)
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = real(state_root / (ack["dataset"] + ".lock"))
    with lock.open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan = retirement_plan(artifact_root, repo_root, ack)
        atomic_write_json(state_root / (ack["dataset"] + "-plan.json"), plan)
        if not apply or plan["blockers"]:
            return {"state": "retirement-blocked" if plan["blockers"] else "would-retire",
                    "deleted": False, "blockers": plan["blockers"], "plan_fingerprint": plan["fingerprint"]}
        repeated = retirement_plan(artifact_root, repo_root, ack)
        if repeated["fingerprint"] != plan["fingerprint"] or repeated["blockers"]:
            raise SnapshotError("retirement dry run changed; source preserved")
        source = Path(plan["source"])
        quarantine = real(state_root / ("quarantine-" + uuid.uuid4().hex))
        if source.stat().st_dev != state_root.stat().st_dev:
            raise SnapshotError("retirement quarantine crosses a filesystem")
        journal = state_root / (ack["dataset"] + "-retirement.json")
        atomic_write_json(journal, {"state": "prepared", "plan": plan, "ack": ack, "quarantine": str(quarantine)})
        source.rename(quarantine)
        atomic_write_json(journal, {"state": "quarantined", "plan": plan, "ack": ack, "quarantine": str(quarantine)})
        current = metadata_tree(quarantine)
        if current["rows"] != plan["rows"] or process_references(quarantine) or artifact_process_references(source, artifact_root):
            raise SnapshotError("quarantined artifact changed or is active; quarantine retained")
        originals = {r["path"]: r["original_sha256"] for r in ack["archive_manifest"]["files"]}
        for row in current["rows"]:
            if row["kind"] == "file":
                path = quarantine / row["path"]
                if signature(path.lstat()) != row["signature"] or sha256_file(path) != originals[row["path"]]:
                    raise SnapshotError("quarantined original differs; retain for audit")
        from scripts.configure_artifact_ingress_syncthing import credentials
        from scripts.manage_packed_edge import _convergence
        base, key = credentials()
        if (_convergence(base, key, "stockagent-packed", "penguin").get("ok") is not True
            or ack["snapshot_id"] in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized"))
            or artifact_service_references([source, quarantine], repo_root).get(str(source))
            or artifact_service_references([source, quarantine], repo_root).get(str(quarantine))
            or active_configuration_references(source, repo_root)
            or metadata_tree(quarantine)["rows"] != plan["rows"] or process_references(quarantine)):
            raise SnapshotError("post-quarantine recovery/consumer/transport gate failed; retained")
        for row in current["rows"]:
            if row["kind"] == "file":
                path = quarantine / row["path"]
                if signature(path.lstat()) != row["signature"]:
                    raise SnapshotError("file mutated before unlink; retain quarantine")
                path.unlink()
        for row in sorted(current["rows"], key=lambda r: len(PurePosixPath(r["path"]).parts), reverse=True):
            if row["kind"] == "directory":
                (quarantine / row["path"]).rmdir()
        quarantine.rmdir()
        result = {"state": "exact-d-backed-source-retired", "deleted": True,
                  "reclaimed_allocated_bytes": plan["reclaimable_allocated_file_bytes"],
                  "snapshot_id": ack["snapshot_id"], "manifest_sha256": ack["manifest_sha256"],
                  "cold_deleted": False}
        atomic_write_json(journal, {"state": "retired", "plan": plan, "ack": ack, "result": result})
        return result
