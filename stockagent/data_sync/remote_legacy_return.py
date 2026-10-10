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
from collections import defaultdict

from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path, atomic_write_json, sha256_file
from stockagent.data_sync.legacy_artifact_archive import (
    LegacyArchiveSpec, _archive_directories, source_plan, MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT,
    reviewed_vast_offline_root,
)
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids, process_references
from stockagent.data_sync.packed_snapshots import _load_inventory, _validate_inventory, resolve_packed_snapshot_id
from stockagent.data_sync.training_return import shared_inode_references, training_hot_retention

RECOVERY_HOLDS = Path("/var/lib/stockagent-legacy-return/recovery-holds.json")
_ACTIVE_CONFIG_VERSIONS = {}


def _active_configuration(path: Path, cwd: Path, argv: list[str]):
    """Resolve using the actual selected trainer when checkout schemas differ."""
    from stockagent.config import load_config
    try:
        signatures = {}
        return asdict(load_config(path, source_signatures=signatures)), list(signatures)
    except (ValueError, KeyError, TypeError):
        # A new frozen experiment can be valid while this control checkout
        # predates its model fields. Never strip unknown fields or weaken the
        # consumer gate. Use its canonical loader in a fresh CPU process.
        train_entry = next((a for a in argv if a == "train.py" or a.endswith("/train.py")), None)
        if train_entry is None:
            raise
        entry = Path(train_entry)
        root = (entry if entry.is_absolute() else cwd / entry).resolve().parent
        loader = root / 'stockagent/config.py'
        if not loader.is_file() or root == Path(__file__).resolve().parents[2]:
            raise
        key = (str(root), str(path.resolve()))
        cached = _ACTIVE_CONFIG_VERSIONS.get(key)
        if cached and all(p.is_file() and sha256_file(p) == digest for p,digest in cached['proofs'].items()):
            return cached['configuration'], cached['sources']
        import subprocess, sys
        program = '''
import sys,json,hashlib
from pathlib import Path
from dataclasses import asdict
sys.path.insert(0,str(Path.cwd()))
import stockagent.config as module
assert Path(module.__file__).resolve()==Path.cwd()/'stockagent/config.py'
sources={}
config=module.load_config(sys.argv[1],source_signatures=sources)
proofs={}
for p,signature in sources.items():
 assert module._config_source_identity(p)==signature, 'config changed after admission'
 proofs[str(p)]=hashlib.sha256(p.read_bytes()).hexdigest()
 assert module._config_source_identity(p)==signature, 'config changed while hashing'
proofs[str(Path(module.__file__).resolve())]=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
print(json.dumps({'configuration':asdict(config),'sources':[str(p) for p in sources],'proofs':proofs},default=str))
'''
        run = subprocess.run([sys.executable, '-c', program, str(path)], cwd=root,
                             env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},
                             capture_output=True, text=True, timeout=30, check=True)
        result = json.loads(run.stdout)
        source_paths = [Path(p) for p in result['sources']]
        proofs = {Path(p):digest for p,digest in result['proofs'].items()}
        if not all(p.is_file() and sha256_file(p) == digest for p,digest in proofs.items()):
            raise SnapshotError('selected active configuration changed during observation')
        _ACTIVE_CONFIG_VERSIONS[key] = {'configuration':result['configuration'],
                                       'sources':source_paths,'proofs':proofs}
        return result['configuration'], source_paths


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


def active_configuration_references_many(sources, repo_root: Path) -> dict[str, list[str]]:
    """Resolve each active configuration once for a read-only selected cohort."""
    import yaml
    from stockagent.data_sync.artifact_consumers import _strings, is_repository_data_path
    sources = tuple(source.resolve() for source in sources)
    result = {str(source): recovery_hold_references(source, repo_root) for source in sources}
    for process in Path("/proc").glob("[0-9]*"):
        try:
            argv = (process / "cmdline").read_bytes().decode(errors="replace").split("\0")
            cwd = Path(os.readlink(process / "cwd"))
        except OSError:
            continue
        # Operational cache/code inputs remain dependencies after the last fd
        # closes. Inspect only path variables, never credentials or arbitrary
        # environment values. The broad checkout path is not an artifact lease.
        try:
            environment = (process / "environ").read_bytes().decode(errors="replace").split("\0")
        except OSError:
            environment = []
        for item in environment:
            name, sep, value = item.partition("=")
            if not sep or name == "STOCKAGENT_REPO_ROOT" or not (
                name.startswith("STOCKAGENT_") or name in {"PYTHONPATH", "LD_LIBRARY_PATH", "TORCHINDUCTOR_CACHE_DIR", "TRITON_CACHE_DIR"}
            ):
                continue
            for component in value.split(":"):
                if not is_repository_data_path(component):
                    continue
                path = Path(component).expanduser()
                if not path.is_absolute():
                    path = cwd / path
                path = path.resolve()
                if path in {repo_root, repo_root / "artifacts"}:
                    continue
                for source in sources:
                    if path == source or source in path.parents or path in source.parents:
                        result[str(source)].append(f"pid={process.name}:runtime-env:{name}:{path}")
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
                    configuration, sources_used = _active_configuration(path, cwd, argv)
                    documents.append(configuration)
                    # A config's base chain is a runtime/resume dependency,
                    # even when resolved dataclasses omit inheritance keys.
                    for configured_source in sources_used:
                        inherited = Path(configured_source).resolve()
                        for source in sources:
                            if inherited == source or source in inherited.parents or inherited in source.parents:
                                result[str(source)].append(f"pid={process.name}:active-config-source:{inherited}")
                for document in documents:
                    for field, value in _strings(document):
                        if not is_repository_data_path(value):
                            continue
                        paths = [Path(value)] if Path(value).is_absolute() else [cwd / value, repo_root / value]
                        resolved_paths = tuple(p.resolve() for p in paths)
                        for source in sources:
                            if any(p == source or source in p.parents or p in source.parents for p in resolved_paths):
                                result[str(source)].append(f"pid={process.name}:active-config:{path}:{field}")
            except Exception as error:
                for references in result.values():
                    references.append(f"pid={process.name}:active-config-unreadable:{type(error).__name__}")
    return {source: sorted(set(references)) for source, references in result.items()}


def active_configuration_references(source: Path, repo_root: Path) -> list[str]:
    """Protect independent jobs even after they have closed their input fds."""
    return active_configuration_references_many((source,), repo_root)[str(source.resolve())]


def identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def dataset_name(relative: str) -> str:
    path = _safe_relative_path(relative, "remote legacy root")
    if (len(path.parts) < 2 or path.parts[0] not in {"markets", "ablations"}) and not reviewed_vast_offline_root(relative):
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


def verify_original_inode(path, row, expected_sha256, verified_inodes, *, hash_file=None):
    key = tuple(row["signature"])
    actual = verified_inodes.get(key)
    if actual is None:
        actual = (hash_file or sha256_file)(path)
        verified_inodes[key] = actual
    if actual != expected_sha256 or signature(path.lstat()) != row["signature"]:
        raise SnapshotError("original bytes changed or differ from verified D decode")


def unlink_preserved_file_names(root, rows):
    """Unlink only caller-verified names and account for the last inode link."""
    groups = defaultdict(list)
    for row in rows:
        if row["kind"] == "file":
            groups[tuple(row["signature"][:2])].append(row)
    reclaimed = 0
    for members in groups.values():
        first = root / members[0]["path"]
        info = first.lstat()
        if signature(info) != members[0]["signature"]:
            raise SnapshotError("inode changed before unlink; remaining quarantine retained")
        blocks = info.st_blocks * 512
        with first.open("rb") as handle:
            latest = signature(os.fstat(handle.fileno()))
            for row in members:
                path = root / row["path"]
                if signature(path.lstat()) != latest:
                    raise SnapshotError("inode mutated during retirement; remaining names retained")
                path.unlink()
                after = signature(os.fstat(handle.fileno()))
                if after[:4] != latest[:4] or after[5] != latest[5] or after[6] != latest[6] - 1:
                    raise SnapshotError("inode changed during unlink; remaining names retained")
                latest = after
            freed = latest[6] == 0
        if freed:
            reclaimed += blocks
    return reclaimed


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


def inventory_scopes(artifact_root: Path, include_roots: list[str] | None = None,
                     *, capture_contract: str | None = None) -> dict:
    artifact_root = real(artifact_root)
    scopes = ("markets", "ablations")
    if capture_contract is not None:
        if (capture_contract != MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT or not include_roots
                or len(set(include_roots)) != len(include_roots)
                or any(not reviewed_vast_offline_root(root) for root in include_roots)):
            raise SnapshotError("offline inventory requires the exact manually reviewed root list")
        scopes = tuple(sorted({PurePosixPath(root).parts[0] for root in include_roots}))
    elif include_roots and any(PurePosixPath(root).parts[0] not in scopes for root in include_roots):
        raise SnapshotError("default inventory excludes unregistered artifact namespaces")
    results = []
    for scope in scopes:
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
    if capture_contract is not None and {row["relative_root"] for row in results} != set(include_roots):
        raise SnapshotError("reviewed offline root disappeared before fixed inventory")
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
    version = ack.get("contract")
    if (version not in {"d_verified_remote_legacy_return_v1", "d_verified_remote_legacy_return_v2",
                        "d_verified_remote_legacy_return_v3"}
        or identity(body) != ack.get("identity_sha256") or ack.get("cold_verified") is not True
        or ack.get("origin_node_id") != "vastai1T" or ack.get("authority_node_id") != "penguin"
        or ack.get("dataset") != dataset_name(ack.get("relative_root", ""))):
        raise SnapshotError("invalid or stale exact D archive acknowledgement")
    shared_policy = ack.get("shared_file_policy", "reject_unknown_names")
    offline = reviewed_vast_offline_root(ack["relative_root"])
    if (version == "d_verified_remote_legacy_return_v3" and (
            not offline or ack.get("capture_contract") != MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT)
            or version != "d_verified_remote_legacy_return_v3" and (offline or "capture_contract" in ack)):
        raise SnapshotError("offline artifact names require the explicit versioned capture acknowledgement")
    if (version in {"d_verified_remote_legacy_return_v2", "d_verified_remote_legacy_return_v3"}
            and shared_policy != "unlink_preserved_names_only"
            or version == "d_verified_remote_legacy_return_v1" and "shared_file_policy" in ack):
        raise SnapshotError("shared-name policy does not match the acknowledgement version")
    acknowledgement_age(ack.get("verified_at_epoch", 0), 300 if version.endswith("_v1") else 1800)
    root = real(artifact_root)
    source = real(root / ack["relative_root"])
    archive = ack["archive_manifest"]
    if offline and archive.get("capture_contract") != MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT:
        raise SnapshotError("cold manifest does not preserve the reviewed offline capture contract")
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
    retention = training_hot_retention(source, observed["rows"])
    blockers += retention["blockers"]
    if any(r["kind"] == "unsupported" or r["cross_filesystem"] for r in observed["rows"]):
        blockers.append("unsupported-or-cross-filesystem-entry")
    if version.endswith("_v1") and any(r["signature"][6] != 1 for r in file_rows):
        blockers.append("shared-inode-requires-separate-audit")
    inode_refs = shared_inode_references(observed)
    if inode_refs:
        blockers.append("shared-inode-in-use")
    verified_inodes = {}
    for row in file_rows:
        path = source / row["path"]
        expected = expected_files[row["path"]]
        info = path.lstat()
        if (signature(info) != row["signature"] or info.st_size != expected["source"]["size"]
            or info.st_mtime_ns != expected["source"]["mtime_ns"]
            or stat.S_IMODE(info.st_mode) != expected["source"]["mode"]
            or signature(path.lstat()) != row["signature"]):
            raise SnapshotError("remote source bytes or metadata differ from the D archive")
        verify_original_inode(path, row, expected["original_sha256"], verified_inodes)
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
              "shared_file_policy": shared_policy, "inode_references": inode_refs,
              "hot_retention": retention,
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
        if (current["rows"] != plan["rows"] or process_references(quarantine)
                or artifact_process_references(source, artifact_root) or shared_inode_references(current)):
            raise SnapshotError("quarantined artifact changed or is active; quarantine retained")
        originals = {r["path"]: r["original_sha256"] for r in ack["archive_manifest"]["files"]}
        verified_inodes = {}
        for row in current["rows"]:
            if row["kind"] == "file":
                path = quarantine / row["path"]
                if signature(path.lstat()) != row["signature"]:
                    raise SnapshotError("quarantined original differs; retain for audit")
                verify_original_inode(path, row, originals[row["path"]], verified_inodes)
        from scripts.configure_artifact_ingress_syncthing import credentials
        from scripts.manage_packed_edge import _convergence
        base, key = credentials()
        if (_convergence(base, key, "stockagent-packed", "penguin").get("ok") is not True
            or ack["snapshot_id"] in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized"))
            or artifact_service_references([source, quarantine], repo_root).get(str(source))
            or artifact_service_references([source, quarantine], repo_root).get(str(quarantine))
            or active_configuration_references(source, repo_root)
            or training_hot_retention(quarantine, current["rows"])["blockers"]
            or metadata_tree(quarantine)["rows"] != plan["rows"] or process_references(quarantine)
            or shared_inode_references(current)):
            raise SnapshotError("post-quarantine recovery/consumer/transport gate failed; retained")
        acknowledgement_age(ack.get("verified_at_epoch", 0), 300 if ack["contract"].endswith("_v1") else 1800)
        reclaimed = unlink_preserved_file_names(quarantine, current["rows"])
        for row in sorted(current["rows"], key=lambda r: len(PurePosixPath(r["path"]).parts), reverse=True):
            if row["kind"] == "directory":
                (quarantine / row["path"]).rmdir()
        quarantine.rmdir()
        result = {"state": "exact-d-backed-source-retired", "deleted": True,
                  "reclaimed_allocated_bytes": reclaimed,
                  "snapshot_id": ack["snapshot_id"], "manifest_sha256": ack["manifest_sha256"],
                  "cold_deleted": False, "shared_file_policy": plan["shared_file_policy"],
                  "external_shared_names_deleted": False}
        atomic_write_json(journal, {"state": "retired", "plan": plan, "ack": ack, "result": result})
        return result
