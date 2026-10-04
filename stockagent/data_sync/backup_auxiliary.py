"""Closed working-tree and same-MVCC control backup for the NAS relay.

Reuse the canonical code identity and control-state contracts. Private runtime
configuration, ignored files, credentials, raw local-only sources and the live
database directory are not transport inputs.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import zipfile

from scripts.verify_backup_delivery import CONTRACT, verify
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes
from stockagent.data_sync.offhost_backup import _regular, _sha, private_file, private_json
from stockagent.data_sync.packed_backup import signature
from stockagent.runtime_identity import identity_sha256, source_identity, stable_source_sha256

CONTROL = "portable_same_mvcc_control_backup_v1"
CODE = "frozen_public_working_tree_backup_v1"
PRIVACY_FILTER_VERSION = 3
SENSITIVE_PATH = re.compile(r"(?:^|[/._-])(?:credentials?|password|secrets?|private|id_rsa|id_ed25519)(?:$|[/._-])", re.I)
SENSITIVE_SETTING = re.compile(
    r'''(?im)^\s*["']?(?:password|passwd|api[_-]?key|api[_-]?token|access[_-]?token|client[_-]?secret|secret[_-]?key|account_id)["']?\s*[:=]\s*([^\r\n,}]+)''')


def nonpublic_configuration(body: bytes) -> bool:
    sensitive_keys = {"password", "passwd", "apikey", "apitoken", "accesstoken", "refreshtoken",
                      "bearertoken", "clientsecret", "secretkey", "privatekey", "credentials"}
    def actual(value) -> bool:
        if not value:
            return False
        if not isinstance(value, str):
            return True
        value = value.strip().strip("\"'").strip()
        return bool(value and value.lower() not in {"null", "none", "false", "true", "{}", "[]"}
            and not any(x in value.lower() for x in ("${", "getenv", "environ", "env:", "<", "your_", "example", "placeholder")))
    def nested(value) -> bool:
        if isinstance(value, dict):
            return any((re.sub(r"[_-]", "", str(key).lower()) in sensitive_keys and actual(item))
                       or (str(key).lower() == "authorization" and isinstance(item, str)
                           and bool(re.match(r"(?i)^(?:Bearer|Basic|Token)\s+\S+", item)))
                       or nested(item) for key, item in value.items())
        if isinstance(value, list):
            return any(nested(item) for item in value)
        return False
    try:
        if nested(json.loads(body)):
            return True
    except (ValueError, UnicodeDecodeError):
        pass
    text = body.decode("utf-8", errors="replace")
    for match in SENSITIVE_SETTING.finditer(text):
        value = match[1].strip().strip("\"'").strip()
        if (not value or value.lower() in {"null", "none", "false", "true", "{}", "[]"}
                or any(x in value.lower() for x in ("${", "getenv", "environ", "env:", "<", "your_", "example", "placeholder"))):
            continue
        return True
    return False


def working_tree_inventory(root: Path, *, documentation: bool, configs: bool) -> dict:
    base = source_identity(root, allow_tracked_deletions=True)
    patterns = ["AGENTS.md", ".gitignore", "test"]
    if documentation:
        patterns.append("docs")
    if configs:
        patterns.append("configs")
    names = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", *patterns],
        cwd=root, capture_output=True, check=True).stdout.decode().split("\0")
    files = dict(base["files"])
    deleted = list(base["deleted_tracked_paths"])
    excluded = []
    for name in sorted(set(names) - {""}):
        path = root / name
        # Credential documentation/tests are source code. Private runtime
        # config stays excluded; public example configs still undergo the
        # value-based check below before they can enter a frozen snapshot.
        example_config = name.startswith("configs/") and ".example." in path.name
        if (name.startswith("configs/") and SENSITIVE_PATH.search(name) and not example_config) or path.name.startswith(".env"):
            excluded.append(name)
            continue
        if not path.exists():
            if path.is_symlink():
                raise SnapshotError("backup configuration has a broken symlink")
            deleted.append(name)
            continue
        digest = stable_source_sha256(root, name)
        if name.startswith("configs/") and path.suffix.lower() in {".json", ".yml", ".yaml", ".toml"}:
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != digest:
                raise SnapshotError("configuration changed during privacy classification")
            if nonpublic_configuration(raw):
                excluded.append(name)
                continue
        files[name] = digest
    body = {"contract": CODE, "git_head": base["git_head"], "files": dict(sorted(files.items())),
        "privacy_filter_version": PRIVACY_FILTER_VERSION,
        "deleted_tracked_paths": sorted(set(deleted)), "excluded_private_paths": excluded,
        "scope": "current code/public configs/docs, including uncommitted Git-visible work; no Git object database",
        "loaded_service_revision_verified": False, "ignored_private_files_included": False}
    return {**body, "identity_sha256": identity_sha256(body)}


def control_input(receipt_path: Path) -> tuple[dict, Path, Path]:
    receipt_path = private_file(receipt_path)
    receipt = json.loads(receipt_path.read_bytes())
    if receipt.get("state") != "backup_written" or receipt.get("same_mvcc_snapshot_as_dump") is not True:
        raise SnapshotError("control transport needs an exact exported MVCC snapshot")
    archive = private_file(Path(receipt["archive"]))
    state = private_file(Path(receipt["logical_state_file"]))
    if archive.parent != receipt_path.parent or state.parent != receipt_path.parent:
        raise SnapshotError("control objects are outside their private receipt directory")
    if _sha(archive) != receipt["sha256"] or _sha(state) != receipt["logical_state_file_sha256"]:
        raise SnapshotError("control objects differ from their consistent backup receipt")
    logical = json.loads(state.read_bytes())
    if (logical["identity_sha256"] != receipt["logical_state_identity_sha256"]
            or identity_sha256({k: logical[k] for k in ("contract", "tables")}) != logical["identity_sha256"]):
        raise SnapshotError("control logical state identity differs")
    portable = {"contract": CONTROL, "state": "backup_written", "database": "stockagent_control",
        "same_mvcc_snapshot_as_dump": True, "archive": "archive.backup", "logical_state_file": "logical-state.json",
        "sha256": receipt["sha256"], "logical_state_file_sha256": receipt["logical_state_file_sha256"],
        "logical_state_identity_sha256": receipt["logical_state_identity_sha256"],
        "source_receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
        "source_observed_at_utc": receipt["observed_at_utc"], "bytes": archive.stat().st_size,
        "control_database_restore_verified": False}
    return portable, archive, state


def export_auxiliary(root: Path, destination: Path, *, control_receipt: Path,
                     documentation: bool = True, configs: bool = True, maximum_bytes: int = 512 * 1024**2) -> dict:
    root = root.resolve(strict=True)
    destination = destination.absolute()
    if destination.exists() or any(p.is_symlink() for p in (destination, *destination.parents)):
        raise SnapshotError("auxiliary backup needs a fresh unredirected directory")
    tree = working_tree_inventory(root, documentation=documentation, configs=configs)
    control, archive, state = control_input(control_receipt)
    control_signatures = {p: signature(p) for p in (archive, state)}
    if sum((root / name).stat().st_size for name in tree["files"]) + archive.stat().st_size + state.stat().st_size > maximum_bytes:
        raise SnapshotError("auxiliary snapshot exceeds its explicit byte budget")
    destination.mkdir(mode=0o700, parents=True)
    zip_path = destination / "code/working-tree.zip"
    zip_path.parent.mkdir(mode=0o700)
    with zipfile.ZipFile(zip_path, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=3) as target:
        for name, digest in tree["files"].items():
            body = _regular(root / name).read_bytes()
            if hashlib.sha256(body).hexdigest() != digest:
                raise SnapshotError("code changed during its frozen working-tree backup")
            item = zipfile.ZipInfo(name)
            item.compress_type = zipfile.ZIP_DEFLATED
            item.external_attr = 0o100644 << 16
            target.writestr(item, body)
    zip_path.chmod(0o600)
    with zip_path.open("rb") as handle:
        os.fsync(handle.fileno())
    private_json(destination / "code/receipt.json", tree)
    control_dir = destination / "control"
    control_dir.mkdir(mode=0o700)
    for path, name in ((archive, "archive.backup"), (state, "logical-state.json")):
        target = control_dir / name
        shutil.copyfile(path, target)
        target.chmod(0o600)
        with target.open("rb") as handle:
            os.fsync(handle.fileno())
    private_json(control_dir / "receipt.json", control)
    if working_tree_inventory(root, documentation=documentation, configs=configs) != tree:
        raise SnapshotError("working tree changed during backup; READY withheld")
    if any(signature(p) != recorded for p, recorded in control_signatures.items()):
        raise SnapshotError("consistent control files changed during backup")
    verified_control(control_dir)
    verified_working_tree(destination / "code")
    files = []
    for path in sorted(destination.glob("*/*")):
        files.append({"relative": path.relative_to(destination).as_posix(), "sha256": _sha(path), "bytes": path.stat().st_size})
    content_identity = identity_sha256({"code": tree["identity_sha256"], "control": control})
    body = {"contract": CONTRACT, "source_plan_identity_sha256": content_identity, "files": files,
        "producer_node_id": "penguin", "source_scope": "frozen public working tree and same-MVCC control dump",
        "selection": "code_and_control_backup", "all_history_verified": False,
        "unpublished_sources_included": False, "complete_release_in_this_delivery": False}
    envelope = {**body, "identity_sha256": identity_sha256(body)}
    private_json(destination / "backup-envelope.json", envelope)
    verification = verify(destination, envelope["identity_sha256"], require_ready=False)
    atomic_write_bytes(destination / "READY", (envelope["identity_sha256"] + "\n").encode(), mode=0o600)
    return {"state": "closed_code_and_control_delivery_verified", "envelope_identity_sha256": envelope["identity_sha256"],
        "privacy_filter_version": PRIVACY_FILTER_VERSION,
        "transport": verification, "content_identity_sha256": content_identity,
        "code_identity_sha256": tree["identity_sha256"], "code_files": len(tree["files"]),
        "excluded_private_paths": tree["excluded_private_paths"], "control_archive_sha256": control["sha256"],
        "control_logical_identity_sha256": control["logical_state_identity_sha256"],
        "control_database_restore_verified": False, "durable_off_host_backup_verified": False,
        "observed_at_utc": datetime.now(timezone.utc).isoformat()}


def verified_control(root: Path) -> tuple[dict, Path, dict]:
    receipt = json.loads(_regular(root / "receipt.json").read_bytes())
    if (receipt.get("contract") != CONTROL or receipt.get("same_mvcc_snapshot_as_dump") is not True
            or receipt.get("archive") != "archive.backup" or receipt.get("logical_state_file") != "logical-state.json"):
        raise SnapshotError("unsupported portable control receipt")
    archive = _regular(root / "archive.backup")
    state = _regular(root / "logical-state.json")
    if _sha(archive) != receipt["sha256"] or _sha(state) != receipt["logical_state_file_sha256"]:
        raise SnapshotError("restored control bytes differ")
    logical = json.loads(state.read_bytes())
    if (logical["identity_sha256"] != receipt["logical_state_identity_sha256"]
            or identity_sha256({k: logical[k] for k in ("contract", "tables")}) != logical["identity_sha256"]):
        raise SnapshotError("restored control logical identity differs")
    return receipt, archive, logical


def verified_working_tree(root: Path) -> dict:
    tree = json.loads(_regular(root / "receipt.json").read_bytes())
    if tree.get("contract") != CODE or identity_sha256({k: v for k, v in tree.items() if k != "identity_sha256"}) != tree.get("identity_sha256"):
        raise SnapshotError("working-tree receipt identity differs")
    with zipfile.ZipFile(_regular(root / "working-tree.zip")) as archive:
        if set(archive.namelist()) != set(tree["files"]) or len(archive.namelist()) != len(tree["files"]):
            raise SnapshotError("working-tree archive file set differs")
        if archive.testzip() is not None:
            raise SnapshotError("working-tree archive CRC differs")
        for name, digest in tree["files"].items():
            if hashlib.sha256(archive.read(name)).hexdigest() != digest:
                raise SnapshotError("working-tree restored file SHA differs")
    return {"state": "frozen_working_tree_files_verified", "identity_sha256": tree["identity_sha256"],
        "files_verified": len(tree["files"]), "loaded_service_revision_verified": False}
