#!/usr/bin/env python3
"""Install the source backup role without changing the canonical cold store.

Retains the verified small WSL deliveries, copies them to dedicated D: staging,
then mounts that staging at the same Syncthing path. Private credentials and
device identities are never copied, logged or modified.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.configure_artifact_ingress_syncthing import credentials, request  # noqa: E402
from scripts.configure_backup_receipts_syncthing import run as configure_receipts  # noqa: E402
from scripts.verify_backup_delivery import verify  # noqa: E402
from stockagent.data_sync.desync_snapshots import atomic_write_bytes  # noqa: E402
from stockagent.data_sync.offhost_backup import _regular, _sha, private_json  # noqa: E402
from stockagent.data_sync.packed_backup import signature  # noqa: E402
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock  # noqa: E402


def execute(argv: list[str]) -> str:
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError("backup role operation failed; no credentials printed")
    return result.stdout


def inventory(root: Path) -> dict:
    result = {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        if any((Path(directory) / d).is_symlink() for d in dirs):
            raise ValueError("backup transport contains a redirected directory")
        for name in files:
            path = _regular(Path(directory) / name)
            relative = path.relative_to(root).as_posix()
            before = signature(path)
            digest = _sha(path)
            if before != signature(path):
                raise ValueError("backup transport changed during migration capture")
            result[relative] = {"sha256": digest, "bytes": before[2], "signature": list(before)}
    return result


def install(evidence: Path, config: dict, role: Path) -> dict:
    if os.geteuid() != 0:
        raise ValueError("source mount and systemd installation require the existing root owner")
    if evidence.exists():
        raise ValueError("preserve previous source installation evidence")
    evidence.mkdir(mode=0o700, parents=True)
    native_before = runtime_identity()
    role = role.resolve(strict=True)
    if not (role / "conda-meta").is_dir() or not (role / "bin/restic").is_file():
        raise ValueError("reuse the accepted isolated Mamba backup role")
    role_runtime = json.loads(execute([str(role / "bin/python"), "-c",
        "import json; from stockagent.runtime_identity import runtime_identity; print(json.dumps(runtime_identity()))"]))
    source = Path(config["transport_root"])
    backing = Path("/srv/stockagent-d-volume/stockagent-backup-ingress-lab203")
    execute(["bash", str(ROOT / "scripts/mount_packed_d_cold.sh"), "--check"])
    if source.is_symlink() or backing.exists() or backing.is_symlink():
        raise ValueError("source migration needs an existing unredirected ingress and fresh D staging")
    if shutil.disk_usage(backing.parent).free < config["reserve_bytes"] + 700 * 1024**3:
        raise ValueError("source D staging cannot budget this retained-cold initial backup")
    base, key = credentials()
    folder_id = "stockagent-backup-ingress-lab203"
    folder = request(base, key, "/rest/config/folders/" + folder_id)
    if (folder["path"] != str(source) or folder["type"] != "sendonly" or folder["paused"]
            or {d["deviceID"] for d in folder["devices"]} != {config["producer_device_id"], config["receiver_device_id"]}):
        raise ValueError("existing ingress disagrees with the tested paired sender")
    prior_other_folders = {x["id"]: x for x in request(base, key, "/rest/config/folders") if x["id"] != folder_id}
    started = time.perf_counter()
    state = Path(config["state_root"])
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (state / "owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        request(base, key, "/rest/config/folders/" + folder_id, method="PATCH", payload={"paused": True})
        transport_ready = False
        try:
            before = inventory(source)
            for batch in sorted(source.glob("batch-*")):
                verify(batch, batch.name.removeprefix("batch-"))
            backing.mkdir(mode=0o700)
            for directory, dirs, files in os.walk(source):
                target_directory = backing / Path(directory).relative_to(source)
                target_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
                for name in dirs:
                    (target_directory / name).mkdir(mode=0o700, exist_ok=True)
                for name in files:
                    origin = Path(directory) / name
                    target = target_directory / name
                    shutil.copyfile(origin, target)
                    target.chmod(0o600)
                    with target.open("rb") as handle:
                        os.fsync(handle.fileno())
            after = inventory(source)
            copied = inventory(backing)
            if before != after or {k: (v["sha256"], v["bytes"]) for k, v in before.items()} != {
                    k: (v["sha256"], v["bytes"]) for k, v in copied.items()}:
                raise ValueError("source or copied D transport differs from its exact file inventory")
            for batch in sorted(backing.glob("batch-*")):
                verify(batch, batch.name.removeprefix("batch-"))
            private_json(evidence / "pre-migration-file-sha256.json", before)
            private_json(evidence / "D-copy-file-sha256.json", copied)
            atomic_write_bytes(backing / ".transport-owner.json", (ROOT / "configs/data_sync/backup_transport_volume.json").read_bytes(), mode=0o600)
            ignore = backing / ".stignore"
            old_ignore = ignore.read_text() if ignore.exists() else ""
            if old_ignore and not old_ignore.endswith("\n"):
                old_ignore += "\n"
            atomic_write_bytes(ignore, (old_ignore + "(?d).staging\n(?d).staging/**\n").encode(), mode=0o600)
            private_json(state / "runtime-lock.json", role_runtime)
            env_file = Path("/etc/stockagent/backup-stream.env")
            if env_file.exists():
                raise ValueError("preserve any existing source backup environment")
            atomic_write_bytes(env_file, ("FINTECH_ENV_PATH=" + shlex.quote(str(role)) + "\n"
                "STOCKAGENT_BACKUP_RUNTIME_LOCK=" + shlex.quote(str(state / "runtime-lock.json")) + "\n").encode(), mode=0o600)
            for name in ("stockagent-backup-transport-mount.service", "stockagent-backup-stream.service", "stockagent-backup-stream.timer"):
                target = Path("/etc/systemd/system") / name
                if target.exists():
                    raise ValueError("preserve an existing source backup unit")
                body = (ROOT / "deploy/systemd" / (name + ".in")).read_text()
                body = body.replace("__REPO_ROOT__", str(ROOT)).replace("__BACKUP_ENV_FILE__", str(env_file))
                atomic_write_bytes(target, body.encode())
            dropin = Path("/etc/systemd/system/syncthing@root.service.d/backup-transport.conf")
            if dropin.exists():
                raise ValueError("preserve an existing Syncthing backup dependency")
            atomic_write_bytes(dropin, (ROOT / "deploy/systemd/syncthing-root-backup-transport.conf.in").read_bytes())
            execute(["systemctl", "daemon-reload"])
            execute(["systemctl", "enable", "--now", "stockagent-backup-transport-mount.service"])
            execute(["bash", str(ROOT / "scripts/mount_backup_transport.sh"), "--check"])
            for relative, row in before.items():
                path = source / relative
                if path.stat().st_size != row["bytes"] or _sha(path) != row["sha256"]:
                    raise ValueError("post-bind transport content differs")
            transport_ready = True
        finally:
            # Do not reopen a configured D-dependent folder on the underlying
            # C path after a failed mount. A pre-install failure remains safe.
            if transport_ready or not Path("/etc/systemd/system/syncthing@root.service.d/backup-transport.conf").exists():
                request(base, key, "/rest/config/folders/" + folder_id, method="PATCH", payload={"paused": False})
        request(base, key, "/rest/db/scan", {"folder": folder_id}, method="POST")
        for name, value in prior_other_folders.items():
            if request(base, key, "/rest/config/folders/" + name) != value:
                raise ValueError("an unrelated Syncthing folder changed")
        receipts = configure_receipts(json.loads((ROOT / "configs/data_sync/backup_receipts.json").read_bytes()), "producer", "configure")
        private_json(evidence / "receipt-return-channel.json", receipts)
        execute(["systemctl", "enable", "--now", "stockagent-backup-stream.timer"])
    native_after = runtime_identity()
    if validate_runtime_lock(native_before, native_after):
        raise ValueError("native source runtime changed during backup installation")
    result = {"state": "source_backup_stream_installed_waiting_relay_adapter", "source_role": str(role),
        "mamba_managed": True, "runtime_lock_sha256": role_runtime["sha256"], "native_runtime_unchanged": True,
        "transport_backing": str(backing), "transport_path_unchanged": True,
        "copied_files": len(before), "copied_bytes": sum(r["bytes"] for r in before.values()),
        "source_C_deliveries_retained": True, "canonical_objects_changed": False,
        "unrelated_syncthing_folders_preserved": True, "relay_adapter_verified": False,
        "complete_installation_seconds": time.perf_counter() - started,
        "observed_at_utc": datetime.now(timezone.utc).isoformat()}
    private_json(evidence / "installation.json", result)
    return result


def resume(evidence: Path, config: dict, role: Path) -> dict:
    """Recheck a retained migration intent; never recopy/replace previous proof."""
    before = runtime_identity()
    destination = evidence / "installation-resume.json"
    if destination.exists():
        raise ValueError("preserve the previous installation-resume proof")
    captured = json.loads((evidence / "pre-migration-file-sha256.json").read_bytes())
    expected_runtime = json.loads((Path(config["state_root"]) / "runtime-lock.json").read_bytes())
    actual_runtime = json.loads(execute([str(role.resolve() / "bin/python"), "-c",
        "import json; from stockagent.runtime_identity import runtime_identity; print(json.dumps(runtime_identity()))"]))
    if validate_runtime_lock(expected_runtime, actual_runtime):
        raise ValueError("preserve the accepted backup role runtime")
    source = Path(config["transport_root"])
    execute(["systemctl", "start", "stockagent-backup-transport-mount.service"])
    execute(["bash", str(ROOT / "scripts/mount_backup_transport.sh"), "--check"])
    for relative, row in captured.items():
        path = _regular(source / relative)
        if path.stat().st_size != row["bytes"] or _sha(path) != row["sha256"]:
            raise ValueError("resumed D-bound delivery differs from its original fixed file inventory")
    for batch in source.glob("batch-*"):
        verify(batch, batch.name.removeprefix("batch-"))
    execute(["systemctl", "start", "syncthing@root.service"])
    base, key = credentials()
    folder_id = "stockagent-backup-ingress-lab203"
    folder = request(base, key, "/rest/config/folders/" + folder_id)
    if folder["path"] != str(source) or folder["type"] != "sendonly":
        raise ValueError("resumed sender folder identity differs")
    request(base, key, "/rest/config/folders/" + folder_id, method="PATCH", payload={"paused": False})
    receipts = configure_receipts(json.loads((ROOT / "configs/data_sync/backup_receipts.json").read_bytes()), "producer", "configure")
    request(base, key, "/rest/db/scan", {"folder": folder_id}, method="POST", timeout=180)
    execute(["systemd-analyze", "verify", *["/etc/systemd/system/" + name for name in (
        "stockagent-backup-transport-mount.service", "stockagent-backup-stream.service", "stockagent-backup-stream.timer")]])
    execute(["systemctl", "enable", "--now", "stockagent-backup-stream.timer"])
    if validate_runtime_lock(before, runtime_identity()):
        raise ValueError("native runtime changed during source resume")
    result = {"state": "source_backup_stream_installed_waiting_relay_adapter", "resume_verified": True,
        "original_install_failure": "findmnt formats bind SOURCE with a filesystem-root suffix; fixed guard retains D enrollment and inode identity",
        "source_role": str(role.resolve()), "mamba_managed": True,
        "copied_files_verified": len(captured), "copied_bytes_verified": sum(r["bytes"] for r in captured.values()),
        "runtime_lock_sha256": expected_runtime["sha256"], "native_runtime_unchanged_by_resume": True,
        "source_C_deliveries_retained": True, "canonical_objects_changed": False,
        "receipt_return_channel": receipts, "relay_adapter_verified": False,
        "observed_at_utc": datetime.now(timezone.utc).isoformat()}
    private_json(destination, result)
    return result




def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/data_sync/backup_stream.json")
    parser.add_argument("--role-prefix", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    action = resume if args.resume else install
    result = action(args.evidence, json.loads(args.config.read_bytes()), args.role_prefix)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
