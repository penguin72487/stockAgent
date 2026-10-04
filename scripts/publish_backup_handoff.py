#!/usr/bin/env python3
"""Publish a hash-pinned receiver adapter package over the existing ingress."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_backup_delivery import CONTRACT, verify  # noqa: E402
from scripts.backup_delivery_receipt import atomic_public, signed  # noqa: E402
from stockagent.data_sync.backup_stream import BackupStream  # noqa: E402
from stockagent.data_sync.desync_snapshots import atomic_write_bytes  # noqa: E402
from stockagent.data_sync.offhost_backup import private_json  # noqa: E402
from stockagent.runtime_identity import identity_sha256, stable_source_sha256  # noqa: E402

FILES = (
    "scripts/__init__.py", "scripts/verify_backup_delivery.py", "scripts/backup_delivery_receipt.py",
    "scripts/configure_backup_receipts_syncthing.py", "scripts/configure_artifact_ingress_syncthing.py",
    "scripts/verify_restored_backup.py", "scripts/assemble_restored_backup.py", "scripts/runtime_env.sh",
    "stockagent/__init__.py", "stockagent/runtime_identity.py", "stockagent/data_sync/__init__.py",
    "stockagent/data_sync/offhost_backup.py", "stockagent/data_sync/packed_backup.py",
    "stockagent/data_sync/packed_snapshots.py", "stockagent/data_sync/desync_snapshots.py",
    "stockagent/data_sync/backup_auxiliary.py", "stockagent/data_sync/backup_recovery.py",
    "stockagent/control/__init__.py", "stockagent/control/backup.py", "stockagent/control/recovery.py",
    "downloader/__init__.py", "downloader/artifact_io.py", "configs/data_sync/backup_receipts.json",
    "configs/environments/control-recovery.yml",
    "configs/environments/locks/control-recovery-linux-64-20261003.explicit.txt",
    "docs/continuous_nas_backup_2026-10-04.md",
    "docs/lab203_complete_backup_rollout_2026-10-04.md",
    "configs/data_sync/backup_history_disposition_20261004.json",
    "configs/data_sync/backup_stream.json",
    "scripts/run_nas_recovery_acceptance.py", "stockagent/data_sync/nas_recovery_acceptance.py",
    "stockagent/data_sync/nas_target.py", "configs/data_sync/lab203_nas_recovery_plan_20261004.json",
    "configs/data_sync/backup_key_custody_20261004.json", "docs/lab203_nas_recovery_acceptance_2026-10-04.md",
    "stockagent/data_sync/recovery_queue.py", "stockagent/data_sync/materialized_cache.py",
    "scripts/run_lab203_recovery_queue.py", "scripts/run_lab203_recovery_queue.sh",
    "scripts/install_lab203_recovery_queue.py", "docs/lab203_automatic_backup_2026-10-04.md",
)


def publish_handoff(queue: BackupStream, name: str, *, source_root: Path = ROOT,
                    wait_for_owner: bool = False, publish_recovery_tasks: bool = False) -> dict:
    if name not in {"continuous-backup-20261004-v1", "continuous-backup-20261004-v2", "continuous-backup-20261004-v3",
                    "continuous-backup-20261004-v4", "continuous-backup-20261004-v5", "continuous-backup-20261004-v6",
                    "continuous-backup-20261004-v7"}:
        raise ValueError("use this explicitly versioned handoff scope")
    queue.storage_guard()
    destination = queue.transport / "tools" / name
    if any(p.is_symlink() for p in (destination, *destination.parents)):
        raise ValueError("handoff destination is redirected")
    if destination.exists():
        raise ValueError("preserve a previously frozen handoff; use a new reviewed version")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (queue.state / "owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
        queue.storage_guard()
        identities = {name: stable_source_sha256(source_root, name) for name in FILES}
        staging = queue.transport / ".staging" / ("handoff-" + uuid.uuid4().hex)
        staging.mkdir(mode=0o700, parents=True)
        rows = []
        for filename, digest in sorted(identities.items()):
            raw = (source_root / filename).read_bytes()
            if hashlib.sha256(raw).hexdigest() != digest:
                raise ValueError("handoff source changed during capture")
            atomic_write_bytes(staging / filename, raw, mode=0o600)
            rows.append({"relative": filename, "bytes": len(raw), "sha256": digest})
        manifest_body = {"contract": "frozen_backup_receiver_handoff_v1", "files": rows,
            "producer_device_id": queue.config["producer_device_id"], "receiver_device_id": queue.config["receiver_device_id"],
            "repository_id": queue.config["repository_id"], "replace_existing_worker": False,
            "automatic_execution_from_syncthing": False, "private_credentials_included": False}
        manifest = {**manifest_body, "identity_sha256": identity_sha256(manifest_body)}
        private_json(staging / "handoff-manifest.json", manifest)
        raw_manifest = (staging / "handoff-manifest.json").read_bytes()
        rows.append({"relative": "handoff-manifest.json", "bytes": len(raw_manifest), "sha256": hashlib.sha256(raw_manifest).hexdigest()})
        body = {"contract": CONTRACT, "source_plan_identity_sha256": manifest["identity_sha256"], "files": rows,
            "selection": "frozen_backup_receiver_handoff", "all_history_verified": False,
            "unpublished_sources_included": False}
        envelope = {**body, "identity_sha256": identity_sha256(body)}
        private_json(staging / "backup-envelope.json", envelope)
        verify(staging, envelope["identity_sha256"], require_ready=False)
        if {name: stable_source_sha256(source_root, name) for name in FILES} != identities:
            raise ValueError("handoff source changed; publication withheld")
        atomic_write_bytes(staging / "READY", (envelope["identity_sha256"] + "\n").encode(), mode=0o600)
        verified = verify(staging, envelope["identity_sha256"])
        os.rename(staging, destination)
        directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        if name in {"continuous-backup-20261004-v6", "continuous-backup-20261004-v7"}:
            bootstrap = signed({"contract": "fixed_automatic_backup_bootstrap_v1",
                **{k: queue.config[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")},
                "package_relative": "tools/" + name,
                "envelope_identity_sha256": envelope["identity_sha256"],
                "handoff_manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(),
                "one_time_local_installation_required": True, "receiver_hook_deployed": False})
            version = name.rsplit("-", 1)[-1]
            atomic_public(queue.transport / ("tools/automatic-backup-bootstrap-" + version + ".json"), bootstrap)
        recovery_requests = None
        if publish_recovery_tasks:
            from stockagent.data_sync.recovery_queue import publish_requests
            ledger = queue.load_ledger()
            errors = queue.ingest(ledger)
            private_json(queue.ledger_path, ledger)
            try:
                recovery_requests = (publish_requests(queue.config, queue.transport, ledger) if not errors
                                     else {"state": "waiting_valid_file_receipts", "receipt_errors": errors})
            except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                recovery_requests = {"state": "request_publication_failed", "error_type": type(error).__name__}
    return {"state": "frozen_receiver_handoff_published", "source_path": str(destination),
        "receiver_relative": "tools/" + name, "envelope_identity_sha256": envelope["identity_sha256"],
        "handoff_manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(), "source_files": len(FILES),
        "complete_files": verified["files_verified"] + 2,
        "complete_bytes": verified["bytes_verified"] + sum((destination / n).stat().st_size for n in ("READY", "backup-envelope.json")),
        "receiver_adapter_deployed": False, "automatic_recovery_requests": recovery_requests}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/data_sync/backup_stream.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wait-for-owner", action="store_true")
    parser.add_argument("--publish-recovery-requests", action="store_true")
    parser.add_argument("--name", choices=("continuous-backup-20261004-v1", "continuous-backup-20261004-v2",
                                         "continuous-backup-20261004-v3", "continuous-backup-20261004-v4",
                                         "continuous-backup-20261004-v5", "continuous-backup-20261004-v6",
                                         "continuous-backup-20261004-v7"),
                        default="continuous-backup-20261004-v7")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("retain a fresh handoff publication receipt")
    queue = BackupStream(json.loads(args.config.read_bytes()))
    result = publish_handoff(queue, args.name, wait_for_owner=args.wait_for_owner,
                             publish_recovery_tasks=args.publish_recovery_requests)
    private_json(args.output, result)
    queue.scan_transport(queue.load_ledger())
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
