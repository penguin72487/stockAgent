#!/usr/bin/env python3
"""Build an E-only candidate from one pinned, independently restored D release.

This does not update the D authority, any producer alias, NAS, or retirement
state. A candidate is ready only after both versions reconstruct the same
portable source inventory and the E candidate is independently restored.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from stockagent.data_sync import packed_snapshots as packed, workspace_volume as volume  # noqa: E402
from stockagent.data_sync.completed_forward_capacity import reserve_capacity  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402

COLD = Path("/srv/stockagent-packed")
CONTRACT = "stockagent.source_local_E_candidate.v1"
PACK_LAYOUT = "source-dataset-v1"
PACK_TARGET_BYTES = 4 * 1024**3


def _fixed_receipt(path: Path) -> tuple[dict, str]:
    if path.is_symlink() or path.resolve(strict=True) != path.absolute():
        raise SnapshotError("fixed source receipt is redirected")
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= 1024**2:
        raise SnapshotError("fixed source receipt is not bounded regular metadata")
    with path.open("rb") as stream:
        raw = stream.read(1024**2 + 1)
        after = os.fstat(stream.fileno())
    current = path.lstat()
    signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if signature(before) != signature(after) or signature(after) != signature(current) or len(raw) != before.st_size:
        raise SnapshotError("fixed source receipt changed while reading")
    value = json.loads(raw)
    if (not isinstance(value, dict) or value.get("contract") != "stockagent.cftc_closed_E_restore.v1"
            or value.get("state") != "fixed_release_materialized_and_readback_verified"
            or value.get("dataset") != "cftc-legacy-pre2000"):
        raise SnapshotError("candidate needs the original fixed CFTC E restore receipt")
    return value, hashlib.sha256(raw).hexdigest()


def _source_inventory(root: Path, snapshot) -> list[dict]:
    """Compare original names and bytes while allowing different storage mappings."""
    fields = ("path", "kind", "mode", "size", "sha256", "target")
    return [{name: row[name] for name in fields if name in row}
            for row in packed._iter_inventory(root, snapshot.manifest)]


def _existing_candidate(candidate: Path, fixed: dict, fixed_receipt_sha: str, original, source: Path) -> dict:
    """Recheck an already completed fixed candidate without rewriting its proof."""
    ready_path = candidate / "candidate-ready.json"
    if ready_path.is_symlink() or not ready_path.is_file() or ready_path.stat().st_size > 1024**2:
        raise SnapshotError("completed candidate receipt is missing or redirected")
    ready = json.loads(ready_path.read_bytes())
    if (ready.get("contract") != CONTRACT or ready.get("state") != "E_candidate_independently_restored"
            or ready.get("dataset") != fixed["dataset"] or ready.get("original_snapshot_id") != fixed["snapshot_id"]
            or ready.get("original_manifest_sha256") != fixed["manifest_sha256"]
            or ready.get("fixed_receipt_sha256") != fixed_receipt_sha
            or ready.get("candidate_path") != str(candidate)
            or ready.get("D_authority_changed") is not False or ready.get("NAS_accepted") is not False
            or ready.get("source_retirement_authorized") is not False):
        raise SnapshotError("existing candidate does not match its fixed source")
    selected = packed.resolve_packed_snapshot_id(candidate / "cold", fixed["dataset"], ready["candidate_snapshot_id"])
    if (selected.manifest_sha256 != ready.get("candidate_manifest_sha256")
            or selected.manifest["archive"].get("new_pack_layout") != PACK_LAYOUT
            or selected.manifest["archive"].get("new_pack_target_bytes") != PACK_TARGET_BYTES
            or _source_inventory(COLD, original) != _source_inventory(candidate / "cold", selected)):
        raise SnapshotError("existing candidate layout, manifest or original inventory differs")
    packed.verify_packed_snapshot(COLD, original, materialized_path=source, reconstruct_all=True,
        d_primary_native_blob_reads=True, d_primary_native_pack_reads=True)
    restored = candidate / "restore" / fixed["dataset"] / ready["candidate_snapshot_id"]
    packed.verify_packed_snapshot(candidate / "cold", selected, materialized_path=restored, reconstruct_all=True)
    return ready


def stage(receipt_path: Path, candidate: Path) -> dict:
    config = volume.configuration()
    restore_root = volume.path("restore")
    build_root = volume.path("build", create=True)
    receipt_path, candidate = receipt_path.absolute(), candidate.absolute()
    if (not receipt_path.is_relative_to(volume.path("verify")) or not candidate.is_relative_to(build_root)
            or candidate.parent.resolve(strict=True) != candidate.parent or candidate.is_symlink()):
        raise SnapshotError("candidate and fixed receipt must use their enrolled E roots")
    fixed, fixed_receipt_sha = _fixed_receipt(receipt_path)
    source = Path(fixed["E_materialized_path"]).absolute()
    if (not source.is_relative_to(restore_root) or source.is_symlink()
            or source.resolve(strict=True) != source or not source.is_dir()):
        raise SnapshotError("fixed source is not its original E restore directory")
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_d_primary_mount(COLD)
    original = packed.resolve_packed_snapshot_id(COLD, fixed["dataset"], fixed["snapshot_id"])
    if (original.manifest_sha256 != fixed["manifest_sha256"]
            or original.manifest["archive"]["inventory"]["sha256"] != fixed["inventory_sha256"]):
        raise SnapshotError("fixed D release and original E receipt differ")
    logical_bytes = int(original.manifest["source"]["logical_bytes"])
    files = int(original.manifest["source"]["files"])
    if logical_bytes > 1024**3 or files > 100000:
        raise SnapshotError("candidate exceeds this bounded pilot's admitted scope")
    budget = logical_bytes * 2 + 256 * 1024**2
    controls = volume.path("migration", create=True) / "provider-state-temporary-20261103/capacity-leases"
    key = hashlib.sha256(str(candidate).encode()).hexdigest()
    with ExitStack() as stack:
        lock_root = volume.path("migration", create=True) / "source-local-candidate-locks"
        lock_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if lock_root.is_symlink() or lock_root.resolve(strict=True) != lock_root:
            raise SnapshotError("candidate owner lock root is redirected")
        descriptor = os.open(lock_root / (key + ".lock"), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        owner = stack.enter_context(os.fdopen(descriptor, "a+b"))
        lock_info = os.fstat(owner.fileno())
        if (not stat.S_ISREG(lock_info.st_mode) or lock_info.st_uid != os.geteuid()
                or lock_info.st_nlink != 1 or lock_info.st_mode & 0o022
                or not os.path.samestat(lock_info, (lock_root / (key + ".lock")).lstat())):
            raise SnapshotError("candidate owner lock identity differs")
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        if candidate.exists() and (not candidate.is_dir() or candidate.resolve(strict=True) != candidate):
            raise SnapshotError("candidate workspace is redirected or not a directory")
        if candidate.exists():
            info = candidate.lstat()
            if info.st_uid != os.geteuid() or info.st_mode & 0o022 or info.st_dev != build_root.stat().st_dev:
                raise SnapshotError("candidate workspace ownership or filesystem differs")
            allowed = {"intent.json", "status.json", "candidate-ready.json", "cold", "restore"}
            if {item.name for item in candidate.iterdir()} - allowed:
                raise SnapshotError("candidate contains unknown root members")
            for name in allowed:
                child = candidate / name
                if child.is_symlink() or (child.exists() and child.resolve(strict=True) != child):
                    raise SnapshotError("candidate member is redirected")
        if (candidate / "candidate-ready.json").exists():
            result = _existing_candidate(candidate, fixed, fixed_receipt_sha, original, source)
            if _fixed_receipt(receipt_path)[1] != fixed_receipt_sha:
                raise SnapshotError("fixed receipt changed while rechecking the candidate")
            return result
        lease = stack.enter_context(reserve_capacity(controls, key=key, maximum_growth_bytes=budget,
            capacity_bytes=2**63 - 1, minimum_free_bytes=config["minimum_free_bytes"]))
        admission = volume.admit(budget + lease["other_reserved_growth_bytes"], c=config)
        intent = {"contract": CONTRACT + ".intent", "candidate_path": str(candidate),
            "source_path": str(source), "fixed_receipt_sha256": fixed_receipt_sha,
            "dataset": fixed["dataset"], "original_snapshot_id": fixed["snapshot_id"],
            "original_manifest_sha256": fixed["manifest_sha256"],
            "pack_layout": PACK_LAYOUT, "pack_target_bytes": PACK_TARGET_BYTES,
            "large_file_layout": "whole-file-v1"}
        intent_path = candidate / "intent.json"
        if candidate.exists():
            if intent_path.is_symlink() or not intent_path.is_file() or intent_path.stat().st_size > 1024**2:
                raise SnapshotError("interrupted candidate lacks its original intent")
            if json.loads(intent_path.read_bytes()) != intent:
                raise SnapshotError("interrupted candidate intent or fixed source changed")
        else:
            candidate.mkdir(mode=0o700)
            atomic_write_json(intent_path, intent, durable=True)
        status = candidate / "status.json"
        def record(state, **extra):
            atomic_write_json(status, {"contract": CONTRACT, "state": state,
                "dataset": fixed["dataset"], "original_snapshot_id": fixed["snapshot_id"],
                "observed_at_epoch": time.time(), **extra}, durable=True)
        try:
            record("checking_fixed_D_and_E")
            old_proof = packed.verify_packed_snapshot(COLD, original, materialized_path=source,
                reconstruct_all=True, d_primary_native_blob_reads=True, d_primary_native_pack_reads=True)
            original_rows = _source_inventory(COLD, original)
            record("building_E_candidate", source_files=files, source_bytes=logical_bytes)
            candidate_cold = candidate / "cold"
            selected = packed.publish_packed_snapshot(candidate_cold, fixed["dataset"], source,
                node_id="penguin-stage", pack_buckets=64, loose_file_threshold_bytes=PACK_TARGET_BYTES,
                file_chunk_bytes=8 * 1024**2, pack_layout=PACK_LAYOUT,
                pack_target_bytes=PACK_TARGET_BYTES, large_file_layout="whole-file-v1",
                metadata={"role": "E-only-candidate-not-authority",
                          "original_snapshot_id": fixed["snapshot_id"],
                          "original_manifest_sha256": fixed["manifest_sha256"]})
            candidate_rows = _source_inventory(candidate_cold, selected)
            if original_rows != candidate_rows:
                raise SnapshotError("candidate portable inventory differs from the fixed original")
            record("restoring_E_candidate", source_files=files, source_bytes=logical_bytes)
            restored = packed.fetch_packed_snapshot(candidate_cold, candidate / "restore", selected)
            new_proof = packed.verify_packed_snapshot(candidate_cold, selected, materialized_path=restored,
                                                      reconstruct_all=True)
            if (_fixed_receipt(receipt_path)[1] != fixed_receipt_sha
                    or packed.resolve_packed_snapshot_id(COLD, fixed["dataset"], fixed["snapshot_id"]).manifest_sha256
                    != fixed["manifest_sha256"]):
                raise SnapshotError("original release or restore receipt changed while staging")
            result = {"contract": CONTRACT, "state": "E_candidate_independently_restored",
                "dataset": fixed["dataset"], "original_snapshot_id": fixed["snapshot_id"],
                "original_manifest_sha256": fixed["manifest_sha256"], "fixed_receipt_sha256": fixed_receipt_sha,
                "candidate_snapshot_id": selected.manifest["snapshot_id"],
                "candidate_manifest_sha256": selected.manifest_sha256,
                "files": files, "logical_bytes": logical_bytes,
                "candidate_object_count": selected.manifest["archive"]["object_count"],
                "candidate_stored_bytes": selected.manifest["archive"]["stored_bytes"],
                "new_pack_layout": PACK_LAYOUT, "new_pack_target_bytes": PACK_TARGET_BYTES,
                "original_reconstructed_files": old_proof["independently_reconstructed_files"],
                "candidate_reconstructed_files": new_proof["independently_reconstructed_files"],
                "portable_inventories_equal": True, "capacity_admission": admission,
                "D_authority_changed": False, "NAS_accepted": False, "source_retirement_authorized": False,
                "candidate_path": str(candidate), "observed_at_epoch": time.time()}
            atomic_write_json(candidate / "candidate-ready.json", result, durable=True)
            record("E_candidate_independently_restored")
            return result
        except Exception as error:
            record("failed_retained_for_inspection", error_type=type(error).__name__, detail=str(error)[:300])
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixed-receipt", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(stage(args.fixed_receipt, args.candidate), ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
