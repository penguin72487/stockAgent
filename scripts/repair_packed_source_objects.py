#!/usr/bin/env python3
"""Restore missing referenced cold objects only when exact source bytes prove it.

Uses the existing catalog, manifest verifier, pack writer and atomic installer.
No head updates, deletions, guessed versions or changed-source substitutions.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import contextlib
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.publish_data_releases import DEFAULT_CATALOG, _load_catalog, _source_path, _blockers, _running_commands
from stockagent.data_sync.desync_snapshots import _exclusive_lock, atomic_write_json, sha256_file
from stockagent.data_sync.packed_snapshots import (
    _SourceEntry, _load_inventory, _validate_inventory, _write_pack,
    _copy_and_hash, _install_immutable_object, resolve_latest_packed, resolve_packed_snapshot_id,
    _validate_manifest, _storage_extents,
)


def repair_transport(c: dict, identity: str, *, apply: bool, receipt: Path) -> dict:
    """Recover exact missing CAS members from an ACKed, still sealed delivery.

    This is the reverse of the existing transport copy, not a new publisher.
    A retained transport may be the only local donor for an old source revision;
    recover it before the original GC owner can retire the redundant transport.
    No head, manifest, source, existing object or archive is overwritten/deleted.
    """
    from stockagent.control.lakehouse import guard
    from stockagent.data_sync import immutable_transport_cache as transport
    from stockagent.data_sync.immutable_replication import HASH, read_manifest, safe
    from stockagent.data_sync.offhost_backup import object_descriptor
    from stockagent.data_sync.packed_backup import signature

    if receipt.exists() or receipt.is_symlink():
        raise ValueError("preserve prior transport recovery receipts")
    if not isinstance(identity, str) or not HASH.fullmatch(identity):
        raise ValueError("an exact immutable delivery identity is required")
    guard(c)
    cold, state = Path(c["cold_root"]), Path(c["state_root"])
    physical = transport.PHYSICAL_TRANSPORT_ROOT
    if not physical.samefile(Path(c["transport_root"]).parent):
        raise ValueError("transport physical/bind roots differ")
    root = safe(physical, "lakehouse/lake-" + identity)
    retirement_lock = state / ("cache-retirement-owner-" + identity + ".lock")
    publication_lock = cold / ".local-state/locks/publish-retention-global.lock"
    if not publication_lock.is_file() or publication_lock.is_symlink():
        raise ValueError("the existing canonical publication lock is required")
    result = {"contract": "packed_exact_transport_recovery_v1", "delivery_identity_sha256": identity,
              "apply": apply, "head_changed": False, "restored": [], "recoverable": [],
              "source_files_deleted": 0, "cold_objects_deleted": 0, "nas_archive_files_deleted": 0}
    # GC takes the same per-delivery lock. The publication lock fences source
    # admission and ensures the install cannot race another cooperating writer.
    with contextlib.ExitStack() as locks:
        locks.enter_context(_exclusive_lock(retirement_lock))
        locks.enter_context(_exclusive_lock(publication_lock))
        manifest = read_manifest(root)
        before = signature(root / "manifest.json")
        raw = (root / "manifest.json").read_bytes()
        context = manifest["context"]
        if (manifest["identity_sha256"] != identity or context.get("kind") != "immutable_source_objects"
                or any(context.get(k) != c[k] for k in ("producer_device_id", "receiver_device_id"))):
            raise ValueError("transport authority/kind/identity differs")
        ack = transport.validate_archive_receipt(c, identity, manifest, raw)
        ledger_path = state / "source-replication-ledger.json"
        ledger_sha = sha256_file(ledger_path)
        row = json.loads(ledger_path.read_bytes())["deliveries"][identity]
        if row.get("nas_acceptance", {}).get("state") != "nas_archive_file_recovery_verified":
            raise ValueError("transport lacks enrolled NAS acceptance")
        keys = set(row["file_keys"])
        candidates = []
        for relative, descriptor in manifest["files"].items():
            original = context.get("source_object_paths", {}).get(relative, relative)
            expected = descriptor["sha256"]
            if object_descriptor(original) != expected or original + "@" + expected not in keys:
                raise ValueError("transport member is not an enrolled exact cold object")
            target, donor = safe(cold, original), safe(root, relative)
            if target.exists() or target.is_symlink():
                continue  # Never overwrite even a damaged existing object.
            donor_before = signature(donor)
            if donor_before[2] != descriptor["bytes"] or sha256_file(donor) != expected:
                raise ValueError("transport donor checksum/size differs")
            if signature(donor) != donor_before:
                raise ValueError("transport donor changed during recovery audit")
            item = {"relpath": original, "sha256": expected, "bytes": descriptor["bytes"]}
            candidates.append((item, donor, donor_before, target))
            result["recoverable"].append(item)
        if (signature(root / "manifest.json") != before or read_manifest(root) != manifest
                or sha256_file(ledger_path) != ledger_sha):
            raise ValueError("transport metadata/ledger changed during recovery audit")
        transport.validate_archive_receipt(c, identity, manifest, raw)
        if apply:
            guard(c)
            for item, donor, donor_before, target in candidates:
                if signature(donor) != donor_before:
                    raise ValueError("transport donor changed before exact copy")
                # Keep failed staging for inspection. Only a successful install
                # may consume this exact temporary file and empty directory.
                stage = Path(tempfile.mkdtemp(prefix="transport-repair-", dir=cold / ".local-state/staging"))
                temporary = stage / "object.partial"
                digest = _copy_and_hash(donor, temporary)
                if (digest != item["sha256"] or temporary.stat().st_size != item["bytes"]
                        or signature(donor) != donor_before):
                    raise ValueError("exact transport reconstruction changed")
                _install_immutable_object(cold, temporary, target, expected_sha256=digest)
                if sha256_file(target) != digest or target.stat().st_size != item["bytes"]:
                    raise ValueError("recovered cold object failed independent readback")
                stage.rmdir()
                result["restored"].append(item)
        result.update(state="repaired" if apply else "audited",
                      restored_bytes=sum(item["bytes"] for item in result["restored"]),
                      paired_ack_identity_sha256=ack["identity_sha256"],
                      original_manifest_sha256=hashlib.sha256(raw).hexdigest(),
                      existing_objects_verified=False)
        atomic_write_json(receipt, result)
    return result


def original_members(sync_root: Path, dataset: str, needed: set[str]) -> dict:
    """Recover full pack membership from verified retained inventories.

    A later release may use only a subset of an old pack. Repacking that subset
    cannot reproduce its object hash. Retained manifests supply the original
    ordered members; the reconstructed object must still match the exact head
    digest, so this does not promote or trust an old head as current data.
    """
    found, inventories = {}, {}
    for path in sorted((sync_root / "manifests" / dataset).glob("*.json")):
        manifest = json.loads(path.read_text())
        _validate_manifest(manifest)
        if manifest["dataset"] != dataset:
            raise ValueError("retained manifest dataset mismatch")
        objects = [o for o in manifest["archive"]["objects"]
                   if o["sha256"] in needed - found.keys()
                   and o.get("member_selection") != "subset"]
        if not objects:
            continue
        digest = manifest["archive"]["inventory"]["sha256"]
        if digest not in inventories:
            inventories[digest] = _load_inventory(sync_root, manifest)
        inventory = inventories[digest]
        _validate_inventory(manifest, inventory)
        groups = defaultdict(list)
        wanted = {o["sha256"] for o in objects}
        for row in inventory:
            if (row.get("kind") == "file" and row["storage"].get("kind") != "chunks"
                    and row["storage"]["object_sha256"] in wanted):
                groups[row["storage"]["object_sha256"]].append(row)
        for obj in objects:
            found[obj["sha256"]] = {
                "rows": groups[obj["sha256"]], "manifest_path": str(path),
                "manifest_sha256": sha256_file(path),
                "compression_level": int(manifest["archive"]["compression_level"]),
            }
        if needed <= found.keys():
            break
    return found


def _copy_exact_chunk(source: Path, destination: Path, offset: int, length: int) -> str:
    """Reconstruct one bounded extent, never mistake a chunk for its full file."""
    digest = hashlib.sha256()
    with source.open("rb") as original, destination.open("xb") as target:
        original.seek(offset)
        remaining = length
        while remaining:
            block = original.read(min(remaining, 8 * 1024**2))
            if not block:
                raise ValueError("source chunk was truncated during recovery")
            target.write(block)
            digest.update(block)
            remaining -= len(block)
        target.flush()
        os.fsync(target.fileno())
    return digest.hexdigest()


def repair(dataset: str, sync_root: Path, *, apply: bool, receipt: Path, snapshot_id: str | None = None,
           original_member_cache: dict | None = None) -> dict:
    if receipt.exists():
        raise ValueError("preserve prior exact-source recovery receipts")
    catalog = next(e for e in _load_catalog(DEFAULT_CATALOG) if e["dataset"] == dataset)
    if not catalog.get("publish") or _blockers(catalog, _running_commands()):
        raise RuntimeError("catalog excludes publication or source writer is active")
    source = _source_path(catalog).resolve()
    sync_root = sync_root.resolve()
    resolved = (resolve_packed_snapshot_id(sync_root, dataset, snapshot_id, require_objects=False) if snapshot_id
                else resolve_latest_packed(sync_root, dataset, require_objects=False))
    manifest = resolved.manifest
    inventory = _load_inventory(sync_root, manifest)
    _validate_inventory(manifest, inventory)
    groups, grouped_paths, chunk_sources = defaultdict(list), defaultdict(set), {}
    for row in inventory:
        if row.get("kind") == "file":
            offset = 0
            for sha, length in _storage_extents(row["storage"], row["size"]):
                if row["path"] not in grouped_paths[sha]:
                    groups[sha].append(row)
                    grouped_paths[sha].add(row["path"])
                if row["storage"].get("kind") == "chunks":
                    chunk_sources.setdefault(sha, (row["path"], offset, length))
                offset += length
    result = {"dataset": dataset, "snapshot_id": manifest["snapshot_id"],
              "apply": apply, "head_changed": False, "restored": [], "recoverable": [], "unresolved": []}
    needed = {o["sha256"] for o in manifest["archive"]["objects"]
              if o.get("member_selection") == "subset" and not (sync_root / o["relpath"]).exists()}
    originals = (original_member_cache if original_member_cache is not None else original_members(sync_root, dataset, needed)) if needed else {}
    for obj in manifest["archive"]["objects"]:
        target = sync_root / obj["relpath"]
        if target.exists():
            continue
        rows = groups[obj["sha256"]]
        reason = None
        original = originals.get(obj["sha256"])
        if obj.get("member_selection") == "subset":
            if original:
                origin = Path(original["manifest_path"])
                if (not origin.resolve().is_relative_to(sync_root / "manifests" / dataset)
                        or sha256_file(origin) != original["manifest_sha256"]):
                    raise ValueError("cached original pack membership manifest changed")
                rows = original["rows"]
            else:
                reason = "original_pack_members_not_all_in_selected_inventory"
        entries = []
        for row in rows if reason is None else ():
            path = (source / row["path"]).resolve()
            if not path.is_relative_to(source) or not path.is_file():
                reason = "source_file_unavailable"
                break
            info = path.stat()
            if info.st_size != row["size"] or sha256_file(path) != row["sha256"]:
                reason = "source_bytes_differ_from_retained_release"
                break
            entries.append(_SourceEntry(path=row["path"], kind="file", mode=row["mode"],
                size=row["size"], sha256=row["sha256"],
                source_stat=(info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)))
        item = {"relpath": obj["relpath"], "sha256": obj["sha256"], "bytes": obj["bytes"]}
        if original:
            item.update(original_manifest_path=original["manifest_path"],
                        original_manifest_sha256=original["manifest_sha256"],
                        original_member_count=len(rows))
        if reason:
            result["unresolved"].append(item | {"reason": reason, "first_unavailable_source": row["path"] if entries or reason.startswith("source_") else None})
            continue
        result["recoverable"].append(item)
        if not apply:
            continue
        # Verify the exact object digest again after reconstructing it. The
        # temporary directory is local-only and contains only our scratch file.
        with tempfile.TemporaryDirectory(prefix="packed-restore-", dir=sync_root / ".local-state") as temporary_dir:
            temporary = Path(temporary_dir) / "object.partial"
            if obj["kind"] == "blob":
                if obj["sha256"] in chunk_sources:
                    name, offset, length = chunk_sources[obj["sha256"]]
                    digest = _copy_exact_chunk(source / name, temporary, offset, length)
                else:
                    digest = _copy_and_hash(source / rows[0]["path"], temporary)
            else:
                _write_pack(source, entries, temporary, compression_level=(original["compression_level"]
                            if original else int(manifest["archive"]["compression_level"])))
                digest = sha256_file(temporary)
            if digest != obj["sha256"] or temporary.stat().st_size != obj["bytes"]:
                result["unresolved"].append(item | {"reason": "reconstructed_object_hash_mismatch"})
                continue
            if _blockers(catalog, _running_commands()):
                result["unresolved"].append(item | {"reason": "source_writer_started_before_install"})
                continue
            _install_immutable_object(sync_root, temporary, target, expected_sha256=digest)
            if sha256_file(target) != digest:
                raise RuntimeError("restored immutable object failed verification")
            result["restored"].append(item)
    result["restored_bytes"] = sum(row["bytes"] for row in result["restored"])
    atomic_write_json(receipt, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", nargs="?")
    parser.add_argument("--sync-root", type=Path)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--snapshot-id", help="fixed retained source release; default remains the current head")
    parser.add_argument("--transport-delivery", help="recover missing exact objects from one enrolled NAS-ACKed sealed delivery")
    parser.add_argument("--lakehouse-config", type=Path, default=Path("/etc/stockagent/lakehouse-control.json"))
    args = parser.parse_args()
    if args.transport_delivery:
        if args.dataset or args.sync_root or args.snapshot_id:
            parser.error("transport recovery uses its fixed private authority, not dataset/sync-root/snapshot overrides")
        from stockagent.control.lakehouse import configuration
        result = repair_transport(configuration(args.lakehouse_config), args.transport_delivery,
                                  apply=args.apply, receipt=args.receipt)
    else:
        if not args.dataset or not args.sync_root:
            parser.error("source recovery requires dataset and --sync-root")
        result = repair(args.dataset, args.sync_root, apply=args.apply, receipt=args.receipt, snapshot_id=args.snapshot_id)
    print(json.dumps({k: len(v) if isinstance(v, list) else v for k, v in result.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
