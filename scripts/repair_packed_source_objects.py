#!/usr/bin/env python3
"""Restore missing referenced cold objects only when exact source bytes prove it.

Uses the existing catalog, manifest verifier, pack writer and atomic installer.
No head updates, deletions, guessed versions or changed-source substitutions.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.publish_data_releases import DEFAULT_CATALOG, _load_catalog, _source_path, _blockers, _running_commands
from stockagent.data_sync.desync_snapshots import atomic_write_json, sha256_file
from stockagent.data_sync.packed_snapshots import (
    _SourceEntry, _load_inventory, _validate_inventory, _write_pack,
    _copy_and_hash, _install_immutable_object, resolve_latest_packed,
    _validate_manifest,
)


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
            if row.get("kind") == "file" and row["storage"]["object_sha256"] in wanted:
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


def repair(dataset: str, sync_root: Path, *, apply: bool, receipt: Path) -> dict:
    catalog = next(e for e in _load_catalog(DEFAULT_CATALOG) if e["dataset"] == dataset)
    if not catalog.get("publish") or _blockers(catalog, _running_commands()):
        raise RuntimeError("catalog excludes publication or source writer is active")
    source = _source_path(catalog).resolve()
    sync_root = sync_root.resolve()
    resolved = resolve_latest_packed(sync_root, dataset, require_objects=False)
    manifest = resolved.manifest
    inventory = _load_inventory(sync_root, manifest)
    _validate_inventory(manifest, inventory)
    groups = defaultdict(list)
    for row in inventory:
        if row.get("kind") == "file":
            groups[row["storage"]["object_sha256"]].append(row)
    result = {"dataset": dataset, "snapshot_id": manifest["snapshot_id"],
              "apply": apply, "head_changed": False, "restored": [], "recoverable": [], "unresolved": []}
    needed = {o["sha256"] for o in manifest["archive"]["objects"]
              if o.get("member_selection") == "subset" and not (sync_root / o["relpath"]).exists()}
    originals = original_members(sync_root, dataset, needed) if needed else {}
    for obj in manifest["archive"]["objects"]:
        target = sync_root / obj["relpath"]
        if target.exists():
            continue
        rows = groups[obj["sha256"]]
        reason = None
        original = originals.get(obj["sha256"])
        if obj.get("member_selection") == "subset":
            if original:
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
                digest = _copy_and_hash(source / rows[0]["path"], temporary)
            else:
                _write_pack(source, entries, temporary, compression_level=(original["compression_level"]
                            if original else int(manifest["archive"]["compression_level"])))
                digest = sha256_file(temporary)
            if digest != obj["sha256"] or temporary.stat().st_size != obj["bytes"]:
                result["unresolved"].append(item | {"reason": "reconstructed_object_hash_mismatch"})
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
    parser.add_argument("dataset")
    parser.add_argument("--sync-root", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = repair(args.dataset, args.sync_root, apply=args.apply, receipt=args.receipt)
    print(json.dumps({k: len(v) if isinstance(v, list) else v for k, v in result.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
