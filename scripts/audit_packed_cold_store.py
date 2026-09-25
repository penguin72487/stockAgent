#!/usr/bin/env python3
"""Read-only reachability inventory for a packed cold store.

This reports candidates, never deletion eligibility. In particular an object
without a manifest may be an interrupted publication or unique evidence.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import stat
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("expected JSON object")
    return value


def _references(manifest: dict[str, Any]) -> dict[str, int]:
    archive = manifest["archive"]
    rows = [archive["inventory"], *archive["objects"]]
    result: dict[str, int] = {}
    for row in rows:
        relative = str(row["relpath"])
        if not relative.startswith("objects/") or ".." in Path(relative).parts:
            raise ValueError(f"unsafe object path: {relative}")
        size = int(row["bytes"])
        if size < 0 or (relative in result and result[relative] != size):
            raise ValueError(f"invalid object size: {relative}")
        result[relative] = size
    return result


def _group(dataset: str) -> str:
    if dataset.startswith("artifact-"):
        return "artifact"
    if dataset.startswith("legacy-"):
        return "legacy"
    return dataset.split("-", 1)[0]


def audit(root: Path, compare_root: Path | None = None, *, fast: bool = False) -> dict[str, Any]:
    root = root.resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"not a directory: {root}")
    errors: list[str] = []
    heads: dict[str, dict[str, Any]] = {}
    for path in sorted((root / "heads").glob("*/*.json")):
        try:
            head = _read_json(path)
            if head["dataset"] != path.parent.name or head["node_id"] != path.stem:
                raise ValueError("head path/identity mismatch")
            heads[str(head["snapshot_id"])] = head
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"head {path}: {exc}")

    manifests: dict[str, dict[str, Any]] = {}
    references: dict[str, dict[str, int]] = {}
    for path in sorted((root / "manifests").glob("*/*.json")):
        try:
            manifest = _read_json(path)
            snapshot_id = str(manifest["snapshot_id"])
            if manifest["dataset"] != path.parent.name or snapshot_id != path.stem:
                raise ValueError("manifest path/identity mismatch")
            if snapshot_id in manifests:
                raise ValueError("duplicate snapshot ID")
            refs = _references(manifest)
            manifests[snapshot_id] = manifest
            references[snapshot_id] = refs
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"manifest {path}: {exc}")

    all_expected: dict[str, int] = {}
    for refs in references.values():
        for relative, size in refs.items():
            if relative in all_expected and all_expected[relative] != size:
                errors.append(f"conflicting object size: {relative}")
            all_expected[relative] = size
    current_refs: set[str] = set()
    groups: Counter[str] = Counter()
    largest: list[dict[str, Any]] = []
    for snapshot_id, head in heads.items():
        manifest = manifests.get(snapshot_id)
        if manifest is None:
            errors.append(f"missing current manifest: {snapshot_id}")
            continue
        if manifest["dataset"] != head["dataset"]:
            errors.append(f"head/manifest dataset mismatch: {snapshot_id}")
            continue
        refs = references[snapshot_id]
        current_refs.update(refs)
        groups[_group(str(manifest["dataset"]))] += 1
        largest.append({
            "dataset": manifest["dataset"],
            "snapshot_id": snapshot_id,
            "referenced_bytes_non_deduplicated": sum(refs.values()),
        })

    objects: dict[str, int] = {}
    for directory, subdirs, names in os.walk(root / "objects", followlinks=False):
        if not fast:
            for name in subdirs:
                path = Path(directory) / name
                if path.is_symlink():
                    errors.append(f"object directory symlink: {path}")
        for name in names:
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            if fast and relative in all_expected:
                objects[relative] = all_expected[relative]
                continue
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode):
                errors.append(f"non-regular object: {relative}")
                continue
            objects[relative] = info.st_size
    if not fast:
        for relative, size in all_expected.items():
            if relative in objects and objects[relative] != size:
                errors.append(f"object size mismatch: {relative}")

    missing = set(all_expected) - set(objects)
    missing_current = missing & current_refs
    missing_by_dataset: dict[str, set[str]] = defaultdict(set)
    for snapshot_id, refs in references.items():
        for relative in refs.keys() & missing:
            missing_by_dataset[str(manifests[snapshot_id]["dataset"])].add(relative)
    compare_present = 0
    if compare_root is not None:
        compare_root = compare_root.resolve(strict=True)
        compare_present = sum((compare_root / relative).is_file() for relative in missing)
    orphan = set(objects) - set(all_expected)
    historical = (set(all_expected) - current_refs) & set(objects)
    return {
        "root": str(root),
        "proof_level": (
            "metadata_and_presence_only; referenced sizes and hashes not checked"
            if fast else "metadata_and_size_only; object hashes not checked"
        ),
        "deletion_authorized": False,
        "head_count": len(heads),
        "manifest_count": len(manifests),
        "object_count": len(objects),
        "object_bytes": sum(objects.values()),
        "current_unique_object_count": len(current_refs & set(objects)),
        "current_unique_object_bytes": sum(objects[r] for r in current_refs & set(objects)),
        "historical_only_object_count": len(historical),
        "historical_only_object_bytes": sum(objects[r] for r in historical),
        "unreferenced_object_count": len(orphan),
        "unreferenced_object_bytes": sum(objects[r] for r in orphan),
        "missing_referenced_object_count": len(missing),
        "missing_referenced_expected_bytes": sum(all_expected[r] for r in missing),
        "missing_current_object_count": len(missing_current),
        "missing_objects_present_in_compare_root": compare_present,
        "missing_by_dataset": {key: len(value) for key, value in sorted(missing_by_dataset.items())},
        "current_groups": dict(sorted(groups.items())),
        "largest_current_releases": sorted(
            largest, key=lambda row: row["referenced_bytes_non_deduplicated"], reverse=True
        )[:15],
        "unreferenced_largest": sorted(
            ({"relpath": r, "bytes": objects[r]} for r in orphan),
            key=lambda row: row["bytes"], reverse=True,
        )[:15],
        "errors": errors[:100],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--compare-root", type=Path)
    parser.add_argument("--fast", action="store_true", help="Skip per-object stat for referenced files")
    args = parser.parse_args()
    try:
        result = audit(args.root, args.compare_root, fast=args.fast)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not result["errors"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
