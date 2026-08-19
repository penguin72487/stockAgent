#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    initialize_packed_layout,
    publish_packed_snapshot,
)

DEFAULT_CATALOG = REPO_ROOT / "configs" / "data_sync" / "packed_datasets.json"


def _load_catalog(path: Path) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read data release catalog {path}: {exc}") from exc
    if not isinstance(value, dict) or int(value.get("schema_version", -1)) != 1:
        raise SnapshotError(f"unsupported data release catalog: {path}")
    datasets = value.get("datasets")
    if not isinstance(datasets, list):
        raise SnapshotError("catalog datasets must be a list")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in datasets:
        if not isinstance(raw, dict):
            raise SnapshotError("catalog dataset entry must be an object")
        dataset = str(raw.get("dataset", ""))
        if not dataset or dataset in seen:
            raise SnapshotError(f"duplicate or empty catalog dataset: {dataset!r}")
        seen.add(dataset)
        result.append(raw)
    return result


def _running_commands() -> list[tuple[int, str]]:
    commands: list[tuple[int, str]] = []
    own_pid = os.getpid()
    for path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            pid = int(path.parent.name)
            if pid == own_pid:
                continue
            command = path.read_bytes().replace(b"\0", b" ").decode(
                "utf-8", errors="replace"
            )
        except (OSError, ValueError):
            continue
        if command:
            commands.append((pid, command))
    return commands


def _blockers(entry: Mapping[str, Any], commands: list[tuple[int, str]]) -> list[dict[str, Any]]:
    patterns = [str(item) for item in entry.get("active_process_substrings", [])]
    return [
        {"pid": pid, "pattern": pattern, "command": command}
        for pid, command in commands
        for pattern in patterns
        if pattern in command
    ]


def _source_path(entry: Mapping[str, Any]) -> Path:
    source = Path(str(entry["source"]))
    return source if source.is_absolute() else REPO_ROOT / source


def _status(entry: Mapping[str, Any], commands: list[tuple[int, str]]) -> dict[str, Any]:
    source = _source_path(entry)
    blockers = _blockers(entry, commands)
    return {
        "dataset": entry["dataset"],
        "role": entry["role"],
        "source": str(source),
        "source_exists": source.exists(),
        "source_resolved": str(source.resolve()) if source.exists() else None,
        "publish": bool(entry["publish"]),
        "publish_ready": bool(entry["publish"]) and source.is_dir() and not blockers,
        "active_blockers": blockers,
        "note": entry["note"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Publish catalogued canonical datasets through the packed Syncthing "
            "release layer. Downloader work shards are never selected implicitly."
        )
    )
    parser.add_argument("command", choices=("status", "publish"))
    parser.add_argument("dataset", nargs="?")
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--sync-root", type=Path)
    parser.add_argument("--node-id")
    parser.add_argument(
        "--all-ready",
        action="store_true",
        help="Publish every catalog entry that is present, publishable and inactive.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        entries = _load_catalog(args.catalog.resolve())
        commands = _running_commands()
        if args.command == "status":
            selected = entries
            if args.dataset:
                selected = [item for item in entries if item["dataset"] == args.dataset]
                if not selected:
                    raise SnapshotError(f"unknown catalog dataset: {args.dataset}")
            print(
                json.dumps(
                    {"schema_version": 1, "datasets": [_status(item, commands) for item in selected]},
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.sync_root is None:
            raise SnapshotError("publish requires --sync-root")
        if bool(args.dataset) == bool(args.all_ready):
            raise SnapshotError("select exactly one dataset or --all-ready")
        selected = (
            [item for item in entries if item["dataset"] == args.dataset]
            if args.dataset
            else entries
        )
        if not selected:
            raise SnapshotError(f"unknown catalog dataset: {args.dataset}")
        sync_root = args.sync_root.resolve()
        initialize_packed_layout(sync_root, node_id=args.node_id)
        results: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for entry in selected:
            status = _status(entry, commands)
            if not status["publish_ready"]:
                if args.all_ready:
                    skipped.append(status)
                    continue
                raise SnapshotError(
                    f"dataset {entry['dataset']} is not publish-ready: "
                    f"publish={entry['publish']} source_exists={status['source_exists']} "
                    f"active_blockers={status['active_blockers']}"
                )
            resolved = publish_packed_snapshot(
                sync_root,
                str(entry["dataset"]),
                _source_path(entry),
                node_id=args.node_id,
                loose_file_threshold_bytes=int(entry["loose_threshold_mib"])
                * 1024
                * 1024,
                pack_buckets=int(entry["pack_buckets"]),
                excluded_subtrees=[
                    str(value) for value in entry.get("excluded_subtrees", [])
                ],
                metadata={"role": str(entry["role"]), "catalog_schema": "1"},
                repo_root=REPO_ROOT,
            )
            results.append(
                {
                    "dataset": entry["dataset"],
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "source_files": resolved.manifest["source"]["files"],
                    "source_bytes": resolved.manifest["source"]["logical_bytes"],
                    "sync_objects": resolved.manifest["archive"]["object_count"] + 1,
                    "sync_bytes": resolved.manifest["archive"]["stored_bytes"],
                    "base_snapshot_id": resolved.manifest["archive"].get(
                        "base_snapshot_id"
                    ),
                    "reused_files": resolved.manifest["archive"].get(
                        "reused_files", 0
                    ),
                    "changed_files": resolved.manifest["archive"].get(
                        "changed_files"
                    ),
                    "new_sync_objects": resolved.manifest["archive"].get(
                        "new_object_count"
                    ),
                    "new_sync_bytes": resolved.manifest["archive"].get(
                        "new_stored_bytes"
                    ),
                }
            )
        print(
            json.dumps(
                {"published": results, "skipped": skipped},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (OSError, SnapshotError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
