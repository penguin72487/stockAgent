#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path, PurePosixPath
import sys
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    sha256_file,
)
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    initialize_packed_layout,
    publish_packed_snapshot,
    resolve_latest_packed,
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


def _source_freshness(entry: Mapping[str, Any]) -> dict[str, Any] | None:
    config = entry.get("freshness")
    if config is None:
        return None
    if not isinstance(config, Mapping):
        raise SnapshotError(f"dataset {entry['dataset']} freshness must be an object")
    relative = PurePosixPath(str(config.get("receipt", "")))
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotError(f"invalid freshness receipt path: {relative}")
    receipt_path = _source_path(entry).joinpath(*relative.parts)
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(
            f"cannot read freshness receipt {receipt_path}: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise SnapshotError(f"freshness receipt must be an object: {receipt_path}")
    field = str(config.get("field", ""))
    value = str(payload.get(field, ""))
    if config.get("format") != "iso-date":
        raise SnapshotError(f"unsupported freshness format for {entry['dataset']}")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise SnapshotError(
            f"freshness field {field} is not an ISO date: {value!r}"
        ) from exc
    required = config.get("required_values", {})
    if not isinstance(required, Mapping):
        raise SnapshotError("freshness required_values must be an object")
    mismatches = {
        str(key): {"expected": expected, "actual": payload.get(key)}
        for key, expected in required.items()
        if payload.get(key) != expected
    }
    if mismatches:
        raise SnapshotError(
            f"freshness receipt completion gate failed: {mismatches}"
        )
    return {
        "field": field,
        "format": "iso-date",
        "receipt": relative.as_posix(),
        "receipt_sha256": sha256_file(receipt_path),
        "value": value,
    }


def _latest_cold_freshness(
    sync_root: Path | None, dataset: str
) -> dict[str, Any] | None:
    if sync_root is None:
        return None
    try:
        resolved = resolve_latest_packed(sync_root, dataset)
    except SnapshotError:
        return None
    metadata = resolved.manifest.get("metadata", {})
    value = metadata.get("freshness_value") if isinstance(metadata, Mapping) else None
    if not value:
        return None
    return {
        "snapshot_id": resolved.manifest["snapshot_id"],
        "field": metadata.get("freshness_field"),
        "format": metadata.get("freshness_format"),
        "value": str(value),
    }


def _status(
    entry: Mapping[str, Any],
    commands: list[tuple[int, str]],
    *,
    sync_root: Path | None = None,
) -> dict[str, Any]:
    source = _source_path(entry)
    blockers = _blockers(entry, commands)
    freshness_error: str | None = None
    try:
        source_freshness = _source_freshness(entry)
    except SnapshotError as exc:
        source_freshness = None
        freshness_error = str(exc)
    cold_freshness = _latest_cold_freshness(
        sync_root, str(entry["dataset"])
    )
    non_regression = (
        source_freshness is None
        or cold_freshness is None
        or str(source_freshness["value"]) >= str(cold_freshness["value"])
    )
    return {
        "dataset": entry["dataset"],
        "role": entry["role"],
        "source": str(source),
        "source_exists": source.exists(),
        "source_resolved": str(source.resolve()) if source.exists() else None,
        "publish": bool(entry["publish"]),
        "publish_ready": bool(entry["publish"])
        and source.is_dir()
        and not blockers
        and freshness_error is None
        and non_regression,
        "active_blockers": blockers,
        "source_freshness": source_freshness,
        "latest_cold_freshness": cold_freshness,
        "freshness_non_regression": non_regression,
        "freshness_error": freshness_error,
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
        status_sync_root = args.sync_root.resolve() if args.sync_root else None
        if args.command == "status":
            selected = entries
            if args.dataset:
                selected = [item for item in entries if item["dataset"] == args.dataset]
                if not selected:
                    raise SnapshotError(f"unknown catalog dataset: {args.dataset}")
            print(
                json.dumps(
                    {
                        "schema_version": 1,
                        "datasets": [
                            _status(item, commands, sync_root=status_sync_root)
                            for item in selected
                        ],
                    },
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
            status = _status(entry, commands, sync_root=sync_root)
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
                metadata={
                    "role": str(entry["role"]),
                    "catalog_schema": "1",
                    **(
                        {
                            "freshness_field": str(
                                status["source_freshness"]["field"]
                            ),
                            "freshness_format": str(
                                status["source_freshness"]["format"]
                            ),
                            "freshness_receipt": str(
                                status["source_freshness"]["receipt"]
                            ),
                            "freshness_receipt_sha256": str(
                                status["source_freshness"]["receipt_sha256"]
                            ),
                            "freshness_value": str(
                                status["source_freshness"]["value"]
                            ),
                        }
                        if status["source_freshness"] is not None
                        else {}
                    ),
                },
                repo_root=REPO_ROOT,
            )
            results.append(
                {
                    "dataset": entry["dataset"],
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "inventory_sha256": resolved.manifest["archive"]["inventory"]["sha256"],
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
