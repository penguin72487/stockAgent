#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    SnapshotError,
)
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    DEFAULT_COMPRESSION_LEVEL,
    DEFAULT_LOOSE_FILE_THRESHOLD_BYTES,
    DEFAULT_PACK_BUCKETS,
    fetch_packed_snapshot,
    initialize_packed_layout,
    publish_packed_snapshot,
    referenced_packed_objects,
    resolve_latest_packed,
    resolve_packed_snapshot_id,
    verify_packed_snapshot,
    write_packed_pin,
)


def _sync_root(value: str | None) -> Path:
    selected = value or os.environ.get("STOCKAGENT_PACKED_SYNC_ROOT")
    if not selected:
        raise SnapshotError(
            "provide --sync-root or STOCKAGENT_PACKED_SYNC_ROOT"
        )
    return Path(selected).expanduser()


def _materialized_root(value: str | None) -> Path:
    selected = value or os.environ.get("STOCKAGENT_MATERIALIZED_ROOT")
    if not selected:
        raise SnapshotError(
            "provide --materialized-root or STOCKAGENT_MATERIALIZED_ROOT"
        )
    return Path(selected).expanduser()


def _metadata(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key.strip():
            raise SnapshotError(f"metadata must use KEY=VALUE: {value!r}")
        result[key.strip()] = item
    return result


def _resolved(args: argparse.Namespace):
    root = _sync_root(args.sync_root)
    if getattr(args, "snapshot_id", None):
        return resolve_packed_snapshot_id(root, args.dataset, args.snapshot_id)
    return resolve_latest_packed(
        root,
        args.dataset,
        max_clock_skew_seconds=args.max_clock_skew_seconds,
    )


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _add_resolution_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("dataset")
    parser.add_argument("--snapshot-id")
    parser.add_argument("--sync-root")
    parser.add_argument(
        "--max-clock-skew-seconds",
        type=int,
        default=DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Publish small files as stable path-hash ZIP packs and large files as "
            "content-addressed blobs for Syncthing"
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="initialize a packed sync root")
    init.add_argument("--sync-root")
    init.add_argument("--node-id")
    init.add_argument("--replace-ignore", action="store_true")
    init.add_argument("--replace-node-id", action="store_true")

    publish = subparsers.add_parser("publish", help="publish an immutable packed snapshot")
    publish.add_argument("dataset")
    publish.add_argument("source", type=Path)
    publish.add_argument("--sync-root")
    publish.add_argument("--node-id")
    publish.add_argument(
        "--loose-threshold-mib",
        type=int,
        default=DEFAULT_LOOSE_FILE_THRESHOLD_BYTES // (1024 * 1024),
    )
    publish.add_argument("--pack-buckets", type=int, default=DEFAULT_PACK_BUCKETS)
    publish.add_argument(
        "--compression-level", type=int, default=DEFAULT_COMPRESSION_LEVEL
    )
    publish.add_argument("--metadata", action="append", default=[])
    publish.add_argument(
        "--max-clock-skew-seconds",
        type=int,
        default=DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    )

    resolve = subparsers.add_parser("resolve", help="resolve deterministic latest")
    _add_resolution_arguments(resolve)
    resolve.add_argument("--pin", type=Path)

    status = subparsers.add_parser("status", help="show the current winner and size")
    _add_resolution_arguments(status)

    verify = subparsers.add_parser("verify", help="verify all object and file metadata")
    _add_resolution_arguments(verify)
    verify.add_argument("--materialized", type=Path)

    fetch = subparsers.add_parser("fetch", help="atomically materialize a snapshot")
    _add_resolution_arguments(fetch)
    fetch.add_argument("--materialized-root")
    fetch.add_argument("--pin", type=Path)

    objects = subparsers.add_parser(
        "objects", help="count stored and manifest-referenced objects; never deletes"
    )
    objects.add_argument("--sync-root")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "init":
            root = _sync_root(args.sync_root)
            node_id = initialize_packed_layout(
                root,
                node_id=args.node_id,
                replace_ignore=args.replace_ignore,
                replace_node_id=args.replace_node_id,
            )
            _print({"sync_root": str(root.resolve()), "node_id": node_id})
            return 0
        if args.command == "publish":
            root = _sync_root(args.sync_root)
            resolved = publish_packed_snapshot(
                root,
                args.dataset,
                args.source,
                node_id=args.node_id,
                loose_file_threshold_bytes=args.loose_threshold_mib * 1024 * 1024,
                pack_buckets=args.pack_buckets,
                compression_level=args.compression_level,
                max_clock_skew_seconds=args.max_clock_skew_seconds,
                metadata=_metadata(args.metadata),
                repo_root=REPO_ROOT,
            )
            archive = resolved.manifest["archive"]
            _print(
                {
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "manifest": str(resolved.manifest_path),
                    "manifest_sha256": resolved.manifest_sha256,
                    "head": str(resolved.head_path),
                    "source_files": resolved.manifest["source"]["files"],
                    "source_bytes": resolved.manifest["source"]["logical_bytes"],
                    "sync_objects": archive["object_count"] + 1,
                    "sync_bytes": archive["stored_bytes"],
                }
            )
            return 0
        if args.command in {"resolve", "status"}:
            resolved = _resolved(args)
            if getattr(args, "pin", None):
                write_packed_pin(args.pin, resolved)
            archive = resolved.manifest["archive"]
            _print(
                {
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "publisher": resolved.manifest["publisher"]["node_id"],
                    "manifest_sha256": resolved.manifest_sha256,
                    "source": resolved.manifest["source"],
                    "sync_objects": archive["object_count"] + 1,
                    "sync_bytes": archive["stored_bytes"],
                    "ignored_invalid_heads": list(resolved.diagnostics),
                }
            )
            return 0
        if args.command == "verify":
            root = _sync_root(args.sync_root)
            _print(
                verify_packed_snapshot(
                    root,
                    _resolved(args),
                    materialized_path=args.materialized,
                )
            )
            return 0
        if args.command == "fetch":
            root = _sync_root(args.sync_root)
            resolved = _resolved(args)
            target = fetch_packed_snapshot(
                root, _materialized_root(args.materialized_root), resolved
            )
            if args.pin:
                write_packed_pin(args.pin, resolved)
            _print(
                {
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "materialized_path": str(target),
                    "manifest_sha256": resolved.manifest_sha256,
                }
            )
            return 0
        if args.command == "objects":
            root = _sync_root(args.sync_root).resolve()
            stored = {
                path.resolve()
                for path in (root / "objects").glob("*/*/*")
                if path.is_file()
            }
            referenced = referenced_packed_objects(root)
            _print(
                {
                    "stored_objects": len(stored),
                    "referenced_objects": len(referenced),
                    "unreferenced_objects": len(stored - referenced),
                    "unreferenced_bytes": sum(
                        path.stat().st_size for path in stored - referenced
                    ),
                    "deleted": 0,
                }
            )
            return 0
    except (OSError, SnapshotError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

