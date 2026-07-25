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
    DEFAULT_CHUNK_SIZE_KIB,
    DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    SnapshotError,
    fetch_snapshot,
    initialize_layout,
    publish_snapshot,
    resolve_latest,
    resolve_snapshot_id,
    verify_snapshot,
    write_pin,
)


def _sync_root(value: str | None) -> Path:
    selected = value or os.environ.get("STOCKAGENT_SYNC_ROOT")
    if not selected:
        raise SnapshotError("provide --sync-root or STOCKAGENT_SYNC_ROOT")
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
        return resolve_snapshot_id(root, args.dataset, args.snapshot_id)
    return resolve_latest(
        root,
        args.dataset,
        max_clock_skew_seconds=args.max_clock_skew_seconds,
    )


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Multi-writer immutable dataset snapshots using desync + Syncthing"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="initialize a node-local sync layout")
    init.add_argument("--sync-root")
    init.add_argument("--node-id")
    init.add_argument("--replace-ignore", action="store_true")
    init.add_argument("--replace-node-id", action="store_true")

    publish = subparsers.add_parser("publish", help="publish an immutable snapshot")
    publish.add_argument("dataset")
    publish.add_argument("source", type=Path)
    publish.add_argument("--sync-root")
    publish.add_argument("--node-id")
    publish.add_argument("--desync-bin")
    publish.add_argument("--chunk-size-kib", default=DEFAULT_CHUNK_SIZE_KIB)
    publish.add_argument("--preserve-times", action="store_true")
    publish.add_argument("--metadata", action="append", default=[])
    publish.add_argument(
        "--max-clock-skew-seconds",
        type=int,
        default=DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    )

    resolve = subparsers.add_parser("resolve", help="resolve deterministic latest")
    resolve.add_argument("dataset")
    resolve.add_argument("--snapshot-id")
    resolve.add_argument("--sync-root")
    resolve.add_argument("--pin", type=Path)
    resolve.add_argument(
        "--max-clock-skew-seconds",
        type=int,
        default=DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    )

    fetch = subparsers.add_parser("fetch", help="materialize and verify a snapshot")
    fetch.add_argument("dataset")
    fetch.add_argument("--snapshot-id")
    fetch.add_argument("--sync-root")
    fetch.add_argument("--materialized-root")
    fetch.add_argument("--desync-bin")
    fetch.add_argument("--pin", type=Path)
    fetch.add_argument(
        "--max-clock-skew-seconds",
        type=int,
        default=DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    )

    verify = subparsers.add_parser("verify", help="verify metadata, index and chunks")
    verify.add_argument("dataset")
    verify.add_argument("--snapshot-id")
    verify.add_argument("--sync-root")
    verify.add_argument("--desync-bin")
    verify.add_argument("--materialized", type=Path)
    verify.add_argument(
        "--max-clock-skew-seconds",
        type=int,
        default=DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    )

    status = subparsers.add_parser("status", help="show current winner and diagnostics")
    status.add_argument("dataset")
    status.add_argument("--sync-root")
    status.add_argument(
        "--max-clock-skew-seconds",
        type=int,
        default=DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "init":
            root = _sync_root(args.sync_root)
            node_id = initialize_layout(
                root,
                node_id=args.node_id,
                replace_ignore=args.replace_ignore,
                replace_node_id=args.replace_node_id,
            )
            _print({"sync_root": str(root.resolve()), "node_id": node_id})
            return 0
        if args.command == "publish":
            root = _sync_root(args.sync_root)
            resolved = publish_snapshot(
                root,
                args.dataset,
                args.source,
                node_id=args.node_id,
                desync_binary=args.desync_bin,
                chunk_size_kib=args.chunk_size_kib,
                preserve_times=args.preserve_times,
                max_clock_skew_seconds=args.max_clock_skew_seconds,
                metadata=_metadata(args.metadata),
                repo_root=REPO_ROOT,
            )
            _print(
                {
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "manifest": str(resolved.manifest_path),
                    "manifest_sha256": resolved.manifest_sha256,
                    "head": str(resolved.head_path),
                }
            )
            return 0
        if args.command in {"resolve", "status"}:
            resolved = _resolved(args)
            if getattr(args, "pin", None):
                write_pin(args.pin, resolved)
            _print(
                {
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "manifest": resolved.manifest,
                    "manifest_sha256": resolved.manifest_sha256,
                    "head": str(resolved.head_path) if resolved.head_path else None,
                    "ignored_invalid_heads": list(resolved.diagnostics),
                }
            )
            return 0
        if args.command == "fetch":
            root = _sync_root(args.sync_root)
            resolved = _resolved(args)
            target = fetch_snapshot(
                root,
                _materialized_root(args.materialized_root),
                resolved,
                desync_binary=args.desync_bin,
            )
            if args.pin:
                write_pin(args.pin, resolved)
            _print(
                {
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "materialized_path": str(target),
                    "manifest_sha256": resolved.manifest_sha256,
                }
            )
            return 0
        if args.command == "verify":
            root = _sync_root(args.sync_root)
            resolved = _resolved(args)
            _print(
                verify_snapshot(
                    root,
                    resolved,
                    desync_binary=args.desync_bin,
                    materialized_path=args.materialized,
                )
            )
            return 0
    except (OSError, SnapshotError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
