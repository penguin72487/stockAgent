#!/usr/bin/env python3
"""CLI for immutable, Syncthing-transported desync snapshots."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    fetch_snapshot,
    init_sync_root,
    publish_snapshot,
    resolve_status,
    verify_snapshot,
)


def _metadata(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise argparse.ArgumentTypeError(f"metadata must be KEY=VALUE: {value!r}")
        key, item = value.split("=", 1)
        if not key:
            raise argparse.ArgumentTypeError("metadata key cannot be empty")
        result[key] = item
    return result


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--desync-bin", default="desync")
    commands = root.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("--sync-root", type=Path, required=True)
    init.add_argument("--node-id", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("dataset")
    publish.add_argument("source", type=Path)
    publish.add_argument("--sync-root", type=Path, required=True)
    publish.add_argument("--metadata", action="append", default=[])
    for name in ("status", "verify"):
        sub = commands.add_parser(name)
        sub.add_argument("dataset")
        sub.add_argument("--sync-root", type=Path, required=True)
    fetch = commands.add_parser("fetch")
    fetch.add_argument("dataset")
    fetch.add_argument("--sync-root", type=Path, required=True)
    fetch.add_argument("--materialized-root", type=Path, required=True)
    fetch.add_argument("--pin", type=Path)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    template = repo_root / "deploy" / "syncthing" / "stockagent-desync.stignore"
    try:
        if args.command == "init":
            value = init_sync_root(args.sync_root, args.node_id, template)
        elif args.command == "publish":
            value = publish_snapshot(
                args.dataset,
                args.source,
                args.sync_root,
                _metadata(args.metadata),
                args.desync_bin,
            )
        elif args.command == "status":
            value = resolve_status(args.dataset, args.sync_root, args.desync_bin)
        elif args.command == "fetch":
            value = fetch_snapshot(
                args.dataset, args.sync_root, args.materialized_root, args.pin, args.desync_bin
            )
        else:
            value = verify_snapshot(args.dataset, args.sync_root, args.desync_bin)
    except (SnapshotError, OSError, ValueError, argparse.ArgumentTypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
