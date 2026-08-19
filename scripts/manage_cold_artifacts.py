#!/usr/bin/env python3
"""Publish and activate verified small-file cold artifact releases."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.cold_artifacts import (  # noqa: E402
    activate_cold_artifact,
    load_cold_artifact_registry,
    publish_cold_artifact,
    rebuild_cold_ignore,
    validate_cold_artifact_source,
)
from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    resolve_latest_packed,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        type=Path,
        default=REPO_ROOT / "configs/data_sync/cold_artifacts.json",
    )
    parser.add_argument("--artifact-root", type=Path, default=REPO_ROOT / "artifacts")
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument(
        "--live-sync-root", type=Path, default=Path("/srv/stockagent-artifacts-live")
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        default=Path("/var/lib/stockagent-cold-artifacts"),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status")
    status.add_argument("dataset", nargs="?")
    publish = subparsers.add_parser("publish")
    publish.add_argument("dataset")
    publish.add_argument("--node-id")
    activate = subparsers.add_parser("activate")
    activate.add_argument("dataset")
    activate.add_argument(
        "--conflict-policy",
        choices=("fail", "local-wins", "packed-wins"),
        default="fail",
    )
    subparsers.add_parser("rebuild-ignore")
    return parser


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def main() -> int:
    args = build_parser().parse_args()
    try:
        registry = load_cold_artifact_registry(args.registry)
        if args.command == "rebuild-ignore":
            include = rebuild_cold_ignore(
                args.live_sync_root,
                args.state_root / "activations",
            )
            _print(
                {
                    "live_sync_root": str(args.live_sync_root.resolve()),
                    "include": str(include),
                    "patterns": sum(
                        1
                        for line in include.read_text(encoding="utf-8").splitlines()
                        if line.startswith("(?d)/")
                    ),
                }
            )
            return 0
        selected = [args.dataset] if getattr(args, "dataset", None) else sorted(registry)
        missing = [dataset for dataset in selected if dataset not in registry]
        if missing:
            raise SnapshotError(f"unknown cold artifact dataset: {missing[0]}")
        if args.command == "status":
            rows = []
            for dataset in selected:
                spec = registry[dataset]
                try:
                    source = validate_cold_artifact_source(args.artifact_root, spec)
                except SnapshotError as exc:
                    source = {"dataset": dataset, "contract_ok": False, "error": str(exc)}
                try:
                    resolved = resolve_latest_packed(args.sync_root, dataset)
                    release = {
                        "snapshot_id": resolved.manifest["snapshot_id"],
                        "manifest_sha256": resolved.manifest_sha256,
                        "objects": resolved.manifest["archive"]["object_count"],
                        "stored_bytes": resolved.manifest["archive"]["stored_bytes"],
                    }
                except SnapshotError:
                    release = None
                rows.append({"source": source, "release": release})
            _print(rows)
            return 0
        spec = registry[args.dataset]
        if args.command == "publish":
            resolved = publish_cold_artifact(
                args.sync_root,
                args.artifact_root,
                spec,
                node_id=args.node_id,
                repo_root=REPO_ROOT,
            )
            _print(
                {
                    "dataset": spec.dataset,
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "manifest_sha256": resolved.manifest_sha256,
                    "source": resolved.manifest["source"],
                    "archive": resolved.manifest["archive"],
                }
            )
            return 0
        if args.command == "activate":
            receipt = activate_cold_artifact(
                args.sync_root,
                args.artifact_root,
                args.live_sync_root,
                args.state_root,
                spec,
                conflict_policy=args.conflict_policy,
            )
            _print(
                {
                    key: value
                    for key, value in receipt.items()
                    if key != "ignored_file_paths"
                }
                | {"ignored_files": len(receipt["ignored_file_paths"])}
            )
            return 0
    except (OSError, SnapshotError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
