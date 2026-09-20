#!/usr/bin/env python3
"""Publish and activate verified small-file cold artifact releases."""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
import subprocess
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
from stockagent.data_sync.artifact_retirement import (  # noqa: E402
    apply_artifact_retirement,
    load_retirement_peer_names,
    plan_artifact_retirement,
)
from stockagent.data_sync.materialized_cache import (  # noqa: E402
    use_materialized_snapshot,
)
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    resolve_latest_packed,
)
from stockagent.data_sync.packed_retention import RetentionConfig  # noqa: E402


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
        "--live-sync-root", type=Path, default=Path("/srv/stockagent-artifacts-hot")
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        default=Path("/var/lib/stockagent-cold-artifacts"),
    )
    parser.add_argument(
        "--materialized-root",
        type=Path,
        default=Path("/srv/stockagent-packed-materialized"),
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
    use = subparsers.add_parser(
        "use", help="restore one retired full run with a seven-day managed lease"
    )
    use.add_argument("dataset")
    use.add_argument("--path-only", action="store_true")
    use.add_argument("--verify", action="store_true")
    retire = subparsers.add_parser(
        "retire", help="audit or retire one D-backed complete run from both hot roots"
    )
    retire.add_argument("dataset")
    retire.add_argument("--apply", action="store_true")
    retire.add_argument("--plan-fingerprint")
    retire.add_argument("--retention-days", type=float, default=7.0)
    retire.add_argument(
        "--retention-config",
        type=Path,
        default=REPO_ROOT / "configs/data_sync/packed_retention.json",
    )
    retire.add_argument(
        "--retirement-policy",
        type=Path,
        default=REPO_ROOT / "configs/data_sync/artifact_retirement.json",
    )
    subparsers.add_parser("rebuild-ignore")
    return parser


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _bridge_inactive(hot_root: Path) -> bool:
    try:
        service = subprocess.run(
            ["systemctl", "is-active", "stockagent-hot-artifact-sync.service"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        service = None
    if service is not None and service.stdout.strip() == "active":
        return False
    for process in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            command = process.read_bytes()
        except OSError:
            continue
        if b"run_live_artifact_sync.py" in command and str(hot_root).encode() in command:
            return False
    return True


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
                retirement_path = (
                    args.state_root / "retirements" / f"{dataset}.json"
                )
                retirement = (
                    json.loads(retirement_path.read_text(encoding="utf-8"))
                    if retirement_path.exists()
                    else None
                )
                rows.append(
                    {"source": source, "release": release, "retirement": retirement}
                )
            _print(rows)
            return 0
        spec = registry[args.dataset]
        if args.command == "use":
            if spec.maximum_file_bytes is not None:
                raise SnapshotError("partial cold artifact releases cannot restore a whole run")
            state_path = args.state_root / "retirements" / f"{spec.dataset}.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if (
                state.get("state") != "cold-only"
                or state.get("artifact_relative_root") != spec.relative_root
            ):
                raise SnapshotError("artifact is not a managed cold-only run")
            destination = args.artifact_root / spec.relative_root
            if destination.exists() and not destination.is_symlink():
                raise SnapshotError("refusing to replace a writable artifact directory")
            if destination.is_symlink():
                current = destination.resolve(strict=False)
                managed = (args.materialized_root / spec.dataset).resolve(strict=False)
                if current != managed and managed not in current.parents:
                    raise SnapshotError("refusing to replace an unrelated artifact symlink")
            lease = use_materialized_snapshot(
                args.sync_root,
                args.materialized_root,
                spec.dataset,
                snapshot_id=str(state["snapshot_id"]),
                ttl_days=7.0,
                links=[destination],
                verify_existing=args.verify,
            )
            if args.path_only:
                print(destination)
            else:
                _print(lease)
            return 0
        if args.command == "retire":
            if args.apply and not args.plan_fingerprint:
                raise SnapshotError("--apply requires the exact dry-run --plan-fingerprint")
            if args.plan_fingerprint and not args.apply:
                raise SnapshotError("--plan-fingerprint requires --apply")
            cfg = RetentionConfig.load(args.retention_config, repo_root=REPO_ROOT)
            if (
                cfg.sync_root.resolve() != args.sync_root.resolve()
                or cfg.materialized_root.resolve() != args.materialized_root.resolve()
            ):
                raise SnapshotError("retirement paths differ from authority retention config")
            required_peers = load_retirement_peer_names(
                args.retirement_policy, authority_node_id=cfg.authority_node_id
            )
            from manage_packed_retention import _syncthing

            try:
                peer_proof = _syncthing(replace(cfg, required_peer_names=required_peers))
            except (OSError, ValueError) as exc:
                peer_proof = {"ok": False, "error": str(exc)}
            options = {
                "artifact_root": args.artifact_root,
                "hot_root": args.live_sync_root,
                "sync_root": args.sync_root,
                "materialized_root": args.materialized_root,
                "state_root": args.state_root,
                "backup_config": cfg.backup_config,
                "peer_proof": peer_proof,
                "required_peer_names": required_peers,
                "bridge_inactive": _bridge_inactive(args.live_sync_root),
                "retention_days": args.retention_days,
            }
            result = (
                apply_artifact_retirement(
                    spec, expected_fingerprint=args.plan_fingerprint, **options
                )
                if args.apply
                else plan_artifact_retirement(spec, **options)
            )
            _print(result)
            return 0
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
