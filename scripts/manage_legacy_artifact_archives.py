#!/usr/bin/env python3
"""Preserve legacy market artifacts as non-deployable compressed cold archives."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402
from stockagent.data_sync.desync_snapshots import sha256_file  # noqa: E402
from stockagent.data_sync.legacy_artifact_archive import (  # noqa: E402
    load_legacy_specs,
    prepare_archive,
    publish_archive,
    restore_archive,
    verify_archive_directory,
)
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    resolve_latest_packed,
    verify_packed_snapshot,
)
from stockagent.data_sync.legacy_artifact_retirement import (  # noqa: E402
    apply_legacy_retirement,
    plan_legacy_retirement,
    renew_legacy_lease,
)
from stockagent.data_sync.packed_retention import RetentionConfig  # noqa: E402
from scripts.manage_cold_artifacts import _bridge_inactive  # noqa: E402
from scripts.manage_packed_retention import _syncthing  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("plan", "prepare", "publish", "verify", "restore", "renew", "retire-plan", "retire-apply"),
    )
    parser.add_argument("dataset")
    parser.add_argument(
        "--catalog", type=Path, default=REPO_ROOT / "configs/data_sync/legacy_artifact_archives.json"
    )
    parser.add_argument("--artifact-root", type=Path, default=REPO_ROOT / "artifacts")
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--plan-fingerprint")
    parser.add_argument(
        "--retention-config", type=Path, default=REPO_ROOT / "configs/data_sync/packed_retention.json"
    )
    parser.add_argument(
        "--retirement-state-root", type=Path, default=Path("/var/lib/stockagent-legacy-artifacts")
    )
    parser.add_argument(
        "--restore-cache", type=Path, default=Path("/mnt/d/stockagent-legacy-archive-restore")
    )
    args = parser.parse_args()
    try:
        specs = load_legacy_specs(args.catalog)
        if args.dataset not in specs:
            raise SnapshotError(f"legacy dataset is not allowlisted: {args.dataset}")
        spec = specs[args.dataset]
        source = args.artifact_root.resolve() / spec.relative_root
        archive = spec.stage_root / spec.dataset / "archive"
        if args.command == "plan":
            from stockagent.data_sync.legacy_artifact_archive import source_plan

            rows = source_plan(source, spec)
            result = {
                "dataset": spec.dataset,
                "relative_root": spec.relative_root,
                "deployable": False,
                "files": len(rows),
                "source_bytes": sum(row["source"]["size"] for row in rows),
                "stage": str(archive),
                "source_unchanged_days": spec.minimum_stable_days,
            }
        elif args.command == "prepare":
            result = prepare_archive(spec, args.artifact_root)
            result = {
                "dataset": spec.dataset,
                "files": len(result["files"]),
                "stage_bytes": sum(row["encoded_size"] for row in result["files"]),
                "stage": str(archive),
                "deployable": False,
            }
        elif args.command == "publish":
            result = publish_archive(
                spec, args.artifact_root, args.sync_root, repo_root=REPO_ROOT
            )
        elif args.command == "verify":
            result = verify_archive_directory(archive, source, spec=spec)
            result.pop("manifest")
            resolved = resolve_latest_packed(args.sync_root, spec.dataset)
            metadata = resolved.manifest.get("metadata", {})
            if (
                metadata.get("transport_role") != "legacy-quarantine-archive"
                or metadata.get("deployable") != "false"
                or metadata.get("source_relative_root") != spec.relative_root
                or metadata.get("legacy_manifest_sha256")
                != sha256_file(archive / "legacy_archive_manifest.json")
            ):
                raise SnapshotError("cold release does not match the allowlisted legacy archive")
            verify_packed_snapshot(args.sync_root, resolved, materialized_path=archive)
            result.update(snapshot_id=resolved.manifest["snapshot_id"], cold_verified=True)
        elif args.command == "renew":
            result = renew_legacy_lease(
                spec,
                artifact_root=args.artifact_root,
                state_root=args.retirement_state_root,
            )
        elif args.command in {"retire-plan", "retire-apply"}:
            if args.command == "retire-apply" and not args.plan_fingerprint:
                raise SnapshotError("retire-apply requires --plan-fingerprint from retire-plan")
            cfg = RetentionConfig.load(args.retention_config, repo_root=REPO_ROOT)
            if cfg.sync_root.resolve() != args.sync_root.resolve():
                raise SnapshotError("retirement C root differs from configured cold authority")
            options = {
                "repo_root": REPO_ROOT,
                "artifact_root": args.artifact_root,
                "hot_root": Path("/srv/stockagent-artifacts-hot"),
                "sync_root": args.sync_root,
                "state_root": args.retirement_state_root,
                "activation_root": Path("/var/lib/stockagent-cold-artifacts/activations"),
                "backup_config": cfg.backup_config,
                "peer_proof": _syncthing(cfg),
                "peer_probe": lambda: _syncthing(cfg),
                "bridge_inactive": _bridge_inactive(Path("/srv/stockagent-artifacts-hot")),
            }
            result = (
                apply_legacy_retirement(
                    spec, expected_fingerprint=args.plan_fingerprint, **options
                )
                if args.command == "retire-apply"
                else plan_legacy_retirement(spec, **options)
            )
        else:
            if args.destination is None:
                raise SnapshotError("restore requires --destination")
            result = restore_archive(
                spec,
                args.sync_root,
                args.destination,
                materialized_root=args.restore_cache,
            )
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, SnapshotError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
