#!/usr/bin/env python3
"""Retire only explicitly allowlisted, already-enrolled complete hot runs."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.manage_cold_artifacts import _bridge_inactive  # noqa: E402
from scripts.manage_packed_retention import _syncthing  # noqa: E402
from stockagent.data_sync.artifact_retirement import (  # noqa: E402
    apply_artifact_retirement,
    load_retirement_peer_names,
    plan_artifact_retirement,
)
from stockagent.data_sync.cold_artifacts import load_cold_artifact_registry  # noqa: E402
from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    _utc_iso_from_ns,
)
from stockagent.data_sync.legacy_artifact_archive import load_legacy_specs  # noqa: E402
from stockagent.data_sync.legacy_artifact_retirement import (  # noqa: E402
    apply_legacy_retirement,
    plan_legacy_retirement,
)
from stockagent.data_sync.packed_retention import RetentionConfig  # noqa: E402


def scheduled_specs(policy_path: Path, registry_path: Path, authority: str):
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != 1 or policy.get("authority_node_id") != authority:
        raise SnapshotError("scheduled artifact policy authority/schema mismatch")
    names = policy.get("scheduled_datasets")
    if not isinstance(names, list) or not names or any(
        not isinstance(name, str) or not name for name in names
    ) or len(names) != len(set(names)):
        raise SnapshotError("scheduled artifact dataset allowlist is invalid")
    registry = load_cold_artifact_registry(registry_path)
    specs = []
    for name in names:
        spec = registry.get(name)
        if spec is None or spec.maximum_file_bytes is not None:
            raise SnapshotError(f"scheduled artifact is not a registered full run: {name}")
        if not spec.relative_root.startswith("markets/"):
            raise SnapshotError(f"scheduled artifact is outside markets: {name}")
        specs.append(spec)
    return specs


def scheduled_legacy_specs(policy_path: Path, catalog_path: Path, authority: str):
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != 1 or policy.get("authority_node_id") != authority:
        raise SnapshotError("scheduled legacy policy authority/schema mismatch")
    names = policy.get("scheduled_legacy_datasets", [])
    if not isinstance(names, list) or any(
        not isinstance(name, str) or not name for name in names
    ) or len(names) != len(set(names)):
        raise SnapshotError("scheduled legacy dataset allowlist is invalid")
    catalog = load_legacy_specs(catalog_path)
    specs = []
    for name in names:
        spec = catalog.get(name)
        if spec is None:
            raise SnapshotError(f"scheduled legacy dataset is not allowlisted: {name}")
        specs.append(spec)
    return specs


def _active_lease_deferred_row(
    dataset: str, state: dict, *, now_ns: int
) -> dict[str, object] | None:
    """Do not hash C/D payloads when a verified hot-use lease forbids retirement.

    This is only a cheap negative gate. An expired or invalid lease still takes
    the full source, mirror, backup, process-reference, and peer verification
    path before any plan can become apply-ready.
    """

    if state.get("schema_version") != 1 or state.get("state") != "hot-enrolled":
        return None
    try:
        last_used_ns = int(state["last_used_ns"])
    except (KeyError, TypeError, ValueError):
        return None
    if last_used_ns <= 0 or last_used_ns > now_ns + 5_000_000_000:
        return None
    expires_ns = last_used_ns + 7 * 86_400_000_000_000
    if now_ns >= expires_ns:
        return None
    return {
        "dataset": dataset,
        "action": "deferred",
        "blockers": ["seven-day-use-lease-active"],
        "lease_expires_at": _utc_iso_from_ns(expires_ns),
        "snapshot_id": None,
        "verification": "not_checked_active_lease",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--registry", type=Path, default=REPO_ROOT / "configs/data_sync/cold_artifacts.json"
    )
    parser.add_argument(
        "--policy", type=Path, default=REPO_ROOT / "configs/data_sync/artifact_retirement.json"
    )
    parser.add_argument(
        "--retention-config",
        type=Path,
        default=REPO_ROOT / "configs/data_sync/packed_retention.json",
    )
    parser.add_argument("--state-root", type=Path, default=Path("/var/lib/stockagent-cold-artifacts"))
    parser.add_argument(
        "--legacy-catalog",
        type=Path,
        default=REPO_ROOT / "configs/data_sync/legacy_artifact_archives.json",
    )
    parser.add_argument(
        "--legacy-state-root", type=Path, default=Path("/var/lib/stockagent-legacy-artifacts")
    )
    parser.add_argument("--artifact-root", type=Path, default=REPO_ROOT / "artifacts")
    parser.add_argument("--hot-root", type=Path, default=Path("/srv/stockagent-artifacts-hot"))
    args = parser.parse_args()
    try:
        cfg = RetentionConfig.load(args.retention_config, repo_root=REPO_ROOT)
        if cfg.authority_node_id != "penguin":
            raise SnapshotError("automatic hot retirement is penguin-only")
        specs = scheduled_specs(args.policy, args.registry, cfg.authority_node_id)
        legacy_specs = scheduled_legacy_specs(
            args.policy, args.legacy_catalog, cfg.authority_node_id
        )
        required_peers = load_retirement_peer_names(
            args.policy, authority_node_id=cfg.authority_node_id
        )
        rows = []
        peer_proof = _syncthing(cfg)
        if peer_proof.get("ok") is not True:
            print(json.dumps({"state": "deferred", "reason": "cold-peer-not-converged", "peer": peer_proof}))
            return 0
        for spec in specs:
            state_path = args.state_root / "retirements" / f"{spec.dataset}.json"
            if not state_path.is_file():
                rows.append({"dataset": spec.dataset, "action": "skip-not-enrolled"})
                continue
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if (
                state.get("dataset") != spec.dataset
                or state.get("artifact_relative_root") != spec.relative_root
            ):
                raise SnapshotError(f"retirement state identity mismatch: {state_path}")
            if state.get("state") != "hot-enrolled":
                rows.append({"dataset": spec.dataset, "action": "skip-state", "state": state.get("state")})
                continue
            lease_row = _active_lease_deferred_row(
                spec.dataset, state, now_ns=time.time_ns()
            )
            if lease_row is not None:
                rows.append(lease_row)
                continue
            options = {
                "artifact_root": args.artifact_root,
                "hot_root": args.hot_root,
                "sync_root": cfg.sync_root,
                "materialized_root": cfg.materialized_root,
                "state_root": args.state_root,
                "backup_config": cfg.backup_config,
                "peer_proof": peer_proof,
                "required_peer_names": required_peers,
                "bridge_inactive": _bridge_inactive(args.hot_root),
            }
            plan = plan_artifact_retirement(spec, **options)
            row = {
                "dataset": spec.dataset,
                "action": "eligible" if plan["apply_ready"] else "deferred",
                "blockers": plan["blockers"],
                "lease_expires_at": plan["lease_expires_at"],
                "snapshot_id": plan["snapshot_id"],
            }
            # A live reference must renew its lease even when retirement is
            # blocked; otherwise a short gap between daily scans could retire
            # a recently used run immediately after its original expiry.
            if args.apply and (
                plan["apply_ready"] or "artifact-has-process-references" in plan["blockers"]
            ):
                result = apply_artifact_retirement(
                    spec, expected_fingerprint=plan["plan_fingerprint"], **options
                )
                row["action"] = result["action"]
                row["deleted"] = result["deleted"]
            rows.append(row)
        for spec in legacy_specs:
            state_path = args.legacy_state_root / "retirements" / f"{spec.dataset}.json"
            if not state_path.is_file():
                rows.append({"dataset": spec.dataset, "action": "skip-not-enrolled"})
                continue
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if (
                state.get("dataset") != spec.dataset
                or state.get("relative_root") != spec.relative_root
            ):
                raise SnapshotError(f"legacy retirement state identity mismatch: {state_path}")
            if state.get("state") != "hot-enrolled":
                rows.append({"dataset": spec.dataset, "action": "skip-state", "state": state.get("state")})
                continue
            lease_row = _active_lease_deferred_row(
                spec.dataset, state, now_ns=time.time_ns()
            )
            if lease_row is not None:
                rows.append(lease_row)
                continue
            options = {
                "repo_root": REPO_ROOT,
                "artifact_root": args.artifact_root,
                "hot_root": args.hot_root,
                "sync_root": cfg.sync_root,
                "state_root": args.legacy_state_root,
                "activation_root": args.state_root / "activations",
                "backup_config": cfg.backup_config,
                "peer_proof": peer_proof,
                "peer_probe": lambda: _syncthing(cfg),
                "bridge_inactive": _bridge_inactive(args.hot_root),
            }
            plan = plan_legacy_retirement(spec, **options)
            row = {
                "dataset": spec.dataset,
                "action": "eligible" if plan["apply_ready"] else "deferred",
                "blockers": plan["blockers"],
                "lease_expires_at": plan["lease_expires_at"],
                "snapshot_id": plan["snapshot_id"],
            }
            if args.apply and (
                plan["apply_ready"] or "artifact-has-process-references" in plan["blockers"]
            ):
                result = apply_legacy_retirement(
                    spec, expected_fingerprint=plan["plan_fingerprint"], **options
                )
                row["action"] = result["action"]
                row["deleted"] = result["deleted"]
            rows.append(row)
        print(json.dumps({"state": "ok", "apply": args.apply, "rows": rows}, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, SnapshotError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
