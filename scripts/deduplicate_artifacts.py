#!/usr/bin/env python3
"""Audit or hard-link byte-identical stable StockAgent artifacts."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.artifact_dedup import (  # noqa: E402
    DEFAULT_EXCLUDED_SUFFIXES,
    DEFAULT_EXCLUDED_TOP,
    apply_duplicate_groups,
    find_duplicate_groups,
    groups_as_json,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT / "artifacts")
    parser.add_argument("--min-age-hours", type=float, default=24.0)
    parser.add_argument(
        "--complete-runs-only",
        action="store_true",
        help=(
            "only consider paths owned by a lifecycle progress.json with "
            "state=complete and phase=complete"
        ),
    )
    parser.add_argument("--apply", action="store_true")
    policies = parser.add_mutually_exclusive_group()
    policies.add_argument(
        "--service-cache-policy", type=Path,
        help="explicit local v12 immutable-session policy; separate from generic artifact dedup",
    )
    policies.add_argument(
        "--completed-forward-cache-policy", type=Path,
        help="explicit local committed v3 forward policy; preserves every key and receipt",
    )
    policies.add_argument(
        "--finlab-history-root", type=Path,
        help="native FinLab exact history sharing under its original sync lock; preserves all receipts/paths",
    )
    parser.add_argument("--finlab-wait-owner-seconds", type=float, default=0,
                        help="bounded wait for the native FinLab writer; never terminates a download")
    parser.add_argument("--expected-plan-fingerprint", help="require the exact service-cache dry-run plan")
    parser.add_argument(
        "--sharing-candidates-only", action="store_true",
        help="service maintenance: full SHA only complete duplicate candidates; not a whole-cache integrity audit",
    )
    parser.add_argument(
        "--forward-capacity-bytes", type=int,
        help="optional forward-cache soft target for the metadata plan; never authorizes eviction",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=Path("/var/lib/stockagent-artifact-dedup/receipts"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.finlab_history_root:
        if args.sharing_candidates_only or args.forward_capacity_bytes is not None:
            raise ValueError("FinLab source history is not a service-cache capacity policy")
        from stockagent.data.finlab_storage import compact_history
        result = compact_history(args.finlab_history_root, args.report_dir, apply=args.apply,
                                 expected_fingerprint=args.expected_plan_fingerprint,
                                 wait_owner_seconds=args.finlab_wait_owner_seconds)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 75 if result["state"] == "deferred" else 0
    if args.finlab_wait_owner_seconds != 0:
        raise ValueError("FinLab owner wait requires --finlab-history-root")
    if args.forward_capacity_bytes is not None and (
            not args.completed_forward_cache_policy or args.forward_capacity_bytes <= 0):
        raise ValueError("a positive capacity target requires an explicit completed-forward policy")
    if args.service_cache_policy or args.completed_forward_cache_policy:
        from stockagent.data_sync.physical_cache_compaction import (
            CONTRACT, compact_physical_minute_cache, policy_roots,
        )
        compact = compact_physical_minute_cache
        policy, category = args.service_cache_policy, "physical-minute"
        if args.completed_forward_cache_policy:
            from stockagent.data_sync.artifact_consumers import completed_forward_consumer_pins
            from stockagent.data_sync.completed_forward_compaction import (
                CONTRACT, compact_completed_forward_cache,
            )
            compact = compact_completed_forward_cache
            policy, category = args.completed_forward_cache_policy, "completed-forward"
        roots = policy_roots(policy, REPO_ROOT, contract=CONTRACT)
        if args.completed_forward_cache_policy and any(root.name != "completed_live_inference" for root in roots):
            raise ValueError("forward policy may name only the canonical completed_live_inference cache")
        if args.expected_plan_fingerprint and len(roots) != 1:
            raise ValueError("one exact cache required for a dry-run fingerprint")
        results = [compact(
            root, args.report_dir / category / root.name,
            apply=args.apply, expected_fingerprint=args.expected_plan_fingerprint,
            full_audit=not args.sharing_candidates_only,
            progress=lambda value: print(json.dumps(value), flush=True),
            **({"capacity_target_bytes": args.forward_capacity_bytes,
                "consumer_pin_probe": lambda: completed_forward_consumer_pins(REPO_ROOT)}
               if args.completed_forward_cache_policy else {}),
        ) for root in roots]
        print(json.dumps({"contract": CONTRACT, "results": results},
                         ensure_ascii=False, indent=2))
        return 1 if any(result["state"] == "failed" for result in results) else 0
    if args.expected_plan_fingerprint or args.sharing_candidates_only:
        raise ValueError("an explicit service-cache policy is required for this fingerprint")
    started = datetime.now(UTC)
    groups, counters = find_duplicate_groups(
        args.root,
        min_age_hours=args.min_age_hours,
        require_complete_marker=args.complete_runs_only,
    )
    replaced: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    if args.apply:
        replaced, skipped = apply_duplicate_groups(args.root, groups)
    completed = datetime.now(UTC)
    payload = {
        "schema_version": 1,
        "mode": "apply" if args.apply else "audit",
        "root": str(args.root.resolve()),
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "min_age_hours": args.min_age_hours,
        "complete_runs_only": args.complete_runs_only,
        "excluded_top": sorted(DEFAULT_EXCLUDED_TOP),
        "excluded_suffixes": sorted(DEFAULT_EXCLUDED_SUFFIXES),
        "counters": counters,
        "groups": groups_as_json(groups),
        "replaced": replaced,
        "skipped": skipped,
        "applied_replacements": len(replaced),
        "applied_logical_bytes": sum(int(item["size"]) for item in replaced),
    }
    args.report_dir.mkdir(parents=True, exist_ok=True)
    stamp = completed.strftime("%Y%m%dT%H%M%S.%fZ")
    report = args.report_dir / f"artifact-dedup-{args.root.resolve().name}-{stamp}.json"
    temporary = report.with_name(f".{report.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, report)
    finally:
        temporary.unlink(missing_ok=True)
    summary = {
        "report": str(report),
        "mode": payload["mode"],
        **counters,
        "applied_replacements": len(replaced),
        "applied_logical_bytes": payload["applied_logical_bytes"],
        "skipped": len(skipped),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
