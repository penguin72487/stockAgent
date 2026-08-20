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
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=Path("/var/lib/stockagent-artifact-dedup/receipts"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
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
