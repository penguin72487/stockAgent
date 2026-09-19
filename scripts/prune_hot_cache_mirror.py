#!/usr/bin/env python3
"""Audit or remove penguin's old hard-linked cache names from hot transport."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402
from stockagent.data_sync.hot_cache_mirror import (  # noqa: E402
    apply_hot_cache_mirror,
    plan_hot_cache_mirror,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-root", type=Path, default=REPO_ROOT / "artifacts")
    parser.add_argument("--hot-root", type=Path, default=Path("/srv/stockagent-artifacts-hot"))
    parser.add_argument(
        "--quarantine-root",
        type=Path,
        default=Path("/var/lib/stockagent-hot-artifact-sync/cache-quarantine"),
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--plan-fingerprint")
    args = parser.parse_args()
    if args.apply != bool(args.plan_fingerprint):
        parser.error("--apply and --plan-fingerprint must be supplied together")
    active = subprocess.run(
        ["systemctl", "is-active", "stockagent-hot-artifact-sync.service"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip() == "active"
    try:
        result = (
            apply_hot_cache_mirror(
                args.local_root,
                args.hot_root,
                args.quarantine_root,
                expected_fingerprint=args.plan_fingerprint,
                bridge_inactive=not active,
            )
            if args.apply
            else plan_hot_cache_mirror(
                args.local_root, args.hot_root, bridge_inactive=not active
            )
        )
    except (SnapshotError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
