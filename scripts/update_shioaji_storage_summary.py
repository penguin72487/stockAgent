#!/usr/bin/env python3
"""Refresh the compact Shioaji storage snapshot consumed by the dashboard."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.shioaji_storage_monitor import write_shioaji_storage_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "artifacts/live/shioaji_storage/summary.json",
    )
    args = parser.parse_args()
    payload = write_shioaji_storage_snapshot(args.repo_root, args.output)
    summary = payload["summary"]
    print(
        json.dumps(
            {
                "status": payload["status"],
                "datasets": summary["datasets"],
                "files": summary["files"],
                "total_bytes": summary["total_bytes"],
                "average_daily_growth_bytes": summary["average_daily_growth_bytes"],
                "scan_seconds": payload["scan_seconds"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
