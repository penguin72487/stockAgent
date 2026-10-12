#!/usr/bin/env python3
"""Credential-free dispatch settings for the existing account owner."""
from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.finlab_quota_policy import catalog_current, load_policy, quota_admission


def main() -> int:
    policy = load_policy(ROOT / "configs/finlab_quota_policy.json")
    try:
        discovery = json.loads((ROOT / "data_finlab/catalog/discovery.json").read_text())
    except (OSError, ValueError):
        discovery = {}
    now = datetime.now(UTC)
    print(json.dumps({**asdict(policy), "catalog_current": catalog_current(discovery, now=now, policy=policy),
        "history_floor_mb": quota_admission(kind="tw_minute", now=now, policy=policy)["effective_reserve_mb"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
