#!/usr/bin/env python3
"""Publish a sanitized systemd snapshot for the hardened public dashboard."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.data_monitor_dashboard import _refresh_service_states  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/live/data_monitor/refresh_services.json"),
    )
    return parser.parse_args()


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    args = parse_args()
    output = args.output if args.output.is_absolute() else REPO_ROOT / args.output
    observed = datetime.now(UTC)
    services = _refresh_service_states(now=observed)
    if not any(
        state.get("evidence_source") == "systemd_live"
        for state in services.values()
    ):
        print("[data-refresh-status] systemd properties unavailable", file=sys.stderr)
        return 1
    _atomic_json(
        output,
        {
            "schema_version": 1,
            "generated_at_utc": observed.isoformat(),
            "services": services,
        },
    )
    print(
        f"[data-refresh-status] services={len(services)} output={output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
