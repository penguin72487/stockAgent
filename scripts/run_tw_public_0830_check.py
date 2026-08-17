#!/usr/bin/env python3
"""Run and receipt the unified 08:30 TW official-data acceptance gate."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
TAIPEI = ZoneInfo("Asia/Taipei")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/markets/tw_day_trade_10m.yaml"),
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/0830/latest.json"),
    )
    return parser.parse_args()


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _json_or_none(path: Path) -> dict[str, object] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def refresh_command(config: Path) -> list[str]:
    return [
        sys.executable,
        str(REPO_ROOT / "scripts" / "refresh_tw_public_live_snapshot.py"),
        "--config",
        str(config),
        "--force",
    ]


def _same_session_accepted(
    snapshot: dict[str, object] | None,
    *,
    trading_date: str,
) -> bool:
    if not snapshot:
        return False
    same_session = snapshot.get("same_session_eligibility")
    if not isinstance(same_session, dict):
        return False
    if str(same_session.get("trading_date") or "") != trading_date:
        return False
    venues = same_session.get("venues")
    if not isinstance(venues, dict) or set(venues) != {"twse", "tpex"}:
        return False
    return all(
        isinstance(row, dict)
        and row.get("covered") is True
        and str(row.get("target_date") or "") == trading_date
        for row in venues.values()
    )


def main() -> int:
    args = parse_args()
    config = (
        args.config if args.config.is_absolute() else REPO_ROOT / args.config
    ).resolve(strict=True)
    receipt = (
        args.receipt if args.receipt.is_absolute() else REPO_ROOT / args.receipt
    ).resolve(strict=False)
    snapshot_receipt = REPO_ROOT / "artifacts/data_refresh/tw_public/latest.json"
    started = datetime.now(TAIPEI)
    command = refresh_command(config)
    completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
    finished = datetime.now(TAIPEI)
    snapshot = _json_or_none(snapshot_receipt)
    receipt_fresh = bool(
        snapshot_receipt.is_file()
        and snapshot_receipt.stat().st_mtime >= started.timestamp() - 1.0
    )
    same_session_accepted = _same_session_accepted(
        snapshot,
        trading_date=started.date().isoformat(),
    )
    accepted = bool(
        completed.returncode == 0
        and receipt_fresh
        and snapshot
        and snapshot.get("status") == "ok"
        and snapshot.get("coverage_complete") is True
        and same_session_accepted
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "ok" if accepted else "failed",
        "started_at_taipei": started.isoformat(),
        "completed_at_taipei": finished.isoformat(),
        "elapsed_seconds": (finished - started).total_seconds(),
        "command": command,
        "return_code": int(completed.returncode),
        "snapshot_receipt": str(snapshot_receipt),
        "snapshot": snapshot,
        "acceptance": {
            "subprocess_ok": completed.returncode == 0,
            "snapshot_receipt_fresh": receipt_fresh,
            "snapshot_status_ok": bool(snapshot and snapshot.get("status") == "ok"),
            "coverage_complete": bool(
                snapshot and snapshot.get("coverage_complete") is True
            ),
            "same_session_eligibility": same_session_accepted,
        },
    }
    _atomic_json(receipt, payload)
    run_name = started.strftime("%Y%m%dT%H%M%S%f") + ".json"
    _atomic_json(receipt.parent / "runs" / run_name, payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
