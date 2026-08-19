#!/usr/bin/env python3
"""Reload TW paper-trading services only after every persisted position is flat."""

from __future__ import annotations

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
STATE_PATH = REPO_ROOT / "artifacts/live/tw_day_trade_simulation/state.json"
RECEIPT_PATH = (
    REPO_ROOT / "artifacts/live/tw_day_trade_simulation/safe_reload_receipt.json"
)
SERVICES = (
    "stockagent-discord-bot.service",
    "stockagent-tw-day-trade-simulation.service",
)


def _atomic_json(payload: dict[str, object]) -> None:
    RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = RECEIPT_PATH.with_suffix(
        RECEIPT_PATH.suffix + f".tmp.{uuid.uuid4().hex}"
    )
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, RECEIPT_PATH)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    observed = datetime.now(TAIPEI)
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError) as exc:
        payload = {
            "status": "blocked",
            "observed_at": observed.isoformat(timespec="seconds"),
            "error": f"cannot read simulation state: {type(exc).__name__}: {exc}",
        }
        _atomic_json(payload)
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        return 75
    nonflat: list[dict[str, object]] = []
    for market, raw_mode in (state.get("modes") or {}).items():
        mode = raw_mode if isinstance(raw_mode, dict) else {}
        for position in (mode.get("positions") or {}).values():
            if not isinstance(position, dict):
                continue
            signed_shares = int(position.get("signed_shares") or 0)
            if signed_shares:
                nonflat.append(
                    {
                        "market": str(market),
                        "symbol": str(position.get("symbol") or ""),
                        "signed_shares": signed_shares,
                    }
                )
    if nonflat:
        payload = {
            "status": "blocked_nonflat",
            "observed_at": observed.isoformat(timespec="seconds"),
            "nonflat_count": len(nonflat),
            "sample": nonflat[:20],
        }
        _atomic_json(payload)
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        return 75

    for service in SERVICES:
        subprocess.run(("systemctl", "restart", service), check=True)
    payload = {
        "status": "reloaded",
        "observed_at": observed.isoformat(timespec="seconds"),
        "services": list(SERVICES),
        "nonflat_count": 0,
    }
    _atomic_json(payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
