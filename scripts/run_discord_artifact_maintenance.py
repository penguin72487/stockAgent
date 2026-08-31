#!/usr/bin/env python3
"""Run one isolated pass of Discord-owned artifact maintenance.

The Discord Gateway must stay responsive even when full-universe history
inference consumes substantial CPU or memory.  This entry point deliberately
runs outside the bot service cgroup and communicates only through the existing
durable artifact-maintenance receipt.
"""

from __future__ import annotations

import fcntl
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.discord_bot import bot as discord_bot


def _emit(**payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def run_once() -> int:
    discord_bot._rotate_error_log_if_needed()
    status_path = discord_bot._artifact_backfill_status_path()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    worker_lock_path = status_path.with_name(f"{status_path.name}.worker.lock")

    with worker_lock_path.open("a+", encoding="utf-8") as worker_lock:
        try:
            fcntl.flock(worker_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _emit(status="already_running", lock_path=str(worker_lock_path))
            return 0

        if discord_bot._opening_critical_work_pending():
            _emit(status="deferred", reason="opening_critical_work_pending")
            return 0
        if discord_bot._interactive_signal_work_pending():
            _emit(status="deferred", reason="interactive_signal_work_pending")
            return 0

        attempted = 0
        failures = 0
        for market in discord_bot._scheduled_markets():
            cfg = discord_bot._resolve_market(market)
            now = datetime.now(ZoneInfo(cfg.timezone or "Asia/Taipei"))
            key = discord_bot._artifact_backfill_key(cfg, now)
            if key is None or not discord_bot._market_has_model(cfg):
                continue
            if not discord_bot._artifact_backfill_retry_allowed(key):
                discord_bot._reconcile_artifact_backfill_if_current(
                    cfg,
                    key=key,
                    market=market,
                )
                continue

            attempted += 1
            discord_bot._begin_artifact_backfill(key, market)
            try:
                result = discord_bot._run_artifact_backfill_sync(cfg)
            except Exception as exc:
                failures += 1
                failed = discord_bot._finish_artifact_backfill(
                    key,
                    market,
                    status="failed",
                    exc=exc,
                )
                discord_bot._log_exception(f"artifact_maintenance:{market}", exc)
                _emit(
                    status="failed",
                    market=market,
                    key=key,
                    attempt=failed.get("attempt"),
                    next_retry_at=failed.get("next_retry_at"),
                    error_type=type(exc).__name__,
                    error_message=str(exc)[:500],
                )
                continue

            ready = discord_bot._finish_artifact_backfill(
                key,
                market,
                status="ready",
            )
            summary = result.summary if result is not None else {}
            _emit(
                status="ready",
                market=market,
                key=key,
                attempt=ready.get("attempt"),
                signal_id=summary.get("signal_id"),
                panel_date=summary.get("panel_date"),
                output_dir=str(result.output_dir) if result is not None else None,
            )

        _emit(
            status="complete" if failures == 0 else "degraded",
            attempted=attempted,
            failures=failures,
        )
        return 0 if failures == 0 else 1


def main() -> None:
    raise SystemExit(run_once())


if __name__ == "__main__":
    main()
