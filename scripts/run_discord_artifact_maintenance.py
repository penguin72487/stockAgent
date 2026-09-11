#!/usr/bin/env python3
"""Run one isolated pass of Discord-owned artifact maintenance.

The Discord Gateway must stay responsive even when full-universe history
inference consumes substantial CPU or memory.  This entry point deliberately
runs outside the bot service cgroup and communicates only through the existing
durable artifact-maintenance receipt.
"""

from __future__ import annotations

import argparse
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


def _populate_postclose_signal_caches(markets: list[str]) -> tuple[int, int]:
    attempted = 0
    failures = 0
    for market in markets:
        cfg = discord_bot._resolve_market(market)
        if not discord_bot._is_tw_daily_signal_market(cfg):
            continue
        attempted += 1
        try:
            result = discord_bot._run_completed_session_signal_cache_sync(cfg)
        except Exception as exc:
            failures += 1
            discord_bot._log_exception(
                f"postclose_signal_cache:{market}",
                exc,
            )
            _emit(
                status="postclose_cache_failed",
                market=market,
                error_type=type(exc).__name__,
                error_message=str(exc)[:500],
            )
            continue
        if result is not None:
            _emit(
                status="postclose_cache_ready",
                market=market,
                signal_id=result.summary.get("signal_id"),
                panel_date=result.summary.get("panel_date"),
                output_dir=str(result.output_dir),
            )
    return attempted, failures


def run_once(*, signal_cache_only: bool = False) -> int:
    discord_bot._rotate_error_log_if_needed()
    status_path = discord_bot._artifact_backfill_status_path()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    full_worker_lock_path = status_path.with_name(f"{status_path.name}.worker.lock")
    cache_worker_lock_path = status_path.with_name("postclose_signal_cache.worker.lock")
    worker_lock_path = cache_worker_lock_path if signal_cache_only else full_worker_lock_path

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
        if discord_bot._tw_public_refresh_in_progress():
            _emit(status="deferred", reason="tw_public_refresh_in_progress")
            return 0

        markets = discord_bot._artifact_maintenance_markets()
        if signal_cache_only:
            cache_attempted, cache_failures = _populate_postclose_signal_caches(markets)
            _emit(
                status="signal_cache_complete" if cache_failures == 0 else "signal_cache_degraded",
                attempted=cache_attempted,
                failures=cache_failures,
            )
            return 0 if cache_failures == 0 else 1

        with cache_worker_lock_path.open("a+", encoding="utf-8") as cache_lock:
            try:
                fcntl.flock(cache_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                _emit(
                    status="deferred",
                    reason="postclose_signal_cache_in_progress",
                )
                return 0
            _populate_postclose_signal_caches(markets)

        attempted = 0
        failures = 0
        for market in markets:
            cfg = discord_bot._resolve_market(market)
            if bool(getattr(cfg, "day_trade_simulation_enabled", False)):
                runtime_status = discord_bot._ensure_signal_ready_cached(cfg)
                if (
                    not runtime_status.data.fresh
                    or not discord_bot._completed_session_receipt_ready(runtime_status)
                ):
                    _emit(
                        status="deferred",
                        reason="completed_session_not_ready",
                        market=market,
                        latest=runtime_status.data.last_data_date,
                        target=runtime_status.data.expected_latest_date,
                    )
                    continue
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--signal-cache-only",
        action="store_true",
        help="Generate bounded latest-close caches without formal-history inference.",
    )
    args = parser.parse_args()
    raise SystemExit(run_once(signal_cache_only=bool(args.signal_cache_only)))


if __name__ == "__main__":
    main()
