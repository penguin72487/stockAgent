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
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, time as wall_time, timezone
from pathlib import Path
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

discord_bot: ModuleType | None = None
ARTIFACT_OWNER_UNIT = "stockagent-discord-artifact-maintenance.service"
# Existing owner: 120min start budget + 90s stop grace; reserve controller/queue
# overhead too. Do not dispatch up to the opening fence using only start time.
ARTIFACT_RETRY_RUNWAY_MINUTES = 123
RETRY_RECEIPT_MAX_BYTES = 16 * 1024 * 1024
UNCHANGED_RETRY_WAKEUP_SECONDS = 300


def _load_discord_bot() -> ModuleType:
    """Do not import Torch/model code for a receipt-only retry probe."""
    global discord_bot
    if discord_bot is None:
        from services.discord_bot import bot

        discord_bot = bot
    return discord_bot


def _read_retry_json(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            raw = handle.read(RETRY_RECEIPT_MAX_BYTES + 1)
    except FileNotFoundError:
        return {}
    if len(raw) > RETRY_RECEIPT_MAX_BYTES:
        raise ValueError("artifact retry receipt exceeds the bounded reader size")
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise ValueError("artifact retry receipt must be a JSON object")
    return result


def _retry_wakeup_fingerprint(jobs: dict, markets_dir: Path) -> str:
    """Coalesce identical wakeups, never certify source freshness from stat.

    These are the same bounded close-publication event paths used by manual
    signals. A changed hint bypasses coalescing, but only the canonical worker
    may validate the contents and produce a ready artifact receipt.
    """
    receipt = Path(
        os.environ.get(
            "STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT",
            "artifacts/data_refresh/tw_public/completed_session/latest.json",
        )
    )
    publication = Path(
        os.environ.get(
            "STOCKAGENT_TW_PUBLICATION_RECEIPT_ROOT",
            "artifacts/data_refresh/tw_public/publications",
        )
    )
    paths = [receipt, ROOT / "artifacts/discord_bot/state.json"]
    paths.extend(
        publication / phase / "latest.json"
        for phase in ("close_final", "close_revision", "close_initial", "close_event")
    )
    paths.extend(sorted(markets_dir.glob("*.yaml")))
    paths.extend(sorted(markets_dir.glob("*.yml")))
    hints = []
    for path in paths:
        if not path.is_absolute():
            path = ROOT / path
        try:
            stat = path.stat()
        except FileNotFoundError:
            hints.append((str(path), None))
        else:
            hints.append((str(path), stat.st_ino, stat.st_mtime_ns, stat.st_size))
    content = json.dumps(
        {"jobs": jobs, "source_event_hints": hints},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _retry_dispatch_state_path() -> Path:
    path = Path(
        os.environ.get(
            "STOCKAGENT_ARTIFACT_RETRY_STATE_PATH",
            str(ROOT / "artifacts/discord_bot/artifact_retry_dispatch_latest.json"),
        )
    )
    return path if path.is_absolute() else ROOT / path


def retry_probe(*, observed: datetime | None = None) -> dict:
    """Advisory wakeup only; the canonical worker still owns every readiness gate.

    A ready receipt is not re-certified here. Only the newest job per currently
    scheduled market can wake the worker. A kernel-held worker lock, not a PID
    or timestamp, suppresses duplicate requests and proves a running owner.
    """
    started = time.perf_counter()
    observed = observed or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        raise ValueError("retry probe requires a timezone-aware observation")
    status_path = Path(
        os.environ.get(
            "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
            str(ROOT / "artifacts/discord_bot/artifact_backfill_status.json"),
        )
    )
    if not status_path.is_absolute():
        status_path = ROOT / status_path
    result = {
        "schema_version": 1,
        "observed_at": observed.isoformat(),
        "advisory_only": True,
        "due": False,
        "due_keys": [],
        "status_path": str(status_path),
        "reason": "no_due_retry",
        "job_count": 0,
        "ignored_invalid_jobs": 0,
    }

    def finish() -> dict:
        result["probe_seconds"] = time.perf_counter() - started
        result["model_runtime_imported"] = "torch" in sys.modules
        return result

    lock_path = status_path.with_name(f"{status_path.name}.worker.lock")
    try:
        with lock_path.open("rb") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                result["reason"] = "canonical_worker_running"
                return finish()
            fcntl.flock(handle, fcntl.LOCK_UN)
    except FileNotFoundError:
        pass
    payload = _read_retry_json(status_path)
    jobs = payload.get("jobs", {})
    if not isinstance(jobs, dict):
        raise ValueError("artifact retry jobs must be a JSON object")
    result["job_count"] = len(jobs)
    latest: dict[str, tuple[str, dict]] = {}
    for key, job in jobs.items():
        if not isinstance(job, dict):
            result["ignored_invalid_jobs"] += 1
            continue
        market = job.get("market")
        if (
            not isinstance(market, str)
            or not market
            or not key.endswith(f":{market}:artifact_backfill")
            or job.get("key", key) != key
        ):
            result["ignored_invalid_jobs"] += 1
            continue
        try:
            datetime.strptime(key.split(":", 1)[0], "%Y-%m-%d")
        except ValueError:
            result["ignored_invalid_jobs"] += 1
            continue
        if market not in latest or key > latest[market][0]:
            latest[market] = key, job
    candidates = []
    for market, (key, job) in latest.items():
        if job.get("status") == "running":
            # No canonical lock owner exists. The full worker will verify its
            # current artifacts/clock before recovering this interrupted job.
            candidates.append((key, market))
        elif job.get("status") == "failed":
            retry_at = job.get("next_retry_at")
            if not isinstance(retry_at, str):
                raise ValueError("failed artifact job has no retry deadline")
            deadline = datetime.fromisoformat(retry_at)
            if deadline.tzinfo is None:
                raise ValueError("artifact retry deadline must be timezone-aware")
            if deadline <= observed:
                candidates.append((key, market))
    if not candidates:
        return finish()

    # Reuse the lightweight canonical YAML loader, never load the trading/model
    # runtime merely to resolve the current scheduled/enabled market scope.
    from stockagent.live.market_config import load_market_configs

    markets_dir = Path(
        os.environ.get(
            "STOCKAGENT_MARKETS_DIR", str(ROOT / "services/discord_bot/markets")
        )
    )
    if not markets_dir.is_absolute():
        markets_dir = ROOT / markets_dir
    configs = load_market_configs(markets_dir)
    overrides = _read_retry_json(ROOT / "artifacts/discord_bot/state.json").get(
        "markets", {}
    )
    if not isinstance(overrides, dict):
        raise ValueError("market-state overrides must be a JSON object")
    scheduled = os.environ.get("STOCKAGENT_SCHEDULED_MARKETS", "").strip()
    selection = {item.strip() for item in scheduled.split(",") if item.strip()}
    all_markets = not selection or any(
        item.lower() in {"all", "*"} for item in selection
    )

    def enabled_override(name: str, default: bool) -> bool:
        entry = overrides.get(name, {})
        return (
            bool(entry.get("enabled", default)) if isinstance(entry, dict) else default
        )

    enabled = {
        name
        for name, cfg in configs.items()
        if (all_markets or name in selection) and enabled_override(name, cfg.enabled)
    }
    if not configs:
        fallback = os.environ.get("STOCKAGENT_DEFAULT_MARKET", "default")
        if (all_markets or fallback in selection) and enabled_override(fallback, True):
            enabled.add(fallback)
    result["due_keys"] = sorted(key for key, market in candidates if market in enabled)
    result["due"] = bool(result["due_keys"])
    result["reason"] = "retry_due" if result["due"] else "no_due_scheduled_retry"
    if result["due"]:
        result["wakeup_fingerprint"] = _retry_wakeup_fingerprint(
            {key: jobs[key] for key in result["due_keys"]}, markets_dir
        )
    return finish()


def request_due_retry() -> int:
    probe = retry_probe()
    if not probe["due"]:
        _emit(**probe, requested=False)
        return 0
    state_path = _retry_dispatch_state_path()
    previous = _read_retry_json(state_path)
    now = datetime.now(timezone.utc)
    if previous.get("wakeup_fingerprint") == probe["wakeup_fingerprint"]:
        accepted_at = datetime.fromisoformat(previous["dispatch_accepted_at"])
        if accepted_at.tzinfo is None:
            raise ValueError("retry dispatch time must be timezone-aware")
        # A backwards wall-clock jump must not stall recovery indefinitely.
        # The canonical worker lock still protects a conservative fresh wakeup.
        elapsed = (now - accepted_at).total_seconds()
        if 0 <= elapsed < UNCHANGED_RETRY_WAKEUP_SECONDS:
            _emit(
                **probe,
                requested=False,
                dispatch_reason="unchanged_retry_wakeup_coalesced",
                unchanged_wakeup_seconds=UNCHANGED_RETRY_WAKEUP_SECONDS,
            )
            return 0
    from scripts.check_outside_tw_opening_resource_window import evaluate

    window = evaluate(
        datetime.now(timezone.utc),
        minimum_runway_minutes=ARTIFACT_RETRY_RUNWAY_MINUTES,
        protected_until=wall_time(13, 40),
    )
    if not window["allowed"]:
        _emit(**probe, requested=False, reason_window="protected_market_runway")
        return 0
    subprocess.run(
        ["systemctl", "start", "--no-block", ARTIFACT_OWNER_UNIT],
        check=True,
        timeout=10,
    )
    from stockagent.data_sync.desync_snapshots import atomic_write_json

    dispatch = {
        "schema_version": 1,
        "advisory_only": True,
        "dispatch_accepted_at": datetime.now(timezone.utc).isoformat(),
        "owner_unit": ARTIFACT_OWNER_UNIT,
        "due_keys": probe["due_keys"],
        "wakeup_fingerprint": probe["wakeup_fingerprint"],
    }
    try:
        atomic_write_json(state_path, dispatch)
    except Exception:
        # A start request is not completion. If persistence fails, retain that
        # distinction and fail the controller instead of claiming a durable retry.
        _emit(
            **probe,
            requested=True,
            owner_unit=ARTIFACT_OWNER_UNIT,
            dispatch_state_persisted=False,
        )
        raise
    _emit(
        **probe,
        requested=True,
        owner_unit=ARTIFACT_OWNER_UNIT,
        dispatch_state_persisted=True,
    )
    return 0


def _emit(**payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def _populate_postclose_signal_caches(markets: list[str]) -> tuple[int, int]:
    discord_bot = _load_discord_bot()
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


def _wait_for_tw_public_refresh() -> bool:
    """Wait through a transient writer collision instead of losing the run."""

    discord_bot = _load_discord_bot()
    timeout = max(
        0.0,
        discord_bot._env_float("STOCKAGENT_ARTIFACT_SOURCE_WAIT_SECONDS", 900.0),
    )
    poll = max(
        0.1,
        discord_bot._env_float("STOCKAGENT_ARTIFACT_SOURCE_POLL_SECONDS", 5.0),
    )
    started = time.monotonic()
    first = True
    while discord_bot._tw_public_refresh_in_progress():
        elapsed = time.monotonic() - started
        if first:
            discord_bot._record_artifact_maintenance_run(
                "waiting_source",
                reason="tw_public_refresh_in_progress",
            )
            _emit(
                status="waiting_source",
                reason="tw_public_refresh_in_progress",
                timeout_seconds=timeout,
            )
            first = False
        if elapsed >= timeout:
            return False
        time.sleep(min(poll, max(0.0, timeout - elapsed)))
    if not first:
        # The source became available; make the durable worker state match the
        # work that is about to resume rather than leaving it at waiting_source.
        discord_bot._record_artifact_maintenance_run("running")
    return True


def run_once(*, signal_cache_only: bool = False) -> int:
    discord_bot = _load_discord_bot()
    discord_bot._rotate_error_log_if_needed()
    status_path = discord_bot._artifact_backfill_status_path()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    full_worker_lock_path = status_path.with_name(f"{status_path.name}.worker.lock")
    cache_worker_lock_path = status_path.with_name("postclose_signal_cache.worker.lock")
    worker_lock_path = (
        cache_worker_lock_path if signal_cache_only else full_worker_lock_path
    )

    with worker_lock_path.open("a+", encoding="utf-8") as worker_lock:
        try:
            fcntl.flock(worker_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _emit(status="already_running", lock_path=str(worker_lock_path))
            return 0

        discord_bot._record_artifact_maintenance_run("running")
        if discord_bot._opening_critical_work_pending():
            discord_bot._record_artifact_maintenance_run(
                "deferred", reason="opening_critical_work_pending"
            )
            _emit(status="deferred", reason="opening_critical_work_pending")
            return 0
        if discord_bot._interactive_signal_work_pending():
            discord_bot._record_artifact_maintenance_run(
                "deferred", reason="interactive_signal_work_pending"
            )
            _emit(status="deferred", reason="interactive_signal_work_pending")
            return 0
        if not _wait_for_tw_public_refresh():
            discord_bot._record_artifact_maintenance_run(
                "deferred", reason="tw_public_refresh_wait_timeout"
            )
            _emit(status="deferred", reason="tw_public_refresh_wait_timeout")
            return 0

        markets = discord_bot._artifact_maintenance_markets()
        if signal_cache_only:
            cache_attempted, cache_failures = _populate_postclose_signal_caches(markets)
            _emit(
                status="signal_cache_complete"
                if cache_failures == 0
                else "signal_cache_degraded",
                attempted=cache_attempted,
                failures=cache_failures,
            )
            discord_bot._record_artifact_maintenance_run(
                "complete" if cache_failures == 0 else "degraded",
                attempted=cache_attempted,
                failures=cache_failures,
            )
            return 0 if cache_failures == 0 else 1

        with cache_worker_lock_path.open("a+", encoding="utf-8") as cache_lock:
            try:
                fcntl.flock(cache_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                discord_bot._record_artifact_maintenance_run(
                    "deferred", reason="postclose_signal_cache_in_progress"
                )
                _emit(
                    status="deferred",
                    reason="postclose_signal_cache_in_progress",
                )
                return 0
            _populate_postclose_signal_caches(markets)

        attempted = 0
        failures = 0
        deferred = 0
        retry_deferred = 0
        for market in markets:
            cfg = discord_bot._resolve_market(market)
            if bool(getattr(cfg, "day_trade_simulation_enabled", False)):
                runtime_status = discord_bot._ensure_signal_ready_cached(cfg)
                if (
                    not runtime_status.data.fresh
                    or not discord_bot._completed_session_receipt_ready(runtime_status)
                ):
                    deferred += 1
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
                recovered = discord_bot._reconcile_artifact_backfill_if_current(
                    cfg,
                    key=key,
                    market=market,
                )
                if not recovered:
                    # Skipping a failed job during backoff does not prove its
                    # artifacts are current. Preserve its per-market receipt
                    # and make the worker's unfinished work visible as well.
                    deferred += 1
                    retry_deferred += 1
                    _emit(
                        status="deferred",
                        reason="artifact_retry_deferred",
                        market=market,
                        key=key,
                    )
                continue

            # A multi-market pass can run for hours. Recheck the independent
            # priority gates between expensive inferences, not only once at
            # worker startup; a close refresh or an interactive request may
            # begin while the previous market is still running.
            if discord_bot._opening_critical_work_pending():
                deferred += 1
                _emit(
                    status="deferred",
                    reason="opening_critical_work_pending",
                    market=market,
                )
                continue
            if discord_bot._interactive_signal_work_pending():
                deferred += 1
                _emit(
                    status="deferred",
                    reason="interactive_signal_work_pending",
                    market=market,
                )
                continue
            if not _wait_for_tw_public_refresh():
                deferred += 1
                _emit(
                    status="deferred",
                    reason="tw_public_refresh_wait_timeout",
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

        final_status = (
            "degraded" if failures else "waiting_source" if deferred else "complete"
        )
        _emit(
            status=final_status,
            attempted=attempted,
            failures=failures,
            deferred=deferred,
            retry_deferred=retry_deferred,
        )
        discord_bot._record_artifact_maintenance_run(
            final_status,
            reason="artifact_retry_deferred" if retry_deferred else None,
            attempted=attempted,
            failures=failures,
            deferred=deferred,
        )
        return 0 if failures == 0 else 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--signal-cache-only",
        action="store_true",
        help="Generate bounded latest-close caches without formal-history inference.",
    )
    retry_mode = parser.add_mutually_exclusive_group()
    retry_mode.add_argument("--retry-check-only", action="store_true")
    retry_mode.add_argument("--request-due-retry", action="store_true")
    args = parser.parse_args()
    if args.signal_cache_only and (args.retry_check_only or args.request_due_retry):
        parser.error("retry probe modes cannot generate signal caches")
    if args.retry_check_only or args.request_due_retry:
        try:
            if args.request_due_retry:
                return_code = request_due_retry()
            else:
                probe = retry_probe()
                _emit(**probe)
                return_code = 0 if probe["due"] else 1
        except Exception as exc:
            _emit(
                status="retry_probe_failed",
                error_type=type(exc).__name__,
                error_message=str(exc)[:500],
            )
            # Unlike ExecCondition's normal skip range, 255 records a real
            # invalid-receipt/controller failure rather than silent success.
            return_code = 255
        raise SystemExit(return_code)
    try:
        return_code = run_once(signal_cache_only=bool(args.signal_cache_only))
    except Exception as exc:
        # A worker-level crash must not leave the durable state stuck at
        # `running`; per-market receipts remain untouched for diagnosis/retry.
        if discord_bot is not None:
            discord_bot._record_artifact_maintenance_run(
                "degraded",
                reason=f"unhandled_{type(exc).__name__}",
                failures=1,
            )
            discord_bot._log_exception("artifact_maintenance_worker", exc)
        _emit(
            status="degraded",
            reason=f"unhandled_{type(exc).__name__}",
            error_message=str(exc)[:500],
        )
        raise
    raise SystemExit(return_code)


if __name__ == "__main__":
    main()
