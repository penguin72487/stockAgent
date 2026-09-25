#!/usr/bin/env python3
"""Fetch next-session TWSE/TPEx day-trade rules as soon as both publish.

The official TWSE TWTB4U master exposes its effective session date in every
row.  That date is the trigger and the holiday-safe source of truth: this
watcher never guesses the next session from weekdays.  It then requires the
TPEx response for the exact same date before invoking the existing downloader
and atomically updating the mutable live rule tree.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import (  # noqa: E402
    DEFAULT_DATASETS,
    TWSE_DAY_TRADE_OPENAPI_URL,
    _historical_cache_busted_url,
    _historical_request_info,
    _http_get,
    _parse_historical_response_content,
    _parse_roc_compact_date,
    _parse_twse_day_trade_openapi_payload,
)
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    require_exact_session_eligibility,
)


TAIPEI = ZoneInfo("Asia/Taipei")
_TWSE_UNCONDITIONAL_RECHECK_SECONDS = 60.0


class PublicationPending(RuntimeError):
    """The official next-session payload is not complete yet."""


@dataclass
class _TwseConditionalCache:
    result: dict[str, object] | None = None
    etag: str | None = None
    last_modified: str | None = None
    last_full_at: float = float("-inf")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-root",
        type=Path,
        default=Path("/srv/stockagent-live/data_tw_public"),
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=Path(
            "artifacts/data_refresh/tw_day_trade_eligibility/latest.json"
        ),
    )
    parser.add_argument("--poll-interval-seconds", type=float, default=2.0)
    parser.add_argument("--heartbeat-seconds", type=float, default=30.0)
    parser.add_argument("--max-wait-seconds", type=float, default=5400.0)
    parser.add_argument("--request-timeout-seconds", type=int, default=10)
    parser.add_argument(
        "--once",
        action="store_true",
        help="probe once instead of waiting; useful for health checks",
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


def _minimum_acceptable_rule_date(observed: datetime) -> date:
    local = observed.astimezone(TAIPEI)
    # At night the current-session master is stale; wait for a future-dated
    # master.  A Persistent timer started after reboot the next morning may
    # legitimately accept the current date.
    if local.timetz().replace(tzinfo=None) >= datetime_time(22, 0):
        return local.date() + timedelta(days=1)
    return local.date()


def _scheduled_publication_at(started: datetime) -> datetime:
    local = started.astimezone(TAIPEI)
    boundary = datetime.combine(
        local.date(), datetime_time(22, 30), tzinfo=TAIPEI
    )
    if local.timetz().replace(tzinfo=None) < datetime_time(12, 0):
        boundary -= timedelta(days=1)
    return boundary


def _probe_twse(
    *,
    timeout: int,
    minimum_date: date | None = None,
    cache: _TwseConditionalCache | None = None,
) -> dict[str, object]:
    url = _historical_cache_busted_url(TWSE_DAY_TRADE_OPENAPI_URL)
    conditional = (
        cache is not None
        and cache.result is not None
        and bool(cache.etag or cache.last_modified)
        and time.monotonic() - cache.last_full_at
        < _TWSE_UNCONDITIONAL_RECHECK_SECONDS
    )
    response = _http_get(
        url,
        timeout=timeout,
        verify_ssl=True,
        conditional_etag=cache.etag if conditional and cache else None,
        conditional_modified_since=(
            cache.last_modified if conditional and cache else None
        ),
        retries=0,
        retry_security_blocks=False,
    )
    status_code = int(getattr(response, "status_code", 200))
    if status_code == 304:
        if not conditional or cache is None or cache.result is None:
            raise PublicationPending("TWSE returned 304 without a validated local body")
        return {**cache.result, "http_status": 304, "body_bytes": 0}
    if status_code != 200:
        raise PublicationPending(f"TWSE OpenAPI returned HTTP {status_code}")
    try:
        payload = json.loads(response.content)
    except (TypeError, ValueError) as exc:
        raise PublicationPending("TWSE OpenAPI is not valid JSON yet") from exc
    if not isinstance(payload, list) or not payload:
        raise PublicationPending("TWSE OpenAPI has no rule rows yet")
    try:
        declared_dates = {
            _parse_roc_compact_date(str(row["Date"]))
            for row in payload
            if isinstance(row, dict) and "Date" in row
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise PublicationPending("TWSE OpenAPI Date is not valid yet") from exc
    if len(declared_dates) != 1:
        raise PublicationPending(
            "TWSE OpenAPI does not declare exactly one session date"
        )
    trading_date = next(iter(declared_dates))
    # A stale declared session cannot satisfy the publication gate. Defer the
    # comparatively expensive complete table validation until its date can.
    rows = len(payload) if minimum_date and trading_date < minimum_date else (
        _parse_twse_day_trade_openapi_payload(payload, trading_date).height
    )
    result: dict[str, object] = {
        "trading_date": trading_date,
        "rows": rows,
        "url": TWSE_DAY_TRADE_OPENAPI_URL,
        "body_sha256": hashlib.sha256(response.content).hexdigest(),
    }
    if cache is not None:
        headers = getattr(response, "headers", {})
        cache.result = result
        cache.etag = str(headers.get("ETag") or "").strip() or None
        cache.last_modified = str(headers.get("Last-Modified") or "").strip() or None
        cache.last_full_at = time.monotonic()
    return {**result, "http_status": 200, "body_bytes": len(response.content)}


def _probe_tpex(trading_date: date, *, timeout: int) -> dict[str, object]:
    spec = DEFAULT_DATASETS["tpex_day_trade_eligibility"]
    base_url, response_kind = _historical_request_info(spec, trading_date)
    response = _http_get(
        _historical_cache_busted_url(base_url),
        timeout=timeout,
        verify_ssl=True,
        retries=0,
        retry_security_blocks=False,
    )
    try:
        frame, _suffix = _parse_historical_response_content(
            spec,
            trading_date,
            response.content,
            response_kind,
        )
    except Exception as exc:
        raise PublicationPending(
            f"TPEx exact-session rules are not complete yet: {exc}"
        ) from exc
    if frame.is_empty():
        raise PublicationPending("TPEx exact-session rules have no rows yet")
    return {
        "trading_date": trading_date,
        "rows": frame.height,
        "url": base_url,
        "body_sha256": hashlib.sha256(response.content).hexdigest(),
    }


def _download_command(*, live_root: Path, trading_date: date) -> list[str]:
    # The next-session date is deliberately newer than the completed TAIEX
    # archive. Exact row dates from both official rule sources are the gate;
    # requiring a not-yet-existent next-session index row would be impossible.
    return [
        sys.executable,
        str(REPO_ROOT / "downloader" / "download_tw_public_data.py"),
        "--mode",
        "daily",
        "--datasets",
        "twse_day_trade_eligibility",
        "tpex_day_trade_eligibility",
        "--end-date",
        trading_date.isoformat(),
        "--same-session-rule-date",
        trading_date.isoformat(),
        "--require-taiex-session-calendar",
        "--output-dir",
        str(live_root),
        "--workers",
        "2",
        "--date-workers",
        "2",
        "--daily-overlap-days",
        "1",
        "--no-progress",
        "--no-write-run-metadata",
    ]


def _ready_coverage(
    live_root: Path, trading_date: date
) -> dict[str, object] | None:
    try:
        coverage = require_exact_session_eligibility(
            rule_data_dir=live_root,
            parquet_root=live_root / "stocks",
            trading_date=trading_date,
        )
    except (OSError, RuntimeError, ValueError):
        return None
    if not coverage or not all(
        bool(venue.get("covered")) for venue in coverage.values()
    ):
        return None
    return coverage


def _stable_existing_coverage(
    live_root: Path, trading_date: date
) -> dict[str, object] | None:
    """Reuse exact-session rules without waiting behind unrelated data writers.

    The two rule files are atomically replaced. Compare inode/size/mtime on
    both sides of the read so a concurrent replacement never yields a receipt
    for a mixed generation. A miss still follows the canonical write lock.
    """
    paths = tuple(
        live_root / f"{venue}_day_trade_eligibility.parquet"
        for venue in ("twse", "tpex")
    )

    def signature() -> tuple[tuple[int, int, int, int], ...] | None:
        try:
            return tuple(
                (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
                for stat in (path.stat() for path in paths)
            )
        except OSError:
            return None

    before = signature()
    if before is None:
        return None
    coverage = _ready_coverage(live_root, trading_date)
    return coverage if coverage is not None and signature() == before else None


def _ensure_exact_session_coverage(
    live_root: Path, trading_date: date, *, timing_ms: dict[str, float] | None = None
) -> tuple[dict[str, object], bool]:
    started = time.perf_counter()
    coverage = _stable_existing_coverage(live_root, trading_date)
    fast_check_done = time.perf_counter()
    if timing_ms is not None:
        timing_ms["stable_existing_check"] = round((fast_check_done - started) * 1000, 3)
    if coverage is not None:
        if timing_ms is not None:
            timing_ms["total"] = round((time.perf_counter() - started) * 1000, 3)
        return coverage, True
    lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        lock_started = time.perf_counter()
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        lock_acquired = time.perf_counter()
        if timing_ms is not None:
            timing_ms["producer_lock_wait"] = round((lock_acquired - lock_started) * 1000, 3)
        coverage = _ready_coverage(live_root, trading_date)
        locked_check_done = time.perf_counter()
        if timing_ms is not None:
            timing_ms["locked_existing_check"] = round(
                (locked_check_done - lock_acquired) * 1000, 3
            )
        reused = coverage is not None
        if coverage is None:
            subprocess.run(
                _download_command(live_root=live_root, trading_date=trading_date),
                cwd=REPO_ROOT,
                check=True,
            )
            if timing_ms is not None:
                timing_ms["official_download"] = round(
                    (time.perf_counter() - locked_check_done) * 1000, 3
                )
            coverage = _ready_coverage(live_root, trading_date)
        if coverage is None:
            raise RuntimeError("downloader completed without exact-session coverage")
    if timing_ms is not None:
        timing_ms["total"] = round((time.perf_counter() - started) * 1000, 3)
    return coverage, reused


def _write_run_receipts(
    receipt_path: Path,
    payload: dict[str, object],
    *,
    started: datetime,
) -> None:
    _atomic_json(receipt_path, payload)
    run_name = started.astimezone(TAIPEI).strftime("%Y%m%dT%H%M%S%f") + ".json"
    run_path = receipt_path.parent / "runs" / run_name
    _atomic_json(run_path, payload)


def _write_waiting_receipt(
    receipt_path: Path,
    *,
    started: datetime,
    scheduled_at: datetime,
    minimum_date: date,
    attempt_count: int,
    poll_interval_seconds: float,
    first_twse_observed_at: str | None,
    first_tpex_observed_at: str | None,
    both_sources_observed_at: str | None,
    last_error: str,
    live_root: Path,
    twse_transport: dict[str, int] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "waiting_source",
        "started_at": started.isoformat(),
        "updated_at": datetime.now(TAIPEI).isoformat(),
        "scheduled_publication_at": scheduled_at.isoformat(),
        "minimum_acceptable_rule_date": minimum_date.isoformat(),
        "attempt_count": attempt_count,
        "poll_interval_seconds": poll_interval_seconds,
        "first_twse_observed_at": first_twse_observed_at,
        "first_tpex_observed_at": first_tpex_observed_at,
        "both_sources_observed_at": both_sources_observed_at,
        "last_error": last_error,
        "live_root": str(live_root),
        "twse_transport": dict(twse_transport or {}),
    }
    # ``latest`` is mutable liveness telemetry. Immutable run receipts remain
    # reserved for terminal success/timeout so waiting does not create one file
    # every heartbeat.
    _atomic_json(receipt_path, payload)
    return payload


def main() -> int:
    args = parse_args()
    if args.poll_interval_seconds <= 0:
        raise ValueError("--poll-interval-seconds must be positive")
    if args.heartbeat_seconds <= 0:
        raise ValueError("--heartbeat-seconds must be positive")
    if args.max_wait_seconds < 0:
        raise ValueError("--max-wait-seconds must be non-negative")
    if args.request_timeout_seconds <= 0:
        raise ValueError("--request-timeout-seconds must be positive")

    live_root = args.live_root.expanduser().resolve(strict=True)
    receipt_path = (
        args.receipt
        if args.receipt.is_absolute()
        else (REPO_ROOT / args.receipt)
    ).resolve(strict=False)
    started = datetime.now(TAIPEI)
    minimum_date = _minimum_acceptable_rule_date(started)
    scheduled_at = _scheduled_publication_at(started)
    deadline = time.monotonic() + float(args.max_wait_seconds)
    attempt_count = 0
    first_twse_observed_at: str | None = None
    first_tpex_observed_at: str | None = None
    both_sources_observed_at: str | None = None
    last_error = "publication not observed"
    last_heartbeat = float("-inf")
    twse_cache = _TwseConditionalCache()
    twse_transport = {"full_bodies": 0, "not_modified": 0, "body_bytes": 0}

    _write_waiting_receipt(
        receipt_path,
        started=started,
        scheduled_at=scheduled_at,
        minimum_date=minimum_date,
        attempt_count=attempt_count,
        poll_interval_seconds=float(args.poll_interval_seconds),
        first_twse_observed_at=first_twse_observed_at,
        first_tpex_observed_at=first_tpex_observed_at,
        both_sources_observed_at=both_sources_observed_at,
        last_error=last_error,
        live_root=live_root,
        twse_transport=twse_transport,
    )
    last_heartbeat = time.monotonic()

    while True:
        attempt_count += 1
        try:
            probe_started = time.perf_counter()
            twse = _probe_twse(
                timeout=int(args.request_timeout_seconds),
                minimum_date=minimum_date,
                cache=twse_cache,
            )
            twse_done = time.perf_counter()
            if twse["http_status"] == 304:
                twse_transport["not_modified"] += 1
            else:
                twse_transport["full_bodies"] += 1
                twse_transport["body_bytes"] += int(twse["body_bytes"])
            trading_date = twse["trading_date"]
            assert isinstance(trading_date, date)
            if trading_date < minimum_date:
                raise PublicationPending(
                    "TWSE master is still stale: "
                    f"declared={trading_date.isoformat()} "
                    f"minimum={minimum_date.isoformat()}"
                )
            observed = datetime.now(TAIPEI)
            first_twse_observed_at = first_twse_observed_at or observed.isoformat()
            tpex = _probe_tpex(
                trading_date,
                timeout=int(args.request_timeout_seconds),
            )
            tpex_done = time.perf_counter()
            observed = datetime.now(TAIPEI)
            first_tpex_observed_at = first_tpex_observed_at or observed.isoformat()
            both_sources_observed_at = both_sources_observed_at or observed.isoformat()

            coverage_timing_ms: dict[str, float] = {}
            coverage, reused = _ensure_exact_session_coverage(
                live_root, trading_date, timing_ms=coverage_timing_ms
            )

            completed = datetime.now(TAIPEI)
            payload: dict[str, object] = {
                "schema_version": 1,
                "status": "ok",
                "started_at": started.isoformat(),
                "scheduled_publication_at": scheduled_at.isoformat(),
                "first_twse_observed_at": first_twse_observed_at,
                "first_tpex_observed_at": first_tpex_observed_at,
                "both_sources_observed_at": both_sources_observed_at,
                "completed_at": completed.isoformat(),
                "trading_date": trading_date.isoformat(),
                "minimum_acceptable_rule_date": minimum_date.isoformat(),
                "attempt_count": attempt_count,
                "poll_interval_seconds": float(args.poll_interval_seconds),
                "detection_after_schedule_ms": max(
                    0.0, (observed - scheduled_at).total_seconds() * 1000.0
                ),
                "twse": {**twse, "trading_date": trading_date.isoformat()},
                "tpex": {**tpex, "trading_date": trading_date.isoformat()},
                "coverage": coverage,
                "reused_existing_exact_session": reused,
                "coverage_stage_elapsed_ms": coverage_timing_ms,
                "probe_stage_elapsed_ms": {
                    "twse": round((twse_done - probe_started) * 1000, 3),
                    "tpex": round((tpex_done - twse_done) * 1000, 3),
                },
                "twse_transport": dict(twse_transport),
                "live_root": str(live_root),
            }
            _write_run_receipts(receipt_path, payload, started=started)
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if args.once or time.monotonic() >= deadline:
                failed_at = datetime.now(TAIPEI)
                payload = {
                    "schema_version": 1,
                    "status": "publication_pending_timeout",
                    "started_at": started.isoformat(),
                    "failed_at": failed_at.isoformat(),
                    "scheduled_publication_at": scheduled_at.isoformat(),
                    "minimum_acceptable_rule_date": minimum_date.isoformat(),
                    "attempt_count": attempt_count,
                    "poll_interval_seconds": float(args.poll_interval_seconds),
                    "first_twse_observed_at": first_twse_observed_at,
                    "first_tpex_observed_at": first_tpex_observed_at,
                    "both_sources_observed_at": both_sources_observed_at,
                    "last_error": last_error,
                    "twse_transport": dict(twse_transport),
                    "live_root": str(live_root),
                }
                _write_run_receipts(receipt_path, payload, started=started)
                print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
                return 75
            if time.monotonic() - last_heartbeat >= float(args.heartbeat_seconds):
                _write_waiting_receipt(
                    receipt_path,
                    started=started,
                    scheduled_at=scheduled_at,
                    minimum_date=minimum_date,
                    attempt_count=attempt_count,
                    poll_interval_seconds=float(args.poll_interval_seconds),
                    first_twse_observed_at=first_twse_observed_at,
                    first_tpex_observed_at=first_tpex_observed_at,
                    both_sources_observed_at=both_sources_observed_at,
                    last_error=last_error,
                    live_root=live_root,
                    twse_transport=twse_transport,
                )
                last_heartbeat = time.monotonic()
            time.sleep(float(args.poll_interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
