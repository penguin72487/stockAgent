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


class PublicationPending(RuntimeError):
    """The official next-session payload is not complete yet."""


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


def _probe_twse(*, timeout: int) -> dict[str, object]:
    url = _historical_cache_busted_url(TWSE_DAY_TRADE_OPENAPI_URL)
    response = _http_get(
        url,
        timeout=timeout,
        verify_ssl=True,
        retries=0,
        retry_security_blocks=False,
    )
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
    frame = _parse_twse_day_trade_openapi_payload(payload, trading_date)
    return {
        "trading_date": trading_date,
        "rows": frame.height,
        "url": TWSE_DAY_TRADE_OPENAPI_URL,
        "body_sha256": hashlib.sha256(response.content).hexdigest(),
    }


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


def main() -> int:
    args = parse_args()
    if args.poll_interval_seconds <= 0:
        raise ValueError("--poll-interval-seconds must be positive")
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

    while True:
        attempt_count += 1
        try:
            twse = _probe_twse(timeout=int(args.request_timeout_seconds))
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
            observed = datetime.now(TAIPEI)
            first_tpex_observed_at = first_tpex_observed_at or observed.isoformat()
            both_sources_observed_at = both_sources_observed_at or observed.isoformat()

            lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with lock_path.open("a+", encoding="utf-8") as lock_handle:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                coverage = _ready_coverage(live_root, trading_date)
                reused = coverage is not None
                if coverage is None:
                    subprocess.run(
                        _download_command(
                            live_root=live_root,
                            trading_date=trading_date,
                        ),
                        cwd=REPO_ROOT,
                        check=True,
                    )
                    coverage = _ready_coverage(live_root, trading_date)
                if coverage is None:
                    raise RuntimeError(
                        "downloader completed without exact-session coverage"
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
                    "live_root": str(live_root),
                }
                _write_run_receipts(receipt_path, payload, started=started)
                print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
                return 75
            time.sleep(float(args.poll_interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
