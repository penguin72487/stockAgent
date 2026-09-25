"""Resumable, rate-limited FinMind public Taiwan market history.

Only explicitly allowlisted anonymous-access endpoints are scheduled.  This
workspace is raw research data: downloaded rows are not point-in-time training
features and are never published to the packed cold store automatically.
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, time as wall_time, timedelta
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import requests

from downloader.artifact_io import atomic_write_json, atomic_write_parquet
from downloader.common import SharedRateLimiter, load_env_file
from downloader.finmind_account import rate_limiter, verified_account


API_URL = "https://api.finmindtrade.com/api/v4/data"
TAIPEI = ZoneInfo("Asia/Taipei")
CALENDAR_DATASET = "TaiwanStockTradingDate"
MASTER_DATASET = "TaiwanStockInfoWithWarrant"
SESSION_DATASETS = (
    "TaiwanStockStatisticsOfOrderBookAndTrade",
    "TaiwanVariousIndicators5Seconds",
)
HISTORY_START = date(2005, 1, 1)
SCHEMA_VERSION = 1


class ProviderError(RuntimeError):
    def __init__(self, code: str, *, retry_after: float = 0.0) -> None:
        super().__init__(code)
        self.code = code
        self.retry_after = retry_after


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _retry_at(receipt: dict[str, Any]) -> datetime | None:
    value = receipt.get("retry_at_utc")
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _receipt_usable(receipt: dict[str, Any], root: Path, *, now: datetime) -> bool:
    if receipt.get("status") == "complete":
        relative = receipt.get("parquet_path")
        if not isinstance(relative, str):
            return False
        path = (root / relative).resolve()
        if not path.is_relative_to(root.resolve()) or not path.is_file():
            return False
        return path.stat().st_size == receipt.get("parquet_size_bytes")
    retry = _retry_at(receipt)
    return retry is not None and retry > now


def _request(
    session: requests.Session,
    limiter: SharedRateLimiter,
    dataset: str,
    *,
    start_date: date | None,
    token: str,
    traffic_root: Path | None = None,
) -> list[dict[str, Any]]:
    limiter.wait()
    if traffic_root is not None:
        _record_request_start(traffic_root, dataset)
    params = {"dataset": dataset}
    if start_date is not None:
        params["start_date"] = start_date.isoformat()
    if token:
        params["token"] = token
    try:
        response = session.get(API_URL, params=params, timeout=(10, 45))
    except requests.RequestException as exc:
        raise ProviderError(type(exc).__name__) from exc
    if response.status_code in {402, 429}:
        try:
            retry_after = max(60.0, float(response.headers.get("Retry-After", "3600")))
        except ValueError:
            retry_after = 3600.0
        limiter.defer(retry_after)
        raise ProviderError("rate_limited", retry_after=retry_after)
    if response.status_code == 401:
        raise ProviderError("invalid_token")
    if response.status_code in {400, 403}:
        try:
            message = str(response.json().get("msg", "")).lower()
        except (ValueError, AttributeError):
            message = ""
        if "ip banned" in message:
            limiter.defer(1800)
            raise ProviderError("ip_banned", retry_after=1800)
        if "tokenillegal" in message or "invalid token" in message:
            raise ProviderError("invalid_token")
        if "level" in message and ("update" in message or "sponsor" in message):
            raise ProviderError("not_entitled")
        raise ProviderError("invalid_request")
    if 400 <= response.status_code < 500:
        raise ProviderError("invalid_request")
    if response.status_code != 200:
        raise ProviderError(f"http_{response.status_code}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise ProviderError("invalid_json") from exc
    if isinstance(payload, dict) and payload.get("status") in {402, 429}:
        limiter.defer(3600)
        raise ProviderError("rate_limited", retry_after=3600)
    if isinstance(payload, dict) and payload.get("status") == 403 and "ip banned" in str(payload.get("msg", "")).lower():
        limiter.defer(1800)
        raise ProviderError("ip_banned", retry_after=1800)
    if isinstance(payload, dict) and payload.get("status") in {400, 401, 403, 404}:
        message = str(payload.get("msg", "")).lower()
        if "tokenillegal" in message or "invalid token" in message:
            raise ProviderError("invalid_token")
        if "level" in message and ("update" in message or "sponsor" in message):
            raise ProviderError("not_entitled")
        raise ProviderError("invalid_request")
    if not isinstance(payload, dict) or payload.get("status") != 200 or payload.get("msg") != "success":
        raise ProviderError("provider_rejected")
    rows = payload.get("data")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ProviderError("invalid_rows")
    return rows


def _record_request_start(root: Path, dataset: str) -> None:
    """Durably count only this worker's outbound requests, never account usage.

    The claim is written before the network call, so transport failures also
    consume a request slot. The worker holds its one-process lock; SQLite makes
    the read-only dashboard's concurrent snapshots safe.
    """
    path = root / "request_traffic.sqlite3"
    with sqlite3.connect(path, timeout=2.0) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS requests ("
            "started_at_utc TEXT NOT NULL, dataset TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_finmind_requests_started "
            "ON requests(started_at_utc)"
        )
        connection.execute(
            "INSERT INTO requests (started_at_utc, dataset) VALUES (?, ?)",
            (_iso(_utc_now()), dataset),
        )


def _calendar_dates(rows: list[dict[str, Any]]) -> list[date]:
    dates: set[date] = set()
    for row in rows:
        try:
            item = date.fromisoformat(str(row["date"])[:10])
        except (KeyError, ValueError) as exc:
            raise ProviderError("invalid_calendar") from exc
        if item >= HISTORY_START:
            dates.add(item)
    if not dates:
        raise ProviderError("empty_calendar")
    return sorted(dates)


def _session_cutoff(now: datetime) -> date:
    local = now.astimezone(TAIPEI)
    return local.date() if local.time() >= wall_time(14, 0) else local.date() - timedelta(days=1)


def _opening_window(now: datetime, sessions: set[date]) -> bool:
    local = now.astimezone(TAIPEI)
    return local.date() in sessions and wall_time(8, 20) <= local.time() < wall_time(9, 10)


def _load_calendar(
    root: Path,
    session: requests.Session,
    limiter: SharedRateLimiter,
    token: str,
    now: datetime,
) -> tuple[list[date], int]:
    path = root / "calendar.json"
    cached = _read_json(path)
    observed = cached.get("observed_at_utc")
    fresh = False
    if isinstance(observed, str):
        try:
            fresh = now - datetime.fromisoformat(observed.replace("Z", "+00:00")) < timedelta(hours=20)
        except ValueError:
            pass
    if fresh:
        try:
            return _calendar_dates([{"date": item} for item in cached["dates"]]), 0
        except (KeyError, TypeError, ProviderError):
            pass
    try:
        dates = _calendar_dates(_request(session, limiter, CALENDAR_DATASET, start_date=HISTORY_START, token=token, traffic_root=root))
    except ProviderError:
        if isinstance(cached.get("dates"), list):
            return _calendar_dates([{"date": item} for item in cached["dates"]]), 1
        raise
    atomic_write_json(path, {
        "schema_version": SCHEMA_VERSION,
        "source_dataset": CALENDAR_DATASET,
        "observed_at_utc": _iso(now),
        "dates": [item.isoformat() for item in dates],
    })
    return dates, 1


def _validated_session_rows(dataset: str, day: date, rows: list[dict[str, Any]]) -> tuple[str, str | None, int | None, int | None]:
    """Accept only exact 1m or 5s grids; retain any imperfect raw response."""
    if not rows:
        return "provider_empty", None, None, None
    seconds: list[int] = []
    for row in rows:
        raw = row.get("Time") if dataset == SESSION_DATASETS[0] else row.get("date")
        if not isinstance(raw, str):
            return "partial", None, None, None
        if dataset == SESSION_DATASETS[1] and not raw.startswith(day.isoformat()):
            return "partial", None, None, None
        try:
            stamp = wall_time.fromisoformat(raw[-8:])
        except ValueError:
            return "partial", None, None, None
        if dataset == SESSION_DATASETS[0] and str(row.get("date"))[:10] != day.isoformat():
            return "partial", None, None, None
        numeric_keys = ("TotalBuyOrder", "TotalDealVolume") if dataset == SESSION_DATASETS[0] else ("TAIEX",)
        try:
            values = [float(row[key]) for key in numeric_keys]
        except (KeyError, TypeError, ValueError):
            return "partial", None, None, None
        if any(not math.isfinite(value) or value < 0 for value in values):
            return "partial", None, None, None
        if dataset == SESSION_DATASETS[1] and values[0] == 0:
            return "partial", None, None, None
        seconds.append(stamp.hour * 3600 + stamp.minute * 60 + stamp.second)
    if len(seconds) < 2 or seconds != sorted(set(seconds)):
        return "partial", None, None, None
    step = seconds[1] - seconds[0]
    grain = {5: "5s", 60: "1m"}.get(step)
    if grain is None:
        return "partial", None, None, None
    expected = 3241 if step == 5 else 271
    first, last = 9 * 3600, 13 * 3600 + 30 * 60
    missing = expected - len(seconds)
    if seconds[0] == first and seconds[-1] == last and missing == 0 and all(
        right - left == step for left, right in zip(seconds, seconds[1:])
    ):
        return "complete", grain, expected, 0
    return "partial", grain, expected, max(0, missing)


def _candidate_days(dates: list[date], root: Path, *, now: datetime) -> tuple[list[tuple[str, date]], dict[str, Any]]:
    cutoff = _session_cutoff(now)
    eligible = [item for item in dates if item <= cutoff]
    recent = set(eligible[-5:])
    pending_recent: list[tuple[str, date]] = []
    pending_old: list[tuple[str, date]] = []
    counts: dict[str, Any] = {
        "total": len(eligible) * len(SESSION_DATASETS),
        "complete": 0,
        "deferred": 0,
        "series": {
            dataset: {"total": len(eligible), "complete": 0, "deferred": 0, "rows": 0,
                      "bytes": 0, "first_complete_date": None, "last_complete_date": None,
                      "last_receipt_at_utc": None, "observed_grains": {}}
            for dataset in SESSION_DATASETS
        },
    }
    for day in eligible:
        for dataset in SESSION_DATASETS:
            receipt = _read_json(root / "receipts" / dataset / f"{day}.json")
            if receipt.get("status") == "complete" and _receipt_usable(receipt, root, now=now):
                counts["complete"] += 1
                item = counts["series"][dataset]
                item["complete"] += 1
                item["rows"] += int(receipt.get("rows") or 0)
                item["bytes"] += int(receipt.get("parquet_size_bytes") or 0)
                item["first_complete_date"] = item["first_complete_date"] or day.isoformat()
                item["last_complete_date"] = day.isoformat()
                fetched = receipt.get("fetched_at_utc")
                if isinstance(fetched, str) and (item["last_receipt_at_utc"] is None or fetched > item["last_receipt_at_utc"]):
                    item["last_receipt_at_utc"] = fetched
                grain = receipt.get("observed_grain")
                if grain in {"1m", "5s"}:
                    item["observed_grains"][grain] = item["observed_grains"].get(grain, 0) + 1
                continue
            if receipt.get("status") != "complete" and _receipt_usable(receipt, root, now=now):
                counts["deferred"] += 1
                counts["series"][dataset]["deferred"] += 1
                continue
            task = (dataset, day)
            (pending_recent if day in recent else pending_old).append(task)
    return sorted(pending_recent, key=lambda item: (-item[1].toordinal(), item[0])) + pending_old, counts


def _record_session(root: Path, dataset: str, day: date, rows: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    status, grain, expected, missing = _validated_session_rows(dataset, day, rows)
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset": dataset,
        "date": day.isoformat(),
        "status": status,
        "rows": len(rows),
        "observed_grain": grain,
        "expected_rows_for_observed_grain": expected,
        "missing_grid_points": missing,
        "fetched_at_utc": _iso(now),
        "point_in_time_training_safe": False,
    }
    if rows:
        path = root / "market_intraday" / dataset / f"year={day.year}" / f"date={day}.parquet"
        table = pa.Table.from_pylist(rows)
        atomic_write_parquet(path, table, compression="zstd")
        receipt.update({
            "parquet_path": str(path.relative_to(root)),
            "parquet_size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
    if status != "complete":
        receipt["retry_at_utc"] = _iso(now + timedelta(hours=1 if day >= now.astimezone(TAIPEI).date() - timedelta(days=7) else 6))
    atomic_write_json(root / "receipts" / dataset / f"{day}.json", receipt)
    return receipt


def _record_failure(root: Path, dataset: str, day: date, error: ProviderError, *, now: datetime) -> dict[str, Any]:
    wait = max(300.0, error.retry_after)
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "dataset": dataset,
        "date": day.isoformat(),
        "status": "not_entitled" if error.code == "not_entitled" else "failed",
        "error_code": error.code,
        "fetched_at_utc": _iso(now),
        "retry_at_utc": _iso(now if error.code in {"not_entitled", "invalid_token", "invalid_request"}
                              else now + timedelta(seconds=wait)),
    }
    atomic_write_json(root / "receipts" / dataset / f"{day}.json", receipt)
    return receipt


def _master_due(root: Path, now: datetime) -> bool:
    local = now.astimezone(TAIPEI)
    if local.time() < wall_time(14, 0):
        return False
    receipt = _read_json(root / "receipts" / MASTER_DATASET / f"{local.date()}.json")
    if receipt.get("status") != "complete" and _receipt_usable(receipt, root, now=now):
        return False
    return receipt.get("query_scope") != "full_table_snapshot" or not _receipt_usable(receipt, root, now=now)


def _record_master(root: Path, rows: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    local_day = now.astimezone(TAIPEI).date()
    if not rows or not all(isinstance(row.get("stock_id"), str) for row in rows):
        raise ProviderError("invalid_master")
    path = root / "snapshots" / MASTER_DATASET / f"snapshot={local_day}-full.parquet"
    atomic_write_parquet(path, pa.Table.from_pylist(rows), compression="zstd")
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "dataset": MASTER_DATASET,
        "snapshot_date_taipei": local_day.isoformat(),
        "status": "complete",
        "query_scope": "full_table_snapshot",
        "rows": len(rows),
        "source_first_date": min((str(row.get("date"))[:10] for row in rows if row.get("date")), default=None),
        "source_last_date": max((str(row.get("date"))[:10] for row in rows if row.get("date")), default=None),
        "fetched_at_utc": _iso(now),
        "parquet_path": str(path.relative_to(root)),
        "parquet_size_bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "point_in_time_history_available": False,
    }
    atomic_write_json(root / "receipts" / MASTER_DATASET / f"{local_day}.json", receipt)
    return receipt


def _write_status(root: Path, *, state: str, counts: dict[str, Any], requests_used: int, quota: int, token: bool, last: dict[str, Any] | None = None, active: dict[str, Any] | None = None) -> None:
    pending = max(0, counts["total"] - counts["complete"])
    atomic_write_json(root / "status.json", {
        "schema_version": SCHEMA_VERSION,
        "state": state,
        "observed_at_utc": _iso(_utc_now()),
        "session_day_datasets": list(SESSION_DATASETS),
        "calendar_dataset": CALENDAR_DATASET,
        "master_dataset": MASTER_DATASET,
        "total_session_day_tasks": counts["total"],
        "complete_session_day_tasks": counts["complete"],
        "pending_session_day_tasks": pending,
        "retry_deferred_tasks": counts["deferred"],
        "series": counts.get("series", {}),
        "requests_this_process": requests_used,
        "official_requests_per_hour": quota,
        "token_configured": token,
        "minimum_network_seconds_for_pending": round(pending * 3600 / quota),
        "eta_basis": "Only a lower bound from the official request cadence; excludes retries, provider gaps and idle windows.",
        "last_task": last,
        "active_task": active,
        "queue_preview": counts.get("queue_preview", []),
        "news": "disabled_by_user",
        "training_status": "raw_downloaded_not_pit_validated",
    })


def run_once(root: Path, *, max_requests: int = 0) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    load_env_file(Path(__file__).resolve().parents[1] / ".env", allowed_names=("FINMIND_TOKEN",))
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    used = 0
    with requests.Session() as session:
        if token:
            try:
                account = verified_account(session, token, root)
                quota = int(account["official_requests_per_hour"])
            except (requests.RequestException, RuntimeError, ValueError):
                # Continue the public worker at the registered Free ceiling;
                # paid access is never inferred from an unreachable account API.
                quota = 600
        else:
            quota = 300
        limiter = rate_limiter({"official_requests_per_hour": quota})
        now = _utc_now()
        try:
            dates, calendar_requests = _load_calendar(root, session, limiter, token, now)
            used += calendar_requests
        except ProviderError as error:
            _write_status(root, state=f"calendar_{error.code}", counts={"total": 0, "complete": 0, "deferred": 0, "series": {}}, requests_used=1, quota=quota, token=bool(token))
            if error.code in {"invalid_token", "invalid_request", "not_entitled", "ip_banned", "rate_limited"}:
                return {"state": error.code, "requests": 1}
            raise
        tasks, counts = _candidate_days(dates, root, now=now)
        counts["queue_preview"] = [
            {"dataset": dataset, "date": day.isoformat()}
            for dataset, day in tasks[:12]
        ]
        sessions = set(dates)
        if _opening_window(now, sessions):
            _write_status(root, state="protected_opening", counts=counts, requests_used=used, quota=quota, token=bool(token))
            return {"state": "protected_opening", "requests": used, **counts}
        _write_status(root, state="running" if tasks else "current", counts=counts, requests_used=used, quota=quota, token=bool(token))
        if _master_due(root, now) and (not max_requests or used < max_requests):
            try:
                rows = _request(session, limiter, MASTER_DATASET, start_date=None, token=token, traffic_root=root)
                used += 1
                _record_master(root, rows, now=_utc_now())
            except ProviderError as error:
                used += 1
                local_day = _utc_now().astimezone(TAIPEI).date()
                _record_failure(root, MASTER_DATASET, local_day, error, now=_utc_now())
                if error.code in {"rate_limited", "ip_banned", "invalid_token", "invalid_request", "not_entitled"}:
                    _write_status(root, state=error.code, counts=counts, requests_used=used, quota=quota, token=bool(token))
                    return {"state": error.code, "requests": used, **counts}
        last_task: dict[str, Any] | None = None
        for dataset, day in tasks:
            if max_requests and used >= max_requests:
                break
            if _opening_window(_utc_now(), sessions):
                break
            _write_status(root, state="running", counts=counts, requests_used=used, quota=quota, token=bool(token), last=last_task,
                          active={"dataset": dataset, "date": day.isoformat(), "started_at_utc": _iso(_utc_now())})
            try:
                rows = _request(session, limiter, dataset, start_date=day, token=token, traffic_root=root)
                used += 1
                result = _record_session(root, dataset, day, rows, now=_utc_now())
            except ProviderError as error:
                used += 1
                result = _record_failure(root, dataset, day, error, now=_utc_now())
                if error.code in {"rate_limited", "ip_banned", "not_entitled", "invalid_token", "invalid_request"}:
                    _write_status(root, state=error.code, counts=counts, requests_used=used, quota=quota, token=bool(token), last=result)
                    return {"state": error.code, "requests": used, **counts}
            if result["status"] == "complete":
                counts["complete"] += 1
                item = counts["series"][dataset]
                item["complete"] += 1
                item["rows"] += int(result.get("rows") or 0)
                item["bytes"] += int(result.get("parquet_size_bytes") or 0)
                item["first_complete_date"] = min(
                    item["first_complete_date"] or day.isoformat(), day.isoformat()
                )
                item["last_complete_date"] = max(
                    item["last_complete_date"] or day.isoformat(), day.isoformat()
                )
                item["last_receipt_at_utc"] = result.get("fetched_at_utc")
                grain = result.get("observed_grain")
                if grain in {"1m", "5s"}:
                    item["observed_grains"][grain] = item["observed_grains"].get(grain, 0) + 1
            last_task = {"dataset": dataset, "date": day.isoformat(), "status": result["status"], "rows": result.get("rows")}
            _write_status(root, state="running", counts=counts, requests_used=used, quota=quota, token=bool(token), last=last_task)
        state = (
            "current" if counts["complete"] >= counts["total"]
            else "waiting_retry" if not tasks
            else "backfilling"
        )
        _write_status(root, state=state, counts=counts, requests_used=used, quota=quota, token=bool(token), last=last_task)
        return {"state": state, "requests": used, **counts}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_finmind"))
    parser.add_argument("--max-requests", type=int, default=0, help="0 means complete all due work at the official pace")
    parser.add_argument("--loop", action="store_true", help="stay alive; recheck new sessions and repairs hourly")
    args = parser.parse_args(argv)
    if args.max_requests < 0:
        parser.error("--max-requests must be nonnegative")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / "worker.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("FinMind worker already running", file=sys.stderr)
            return 2
        try:
            while True:
                result = run_once(root, max_requests=args.max_requests)
                print(json.dumps(result, ensure_ascii=False), flush=True)
                if not args.loop:
                    return 0
                if result["state"] in {"invalid_token", "invalid_request", "not_entitled"}:
                    return 0  # Wait for corrected credentials/parameters and explicit restart.
                time.sleep(
                    1800 if result["state"] == "ip_banned"
                    else 600 if result["state"] in {"rate_limited", "protected_opening", "waiting_retry"}
                    else 3600 if result["state"] == "current" else 5
                )
        except KeyboardInterrupt:
            return 130
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


if __name__ == "__main__":
    raise SystemExit(main())
