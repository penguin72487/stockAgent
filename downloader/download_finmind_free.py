"""Resumable, rate-limited FinMind public Taiwan market history.

Only explicitly allowlisted anonymous-access endpoints are scheduled.  This
workspace is raw research data: downloaded rows are not point-in-time training
features and are never published to the packed cold store automatically.
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, time as wall_time, timedelta
import errno
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
import pyarrow.parquet as pq
import requests

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet, sha256_file
from downloader.common import SharedRateLimiter, load_env_file
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_scheduling import fixed_incremental_demand
from downloader.finmind_corrections import free_correction_due, correction_receipt_metadata
from downloader.finmind_volume_units import (
    ORDER_BOOK_CANONICAL_FIELDS, ORDER_BOOK_DATASET, annotate_stock_share_units,
)


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
SESSION_GRID_CONTRACT_VERSION = 2
SESSION_GRAINS = frozenset({"1m", "15s", "10s", "5s"})


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
    correction = free_correction_due(root, CALENDAR_DATASET, now.astimezone(TAIPEI).date(), cached, now)
    observed = cached.get("observed_at_utc")
    fresh = False
    if isinstance(observed, str):
        try:
            fresh = now - datetime.fromisoformat(observed.replace("Z", "+00:00")) < timedelta(hours=20)
        except ValueError:
            pass
    if fresh and not correction['due']:
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
    if path.is_file():
        previous = path.read_bytes()
        atomic_write_bytes(root / 'versions' / CALENDAR_DATASET / f'{hashlib.sha256(previous).hexdigest()}.json', previous, durable=True)
    atomic_write_json(path, {
        "schema_version": SCHEMA_VERSION,
        "source_dataset": CALENDAR_DATASET,
        "observed_at_utc": _iso(now),
        "dates": [item.isoformat() for item in dates],
        **correction_receipt_metadata(correction['context']),
    })
    return dates, 1


def _order_book_step(day: date) -> int:
    """FinMind's documented historical cadence, not an inferred gap size."""
    if day < date(2011, 1, 17):
        return 60
    if day < date(2014, 2, 24):
        return 15
    return 10 if day < date(2014, 12, 29) else 5


def _validated_session_rows(dataset: str, day: date, rows: list[dict[str, Any]]) -> tuple[str, str | None, int | None, int | None]:
    """Accept exact source-native grids; retain every imperfect raw response."""
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
    # The local index archive shares these dated native TWSE calculation
    # grids. Use the dated grid, not the first observed delta: losing every
    # other row must never reclassify a damaged 5s response as complete 10s.
    step = _order_book_step(day)
    grain = {5: "5s", 10: "10s", 15: "15s", 60: "1m"}.get(step)
    if grain is None:
        return "partial", None, None, None
    expected = (4 * 3600 + 30 * 60) // step + 1
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
    pending_corrections: list[tuple[str, date]] = []
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
            correction = free_correction_due(root, dataset, day, receipt, now)
            if correction['due']:
                (pending_recent if day in recent else pending_corrections).append((dataset, day))
                continue
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
                if grain in SESSION_GRAINS:
                    item["observed_grains"][grain] = item["observed_grains"].get(grain, 0) + 1
                continue
            if receipt.get("status") != "complete" and _receipt_usable(receipt, root, now=now):
                counts["deferred"] += 1
                counts["series"][dataset]["deferred"] += 1
                continue
            task = (dataset, day)
            (pending_recent if day in recent else pending_old).append(task)
    return sorted(pending_recent, key=lambda item: (-item[1].toordinal(), item[0])) + pending_corrections + pending_old, counts


def _session_path(root: Path, dataset: str, day: date) -> Path:
    return root / "market_intraday" / dataset / f"year={day.year}" / f"date={day}.parquet"


def _verified_session_source(root: Path, dataset: str, day: date, receipt: dict[str, Any]) -> Path:
    path = _session_path(root, dataset, day)
    if (dataset not in SESSION_DATASETS or receipt.get("dataset") != dataset or receipt.get("date") != str(day)
            or receipt.get("parquet_path") != str(path.relative_to(root))
            or not path.resolve().is_relative_to(root.resolve()) or not path.is_file()):
        raise ValueError("invalid order-book source identity/path")
    digest = receipt.get("sha256")
    if (not isinstance(digest, str) or len(digest) != 64
            or path.stat().st_size != receipt.get("parquet_size_bytes")
            or sha256_file(path) != digest):
        raise ValueError("order-book source size/SHA256 mismatch")
    return path


def _verified_order_book_source(root: Path, day: date, receipt: dict[str, Any]) -> Path:
    return _verified_session_source(root, ORDER_BOOK_DATASET, day, receipt)


def _archive_order_book_source(root: Path, day: date, receipt: dict[str, Any]) -> dict[str, str]:
    """Preserve the verified old source and receipt before an atomic replacement."""
    if not receipt.get("parquet_path") and isinstance(receipt.get("previous_source"), dict):
        # A malformed later response may have been preserved as raw JSON only.
        # Its receipt keeps the exact preceding Parquet version for this case.
        prior = receipt["previous_source"]
        digest = prior.get("sha256", "")
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("invalid previous order-book source digest")
        version_root = root / "versions" / ORDER_BOOK_DATASET / str(day)
        archived = version_root / f"{digest}.parquet"
        archived_receipt = version_root / f"{digest}.json"
        if (prior.get("parquet_path") != str(archived.relative_to(root))
                or prior.get("receipt_path") != str(archived_receipt.relative_to(root))):
            raise ValueError("invalid previous order-book source paths")
        old = _read_json(archived_receipt)
        _verified_order_book_source(root, day, old)
        if sha256_file(archived) != digest or old.get("sha256") != digest:
            raise ValueError("previous order-book source hash mismatch")
        return prior
    path = _verified_order_book_source(root, day, receipt)
    receipt_path = root / "receipts" / ORDER_BOOK_DATASET / f"{day}.json"
    if _read_json(receipt_path) != receipt:
        raise ValueError("order-book receipt changed during unit normalization")
    version_root = root / "versions" / ORDER_BOOK_DATASET / str(day)
    version_root.mkdir(parents=True, exist_ok=True)
    digest = receipt["sha256"]
    archived = version_root / f"{digest}.parquet"
    archived_receipt = version_root / f"{digest}.json"
    if not archived.exists():
        try:
            os.link(path, archived)
        except OSError as exc:
            if exc.errno not in {errno.EXDEV, errno.EPERM, errno.EOPNOTSUPP}:
                raise
            atomic_write_bytes(archived, path.read_bytes(), durable=True)
    if archived.stat().st_size != receipt["parquet_size_bytes"] or sha256_file(archived) != digest:
        raise ValueError("archived order-book source hash mismatch")
    if not archived_receipt.exists():
        atomic_write_bytes(archived_receipt, receipt_path.read_bytes(), durable=True)
    version = _read_json(archived_receipt)
    if (version.get("dataset") != ORDER_BOOK_DATASET or version.get("date") != str(day)
            or version.get("sha256") != digest):
        raise ValueError("archived order-book receipt mismatch")
    return {"sha256": digest, "parquet_path": str(archived.relative_to(root)),
            "receipt_path": str(archived_receipt.relative_to(root))}


def _order_book_share_table(table: pa.Table) -> tuple[pa.Table, dict[str, Any]]:
    # Rebuild only our derived columns. Original Arrow arrays/dtypes remain
    # intact, including null/invalid provider values and order counts.
    raw = table.drop([name for name in ORDER_BOOK_CANONICAL_FIELDS if name in table.column_names])
    rows, units = annotate_stock_share_units(ORDER_BOOK_DATASET, raw.to_pylist())
    result = raw
    for name in ORDER_BOOK_CANONICAL_FIELDS:
        dtype = pa.float64() if name.endswith("_twd") else pa.int64()
        result = result.append_column(name, pa.array([row[name] for row in rows], type=dtype))
    return result, units


def _recover_interrupted_order_book_receipt(
    root: Path, day: date, old: dict[str, Any],
) -> bool:
    """Complete only a proven interrupted local Parquet/receipt pair commit.

    The old sealed source must reconstruct every current column, dtype and row
    exactly. A changed provider row or canonical quantity remains a hard error.
    This never replaces the current Parquet or infers provenance for an orphan.
    """
    digest = old.get("sha256", "")
    if (not isinstance(digest, str) or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)):
        return False
    path = _session_path(root, ORDER_BOOK_DATASET, day)
    receipt_path = root / "receipts" / ORDER_BOOK_DATASET / f"{day}.json"
    version_root = root / "versions" / ORDER_BOOK_DATASET / str(day)
    archived = version_root / f"{digest}.parquet"
    archived_receipt = version_root / f"{digest}.json"
    if not archived.is_file() or not archived_receipt.is_file():
        return False
    if (not archived.resolve().is_relative_to(root.resolve())
            or not archived_receipt.resolve().is_relative_to(root.resolve())
            or _read_json(archived_receipt) != old
            or _read_json(receipt_path) != old
            or archived.stat().st_size != old.get("parquet_size_bytes")
            or sha256_file(archived) != digest):
        raise ValueError("interrupted order-book normalization archive/receipt mismatch")
    source = pq.ParquetFile(archived).read()
    if source.num_rows != old.get("rows"):
        raise ValueError("interrupted order-book normalization source row count mismatch")
    expected, units = _order_book_share_table(source)
    if not units["normalization_valid"]:
        raise ValueError("interrupted order-book normalization has invalid source units")

    def signature() -> tuple[int, int, int, int, int]:
        stat = path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    original_signature = signature()
    current_sha = sha256_file(path)
    current = pq.ParquetFile(path).read()
    if not current.equals(expected, check_metadata=True):
        raise ValueError("interrupted order-book normalization canonical table mismatch")
    if (signature() != original_signature or sha256_file(path) != current_sha
            or sha256_file(archived) != digest or _read_json(receipt_path) != old):
        raise ValueError("interrupted order-book normalization source changed during verification")
    recovered_at = _iso(_utc_now())
    archive = {"sha256": digest, "parquet_path": str(archived.relative_to(root)),
               "receipt_path": str(archived_receipt.relative_to(root))}
    updated = {
        **old, "volume_units": units, "parquet_size_bytes": original_signature[2],
        "sha256": current_sha, "unit_normalized_at_utc": recovered_at,
        "unit_normalization_previous_source": archive,
        "unit_normalization_recovery": {
            "reason": "interrupted_parquet_receipt_commit", "recovered_at_utc": recovered_at,
            "reconstructed_from_sha256": digest, "verified_new_sha256": current_sha,
            "canonical_table_equal": True,
        },
    }
    atomic_write_json(receipt_path, updated)
    return True


def normalize_units_local(root: Path) -> dict[str, Any]:
    """Called under worker.lock; upgrade only existing order-book partitions."""
    result: dict[str, Any] = {
        "dataset": ORDER_BOOK_DATASET, "mode": "normalize_units_local", "api_requests": 0,
        "scanned": 0, "updated": 0, "already_current": 0, "recovered_interrupted": 0,
        "without_source": 0, "failures": [],
    }
    for receipt_path in sorted((root / "receipts" / ORDER_BOOK_DATASET).glob("*.json")):
        result["scanned"] += 1
        try:
            day = date.fromisoformat(receipt_path.stem)
            old = _read_json(receipt_path)
            if not old:
                raise ValueError("invalid order-book receipt JSON")
            if not old.get("parquet_path"):
                if old.get("status") in {"complete", "partial"}:
                    raise ValueError("order-book data receipt lacks source Parquet path")
                result["without_source"] += 1
                continue
            try:
                path = _verified_order_book_source(root, day, old)
            except ValueError as exc:
                if (str(exc) != "order-book source size/SHA256 mismatch"
                        or not _recover_interrupted_order_book_receipt(root, day, old)):
                    raise
                result["updated"] += 1
                result["recovered_interrupted"] += 1
                continue
            table = pq.ParquetFile(path).read()
            if table.num_rows != old.get("rows"):
                raise ValueError("order-book source row count mismatch")
            normalized, units = _order_book_share_table(table)
            if not units["normalization_valid"]:
                raise ValueError("invalid order-book source values: " + json.dumps(units["invalid_fields"], sort_keys=True))
            same_table = normalized.equals(table)
            if same_table and old.get("volume_units") == units:
                result["already_current"] += 1
                continue
            archive = _archive_order_book_source(root, day, old)
            if not same_table:
                atomic_write_parquet(
                    path, normalized, compression="zstd",
                    before_replace=lambda: _verified_order_book_source(root, day, old),
                )
            updated = {
                **old, "volume_units": units,
                "parquet_size_bytes": path.stat().st_size, "sha256": sha256_file(path),
                "unit_normalized_at_utc": _iso(_utc_now()), "unit_normalization_previous_source": archive,
            }
            atomic_write_json(receipt_path, updated)
            result["updated"] += 1
        except (OSError, ValueError, TypeError, KeyError, pa.ArrowException) as exc:
            result["failures"].append({"receipt": receipt_path.name, "error": str(exc)})
        if result["scanned"] % 100 == 0:
            print(f"[finmind-units] scanned={result['scanned']} updated={result['updated']} failures={len(result['failures'])}", flush=True)
    result["state"] = "failed" if result["failures"] else "complete"
    atomic_write_json(root / "unit_normalization" / f"{ORDER_BOOK_DATASET}.json", result)
    return result


def revalidate_sessions_local(root: Path) -> dict[str, Any]:
    """Under worker.lock, verify dated grids without downloading the same bytes."""
    result: dict[str, Any] = {
        "mode": "revalidate_sessions_local", "api_requests": 0,
        "datasets": list(SESSION_DATASETS), "scanned": 0, "updated": 0,
        "newly_complete": 0, "still_partial": 0, "already_current": 0,
        "without_source": 0, "failures": [], "newly_complete_by_dataset": {},
    }
    for dataset, receipt_path in (
        (dataset, path) for dataset in SESSION_DATASETS
        for path in sorted((root / "receipts" / dataset).glob("*.json"))
    ):
        result["scanned"] += 1
        try:
            old = _read_json(receipt_path)
            day = date.fromisoformat(receipt_path.stem)
            if not old:
                raise ValueError("invalid source receipt")
            if not old.get("parquet_path"):
                result["without_source"] += 1
                continue
            source = _verified_session_source(root, dataset, day, old)
            table = pq.ParquetFile(source).read()
            if table.num_rows != old.get("rows"):
                raise ValueError("source row count mismatch")
            status, grain, expected, missing = _validated_session_rows(
                dataset, day, table.to_pylist(),
            )
            if dataset == ORDER_BOOK_DATASET:
                _, units = _order_book_share_table(table)
                if not units["normalization_valid"]:
                    status = "partial"
            proof = {
                "status": status, "observed_grain": grain,
                "expected_rows_for_observed_grain": expected, "missing_grid_points": missing,
                "session_grid_contract_version": SESSION_GRID_CONTRACT_VERSION,
            }
            result["still_partial"] += status != "complete"
            if all(old.get(key) == value for key, value in proof.items()):
                result["already_current"] += 1
                continue
            version_root = root / "versions" / dataset / str(day)
            version_root.mkdir(parents=True, exist_ok=True)
            archived_source = version_root / f"{old['sha256']}.parquet"
            if not archived_source.exists():
                try:
                    os.link(source, archived_source)
                except OSError as exc:
                    if exc.errno not in {errno.EXDEV, errno.EPERM, errno.EOPNOTSUPP}:
                        raise
                    atomic_write_bytes(archived_source, source.read_bytes(), durable=True)
            if sha256_file(archived_source) != old["sha256"]:
                raise ValueError("archived source hash mismatch")
            previous = {"sha256": old["sha256"], "parquet_path": str(archived_source.relative_to(root))}
            old_bytes = receipt_path.read_bytes()
            if json.loads(old_bytes) != old:
                raise ValueError("source receipt changed during grid revalidation")
            old_digest = hashlib.sha256(old_bytes).hexdigest()
            exact_receipt = version_root / f"{old_digest}.grid-receipt.json"
            if exact_receipt.exists():
                if sha256_file(exact_receipt) != old_digest:
                    raise ValueError("archived grid receipt hash mismatch")
            else:
                atomic_write_bytes(exact_receipt, old_bytes, durable=True)
            previous = {**previous, "receipt_path": str(exact_receipt.relative_to(root)),
                        "receipt_sha256": old_digest}
            updated = {
                **old, **proof,
                "grid_revalidated_at_utc": _iso(_utc_now()),
                "grid_revalidation_previous_source": previous,
                "grid_cadence_source": "https://finmind.github.io/tutor/TaiwanMarket/Technical/",
            }
            if status == "complete":
                updated.pop("retry_at_utc", None)
                result["newly_complete"] += old.get("status") != "complete"
                result["newly_complete_by_dataset"][dataset] = (
                    result["newly_complete_by_dataset"].get(dataset, 0) + (old.get("status") != "complete")
                )
            else:
                updated["retry_at_utc"] = _iso(_utc_now())
            _verified_session_source(root, dataset, day, old)
            atomic_write_json(receipt_path, updated)
            result["updated"] += 1
        except (OSError, ValueError, TypeError, KeyError, pa.ArrowException) as exc:
            result["failures"].append({"dataset": dataset, "receipt": receipt_path.name, "error": str(exc)})
        if result["scanned"] % 100 == 0:
            print(f"[finmind-grids] scanned={result['scanned']} newly_complete={result['newly_complete']} failures={len(result['failures'])}", flush=True)
    result["state"] = "failed" if result["failures"] else "complete"
    atomic_write_json(root / "grid_revalidation" / "session_grids.json", result)
    return result


def _archive_free_head(root: Path, receipt_path: Path, data_path: Path) -> None:
    """Keep verified previous bytes when correcting non-order-book Free data."""
    if not receipt_path.is_file():
        return
    previous = receipt_path.read_bytes()
    old = json.loads(previous)
    proof = old if old.get('parquet_path') else old.get('previous_source_receipt', {})
    folder = root / 'receipt_history' / receipt_path.relative_to(root / 'receipts').with_suffix('')
    if data_path.is_file():
        if (proof.get('parquet_path') != str(data_path.relative_to(root)) or
                proof.get('parquet_size_bytes') != data_path.stat().st_size or
                proof.get('sha256') != sha256_file(data_path)):
            raise ValueError('previous Free source proof mismatch')
        archived = folder / f"{proof['sha256']}.parquet"
        if not archived.exists():
            atomic_write_bytes(archived, data_path.read_bytes(), durable=True)
        elif sha256_file(archived) != proof['sha256']:
            raise ValueError('previous Free archive proof mismatch')
    atomic_write_bytes(folder / f'{hashlib.sha256(previous).hexdigest()}.json', previous, durable=True)


def _record_session(root: Path, dataset: str, day: date, rows: list[dict[str, Any]], *, now: datetime,
                    correction_context: dict[str, Any] | None = None) -> dict[str, Any]:
    previous = _read_json(root / 'receipts' / dataset / f'{day}.json')
    if not rows and (_session_path(root, dataset, day).is_file() or int(previous.get('rows') or 0) > 0):
        raise ProviderError('unexpected_empty_after_nonempty', retry_after=3600)
    status, grain, expected, missing = _validated_session_rows(dataset, day, rows)
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "session_grid_contract_version": SESSION_GRID_CONTRACT_VERSION,
        "dataset": dataset,
        "date": day.isoformat(),
        "status": status,
        "rows": len(rows),
        "observed_grain": grain,
        "expected_rows_for_observed_grain": expected,
        "missing_grid_points": missing,
        "fetched_at_utc": _iso(now),
        "point_in_time_training_safe": False,
        **correction_receipt_metadata(correction_context or {}),
    }
    path = _session_path(root, dataset, day)
    if dataset == ORDER_BOOK_DATASET and path.is_file():
        old = _read_json(root / "receipts" / dataset / f"{day}.json")
        receipt["previous_source"] = _archive_order_book_source(root, day, old)
    elif path.is_file():
        _archive_free_head(root, root / 'receipts' / dataset / f'{day}.json', path)
    if rows:
        if dataset == ORDER_BOOK_DATASET:
            _, units = annotate_stock_share_units(dataset, rows)
            receipt["volume_units"] = units
            if not units["normalization_valid"] and status == "complete":
                status = receipt["status"] = "partial"
        try:
            if dataset == ORDER_BOOK_DATASET:
                # Explicit union keeps a later provider field even if the
                # first malformed row omitted it.
                fields = dict.fromkeys(field for row in rows for field in row)
                table = pa.Table.from_pydict({field: [row.get(field) for row in rows] for field in fields})
                table, _ = _order_book_share_table(table)
            else:
                table = pa.Table.from_pylist(rows)
        except (pa.ArrowException, TypeError, OverflowError):
            if dataset != ORDER_BOOK_DATASET:
                raise
            # Arrow cannot losslessly put mixed numeric/string/bool provider
            # values in one column. Preserve the exact response values as JSON.
            status = receipt["status"] = "partial"
            raw_bytes = json.dumps({"rows": rows, "fetched_at_utc": _iso(now)}, ensure_ascii=False).encode("utf-8")
            digest = hashlib.sha256(raw_bytes).hexdigest()
            raw_path = root / "versions" / dataset / str(day) / f"{digest}.raw.json"
            if raw_path.exists():
                if sha256_file(raw_path) != digest:
                    raise ValueError("raw order-book response archive hash mismatch")
            else:
                atomic_write_bytes(raw_path, raw_bytes, durable=True)
            receipt.update({"raw_response_path": str(raw_path.relative_to(root)),
                            "raw_response_sha256": digest, "raw_storage_error": "unrepresentable_arrow_source_types"})
        else:
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
    if dataset == ORDER_BOOK_DATASET and _session_path(root, dataset, day).is_file():
        old = _read_json(root / "receipts" / dataset / f"{day}.json")
        receipt["previous_source"] = _archive_order_book_source(root, day, old)
    elif dataset in {*SESSION_DATASETS, MASTER_DATASET}:
        data_path = (_session_path(root, dataset, day) if dataset in SESSION_DATASETS else
                     root / 'snapshots' / dataset / f'snapshot={day}-full.parquet')
        if data_path.is_file():
            receipt_path = root / 'receipts' / dataset / f'{day}.json'
            old = _read_json(receipt_path)
            _archive_free_head(root, receipt_path, data_path)
            receipt['previous_source_receipt'] = old if old.get('parquet_path') else old.get('previous_source_receipt', {})
    atomic_write_json(root / "receipts" / dataset / f"{day}.json", receipt)
    return receipt


def _master_due(root: Path, now: datetime) -> bool:
    local = now.astimezone(TAIPEI)
    receipt = _read_json(root / "receipts" / MASTER_DATASET / f"{local.date()}.json")
    if free_correction_due(root, MASTER_DATASET, local.date(), receipt, now)['due']:
        return True
    if local.time() < wall_time(14, 0):
        return False
    receipt = _read_json(root / "receipts" / MASTER_DATASET / f"{local.date()}.json")
    if receipt.get("status") != "complete" and _receipt_usable(receipt, root, now=now):
        return False
    return receipt.get("query_scope") != "full_table_snapshot" or not _receipt_usable(receipt, root, now=now)


def _record_master(root: Path, rows: list[dict[str, Any]], *, now: datetime,
                   correction_context: dict[str, Any] | None = None) -> dict[str, Any]:
    local_day = now.astimezone(TAIPEI).date()
    if not rows or not all(isinstance(row.get("stock_id"), str) for row in rows):
        raise ProviderError("invalid_master")
    path = root / "snapshots" / MASTER_DATASET / f"snapshot={local_day}-full.parquet"
    receipt_path = root / 'receipts' / MASTER_DATASET / f'{local_day}.json'
    _archive_free_head(root, receipt_path, path)
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
        **correction_receipt_metadata(correction_context or {}),
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
    account: dict[str, Any] = {"official_requests_per_hour": 300}
    with requests.Session() as session:
        if token:
            try:
                account = verified_account(session, token, root)
                quota = int(account["official_requests_per_hour"])
            except (requests.RequestException, RuntimeError, ValueError):
                # Continue the public worker at the registered Free ceiling;
                # paid access is never inferred from an unreachable account API.
                quota = 600
                account = {"official_requests_per_hour": quota}
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
                master_day = _utc_now().astimezone(TAIPEI).date()
                correction = free_correction_due(root, MASTER_DATASET, master_day,
                    _read_json(root / 'receipts' / MASTER_DATASET / f'{master_day}.json'), _utc_now())
                rows = _request(session, limiter, MASTER_DATASET, start_date=None, token=token, traffic_root=root)
                used += 1
                _record_master(root, rows, now=_utc_now(), correction_context=correction['context'])
            except ProviderError as error:
                used += 1
                local_day = _utc_now().astimezone(TAIPEI).date()
                _record_failure(root, MASTER_DATASET, local_day, error, now=_utc_now())
                if error.code in {"rate_limited", "ip_banned", "invalid_token", "invalid_request", "not_entitled"}:
                    _write_status(root, state=error.code, counts=counts, requests_used=used, quota=quota, token=bool(token))
                    return {"state": error.code, "requests": used, **counts}
        last_task: dict[str, Any] | None = None
        latest_session = max((day for day in dates if day <= _session_cutoff(now)), default=None)
        reserve_wait = False
        for dataset, day in tasks:
            if max_requests and used >= max_requests:
                break
            if _opening_window(_utc_now(), sessions):
                break
            dispatch_now = _utc_now()
            if day != latest_session and not backfill_budget(
                account, root, fixed_incremental_requests=fixed_incremental_demand(root, dispatch_now),
                in_flight=0, now=dispatch_now,
            )["allowed"]:
                reserve_wait = True
                break
            _write_status(root, state="running", counts=counts, requests_used=used, quota=quota, token=bool(token), last=last_task,
                          active={"dataset": dataset, "date": day.isoformat(), "started_at_utc": _iso(_utc_now())})
            try:
                correction = free_correction_due(root, dataset, day,
                    _read_json(root / 'receipts' / dataset / f'{day}.json'), _utc_now())
                rows = _request(session, limiter, dataset, start_date=day, token=token, traffic_root=root)
                used += 1
                result = _record_session(root, dataset, day, rows, now=_utc_now(),
                                         correction_context=correction['context'])
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
                if grain in SESSION_GRAINS:
                    item["observed_grains"][grain] = item["observed_grains"].get(grain, 0) + 1
            last_task = {"dataset": dataset, "date": day.isoformat(), "status": result["status"], "rows": result.get("rows")}
            _write_status(root, state="running", counts=counts, requests_used=used, quota=quota, token=bool(token), last=last_task)
        state = (
            "incremental_reserve" if reserve_wait else
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
    parser.add_argument("--normalize-units-local", action="store_true",
                        help="verify and add share/TWD fields to local order-book partitions; no API calls")
    parser.add_argument("--revalidate-sessions-local", action="store_true",
                        help="verify historical native grids and update receipts locally; no API calls")
    args = parser.parse_args(argv)
    if args.max_requests < 0:
        parser.error("--max-requests must be nonnegative")
    if (args.normalize_units_local or args.revalidate_sessions_local) and (args.loop or args.max_requests):
        parser.error("local migrations cannot be combined with --loop or --max-requests")
    if args.normalize_units_local and args.revalidate_sessions_local:
        parser.error("run one local migration at a time")
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
            if args.revalidate_sessions_local:
                result = revalidate_sessions_local(root)
                print(json.dumps(result, ensure_ascii=False), flush=True)
                return 1 if result["failures"] else 0
            if args.normalize_units_local:
                result = normalize_units_local(root)
                print(json.dumps(result, ensure_ascii=False), flush=True)
                return 1 if result["failures"] else 0
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
                    else 3600 if result["state"] == "current"
                    else 60 if result["state"] == "incremental_reserve" else 5
                )
        except KeyboardInterrupt:
            return 130
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


if __name__ == "__main__":
    raise SystemExit(main())
