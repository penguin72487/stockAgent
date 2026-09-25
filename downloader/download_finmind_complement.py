"""Acquire every documented FinMind Free dataset not owned by the four base jobs.

The FinLab catalog is a field catalog, not a proof of equivalent provider rows or
historical coverage.  Keep these FinMind responses separate and research-only.
One SQLite queue and the same host-global FinMind limiter make a large universe
resumable without loading hundreds of thousands of task receipts into memory.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from downloader.artifact_io import atomic_write_json, atomic_write_parquet
from downloader.common import SharedRateLimiter, load_env_file
from downloader.download_finmind_free import API_URL, TAIPEI, _record_request_start
from downloader.finmind_account import rate_limiter, verified_account


SOURCE_CATALOG = "https://github.com/FinMind/FinMind-MCP/blob/master/knowledge/datasets.md"
MIN_FREE_BYTES = 25 * 1024**3
SNAPSHOTS = (
    "TaiwanStockInfo", "TaiwanSecuritiesTraderInfo", "TaiwanStockActiveETFInfo",
    "TaiwanFutOptDailyInfo", "USStockInfo", "UKStockInfo", "EuropeStockInfo",
    "JapanStockInfo",
)
GLOBAL_HISTORY = (
    "TaiwanStockTotalMarginPurchaseShortSale",
    "TaiwanStockTotalInstitutionalInvestors",
    "TaiwanStockCapitalReductionReferencePrice", "TaiwanStockDelisting",
    "TaiwanStockSplitPrice", "TaiwanStockParValueChange",
    "TaiwanFuturesDealerTradingVolumeDaily", "TaiwanOptionDealerTradingVolumeDaily",
    "TaiwanExchangeRate", "GoldPrice",
)
# The official Free tier requires data_id for these four datasets. A date-only
# whole-market query is a different (paid) entitlement, even though the catalog
# labels the dataset itself Free.
PER_ID_REQUIRED = {
    "TaiwanStockCapitalReductionReferencePrice": "stock",
    "TaiwanFuturesDealerTradingVolumeDaily": "futures",
    "TaiwanOptionDealerTradingVolumeDaily": "options",
    "TaiwanExchangeRate": "currency",
}
CURRENCIES = (
    "USD", "EUR", "JPY", "GBP", "CNY", "HKD", "AUD", "CAD", "CHF", "IDR",
    "KRW", "MYR", "NZD", "PHP", "SEK", "SGD", "THB", "VND", "ZAR",
)
GLOBAL_START_YEAR = {
    "TaiwanStockTotalMarginPurchaseShortSale": 2001,
    "TaiwanStockTotalInstitutionalInvestors": 2004,
    "TaiwanStockDelisting": 2001,
    "TaiwanStockSplitPrice": 1900,  # The provider does not document a first year.
    # Docs say 2020, but a verified local API receipt contains 2019-09-09.
    # This sparse whole-market query is cheap enough to search further back.
    "TaiwanStockParValueChange": 1900,
    "GoldPrice": 1900,  # The provider does not document a first year.
}
# These whole-market series are small enough to request once for historical
# backfill, then persist the response in the existing annual receipt layout.
BULK_GLOBAL_HISTORY = frozenset(GLOBAL_START_YEAR) - {"GoldPrice"}
GLOBAL_RELEASE_HOUR_TAIPEI = {
    "TaiwanStockTotalMarginPurchaseShortSale": 21,
    "TaiwanStockTotalInstitutionalInvestors": 15,
    # The official docs give no intraday publish time for these event tables.
    # One daily check after the TW close is a request budget, not a PIT claim.
    "TaiwanStockDelisting": 14,
    "TaiwanStockSplitPrice": 14,
    "TaiwanStockParValueChange": 14,
}
TW_SYMBOL_HISTORY = (
    "TaiwanStockPrice", "TaiwanStockPriceAdj", "TaiwanStockPER",
    "TaiwanStockDayTrading", "TaiwanStockPriceLimit",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockShareholding",
    "TaiwanStockSecuritiesLending", "TaiwanStockMarginShortSaleSuspension",
    "TaiwanDailyShortSaleBalances", "TaiwanStockFinancialStatements",
    "TaiwanStockBalanceSheet", "TaiwanStockCashFlowsStatement",
    "TaiwanStockDividend", "TaiwanStockDividendResult", "TaiwanStockMonthRevenue",
)
LONG_INSTITUTIONAL = "TaiwanStockInstitutionalInvestorsBuySell"
WIDE_INSTITUTIONAL = "TaiwanStockInstitutionalInvestorsBuySellWide"
INSTITUTIONAL_NAMES = (
    "Foreign_Investor", "Foreign_Dealer_Self", "Investment_Trust",
    "Dealer", "Dealer_self", "Dealer_Hedging",
)
DERIVATIVE_HISTORY = (
    "TaiwanFuturesDaily", "TaiwanOptionDaily",
    "TaiwanFuturesInstitutionalInvestors", "TaiwanOptionInstitutionalInvestors",
)
GLOBAL_EQUITY_HISTORY = {
    "USStockPrice": "USStockInfo",
    "UKStockPrice": "UKStockInfo",
    "EuropeStockPrice": "EuropeStockInfo",
    "JapanStockPrice": "JapanStockInfo",
}
FIXED_ID_HISTORY = {
    "TaiwanStockTotalReturnIndex": ("TAIEX", "TPEx"),
    "InterestRate": ("FED", "ECB", "BOJ", "BOE", "RBA", "PBOC", "BOC", "RBNZ", "RBI", "CBR", "BCB", "SNB"),
    "CrudeOilPrices": ("WTI", "Brent"),
    "GovernmentBondsYield": tuple(
        f"United States {term}" for term in
        ("1-Month", "3-Month", "6-Month", "1-Year", "2-Year", "3-Year",
         "5-Year", "7-Year", "10-Year", "20-Year", "30-Year")
    ),
}
ALL_DATASETS = (
    *SNAPSHOTS[:4], *GLOBAL_HISTORY[:8], *TW_SYMBOL_HISTORY, WIDE_INSTITUTIONAL,
    *DERIVATIVE_HISTORY, "TaiwanStockTotalReturnIndex",
    *SNAPSHOTS[4:], *GLOBAL_EQUITY_HISTORY,
    *GLOBAL_HISTORY[8:], "InterestRate", "CrudeOilPrices", "GovernmentBondsYield",
)
assert len(ALL_DATASETS) == len(set(ALL_DATASETS)) == 48
assert "TaiwanStockNews" not in ALL_DATASETS


@dataclass(frozen=True)
class Task:
    dataset: str
    data_id: str
    partition: str
    kind: str
    priority: int
    state: str


class SourceError(RuntimeError):
    def __init__(self, code: str, *, retry_after: int = 300) -> None:
        super().__init__(code)
        self.code = code
        self.retry_after = retry_after


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            dataset TEXT NOT NULL, data_id TEXT NOT NULL, partition TEXT NOT NULL,
            kind TEXT NOT NULL, priority INTEGER NOT NULL, state TEXT NOT NULL,
            next_attempt_at_utc TEXT, last_attempt_at_utc TEXT,
            rows INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
            first_data_date TEXT, last_data_date TEXT, receipt_path TEXT,
            error_code TEXT,
            PRIMARY KEY (dataset, data_id, partition)
        )
    """)
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_finmind_complement_queue "
        "ON tasks(priority, next_attempt_at_utc, state)"
    )
    return connection


def _add_tasks(connection: sqlite3.Connection, rows: list[tuple[str, str, str, str, int]]) -> None:
    if not rows:
        return
    connection.executemany(
        "INSERT OR IGNORE INTO tasks(dataset,data_id,partition,kind,priority,state) "
        "VALUES (?,?,?,?,?,'pending')", rows,
    )
    connection.commit()


def _snapshot_receipt(root: Path, dataset: str) -> dict[str, Any]:
    path = root / "receipts" / dataset / "all" / "latest.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _snapshot_ids(root: Path, dataset: str, column: str) -> list[str]:
    receipt = _snapshot_receipt(root, dataset)
    relative = receipt.get("parquet_path")
    if receipt.get("status") != "complete" or not isinstance(relative, str):
        return []
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        return []
    if path.stat().st_size != receipt.get("parquet_size_bytes"):
        return []
    try:
        table = pq.read_table(path, columns=[column, "type"] if dataset == "TaiwanFutOptDailyInfo" else [column])
    except (OSError, ValueError, KeyError, pa.ArrowException):
        return []
    rows = table.to_pylist()
    return sorted({str(row[column]).strip() for row in rows if row.get(column)})


def _derivative_ids(root: Path, kind: str) -> list[str]:
    receipt = _snapshot_receipt(root, "TaiwanFutOptDailyInfo")
    relative = receipt.get("parquet_path")
    if receipt.get("status") != "complete" or not isinstance(relative, str):
        return []
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        return []
    try:
        rows = pq.read_table(path, columns=["code", "type"]).to_pylist()
    except (OSError, ValueError, KeyError, pa.ArrowException):
        return []
    return sorted({str(row["code"]).strip() for row in rows
                   if row.get("code") and row.get("type") == kind})


def _official_delisted_ids(root: Path) -> set[str]:
    """Retain former stocks missing from FinMind's current security snapshot."""
    public_root = root.parents[1] / "data_tw_public"
    symbols: set[str] = set()
    for venue in ("twse", "tpex"):
        path = public_root / f"{venue}_delisted_company.parquet"
        if not path.is_file():
            continue
        try:
            values = pq.read_table(path, columns=["symbol"]).column("symbol").to_pylist()
        except (OSError, ValueError, KeyError, pa.ArrowException):
            continue
        symbols.update(str(value).strip() for value in values
                       if value and re.fullmatch(r"[1-9][0-9]{3}|9[0-9]{5}", str(value).strip()))
    return symbols


def _add_missing_identifiers(connection: sqlite3.Connection, dataset: str,
                             identifiers: list[str], *, priority: int) -> None:
    if not identifiers:
        return
    present = {row[0] for row in connection.execute(
        "SELECT data_id FROM tasks WHERE dataset=?", (dataset,)
    )}
    _add_tasks(connection, [(dataset, identifier, "history", "id_history", priority)
                            for identifier in identifiers if identifier not in present])


def _populate(connection: sqlite3.Connection, root: Path, *, today: date) -> None:
    jobs: list[tuple[str, str, str, str, int]] = []
    jobs.extend((dataset, "", "latest", "snapshot", 0) for dataset in SNAPSHOTS)
    for dataset, first_year in GLOBAL_START_YEAR.items():
        jobs.extend((dataset, "", str(year), "year",
                     0 if year == today.year else 1 if year >= 2014 else 3)
                    for year in range(first_year, today.year + 1))
    for dataset, identifiers in FIXED_ID_HISTORY.items():
        jobs.extend((dataset, identifier, "history", "id_history", 1)
                    for identifier in identifiers)
    _add_tasks(connection, jobs)
    connection.execute(
        "UPDATE tasks SET state='pending', next_attempt_at_utc=NULL "
        "WHERE dataset='TaiwanStockParValueChange' AND state='outside_documented_range'"
    )
    # Migrate the old date-only requests without deleting their audit rows.
    # They were denied because their query shape needs a paid entitlement.
    for dataset in PER_ID_REQUIRED:
        connection.execute(
            "UPDATE tasks SET state='deprecated_query_shape', next_attempt_at_utc=NULL "
            "WHERE dataset=? AND kind='year' AND state!='deprecated_query_shape'",
            (dataset,),
        )
    for dataset, first_year in GLOBAL_START_YEAR.items():
        connection.execute(
            "UPDATE tasks SET state='outside_documented_range', next_attempt_at_utc=NULL "
            "WHERE dataset=? AND kind='year' AND CAST(partition AS INTEGER)<? "
            "AND state='pending'",
            (dataset, first_year),
        )
    # Queue priorities are policy, not immutable receipts: adjust earlier
    # deployments that seeded every historical year with the same priority.
    connection.execute(
        "UPDATE tasks SET priority=CASE WHEN partition=? THEN 0 "
        "WHEN CAST(partition AS INTEGER)>=2014 THEN 1 ELSE 3 END "
        "WHERE kind='year' AND priority!=CASE WHEN partition=? THEN 0 "
        "WHEN CAST(partition AS INTEGER)>=2014 THEN 1 ELSE 3 END",
        (str(today.year), str(today.year)),
    )
    for dataset in BULK_GLOBAL_HISTORY:
        if connection.execute(
            "SELECT 1 FROM tasks WHERE dataset=? AND kind='year' AND partition<? "
            "AND state='pending' LIMIT 1", (dataset, str(today.year)),
        ).fetchone():
            connection.execute(
                "UPDATE tasks SET next_attempt_at_utc=?, error_code='bulk_scheduled' "
                "WHERE dataset=? AND kind='year' AND partition=? "
                "AND state IN ('complete','observed_empty') "
                "AND error_code IS NULL",
                (_iso(_now()), dataset, str(today.year)),
            )
    connection.commit()
    for dataset, partition, kind, task_state, last_attempt, next_attempt in connection.execute(
        "SELECT dataset,partition,kind,state,last_attempt_at_utc,next_attempt_at_utc "
        "FROM tasks WHERE state IN ('complete','observed_empty') "
        "AND (kind='snapshot' OR (kind='year' AND partition=?))",
        (str(today.year),),
    ).fetchall():
        if not last_attempt:
            continue
        try:
            refreshed_at = datetime.fromisoformat(last_attempt)
        except ValueError:
            continue
        earlier = _next_refresh(Task(dataset, "", partition, kind, 0, task_state),
                                refreshed_at, empty=task_state == "observed_empty")
        if next_attempt is None or earlier < next_attempt:
            connection.execute(
                "UPDATE tasks SET next_attempt_at_utc=? WHERE dataset=? AND data_id='' AND partition=?",
                (earlier, dataset, partition),
            )
    connection.commit()
    stocks = sorted(set(_snapshot_ids(root, "TaiwanStockInfo", "stock_id")) |
                    _official_delisted_ids(root))
    if stocks:
        for dataset in TW_SYMBOL_HISTORY:
            _add_missing_identifiers(connection, dataset, stocks, priority=2)
        _add_missing_identifiers(connection, "TaiwanStockCapitalReductionReferencePrice",
                                 stocks, priority=2)
        _add_missing_identifiers(connection, WIDE_INSTITUTIONAL, stocks, priority=3)
    connection.execute(
        "UPDATE tasks SET kind='derived' WHERE dataset=? AND kind='id_history'",
        (WIDE_INSTITUTIONAL,),
    )
    for dataset in DERIVATIVE_HISTORY:
        kind = "TaiwanOptionDaily" if "Option" in dataset else "TaiwanFuturesDaily"
        identifiers = _derivative_ids(root, kind)
        _add_missing_identifiers(connection, dataset, identifiers, priority=3)
    for dataset, kind in (("TaiwanFuturesDealerTradingVolumeDaily", "TaiwanFuturesDaily"),
                          ("TaiwanOptionDealerTradingVolumeDaily", "TaiwanOptionDaily")):
        _add_missing_identifiers(connection, dataset, _derivative_ids(root, kind), priority=3)
    _add_missing_identifiers(connection, "TaiwanExchangeRate", list(CURRENCIES), priority=1)
    for dataset, source in GLOBAL_EQUITY_HISTORY.items():
        identifiers = _snapshot_ids(root, source, "stock_id")
        _add_missing_identifiers(connection, dataset, identifiers, priority=4)
    # Invalid requests and account denials must not be retried automatically:
    # FinMind documents that repeated 4xx responses can block the entire IP.
    for (dataset,) in connection.execute(
        "SELECT DISTINCT dataset FROM tasks WHERE state='not_entitled' AND kind!='year'"
    ).fetchall():
        connection.execute(
            "UPDATE tasks SET state='not_entitled', error_code='not_entitled', "
            "next_attempt_at_utc=NULL WHERE dataset=? AND state='pending'",
            (dataset,),
        )
    connection.commit()


def _next_task(connection: sqlite3.Connection, now: datetime,
               *, delegated: frozenset[str] = frozenset()) -> Task | None:
    excluded = " AND dataset NOT IN (" + ",".join("?" for _ in delegated) + ")" if delegated else ""
    row = connection.execute(
        "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
        "WHERE ((state='pending' AND next_attempt_at_utc IS NULL) "
        "OR (state IN ('pending','complete','observed_empty','failed') "
        "AND next_attempt_at_utc <= ?)) "
        + excluded + " AND (kind!='derived' OR EXISTS (SELECT 1 FROM tasks AS source "
        "WHERE source.dataset=? AND source.data_id=tasks.data_id "
        "AND source.partition='history' AND source.state='complete')) "
        "ORDER BY priority, CASE WHEN state='pending' THEN 0 ELSE 1 END, "
        "COALESCE(next_attempt_at_utc,''), dataset, data_id LIMIT 1",
        (_iso(now), *sorted(delegated), LONG_INSTITUTIONAL),
    ).fetchone()
    return Task(*row) if row else None


def _sponsor_delegated(root: Path, now: datetime) -> frozenset[str]:
    """Pause redundant per-ID FinMind calls only while Sponsor is healthy.

    An unavailable/stale/blocked Sponsor worker returns ownership to Free.
    No task or receipt is deleted or marked complete by this routing decision.
    """
    path = root.parent / "sponsor" / "status.json"
    try:
        status = json.loads(path.read_text(encoding="utf-8"))
        stamp = datetime.fromisoformat(status["observed_at_utc"])
    except (OSError, ValueError, KeyError, TypeError):
        return frozenset()
    if now - stamp > timedelta(minutes=15) or status.get("tier") not in {"Sponsor", "SponsorPro"}:
        return frozenset()
    if status.get("state") not in {"running", "batch_complete", "current_queue", "protected_opening"}:
        return frozenset()
    series = status.get("series")
    if not isinstance(series, dict):
        return frozenset()
    candidates = set(TW_SYMBOL_HISTORY) | set(DERIVATIVE_HISTORY) | {
        WIDE_INSTITUTIONAL, "TaiwanStockCapitalReductionReferencePrice",
        "TaiwanFuturesDealerTradingVolumeDaily", "TaiwanOptionDealerTradingVolumeDaily",
    }
    return frozenset(dataset for dataset in candidates
                     if isinstance(series.get(dataset), dict)
                     and series[dataset].get("target", 0) > 0
                     and not series[dataset].get("blocked", 0))


def _bulk_needed(connection: sqlite3.Connection, task: Task, today: date) -> bool:
    if task.kind != "year" or task.dataset not in BULK_GLOBAL_HISTORY or task.partition != str(today.year):
        return False
    return connection.execute(
        "SELECT 1 FROM tasks WHERE dataset=? AND kind='year' AND partition<? "
        "AND state='pending' LIMIT 1", (task.dataset, task.partition),
    ).fetchone() is not None


def _request(session: requests.Session, limiter: SharedRateLimiter, root: Path,
             task: Task, token: str, *, today: date,
             full_history: bool = False) -> list[dict[str, Any]]:
    params: dict[str, str] = {"dataset": task.dataset}
    if task.data_id:
        params["data_id"] = task.data_id
    if task.kind == "year":
        year = int(task.partition)
        params["start_date"] = (f"{GLOBAL_START_YEAR[task.dataset]}-01-01" if full_history
                                else f"{year}-01-01")
        if not full_history:
            params["end_date"] = min(date(year, 12, 31), today).isoformat()
    elif task.kind == "id_history":
        params["start_date"] = {
            "TaiwanExchangeRate": "2006-01-01",
            "TaiwanFuturesDealerTradingVolumeDaily": "2021-04-01",
            "TaiwanOptionDealerTradingVolumeDaily": "2021-04-01",
            "TaiwanStockCapitalReductionReferencePrice": "2011-01-01",
        }.get(task.dataset, "1900-01-01")
    return _fetch_rows(session, limiter, root, task.dataset, token, params)


def _fetch_rows(session: requests.Session, limiter: SharedRateLimiter, root: Path,
                dataset: str, token: str, params: dict[str, str],
                *, endpoint: str = API_URL) -> list[dict[str, Any]]:
    """One authenticated request; shared by Free and Sponsor query plans."""
    limiter.wait()
    _record_request_start(root, dataset)
    try:
        response = session.get(endpoint, params=params, headers={"Authorization": f"Bearer {token}"},
                               timeout=(10, 90))
    except requests.RequestException as exc:
        raise SourceError(type(exc).__name__) from exc
    if response.status_code in {402, 429}:
        try:
            retry = max(60, int(float(response.headers.get("Retry-After", "3600"))))
        except ValueError:
            retry = 3600
        limiter.defer(retry)
        raise SourceError("rate_limited", retry_after=retry)
    if response.status_code == 401:
        raise SourceError("invalid_token", retry_after=0)
    if response.status_code == 403:
        try:
            message = str(response.json().get("msg", "")).lower()
        except (ValueError, AttributeError):
            message = ""
        if "ip banned" in message:
            limiter.defer(1800)
            raise SourceError("ip_banned", retry_after=1800)
        raise SourceError("not_entitled", retry_after=0)
    if response.status_code == 400:
        try:
            message = str(response.json().get("msg", "")).lower()
        except (ValueError, AttributeError):
            message = ""
        if "tokenillegal" in message or "invalid token" in message:
            raise SourceError("invalid_token", retry_after=0)
        if "level" in message and ("update" in message or "sponsor" in message):
            raise SourceError("not_entitled", retry_after=0)
        if "too many" in message or "rate limit" in message:
            limiter.defer(3600)
            raise SourceError("rate_limited", retry_after=3600)
        raise SourceError("provider_bad_request", retry_after=0)
    if 400 <= response.status_code < 500:
        raise SourceError("provider_bad_request", retry_after=0)
    if response.status_code != 200:
        raise SourceError(f"http_{response.status_code}", retry_after=3600)
    try:
        payload = response.json()
    except ValueError as exc:
        raise SourceError("invalid_json") from exc
    if isinstance(payload, dict) and payload.get("status") in {402, 429}:
        limiter.defer(3600)
        raise SourceError("rate_limited", retry_after=3600)
    if isinstance(payload, dict) and payload.get("status") == 400:
        message = str(payload.get("msg", "")).lower()
        if "tokenillegal" in message or "invalid token" in message:
            raise SourceError("invalid_token", retry_after=0)
        if "level" in message and ("update" in message or "sponsor" in message):
            raise SourceError("not_entitled", retry_after=0)
        raise SourceError("provider_bad_request", retry_after=0)
    if isinstance(payload, dict) and payload.get("status") == 403 and "ip banned" in str(payload.get("msg", "")).lower():
        limiter.defer(1800)
        raise SourceError("ip_banned", retry_after=1800)
    if not isinstance(payload, dict) or payload.get("status") != 200 or payload.get("msg") != "success":
        raise SourceError("provider_rejected", retry_after=3600)
    rows = payload.get("data")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise SourceError("invalid_rows")
    return rows


def _dates(rows: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    observed = [str(row["date"])[:10] for row in rows
                if isinstance(row.get("date"), str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(row["date"])[:10])]
    return (min(observed), max(observed)) if observed else (None, None)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _store(root: Path, task: Task, rows: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
    if task.kind == "year" and len(task.partition) == 4 and any(
        not isinstance(row.get("date"), str) or
        not re.match(rf"^{re.escape(task.partition)}-\d{{2}}-\d{{2}}", row["date"])
        for row in rows
    ):
        raise SourceError("invalid_year_rows")
    first, last = _dates(rows)
    if task.kind == "year" and len(task.partition) == 4 and rows:
        if first is None or last is None or first[:4] != task.partition or last[:4] != task.partition:
            raise SourceError("wrong_year")
    if task.kind in {"id_history", "derived"}:
        for field in ("stock_id", "futures_id", "option_id"):
            ids = {str(row[field]) for row in rows if row.get(field) is not None}
            if ids and ids != {task.data_id}:
                raise SourceError("wrong_data_id")
    receipt: dict[str, Any] = {
        "schema_version": 1, "dataset": task.dataset, "data_id": task.data_id,
        "partition": task.partition, "kind": task.kind,
        "status": "complete" if rows else "observed_empty", "rows": len(rows),
        "source_first_date": first, "source_last_date": last,
        "fetched_at_utc": _iso(now), "historical_point_in_time": False,
        "coverage_claim": ("derived_from_observed_long_response_not_provider_completeness"
                           if task.kind == "derived" else
                           "observed_response_only_not_provider_completeness"),
    }
    if task.kind == "derived":
        receipt["derived_from"] = LONG_INSTITUTIONAL
    if rows:
        id_hash = hashlib.sha256(task.data_id.encode()).hexdigest()[:12] if task.data_id else "all"
        folder = root / "parquet" / task.dataset / id_hash / task.partition
        folder.mkdir(parents=True, exist_ok=True)
        staged = folder / "latest.parquet"
        atomic_write_parquet(staged, pa.Table.from_pylist(rows), compression="zstd")
        digest = _sha256(staged)
        final = folder / f"{digest}.parquet"
        if final.exists():
            if _sha256(final) != digest:
                raise SourceError("existing_parquet_corrupt")
            staged.unlink()
        else:
            staged.replace(final)
        receipt.update({"parquet_path": str(final.relative_to(root)),
                        "parquet_size_bytes": final.stat().st_size, "sha256": digest})
    receipt_path = root / "receipts" / task.dataset / (
        hashlib.sha256(task.data_id.encode()).hexdigest()[:12] if task.data_id else "all"
    ) / f"{task.partition}.json"
    atomic_write_json(receipt_path, receipt)
    receipt["receipt_path"] = str(receipt_path.relative_to(root))
    return receipt


def _derive_wide(root: Path, connection: sqlite3.Connection, task: Task) -> list[dict[str, Any]]:
    record = connection.execute(
        "SELECT receipt_path FROM tasks WHERE dataset=? AND data_id=? "
        "AND partition='history' AND state='complete'",
        (LONG_INSTITUTIONAL, task.data_id),
    ).fetchone()
    if not record or not record[0]:
        raise SourceError("missing_long_source", retry_after=900)
    receipt_path = (root / record[0]).resolve()
    if not receipt_path.is_relative_to(root.resolve()):
        raise SourceError("unsafe_long_receipt", retry_after=0)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    relative = receipt.get("parquet_path")
    if not isinstance(relative, str):
        raise SourceError("missing_long_parquet", retry_after=900)
    source_path = (root / relative).resolve()
    if not source_path.is_relative_to(root.resolve()) or not source_path.is_file():
        raise SourceError("missing_long_parquet", retry_after=900)
    if _sha256(source_path) != receipt.get("sha256"):
        raise SourceError("corrupt_long_parquet", retry_after=900)
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in pq.read_table(source_path).to_pylist():
        name = row.get("name")
        stock_id = str(row.get("stock_id", ""))
        stamp = row.get("date")
        if name not in INSTITUTIONAL_NAMES or stock_id != task.data_id or not isinstance(stamp, str):
            raise SourceError("invalid_long_row", retry_after=0)
        key = (stamp, stock_id)
        wide = grouped.setdefault(key, {
            "date": stamp, "stock_id": stock_id,
            **{f"{category}_{side}": 0 for category in INSTITUTIONAL_NAMES for side in ("buy", "sell")},
        })
        for side in ("buy", "sell"):
            value = row.get(side)
            if not isinstance(value, (int, float)):
                raise SourceError("invalid_long_row", retry_after=0)
            wide[f"{name}_{side}"] += value
    return [grouped[key] for key in sorted(grouped)]


def _store_bulk_years(connection: sqlite3.Connection, root: Path, task: Task,
                      rows: list[dict[str, Any]], now: datetime, today: date) -> None:
    """Fan out one range response into existing per-year, auditable receipts."""
    first_year = GLOBAL_START_YEAR[task.dataset]
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        stamp = row.get("date")
        if not isinstance(stamp, str) or not re.match(r"^\d{4}-\d{2}-\d{2}", stamp):
            raise SourceError("invalid_bulk_date", retry_after=0)
        year = int(stamp[:4])
        if year < first_year or year > today.year:
            raise SourceError("invalid_bulk_range", retry_after=0)
        grouped.setdefault(year, []).append(row)
    # A provider-side truncated response must not overwrite a nonempty year
    # with an empty receipt. Fall back to the original annual jobs instead.
    for (partition,) in connection.execute(
        "SELECT partition FROM tasks WHERE dataset=? AND kind='year' "
        "AND state='complete' AND rows>0", (task.dataset,),
    ):
        if not grouped.get(int(partition)):
            raise SourceError("incomplete_bulk_response", retry_after=3600)
    for year in range(first_year, today.year + 1):
        year_task = Task(task.dataset, "", str(year), "year",
                         0 if year == today.year else 1, "pending")
        receipt = _store(root, year_task, grouped.get(year, []), now)
        _save_result(connection, year_task, receipt, now)


def _next_refresh(task: Task, now: datetime, *, empty: bool) -> str:
    if task.kind == "snapshot":
        local = now.astimezone(TAIPEI)
        next_check = local.replace(hour=14, minute=0, second=0, microsecond=0)
        if next_check <= local:
            next_check += timedelta(days=1)
        return _iso(next_check)
    if task.kind == "year" and task.partition == str(now.astimezone(TAIPEI).year) and task.dataset in GLOBAL_RELEASE_HOUR_TAIPEI:
        local = now.astimezone(TAIPEI)
        next_check = local.replace(hour=GLOBAL_RELEASE_HOUR_TAIPEI[task.dataset],
                                   minute=10, second=0, microsecond=0)
        while next_check <= local or next_check.weekday() >= 5:
            next_check += timedelta(days=1)
        return _iso(next_check)
    if task.kind == "year" and task.partition == str(now.astimezone(TAIPEI).year):
        delay = timedelta(hours=4)
    elif task.kind == "derived":
        delay = timedelta(days=3650)  # Rebuilt when its long source changes.
    elif empty:
        delay = timedelta(days=90 if task.kind == "year" else 30)
    elif task.kind == "year":
        delay = timedelta(days=90)
    elif task.dataset in GLOBAL_EQUITY_HISTORY:
        delay = timedelta(days=30)
    else:
        delay = timedelta(days=14)
    return _iso(now + delay)


def _save_result(connection: sqlite3.Connection, task: Task, receipt: dict[str, Any], now: datetime) -> None:
    connection.execute(
        "UPDATE tasks SET state=?, next_attempt_at_utc=?, last_attempt_at_utc=?, "
        "rows=?,bytes=?,first_data_date=?,last_data_date=?,receipt_path=?,error_code=NULL "
        "WHERE dataset=? AND data_id=? AND partition=?",
        (receipt["status"], _next_refresh(task, now, empty=receipt["status"] == "observed_empty"),
         _iso(now), receipt["rows"], receipt.get("parquet_size_bytes", 0),
         receipt.get("source_first_date"), receipt.get("source_last_date"), receipt["receipt_path"],
         task.dataset, task.data_id, task.partition),
    )
    if task.dataset == LONG_INSTITUTIONAL and receipt["status"] == "complete":
        connection.execute(
            "UPDATE tasks SET state='pending', next_attempt_at_utc=NULL "
            "WHERE dataset=? AND data_id=? AND partition='history' AND kind='derived'",
            (WIDE_INSTITUTIONAL, task.data_id),
        )
    connection.commit()


def _save_failure(connection: sqlite3.Connection, task: Task, error: SourceError, now: datetime) -> None:
    if error.code == "not_entitled":
        connection.execute(
            "UPDATE tasks SET state='not_entitled', error_code='not_entitled', "
            "next_attempt_at_utc=NULL WHERE dataset=? AND state='pending'",
            (task.dataset,),
        )
    permanent = error.code in {"not_entitled", "invalid_token", "provider_bad_request",
                                "invalid_bulk_date", "invalid_bulk_range"}
    connection.execute(
        "UPDATE tasks SET state=?, error_code=?, last_attempt_at_utc=?, next_attempt_at_utc=? "
        "WHERE dataset=? AND data_id=? AND partition=?",
        ("not_entitled" if error.code == "not_entitled" else
         "invalid_request" if permanent else "failed", error.code,
         _iso(now), None if permanent else _iso(now + timedelta(seconds=error.retry_after)),
         task.dataset, task.data_id, task.partition),
    )
    connection.commit()


def _status(connection: sqlite3.Connection, root: Path, *, state: str,
            active: Task | None = None, last: dict[str, Any] | None = None,
            delegated: frozenset[str] = frozenset()) -> dict[str, Any]:
    summary: dict[str, dict[str, Any]] = {
        dataset: {"target": 0, "complete": 0, "observed_empty": 0, "failed": 0,
                  "not_entitled": 0, "invalid_request": 0,
                  "rows": 0, "bytes": 0, "first_data_date": None,
                  "last_data_date": None, "last_attempt_at_utc": None}
        for dataset in ALL_DATASETS
    }
    for row in connection.execute(
        "SELECT dataset,state,COUNT(*),SUM(rows),SUM(bytes),MIN(first_data_date),"
        "MAX(last_data_date),MAX(last_attempt_at_utc) FROM tasks GROUP BY dataset,state"
    ):
        dataset, task_state, count, rows, size, first, last_date, attempted = row
        if task_state in {"deprecated_query_shape", "outside_documented_range"}:
            continue
        item = summary[dataset]
        item["target"] += count
        if task_state in {"complete", "observed_empty", "failed", "not_entitled", "invalid_request"}:
            item[task_state] += count
        if task_state == "complete":
            item["rows"] += rows or 0
            item["bytes"] += size or 0
            if first and (item["first_data_date"] is None or first < item["first_data_date"]):
                item["first_data_date"] = first
            if last_date and (item["last_data_date"] is None or last_date > item["last_data_date"]):
                item["last_data_date"] = last_date
        if attempted and (item["last_attempt_at_utc"] is None or attempted > item["last_attempt_at_utc"]):
            item["last_attempt_at_utc"] = attempted
    next_task = _next_task(connection, _now(), delegated=delegated)
    current_stock_ids = set(_snapshot_ids(root, "TaiwanStockInfo", "stock_id"))
    delisted_stock_ids = _official_delisted_ids(root)
    result = {
        "schema_version": 1, "state": state, "observed_at_utc": _iso(_now()),
        "delegated_to_sponsor": sorted(delegated),
        "catalog_source": SOURCE_CATALOG, "scheduled_datasets": list(ALL_DATASETS),
        "news": "disabled_by_user", "training": "raw_not_pit_validated",
        "series": summary,
        "candidate_universe": {
            "finmind_current_master_stock_ids": len(current_stock_ids),
            "official_delisted_stock_ids": len(delisted_stock_ids),
            "additional_official_delisted_ids": len(delisted_stock_ids - current_stock_ids),
            "historical_universe_verified_complete": False,
        },
        "active_task": {"dataset": active.dataset, "data_id": active.data_id,
                        "partition": active.partition} if active else None,
        "next_task": {"dataset": next_task.dataset, "data_id": next_task.data_id,
                      "partition": next_task.partition} if next_task else None,
        "last_task": last,
    }
    atomic_write_json(root / "status.json", result)
    return result


def run_once(root: Path, *, max_requests: int = 0) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    load_env_file(Path(__file__).resolve().parents[1] / ".env", allowed_names=("FINMIND_TOKEN",))
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if not token:
        raise RuntimeError("FINMIND_TOKEN is required for the complementary all-free catalog")
    completed = 0
    last: dict[str, Any] | None = None
    with _db(root / "queue.sqlite3") as connection, requests.Session() as session:
        try:
            account = verified_account(session, token, root.parent)
            limiter = rate_limiter(account)
        except (requests.RequestException, RuntimeError, ValueError):
            account = {"tier": "Free", "official_requests_per_hour": 600}
            limiter = rate_limiter(account)
        _populate(connection, root, today=_now().astimezone(TAIPEI).date())
        delegated = _sponsor_delegated(root, _now()) if account["tier"] in {"Sponsor", "SponsorPro"} else frozenset()
        _status(connection, root, state="running", delegated=delegated)
        last_status_at = time.monotonic()
        while not max_requests or completed < max_requests:
            now = _now()
            delegated = _sponsor_delegated(root, now) if account["tier"] in {"Sponsor", "SponsorPro"} else frozenset()
            local = now.astimezone(TAIPEI)
            if shutil.disk_usage(root).free < MIN_FREE_BYTES:
                return _status(connection, root, state="disk_guard", last=last, delegated=delegated)
            calendar = root.parent / "calendar.json"
            try:
                sessions = set(json.loads(calendar.read_text(encoding="utf-8")).get("dates", []))
            except (OSError, ValueError):
                sessions = set()
            market_session = local.date().isoformat() in sessions if sessions else local.weekday() < 5
            if market_session and ((local.hour == 8 and local.minute >= 20) or
                                   (local.hour == 9 and local.minute < 10)):
                return _status(connection, root, state="protected_opening", last=last, delegated=delegated)
            task = _next_task(connection, now, delegated=delegated)
            if task is None:
                return _status(connection, root, state="current_queue", last=last, delegated=delegated)
            if time.monotonic() - last_status_at >= 55:
                _status(connection, root, state="running", active=task, last=last, delegated=delegated)
                last_status_at = time.monotonic()
            try:
                bulk = _bulk_needed(connection, task, local.date())
                rows = (_derive_wide(root, connection, task) if task.kind == "derived" else
                        _request(session, limiter, root.parent, task, token,
                                 today=local.date(), full_history=bulk))
                if bulk:
                    _store_bulk_years(connection, root, task, rows, _now(), local.date())
                    result_status = "complete" if rows else "observed_empty"
                else:
                    receipt = _store(root, task, rows, _now())
                    _save_result(connection, task, receipt, _now())
                    result_status = receipt["status"]
                last = {"dataset": task.dataset, "data_id": task.data_id,
                        "partition": "bulk_history" if bulk else task.partition,
                        "status": result_status, "rows": len(rows)}
                if task.kind == "snapshot" and rows:
                    _populate(connection, root, today=local.date())
            except SourceError as error:
                _save_failure(connection, task, error, _now())
                last = {"dataset": task.dataset, "data_id": task.data_id,
                        "partition": task.partition, "status": "failed", "error_code": error.code}
                if error.code in {"rate_limited", "invalid_token", "ip_banned"}:
                    return _status(connection, root, state=error.code, last=last, delegated=delegated)
            except (OSError, ValueError, pa.ArrowException) as exc:
                error = SourceError("storage_or_schema_error", retry_after=900)
                _save_failure(connection, task, error, _now())
                last = {"dataset": task.dataset, "data_id": task.data_id,
                        "partition": task.partition, "status": "failed",
                        "error_code": error.code, "exception_type": type(exc).__name__}
            completed += int(task.kind != "derived")
            if time.monotonic() - last_status_at >= 55:
                _status(connection, root, state="running", last=last, delegated=delegated)
                last_status_at = time.monotonic()
    return _status(connection, root, state="batch_complete", last=last, delegated=delegated)


def _retry_blocked(root: Path) -> int:
    """Explicit operator action after fixing credentials or request parameters."""
    with _db(root / "queue.sqlite3") as connection:
        cursor = connection.execute(
            "UPDATE tasks SET state='pending', next_attempt_at_utc=NULL, error_code=NULL "
            "WHERE state IN ('not_entitled','invalid_request') AND kind!='year'"
        )
        connection.commit()
        return cursor.rowcount


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_finmind/complement"))
    parser.add_argument("--max-requests", type=int, default=120)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--retry-blocked", action="store_true",
                        help="after correcting token/parameters, explicitly requeue blocked per-ID tasks")
    args = parser.parse_args(argv)
    if args.max_requests < 0:
        parser.error("--max-requests must be nonnegative")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    with (root / "worker.lock").open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("FinMind complement worker already running", file=sys.stderr)
            return 2
        try:
            if args.retry_blocked:
                print(json.dumps({"requeued_blocked_tasks": _retry_blocked(root)}), flush=True)
            while True:
                result = run_once(root, max_requests=args.max_requests)
                print(json.dumps({"state": result["state"], "last_task": result.get("last_task")},
                                 ensure_ascii=False), flush=True)
                if not args.loop:
                    return 0
                if result["state"] == "invalid_token":
                    return 0  # Requires token replacement and an explicit restart.
                time.sleep(3600 if result["state"] in {"current_queue", "disk_guard"}
                           else 1800 if result["state"] == "ip_banned"
                           else 600 if result["state"] in {"rate_limited", "protected_opening", "not_entitled"}
                           else 5)
        except KeyboardInterrupt:
            return 130
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


if __name__ == "__main__":
    raise SystemExit(main())
