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
from urllib3.exceptions import ReadTimeoutError

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet
from downloader.finmind_parent_recovery import repair_content_addressed_collision
from downloader.common import SharedRateLimiter, load_env_file
from downloader.download_finmind_free import API_URL, TAIPEI, _record_request_start
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_batching import RangeBatch
from downloader.finmind_scheduling import PRODUCT_HISTORY_STARTS, fixed_incremental_demand
from downloader.finmind_volume_units import annotate_stock_share_units


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
BULK_GLOBAL_HISTORY = frozenset(GLOBAL_START_YEAR)
BULK_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
BULK_MAX_RESPONSE_ROWS = 1_000_000
GOLD_TIMESTAMP_CONTRACT_VERSION = 1
# One shared-quota probe plus hash-verified local replay establishes the upper
# bound at inclusive midnight, not at the end of that calendar day.
GOLD_RANGE_PROOF = "artifacts/data_quality/finmind_gold_range_20260927T030804094218Z.json"
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
    *PRODUCT_HISTORY_STARTS,
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
assert len(ALL_DATASETS) == len(set(ALL_DATASETS)) == 50
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
    # Sponsor rotates equally ranked whole-market datasets.  The queue can
    # contain hundreds of thousands of daily partitions; without this index,
    # each dispatch scans and sorts the entire priority group again.
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_finmind_priority_dataset_partition "
        "ON tasks(priority, dataset, partition DESC)"
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
        if (path.stat().st_size != receipt.get("parquet_size_bytes")
                or _sha256(path) != receipt.get("sha256")):
            return []
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
    _recover_bulk_year_claims(connection)
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
    _migrate_gold_timestamp_boundary(connection, root, _now())
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
               *, delegated: frozenset[str] = frozenset(),
               incremental_only: bool = False,
               datasets: tuple[str, ...] | None = None) -> Task | None:
    excluded = " AND dataset NOT IN (" + ",".join("?" for _ in delegated) + ")" if delegated else ""
    selected = tuple(datasets) if datasets is not None else ()
    if datasets is not None:
        if not selected:
            return None
        excluded += " AND dataset IN (" + ",".join("?" for _ in selected) + ")"
    if incremental_only:
        excluded += " AND (priority=0 OR kind='derived')"
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
        (_iso(now), *sorted(delegated), *selected, LONG_INSTITUTIONAL),
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
    state = status.get("state")
    # The same token's hourly cooldown also applies to Free. Handing its
    # dataset to per-ID requests cannot acquire an independent quota bucket.
    max_age = timedelta(minutes=35 if state in {"rate_limited", "ip_banned"} else 15)
    if (stamp.tzinfo is None or stamp > now + timedelta(minutes=1) or
            now - stamp > max_age or status.get("tier") not in {"Sponsor", "SponsorPro"}):
        return frozenset()
    if state not in {"running", "batch_complete", "current_queue", "protected_opening",
                     "incremental_reserve", "waiting_necessary_acquisition", "rate_limited", "ip_banned"}:
        return frozenset()
    series = status.get("series")
    if not isinstance(series, dict):
        return frozenset()
    candidates = set(TW_SYMBOL_HISTORY) | (set(DERIVATIVE_HISTORY) - set(PRODUCT_HISTORY_STARTS)) | {
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


class _GlobalYearBatch(RangeBatch[Task]):
    """Reuse the shared request envelope for Complement's YYYY partitions."""

    def params(self) -> dict[str, str]:
        params = super().params()
        if self.dataset == "GoldPrice":
            params["end_date"] = (self.end_date + timedelta(days=1)).isoformat()
        return params

    def metadata(self) -> dict[str, Any]:
        metadata = {
            "complement_year_batch_contract_version": 1,
            "query_shape": "whole_market_inclusive_date_range",
            "request_start_date": self.start_date.isoformat(),
            "request_end_date": self.end_date.isoformat(),
            "request_end_inclusive": True, "request_count": 1,
            "request_id": self.request_id,
            "partition_count": len(self.tasks),
            "partitions": [task.partition for task in self.tasks],
            "documentation_url": SOURCE_CATALOG,
            "decoded_response_byte_limit": BULK_MAX_RESPONSE_BYTES,
            "observed_through": self.observed_through.isoformat(),
        }
        if self.dataset == "GoldPrice":
            metadata.update({
                "gold_timestamp_contract_version": GOLD_TIMESTAMP_CONTRACT_VERSION,
                "query_shape": "whole_market_naive_timestamp_range_client_half_open",
                "request_end_date": self.params()["end_date"],
                "request_end_inclusive": True,
                "request_end_semantics": "naive_midnight_not_inclusive_calendar_day",
                "covered_start_date": self.start_date.isoformat(),
                "covered_end_date": self.end_date.isoformat(),
                "covered_end_timestamp_exclusive": f"{self.params()['end_date']} 00:00:00",
                "current_day_is_partial": self.end_date == self.observed_through,
                "source_timezone": "not_declared_naive_provider_timestamp",
                "documentation_url": "https://finmind.github.io/tutor/Materials/",
                "range_boundary_proof": GOLD_RANGE_PROOF,
            })
        return metadata


def _gold_covered_rows(rows: list[dict[str, Any]], start: date, end: date,
                       ) -> tuple[list[dict[str, Any]], int]:
    """Keep the whole covered end day and discard only next midnight context."""
    if len(rows) > BULK_MAX_RESPONSE_ROWS:
        raise SourceError("bulk_response_row_limit", retry_after=60)
    lower = datetime.combine(start, datetime.min.time())
    upper = datetime.combine(end + timedelta(days=1), datetime.min.time())
    covered = []
    excluded = 0
    for row in rows:
        raw = row.get("date")
        try:
            if not isinstance(raw, str) or len(raw) < 10:
                raise ValueError("missing timestamp")
            stamp = datetime.fromisoformat(raw)
            if stamp.tzinfo is not None or stamp.date().isoformat() != raw[:10]:
                raise ValueError("unproven source timestamp clock")
        except (TypeError, ValueError):
            raise SourceError("invalid_bulk_date", retry_after=0)
        if not lower <= stamp <= upper or ("dataset" in row and row["dataset"] != "GoldPrice"):
            raise SourceError("invalid_bulk_range", retry_after=0)
        if stamp == upper:
            excluded += 1
        else:
            covered.append(row)
    return covered, excluded


def _migrate_gold_timestamp_boundary(connection: sqlite3.Connection, root: Path, now: datetime) -> int:
    """Recheck each unproven legacy year once, preserving exact old evidence."""
    connection.execute(
        "CREATE TABLE IF NOT EXISTS gold_timestamp_boundary_migration ("
        "version INTEGER NOT NULL,partition TEXT NOT NULL,observed_at_utc TEXT NOT NULL,"
        "prior_task_json TEXT NOT NULL,prior_receipt_sha256 TEXT,prior_receipt_json TEXT,"
        "PRIMARY KEY(version,partition))"
    )
    cursor = connection.execute(
        "SELECT * FROM tasks WHERE dataset='GoldPrice' AND data_id='' AND kind='year' "
        "AND state IN ('complete','observed_empty') AND NOT EXISTS ("
        "SELECT 1 FROM gold_timestamp_boundary_migration m "
        "WHERE m.version=? AND m.partition=tasks.partition)",
        (GOLD_TIMESTAMP_CONTRACT_VERSION,),
    )
    columns = [column[0] for column in cursor.description]
    selected = [dict(zip(columns, row)) for row in cursor.fetchall()]
    changed = 0
    for prior in selected:
        receipt_raw = None
        receipt = {}
        if prior.get("receipt_path"):
            path = (root / prior["receipt_path"]).resolve()
            if not path.is_relative_to(root.resolve()):
                raise SourceError("unsafe_gold_receipt_path", retry_after=0)
            try:
                receipt_raw = path.read_bytes()
                receipt = json.loads(receipt_raw)
            except (OSError, ValueError):
                # An absent/malformed old receipt is not proof of a safe upper
                # bound; retain its bytes when present and recheck that year.
                receipt = {}
        request = receipt.get("request", {}) if isinstance(receipt, dict) else {}
        if isinstance(request, dict) and request.get("gold_timestamp_contract_version") == GOLD_TIMESTAMP_CONTRACT_VERSION:
            continue
        connection.execute(
            "INSERT INTO gold_timestamp_boundary_migration VALUES (?,?,?,?,?,?)",
            (GOLD_TIMESTAMP_CONTRACT_VERSION, prior["partition"], _iso(now), json.dumps(prior, sort_keys=True),
             hashlib.sha256(receipt_raw).hexdigest() if receipt_raw is not None else None,
             receipt_raw.decode("utf-8") if receipt_raw is not None else None),
        )
        connection.execute(
            "UPDATE tasks SET state='pending',next_attempt_at_utc=?,error_code='timestamp_end_boundary_repair_due' "
            "WHERE dataset='GoldPrice' AND data_id='' AND partition=?",
            (_iso(now), prior["partition"]),
        )
        changed += 1
    return changed


def _bulk_year_schema(connection: sqlite3.Connection) -> None:
    connection.execute("CREATE TABLE IF NOT EXISTS complement_year_batch_policy ("
                       "dataset TEXT PRIMARY KEY,max_partitions INTEGER NOT NULL,"
                       "error_code TEXT NOT NULL,observed_at_utc TEXT NOT NULL)")
    connection.execute("CREATE TABLE IF NOT EXISTS complement_year_batch_claims ("
                       "dataset TEXT NOT NULL,partition TEXT NOT NULL,prior_state TEXT NOT NULL,"
                       "request_id TEXT NOT NULL,PRIMARY KEY(dataset,partition))")
    connection.execute("CREATE TABLE IF NOT EXISTS complement_year_batch_failures ("
                       "id INTEGER PRIMARY KEY AUTOINCREMENT,failed_at_utc TEXT NOT NULL,"
                       "dataset TEXT NOT NULL,error_code TEXT NOT NULL,"
                       "request_metadata_json TEXT NOT NULL,prior_tasks_json TEXT NOT NULL)")


def _recover_bulk_year_claims(connection: sqlite3.Connection) -> None:
    """A restarted single worker restores only its unfinished batch claims."""
    with connection:
        _bulk_year_schema(connection)
        connection.execute(
            "UPDATE tasks SET state=(SELECT prior_state FROM complement_year_batch_claims c "
            "WHERE c.dataset=tasks.dataset AND c.partition=tasks.partition) "
            "WHERE state='inflight' AND data_id='' AND EXISTS ("
            "SELECT 1 FROM complement_year_batch_claims c "
            "WHERE c.dataset=tasks.dataset AND c.partition=tasks.partition)"
        )
        connection.execute("DELETE FROM complement_year_batch_claims")


def _claim_bulk_years(connection: sqlite3.Connection, task: Task, now: datetime,
                      *, allow_history: bool) -> _GlobalYearBatch | None:
    """Claim one contiguous due interval without importing not-due years."""
    if task.dataset not in BULK_GLOBAL_HISTORY or task.kind != "year" or task.data_id:
        return None
    if not allow_history and (task.dataset != "GoldPrice" or task.priority != 0):
        return None
    today = now.astimezone(TAIPEI).date()
    connection.execute("SAVEPOINT complement_year_claim")
    try:
        _bulk_year_schema(connection)
        policy = connection.execute(
            "SELECT max_partitions FROM complement_year_batch_policy WHERE dataset=?", (task.dataset,),
        ).fetchone()
        limit = policy[0] if policy else today.year - GLOBAL_START_YEAR[task.dataset] + 1
        if not allow_history:
            limit = 1
        rows = connection.execute(
            "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
            "WHERE dataset=? AND data_id='' AND kind='year' AND priority>=0 AND priority<8 "
            "AND ((state='pending' AND next_attempt_at_utc IS NULL) OR "
            "(state IN ('pending','complete','observed_empty','failed') AND next_attempt_at_utc<=?))",
            (task.dataset, _iso(now)),
        ).fetchall()
        eligible = {int(row[2]): Task(*row) for row in rows
                    if re.fullmatch(r"\d{4}", row[2])
                    and GLOBAL_START_YEAR[task.dataset] <= int(row[2]) <= today.year}
        year = int(task.partition)
        if year not in eligible or (limit < 2 and task.dataset != "GoldPrice"):
            connection.execute("RELEASE SAVEPOINT complement_year_claim")
            return None
        low = high = year
        while low - 1 in eligible and high - low + 1 < limit:
            low -= 1
        while high + 1 in eligible and high - low + 1 < limit:
            high += 1
        if low == high and task.dataset != "GoldPrice":
            connection.execute("RELEASE SAVEPOINT complement_year_claim")
            return None
        batch = _GlobalYearBatch(task.dataset, "year", tuple(eligible[y] for y in range(low, high + 1)),
                                 date(low, 1, 1), min(date(high, 12, 31), today), today)
        for part in batch.tasks:
            connection.execute("INSERT INTO complement_year_batch_claims VALUES (?,?,?,?)",
                               (part.dataset, part.partition, part.state, batch.request_id))
            changed = connection.execute(
                "UPDATE tasks SET state='inflight' WHERE dataset=? AND data_id='' AND partition=? AND state=?",
                (part.dataset, part.partition, part.state),
            )
            if changed.rowcount != 1:
                raise SourceError("bulk_claim_changed", retry_after=60)
        connection.execute("RELEASE SAVEPOINT complement_year_claim")
        return batch
    except BaseException:
        connection.execute("ROLLBACK TO SAVEPOINT complement_year_claim")
        connection.execute("RELEASE SAVEPOINT complement_year_claim")
        raise


def _release_bulk_years(connection: sqlite3.Connection, batch: _GlobalYearBatch) -> None:
    with connection:
        for task in batch.tasks:
            connection.execute(
                "UPDATE tasks SET state=? WHERE dataset=? AND data_id='' AND partition=? AND state='inflight'",
                (task.state, task.dataset, task.partition),
            )
        connection.execute("DELETE FROM complement_year_batch_claims WHERE request_id=?", (batch.request_id,))


def _fail_bulk_years(connection: sqlite3.Connection, batch: _GlobalYearBatch,
                     error: SourceError, now: datetime) -> None:
    resource = error.code in {"response_size_limit", "bulk_response_row_limit", "ReadTimeout",
                              "ConnectTimeout", "Timeout", "http_502", "http_504"}
    invalid_contract = error.code in {"provider_bad_request", "invalid_rows", "invalid_bulk_date",
                                     "invalid_bulk_range", "invalid_bulk_schema", "incomplete_bulk_response"}
    with connection:
        prior = []
        for task in batch.tasks:
            cursor = connection.execute("SELECT * FROM tasks WHERE dataset=? AND data_id='' AND partition=?",
                                        (task.dataset, task.partition))
            row = cursor.fetchone()
            if row:
                item = dict(zip((column[0] for column in cursor.description), row))
                item["before_claim_state"] = task.state
                prior.append(item)
        connection.execute(
            "INSERT INTO complement_year_batch_failures "
            "(failed_at_utc,dataset,error_code,request_metadata_json,prior_tasks_json) VALUES (?,?,?,?,?)",
            (_iso(now), batch.dataset, error.code, json.dumps(batch.metadata(), sort_keys=True),
             json.dumps(prior, sort_keys=True)),
        )
        if resource or invalid_contract:
            limit = max(1, len(batch.tasks) // 2) if resource else 1
            connection.execute(
                "INSERT INTO complement_year_batch_policy VALUES (?,?,?,?) ON CONFLICT(dataset) DO UPDATE SET "
                "max_partitions=min(complement_year_batch_policy.max_partitions,excluded.max_partitions),"
                "error_code=excluded.error_code,observed_at_utc=excluded.observed_at_utc",
                (batch.dataset, limit, error.code, _iso(now)),
            )
            for task in batch.tasks:
                connection.execute(
                    "UPDATE tasks SET state=?,next_attempt_at_utc=?,error_code=?,last_attempt_at_utc=? "
                    "WHERE dataset=? AND data_id='' AND partition=? AND state='inflight'",
                    (task.state, _iso(now + timedelta(seconds=60 if resource else max(60, error.retry_after))), error.code,
                     _iso(now), task.dataset, task.partition),
                )
        else:
            for task in batch.tasks:
                if connection.execute(
                    "SELECT state FROM tasks WHERE dataset=? AND data_id='' AND partition=?",
                    (task.dataset, task.partition),
                ).fetchone() == ("inflight",):
                    _save_failure(connection, task, error, now)


def _request(session: requests.Session, limiter: SharedRateLimiter, root: Path,
             task: Task, token: str, *, today: date,
             full_history: bool = False) -> list[dict[str, Any]]:
    params: dict[str, str] = {"dataset": task.dataset}
    product_history = task.dataset in PRODUCT_HISTORY_STARTS
    if product_history and (task.kind != "id_history" or not task.data_id):
        raise SourceError("provider_bad_request", retry_after=0)
    if task.data_id:
        params["data_id"] = task.data_id
    if task.kind == "year":
        year = int(task.partition)
        params["start_date"] = (f"{GLOBAL_START_YEAR[task.dataset]}-01-01" if full_history
                                else f"{year}-01-01")
        if not full_history:
            params["end_date"] = min(date(year, 12, 31), today).isoformat()
        if task.dataset == "GoldPrice":
            covered_end = min(date(year, 12, 31), today)
            params["end_date"] = (covered_end + timedelta(days=1)).isoformat()
            rows = _fetch_rows(session, limiter, root, task.dataset, token, params,
                               max_response_bytes=BULK_MAX_RESPONSE_BYTES)
            return _gold_covered_rows(rows, date.fromisoformat(params["start_date"]), covered_end)[0]
    elif task.kind == "id_history":
        params["start_date"] = {
            "TaiwanExchangeRate": "2006-01-01",
            "TaiwanFuturesDealerTradingVolumeDaily": "2021-04-01",
            "TaiwanOptionDealerTradingVolumeDaily": "2021-04-01",
            "TaiwanStockCapitalReductionReferencePrice": "2011-01-01",
        }.get(task.dataset, "1900-01-01")
        if product_history:
            params["start_date"] = PRODUCT_HISTORY_STARTS[task.dataset].isoformat()
            params["end_date"] = today.isoformat()
    if product_history:
        rows = _fetch_rows(session, limiter, root, task.dataset, token, params,
                           max_response_bytes=BULK_MAX_RESPONSE_BYTES)
        if len(rows) > BULK_MAX_RESPONSE_ROWS:
            raise SourceError("response_row_limit")
        for row in rows:
            stamp = row.get("date")
            try:
                observed = date.fromisoformat(stamp[:10]) if isinstance(stamp, str) else None
            except ValueError:
                observed = None
            if (observed is None or stamp[:10] != observed.isoformat()
                    or not PRODUCT_HISTORY_STARTS[task.dataset] <= observed <= today):
                raise SourceError("response_outside_partition", retry_after=3600)
            identifier_field = "option_id" if "Option" in task.dataset else "futures_id"
            if row.get(identifier_field) != task.data_id:
                raise SourceError("wrong_data_id", retry_after=3600)
        return rows
    if task.kind == "year" and task.dataset in BULK_GLOBAL_HISTORY and not full_history:
        return _fetch_rows(session, limiter, root, task.dataset, token, params,
                           max_response_bytes=BULK_MAX_RESPONSE_BYTES)
    return _fetch_rows(session, limiter, root, task.dataset, token, params)


def _fetch_rows(session: requests.Session, limiter: SharedRateLimiter, root: Path,
                dataset: str, token: str, params: dict[str, str],
                *, endpoint: str = API_URL,
                max_response_bytes: int | None = None) -> list[dict[str, Any]]:
    """One authenticated request, optionally bounding the decoded HTTP body.

    The bound controls local buffering, not provider entitlement or query range.
    ``None`` preserves the ordinary, non-streaming request path.
    """
    if max_response_bytes is not None and (
        isinstance(max_response_bytes, bool)
        or not isinstance(max_response_bytes, int)
        or max_response_bytes <= 0
    ):
        raise ValueError("max_response_bytes must be a positive integer")
    limiter.wait()
    _record_request_start(root, dataset)
    try:
        response = session.get(endpoint, params=params, headers={"Authorization": f"Bearer {token}"},
                               timeout=(10, 90),
                               **({"stream": True} if max_response_bytes is not None else {}))
        try:
            return _response_rows(response, limiter, max_response_bytes=max_response_bytes)
        finally:
            if max_response_bytes is not None:
                response.close()
    except requests.RequestException as exc:
        # Request exceptions can contain URLs or provider text. Keep only their
        # classification in persisted errors and tracebacks.
        code = type(exc).__name__
        if isinstance(exc, requests.ConnectionError):
            # requests.iter_content wraps urllib3 read timeouts in a generic
            # ConnectionError. Inspect exception types, never message text, so
            # genuine connection failures do not trigger range-size backoff.
            pending: list[BaseException] = [exc]
            seen: set[int] = set()
            while pending:
                nested = pending.pop()
                if id(nested) in seen:
                    continue
                seen.add(id(nested))
                if isinstance(nested, ReadTimeoutError):
                    code = "ReadTimeout"
                    break
                pending.extend(
                    value for value in (*nested.args, nested.__cause__, nested.__context__)
                    if isinstance(value, BaseException)
                )
        raise SourceError(code) from None


def _response_json(response: requests.Response, max_response_bytes: int | None) -> Any:
    if max_response_bytes is None:
        return response.json()
    body = bytearray()
    # iter_content transparently decodes transfer/content encodings. A wire
    # Content-Length can describe compressed bytes and is not this RAM bound.
    for chunk in response.iter_content(chunk_size=min(64 * 1024, max_response_bytes + 1)):
        if len(body) + len(chunk) > max_response_bytes:
            raise SourceError("response_size_limit", retry_after=0)
        body.extend(chunk)
    return json.loads(body)


def _response_rows(response: requests.Response, limiter: SharedRateLimiter,
                   *, max_response_bytes: int | None) -> list[dict[str, Any]]:
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
            message = str(_response_json(response, max_response_bytes).get("msg", "")).lower()
        except (ValueError, AttributeError):
            message = ""
        if "ip banned" in message:
            limiter.defer(1800)
            raise SourceError("ip_banned", retry_after=1800)
        raise SourceError("not_entitled", retry_after=0)
    if response.status_code == 400:
        try:
            message = str(_response_json(response, max_response_bytes).get("msg", "")).lower()
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
        payload = _response_json(response, max_response_bytes)
    except ValueError:
        raise SourceError("invalid_json") from None
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


def _store(root: Path, task: Task, rows: list[dict[str, Any]], now: datetime, *,
           request_metadata: dict[str, Any] | None = None,
           correction: dict[str, Any] | None = None) -> dict[str, Any]:
    if correction and not rows and not correction.get('allow_empty'):
        previous_path = root / 'receipts' / task.dataset / (
            hashlib.sha256(task.data_id.encode()).hexdigest()[:12] if task.data_id else 'all'
        ) / f'{task.partition}.json'
        if previous_path.is_file():
            previous = json.loads(previous_path.read_bytes())
            if int(previous.get('rows') or 0) > 0:
                raise SourceError('unexpected_empty_after_nonempty', retry_after=900)
    rows, volume_units = annotate_stock_share_units(task.dataset, rows)
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
    # Sponsor's all-market wide frame is derived from an all-market long
    # partition and legitimately has an empty data_id. Per-symbol derivations
    # must still match their one requested identifier exactly.
    if task.kind == "id_history" or (task.kind == "derived" and task.data_id):
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
        "volume_units": volume_units,
    }
    if task.kind == "derived":
        receipt["derived_from"] = LONG_INSTITUTIONAL
    if request_metadata is not None:
        receipt["request"] = request_metadata
    if correction:
        from downloader.finmind_corrections import correction_receipt_metadata

        if any(correction.get(key) != getattr(task, key) for key in ('dataset', 'data_id', 'partition')):
            raise SourceError('correction_identity_mismatch')
        authoritative_empty = not rows and correction.get('allow_empty') is True
        receipt.update(correction_receipt_metadata(correction, authoritative_empty=authoritative_empty))
        if authoritative_empty:
            receipt['status'] = 'complete'
            receipt['coverage_claim'] = 'official_correction_exact_day_authoritative_empty'
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
                installed = repair_content_addressed_collision(staged, final, root)
                if not installed:
                    staged.unlink()
            else:
                staged.unlink()
        else:
            staged.replace(final)
        receipt.update({"parquet_path": str(final.relative_to(root)),
                        "parquet_size_bytes": final.stat().st_size, "sha256": digest})
    receipt_path = root / "receipts" / task.dataset / (
        hashlib.sha256(task.data_id.encode()).hexdigest()[:12] if task.data_id else "all"
    ) / f"{task.partition}.json"
    # Refreshing mutable provider values must not erase the old fetch-time /
    # source-hash proof. Raw Parquet was already content-addressed; preserve
    # the corresponding exact-byte receipt before advancing its current head.
    if receipt_path.is_file():
        previous = receipt_path.read_bytes()
        previous_digest = hashlib.sha256(previous).hexdigest()
        archived = root / "receipt_history" / receipt_path.relative_to(root / "receipts").with_suffix('') / f"{previous_digest}.json"
        if archived.exists():
            if archived.read_bytes() != previous:
                raise SourceError("receipt_history_corrupt", retry_after=0)
        else:
            atomic_write_bytes(archived, previous, durable=True)
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


def _preflight_bulk_year_schemas(dataset: str, grouped: dict[str, list[dict[str, Any]]]) -> None:
    """Validate every year's canonical Arrow input before advancing any head.

    ``from_pylist`` infers top-level fields from the first row and can silently
    drop keys added by later rows. Reject heterogeneous provider field sets
    across the whole response, including across year boundaries, rather than
    accepting a lossy annual partition. Do not retain a second full-response
    Arrow copy: validate and release one year's annotated table at a time.
    These documented range endpoints have flat scalar fields. Nested Arrow
    types are not that contract; an empty struct would also pass Arrow creation
    but fail later at Parquet serialization, after another year had advanced.
    """
    fields: frozenset[str] | None = None
    for rows in grouped.values():
        for row in rows:
            names = frozenset(row)
            if any(not isinstance(name, str) for name in names):
                raise SourceError("invalid_bulk_schema", retry_after=3600)
            if fields is None:
                fields = names
            elif names != fields:
                raise SourceError("invalid_bulk_schema", retry_after=3600)
    for rows in grouped.values():
        if not rows:
            continue
        try:
            annotated, _ = annotate_stock_share_units(dataset, rows)
            table = pa.Table.from_pylist(annotated)
        except (pa.ArrowException, TypeError, ValueError):
            raise SourceError("invalid_bulk_schema", retry_after=3600) from None
        nested = any(pa.types.is_nested(field.type) for field in table.schema)
        del table, annotated
        if nested:
            raise SourceError("invalid_bulk_schema", retry_after=3600)


def _store_bulk_years(connection: sqlite3.Connection, root: Path, task: Task,
                      rows: list[dict[str, Any]], now: datetime, today: date,
                      *, batch: _GlobalYearBatch | None = None) -> int:
    """Fan out one range response into existing per-year, auditable receipts."""
    first_year = GLOBAL_START_YEAR[task.dataset]
    # Old explicit full-history callers retain their selected full interval;
    # the worker always supplies the exact due tasks claimed before its HTTP.
    selected = (batch.tasks if batch is not None else tuple(
        Task(task.dataset, "", str(year), "year", 0 if year == today.year else 1, "pending")
        for year in range(first_year, today.year + 1)
    ))
    start = batch.start_date if batch else date(first_year, 1, 1)
    end = batch.end_date if batch else today
    response_rows = len(rows)
    excluded_midnight = 0
    if task.dataset == "GoldPrice":
        rows, excluded_midnight = _gold_covered_rows(rows, start, end)
    if len(rows) > BULK_MAX_RESPONSE_ROWS:
        raise SourceError("bulk_response_row_limit", retry_after=60)
    grouped: dict[str, list[dict[str, Any]]] = {part.partition: [] for part in selected}
    for row in rows:
        stamp = row.get("date")
        try:
            if not isinstance(stamp, str) or len(stamp) < 10:
                raise ValueError("missing date")
            day = date.fromisoformat(stamp[:10])
            if day.isoformat() != stamp[:10]:
                raise ValueError("noncanonical date")
            if len(stamp) > 10:
                if stamp[10] not in {"T", " "}:
                    raise ValueError("invalid timestamp delimiter")
                datetime.fromisoformat(stamp)
        except (TypeError, ValueError):
            raise SourceError("invalid_bulk_date", retry_after=0)
        if (not start <= day <= end or str(day.year) not in grouped
                or ("dataset" in row and row["dataset"] != task.dataset)):
            raise SourceError("invalid_bulk_range", retry_after=0)
        grouped[str(day.year)].append(row)
    # A provider-side truncated response must not overwrite a nonempty year
    # with an empty receipt. Fall back to the original annual jobs instead.
    for (partition,) in connection.execute(
        "SELECT partition FROM tasks WHERE dataset=? AND kind='year' "
        "AND rows>0", (task.dataset,),
    ):
        if partition in grouped and not grouped[partition]:
            raise SourceError("incomplete_bulk_response", retry_after=3600)
    _preflight_bulk_year_schemas(task.dataset, grouped)
    metadata = batch.metadata() if batch is not None else None
    if metadata is not None:
        metadata.update({"response_rows": response_rows, "stored_rows": len(rows),
                         "response_partition_rows": {key: len(value) for key, value in grouped.items()}})
        if task.dataset == "GoldPrice":
            metadata["excluded_next_midnight_rows"] = excluded_midnight
    for year_task in selected:
        from downloader.finmind_corrections import correction_context

        context = correction_context(connection, year_task)
        receipt = _store(root, year_task, grouped[year_task.partition], now, request_metadata=metadata,
                         **({'correction': context} if context else {}))
        _save_result(connection, year_task, receipt, now)
    return len(rows)


def _next_refresh(task: Task, now: datetime, *, empty: bool) -> str:
    if task.dataset in PRODUCT_HISTORY_STARTS and task.kind == "id_history" and not empty:
        return _iso(now + timedelta(hours=3))
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
    if task.dataset in PRODUCT_HISTORY_STARTS and receipt["status"] == "complete" and receipt["rows"] > 0:
        connection.execute("UPDATE tasks SET priority=0 WHERE dataset=? AND data_id=? AND partition=?",
                           (task.dataset, task.data_id, task.partition))
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


def run_once(root: Path, *, max_requests: int = 0,
             datasets: tuple[str, ...] | None = None) -> dict[str, Any]:
    if datasets is not None:
        datasets = tuple(dict.fromkeys(datasets))
        unknown = set(datasets) - set(ALL_DATASETS)
        if not datasets or unknown:
            raise ValueError(f"datasets must select existing Complement sources; unknown={sorted(unknown)}")
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
        from downloader.finmind_corrections import apply_worker_corrections, correction_context, reconcile_worker_corrections

        _populate(connection, root, today=_now().astimezone(TAIPEI).date())
        reconcile_worker_corrections(connection, root, 'complement', _now())
        correction_summary = apply_worker_corrections(connection, root, 'complement', _now())
        connection.commit()
        atomic_write_json(root / 'correction_status.json', {'observed_at_utc': _iso(_now()), **correction_summary})
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
            budget = backfill_budget(
                account, root.parent, fixed_incremental_requests=fixed_incremental_demand(root.parent, now),
                in_flight=0, now=now,
            )
            task = _next_task(connection, now, delegated=delegated,
                              incremental_only=not budget["allowed"], datasets=datasets)
            if task is None:
                return _status(connection, root,
                               state="current_queue" if budget["allowed"] else "incremental_reserve",
                               last=last, delegated=delegated)
            if time.monotonic() - last_status_at >= 55:
                _status(connection, root, state="running", active=task, last=last, delegated=delegated)
                last_status_at = time.monotonic()
            bulk = None
            try:
                bulk = _claim_bulk_years(connection, task, now, allow_history=budget["allowed"])
                if bulk is not None:
                    rows = _fetch_rows(session, limiter, root.parent, task.dataset, token, bulk.params(),
                                       max_response_bytes=BULK_MAX_RESPONSE_BYTES)
                    stored_rows = _store_bulk_years(connection, root, task, rows, _now(), local.date(), batch=bulk)
                    result_status = "complete" if stored_rows else "observed_empty"
                else:
                    rows = (_derive_wide(root, connection, task) if task.kind == "derived" else
                            _request(session, limiter, root.parent, task, token, today=local.date()))
                    if not rows and ((task.kind == "year" and task.dataset in BULK_GLOBAL_HISTORY)
                                     or task.dataset in PRODUCT_HISTORY_STARTS):
                        previous = connection.execute(
                            "SELECT rows FROM tasks WHERE dataset=? AND data_id=? AND partition=?",
                            (task.dataset, task.data_id, task.partition),
                        ).fetchone()
                        if previous and previous[0] > 0:
                            raise SourceError("unexpected_empty_after_nonempty", retry_after=3600)
                    metadata = ({
                        "query_shape": "product_history_inclusive_date_range",
                        "data_id": task.data_id,
                        "request_start_date": PRODUCT_HISTORY_STARTS[task.dataset].isoformat(),
                        "request_end_date": local.date().isoformat(),
                        "request_end_inclusive": True, "request_count": 1,
                        "source_date_semantics": "expiry_date",
                        "historical_product_universe_verified_complete": False,
                    } if task.dataset in PRODUCT_HISTORY_STARTS else None)
                    if task.kind == "year" and task.dataset in BULK_GLOBAL_HISTORY:
                        _preflight_bulk_year_schemas(task.dataset, {task.partition: rows})
                    context = correction_context(connection, task)
                    receipt = _store(root, task, rows, _now(), request_metadata=metadata,
                                     **({'correction': context} if context else {}))
                    _save_result(connection, task, receipt, _now())
                    result_status = receipt["status"]
                    stored_rows = len(rows)
                last = {"dataset": task.dataset, "data_id": task.data_id,
                        "partition": "bulk_history" if bulk else task.partition,
                        "status": result_status, "rows": stored_rows}
                if bulk is not None:
                    last["request_batch"] = bulk.metadata()
                    last["request_batch"].update({"response_rows": len(rows), "stored_rows": stored_rows})
                if task.kind == "snapshot" and rows:
                    _populate(connection, root, today=local.date())
                reconcile_worker_corrections(connection, root, 'complement', _now())
                connection.commit()
            except SourceError as error:
                if bulk is not None:
                    _fail_bulk_years(connection, bulk, error, _now())
                else:
                    _save_failure(connection, task, error, _now())
                last = {"dataset": task.dataset, "data_id": task.data_id,
                        "partition": task.partition, "status": "failed", "error_code": error.code}
                if error.code in {"rate_limited", "invalid_token", "ip_banned"}:
                    return _status(connection, root, state=error.code, last=last, delegated=delegated)
            except (OSError, ValueError, pa.ArrowException) as exc:
                error = SourceError("storage_or_schema_error", retry_after=900)
                if bulk is not None:
                    _fail_bulk_years(connection, bulk, error, _now())
                else:
                    _save_failure(connection, task, error, _now())
                last = {"dataset": task.dataset, "data_id": task.data_id,
                        "partition": task.partition, "status": "failed",
                        "error_code": error.code, "exception_type": type(exc).__name__}
            finally:
                if bulk is not None:
                    _release_bulk_years(connection, bulk)
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
    parser.add_argument("--dataset", action="append", choices=sorted(ALL_DATASETS),
                        help="Dispatch only this existing dataset; repeat to select more than one")
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
                result = run_once(root, max_requests=args.max_requests,
                                  datasets=tuple(args.dataset) if args.dataset is not None else None)
                print(json.dumps({"state": result["state"], "last_task": result.get("last_task")},
                                 ensure_ascii=False), flush=True)
                if not args.loop:
                    return 0
                if result["state"] == "invalid_token":
                    return 0  # Requires token replacement and an explicit restart.
                time.sleep(3600 if result["state"] in {"current_queue", "disk_guard"}
                           else 1800 if result["state"] == "ip_banned"
                           else 600 if result["state"] in {"rate_limited", "protected_opening", "not_entitled"}
                           else 60 if result["state"] == "incremental_reserve" else 5)
        except KeyboardInterrupt:
            return 130
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


if __name__ == "__main__":
    raise SystemExit(main())
