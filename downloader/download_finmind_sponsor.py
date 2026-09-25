"""Sponsor-tier whole-market historical backfill, separate from Free receipts.

Only documented data_id-free query shapes are enabled.  Per-stock/day tick,
minute and broker-branch tables are catalogued but not falsely labelled as a
whole-market Sponsor download: those need millions of calls or SponsorPro's
different storage-object entitlement.  No news is requested.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import fcntl
import json
import os
from pathlib import Path
import requests
import shutil
import sqlite3
import sys
import threading
import time
from typing import Any

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import (
    INSTITUTIONAL_NAMES, LONG_INSTITUTIONAL, WIDE_INSTITUTIONAL,
    SourceError, Task, _db, _fetch_rows, _sha256, _store,
)
from downloader.download_finmind_free import TAIPEI
from downloader.finmind_account import rate_limiter, verified_account


CATALOG_URL = "https://finmind.github.io/llms-full.txt"
MIN_FREE_BYTES = 25 * 1024**3
_THREAD = threading.local()


@dataclass(frozen=True)
class Source:
    dataset: str
    first_date: date | None
    grain: str  # snapshot, day, two_day, month, year
    priority: int = 2
    release_hour: int = 14


def _s(dataset: str, first: str | None, grain: str, priority: int = 2,
       release_hour: int = 14) -> Source:
    return Source(dataset, date.fromisoformat(first) if first else None,
                  grain, priority, release_hour)


# Entire-market queries documented for Backer/Sponsor.  Big rows use one/two
# days; sparse fundamentals use monthly/annual chunks. Never fetch an open-ended
# all-market history response into one Python process.
SOURCES = (
    _s("TaiwanStockPrice", "1994-10-01", "day", 1, 18),
    _s("TaiwanStockPriceAdj", "1994-10-01", "day", 2, 20),
    _s("TaiwanStockDayTrading", "2014-01-01", "day", 1, 18),
    _s("TaiwanStockPriceLimit", "2000-01-01", "day", 1, 18),
    _s("TaiwanStockMarginPurchaseShortSale", "2001-01-01", "day", 1, 21),
    _s("TaiwanStockInstitutionalInvestorsBuySell", "2005-01-01", "day", 1, 18),
    _s("TaiwanStockInstitutionalInvestorsBuySellWide", "2005-01-01", "day", 2, 18),
    _s("TaiwanStockShareholding", "2004-02-01", "day", 2, 21),
    _s("TaiwanStockSecuritiesLending", "2001-05-01", "day", 2, 21),
    _s("TaiwanStockMarginShortSaleSuspension", "2015-01-01", "year"),
    _s("TaiwanDailyShortSaleBalances", "2005-07-01", "day"),
    _s("TaiwanStockFinancialStatements", "1990-03-01", "day", 1),
    _s("TaiwanStockBalanceSheet", "2011-12-01", "day", 1),
    _s("TaiwanStockCashFlowsStatement", "2008-06-01", "day", 1),
    _s("TaiwanStockDividend", "2005-05-01", "day", 1),
    _s("TaiwanStockDividendResult", "2003-05-01", "day", 1),
    _s("TaiwanStockMonthRevenue", "2002-02-01", "day", 1),
    _s("TaiwanStockCapitalReductionReferencePrice", "2011-01-01", "year"),
    _s("TaiwanFuturesDaily", "1998-07-01", "day", 1),
    _s("TaiwanOptionDaily", "2001-12-01", "day", 1),
    _s("TaiwanFuturesInstitutionalInvestors", "2018-06-05", "day", 1),
    _s("TaiwanOptionInstitutionalInvestors", "2018-06-05", "day", 1),
    _s("TaiwanFuturesDealerTradingVolumeDaily", "2021-04-01", "day"),
    _s("TaiwanOptionDealerTradingVolumeDaily", "2021-04-01", "day"),
    _s("TaiwanStock10Year", "2011-01-24", "day"),
    _s("TaiwanStockInfoWithWarrantSummary", "2011-01-03", "month"),
    _s("TaiwanStockWeekPrice", "2000-01-01", "day"),
    _s("TaiwanStockMonthPrice", "2000-01-01", "day"),
    _s("TaiwanStockEvery5SecondsIndex", "2005-01-03", "day", 4),
    _s("TaiwanStockSuspended", "2011-10-06", "year"),
    _s("TaiwanStockDayTradingSuspension", "2014-06-01", "year"),
    _s("TaiwanStockHoldingSharesPer", "2010-01-29", "day"),
    _s("TaiwanStockGovernmentBankBuySell", "2021-06-30", "day", 2, 23),
    _s("TaiwanTotalExchangeMarginMaintenance", "2001-01-05", "year", 1, 21),
    _s("TaiwanStockBlockTradingDailyReport", "2026-04-28", "day", 2, 21),
    _s("TaiwanStockBlockTrade", "2005-04-04", "day"),
    _s("TaiwanStockLoanCollateralBalance", "2006-10-02", "day"),
    _s("TaiwanStockActiveETFHolding", "2025-05-05", "day"),
    _s("TaiwanStockActiveETFHoldingChange", "2025-05-05", "day"),
    _s("TaiwanStockIndustryChainMoneyFlow", "1992-01-04", "day"),
    _s("TaiwanStockMarginMaintenance", "2001-01-05", "day", 2, 23),
    _s("TaiwanStockDispositionSecuritiesPeriod", "2001-01-01", "year"),
    _s("TaiwanStockMarketValue", "2004-01-01", "day"),
    _s("TaiwanStockMarketValueWeight", "2024-10-30", "day"),
    _s("TaiwanFuturesInstitutionalInvestorsAfterHours", "2021-10-12", "day"),
    _s("TaiwanOptionInstitutionalInvestorsAfterHours", "2021-10-12", "day"),
    _s("TaiwanFuturesOpenInterestLargeTraders", "1998-07-01", "day"),
    _s("TaiwanOptionOpenInterestLargeTraders", "1998-07-01", "day"),
    _s("TaiwanFuturesFinalSettlementPrice", "1998-01-01", "year"),
    _s("TaiwanOptionFinalSettlementPrice", "2001-01-01", "year"),
    _s("TaiwanOptionVix", "2026-03-01", "month", 2, 18),
    _s("TaiwanStockConvertibleBondInfo", None, "snapshot"),
    _s("TaiwanStockConvertibleBondDaily", "2011-01-01", "day"),
    _s("TaiwanStockConvertibleBondInstitutionalInvestors", "2011-01-01", "day"),
    _s("TaiwanStockConvertibleBondDailyOverview", "2011-01-01", "day"),
    _s("TaiwanStockConvertibleBondPutProvision", "2011-06-22", "year"),
    _s("TaiwanBusinessIndicator", "1982-01-01", "year"),
    _s("TaiwanStockIndustryChain", None, "snapshot"),
    _s("CnnFearGreedIndex", "2011-01-03", "year"),
)
assert len({source.dataset for source in SOURCES}) == len(SOURCES)
SPECS = {source.dataset: source for source in SOURCES}
# The official TWSE/TPEx daily OHLCV collectors own the modern unadjusted
# price history. FinMind's older price observations can extend that history;
# the overlapping range is kept as a gap/independent-validation source, after
# the other Sponsor datasets. This changes queue order, not receipt validity.
SECONDARY_VALIDATION_PRIORITY = 8
# For the three probed endpoints, the old extra end_date caused HTTP 400.
REPAIRED_400_DATASETS = frozenset({
    "TaiwanStockEvery5SecondsIndex",
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockBlockTradingDailyReport",
})
# All ``day`` specs use the provider's whole-market, one-date query shape.
START_DATE_ONLY_DATASETS = frozenset(spec.dataset for spec in SOURCES if spec.grain == "day")
QUERY_SHAPE_VERSION = 4


def _official_price_coverage(catalog_path: Path | None = None) -> tuple[date, date] | None:
    """Use accepted official date receipts to bound a secondary price sweep.

    A date-level receipt does not prove per-symbol completeness; FinMind stays
    queued for later gap audits and validation rather than being discarded.
    """
    catalog_path = catalog_path or Path(__file__).resolve().parents[1] / "configs/data_sync/packed_datasets.json"
    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        source = next(item["source"] for item in catalog["datasets"]
                      if item["dataset"] == "tw-public")
        root = Path(source)
        if not root.is_absolute():
            root = catalog_path.resolve().parents[2] / root
        coverage = []
        for dataset in ("twse_daily_ohlcv", "tpex_daily_ohlcv"):
            state = json.loads((root / "state" / f"{dataset}.json").read_text(encoding="utf-8"))
            if (state.get("dataset") != dataset or not state.get("baseline_established") or
                    state.get("coverage_complete") is not True or
                    state.get("missing_dates_after") != 0 or state.get("failed_dates") or
                    state.get("coverage_calendar_kind") != "receipt_verified_official_open_sessions"):
                return None
            coverage.append((date.fromisoformat(state["coverage_start"]),
                             date.fromisoformat(state["coverage_end"])))
        start = max(item[0] for item in coverage)
        end = min(item[1] for item in coverage)
        return (start, end) if start <= end else None
    except (OSError, ValueError, KeyError, TypeError, StopIteration):
        return None

# Provider-documented paid datasets that lack an efficient full-history Sponsor
# query are explicit, not silently absent. SponsorPro objects are a separate tier.
UNSCHEDULED = {
    "TaiwanStockPriceTick": "per_symbol_per_day_only_sponsorpro_bulk",
    "TaiwanStockKBar": "per_symbol_per_day_only_sponsorpro_bulk",
    "TaiwanStockTradingDailyReport": "per_stock_or_broker_per_day_sponsorpro_bulk",
    "TaiwanStockWarrantTradingDailyReport": "per_stock_or_broker_per_day_sponsorpro_bulk",
    "TaiwanStockTradingDailyReportSecIdAgg": "per_symbol_history_special_endpoint",
    "TaiwanFuturesKBar": "per_product_per_day_sponsorpro_bulk",
    "TaiwanFuturesTick": "per_product_per_day_sponsorpro_bulk",
    "TaiwanFuturesSpreadTick": "per_product_per_day_only",
    "TaiwanOptionTick": "per_product_per_day_sponsorpro_bulk",
    "TaiwanFuturesSpreadTrading": "per_product_only",
    "TaiwanStockConvertibleBondMonthlyAnalysis": "per_bond_only",
    "TaiwanAssetSwapFixedIncomeDaily": "per_bond_only",
    "TaiwanAssetSwapOptionDaily": "per_bond_only",
    "USStockPriceMinute": "per_symbol_only_outside_taiwan_scope",
    "taiwan_stock_tick_snapshot": "live_snapshot_not_history",
    "taiwan_futures_snapshot": "live_snapshot_not_history",
    "taiwan_options_snapshot": "live_snapshot_not_history",
}


def _end(start: date, grain: str) -> date:
    if grain == "day":
        return start + timedelta(days=1)
    if grain == "two_day":
        return start + timedelta(days=2)
    if grain == "month":
        return date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    if grain == "year":
        return date(start.year + 1, 1, 1)
    raise ValueError(grain)


def _migrate_query_shape(conn: sqlite3.Connection, root: Path) -> None:
    """Preserve old receipts while correcting the one-day coverage denominator."""
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version >= QUERY_SHAPE_VERSION:
        return
    if version < 2:
        # Old HTTP 400s came from sending end_date to documented start-only
        # endpoints. Never reopen subsequent independent 400s on each restart.
        conn.executemany(
            "UPDATE tasks SET state='pending',error_code=NULL,next_attempt_at_utc=NULL "
            "WHERE dataset=? AND state='blocked' AND error_code='provider_bad_request'",
            ((dataset,) for dataset in REPAIRED_400_DATASETS),
        )
    for spec in SOURCES:
        if spec.grain != "day":
            continue
        new_kind = "derived" if spec.dataset == WIDE_INSTITUTIONAL else "day"
        old_predicate = "dataset=?" if spec.dataset == WIDE_INSTITUTIONAL else "dataset=? AND kind!=?"
        predicate_args = (spec.dataset,) if spec.dataset == WIDE_INSTITUTIONAL else (spec.dataset, new_kind)
        old = conn.execute(
            f"SELECT receipt_path FROM tasks WHERE {old_predicate} AND receipt_path IS NOT NULL",
            predicate_args,
        ).fetchall()
        for (receipt_name,) in old:
            relative = Path(receipt_name)
            if relative.is_absolute() or ".." in relative.parts or relative.parts[:1] != ("receipts",):
                raise ValueError("unsafe legacy FinMind receipt path")
            source = root / relative
            if not source.is_file() or source.is_symlink():
                raise ValueError("missing legacy FinMind receipt")
            target = root / "legacy_query_shape_receipts" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                shutil.copy2(source, target)
        conn.execute(
            "UPDATE tasks SET kind=?,state=CASE "
            "WHEN state='complete' AND first_data_date=partition AND last_data_date=partition "
            "THEN 'complete' WHEN state='blocked' AND error_code!='provider_bad_request' "
            "THEN 'blocked' ELSE 'pending' END, "
            "next_attempt_at_utc=NULL,error_code=CASE "
            "WHEN state='blocked' AND error_code!='provider_bad_request' THEN error_code ELSE NULL END "
            f"WHERE {old_predicate}",
            (new_kind, *predicate_args),
        )
    conn.execute(f"PRAGMA user_version={QUERY_SHAPE_VERSION}")
    conn.commit()


def _seed(conn: sqlite3.Connection, now: datetime,
          *, official_price_coverage: tuple[date, date] | None = None,
          root: Path | None = None) -> None:
    if root is None:
        root = Path(conn.execute("PRAGMA database_list").fetchone()[2]).parent
    _migrate_query_shape(conn, root)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS seed_state (dataset TEXT PRIMARY KEY, "
        "last_partition TEXT NOT NULL)"
    )
    local = now.astimezone(TAIPEI)
    rows: list[tuple[str, str, str, str, int]] = []
    for spec in SOURCES:
        if spec.grain == "snapshot":
            rows.append((spec.dataset, "", "latest", "snapshot", spec.priority))
            continue
        assert spec.first_date is not None
        max_day = local.date() if local.hour >= spec.release_hour else local.date() - timedelta(days=1)
        previous = conn.execute(
            "SELECT last_partition FROM seed_state WHERE dataset=?", (spec.dataset,)
        ).fetchone()
        cursor = _end(date.fromisoformat(previous[0]), spec.grain) if previous else spec.first_date
        last_seeded = None
        while cursor <= max_day:
            end = _end(cursor, spec.grain)
            priority = 0 if end > max_day else spec.priority if cursor.year >= 2014 else spec.priority + 2
            if (spec.dataset == "TaiwanStockPrice" and official_price_coverage and
                    cursor >= official_price_coverage[0] and
                    end <= official_price_coverage[1] + timedelta(days=1) and
                    end <= max_day):
                priority = SECONDARY_VALIDATION_PRIORITY
            kind = "derived" if spec.dataset == WIDE_INSTITUTIONAL else spec.grain
            rows.append((spec.dataset, "", cursor.isoformat(), kind, priority))
            last_seeded = cursor
            cursor = end
        if last_seeded is not None:
            conn.execute(
                "INSERT INTO seed_state(dataset,last_partition) VALUES (?,?) ON CONFLICT(dataset) "
                "DO UPDATE SET last_partition=excluded.last_partition",
                (spec.dataset, last_seeded.isoformat()),
            )
        # Only the once-current partition needs re-ranking on a later day.
        current_start = (max_day if spec.grain in {"day", "two_day"} else
                         date(max_day.year, max_day.month, 1) if spec.grain == "month" else
                         date(max_day.year, 1, 1))
        historical_priority = spec.priority if current_start.year >= 2014 else spec.priority + 2
        conn.execute(
            "UPDATE tasks SET priority=? WHERE dataset=? AND priority=0 AND partition<?",
            (historical_priority, spec.dataset, current_start.isoformat()),
        )
    conn.executemany(
        "INSERT OR IGNORE INTO tasks(dataset,data_id,partition,kind,priority,state) "
        "VALUES (?,?,?,?,?,'pending')", rows,
    )
    if official_price_coverage:
        start, end = official_price_coverage
        conn.execute(
            "UPDATE tasks SET priority=? WHERE dataset='TaiwanStockPrice' AND "
            "partition>=? AND partition<=? AND priority!=? AND priority!=0",
            (SECONDARY_VALIDATION_PRIORITY, start.isoformat(), end.isoformat(),
             SECONDARY_VALIDATION_PRIORITY),
        )
        conn.execute(
            "UPDATE tasks SET priority=CASE WHEN partition>='2014-01-01' THEN 1 ELSE 3 END "
            "WHERE dataset='TaiwanStockPrice' AND priority=? AND "
            "(partition<? OR partition>?)",
            (SECONDARY_VALIDATION_PRIORITY, start.isoformat(), end.isoformat()),
        )
    else:
        conn.execute(
            "UPDATE tasks SET priority=CASE WHEN partition>='2014-01-01' THEN 1 ELSE 3 END "
            "WHERE dataset='TaiwanStockPrice' AND priority=?",
            (SECONDARY_VALIDATION_PRIORITY,),
        )
    # A killed process leaves no false completion; retained receipts are not
    # marked complete by this recovery.
    conn.execute("UPDATE tasks SET state='pending' WHERE state='inflight'")
    conn.commit()


def _next(conn: sqlite3.Connection, now: datetime) -> Task | None:
    row = conn.execute(
        "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
        "WHERE (state='pending' OR (state IN ('complete','observed_empty','failed') "
        "AND next_attempt_at_utc<=?)) "
        "AND (dataset!=? OR EXISTS (SELECT 1 FROM tasks parent WHERE "
        "parent.dataset=? AND parent.partition=tasks.partition AND "
        "parent.state IN ('complete','observed_empty'))) "
        "ORDER BY priority,CASE WHEN state='pending' THEN 0 ELSE 1 END, "
        "COALESCE(next_attempt_at_utc,''),dataset,partition DESC LIMIT 1",
        (now.isoformat(), WIDE_INSTITUTIONAL, LONG_INSTITUTIONAL),
    ).fetchone()
    if not row:
        return None
    task = Task(*row)
    conn.execute("UPDATE tasks SET state='inflight' WHERE dataset=? AND data_id='' AND partition=?",
                 (task.dataset, task.partition))
    conn.commit()
    return task


def _derive_wide(root: Path, partition: str) -> list[dict[str, Any]]:
    """Materialize the documented wide representation from verified long rows."""
    receipt_path = root / "receipts" / LONG_INSTITUTIONAL / "all" / f"{partition}.json"
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SourceError("missing_long_source", retry_after=900) from exc
    if receipt.get("dataset") != LONG_INSTITUTIONAL or receipt.get("partition") != partition:
        raise SourceError("invalid_long_receipt", retry_after=0)
    if receipt.get("status") == "observed_empty":
        return []
    relative = receipt.get("parquet_path")
    if receipt.get("status") != "complete" or not isinstance(relative, str):
        raise SourceError("missing_long_parquet", retry_after=900)
    source = (root / relative).resolve()
    if not source.is_relative_to(root.resolve()) or not source.is_file():
        raise SourceError("missing_long_parquet", retry_after=900)
    if _sha256(source) != receipt.get("sha256"):
        raise SourceError("corrupt_long_parquet", retry_after=0)
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    start = date.fromisoformat(partition)
    end = _end(start, "day")
    for row in pq.read_table(source).to_pylist():
        stamp, stock_id, name = row.get("date"), row.get("stock_id"), row.get("name")
        if (not isinstance(stamp, str) or not start.isoformat() <= stamp[:10] < end.isoformat() or
                not isinstance(stock_id, str) or name not in INSTITUTIONAL_NAMES):
            raise SourceError("invalid_long_row", retry_after=0)
        key = (stamp, stock_id)
        wide = grouped.setdefault(key, {
            "date": stamp, "stock_id": stock_id,
            **{f"{category}_{side}": 0 for category in INSTITUTIONAL_NAMES
               for side in ("buy", "sell")},
        })
        for side in ("buy", "sell"):
            value = row.get(side)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise SourceError("invalid_long_row", retry_after=0)
            wide[f"{name}_{side}"] += value
    return [grouped[key] for key in sorted(grouped)]


def _fetch(task: Task, token: str, limiter: Any, root: Path,
           today: date) -> list[dict[str, Any]]:
    if task.kind == "derived":
        return _derive_wide(root, task.partition)
    session = getattr(_THREAD, "session", None)
    if session is None:
        session = requests.Session()
        _THREAD.session = session
    params = {"dataset": task.dataset}
    if task.kind != "snapshot":
        start = date.fromisoformat(task.partition)
        end = min(_end(start, task.kind), today + timedelta(days=1))
        params["start_date"] = start.isoformat()
        if task.dataset not in START_DATE_ONLY_DATASETS:
            params["end_date"] = end.isoformat()
    rows = _fetch_rows(session, limiter, root.parent, task.dataset, token, params)
    if task.kind != "snapshot":
        for row in rows:
            stamp = row.get("date")
            if not isinstance(stamp, str) or not start.isoformat() <= stamp[:10] < end.isoformat():
                raise SourceError("response_outside_partition", retry_after=0)
    return rows


def _finish(conn: sqlite3.Connection, root: Path, task: Task,
            rows: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
    receipt = _store(root, task, rows, now)
    current = task.kind == "snapshot" or (task.kind != "snapshot" and
              _end(date.fromisoformat(task.partition), SPECS[task.dataset].grain) >=
              now.astimezone(TAIPEI).date())
    next_at = (now + (timedelta(hours=4) if rows else timedelta(days=1))) if current else None
    conn.execute(
        "UPDATE tasks SET state=?,next_attempt_at_utc=?,last_attempt_at_utc=?,"
        "rows=?,bytes=?,first_data_date=?,last_data_date=?,receipt_path=?,error_code=NULL "
        "WHERE dataset=? AND data_id='' AND partition=?",
        (receipt["status"], next_at.isoformat() if next_at else None, now.isoformat(), receipt["rows"],
         receipt.get("parquet_size_bytes", 0), receipt.get("source_first_date"),
         receipt.get("source_last_date"), receipt["receipt_path"], task.dataset, task.partition),
    )
    conn.commit()
    return receipt


def _fail(conn: sqlite3.Connection, task: Task, error: SourceError, now: datetime) -> None:
    terminal = error.code in {"not_entitled", "invalid_token", "provider_bad_request",
                              "response_outside_partition"}
    if terminal:
        conn.execute(
            "UPDATE tasks SET state='blocked',error_code=?,next_attempt_at_utc=NULL "
            "WHERE dataset=? AND state='pending'",
            (error.code, task.dataset),
        )
    conn.execute(
        "UPDATE tasks SET state=?,error_code=?,last_attempt_at_utc=?,next_attempt_at_utc=? "
        "WHERE dataset=? AND data_id='' AND partition=?",
        ("blocked" if terminal else "failed", error.code, now.isoformat(),
         None if terminal else (now + timedelta(seconds=max(60, error.retry_after))).isoformat(),
         task.dataset, task.partition),
    )
    conn.commit()


def _status(conn: sqlite3.Connection, root: Path, state: str,
            *, account: dict[str, object] | None = None,
            active: list[Task] | None = None, last: dict[str, Any] | None = None) -> dict[str, Any]:
    series = {spec.dataset: {"target": 0, "complete": 0, "observed_empty": 0,
                             "failed": 0, "blocked": 0, "rows": 0, "bytes": 0,
                             "first_data_date": None, "last_data_date": None,
                             "last_attempt_at_utc": None}
              for spec in SOURCES}
    for dataset, task_state, count, row_count, size, first, last_date, attempted in conn.execute(
        "SELECT dataset,state,COUNT(*),SUM(rows),SUM(bytes),MIN(first_data_date),"
        "MAX(last_data_date),MAX(last_attempt_at_utc) FROM tasks GROUP BY dataset,state"
    ):
        if dataset not in series:
            continue
        item = series[dataset]
        item["target"] += count
        if task_state in item:
            item[task_state] += count
        if attempted and (item["last_attempt_at_utc"] is None or attempted > item["last_attempt_at_utc"]):
            item["last_attempt_at_utc"] = attempted
        if task_state == "complete":
            item["rows"] += row_count or 0
            item["bytes"] += size or 0
            if first and (item["first_data_date"] is None or first < item["first_data_date"]):
                item["first_data_date"] = first
            if last_date and (item["last_data_date"] is None or last_date > item["last_data_date"]):
                item["last_data_date"] = last_date
    result = {
        "schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
        "state": state, "tier": account.get("tier") if account else None,
        "official_requests_per_hour": account.get("official_requests_per_hour") if account else None,
        "series": series, "unscheduled": UNSCHEDULED,
        "active_tasks": [{"dataset": task.dataset, "partition": task.partition} for task in active or []],
        "last_task": last, "catalog_source": CATALOG_URL,
        "training": "raw_not_point_in_time_validated", "news": "disabled_by_user",
    }
    atomic_write_json(root / "status.json", result)
    return result


def run_once(root: Path, *, max_requests: int = 100, workers: int = 4) -> dict[str, Any]:
    if workers < 1 or workers > 8 or max_requests < 0:
        raise ValueError("workers must be 1..8 and max_requests nonnegative")
    root.mkdir(parents=True, exist_ok=True)
    load_env_file(Path(__file__).resolve().parents[1] / ".env", allowed_names=("FINMIND_TOKEN",))
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    with requests.Session() as session:
        try:
            account = verified_account(session, token, root.parent)
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            with _db(root / "queue.sqlite3") as conn:
                return _status(conn, root, "account_unverified", last={"error_type": type(exc).__name__})
    if account["tier"] not in {"Sponsor", "SponsorPro"}:
        with _db(root / "queue.sqlite3") as conn:
            return _status(conn, root, "not_entitled", account=account)
    limiter = rate_limiter(account)
    with _db(root / "queue.sqlite3") as conn:
        _seed(conn, datetime.now(UTC), official_price_coverage=_official_price_coverage(), root=root)
        _status(conn, root, "running", account=account)
        last_status_at = time.monotonic()
        last: dict[str, Any] | None = None
        in_flight: dict[Future[list[dict[str, Any]]], Task] = {}
        sent = 0
        halt = False
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="finmind-sponsor") as pool:
            while in_flight or (not halt and (not max_requests or sent < max_requests)):
                local = datetime.now(TAIPEI)
                if shutil.disk_usage(root).free < MIN_FREE_BYTES:
                    halt = True
                    reason = "disk_guard"
                elif local.weekday() < 5 and (local.hour == 8 and local.minute >= 20 or
                                                local.hour == 9 and local.minute < 10):
                    halt = True
                    reason = "protected_opening"
                while not halt and len(in_flight) < workers and (not max_requests or sent < max_requests):
                    task = _next(conn, datetime.now(UTC))
                    if task is None:
                        halt = True
                        reason = "current_queue"
                        break
                    future = pool.submit(_fetch, task, token, limiter, root, local.date())
                    in_flight[future] = task
                    sent += task.kind != "derived"
                if not in_flight:
                    break
                done, _ = wait(in_flight, timeout=55, return_when=FIRST_COMPLETED)
                if not done:
                    _status(conn, root, "running", account=account, active=list(in_flight.values()), last=last)
                    last_status_at = time.monotonic()
                for future in done:
                    task = in_flight.pop(future)
                    now = datetime.now(UTC)
                    try:
                        rows = future.result()
                        receipt = _finish(conn, root, task, rows, now)
                        last = {"dataset": task.dataset, "partition": task.partition,
                                "status": receipt["status"], "rows": len(rows)}
                    except SourceError as error:
                        _fail(conn, task, error, now)
                        last = {"dataset": task.dataset, "partition": task.partition,
                                "status": "failed", "error_code": error.code}
                        if error.code in {"rate_limited", "invalid_token", "ip_banned", "not_entitled",
                                          "provider_bad_request", "response_outside_partition"}:
                            halt = True
                            reason = error.code
                    except (OSError, ValueError, RuntimeError) as exc:
                        _fail(conn, task, SourceError("storage_or_schema_error", retry_after=900), now)
                        last = {"dataset": task.dataset, "partition": task.partition,
                                "status": "failed", "error_code": "storage_or_schema_error",
                                "exception_type": type(exc).__name__}
                    if time.monotonic() - last_status_at >= 20:
                        _status(conn, root, "running", account=account,
                                active=list(in_flight.values()), last=last)
                        last_status_at = time.monotonic()
        return _status(conn, root, reason if halt else "batch_complete", account=account, last=last)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_finmind/sponsor"))
    parser.add_argument("--max-requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    with (root / "worker.lock").open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("FinMind Sponsor worker already running", file=sys.stderr)
            return 2
        try:
            while True:
                result = run_once(root, max_requests=args.max_requests, workers=args.workers)
                print(json.dumps({"state": result["state"], "last_task": result.get("last_task")},
                                 ensure_ascii=False), flush=True)
                if not args.loop or result["state"] in {"account_unverified", "not_entitled", "invalid_token"}:
                    return 0 if result["state"] not in {"account_unverified", "not_entitled"} else 2
                time.sleep(1800 if result["state"] in {"rate_limited", "ip_banned"} else
                           600 if result["state"] in {"current_queue", "protected_opening", "disk_guard"} else 2)
        except KeyboardInterrupt:
            return 130


if __name__ == "__main__":
    raise SystemExit(main())
