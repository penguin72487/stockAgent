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

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import (
    SourceError, Task, _db, _fetch_rows, _store,
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
    _s("TaiwanStockPrice", "1994-10-01", "two_day", 1, 18),
    _s("TaiwanStockPriceAdj", "1994-10-01", "two_day", 2, 20),
    _s("TaiwanStockDayTrading", "2014-01-01", "month", 1, 18),
    _s("TaiwanStockPriceLimit", "2000-01-01", "month", 1, 18),
    _s("TaiwanStockMarginPurchaseShortSale", "2001-01-01", "month", 1, 21),
    _s("TaiwanStockInstitutionalInvestorsBuySell", "2005-01-01", "month", 1, 18),
    _s("TaiwanStockInstitutionalInvestorsBuySellWide", "2005-01-01", "month", 2, 18),
    _s("TaiwanStockShareholding", "2004-02-01", "month", 2, 21),
    _s("TaiwanStockSecuritiesLending", "2001-05-01", "month", 2, 21),
    _s("TaiwanStockMarginShortSaleSuspension", "2015-01-01", "year"),
    _s("TaiwanDailyShortSaleBalances", "2005-07-01", "month"),
    _s("TaiwanStockFinancialStatements", "1990-03-01", "month", 1),
    _s("TaiwanStockBalanceSheet", "2011-12-01", "month", 1),
    _s("TaiwanStockCashFlowsStatement", "2008-06-01", "month", 1),
    _s("TaiwanStockDividend", "2005-05-01", "month", 1),
    _s("TaiwanStockDividendResult", "2003-05-01", "month", 1),
    _s("TaiwanStockMonthRevenue", "2002-02-01", "month", 1),
    _s("TaiwanStockCapitalReductionReferencePrice", "2011-01-01", "year"),
    _s("TaiwanFuturesDaily", "1998-07-01", "month", 1),
    _s("TaiwanOptionDaily", "2001-12-01", "month", 1),
    _s("TaiwanFuturesInstitutionalInvestors", "2018-06-05", "month", 1),
    _s("TaiwanOptionInstitutionalInvestors", "2018-06-05", "month", 1),
    _s("TaiwanFuturesDealerTradingVolumeDaily", "2021-04-01", "month"),
    _s("TaiwanOptionDealerTradingVolumeDaily", "2021-04-01", "month"),
    _s("TaiwanStock10Year", "2011-01-24", "month"),
    _s("TaiwanStockInfoWithWarrantSummary", "2011-01-03", "month"),
    _s("TaiwanStockWeekPrice", "2000-01-01", "year"),
    _s("TaiwanStockMonthPrice", "2000-01-01", "year"),
    _s("TaiwanStockEvery5SecondsIndex", "2005-01-03", "day", 4),
    _s("TaiwanStockSuspended", "2011-10-06", "year"),
    _s("TaiwanStockDayTradingSuspension", "2014-06-01", "year"),
    _s("TaiwanStockHoldingSharesPer", "2010-01-29", "month"),
    _s("TaiwanStockGovernmentBankBuySell", "2021-06-30", "day", 2, 23),
    _s("TaiwanTotalExchangeMarginMaintenance", "2001-01-05", "year", 1, 21),
    _s("TaiwanStockBlockTradingDailyReport", "2026-04-28", "day", 2, 21),
    _s("TaiwanStockBlockTrade", "2005-04-04", "month"),
    _s("TaiwanStockLoanCollateralBalance", "2006-10-02", "day"),
    _s("TaiwanStockActiveETFHolding", "2025-05-05", "day"),
    _s("TaiwanStockActiveETFHoldingChange", "2025-05-05", "day"),
    _s("TaiwanStockIndustryChainMoneyFlow", "1992-01-04", "day"),
    _s("TaiwanStockMarginMaintenance", "2001-01-05", "day", 2, 23),
    _s("TaiwanStockDispositionSecuritiesPeriod", "2001-01-01", "year"),
    _s("TaiwanStockMarketValue", "2004-01-01", "two_day"),
    _s("TaiwanStockMarketValueWeight", "2024-10-30", "day"),
    _s("TaiwanFuturesInstitutionalInvestorsAfterHours", "2021-10-12", "month"),
    _s("TaiwanOptionInstitutionalInvestorsAfterHours", "2021-10-12", "month"),
    _s("TaiwanFuturesOpenInterestLargeTraders", "1998-07-01", "month"),
    _s("TaiwanOptionOpenInterestLargeTraders", "1998-07-01", "month"),
    _s("TaiwanFuturesFinalSettlementPrice", "1998-01-01", "year"),
    _s("TaiwanOptionFinalSettlementPrice", "2001-01-01", "year"),
    _s("TaiwanOptionVix", "2026-03-01", "month", 2, 18),
    _s("TaiwanStockConvertibleBondInfo", None, "snapshot"),
    _s("TaiwanStockConvertibleBondDaily", "2011-01-01", "month"),
    _s("TaiwanStockConvertibleBondInstitutionalInvestors", "2011-01-01", "month"),
    _s("TaiwanStockConvertibleBondDailyOverview", "2011-01-01", "month"),
    _s("TaiwanStockConvertibleBondPutProvision", "2011-06-22", "year"),
    _s("TaiwanBusinessIndicator", "1982-01-01", "year"),
    _s("TaiwanStockIndustryChain", None, "snapshot"),
    _s("CnnFearGreedIndex", "2011-01-03", "year"),
)
assert len({source.dataset for source in SOURCES}) == len(SOURCES)
SPECS = {source.dataset: source for source in SOURCES}

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


def _seed(conn: sqlite3.Connection, now: datetime) -> None:
    local = now.astimezone(TAIPEI)
    rows: list[tuple[str, str, str, str, int]] = []
    for spec in SOURCES:
        if spec.grain == "snapshot":
            rows.append((spec.dataset, "", "latest", "snapshot", spec.priority))
            continue
        assert spec.first_date is not None
        max_day = local.date() if local.hour >= spec.release_hour else local.date() - timedelta(days=1)
        cursor = spec.first_date
        while cursor <= max_day:
            end = _end(cursor, spec.grain)
            priority = 0 if end > max_day else spec.priority if cursor.year >= 2014 else spec.priority + 2
            rows.append((spec.dataset, "", cursor.isoformat(), spec.grain, priority))
            cursor = end
    conn.executemany(
        "INSERT OR IGNORE INTO tasks(dataset,data_id,partition,kind,priority,state) "
        "VALUES (?,?,?,?,?,'pending')", rows,
    )
    # A killed process leaves no false completion, and refresh priorities follow
    # the latest current/historical partition rather than the first seeding day.
    conn.execute("UPDATE tasks SET state='pending' WHERE state='inflight'")
    conn.commit()


def _next(conn: sqlite3.Connection, now: datetime) -> Task | None:
    row = conn.execute(
        "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
        "WHERE state='pending' OR (state IN ('complete','observed_empty','failed') "
        "AND next_attempt_at_utc<=?) "
        "ORDER BY priority,CASE WHEN state='pending' THEN 0 ELSE 1 END, "
        "COALESCE(next_attempt_at_utc,''),dataset,partition DESC LIMIT 1",
        (now.isoformat(),),
    ).fetchone()
    if not row:
        return None
    task = Task(*row)
    conn.execute("UPDATE tasks SET state='inflight' WHERE dataset=? AND data_id='' AND partition=?",
                 (task.dataset, task.partition))
    conn.commit()
    return task


def _fetch(task: Task, token: str, limiter: Any, traffic_root: Path,
           today: date) -> list[dict[str, Any]]:
    session = getattr(_THREAD, "session", None)
    if session is None:
        session = requests.Session()
        _THREAD.session = session
    params = {"dataset": task.dataset}
    if task.kind != "snapshot":
        start = date.fromisoformat(task.partition)
        end = min(_end(start, task.kind), today + timedelta(days=1))
        params["start_date"] = start.isoformat()
        if task.dataset != "TaiwanStockIndustryChainMoneyFlow":
            params["end_date"] = end.isoformat()
    rows = _fetch_rows(session, limiter, traffic_root, task.dataset, token, params)
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
              _end(date.fromisoformat(task.partition), task.kind) >= now.astimezone(TAIPEI).date())
    next_at = now + (timedelta(hours=4) if current and not rows else
                     timedelta(days=1) if current else timedelta(days=30))
    conn.execute(
        "UPDATE tasks SET state=?,next_attempt_at_utc=?,last_attempt_at_utc=?,"
        "rows=?,bytes=?,first_data_date=?,last_data_date=?,receipt_path=?,error_code=NULL "
        "WHERE dataset=? AND data_id='' AND partition=?",
        (receipt["status"], next_at.isoformat(), now.isoformat(), receipt["rows"],
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
        _seed(conn, datetime.now(UTC))
        _status(conn, root, "running", account=account)
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
                    future = pool.submit(_fetch, task, token, limiter, root.parent, local.date())
                    in_flight[future] = task
                    sent += 1
                if not in_flight:
                    break
                done, _ = wait(in_flight, timeout=55, return_when=FIRST_COMPLETED)
                if not done:
                    _status(conn, root, "running", account=account, active=list(in_flight.values()), last=last)
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
                    _status(conn, root, "running", account=account,
                            active=list(in_flight.values()), last=last)
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
