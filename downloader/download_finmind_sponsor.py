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
from typing import Any, Callable

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import (
    INSTITUTIONAL_NAMES, LONG_INSTITUTIONAL, WIDE_INSTITUTIONAL,
    SourceError, Task, _db, _fetch_rows, _sha256, _store, _preflight_bulk_year_schemas,
)
from downloader.download_finmind_free import TAIPEI
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account, refresh_dispatch_account
from downloader.finmind_batching import (
    BATCH_CONTRACT_VERSION, RANGE_CONTRACTS, BatchContractError, RangeBatch,
    coalesce_pending_tasks, split_batch_rows,
)
from downloader.finmind_parent_recovery import recover_failed_long_parent
from downloader.finmind_runtime import wait_for_next_cycle
from downloader.finmind_scheduling import (
    SOURCES, SPECS, SESSION_DAY_DATASETS, PRODUCT_HISTORY_STARTS, Source, _s,
    fixed_incremental_demand, protected_stock_opening,
)
from downloader.finmind_observation_dates import (
    EXCLUDED_STATE, PERIOD_DATASETS, next_period_refresh, reconcile_observation_dates,
)
from stockagent.live.market_status import TwStockDayDecision, tw_stock_day_decision


CATALOG_URL = "https://finmind.github.io/llms-full.txt"
MIN_FREE_BYTES = 25 * 1024**3
_THREAD = threading.local()


# The official TWSE/TPEx daily OHLCV collectors own the modern unadjusted
# price history. FinMind's older price observations can extend that history;
# the overlapping range is kept as a gap/independent-validation source, after
# the other Sponsor datasets. This changes queue order, not receipt validity.
SECONDARY_VALIDATION_PRIORITY = 8


@dataclass(frozen=True)
class OfficialSessions:
    first: date
    last: date
    days: frozenset[date]
    receipt_sha256: str


def _official_session_calendar(catalog_path: Path | None = None) -> OfficialSessions | None:
    """Trust only the canonical TWSE archive and its exact-byte receipt."""
    catalog_path = catalog_path or Path(__file__).resolve().parents[1] / "configs/data_sync/packed_datasets.json"
    try:
        from scripts.build_tw_official_symbol_parquets import _load_verified_taiex_session_calendar

        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        source = next(item["source"] for item in catalog["datasets"]
                      if item["dataset"] == "tw-public")
        public_root = Path(source)
        if not public_root.is_absolute():
            public_root = catalog_path.resolve().parents[2] / public_root
        frame, receipt, _ = _load_verified_taiex_session_calendar(public_root)
        days = frozenset(date.fromisoformat(str(value)[:10]) for value in frame["date"].to_list())
        if not days:
            return None
        return OfficialSessions(min(days), max(days), days, str(receipt["sha256"]))
    except (OSError, RuntimeError, ValueError, KeyError, TypeError, StopIteration):
        return None


# For the three probed endpoints, the old extra end_date caused HTTP 400.
REPAIRED_400_DATASETS = frozenset({
    "TaiwanStockEvery5SecondsIndex",
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockBlockTradingDailyReport",
})
# All ``day`` specs use the provider's whole-market, one-date query shape.
START_DATE_ONLY_DATASETS = frozenset(spec.dataset for spec in SOURCES if spec.grain == "day")
# Range endpoints are individually probed for rows exactly on end_date before
# admission, including sparse event sources. See the partition_semantics and
# max_ranges boundary receipts in artifacts/data_quality.
INCLUSIVE_END_DATE_DATASETS = frozenset({
    "TaiwanStockInfoWithWarrantSummary", "TaiwanBusinessIndicator",
    "CnnFearGreedIndex", "TaiwanOptionVix",
}) | frozenset(RANGE_CONTRACTS)
QUERY_SHAPE_VERSION = 6


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
UNSCHEDULED: dict[str, str] = {}  # Per-ID histories and daily captured snapshots belong to Complement.


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
    for spec in SOURCES if version < 4 else ():
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
    if version < 5:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS query_shape_repair_audit ("
            "repair_version INTEGER,dataset TEXT,data_id TEXT,partition TEXT,"
            "old_state TEXT,old_error_code TEXT,old_next_attempt_at_utc TEXT,repaired_at_utc TEXT,"
            "PRIMARY KEY(repair_version,dataset,data_id,partition))"
        )
        for dataset in INCLUSIVE_END_DATE_DATASETS:
            conn.execute(
                "INSERT OR IGNORE INTO query_shape_repair_audit SELECT 5,dataset,data_id,partition,"
                "state,error_code,next_attempt_at_utc,? FROM tasks WHERE dataset=? "
                "AND state='blocked' AND error_code='response_outside_partition'",
                (datetime.now(UTC).isoformat(), dataset),
            )
            conn.execute(
                "UPDATE tasks SET state='pending',error_code=NULL,next_attempt_at_utc=NULL "
                "WHERE dataset=? AND state='blocked' AND error_code='response_outside_partition'",
                (dataset,),
            )
    if version < 6:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS product_query_migration_audit ("
            "dataset TEXT,partition TEXT,old_row_json TEXT,changed_at_utc TEXT,"
            "PRIMARY KEY(dataset,partition))"
        )
        for dataset in PRODUCT_HISTORY_STARTS:
            cursor = conn.execute("SELECT * FROM tasks WHERE dataset=? AND data_id=''", (dataset,))
            columns = [column[0] for column in cursor.description]
            for values in cursor.fetchall():
                old = dict(zip(columns, values))
                conn.execute(
                    "INSERT OR IGNORE INTO product_query_migration_audit VALUES (?,?,?,?)",
                    (dataset, old['partition'], json.dumps(old, sort_keys=True), datetime.now(UTC).isoformat()),
                )
            # Preserve raw receipts and all observed values; retire only the
            # unsupported no-ID query shape, not the endpoint's product work.
            conn.execute(
                "UPDATE tasks SET state='deprecated_query_shape',next_attempt_at_utc=NULL,"
                "error_code='requires_product_id' WHERE dataset=? AND data_id=''",
                (dataset,),
            )
    conn.execute(f"PRAGMA user_version={QUERY_SHAPE_VERSION}")
    conn.commit()


def _seed(conn: sqlite3.Connection, now: datetime,
          *, official_price_coverage: tuple[date, date] | None = None,
          official_sessions: OfficialSessions | None = None,
          root: Path | None = None) -> dict[str, Any]:
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
        if spec.dataset in PRODUCT_HISTORY_STARTS:
            continue
        if spec.grain == "snapshot":
            rows.append((spec.dataset, "", "latest", "snapshot", spec.priority))
            continue
        assert spec.first_date is not None
        released = (local.hour, local.minute) >= (spec.release_hour, spec.release_minute)
        max_day = local.date() if released else local.date() - timedelta(days=1)
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
    # A killed process leaves no false completion; retained receipts are not
    # marked complete by this recovery. Do it before session classification so
    # an interrupted non-session request cannot immediately be dispatched.
    conn.execute("UPDATE tasks SET state='pending' WHERE state='inflight'")
    session_policy: dict[str, Any] = {
        "state": "calendar_unverified", "excluded_non_session": 0,
        "newly_excluded": 0, "conflicting_datasets": [],
    }
    datasets = tuple(sorted(SESSION_DAY_DATASETS))
    placeholders = ",".join("?" for _ in datasets)
    if official_sessions is None:
        # An old exclusion may not remain authoritative after its calendar
        # proof disappears. Reopen it rather than silently losing coverage.
        conn.execute(
            f"UPDATE tasks SET state='pending' WHERE dataset IN ({placeholders}) "
            "AND state='non_session'", datasets,
        )
    else:
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS finmind_verified_sessions "
                     "(day TEXT PRIMARY KEY)")
        conn.execute("DELETE FROM finmind_verified_sessions")
        conn.executemany(
            "INSERT INTO finmind_verified_sessions(day) VALUES (?)",
            ((day.isoformat(),) for day in sorted(official_sessions.days)),
        )
        bounds = (official_sessions.first.isoformat(), official_sessions.last.isoformat())
        conflicts = {row[0] for row in conn.execute(
            f"SELECT DISTINCT dataset FROM tasks WHERE dataset IN ({placeholders}) "
            "AND partition BETWEEN ? AND ? AND state='complete' AND rows>0 "
            "AND NOT EXISTS (SELECT 1 FROM finmind_verified_sessions "
            "WHERE day=tasks.partition)", (*datasets, *bounds),
        )}
        eligible = tuple(dataset for dataset in datasets if dataset not in conflicts)
        # A calendar revision can add a previously excluded session; a source
        # conflict disables pruning for that entire dataset until reviewed.
        if eligible:
            allowed = ",".join("?" for _ in eligible)
            conn.execute(
                f"UPDATE tasks SET state='pending',next_attempt_at_utc=NULL "
                f"WHERE dataset IN ({allowed}) AND state='calendar_wait' AND "
                "EXISTS (SELECT 1 FROM finmind_verified_sessions WHERE day=tasks.partition)",
                eligible,
            )
            conn.execute(
                f"UPDATE tasks SET state='pending' WHERE dataset IN ({allowed}) "
                "AND state='non_session' AND (partition NOT BETWEEN ? AND ? OR "
                "EXISTS (SELECT 1 FROM finmind_verified_sessions WHERE day=tasks.partition))",
                (*eligible, *bounds),
            )
            conn.execute(
                f"UPDATE tasks SET state='non_session',next_attempt_at_utc=NULL "
                f"WHERE dataset IN ({allowed}) AND partition BETWEEN ? AND ? "
                "AND state IN ('pending','observed_empty','failed','calendar_wait') AND rows=0 "
                "AND NOT EXISTS (SELECT 1 FROM finmind_verified_sessions "
                "WHERE day=tasks.partition)", (*eligible, *bounds),
            )
            session_policy["newly_excluded"] = conn.execute("SELECT changes()").fetchone()[0]
        if conflicts:
            blocked = tuple(sorted(conflicts))
            blocked_placeholders = ",".join("?" for _ in blocked)
            conn.execute(
                f"UPDATE tasks SET state='pending' WHERE dataset IN ({blocked_placeholders}) "
                "AND state IN ('non_session','calendar_wait')", blocked,
            )
        session_policy.update({
            "state": "receipt_verified", "calendar_start": bounds[0],
            "calendar_end": bounds[1], "calendar_sha256": official_sessions.receipt_sha256,
            "conflicting_datasets": sorted(conflicts),
        })
    session_policy["excluded_non_session"] = conn.execute(
        f"SELECT COUNT(*) FROM tasks WHERE dataset IN ({placeholders}) "
        "AND state='non_session'", datasets,
    ).fetchone()[0]
    # A scheduled closure is not an acquired observation or a historical
    # non-session receipt. Keep a reversible, separately counted calendar hold;
    # otherwise undownloadable holidays become mandatory debt forever. Revisit
    # held dates as well as recent dates so a changed schedule reopens the work.
    conn.execute("CREATE TABLE IF NOT EXISTS calendar_holds ("
                 "day TEXT PRIMARY KEY,status TEXT NOT NULL,reason TEXT NOT NULL,"
                 "observed_at_utc TEXT NOT NULL,policy_version INTEGER NOT NULL)")
    today_decision = tw_stock_day_decision(
        local.date(), parquet_root=Path(__file__).resolve().parents[1] / "data_tw_public",
        observed=now,
    )
    days_to_check = {local.date(), local.date() - timedelta(days=1)}
    days_to_check.update(date.fromisoformat(row[0]) for row in conn.execute(
        f"SELECT DISTINCT partition FROM tasks WHERE dataset IN ({placeholders}) "
        "AND (state='calendar_wait' OR (partition>=? AND rows=0 "
        "AND state IN ('pending','observed_empty'))) AND partition<=?",
        (*datasets, (local.date() - timedelta(days=7)).isoformat(), local.date().isoformat()),
    ))
    closure_datasets = tuple(name for name in datasets if name not in session_policy['conflicting_datasets'])
    closure_placeholders = ','.join('?' for _ in closure_datasets)
    for day in sorted(days_to_check):
        decision = today_decision if day == local.date() else tw_stock_day_decision(
            day, parquet_root=Path(__file__).resolve().parents[1] / 'data_tw_public', observed=now,
        )
        if official_sessions is not None and day in official_sessions.days:
            decision = TwStockDayDecision('actual_open', 'receipt-verified official session')
        conn.execute(
            "INSERT INTO calendar_holds VALUES (?,?,?,?,1) ON CONFLICT(day) DO UPDATE SET "
            "status=excluded.status,reason=excluded.reason,observed_at_utc=excluded.observed_at_utc,"
            "policy_version=excluded.policy_version",
            (day.isoformat(), decision.status, decision.reason, now.isoformat()),
        )
        if decision.status == "closed" and closure_datasets:
            next_check = (now + timedelta(hours=1)).isoformat()
            conn.execute(
                f"UPDATE tasks SET state='calendar_wait',next_attempt_at_utc=? "
                f"WHERE dataset IN ({closure_placeholders}) AND partition=? "
                "AND state IN ('pending','observed_empty','calendar_wait') AND rows=0",
                (next_check, *closure_datasets, day.isoformat()),
            )
        else:
            # Loss of calendar evidence is not permission to keep excluding a
            # date. A nonempty source/calendar conflict likewise reopens holds.
            conn.execute(
                f"UPDATE tasks SET state='pending',next_attempt_at_utc=NULL "
                f"WHERE dataset IN ({placeholders}) AND partition=? AND state='calendar_wait'",
                (*datasets, day.isoformat()),
            )
        if decision.is_session:
            conn.execute(
                f"UPDATE tasks SET next_attempt_at_utc=NULL WHERE dataset IN ({placeholders}) "
                "AND partition=? AND state='pending' AND next_attempt_at_utc>?",
                (*datasets, day.isoformat(), now.isoformat()),
            )
    session_policy["today_status"] = today_decision.status
    session_policy["today_evidence"] = today_decision.reason
    session_policy["calendar_hold_policy_version"] = 1
    session_policy["calendar_wait"] = conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE state='calendar_wait'",
    ).fetchone()[0]
    session_policy["observation_dates"] = reconcile_observation_dates(
        conn, now, {spec.dataset for spec in SOURCES},
    )
    _reconcile_daily_refresh(conn, now)
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
    conn.commit()
    return session_policy


def _due_query(now: datetime, *, incremental_only: bool = False,
               datasets: tuple[str, ...] | None = None) -> tuple[str, tuple]:
    """One eligibility predicate for both dispatch and read-only wake checks."""
    due = (
        "((state='pending' AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?)) "
        "OR (state IN ('complete','observed_empty','failed') "
        "AND next_attempt_at_utc<=?)) AND (dataset!=? OR EXISTS "
        "(SELECT 1 FROM tasks parent WHERE parent.dataset=? "
        "AND parent.partition=tasks.partition AND "
        "parent.state IN ('complete','observed_empty')))"
    )
    if incremental_only:
        due += " AND (priority=0 OR kind='derived')"
    values = (now.isoformat(), now.isoformat(), WIDE_INSTITUTIONAL, LONG_INSTITUTIONAL)
    due += f" AND dataset NOT IN ({','.join('?' for _ in PRODUCT_HISTORY_STARTS)})"
    values += tuple(PRODUCT_HISTORY_STARTS)
    if datasets is not None:
        if not datasets:
            return '0', ()
        due += f" AND dataset IN ({','.join('?' for _ in datasets)})"
        values += tuple(datasets)
    return due, values


def _primary_unfinished(conn: sqlite3.Connection) -> bool:
    return conn.execute(
        "SELECT 1 FROM tasks WHERE priority<? AND "
        "state NOT IN ('complete','observed_empty','non_session','not_observation_date',"
        "'deprecated_query_shape','calendar_wait') LIMIT 1", (SECONDARY_VALIDATION_PRIORITY,),
    ).fetchone() is not None


def _ready_after_wait(root: Path, now: datetime, *, secondary_admission: Callable[[], dict[str, Any]],
                       datasets: tuple[str, ...] | None = None) -> bool:
    """A primary retry becoming due must wake even while validation is vetoed."""
    try:
        due, values = _due_query(now, datasets=datasets)
        with sqlite3.connect((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro', uri=True,
                             timeout=0.2) as conn:
            first = conn.execute(f'SELECT priority FROM tasks WHERE {due} ORDER BY priority LIMIT 1',
                                 values).fetchone()
            if first is None:
                return False
            if first[0] < SECONDARY_VALIDATION_PRIORITY:
                return True
            if _primary_unfinished(conn):
                return False
        return secondary_admission().get('allowed') is True
    except (sqlite3.Error, OSError):
        return False


def _next(conn: sqlite3.Connection, now: datetime, *, incremental_only: bool = False,
          secondary_admission: Callable[[], dict[str, Any]] | None = None,
          datasets: tuple[str, ...] | None = None) -> Task | None:
    conn.execute("CREATE TABLE IF NOT EXISTS dispatch_cursor ("
                 "priority INTEGER PRIMARY KEY,last_dataset TEXT NOT NULL)")
    due, values = _due_query(now, incremental_only=incremental_only, datasets=datasets)
    first = conn.execute(
        f"SELECT priority FROM tasks WHERE {due} ORDER BY priority LIMIT 1", values
    ).fetchone()
    if not first:
        return None
    priority = first[0]
    if priority >= SECONDARY_VALIDATION_PRIORITY:
        # A cooling/failed/in-flight mandatory partition is unfinished work,
        # not spare capacity. Priority ordering among *due* rows alone allowed
        # secondary requests to jump this gate whenever necessary work slept.
        # Keep this global even when dispatch is restricted to selected datasets.
        if _primary_unfinished(conn):
            return None
        if secondary_admission is None or secondary_admission().get("allowed") is not True:
            return None
    previous = conn.execute(
        "SELECT last_dataset FROM dispatch_cursor WHERE priority=?", (priority,)
    ).fetchone()
    cursor = previous[0] if previous else ""
    row = conn.execute(
        "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
        f"WHERE priority=? AND dataset>? AND {due} "
        "ORDER BY CASE WHEN error_code LIKE 'local_integrity:%' THEN 0 ELSE 1 END, dataset,partition DESC LIMIT 1",
        (priority, cursor, *values),
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
            f"WHERE priority=? AND {due} ORDER BY CASE WHEN error_code LIKE 'local_integrity:%' THEN 0 ELSE 1 END, dataset,partition DESC LIMIT 1",
            (priority, *values),
        ).fetchone()
    if not row:
        return None
    task = Task(*row)
    conn.execute("UPDATE tasks SET state='inflight' WHERE dataset=? AND data_id='' AND partition=?",
                 (task.dataset, task.partition))
    conn.execute(
        "INSERT INTO dispatch_cursor(priority,last_dataset) VALUES (?,?) "
        "ON CONFLICT(priority) DO UPDATE SET last_dataset=excluded.last_dataset",
        (priority, task.dataset),
    )
    conn.commit()
    return task


def _fixed_incremental_demand(root: Path, now: datetime) -> int:
    # Compatibility wrapper also keeps controlled source/calendar tests local.
    return fixed_incremental_demand(
        root, now, sources=SOURCES, session_datasets=SESSION_DAY_DATASETS,
        day_decision=tw_stock_day_decision,
    )


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
    from downloader.finmind_corrections import verified_authoritative_empty

    if verified_authoritative_empty(receipt, owner='sponsor', dataset=LONG_INSTITUTIONAL,
                                    data_id='', partition=partition):
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
    if task.dataset in PRODUCT_HISTORY_STARTS:
        raise SourceError("requires_product_id", retry_after=0)
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
            provider_end = end - timedelta(days=1) if task.dataset in INCLUSIVE_END_DATE_DATASETS else end
            params["end_date"] = provider_end.isoformat()
    rows = _fetch_rows(session, limiter, root.parent, task.dataset, token, params)
    if task.kind != "snapshot":
        for row in rows:
            stamp = row.get("date")
            if not isinstance(stamp, str) or not start.isoformat() <= stamp[:10] < end.isoformat():
                raise SourceError("response_outside_partition", retry_after=0)
    return rows


def _batch_policy_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS request_batch_policy (
            dataset TEXT PRIMARY KEY, disabled_at_utc TEXT NOT NULL,
            error_code TEXT NOT NULL, contract_version INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS request_batch_limits (
            dataset TEXT PRIMARY KEY, max_partitions INTEGER NOT NULL,
            observed_at_utc TEXT NOT NULL, error_code TEXT NOT NULL
        )
    """)


def _claim_batch(conn: sqlite3.Connection, task: Task,
                 now: datetime) -> RangeBatch[Task] | None:
    """Atomically extend the normal single-owner claim into a bounded range."""
    contract = RANGE_CONTRACTS.get(task.dataset)
    if (contract is None or not 0 <= task.priority < SECONDARY_VALIDATION_PRIORITY or task.data_id
            or task.kind != contract.grain or task.state not in {"pending", "failed", "complete", "observed_empty"}):
        return None
    conn.execute("SAVEPOINT finmind_batch_claim")
    try:
        _batch_policy_schema(conn)
        if conn.execute("SELECT 1 FROM request_batch_policy WHERE dataset=?",
                        (task.dataset,)).fetchone():
            conn.execute("RELEASE SAVEPOINT finmind_batch_claim")
            return None
        seed_state = conn.execute(
            "SELECT state FROM tasks WHERE dataset=? AND data_id=? AND partition=?",
            (task.dataset, task.data_id, task.partition),
        ).fetchone()
        if seed_state != ("inflight",):
            conn.execute("RELEASE SAVEPOINT finmind_batch_claim")
            return None
        candidates = [Task(*row) for row in conn.execute(
            "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
            "WHERE dataset=? AND data_id='' AND kind=? AND priority>=0 AND priority<? "
            "AND ((state='pending' AND next_attempt_at_utc IS NULL) OR "
            "(state IN ('pending','failed','complete','observed_empty') AND next_attempt_at_utc<=?)) "
            "ORDER BY partition DESC",
            (task.dataset, task.kind, SECONDARY_VALIDATION_PRIORITY, now.isoformat()),
        )]
        learned = conn.execute("SELECT max_partitions FROM request_batch_limits WHERE dataset=?", (task.dataset,)).fetchone()
        bound = learned[0] if learned else contract.max_partitions
        batch = coalesce_pending_tasks(task, candidates, today=now.astimezone(TAIPEI).date(),
                                       max_years=bound, max_months=bound, include_due_refresh=True)
        if batch is not None:
            for neighbor in batch.tasks:
                if neighbor.partition == task.partition:
                    continue
                claimed = conn.execute(
                    "UPDATE tasks SET state='inflight' WHERE dataset=? AND data_id='' "
                    "AND partition=? AND state=? "
                    "AND ((state='pending' AND next_attempt_at_utc IS NULL) OR next_attempt_at_utc<=?)",
                    (neighbor.dataset, neighbor.partition, neighbor.state, now.isoformat()),
                )
                if claimed.rowcount != 1:
                    raise BatchContractError("batch_claim_changed")
        conn.execute("RELEASE SAVEPOINT finmind_batch_claim")
        return batch
    except BaseException:
        conn.execute("ROLLBACK TO SAVEPOINT finmind_batch_claim")
        conn.execute("RELEASE SAVEPOINT finmind_batch_claim")
        raise


def _fetch_batch(batch: RangeBatch[Task], token: str, limiter: Any,
                 root: Path) -> dict[str, list[dict[str, Any]]]:
    """One normal, shared-limited HTTP request; validate every row before store."""
    session = getattr(_THREAD, "session", None)
    if session is None:
        session = requests.Session()
        _THREAD.session = session
    rows = _fetch_rows(session, limiter, root.parent, batch.dataset, token, batch.params(),
                       max_response_bytes=RANGE_CONTRACTS[batch.dataset].max_response_bytes)
    grouped = split_batch_rows(batch, rows)
    _preflight_bulk_year_schemas(batch.dataset, grouped)
    return grouped


def _defer_failed_batch(conn: sqlite3.Connection, batch: RangeBatch[Task],
                        error_code: str, now: datetime) -> str:
    """Keep exact queue evidence; shrink resource failures, isolate bad contracts.

    A rejected optimization must not permanently block a valid dataset or be
    retried as the same rejected multi-partition request after every restart.
    """
    conn.execute("SAVEPOINT finmind_batch_disable")
    try:
        _batch_policy_schema(conn)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS request_batch_failure_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                failed_at_utc TEXT NOT NULL, dataset TEXT NOT NULL,
                error_code TEXT NOT NULL, request_metadata_json TEXT NOT NULL,
                prior_tasks_json TEXT NOT NULL
            )
        """)
        prior = []
        for task in batch.tasks:
            cursor = conn.execute(
                "SELECT * FROM tasks WHERE dataset=? AND data_id=? AND partition=?",
                (task.dataset, task.data_id, task.partition),
            )
            row = cursor.fetchone()
            if row is not None:
                prior.append(dict(zip((column[0] for column in cursor.description), row)))
        resource_failure = error_code in {'response_size_limit', 'batch_response_row_limit',
                                          'ReadTimeout', 'ConnectTimeout', 'Timeout', 'http_504', 'http_502'}
        if resource_failure:
            conn.execute(
                "INSERT INTO request_batch_limits VALUES (?,?,?,?) ON CONFLICT(dataset) DO UPDATE SET "
                "max_partitions=min(request_batch_limits.max_partitions,excluded.max_partitions),"
                "observed_at_utc=excluded.observed_at_utc,error_code=excluded.error_code",
                (batch.dataset, max(1, len(batch.tasks) // 2), now.isoformat(), error_code),
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO request_batch_policy "
                "(dataset,disabled_at_utc,error_code,contract_version) VALUES (?,?,?,?)",
                (batch.dataset, now.isoformat(), error_code, BATCH_CONTRACT_VERSION),
            )
        conn.execute(
            "INSERT INTO request_batch_failure_audit "
            "(failed_at_utc,dataset,error_code,request_metadata_json,prior_tasks_json) "
            "VALUES (?,?,?,?,?)",
            (now.isoformat(), batch.dataset, error_code,
             json.dumps(batch.metadata(), sort_keys=True), json.dumps(prior, sort_keys=True)),
        )
        for task in batch.tasks:
            conn.execute(
                "UPDATE tasks SET state='pending',error_code=?,last_attempt_at_utc=?,"
                "next_attempt_at_utc=? WHERE dataset=? AND data_id=? AND partition=? "
                "AND state='inflight'",
                (f"{'batch_reduced' if resource_failure else 'batch_disabled'}:{error_code}", now.isoformat(),
                 (now + timedelta(seconds=60)).isoformat(),
                 task.dataset, task.data_id, task.partition),
            )
        conn.execute("RELEASE SAVEPOINT finmind_batch_disable")
        return 'batch_reduced_retry' if resource_failure else 'batch_disabled_single_retry'
    except BaseException:
        conn.execute("ROLLBACK TO SAVEPOINT finmind_batch_disable")
        conn.execute("RELEASE SAVEPOINT finmind_batch_disable")
        raise


def _late_daily_retry(dataset: str, partition: str, now: datetime) -> bool:
    day = date.fromisoformat(partition)
    if not 1 <= (now.astimezone(TAIPEI).date() - day).days <= 7:
        return False
    if dataset not in SESSION_DAY_DATASETS:
        return True  # Event dates need not be exchange sessions.
    decision = tw_stock_day_decision(
        day, parquet_root=Path(__file__).resolve().parents[1] / "data_tw_public", observed=now,
    )
    return decision.status != 'closed'  # Unknown is not a no-data proof.


def _reconcile_daily_refresh(conn: sqlite3.Connection, now: datetime) -> None:
    local_day = now.astimezone(TAIPEI).date()
    period_names = tuple(sorted(PERIOD_DATASETS))
    predicate = ("kind IN ('day','derived') AND partition<? "
                 f"AND dataset NOT IN ({','.join('?' for _ in period_names)})")
    params = (local_day.isoformat(), *period_names)
    conn.execute(
        f"UPDATE tasks SET next_attempt_at_utc=NULL WHERE {predicate} AND "
        "next_attempt_at_utc IS NOT NULL AND (state='complete' OR "
        "(state='observed_empty' AND partition<?))",
        (*params, (local_day - timedelta(days=7)).isoformat()),
    )
    recent = conn.execute(
        f"SELECT dataset,partition,last_attempt_at_utc FROM tasks WHERE {predicate} "
        "AND state='observed_empty' AND partition>=?",
        (*params, (local_day - timedelta(days=7)).isoformat()),
    ).fetchall()
    for dataset, partition, attempted in recent:
        next_at = None
        if _late_daily_retry(dataset, partition, now):
            try:
                last = datetime.fromisoformat(attempted)
                if last.tzinfo is None or last > now:
                    raise ValueError('untrusted attempt time')
                next_at = last + timedelta(hours=4)
            except (ValueError, TypeError):
                next_at = now
        conn.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE dataset=? AND data_id='' AND partition=?",
                     (next_at.isoformat() if next_at else None, dataset, partition))


def _finish(conn: sqlite3.Connection, root: Path, task: Task,
            rows: list[dict[str, Any]], now: datetime, *,
            request_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    from downloader.finmind_corrections import correction_context

    context = correction_context(conn, task)
    if not rows and not context.get('allow_empty'):
        previous = conn.execute(
            "SELECT rows,receipt_path FROM tasks WHERE dataset=? AND data_id=? AND partition=?",
            (task.dataset, task.data_id, task.partition),
        ).fetchone()
        if previous and previous[0] > 0:
            atomic_write_json(
                root / 'failed_response_receipts' / task.dataset / task.partition /
                f"{now.strftime('%Y%m%dT%H%M%S%fZ')}.json",
                {'error_code': 'unexpected_empty_after_nonempty', 'dataset': task.dataset,
                 'partition': task.partition, 'observed_at_utc': now.isoformat(),
                 'last_good_receipt': previous[1], 'previous_rows': previous[0],
                 'request': request_metadata},
            )
            raise SourceError('unexpected_empty_after_nonempty', retry_after=900)
    if task.dataset in RANGE_CONTRACTS and request_metadata is None:
        # A rejected batch may fall back to one period; it must not bypass the
        # same flat-schema checks and silently lose fields absent in row one.
        _preflight_bulk_year_schemas(task.dataset, {task.partition: rows})
    store_options = {'correction': context} if context else {}
    if request_metadata is not None:
        store_options['request_metadata'] = request_metadata
    receipt = _store(root, task, rows, now, **store_options)
    current = task.kind == "snapshot" or (task.kind != "snapshot" and
              _end(date.fromisoformat(task.partition), SPECS[task.dataset].grain) >
              now.astimezone(TAIPEI).date())
    # A current-period empty response may mean "not published yet" rather than
    # a genuine zero event. Probe again within the same session; never impose
    # an arbitrary whole-day delay on fixed incremental data.
    next_at = (now + (timedelta(hours=4) if rows else timedelta(minutes=15))) if current else None
    if task.dataset in PERIOD_DATASETS:
        next_at = next_period_refresh(task.dataset, task.partition, now)
    elif not rows and not context.get('allow_empty') and task.kind in {'day', 'derived'} and _late_daily_retry(task.dataset, task.partition, now):
        next_at = now + timedelta(hours=4)
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
            active: list[Task] | None = None, last: dict[str, Any] | None = None,
            session_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    from downloader.acquisition_policy import finmind_secondary_mode
    series = {spec.dataset: {"target": 0, "complete": 0, "observed_empty": 0,
                             "failed": 0, "blocked": 0, "non_session": 0,
                             "not_observation_date": 0,
                             "calendar_wait": 0,
                             "deprecated_query_shape": 0,
                             "source_status": ("delegated_to_complement_product_history"
                                               if spec.dataset in PRODUCT_HISTORY_STARTS else
                                               "observed_sponsor_market_response_not_provider_completeness"),
                             "rows": 0, "bytes": 0,
                             "first_data_date": None, "last_data_date": None,
                             "last_attempt_at_utc": None,
                             "last_checked_partition": None}
              for spec in SOURCES}
    for dataset, task_state, count, row_count, size, first, last_date, attempted, partition in conn.execute(
        "SELECT dataset,state,COUNT(*),SUM(rows),SUM(bytes),MIN(first_data_date),"
        "MAX(last_data_date),MAX(last_attempt_at_utc),MAX(partition) FROM tasks GROUP BY dataset,state"
    ):
        if dataset not in series:
            continue
        item = series[dataset]
        if task_state not in {"non_session", EXCLUDED_STATE, "deprecated_query_shape", "calendar_wait"}:
            item["target"] += count
        if task_state in item:
            item[task_state] += count
        if attempted and (item["last_attempt_at_utc"] is None or attempted > item["last_attempt_at_utc"]):
            item["last_attempt_at_utc"] = attempted
        if (task_state in {"complete", "observed_empty"} and partition
                and (item["last_checked_partition"] is None
                     or partition > item["last_checked_partition"])):
            item["last_checked_partition"] = partition
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
        "session_policy": session_policy or {"state": "calendar_unverified"},
        "active_tasks": [{"dataset": task.dataset, "partition": task.partition} for task in active or []],
        "last_task": last, "catalog_source": CATALOG_URL,
        "training": "raw_not_point_in_time_validated", "news": "owned_by_complement",
        "acquisition_policy": {
            "required_unfinished": conn.execute(
                "SELECT count(*) FROM tasks WHERE priority<? AND "
                "state NOT IN ('complete','observed_empty','non_session','not_observation_date','deprecated_query_shape','calendar_wait')",
                (SECONDARY_VALIDATION_PRIORITY,),
            ).fetchone()[0],
            "secondary_unfinished": conn.execute(
                "SELECT count(*) FROM tasks WHERE priority>=? AND "
                "state NOT IN ('complete','observed_empty','non_session','not_observation_date','deprecated_query_shape','calendar_wait')",
                (SECONDARY_VALIDATION_PRIORITY,),
            ).fetchone()[0],
            "secondary_rule": finmind_secondary_mode(),
            "observed_empty_is_data_complete": False,
        },
    }
    atomic_write_json(root / "status.json", result)
    return result


def run_once(root: Path, *, max_requests: int = 100, workers: int = 4,
             secondary_admission: Callable[[], dict[str, Any]] | None = None,
             datasets: tuple[str, ...] | None = None) -> dict[str, Any]:
    if workers < 1 or workers > 8 or max_requests < 0:
        raise ValueError("workers must be 1..8 and max_requests nonnegative")
    if datasets is not None:
        datasets = tuple(dict.fromkeys(datasets))
        unknown = set(datasets) - {spec.dataset for spec in SOURCES}
        if not datasets or unknown:
            raise ValueError(f"datasets must select existing Sponsor sources; unknown={sorted(unknown)}")
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
    if secondary_admission is None:
        from downloader.acquisition_policy import evaluate_finmind_secondary_admission

        secondary_admission = lambda: evaluate_finmind_secondary_admission(root=root.parent)
    with _db(root / "queue.sqlite3") as conn:
        from downloader.finmind_corrections import apply_worker_corrections, reconcile_worker_corrections

        session_policy = _seed(
            conn, datetime.now(UTC), official_price_coverage=_official_price_coverage(),
            official_sessions=_official_session_calendar(), root=root,
        )
        from downloader.finmind_integrity import audit_completed_batch

        if not protected_stock_opening(datetime.now(UTC)):
            audit_completed_batch(conn, root)
        reconcile_worker_corrections(conn, root, 'sponsor', datetime.now(UTC))
        correction_summary = apply_worker_corrections(conn, root, 'sponsor', datetime.now(UTC))
        conn.commit()
        atomic_write_json(root / 'correction_status.json', {
            'observed_at_utc': datetime.now(UTC).isoformat(), **correction_summary,
        })
        _status(conn, root, "running", account=account, session_policy=session_policy)
        last_status_at = time.monotonic()
        last: dict[str, Any] | None = None
        in_flight: dict[Future[Any], tuple[Task, RangeBatch[Task] | None]] = {}

        def active_tasks() -> list[Task]:
            return [part for seed, batch in in_flight.values()
                    for part in (batch.tasks if batch is not None else (seed,))]

        sent = 0
        halt = False
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="finmind-sponsor") as pool:
            while in_flight or (not halt and (not max_requests or sent < max_requests)):
                local = datetime.now(TAIPEI)
                if shutil.disk_usage(root).free < MIN_FREE_BYTES:
                    halt = True
                    reason = "disk_guard"
                elif protected_stock_opening(local):
                    halt = True
                    reason = "protected_opening"
                while not halt and len(in_flight) < workers and (not max_requests or sent < max_requests):
                    dispatch_now = datetime.now(UTC)
                    account = refresh_dispatch_account(account, token, root.parent, dispatch_now)
                    budget = backfill_budget(
                        account, root.parent,
                        fixed_incremental_requests=_fixed_incremental_demand(root.parent, dispatch_now),
                        in_flight=len(in_flight), now=dispatch_now, prioritize_due=True,
                    )
                    if budget.get('remaining', len(in_flight) + 1) <= len(in_flight):
                        halt = True
                        reason = 'incremental_reserve'
                        break
                    task = _next(conn, dispatch_now, incremental_only=not budget["allowed"],
                                 secondary_admission=secondary_admission, datasets=datasets)
                    if task is None:
                        halt = True
                        reason = "incremental_reserve" if not budget["allowed"] else "current_queue"
                        if budget["allowed"] and conn.execute(
                            "SELECT 1 FROM tasks WHERE priority>=? AND "
                            "state IN ('pending','failed') LIMIT 1",
                            (SECONDARY_VALIDATION_PRIORITY,),
                        ).fetchone():
                            reason = "waiting_necessary_acquisition"
                        break
                    batch = _claim_batch(conn, task, dispatch_now) if budget['allowed'] else None
                    future = (pool.submit(_fetch_batch, batch, token, limiter, root)
                              if batch is not None else
                              pool.submit(_fetch, task, token, limiter, root, local.date()))
                    in_flight[future] = (task, batch)
                    sent += task.kind != "derived"
                if not in_flight:
                    break
                done, _ = wait(in_flight, timeout=55, return_when=FIRST_COMPLETED)
                if not done:
                    _status(conn, root, "running", account=account, active=active_tasks(),
                            last=last, session_policy=session_policy)
                    last_status_at = time.monotonic()
                for future in done:
                    task, batch = in_flight.pop(future)
                    unfinished = list(batch.tasks if batch is not None else (task,))
                    now = datetime.now(UTC)
                    try:
                        result = future.result()
                        # _fetch_batch validates and splits the *complete* body
                        # before any constituent receipt becomes visible.
                        if batch is not None:
                            metadata = batch.metadata()
                            metadata['response_rows'] = sum(len(part_rows) for part_rows in result.values())
                            metadata['response_partition_rows'] = {key: len(value) for key, value in result.items()}
                            rows_total = 0
                            for part in batch.tasks:
                                rows = result[part.partition]
                                receipt = _finish(conn, root, part, rows, now,
                                                  request_metadata=metadata)
                                unfinished.remove(part)
                                rows_total += len(rows)
                            last = {"dataset": task.dataset, "partition": task.partition,
                                    "status": receipt["status"], "rows": rows_total,
                                    "request_batch": metadata}
                        else:
                            receipt = _finish(conn, root, task, result, now)
                            unfinished.clear()
                            last = {"dataset": task.dataset, "partition": task.partition,
                                    "status": receipt["status"], "rows": len(result)}
                    except BatchContractError as error:
                        retry_status = 'failed'
                        if batch is None:
                            for part in unfinished:
                                _fail(conn, part, SourceError("invalid_batch_contract", retry_after=900), now)
                        else:
                            retry_status = _defer_failed_batch(conn, batch, str(error), now)
                        last = {"dataset": task.dataset, "partition": task.partition,
                                "status": retry_status, "error_code": str(error)}
                    except SourceError as error:
                        if batch is not None and error.code in {
                                "provider_bad_request", "response_outside_partition", "invalid_rows", "invalid_bulk_schema",
                                "response_size_limit", "ReadTimeout", "ConnectTimeout", "Timeout", "http_504", "http_502"}:
                            retry_status = _defer_failed_batch(conn, batch, error.code, now)
                            last = {"dataset": task.dataset, "partition": task.partition,
                                    "status": retry_status, "error_code": error.code}
                        else:
                            recovered = None
                            for part in unfinished:
                                recovery = recover_failed_long_parent(conn, root, part, error, now)
                                if recovery is not None:
                                    recovered = recovery
                                else:
                                    _fail(conn, part, error, now)
                            last = {"dataset": task.dataset, "partition": task.partition,
                                    "status": "waiting_long_parent" if recovered else "failed",
                                    "error_code": error.code}
                            if recovered is not None:
                                last["parent_recovery"] = recovered
                            if error.code in {"rate_limited", "invalid_token", "ip_banned", "not_entitled",
                                              "provider_bad_request", "response_outside_partition"}:
                                halt = True
                                reason = error.code
                    except (OSError, ValueError, RuntimeError, TypeError) as exc:
                        for part in unfinished:
                            _fail(conn, part, SourceError("storage_or_schema_error", retry_after=900), now)
                        last = {"dataset": task.dataset, "partition": task.partition,
                                "status": "failed", "error_code": "storage_or_schema_error",
                                "exception_type": type(exc).__name__}
                    if time.monotonic() - last_status_at >= 20:
                        _status(conn, root, "running", account=account,
                                active=active_tasks(), last=last,
                                session_policy=session_policy)
                        last_status_at = time.monotonic()
        correction_progress = reconcile_worker_corrections(conn, root, 'sponsor', datetime.now(UTC))
        conn.commit()
        atomic_write_json(root / 'correction_status.json', {
            'observed_at_utc': datetime.now(UTC).isoformat(), **correction_summary,
            'verification': correction_progress,
        })
        return _status(conn, root, reason if halt else "batch_complete", account=account,
                       last=last, session_policy=session_policy)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_finmind/sponsor"))
    parser.add_argument("--max-requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--dataset", action="append", choices=sorted(spec.dataset for spec in SOURCES),
                        help="Dispatch only this existing dataset; repeat to select more than one")
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
                result = run_once(root, max_requests=args.max_requests, workers=args.workers,
                                  datasets=tuple(args.dataset) if args.dataset is not None else None)
                print(json.dumps({"state": result["state"], "last_task": result.get("last_task")},
                                 ensure_ascii=False), flush=True)
                if not args.loop or result["state"] in {"account_unverified", "not_entitled", "invalid_token"}:
                    return 0 if result["state"] not in {"account_unverified", "not_entitled"} else 2
                delay = (1800 if result["state"] in {"rate_limited", "ip_banned"} else
                         600 if result["state"] in {"current_queue", "protected_opening", "disk_guard",
                                                     "waiting_necessary_acquisition"} else
                         60 if result["state"] == "incremental_reserve" else 2)
                from downloader.acquisition_policy import evaluate_finmind_secondary_admission
                wake_if = (lambda: _ready_after_wait(
                    root, datetime.now(UTC),
                    secondary_admission=lambda: evaluate_finmind_secondary_admission(root=root.parent),
                    datasets=tuple(args.dataset) if args.dataset is not None else None,
                )) if result['state'] == 'waiting_necessary_acquisition' else None
                wait_for_next_cycle(root, result, delay, sources=SOURCES, wake_if=wake_if)
        except KeyboardInterrupt:
            return 130


if __name__ == "__main__":
    raise SystemExit(main())
