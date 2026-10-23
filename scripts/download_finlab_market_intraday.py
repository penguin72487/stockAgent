#!/usr/bin/env python3
"""Bounded all-stock FinLab minute-first acquisition and residual Tick discovery.

The symbol universe is the union of the official current manifest and official
delisting tables. Local daily-price bounds are *candidate* lifecycle evidence,
not proof that FinLab has published the corresponding intraday partition.
"""

from __future__ import annotations

import argparse
from bisect import bisect_left, bisect_right
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import fcntl
import gc
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import sys
import time

import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.download_finlab_history import (  # noqa: E402
    DEFAULT_OUTPUT, TAIPEI, _atomic_json, credential_available, quota_cycle_start, safe_stem,
    quota_room_mb,
    general_work_status, load_catalog,
)
from scripts.download_finlab_intraday import (  # noqa: E402
    _checked_this_cycle, _partition_paths, _stored_receipt, fetch_partition,
    reconcile_stored_tick_unit, fetch_partitions, initialize_intraday_worker,
)
from scripts.derive_finlab_minute import derive_partition  # noqa: E402
from stockagent.data.finlab_acquisition_contract import (  # noqa: E402
    INTRADAY_PROGRESS_VERSION, INTRADAY_MINIMUM_FREE_GB, process_owner,
)
from stockagent.data.finlab_progress import estimate_attempt_batch  # noqa: E402
from stockagent.data.finlab_tick_eta import (  # noqa: E402
    ACCEPTED, BATCH_METRICS_VERSION, phase_for_day, read_json,
)
from stockagent.data.finlab_quota_policy import (  # noqa: E402
    ACQUISITION_POLICY_VERSION, QuotaPolicy, QuotaSampleWindow, load_policy, quota_admission,
    publication_recheck_delay,
)
from stockagent.data.finlab_transport import HTTP_COUNTERS, validate_prefetch_sdk  # noqa: E402
from scripts.check_outside_tw_opening_resource_window import (  # noqa: E402
    WINDOW_START, WINDOW_END, evaluate as opening_admission,
)


DEFAULT_PUBLIC_ROOT = REPO_ROOT / "data_tw_public"
KINDS = ("tw_minute", "tw_tick")
DISPLAY_KINDS = ("tw_minute", "tw_tick")
DERIVED_INDEX_KIND = "tw_minute_derived"


def default_market_end(now: datetime) -> date:
    """Allow today's Taipei session after regular close, not a fixed D-1 lag.

    session_days limits this bound to actual observed official trading dates;
    FinLab's published-partition response separately determines availability.
    Do not spend calls on an unfinished session before 13:30. After that, an
    unconditional D-1 bound would delay already available aftermarket data.
    """
    local = now.astimezone(TAIPEI)
    return local.date() if (local.hour, local.minute) >= (13, 30) else local.date() - timedelta(days=1)


def plan_minute_window(anchor: date, sessions: list[date], *, lower_bound: date,
                       calendar_days: int, limit: int, available,
                       upper_bound: date | None = None) -> list[date]:
    """Consecutive missing verified sessions only; never span a cached hole.

    Actual provider supply remains unverified. Latest-day and discovery probes
    use single dates; this bounded range planner is for known-history work.
    Prefer older sessions, then fill forward if the caller supplies a verified
    history bound. An oldest-first frontier need not cost a one-day SDK query.
    """
    result = [anchor]
    cutoff = max(lower_bound, anchor - timedelta(days=calendar_days - 1))
    position = bisect_left(sessions, anchor)
    for day in reversed(sessions[:position]):
        if day < cutoff or len(result) >= limit or not available(day):
            break
        result.append(day)
    if upper_bound is not None:
        ceiling = min(upper_bound, min(result) + timedelta(days=calendar_days - 1))
        for day in sessions[position+1:]:
            if day > ceiling or len(result) >= limit or not available(day):
                break
            result.append(day)
    return sorted(result)


def _minute_pool(workers: int, *, keepalive: bool = True, download_threads: int = 1,
                 rate_gate=None, authorization_gate=None):
    from finlab import get_token
    from finlab.auth import get_session
    from finlab import utils

    if keepalive and getattr(utils.requests, "__name__", None) != "requests":
        raise RuntimeError("unsupported FinLab HTTP transport; disable minute_http_keepalive to retain native transport")
    if download_threads > 1:
        validate_prefetch_sdk()  # Reject unsupported code before Pool respawn loops.
    get_token()  # Refresh once before process startup, never print credentials.
    session = get_session()
    return multiprocessing.get_context("spawn").Pool(processes=workers,
        initializer=initialize_intraday_worker,
        initargs=(session, keepalive, download_threads, rate_gate, authorization_gate))


@contextmanager
def _account_sync_lock(root: Path, inherited_fd: int | None = None):
    """Standalone and timer paths share one real account acquisition lock."""
    root.mkdir(parents=True, exist_ok=True)
    path = root / ".sync.lock"
    handle = None
    if inherited_fd is None:
        handle = path.open("a")
        fd = handle.fileno()
    else:
        fd = inherited_fd
        actual, expected = os.fstat(fd), path.stat()
        if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
            raise ValueError("inherited FinLab lock does not match this output root")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        # Never unlock the shell's inherited open-file description.
        if handle is not None:
            handle.close()


def _general_work_admission(root: Path, *, now: datetime) -> dict:
    try:
        discovery = json.loads((root / "catalog/discovery.json").read_text())
    except (OSError, ValueError):
        discovery = {}
    curated = {item["key"]: item for item in load_catalog()["datasets"]}
    # The ordinary schedule uses a daily quota cycle. A manual Tick command
    # must not silently weaken this gate to the CLI history default of 30 days.
    return general_work_status(discovery, curated, root, now=now, refresh_days=1)


def _dates_from_parquet(path: Path) -> tuple[date, date] | None:
    if not path.is_file():
        return None
    row = pl.scan_parquet(path).select(
        pl.col("date").min().alias("first"),
        pl.col("date").max().alias("last"),
    ).collect().row(0, named=True)
    if row["first"] is None or row["last"] is None:
        return None
    return date.fromisoformat(str(row["first"])), date.fromisoformat(str(row["last"]))


def load_universe(public_root: Path, *, start: date = date(2003, 8, 1),
                  include_etf: bool = False) -> list[dict]:
    """Keep former stocks even when the current contract directory omits them."""
    stocks = public_root / "stocks"
    current = pl.read_csv(stocks / "symbols.csv", infer_schema_length=0)
    wanted = {"stock", "etf"} if include_etf else {"stock"}
    current_codes: set[str] = set()
    for filename, code_col in (
        ("twse_listed_company_basic.parquet", "公司代號"),
        ("tpex_basic_company.parquet", "SecuritiesCompanyCode"),
    ):
        source = public_root / filename
        if not source.is_file():
            raise FileNotFoundError(f"official current listing snapshot missing: {source}")
        frame = pl.read_parquet(source, columns=[code_col, "date"])
        latest = frame["date"].max()
        current_codes.update(str(value).strip() for value in frame.filter(
            pl.col("date") == latest
        )[code_col].to_list() if value)
    entries: dict[str, dict] = {}
    for row in current.iter_rows(named=True):
        if str(row["security_type"]).lower() not in wanted:
            continue
        symbol = str(row["code"]).strip()
        if symbol:
            entries[symbol] = {
                "symbol": symbol, "market": str(row["market"]),
                "security_type": str(row["security_type"]),
                "current": symbol in current_codes,
                "delisting_date": None,
            }
    for venue in ("twse", "tpex"):
        path = public_root / f"{venue}_delisted_company.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"official delisted universe missing: {path}")
        for row in pl.read_parquet(path, columns=["symbol", "date"]).iter_rows(named=True):
            symbol = str(row["symbol"] or "").strip()
            if not symbol:
                continue
            item = entries.setdefault(symbol, {
                "symbol": symbol, "market": venue,
                "security_type": "stock" if re.fullmatch(r"[1-9][0-9]{3}", symbol)
                                 else "tdr" if re.fullmatch(r"9[0-9]{5}", symbol)
                                 else "other_historical_equity", "current": False,
                "delisting_date": None,
            })
            if not item["current"]:
                old = item["delisting_date"]
                item["delisting_date"] = max(str(row["date"]), old or "")
    for item in entries.values():
        bounds = _dates_from_parquet(stocks / f"{item['symbol']}_features.parquet")
        item["first_local_daily_date"] = bounds[0].isoformat() if bounds else None
        item["last_local_daily_date"] = bounds[1].isoformat() if bounds else None
        item["first_candidate_date"] = (
            bounds[0].isoformat() if bounds else start.isoformat()
        )
        item["lifecycle_basis"] = (
            "local_daily_bounds_and_official_delisting" if bounds and not item["current"]
            else "local_daily_bounds_and_current_manifest" if bounds
            else "missing_local_daily_bounds"
        )
    return sorted(entries.values(), key=lambda item: item["symbol"])


def session_days(public_root: Path, *, start: date, end: date) -> list[date]:
    """Use observed official TWSE/TPEx daily sessions, not generic weekdays."""
    path = public_root / "twse_market_index.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"official TWSE session source missing: {path}")
    values = pl.scan_parquet(path).select("date").unique().collect()["date"].to_list()
    if start < date(2009, 1, 5):
        for filename in ("twse_daily_ohlcv.parquet", "tpex_daily_ohlcv.parquet"):
            source = public_root / filename
            if not source.is_file():
                raise FileNotFoundError(f"official pre-2009 session source missing: {source}")
            values.extend(pl.scan_parquet(source).filter(
                pl.col("date") < "2009-01-05"
            ).select("date").unique().collect()["date"].to_list())
    return sorted({day for value in values
                   if (day := date.fromisoformat(str(value))) >= start and day <= end})


def _eligible(item: dict, day: date) -> bool:
    first = item["first_candidate_date"]
    if first is None or day < date.fromisoformat(first):
        return False
    if item["current"]:
        return True
    final = item["delisting_date"] or item["last_local_daily_date"]
    return final is not None and day <= date.fromisoformat(final)


def _target_count(item: dict, days: list[date]) -> int:
    if not days:
        return 0
    first = date.fromisoformat(item["first_candidate_date"])
    final = days[-1] if item["current"] else date.fromisoformat(
        item["delisting_date"] or item["last_local_daily_date"]
    )
    return max(0, bisect_right(days, final) - bisect_left(days, first))


def annotate_daily_close_coverage(root: Path, universe: list[dict]) -> dict:
    """Check actual non-null symbol columns, not merely a wide key receipt."""
    key = "price:收盤價"
    receipt_path = root / "receipts" / f"{safe_stem(key)}.json"
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        relative = Path(receipt["parquet_path"])
        source = (root / relative).resolve()
        if (receipt.get("dataset") != key or
                receipt.get("status") != "downloaded_unverified_for_pit" or
                relative.is_absolute() or relative.parts[:1] != ("datasets",) or
                not source.is_relative_to(root.resolve()) or not source.is_file()):
            raise ValueError("daily close receipt is not a valid local source")
        parquet = pq.ParquetFile(source)
        positions = {name: idx for idx, name in enumerate(parquet.schema_arrow.names)}
        for item in universe:
            position = positions.get(item["symbol"])
            if position is None:
                item["finlab_daily_close_rows"] = 0
                continue
            rows = 0
            for group in range(parquet.metadata.num_row_groups):
                metadata = parquet.metadata.row_group(group)
                stats = metadata.column(position).statistics
                if stats is None or stats.null_count is None:
                    rows = None
                    break
                rows += metadata.num_rows - stats.null_count
            item["finlab_daily_close_rows"] = rows
    except (OSError, KeyError, ValueError, TypeError):
        for item in universe:
            item["finlab_daily_close_rows"] = None
        return {"state": "source_unverified", "source_key": key}
    present = sum((item["finlab_daily_close_rows"] or 0) > 0 for item in universe)
    missing = [item for item in universe if item["finlab_daily_close_rows"] == 0]
    unknown = sum(item["finlab_daily_close_rows"] is None for item in universe)
    return {
        "state": "parquet_footer_nonnull_counts" if not unknown else "partial_footer_stats",
        "source_key": key,
        "source_first_index": receipt.get("first_non_null_source_index"),
        "source_last_index": receipt.get("last_non_null_source_index"),
        "symbols_with_values": present,
        "symbols_missing": len(missing), "symbols_unknown": unknown,
        "former_with_values": sum(not item["current"] and
                                  (item["finlab_daily_close_rows"] or 0) > 0
                                  for item in universe),
        "former_missing": sum(not item["current"] and
                              item["finlab_daily_close_rows"] == 0
                              for item in universe),
        "missing_but_local_daily": sum(item["first_local_daily_date"] is not None
                                       for item in missing),
        "missing_both_sources": sum(item["first_local_daily_date"] is None
                                    for item in missing),
        "basis": "FinLab downloaded price:收盤價 Parquet footer non-null counts by symbol; local daily presence is a separate source and not a FinLab download",
    }


def _open_index(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""CREATE TABLE IF NOT EXISTS partitions (
        kind TEXT NOT NULL, symbol TEXT NOT NULL, trade_date TEXT NOT NULL,
        status TEXT NOT NULL, rows INTEGER NOT NULL DEFAULT 0,
        parquet_bytes INTEGER NOT NULL DEFAULT 0, checked_at_utc TEXT NOT NULL,
        PRIMARY KEY(kind, symbol, trade_date))""")
    db.execute("CREATE INDEX IF NOT EXISTS partitions_retry ON partitions(status, checked_at_utc)")
    _create_effective_partitions_view(db)
    return db


def _create_effective_partitions_view(db: sqlite3.Connection) -> None:
    """Select the same effective source without ranking the entire minute index.

    Accepted direct data wins; a failed direct probe loses only to an accepted
    local derivation. With two failures direct still wins. Missing direct data
    never hides a derived row, including its errors. The existing primary key
    bounds every partner probe. No persistent index, materialized cache, source
    mutation or refresh delay is introduced; reads see the current transaction.
    """
    accepted = ("('downloaded_unverified_for_pit','derived_unverified_for_pit',"
                "'derived_no_regular_trades','verified_closed_date','verified_no_trade')")
    db.execute(f"""CREATE TEMP VIEW effective_partitions AS
      SELECT kind,symbol,trade_date,status,rows,parquet_bytes,checked_at_utc,
             'finlab_api' AS source_kind FROM partitions WHERE kind='tw_tick'
      UNION ALL
      SELECT kind,symbol,trade_date,status,rows,parquet_bytes,checked_at_utc,
             'finlab_api' AS source_kind FROM partitions AS direct
      WHERE kind='tw_minute' AND (status IN {accepted} OR NOT EXISTS (
        SELECT 1 FROM partitions AS derived
        WHERE derived.kind='tw_minute_derived' AND derived.symbol=direct.symbol
          AND derived.trade_date=direct.trade_date AND derived.status IN {accepted}))
      UNION ALL
      SELECT 'tw_minute',symbol,trade_date,status,rows,parquet_bytes,checked_at_utc,
             'derived_from_tw_tick' AS source_kind FROM partitions AS derived
      WHERE kind='tw_minute_derived' AND NOT EXISTS (
        SELECT 1 FROM partitions AS direct
        WHERE direct.kind='tw_minute' AND direct.symbol=derived.symbol
          AND direct.trade_date=derived.trade_date
          AND (direct.status IN {accepted} OR derived.status NOT IN {accepted}))""")


def _effective_partitions_view_identity(db: sqlite3.Connection) -> dict:
    """Observe the actually opened owner connection, not a code-file guess."""
    row = db.execute("SELECT sql FROM sqlite_temp_master "
                     "WHERE type='view' AND name='effective_partitions'").fetchone()
    if row is None or not isinstance(row[0], str):
        raise ValueError("effective partition view is missing")
    return {"contract": "finlab_effective_partition_view_observation_v1",
            "sql_sha256": hashlib.sha256(row[0].encode("utf-8")).hexdigest()}


def _record(db: sqlite3.Connection, kind: str, symbol: str, day: date,
            status: str, rows: int = 0, parquet_bytes: int = 0) -> None:
    db.execute("""INSERT INTO partitions VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(kind,symbol,trade_date) DO UPDATE SET
        status=excluded.status, rows=excluded.rows,
        parquet_bytes=excluded.parquet_bytes, checked_at_utc=excluded.checked_at_utc""",
        (kind, symbol, day.isoformat(), status, rows, parquet_bytes,
         datetime.now(UTC).isoformat()))
    db.commit()


def _record_tick_and_minute(db: sqlite3.Connection, root: Path, symbol: str,
                            day: date, receipt: dict) -> None:
    _record(db, "tw_tick", symbol, day, receipt["status"],
            int(receipt.get("rows") or 0), int(receipt.get("parquet_size_bytes") or 0))
    key = f"tw_minute:{symbol}"
    direct = _stored_receipt(_partition_paths(root, key, day)[0], root, key, day)
    if direct is not None:
        _record(db, "tw_minute", symbol, day, direct["status"],
                int(direct.get("rows") or 0), int(direct.get("parquet_size_bytes") or 0))
        return  # Do not group/sort Tick again for an already authoritative minute.
    try:
        derived = derive_partition(root, symbol, day)
        _record(db, DERIVED_INDEX_KIND, symbol, day, derived["status"],
                int(derived.get("rows") or 0),
                int(derived.get("parquet_size_bytes") or 0))
    except Exception as exc:
        _record(db, DERIVED_INDEX_KIND, symbol, day, "derivation_error")
        print(f"[finlab-market] derive tw_minute:{symbol} {day}: {type(exc).__name__}", flush=True)


def _record_source(db: sqlite3.Connection, root: Path, kind: str, symbol: str,
                   day: date, receipt: dict) -> None:
    if kind == "tw_tick":
        _record_tick_and_minute(db, root, symbol, day, receipt)
    else:
        _record(db, kind, symbol, day, receipt["status"],
                int(receipt.get("rows") or 0), int(receipt.get("parquet_size_bytes") or 0))


def _available_receipt(root: Path, kind: str, symbol: str, day: date) -> dict | None:
    """Reuse actual stored sources, never an index-only completion claim."""
    key = f"{kind}:{symbol}"
    direct = _stored_receipt(_partition_paths(root, key, day)[0], root, key, day)
    if direct is not None or kind != "tw_minute":
        return direct
    tick_key = f"tw_tick:{symbol}"
    tick = _stored_receipt(_partition_paths(root, tick_key, day)[0], root, tick_key, day)
    if tick is None:
        return None
    try:
        return derive_partition(root, symbol, day)
    except Exception as exc:
        print(f"[finlab-market] cached minute derivation {symbol} {day}: {type(exc).__name__}", flush=True)
        return None  # Direct minutes can still repair a local derivation failure.


def _unchecked_known_minutes(db: sqlite3.Connection, universe: list[dict], days: list[date], *,
                             first_observed: str | None, limit: int) -> list[dict]:
    """Recover holes behind an advanced frontier, bounded to a proven date window.

    Index presence is only candidate exclusion here. Actual receipt/object
    validation still owns every download decision. Work is bounded by existing
    index rows plus per-symbol session sets; never materialize the full-market
    multi-decade candidate Cartesian product.
    """
    if first_observed is None or limit < 1:
        return []
    known_days = days[bisect_left(days, date.fromisoformat(first_observed)):]
    indexed = {}
    for symbol, text in db.execute("SELECT symbol,trade_date FROM effective_partitions "
                                  "WHERE kind='tw_minute' AND trade_date>=?", (first_observed,)):
        indexed.setdefault(symbol, set()).add(text)
    result = []
    # Latest evidence first, while each selected hole retains its exact key/day.
    for item in universe:
        lower = max(known_days[0], date.fromisoformat(item["first_candidate_date"])) if known_days else None
        if lower is None:
            continue
        observed = indexed.get(item["symbol"], set())
        for day in reversed(known_days[bisect_left(known_days, lower):]):
            if _eligible(item, day) and day.isoformat() not in observed:
                result.append({"symbol": item["symbol"], "day": day.isoformat()})
                if len(result) >= limit:
                    return result
    return result


def _bootstrap_derived(db: sqlite3.Connection, root: Path) -> None:
    """One-time local replay of already downloaded ticks; no API or quota use."""
    for path in (root / "intraday/receipts").glob("*/*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            key = str(payload["dataset"])
            if not key.startswith("tw_tick:"):
                continue
            symbol = key.split(":", 1)[1]
            day = date.fromisoformat(payload["trade_date"])
            receipt = _stored_receipt(path, root, key, day)
            if receipt is not None:
                from scripts.finlab_volume_units import volume_reconciliation_due

                if (receipt.get("status") == "downloaded_unverified_for_pit"
                        and volume_reconciliation_due(receipt, symbol, day)):
                    receipt = reconcile_stored_tick_unit(root, key, day)
                _record_tick_and_minute(db, root, symbol, day, receipt)
        except (OSError, ValueError, KeyError, TypeError):
            continue


def _repair_derived_errors(db: sqlite3.Connection, root: Path, *, now: datetime) -> int:
    """Retry a bounded local derivation queue, not the already stored API data.

    Malformed partitions remain explicit errors and do not block the batch.
    An hourly local retry protects CPU; new source receipts are also repaired
    by the ordinary cached/new-partition path without that delay.
    """
    rows = db.execute("""SELECT symbol,trade_date FROM partitions
                         WHERE kind=? AND status='derivation_error' AND checked_at_utc < ?
                         AND NOT EXISTS (SELECT 1 FROM partitions direct
                             WHERE direct.kind='tw_minute' AND direct.symbol=partitions.symbol
                             AND direct.trade_date=partitions.trade_date AND direct.status IN
                             ('downloaded_unverified_for_pit','verified_closed_date','verified_no_trade'))
                         ORDER BY checked_at_utc LIMIT 8""",
                      (DERIVED_INDEX_KIND, (now-timedelta(hours=1)).isoformat())).fetchall()
    repaired = 0
    for symbol, day_text in rows:
        day = date.fromisoformat(day_text)
        key = f"tw_tick:{symbol}"
        receipt = _stored_receipt(_partition_paths(root, key, day)[0], root, key, day)
        if receipt is None:
            continue
        _record_tick_and_minute(db, root, symbol, day, receipt)
        current = db.execute("SELECT status FROM effective_partitions WHERE kind='tw_minute' AND symbol=? AND trade_date=?",
                             (symbol, day_text)).fetchone()
        repaired += bool(current and current[0] != "derivation_error")
    return repaired


def _candidate(flat: int, days: list[date], universe: list[dict],
               *, newest_first: bool, kind: str = "tw_tick") -> tuple[str, dict, date]:
    width = len(universe)
    day_number, remainder = divmod(flat, width)
    day = days[-1 - day_number] if newest_first else days[day_number]
    return kind, universe[remainder], day


def _former_probe_tasks(universe: list[dict], days: list[date], *, kind: str = "tw_tick") -> list[tuple[str, dict, date]]:
    """Check each former stock near its last locally observed trading day early."""
    candidates = []
    for item in universe:
        if item["current"]:
            continue
        final_text = item["last_local_daily_date"] or item["delisting_date"]
        if not final_text:
            continue
        position = bisect_right(days, date.fromisoformat(final_text)) - 1
        if position < 0:
            continue
        day = days[position]
        if _eligible(item, day):
            candidates.append((item, day))
    candidates.sort(key=lambda pair: (pair[1], pair[0]["symbol"]), reverse=True)
    return [(kind, item, day) for item, day in candidates]


def _coverage_context(db: sqlite3.Connection, universe: list[dict], days: list[date]) -> None:
    """Small immutable lifecycle/session dimensions, never a cached decision.

    Keep the ~symbols + sessions inputs in TEMP, not their Cartesian product
    or a duplicate source table. Actual partition counts are always read live.
    A SAVEPOINT preserves the caller's transaction and rollback semantics.
    """
    symbols = [(item['symbol'], item['first_candidate_date'],
                None if item['current'] else item['delisting_date'] or item['last_local_daily_date'] or '')
               for item in universe]
    sessions = [(day.isoformat(),) for day in days]
    identity = hashlib.sha256(json.dumps([symbols, sessions], separators=(',', ':')).encode()).hexdigest()
    db.execute('CREATE TEMP TABLE IF NOT EXISTS _finlab_coverage_identity(identity TEXT)')
    db.execute('CREATE TEMP TABLE IF NOT EXISTS _finlab_coverage_symbols('
               'symbol TEXT PRIMARY KEY,first TEXT,last TEXT) WITHOUT ROWID')
    db.execute('CREATE TEMP TABLE IF NOT EXISTS _finlab_coverage_sessions(day TEXT PRIMARY KEY) WITHOUT ROWID')
    if db.execute('SELECT identity FROM _finlab_coverage_identity').fetchone() == (identity,):
        return
    db.execute('SAVEPOINT finlab_coverage_context')
    try:
        db.execute('DELETE FROM _finlab_coverage_identity')
        db.execute('DELETE FROM _finlab_coverage_symbols')
        db.execute('DELETE FROM _finlab_coverage_sessions')
        db.executemany('INSERT INTO _finlab_coverage_symbols VALUES (?,?,?)', symbols)
        db.executemany('INSERT INTO _finlab_coverage_sessions VALUES (?)', sessions)
        db.execute('INSERT INTO _finlab_coverage_identity VALUES (?)', (identity,))
        db.execute('RELEASE finlab_coverage_context')
    except BaseException:
        db.execute('ROLLBACK TO finlab_coverage_context')
        db.execute('RELEASE finlab_coverage_context')
        raise


def _coverage_phases(db: sqlite3.Connection, universe: list[dict], days: list[date],
                     *, first_observed: str | None, kind: str = "tw_tick") -> list[dict]:
    """Disjoint candidate windows; published partitions are an unknown subset.

    Cost is O(indexed partitions + symbols), not an 8-million-row Cartesian
    product. Only the acquisition owner reads the index, once per summary.
    The public reader consumes this small receipt, never opens the writer DB.
    """
    latest = days[-1]
    earlier = days[:-1]
    first = date.fromisoformat(first_observed) if first_observed else latest
    ranges = [
        ("latest_session", "最新交易日候選回補", [latest]),
        ("observed_history", "已觀測起點以後的歷史回補", [d for d in earlier if d >= first]),
        ("older_discovery", "更早歷史可得性探索", [d for d in earlier if d < first]),
    ]
    accepted = ACCEPTED | {"derived_unverified_for_pit", "derived_no_regular_trades"}
    _coverage_context(db, universe, days)
    counts = {name: [0, 0, 0, 0, 0] for name, _, _ in ranges}
    # Filter official sessions/lifecycles and aggregate inside SQLite. The old
    # Python path decoded and parsed every partition, three times per summary.
    # Do not turn a calendar candidate into an assertion of published supply.
    for phase, status, number, rows, size in db.execute(
        "SELECT CASE WHEN p.trade_date=? THEN 'latest_session' "
        "WHEN p.trade_date>=? THEN 'observed_history' ELSE 'older_discovery' END AS phase,"
        "p.status,COUNT(*),SUM(p.rows),SUM(p.parquet_bytes) FROM effective_partitions AS p "
        "JOIN _finlab_coverage_symbols AS u ON u.symbol=p.symbol "
        "JOIN _finlab_coverage_sessions AS s ON s.day=p.trade_date "
        "WHERE p.kind=? AND p.trade_date>=u.first AND (u.last IS NULL OR p.trade_date<=u.last) "
        "GROUP BY phase,p.status", (latest.isoformat(), first.isoformat(), kind),
    ):
        values = counts[phase]
        if status in accepted:
            values[0] += number
            values[3] += rows
            values[4] += size
        elif status == 'partition_not_ready':
            values[1] += number
        else:
            values[2] += number
    result = []
    for identity, label, window in ranges:
        target = sum(_target_count(item, window) for item in universe)
        got, not_ready, failed, rows, size = counts[identity]
        tested = got + not_ready + failed
        result.append({
            "id": identity, "label": label,
            "start_date": window[0].isoformat() if window else None,
            "end_date": window[-1].isoformat() if window else None,
            "candidate_partitions": target, "receipted_partitions": got,
            "provider_not_ready_partitions": not_ready, "other_failed_partitions": failed,
            "untested_candidate_partitions": max(0, target - tested),
            "rows": rows, "local_parquet_bytes": size,
            "candidate_coverage_ratio": got / target if target else None,
            "candidate_check_ratio": tested / target if target else None,
            "state": ("candidate_window_acquired" if target and got == target else
                      "not_required" if not target else "source_range_unverified"),
            "remaining_transfer_bytes_estimate": None, "finish_at_utc": None,
            "reason": "候選範圍不是官方已發布分母；來源未上架保留等待，不能保證整個日期窗的資料完成日。",
        })
    return result


def _minute_priority(db: sqlite3.Connection, universe: list[dict], days: list[date],
                     *, first_observed: str | None, phases: list[dict],
                     source_root: Path | None, quota_policy: QuotaPolicy | None) -> dict:
    """Shared minute-before-tick decision; never changes publication/retry clocks.

    The full dashboard already owns its minute phases. Admission supplies the
    same phases without building tick statistics and thousands of display rows.
    No cached decision survives a transaction or the existing admission boundary.
    """
    minute_pending = sum(row["untested_candidate_partitions"] + row["other_failed_partitions"]
                         for row in phases if row["id"] != "older_discovery")
    observed = datetime.now(UTC)
    blocked_failures = 0
    failure_wakes = []
    if first_observed and source_root is not None:
        known = {item["symbol"]: item for item in universe}
        session_set = set(days)
        for symbol, text, status in db.execute("SELECT symbol,trade_date,status FROM effective_partitions "
                "WHERE kind='tw_minute' AND trade_date>=? AND status NOT IN "
                "('downloaded_unverified_for_pit','derived_unverified_for_pit','derived_no_regular_trades',"
                "'verified_closed_date','verified_no_trade','partition_not_ready')", (first_observed,)):
            day = date.fromisoformat(text)
            item = known.get(symbol)
            if item is None or day not in session_set or not _eligible(item, day):
                continue
            path = _partition_paths(source_root, f"tw_minute:{symbol}", day)[1]
            if not _checked_this_cycle(path, "attempted_at_utc", observed):
                continue
            blocked_failures += 1
            attempt = read_json(path)
            wake = quota_cycle_start(observed) + timedelta(days=1)
            if not attempt.get("retry_exhausted"):
                try:
                    explicit = datetime.fromisoformat(attempt["retry_at_utc"])
                    if explicit.tzinfo is not None and observed < explicit:
                        wake = min(wake, explicit)
                except (ValueError, KeyError, TypeError):
                    pass
            failure_wakes.append(wake)
    publication_policy = quota_policy or load_policy()
    publication_delay = publication_recheck_delay(now=observed, policy=publication_policy)
    minute_due = db.execute("SELECT COUNT(*) FROM effective_partitions WHERE kind='tw_minute' "
        "AND status='partition_not_ready' AND trade_date>=? AND checked_at_utc<?",
        ((days[-1]-timedelta(days=30)).isoformat(),
         (observed-publication_delay).isoformat())).fetchone()[0]
    next_check = db.execute("SELECT MIN(checked_at_utc) FROM effective_partitions WHERE kind='tw_minute' "
        "AND status='partition_not_ready' AND trade_date>=?",
        ((days[-1]-timedelta(days=30)).isoformat(),)).fetchone()[0]
    next_check_at = min(datetime.fromisoformat(next_check)+publication_delay,
                        quota_cycle_start(observed)+timedelta(days=1)) if next_check else quota_cycle_start(observed)+timedelta(days=1)
    drain_at = quota_cycle_start(observed)+timedelta(days=1, minutes=-publication_policy.drain_before_reset_minutes)
    if next_check and publication_policy.drain_before_reset_minutes and observed < drain_at:
        next_check_at = min(next_check_at, drain_at)
    next_check_at = min([next_check_at, *failure_wakes])
    actionable_pending = max(0, minute_pending - blocked_failures)
    return {"pending_known_window_partitions": minute_pending,
        "actionable_known_window_partitions": actionable_pending,
        "blocked_known_window_partitions": blocked_failures,
        "due_publication_rechecks": minute_due,
        "publication_recheck_seconds": publication_delay.total_seconds(),
        "tick_residual_allowed": actionable_pending == 0 and minute_due == 0,
        "next_source_check_at_utc": next_check_at.isoformat() if actionable_pending == 0 and minute_due == 0 else None,
        "basis": "available minute history first; unpublished and older unverified supply stay incomplete",
        "older_discovery_is_bounded": True}


def _minute_admission(db: sqlite3.Connection, universe: list[dict], days: list[date],
                      *, source_root: Path | None, quota_policy: QuotaPolicy | None) -> dict:
    """Compute only the live minute-priority inputs, not a complete inventory.

    Match dashboard bounds exactly: known symbols and the selected date horizon,
    accepted data only, prefer minute history then tick history. Lifecycle/session
    eligibility remains in _coverage_phases, just as in the full inventory.
    """
    known = {item["symbol"] for item in universe}
    first_by_kind = {}
    for kind, symbol, first in db.execute(
            "SELECT kind,symbol,MIN(trade_date) FROM effective_partitions "
            "WHERE status IN ('downloaded_unverified_for_pit','derived_unverified_for_pit') "
            "AND trade_date BETWEEN ? AND ? GROUP BY kind,symbol",
            (days[0].isoformat(), days[-1].isoformat())):
        if symbol in known:
            first_by_kind[kind] = min(first_by_kind.get(kind, first), first)
    first = first_by_kind.get("tw_minute") or first_by_kind.get("tw_tick")
    phases = _coverage_phases(db, universe, days, first_observed=first, kind="tw_minute")
    return _minute_priority(db, universe, days, first_observed=first, phases=phases,
                            source_root=source_root, quota_policy=quota_policy)


def _summarize(db: sqlite3.Connection, universe: list[dict], days: list[date],
               *, started: datetime, attempts: int, successes: int,
               state: str, daily_price_coverage: dict | None = None,
               active_kind: str = "tw_tick", source_root: Path | None = None,
               quota_policy: QuotaPolicy | None = None) -> dict:
    counts: dict[tuple[str, str], dict] = {}
    for kind, symbol, status, partitions, rows, size in db.execute(
        """SELECT kind,symbol,status,COUNT(*),SUM(rows),SUM(parquet_bytes)
           FROM effective_partitions WHERE trade_date BETWEEN ? AND ? GROUP BY kind,symbol,status""",
        (days[0].isoformat(), days[-1].isoformat()),
    ):
        item = counts.setdefault((kind, symbol), {
            "receipted_partitions": 0, "provider_not_ready_partitions": 0,
            "other_failed_partitions": 0, "rows": 0, "parquet_bytes": 0,
        })
        if status in {"downloaded_unverified_for_pit", "derived_unverified_for_pit",
                      "derived_no_regular_trades", "verified_closed_date", "verified_no_trade"}:
            item["receipted_partitions"] += partitions
            item["rows"] += rows or 0
            item["parquet_bytes"] += size or 0
        elif status == "partition_not_ready":
            item["provider_not_ready_partitions"] += partitions
        else:
            item["other_failed_partitions"] += partitions
    data_bounds = {
        (kind, symbol): (first, last)
        for kind, symbol, first, last in db.execute(
            """SELECT kind,symbol,MIN(trade_date),MAX(trade_date) FROM effective_partitions
               WHERE status IN ('downloaded_unverified_for_pit','derived_unverified_for_pit')
               AND trade_date BETWEEN ? AND ? GROUP BY kind,symbol""",
            (days[0].isoformat(), days[-1].isoformat()),
        )
    }
    by_kind = {}
    symbols = []
    for kind in DISPLAY_KINDS:
        indexed_kind = kind
        total = received = rows = size = not_ready = failed = 0
        for item in universe:
            symbol = item["symbol"]
            target = _target_count(item, days)
            facts = counts.get((indexed_kind, symbol), {})
            got = int(facts.get("receipted_partitions", 0))
            total += target
            received += got
            rows += int(facts.get("rows", 0))
            size += int(facts.get("parquet_bytes", 0))
            source_not_ready = facts.get("provider_not_ready_partitions", 0)
            not_ready += int(source_not_ready)
            failed += int(facts.get("other_failed_partitions", 0))
            symbols.append({
                "key": f"{kind}:{symbol}", "market": item["market"],
                "security_type": item["security_type"],
                "current": item["current"], "delisting_date": item["delisting_date"],
                "first_local_daily_date": item["first_local_daily_date"],
                "last_local_daily_date": item["last_local_daily_date"],
                "target_partitions": target, "receipted_partitions": got,
                "provider_not_ready_partitions": source_not_ready,
                "other_failed_partitions": facts.get("other_failed_partitions", 0),
                "rows": facts.get("rows", 0), "parquet_bytes": facts.get("parquet_bytes", 0),
                "finlab_daily_close_rows": item.get("finlab_daily_close_rows"),
                "first_data_date": data_bounds.get((indexed_kind, symbol), (None, None))[0],
                "last_data_date": data_bounds.get((indexed_kind, symbol), (None, None))[1],
                "source_kind": "finlab_api_or_local_tick_derivation" if kind == "tw_minute" else "finlab_api",
            })
        by_kind[kind] = {
            "source_kind": "finlab_api_or_local_tick_derivation" if kind == "tw_minute" else "finlab_api",
            "symbol_count": len(universe), "target_partitions": total,
            "receipted_partitions": received, "remaining_partitions": max(0, total - received),
            "provider_not_ready_partitions": not_ready,
            "other_failed_partitions": failed,
            "rows": rows, "parquet_bytes": size,
            "first_data_date": min((row["first_data_date"] for row in symbols
                                    if row["key"].startswith(kind + ":")
                                    and row["first_data_date"]), default=None),
            "last_data_date": max((row["last_data_date"] for row in symbols
                                   if row["key"].startswith(kind + ":")
                                   and row["last_data_date"]), default=None),
        }
        origins = dict(db.execute("SELECT source_kind,COUNT(*) FROM effective_partitions "
            "WHERE kind=? AND trade_date BETWEEN ? AND ? AND status IN "
            "('downloaded_unverified_for_pit','derived_unverified_for_pit',"
            "'derived_no_regular_trades','verified_closed_date','verified_no_trade') GROUP BY source_kind",
            (kind, days[0].isoformat(), days[-1].isoformat())))
        by_kind[kind]["direct_api_partitions"] = origins.get("finlab_api", 0)
        by_kind[kind]["derived_partitions"] = origins.get("derived_from_tw_tick", 0)
        for origin, prefix in (("finlab_api", "direct_api"), ("derived_from_tw_tick", "derived")):
            bounds = db.execute("SELECT MIN(trade_date),MAX(trade_date) FROM effective_partitions "
                "WHERE kind=? AND source_kind=? AND trade_date BETWEEN ? AND ? "
                "AND status IN ('downloaded_unverified_for_pit','derived_unverified_for_pit')",
                (kind, origin, days[0].isoformat(), days[-1].isoformat())).fetchone()
            by_kind[kind][prefix + "_first_data_date"] = bounds[0]
            by_kind[kind][prefix + "_last_data_date"] = bounds[1]
        if kind == "tw_minute" and not origins.get("finlab_api"):
            by_kind[kind]["source_kind"] = "derived_from_tw_tick"
    minute_first = by_kind["tw_minute"]["first_data_date"] or by_kind["tw_tick"]["first_data_date"]
    minute_phases = _coverage_phases(db, universe, days, first_observed=minute_first, kind="tw_minute")
    minute_priority = _minute_priority(db, universe, days, first_observed=minute_first,
        phases=minute_phases, source_root=source_root, quota_policy=quota_policy)
    return {
        "schema_version": 4, "observed_at_utc": datetime.now(UTC).isoformat(),
        "progress_contract_version": 1,
        "acquisition_mode": "minute_first_tick_residual",
        "acquisition_policy_version": ACQUISITION_POLICY_VERSION, "active_kind": active_kind,
        "minute_priority": minute_priority,
        "legacy_direct_minute_partitions": db.execute(
            "SELECT COUNT(*) FROM partitions WHERE kind='tw_minute' AND status IN "
            "('downloaded_unverified_for_pit','verified_closed_date','verified_no_trade')"
        ).fetchone()[0],
        "run_started_at_utc": started.isoformat(),
        "target_start_date": days[0].isoformat() if days else None,
        "target_end_date": days[-1].isoformat() if days else None,
        "session_basis": "observed TWSE index dates since 2009 and official TWSE/TPEx daily rows in 2003-2008; unrecorded sessions unverified",
        "universe_basis": "official current stock manifest plus official TWSE/TPEx delisting lists; local daily bounds are candidate lifecycle, not FinLab availability",
        "universe_symbols": len(universe),
        "ordinary_stock_symbols": sum(item["security_type"] == "stock" for item in universe),
        "tdr_symbols": sum(item["security_type"] == "tdr" for item in universe),
        "current_symbols": sum(bool(item["current"]) for item in universe),
        "former_symbols": sum(not item["current"] for item in universe),
        "missing_daily_bounds": sum(item["first_local_daily_date"] is None for item in universe),
        "pre_horizon_former_symbols": sum(
            not item["current"] and _target_count(item, days) == 0
            for item in universe
        ),
        "by_kind": by_kind, "symbols": symbols,
        "coverage_phases": _coverage_phases(
            db, universe, days, first_observed=by_kind["tw_tick"]["first_data_date"]),
        "minute_coverage_phases": minute_phases,
        "daily_price_coverage": daily_price_coverage or {"state": "not_checked"},
        "attempted_this_run": attempts, "successes_this_run": successes,
        "state": state, "completion_eta": None,
        "completion_eta_reason": "FinLab partition availability, per-request bytes and future shared account quota are unknown",
    }


def sync_market(root: Path, public_root: Path, *, start: date, end: date,
                limit: int, reserve_mb: float | None, minimum_free_gb: float,
                now: datetime, fetch=fetch_partition, max_run_seconds: float | None = None,
                kind: str = "tw_tick", enforce_minute_priority: bool = False,
                quota_policy: QuotaPolicy | None = None, workers: int = 1,
                range_calendar_days: int = 1, range_fetch=fetch_partitions,
                download_threads: int | None = None, queue_depth: int | None = None,
                authorization_slots: int | None = None) -> dict:
    if kind not in KINDS:
        raise ValueError("unsupported FinLab intraday family")
    if (type(workers) is not int or not 1 <= workers <= 32
            or type(range_calendar_days) is not int or not 1 <= range_calendar_days <= 31):
        raise ValueError("invalid bounded FinLab pipeline")
    if kind == "tw_tick" and workers > 8:
        raise ValueError("Tick uses at most eight bounded processes")
    source_kind = kind
    quota_policy = quota_policy or load_policy()
    download_threads = (quota_policy.minute_download_threads if kind == "tw_minute" else 1
                        ) if download_threads is None else download_threads
    # Explicit serial library callers (including custom per-day fetch hooks)
    # must not silently inherit a deployed process queue and invoke real SDK
    # range downloads. CLI production supplies its configured worker/range.
    queue_depth = ((quota_policy.minute_queue_depth if kind == "tw_minute" else quota_policy.tick_queue_depth)
                   if workers > 1 or range_calendar_days > 1 or download_threads > 1 else 1
                   ) if queue_depth is None else queue_depth
    authorization_slots = (quota_policy.minute_authorization_slots if kind == "tw_minute" else quota_policy.tick_authorization_slots
                           ) if authorization_slots is None else authorization_slots
    if type(download_threads) is not int or not 1 <= download_threads <= 5 or kind != "tw_minute" and download_threads != 1:
        raise ValueError("invalid bounded signed-object parallelism")
    if type(queue_depth) is not int or not 1 <= queue_depth <= 4:
        raise ValueError("invalid bounded FinLab work queue")
    if type(authorization_slots) is not int or not 1 <= authorization_slots <= (32 if kind == "tw_minute" else 8):
        raise ValueError("invalid bounded FinLab authorization concurrency")
    if kind == "tw_minute":
        # CLI experiments have an exact effective fingerprint, not the deployed
        # policy's fingerprint with silently different worker/thread settings.
        quota_policy = replace(quota_policy, minute_workers=workers,
            minute_range_calendar_days=range_calendar_days, minute_download_threads=download_threads,
            minute_queue_depth=queue_depth, minute_authorization_slots=authorization_slots)
    else:
        quota_policy = replace(quota_policy, tick_workers=workers, tick_range_calendar_days=range_calendar_days,
            tick_queue_depth=queue_depth, tick_authorization_slots=authorization_slots)
    http_keepalive = quota_policy.minute_http_keepalive if kind == "tw_minute" else quota_policy.tick_http_keepalive
    authorization_slots = min(workers, authorization_slots)
    pipeline = workers > 1 or range_calendar_days > 1 or download_threads > 1 or queue_depth > 1
    engine = ("rolling_range_process_pool" if pipeline and queue_depth > 1 else
              "bounded_range_process_pool" if pipeline else "single_stock_day")
    quota_window = QuotaSampleWindow(quota_policy, kind=source_kind)
    policy_state = quota_admission(kind=kind, now=now, reserve_mb=reserve_mb, policy=quota_policy)
    full_started_clock = time.monotonic()
    full_started_at = datetime.now(UTC)
    if not credential_available():
        raise RuntimeError("no usable FinLab session")
    universe = load_universe(public_root, start=start)
    daily_price_coverage = annotate_daily_close_coverage(root, universe)
    days = session_days(public_root, start=start, end=end)
    if not days or not universe:
        raise ValueError("no verified stock universe or observed session days")
    index = _open_index(root / "intraday/market_index.sqlite3")
    index_view_identity = _effective_partitions_view_identity(index)
    cursor_path = root / ("intraday/minute_cursor.json" if kind == "tw_minute" else "intraday/market_cursor.json")
    try:
        cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        cursor = {}
    width = len(universe)
    universe_hash = hashlib.sha256("\n".join(
        f"{item['symbol']}:{item['current']}:{item['delisting_date']}:{item['first_candidate_date']}"
        for item in universe
    ).encode("utf-8")).hexdigest()
    base = [days[0].isoformat(), len(universe), universe_hash,
            "minute_first_v1" if kind == "tw_minute" else "tick_only_v1"]
    old_count = cursor.get("day_count")
    old_last = cursor.get("last_day")
    if (cursor.get("base") == base and isinstance(old_count, int)
            and 0 < old_count <= len(days)
            and days[old_count - 1].isoformat() == old_last):
        added = days[old_count:]
        if added:
            # Existing newest/oldest positions retain their historical day;
            # new sessions get their own durable queue rather than resetting
            # all progress at each open.
            cursor["newest"] = int(cursor["newest"]) + len(added) * width
            cursor["recent_queue"] = [
                {"day": day.isoformat(), "next": 0} for day in reversed(added)
            ] + list(cursor.get("recent_queue", []))
    else:
        cursor = {"base": base, "newest": width, "oldest": 0,
                  "former": 0,
                  "recent_queue": [{"day": days[-1].isoformat(), "next": 0}],
                  "bootstrapped": cursor.get("bootstrapped") is True,
                  # A universe/range frontier change does not invalidate the
                  # same index's completed one-time local Tick replay. Newly
                  # acquired Tick partitions already record their minutes via
                  # _record_tick_and_minute. Losing this independent flag made
                  # every universe change replay the whole history, often
                  # timing out before the new frontier could be checkpointed.
                  "derived_bootstrapped": cursor.get("derived_bootstrapped") is True}
    cursor["day_count"] = len(days)
    cursor["last_day"] = days[-1].isoformat()
    cursor.setdefault("former", 0)
    cycle = quota_cycle_start(now).isoformat()
    if cursor.get("older_probe_cycle") != cycle:
        cursor.update(older_probe_cycle=cycle, older_probe_attempts=0, older_retry_attempts=0)
    if not cursor.get("bootstrapped"):
        for path in (root / "intraday/receipts").glob("*/*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                key = str(payload["dataset"])
                kind, symbol = key.split(":", 1)
                day = date.fromisoformat(payload["trade_date"])
            except (OSError, ValueError, KeyError, TypeError):
                continue
            if kind != source_kind or _stored_receipt(path, root, key, day) is None:
                continue
            _record(index, kind, symbol, day, payload["status"],
                    int(payload.get("rows") or 0),
                    int(payload.get("parquet_size_bytes") or 0))
        cursor["bootstrapped"] = True
    if not cursor.get("derived_bootstrapped") and source_kind == "tw_tick":
        _bootstrap_derived(index, root)
        cursor["derived_bootstrapped"] = True
    _repair_derived_errors(index, root, now=now)
    slots = len(days) * width
    former_tasks = _former_probe_tasks(universe, days, kind=source_kind)
    started = datetime.now(UTC)
    attempts = successes = 0
    state = "batch_limit"
    scanned = 0
    general_admission: dict = {}
    general_recheck_at = datetime.min.replace(tzinfo=UTC)
    minute_admission_checks = 0
    minute_admission_observation = None
    started_clock = time.monotonic()
    deadline = started_clock + max_run_seconds if max_run_seconds is not None else None
    active_key = active_day = None
    last_success_at = None
    durations: list[float] = []
    last_completed_clock = started_clock
    quota_checks = quota_cache_hits = cached_skips = 0
    last_status_clock = started_clock
    owner = process_owner()
    first_observed = index.execute(
        "SELECT MIN(trade_date) FROM effective_partitions WHERE kind=? "
        "AND status IN ('downloaded_unverified_for_pit','derived_unverified_for_pit')", (source_kind,),
    ).fetchone()[0]
    # A source/range interruption can leave untouched days behind a committed
    # scan frontier. Repair those finite holes explicitly rather than looping
    # on publication retries while the reported required gap never decreases.
    if source_kind == "tw_minute":
        cursor["known_gap_queue"] = _unchecked_known_minutes(index, universe, days,
            first_observed=first_observed, limit=limit)
    phase_metrics = {key: {"attempts": 0, "newly_checked": 0, "new_receipted": 0,
                           "rows": 0, "local_bytes": 0, "cycle_seconds": 0.0}
                     for key in ("latest_session", "observed_history", "older_discovery")}
    account_delta_bytes = 0.0
    quota_sample_successes = 0
    previous_quota = None
    quota_pending_successes = 0
    older_paused = False
    pool = None
    rate_shared = multiprocessing.get_context("spawn").Array("d", [0, 0]) if pipeline else None
    authorization_gate = multiprocessing.get_context("spawn").BoundedSemaphore(authorization_slots) if pipeline else None
    http_metrics = {key: 0.0 if key.endswith("seconds") else 0 for key in HTTP_COUNTERS}
    http_peak_inflight = 0
    queued_windows = inflight_windows = peak_inflight_windows = 0
    worker_slot_seconds = 0.0
    source_backoff_until = read_json(root / "intraday/source_backoff.json").get("retry_at_utc")
    sdk_calls = fallback_calls = window_requests = 0
    source_seconds = persist_seconds = 0.0
    inflight_partitions = 0
    _atomic_json(cursor_path, cursor)

    def persist_source_backoff():
        """Retain shared provider cooldown even when a worker/owner is killed."""
        nonlocal source_backoff_until, state
        if rate_shared is None:
            return
        with rate_shared.get_lock():
            retry_at, events = rate_shared[:]
        if not retry_at:
            return
        source_backoff_until = datetime.fromtimestamp(retry_at, UTC).isoformat()
        _atomic_json(root / "intraday/source_backoff.json", {
            "contract_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
            "retry_at_utc": source_backoff_until, "rate_limit_events": int(events),
            "basis": "shared_HTTP_429_or_503_Retry_After; no_response_text_retained",
        })
        if state == "batch_limit":
            state = "source_rate_limited"

    def checkpoint(*, full=False, finished=False):
        """Persist only committed frontiers; in-flight reads resume after a kill."""
        nonlocal last_status_clock
        boundary = datetime.now(UTC)
        if active_key is None:
            _atomic_json(cursor_path, cursor)
        measured = estimate_attempt_batch(
            durations, limit=limit,
            since_completion_seconds=time.monotonic() - last_completed_clock,
        )
        if pipeline and durations:
            per_partition = (time.monotonic() - started_clock) / len(durations)
            measured["seconds_per_attempt_estimate"] = per_partition
            measured["remaining_batch_seconds_estimate"] = max(0, limit - attempts) * per_partition
            measured["eta_measurement_basis"] = "current_pool_owner_wall_per_completed_stock_day"
        batch_seconds = measured["remaining_batch_seconds_estimate"]
        progress = {
            "contract_version": INTRADAY_PROGRESS_VERSION, "owner": owner,
            "kind": source_kind, "acquisition_policy_version": ACQUISITION_POLICY_VERSION,
            "quota_policy": policy_state,
            "run_started_at_utc": started.isoformat(), "observed_at_utc": boundary.isoformat(),
            "state": "running" if state == "batch_limit" and not finished else state,
            "active_key": active_key, "active_trade_date": active_day,
            "attempt_limit": limit, "attempted": attempts, "successful": successes,
            "scanned_candidates": scanned, "cached_skips": cached_skips,
            "quota_checks": quota_checks, "quota_cache_hits": quota_cache_hits,
            "quota_observation_bounds": {"maximum_requests": quota_window.maximum_requests,
                "maximum_age_seconds": quota_window.interval_seconds,
                "per_request_within_reserve_mb": quota_window.near_floor_mb},
            "processing_engine": engine,
            "partition_index_view": index_view_identity,
            "minute_priority_admission": minute_admission_observation,
            "data_processing_engine": "polars_arrow_v1", "processing_threads": quota_policy.processing_threads,
            "pipeline_workers": workers, "range_calendar_days": range_calendar_days,
            "download_threads": download_threads, "queue_depth": queue_depth, "authorization_slots": authorization_slots,
            "http_keepalive": http_keepalive if pipeline else False,
            "sdk_calls": sdk_calls if pipeline else attempts,
            "window_requests": window_requests, "fallback_calls": fallback_calls,
            "inflight_partitions": inflight_partitions,
            "queued_windows": queued_windows, "inflight_windows": inflight_windows,
            "peak_inflight_windows": peak_inflight_windows,
            **http_metrics, "http_peak_inflight_per_worker": http_peak_inflight,
            "source_backoff_until_utc": source_backoff_until,
            "elapsed_seconds": round(time.monotonic() - started_clock, 3),
            **measured,
            "last_success_at_utc": last_success_at,
            "estimated_batch_finish_at_utc": (boundary + timedelta(seconds=batch_seconds)).isoformat()
                if batch_seconds is not None and len(durations) < limit
                and state == "batch_limit" and not finished else None,
            "stop_by_at_utc": (started + timedelta(seconds=max_run_seconds)).isoformat()
                if max_run_seconds is not None else None,
            "estimate_scope": "current_attempt_batch_not_all_history",
        }
        _atomic_json(root / "intraday/active_run.json", progress)
        if full or time.monotonic() - last_status_clock >= 60:
            summary = _summarize(index, universe, days, started=started,
                                 attempts=attempts, successes=successes, state=progress["state"],
                                 daily_price_coverage=daily_price_coverage, active_kind=source_kind,
                                 source_root=root, quota_policy=quota_policy)
            summary["general_acquisition_admission"] = general_admission
            summary["quota_policy"] = policy_state
            _atomic_json(root / "intraday/market_status.json", summary)
            last_status_clock = time.monotonic()

    def may_acquire() -> bool:
        nonlocal general_admission, general_recheck_at, state, quota_checks, quota_cache_hits, policy_state
        nonlocal previous_quota, quota_pending_successes, account_delta_bytes, quota_sample_successes
        nonlocal minute_admission_checks, minute_admission_observation
        # Leave a complete SDK request's timeout runway before the outer shell
        # watchdog. Stop normally so source/index/frontiers stay recoverable.
        if deadline is not None and time.monotonic() + 180 >= deadline:
            state = "run_time_budget_reached"
            return False
        boundary = datetime.now(UTC)
        if source_backoff_until:
            try:
                retry_at = datetime.fromisoformat(source_backoff_until)
                if retry_at.tzinfo is not None and boundary < retry_at:
                    state = "source_rate_limited"
                    return False
            except (ValueError, TypeError):
                raise ValueError("invalid FinLab source cooldown receipt") from None
        # A drain pass can straddle 08:00. Admission only at service startup
        # cannot protect 08:20 when a long-lived owner is already running.
        # Leave the full SDK retry/timeout runway; verified holidays keep going.
        local = boundary.astimezone(TAIPEI)
        protection_start = (datetime.combine(local.date(), WINDOW_START) - timedelta(minutes=10)).time()
        if protection_start <= local.time().replace(tzinfo=None) < WINDOW_END:
            if not opening_admission(boundary, minimum_runway_minutes=10,
                                     official_calendar_root=public_root)["allowed"]:
                state = "opening_window_yield"
                return False
        if source_kind == "tw_tick" and enforce_minute_priority and boundary >= general_recheck_at:
            admission_clock = time.monotonic()
            admission = _minute_admission(index, universe, days, source_root=root,
                                          quota_policy=quota_policy)
            minute_admission_checks += 1
            minute_admission_observation = {"contract_version": 1,
                "engine": "minute_priority_only_v1", "checks": minute_admission_checks,
                "checked_at_utc": datetime.now(UTC).isoformat(),
                "wall_seconds": time.monotonic() - admission_clock,
                "complete_inventory_generated": False, "decision": admission}
            if not admission["tick_residual_allowed"]:
                state = "waiting_required_minutes"
                return False
        # No other regular writer can change receipts under the account lock.
        # Recheck at most once a minute, and always at the daily reset, instead
        # of rereading the thousand-key catalog for every Tick partition.
        if boundary >= general_recheck_at:
            general_admission = _general_work_admission(root, now=boundary)
            general_recheck_at = min(boundary + timedelta(minutes=1),
                                    quota_cycle_start(boundary) + timedelta(days=1))
        allowed = (general_admission.get("minute_residual_allowed", general_admission.get("supplemental_allowed", False))
                   if source_kind == "tw_minute" else general_admission.get("supplemental_allowed", False))
        if not allowed:
            state = "waiting_required_general_data"
            return False
        policy_state = quota_admission(kind=source_kind, now=boundary, reserve_mb=reserve_mb,
            scheduled_reserve_mb=general_admission.get("scheduled_reserve_mb", 0), policy=quota_policy)
        effective_reserve = policy_state["effective_reserve_mb"]
        # The SDK still enforces official quota on every source request.
        # Reuse local observations for at most 4 Tick requests / 5 seconds;
        # within 100 MB of the reserve Tick returns to per-request sampling.
        fresh = quota_window.needs_refresh(
            now=boundary, clock=time.monotonic(), reserve_mb=effective_reserve)
        if fresh:
            quota_checks += 1
            room = quota_window.observe(quota_room_mb(), now=boundary,
                clock=time.monotonic(), reserve_mb=effective_reserve)
        else:
            quota_cache_hits += 1
            room = quota_window.sample
        if room is None:
            state = "quota_unknown"
            return False
        # Reuse existing quota checks; monitoring must never add an SDK call.
        # This samples the preceding completed query, not this in-flight one.
        if (fresh and previous_quota is not None and quota_pending_successes > 0
                and previous_quota[1] == room[1] and previous_quota[0] >= room[0]
                and quota_cycle_start(previous_quota[2]) == quota_cycle_start(boundary)):
            account_delta_bytes += (previous_quota[0] - room[0]) * 1024**2
            quota_sample_successes += quota_pending_successes
        if fresh:
            previous_quota = (room[0], room[1], boundary)
            quota_pending_successes = 0
        if room[0] <= effective_reserve:
            state = "quota_margin_reached"
            return False
        if shutil.disk_usage(root).free < minimum_free_gb * 1024**3:
            state = "disk_reserve_reached"
            return False
        return True

    def acquire(kind: str, symbol: str, day: date, *, retry=False):
        nonlocal attempts, successes, active_key, active_day, state, last_success_at, last_completed_clock
        nonlocal first_observed, quota_pending_successes, sdk_calls
        old = index.execute("SELECT status FROM partitions WHERE kind=? AND symbol=? AND trade_date=?",
                            (kind, symbol, day.isoformat())).fetchone()
        identity = phase_for_day(day.isoformat(), latest=days[-1].isoformat(), first_observed=first_observed)
        if identity == "older_discovery":
            cursor["older_probe_attempts"] = int(cursor.get("older_probe_attempts", 0)) + 1
            if retry:
                cursor["older_retry_attempts"] = int(cursor.get("older_retry_attempts", 0)) + 1
        phase = phase_metrics[identity]
        phase["attempts"] += 1
        phase["newly_checked"] += int(old is None)
        quota_window.requested()
        if pipeline:
            sdk_calls += 1  # Bounded single-day rechecks are part of real calls.
        active_key, active_day = f"{kind}:{symbol}", day.isoformat()
        attempts += 1
        checkpoint()
        try:
            receipt = fetch(root, active_key, day, now=datetime.now(UTC))
            _record_source(index, root, kind, symbol, day, receipt)
            successes += 1
            quota_pending_successes += 1
            if old is None or old[0] not in ACCEPTED:
                phase["new_receipted"] += 1
                phase["rows"] += int(receipt.get("rows") or 0)
                phase["local_bytes"] += int(receipt.get("parquet_size_bytes") or 0)
            if receipt.get("status") == "downloaded_unverified_for_pit":
                first_observed = min(first_observed, day.isoformat()) if first_observed else day.isoformat()
            last_success_at = datetime.now(UTC).isoformat()
            print(f"[finlab-market] {'retry ' if retry else ''}{active_key} {day}: {receipt['status']}", flush=True)
        except Exception as exc:
            from scripts.download_finlab_intraday import record_failed_partition
            failure = record_failed_partition(root, active_key, day, exc)
            _record(index, kind, symbol, day, failure)
            print(f"[finlab-market] {'retry ' if retry else ''}{active_key} {day}: {failure}", flush=True)
            if failure in {"quota_exhausted", "authentication_failed", "source_rate_limited"}:
                state = failure
        # A BaseException (SIGTERM/system exit) deliberately leaves active_key
        # and the previous committed cursor intact. The next owner reconciles
        # any saved receipt before trying this same candidate again.
        completed_clock = time.monotonic()
        durations.append(completed_clock - last_completed_clock)
        phase["cycle_seconds"] += durations[-1]
        last_completed_clock = completed_clock
        active_key = active_day = None
        checkpoint()
        if attempts % 64 == 0:
            gc.collect()  # Normal GC remains enabled; no full-heap scan per stock-day.

    def acquire_wave(jobs):
        """A bounded work-conserving pool; the cohort frontier commits last.

        Refill free slots as their receipts become durable, not after the slowest
        stock finishes a wave. Preempted/undispatched work keeps the pre-cohort
        frontier; immutable completed receipts reconcile on the next pass.
        """
        nonlocal pool, attempts, successes, active_key, active_day, state, last_success_at
        nonlocal first_observed, quota_pending_successes, last_completed_clock, inflight_partitions
        nonlocal sdk_calls, fallback_calls, window_requests, source_seconds, persist_seconds
        nonlocal source_backoff_until, http_peak_inflight
        nonlocal queued_windows, inflight_windows, peak_inflight_windows, worker_slot_seconds
        cohort_remaining = sum(len(job[2]) for job in jobs)
        inflight_partitions = 0
        queued_windows = len(jobs)
        active_key, active_day = f"{source_kind}:{jobs[0][0]}", jobs[0][2][0].isoformat()
        # Mark in-flight before auth/pool startup: an initialization failure
        # must not let the final checkpoint publish the advanced frontier.
        checkpoint()
        if pool is None:
            pool = _minute_pool(workers, keepalive=http_keepalive,
                                download_threads=download_threads, rate_gate=rate_shared,
                                authorization_gate=authorization_gate)
        old_states = {}
        identities = {}
        futures = {}
        next_job = 0

        def dispatch(job):
            nonlocal inflight_partitions, inflight_windows, peak_inflight_windows, queued_windows, window_requests
            symbol, item, target_days = job
            for day in target_days:
                key = (symbol, day.isoformat())
                old_states[key] = index.execute("SELECT status FROM partitions WHERE kind=? AND symbol=? AND trade_date=?",
                    (source_kind, symbol, day.isoformat())).fetchone()
                identity = phase_for_day(day.isoformat(), latest=days[-1].isoformat(), first_observed=first_observed)
                identities[key] = identity
                if identity == "older_discovery":
                    cursor["older_probe_attempts"] = int(cursor.get("older_probe_attempts", 0)) + 1
                    if item.get("_publication_retry"):
                        cursor["older_retry_attempts"] = int(cursor.get("older_retry_attempts", 0)) + 1
            quota_window.requested()
            window_requests += 1
            sent_at = time.monotonic()
            future = pool.apply_async(range_fetch, (root, f"{source_kind}:{symbol}", target_days),
                                      {"now": datetime.now(UTC)})
            futures[future] = (symbol, target_days, sent_at)
            inflight_partitions += len(target_days)
            inflight_windows = len(futures)
            peak_inflight_windows = max(peak_inflight_windows, inflight_windows)
            queued_windows -= 1

        def completed():
            nonlocal pool, state, next_job, inflight_windows, worker_slot_seconds
            heartbeat = time.monotonic()
            while futures or next_job < len(jobs):
                # Source/clock/quota/general-work admission is checked at every
                # actual dispatch, including rolling replacements. An active
                # owner is not permission to keep sending after preemption.
                while state == "batch_limit" and next_job < len(jobs) and len(futures) < workers:
                    if rate_shared is not None and rate_shared[0] > time.time():
                        state = "source_rate_limited"
                        break
                    if not may_acquire():
                        break
                    small_room = quota_window.sample[0] <= (
                        policy_state["effective_reserve_mb"] + quota_window.near_floor_mb)
                    if small_room and (len(jobs[next_job][2]) > 1 or futures):
                        break  # Replan single stock-days after admitted slots drain.
                    dispatch(jobs[next_job])
                    next_job += 1
                ready = [future for future in futures if future.ready()]
                for future in ready:
                    symbol, target_days, sent_at = futures.pop(future)
                    worker_slot_seconds += time.monotonic() - sent_at
                    inflight_windows = len(futures)
                    yield symbol, target_days, future.get()
                    # Parent validation/index/derivation may take time. Refill
                    # this released worker before consuming another completed
                    # range; queued ready results still count against workers.
                    break
                if not futures and (state != "batch_limit" or not ready and next_job < len(jobs)):
                    break
                if futures and any(time.monotonic() >= sent_at + 180 for _, _, sent_at in futures.values()):
                    # Public Pool lifecycle APIs, not private process state.
                    # Immutable worker receipts recover on the next owner.
                    pool.terminate()
                    pool.join()
                    pool = None
                    state = "window_time_budget_reached"
                    return
                if futures and time.monotonic() - heartbeat >= 15:
                    checkpoint()
                    heartbeat = time.monotonic()
                if futures and not ready:
                    time.sleep(0.05)

        for symbol, target_days, result in completed():
            transport = result.get("http", {})
            for field in HTTP_COUNTERS:
                value = transport.get(field, 0)
                if (not isinstance(value, (int, float)) or isinstance(value, bool)
                        or not math.isfinite(value) or value < 0):
                    raise ValueError("invalid bounded worker HTTP metric")
                http_metrics[field] += value
            http_peak_inflight = max(http_peak_inflight, transport.get("http_peak_inflight") or 0)
            sdk_calls += result["sdk_calls"]
            fallback_calls += result["fallback_calls"]
            source_seconds += result["source_seconds"]
            persist_seconds += result["persist_seconds"]
            outcomes = result["outcomes"]
            planned_keys = {(symbol, day.isoformat()) for day in target_days}
            elapsed = time.monotonic() - last_completed_clock
            cycle_seconds = elapsed / len(outcomes) if outcomes else 0.0
            reported = set()
            for outcome in outcomes:
                day = date.fromisoformat(outcome["trade_date"])
                key = (symbol, outcome["trade_date"])
                if key not in planned_keys:
                    raise ValueError("worker returned an unplanned partition")
                if key in reported:
                    raise ValueError("worker returned a duplicate partition")
                reported.add(key)
                phase = phase_metrics[identities[key]]
                phase["attempts"] += 1
                phase["newly_checked"] += int(old_states[key] is None)
                attempts += 1
                receipt = outcome.get("receipt")
                if receipt:
                    _record_source(index, root, source_kind, symbol, day, receipt)
                    successes += 1
                    quota_pending_successes += 1
                    if old_states[key] is None or old_states[key][0] not in ACCEPTED:
                        phase["new_receipted"] += 1
                        phase["rows"] += int(receipt.get("rows") or 0)
                        phase["local_bytes"] += int(receipt.get("parquet_size_bytes") or 0)
                    if receipt.get("status") == "downloaded_unverified_for_pit":
                        first_observed = min(first_observed, day.isoformat()) if first_observed else day.isoformat()
                    last_success_at = datetime.now(UTC).isoformat()
                else:
                    failure = outcome["failure"]
                    _record(index, source_kind, symbol, day, failure)
                    if failure in {"quota_exhausted", "authentication_failed", "source_rate_limited"}:
                        state = failure
                phase["cycle_seconds"] += cycle_seconds
                durations.append(cycle_seconds)
            # A quota/auth failure during single-day fallback can leave an
            # unrequested suffix. Keep the pre-wave frontier until every
            # planned day has a durable outcome; never skip that suffix.
            cohort_remaining -= len(reported)
            inflight_partitions -= len(target_days)
            if len(reported) != len(target_days) and state == "batch_limit":
                state = "incomplete_window_result"
            last_completed_clock = time.monotonic()
            print(f"[finlab-market] window {source_kind}:{symbol} {min(target_days)}..{max(target_days)}: "
                  f"{sum('receipt' in outcome for outcome in outcomes)}/{len(target_days)} persisted", flush=True)
            checkpoint()
        persist_source_backoff()
        # A near-floor replan leaves state batch_limit so the outer loop can
        # reconcile completed receipts and select bounded single-day work.
        return cohort_remaining == 0 and state not in {
            "quota_exhausted", "authentication_failed", "source_rate_limited"}

    def next_candidate(ordinal):
        """Advance only the in-memory frontier until an entire wave is durable."""
        nonlocal state
        queue = cursor["recent_queue"]
        repairs = cursor.get("known_gap_queue", [])
        if source_kind == "tw_minute" and repairs and not queue:
            repair = repairs.pop(0)
            kind, day = source_kind, date.fromisoformat(repair["day"])
            item = known[repair["symbol"]]
        elif not older_paused and ordinal % 4 == 2 and int(cursor["former"]) < len(former_tasks):
            kind, item, day = former_tasks[int(cursor["former"])]
            cursor["former"] = int(cursor["former"]) + 1
        elif queue and (source_kind == "tw_minute" or ordinal % 4 != 3 or
                       (int(cursor["oldest"]) >= slots and int(cursor["newest"]) >= slots)):
            head = queue[0]
            flat = int(head["next"])
            if flat >= width:
                queue.pop(0)
                return None
            item, kind, day = universe[flat], source_kind, date.fromisoformat(head["day"])
            head["next"] = flat + 1
        else:
            stream = "oldest" if not older_paused and ordinal % (16 if source_kind == "tw_minute" else 4) == (
                15 if source_kind == "tw_minute" else 3) else "newest"
            flat = int(cursor[stream])
            if flat >= slots:
                other = "newest" if stream == "oldest" else "oldest"
                if int(cursor[other]) >= slots:
                    state = "frontiers_scanned; retries_due_next_cycle"
                    return None
                stream, flat = other, int(cursor[other])
            kind, item, day = _candidate(flat, days, universe, newest_first=stream == "newest", kind=source_kind)
            cursor[stream] = flat + 1
        return kind, item, day

    try:
        checkpoint(full=True)
        # Recent unpublished partitions can appear later the same session;
        # older failures get a fair retry after the next account reset. Do
        # not let retries consume the entire new-symbol discovery budget.
        recent_cutoff = (days[-1] - timedelta(days=30)).isoformat()
        publication_delay = publication_recheck_delay(now=now, policy=quota_policy)
        retry_due = (now - publication_delay).isoformat()
        cycle_start = quota_cycle_start(now).isoformat()
        # Longer useful-data passes must not multiply unpublished re-probes.
        # The old hard 16-window cap became the whole batch once the scan
        # reached unpublished history, even when every recent retry succeeded.
        # Keep unknown old discovery separate; due known-window work can fill
        # the measured bounded pool without waiting for another timer owner.
        retry_budget = min(limit, max(16, workers * queue_depth)) if pipeline else min(16, max(1, limit // 8))
        # A rejected signing transaction is transient, not an empty partition.
        # Select bounded extra rows so its stock-day range can be retried as one
        # native query instead of spending one call per failed day.
        due = index.execute(
            """SELECT kind,symbol,trade_date FROM effective_partitions
               WHERE kind=? AND status NOT IN ('downloaded_unverified_for_pit',
                                    'derived_unverified_for_pit','derived_no_regular_trades',
                                    'verified_closed_date','verified_no_trade')
                 AND (checked_at_utc < ? OR (status='provider_error' AND checked_at_utc < ?) OR
                      (status='source_rate_limited' AND checked_at_utc < ?) OR
                      (status='partition_not_ready' AND trade_date >= ?
                       AND checked_at_utc < ?))
               ORDER BY CASE WHEN status='provider_error' THEN 0 ELSE 1 END,
                        checked_at_utc,trade_date DESC LIMIT ?""",
            (source_kind, cycle_start, (now-timedelta(seconds=30)).isoformat(),
             (now-timedelta(seconds=60)).isoformat(), recent_cutoff, retry_due,
             retry_budget*31 if pipeline else retry_budget),
        ).fetchall()
        # Workers wrote day receipts oldest first. Start each symbol's retry
        # at its newest due day so the backward 31-calendar-day planner can
        # recover the whole failed window, not one day per native query.
        due_by_symbol = {}
        for row in due:
            due_by_symbol.setdefault(row[:2], []).append(row)
        due = [row for group in due_by_symbol.values()
               for row in sorted(group, key=lambda row: row[2], reverse=True)]
        known = {item["symbol"]: item for item in universe}
        retry_jobs = []
        retry_planned = set()
        retry_older_planned = 0
        for kind, symbol, day_text in due:
            if attempts + sum(len(job[2]) for job in retry_jobs) >= limit or len(retry_jobs) >= retry_budget:
                break
            day = date.fromisoformat(day_text)
            if kind != source_kind or symbol not in known or day not in days or not _eligible(known[symbol], day):
                continue
            older_retry = phase_for_day(day_text, latest=days[-1].isoformat(), first_observed=first_observed) == "older_discovery"
            if ((int(cursor.get("older_probe_attempts", 0)) + retry_older_planned >= 16
                    or int(cursor.get("older_retry_attempts", 0)) + retry_older_planned >= 4)
                    and older_retry):
                continue
            key = f"{kind}:{symbol}"
            receipt = _available_receipt(root, kind, symbol, day)
            if receipt:
                _record_source(index, root, DERIVED_INDEX_KIND if receipt.get("source_kind") == "derived_from_tw_tick" else kind,
                               symbol, day, receipt)
                cached_skips += 1
                continue
            _, attempt_path = _partition_paths(root, key, day)
            attempt_payload = read_json(attempt_path)
            rejected_signing = attempt_payload.get("provider_code") == "request_conflict"
            if attempt_payload.get("status") in {"provider_error", "source_rate_limited"} and not rejected_signing and _checked_this_cycle(
                    attempt_path, "attempted_at_utc", now):
                continue
            if rejected_signing and _checked_this_cycle(attempt_path, "attempted_at_utc", now):
                continue
            if (symbol, day) in retry_planned:
                continue
            if not may_acquire():
                break
            if pipeline:
                target_days = [day]
                small_room = quota_window.sample[0] <= (policy_state["effective_reserve_mb"]
                                                       + quota_window.near_floor_mb)
                publication_recheck = (attempt_payload.get("status") == "partition_not_ready"
                    and phase_for_day(day_text, latest=days[-1].isoformat(), first_observed=first_observed)
                        != "older_discovery")
                if (source_kind == 'tw_minute' and not older_retry
                        and (attempt_payload.get("status") == "provider_error" or publication_recheck) and not small_room):
                    def retry_available(candidate):
                        if (symbol, candidate) in retry_planned or not _eligible(known[symbol], candidate):
                            return False
                        if _available_receipt(root, kind, symbol, candidate) is not None:
                            return False
                        _, path = _partition_paths(root, key, candidate)
                        payload = read_json(path)
                        if payload.get("status") == "provider_error":
                            return not _checked_this_cycle(path, "attempted_at_utc", now)
                        if payload.get("status") == "partition_not_ready":
                            try:
                                checked = datetime.fromisoformat(payload["attempted_at_utc"])
                                return checked.tzinfo is not None and (
                                    checked < quota_cycle_start(now) or candidate.isoformat() >= recent_cutoff
                                    and checked < now-publication_delay)
                            except (ValueError, KeyError, TypeError):
                                return False
                        return False
                    target_days = plan_minute_window(day, days, lower_bound=days[0],
                        calendar_days=range_calendar_days,
                        limit=limit-attempts-sum(len(job[2]) for job in retry_jobs), available=retry_available)
                retry_jobs.append((symbol, {**known[symbol], "_publication_retry": True}, target_days))
                retry_older_planned += len(target_days) if older_retry else 0
                retry_planned.update((symbol, value) for value in target_days)
                if small_room:
                    break
                continue
            acquire(kind, symbol, day, retry=True)
            if state != "batch_limit":
                break
        if retry_jobs:
            before_retries = json.dumps(cursor)
            if state == "batch_limit":
                if not acquire_wave(retry_jobs):
                    cursor = json.loads(before_retries)
                active_key = active_day = None
                inflight_partitions = 0
                queued_windows = inflight_windows = 0
                checkpoint()
        while state == "batch_limit" and attempts < limit and scanned < max(100_000, limit * 100):
            before_wave = json.dumps(cursor)
            jobs = []
            planned = set()
            planned_older = 0
            # Near the 50 MB floor use one stock-day per request, not a multi-
            # date/pool overshoot. Official SDK quota always remains binding.
            small_room = quota_window.sample is not None and quota_window.sample[0] <= (
                policy_state["effective_reserve_mb"] + quota_window.near_floor_mb)
            wave_width = workers * queue_depth if pipeline and not small_room else 1
            # The quota-sampling request bound is real even when testing more
            # workers. A fresh sample admits one bounded wave, never more than
            # the configured SDK-request budget before the next observation.
            # A larger *local* queue does not authorize extra active requests.
            # Dispatch refreshes quota every bounded request window below.
            if pipeline and quota_window.requests + wave_width > quota_policy.quota_sample_max_requests:
                quota_window.at_clock = None  # Sample before a wave would cross the request budget.
            while state == "batch_limit" and len(jobs) < wave_width and attempts + len(planned) < limit:
                # Save the frontier before selecting: resource/preemption
                # refusal must not skip a never-requested candidate.
                before_candidate = json.dumps(cursor)
                selected = next_candidate(attempts + len(planned))
                if selected is None:
                    if scanned >= max(100_000, limit * 100) or state != "batch_limit":
                        break
                    continue
                kind, item, day = selected
                scanned += 1
                if scanned >= max(100_000, limit * 100):
                    state = "scan_budget_reached"
                    cursor = json.loads(before_candidate)
                    break
                if scanned % 64 == 0 and not jobs:
                    checkpoint()
                if not _eligible(item, day):
                    continue
                symbol = item["symbol"]
                key = f"{kind}:{symbol}"
                if (symbol, day) in planned:
                    continue
                receipt_path, attempt_path = _partition_paths(root, key, day)
                receipt = _available_receipt(root, kind, symbol, day)
                if receipt:
                    _record_source(index, root, DERIVED_INDEX_KIND if receipt.get("source_kind") == "derived_from_tw_tick" else kind,
                                   symbol, day, receipt)
                    cached_skips += 1
                    continue
                if _checked_this_cycle(attempt_path, "attempted_at_utc", now):
                    continue
                if first_observed is None or day.isoformat() < first_observed:
                    # Known old failures belong to the fair, bounded retry
                    # queue above. The two discovery frontiers must not bypass
                    # its retry cap and probe the same failed stock-day again.
                    if read_json(attempt_path).get("status") in {
                        "partition_not_ready", "provider_error", "source_rate_limited"
                    }:
                        continue
                    # Unpublished ancient candidates are not millions of
                    # required downloads. Retain their discovery frontier.
                    if int(cursor.get("older_probe_attempts", 0)) + planned_older >= 16:
                        cursor = json.loads(before_candidate)
                        if older_paused:
                            state = "waiting_older_publication"
                            break
                        older_paused = True
                        continue
                    planned_older += 1
                if not jobs and not may_acquire():
                    cursor = json.loads(before_candidate)
                    break
                if not jobs and pipeline:
                    small_room = quota_window.sample is not None and quota_window.sample[0] <= (
                        policy_state["effective_reserve_mb"] + quota_window.near_floor_mb)
                    if small_room:
                        wave_width = 1
                target_days = [day]
                if (pipeline and not small_room and first_observed
                        and first_observed <= day.isoformat() < days[-1].isoformat()):
                    def available(candidate):
                        if not _eligible(item, candidate) or (symbol, candidate) in planned:
                            return False
                        rpath, apath = _partition_paths(root, key, candidate)
                        return (_available_receipt(root, kind, symbol, candidate) is None
                                and not _checked_this_cycle(apath, "attempted_at_utc", now))
                    target_days = plan_minute_window(day, days, lower_bound=date.fromisoformat(first_observed),
                        calendar_days=range_calendar_days, limit=limit-attempts-len(planned), available=available,
                        upper_bound=days[-1] - timedelta(days=1))
                jobs.append((symbol, item, target_days))
                planned.update((symbol, candidate) for candidate in target_days)
                if not pipeline:
                    break
            if not jobs:
                continue
            exhausted_state = state if state != "batch_limit" else None
            # A frontier/scan exhaustion while filling the last small wave is
            # not an admission refusal. Its already eligible jobs still run.
            state = "batch_limit"
            _atomic_json(cursor_path, json.loads(before_wave))
            if pipeline:
                complete = acquire_wave(jobs)
                if not complete:
                    cursor = json.loads(before_wave)
                active_key = active_day = None
                inflight_partitions = 0
                queued_windows = inflight_windows = 0
                checkpoint()
            else:
                symbol, item, target_days = jobs[0]
                acquire(source_kind, symbol, target_days[0])
            if exhausted_state and state == "batch_limit":
                state = exhausted_state
        _atomic_json(cursor_path, cursor)
        summary = _summarize(index, universe, days, started=started,
                             attempts=attempts, successes=successes, state=state,
                             daily_price_coverage=daily_price_coverage, active_kind=source_kind,
                             source_root=root, quota_policy=quota_policy)
        summary["general_acquisition_admission"] = general_admission
        summary["quota_policy"] = policy_state
        _atomic_json(root / "intraday/market_status.json", summary)
        last_status_clock = time.monotonic()
        return summary
    except BaseException:
        state = "interrupted"
        raise
    finally:
        try:
            # Include worker shutdown in measured owner wall time. Keep the
            # progress checkpoint recoverable if interrupted during teardown.
            if pool is not None:
                if state == "interrupted":
                    pool.terminate()
                else:
                    pool.close()
                pool.join()
                pool = None
            persist_source_backoff()
            checkpoint(full=state == "interrupted", finished=True)
            # One immutable batch receipt and a bounded reader index. Interrupted
            # work remains evidence, but cannot calibrate accepted throughput.
            # The final unsampled request's quota is intentionally not invented.
            finished_at = datetime.now(UTC)
            metrics = {
                "contract_version": BATCH_METRICS_VERSION, "origin": "canonical_owner",
                "kind": source_kind, "acquisition_policy_version": ACQUISITION_POLICY_VERSION,
                "quota_policy": policy_state, "quota_checks": quota_checks,
                "quota_cache_hits": quota_cache_hits,
                "quota_observation_bounds": {"maximum_requests": quota_window.maximum_requests,
                    "maximum_age_seconds": quota_window.interval_seconds,
                    "per_request_within_reserve_mb": quota_window.near_floor_mb},
                "processing_engine": engine,
                "data_processing_engine": "polars_arrow_v1", "processing_threads": quota_policy.processing_threads,
                "pipeline_workers": workers, "range_calendar_days": range_calendar_days,
                "download_threads": download_threads, "queue_depth": queue_depth, "authorization_slots": authorization_slots,
                "http_keepalive": http_keepalive if pipeline else False,
                "sdk_calls": sdk_calls if pipeline else attempts, "window_requests": window_requests,
                "fallback_calls": fallback_calls, "source_seconds": source_seconds,
                "persist_seconds": persist_seconds,
                **http_metrics, "http_peak_inflight_per_worker": http_peak_inflight,
                "peak_inflight_windows": peak_inflight_windows, "worker_slot_seconds": worker_slot_seconds,
                "source_backoff_until_utc": source_backoff_until,
                "started_at_utc": full_started_at.isoformat(), "finished_at_utc": finished_at.isoformat(),
                "attempt_limit": limit, "attempted": attempts, "completed_attempts": len(durations),
                "successful": successes, "state": state,
                "elapsed_seconds": round(time.monotonic() - full_started_clock, 6),
                "new_receipted_partitions": sum(row["new_receipted"] for row in phase_metrics.values()),
                "account_delta_bytes": round(account_delta_bytes),
                "quota_sample_successes": quota_sample_successes,
                "billing_basis": "bounded_pre_request_shared_account_deltas_excluding_unsampled_tail",
                "phases": phase_metrics,
            }
            history_path = root / "intraday/batch_metrics.json"
            history = read_json(history_path)
            batches = history.get("batches", []) if history.get("contract_version") == BATCH_METRICS_VERSION else []
            _atomic_json(root / "intraday/batch_receipts" / (full_started_at.strftime("%Y%m%dT%H%M%S%fZ") + ".json"), metrics)
            _atomic_json(history_path, {"contract_version": BATCH_METRICS_VERSION,
                                       "batches": [*batches[-255:], metrics]})
        finally:
            if pool is not None:
                if state == "interrupted":
                    pool.terminate()
                else:
                    pool.close()
                pool.join()
            index.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2003, 8, 1))
    parser.add_argument("--end-date", type=date.fromisoformat,
                        default=default_market_end(datetime.now(UTC)))
    parser.add_argument("--limit", type=int, default=64)
    parser.add_argument("--kind", choices=KINDS, default="tw_minute",
                        help="Download minutes first; Tick is admitted only after the minute pass")
    parser.add_argument("--workers", type=int, default=None, help="Bounded SDK processes; defaults to the family policy")
    parser.add_argument("--range-calendar-days", type=int, default=None, help="Intraday stock windows, at most 31 calendar days")
    parser.add_argument("--download-threads", type=int, default=None,
                        help="Parallel reads inside the native signed group; minute-only, at most 5")
    parser.add_argument("--queue-depth", type=int, default=None,
                        help="Local queue slots per worker (1..4); >1 uses rolling refill")
    parser.add_argument("--authorization-slots", type=int, default=None,
                        help="Shared manifest/signing concurrency, separate from data workers (1..32)")
    parser.add_argument("--reserve-mb", type=float, default=None,
                        help="Local floor override; defaults to the shared per-family quota policy")
    parser.add_argument("--minimum-free-gb", type=float, default=INTRADAY_MINIMUM_FREE_GB)
    parser.add_argument("--sync-lock-fd", type=int, default=None,
                        help="Existing parent .sync.lock descriptor; otherwise acquire the account lock")
    parser.add_argument("--max-run-seconds", type=float, default=None,
                        help="Graceful batch wall-clock budget; leave 180 seconds for an in-flight SDK request")
    parser.add_argument("--status-only", action="store_true",
                        help="rebuild local candidate/receipt inventory without calling FinLab")
    parser.add_argument("--derive-existing", action="store_true",
                        help="with --status-only, replay all stored ticks into derived minutes")
    args = parser.parse_args()
    settings = load_policy()
    workers = args.workers if args.workers is not None else (
        settings.minute_workers if args.kind == "tw_minute" else settings.tick_workers)
    range_days = args.range_calendar_days if args.range_calendar_days is not None else (
        settings.minute_range_calendar_days if args.kind == "tw_minute" else settings.tick_range_calendar_days)
    if (args.end_date < args.start_date or args.limit < 1
            or args.reserve_mb is not None and args.reserve_mb < 0 or args.minimum_free_gb < 0
            or args.max_run_seconds is not None and not 180 < args.max_run_seconds <= 3600):
        parser.error("invalid market range or resource guard")
    if args.derive_existing and not args.status_only:
        parser.error("--derive-existing requires --status-only")
    if args.status_only:
        # Do not let an inventory-only invocation race the account's writer.
        try:
            with _account_sync_lock(args.output_root, args.sync_lock_fd):
                return _inventory_only(args)
        except BlockingIOError:
            print(json.dumps({"state": "account_sync_active", "attempted_this_run": 0}))
            return 0
    else:
        def terminate(signum, frame):
            raise SystemExit(128 + signum)
        previous_handler = signal.signal(signal.SIGTERM, terminate)
        try:
            with _account_sync_lock(args.output_root, args.sync_lock_fd):
                summary = sync_market(args.output_root, args.public_root, start=args.start_date,
                                      end=args.end_date, limit=args.limit,
                                      reserve_mb=args.reserve_mb,
                                      minimum_free_gb=args.minimum_free_gb,
                                      now=datetime.now(UTC), max_run_seconds=args.max_run_seconds, kind=args.kind,
                                      enforce_minute_priority=True, workers=workers, range_calendar_days=range_days,
                                      download_threads=args.download_threads,
                                      queue_depth=args.queue_depth,
                                      authorization_slots=args.authorization_slots,
                                      quota_policy=settings)
        except BlockingIOError:
            print(json.dumps({"state": "account_sync_active", "attempted_this_run": 0}))
            return 0
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
    print(json.dumps({key: value for key, value in summary.items() if key != "symbols"},
                     ensure_ascii=False, sort_keys=True), flush=True)
    return 1 if summary["state"] in {"quota_unknown", "authentication_failed"} else 0


def _inventory_only(args) -> int:
    universe = load_universe(args.public_root, start=args.start_date)
    daily_price_coverage = annotate_daily_close_coverage(args.output_root, universe)
    days = session_days(args.public_root, start=args.start_date, end=args.end_date)
    if not days or not universe:
        raise ValueError("no verified stock universe or observed session days")
    index = _open_index(args.output_root / "intraday/market_index.sqlite3")
    try:
        if args.derive_existing:
            _bootstrap_derived(index, args.output_root)
        summary = _summarize(index, universe, days, started=datetime.now(UTC),
                             attempts=0, successes=0, state="inventory_only",
                             daily_price_coverage=daily_price_coverage, active_kind=args.kind, source_root=args.output_root)
    finally:
        index.close()
    summary["inventory_rebuilt_without_api"] = True
    _atomic_json(args.output_root / "intraday/market_status.json", summary)
    print(json.dumps({key: value for key, value in summary.items() if key != "symbols"},
                     ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
