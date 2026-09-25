#!/usr/bin/env python3
"""Bounded all-stock FinLab tick discovery and locally derived minute backfill.

The symbol universe is the union of the official current manifest and official
delisting tables. Local daily-price bounds are *candidate* lifecycle evidence,
not proof that FinLab has published the corresponding intraday partition.
"""

from __future__ import annotations

import argparse
from bisect import bisect_left, bisect_right
from datetime import UTC, date, datetime, timedelta
import gc
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import sys

import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.download_finlab_history import (  # noqa: E402
    DEFAULT_OUTPUT, TAIPEI, _atomic_json, credential_available, quota_cycle_start, safe_stem,
    quota_room_mb,
)
from scripts.download_finlab_intraday import (  # noqa: E402
    _checked_this_cycle, _partition_paths, _stored_receipt, fetch_partition,
)
from scripts.derive_finlab_minute import derive_partition  # noqa: E402


DEFAULT_PUBLIC_ROOT = REPO_ROOT / "data_tw_public"
KINDS = ("tw_tick",)  # Only this family may consume provider quota.
DISPLAY_KINDS = ("tw_minute", "tw_tick")
DERIVED_INDEX_KIND = "tw_minute_derived"


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
    return db


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
    try:
        derived = derive_partition(root, symbol, day)
        _record(db, DERIVED_INDEX_KIND, symbol, day, derived["status"],
                int(derived.get("rows") or 0),
                int(derived.get("parquet_size_bytes") or 0))
    except Exception as exc:
        _record(db, DERIVED_INDEX_KIND, symbol, day, "derivation_error")
        print(f"[finlab-market] derive tw_minute:{symbol} {day}: {type(exc).__name__}", flush=True)


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
                _record_tick_and_minute(db, root, symbol, day, receipt)
        except (OSError, ValueError, KeyError, TypeError):
            continue


def _candidate(flat: int, days: list[date], universe: list[dict],
               *, newest_first: bool) -> tuple[str, dict, date]:
    width = len(universe) * len(KINDS)
    day_number, remainder = divmod(flat, width)
    day = days[-1 - day_number] if newest_first else days[day_number]
    item = universe[remainder // len(KINDS)]
    return KINDS[remainder % len(KINDS)], item, day


def _former_probe_tasks(universe: list[dict], days: list[date]) -> list[tuple[str, dict, date]]:
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
    return [(kind, item, day) for item, day in candidates for kind in KINDS]


def _summarize(db: sqlite3.Connection, universe: list[dict], days: list[date],
               *, started: datetime, attempts: int, successes: int,
               state: str, daily_price_coverage: dict | None = None) -> dict:
    counts: dict[tuple[str, str], dict] = {}
    for kind, symbol, status, partitions, rows, size in db.execute(
        """SELECT kind,symbol,status,COUNT(*),SUM(rows),SUM(parquet_bytes)
           FROM partitions WHERE trade_date BETWEEN ? AND ? GROUP BY kind,symbol,status""",
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
            """SELECT kind,symbol,MIN(trade_date),MAX(trade_date) FROM partitions
               WHERE status IN ('downloaded_unverified_for_pit','derived_unverified_for_pit')
               AND trade_date BETWEEN ? AND ? GROUP BY kind,symbol""",
            (days[0].isoformat(), days[-1].isoformat()),
        )
    }
    by_kind = {}
    symbols = []
    for kind in DISPLAY_KINDS:
        indexed_kind = DERIVED_INDEX_KIND if kind == "tw_minute" else kind
        total = received = rows = size = not_ready = failed = 0
        for item in universe:
            symbol = item["symbol"]
            target = _target_count(item, days)
            facts = counts.get((indexed_kind, symbol), {})
            source_facts = counts.get(("tw_tick", symbol), {})
            got = int(facts.get("receipted_partitions", 0))
            total += target
            received += got
            rows += int(facts.get("rows", 0))
            size += int(facts.get("parquet_bytes", 0))
            source_not_ready = (source_facts if kind == "tw_minute" else facts).get(
                "provider_not_ready_partitions", 0)
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
                "source_kind": "derived_from_tw_tick" if kind == "tw_minute" else "finlab_api",
            })
        by_kind[kind] = {
            "source_kind": "derived_from_tw_tick" if kind == "tw_minute" else "finlab_api",
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
    return {
        "schema_version": 2, "observed_at_utc": datetime.now(UTC).isoformat(),
        "acquisition_mode": "tick_api_only_minute_derived_locally",
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
        "daily_price_coverage": daily_price_coverage or {"state": "not_checked"},
        "attempted_this_run": attempts, "successes_this_run": successes,
        "state": state, "completion_eta": None,
        "completion_eta_reason": "FinLab partition availability, per-request bytes and future shared account quota are unknown",
    }


def sync_market(root: Path, public_root: Path, *, start: date, end: date,
                limit: int, reserve_mb: float, minimum_free_gb: float,
                now: datetime, fetch=fetch_partition) -> dict:
    if not credential_available():
        raise RuntimeError("no usable FinLab session")
    universe = load_universe(public_root, start=start)
    daily_price_coverage = annotate_daily_close_coverage(root, universe)
    days = session_days(public_root, start=start, end=end)
    if not days or not universe:
        raise ValueError("no verified stock universe or observed session days")
    index = _open_index(root / "intraday/market_index.sqlite3")
    cursor_path = root / "intraday/market_cursor.json"
    try:
        cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        cursor = {}
    width = len(universe) * len(KINDS)
    universe_hash = hashlib.sha256("\n".join(
        f"{item['symbol']}:{item['current']}:{item['delisting_date']}:{item['first_candidate_date']}"
        for item in universe
    ).encode("utf-8")).hexdigest()
    base = [days[0].isoformat(), len(universe), universe_hash, "tick_only_v1"]
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
                  "bootstrapped": cursor.get("bootstrapped") is True}
    cursor["day_count"] = len(days)
    cursor["last_day"] = days[-1].isoformat()
    cursor.setdefault("former", 0)
    if not cursor.get("bootstrapped"):
        for path in (root / "intraday/receipts").glob("*/*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                key = str(payload["dataset"])
                kind, symbol = key.split(":", 1)
                day = date.fromisoformat(payload["trade_date"])
            except (OSError, ValueError, KeyError, TypeError):
                continue
            if kind not in KINDS or _stored_receipt(path, root, key, day) is None:
                continue
            _record(index, kind, symbol, day, payload["status"],
                    int(payload.get("rows") or 0),
                    int(payload.get("parquet_size_bytes") or 0))
        cursor["bootstrapped"] = True
    if not cursor.get("derived_bootstrapped"):
        _bootstrap_derived(index, root)
        cursor["derived_bootstrapped"] = True
    slots = len(days) * width
    former_tasks = _former_probe_tasks(universe, days)
    started = datetime.now(UTC)
    attempts = successes = 0
    state = "batch_limit"
    scanned = 0
    try:
        # Recent unpublished partitions can appear later the same session;
        # older failures get a fair retry after the next account reset. Do
        # not let retries consume the entire new-symbol discovery budget.
        recent_cutoff = (days[-1] - timedelta(days=30)).isoformat()
        retry_due = (now - timedelta(hours=2)).isoformat()
        cycle_start = quota_cycle_start(now).isoformat()
        retry_budget = max(1, limit // 8)
        due = index.execute(
            """SELECT kind,symbol,trade_date FROM partitions
               WHERE kind='tw_tick' AND status NOT IN ('downloaded_unverified_for_pit',
                                    'verified_closed_date','verified_no_trade')
                 AND (checked_at_utc < ? OR
                      (status='partition_not_ready' AND trade_date >= ?
                       AND checked_at_utc < ?))
               ORDER BY checked_at_utc,trade_date DESC LIMIT ?""",
            (cycle_start, recent_cutoff, retry_due, retry_budget),
        ).fetchall()
        known = {item["symbol"]: item for item in universe}
        for kind, symbol, day_text in due:
            day = date.fromisoformat(day_text)
            if kind not in KINDS or symbol not in known or day not in days or not _eligible(known[symbol], day):
                continue
            room = quota_room_mb()
            if room is None:
                state = "quota_unknown"
                break
            if room[0] <= reserve_mb:
                state = "quota_margin_reached"
                break
            if shutil.disk_usage(root).free < minimum_free_gb * 1024**3:
                state = "disk_reserve_reached"
                break
            key = f"{kind}:{symbol}"
            receipt_path, _ = _partition_paths(root, key, day)
            receipt = _stored_receipt(receipt_path, root, key, day)
            if receipt:
                _record_tick_and_minute(index, root, symbol, day, receipt)
                continue
            attempts += 1
            try:
                receipt = fetch(root, key, day, now=datetime.now(UTC))
                _record_tick_and_minute(index, root, symbol, day, receipt)
                successes += 1
                print(f"[finlab-market] retry {key} {day}: {receipt['status']}", flush=True)
            except Exception as exc:
                from scripts.download_finlab_intraday import record_failed_partition
                failure = record_failed_partition(root, key, day, exc)
                _record(index, kind, symbol, day, failure)
                print(f"[finlab-market] retry {key} {day}: {failure}", flush=True)
            gc.collect()
        while state == "batch_limit" and attempts < limit and scanned < max(100_000, limit * 100):
            room = quota_room_mb()
            if room is None:
                state = "quota_unknown"
                break
            if room[0] <= reserve_mb:
                state = "quota_margin_reached"
                break
            if shutil.disk_usage(root).free < minimum_free_gb * 1024**3:
                state = "disk_reserve_reached"
                break
            queue = cursor["recent_queue"]
            if attempts % 4 == 2 and int(cursor["former"]) < len(former_tasks):
                kind, item, day = former_tasks[int(cursor["former"])]
                cursor["former"] = int(cursor["former"]) + 1
            elif queue and (attempts % 4 != 3 or
                            (int(cursor["oldest"]) >= slots and int(cursor["newest"]) >= slots)):
                head = queue[0]
                flat = int(head["next"])
                if flat >= width:
                    queue.pop(0)
                    continue
                item = universe[flat // len(KINDS)]
                kind = KINDS[flat % len(KINDS)]
                day = date.fromisoformat(head["day"])
                head["next"] = flat + 1
            else:
                stream = "oldest" if attempts % 4 == 3 else "newest"
                flat = int(cursor[stream])
                if flat >= slots:
                    other = "newest" if stream == "oldest" else "oldest"
                    if int(cursor[other]) >= slots:
                        state = "frontiers_scanned; retries_due_next_cycle"
                        break
                    stream, flat = other, int(cursor[other])
                kind, item, day = _candidate(flat, days, universe,
                                             newest_first=stream == "newest")
                cursor[stream] = flat + 1
            scanned += 1
            if not _eligible(item, day):
                continue
            symbol = item["symbol"]
            key = f"{kind}:{symbol}"
            receipt_path, attempt_path = _partition_paths(root, key, day)
            receipt = _stored_receipt(receipt_path, root, key, day)
            if receipt:
                _record_tick_and_minute(index, root, symbol, day, receipt)
                continue
            if _checked_this_cycle(attempt_path, "attempted_at_utc", now):
                continue
            attempts += 1
            try:
                receipt = fetch(root, key, day, now=datetime.now(UTC))
                _record_tick_and_minute(index, root, symbol, day, receipt)
                successes += 1
                print(f"[finlab-market] {key} {day}: {receipt['status']} rows={receipt['rows']}", flush=True)
            except Exception as exc:
                # The canonical per-day downloader preserves a sanitized attempt.
                from scripts.download_finlab_intraday import record_failed_partition
                failure = record_failed_partition(root, key, day, exc)
                _record(index, kind, symbol, day, failure)
                print(f"[finlab-market] {key} {day}: {failure}", flush=True)
            gc.collect()
        _atomic_json(cursor_path, cursor)
        summary = _summarize(index, universe, days, started=started,
                             attempts=attempts, successes=successes, state=state,
                             daily_price_coverage=daily_price_coverage)
        _atomic_json(root / "intraday/market_status.json", summary)
        return summary
    finally:
        index.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2003, 8, 1))
    parser.add_argument("--end-date", type=date.fromisoformat,
                        default=datetime.now(TAIPEI).date() - timedelta(days=1))
    parser.add_argument("--limit", type=int, default=64)
    parser.add_argument("--reserve-mb", type=float, default=50.0)
    parser.add_argument("--minimum-free-gb", type=float, default=25.0)
    parser.add_argument("--status-only", action="store_true",
                        help="rebuild local candidate/receipt inventory without calling FinLab")
    parser.add_argument("--derive-existing", action="store_true",
                        help="with --status-only, replay all stored ticks into derived minutes")
    args = parser.parse_args()
    if args.end_date < args.start_date or args.limit < 1 or args.reserve_mb < 0 or args.minimum_free_gb < 0:
        parser.error("invalid market range or resource guard")
    if args.derive_existing and not args.status_only:
        parser.error("--derive-existing requires --status-only")
    if args.status_only:
        universe = load_universe(args.public_root, start=args.start_date)
        daily_price_coverage = annotate_daily_close_coverage(args.output_root, universe)
        days = session_days(args.public_root, start=args.start_date, end=args.end_date)
        if not days or not universe:
            parser.error("no verified stock universe or observed session days")
        index = _open_index(args.output_root / "intraday/market_index.sqlite3")
        try:
            if args.derive_existing:
                _bootstrap_derived(index, args.output_root)
            summary = _summarize(index, universe, days, started=datetime.now(UTC),
                                 attempts=0, successes=0, state="inventory_only",
                                 daily_price_coverage=daily_price_coverage)
        finally:
            index.close()
        summary["inventory_rebuilt_without_api"] = True
        _atomic_json(args.output_root / "intraday/market_status.json", summary)
    else:
        summary = sync_market(args.output_root, args.public_root, start=args.start_date,
                              end=args.end_date, limit=args.limit,
                              reserve_mb=args.reserve_mb,
                              minimum_free_gb=args.minimum_free_gb,
                              now=datetime.now(UTC))
    print(json.dumps({key: value for key, value in summary.items() if key != "symbols"},
                     ensure_ascii=False, sort_keys=True), flush=True)
    return 1 if summary["state"] == "quota_unknown" else 0


if __name__ == "__main__":
    raise SystemExit(main())
