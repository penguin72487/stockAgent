"""Auditable per-symbol daily refresh; never replace a history with its tail.

The release clock is a polling budget, not historical publication-time proof.
US prices are documented at 08:00 Taipei. Other overseas venues have a daily
check at that time until an exact provider release clock is established.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any

import pyarrow.parquet as pq

from downloader.parquet_integrity import parquet_receipt_error


DAILY_EQUITY = frozenset({"USStockPrice", "UKStockPrice", "EuropeStockPrice", "JapanStockPrice"})
CONTRACT_VERSION = 2  # An empty tail is not an empty replacement of all history.
FULL_RECHECK_DAYS = 7
OVERLAP_DAYS = 7


def canonical_us_id(identifier: str) -> str:
    # A verified provider mismatch: master BRK/A, accepted price ID BRK-A.
    # Dots are NOT generally aliases (e.g. exchange suffixes); leave them alone.
    return identifier.replace("/", "-") if re.fullmatch(r"[A-Z0-9-]+/[A-Z]{1,2}", identifier) else identifier


def migrate_us_aliases(connection: sqlite3.Connection) -> int:
    """Retain original task evidence while giving the accepted ID one owner."""
    connection.execute("CREATE TABLE IF NOT EXISTS finmind_identifier_aliases ("
                       "dataset TEXT,original_id TEXT,canonical_id TEXT,prior_task_json TEXT,"
                       "PRIMARY KEY(dataset,original_id))")
    cursor = connection.execute("SELECT * FROM tasks WHERE dataset='USStockPrice' AND data_id LIKE '%/%' "
                                "AND state!='identifier_alias'")
    names = [column[0] for column in cursor.description]
    changed = 0
    for values in cursor.fetchall():
        old = dict(zip(names, values))
        canonical = canonical_us_id(old["data_id"])
        # Do not hide an already-valid provider identity.
        if canonical == old["data_id"] or old["state"] == "complete":
            continue
        connection.execute("INSERT OR IGNORE INTO finmind_identifier_aliases VALUES (?,?,?,?)",
                           (old["dataset"], old["data_id"], canonical, json.dumps(old, sort_keys=True)))
        connection.execute("INSERT OR IGNORE INTO tasks(dataset,data_id,partition,kind,priority,state) "
                           "VALUES (?,?,?,'id_history',0,'pending')",
                           (old["dataset"], canonical, old["partition"]))
        connection.execute("UPDATE tasks SET state='identifier_alias',next_attempt_at_utc=NULL "
                           "WHERE dataset=? AND data_id=? AND partition=?",
                           (old["dataset"], old["data_id"], old["partition"]))
        changed += 1
    # The new master may have seeded the canonical ID before migration. A
    # verified parameter repair takes precedence over ordinary overseas backfill.
    connection.execute("UPDATE tasks SET priority=0 WHERE dataset='USStockPrice' AND state='pending' "
                       "AND data_id IN (SELECT canonical_id FROM finmind_identifier_aliases WHERE dataset='USStockPrice')")
    return changed


def read_baseline(root: Path, task: Any) -> tuple[dict, list[dict]]:
    path = root / "receipts" / task.dataset / hashlib.sha256(task.data_id.encode()).hexdigest()[:12] / "history.json"
    if not path.exists():
        return {}, []
    receipt = json.loads(path.read_bytes())
    if any(receipt.get(key) != getattr(task, key) for key in ("dataset", "data_id", "partition")):
        raise ValueError("history_receipt_identity_mismatch")
    if receipt.get("status") != "complete" or not receipt.get("rows"):
        return {}, []
    error = parquet_receipt_error(root, receipt)
    if error:
        raise ValueError("history_baseline:" + error)
    rows = pq.read_table(root / receipt["parquet_path"]).to_pylist()
    validate_rows(rows, task.data_id, date(1900, 1, 1), date.max)
    return receipt, rows


def request_plan(baseline: dict, now: datetime, today: date) -> dict:
    previous = baseline.get("request") or {}
    checked = previous.get("full_history_checked_at_utc") or baseline.get("fetched_at_utc")
    try:
        stamp = datetime.fromisoformat(checked)
        recent = stamp.tzinfo is not None and timedelta(0) <= now - stamp <= timedelta(days=FULL_RECHECK_DAYS)
        last = date.fromisoformat(baseline["source_last_date"][:10])
    except (TypeError, ValueError, KeyError):
        recent, last = False, date(1900, 1, 1)
    incremental = bool(baseline) and recent
    return {
        "history_refresh_contract_version": CONTRACT_VERSION,
        "query_shape": "per_id_incremental_overlap" if incremental else "per_id_full_history",
        "request_start_date": (max(date(1900, 1, 1), last - timedelta(days=OVERLAP_DAYS))
                               if incremental else date(1900, 1, 1)).isoformat(),
        "request_end_date": today.isoformat(), "request_end_inclusive": True, "request_count": 1,
        "full_history_checked_at_utc": checked if incremental else now.astimezone(UTC).isoformat(),
        "baseline_sha256": baseline.get("sha256"),
        "historical_point_in_time": False,
    }


def validate_rows(rows: list[dict], identifier: str, start: date, end: date) -> None:
    seen: set[str] = set()
    for row in rows:
        raw = row.get("date")
        try:
            day = date.fromisoformat(raw) if isinstance(raw, str) else None
        except ValueError:
            day = None
        if day is None or raw != day.isoformat() or not start <= day <= end:
            raise ValueError("history_response_outside_range")
        if str(row.get("stock_id")) != identifier:
            raise ValueError("history_wrong_data_id")
        if raw in seen:
            raise ValueError("history_duplicate_date")
        seen.add(raw)


def merge_response(old: list[dict], rows: list[dict], identifier: str, plan: dict) -> list[dict]:
    start, end = (date.fromisoformat(plan[key]) for key in ("request_start_date", "request_end_date"))
    validate_rows(rows, identifier, start, end)
    if old and not rows:
        if plan['query_shape'] != 'per_id_incremental_overlap':
            raise ValueError("unexpected_empty_after_nonempty")
        # A no-new-rows observation cannot prove deletion of historical rows.
        # Retain the verified baseline verbatim, with its original date range;
        # a later nonempty overlap/full response still applies real corrections.
        validate_rows(old, identifier, date(1900, 1, 1), date.max)
        plan['empty_response_policy'] = 'retain_verified_baseline_no_new_observation'
        return sorted(old, key=lambda row: row['date'])
    if plan["query_shape"] == "per_id_full_history":
        return sorted(rows, key=lambda row: row["date"])
    # The overlap is replaced, not appended; corrections/deletions can propagate.
    merged = [row for row in old if not start.isoformat() <= row["date"] <= end.isoformat()]
    merged.extend(rows)
    return sorted(merged, key=lambda row: row["date"])


def adjusted_history_changed(old: list[dict], rows: list[dict]) -> bool:
    """A changed adjustment factor may revise the entire past, not just a tail."""
    previous = {row["date"]: row.get("Adj_Close") for row in old if "Adj_Close" in row}
    return any(row.get("date") in previous and "Adj_Close" in row
               and row["Adj_Close"] != previous[row["date"]] for row in rows)
