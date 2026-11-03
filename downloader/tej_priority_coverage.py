"""Exclude already queried finite scopes from later lazy TEJ history tiles.

The finite tasks retain their original receipts and are never treated as proof
that omitted vendor rows exist.  Their verified query rectangles resolve the
same source query scope, including an explicit empty response.  This module
does not submit queries, change a vendor session, or manufacture observations.
"""
from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
from typing import Any

from downloader.artifact_io import sha256_file
from downloader.tej_history import (SOURCE_SCOPE_CONTRACT, company_code, expand_request,
                                    normalize_period, stable_id)
from downloader.tej_key_layout import key_count

CONTRACT = "finite_priority_lazy_coverage_exclusion_v1"
INDEX_TABLE = "finite_priority_coverage_company_v1"
INDEX_MARKER = "finite_priority_coverage_company_index_v1"


def _scope(request: dict) -> tuple[list[str], list[str | None], set[str]]:
    companies = request.get("company_labels")
    dates = request.get("date_labels")
    fields = request.get("fields")
    if (not isinstance(companies, list) or not companies
            or not isinstance(dates, list) or not isinstance(fields, list) or not fields
            or any(not isinstance(x, str) or not x for x in companies + dates + fields)
            or len(set(fields)) != len(fields)):
        raise ValueError("Malformed priority coverage scope")
    codes = [company_code(label) for label in companies]
    if len(set(codes)) != len(codes):
        raise ValueError("Ambiguous company identity in priority coverage")
    mode = key_count(request)
    if mode != 2 or request.get("frequency") != "daily":
        raise ValueError("Only dated daily Key=2 scopes support this coverage contract")
    periods: list[str | None] = [normalize_period(value) for value in dates]
    if not periods or any(len(value) != 10 for value in periods) or len(set(periods)) != len(periods):
        raise ValueError("Daily coverage requires distinct daily source periods")
    return codes, periods, set(fields)


def exclude_verified_scopes(table_id: str, ordinal: int, request: dict,
                            verified: list[dict]) -> tuple[list[dict], dict]:
    """Return disjoint remaining rectangles and their exact scope audit.

    Each input witness is a retained, already checked native task request;
    callers must establish its state, identity and source receipt before use.
    Partially overlapping field sets fail closed: one work-row count represents
    a whole field batch, so partial-field subtraction needs a different ABI.
    """
    if not isinstance(table_id, str) or not table_id or type(ordinal) is not int or ordinal < 0:
        raise ValueError("Exact lazy plan identity required")
    codes, periods, fields = _scope(request)
    source = tuple(request.get(k) for k in ("type", "smart_id", "table"))
    if not all(isinstance(value, str) and value for value in source):
        raise ValueError("Exact vendor binding required")
    covered: dict[str, set[str | None]] = defaultdict(set)
    used: set[str] = set()
    for witness in verified:
        if not isinstance(witness, dict) or not isinstance(witness.get("task_id"), str) or not witness["task_id"]:
            raise ValueError("Retained priority task identity required")
        other = witness.get("request")
        if not isinstance(other, dict) or tuple(other.get(k) for k in ("type", "smart_id", "table")) != source:
            raise ValueError("Priority witness belongs to a different source binding")
        other_codes, other_periods, other_fields = _scope(other)
        if key_count(request) != key_count(other):
            raise ValueError("Priority witness uses a different native key layout")
        common_codes = set(codes).intersection(other_codes)
        common_periods = set(periods).intersection(other_periods)
        common_fields = fields.intersection(other_fields)
        if not common_codes or not common_periods or not common_fields:
            continue
        if not fields.issubset(other_fields):
            raise ValueError("Partial-field overlap requires explicit reconciliation")
        for code in common_codes:
            covered[code].update(common_periods)
        used.add(witness["task_id"])
    original_rows = len(codes) * len(periods)
    covered_rows = sum(len(covered[code]) for code in codes)
    audit: dict[str, Any] = {"contract": CONTRACT, "table_id": table_id,
                             "parent_task_id": stable_id([table_id, request]),
                             "plan_ordinal": ordinal, "original_work_rows": original_rows,
                             "excluded_work_rows": covered_rows,
                             "priority_task_ids": sorted(used)}
    if not covered_rows:
        return [request], {**audit, "remaining_work_rows": original_rows}
    by_company: dict[tuple[int, ...], list[int]] = {}
    for index, code in enumerate(codes):
        keep = tuple(i for i, period in enumerate(periods) if period not in covered[code])
        if keep:
            by_company.setdefault(keep, []).append(index)
    by_period: dict[tuple[int, ...], list[int]] = {}
    for index, period in enumerate(periods):
        keep = tuple(i for i, code in enumerate(codes) if period not in covered[code])
        if keep:
            by_period.setdefault(keep, []).append(index)
    # Both partitions are exact; use the one requiring fewer native calls.
    # This compares two constructive tilings, not an unrestricted optimum.
    if len(by_period) < len(by_company):
        groups = [(tuple(date_indices), list(company_indices))
                  for company_indices, date_indices in by_period.items()]
    else:
        groups = list(by_company.items())
    children = []
    for keep, company_indices in groups:
        child = {**request,
                 "company_labels": [request["company_labels"][i] for i in company_indices],
                 "date_labels": [request["date_labels"][i] for i in keep],
                 "priority_coverage_exclusion": audit}
        children.append(child)
    remaining = sum(len(child["company_labels"]) * len(child["date_labels"])
                    for child in children)
    if remaining + covered_rows != original_rows:
        raise ValueError("Priority exclusion changed the work denominator")
    return children, {**audit, "remaining_work_rows": remaining}


def _index_task(con, task_id: str, expected_table: str | None = None) -> None:
    row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if (row is None or row["kind"] != "download" or row["scope_contract"] != SOURCE_SCOPE_CONTRACT
            or expected_table is not None and row["table_id"] != expected_table):
        raise ValueError("Finite priority index references a different retained task")
    definition = con.execute("SELECT * FROM tables WHERE table_id=?", (row["table_id"],)).fetchone()
    if definition is None:
        raise ValueError("Finite priority index table is missing")
    req = expand_request(json.loads(row["request_json"]), dict(definition), row["table_id"])
    # Snapshots and Key=3 have different time/record semantics; they never
    # enter this daily Key=2 index and remain on their original schedule.
    if key_count(req) != 2 or req.get("frequency") != "daily":
        return
    codes, _, _ = _scope(req)
    con.executemany(f"INSERT OR IGNORE INTO {INDEX_TABLE}(task_id,table_id,company_code) VALUES(?,?,?)",
                    ((task_id, row["table_id"], code) for code in codes))


def ensure_priority_coverage_index(con) -> None:
    """Install and backfill a small index inside the caller's transaction."""
    if not con.in_transaction:
        con.execute("BEGIN IMMEDIATE")
    con.execute(f"CREATE TABLE IF NOT EXISTS {INDEX_TABLE} ("
                "task_id TEXT NOT NULL, table_id TEXT NOT NULL, company_code TEXT NOT NULL, "
                "PRIMARY KEY(task_id,company_code))")
    con.execute(f"CREATE INDEX IF NOT EXISTS {INDEX_TABLE}_lookup "
                f"ON {INDEX_TABLE}(table_id,company_code,task_id)")
    marker = con.execute("SELECT value FROM meta WHERE key=?", (INDEX_MARKER,)).fetchone()
    if marker is not None:
        if marker[0] != CONTRACT:
            raise ValueError("Incompatible finite-priority coverage index")
        return
    records = con.execute("SELECT value FROM meta WHERE key LIKE 'finite_priority_plan:%'").fetchall()
    for record in records:
        result = json.loads(record[0])
        tasks = result.get("tasks")
        if not isinstance(tasks, list):
            raise ValueError("Malformed retained finite priority receipt")
        for task in tasks:
            if not isinstance(task, dict) or not task.get("task_id") or not task.get("table_id"):
                raise ValueError("Malformed finite priority task identity")
            _index_task(con, task["task_id"], task["table_id"])
    con.execute("INSERT INTO meta(key,value) VALUES(?,?)", (INDEX_MARKER, CONTRACT))


def index_new_priority_tasks(con, receipt: dict) -> None:
    """Keep a previously built index current in the registration transaction."""
    if con.execute("SELECT 1 FROM meta WHERE key=?", (INDEX_MARKER,)).fetchone() is None:
        return  # The next lazy refill atomically indexes all committed receipts.
    for task in receipt["tasks"]:
        _index_task(con, task["task_id"], task["table_id"])


def inherit_priority_coverage(con, parent: str, children: list[str]) -> None:
    """Carry the index through the canonical disjoint native task splits."""
    if con.execute("SELECT 1 FROM meta WHERE key=?", (INDEX_MARKER,)).fetchone() is None:
        return
    row = con.execute(f"SELECT table_id FROM {INDEX_TABLE} WHERE task_id=? LIMIT 1", (parent,)).fetchone()
    if row is None:
        return
    for child in children:
        _index_task(con, child, row[0])


def _retained_receipt(root: Path, task: dict) -> None:
    rel = task.get("receipt_path")
    if not isinstance(rel, str):
        raise ValueError("Completed priority scope has no retained source receipt")
    path = (root / rel).resolve(strict=True)
    if not path.is_relative_to(root.resolve()) or not path.is_file() or path.stat().st_size > 2 * 1024**2:
        raise ValueError("Priority source receipt is outside its owner or unbounded")
    receipt = json.loads(path.read_text(encoding="utf-8-sig"))
    if (receipt.get("task_id") != task["task_id"] or receipt.get("table_id") != task["table_id"]
            or receipt.get("source_values_retained") is not True
            or receipt.get("training_auto_injected") is not False):
        raise ValueError("Priority source receipt identity or retention differs")
    for kind in ("raw", "parquet"):
        name, digest = receipt.get(kind + "_path"), receipt.get(kind + "_sha256")
        if not isinstance(name, str) or not isinstance(digest, str):
            raise ValueError("Priority source receipt lacks an original hash")
        source = (root / name).resolve(strict=True)
        if not source.is_relative_to(root.resolve()) or not source.is_file() or sha256_file(source) != digest:
            raise ValueError("Retained priority source differs from its receipt")


def matching_priority_witnesses(con, root: Path, table_id: str, request: dict) -> list[dict]:
    """Read intersecting original receipts; never replay an unknown outcome."""
    codes, periods, fields = _scope(request)
    placeholders = ",".join("?" for _ in codes)
    rows = con.execute(
        f"SELECT DISTINCT t.* FROM {INDEX_TABLE} i JOIN tasks t ON t.task_id=i.task_id "
        f"WHERE i.table_id=? AND i.company_code IN ({placeholders}) AND "
        "(t.state='complete' OR (t.state='pending' AND "
        "t.last_error_code='source_company_labels_absent_fragment'))",
        (table_id, *codes)).fetchall()
    if not rows:
        return []
    definition = con.execute("SELECT * FROM tables WHERE table_id=?", (table_id,)).fetchone()
    if definition is None:
        raise ValueError("Priority coverage table disappeared")
    result = []
    for row in rows:
        task = dict(row)
        if task["table_id"] != table_id or task["scope_contract"] != SOURCE_SCOPE_CONTRACT:
            raise ValueError("Priority coverage index task identity changed")
        other = expand_request(json.loads(task["request_json"]), dict(definition), table_id)
        other_codes, other_periods, other_fields = _scope(other)
        if (not set(codes).intersection(other_codes) or not set(periods).intersection(other_periods)
                or not fields.intersection(other_fields)):
            continue
        if task["state"] == "complete":
            _retained_receipt(root, task)
        result.append({"task_id": task["task_id"], "request": other,
                       "state": task["state"]})
    return result
