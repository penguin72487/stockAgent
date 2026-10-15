"""Verify the physical task lookup before binding the measured L1 query.

This helper performs no DDL. An absent or incompatible index leaves the
canonical query untouched. Source predicates and mutation-journal ownership
remain in their original modules, including the daily full audit.
"""

from __future__ import annotations

import re
import sqlite3

from scripts.openbb_l1_source_journal import TRACKED_TASK_COLUMNS

TASK_CONTRACT_INDEX = "idx_l1_tasks_source_contract_cover"
TASK_CONTRACT_COLUMNS = (
    "task_id",
    "active",
    "status",
    "endpoint",
    "output_path",
    "rows",
    "updated_at",
)
LOOKUP_POLICY = "openbb_verified_task_contract_covering_lookup_v1"


def verified_task_contract_index(
    connection: sqlite3.Connection,
    *,
    name: str = TASK_CONTRACT_INDEX,
) -> str | None:
    """Accept only the measured full, ascending, BINARY, nonunique layout.

    The explicit name argument lets a private benchmark verify its own alias;
    canonical callers use the fixed production name. Names are identifiers,
    never caller-supplied SQL. Checking the actual xinfo also rejects expression
    indexes and implicit column collations that a DDL substring check misses.
    """
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
        raise ValueError("task contract index must have a safe identifier")
    if frozenset(TASK_CONTRACT_COLUMNS) != TRACKED_TASK_COLUMNS:
        raise RuntimeError("task lookup and source mutation contract differ")
    index = next(
        (
            row
            for row in connection.execute("PRAGMA main.index_list(tasks)")
            if str(row[1]) == name
        ),
        None,
    )
    if index is None or bool(index[2]) or str(index[3]) != "c" or bool(index[4]):
        return None
    keys = [
        row for row in connection.execute(f"PRAGMA main.index_xinfo({name})") if row[5]
    ]
    if tuple(str(row[2]) for row in keys) != TASK_CONTRACT_COLUMNS or any(
        int(row[1]) < 0 or bool(row[3]) or str(row[4]).upper() != "BINARY"
        for row in keys
    ):
        return None
    return name


def bind_task_contract_index(query: str, index: str | None) -> str:
    """Change only the exact shared lookup; never rewrite predicates."""
    if index is None:
        return query
    if not isinstance(index, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", index):
        raise ValueError("task contract index must have a safe identifier")
    original = "LEFT JOIN tasks AS t ON t.task_id=m.task_id"
    if query.count(original) != 1:
        raise RuntimeError(
            "canonical task lookup shape changed; refusing an implicit rewrite"
        )
    return query.replace(
        original, f"LEFT JOIN tasks AS t INDEXED BY {index} ON t.task_id=m.task_id"
    )
