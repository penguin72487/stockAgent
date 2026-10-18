"""Exact connection-local row deltas for the canonical FinMind owner.

Not a queue owner or a TTL: no main-schema/data/receipt changes. TEMP triggers
observe this connection's writes; another connection's commit invalidates the
whole memo through SQLite data_version. Uncommitted reads never populate it.
"""
from __future__ import annotations

import sqlite3
import json

AGGREGATE_SQL = (
    "SELECT dataset,state,COUNT(*),SUM(rows),SUM(bytes),MIN(first_data_date),"
    "MAX(last_data_date),MAX(last_attempt_at_utc) FROM tasks GROUP BY dataset,state"
)
GROUP_SQL = AGGREGATE_SQL.replace(" GROUP BY", " WHERE dataset=? AND state=? GROUP BY")
FUNCTION = "_finmind_status_dirty_v1"
ROW_FUNCTION = "_finmind_status_before_row_v2"
PREFIX = "_finmind_status_memo_v1_"
MAX_GROUPS = 4096
CONTRACT_VERSION = 2
ROW_SQL = ("SELECT state,rows,bytes,first_data_date,last_data_date,last_attempt_at_utc "
           "FROM tasks WHERE dataset=? AND data_id=? AND partition=?")


class _Memo:
    def __init__(self):
        self.groups = None
        self.before = {}
        self.all_dirty = True
        self.version = None
        self.temp_schema_version = None
        self.reported_modes = set()

    def mark(self, dataset, state):
        # Inserts/identity changes invalidate everything, including REPLACE's
        # implicit delete, whose DELETE trigger need not fire in SQLite.
        self.all_dirty = True
        self.before.clear()

    def observe(self, dataset, data_id, partition, present, *values):
        """Remember the first pre-write value, never a tentative SQL result.

        At the next committed read, fetch the actual row by its primary key.
        Thus rollback, ignored INSERT, aborted statements and savepoints need
        no Python imitation of SQLite's transaction/savepoint stack.
        """
        if self.all_dirty:
            return
        key = (dataset, data_id, partition)
        if len(self.before) >= MAX_GROUPS and key not in self.before:
            self.mark(None, None)
        else:
            self.before.setdefault(key, (dataset, *values) if present else None)


class StatusConnection(sqlite3.Connection):
    """Owner connection; ordinary/read-only connections retain the full query."""


def _install(connection, memo):
    connection.create_function(FUNCTION, 2, memo.mark)
    connection.create_function(ROW_FUNCTION, 10, memo.observe)
    # execute(), never executescript(): installing observation must not commit
    # an existing transaction. TEMP objects are connection-local and ephemeral.
    statements = {
        # BEFORE INSERT sees the replaced row even with recursive_triggers=OFF.
        # INSERT OR IGNORE / UPSERT may not insert: actual committed readback
        # below, rather than an assumed insert delta, resolves both exactly.
        "insert": f"BEFORE INSERT ON main.tasks BEGIN SELECT {ROW_FUNCTION}("
                  "NEW.dataset,NEW.data_id,NEW.partition,prior.rowid IS NOT NULL,"
                  "prior.state,prior.rows,prior.bytes,prior.first_data_date,"
                  "prior.last_data_date,prior.last_attempt_at_utc) FROM (SELECT 1) "
                  "LEFT JOIN main.tasks AS prior ON prior.dataset=NEW.dataset "
                  "AND prior.data_id=NEW.data_id AND prior.partition=NEW.partition; END",
        "delete": f"AFTER DELETE ON main.tasks BEGIN SELECT {ROW_FUNCTION}("
                  "OLD.dataset,OLD.data_id,OLD.partition,1,OLD.state,OLD.rows,OLD.bytes,"
                  "OLD.first_data_date,OLD.last_data_date,OLD.last_attempt_at_utc); END",
        "identity": f"AFTER UPDATE OF dataset,data_id,partition ON main.tasks "
                    f"WHEN OLD.dataset IS NOT NEW.dataset OR OLD.data_id IS NOT NEW.data_id "
                    f"OR OLD.partition IS NOT NEW.partition BEGIN SELECT {FUNCTION}(NULL,NULL); END",
        "update": "AFTER UPDATE OF state,rows,bytes,first_data_date,last_data_date,last_attempt_at_utc "
                  "ON main.tasks WHEN OLD.state IS NOT NEW.state OR OLD.rows IS NOT NEW.rows "
                  "OR OLD.bytes IS NOT NEW.bytes OR OLD.first_data_date IS NOT NEW.first_data_date "
                  "OR OLD.last_data_date IS NOT NEW.last_data_date "
                  "OR OLD.last_attempt_at_utc IS NOT NEW.last_attempt_at_utc BEGIN "
                  f"SELECT {ROW_FUNCTION}(OLD.dataset,OLD.data_id,OLD.partition,1,"
                  "OLD.state,OLD.rows,OLD.bytes,OLD.first_data_date,OLD.last_data_date,"
                  "OLD.last_attempt_at_utc); END",
    }
    for suffix, statement in statements.items():
        connection.execute(f"DROP TRIGGER IF EXISTS temp.{PREFIX}{suffix}")
        connection.execute(f"CREATE TEMP TRIGGER {PREFIX}{suffix} {statement}")
    memo.all_dirty = True
    memo.temp_schema_version = connection.execute("PRAGMA temp.schema_version").fetchone()[0]


def _row_deltas(connection, memo):
    groups = {key: list(row) for key, row in memo.groups.items()}
    fallback, removed, added = set(), {}, {}
    for identity, before in memo.before.items():
        current = connection.execute(ROW_SQL, identity).fetchone()
        after = (identity[0], *current) if current else None
        if before == after:
            continue
        for sign, value in ((-1, before), (1, after)):
            if value is None:
                continue
            key = value[:2]
            row = groups.setdefault(key, [*key, 0, 0, 0, None, None, None])
            row[2] += sign
            for column in (3, 4):
                number = value[column - 1]
                if (type(number) is not int or number < 0 or type(row[column]) is not int
                        or row[column] < 0):
                    # Retain SQL NULL/REAL/overflow semantics, not a new sum ABI.
                    fallback.add(key)
                else:
                    row[column] += sign * number
                    if not -(2**63) <= row[column] < 2**63:
                        fallback.add(key)
            for column in (5, 6, 7):
                number = value[column - 1]
                if number is None:
                    continue
                if (not isinstance(number, str)
                        or row[column] is not None and not isinstance(row[column], str)):
                    fallback.add(key)
                    continue
                if sign < 0:
                    original = memo.groups.get(key)
                    if original and number == original[column]:
                        removed[key, column] = number
                else:
                    previous = row[column]
                    if previous is None or (number < previous if column == 5 else number > previous):
                        row[column] = number
                    candidate = added.get((key, column))
                    if candidate is None or (number < candidate if column == 5 else number > candidate):
                        added[key, column] = number
    for (key, column), previous in removed.items():
        candidate = added.get((key, column))
        if candidate is None or (candidate > previous if column == 5 else candidate < previous):
            fallback.add(key)
    for key in list(groups):
        if groups[key][2] == 0:
            groups.pop(key)
            fallback.discard(key)
        elif groups[key][2] < 0:
            fallback.add(key)
    for key in sorted(fallback):
        rows = connection.execute(GROUP_SQL, key).fetchall()
        if rows:
            groups[key] = list(rows[0])
        else:
            groups.pop(key, None)
    return {key: tuple(row) for key, row in groups.items()}, len(fallback)


def task_status_aggregates(connection, *, report_first_use=False):
    """Return exactly the canonical SQL aggregates, in dataset/state order.

    Read-only/foreign connections and reads inside a write transaction use the
    original full scan. The Python dirty set intentionally survives rollback;
    over-invalidation is safe, but caching uncommitted results would not be.
    """
    if not isinstance(connection, StatusConnection) or connection.in_transaction:
        return connection.execute(AGGREGATE_SQL).fetchall()
    memo = getattr(connection, "_finmind_status_memo", None)
    if memo is None:
        memo = connection._finmind_status_memo = _Memo()
        _install(connection, memo)
    elif memo.temp_schema_version != connection.execute("PRAGMA temp.schema_version").fetchone()[0]:
        _install(connection, memo)
    connection.execute("SAVEPOINT finmind_status_memo_read_v1")
    try:
        version = (connection.execute("PRAGMA main.data_version").fetchone()[0],
                   connection.execute("PRAGMA main.schema_version").fetchone()[0])
        if memo.groups is None or memo.all_dirty or version != memo.version:
            rows = connection.execute(AGGREGATE_SQL).fetchall()
            groups = {(row[0], row[1]): row for row in rows}
            mode, recomputed = "full", len(groups)
        else:
            groups, recomputed = _row_deltas(connection, memo)
            mode = "changed_rows" if memo.before else "unchanged"
        connection.execute("RELEASE finmind_status_memo_read_v1")
    except BaseException:
        connection.execute("ROLLBACK TO finmind_status_memo_read_v1")
        connection.execute("RELEASE finmind_status_memo_read_v1")
        memo.all_dirty = True
        raise
    if len(groups) <= MAX_GROUPS:
        memo.groups, memo.version = groups, version
        memo.before.clear()
        memo.all_dirty = False
    else:
        memo.groups = None
        memo.all_dirty = True
    if report_first_use and mode not in memo.reported_modes:
        print(json.dumps({"finmind_status_aggregation": {
            "contract_version": CONTRACT_VERSION, "mode": mode,
            "groups_recomputed": recomputed, "total_groups": len(groups),
        }}), flush=True)
        memo.reported_modes.add(mode)
    return [groups[key] for key in sorted(groups)]
