"""Exact connection-local group invalidation for the canonical FinMind owner.

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
PREFIX = "_finmind_status_memo_v1_"
MAX_GROUPS = 4096
CONTRACT_VERSION = 1


class _Memo:
    def __init__(self):
        self.groups = None
        self.dirty = set()
        self.all_dirty = True
        self.version = None
        self.temp_schema_version = None
        self.reported_modes = set()

    def mark(self, dataset, state):
        # Inserts/identity changes invalidate everything, including REPLACE's
        # implicit delete, whose DELETE trigger need not fire in SQLite.
        if dataset is None or len(self.dirty) >= MAX_GROUPS:
            self.all_dirty = True
            self.dirty.clear()
        elif not self.all_dirty:
            self.dirty.add((dataset, state))


class StatusConnection(sqlite3.Connection):
    """Owner connection; ordinary/read-only connections retain the full query."""


def _install(connection, memo):
    connection.create_function(FUNCTION, 2, memo.mark)
    # execute(), never executescript(): installing observation must not commit
    # an existing transaction. TEMP objects are connection-local and ephemeral.
    statements = {
        "insert": f"AFTER INSERT ON main.tasks BEGIN SELECT {FUNCTION}(NULL,NULL); END",
        "delete": f"AFTER DELETE ON main.tasks BEGIN SELECT {FUNCTION}(NULL,NULL); END",
        "identity": f"AFTER UPDATE OF dataset,data_id,partition ON main.tasks "
                    f"WHEN OLD.dataset IS NOT NEW.dataset OR OLD.data_id IS NOT NEW.data_id "
                    f"OR OLD.partition IS NOT NEW.partition BEGIN SELECT {FUNCTION}(NULL,NULL); END",
        "update": "AFTER UPDATE OF state,rows,bytes,first_data_date,last_data_date,last_attempt_at_utc "
                  "ON main.tasks WHEN OLD.state IS NOT NEW.state OR OLD.rows IS NOT NEW.rows "
                  "OR OLD.bytes IS NOT NEW.bytes OR OLD.first_data_date IS NOT NEW.first_data_date "
                  "OR OLD.last_data_date IS NOT NEW.last_data_date "
                  "OR OLD.last_attempt_at_utc IS NOT NEW.last_attempt_at_utc BEGIN "
                  f"SELECT {FUNCTION}(OLD.dataset,OLD.state); "
                  f"SELECT {FUNCTION}(NEW.dataset,NEW.state); END",
    }
    for suffix, statement in statements.items():
        connection.execute(f"DROP TRIGGER IF EXISTS temp.{PREFIX}{suffix}")
        connection.execute(f"CREATE TEMP TRIGGER {PREFIX}{suffix} {statement}")
    memo.all_dirty = True
    memo.temp_schema_version = connection.execute("PRAGMA temp.schema_version").fetchone()[0]


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
            groups = memo.groups.copy()
            mode = "changed_groups" if memo.dirty else "unchanged"
            recomputed = len(memo.dirty)
            for key in sorted(memo.dirty):
                rows = connection.execute(GROUP_SQL, key).fetchall()
                if rows:
                    groups[key] = rows[0]
                else:
                    groups.pop(key, None)
        connection.execute("RELEASE finmind_status_memo_read_v1")
    except BaseException:
        connection.execute("ROLLBACK TO finmind_status_memo_read_v1")
        connection.execute("RELEASE finmind_status_memo_read_v1")
        memo.all_dirty = True
        raise
    if len(groups) <= MAX_GROUPS:
        memo.groups, memo.version = groups, version
        memo.dirty.clear()
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
