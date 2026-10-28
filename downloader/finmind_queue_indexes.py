"""Physical queue indexes; no cached counters, task transitions or API policy.

The exact heartbeat GROUP BY needs every aggregate column. A dataset/state-only
index forces a table lookup for each historical key. Keep that narrower index
for dispatch and add this covering projection for status reads. SQLite maintains
it in the same transaction as the task; no TTL or separate summary owner exists.
"""
from __future__ import annotations

import sqlite3

STATUS_INDEX = "idx_finmind_status_covering"
STATUS_COLUMNS = (
    "dataset", "state", "rows", "bytes", "first_data_date",
    "last_data_date", "last_attempt_at_utc",
)
MONITOR_COUNTS_INDEX = "idx_finmind_monitor_dates_typed"
MONITOR_COUNTS_COLUMNS = ("dataset", "typeof(rows)", "first_data_date", "last_data_date", "rows")
MONITOR_COUNTS_PREDICATE = "state='complete' AND rows>0"
RELEASE_RESERVE_INDEX = "idx_finmind_release_reserve_covering"
RELEASE_RESERVE_COLUMNS = ("priority", "state", "next_attempt_at_utc", "dataset", "kind",
                           "data_id", "error_code", "rows")
RELEASE_RESERVE_PREDICATE = "priority=0 AND kind!='derived'"
US_PUBLICATION_INDEX = "idx_finmind_us_unfinalized"
US_PUBLICATION_COLUMNS = ("dataset", "data_id", "partition", "last_attempt_at_utc")
US_PUBLICATION_PREDICATE = ("state='complete' AND rows>0 AND "
                            "julianday(last_attempt_at_utc)<julianday(partition)+2")
PERIODIC_INDEX = "idx_finmind_periodic_covering"
PERIODIC_COLUMNS = ("kind", "partition", "state", "dataset", "data_id", "last_attempt_at_utc",
                    "next_attempt_at_utc", "priority")
PERIODIC_PREDICATE = "kind IN ('snapshot','year')"
ETA_WORK_INDEX = "idx_finmind_eta_work_covering"
ETA_WORK_COLUMNS = ("dataset", "kind", "priority", "state", "next_attempt_at_utc")
ETA_RECENT_INDEX = "idx_finmind_eta_recent_covering"
ETA_RECENT_COLUMNS = ("last_attempt_at_utc", "state", "kind", "rows", "bytes")
ETA_RECENT_PREDICATE = "last_attempt_at_utc IS NOT NULL"
DEADLINE_INDEX = "idx_finmind_complement_queue"
DEADLINE_COLUMNS = ("priority", "next_attempt_at_utc", "state")


def ensure_eta_read_indexes(connection: sqlite3.Connection) -> None:
    """Owner-maintained projections for the minute-sampled exact ETA.

    Inventory still reads every registered task, but only these narrow keys.
    Recent outcomes seek by observation time instead of scanning all history.
    No counters, task changes, triggers or additional refresh owner are needed.
    Run the owner's bounded optimize_queue after setup, as for status indexes.
    """
    connection.execute(f"CREATE INDEX IF NOT EXISTS {ETA_WORK_INDEX} "
                       f"ON tasks({','.join(ETA_WORK_COLUMNS)})")
    connection.execute(f"CREATE INDEX IF NOT EXISTS {ETA_RECENT_INDEX} "
                       f"ON tasks({','.join(ETA_RECENT_COLUMNS)}) WHERE {ETA_RECENT_PREDICATE}")


def _eta_index(connection: sqlite3.Connection, name: str, columns: tuple[str, ...],
               predicate: str = "") -> str:
    row = connection.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)).fetchone()
    expected = f"CREATE INDEX {name} ON tasks({','.join(columns)})"
    if predicate:
        expected += f" WHERE {predicate}"
    return (f" INDEXED BY {name}"
            if row and isinstance(row[0], str) and ''.join(row[0].split()) == ''.join(expected.split()) else "")


def eta_work_index(connection: sqlite3.Connection) -> str:
    """Read-only, exact-schema hint; older owners keep their original query."""
    return _eta_index(connection, ETA_WORK_INDEX, ETA_WORK_COLUMNS)


def eta_recent_index(connection: sqlite3.Connection) -> str:
    return _eta_index(connection, ETA_RECENT_INDEX, ETA_RECENT_COLUMNS, ETA_RECENT_PREDICATE)


def deadline_index(connection: sqlite3.Connection) -> str:
    """The original priority/time projection, shared by both queue owners."""
    return _eta_index(connection, DEADLINE_INDEX, DEADLINE_COLUMNS)


def ensure_monitor_counts_index(connection: sqlite3.Connection) -> None:
    """Queue-owner projection for exact nonempty count/date monitor queries.

    A typed prefix makes the same-snapshot legacy-number guard a small seek.
    Only positive integer cohorts may change traversal order: floating sums
    retain the original status projection and its accumulation order.
    """
    connection.execute(f"CREATE INDEX IF NOT EXISTS {MONITOR_COUNTS_INDEX} "
                       f"ON tasks({','.join(MONITOR_COUNTS_COLUMNS)}) WHERE {MONITOR_COUNTS_PREDICATE}")


def monitor_counts_index(connection: sqlite3.Connection) -> str:
    return _eta_index(connection, MONITOR_COUNTS_INDEX, MONITOR_COUNTS_COLUMNS, MONITOR_COUNTS_PREDICATE)


def monitor_counts_sql(connection: sqlite3.Connection, query: str, parameters: tuple) -> str:
    """Choose a physical projection inside the caller's existing transaction.

    Counts remain exact SQL aggregates; no derived counters or task edits.
    This accepts only the bar owner's dataset cohort aggregate. Unknown query
    shapes and older owners retain their original SQL. All positive integers
    can be added in any order without changing SUM or overflow behavior.
    """
    if (query.count("FROM tasks WHERE dataset IN (") != 1
            or not query.endswith("AND state='complete' AND rows>0 GROUP BY dataset")):
        return query
    hint = monitor_counts_index(connection)
    if not hint:
        return query
    legacy_hint = _eta_index(connection, STATUS_INDEX, STATUS_COLUMNS)
    # The owner installs both indexes. Without its original projection there
    # is no proven legacy accumulation order to fall back to.
    if not legacy_hint:
        return query
    non_integer = connection.execute(
        f"SELECT 1 FROM tasks{hint} WHERE dataset IN ("
        + ",".join("?" for _ in parameters)
        + ") AND state='complete' AND rows>0 "
        "AND typeof(rows) IN ('null','real','text','blob') LIMIT 1", parameters,
    ).fetchone()
    if non_integer:
        return query.replace("FROM tasks WHERE", f"FROM tasks{legacy_hint} WHERE", 1)
    return query.replace("FROM tasks WHERE", f"FROM tasks{hint} WHERE", 1).replace(
        "GROUP BY dataset", "AND typeof(rows)='integer' GROUP BY dataset", 1)


def ensure_release_reserve_index(connection: sqlite3.Connection) -> None:
    """Original owner only: the small release-hour cohort, not all history."""
    connection.execute(f"CREATE INDEX IF NOT EXISTS {RELEASE_RESERVE_INDEX} "
                       f"ON tasks({','.join(RELEASE_RESERVE_COLUMNS)}) WHERE {RELEASE_RESERVE_PREDICATE}")


def ensure_us_publication_index(connection: sqlite3.Connection) -> None:
    """Keep early observations searchable without scanning finalized history."""
    connection.execute(f"CREATE INDEX IF NOT EXISTS {US_PUBLICATION_INDEX} "
                       f"ON tasks({','.join(US_PUBLICATION_COLUMNS)}) WHERE {US_PUBLICATION_PREDICATE}")


def ensure_periodic_index(connection: sqlite3.Connection) -> None:
    """Periodic policy reconciliation must not traverse all minute history."""
    connection.execute(f"CREATE INDEX IF NOT EXISTS {PERIODIC_INDEX} "
                       f"ON tasks({','.join(PERIODIC_COLUMNS)}) WHERE {PERIODIC_PREDICATE}")


def periodic_index(connection: sqlite3.Connection) -> str:
    row = connection.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                             (PERIODIC_INDEX,)).fetchone()
    expected = (f"CREATE INDEX {PERIODIC_INDEX} "
                f"ON tasks({','.join(PERIODIC_COLUMNS)}) WHERE {PERIODIC_PREDICATE}")
    return (f" INDEXED BY {PERIODIC_INDEX}"
            if row and isinstance(row[0], str) and row[0].split() == expected.split() else "")


def us_publication_index(connection: sqlite3.Connection) -> str:
    row = connection.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                             (US_PUBLICATION_INDEX,)).fetchone()
    expected = (f"CREATE INDEX {US_PUBLICATION_INDEX} "
                f"ON tasks({','.join(US_PUBLICATION_COLUMNS)}) WHERE {US_PUBLICATION_PREDICATE}")
    return (f" INDEXED BY {US_PUBLICATION_INDEX}"
            if row and isinstance(row[0], str) and row[0].split() == expected.split() else "")


def release_reserve_index(connection: sqlite3.Connection) -> str:
    """Pin only an exactly recognized physical projection; legacy readers work."""
    row = connection.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                             (RELEASE_RESERVE_INDEX,)).fetchone()
    expected = (f"CREATE INDEX {RELEASE_RESERVE_INDEX} "
                f"ON tasks({','.join(RELEASE_RESERVE_COLUMNS)}) WHERE {RELEASE_RESERVE_PREDICATE}")
    if not row or not isinstance(row[0], str) or row[0].split() != expected.split():
        return ""
    return f" INDEXED BY {RELEASE_RESERVE_INDEX}"


def ensure_status_index(connection: sqlite3.Connection) -> None:
    """Owner setup only; run its existing bounded optimize_queue afterwards.

    Creating an index without refreshing planner statistics is not a supported
    deployment: other task joins share this table. Do not remove the narrower
    existing indexes or maintain statistics from a read-only telemetry reader.
    """
    connection.execute(
        f"CREATE INDEX IF NOT EXISTS {STATUS_INDEX} "
        f"ON tasks({','.join(STATUS_COLUMNS)})"
    )
    ensure_monitor_counts_index(connection)


def dispatch_indexes(connection: sqlite3.Connection) -> tuple[str, str, str]:
    """Keep the new status projection out of outstanding/due admission scans.

    Fresh/legacy queues may not have useful statistics yet. Preserve the
    selector's existing bounded index contract independently of the heartbeat
    projection. Read-only older schemas retain their original automatic plan;
    this function neither creates an index nor changes a task/cursor.
    """
    names = (STATUS_INDEX, "idx_finmind_outstanding_dispatch",
             "idx_finmind_complement_queue", "idx_finmind_due_refresh")
    found = {row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name IN (?,?,?,?)", names
    )}
    if STATUS_INDEX not in found:
        return "", "", ""
    return tuple(f" INDEXED BY {name}" if name in found else "" for name in names[1:])
