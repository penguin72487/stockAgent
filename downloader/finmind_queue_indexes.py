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
