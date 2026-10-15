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
