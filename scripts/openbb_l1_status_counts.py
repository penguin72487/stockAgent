"""Exact L1 membership counts using bounded endpoint index seeks.

The recursive query still counts every member. It only removes the per-row
GROUP BY machinery, not an integrity check or a source observation. Unknown
schemas, collations, indexes and filtered calls retain the canonical query.
"""
from __future__ import annotations

import sqlite3
from typing import MutableMapping, Sequence


MEMBER_TABLE = "l1_compaction_members"
ENDPOINT_INDEX = "idx_l1_members_endpoint"
POLICY = "openbb_l1_endpoint_seek_counts_v1"
SEEK_SQL = f"""
    WITH RECURSIVE endpoints(endpoint) AS (
        SELECT (SELECT MIN(endpoint) FROM {MEMBER_TABLE})
        UNION ALL
        SELECT (SELECT MIN(endpoint) FROM {MEMBER_TABLE}
                WHERE endpoint > e.endpoint)
        FROM endpoints AS e WHERE e.endpoint IS NOT NULL
    )
    SELECT e.endpoint,
           (SELECT COUNT(*) FROM {MEMBER_TABLE} AS m
            WHERE m.endpoint=e.endpoint) AS source_files
    FROM endpoints AS e WHERE e.endpoint IS NOT NULL
    ORDER BY e.endpoint
"""


def _canonical_seek_schema(connection: sqlite3.Connection) -> bool:
    """Only opt in to the maintained non-NULL TEXT/BINARY endpoint index."""
    schema = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (MEMBER_TABLE,)
    ).fetchone()
    if schema is None or not schema[0] or "COLLATE" in str(schema[0]).upper():
        return False
    columns = connection.execute(f"PRAGMA table_info({MEMBER_TABLE})").fetchall()
    if not any(str(row[1]) == "endpoint" and str(row[2]).upper() == "TEXT"
               and row[3] == 1 for row in columns):
        return False
    indexes = connection.execute(f"PRAGMA index_list({MEMBER_TABLE})").fetchall()
    if not any(str(row[1]) == ENDPOINT_INDEX and row[4] == 0 for row in indexes):
        return False
    keys = [row for row in connection.execute(f"PRAGMA index_xinfo({ENDPOINT_INDEX})")
            if row[5] == 1]
    return len(keys) == 1 and str(keys[0][2]) == "endpoint" and keys[0][4] == "BINARY"


def member_endpoint_counts(
    connection: sqlite3.Connection,
    filter_clause: str,
    filter_parameters: Sequence[object],
    *,
    audit: MutableMapping[str, object] | None = None,
) -> dict[str, int]:
    """One-statement snapshot, exact counts, with the original fallback."""
    use_seeks = not filter_clause and not filter_parameters and _canonical_seek_schema(connection)
    sql = SEEK_SQL if use_seeks else (
        f"SELECT m.endpoint, COUNT(*) AS source_files FROM {MEMBER_TABLE} AS m "
        f"WHERE 1=1{filter_clause} GROUP BY m.endpoint"
    )
    rows = connection.execute(sql, () if use_seeks else filter_parameters)
    result = {str(row[0]): int(row[1]) for row in rows}
    if audit is not None:
        audit.update(policy=POLICY, path="endpoint_index_seeks" if use_seeks else "canonical_group_by",
                     endpoints=len(result), members=sum(result.values()))
    return result
