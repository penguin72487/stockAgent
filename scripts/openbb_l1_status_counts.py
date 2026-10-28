"""Exact L1 membership counts using seeks or opt-in transactional aggregates.

The recursive query still counts every member. It only removes the per-row
GROUP BY machinery, not an integrity check or a source observation. Unknown
schemas, collations, indexes and filtered calls retain the canonical query.
Enrolled canonical writers may maintain exact totals in SQLite triggers; readers
reuse them only with verified hooks and a compatible bootstrap schema cookie.
"""
from __future__ import annotations

import sqlite3
import json
from typing import MutableMapping, Sequence


MEMBER_TABLE = "l1_compaction_members"
ENDPOINT_INDEX = "idx_l1_members_endpoint"
POLICY = "openbb_l1_endpoint_seek_counts_v1"
INCREMENTAL_POLICY = 'openbb_l1_transactional_member_counts_v1'
COUNT_TABLE = 'l1_member_endpoint_counts_v1'
CHECKPOINT = 'l1_member_endpoint_counts_checkpoint_v1'
_COUNT_DDL = f'CREATE TABLE IF NOT EXISTS {COUNT_TABLE} (endpoint TEXT PRIMARY KEY, members INTEGER NOT NULL CHECK(members>=0))'
_TRIGGERS = {
    # SQLite REPLACE can silently delete a conflicting row without firing its
    # delete hook when recursive_triggers is off. The canonical member writer
    # inserts new unique task IDs; reject replacement rather than miscount it.
    'l1_member_count_unique_insert_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_member_count_unique_insert_v1
        BEFORE INSERT ON {MEMBER_TABLE} WHEN EXISTS(SELECT 1 FROM {MEMBER_TABLE} WHERE task_id=NEW.task_id)
        BEGIN SELECT RAISE(ABORT,'L1 member replacement requires explicit delete'); END''',
    'l1_member_count_unique_update_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_member_count_unique_update_v1
        BEFORE UPDATE OF task_id ON {MEMBER_TABLE} WHEN NEW.task_id IS NOT OLD.task_id
        AND EXISTS(SELECT 1 FROM {MEMBER_TABLE} WHERE task_id=NEW.task_id)
        BEGIN SELECT RAISE(ABORT,'L1 member replacement requires explicit delete'); END''',
    'l1_member_count_insert_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_member_count_insert_v1
        AFTER INSERT ON {MEMBER_TABLE} BEGIN
        INSERT INTO {COUNT_TABLE}(endpoint,members) VALUES(NEW.endpoint,1)
        ON CONFLICT(endpoint) DO UPDATE SET members=members+1; END''',
    'l1_member_count_delete_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_member_count_delete_v1
        AFTER DELETE ON {MEMBER_TABLE} BEGIN
        UPDATE {COUNT_TABLE} SET members=members-1 WHERE endpoint=OLD.endpoint;
        DELETE FROM {COUNT_TABLE} WHERE endpoint=OLD.endpoint AND members=0; END''',
    'l1_member_count_update_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_member_count_update_v1
        AFTER UPDATE OF endpoint ON {MEMBER_TABLE} WHEN OLD.endpoint IS NOT NEW.endpoint BEGIN
        UPDATE {COUNT_TABLE} SET members=members-1 WHERE endpoint=OLD.endpoint;
        DELETE FROM {COUNT_TABLE} WHERE endpoint=OLD.endpoint AND members=0;
        INSERT INTO {COUNT_TABLE}(endpoint,members) VALUES(NEW.endpoint,1)
        ON CONFLICT(endpoint) DO UPDATE SET members=members+1; END''',
}


def _normalize(sql):
    return ' '.join(sql.replace('IF NOT EXISTS ', '').split())


def _count_schema_valid(connection):
    expected = {COUNT_TABLE: _COUNT_DDL, **_TRIGGERS}
    rows = connection.execute('SELECT name,sql FROM sqlite_schema WHERE name IN ('
                              + ','.join('?' for _ in expected) + ')', tuple(expected))
    actual = {str(row[0]): _normalize(str(row[1])) for row in rows}
    return actual == {name: _normalize(ddl) for name, ddl in expected.items()}


def install_member_count_proof(connection):
    """Install before journal watermarks; populate only after all DDL is done."""
    # Preserve the historical early-rejection boundary: an unsupported source
    # schema must not enter a context that could roll back the caller's work.
    if not _canonical_seek_schema(connection):
        raise RuntimeError('Incremental member counts require the canonical endpoint schema')
    with connection:
        # sqlite3's connection context does not begin a transaction for DDL.
        # One explicit writer transaction makes the complete hook installation
        # atomic and avoids an individual durable commit for every new hook.
        if not connection.in_transaction:
            connection.execute('BEGIN IMMEDIATE')
        if not _canonical_seek_schema(connection):
            raise RuntimeError('Incremental member counts require the canonical endpoint schema')
        connection.execute(_COUNT_DDL)
        for ddl in _TRIGGERS.values():
            connection.execute(ddl)
        if not _count_schema_valid(connection):
            raise RuntimeError('OpenBB member count proof schema changed')


def _count_proof_valid(connection):
    if not _count_schema_valid(connection):
        return False
    row = connection.execute('SELECT value FROM archive_meta WHERE key=?', (CHECKPOINT,)).fetchone()
    try:
        proof = json.loads(row[0]) if row else None
    except (TypeError, ValueError):
        return False
    return proof == dict(policy=INCREMENTAL_POLICY,
                         schema_version=int(connection.execute('PRAGMA schema_version').fetchone()[0]))


def baseline_member_count_proof(connection):
    """Bootstrap exact totals and trigger coverage in one writer transaction."""
    with connection:
        # BEGIN IMMEDIATE also covers a missing/invalid proof: no writer can
        # insert between the snapshot baseline and its committed watermark.
        if not connection.in_transaction:
            connection.execute('BEGIN IMMEDIATE')
        if _count_proof_valid(connection):
            return False
        if not _count_schema_valid(connection):
            raise RuntimeError('OpenBB member count hooks changed before bootstrap')
        connection.execute(f'DELETE FROM {COUNT_TABLE}')
        connection.execute(f'INSERT INTO {COUNT_TABLE} SELECT endpoint,COUNT(*) FROM {MEMBER_TABLE} GROUP BY endpoint')
        proof = json.dumps(dict(policy=INCREMENTAL_POLICY,
                                schema_version=int(connection.execute('PRAGMA schema_version').fetchone()[0])))
        columns = {str(row[1]) for row in connection.execute('PRAGMA table_info(archive_meta)')}
        if 'updated_at' in columns:
            connection.execute('INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,CURRENT_TIMESTAMP) '
                               'ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at',
                               (CHECKPOINT, proof))
        else:
            connection.execute('INSERT INTO archive_meta(key,value) VALUES(?,?) '
                               'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (CHECKPOINT, proof))
        return True
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
    incremental: bool = False,
) -> dict[str, int]:
    """One-statement snapshot, exact counts, with the original fallback."""
    if incremental and not filter_clause and not filter_parameters:
        # Hold validation and the small aggregate read in one SQLite snapshot.
        connection.execute('SAVEPOINT l1_member_count_read')
        try:
            if _count_proof_valid(connection):
                result = {str(row[0]): int(row[1]) for row in connection.execute(
                    f'SELECT endpoint,members FROM {COUNT_TABLE} ORDER BY endpoint')}
                if audit is not None:
                    audit.update(policy=INCREMENTAL_POLICY, path='transactional_counts',
                                 endpoints=len(result), members=sum(result.values()))
                return result
        finally:
            connection.execute('RELEASE l1_member_count_read')
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
