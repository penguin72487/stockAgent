"""Stable logical state for an exported engineering-control DB snapshot."""
from __future__ import annotations

import hashlib

from stockagent.runtime_identity import identity_sha256

STATE_CONTRACT = "all_user_table_columns_sorted_jsonb_rows_utc_v1"


def logical_database_state(connection, *, schemas: tuple[str, ...] | None = None) -> dict:
    """Fingerprint actual rows and columns, using the caller's MVCC snapshot.

    UTC and C collation make the result independent of host/session defaults.
    Stream row hashes instead of creating a second complete database copy.
    """
    from psycopg import sql

    connection.execute("SET LOCAL TIME ZONE 'UTC'")
    tables = connection.execute("""SELECT n.nspname, c.relname, c.oid FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relkind IN ('r','p') AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname<>'information_schema'
        ORDER BY n.nspname COLLATE "C", c.relname COLLATE "C"
        """).fetchall()
    records = []
    for schema, table, oid in tables:
        if schemas is not None and schema not in schemas:
            continue
        columns = connection.execute("""SELECT a.attname, format_type(a.atttypid,a.atttypmod),
            a.attnotnull, pg_get_expr(d.adbin,d.adrelid)
            FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
            WHERE a.attrelid=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum""", (oid,)).fetchall()
        statement = sql.SQL('SELECT to_jsonb(t)::text FROM {}.{} t ORDER BY to_jsonb(t)::text COLLATE "C"').format(
            sql.Identifier(schema), sql.Identifier(table))
        digest = hashlib.sha256(); count = 0
        # A server cursor keeps large work histories out of Python RAM.
        with connection.cursor(name='state_'+hashlib.sha256(f'{schema}.{table}'.encode()).hexdigest()[:24]) as cursor:
            cursor.execute(statement)
            for (text,) in cursor:
                body = text.encode();digest.update(len(body).to_bytes(8,'big'));digest.update(body);count += 1
        records.append({'schema':schema,'table':table,'columns':[list(row) for row in columns],
                        'rows':count,'rows_sha256':digest.hexdigest()})
    state = {'contract':STATE_CONTRACT,'tables':records}
    return {**state,'identity_sha256':identity_sha256(state),'table_count':len(records),
            'row_count':sum(row['rows'] for row in records)}
