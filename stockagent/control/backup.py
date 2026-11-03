"""Stable logical state for an exported engineering-control DB snapshot."""
from __future__ import annotations

import hashlib

from stockagent.runtime_identity import identity_sha256

STATE_CONTRACT = "all_user_table_columns_sorted_jsonb_rows_utc_v1"


def control_backup_capacity(*, free_bytes: int, required_growth_bytes: int, configuration: dict) -> dict:
    """Admit only SQL backup scratch into an explicitly enrolled backup pool.

    General source acquisition keeps its original reserve. This maintenance
    class retains the native D writer's 32 GiB floor and rechecks real free
    space before each dump is written. It never grants retirement or NAS ACK.
    """
    normal = configuration['reserve_bytes']
    if any(type(value) is not int or value < 0 for value in (free_bytes, required_growth_bytes, normal)):
        raise ValueError('control backup capacity requires nonnegative integer bytes')
    policy = configuration.get('lifecycle_sql_backup_capacity', {})
    if not isinstance(policy, dict) or type(policy.get('use_backup_reserve', False)) is not bool:
        raise ValueError('invalid lifecycle SQL backup capacity policy')
    reserve = normal
    if policy.get('use_backup_reserve'):
        if (policy.get('contract') != 'lifecycle_sql_backup_reserve_v1'
                or not isinstance(policy.get('user_instruction'), str)
                or not policy['user_instruction'].strip() or normal < 32 * 1024**3):
            raise ValueError('SQL backup reserve use needs its explicit local enrollment')
        reserve = 32 * 1024**3
    required = reserve + required_growth_bytes
    return {'contract': 'stockagent.lifecycle_sql_capacity.v1',
        'state': 'admitted' if free_bytes >= required else 'waiting_capacity',
        'physical_free_bytes': free_bytes, 'required_growth_bytes': required_growth_bytes,
        'operational_floor_bytes': reserve, 'ordinary_reserve_bytes': normal,
        'using_backup_reserve': reserve < normal, 'required_free_bytes': required,
        'source_retirement': False, 'nas_acceptance': False}


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
