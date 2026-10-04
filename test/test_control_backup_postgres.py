"""Real MVCC and restore-state semantics, confined to disposable test schemas."""
import os
import uuid

import pytest

from stockagent.control.backup import logical_database_state


@pytest.fixture
def database():
    dsn = os.environ.get('CONTROL_PLANE_TEST_DSN')
    if not dsn:
        pytest.skip('explicit PostgreSQL integration environment is not selected')
    import psycopg
    from psycopg import sql

    schema = 'backup_test_' + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        connection.execute(sql.SQL('''CREATE TABLE {}.observations (
            id bigint PRIMARY KEY, amount numeric(30,8) NOT NULL, metadata jsonb,
            observed_at timestamptz NOT NULL, description text DEFAULT 'unset')''').format(sql.Identifier(schema)))
        connection.execute(sql.SQL('''INSERT INTO {}.observations VALUES
            (1, 12345678901234567890.12345678, %s, '2026-10-03 15:00:00+08', %s),
            (2, -0.00000001, NULL, '2026-10-03 07:00:00+00', NULL)''').format(sql.Identifier(schema)),
                           ('{"中文": [null, true, 12345678901234567890]}', '中文\nexact row'))
        try:
            yield connection, dsn, schema
        finally:
            connection.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


def state(connection, schema):
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        return logical_database_state(connection, schemas=(schema,))


def test_snapshot_state_excludes_later_commits_and_preserves_exact_values(database):
    import psycopg
    from psycopg import sql

    connection, dsn, schema = database
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        first = logical_database_state(connection, schemas=(schema,))
        with psycopg.connect(dsn, autocommit=True) as writer:
            writer.execute(sql.SQL('UPDATE {}.observations SET amount=amount+1 WHERE id=1').format(sql.Identifier(schema)))
        assert logical_database_state(connection, schemas=(schema,)) == first
    changed = state(connection, schema)
    assert changed['row_count'] == first['row_count'] == 2
    assert changed['identity_sha256'] != first['identity_sha256']
    assert first['tables'][0]['columns'][1] == ['amount', 'numeric(30,8)', True, None]
    assert first['tables'][0]['columns'][4][3] == "'unset'::text"


def test_timezone_and_physical_row_order_do_not_change_logical_state(database):
    from psycopg import sql

    connection, _, schema = database
    connection.execute("SET TIME ZONE 'Asia/Taipei'")
    first = state(connection, schema)
    connection.execute(sql.SQL('CREATE TEMP TABLE reordered AS SELECT * FROM {}.observations ORDER BY id DESC').format(sql.Identifier(schema)))
    connection.execute(sql.SQL('TRUNCATE {}.observations').format(sql.Identifier(schema)))
    connection.execute(sql.SQL('INSERT INTO {}.observations SELECT * FROM reordered').format(sql.Identifier(schema)))
    connection.execute("SET TIME ZONE 'America/New_York'")
    assert state(connection, schema) == first


def test_column_default_nullability_and_type_are_part_of_state(database):
    from psycopg import sql

    connection, _, schema = database
    first = state(connection, schema)
    connection.execute(sql.SQL('ALTER TABLE {}.observations ALTER COLUMN description SET DEFAULT NULL').format(sql.Identifier(schema)))
    second = state(connection, schema)
    assert second['tables'][0]['rows_sha256'] == first['tables'][0]['rows_sha256']
    assert second['identity_sha256'] != first['identity_sha256']
    connection.execute(sql.SQL('ALTER TABLE {}.observations ADD COLUMN provenance text').format(sql.Identifier(schema)))
    assert state(connection, schema)['identity_sha256'] != second['identity_sha256']
