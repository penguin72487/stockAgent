import sqlite3
import json

import pytest

from scripts.openbb_l1_unassigned_index import (
    baseline_unassigned_index, install_unassigned_index, unassigned_rows,
)


def database():
    connection = sqlite3.connect(':memory:')
    connection.executescript('''CREATE TABLE tasks(task_id TEXT PRIMARY KEY, endpoint TEXT,
        plan_token TEXT, active INTEGER, status TEXT, output_path TEXT, rows INTEGER, updated_at TEXT);
        CREATE TABLE l1_compaction_members(task_id TEXT PRIMARY KEY,endpoint TEXT);
        CREATE TABLE archive_meta(key TEXT PRIMARY KEY,value TEXT);''')
    connection.executemany('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',[
        ('a','e1','plan1',1,'success','a',1,'original-a'),
        ('b','e1','plan1',1,'success','b',2,'original-b'),
        ('c','e2','plan2',1,'pending','c',0,'original-c'),
        ('d','e2','plan2',0,'success','d',4,'original-d'),
    ])
    connection.execute("INSERT INTO l1_compaction_members VALUES('a','e1')")
    connection.commit()
    install_unassigned_index(connection)
    assert baseline_unassigned_index(connection)
    assert not baseline_unassigned_index(connection)
    return connection


def reference(connection, plan, limit):
    clause = '' if plan is None else ' AND t.plan_token=?'
    return connection.execute('''SELECT t.task_id,t.endpoint,t.output_path,t.rows,t.updated_at
        FROM tasks AS t LEFT JOIN l1_compaction_members AS m ON m.task_id=t.task_id
        WHERE t.active=1 AND t.status='success' AND m.task_id IS NULL'''+clause+
        ' ORDER BY t.endpoint,t.task_id LIMIT ?', ([] if plan is None else [plan])+[limit]).fetchall()


@pytest.mark.parametrize('mutation',[
    "INSERT INTO tasks VALUES('new','e0','plan1',1,'success','new',3,'new-clock')",
    "DELETE FROM tasks WHERE task_id='b'",
    "UPDATE tasks SET status='success',rows=3 WHERE task_id='c'",
    "UPDATE tasks SET active=1 WHERE task_id='d'",
    "UPDATE tasks SET status='pending' WHERE task_id='b'",
    "UPDATE tasks SET plan_token='plan2',endpoint='e3' WHERE task_id='b'",
    "UPDATE tasks SET task_id='renamed',output_path='different',rows=7,updated_at='new-clock' WHERE task_id='b'",
    "DELETE FROM l1_compaction_members WHERE task_id='a'",
    "INSERT INTO l1_compaction_members VALUES('b','e1')",
    "UPDATE l1_compaction_members SET task_id='b' WHERE task_id='a'",
    "INSERT OR REPLACE INTO tasks VALUES('b','e4','plan2',1,'success','new',8,'new-clock')",
    # REPLACE may omit SQLite's delete hook. Even with a conservative retained
    # candidate key, the query must use the current task's eligibility/values.
    "INSERT OR REPLACE INTO tasks VALUES('b','e4','plan2',1,'failed','new',8,'new-clock')",
    "INSERT OR REPLACE INTO tasks VALUES('b','e4','plan2',0,'success','new',8,'new-clock')",
])
def test_pending_workset_tracks_both_join_sides_and_all_plan_tokens(mutation):
    connection = database()
    connection.execute(mutation)
    for plan in (None,'plan1','plan2','missing'):
        for limit in (1,100):
            assert unassigned_rows(connection,plan,'',(),limit)==reference(connection,plan,limit)
    connection.rollback()
    assert unassigned_rows(connection,None,'',(),100)==reference(connection,None,100)


def test_pending_workset_gap_rejects_then_rebuilds_full_reference():
    connection = database()
    connection.execute('DROP TRIGGER l1_pending_member_delete_v1')
    connection.execute("DELETE FROM l1_compaction_members WHERE task_id='a'")
    connection.commit()
    assert unassigned_rows(connection,None,'',(),100) is None
    install_unassigned_index(connection)
    assert baseline_unassigned_index(connection)
    assert unassigned_rows(connection,None,'',(),100)==reference(connection,None,100)


def test_pending_workset_snapshot_and_filter_preserve_source_values():
    connection = database()
    connection.execute("UPDATE tasks SET status='success' WHERE task_id='c'")
    connection.commit()
    connection.execute('PRAGMA query_only=ON')
    connection.execute('BEGIN')
    assert unassigned_rows(connection,None,' AND t.endpoint=?',('e2',),100)==[
        ('c','e2','c',0,'original-c')]
    assert connection.in_transaction


def test_non_success_writer_transitions_do_not_touch_pending_rows():
    connection = database()
    connection.execute("UPDATE tasks SET status='running' WHERE task_id='c'")
    assert connection.total_changes > 0
    before = connection.total_changes
    connection.execute("UPDATE tasks SET status='failed' WHERE task_id='c'")
    assert connection.total_changes-before == 1
    assert unassigned_rows(connection,None,'',(),100)==reference(connection,None,100)


@pytest.mark.parametrize('prior_gap', [False, True])
def test_witnessed_private_hook_migration_preserves_only_current_schema_watermarks(prior_gap):
    from scripts import openbb_l1_unassigned_index as owner
    connection = database()
    for name, ddl in owner._LEGACY_TRIGGERS.items():
        connection.execute(f'DROP TRIGGER {name}')
        connection.execute(ddl)
    cookie = connection.execute('PRAGMA schema_version').fetchone()[0]
    old = dict(policy=owner.POLICY, schema_version=cookie-int(prior_gap))
    source = dict(schema_version=cookie-int(prior_gap), cutoff_seq=17,
                  last_full_at_utc='2026-10-20T00:00:00+00:00', database_inode=5,
                  contract_fingerprint='original-full-contract')
    connection.execute('UPDATE archive_meta SET value=? WHERE key=?', (json.dumps(old),owner.KEY))
    for key in ('l1_source_audit_checkpoint_v1','l1_member_endpoint_counts_checkpoint_v1'):
        connection.execute('INSERT INTO archive_meta VALUES(?,?)', (key,json.dumps(source)))
    connection.commit()
    install_unassigned_index(connection)
    expected_cookie = cookie-int(prior_gap) if prior_gap else connection.execute('PRAGMA schema_version').fetchone()[0]
    for key in ('l1_source_audit_checkpoint_v1','l1_member_endpoint_counts_checkpoint_v1'):
        value = json.loads(connection.execute('SELECT value FROM archive_meta WHERE key=?',(key,)).fetchone()[0])
        assert value == {**source, 'schema_version':expected_cookie}
    assert baseline_unassigned_index(connection) is prior_gap
    assert unassigned_rows(connection,None,'',(),100)==reference(connection,None,100)


def test_unknown_pending_hook_is_rejected_without_replacing_it():
    connection = database()
    connection.execute('DROP TRIGGER l1_pending_task_insert_v1')
    sql = 'CREATE TRIGGER l1_pending_task_insert_v1 AFTER INSERT ON tasks BEGIN SELECT 42; END'
    connection.execute(sql)
    connection.commit()
    with pytest.raises(RuntimeError, match='schema changed'):
        install_unassigned_index(connection)
    assert connection.execute("SELECT sql FROM sqlite_schema WHERE name='l1_pending_task_insert_v1'").fetchone()[0]==sql
