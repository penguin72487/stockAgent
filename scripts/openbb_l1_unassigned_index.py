"""Exact pending-success workset maintained in the canonical SQLite transaction.

The initial/full rebuild uses the original task/member anti-join. Verified
triggers then capture both sides of that join; missing hooks or a schema-cookie
change require a full rebuild. All plan tokens are retained, so changing the
active plan does not hide previously pending work.
"""
from __future__ import annotations

import json

POLICY = 'openbb_l1_transactional_unassigned_workset_v1'
TABLE = 'l1_unassigned_success_v1'
KEY = 'l1_unassigned_success_checkpoint_v1'
_DDL = f'CREATE TABLE IF NOT EXISTS {TABLE} (task_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, plan_token TEXT)'
_INDEXES = {
    'idx_l1_unassigned_plan_v1': f'CREATE INDEX IF NOT EXISTS idx_l1_unassigned_plan_v1 ON {TABLE}(plan_token,endpoint,task_id)',
    'idx_l1_unassigned_order_v1': f'CREATE INDEX IF NOT EXISTS idx_l1_unassigned_order_v1 ON {TABLE}(endpoint,task_id)',
}


def _add_task(prefix):
    return f'''INSERT INTO {TABLE}(task_id,endpoint,plan_token)
        SELECT {prefix}.task_id,{prefix}.endpoint,{prefix}.plan_token
        WHERE {prefix}.active=1 AND {prefix}.status='success'
        AND NOT EXISTS(SELECT 1 FROM l1_compaction_members WHERE task_id={prefix}.task_id)
        ON CONFLICT(task_id) DO UPDATE SET endpoint=excluded.endpoint,plan_token=excluded.plan_token;'''


_TRIGGERS = {
    'l1_pending_task_insert_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_pending_task_insert_v1
        AFTER INSERT ON tasks BEGIN {_add_task('NEW')} END''',
    'l1_pending_task_delete_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_pending_task_delete_v1
        AFTER DELETE ON tasks BEGIN DELETE FROM {TABLE} WHERE task_id=OLD.task_id; END''',
    'l1_pending_task_update_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_pending_task_update_v1
        AFTER UPDATE ON tasks WHEN OLD.task_id IS NOT NEW.task_id OR OLD.endpoint IS NOT NEW.endpoint
        OR OLD.plan_token IS NOT NEW.plan_token OR OLD.active IS NOT NEW.active OR OLD.status IS NOT NEW.status
        BEGIN DELETE FROM {TABLE} WHERE task_id IN (OLD.task_id,NEW.task_id); {_add_task('NEW')} END''',
    'l1_pending_member_insert_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_pending_member_insert_v1
        AFTER INSERT ON l1_compaction_members BEGIN DELETE FROM {TABLE} WHERE task_id=NEW.task_id; END''',
    'l1_pending_member_delete_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_pending_member_delete_v1
        AFTER DELETE ON l1_compaction_members BEGIN
        INSERT INTO {TABLE}(task_id,endpoint,plan_token)
        SELECT task_id,endpoint,plan_token FROM tasks WHERE task_id=OLD.task_id AND active=1 AND status='success'
        AND NOT EXISTS(SELECT 1 FROM l1_compaction_members WHERE task_id=OLD.task_id)
        ON CONFLICT(task_id) DO UPDATE SET endpoint=excluded.endpoint,plan_token=excluded.plan_token; END''',
    'l1_pending_member_update_v1': f'''CREATE TRIGGER IF NOT EXISTS l1_pending_member_update_v1
        AFTER UPDATE OF task_id ON l1_compaction_members WHEN OLD.task_id IS NOT NEW.task_id BEGIN
        DELETE FROM {TABLE} WHERE task_id=NEW.task_id;
        INSERT INTO {TABLE}(task_id,endpoint,plan_token)
        SELECT task_id,endpoint,plan_token FROM tasks WHERE task_id=OLD.task_id AND active=1 AND status='success'
        AND NOT EXISTS(SELECT 1 FROM l1_compaction_members WHERE task_id=OLD.task_id)
        ON CONFLICT(task_id) DO UPDATE SET endpoint=excluded.endpoint,plan_token=excluded.plan_token; END''',
}

# Retain the exact former SQL as the only accepted in-place migration source.
# The candidate set invariant and policy are unchanged; only irrelevant writer
# states and needless delete/reinsert operations are removed.
_LEGACY_TRIGGERS = dict(_TRIGGERS)
_TRIGGERS['l1_pending_task_insert_v1'] = f'''CREATE TRIGGER IF NOT EXISTS l1_pending_task_insert_v1
    AFTER INSERT ON tasks WHEN NEW.active=1 AND NEW.status='success'
    BEGIN {_add_task('NEW')} END'''
_TRIGGERS['l1_pending_task_update_v1'] = f'''CREATE TRIGGER IF NOT EXISTS l1_pending_task_update_v1
    AFTER UPDATE ON tasks
    WHEN (OLD.active=1 AND OLD.status='success' OR NEW.active=1 AND NEW.status='success')
    AND (OLD.task_id IS NOT NEW.task_id OR OLD.endpoint IS NOT NEW.endpoint
         OR OLD.plan_token IS NOT NEW.plan_token OR OLD.active IS NOT NEW.active OR OLD.status IS NOT NEW.status)
    BEGIN
    DELETE FROM {TABLE} WHERE task_id=OLD.task_id
      AND (OLD.task_id IS NOT NEW.task_id OR NEW.active IS NOT 1 OR NEW.status IS NOT 'success');
    {_add_task('NEW')}
    END'''


def _normal(sql):
    return ' '.join(sql.replace('IF NOT EXISTS ', '').split())


def _schema_valid(connection, *, triggers=None):
    expected = {TABLE: _DDL, **_INDEXES, **(_TRIGGERS if triggers is None else triggers)}
    actual = dict(connection.execute('SELECT name,sql FROM sqlite_schema WHERE name IN ('
                                     + ','.join('?' for _ in expected) + ')', tuple(expected)))
    return {key: _normal(str(value)) for key, value in actual.items()} == {
        key: _normal(value) for key, value in expected.items()}


def _valid(connection):
    if not _schema_valid(connection):
        return False
    row = connection.execute('SELECT value FROM archive_meta WHERE key=?', (KEY,)).fetchone()
    try:
        proof = json.loads(row[0]) if row else None
    except (ValueError, TypeError):
        return False
    return proof == dict(policy=POLICY, schema_version=int(connection.execute('PRAGMA schema_version').fetchone()[0]))


def install_unassigned_index(connection):
    with connection:
        if not connection.in_transaction:
            connection.execute('BEGIN IMMEDIATE')
        legacy = _schema_valid(connection, triggers=_LEGACY_TRIGGERS)
        if legacy and not _schema_valid(connection):
            _migrate_private_hooks(connection)
        connection.execute(_DDL)
        for ddl in (*_INDEXES.values(), *_TRIGGERS.values()):
            connection.execute(ddl)
        if not _schema_valid(connection):
            raise RuntimeError('OpenBB pending workset schema changed')


def _migrate_private_hooks(connection):
    """Prove this exact private DDL leaves all source capture coverage intact.

    The caller's writer transaction excludes concurrent mutations. Only the
    two witnessed old hooks may change; all other schema SQL must stay byte
    identical. Advance only checkpoints bound to the pre-DDL schema cookie.
    Other validation (DB identity, journal gaps, daily age, contract hash)
    remains with each canonical reader and no cutoff/clock is refreshed here.
    """
    before = dict(connection.execute('SELECT name,sql FROM sqlite_schema'))
    cookie = int(connection.execute('PRAGMA schema_version').fetchone()[0])
    changed = {name for name in _TRIGGERS if _TRIGGERS[name] != _LEGACY_TRIGGERS[name]}
    for name in changed:
        connection.execute(f'DROP TRIGGER {name}')
        connection.execute(_TRIGGERS[name])
    after = dict(connection.execute('SELECT name,sql FROM sqlite_schema'))
    if ({key:value for key,value in before.items() if key not in changed}
            != {key:value for key,value in after.items() if key not in changed}
            or not _schema_valid(connection)):
        raise RuntimeError('OpenBB private hook migration changed unrelated schema')
    new_cookie = int(connection.execute('PRAGMA schema_version').fetchone()[0])
    keys = (KEY, 'l1_member_endpoint_counts_checkpoint_v1', 'l1_source_audit_checkpoint_v1')
    for key in keys:
        row = connection.execute('SELECT value FROM archive_meta WHERE key=?', (key,)).fetchone()
        try:
            proof = json.loads(row[0]) if row else None
        except (TypeError, ValueError):
            continue
        if type(proof) is dict and type(proof.get('schema_version')) is int and proof['schema_version'] == cookie:
            proof['schema_version'] = new_cookie
            connection.execute('UPDATE archive_meta SET value=? WHERE key=?', (json.dumps(proof,sort_keys=True),key))


def baseline_unassigned_index(connection):
    with connection:
        if not connection.in_transaction:
            connection.execute('BEGIN IMMEDIATE')
        if _valid(connection):
            return False
        if not _schema_valid(connection):
            raise RuntimeError('OpenBB pending workset hooks changed before bootstrap')
        connection.execute(f'DELETE FROM {TABLE}')
        connection.execute(f'''INSERT INTO {TABLE}(task_id,endpoint,plan_token)
            SELECT t.task_id,t.endpoint,t.plan_token FROM tasks AS t
            LEFT JOIN l1_compaction_members AS m ON m.task_id=t.task_id
            WHERE t.active=1 AND t.status='success' AND m.task_id IS NULL''')
        body = json.dumps(dict(policy=POLICY,schema_version=int(connection.execute('PRAGMA schema_version').fetchone()[0])))
        columns = {str(row[1]) for row in connection.execute('PRAGMA table_info(archive_meta)')}
        if 'updated_at' in columns:
            connection.execute('INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,CURRENT_TIMESTAMP) '
                               'ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at', (KEY,body))
        else:
            connection.execute('INSERT INTO archive_meta(key,value) VALUES(?,?) '
                               'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (KEY,body))
        return True


def unassigned_rows(connection, plan_token, filter_clause, filter_parameters, limit):
    connection.execute('SAVEPOINT l1_pending_read')
    try:
        if not _valid(connection):
            return None
        clause = '' if plan_token is None else ' AND p.plan_token=?'
        parameters = ([] if plan_token is None else [plan_token]) + list(filter_parameters) + [int(limit)]
        index = 'idx_l1_unassigned_order_v1' if plan_token is None else 'idx_l1_unassigned_plan_v1'
        return connection.execute(f'''SELECT t.task_id,t.endpoint,t.output_path,t.rows,t.updated_at
            FROM {TABLE} AS p INDEXED BY {index} CROSS JOIN tasks AS t
            WHERE t.task_id=p.task_id AND t.active=1 AND t.status='success'{clause}{filter_clause}
            ORDER BY p.endpoint,p.task_id LIMIT ?''', parameters).fetchall()
    finally:
        connection.execute('RELEASE l1_pending_read')
