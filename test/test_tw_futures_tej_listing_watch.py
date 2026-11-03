import json
import sqlite3

import pytest

from scripts.watch_tw_futures_tej_listing_priority import TABLE_ID, observe


def test_observer_reads_owner_state_and_rejects_changed_scope(tmp_path):
    request = {'company_labels': ['CCF201104==>CCF'], 'date_labels': [],
               'source_key_mode': 1, 'key_layout_contract': 'native_company_observed_snapshot_key1_v1',
               'frequency': 'snapshot', 'fields': ['Listed Date']}
    plan = {'requests': [{'table_id': TABLE_ID, 'request': request}]}
    registration = {'tasks': [{'task_id': 'task1'}]}
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as db:
        db.executescript('''
            CREATE TABLE tables(table_id TEXT, fields_json TEXT);
            CREATE TABLE tasks(task_id TEXT, table_id TEXT, scope_contract TEXT,
                               request_json TEXT, state TEXT, actual_rows INTEGER,
                               receipt_path TEXT, last_error_code TEXT);
        ''')
        db.execute('INSERT INTO tables VALUES (?,?)', (TABLE_ID, '["Listed Date"]'))
        db.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)',
                   ('task1', TABLE_ID, 'editable_source_scope_v1', json.dumps(request),
                    'pending', None, None, None))
    status = observe(tmp_path, plan, registration)
    assert status['state_counts'] == {'pending': 1}
    assert status['provider_calls'] == 0
    assert status['training_admitted'] is False
    changed = dict(request, company_labels=['CCF201105==>CCF'])
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as db:
        db.execute('UPDATE tasks SET request_json=?', (json.dumps(changed),))
    with pytest.raises(ValueError, match='scope differs'):
        observe(tmp_path, plan, registration)
