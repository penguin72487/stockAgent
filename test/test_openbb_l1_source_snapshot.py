from pathlib import Path
import sqlite3

import pytest

from scripts import compact_openbb_l1 as c
from scripts import openbb_l1_source_snapshot as snapshot


@pytest.fixture
def source(tmp_path):
    connection = sqlite3.connect(tmp_path/'source.sqlite3')
    connection.row_factory = sqlite3.Row
    connection.execute('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,active,status,endpoint,output_path,rows,updated_at)')
    connection.execute('CREATE TABLE l1_compaction_members(task_id TEXT PRIMARY KEY,segment_id,endpoint,source_path,source_rows,task_updated_at)')
    connection.execute('CREATE TABLE l1_compaction_segments(segment_id TEXT PRIMARY KEY,endpoint,output_path,status)')
    for i in range(3):
        connection.execute('INSERT INTO tasks VALUES(?,1,\'success\',\'e\',?,1,\'clock\')',(str(i),f'data/{i}'))
        connection.execute('INSERT INTO l1_compaction_members VALUES(?,?,\'e\',?,1,\'clock\')',(str(i),str(i),str(Path.cwd()/f'data/{i}')))
        connection.execute('INSERT INTO l1_compaction_segments VALUES(?,\'e\',?,\'success\')',(str(i),f'output/{i}'))
    connection.commit()
    connection.create_function('stockagent_resolve_path',1,c._lexical_absolute_path,deterministic=True)
    return connection


@pytest.mark.parametrize('mutation',['none','missing','retired','failed','endpoint','path','rows','clock','null','absolute','dotted'])
def test_complete_sql_snapshot_keeps_every_original_row_and_null_semantics(source, monkeypatch, mutation):
    monkeypatch.setattr(snapshot,'available_memory',lambda:10**12)
    if mutation=='missing':source.execute("DELETE FROM tasks WHERE task_id='0'")
    else:
        changes = {
            'retired':('active',0), 'failed':('status','failed'), 'endpoint':('endpoint','changed'),
            'path':('output_path','different'), 'rows':('rows',2), 'clock':('updated_at','new'),
            'null':('status',None), 'absolute':('output_path',str(Path.cwd()/'data/0')),
            'dotted':('output_path','data/../data/0'),
        }
        if mutation in changes:
            key,value = changes[mutation]
            source.execute(f"UPDATE tasks SET {key}=? WHERE task_id='0'",(value,))
    source.commit()
    before = source.total_changes, Path(source.execute('PRAGMA database_list').fetchone()[2]).read_bytes()
    sql = c._stale_source_contract_sql('',member_first=True)
    parameters = (str(Path.cwd())+'/',)*2
    expected = [tuple(row) for row in source.execute(sql,parameters)]
    audit = {}
    actual = snapshot.source_rows(source,sql,parameters,resolver=c._lexical_absolute_path,audit=audit)
    assert [tuple(row) for row in actual]==expected
    assert audit['state']=='complete_snapshot_query'
    assert audit['wall_s']>=audit['backup_wall_s']>=0
    assert (source.total_changes, Path(source.execute('PRAGMA database_list').fetchone()[2]).read_bytes())==before


@pytest.mark.parametrize('reason',['capacity','transaction','temp'])
def test_unadmitted_snapshot_never_commits_or_discards_live_work(source, monkeypatch, reason):
    monkeypatch.setattr(snapshot,'available_memory',lambda:0 if reason=='capacity' else 10**12)
    if reason=='transaction':source.execute("UPDATE tasks SET status='pending' WHERE task_id='0'")
    if reason=='temp':source.execute('CREATE TEMP TABLE tasks AS SELECT * FROM main.tasks')
    in_transaction = source.in_transaction
    audit = {}
    actual = snapshot.source_rows(source,'SELECT * FROM tasks ORDER BY task_id',(),resolver=c._lexical_absolute_path,audit=audit)
    assert [tuple(row) for row in actual]==[tuple(row) for row in source.execute('SELECT * FROM tasks ORDER BY task_id')]
    assert audit['state']=='canonical_live_query' and audit['reason']
    assert source.in_transaction==in_transaction


def test_compactor_copies_only_complete_source_audits(tmp_path, monkeypatch):
    import json
    from test_openbb_l1_compaction import _publish_tasks, _incremental_args

    monkeypatch.setattr(snapshot,'available_memory',lambda:10**12)
    _publish_tasks(tmp_path,['aa','bb'])
    args = [*_incremental_args(tmp_path),'--memory-source-audit']
    assert c.run(args)==0
    attempt_path = tmp_path/'_state/l1_compaction_attempt_latest.json'
    assert json.loads(attempt_path.read_bytes())['source_sql_snapshot']['state']=='complete_snapshot_query'
    assert c.run(args)==0
    assert json.loads(attempt_path.read_bytes())['source_sql_snapshot']=={}
    assert c.run([*args,'--force-full-source-audit'])==0
    attempt = json.loads(attempt_path.read_bytes())
    assert attempt['source_sql_snapshot']['state']=='complete_snapshot_query'
    assert attempt['source_sql_snapshot']['rows']==0
    assert attempt['stale_segments']==0 and attempt['compacted_files']==2
