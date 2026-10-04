"""Only a finished, exact native response can unblock unrelated TEJ tables."""
from contextlib import closing
from datetime import UTC, datetime
import json
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import begin_attempt, finish_attempt
from downloader.tej_history import (PREVIEW_SUBMISSION_CONTRACT, _ready_task, configure_runtime_policy,
    connect, run_one, task_request)
from downloader.tej_source_isolation import STATE, isolate_invalid_response
from test_tej_history import FakeBridge, full_grid_export, registry as registry, scope_proof


class InvalidNativeBridge:
    def execute(self, root, task):
        req = task_request(root,task)
        attempt = task['task_id']+'-'+uuid.uuid4().hex
        prepared = root/'requests'/(attempt+'.json')
        output = root/'raw'/(attempt+'.json')
        atomic_write_json(prepared,{**req,'task_id':task['task_id'],'query_attempt_id':attempt})
        begin_attempt(root,task,attempt,prepared,output)
        stage = {**{k:req[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
                 'contract_version':4,'task_id':task['task_id'],'stage':'prepreview_verified',
                 'query_attempt_id':attempt,'source_scope_proof':scope_proof(),
                 'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,
                 'observed_at_utc':datetime.now(UTC).isoformat()}
        atomic_write_json(root/'raw'/(attempt+'.json.stage.json'),stage)
        doc = {**full_grid_export(),'task_id':task['task_id'],'query_attempt_id':attempt,
               'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,
               'fresh_preview_transition_verified':True,'source_key_headers':['CO_ID','Date'],
               'observed_at_utc':datetime.now(UTC).isoformat()}
        doc['cells'][1][0] = '9999 outside'  # Preview shares the exact original native strings.
        atomic_write_json(output,doc)
        finish_attempt(root,attempt,'response_received_not_yet_adopted')
        return doc,output,1.0


def failed(root):
    assert run_one(root,FakeBridge()) == 'completed_task'
    assert run_one(root,InvalidNativeBridge()) == 'source_validation_failed'
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
    return task,root/'raw'/(task['active_attempt_id']+'.json')


def test_quarantine_retains_all_source_bytes_and_zero_adoption(registry):
    _,root,_,_ = registry
    task,raw = failed(root)
    files = [raw,root/'requests'/(task['active_attempt_id']+'.json'),
             root/'raw'/(task['active_attempt_id']+'.json.stage.json')]
    original = {p:p.read_bytes() for p in files}
    result = isolate_invalid_response(root,task['task_id'])
    assert result['state'] == STATE and result['provider_queries_sent'] == 0
    assert result['source_rows_adopted'] is False and result['data_query_repeated'] is False
    assert isolate_invalid_response(root,task['task_id'])['already_isolated']
    assert all(p.read_bytes()==b for p,b in original.items())
    audit = json.loads((root/result['audit']).read_text())
    assert audit['state']=='committed' and audit['validation_relaxed'] is False
    with closing(connect(root)) as con:
        assert con.execute('SELECT actual_rows,receipt_path,completed_at_utc FROM tasks WHERE task_id=?',
                           (task['task_id'],)).fetchone()[:] == (None,None,None)
        assert con.execute('SELECT SUM(exported_non_null_cells) FROM features').fetchone()[0] is None


@pytest.mark.parametrize('field,value',[
    ('fresh_preview_transition_verified',False),('query_attempt_id','foreign'),
    ('capture_method','unknown'),('source_grid_rows',0),('source_key_headers',['wrong','Date']),
    ('query_comments_read',True),('observed_at_utc','1900-01-01T00:00:00+00:00'),
    ('cells',[]),('source_scope_proof',{}),('preview',[1,[]]),
])
def test_unproved_or_malformed_response_keeps_global_failure(registry,field,value):
    _,root,_,_ = registry
    task,raw = failed(root)
    doc = json.loads(raw.read_text());doc[field]=value;atomic_write_json(raw,doc)
    with pytest.raises(ValueError):isolate_invalid_response(root,task['task_id'])
    with closing(connect(root)) as con:
        assert con.execute('SELECT last_error_code FROM tasks WHERE task_id=?',
                           (task['task_id'],)).fetchone()[0]=='source_validation_failed'
    assert not (root/'source_isolation').exists()


def test_automatic_isolation_requires_explicit_policy_and_exact_terminal_evidence(registry):
    _,root,_,config = registry
    configure_runtime_policy(root,{**config,'source_validation_isolation':True})
    assert run_one(root,FakeBridge())=='completed_task'
    assert run_one(root,InvalidNativeBridge())==STATE


@pytest.mark.parametrize('kind',[None,'download','discover'])
def test_table_isolation_applies_to_normal_rotation_and_targeted_selection(registry,kind):
    _,root,_,_ = registry
    task,_=failed(root);isolate_invalid_response(root,task['task_id'])
    with closing(connect(root)) as con,con:
        for identity,table in [('a-same',task['table_id']),('b-other','other')]:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) VALUES(?,?,?,?,?,?)",
                        (identity,table,kind or 'download','P1',100,'{}'))
        con.execute("INSERT OR REPLACE INTO meta VALUES('scheduler_last_download_table',?)",(task['table_id'],))
        now=datetime.now(UTC).isoformat()
        assert _ready_task(con,now,kind,{})['task_id']=='b-other'
        if kind!='discover':
            assert _ready_task(con,now,'download',{},task['table_id']) is None


@pytest.mark.parametrize('barrier',['running','unknown','interface','period','unfinished'])
def test_foreign_or_unfinished_action_is_not_licensed_to_continue(registry,barrier):
    _,root,_,_ = registry
    task,_=failed(root)
    with closing(connect(root)) as con,con:
        if barrier in ('running','unknown'):
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,last_error_code) "
                        "VALUES('foreign','foreign','download','P1',100,'{}',?,?)",
                        ('running' if barrier=='running' else 'blocked','unknown_outcome_no_auto_retry'))
        elif barrier in ('interface','period'):
            key='desktop_interface_recovery_required' if barrier=='interface' else 'source_period_replan_required'
            con.execute('INSERT INTO meta VALUES(?,?)',(key,'1'))
        else:
            con.execute("UPDATE desktop_attempts SET state='prepared',finished_at_utc=NULL")
    with pytest.raises(ValueError):isolate_invalid_response(root,task['task_id'])


def test_changed_quarantined_source_cannot_be_silently_reconciled(registry):
    _,root,_,_ = registry
    task,raw=failed(root);isolate_invalid_response(root,task['task_id'])
    doc=json.loads(raw.read_text());doc['cells'][2][2]='123';doc['preview'][1][2][1][2]='123'
    atomic_write_json(raw,doc)
    with pytest.raises(ValueError):isolate_invalid_response(root,task['task_id'])
