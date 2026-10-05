from contextlib import closing
import json
from pathlib import Path

import pytest

from downloader.tej_history import SOURCE_SCOPE_CONTRACT, connect
from scripts.benchmark_tej_full_flow import prepare_trial, with_field_batch


def test_trial_preserves_scope_policy_and_has_no_live_source_history(tmp_path):
    original=tmp_path/'original'
    with closing(connect(original)) as con, con:
        con.execute("INSERT INTO tables(table_id,smart_id,name,category,phase,frequency,query_type,schema_sha256,fields_json,state) "
                    "VALUES ('t','s','table','TW','P1','daily','LISTED & DELISTED','hash','[\"field\"]','ready')")
        con.execute("INSERT INTO features(feature_id,table_id,field_index,name,phase,unit,role,raw_input_policy,exported_non_null_cells) "
                    "VALUES ('f','t',0,'field','P1','shares','feature','original',100)")
        definition=dict(con.execute('SELECT * FROM tables').fetchone())
        feature=dict(con.execute('SELECT * FROM features').fetchone())
    case={'definition':definition,'features':[feature],
          'request':{'fields':['field'],'company_labels':['2330=>name'],'date_labels':['2026/01/01']},
          'task':{'task_id':'task','table_id':'t','kind':'download','phase':'P1','priority':3,
                  'expected_rows':1,'scope_contract':SOURCE_SCOPE_CONTRACT}}
    target=tmp_path/'trial';policy={'minimum_export_interval_seconds':2};relay={'state':'active'}
    prepare_trial(target,case,{'max_rows_per_export':10000},policy,relay)
    with closing(connect(target)) as con:
        task=dict(con.execute('SELECT * FROM tasks').fetchone())
        assert json.loads(task['request_json'])==case['request']
        assert task['state']=='pending' and task['active_attempt_id'] is None and task['receipt_path'] is None
        assert task['scope_contract']==SOURCE_SCOPE_CONTRACT
        assert json.loads(con.execute("SELECT value FROM meta WHERE key='runtime_policy'").fetchone()[0])==policy
        assert con.execute('SELECT exported_non_null_cells FROM features').fetchone()[0] is None
        assert con.execute('SELECT COUNT(*) FROM desktop_attempts').fetchone()[0]==0
    with closing(connect(original)) as con:
        assert con.execute('SELECT exported_non_null_cells FROM features').fetchone()[0]==100
    with pytest.raises(FileExistsError):prepare_trial(target,case,{},policy,relay)


def test_real_comparison_requires_explicit_requery_flag():
    from scripts.benchmark_tej_full_flow import main
    with pytest.raises(SystemExit) as stopped:
        main(['--baseline','existing','--output','new','--case','exact'])
    assert stopped.value.code==2


def test_adjacent_field_batch_preserves_source_axes_and_immutable_original_case():
    case = {'label': 'event', 'request': {'fields': ['a', 'b', 'c', 'd'],
                'catalog_fields': ['a', 'b', 'c', 'd'], 'company_labels': ['one', 'two'],
                'date_labels': ['2026/01/01'], 'max_rows': 10000, 'max_cells': 400000},
            'task': {'task_id': 'a'*24, 'kind': 'download', 'expected_rows': 2}}
    original = json.dumps(case, sort_keys=True)
    cases = with_field_batch([case], 'a'*24)
    assert json.dumps(case, sort_keys=True) == original
    assert cases[0] is case and cases[1]['label'] == 'event_adjacent_field_batch'
    assert cases[1]['request']['fields'] == ['a', 'b']
    assert cases[1]['task']['task_id'] != case['task']['task_id']
    assert cases[1]['task']['expected_rows'] == 2
    for key in ('catalog_fields', 'company_labels', 'date_labels', 'max_rows', 'max_cells'):
        assert cases[1]['request'][key] == case['request'][key]
    assert with_field_batch([case], None) == [case]


@pytest.mark.parametrize('task_id,kind,fields', [
    ('missing', 'download', ['a', 'b']), ('a'*24, 'discover', ['a', 'b']),
    ('a'*24, 'download', ['a']), ('a'*24, 'download', None),
])
def test_adjacent_field_batch_rejects_unknown_or_invalid_original(task_id,kind,fields):
    case = {'label': 'event', 'request': {'fields': fields},
            'task': {'task_id': 'a'*24, 'kind': kind}}
    with pytest.raises(ValueError):
        with_field_batch([case], task_id)


def test_balanced_comparison_uses_original_source_lock_and_keeps_unknown_barrier():
    source=(Path(__file__).parents[1]/'scripts/benchmark_tej_full_flow.py').read_text()
    assert "root/'.download.lock'" in source
    assert "(('baseline','candidate'),('candidate','baseline'))" in source
    assert "'production_gate_retained':True" in source
    assert "'production_receipts_modified':False" in source
    assert "run_one(path, DesktopBridge(repo, session)" in source
    assert "audit(path, path/'acceptance'/task['task_id'], task_ids=[task['task_id']])" in source
    assert "con.execute(\"DELETE FROM meta WHERE key='desktop_interface_recovery_required' AND value=?\", (marker,))" in source


def test_unaccepted_bulk_readback_is_fixture_only_not_a_production_path():
    source=(Path(__file__).parents[1]/'scripts/benchmark_tej_bulk_readback.ps1').read_text()
    guard="if(-not $Fixture){throw 'Unaccepted cache candidate may only access its owned fixture'}"
    assert guard in source and source.index(guard)<source.index('Add-Type')
    assert '[TejReadbackBenchmark]::Open($FixtureRows,30)' in source
    assert 'GetCachedChildren' not in source and '.CachedChildren' in source
    assert 'completed_measurements=$records.ToArray()' in source
    for action in ('SourceResult','Prepared','Session','BeginPreviewDefaultAction','SetForegroundWindow','SendKeys.Send','Clip'+'board'):
        assert action not in source
