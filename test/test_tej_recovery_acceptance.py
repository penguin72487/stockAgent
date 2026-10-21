"""An explicit-empty response is not a claim of nonempty value equivalence."""
import json
import pytest

from scripts.verify_tej_self_healing import verified_value_reference
from test_tej_history import FakeBridge, empty_export, registry


CODE={'scripts/tej_smart_wizard_bridge.ps1':'capture-sha','downloader/tej_history.py':'ingest-sha'}


@pytest.mark.parametrize('fault',[None,'missing_task','duplicate_task','disabled','logon','launcher','executor',
    'service_only','parallel','no_restart','interval','missing_trigger','disabled_trigger','watchdog_interval'])
def test_boot_registration_readback_validates_both_actual_owners(fault):
    from scripts.verify_tej_self_healing import validate_boot_tasks
    tasks = [{'name':name,'enabled':True,'logon_type':logon,'service_only':service,
              'launcher_sha256':'reviewed-launcher','executor_verified':True,
              'multiple_instances':'IgnoreNew','restart_count':3,'restart_interval':'PT1M',
              'triggers':[{'type':t,'enabled':True,'interval':'PT1M' if t=='MSFT_TaskTimeTrigger' else ''} for t in triggers]}
             for name,logon,service,triggers in [
                 ('StockAgent TEJ WSL Startup','S4U',True,['MSFT_TaskBootTrigger']),
                 ('StockAgent TEJ Desktop Recovery','Interactive',False,['MSFT_TaskLogonTrigger','MSFT_TaskTimeTrigger'])]]
    if fault == 'missing_task':tasks.pop()
    elif fault == 'duplicate_task':tasks[1] = dict(tasks[0])
    elif fault == 'disabled':tasks[0]['enabled'] = False
    elif fault == 'logon':tasks[1]['logon_type'] = 'S4U'
    elif fault == 'launcher':tasks[0]['launcher_sha256'] = 'unreviewed'
    elif fault == 'executor':tasks[1]['executor_verified'] = False
    elif fault == 'service_only':tasks[0]['service_only'] = False
    elif fault == 'parallel':tasks[1]['multiple_instances'] = 'Parallel'
    elif fault == 'no_restart':tasks[0]['restart_count'] = 0
    elif fault == 'interval':tasks[1]['restart_interval'] = 'PT7H'
    elif fault == 'missing_trigger':tasks[1]['triggers'].pop()
    elif fault == 'disabled_trigger':tasks[0]['triggers'][0]['enabled'] = False
    elif fault == 'watchdog_interval':tasks[1]['triggers'][1]['interval'] = 'PT1D'
    if fault:
        with pytest.raises(ValueError):validate_boot_tasks({'tasks':tasks},'reviewed-launcher')
    else:
        validate_boot_tasks({'tasks':tasks},'reviewed-launcher')


def test_acceptance_uses_private_owner_status_not_the_public_gateway():
    from scripts.verify_tej_self_healing import OWNER_STATUS_URL
    assert OWNER_STATUS_URL == 'http://127.0.0.1:8771/tej/api/status'


def test_empty_progress_reference_preserves_actual_value_evidence(tmp_path):
    path=tmp_path/'reference.json'
    path.write_text(json.dumps({'accepted':True,'source_rows':125,'original_cells_checked':3618,'code_sha256':CODE}))
    result=verified_value_reference(path,CODE)
    assert result['source_rows']==125 and result['original_cells_checked']==3618
    assert 'not_current_empty_values' in result['basis']


@pytest.mark.parametrize('fault',['rejected','zero_rows','zero_cells','capture_changed','ingest_changed'])
def test_empty_reference_rejects_missing_value_proof_or_changed_code(tmp_path,fault):
    path=tmp_path/'reference.json'
    value={'accepted':True,'source_rows':125,'original_cells_checked':3618,'code_sha256':dict(CODE)}
    if fault=='rejected':value['accepted']=False
    elif fault=='zero_rows':value['source_rows']=0
    elif fault=='zero_cells':value['original_cells_checked']=0
    else:value['code_sha256']['scripts/tej_smart_wizard_bridge.ps1' if fault=='capture_changed' else 'downloader/tej_history.py']='different'
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):verified_value_reference(path,CODE)


def test_explicit_empty_audit_resolves_query_but_never_claims_value_verification(registry,tmp_path):
    from contextlib import closing
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_history import connect,run_one
    from scripts.audit_tej_history import audit
    _,root,_,_=registry
    assert run_one(root,FakeBridge())=='completed_task'
    class Empty(FakeBridge):
        def execute(self,root,task):
            payload={**empty_export(),'task_id':task['task_id']}
            path=root/'raw'/(task['task_id']+'-empty.json')
            atomic_write_json(path,payload)
            return payload,path,1.0
    assert run_one(root,Empty())=='completed_task'
    with closing(connect(root)) as con:
        task_id=con.execute("SELECT task_id FROM tasks WHERE kind='download'").fetchone()[0]
    result=audit(root,tmp_path/'empty-audit',task_ids=[task_id],verify_original_cells=True)
    assert result['accepted'] and result['exported_rows']==0
    assert result['original_source_cells_checked']==0 and not result['original_source_cells_verified']


def test_retained_nonempty_reference_is_explicitly_not_a_new_download(registry, tmp_path):
    from contextlib import closing
    from downloader.tej_history import connect, run_one
    from scripts.verify_tej_self_healing import main
    _, root, _, _ = registry
    assert run_one(root, FakeBridge()) == 'completed_task'
    assert run_one(root, FakeBridge()) == 'completed_task'
    with closing(connect(root)) as con:
        task_id = con.execute("SELECT task_id FROM tasks WHERE kind='download' AND state='complete'").fetchone()[0]
        before = con.execute('SELECT COUNT(*) FROM traffic').fetchone()[0]
    output = tmp_path / 'retained-value-reference.json'
    assert main(['--root', str(root), '--output', str(output), '--values-reference-task', task_id]) == 0
    reference = json.loads(output.read_text())
    assert reference['accepted'] and reference['source_rows'] > 0 and reference['original_cells_checked'] > 0
    assert reference['mode'] == 'retained_nonempty_value_reference'
    assert reference['reference_scope'].endswith('not_a_new_download')
    assert reference['provider_queries_sent'] == 0 and not reference['all_history_complete']
    with closing(connect(root)) as con:
        assert con.execute('SELECT COUNT(*) FROM traffic').fetchone()[0] == before
