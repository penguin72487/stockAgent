"""An explicit-empty response is not a claim of nonempty value equivalence."""
import json
import pytest

from scripts.verify_tej_self_healing import verified_value_reference
from test_tej_history import FakeBridge, empty_export, registry


CODE={'scripts/tej_smart_wizard_bridge.ps1':'capture-sha','downloader/tej_history.py':'ingest-sha'}


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
