"""A finite provider batch keeps its denominator separate from the full registry."""
from contextlib import closing

import pytest

from downloader.tej_history import configure_preview_planning, connect, run_one
from scripts.audit_tej_history import audit
from test_tej_planning import DynamicBridge, registry


def completed_batch(registry):
    root,config=registry
    configure_preview_planning(root,config)
    bridge=DynamicBridge()
    assert run_one(root,bridge)=='completed_task'
    for _ in range(5):
        assert run_one(root,bridge)=='completed_task'
    with closing(connect(root)) as con:
        tasks=[r[0] for r in con.execute("SELECT task_id FROM tasks WHERE kind='download' ORDER BY task_id")]
    return root,tasks


def test_scoped_receipt_counts_do_not_use_the_global_feature_denominator(registry,tmp_path):
    root,tasks=completed_batch(registry)
    result=audit(root,tmp_path/'subset',task_ids=tasks[:1])
    assert result['accepted'] and result['completed_download_tasks_audited']==1
    assert result['selected_task_ids']==tasks[:1]
    assert result['feature_count_mismatches'] is None
    assert result['feature_registry_checked'] is False
    assert result['all_selected_tasks_completed']
    assert sum(r['tasks'] for r in result['queue_states_at_snapshot'])==1
    full=audit(root,tmp_path/'full')
    assert full['accepted'] and full['feature_registry_checked']
    assert full['feature_count_mismatches']==0


def test_pending_selected_task_cannot_be_reported_as_complete(registry,tmp_path):
    root,tasks=completed_batch(registry)
    with closing(connect(root)) as con:
        con.execute("UPDATE tasks SET state='pending' WHERE task_id=?",(tasks[-1],))
        con.commit()
    result=audit(root,tmp_path/'partial',task_ids=tasks)
    assert not result['accepted'] and not result['all_selected_tasks_completed']
    assert result['completed_download_tasks_audited']==4


def test_scoped_exact_original_cell_verification(registry,tmp_path):
    root,tasks=completed_batch(registry)
    result=audit(root,tmp_path/'exact',task_ids=tasks[:1],verify_original_cells=True)
    assert result['accepted'] and result['original_source_cells_verified']
    assert result['original_source_cells_checked']>0


def test_rehashed_changed_source_value_still_fails_exact_cell_verification(registry,tmp_path):
    import hashlib
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from downloader.artifact_io import atomic_write_json
    root,tasks=completed_batch(registry)
    task_id=tasks[0]
    receipt_path=root/'receipts'/(task_id+'.json')
    receipt=json.loads(receipt_path.read_text())
    path=root/receipt['parquet_path']; frame=pq.read_table(path)
    column=frame.column_names[2];values=frame[column].to_pylist()
    values[0]=values[0]+'-changed' if isinstance(values[0],str) else values[0]+1
    frame=frame.set_column(2,frame.schema.field(2),pa.array(values,type=frame[column].type))
    pq.write_table(frame,path,compression='zstd')
    receipt['parquet_bytes']=path.stat().st_size
    receipt['parquet_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    atomic_write_json(receipt_path,receipt)
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET actual_bytes=? WHERE task_id=?',(receipt['raw_bytes']+receipt['parquet_bytes'],task_id))
    result=audit(root,tmp_path/'changed',task_ids=[task_id],verify_original_cells=True)
    assert not result['accepted'] and not result['original_source_cells_verified']
    assert 'original_source_cell_mismatch' in (tmp_path/'changed'/'receipt_integrity.csv').read_text()


@pytest.mark.parametrize('selection',[[],['unknown'],['one','one']])
def test_invalid_selected_scope_fails_before_an_acceptance_artifact(registry,tmp_path,selection):
    root,_=completed_batch(registry)
    output=tmp_path/'invalid'
    with pytest.raises(ValueError):
        audit(root,output,task_ids=selection)
    assert not output.exists()
