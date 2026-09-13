import json
from types import SimpleNamespace

import pytest
import train


def receipt(path, physical='RFF:202303'):
    path.write_text(json.dumps({'status':'data_invalid','evidence':{
        'date':'2023-03-10','reason':'unresolved_corporate_contract_transition','scope':'train',
        'positions':[{'held_quantity':1,'held_physical_contract':'DNF:202303','unsupported_corporate_transition':False},
                     {'held_quantity':1,'held_physical_contract':physical,'unsupported_corporate_transition':True}]}}))


def test_current_child_failure_survives_elastic_footer(tmp_path, monkeypatch):
    def child(*args, **kwargs):
        receipt(tmp_path/'futures_data_failure_rank0.json')
        return SimpleNamespace(returncode=1)
    monkeypatch.setattr(train.subprocess,'run',child)
    with pytest.raises(RuntimeError,match='root_cause=unresolved_corporate_contract_transition date=2023-03-10 scope=train contracts=RFF:202303') as error:
        train._run_isolated_train_fold_processes([SimpleNamespace(fold_id=4)],argv=[],output_dir=tmp_path)
    assert 'DNF:202303' not in str(error.value)


def test_stale_failure_cannot_explain_another_child_error(tmp_path, monkeypatch):
    receipt(tmp_path/'futures_data_failure_rank0.json')
    monkeypatch.setattr(train.subprocess,'run',lambda *a,**kw:SimpleNamespace(returncode=1))
    with pytest.raises(RuntimeError) as error:
        train._run_isolated_train_fold_processes([SimpleNamespace(fold_id=4)],argv=[],output_dir=tmp_path)
    assert 'root_cause' not in str(error.value)


def test_invalid_receipt_never_masks_original_failure(tmp_path):
    (tmp_path/'futures_data_failure_rank0.json').write_text('incomplete')
    assert train._isolated_futures_failure_detail(tmp_path,{}) == ''


def test_identical_error_written_by_new_attempt_remains_visible(tmp_path):
    path=tmp_path/'futures_data_failure_rank0.json'
    receipt(path)
    before=train._isolated_futures_failure_receipts(tmp_path)
    temporary=tmp_path/'replacement.json'
    temporary.write_bytes(path.read_bytes())
    temporary.replace(path)
    assert 'contracts=RFF:202303' in train._isolated_futures_failure_detail(tmp_path,before)


def test_failure_helpers_do_not_read_working_directory_without_output():
    assert train._isolated_futures_failure_receipts(None) == {}
