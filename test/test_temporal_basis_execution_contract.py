from pathlib import Path
from types import SimpleNamespace
import numpy as np
import pytest
import torch
import yaml

from stockagent.config import load_config
from stockagent.training.checkpoint_contract import _training_checkpoint_contract
import stockagent.training.trainer as trainer


def config():
    value=load_config('configs/markets/tw_public_multi_basis.yaml')
    value.training.financial_transformer.temporal_basis_families=['haar','pca_klt']
    return value


def test_default_pca_execution_preserves_historical_training_contract():
    value=config()
    expected=_training_checkpoint_contract(value)
    assert 'temporal_basis_covariance_schedule' not in expected
    value.training.temporal_basis_covariance_cpu_threads=4
    assert _training_checkpoint_contract(value)==expected
    value.training.temporal_basis_covariance_lag_batch_size=8
    actual=_training_checkpoint_contract(value)
    assert actual['temporal_basis_covariance_schedule']['lag_batch_size']==8
    value.training.financial_transformer.temporal_basis_families=['haar']
    assert 'temporal_basis_covariance_schedule' not in _training_checkpoint_contract(value)


@pytest.mark.parametrize('field',[
    'temporal_basis_covariance_lag_batch_size',
    'temporal_basis_covariance_cpu_threads',
])
def test_pca_execution_controls_roundtrip_and_reject_zero(tmp_path,field):
    path=tmp_path/'pca.yaml'
    base=str(Path('configs/markets/tw_public_multi_basis.yaml').resolve())
    path.write_text(yaml.safe_dump({'base_config':base,'training':{field:4}}))
    assert getattr(load_config(path).training,field)==4
    path.write_text(yaml.safe_dump({'base_config':base,'training':{field:0}}))
    with pytest.raises(ValueError,match=field):load_config(path)


def test_pca_thread_scope_is_restored_when_fit_raises(tmp_path,monkeypatch):
    value=config();value.training.temporal_basis_covariance_cpu_threads=1
    before=torch.get_num_threads();seen=[]
    def reject(*args,**kwargs):
        seen.append(torch.get_num_threads())
        raise RuntimeError('bounded PCA diagnostic failure')
    monkeypatch.setattr(trainer,'fit_training_only_pca_klt',reject)
    dataset=SimpleNamespace(features_t=torch.randn(65,2,3),
        valid_indices=np.arange(32,64),execution_mode='naive')
    with pytest.raises(RuntimeError,match='bounded PCA diagnostic failure'):
        trainer._fit_group_temporal_basis(config=value,train_ds=dataset,
            train_years=[2014],group_folds=[],output_path=tmp_path)
    assert seen==[1] and torch.get_num_threads()==before
