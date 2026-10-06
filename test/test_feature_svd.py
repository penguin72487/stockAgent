"""Feature-axis fit, deployment, leakage and checkpoint contracts."""
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest
import torch

from stockagent.config import load_config
from stockagent.models.feature_svd import (
    fit_training_feature_svd, full_training_feature_spectrum, training_feature_rows,
)
from stockagent.models.financial_transformer import CandleEncoder
from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config


def fit(values, targets=None, **overrides):
    options = dict(lookback=3, feature_lag=1, scale=np.ones(values.shape[-1]),
        active_mask=np.ones(values.shape[-1], dtype=bool), components=2,
        analysis_components=values.shape[-1], oversampling=0, power_iterations=1)
    options.update(overrides)
    return fit_training_feature_svd(values, np.ones(values.shape[:2], dtype=bool),
        np.arange(3, 12) if targets is None else targets, **options)


def test_full_range_matches_dense_svd_and_full_energy_denominator():
    values = np.random.default_rng(7).normal(size=(20, 3, 7)).astype(np.float32)
    result = fit(values)
    x = values[:11].reshape(-1, 7).astype(np.float64)
    singular = np.linalg.svd(x, compute_uv=False)
    np.testing.assert_allclose(result.squared_singular_values, singular**2, rtol=2e-6, atol=2e-5)
    np.testing.assert_allclose(result.metadata['total_squared_energy'], np.square(x).sum(), rtol=1e-12)
    expected = (singular[:2]**2).sum() / np.square(x).sum()
    assert result.metadata['selected_energy_ratio'] == pytest.approx(expected, rel=2e-6)
    assert result.metadata['selected_energy_ratio'] < 1
    assert result.metadata['validation_rows_used'] == result.metadata['test_rows_used'] == 0
    assert result.metadata['axis'] == 'feature' and not result.metadata['centered']


def test_future_changes_cannot_change_training_directions():
    values = np.random.default_rng(3).normal(size=(20, 2, 5)).astype(np.float32)
    reference = fit(values)
    values[11:] = 1e10
    actual = fit(values)
    assert torch.equal(actual.directions, reference.directions)
    assert torch.equal(actual.squared_singular_values, reference.squared_singular_values)


def test_compact_columns_are_exact_operator_not_columnwise_pca():
    values = np.random.default_rng(9).normal(size=(20, 3, 7)).astype(np.float32)
    values[..., [1, 4]] = 0
    class Compact:
        _stockagent_factorized_features = True
        shape = values.shape
        def iter_reduction_column_slabs(self, rows):
            yield rows, slice(None), np.array([0,2,3,5,6]), values[rows][...,[0,2,3,5,6]]
    reference = fit(values)
    compact = fit_training_feature_svd(Compact(), np.ones(values.shape[:2], dtype=bool), np.arange(3,12),
        lookback=3, feature_lag=1, scale=np.ones(7), active_mask=np.ones(7,dtype=bool),
        components=2, analysis_components=7, oversampling=0, power_iterations=1)
    torch.testing.assert_close(compact.directions, reference.directions, rtol=1e-5, atol=1e-6)
    assert compact.metadata['total_squared_energy'] == reference.metadata['total_squared_energy']


def test_alive_and_inactive_columns_do_not_influence_fit():
    values = np.random.default_rng(9).normal(size=(20,3,4)).astype(np.float32)
    alive = np.ones((20,3),dtype=bool);alive[:,2]=False
    options = dict(lookback=3,feature_lag=1,scale=np.ones(4),active_mask=[True,True,True,False],
                   components=2,analysis_components=4,oversampling=0,power_iterations=1)
    reference=fit_training_feature_svd(values,alive,np.arange(3,12),**options)
    values[:,2,:]=1e12;values[:,:,-1]=1e12
    actual=fit_training_feature_svd(values,alive,np.arange(3,12),**options)
    torch.testing.assert_close(actual.directions,reference.directions,rtol=0,atol=0)
    assert actual.metadata['alive_training_cells']==22


@pytest.mark.parametrize('targets,lookback,lag', [([],3,1),([3,3],3,1),([2,1],1,0),([1],3,1),([99],3,1)])
def test_invalid_causal_scope_fails(targets,lookback,lag):
    with pytest.raises(ValueError):
        training_feature_rows(targets,lookback=lookback,feature_lag=lag,rows=20)


def test_zero_energy_is_not_reported_as_one_hundred_percent():
    with pytest.raises(ValueError,match='energy'):
        fit(np.zeros((20,2,5),dtype=np.float32))


def test_complete_spectrum_matches_numpy_svd_without_tail_extrapolation():
    values=np.random.default_rng(31).normal(size=(20,3,7)).astype(np.float32)
    values[...,6]=0
    scale=np.array([1,2,3,4,5,6,1],dtype=np.float32)
    active=np.array([True]*6+[False])
    alive=np.ones(values.shape[:2],dtype=bool);alive[:,2]=False
    result=full_training_feature_spectrum(values,alive,np.arange(3,12),
        lookback=3,feature_lag=1,scale=scale,active_mask=active)
    x=(values[:11,:2]/scale)[...,:6].reshape(-1,6).astype(np.float64)
    expected=np.r_[np.linalg.svd(x,compute_uv=False)**2,0]
    np.testing.assert_allclose(result.squared_singular_values,expected,rtol=1e-12,atol=1e-12)
    metadata=result.metadata
    assert metadata['total_squared_energy']==pytest.approx(np.square(x).sum(),rel=1e-14)
    assert metadata['trace_relative_error']<1e-14
    assert metadata['eigen_sum_relative_error']<1e-14
    assert not metadata['spectrum_is_truncated']
    assert metadata['numerical_rank']==6 and metadata['alive_training_cells']==22
    assert not metadata['model_projection_fitted'] and not metadata['optimizer_started']
    thresholds={row['target_percent']:row for row in metadata['percent_to_dimensions']}
    assert len(thresholds)==104 and thresholds[0]['dimensions']==0
    assert thresholds[100]['dimensions']==6
    for percent in range(1,100):
        dimension=thresholds[percent]['dimensions']
        cumulative=np.cumsum(expected)/np.square(x).sum()
        assert cumulative[dimension-1]>=percent/100
        assert dimension==1 or cumulative[dimension-2]<percent/100
    # Future source rows and non-alive cells must not alter any eigenvalue.
    values[11:]=1e20;values[:,2]=1e20
    changed=full_training_feature_spectrum(values,alive,np.arange(3,12),
        lookback=3,feature_lag=1,scale=scale,active_mask=active)
    torch.testing.assert_close(changed.squared_singular_values,result.squared_singular_values,rtol=0,atol=0)


def test_full_spectrum_preserves_implicit_storage_zero_columns():
    values=np.random.default_rng(11).normal(size=(20,3,7)).astype(np.float32)
    values[..., [1,4]]=0
    class Compact:
        _stockagent_factorized_features=True
        shape=values.shape
        def iter_reduction_column_slabs(self,rows):
            yield rows,slice(None),np.array([0,2,3,5,6]),values[rows][...,[0,2,3,5,6]]
    kwargs=dict(lookback=3,feature_lag=1,scale=np.ones(7),active_mask=np.ones(7,dtype=bool))
    alive=np.ones(values.shape[:2],dtype=bool)
    dense=full_training_feature_spectrum(values,alive,np.arange(3,12),**kwargs)
    compact=full_training_feature_spectrum(Compact(),alive,np.arange(3,12),**kwargs)
    torch.testing.assert_close(dense.squared_singular_values,compact.squared_singular_values,
                             rtol=1e-12,atol=1e-12)
    assert compact.metadata['numerical_rank']==5
    assert compact.metadata['features']==7 and len(compact.squared_singular_values)==7


def test_full_spectrum_zero_energy_fails_closed():
    with pytest.raises(ValueError,match='energy'):
        full_training_feature_spectrum(np.zeros((20,2,5),dtype=np.float32),
            np.ones((20,2),dtype=bool),np.arange(3,12),lookback=3,feature_lag=1,
            scale=np.ones(5),active_mask=np.ones(5,dtype=bool))


def test_stable_joint_stem_is_an_explicit_precision_island_not_a_bottleneck():
    ordinary=CandleEncoder(num_features=131,d_model=32,dropout=0,norm_type='rmsnorm',
        ffn_type='gelu',sanitize_inputs=True)
    stable=CandleEncoder(num_features=131,d_model=32,dropout=0,norm_type='rmsnorm',
        ffn_type='gelu',sanitize_inputs=True,projection_fp32=True)
    stable.load_state_dict(ordinary.state_dict(),strict=True)
    x=torch.randn(3,4,131)
    expected=ordinary(x)[0]
    with torch.autocast('cpu',dtype=torch.bfloat16):
        output=stable(x)[0]
    assert output.dtype==torch.float32
    torch.testing.assert_close(output,expected,rtol=0,atol=0)
    output.square().sum().backward()
    assert all(torch.isfinite(p.grad).all() for p in stable.parameters() if p.grad is not None)
    assert stable.joint_projection.proj.in_features==131
    assert stable.continuous_feature_bottleneck is None and stable.feature_svd_components==0
    assert set(ordinary.state_dict())==set(stable.state_dict())
    assert 'candle_projection_fp32' not in _project_temporal_basis_model_config({'candle_projection_fp32':False})
    assert _project_temporal_basis_model_config({'candle_projection_fp32':True})['candle_projection_precision_contract']=='fp32_full_feature_joint_stem_v1'


def make_encoder():
    return CandleEncoder(num_features=5,d_model=4,dropout=0,norm_type='layernorm',
                         ffn_type='gelu',sanitize_inputs=True,
                         causal_feature_rms_normalization=True,feature_svd_components=2)


def test_frozen_projection_roundtrip_and_zero_preservation():
    encoder=make_encoder()
    with pytest.raises(RuntimeError,match='fitted'):
        encoder(torch.zeros(1,5))
    directions=torch.linalg.qr(torch.randn(5,2)).Q.T.contiguous()
    encoder.set_feature_svd_projection(directions)
    x=torch.randn(3,5)
    torch.testing.assert_close(encoder._base_joint_features(x),x@directions.T)
    assert torch.equal(encoder._base_joint_features(torch.zeros(3,5)),torch.zeros(3,2))
    assert 'feature_svd_directions' in encoder.state_dict()
    assert 'feature_svd_directions' not in dict(encoder.named_parameters())
    assert encoder.joint_projection.proj.in_features==2
    restored=make_encoder();restored.load_state_dict(encoder.state_dict(),strict=True)
    torch.testing.assert_close(restored(x)[0],encoder(x)[0],rtol=0,atol=0)
    bad=dict(encoder.state_dict());bad['feature_svd_directions']=torch.zeros_like(directions)
    with pytest.raises(RuntimeError,match='orthonormal'):
        make_encoder().load_state_dict(bad,strict=True)


def test_disabled_defaults_preserve_existing_checkpoint_contract():
    value={'feature_svd_components':0,'feature_svd_analysis_components':128,
           'feature_svd_oversampling':32,'feature_svd_power_iterations':2,'feature_svd_seed':7}
    projected=_project_temporal_basis_model_config(value)
    assert not any(key.startswith('feature_svd') for key in projected)
    value['feature_svd_components']=32
    assert _project_temporal_basis_model_config(value)['feature_svd_contract'].endswith('_v1')


def test_compile_dtype_policy_is_scoped_to_the_explicit_svd_experiment():
    from types import SimpleNamespace
    from stockagent.models.factorized_input import _partition_compile_options
    model=SimpleNamespace(temporal_basis_feature_encoder=None,
                          candle_encoder=SimpleNamespace(feature_svd_components=32))
    assert _partition_compile_options(model)=={'triton.cudagraphs':False,'emulate_precision_casts':True}
    model.candle_encoder.feature_svd_components=0
    assert _partition_compile_options(model)=={'triton.cudagraphs':False}


def test_remote_config_is_single_svd_not_multi_basis():
    root=Path('configs/deployments')
    reference=load_config(root/'tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v1.yaml')
    selected=load_config(root/'tw_day_trade_factorized_values_20261005_gaprepair_v4_svd32_bf16_v1.yaml')
    assert selected.data==reference.data and selected.trading==reference.trading
    model=selected.training.financial_transformer
    assert model.feature_svd_components==model.d_model==32
    assert not model.temporal_basis_families and not model.feature_bottleneck_dim
    assert selected.environment.amp_dtype=='bf16' and selected.training.batch_size_train==32
    assert selected.training.multi_gpu_strategy=='distributed_data_parallel'
    assert replace(model,feature_svd_components=0)==reference.training.financial_transformer
