from types import SimpleNamespace

import numpy as np
import pytest
import torch
from scipy import sparse

from stockagent.training.classical_data import CausalWindowMatrix, PhysicalCSRStore
from stockagent.training.classical_solvers import LaggedLinearOperator, lightgbm_dataset
from stockagent.models.classical import CrossSectionalClassical


def fixture_matrix(tmp_path, *, training=True):
    rng = np.random.default_rng(123)
    x = rng.normal(size=(12, 3, 5)).astype(np.float32)
    x[rng.random(x.shape) < .6] = 0
    x[0, 0, 0] = np.nan
    mask = torch.tensor([[True, False, True]] * 12)
    mask[8] = False
    ds = SimpleNamespace(valid_indices=np.array([4, 6, 8, 10]), lookback=3,
        execution_mode="tw_day_trade", future_log_returns_t=torch.randn(12, 3),
        tradable_mask_t=mask)
    return CausalWindowMatrix(PhysicalCSRStore(x, tmp_path, block_rows=2), ds,
                              training=training, batch_rows=2), x


def test_full_window_zero_and_causal_geometry(tmp_path):
    m, x = fixture_matrix(tmp_path)
    dense = np.stack([np.nan_to_num(x[d-3:d]).transpose(1,0,2).reshape(3,15)
                      for d in m.dates]).reshape(-1,15)[m.flat]
    np.testing.assert_array_equal(m.rows(slice(None)).toarray(), dense)
    np.testing.assert_array_equal(m.rows(np.array([3,0,4])).toarray(), dense[[3,0,4]])
    np.testing.assert_array_equal(m.materialize(budget_bytes=100000).toarray(), dense)
    assert m.groups.tolist() == [2,2,0,2]
    with pytest.raises(MemoryError): m.materialize(budget_bytes=1)
    with pytest.raises(ValueError, match="symbol"): m.store.rows([1],[-1])


def test_matrix_free_forward_adjoint_and_ridge_dense_oracle(tmp_path):
    m, _ = fixture_matrix(tmp_path)
    x = m.rows(slice(None)).toarray().astype(np.float64)
    operator = LaggedLinearOperator(m)
    rng = np.random.default_rng(7)
    w, r = rng.normal(size=15), rng.normal(size=m.shape[0])
    np.testing.assert_allclose(operator._matvec(w), x@w, atol=1e-7)
    np.testing.assert_allclose(operator._rmatvec(r), x.T@r, atol=1e-7)
    np.testing.assert_allclose(operator._matvec(w)@r, w@operator._rmatvec(r), atol=1e-7)
    y = rng.normal(size=m.shape[0])
    coefficient, intercept, state = operator.solve(y, alpha=2, tolerance=1e-10, max_iter=100)
    scale = x.std(0); scale[scale<1e-12] = 1
    a = (x-x.mean(0))/scale
    oracle = np.linalg.solve(a.T@a + 2*np.eye(15), a.T@(y-y.mean()))/scale
    np.testing.assert_allclose(coefficient, oracle, atol=1e-7)
    np.testing.assert_allclose(x@coefficient+intercept, x@oracle+y.mean()-x.mean(0)@oracle, atol=1e-7)
    assert state['converged']
    resumed,offset,resumed_state=operator.solve(y,alpha=2,tolerance=1e-10,
        max_iter=state['iteration'],state=state)
    np.testing.assert_array_equal(resumed,coefficient)
    assert offset==intercept and resumed_state['iteration']==state['iteration']


def test_ridge_constant_features_produce_the_finite_intercept_optimum(tmp_path):
    m,x=fixture_matrix(tmp_path/'original')
    constant=CausalWindowMatrix(PhysicalCSRStore(np.zeros_like(x),tmp_path/'zero'),m.dataset)
    op=LaggedLinearOperator(constant)
    y=np.arange(constant.shape[0],dtype=np.float64)
    calls=[]
    coefficient,intercept,state=op.solve(y,callback=lambda *a:calls.append(a))
    np.testing.assert_array_equal(coefficient,np.zeros(constant.shape[1]))
    assert intercept==y.mean() and state['converged'] and len(calls)==1


def test_inactive_classical_settings_preserve_existing_configuration_fingerprint():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot
    config=load_config('configs/markets/crypto.yaml')
    before=_configuration_fingerprint_snapshot(config)
    config.training.classical.learning_rate=.023
    assert _configuration_fingerprint_snapshot(config)==before
    config.training.model_name='classical'
    active=_configuration_fingerprint_snapshot(config)
    config.training.classical.learning_rate=.024
    assert _configuration_fingerprint_snapshot(config)!=active


def test_source_cache_fails_closed_on_corruption(tmp_path):
    m, _ = fixture_matrix(tmp_path)
    m.store.block(0)
    path = m.store.root/'block_00000'/'data.npy'
    content = bytearray(path.read_bytes()); content[-1] ^= 1; path.write_bytes(content)
    fresh = PhysicalCSRStore(m.store.source, tmp_path, block_rows=2)
    with pytest.raises(ValueError, match="corrupt"): fresh.block(0)


def test_shared_channel_contraction_keeps_forward_and_adjoint(tmp_path):
    m,x=fixture_matrix(tmp_path/'dense')
    x=np.nan_to_num(x)
    shared=np.arange(24,dtype=np.float32).reshape(12,2)/100
    x[:,:,3:]=shared[:,None,:]
    def rectangles(rows):
        yield np.arange(len(rows)),np.arange(3),np.array([2]),x[rows,:,2:3]
    source=SimpleNamespace(shape=x.shape,_stockagent_factorized_features=True,
        content_fingerprint=dict(fixture='shared'),base=x[:,:,:2],common=shared,
        symbol_indices=np.arange(3),manifest={'individual_channels':['asset']},
        compact_transfer_rectangles=rectangles)
    matrix=CausalWindowMatrix(PhysicalCSRStore(source,tmp_path/'shared',block_rows=2),m.dataset)
    dense=matrix.rows(slice(None)).toarray().astype(np.float64)
    operator=LaggedLinearOperator(matrix)
    w=np.arange(15,dtype=np.float64)/10
    r=np.arange(matrix.shape[0],dtype=np.float64)/10
    np.testing.assert_allclose(operator._matvec(w),dense@w,atol=1e-7)
    np.testing.assert_allclose(operator._rmatvec(r),dense.T@r,atol=1e-7)
    np.testing.assert_allclose(operator._rmatvec(r,squared=True),(dense*dense).T@r,atol=1e-7)


def test_parallel_sparse_operator_keeps_deterministic_reduction(tmp_path):
    m,_=fixture_matrix(tmp_path)
    a,b=LaggedLinearOperator(m),LaggedLinearOperator(m,threads=2)
    w=np.arange(m.shape[1],dtype=np.float64)/10
    r=np.arange(m.shape[0],dtype=np.float64)/10
    np.testing.assert_array_equal(a._matvec(w),b._matvec(w))
    np.testing.assert_array_equal(a._rmatvec(r),b._rmatvec(r))


@pytest.mark.skipif(not torch.cuda.is_available(),reason='CUDA required')
@pytest.mark.filterwarnings('ignore:Sparse CSR tensor support is in beta state:UserWarning')
def test_cuda_lagged_operator_matches_dense_financial_input(tmp_path):
    m,_=fixture_matrix(tmp_path)
    dense=m.rows(slice(None)).toarray().astype(np.float64)
    op=LaggedLinearOperator(m,backend='cuda')
    w=np.arange(m.shape[1],dtype=np.float64)/10
    r=np.arange(m.shape[0],dtype=np.float64)/10
    np.testing.assert_allclose(op._matvec(w),dense@w,atol=2e-5,rtol=1e-5)
    np.testing.assert_allclose(op._rmatvec(r),dense.T@r,atol=2e-5,rtol=1e-5)
    mean,scale=op.moments()
    expected=dense.std(0);expected[expected<1e-12]=1
    np.testing.assert_allclose(mean,dense.mean(0),atol=1e-12)
    np.testing.assert_allclose(scale,expected,atol=1e-12)
    y=np.arange(m.shape[0],dtype=np.float64)/10
    coefficient,intercept,state=op.solve(y,alpha=2,tolerance=1e-5,max_iter=100)
    centered=(dense-dense.mean(0))/expected
    oracle=np.linalg.solve(centered.T@centered+2*np.eye(dense.shape[1]),centered.T@(y-y.mean()))/expected
    np.testing.assert_allclose(dense@coefficient+intercept,dense@oracle+y.mean()-dense.mean(0)@oracle,atol=1e-4)
    assert state['converged']


def test_streamed_lightgbm_matches_numeric_zero_dense_dataset(tmp_path):
    lgb = pytest.importorskip('lightgbm')
    m, _ = fixture_matrix(tmp_path)
    params = dict(objective='regression', num_threads=1, verbosity=-1,
        max_bin=15, bin_construct_sample_cnt=m.shape[0], min_data_in_leaf=1,
        min_data_in_bin=1, num_leaves=3, zero_as_missing=False,
        feature_pre_filter=False, deterministic=True, force_col_wise=True)
    ds = lightgbm_dataset(m, m.y, params)
    a = lgb.train(params, ds, num_boost_round=4)
    dense = m.rows(slice(None)).toarray()
    b = lgb.train(params,lgb.Dataset(dense,label=m.y,params=params),num_boost_round=4)
    np.testing.assert_allclose(a.predict(dense), b.predict(dense), atol=1e-7)


def test_native_bin_cache_reuses_features_and_replaces_labels(tmp_path,monkeypatch):
    lgb=pytest.importorskip('lightgbm')
    m,_=fixture_matrix(tmp_path/'physical')
    params=dict(objective='regression',num_threads=1,verbosity=-1,max_bin=15,
        bin_construct_sample_cnt=m.shape[0],min_data_in_leaf=1,min_data_in_bin=1,
        num_leaves=3,feature_pre_filter=False,force_col_wise=True)
    cache=tmp_path/'bins'
    first=lightgbm_dataset(m,m.y,params,cache_root=cache)
    dense=m.rows(slice(None)).toarray()
    monkeypatch.setattr(m,'rows',lambda _:pytest.fail('bin cache repeated feature gathering'))
    changed=-m.y
    second=lightgbm_dataset(m,changed,params,cache_root=cache)
    np.testing.assert_array_equal(second.get_label(),changed)
    cached=lgb.train(params,second,num_boost_round=3)
    oracle=lgb.train(params,lgb.Dataset(dense,label=changed,params=params),num_boost_round=3)
    np.testing.assert_allclose(cached.predict(dense),oracle.predict(dense),atol=1e-7)
    assert first._stockagent_bin_key==second._stockagent_bin_key


def test_ridge_safe_checkpoint_and_cash_allocation(tmp_path):
    kwargs=dict(lookback=3,num_features=5,num_symbols=3,long_only=False,
                portfolio_activation='identity',kind='ridge')
    a = CrossSectionalClassical(**kwargs)
    a.coefficient=np.arange(15,dtype=np.float64)/100; a.iteration=2
    p=tmp_path/'model.pt'; torch.save(a.state_dict(),p)
    b=CrossSectionalClassical(**kwargs)
    b.load_state_dict(torch.load(p,weights_only=True))
    x=torch.randn(2,3,3,5); mask=torch.tensor([[True,False,True],[True,True,True]])
    torch.testing.assert_close(a(x,mask),b(x,mask))
    assert torch.all(a(x,mask).abs().sum(-1)<1)
    assert a(x,mask)[0,1]==0


def test_native_booster_continuation_matches_uninterrupted(tmp_path):
    lgb=pytest.importorskip('lightgbm')
    from stockagent.training.classical_solvers import lightgbm_continue, lightgbm_scores
    m,_=fixture_matrix(tmp_path)
    params=dict(objective='regression',num_threads=1,verbosity=-1,
        min_data_in_leaf=1,min_data_in_bin=1,num_leaves=3,max_bin=15,
        bin_construct_sample_cnt=m.shape[0],feature_pre_filter=False,force_col_wise=True)
    ds=lightgbm_dataset(m,m.y,params)
    first=lightgbm_continue(params,[ds]);first.update();first.update()
    initial=lightgbm_scores(first,0).copy()
    restored=lgb.Booster(model_str=first.model_to_string())
    ds2=lightgbm_dataset(m,m.y,params)
    continued=lightgbm_continue(params,[ds2],previous=restored,initial_scores=[initial])
    continued.update();continued.update()
    first.update();first.update()
    np.testing.assert_allclose(continued.predict(m.rows(slice(None))),first.predict(m.rows(slice(None))),atol=1e-10)


@pytest.mark.parametrize('kind',['ridge','lightgbm','lightgbm_ranker','xgboost','catboost'])
def test_tiny_canonical_lifecycle(kind,tmp_path,monkeypatch):
    monkeypatch.setenv('MPLBACKEND','Agg')
    if kind.startswith('lightgbm'):
        pytest.importorskip('lightgbm',minversion='4.7')
    elif kind=='xgboost':
        pytest.importorskip('xgboost',minversion='3.2')
    elif kind=='catboost':
        pytest.importorskip('catboost',minversion='1.2.8')
    from stockagent.config import load_config
    from stockagent.data.panel import PanelData
    from stockagent.data.walkforward import WalkForwardFold
    from stockagent.training.trainer import run_training
    from stockagent.training.lifecycle import validate_completed_training_artifacts
    rng=np.random.default_rng(48)
    dates=np.concatenate([np.arange(f'{y}-02-01',f'{y}-02-21',dtype='datetime64[D]')
                          for y in [2019,2020,2021]])
    n,s,f=len(dates),3,5
    mask=np.ones((n,s),dtype=bool)
    returns=rng.normal(.001,.01,(n,s)).astype(np.float32)
    panel=PanelData(dates,['A','B','C'],[f'f{i}' for i in range(f)],
        rng.normal(size=(n,s,f)).astype(np.float32),returns,mask,mask,
        returns.mean(1),np.full((n,s),100,dtype=np.float32),
        can_buy_mask=mask,can_sell_mask=mask)
    config=load_config('configs/markets/crypto.yaml')
    config.environment.device='cpu'
    config.runner.require_cuda=False
    config.trading.execution_mode='naive'
    config.trading.long_only=True
    config.trading.portfolio_activation='pre_normalized'
    config.training.model_name='classical'
    config.training.multi_gpu_strategy='none'
    config.training.pretrained_initialization_root=None
    config.training.lookback=3
    config.training.loss_type='log_utility'
    config.training.epochs=3
    config.training.record_epoch_curve=True
    config.training.curve_plot_async=False
    config.training.defer_epoch_curve_plot_until_end=True
    config.training.classical.kind=kind
    config.training.classical.threads=1
    config.training.classical.cache_root=str(tmp_path/'cache')
    config.training.classical.bin_sample_count=20
    config.training.classical.min_data_in_leaf=2
    config.training.classical.batch_rows=7
    config.training.classical.num_leaves=3
    fold=WalkForwardFold(1,np.arange(3,20),np.arange(20,40),np.arange(40,59),[2019],[2020],[2021])
    output=tmp_path/'run'
    results=run_training(panel,[fold],config,output,resume=False)
    assert len(results)==1
    report=validate_completed_training_artifacts(output,fold_ids=[1],group_names=['train_2019'])
    assert report.ok,report
