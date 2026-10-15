"""Full-schema mechanism/adjoint oracles for the opt-in resident CSR stem."""
from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.data.factorized_panel import FactorizedPanelFeatures, FactorizedPanelSlab, write_array_block
from stockagent.models.financial_transformer import CandleEncoder, FinancialTransformerModel
from stockagent.models.factorized_sparse_projection import project_factorized_sparse_rows

pytestmark=pytest.mark.filterwarnings('ignore:Sparse CSR tensor support is in beta state:UserWarning')


def source_fixture(path, *, constant=False):
    rng=np.random.default_rng(9)
    base=rng.normal(size=(9,4,2)).astype(np.float32)
    individual=np.zeros((9,4,23),np.float32)
    individual[:4,:,[2,5]]=rng.normal(size=(4,4,2))
    individual[4:,:,[8,10]]=rng.normal(size=(5,4,2))
    individual[3,2,20]=-0.
    individual[6,1,21]=1e-18
    common=rng.normal(size=(9,3)).astype(np.float32)
    if constant:
        base[:]=1000.+rng.normal(size=base.shape)*.02
        individual[:]=1000.+rng.normal(size=individual.shape)*.02
        common[:]=1000.+rng.normal(size=common.shape)*.02
    np.save(path/'common.npy',common)
    blocks=[]
    for a,b in [(0,4),(4,9)]:
        parts=[]
        for c,d in [(0,2),(2,4)]:
            proof=write_array_block(path/f'{a}_{c}.zst',individual[a:b,c:d],omit_zero_columns=True)
            parts.append({**proof,'symbol_start':c})
        blocks.append({'start':a,'shape':[b-a,4,23],'symbol_blocks':parts})
    return FactorizedPanelFeatures(path,{'common':{'path':'common.npy'},'blocks':blocks,
        'individual_channels':[str(i) for i in range(23)],'common_channels':['a','b','c']},base)


@pytest.mark.parametrize('norm',['layernorm','rmsnorm'])
@pytest.mark.parametrize('device',['cpu','cuda'])
def test_full_affine_every_parameter_adjoint_and_cache_reuse(tmp_path,norm,device):
    if device=='cuda' and not torch.cuda.is_available():pytest.skip('requires CUDA')
    source=source_fixture(tmp_path)
    torch.manual_seed(37)
    candle=CandleEncoder(num_features=28,d_model=8,dropout=0.,norm_type=norm,
        ffn_type='swiglu',sanitize_inputs=True,projection_fp32=True,causal_feature_rms_normalization=True)
    candle.set_causal_feature_rms_normalizer(torch.rand(28)+.5,torch.arange(28)!=13)
    with torch.no_grad():
        candle.input_norm.weight.copy_(torch.randn(28))
        if getattr(candle.input_norm,'bias',None) is not None:
            candle.input_norm.bias.copy_(torch.randn(28))
    candle=candle.to(device)
    oracle=deepcopy(candle)
    model=SimpleNamespace(candle_encoder=candle,factorized_sparse_cache_bytes=1024**2)
    rows=np.array([3,4,6,1,3,3])
    slab=FactorizedPanelSlab(source,rows,device=device)
    expected=oracle(torch.from_numpy(source[rows]).to(device))[0]
    actual=project_factorized_sparse_rows(model,slab)
    torch.testing.assert_close(actual,expected,rtol=3e-5,atol=3e-5)
    coefficients=torch.randn_like(actual)
    (actual*coefficients).sum().backward();(expected*coefficients).sum().backward()
    for (name,p),(_,q) in zip(candle.named_parameters(),oracle.named_parameters(),strict=True):
        assert (p.grad is None)==(q.grad is None),name
        if p.grad is not None:
            torch.testing.assert_close(p.grad,q.grad,rtol=7e-5,atol=7e-5,msg=lambda text:name+': '+text)
    store=model._factorized_sparse_row_store
    before=store.host_payload_bytes
    with torch.no_grad():
        candle.joint_projection.proj.weight.add_(.02)
    changed=project_factorized_sparse_rows(model,slab)
    assert not torch.equal(changed,actual)
    assert store.host_payload_bytes==before
    assert store.hits>0 and store.bytes<=store.budget_bytes
    with torch.no_grad():
        candle.causal_feature_rms_scale.copy_(candle.causal_feature_rms_scale.clone())
        candle.causal_feature_active_mask.copy_(candle.causal_feature_active_mask.clone())
    project_factorized_sparse_rows(model,slab)
    assert store.host_payload_bytes==before
    candle.causal_feature_rms_scale.add_(.1)
    with pytest.raises(RuntimeError,match='normalizer changed'):
        project_factorized_sparse_rows(model,slab)


def test_centering_remains_stable_with_nearly_constant_full_rows(tmp_path):
    source=source_fixture(tmp_path,constant=True)
    candle=CandleEncoder(num_features=28,d_model=8,dropout=0.,norm_type='layernorm',
        ffn_type='gelu',sanitize_inputs=True,projection_fp32=True)
    model=SimpleNamespace(candle_encoder=candle,factorized_sparse_cache_bytes=1024**2)
    slab=FactorizedPanelSlab(source,np.arange(9))
    actual=project_factorized_sparse_rows(model,slab)
    # A high-precision dense oracle checks the centered representation directly;
    # compare in FP64 so native FP32 LayerNorm's Welford rounding is not mistaken
    # for catastrophic projected-mean cancellation.
    raw=torch.from_numpy(source[np.arange(9)]).double()
    norm=candle.input_norm
    z=torch.nn.functional.layer_norm(raw,(28,),norm.weight.double(),norm.bias.double(),norm.eps)
    p=torch.nn.functional.linear(z,candle.joint_projection.proj.weight.double(),
        candle.joint_projection.proj.bias.double()).float()
    expected=candle._finish_embedding(candle._activate_projected(p))
    torch.testing.assert_close(actual,expected,rtol=2e-4,atol=2e-4)


@pytest.mark.parametrize('checkpoint',[False,True])
def test_full_model_shared_temporal_and_portfolio_adjoint(tmp_path,checkpoint):
    source=source_fixture(tmp_path)
    torch.manual_seed(15)
    model=FinancialTransformerModel(lookback=2,num_features=28,num_symbols=4,d_model=8,
        attention_mode='market_token',temporal_layers=1,temporal_heads=2,
        cross_heads=2,num_market_tokens=2,market_layers=1,head_hidden_dim=8,
        dropout=0.,input_dropout=0.,candle_dropout=0.,candle_projection_fp32=True,
        factorized_compact_projection=True,factorized_sparse_projection=True,
        factorized_sparse_cache_bytes=1024**2,factorized_input_compile=False,
        portfolio_output_mode='score_entmax_scale_separated_cash',return_aux=False)
    model.factorized_encoder_checkpoint=checkpoint
    oracle=deepcopy(model)
    oracle.factorized_sparse_projection=False
    rows=np.arange(2,7)
    mask=torch.ones(4,4,dtype=torch.bool);mask[1,2]=False
    actual=model.forward_from_panel_slab(FactorizedPanelSlab(source,rows),mask)
    expected=oracle.forward_from_panel_slab(torch.from_numpy(source[rows]),mask)
    torch.testing.assert_close(actual,expected,rtol=3e-5,atol=3e-5)
    coefficients=torch.randn_like(actual)
    (actual*coefficients).sum().backward();(expected*coefficients).sum().backward()
    for (name,p),(_,q) in zip(model.named_parameters(),oracle.named_parameters(),strict=True):
        assert (p.grad is None)==(q.grad is None),name
        if p.grad is not None:
            torch.testing.assert_close(p.grad,q.grad,rtol=2e-4,atol=2e-4,msg=lambda text:name+': '+text)


def test_evicted_source_blocks_remain_owned_by_their_pending_adjoint(tmp_path):
    source=source_fixture(tmp_path)
    candle=CandleEncoder(num_features=28,d_model=8,dropout=0.,norm_type='layernorm',
        ffn_type='gelu',sanitize_inputs=True,projection_fp32=True)
    oracle=deepcopy(candle)
    model=SimpleNamespace(candle_encoder=candle,factorized_sparse_cache_bytes=1500)
    rows=np.arange(9)
    actual=project_factorized_sparse_rows(model,FactorizedPanelSlab(source,rows))
    expected=oracle(torch.from_numpy(source[rows]))[0]
    assert len(model._factorized_sparse_row_store.entries)==1
    coefficient=torch.randn_like(actual)
    (actual*coefficient).sum().backward();(expected*coefficient).sum().backward()
    for (name,p),(_,q) in zip(candle.named_parameters(),oracle.named_parameters(),strict=True):
        if p.grad is not None:
            torch.testing.assert_close(p.grad,q.grad,rtol=7e-5,atol=7e-5,msg=lambda text:name+': '+text)


def test_sparse_contract_does_not_change_disabled_fingerprints():
    from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config
    assert _project_temporal_basis_model_config({'factorized_sparse_projection':False,
        'factorized_sparse_cache_bytes':8*1024**3})=={}
    c=_project_temporal_basis_model_config({'factorized_sparse_projection':True,
        'factorized_sparse_cache_bytes':8*1024**3})
    assert c['factorized_sparse_projection_contract']=='full_schema_csr_affine_stable_full_rows_gpu_resident_v2'


def test_evaluation_built_cache_supports_later_training(tmp_path):
    source=source_fixture(tmp_path)
    candle=CandleEncoder(num_features=28,d_model=8,dropout=0.,norm_type='layernorm',
        ffn_type='gelu',sanitize_inputs=True,projection_fp32=True)
    model=SimpleNamespace(candle_encoder=candle,factorized_sparse_cache_bytes=1024**2)
    slab=FactorizedPanelSlab(source,np.arange(9))
    with torch.inference_mode():
        project_factorized_sparse_rows(model,slab)
    result=project_factorized_sparse_rows(model,slab)
    (result*torch.randn_like(result)).sum().backward()
    assert torch.isfinite(candle.joint_projection.proj.weight.grad).all()


@pytest.mark.parametrize('norm',['layernorm','rmsnorm'])
@pytest.mark.parametrize('device',['cpu','cuda'])
def test_verified_disk_rows_reuse_source_and_all_parameter_adjoint(tmp_path,monkeypatch,norm,device):
    if device=='cuda' and not torch.cuda.is_available():pytest.skip('requires CUDA')
    for name in ('first','second'):(tmp_path/name).mkdir()
    monkeypatch.setenv('STOCKAGENT_SPARSE_ROW_DISK_CACHE_DIR',str(tmp_path/'csr-cache'))
    source=source_fixture(tmp_path/'first')
    torch.manual_seed(41)
    candle=CandleEncoder(num_features=28,d_model=8,dropout=0.,norm_type=norm,
        ffn_type='gelu',sanitize_inputs=True,projection_fp32=True,
        causal_feature_rms_normalization=True)
    candle.set_causal_feature_rms_normalizer(torch.arange(28).float()/10+1,torch.arange(28)!=13)
    candle=candle.to(device)
    second_candle=deepcopy(candle)
    first=SimpleNamespace(candle_encoder=candle,factorized_sparse_cache_bytes=1024**2)
    a=project_factorized_sparse_rows(first,FactorizedPanelSlab(source,np.arange(9),device=device))
    assert first._factorized_sparse_row_store.disk.stats['writes']==2
    again=source_fixture(tmp_path/'second')
    assert again.content_fingerprint==source.content_fingerprint
    monkeypatch.setattr(again._cache,'get',lambda _:pytest.fail('disk hit must avoid payload decompression'))
    second=SimpleNamespace(candle_encoder=second_candle,factorized_sparse_cache_bytes=1024**2)
    b=project_factorized_sparse_rows(second,FactorizedPanelSlab(again,np.arange(9),device=device))
    assert second._factorized_sparse_row_store.disk.stats['hits']==2
    torch.testing.assert_close(a,b,rtol=0,atol=0)
    coeff=torch.randn_like(a)
    (a*coeff).sum().backward();(b*coeff).sum().backward()
    for (name,p),(_,q) in zip(candle.named_parameters(),second_candle.named_parameters(),strict=True):
        assert (p.grad is None)==(q.grad is None),name
        if p.grad is not None:torch.testing.assert_close(p.grad,q.grad,rtol=0,atol=0)


def test_disk_rows_reject_damage_and_isolate_fold_normalization(tmp_path,monkeypatch):
    from stockagent.data.factorized_sparse_rows import FactorizedSparseRowStore
    (tmp_path/'source').mkdir()
    source=source_fixture(tmp_path/'source')
    monkeypatch.setenv('STOCKAGENT_SPARSE_ROW_DISK_CACHE_DIR',str(tmp_path/'csr-cache'))
    candle=CandleEncoder(num_features=28,d_model=8,dropout=0.,norm_type='layernorm',
        ffn_type='gelu',sanitize_inputs=True,projection_fp32=True,
        causal_feature_rms_normalization=True)
    store=FactorizedSparseRowStore(source,candle,'cpu',budget_bytes=1024**2)
    before=store._build(0)
    original=before.values.clone()
    path=next(store.disk.root.glob('block_00000/generation-*/values.npy'))
    damaged=bytearray(path.read_bytes());damaged[-1]^=1;path.write_bytes(damaged)
    rebuild=FactorizedSparseRowStore(source,candle,'cpu',budget_bytes=1024**2)
    after=rebuild._build(0)
    assert rebuild.disk.stats['rejected']==1 and rebuild.disk.stats['writes']==1
    assert list(rebuild.disk.root.glob('block_00000/rejected-*.json'))
    torch.testing.assert_close(after.values,original,rtol=0,atol=0)
    changed=deepcopy(candle)
    changed.causal_feature_rms_scale.add_(.1)
    other=FactorizedSparseRowStore(source,changed,'cpu',budget_bytes=1024**2)
    assert other.disk.root!=store.disk.root
    alternate=FactorizedSparseRowStore(source.subset_symbols(np.array([1,0])),candle,'cpu',budget_bytes=1024**2)
    assert alternate.disk.root!=store.disk.root

