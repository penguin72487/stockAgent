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


@pytest.mark.parametrize('device', ['cpu', 'cuda'])
@pytest.mark.parametrize('width', [7, 64, 129])
def test_fixed_row_projection_empty_rows_dense_oracle_and_repeatable_forward(device, width):
    if device == 'cuda' and not torch.cuda.is_available():
        pytest.skip('requires CUDA')
    from stockagent.models.factorized_sparse_projection import fixed_row_csr_projection
    torch.manual_seed(53)
    dense = torch.randn(11, 131, device=device)
    dense[dense.abs() < .8] = 0.
    dense[3] = 0.
    matrix = dense.to_sparse_csr()
    weight = torch.randn(131, width, device=device, requires_grad=True)
    expected = dense.double() @ weight.double()
    actual = fixed_row_csr_projection(matrix, weight)
    torch.testing.assert_close(actual.double(), expected, rtol=2e-5, atol=2e-5)
    coefficients = torch.randn_like(actual)
    (actual * coefficients).sum().backward()
    torch.testing.assert_close(weight.grad.double(), dense.double().t() @ coefficients.double(),
                               rtol=2e-5, atol=2e-5)
    for _ in range(3):
        assert torch.equal(actual.detach(), fixed_row_csr_projection(matrix, weight).detach())
    assert torch.equal(actual[3], torch.zeros_like(actual[3]))


@pytest.mark.parametrize('algorithm', ['0', '6'])
@pytest.mark.parametrize('index_dtype', [torch.int32, torch.int64])
@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA cuSPARSE')
def test_direct_csr_transpose_pending_adjoints_and_captured_replay(monkeypatch, algorithm, index_dtype):
    from stockagent.models.factorized_sparse_projection import fixed_row_csr_projection, _direct_csr_transpose_mm
    monkeypatch.setenv('STOCKAGENT_SPARSE_ADJOINT_BACKEND', 'cusparse_csr_transpose')
    monkeypatch.setenv('STOCKAGENT_SPARSE_ADJOINT_ALGORITHM', algorithm)
    torch.manual_seed(521)
    matrices = []
    for rows in [19, 23]:
        dense = torch.randn(rows, 37, device='cuda'); dense[dense.abs() < .8] = 0.; dense[4] = 0.
        csr = dense.to_sparse_csr()
        with torch.sparse.check_sparse_tensor_invariants(True):
            csr = torch.sparse_csr_tensor(csr.crow_indices().to(index_dtype), csr.col_indices().to(index_dtype),
                csr.values(), size=dense.shape, device='cuda', check_invariants=True)
        matrices.append((dense, csr))
    weight = torch.randn(37, 11, device='cuda', requires_grad=True)
    pending = []
    oracle = torch.zeros_like(weight, dtype=torch.float64)
    for dense, csr in matrices:
        coefficients = torch.randn(11, dense.size(0), device='cuda').t()  # strided incoming adjoint
        result = fixed_row_csr_projection(csr, weight)
        torch.testing.assert_close(result.double(), dense.double() @ weight.double(), rtol=2e-5, atol=2e-5)
        pending.append((result*coefficients).sum())
        oracle += dense.double().t() @ coefficients.double()
    # Backward must use the schedule owned by each pending forward.
    monkeypatch.setenv('STOCKAGENT_SPARSE_ADJOINT_BACKEND', 'native')
    sum(pending).backward()
    torch.testing.assert_close(weight.grad.double(), oracle, rtol=7e-5, atol=7e-5)
    dense, csr = matrices[0]
    coefficients = torch.randn(dense.size(0), 11, device='cuda')
    stream = torch.cuda.Stream()
    stream.wait_stream(torch.cuda.current_stream())
    with torch.cuda.stream(stream):
        for _ in range(2): _direct_csr_transpose_mm(csr, coefficients, int(algorithm))
    stream.synchronize()
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph, stream=stream):
        observed = _direct_csr_transpose_mm(csr, coefficients, int(algorithm))
    torch.cuda.current_stream().wait_stream(stream)
    for scale in [1., -2., .25]:
        coefficients.fill_(scale)
        graph.replay()
        torch.testing.assert_close(observed.double(), dense.double().t() @ coefficients.double(), rtol=7e-5, atol=7e-5)
    empty = torch.zeros_like(dense).to_sparse_csr()
    assert torch.equal(_direct_csr_transpose_mm(empty, coefficients, int(algorithm)), torch.zeros_like(observed))


def test_direct_sparse_adjoint_is_opt_in_and_versions_optimizer_contract(monkeypatch):
    from stockagent.models.factorized_sparse_projection import sparse_adjoint_contract
    from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config
    monkeypatch.delenv('STOCKAGENT_SPARSE_ADJOINT_BACKEND', raising=False)
    assert sparse_adjoint_contract() is None
    original = _project_temporal_basis_model_config({'factorized_sparse_projection': True})
    monkeypatch.setenv('STOCKAGENT_SPARSE_ADJOINT_BACKEND', 'cusparse_csr_transpose')
    selected = _project_temporal_basis_model_config({'factorized_sparse_projection': True})
    assert original != selected and selected['factorized_sparse_adjoint_contract']['algorithm'] == 6
    monkeypatch.setenv('STOCKAGENT_SPARSE_ADJOINT_ALGORITHM', '0')
    assert selected != _project_temporal_basis_model_config({'factorized_sparse_projection': True})
    monkeypatch.setenv('STOCKAGENT_SPARSE_ADJOINT_BACKEND', 'unsupported')
    with pytest.raises(ValueError, match='sparse adjoint backend'):
        sparse_adjoint_contract()


def test_direct_csr_schedule_versions_the_full_optimizer_contract(monkeypatch):
    import json
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot, _training_checkpoint_contract
    cfg = load_config('configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml')
    cfg.training.financial_transformer.factorized_sparse_projection = True
    assert cfg.data.factorized_feature_manifest and cfg.training.model_name == 'financial_transformer'
    configurations, training = [], []
    for backend, algorithm in [('native', '6'), ('cusparse_csr_transpose', '6'), ('cusparse_csr_transpose', '0')]:
        monkeypatch.setenv('STOCKAGENT_SPARSE_ADJOINT_BACKEND', backend)
        monkeypatch.setenv('STOCKAGENT_SPARSE_ADJOINT_ALGORITHM', algorithm)
        configurations.append(json.dumps(_configuration_fingerprint_snapshot(cfg), sort_keys=True))
        training.append(json.dumps(_training_checkpoint_contract(cfg), sort_keys=True))
    assert len(set(configurations)) == len(set(training)) == 3


def test_resident_graph_accumulation_storage_versions_both_checkpoint_contracts():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot, _training_checkpoint_contract
    cfg = load_config('configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml')
    cfg.training.financial_transformer.factorized_sparse_projection = True
    native = _configuration_fingerprint_snapshot(cfg), _training_checkpoint_contract(cfg)
    assert all(value['training']['factorized_input_graph_contract'].endswith('abi_v10')
               if 'training' in value else value['factorized_input_graph_contract'].endswith('abi_v10')
               for value in native)
    cfg.training.financial_transformer.factorized_portfolio_cuda_graphs = True
    configuration = _configuration_fingerprint_snapshot(cfg)
    training = _training_checkpoint_contract(cfg)
    assert configuration['training']['factorized_input_graph_contract'] == training['factorized_input_graph_contract']
    assert training['factorized_input_graph_contract'].endswith('abi_v11')
    assert (configuration, training) != native


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


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA graphs')
def test_resident_encoder_and_portfolio_graphs_preserve_pending_full_model_adjoints(tmp_path):
    source = source_fixture(tmp_path)
    torch.manual_seed(15)
    oracle = FinancialTransformerModel(lookback=2,num_features=28,num_symbols=4,d_model=8,
        attention_mode='market_token',temporal_layers=1,temporal_heads=2,
        cross_heads=2,num_market_tokens=2,market_layers=1,head_hidden_dim=8,
        dropout=0.,input_dropout=0.,candle_dropout=0.,candle_projection_fp32=True,
        factorized_compact_projection=True,factorized_sparse_projection=True,
        factorized_sparse_cache_bytes=1024**2,factorized_input_compile=False,
        portfolio_output_mode='score_entmax_scale_separated_cash',return_aux=False).cuda().train()
    oracle.factorized_encoder_checkpoint = False
    oracle.factorized_encoder_force_retention = True
    candidate = deepcopy(oracle)
    candidate.factorized_input_compile = True
    candidate.factorized_cuda_graphs = True
    candidate.factorized_portfolio_cuda_graphs = True
    for _ in range(2):
        references, actuals, coefficients = [], [], []
        for start in [1, 3]:
            rows = np.arange(start, start+5)
            mask = torch.ones(4,4,dtype=torch.bool,device='cuda')
            mask[1,2] = False
            reference = oracle.forward_from_panel_slab(FactorizedPanelSlab(source, rows, device='cuda'), mask)
            actual = candidate.forward_from_panel_slab(FactorizedPanelSlab(source, rows, device='cuda'), mask)
            torch.testing.assert_close(actual, reference, rtol=3e-4, atol=3e-5)
            references.append(reference); actuals.append(actual); coefficients.append(torch.randn_like(actual))
        sum((v*c).sum() for v,c in zip(references,coefficients)).backward()
        sum((v*c).sum() for v,c in zip(actuals,coefficients)).backward()
        for (name, actual), (_, expected) in zip(candidate.named_parameters(), oracle.named_parameters(), strict=True):
            assert (actual.grad is None) == (expected.grad is None), name
            if actual.grad is not None:
                torch.testing.assert_close(actual.grad, expected.grad, rtol=2e-3, atol=2e-4,
                                           msg=lambda message:name+': '+message)
        oracle.zero_grad(set_to_none=True); candidate.zero_grad(set_to_none=True)
    from stockagent.training.trainer import _cuda_graph_capture_snapshot
    evidence = _cuda_graph_capture_snapshot(SimpleNamespace(training=SimpleNamespace(
        financial_transformer=SimpleNamespace(factorized_cuda_graphs=True, factorized_portfolio_cuda_graphs=True))))
    assert evidence['cuda_graph_runtime_inspection_ok'] and evidence['cuda_graph_root_count_local'] > 0


@pytest.mark.parametrize('encoder_graphs', [False, True])
@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA graphs')
def test_marked_annual_graph_replay_preserves_accumulated_leaf_gradients(tmp_path, encoder_graphs, *, replay_passes=2):
    """The record pass and accumulated backwards cross real graph generations."""
    from stockagent.training.trainer import _own_cudagraph_parameter_gradients, _cuda_graph_capture_snapshot
    source = source_fixture(tmp_path)
    torch.manual_seed(15)
    oracle = FinancialTransformerModel(lookback=2, num_features=28, num_symbols=4, d_model=32,
        attention_mode='market_token', use_latent_factors=True, use_market_tokens=True,
        temporal_layers=1, temporal_heads=4, cross_heads=4, num_latent_factors=2,
        num_market_tokens=2, market_layers=1, head_hidden_dim=32,
        dropout=0., input_dropout=0., candle_dropout=0., candle_projection_fp32=True,
        portfolio_blocks_fp32=True, factorized_compact_projection=True,
        factorized_sparse_projection=True, factorized_sparse_cache_bytes=1024**2,
        factorized_input_compile=False, portfolio_output_mode='score_entmax_scale_separated_cash',
        return_aux=False).cuda().train()
    oracle.factorized_encoder_checkpoint = False
    oracle.factorized_encoder_force_retention = True
    candidate = deepcopy(oracle)
    candidate.factorized_input_compile = True
    candidate.factorized_cuda_graphs = encoder_graphs
    candidate.factorized_portfolio_cuda_graphs = True
    mask = torch.ones(4, 4, dtype=torch.bool, device='cuda')

    def forward(model, start):
        torch.compiler.cudagraph_mark_step_begin()
        with torch.autocast('cuda', dtype=torch.bfloat16):
            return model.forward_from_panel_slab(
                FactorizedPanelSlab(source, np.arange(start, start + 5), device='cuda'), mask)

    for _ in range(replay_passes):
        recorded = []
        for start in [1, 3, 1]:
            value = forward(candidate, start)
            recorded.append(value.detach().clone())
            del value
        gradient_ids = {}
        for index, start in enumerate([1, 3, 1]):
            reference, actual = forward(oracle, start), forward(candidate, start)
            torch.testing.assert_close(actual, reference, rtol=3e-3, atol=1e-4)
            torch.testing.assert_close(actual, recorded[index], rtol=0, atol=0)
            coefficient = torch.randn_like(actual)
            (reference * coefficient).sum().backward()
            (actual * coefficient).sum().backward()
            _own_cudagraph_parameter_gradients(candidate)
            for name, parameter in candidate.named_parameters():
                if parameter.grad is not None:
                    if name in gradient_ids:
                        assert id(parameter.grad) == gradient_ids[name], name
                    gradient_ids[name] = id(parameter.grad)
            del reference, actual, coefficient
        for (name, actual), (_, expected) in zip(candidate.named_parameters(), oracle.named_parameters(), strict=True):
            assert (actual.grad is None) == (expected.grad is None), name
            if actual.grad is not None:
                torch.testing.assert_close(actual.grad, expected.grad, rtol=4e-3, atol=8e-4,
                                           msg=lambda message: name + ': ' + message)
        oracle.zero_grad(set_to_none=True)
        candidate.zero_grad(set_to_none=True)
    evidence = _cuda_graph_capture_snapshot(SimpleNamespace(training=SimpleNamespace(
        financial_transformer=SimpleNamespace(factorized_cuda_graphs=encoder_graphs,
                                              factorized_portfolio_cuda_graphs=True))))
    assert evidence['cuda_graph_runtime_inspection_ok'] and evidence['cuda_graph_root_count_local'] > 0


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
    assert c['factorized_sparse_projection_contract']=='full_schema_csr_fixed_row_fp32_forward_v3'


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
    from stockagent.models.factorized_sparse_projection import sparse_adjoint_contract
    # Direct transpose SpMM uses FP32 atomic accumulation. Source and forward
    # bytes must stay identical; its versioned backward may reorder FP32 sums.
    tolerance = (dict(rtol=7e-5, atol=7e-5) if device == 'cuda' and sparse_adjoint_contract()
                 else dict(rtol=0, atol=0))
    for (name,p),(_,q) in zip(candle.named_parameters(),second_candle.named_parameters(),strict=True):
        assert (p.grad is None)==(q.grad is None),name
        if p.grad is not None:torch.testing.assert_close(p.grad,q.grad,**tolerance)


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

