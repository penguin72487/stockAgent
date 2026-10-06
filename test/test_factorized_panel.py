from dataclasses import replace
import json

import numpy as np
import pytest
import torch

from stockagent.data.factorized_panel import CONTRACT, attach_factorized_features, file_sha256, write_array_block
from stockagent.data.panel import PanelData
from stockagent.training.checkpoint_contract import _panel_array_content_fingerprint
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.trainer import _fit_masked_training_feature_rms, _panel_training_transform_fingerprint
from stockagent.training.windowed import dataset_to_windowed_tensors


def test_partitioned_aot_backward_contract_is_scoped():
    from types import SimpleNamespace
    from stockagent.models.factorized_input import _call_partition
    import torch._functorch.config as config
    import torch._dynamo.config as dynamo_config
    model=SimpleNamespace(factorized_input_compile=True,
        candle_encoder=SimpleNamespace(candle_query=SimpleNamespace(device=SimpleNamespace(type="cuda"))))
    previous=config.backward_pass_autocast
    previous_ddp=dynamo_config.optimize_ddp
    seen=_call_partition(model,lambda: (config.backward_pass_autocast,dynamo_config.optimize_ddp))
    assert seen==("off",False) and config.backward_pass_autocast==previous
    assert dynamo_config.optimize_ddp==previous_ddp
    def failing_partition():
        assert config.backward_pass_autocast=="off" and dynamo_config.optimize_ddp is False
        raise ValueError("partition failure")
    with pytest.raises(ValueError,match="partition failure"):
        _call_partition(model,failing_partition)
    assert config.backward_pass_autocast==previous and dynamo_config.optimize_ddp==previous_ddp


def test_factorized_compiled_graph_schedule_versions_new_trajectories():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot, _training_checkpoint_contract
    config=load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_bf16_v2.yaml")
    snapshot=_configuration_fingerprint_snapshot(config)
    assert snapshot["training"]["factorized_input_graph_contract"]=="compiled_partition_no_ddp_resplit_v1"
    assert _training_checkpoint_contract(config)["factorized_input_graph_contract"]=="compiled_partition_no_ddp_resplit_v1"
    eager=replace(config,training=replace(config.training,enable_torch_compile=False))
    assert "factorized_input_graph_contract" not in _configuration_fingerprint_snapshot(eager)["training"]
    assert "factorized_input_graph_contract" not in _training_checkpoint_contract(eager)
    dense=replace(config,data=replace(config.data,factorized_feature_manifest=""))
    assert "factorized_input_graph_contract" not in _configuration_fingerprint_snapshot(dense)["training"]
    assert "factorized_input_graph_contract" not in _training_checkpoint_contract(dense)


@pytest.mark.parametrize("scope",["resume","artifact"])
def test_prior_inner_ddp_split_optimizer_is_rejected(scope,tmp_path):
    from copy import deepcopy
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import (
        _stable_fingerprint,_training_checkpoint_contract,validate_checkpoint_manifest)
    config=load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_bf16_v4.yaml")
    training=_training_checkpoint_contract(config)
    fingerprints={name:"unchanged-fixture-"+name for name in
        ("data","data_schema","model","training","evaluation","trading","walk_forward")}
    fingerprints["training"]=_stable_fingerprint(training)
    expected={"schema_version":4,"contracts":{"training":training},"fingerprints":fingerprints}
    checkpoint={"experiment_manifest":deepcopy(expected)}
    validate_checkpoint_manifest(checkpoint,expected,checkpoint_path=tmp_path/"matching.pt",scope=scope)
    prior=checkpoint["experiment_manifest"]["contracts"]["training"]
    prior.pop("factorized_input_graph_contract")
    checkpoint["experiment_manifest"]["fingerprints"]["training"]=_stable_fingerprint(prior)
    with pytest.raises(RuntimeError,match="semantic fingerprint mismatch.*training"):
        validate_checkpoint_manifest(checkpoint,expected,checkpoint_path=tmp_path/"prior.pt",scope=scope)


def test_partitioned_operation_dtype_policy_is_opt_in():
    from types import SimpleNamespace
    from stockagent.models.factorized_input import _partition_compile_options
    model=SimpleNamespace(temporal_basis_feature_encoder=SimpleNamespace(fp32_contraction=False))
    assert _partition_compile_options(model)=={"triton.cudagraphs":False}
    model.temporal_basis_feature_encoder.fp32_contraction=True
    assert _partition_compile_options(model)=={"triton.cudagraphs":False,"emulate_precision_casts":True}


def test_direct_explicit_fp32_islands_preserve_compile_rounding_and_version():
    from types import SimpleNamespace
    from stockagent.config import load_config
    from stockagent.models.factorized_input import _partition_compile_options
    from stockagent.training.checkpoint_contract import (
        _configuration_fingerprint_snapshot, _training_checkpoint_contract,
    )
    model=SimpleNamespace(temporal_basis_feature_encoder=None,temporal_blocks_fp32=True,
                          portfolio_blocks_fp32=True)
    assert _partition_compile_options(model)=={'triton.cudagraphs':False,'emulate_precision_casts':True}
    config=load_config('configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v2.yaml')
    model_config=config.training.financial_transformer
    assert model_config.feature_svd_components==0 and not model_config.temporal_basis_families
    assert not model_config.feature_bottleneck_dim
    assert _configuration_fingerprint_snapshot(config)['training']['factorized_input_graph_contract']=='compiled_partition_direct_precision_casts_v2'
    assert _training_checkpoint_contract(config)['factorized_input_graph_contract']=='compiled_partition_direct_precision_casts_v2'


def test_basis_fp32_precision_is_explicit_and_changes_contract():
    from stockagent.models.transformer_base_portfolio import TemporalBasisFeatureEncoder
    from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config
    encoder=TemporalBasisFeatureEncoder(lookback=4,dim=4,source_dim=3,
        families=["haar","learned"],components=1,fuse_projection=True)
    source=torch.randn(2,4,3,3);pooled=torch.randn(2,3,4)
    ordinary=encoder._fused_projection(source,pooled)
    encoder.fp32_contraction=True
    with torch.autocast("cpu",dtype=torch.bfloat16):
        stable=encoder._fused_projection(source,pooled)
    assert stable.dtype==torch.float32
    torch.testing.assert_close(stable,ordinary,rtol=0,atol=0)
    stable.sum().backward()
    assert encoder.learned_basis.grad is not None
    assert _project_temporal_basis_model_config({"temporal_basis_fp32_contraction":False})=={}
    assert _project_temporal_basis_model_config({"temporal_basis_fp32_contraction":True})=={
        "temporal_basis_fp32_contraction":True,
        "factorized_compiled_precision_contract":"fp32_basis_operation_dtype_v2"}


def test_temporal_fp32_island_is_scoped_and_changes_contract():
    from stockagent.models.financial_transformer import FinancialTransformerModel
    from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config
    model = FinancialTransformerModel(lookback=4, num_features=3, num_symbols=2, d_model=8,
        attention_mode="market_token", temporal_layers=1, temporal_heads=2,
        cross_heads=2, latent_layers=1, num_latent_factors=2, num_market_tokens=1,
        market_layers=1, head_hidden_dim=8, head_layers=1, dropout=0.,
        input_dropout=0., candle_dropout=0., temporal_blocks_fp32=True)
    h = torch.randn(2, 4, 2, 8, requires_grad=True)
    expected = model._apply_temporal_blocks(h)
    with torch.autocast("cpu", dtype=torch.bfloat16):
        actual = model._apply_temporal_blocks(h)
        assert torch.is_autocast_enabled("cpu"), "the outer AMP policy must be restored"
    torch.testing.assert_close(actual, expected, rtol=0, atol=0)
    actual.square().sum().backward()
    assert h.grad is not None and torch.isfinite(h.grad).all()
    assert _project_temporal_basis_model_config({"temporal_blocks_fp32": False}) == {}
    assert _project_temporal_basis_model_config({"temporal_blocks_fp32": True}) == {
        "temporal_blocks_fp32": True, "temporal_block_precision_contract": "fp32_temporal_blocks_v1"}


@pytest.mark.parametrize("mode", ["market_token", "latent", "latent_only"])
def test_portfolio_fp32_island_matches_shared_head_and_changes_contract(mode):
    from stockagent.models.financial_transformer import FinancialTransformerModel
    from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config
    model = FinancialTransformerModel(lookback=4, num_features=3, num_symbols=2, d_model=8,
        attention_mode=mode, temporal_layers=1, temporal_heads=2,
        cross_heads=2, latent_layers=1, num_latent_factors=2, num_market_tokens=1,
        market_layers=1, head_hidden_dim=8, head_layers=1, dropout=0.,
        input_dropout=0., candle_dropout=0., portfolio_blocks_fp32=True)
    source = torch.randn(2, 2, 8, requires_grad=True)
    mask = torch.ones(2, 2, dtype=torch.bool)
    expected = model._forward_stock_embeddings(source, mask, return_aux=False)
    with torch.autocast("cpu", dtype=torch.bfloat16):
        actual = model._forward_stock_embeddings(source, mask, return_aux=False)
        assert torch.is_autocast_enabled("cpu")
    torch.testing.assert_close(actual, expected, rtol=0, atol=0)
    actual.square().sum().backward()
    assert source.grad is not None and torch.isfinite(source.grad).all()
    assert _project_temporal_basis_model_config({"portfolio_blocks_fp32": False}) == {}
    assert _project_temporal_basis_model_config({"portfolio_blocks_fp32": True}) == {
        "portfolio_blocks_fp32": True, "portfolio_block_precision_contract": "fp32_compact_attention_allocation_v1"}


@pytest.mark.parametrize("stocks",[1,3])
def test_lag_batched_basis_does_not_expand_raw_windows_and_matches_gradients(stocks):
    import copy
    from stockagent.models.transformer_base_portfolio import TemporalBasisFeatureEncoder
    torch.manual_seed(13)
    encoder=TemporalBasisFeatureEncoder(lookback=4,dim=4,source_dim=7,
        families=["haar","learned"],components=2,fuse_projection=True)
    encoder.fp32_contraction=True;oracle=copy.deepcopy(encoder)
    slab=torch.randn(9,stocks,7)
    source=slab.unfold(0,4,1).permute(0,3,1,2)
    by_lag=source.permute(1,0,2,3).reshape(4,6*stocks,7)
    assert by_lag.untyped_storage().data_ptr()==slab.untyped_storage().data_ptr()
    pooled=torch.randn(6,stocks,4)
    actual=encoder._fused_projection(source,pooled,lag_batched=True)
    expected=oracle._fused_projection(source,pooled)
    torch.testing.assert_close(actual,expected,rtol=2e-6,atol=2e-6)
    actual.square().sum().backward();expected.square().sum().backward()
    for parameter,target in zip(encoder.parameters(),oracle.parameters(),strict=True):
        torch.testing.assert_close(parameter.grad,target.grad,rtol=2e-5,atol=2e-5)


def example(tmp_path):
    dates = np.arange("2025-01-01", "2025-01-10", dtype="datetime64[D]")
    t, s = len(dates), 3
    base = np.arange(t*s*2, dtype=np.float32).reshape(t, s, 2)
    individual = np.arange(t*s*4, dtype=np.float32).reshape(t, s, 4)
    # A reported zero is available; a missing value is a numerical zero with
    # availability=0. The raw observations are not this training tensor.
    individual[:, :, 0] = 0.
    individual[:, :, 1] = 1.
    individual[3, 1, 1] = 0.
    common = np.arange(t*4, dtype=np.float32).reshape(t, 4)
    np.save(tmp_path/"common.npy", common, allow_pickle=False)
    blocks = []
    for start in range(0, t, 2):
        proof = write_array_block(tmp_path/f"{start}.npy.zst", individual[start:start+2])
        blocks.append({**proof, "start": start})
    names = ["asset", "asset__available", "asset__age_days", "asset__updated"]
    shared = ["M2", "M2__available", "M2__age_days", "M2__updated"]
    manifest = {"contract": CONTRACT, "status": "complete", "research_only": True,
        "historical_point_in_time": False, "feature_lag": 1,
        "base_feature_names": ["x", "y"], "symbols": ["1", "2", "3"],
        "dates": [str(d) for d in dates], "individual_channels": names, "common_channels": shared,
        "common": {"path": "common.npy", "sha256": file_sha256(tmp_path/"common.npy")},
        "blocks": blocks, "host_cache_bytes": individual[:2].nbytes}
    path = tmp_path/"factorized_manifest.json"
    path.write_text(json.dumps(manifest))
    mask = np.ones((t, s), dtype=bool)
    panel = PanelData(dates, manifest["symbols"], ["x", "y"], base,
        np.full((t,s), .01, dtype=np.float32), mask, mask,
        np.zeros(t, dtype=np.float32), np.ones((t,s), dtype=np.float32),
        can_buy_mask=mask.copy(), can_sell_mask=mask.copy())
    dense = np.concatenate([base, individual, np.broadcast_to(common[:,None,:], (t,s,4))], axis=-1)
    return panel, path, dense


def test_bounded_reads_common_broadcast_masks_and_fingerprint(tmp_path):
    panel, path, dense = example(tmp_path)
    attach_factorized_features(panel, path)
    np.testing.assert_array_equal(panel.features[1:5], dense[1:5])
    np.testing.assert_array_equal(panel.features[[5,2,5]], dense[[5,2,5]])
    assert panel.features[3, 1, 0] == 20.
    with pytest.raises(TypeError, match="whole factorized"):
        np.asarray(panel.features)
    assert _panel_array_content_fingerprint(panel,"features",panel.features)["fingerprint_kind"]
    assert _panel_training_transform_fingerprint(panel,"features",panel.features)["sha256"]
    assert panel.features._cache.bytes <= panel.features._cache.budget_bytes


def test_canonical_windowed_and_rms_parity(tmp_path):
    panel, path, dense = example(tmp_path)
    dense_panel = replace(panel, features=dense, feature_names=["f"+str(i) for i in range(10)])
    attach_factorized_features(panel, path)
    ordinary = CrossSectionalDataset(dense_panel, np.arange(2,8), 2, include_volume_notional=False)
    factored = CrossSectionalDataset(panel, np.arange(2,8), 2, include_volume_notional=False)
    split = dataset_to_windowed_tensors(factored)
    assert split.features.device == torch.device("cpu")
    assert torch.equal(split.features.narrow(0,2,3), torch.tensor(dense[2:5]))
    subset = split.subset_symbols(torch.tensor([2,0]))
    assert torch.equal(subset.features.narrow(0,2,3), torch.tensor(dense[2:5, [2,0]]))
    for i in range(len(factored)):
        assert torch.equal(ordinary[i]["x"], factored[i]["x"])
    kwargs = dict(epsilon=1e-6, minimum_dates=2)
    lhs = _fit_masked_training_feature_rms(dense, panel.alive_mask, np.arange(2,8), **kwargs)
    rhs = _fit_masked_training_feature_rms(panel.features, panel.alive_mask, np.arange(2,8), **kwargs)
    for a,b in zip(lhs,rhs): np.testing.assert_array_equal(a,b)
    projection = torch.nn.Linear(10, 4)
    a = projection(ordinary[2]["x"]).square().mean(); a.backward()
    grad = projection.weight.grad.clone(); projection.zero_grad(set_to_none=True)
    b = projection(factored[2]["x"]).square().mean(); b.backward()
    assert torch.equal(a,b) and torch.equal(grad,projection.weight.grad)


def test_mutated_block_and_calendar_rejected(tmp_path):
    panel,path,_ = example(tmp_path)
    manifest=json.loads(path.read_text()); manifest["dates"][0]="2024-12-31"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match="calendar"):
        attach_factorized_features(panel,path)


def test_raw_coordinate_and_dictionary_provenance_are_verified(tmp_path):
    panel,path,_=example(tmp_path)
    dictionary=tmp_path/"feature_dictionary.json";dictionary.write_text('{"features":[]}')
    raw=tmp_path/"raw.parquet";raw.write_bytes(b"test-source-proof")
    manifest=json.loads(path.read_text())
    manifest.update(feature_dictionary_sha256=file_sha256(dictionary),
        observations=[{"path":raw.name,"sha256":file_sha256(raw)}])
    path.write_text(json.dumps(manifest))
    raw.write_bytes(b"changed")
    with pytest.raises(ValueError,match="member identity"):
        attach_factorized_features(panel,path)


def test_training_only_temporal_covariance_factorized_parity(tmp_path):
    from stockagent.models.temporal_basis_fit import training_temporal_covariance
    panel, path, dense = example(tmp_path)
    attach_factorized_features(panel, path)
    indices = np.array([3, 5, 7, 7])
    expected, metadata = training_temporal_covariance(dense, indices, lookback=3, feature_lag=1)
    for features in (panel.features, panel.features.as_torch()):
        actual, scope = training_temporal_covariance(features, indices, lookback=3, feature_lag=1)
        torch.testing.assert_close(actual, expected, rtol=1e-12, atol=1e-12)
        assert scope == metadata


@pytest.mark.parametrize("elide", [False, True])
def test_training_covariance_date_halos_stock_tiles_and_owned_indices(tmp_path,elide):
    from stockagent.data.factorized_panel import FactorizedPanelFeatures
    from stockagent.models.temporal_basis_fit import training_temporal_covariance

    # Cross both the 32-row ownership and 128-stock tile boundaries, with
    # non-contiguous target dates. Halo rows must never count as extra samples.
    rng = np.random.default_rng(8)
    base = rng.normal(size=(80, 131, 2)).astype(np.float32)
    individual = rng.normal(size=(80, 131, 128)).astype(np.float32)
    individual[..., 4:] = 0.
    individual[33, 3, 99] = 1e8
    individual[70, 130, 127] = 1e-8
    common = rng.normal(size=(80, 4)).astype(np.float32)
    blocks = []
    for start in range(0, 80, 9):
        proof = write_array_block(tmp_path / f"halo_{start}.npy.zst", individual[start:start+9],omit_zero_columns=elide)
        blocks.append({**proof, "start": start})
    np.save(tmp_path / "common.npy", common, allow_pickle=False)
    manifest = {"common": {"path": "common.npy"}, "blocks": blocks,
        "individual_channels": [f"individual_{i}" for i in range(128)],
        "common_channels": [f"common_{i}" for i in range(4)]}
    adapter = FactorizedPanelFeatures(tmp_path, manifest, base,
        cache_bytes=individual[:18].nbytes)
    dense = np.concatenate([base, individual, np.broadcast_to(common[:, None, :], (80, 131, 4))], axis=-1)
    indices = np.array([9, 12, 30, 37, 40, 42, 65, 78])
    expected, metadata = training_temporal_covariance(dense, indices, lookback=7, feature_lag=1,
        feature_chunk_columns=17)
    actual, scope = training_temporal_covariance(adapter, indices, lookback=7, feature_lag=1,
        feature_chunk_columns=17)
    torch.testing.assert_close(actual, expected, rtol=1e-12, atol=1e-12)
    assert scope == metadata


def test_hash_guard_and_financial_transformer_gradient_parity(tmp_path):
    from stockagent.models.financial_transformer import FinancialTransformerModel
    import copy
    panel,path,dense=example(tmp_path)
    attach_factorized_features(panel,path)
    model=FinancialTransformerModel(lookback=2,num_features=10,num_symbols=3,d_model=8,
        attention_mode="market_token",temporal_layers=1,temporal_heads=2,
        cross_heads=2,latent_layers=1,num_latent_factors=2,num_market_tokens=1,
        market_layers=1,head_hidden_dim=8,head_layers=1,dropout=0.,input_dropout=0.,
        candle_dropout=0.,temporal_pooling="last",temporal_query_mode="last_only",
        feature_bottleneck_dim=0,return_aux=False).cpu().train()
    oracle=copy.deepcopy(model)
    a=torch.tensor(dense[2:6]).unfold(0,2,1).permute(0,3,1,2)
    b=panel.features.as_torch().narrow(0,2,4).unfold(0,2,1).permute(0,3,1,2)
    mask=torch.ones((3,3),dtype=torch.bool)
    weights_a=oracle(a,mask);weights_b=model(b,mask)
    assert torch.equal(weights_a,weights_b)
    weights_a.square().sum().backward();weights_b.square().sum().backward()
    for lhs,rhs in zip(oracle.parameters(),model.parameters()):
        if lhs.grad is not None: assert torch.equal(lhs.grad,rhs.grad)
    proof=json.loads(path.read_text())["blocks"][0]
    (tmp_path/proof["path"]).write_bytes(b"mutated")
    with pytest.raises(ValueError,match="identity"):
        attach_factorized_features(replace(panel,features=dense[:,:,:2],feature_names=["x","y"]),path)


def test_streamed_stock_encoder_raw_basis_shared_kernel_forward_gradient_parity(tmp_path):
    from stockagent.models.financial_transformer import FinancialTransformerModel
    import copy
    panel,path,dense=example(tmp_path)
    manifest=json.loads(path.read_text());manifest["stream_chunk_bytes"]=4*4*10
    path.write_text(json.dumps(manifest));attach_factorized_features(panel,path)
    torch.manual_seed(7)
    model=FinancialTransformerModel(lookback=2,num_features=10,num_symbols=3,d_model=8,
        attention_mode="market_token",temporal_layers=1,temporal_heads=2,
        cross_heads=2,latent_layers=1,num_latent_factors=2,num_market_tokens=1,
        market_layers=1,head_hidden_dim=8,head_layers=1,dropout=0.,input_dropout=0.,
        candle_dropout=0.,temporal_pooling="last",temporal_query_mode="last_only",
        norm_type="layernorm",
        causal_feature_rms_normalization=True,temporal_basis_families=["haar","learned"],
        temporal_basis_components=1,temporal_basis_input="raw_features",return_aux=False).cpu().train()
    rms=np.sqrt(np.square(dense[:6].astype(np.float64)).mean(axis=(0,1))).astype(np.float32)
    model.set_causal_feature_rms_normalizer(torch.tensor(np.where(rms>1e-6,rms,1.)),torch.tensor(rms>1e-6))
    oracle=copy.deepcopy(model)
    mask=torch.tensor([[True,True,False],[True,True,True],[False,False,False]])
    a=oracle.forward_from_panel_slab(torch.tensor(dense[2:6]),mask,return_aux=False)
    b=model.forward_from_panel_slab(panel.features.as_torch().slab(2,4),mask,return_aux=False)
    torch.testing.assert_close(a,b,rtol=1e-5,atol=1e-6)
    a.square().sum().backward();b.square().sum().backward()
    for name,lhs in oracle.named_parameters():
        rhs=dict(model.named_parameters())[name]
        assert (lhs.grad is None)==(rhs.grad is None),name
        if lhs.grad is not None:torch.testing.assert_close(lhs.grad,rhs.grad,rtol=2e-4,atol=2e-6,msg=lambda detail:name+": "+detail)
    model.eval()
    with torch.no_grad():
        torch.testing.assert_close(model.forward_from_panel_slab(panel.features.as_torch().slab(2,4).pad_end(2),torch.ones(5,3,dtype=torch.bool),return_aux=False)[:3],
                                  model.forward_from_panel_slab(panel.features.as_torch().slab(2,4),torch.ones(3,3,dtype=torch.bool),return_aux=False),rtol=1e-5,atol=1e-6)


def test_two_axis_storage_reads_and_canonical_slab_metadata(tmp_path):
    panel,path,dense=example(tmp_path);manifest=json.loads(path.read_text())
    replacements=[]
    for block in manifest["blocks"]:
        start=block["start"];parts=[]
        for s in range(3):
            part=write_array_block(tmp_path/f"{start}_{s}.npy.zst",dense[start:start+block["shape"][0],s:s+1,2:6])
            part["symbol_start"]=s;parts.append(part)
        replacements.append({"start":start,"shape":block["shape"],"symbol_blocks":parts})
    manifest["blocks"]=replacements;path.write_text(json.dumps(manifest));attach_factorized_features(panel,path)
    np.testing.assert_array_equal(panel.features[1:5],dense[1:5])
    np.testing.assert_array_equal(panel.features.subset_symbols(np.array([2,0]))[[3,1]],dense[[3,1]][:,[2,0]])
    kwargs=dict(epsilon=1e-6,minimum_dates=2)
    lhs=_fit_masked_training_feature_rms(dense,panel.alive_mask,np.arange(2,8),**kwargs)
    rhs=_fit_masked_training_feature_rms(panel.features,panel.alive_mask,np.arange(2,8),**kwargs)
    for a,b in zip(lhs,rhs):np.testing.assert_array_equal(a,b)

    columns, compact = panel.features.compact_statistics_rows(np.array([7,2,2]))
    np.testing.assert_array_equal(compact.view(np.uint32), dense[[7,2,2]][:,:,columns].view(np.uint32))
    from stockagent.models.temporal_basis_fit import training_temporal_covariance
    indices=np.arange(2,8)
    expected, metadata = training_temporal_covariance(dense, indices, lookback=2, feature_lag=1)
    actual, scope = training_temporal_covariance(panel.features, indices, lookback=2, feature_lag=1)
    torch.testing.assert_close(actual, expected, rtol=1e-12, atol=1e-12)
    assert scope == metadata
    dataset=CrossSectionalDataset(panel,np.arange(2,8),2,include_volume_notional=False)
    windowed=dataset_to_windowed_tensors(dataset)
    batch=windowed.panel_slab_batch_by_rows(0,2,device=torch.device("cpu"),non_blocking=False)
    assert batch["feature_slab"]._stockagent_factorized_slab
    torch.testing.assert_close(batch["feature_slab"].stock_chunk(0,3),torch.tensor(dense[1:4]))


def test_lossless_zero_column_codec_preserves_negative_zero_and_dense_statistics(tmp_path):
    panel,path,dense=example(tmp_path)
    manifest=json.loads(path.read_text())
    dense[2,0,2]=-0.0
    replacements=[]
    for block in manifest["blocks"]:
        start=block["start"]
        proof=write_array_block(tmp_path/f"elided_{start}.npy.zst",dense[start:start+block["shape"][0],:,2:6],omit_zero_columns=True)
        replacements.append({**proof,"start":start})
    assert any(p["stored_shape"][-1] < p["shape"][-1] for p in replacements)
    manifest["blocks"]=replacements;path.write_text(json.dumps(manifest))
    attach_factorized_features(panel,path)
    decoded=panel.features[:]
    np.testing.assert_array_equal(decoded.view(np.uint32),dense.view(np.uint32))
    indices=np.arange(2,8);kwargs=dict(epsilon=1e-6,minimum_dates=2)
    lhs=_fit_masked_training_feature_rms(dense,panel.alive_mask,indices,**kwargs)
    rhs=_fit_masked_training_feature_rms(panel.features,panel.alive_mask,indices,**kwargs)
    for a,b in zip(lhs,rhs):np.testing.assert_array_equal(a,b)
    from stockagent.models.temporal_basis_fit import training_temporal_covariance
    columns, compact=panel.features.compact_statistics_rows(np.array([7,2,2]))
    np.testing.assert_array_equal(compact.view(np.uint32), dense[[7,2,2]][:,:,columns].view(np.uint32))
    expected, metadata=training_temporal_covariance(dense,indices,lookback=2,feature_lag=1)
    actual, scope=training_temporal_covariance(panel.features,indices,lookback=2,feature_lag=1)
    torch.testing.assert_close(actual,expected,rtol=1e-12,atol=1e-12)
    assert metadata==scope


@pytest.mark.parametrize("mode",["compact_cuda","compact_cuda_packed","compact_cuda_cached"])
def test_lossless_compact_transport_preserves_schema_identity_and_signed_zero(tmp_path,mode):
    panel,path,dense=example(tmp_path)
    manifest=json.loads(path.read_text());dense[2,0,2]=-0.0
    blocks=[]
    for block in manifest["blocks"]:
        start=block["start"];parts=[]
        for stock in range(3):
            proof=write_array_block(tmp_path/f"transport_{start}_{stock}.zst",
                dense[start:start+block["shape"][0],stock:stock+1,2:6],omit_zero_columns=True)
            parts.append({**proof,"symbol_start":stock})
        blocks.append({"start":start,"shape":block["shape"],"symbol_blocks":parts})
    manifest["blocks"]=blocks;path.write_text(json.dumps(manifest))
    ordinary=attach_factorized_features(replace(panel),path)
    compact=attach_factorized_features(replace(panel),path,transfer_mode=mode)
    assert ordinary.features.content_fingerprint==compact.features.content_fingerprint
    for symbols in (np.array([0,1,2]),np.array([2,0,2])):
        subset=compact.features.subset_symbols(symbols)
        rows=np.array([7,2,2,0])
        columns,values=subset.compact_statistics_rows(rows)
        restored=np.zeros((len(rows),len(symbols),dense.shape[-1]),dtype=np.float32)
        restored[...,columns]=values
        np.testing.assert_array_equal(restored.view(np.uint32),dense[rows][:,symbols].view(np.uint32))
        rectangles=np.zeros_like(restored)
        rectangles[...,:2]=dense[rows][:,symbols,:2]
        rectangles[...,6:]=dense[rows][:,symbols,6:]
        for selected,positions,columns,values in subset.compact_transfer_rectangles(rows):
            rectangles[np.ix_(selected,positions,columns)]=values
        np.testing.assert_array_equal(rectangles.view(np.uint32),dense[rows][:,symbols].view(np.uint32))
    # CPU semantic oracles keep their ordinary complete Tensor, not CUDA.
    raw=compact.features.as_torch().slab(1,4).stock_chunk(0,3)
    np.testing.assert_array_equal(raw.numpy().view(np.uint32),dense[1:5].view(np.uint32))
    compact.features.max_slab_bytes=1
    with pytest.raises(MemoryError,match="bounded dense slab"):
        compact.features.as_torch().slab(1,4).stock_chunk(0,3)
    with pytest.raises(ValueError,match="transfer mode"):
        attach_factorized_features(replace(panel),path,transfer_mode="unknown")


def test_compact_transport_is_explicit_and_not_a_semantic_input_change(tmp_path):
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot
    dense=load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_bf16_v4.yaml")
    compact=replace(dense,data=replace(dense.data,factorized_transfer_mode="compact_cuda"))
    assert _configuration_fingerprint_snapshot(dense)==_configuration_fingerprint_snapshot(compact)
    packet=replace(dense,data=replace(dense.data,factorized_transfer_mode="compact_cuda_packed"))
    assert _configuration_fingerprint_snapshot(dense)==_configuration_fingerprint_snapshot(packet)
    cached=replace(dense,data=replace(dense.data,factorized_transfer_mode="compact_cuda_cached"))
    assert _configuration_fingerprint_snapshot(dense)==_configuration_fingerprint_snapshot(cached)
    selected=load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_bf16_v5.yaml")
    assert selected.data.factorized_transfer_mode=="compact_cuda"
    assert selected.training.day_trade_sparse_events is False
    selected=load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_bf16_v8.yaml")
    assert selected.data.factorized_transfer_mode=="compact_cuda_packed"
    assert selected.training.day_trade_sparse_events is False
    path=tmp_path/"bad.yaml";path.write_text("data:\n  factorized_transfer_mode: invented\n")
    with pytest.raises(ValueError,match="factorized_transfer_mode"):
        load_config(path)


def test_compact_gpu_packet_cache_is_bounded_and_owned_by_one_slab():
    from types import SimpleNamespace
    from stockagent.data.factorized_panel import FactorizedPanelSlab
    source = SimpleNamespace(max_slab_bytes=1024, transfer_stats={
        "payload_cache_admissions": 0, "payload_cache_peak_bytes": 0})
    slab = FactorizedPanelSlab(source, [1, 2, 3])
    slab._gpu_packet_budget = 16
    packet = torch.arange(10, dtype=torch.uint8)
    assert slab._remember_gpu_packet((0, 2), packet, [], [])
    assert slab._gpu_packets[(0, 2)][0] is packet
    assert not slab._remember_gpu_packet((0, 2), packet, [], [])
    assert not slab._remember_gpu_packet((2, 4), packet, [], [])
    assert slab._gpu_packet_bytes == 10
    assert source.transfer_stats == {"payload_cache_admissions": 1, "payload_cache_peak_bytes": 10}
    assert not slab.to(device="cpu")._gpu_packets
    assert not slab.pad_end(1)._gpu_packets


def test_payload_cache_and_single_fold_lifecycle_preserve_resume_semantics():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot

    original = load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v3.yaml")
    candidate = load_config("configs/deployments/tw_day_trade_factorized_values_20261005_no_basis_payload_cache_v1.yaml")
    assert candidate.data.factorized_transfer_mode == "compact_cuda_cached"
    assert candidate.runner.resume and not candidate.runner.isolate_train_folds
    assert candidate.runner.output_dir == original.runner.output_dir
    left = _configuration_fingerprint_snapshot(candidate)
    right = _configuration_fingerprint_snapshot(original)
    # Runner resume/isolation are recorded audit settings, not optimizer
    # semantics. No other submitted setting may change in this candidate.
    left.pop("runner")
    right.pop("runner")
    assert left == right
    from stockagent.training.checkpoint_contract import _training_checkpoint_contract
    assert _training_checkpoint_contract(candidate) == _training_checkpoint_contract(original)


def test_lossless_transfer_packet_alignment_strides_and_immutable_sources():
    from stockagent.data.factorized_panel import _pack_transfer_payloads,_transfer_packet_views
    individual=np.arange(3*4*5,dtype=np.float32).reshape(3,4,5)
    individual[2,2,1]=-0.0
    individual.setflags(write=False)
    values=[np.array([-0.0],dtype=np.float32),np.array([7,0,7],dtype=np.int64),
        individual[::2,::2,::-1],np.empty((0,3),dtype=np.float32),
        np.array([-0.0,1.0],dtype=np.float32)]
    originals=[value.copy() for value in values]
    packet,layout=_pack_transfer_payloads(values,pin_memory=False,budget_bytes=1024)
    assert packet.dtype==torch.uint8
    assert all(offset%8==0 for offset,*_ in layout)
    views=_transfer_packet_views(packet,layout)
    for original,view in zip(originals,views):
        expected=original.view(np.uint32 if original.dtype==np.float32 else np.uint64)
        actual=view.numpy().view(expected.dtype)
        np.testing.assert_array_equal(actual,expected)
        assert view.is_contiguous()
    views[2].fill_(123)
    np.testing.assert_array_equal(values[2].view(np.uint32),originals[2].view(np.uint32))
    with pytest.raises(MemoryError,match="bounded staging"):
        _pack_transfer_payloads(values,pin_memory=False,budget_bytes=packet.numel()-1)
    with pytest.raises(ValueError,match="Float32 values or Int64"):
        _pack_transfer_payloads([np.ones(2,dtype=np.float64)],pin_memory=False,budget_bytes=1024)
    empty,empty_layout=_pack_transfer_payloads([],pin_memory=False,budget_bytes=0)
    assert empty.numel()==0 and _transfer_packet_views(empty,empty_layout)==[]


def test_direct_input_experiment_removes_every_basis_branch_without_changing_data_or_execution():
    from stockagent.config import load_config
    from stockagent.models.factory import build_model
    from stockagent.training.trainer import _temporal_basis_runtime_config
    base=load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_bf16_v7.yaml")
    direct=load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v1.yaml")
    assert direct.data==base.data and direct.trading==base.trading
    assert direct.walk_forward==base.walk_forward and direct.environment==base.environment
    assert direct.training.batch_size_train==32
    assert not direct.training.financial_transformer.temporal_basis_families
    assert not direct.training.financial_transformer.temporal_basis_fp32_contraction
    assert direct.training.financial_transformer.feature_bottleneck_dim==0
    assert _temporal_basis_runtime_config(direct) is None
    assert direct.runner.output_dir!=base.runner.output_dir and not direct.runner.resume
    assert direct.training.record_epoch_curve and direct.training.curve_plot_interval==1
    assert not direct.training.curve_plot_async and not direct.training.defer_epoch_curve_plot_until_end
    model=build_model(config=direct,lookback=4,num_features=7,num_symbols=3,
        feature_names=[f"f_{i}" for i in range(7)])
    assert model.temporal_basis_feature_encoder is None
    assert model.temporal_basis_input_feature_builder is None
    assert model.candle_encoder.continuous_feature_bottleneck is None
    assert model.d_model==32


@pytest.mark.parametrize("lag_batch_size", [2, 8, 32])
def test_factorized_batched_temporal_covariance_preserves_logical_zero_columns(tmp_path, lag_batch_size):
    panel,path,dense=example(tmp_path)
    attach_factorized_features(panel,path)
    from stockagent.models.temporal_basis_fit import training_temporal_covariance
    targets=np.array([3,5,7,7])
    kwargs=dict(lookback=3,feature_lag=1,feature_chunk_columns=3)
    expected,scope=training_temporal_covariance(dense,targets,**kwargs)
    actual,metadata=training_temporal_covariance(panel.features,targets,**kwargs,lag_batch_size=lag_batch_size)
    torch.testing.assert_close(actual,expected,rtol=1e-12,atol=1e-12)
    assert metadata==scope
