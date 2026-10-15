"""Full-affine zero-column contraction, not input selection or sparse trading."""
from copy import deepcopy
from dataclasses import replace

import numpy as np
import pytest
import torch

from stockagent.data.factorized_panel import FactorizedPanelFeatures, FactorizedPanelSlab, write_array_block
from stockagent.models.financial_transformer import CandleEncoder, FinancialTransformerModel


@pytest.mark.parametrize("norm", ["layernorm", "rmsnorm"])
@pytest.mark.parametrize("columns", [[], [0, 3, 8, 14], list(range(19))])
def test_full_affine_forward_and_every_parameter_gradient(norm, columns):
    torch.manual_seed(53)
    model = CandleEncoder(num_features=19, d_model=8, dropout=0., norm_type=norm,
        ffn_type="swiglu", sanitize_inputs=False, causal_feature_rms_normalization=True,
        projection_fp32=True)
    model.set_causal_feature_rms_normalizer(torch.rand(19)+.4, torch.arange(19) != 8)
    with torch.no_grad():
        model.input_norm.weight.copy_(torch.randn(19))
        if getattr(model.input_norm, "bias", None) is not None:
            model.input_norm.bias.copy_(torch.randn(19))
    oracle = deepcopy(model)
    ids = torch.tensor(columns, dtype=torch.long)
    compact = torch.randn(5, 3, len(columns), requires_grad=True)
    dense = torch.zeros(5, 3, 19).index_copy(-1, ids, compact.detach()).requires_grad_()
    coefficients = torch.randn(5, 3, 8)
    expected = oracle(dense)[0]
    actual = model.forward_compact_columns(compact, ids)
    torch.testing.assert_close(actual, expected, rtol=3e-6, atol=3e-6)
    (actual*coefficients).sum().backward()
    (expected*coefficients).sum().backward()
    torch.testing.assert_close(compact.grad, dense.grad.index_select(-1, ids), rtol=2e-5, atol=2e-5)
    for (name, parameter), (target_name, target) in zip(model.named_parameters(), oracle.named_parameters(), strict=True):
        assert name == target_name
        assert (parameter.grad is None) == (target.grad is None), name
        if target.grad is not None:
            torch.testing.assert_close(parameter.grad, target.grad, rtol=3e-5, atol=3e-5,
                msg=lambda message: name + ": " + message)


def test_near_constant_variance_does_not_cancel_and_compiles():
    torch.manual_seed(3)
    model = CandleEncoder(num_features=64, d_model=8, dropout=0., norm_type="layernorm",
        ffn_type="gelu", sanitize_inputs=False, projection_fp32=True)
    ids = torch.arange(64)
    raw = 1000. + torch.randn(3, 2, 64) * .01
    actual = model.forward_compact_columns(raw, ids)
    expected = model(raw)[0]
    assert torch.isfinite(actual).all()
    torch.testing.assert_close(actual, expected, rtol=0, atol=0)
    compiled = torch.compile(model.forward_compact_columns, backend="eager", fullgraph=True)
    torch.testing.assert_close(compiled(raw, ids), actual, rtol=0, atol=0)


def _source(tmp_path):
    rng = np.random.default_rng(17)
    base = rng.normal(size=(9, 4, 2)).astype(np.float32)
    individual = np.zeros((9, 4, 23), dtype=np.float32)
    individual[:4, :, [2, 5]] = rng.normal(size=(4, 4, 2))
    individual[4:, :, [8, 10]] = rng.normal(size=(5, 4, 2))
    individual[3, 2, 20] = -0.
    individual[6, 1, 21] = 1e-18
    common = rng.normal(size=(9, 3)).astype(np.float32)
    np.save(tmp_path/"common.npy", common, allow_pickle=False)
    blocks = []
    for start, stop in [(0, 4), (4, 9)]:
        parts = []
        for a, b in [(0, 2), (2, 4)]:
            proof = write_array_block(tmp_path/f"{start}_{a}.npy.zst", individual[start:stop, a:b], omit_zero_columns=True)
            parts.append({**proof, "symbol_start": a})
        blocks.append({"start": start, "symbol_blocks": parts})
    source = FactorizedPanelFeatures(tmp_path, {
        "common": {"path": "common.npy"}, "blocks": blocks,
        "individual_channels": [str(i) for i in range(23)],
        "common_channels": ["c1", "c2", "c3"], "stream_chunk_bytes": 3*4*28*4,
    }, base, cache_bytes=individual.nbytes)
    return source


def test_lossless_union_handles_date_symbol_boundary_padding_and_negative_zero(tmp_path):
    source = _source(tmp_path)
    slab = FactorizedPanelSlab(source, np.array([3, 4, 6, 1, 3])).pad_end(2)
    for start, end in [(0, 4), (1, 3), (3, 4)]:
        values, columns = slab.compact_stock_chunk(start, end)
        valid = columns < source.shape[-1]
        restored = torch.zeros(len(slab.rows), end-start, source.shape[-1]).index_copy(-1, columns[valid], values[..., valid])
        expected = torch.from_numpy(source[slab.rows, start:end])
        assert torch.equal(restored.view(torch.int32), expected.view(torch.int32))
        assert values.size(-1) < source.shape[-1]
        assert values.size(-1) & (values.size(-1)-1) == 0
        assert 24 not in columns.tolist()  # A truly omitted positive-zero column.


@pytest.mark.parametrize("full_width",[False,True])
def test_rectangle_packet_reconstruction_is_bit_exact_without_host_union(tmp_path,full_width):
    from stockagent.data.factorized_panel import _reconstruct_transfer_packet,_transfer_packet_views
    source=_source(tmp_path)
    if full_width:
        source=FactorizedPanelFeatures(tmp_path,source.manifest,
            np.repeat(source.base,16,axis=-1),cache=source._cache)
    slab=FactorizedPanelSlab(source,np.array([3,4,6,1,3])).pad_end(2)
    for start,end in [(0,4),(1,3),(3,4)]:
        packet,layout,(queued,base_width,common_start,bucket)=slab._compact_column_packet(start,end)
        columns=_transfer_packet_views(packet,layout)[0]
        values=torch.zeros(len(slab.rows),end-start,bucket)
        _reconstruct_transfer_packet(values,packet,layout,queued,base_width,common_start)
        valid=columns<source.shape[-1]
        restored=torch.zeros(len(slab.rows),end-start,source.shape[-1]).index_copy(
            -1,columns[valid],values[...,valid])
        expected=torch.from_numpy(source[slab.rows,start:end])
        assert torch.equal(restored.view(torch.int32),expected.view(torch.int32))
        assert (bucket==source.shape[-1])==full_width


def test_bucket_sentinels_do_not_become_extra_features_or_parameter_gradients():
    torch.manual_seed(5)
    model = CandleEncoder(num_features=19, d_model=8, dropout=0., norm_type="layernorm",
        ffn_type="swiglu", sanitize_inputs=False, causal_feature_rms_normalization=True,
        projection_fp32=True)
    oracle = deepcopy(model)
    ids = torch.tensor([0, 4, 9, 12, 19, 19, 19, 19])
    values = torch.randn(3, 2, 8)
    values[..., 4:] = 0.
    dense = torch.zeros(3, 2, 19).index_copy(-1, ids[:4], values[..., :4])
    actual = model.forward_compact_columns(values, ids)
    expected = oracle(dense)[0]
    torch.testing.assert_close(actual, expected, rtol=3e-6, atol=3e-6)
    coefficients = torch.randn_like(actual)
    (actual*coefficients).sum().backward(); (expected*coefficients).sum().backward()
    for (name, parameter), (_, target) in zip(model.named_parameters(), oracle.named_parameters(), strict=True):
        torch.testing.assert_close(parameter.grad, target.grad, rtol=3e-5, atol=3e-5,
            msg=lambda message: name + ": " + message)


@pytest.mark.parametrize("checkpoint", [False, True])
@pytest.mark.parametrize("stream_bytes", [0, 448])
def test_full_shared_model_streaming_outputs_and_gradients(tmp_path, checkpoint, stream_bytes):
    source = _source(tmp_path)
    torch.manual_seed(15)
    model = FinancialTransformerModel(lookback=2, num_features=28, num_symbols=4, d_model=8,
        attention_mode="market_token", temporal_layers=1, temporal_heads=2,
        cross_heads=2, num_market_tokens=2, market_layers=1, head_hidden_dim=8,
        head_layers=1, dropout=0., input_dropout=0., candle_dropout=0.,
        temporal_pooling="last", temporal_query_mode="last_only", norm_type="layernorm",
        causal_feature_rms_normalization=True, candle_projection_fp32=True,
        temporal_blocks_fp32=True, portfolio_blocks_fp32=True,
        factorized_compact_projection=True, factorized_compact_stream_chunk_bytes=stream_bytes,
        factorized_input_compile=False,
        portfolio_output_mode="score_entmax_scale_separated_cash", return_aux=False)
    model.factorized_encoder_checkpoint = checkpoint
    model.set_causal_feature_rms_normalizer(torch.ones(28), torch.ones(28, dtype=torch.bool))
    oracle = deepcopy(model)
    oracle.factorized_compact_projection = False
    mask = torch.tensor([[True, True, False, True], [False, True, True, True], [True]*4])
    actual = model.forward_from_panel_slab(FactorizedPanelSlab(source, np.arange(2, 6)), mask)
    expected = oracle.forward_from_panel_slab(torch.tensor(source[2:6]), mask)
    torch.testing.assert_close(actual, expected, rtol=3e-5, atol=3e-6)
    coefficients = torch.randn_like(actual)
    (actual*coefficients).sum().backward(); (expected*coefficients).sum().backward()
    for (name, parameter), (target_name, target) in zip(model.named_parameters(), oracle.named_parameters(), strict=True):
        assert name == target_name
        assert (parameter.grad is None) == (target.grad is None), name
        if target.grad is not None:
            torch.testing.assert_close(parameter.grad, target.grad, rtol=2e-4, atol=2e-5,
                msg=lambda message: name + ": " + message)


def test_contraction_is_explicitly_versioned_without_changing_old_resume():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _training_checkpoint_contract, _project_temporal_basis_model_config
    assert _project_temporal_basis_model_config({"factorized_compact_projection": False}) == {}
    projected = _project_temporal_basis_model_config({"factorized_compact_projection": True})
    assert projected["factorized_compact_projection_contract"] == "full_layernorm_affine_zero_column_contraction_v2"
    base = load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v3.yaml")
    model = replace(base.training.financial_transformer, factorized_compact_projection=True)
    candidate = replace(base, training=replace(base.training, financial_transformer=model))
    assert _training_checkpoint_contract(base) != _training_checkpoint_contract(candidate)
    assert base.trading == candidate.trading and base.walk_forward == candidate.walk_forward


def test_stream_budget_preserves_old_fingerprint_and_versions_opt_in():
    from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config
    assert _project_temporal_basis_model_config({"factorized_compact_stream_chunk_bytes": 0}) == {}
    a = _project_temporal_basis_model_config({"factorized_compact_stream_chunk_bytes": 2 * 1024**3})
    assert a["factorized_compact_stream_contract"] == "bounded_logical_width_stock_stream_v1"


@pytest.mark.parametrize("rows,expected_old", [(95,128), (159,114), (287,63)])
def test_large_batch_bounded_stream_removes_fixed_cap(rows, expected_old):
    from types import SimpleNamespace
    from stockagent.models.factorized_input import _factorized_stock_chunk_size
    source = SimpleNamespace(manifest={"stream_chunk_bytes": 512*1024**2}, max_slab_bytes=12*1024**3)
    shape = (rows, 2757, 14726)
    slab = SimpleNamespace(source=source, size=lambda index: shape[index])
    model = SimpleNamespace(factorized_compact_projection=True, factorized_compact_stream_chunk_bytes=0)
    assert _factorized_stock_chunk_size(model, slab, None) == expected_old
    model.factorized_compact_stream_chunk_bytes = 2 * 1024**3
    actual = _factorized_stock_chunk_size(model, slab, None)
    assert actual * rows * 14726 * 4 <= 2*1024**3
    assert actual > expected_old


def test_unsupported_stem_is_rejected_not_silently_replaced():
    with pytest.raises(ValueError, match="continuous FP32"):
        FinancialTransformerModel(lookback=2, num_features=8, num_symbols=2, d_model=8,
            factorized_compact_projection=True, candle_projection_fp32=False)


def test_static_bucket_compile_budget_is_finite_and_scoped():
    from types import SimpleNamespace
    import torch._dynamo.config as config
    from stockagent.models.factorized_input import _compact_encoder_static_abi_budget, _call_partition
    model = SimpleNamespace(factorized_input_compile=True,
        candle_encoder=SimpleNamespace(candle_query=SimpleNamespace(device=SimpleNamespace(type="cuda"))))
    previous = config.recompile_limit
    def partition():
        return config.recompile_limit
    partition._stockagent_static_abi_budget = _compact_encoder_static_abi_budget(14726)
    assert partition._stockagent_static_abi_budget == 60
    assert _call_partition(model, partition) == max(previous, 60)
    assert config.recompile_limit == previous
    assert _call_partition(model, lambda: config.recompile_limit) == previous
    with pytest.raises(ValueError, match="positive"):
        _compact_encoder_static_abi_budget(0)
