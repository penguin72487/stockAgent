from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
import torch

from stockagent.config import (
    DataConfig, EnvironmentConfig, EvaluationConfig, ExperimentConfig,
    RunnerConfig, TradingConfig, TrainingConfig, WalkForwardConfig,
)
from stockagent.models import factorized_input as owner
from stockagent.models.financial_transformer import FinancialTransformerModel
from stockagent.models import transformer_base_portfolio as attention_owner
from stockagent.training.checkpoint_contract import (
    _configuration_fingerprint_snapshot,
    _stable_fingerprint,
    _training_checkpoint_contract,
    validate_checkpoint_manifest,
)


@pytest.fixture
def factorized_config():
    # The contract test needs no private deployment file or downloaded data.
    cfg = ExperimentConfig(
        experiment_name="numerical-contract-fixture", runner=RunnerConfig(),
        environment=EnvironmentConfig(device="cpu", use_tensor_cores=True, amp_dtype="bf16"),
        data=DataConfig(parquet_root="fixture-data", benchmark_name="fixture-benchmark"),
        walk_forward=WalkForwardConfig(),
        trading=TradingConfig(frequency="daily", buy_fee_rate=0., sell_fee_rate=0., long_only=False),
        training=TrainingConfig(non_blocking_transfer=False), evaluation=EvaluationConfig(),
    )
    model = replace(cfg.training.financial_transformer, d_model=32,
                    portfolio_blocks_fp32=True, factorized_compact_projection=True,
                    factorized_compact_stream_chunk_bytes=2 * 1024**3)
    return replace(cfg, data=replace(cfg.data, factorized_feature_manifest="fixture-manifest.json"),
                   training=replace(cfg.training, model_name="financial_transformer",
                                    enable_torch_compile=True, financial_transformer=model))


@pytest.mark.parametrize("width", [8, 16, 32, 64, 128])
@pytest.mark.parametrize("fp32", [False, True])
def test_compiled_adjoint_contract_is_scoped(width, fp32):
    model = SimpleNamespace(d_model=width, portfolio_blocks_fp32=fp32)
    expected = "sm12_narrow_fp32_head_aligned_inductor_v1" if width < 32 and fp32 else "inductor_v1"
    assert owner.factorized_head_backend_contract(model) == expected


def test_narrow_head_adjoint_versions_both_resume_and_artifact_contracts(factorized_config):
    cfg = factorized_config
    model = replace(cfg.training.financial_transformer, d_model=16)
    narrow = replace(cfg, training=replace(cfg.training, financial_transformer=model))
    expected = "compiled_partition_sm12_narrow_fp32_head_only_aligned_sdpa_adjoint_abi_v9"
    assert _training_checkpoint_contract(narrow)["factorized_input_graph_contract"] == expected
    assert _configuration_fingerprint_snapshot(narrow)["training"]["factorized_input_graph_contract"] == expected
    assert _training_checkpoint_contract(cfg)["factorized_input_graph_contract"] != expected


@pytest.mark.parametrize("scope", ["resume", "artifact"])
def test_prior_narrow_head_optimizer_is_rejected(scope, tmp_path, factorized_config):
    cfg = factorized_config
    narrow = replace(cfg, training=replace(cfg.training, financial_transformer=replace(
        cfg.training.financial_transformer, d_model=16)))
    contract = _training_checkpoint_contract(narrow)
    fingerprints = {name: "unchanged-" + name for name in
                    ("data", "data_schema", "model", "training", "evaluation", "trading", "walk_forward")}
    fingerprints["training"] = _stable_fingerprint(contract)
    expected = {"schema_version": 4, "contracts": {"training": contract}, "fingerprints": fingerprints}
    old = deepcopy(expected)
    old["contracts"]["training"]["factorized_input_graph_contract"] = "compiled_partition_full_affine_bounded_stream_abi_v6"
    old["fingerprints"]["training"] = _stable_fingerprint(old["contracts"]["training"])
    with pytest.raises(RuntimeError, match="semantic fingerprint mismatch.*training"):
        validate_checkpoint_manifest({"experiment_manifest": old}, expected,
                                     checkpoint_path=tmp_path / "old.pt", scope=scope)


@pytest.mark.parametrize("scope", ["resume", "artifact"])
def test_rejected_aot_candidate_optimizer_is_not_compatible(scope, tmp_path, factorized_config):
    cfg = factorized_config
    narrow = replace(cfg, training=replace(cfg.training, financial_transformer=replace(
        cfg.training.financial_transformer, d_model=16)))
    contract = _training_checkpoint_contract(narrow)
    fingerprints = {name: "unchanged-" + name for name in
                    ("data", "data_schema", "model", "training", "evaluation", "trading", "walk_forward")}
    fingerprints["training"] = _stable_fingerprint(contract)
    current = {"schema_version": 4, "contracts": {"training": contract}, "fingerprints": fingerprints}
    rejected = deepcopy(current)
    rejected["contracts"]["training"]["factorized_input_graph_contract"] = "compiled_partition_sm12_narrow_fp32_head_safe_adjoint_abi_v7"
    rejected["fingerprints"]["training"] = _stable_fingerprint(rejected["contracts"]["training"])
    with pytest.raises(RuntimeError, match="semantic fingerprint mismatch.*training"):
        validate_checkpoint_manifest({"experiment_manifest": rejected}, current,
                                     checkpoint_path=tmp_path/"rejected.pt", scope=scope)


def test_inductor_keeps_head_outputs_and_all_adjoints():
    # Small CPU algebra check, not a replacement for the real dual-CUDA replay.
    torch.manual_seed(351)
    eager = FinancialTransformerModel(
        lookback=2, num_features=3, num_symbols=7, d_model=16,
        temporal_heads=4, cross_heads=4, joint_heads=4, dropout=0.,
        use_latent_factors=True, use_market_tokens=True,
        num_latent_factors=3, num_market_tokens=2,
        portfolio_blocks_fp32=True,
    ).train()
    candidate = deepcopy(eager)
    for module in candidate.modules():
        if isinstance(module, attention_owner.FlashSDPAAttention):
            module.compiled_cross_attention_blackwell_workaround = True
    x = torch.randn(3, 7, 16, requires_grad=True)
    expected_x = x.detach().clone().requires_grad_(True)
    mask = torch.rand(3, 7) > .2
    mask[:, 0] = True
    kwargs = dict(temperature=None, return_aux=False, portfolio_context=None)
    expected = eager._forward_stock_embeddings(expected_x, mask, **kwargs)
    fn = torch.compile(candidate._forward_stock_embeddings, backend="inductor", dynamic=False, fullgraph=True)
    actual = fn(x, mask, **kwargs)
    torch.testing.assert_close(actual, expected)
    upstream = torch.randn_like(actual)
    actual.backward(upstream)
    expected.backward(upstream)
    torch.testing.assert_close(x.grad, expected_x.grad)
    for (name, param), (_, oracle) in zip(candidate.named_parameters(), eager.named_parameters(), strict=True):
        if oracle.grad is None:
            assert param.grad is None, name
        else:
            torch.testing.assert_close(param.grad, oracle.grad, msg=lambda message: name + ": " + message)


@pytest.mark.parametrize("keys", [3, 16, 131])
@pytest.mark.parametrize("masked", [False, True])
def test_aligned_sdpa_preserves_logical_scale_mask_and_all_adjoints(keys, masked):
    torch.manual_seed(keys)
    q,k,v = [torch.randn(2,4,n,4,requires_grad=True) for n in (5,keys,keys)]
    originals = [x.detach().clone().requires_grad_(True) for x in (q,k,v)]
    mask = torch.rand(2,1,1,keys) > .3 if masked else None
    if mask is not None:
        mask[0] = False  # An all-masked row is still zero, not fake evidence.
    expected = torch.nn.functional.scaled_dot_product_attention(*originals, attn_mask=mask)
    actual = attention_owner._aligned_narrow_fp32_sdpa(q,k,v,attn_mask=mask)
    torch.testing.assert_close(actual,expected)
    upstream=torch.randn_like(expected)
    expected.backward(upstream); actual.backward(upstream)
    for x,original in zip((q,k,v),originals,strict=True):
        torch.testing.assert_close(x.grad,original.grad,rtol=1e-5,atol=1e-6)


def test_aligned_short_context_does_not_inflate_key_axis(monkeypatch):
    native=torch.nn.functional.scaled_dot_product_attention
    seen=[]
    def wrapped(q,k,v,**kwargs):
        seen.append((k.size(-2),k.size(-1),kwargs["scale"]))
        return native(q,k,v,**kwargs)
    monkeypatch.setattr(torch.nn.functional,"scaled_dot_product_attention",wrapped)
    for keys in (16,2757):
        attention_owner._aligned_narrow_fp32_sdpa(torch.randn(1,4,2,4),
                                                torch.randn(1,4,keys,4),torch.randn(1,4,keys,4))
    assert seen == [(16,16,.5),(2816,16,.5)]


@pytest.mark.parametrize("width", [16,32])
@pytest.mark.parametrize("dtype", [torch.float32,torch.bfloat16])
@pytest.mark.parametrize("compiled", [False,True])
@pytest.mark.parametrize("blackwell", [False,True])
def test_alignment_dispatch_stays_narrow_fp32_compiled_cross(monkeypatch,width,dtype,compiled,blackwell):
    attention=attention_owner.FlashSDPAAttention(width,4,0.).to(dtype=dtype)
    attention.compiled_cross_attention_blackwell_workaround=blackwell
    attention.compiled_narrow_fp32_portfolio_head=True
    calls=[]
    aligned=attention_owner._aligned_narrow_fp32_sdpa
    def counted(*args,**kwargs):
        calls.append(True)
        return aligned(*args,**kwargs)
    monkeypatch.setattr(attention_owner,"_aligned_narrow_fp32_sdpa",counted)
    monkeypatch.setattr(attention_owner,"_torch_is_compiling",lambda:compiled)
    q=torch.randn(2,3,width,dtype=dtype)
    attention(q,torch.randn(2,5,width,dtype=dtype))
    assert bool(calls) == (width==16 and dtype==torch.float32 and compiled and blackwell)
    calls.clear()
    attention(q)  # Ordinary self attention is not redirected.
    assert not calls
    attention.compiled_narrow_fp32_portfolio_head=False
    attention(q,torch.randn(2,5,width,dtype=dtype))
    assert not calls  # Temporal cross attention retains the ordinary kernel.


def test_alignment_scope_is_set_on_head_not_encoder():
    model=FinancialTransformerModel(lookback=4,num_features=3,num_symbols=7,d_model=16,
                                    temporal_heads=4,cross_heads=4,joint_heads=4,dropout=0.)
    assert all(not block.attn.compiled_narrow_fp32_portfolio_head for block in model.temporal_blocks)
    for blocks in (model.latent_blocks,model.market_blocks,model.stock_read_latent_blocks,model.stock_read_market_blocks):
        assert all(block.attn.compiled_narrow_fp32_portfolio_head for block in blocks)
