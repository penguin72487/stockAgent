from copy import deepcopy

import pytest
import torch

import stockagent.models.transformer_base_portfolio as owner
from stockagent.models.financial_transformer import FinancialTransformerModel


@pytest.mark.parametrize("dtype", [torch.float32, torch.bfloat16, torch.float16])
@pytest.mark.parametrize("backend", ["sdpa_explicit", "sdpa_padded"])
def test_explicit_sdpa_owns_blackwell_override(dtype, backend):
    select = owner._compiled_cross_attention_is_manual
    assert select(backend, dtype, 16, 2757, True) is False
    if dtype != torch.float32:
        assert select("sdpa", dtype, 16, 2757, True) is True


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires initialized CUDA")
def test_known_bad_padded_sdpa_is_rejected_before_forward(monkeypatch):
    monkeypatch.setattr(torch, "__version__", "2.11.0+cu128")
    monkeypatch.setattr(torch.cuda, "get_device_capability", lambda device=None: (12, 0))
    module = owner.FlashSDPAAttention(32, 4, dropout=0.).cuda()
    module.compiled_cross_attention_backend = "sdpa_padded"
    monkeypatch.setattr(owner, "_torch_is_compiling", lambda: True)
    query = torch.randn(2, 16, 32, device="cuda")
    context = torch.randn(2, 2757, 32, device="cuda")
    with pytest.raises(RuntimeError, match="failed masked-gradient admission"):
        module(query, context)
    module.compiled_cross_attention_backend = "sdpa_explicit"
    assert torch.isfinite(module(query, context)).all()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires initialized CUDA")
@pytest.mark.parametrize("dtype", [torch.float32, torch.bfloat16], ids=["fp32", "bf16"])
@pytest.mark.filterwarnings("ignore:Attempting to run cuBLAS, but there was no current CUDA context.*:UserWarning:torch.autograd.graph")
def test_cuda_explicit_sdpa_keeps_mask_and_full_parameter_credit(dtype, monkeypatch):
    _cuda_compiled_attention_credit_oracle(dtype, monkeypatch, "sdpa_explicit")


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires initialized CUDA")
@pytest.mark.parametrize("dtype", [torch.float32, torch.bfloat16], ids=["fp32", "bf16"])
@pytest.mark.filterwarnings("ignore:Attempting to run cuBLAS, but there was no current CUDA context.*:UserWarning:torch.autograd.graph")
def test_cuda_padded_sdpa_keeps_mask_and_full_parameter_credit(dtype, monkeypatch):
    _cuda_compiled_attention_credit_oracle(dtype, monkeypatch, "sdpa_padded")


def _cuda_compiled_attention_credit_oracle(dtype, monkeypatch, backend):
    from torch.nn.attention import sdpa_kernel, SDPBackend
    import torch._functorch.config as aot_config
    monkeypatch.setattr(torch.backends.cuda.matmul, "allow_tf32", True)
    torch.manual_seed(692)
    reference = owner.FlashSDPAAttention(32, 4, dropout=0., qk_norm=True).cuda().train()
    candidate = deepcopy(reference)
    candidate.compiled_cross_attention_backend = backend
    candidate.compiled_cross_attention_blackwell_workaround = True
    graph_targets = []
    def capture_backend(graph, inputs):
        graph_targets.extend(str(node.target) for node in graph.graph.nodes)
        return torch._inductor.compile(graph, inputs)
    compiled = torch.compile(candidate, fullgraph=True, dynamic=False, backend=capture_backend)
    query = torch.randn(2, 16, 32, device="cuda", requires_grad=True)
    context = torch.randn(2, 2757, 32, device="cuda", requires_grad=True)
    observed_query = query.detach().clone().requires_grad_()
    observed_context = context.detach().clone().requires_grad_()
    mask = torch.rand(2, 2757, device="cuda") > .3
    mask[0] = False
    with sdpa_kernel(SDPBackend.MATH), torch.autocast("cuda", dtype=dtype, enabled=dtype != torch.float32):
        expected = reference(query, context, key_mask=mask)
    with aot_config.patch(backward_pass_autocast="off"), torch.autocast("cuda", dtype=dtype, enabled=dtype != torch.float32):
        actual = compiled(observed_query, observed_context, key_mask=mask)
    assert any("scaled_dot_product_attention" in target for target in graph_targets)
    torch.testing.assert_close(actual, expected, rtol=.025, atol=.002)
    coefficients = torch.randn_like(expected)
    expected.backward(coefficients)
    actual.backward(coefficients)
    assert torch.count_nonzero(observed_context.grad[~mask]).item() == 0
    assert torch.count_nonzero(observed_context.grad[mask]).item() > 0
    for a, b in ((observed_query.grad, query.grad), (observed_context.grad, context.grad)):
        torch.testing.assert_close(a, b, rtol=.06, atol=.003)
    for (_, a), (_, b) in zip(candidate.named_parameters(), reference.named_parameters(), strict=True):
        assert a.dtype == torch.float32
        if b.grad is None:
            assert a.grad is None
        else:
            assert a.grad is not None and torch.isfinite(a.grad).all()
            # BF16 cancellation near zero is not a lost credit path. Inspect
            # total error while keeping masked source derivatives exactly zero.
            expected_norm = b.grad.double().norm()
            actual_norm = a.grad.double().norm()
            if expected_norm > .1:
                assert actual_norm > 0
            assert (a.grad.double() - b.grad.double()).norm() <= .03 * expected_norm + .02


@pytest.mark.parametrize("dtype",[torch.float32,torch.bfloat16,torch.float64])
@pytest.mark.parametrize("queries",[1,3])
def test_new_backend_is_scoped_to_last_fp32_and_original_manual_half(dtype,queries):
    select=owner._compiled_cross_attention_is_manual
    assert select("manual_fp32_last",dtype,queries,32,False)==(
        dtype==torch.bfloat16 or (dtype==torch.float32 and queries==1))
    if dtype==torch.float32:
        for legacy in ("auto","manual","sdpa"):
            assert not select(legacy,dtype,queries,32,True)
    elif dtype==torch.bfloat16:
        assert select("manual",dtype,queries,32,False)
        assert select("sdpa",dtype,queries,32,True)
        assert not select("sdpa",dtype,queries,32,False)


def test_fp32_last_reductions_preserve_mask_rope_qk_and_all_gradients(monkeypatch):
    torch.manual_seed(173)
    module=owner.FlashSDPAAttention(32,4,dropout=0.,qk_norm=True,max_rope_steps=32)
    oracle=deepcopy(module)
    # CPU algebra oracle for the reused direct-reduction branch; GPU compiler,
    # selection, DDP and throughput are separate remote engineering evidence.
    module.use_flash_attention=False
    monkeypatch.setattr(owner,"_torch_is_compiling",lambda:True)
    query=torch.randn(7,1,32,requires_grad=True)
    context=torch.randn(7,32,32,requires_grad=True)
    expected_query=query.detach().clone().requires_grad_(True)
    expected_context=context.detach().clone().requires_grad_(True)
    mask=torch.rand(7,32)>.3
    mask[:,-1]=True
    kwargs={"key_mask":mask,"rope_positions":torch.arange(32),
            "query_rope_positions":torch.tensor([31])}
    actual=module(query,context,**kwargs)
    expected=oracle(expected_query,expected_context,**kwargs)
    torch.testing.assert_close(actual,expected,rtol=2e-5,atol=2e-6)
    coefficient=torch.randn_like(actual)
    (actual*coefficient).sum().backward();(expected*coefficient).sum().backward()
    for actual_grad,expected_grad in [(query.grad,expected_query.grad),(context.grad,expected_context.grad),
            *[(p.grad,q.grad) for p,q in zip(module.parameters(),oracle.parameters(),strict=True)]]:
        torch.testing.assert_close(actual_grad,expected_grad,rtol=3e-4,atol=3e-6)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires initialized CUDA")
@pytest.mark.filterwarnings("ignore:Attempting to run cuBLAS, but there was no current CUDA context.*:UserWarning:torch.autograd.graph")
def test_cuda_fp32_last_query_reduction_preserves_mask_rope_and_parameter_credit(monkeypatch):
    from torch.nn.attention import sdpa_kernel, SDPBackend
    monkeypatch.setattr(torch.backends.cuda.matmul, "allow_tf32", True)
    torch.manual_seed(697)
    reference = owner.FlashSDPAAttention(32, 4, dropout=0., qk_norm=True, max_rope_steps=32).cuda().train()
    candidate = deepcopy(reference)
    candidate.compiled_cross_attention_backend = "manual_fp32_last"
    graph_targets = []
    def capture_backend(graph, inputs):
        graph_targets.extend(str(node.target) for node in graph.graph.nodes)
        return torch._inductor.compile(graph, inputs)
    compiled = torch.compile(candidate, fullgraph=True, dynamic=False, backend=capture_backend)
    query = torch.randn(7, 1, 32, device="cuda", requires_grad=True)
    context = torch.randn(7, 32, 32, device="cuda", requires_grad=True)
    actual_query = query.detach().clone().requires_grad_()
    actual_context = context.detach().clone().requires_grad_()
    mask = torch.rand(7, 32, device="cuda") > .3
    mask[0] = False
    kwargs = {"key_mask": mask, "rope_positions": torch.arange(32, device="cuda"),
        "query_rope_positions": torch.tensor([31], device="cuda")}
    with sdpa_kernel(SDPBackend.MATH):
        expected = reference(query, context, **kwargs)
    actual = compiled(actual_query, actual_context, **kwargs)
    assert not any("scaled_dot_product_attention" in target for target in graph_targets)
    torch.testing.assert_close(actual, expected, rtol=.003, atol=.0003)
    coefficients = torch.randn_like(expected)
    expected.backward(coefficients)
    actual.backward(coefficients)
    assert torch.count_nonzero(actual_context.grad[~mask]).item() == 0
    assert torch.count_nonzero(actual_context.grad[mask]).item() > 0
    for a, b in [(actual_query.grad, query.grad), (actual_context.grad, context.grad),
            *[(p.grad, q.grad) for p, q in zip(candidate.parameters(), reference.parameters(), strict=True)]]:
        torch.testing.assert_close(a, b, rtol=.006, atol=.001)


def test_new_policy_is_explicit_and_does_not_modify_default():
    kwargs={"lookback":2,"num_features":3,"num_symbols":2,"d_model":8,
            "temporal_heads":2,"cross_heads":2,"joint_heads":2}
    assert FinancialTransformerModel(**kwargs).compiled_cross_attention_backend=="auto"
    selected=FinancialTransformerModel(**kwargs,compiled_cross_attention_backend="manual_fp32_last")
    assert selected.compiled_cross_attention_backend=="manual_fp32_last"
    explicit=FinancialTransformerModel(**kwargs,compiled_cross_attention_backend="sdpa_explicit")
    assert explicit.compiled_cross_attention_backend=="sdpa_explicit"
    assert any(module.compiled_cross_attention_backend == "sdpa_explicit"
        for module in explicit.modules() if isinstance(module, owner.FlashSDPAAttention))
