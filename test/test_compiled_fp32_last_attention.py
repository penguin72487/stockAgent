from copy import deepcopy

import pytest
import torch

import stockagent.models.transformer_base_portfolio as owner
from stockagent.models.financial_transformer import FinancialTransformerModel


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


def test_new_policy_is_explicit_and_does_not_modify_default():
    kwargs={"lookback":2,"num_features":3,"num_symbols":2,"d_model":8,
            "temporal_heads":2,"cross_heads":2,"joint_heads":2}
    assert FinancialTransformerModel(**kwargs).compiled_cross_attention_backend=="auto"
    selected=FinancialTransformerModel(**kwargs,compiled_cross_attention_backend="manual_fp32_last")
    assert selected.compiled_cross_attention_backend=="manual_fp32_last"
