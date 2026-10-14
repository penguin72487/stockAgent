import pytest
import torch

from stockagent.models.financial_transformer import CandleEncoder


@pytest.mark.parametrize("sanitize", [True, False])
@pytest.mark.parametrize("inference", [True, False])
def test_owned_inference_normalization_preserves_bits_and_input(sanitize, inference):
    encoder = CandleEncoder(num_features=6, d_model=8, dropout=0.0, norm_type="layernorm",
                            ffn_type="swiglu", sanitize_inputs=sanitize,
                            causal_feature_rms_normalization=True).eval()
    scale = torch.tensor([3.0, 0.125, 91.0, 1e-5, 7.0, 2.0])
    active = torch.tensor([True, False, True, True, False, True])
    encoder.set_causal_feature_rms_normalizer(scale, active)
    x = torch.tensor([[[[float("nan"), -0.0, 123.456, -1.25, float("inf"), -99.0]]]])
    before = x.view(torch.int32).clone()
    with torch.inference_mode(inference):
        clean = torch.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0) if sanitize else x
        expected = ((clean / scale) * active.to(torch.float32)).index_select(-1, torch.arange(6))
        actual = encoder._base_joint_features(x)
        assert torch.equal(actual.view(torch.int32), expected.view(torch.int32))
    assert torch.equal(x.view(torch.int32), before)


def test_training_autograd_retains_original_normalization_gradient():
    encoder = CandleEncoder(num_features=4, d_model=8, dropout=0.0, norm_type="layernorm",
                            ffn_type="swiglu", sanitize_inputs=False,
                            causal_feature_rms_normalization=True).train()
    scale = torch.tensor([2.0, 3.0, 4.0, 7.0])
    active = torch.tensor([True, False, True, True])
    encoder.set_causal_feature_rms_normalizer(scale, active)
    x = torch.randn(2, 3, 4, requires_grad=True)
    actual = encoder._base_joint_features(x)
    reference = (x / scale) * active.to(torch.float32)
    torch.testing.assert_close(actual, reference, rtol=0, atol=0)
    actual.sum().backward()
    torch.testing.assert_close(x.grad, (active / scale).expand_as(x), rtol=0, atol=0)


def test_owned_normalization_is_not_admitted_with_autograd():
    encoder = CandleEncoder(num_features=4, d_model=8, dropout=0.0, norm_type="layernorm",
                            ffn_type="swiglu", sanitize_inputs=False,
                            causal_feature_rms_normalization=True).eval()
    with pytest.raises(RuntimeError, match="inference-only"):
        encoder._normalize_raw_features(torch.ones(2, 4), owned_inference=True)


@pytest.mark.parametrize("inference", [True, False])
@pytest.mark.parametrize("sanitize", [True, False])
def test_fullgraph_eval_keeps_original_fp32_normalization_and_input_bits(inference, sanitize):
    """Native V8 uses fullgraph eval; training-only compile never hit this guard."""
    encoder = CandleEncoder(num_features=6, d_model=8, dropout=0.0, norm_type="layernorm",
                            ffn_type="swiglu", sanitize_inputs=sanitize,
                            causal_feature_rms_normalization=True).eval()
    scale = torch.tensor([3.0, 0.125, 91.0, 1e-5, 7.0, 2.0])
    active = torch.tensor([True, False, True, True, False, True])
    encoder.set_causal_feature_rms_normalizer(scale, active)
    x = torch.tensor([[[[float("nan"), -0.0, 123.456, -1.25, float("inf"), -99.0]]]])
    before = x.view(torch.int32).clone()
    compiled = torch.compile(encoder._base_joint_features, backend="eager", fullgraph=True)
    with torch.inference_mode(inference), torch.no_grad():
        eager = encoder._base_joint_features(x)
        actual = compiled(x)
        assert torch.equal(actual.view(torch.int32), eager.view(torch.int32))
    assert torch.equal(x.view(torch.int32), before)


def test_fullgraph_candle_eval_compiles_without_changing_outputs_or_state_dict():
    encoder = CandleEncoder(num_features=4, d_model=8, dropout=0.0, norm_type="layernorm",
                            ffn_type="swiglu", sanitize_inputs=True,
                            causal_feature_rms_normalization=True).eval()
    encoder.set_causal_feature_rms_normalizer(torch.tensor([2.0, 3.0, 4.0, 7.0]),
                                            torch.tensor([True, False, True, True]))
    x = torch.randn(2, 3, 4)
    before = x.view(torch.int32).clone()
    state = {key: value.clone() for key, value in encoder.state_dict().items()}
    compiled = torch.compile(encoder, backend="eager", fullgraph=True)
    with torch.inference_mode():
        eager, _ = encoder(x)
        actual, _ = compiled(x)
        torch.testing.assert_close(actual, eager, rtol=0, atol=0)
    assert torch.equal(x.view(torch.int32), before)
    assert state.keys() == encoder.state_dict().keys()
    for key, value in encoder.state_dict().items():
        assert torch.equal(value, state[key])
