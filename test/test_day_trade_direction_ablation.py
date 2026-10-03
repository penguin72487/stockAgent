"""Directional actions must be constrained before selection, with their own ABI."""
from pathlib import Path

import pytest
import torch

from stockagent.config import load_config
from stockagent.models.normalization import masked_cash_entmax15_weights
from stockagent.training.checkpoint_contract import _effective_model_portfolio_mode
from test_financial_transformer import _make_model


@pytest.mark.parametrize("fp32", [False, True])
@pytest.mark.parametrize("zero_gradient", [False, True])
def test_short_only_is_signed_mirror_of_long_only(fp32, zero_gradient):
    scores = torch.tensor([[1., -.3, -2., 0.], [0., 0., 0., 0.]], requires_grad=True)
    mask = torch.tensor([[True, True, True, False], [False, False, False, False]])
    options = dict(preserve_fp32_output=fp32, preserve_zero_score_gradient=zero_gradient)
    short = masked_cash_entmax15_weights(scores, mask, long_mask=torch.zeros_like(mask), **options)
    long = masked_cash_entmax15_weights(-scores, mask, short_mask=torch.zeros_like(mask), **options)
    torch.testing.assert_close(short, -long, rtol=0, atol=0)
    assert (short <= 0).all() and short[0, 0] == 0
    assert short[1].count_nonzero() == 0
    short.square().sum().backward()
    assert scores.grad.isfinite().all() and scores.grad[0, 2] != 0
    assert scores.grad[0, 0] == 0


def test_all_positive_scores_do_not_invent_shorts():
    scores = torch.tensor([[1., 4., 2.]])
    mask = torch.ones_like(scores, dtype=torch.bool)
    actual = masked_cash_entmax15_weights(scores, mask, long_mask=~mask)
    torch.testing.assert_close(actual, torch.zeros_like(actual), rtol=0, atol=0)


def test_unsupported_short_only_output_fails_closed():
    with pytest.raises(ValueError, match="short_only"):
        _make_model(portfolio_mode="short_only", portfolio_output_mode="learned_cash")


@pytest.mark.parametrize("direction,score", [("long_only", 2.), ("short_only", -2.)])
def test_financial_direction_survives_bf16_compile_and_reload(direction, score):
    model = _make_model(portfolio_mode=direction, portfolio_output_mode="score_entmax_cash",
                        center_long_short_logits=False, return_aux=False).train()
    device = next(model.parameters()).device
    last = [m for m in model.score_head.modules() if isinstance(m, torch.nn.Linear)][-1]
    with torch.no_grad():
        last.weight.zero_()
        last.bias.fill_(score)
    x = torch.randn(2, 5, 7, 10, device=device)
    mask = torch.ones(2, 7, device=device, dtype=torch.bool)
    mask[1] = False
    compiled = torch.compile(model, backend="eager", fullgraph=True)
    with torch.autocast(device_type=device.type, dtype=torch.bfloat16):
        w = compiled(x, mask)
    assert w.dtype == torch.float32
    assert (w * score >= 0).all() and w[0].abs().sum() > 0
    assert w[1].count_nonzero() == 0
    w.square().sum().backward()
    assert last.bias.grad.isfinite().all() and last.bias.grad.abs().sum() > 0
    restored = _make_model(portfolio_mode=direction, portfolio_output_mode="score_entmax_cash",
                          center_long_short_logits=False, return_aux=False).train()
    restored.load_state_dict(model.state_dict())
    with torch.autocast(device_type=device.type, dtype=torch.bfloat16):
        torch.testing.assert_close(restored(x, mask), w, rtol=0, atol=0)


def test_short_only_config_and_checkpoint_contract(tmp_path):
    baseline = Path("configs/deployments/tw_day_trade_v8_ofat_resume_corrected_control.yaml").resolve()
    path = tmp_path / "short.yaml"
    path.write_text(f"base_config: {baseline}\ntraining:\n  financial_transformer:\n    portfolio_mode: short_only\n")
    config = load_config(path)
    assert _effective_model_portfolio_mode(config, "financial_transformer", {"portfolio_mode": "short_only"}) == "short_only"
    with pytest.raises(ValueError, match="portfolio_mode"):
        _effective_model_portfolio_mode(config, "gradient_boosted_portfolio_transformer", {"portfolio_mode": "short_only"})
    path.write_text(path.read_text() + "trading:\n  long_only: true\n")
    with pytest.raises(ValueError, match="conflicts"):
        load_config(path)
