"""Independent output coordinates and the entmax simplex VJP contract."""
import pytest
import torch

from stockagent.models.normalization import (
    _masked_entmax15,
    masked_decoupled_entmax_cash_weights,
    masked_score_entmax_scale_separated_cash_weights,
)
from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel


def test_entmax_implicit_vjp_keeps_forward_and_matches_finite_difference():
    x = torch.tensor([[.5, .1, -.2, -20.]], requires_grad=True)
    mask = torch.tensor([[True, True, True, False]])
    cotangent = torch.tensor([[.3, -.6, .8, 2.]])
    old = _masked_entmax15(x, mask)
    new = _masked_entmax15(x, mask, implicit_backward=True)
    assert torch.equal(old, new)
    actual = torch.autograd.grad((new * cotangent).sum(), x)[0]
    numeric = torch.zeros_like(x)
    for column in range(x.size(1)):
        delta = torch.zeros_like(x)
        delta[0, column] = 1e-3
        plus = _masked_entmax15(x.detach() + delta, mask)
        minus = _masked_entmax15(x.detach() - delta, mask)
        numeric[0, column] = ((plus - minus) * cotangent).sum() / .002
    torch.testing.assert_close(actual, numeric, atol=5e-5, rtol=5e-4)
    assert actual[0, -1] == 0
    assert actual.abs().sum() > .1


def test_scale_separated_v2_preserves_exact_actions_but_changes_vjp():
    x = torch.tensor([[.9, -.6, .3]], requires_grad=True)
    old = masked_score_entmax_scale_separated_cash_weights(x, None)
    new = masked_score_entmax_scale_separated_cash_weights(x, None, implicit_backward=True)
    assert torch.equal(old, new)
    vector = torch.tensor([[.2, -.7, .4]])
    old_gradient = torch.autograd.grad((old * vector).sum(), x)[0]
    gradient = torch.autograd.grad((new * vector).sum(), x)[0]
    numeric = torch.zeros_like(x)
    for column in range(x.size(1)):
        delta = torch.zeros_like(x)
        delta[0, column] = 1e-3
        plus = masked_score_entmax_scale_separated_cash_weights(x.detach() + delta, None)
        minus = masked_score_entmax_scale_separated_cash_weights(x.detach() - delta, None)
        numeric[0, column] = ((plus - minus) * vector).sum() / .002
    torch.testing.assert_close(gradient, numeric, atol=5e-5, rtol=5e-4)
    assert not torch.allclose(old_gradient, gradient, atol=1e-4, rtol=1e-4)


def test_decoupled_direction_can_flip_without_changing_stock_selection():
    selection = torch.tensor([[1., .3, -.5]], requires_grad=True)
    direction = torch.tensor([[.4, -.6, .2]], requires_grad=True)
    cash = torch.tensor([0.], requires_grad=True)
    weights, parts = masked_decoupled_entmax_cash_weights(selection, direction, cash, None)
    opposite, other = masked_decoupled_entmax_cash_weights(selection, -direction, cash, None)
    torch.testing.assert_close(opposite, -weights)
    assert torch.equal(parts['decoupled_relative_allocation'], other['decoupled_relative_allocation'])
    torch.testing.assert_close(weights.abs().sum(1) + parts['cash_weight'], torch.ones(1))
    (weights * torch.tensor([[.3, -.5, .7]])).sum().backward()
    for tensor in (selection, direction, cash):
        assert tensor.grad is not None and torch.isfinite(tensor.grad).all()
        assert tensor.grad.abs().sum() > 0


def test_normalized_direction_gives_cash_head_sole_control_of_risky_budget():
    rank = torch.tensor([[1., .3, -.5]])
    direction = torch.tensor([[.002, -.004, .001]])
    cash = torch.tensor([0.])
    original, _ = masked_decoupled_entmax_cash_weights(rank, direction, cash, None)
    normalized, parts = masked_decoupled_entmax_cash_weights(
        rank, direction, cash, None, normalize_direction=True)
    assert original.abs().sum() < .003
    torch.testing.assert_close(normalized.abs().sum(1), torch.tensor([.5]))
    torch.testing.assert_close(parts['cash_weight'], torch.tensor([.5]))
    other, _ = masked_decoupled_entmax_cash_weights(
        rank, direction, torch.tensor([2.]), None, normalize_direction=True)
    torch.testing.assert_close(other, normalized * (2 * torch.sigmoid(torch.tensor(-2.))))
    assert torch.equal(torch.sign(normalized), torch.sign(original))


def test_normalized_direction_vjp_matches_finite_difference_for_all_three_heads():
    values = [torch.tensor([[1., .3, -.5]], requires_grad=True),
              torch.tensor([[.4, -.6, .2]], requires_grad=True),
              torch.tensor([.2], requires_grad=True)]
    cotangent = torch.tensor([[.3, -.5, .7]])

    def score(inputs):
        weights, _ = masked_decoupled_entmax_cash_weights(
            *inputs, None, normalize_direction=True)
        return (weights * cotangent).sum()

    actual = torch.autograd.grad(score(values), values)
    for index, value in enumerate(values):
        numeric = torch.zeros_like(value)
        for column in range(value.numel()):
            delta = torch.zeros_like(value)
            delta.view(-1)[column] = .001
            plus, minus = [v.detach() for v in values], [v.detach() for v in values]
            plus[index], minus[index] = value.detach() + delta, value.detach() - delta
            numeric.view(-1)[column] = (score(plus) - score(minus)) / .002
        torch.testing.assert_close(actual[index], numeric, atol=5e-5, rtol=1e-3)
        assert actual[index].abs().sum() > 0


@pytest.mark.parametrize('long_only', [False, True])
def test_normalized_direction_empty_masks_and_zero_direction_stay_finite(long_only):
    rank = torch.zeros(2, 3, requires_grad=True)
    direction = torch.zeros(2, 3, requires_grad=True)
    cash = torch.zeros(2, requires_grad=True)
    mask = torch.tensor([[True, True, False], [False, False, False]])
    weights, parts = masked_decoupled_entmax_cash_weights(
        rank, direction, cash, mask, long_only=long_only, normalize_direction=True)
    assert torch.equal(weights[~mask], torch.zeros_like(weights[~mask]))
    assert parts['cash_weight'][1] == 1.
    if long_only:
        torch.testing.assert_close(weights.abs().sum(1), torch.tensor([.5, 0.]))
    else:
        assert torch.count_nonzero(weights) == 0
        assert torch.equal(parts['cash_weight'], torch.ones(2))
    weights.sum().backward()
    assert all(torch.isfinite(v.grad).all() for v in (rank, direction, cash))


@pytest.mark.parametrize('long_only', [False, True])
def test_decoupled_masks_empty_rows_and_extremes_are_finite(long_only):
    selection = torch.tensor([[1e38, -1e38, 0.], [0., 0., 0.]], requires_grad=True)
    direction = torch.tensor([[.5, -.2, .1], [0., 0., 0.]], requires_grad=True)
    cash = torch.tensor([0., 0.], requires_grad=True)
    mask = torch.tensor([[True, True, False], [False, False, False]])
    weights, parts = masked_decoupled_entmax_cash_weights(selection, direction, cash, mask, long_only=long_only)
    assert torch.isfinite(weights).all() and weights.dtype == torch.float32
    assert torch.equal(weights[~mask], torch.zeros_like(weights[~mask]))
    assert parts['cash_weight'][1] == 1.
    assert (weights.abs().sum(1) <= 1.).all()
    if long_only:
        assert (weights >= 0).all()
    weights.sum().backward()
    assert all(torch.isfinite(tensor.grad).all() for tensor in (selection, direction, cash))


def _model(mode):
    return TransformerBasePortfolioModel(
        lookback=3, num_features=5, num_symbols=7, d_model=8,
        temporal_heads=2, cross_heads=2,
        temporal_layers=2, num_latent_factors=2, num_market_tokens=2,
        temporal_pooling='last', temporal_query_mode='last_only',
        portfolio_output_mode=mode, portfolio_mode='long_short',
        head_hidden_dim=8, dropout=0., return_aux=False,
    )


@pytest.mark.parametrize('mode', ['decoupled_entmax_cash', 'decoupled_entmax_cash_v2',
                                  'score_entmax_scale_separated_cash_v2'])
def test_model_allocation_heads_receive_gradients_and_strict_reload(mode):
    torch.manual_seed(5)
    model = _model(mode)
    x = torch.randn(2, 3, 7, 5)
    mask = torch.ones(2, 7, dtype=torch.bool)
    weights, _, aux = model(x, mask, return_aux=True)
    assert weights.shape == (2, 7) and torch.isfinite(weights).all()
    assert (weights.abs().sum(1) <= 1.000001).all()
    (weights * torch.linspace(-1., 1., 7)).sum().backward()
    if mode in {'decoupled_entmax_cash', 'decoupled_entmax_cash_v2'}:
        for head in (model.score_head, model.direction_score_head, model.learned_cash_score_head):
            assert sum(p.grad.abs().sum() for p in head.parameters() if p.grad is not None) > 0
        torch.testing.assert_close(aux['weights_with_cash'][:, :-1], weights)
        with pytest.raises(ValueError, match='requires the contextual'):
            model.postprocess_action_logits(torch.zeros(2, 7), mask)
    else:
        assert model.direction_score_head is None and model.cash_asset_token is None
        original = _model('score_entmax_scale_separated_cash')
        original.load_state_dict(model.state_dict(), strict=True)
        assert torch.equal(original(x, mask), model(x, mask))
    restored = _model(mode)
    restored.load_state_dict(model.state_dict(), strict=True)
    assert torch.equal(restored(x, mask), model(x, mask))


@pytest.mark.skipif(not torch.cuda.is_available(), reason='physical CUDA required')
@pytest.mark.parametrize('mode', ['decoupled_entmax_cash', 'decoupled_entmax_cash_v2'])
def test_decoupled_cuda_graph_replay_updates_all_heads(mode):
    model = _model(mode).cuda()
    x = torch.randn(2, 3, 7, 5, device='cuda')
    mask = torch.ones(2, 7, dtype=torch.bool, device='cuda')
    temperature = torch.ones((), device='cuda')
    stream = torch.cuda.Stream()
    stream.wait_stream(torch.cuda.current_stream())
    with torch.cuda.stream(stream):
        for _ in range(3):
            model.zero_grad(set_to_none=True)
            (model(x, mask, temperature=temperature).square().sum()).backward()
    torch.cuda.current_stream().wait_stream(stream)
    model.zero_grad(set_to_none=True)
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        output = model(x, mask, temperature=temperature)
        output.square().sum().backward()
    graph.replay()
    torch.cuda.synchronize()
    assert torch.isfinite(output).all()
    assert model.direction_score_head[-1].weight.grad.abs().sum() > 0
