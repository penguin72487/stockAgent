import pytest
import torch

from stockagent.backtest.tw_inventory_scan import (
    fifo_cumsum, inventory_prefix_sum, inventory_positive_prefix,
    fifo_searchsorted, inventory_searchsorted,
)


@pytest.mark.parametrize('device', ['cpu', pytest.param('cuda', marks=pytest.mark.skipif(
    not torch.cuda.is_available(), reason='requires CUDA'))])
@pytest.mark.parametrize('dtype', [torch.float32, torch.float64])
def test_positive_prefix_preserves_absorbing_zero_negative_nan_and_infinity(device, dtype):
    for length in [0, 1, 2, 270]:
        value = torch.ones((3, length), dtype=dtype, device=device)
        for fault in [0., -0., -1., float('nan'), float('inf'), -float('inf')]:
            for index in sorted({0, length//2, max(0, length-1)}):
                probe = value.clone()
                if length:
                    probe[:, index] = fault
                expected = (probe > 0).cumprod(1).bool()
                actual = inventory_positive_prefix(probe, 1)
                torch.testing.assert_close(actual, expected, rtol=0, atol=0)
                assert actual.dtype == torch.bool and not actual.requires_grad


@pytest.mark.parametrize('device', ['cpu', pytest.param('cuda', marks=pytest.mark.skipif(
    not torch.cuda.is_available(), reason='requires CUDA'))])
def test_batched_positive_prefix_keeps_each_days_failure_on_its_own_minute_axis(device):
    # Loading the canonical parallel owner installs its custom-op vmap rules.
    import stockagent.backtest.tw_day_trade_parallel  # noqa: F401
    value = torch.tensor([[1., 0., 2., 3.], [1., 2., 3., 4.],
                          [1., 2., float('nan'), 4.]], device=device, dtype=torch.float64)
    compiled = torch.compile(lambda x: torch.vmap(
        lambda row: inventory_positive_prefix(row, 0))(x), fullgraph=True, backend='eager')
    torch.testing.assert_close(compiled(value), (value > 0).cumprod(1).bool(), rtol=0, atol=0)


@pytest.mark.parametrize("device", ["cpu", pytest.param("cuda", marks=pytest.mark.skipif(
    not torch.cuda.is_available(), reason="requires CUDA"))])
@pytest.mark.parametrize("shape,dim", [((3, 5), 0), ((3, 5), -1), ((0, 5), 0)])
def test_prefix_scan_schema_fake_autograd_and_device(device, shape, dim):
    value = torch.randn((*shape, 2), device=device, dtype=torch.float64)[..., 0].requires_grad_()
    assert set(torch.library.opcheck(inventory_prefix_sum, (value, dim)).values()) == {"SUCCESS"}
    torch.testing.assert_close(inventory_prefix_sum(value, dim), value.cumsum(dim))
    if value.numel():
        assert torch.autograd.gradcheck(inventory_prefix_sum, (value, dim))
        probe = torch.randn_like(value)
        actual = torch.autograd.grad((inventory_prefix_sum(value, dim) * probe).sum(), value)[0]
        expected = torch.autograd.grad((value.cumsum(dim) * probe).sum(), value)[0]
        torch.testing.assert_close(actual, expected)


def test_full_graph_prefix_sum_preserves_float64_and_gradient():
    value = torch.randn((3, 8), dtype=torch.float64).requires_grad_()
    compiled = torch.compile(lambda x: fifo_cumsum(x, -1), fullgraph=True, backend="eager")
    torch.testing.assert_close(compiled(value), value.cumsum(-1))
    assert torch.autograd.gradcheck(compiled, (value,))


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA')
@pytest.mark.parametrize('shape,dim', [((1, 2757), 0), ((2, 2757), 0),
    ((8, 57), 0), ((57, 1), -1), ((57, 2), -1), ((57, 270), -1),
    ((19, 512), -1), ((3, 513), -1), ((0, 270), -1)])
@pytest.mark.parametrize('strided', [False, True])
def test_bounded_triton_scan_preserves_fp64_prefix_and_reverse_adjoint(monkeypatch, shape, dim, strided):
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', 'triton')
    torch.manual_seed(928)
    value = torch.randn((*shape, 2), dtype=torch.float64, device='cuda')[..., 0]
    if not strided:
        value = value.contiguous()
    value.requires_grad_()
    expected = value.cumsum(dim)
    actual = inventory_prefix_sum(value, dim)
    assert actual.dtype == torch.float64 and actual.is_contiguous()
    assert actual.data_ptr() != value.data_ptr() or not actual.numel()
    torch.testing.assert_close(actual, expected, rtol=2e-13, atol=2e-13)
    probe = torch.randn_like(actual)
    gradient, = torch.autograd.grad(actual, value, probe)
    oracle, = torch.autograd.grad(expected, value, probe)
    torch.testing.assert_close(gradient, oracle, rtol=2e-13, atol=2e-13)
    if actual.numel():
        # Physical share prefixes remain exact integers, including duplicate
        # zero-capacity buckets; no FP32 accumulator enters the lot mechanism.
        shares = (value.detach().abs() * 1000).floor() * 1000
        torch.testing.assert_close(inventory_prefix_sum(shares, dim), shares.cumsum(dim), rtol=0, atol=0)


def test_prefix_backend_versions_graph_contract_and_rejects_invalid_selection(monkeypatch):
    from stockagent.backtest.tw_day_trade_graph import explicit_carry_graph_contract
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', 'native')
    native = explicit_carry_graph_contract()
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', 'triton')
    assert explicit_carry_graph_contract()['prefix_sum'] == 'fp64_strided_short_axis_triton_v2'
    assert 'prefix_sum' not in native
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', 'wrong')
    with pytest.raises(ValueError, match='prefix backend'):
        explicit_carry_graph_contract()
    with pytest.raises(ValueError, match='prefix backend'):
        inventory_prefix_sum(torch.zeros(1, dtype=torch.float64), 0)


def test_prefix_numerical_schedule_changes_optimizer_contract_without_explicit_graph(monkeypatch):
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot, _training_checkpoint_contract
    config = load_config('configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH', '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', 'native')
    original_snapshot = _configuration_fingerprint_snapshot(config)
    original_contract = _training_checkpoint_contract(config)
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', 'triton')
    assert _configuration_fingerprint_snapshot(config) != original_snapshot
    assert _training_checkpoint_contract(config) != original_contract
    assert _training_checkpoint_contract(config)['physical_fifo_prefix_contract'] == 'fp64_strided_short_axis_triton_v2'


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA')
def test_hybrid_dispatch_executes_short_kernel_and_keeps_long_axis_native(monkeypatch):
    import stockagent.backtest.tw_inventory_scan as scan
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREFIX_BACKEND', 'triton')
    original = scan._prefix_kernel
    calls = []
    def observed():
        calls.append(True)
        return original()
    monkeypatch.setattr(scan, '_prefix_kernel', observed)
    short = torch.randn((57, 2), dtype=torch.float64, device='cuda')
    torch.testing.assert_close(inventory_prefix_sum(short, -1), short.cumsum(-1))
    assert len(calls) == 1
    long = torch.randn((57, 270), dtype=torch.float64, device='cuda')
    torch.testing.assert_close(inventory_prefix_sum(long, -1), long.cumsum(-1), rtol=0, atol=0)
    assert len(calls) == 1


@pytest.mark.parametrize('right', [False, True])
@pytest.mark.parametrize('batched', [False, True])
def test_searchsorted_schema_fake_and_boundary_semantics(right, batched):
    boundary = torch.tensor([0., 0., 1000., 3000.], dtype=torch.float64)
    query = torch.tensor([[0., 500., 1000., 3000., 4000.]], dtype=torch.float64).repeat(3, 1)
    if batched:
        boundary = boundary.repeat(3, 1)
    expected = torch.searchsorted(boundary, query, right=right)
    assert set(torch.library.opcheck(inventory_searchsorted, (boundary, query, right)).values()) == {'SUCCESS'}
    torch.testing.assert_close(inventory_searchsorted(boundary, query, right), expected, rtol=0, atol=0)
    compiled = torch.compile(lambda b, q: fifo_searchsorted(b, q, right=right),
                             fullgraph=True, backend='eager')
    torch.testing.assert_close(compiled(boundary, query), expected, rtol=0, atol=0)


def test_searchsorted_keeps_gather_value_gradient_not_index_gradient():
    boundaries = torch.tensor([[0., 2., 4., 8.]], dtype=torch.float64, requires_grad=True)
    query = torch.tensor([[1., 3., 6.]], dtype=torch.float64, requires_grad=True)
    amounts = torch.tensor([[10., 20., 40., 80.]], dtype=torch.float64, requires_grad=True)
    compiled = torch.compile(lambda b, q, a: a.gather(1, fifo_searchsorted(b, q)).sum(),
                             fullgraph=True, backend='eager')
    result = compiled(boundaries, query, amounts)
    actual = torch.autograd.grad(result, (boundaries, query, amounts), allow_unused=True)
    assert actual[0] is None and actual[1] is None
    torch.testing.assert_close(actual[2], torch.tensor([[0., 1., 1., 1.]], dtype=torch.float64))


@pytest.mark.parametrize("shape", [(3, 7), (0, 7)])
def test_native_column_stack_preserves_layout_and_adjoint(shape):
    from stockagent.backtest.tw_inventory_scan import inventory_column_stack, fifo_stack_columns
    columns = [torch.randn(shape, dtype=torch.float64, requires_grad=True) for _ in range(12)]
    actual = inventory_column_stack(columns)
    expected = torch.stack(columns, dim=-1)
    torch.testing.assert_close(actual, expected, rtol=0, atol=0)
    probe = torch.randn_like(actual)
    gradients = torch.autograd.grad((actual * probe).sum(), columns)
    for index, gradient in enumerate(gradients):
        torch.testing.assert_close(gradient, probe[..., index], rtol=0, atol=0)
    torch.library.opcheck(inventory_column_stack, (columns,))
    compiled = torch.compile(fifo_stack_columns, fullgraph=True, backend="eager")
    torch.testing.assert_close(compiled(columns), expected, rtol=0, atol=0)
