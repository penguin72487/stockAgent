import pytest
import torch

from stockagent.backtest.tw_inventory_scan import (
    fifo_cumsum, inventory_prefix_sum, fifo_searchsorted, inventory_searchsorted,
)


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
