import pytest
import torch

from stockagent.backtest.tw_inventory_scan import fifo_cumsum, inventory_prefix_sum


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
