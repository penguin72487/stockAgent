"""Detached projection fusion must preserve integer decisions at boundaries."""
import os

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import (
    _compiled_margin_position_projection, _project_margin_position_limits_impl,
)


@pytest.mark.skipif(os.environ.get('STOCKAGENT_TEST_CUDA_GRAPH') != '1'
                   or not torch.cuda.is_available(), reason='explicit CUDA acceptance required')
def test_fused_integer_projection_matches_eager_at_group_limits():
    deterministic = torch.are_deterministic_algorithms_enabled()
    torch.use_deterministic_algorithms(True)
    try:
        generator = torch.Generator().manual_seed(1882)
        for _ in range(8):
            slots = 1936
            previous = torch.randint(-20, 21, (slots,), generator=generator).cuda()
            proposed = torch.randint(-30, 31, (slots,), generator=generator).cuda()
            groups = torch.arange(slots, device='cuda') // 4
            units = torch.tensor([1., .25, 1., .25], device='cuda').repeat(slots // 4)
            # Exact limits and adjacent representable numbers exercise trunc/ceil.
            limits = torch.randint(0, 50, (slots,), generator=generator).float().cuda()
            limits[::3] = torch.nextafter(limits[::3], torch.full_like(limits[::3], float('inf')))
            options = dict(position_group=groups, position_units=units, group_limits=limits,
                           close_capacity=torch.randint(0, 15, (slots,), generator=generator).float().cuda(),
                           whole_contracts=True)
            eager = _project_margin_position_limits_impl(proposed, previous, **options)
            fused = _compiled_margin_position_projection()(proposed, previous, **options)
            for a, b in zip(eager, fused):
                torch.testing.assert_close(a, b, rtol=0, atol=0)
    finally:
        torch.use_deterministic_algorithms(deterministic)
