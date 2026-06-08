from pathlib import Path

import pytest
import torch

from stockagent.backtest.gpu_plot import plot_equity_curve_tensor_datashader, rapids_datashader_available
from stockagent.backtest.simulator import BacktestResultTensor


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA is required for RAPIDS Datashader plot test")
def test_rapids_datashader_plots_cuda_tensor_directly(tmp_path: Path) -> None:
    if not rapids_datashader_available(require_cuda=True):
        pytest.skip("RAPIDS/cuDF/Datashader is not available")

    rows = 64
    strategy = torch.linspace(-0.001, 0.002, rows, device="cuda")
    benchmark = torch.linspace(0.0005, -0.0005, rows, device="cuda")
    turnovers = torch.zeros(rows, device="cuda")
    weights = torch.zeros(rows, 3, device="cuda")
    result = BacktestResultTensor(strategy, benchmark, turnovers, weights)

    output = tmp_path / "equity_gpu.png"
    ok = plot_equity_curve_tensor_datashader(result, dates=None, output_path=output)

    assert ok
    assert output.exists()
    assert output.stat().st_size > 0
