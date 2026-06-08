from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import torch

from stockagent.backtest.gpu_plot import rapids_datashader_available
from stockagent.explainability import _plot_all_explanation_figures


def _has_rapids_cuda() -> bool:
    return bool(torch.cuda.is_available() and rapids_datashader_available(require_cuda=True))


@pytest.mark.skipif(not _has_rapids_cuda(), reason="CUDA RAPIDS Datashader stack is unavailable")
def test_explainability_dense_plots_use_datashader(tmp_path):
    lookback = 32
    features = [f"feature_{idx:02d}" for idx in range(10)]
    feature_time_rows = []
    for feat_idx, feature in enumerate(features):
        for lookback_from_end in range(lookback):
            value = float((feat_idx + 1) * (lookback_from_end + 1))
            feature_time_rows.append(
                {
                    "feature": feature,
                    "lookback_from_end": lookback_from_end,
                    "grad_x_input_abs": value,
                    "integrated_gradients_abs": value * 0.5,
                    "weight_abs_delta": value * 0.01,
                    "score_abs_delta": value * 0.02,
                }
            )
    feature_time = pd.DataFrame(feature_time_rows)
    decisions = pd.DataFrame(
        {
            "date": np.repeat([f"2024-01-{day:02d}" for day in range(1, 21)], 2),
            "side": ["long", "short"] * 20,
            "weight": np.tile([0.08, -0.05], 20),
        }
    )
    aux_dim = pd.DataFrame(
        {
            "dim": np.arange(24, dtype=np.int64),
            "mean_abs": np.linspace(0.01, 0.24, 24),
            "share": np.linspace(0.01, 0.24, 24) / np.linspace(0.01, 0.24, 24).sum(),
        }
    )

    generated = _plot_all_explanation_figures(
        {
            "feature_time_gradient": feature_time,
            "feature_time_integrated_gradients": feature_time,
            "feature_time_perturbation": feature_time,
            "top_decisions": decisions,
        },
        {"latent_factors": aux_dim},
        tmp_path,
        plot_backend="rapids_datashader",
    )

    expected = [
        tmp_path / "plots" / "feature_time_gradient_grad_x_input_abs_heatmap.png",
        tmp_path / "plots" / "feature_time_integrated_gradients_integrated_gradients_abs_heatmap.png",
        tmp_path / "plots" / "feature_time_perturbation_weight_abs_delta_heatmap.png",
        tmp_path / "plots" / "feature_time_perturbation_score_abs_delta_heatmap.png",
        tmp_path / "plots" / "top_decisions_exposure_by_side.png",
        tmp_path / "plots" / "aux_dims" / "latent_factors.png",
    ]
    assert generated
    for path in expected:
        assert path.exists()
        assert path.stat().st_size > 0
