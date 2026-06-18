from __future__ import annotations

import json
from pathlib import Path

import pytest
import torch

from stockagent.backtest.gpu_plot import rapids_datashader_available
from stockagent.explainability import ExplainabilitySettings, explain_batch, write_explanation_outputs


class AuxProjectionModel(torch.nn.Module):
    def __init__(self, num_features: int, dim: int = 8) -> None:
        super().__init__()
        self.proj = torch.nn.Linear(num_features, dim)
        self.score = torch.nn.Linear(dim, 1)

    def forward(self, x: torch.Tensor, mask: torch.Tensor, return_aux: bool | None = None):
        stock_embedding = self.proj(x[:, -1])
        scores = self.score(stock_embedding).squeeze(-1).masked_fill(~mask, -1e9)
        weights = torch.softmax(scores, dim=1).masked_fill(~mask, 0.0)
        latent_factors = stock_embedding[:, :4, :]
        market_tokens = stock_embedding[:, 4:6, :]
        return {
            "weights": weights,
            "score_logits": scores,
            "rank_logits": scores,
            "aux": {
                "stock_embedding": stock_embedding,
                "latent_factors": latent_factors,
                "market_tokens": market_tokens,
            },
        }


@pytest.mark.skipif(
    not (torch.cuda.is_available() and rapids_datashader_available(require_cuda=True)),
    reason="CUDA RAPIDS Datashader stack is unavailable",
)
def test_explainability_cuml_umap_aux_projection_outputs(tmp_path: Path) -> None:
    torch.manual_seed(7)
    device = torch.device("cuda")
    rows, lookback, symbols, features = 6, 4, 9, 5
    batch = {
        "x": torch.randn(rows, lookback, symbols, features),
        "future_log_returns": torch.randn(rows, symbols) * 0.01,
        "tradable_mask": torch.ones(rows, symbols, dtype=torch.bool),
    }
    output = explain_batch(
        AuxProjectionModel(features).to(device),
        batch,
        feature_names=[f"f{i}" for i in range(features)],
        symbols=[f"S{i}" for i in range(symbols)],
        dates=[f"2026-01-{i + 1:02d}" for i in range(rows)],
        settings=ExplainabilitySettings(
            top_k=2,
            max_rows=rows,
            ig_steps=0,
            perturb=False,
            umap_enabled=True,
            umap_max_points=64,
            umap_n_neighbors=5,
            umap_min_dist=0.1,
        ),
        device=device,
    )

    assert output["aux_projection_frames"]
    assert "stock_embedding" in output["aux_projection_frames"]
    assert not output["aux_projection_frames"]["stock_embedding"].is_empty()

    out_dir = tmp_path / "explain"
    write_explanation_outputs(output, out_dir, metadata={"model_name": "aux"}, plot_backend="rapids_datashader")

    assert (out_dir / "aux_projections" / "stock_embedding.csv").exists()
    assert (out_dir / "plots" / "aux_umap" / "stock_embedding.png").exists()
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["aux_projection_summary"]
    assert summary["aux_projection_summary"][0]["method"] == "cuml_umap"
