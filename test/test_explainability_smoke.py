from __future__ import annotations

from pathlib import Path
import json
import shutil

import torch

from stockagent.explainability import (
    ExplainabilitySettings,
    explain_batch,
    write_explanation_outputs,
)


class ToyExplainableModel(torch.nn.Module):
    def __init__(self, num_features: int) -> None:
        super().__init__()
        self.coef = torch.nn.Parameter(torch.arange(1, num_features + 1, dtype=torch.float32))

    def forward(self, x: torch.Tensor, mask: torch.Tensor, return_aux: bool | None = None):
        del return_aux
        scores = (x[:, -1] * self.coef).sum(dim=-1).masked_fill(~mask, -1e9)
        weights = torch.softmax(scores, dim=1).masked_fill(~mask, 0.0)
        return {
            "weights": weights,
            "score_logits": scores,
            "rank_logits": scores,
            "z_feat": x[:, -1],
            "aux": {"z_set": x.mean(dim=1)},
        }


def test_explainability_smoke(tmp_path: Path) -> None:
    torch.manual_seed(1)
    rows, lookback, symbols, features = 4, 3, 5, 3
    batch = {
        "x": torch.randn(rows, lookback, symbols, features),
        "future_log_returns": torch.randn(rows, symbols) * 0.01,
        "tradable_mask": torch.ones(rows, symbols, dtype=torch.bool),
    }
    output = explain_batch(
        ToyExplainableModel(features),
        batch,
        feature_names=[f"f{i}" for i in range(features)],
        symbols=[f"S{i}" for i in range(symbols)],
        dates=[f"2026-01-0{i + 1}" for i in range(rows)],
        settings=ExplainabilitySettings(top_k=2, max_rows=rows, ig_steps=2, perturb=True),
        device=torch.device("cpu"),
    )

    assert output["summary"]["warnings"]
    assert not output["frames"]["feature_importance_gradient"].empty
    assert not output["frames"]["top_decisions"].empty

    out_dir = tmp_path / "explain"
    shutil.rmtree(out_dir, ignore_errors=True)
    write_explanation_outputs(output, out_dir, metadata={"model_name": "toy"})
    assert (out_dir / "summary.json").exists()
    assert (out_dir / "report.md").exists()
    assert (out_dir / "feature_importance_gradient.csv").exists()
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["plots_generated"]
    assert list((out_dir / "plots").glob("*.png"))
