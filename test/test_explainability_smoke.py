from __future__ import annotations

from pathlib import Path
import json
import shutil

import torch

from stockagent.explainability import (
    ExplainabilitySettings,
    explain_batch,
    write_fold_stability_outputs,
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
    assert (out_dir / "paper_explainability_report.md").exists()
    assert (out_dir / "paper_explainability_summary.json").exists()
    assert (out_dir / "feature_importance_gradient.csv").exists()
    assert (out_dir / "paper_tables" / "global_feature_attribution.csv").exists()
    assert (out_dir / "paper_tables" / "trust_checks.csv").exists()
    assert (out_dir / "paper_tables" / "lookback_consistency.csv").exists()
    assert (out_dir / "plots_paper" / "feature_time_gradient_grad_x_input_abs_heatmap.png").exists()
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["plots_generated"]
    assert summary["paper_plots"]
    assert list((out_dir / "plots").glob("*.png"))


def test_paper_explainability_lookback_warning_and_heatmap_readability(tmp_path: Path) -> None:
    torch.manual_seed(3)
    rows, lookback, symbols, features = 5, 4, 6, 4
    batch = {
        "x": torch.randn(rows, lookback, symbols, features),
        "future_log_returns": torch.randn(rows, symbols) * 0.01,
        "tradable_mask": torch.ones(rows, symbols, dtype=torch.bool),
    }
    output = explain_batch(
        ToyExplainableModel(features),
        batch,
        feature_names=[f"body_feature_{i}" if i == 0 else f"f{i}" for i in range(features)],
        symbols=[f"S{i}" for i in range(symbols)],
        dates=[f"2026-02-{i + 1:02d}" for i in range(rows)],
        settings=ExplainabilitySettings(
            top_k=2,
            max_rows=rows,
            ig_steps=1,
            perturb=True,
            shap_enabled=False,
            umap_enabled=False,
        ),
        device=torch.device("cpu"),
    )

    out_dir = tmp_path / "paper_explain"
    write_explanation_outputs(
        output,
        out_dir,
        metadata={"model_name": "toy", "config_lookback": 32, "fold_id": 1, "split": "test"},
        plot_backend="matplotlib",
    )

    lookback = json.loads((out_dir / "paper_explainability_summary.json").read_text(encoding="utf-8"))[
        "attribution_lookback"
    ]
    assert lookback == 4
    consistency = (out_dir / "paper_tables" / "lookback_consistency.csv").read_text(encoding="utf-8")
    assert "warn" in consistency
    report = (out_dir / "paper_explainability_report.md").read_text(encoding="utf-8")
    assert "Lookback warning" in report
    assert "What it measures" in report
    assert "How to read it" in report
    assert "What would be suspicious" in report

    image_path = out_dir / "plots_paper" / "feature_time_gradient_grad_x_input_abs_heatmap.png"
    assert image_path.exists()
    assert image_path.stat().st_size > 20_000


def test_paper_fold_stability_outputs(tmp_path: Path) -> None:
    root = tmp_path / "explainability"
    for fold_id, shift in ((1, 0.0), (2, 0.02)):
        table_dir = root / f"fold_{fold_id:02d}_test" / "paper_tables"
        table_dir.mkdir(parents=True)
        rows = [
            {
                "feature": "body_ratio",
                "feature_group": "Candlestick",
                "feature_label": "Candlestick / body_ratio",
                "mean_available_share": 0.35 + shift,
            },
            {
                "feature": "close_logret_1d",
                "feature_group": "Return",
                "feature_label": "Return / close_logret_1d",
                "mean_available_share": 0.20 - shift,
            },
        ]
        import pandas as pd

        pd.DataFrame(rows).to_csv(table_dir / "global_feature_attribution.csv", index=False)

    output = write_fold_stability_outputs(root)
    assert output is not None
    assert (output / "paper_tables" / "fold_feature_stability.csv").exists()
    assert (output / "plots_paper" / "fold_stability_feature_share.png").exists()
    assert (output / "paper_fold_stability_report.md").exists()
