from __future__ import annotations

from pathlib import Path
import json
import shutil
import warnings
from types import SimpleNamespace

import numpy as np
import torch

from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.explainability import (
    ExplainabilitySettings,
    _save_matplotlib_figure,
    _with_numeric,
    explain_batch,
    run_loaded_model_explanation,
    write_fold_stability_outputs,
    write_explanation_outputs,
)


class ToyExplainableModel(torch.nn.Module):
    def __init__(self, num_features: int) -> None:
        super().__init__()
        self.coef = torch.nn.Parameter(torch.arange(1, num_features + 1, dtype=torch.float32))
        self.forward_calls = 0

    def forward(self, x: torch.Tensor, mask: torch.Tensor, return_aux: bool | None = None):
        del return_aux
        self.forward_calls += 1
        scores = (x[:, -1] * self.coef).sum(dim=-1).masked_fill(~mask, -1e9)
        weights = torch.softmax(scores, dim=1).masked_fill(~mask, 0.0)
        return {
            "weights": weights,
            "score_logits": scores,
            "rank_logits": scores,
            "z_feat": x[:, -1],
            "aux": {"z_set": x.mean(dim=1)},
        }


def test_save_matplotlib_figure_suppresses_transform_dot_warning(monkeypatch, tmp_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()
    ax.plot([0.0, 1.0], [0.0, 1.0])
    output_path = tmp_path / "plot.png"

    def noisy_savefig(path: str | Path, *args: object, **kwargs: object) -> None:
        del args, kwargs
        warnings.warn("invalid value encountered in dot", RuntimeWarning, stacklevel=1)
        Path(path).write_bytes(b"plot")

    monkeypatch.setattr(fig, "savefig", noisy_savefig)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            _save_matplotlib_figure(fig, output_path)
        assert output_path.exists()
    finally:
        plt.close(fig)


def test_with_numeric_masks_nonfinite_values_before_plotting() -> None:
    import polars as pl

    frame = pl.DataFrame({"metric": [1.0, float("inf"), float("-inf"), float("nan"), None]})

    cleaned = _with_numeric(frame, "metric")

    assert cleaned.get_column("metric").to_list() == [1.0, None, None, None, None]


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
    assert not output["frames"]["feature_importance_gradient"].is_empty()
    assert not output["frames"]["top_decisions"].is_empty()

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


def test_explainability_chunked_attribution_matches_serial_with_fewer_forwards() -> None:
    torch.manual_seed(11)
    rows, lookback, symbols, features = 3, 4, 5, 3
    batch = {
        "x": torch.randn(rows, lookback, symbols, features),
        "future_log_returns": torch.randn(rows, symbols) * 0.01,
        "tradable_mask": torch.ones(rows, symbols, dtype=torch.bool),
    }
    common = dict(
        top_k=2,
        max_rows=rows,
        ig_steps=4,
        perturb=True,
        shap_enabled=False,
        regime_analysis=False,
        umap_enabled=False,
    )
    serial_model = ToyExplainableModel(features)
    serial = explain_batch(
        serial_model,
        batch,
        feature_names=[f"f{i}" for i in range(features)],
        symbols=[f"S{i}" for i in range(symbols)],
        dates=[f"2026-03-0{i + 1}" for i in range(rows)],
        settings=ExplainabilitySettings(**common, ig_batch_size=1, perturb_batch_size=1),
        device=torch.device("cpu"),
    )
    chunked_model = ToyExplainableModel(features)
    chunked = explain_batch(
        chunked_model,
        batch,
        feature_names=[f"f{i}" for i in range(features)],
        symbols=[f"S{i}" for i in range(symbols)],
        dates=[f"2026-03-0{i + 1}" for i in range(rows)],
        settings=ExplainabilitySettings(**common, ig_batch_size=2, perturb_batch_size=4),
        device=torch.device("cpu"),
    )

    for frame_name, value_col in (
        ("feature_time_integrated_gradients", "integrated_gradients_abs"),
        ("feature_time_perturbation", "weight_abs_delta"),
    ):
        left = serial["frames"][frame_name].sort(["lookback_index", "feature"])
        right = chunked["frames"][frame_name].sort(["lookback_index", "feature"])
        assert left.select(["lookback_index", "lookback_from_end", "feature"]).equals(
            right.select(["lookback_index", "lookback_from_end", "feature"])
        )
        np.testing.assert_allclose(left.get_column(value_col).to_numpy(), right.get_column(value_col).to_numpy(), rtol=1e-5, atol=1e-7)

    assert chunked_model.forward_calls < serial_model.forward_calls


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
        import polars as pl

        pl.DataFrame(rows).write_csv(table_dir / "global_feature_attribution.csv")

    output = write_fold_stability_outputs(root)
    assert output is not None
    assert (output / "paper_tables" / "fold_feature_stability.csv").exists()
    assert (output / "plots_paper" / "fold_stability_feature_share.png").exists()
    assert (output / "paper_fold_stability_report.md").exists()


def test_run_loaded_model_explanation_writes_same_runner_outputs(tmp_path: Path) -> None:
    torch.manual_seed(7)
    rows, lookback, symbols, features = 6, 2, 4, 3
    panel = PanelData(
        dates=np.arange(rows).astype("datetime64[D]"),
        symbols=[f"S{i}" for i in range(symbols)],
        feature_names=[f"f{i}" for i in range(features)],
        features=torch.randn(rows, symbols, features).numpy(),
        returns_1d=(torch.randn(rows, symbols) * 0.01).numpy(),
        tradable_mask=torch.ones(rows, symbols, dtype=torch.bool).numpy(),
        can_buy_mask=torch.ones(rows, symbols, dtype=torch.bool).numpy(),
        can_sell_mask=torch.ones(rows, symbols, dtype=torch.bool).numpy(),
        alive_mask=torch.ones(rows, symbols, dtype=torch.bool).numpy(),
        benchmark_returns=(torch.randn(rows) * 0.01).numpy(),
        close_prices=torch.ones(rows, symbols).numpy(),
    )
    fold = WalkForwardFold(
        fold_id=1,
        train_indices=np.arange(0, 2),
        val_indices=np.arange(2, 3),
        test_indices=np.arange(3, rows),
        train_years=[1970],
        val_years=[1970],
        test_years=[1970],
    )
    config = SimpleNamespace(training=SimpleNamespace(model_name="toy", lookback=lookback))
    settings = ExplainabilitySettings(
        top_k=2,
        max_rows=2,
        ig_steps=0,
        perturb=False,
        report_style="none",
        standard_plots=False,
        shap_enabled=False,
        regime_analysis=False,
        fold_stability=False,
        umap_enabled=False,
        cross_asset_enabled=False,
    )

    output = run_loaded_model_explanation(
        config=config,
        panel=panel,
        fold=fold,
        model=ToyExplainableModel(features),
        checkpoint_path=tmp_path / "fold_01" / "checkpoint_best.pt",
        output_dir=tmp_path,
        split="test",
        explain_output_dir=None,
        settings=settings,
        write_plots=False,
        plot_backend="matplotlib",
        device=torch.device("cpu"),
        checkpoint_info={"checkpoint_epoch": 3},
        timing_file_name="train_explainability_timing.json",
    )

    assert output == tmp_path / "explainability" / "fold_01_test"
    assert (output / "summary.json").exists()
    assert (output / "report.md").exists()
    assert (output / "explainability_timing.json").exists()
    assert (output / "train_explainability_timing.json").exists()
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    timing = json.loads((output / "train_explainability_timing.json").read_text(encoding="utf-8"))
    assert summary["report_style"] == "none"
    assert summary["rows"] == 2
    assert timing["loaded_model_reused"] is True
    assert timing["compute_timing"]["total_s"] >= 0
