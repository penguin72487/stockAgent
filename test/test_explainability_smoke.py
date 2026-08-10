from __future__ import annotations

from pathlib import Path
import json
import shutil
import warnings
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.explainability import (
    ExplainabilitySettings,
    _align_panel_to_checkpoint_universe,
    _daily_weight_symbols,
    _evenly_spaced_sample_indices,
    _method_agreement_table,
    _forward_outputs,
    _perturbation_importance,
    _portfolio_j_lens,
    _combine_j_lens_frames_from_chunks,
    _cross_fold_figure_spec,
    _plot_all_explanation_figures,
    _representation_aux_summary,
    _read_cross_fold_source_table,
    _save_matplotlib_figure,
    _score_head_surrogate_shap,
    _score_head_surrogate_shap_chunked,
    _selection_from_weights,
    _streaming_aux_umap_samples,
    _write_decision_inventory_streaming,
    _with_numeric,
    _feature_correlations,
    _feature_correlations_chunked,
    explain_batch,
    run_loaded_model_explanation,
    write_fold_stability_outputs,
    write_explanation_outputs,
)


class _TinyResidualBlock(torch.nn.Module):
    def __init__(self, dim: int) -> None:
        super().__init__()
        self.linear = torch.nn.Linear(dim, dim, bias=False)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return x + 0.1 * torch.tanh(self.linear(x))


class _TinyJLensePortfolio(torch.nn.Module):
    def __init__(self, features: int = 3, dim: int = 4) -> None:
        super().__init__()
        self.input_projection = torch.nn.Linear(features, dim, bias=False)
        self.temporal_blocks = torch.nn.ModuleList([_TinyResidualBlock(dim)])
        self.score_head = torch.nn.Linear(dim, 1, bias=False)

    def forward(self, x: torch.Tensor, mask: torch.Tensor, return_aux: bool | None = None):
        z_stock = self.input_projection(x.mean(dim=1))
        for block in self.temporal_blocks:
            z_stock = block(z_stock)
        scores = self.score_head(z_stock).squeeze(-1).masked_fill(~mask, 0.0)
        weights = scores.masked_fill(~mask, 0.0)
        aux = {"z_stock": z_stock, "score_logits": scores}
        return weights, scores, aux if return_aux else {}


def test_evenly_spaced_sample_indices_stay_in_bounds_above_float32_exact_range() -> None:
    n_points = 216 * 32 * 2735
    indices = _evenly_spaced_sample_indices(
        n_points,
        65_536,
        device=torch.device("cpu"),
    )

    assert indices.dtype == torch.long
    assert indices.numel() == 65_536
    assert int(indices[0]) == 0
    assert int(indices[-1]) == n_points - 1
    assert bool(torch.all(indices[1:] > indices[:-1]))
    assert int(indices.min()) >= 0
    assert int(indices.max()) < n_points


def test_phase_actions_fail_closed_before_portfolio_explainability() -> None:
    weights = torch.zeros((2, 3, 4), dtype=torch.float32)
    mask = torch.ones((2, 4), dtype=torch.bool)

    with pytest.raises(ValueError, match=r"phase actions \[B,P,S\]"):
        _selection_from_weights(weights, mask)


def test_portfolio_j_lens_is_complete_and_faithfulness_checked() -> None:
    torch.manual_seed(9)
    model = _TinyJLensePortfolio()
    x = torch.randn(2, 3, 5, 3)
    mask = torch.ones(2, 5, dtype=torch.bool)
    frames, summary, warnings_out = _portfolio_j_lens(
        model,
        x,
        mask,
        dates=["2026-01-01", "2026-01-02"],
        symbols=[f"S{i}" for i in range(5)],
        enabled=True,
        intervention_fraction=0.01,
        progress_enabled=False,
    )

    assert warnings_out == []
    assert summary["status"] == "ok"
    assert summary["d_model"] == 4
    assert summary["layers"] == 2
    assert frames["j_lens_transport"].height == 2 * 4 * 4
    assert frames["j_lens_dimension_readout"].height == 2 * 4
    assert frames["j_lens_date_readout"].height == 2 * 2
    assert frames["j_lens_stock_readout"].height == 2 * 5
    assert frames["j_lens_faithfulness"].height == 1
    completeness = frames["j_lens_completeness"].row(0, named=True)
    assert completeness["transport_cell_coverage"] == pytest.approx(1.0)
    assert completeness["dimension_cell_coverage"] == pytest.approx(1.0)
    assert completeness["top_k_truncation"] is False


def test_portfolio_j_lens_batched_vjp_matches_scalar_vjp() -> None:
    torch.manual_seed(91)
    model = _TinyJLensePortfolio()
    x = torch.randn(2, 3, 5, 3)
    mask = torch.ones(2, 5, dtype=torch.bool)
    kwargs = {
        "dates": ["2026-01-01", "2026-01-02"],
        "symbols": [f"S{i}" for i in range(5)],
        "enabled": True,
        "intervention_fraction": 0.01,
        "progress_enabled": False,
    }

    scalar_frames, scalar_summary, _ = _portfolio_j_lens(
        model,
        x,
        mask,
        vjp_batch_size=1,
        **kwargs,
    )
    batched_frames, batched_summary, _ = _portfolio_j_lens(
        model,
        x,
        mask,
        vjp_batch_size=4,
        **kwargs,
    )

    scalar = scalar_frames["j_lens_transport"].sort(["layer_order", "output_dim", "input_dim"])
    batched = batched_frames["j_lens_transport"].sort(["layer_order", "output_dim", "input_dim"])
    np.testing.assert_allclose(
        scalar.get_column("jacobian").to_numpy(),
        batched.get_column("jacobian").to_numpy(),
        rtol=1e-6,
        atol=1e-7,
    )
    assert scalar_summary["vjp_passes"] == batched_summary["vjp_passes"] == 4
    assert scalar_summary["autograd_calls"] == 4
    assert batched_summary["autograd_calls"] == 1


def test_portfolio_j_lens_chunk_aggregation_preserves_complete_cells() -> None:
    torch.manual_seed(10)
    model = _TinyJLensePortfolio()
    chunks = []
    for chunk_id in range(2):
        x = torch.randn(1, 3, 5, 3)
        mask = torch.ones(1, 5, dtype=torch.bool)
        frames, summary, _ = _portfolio_j_lens(
            model,
            x,
            mask,
            dates=[f"2026-01-0{chunk_id + 1}"],
            symbols=[f"S{i}" for i in range(5)],
            enabled=True,
            intervention_fraction=0.01,
            progress_enabled=False,
        )
        chunks.append(({"frames": frames, "summary": {"j_lens": summary}}, 1))

    combined, summary = _combine_j_lens_frames_from_chunks(chunks)

    assert summary["status"] == "ok"
    assert summary["chunks_aggregated"] == 2
    assert combined["j_lens_transport"].height == 2 * 4 * 4
    assert combined["j_lens_date_readout"].height == 2 * 2
    assert combined["j_lens_stock_readout"].height == 2 * 5
    assert combined["j_lens_stock_readout"].get_column("active_dates").min() == 2
    assert combined["j_lens_completeness"].row(0, named=True)["transport_cell_coverage"] == pytest.approx(1.0)


def test_portfolio_j_lens_writes_all_diagnostic_plots(tmp_path: Path) -> None:
    torch.manual_seed(11)
    model = _TinyJLensePortfolio()
    frames, _, _ = _portfolio_j_lens(
        model,
        torch.randn(2, 3, 5, 3),
        torch.ones(2, 5, dtype=torch.bool),
        dates=["2026-01-01", "2026-01-02"],
        symbols=[f"S{i}" for i in range(5)],
        enabled=True,
        intervention_fraction=0.01,
        progress_enabled=False,
    )

    generated = _plot_all_explanation_figures(
        frames,
        {},
        tmp_path,
        plot_backend="matplotlib",
        strict_no_fallback=True,
        progress_enabled=False,
    )

    expected = {
        "plots/j_lens_layer_transport_strength.png",
        "plots/j_lens_transport_matrix_heatmap.png",
        "plots/j_lens_layer_stock_score_heatmap.png",
        "plots/j_lens_layer_date_heatmap.png",
        "plots/j_lens_linearization_faithfulness.png",
    }
    assert expected.issubset(set(generated))
    assert all((tmp_path / relative).stat().st_size > 0 for relative in expected)


class _BatchShapeSensitiveExplainModel(torch.nn.Module):
    def forward(self, x, mask, return_aux=None):
        scores = x.sum(dim=(1, 3)) + float(x.size(0)) * 0.01
        scores = scores.masked_fill(~mask, 0.0)
        return scores, scores, {}


class _CountingExplainModel(torch.nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.forward_calls = 0

    def forward(self, x, mask, return_aux=None):
        self.forward_calls += 1
        scores = x.sum(dim=(1, 3)).masked_fill(~mask, 0.0)
        return scores, scores, {}


def test_zero_identity_perturbations_preserve_grid_without_model_forward() -> None:
    x = torch.zeros(2, 3, 4, 5)
    mask = torch.ones(2, 4, dtype=torch.bool)
    model = _CountingExplainModel()
    base_weights = torch.zeros(2, 4)
    base_scores = torch.zeros(2, 4)

    feature_time, summary, diagnostics = _perturbation_importance(
        model,
        x,
        mask,
        base_weights,
        base_scores,
        [f"f{i}" for i in range(5)],
        progress_enabled=False,
    )

    assert model.forward_calls == 0
    assert feature_time.height == 3 * 5
    assert summary.height == 5
    assert feature_time.get_column("weight_abs_delta").max() == 0.0
    assert feature_time.get_column("score_abs_delta").max() == 0.0
    assert diagnostics["num_perturbations"] == 15
    assert diagnostics["forwarded_perturbations"] == 0
    assert diagnostics["zero_identity_perturbations"] == 15


class _BFloat16PerturbationModel(torch.nn.Module):
    def forward(self, x, mask, return_aux=None):
        del return_aux
        scores = x.sum(dim=(1, 3)).masked_fill(~mask, 0.0).to(torch.bfloat16)
        return scores, scores, {}


def test_perturbation_uses_batch_matched_baseline() -> None:
    x = torch.randn(1, 2, 3, 2)
    x[..., 1] = 0.0
    mask = torch.ones(1, 3, dtype=torch.bool)
    model = _BatchShapeSensitiveExplainModel()
    base_weights, base_scores, _ = _forward_outputs(model, x, mask)

    feature_time, _, diagnostics = _perturbation_importance(
        model,
        x,
        mask,
        base_weights,
        base_scores,
        ["signal", "already_zero"],
        batch_size=2,
        progress_enabled=False,
    )

    zero_rows = feature_time.filter(feature_time["feature"] == "already_zero")
    assert zero_rows["weight_abs_delta"].max() == pytest.approx(0.0)
    assert zero_rows["score_abs_delta"].max() == pytest.approx(0.0)
    assert diagnostics["batch_matched_baseline"] is True
    assert diagnostics["baseline_forward_batches"] == 1
    assert diagnostics["original_vs_matched_baseline_weight_abs_delta"] > 0.0


def test_perturbation_exports_bfloat16_outputs_as_float32() -> None:
    x = torch.randn(1, 2, 3, 2)
    mask = torch.ones(1, 3, dtype=torch.bool)
    model = _BFloat16PerturbationModel()
    base_weights, base_scores, _ = _forward_outputs(model, x, mask)

    feature_time, _, _ = _perturbation_importance(
        model,
        x,
        mask,
        base_weights,
        base_scores,
        ["f0", "f1"],
        batch_size=2,
        progress_enabled=False,
    )

    assert feature_time.height == 4
    assert np.isfinite(feature_time["score_abs_delta"].to_numpy()).all()


def test_daily_weight_symbols_supports_parquet_and_csv(tmp_path: Path) -> None:
    import polars as pl

    frame = pl.DataFrame({"date": ["2026-01-02"], "2330": [0.5], "0050": [-0.5]})
    parquet_path = tmp_path / "daily_weights.parquet"
    csv_path = tmp_path / "daily_weights.csv"
    frame.write_parquet(parquet_path)
    frame.write_csv(csv_path)

    assert _daily_weight_symbols(parquet_path) == ["2330", "0050"]
    assert _daily_weight_symbols(csv_path) == ["2330", "0050"]


def test_explainability_alignment_does_not_treat_position_capacity_as_universe(tmp_path: Path) -> None:
    import polars as pl

    symbols = ["0050", "2330", "2317"]
    panel = PanelData(
        dates=np.asarray(["2026-01-02"], dtype="datetime64[D]"),
        symbols=symbols,
        feature_names=["f0"],
        features=np.zeros((1, 3, 1), dtype=np.float32),
        returns_1d=np.zeros((1, 3), dtype=np.float32),
        tradable_mask=np.ones((1, 3), dtype=bool),
        alive_mask=np.ones((1, 3), dtype=bool),
        benchmark_returns=np.zeros(1, dtype=np.float32),
        close_prices=np.ones((1, 3), dtype=np.float32),
    )
    fold_dir = tmp_path / "fold_01"
    fold_dir.mkdir()
    checkpoint_path = fold_dir / "checkpoint_best.pt"
    torch.save({"model_state_dict": {"symbol_position": torch.zeros(1, 1, 2, 4)}}, checkpoint_path)
    pl.DataFrame({"date": ["2026-01-02"], **{symbol: [0.0] for symbol in symbols}}).write_parquet(
        fold_dir / "daily_weights.parquet"
    )

    aligned = _align_panel_to_checkpoint_universe(panel, tmp_path, 1, checkpoint_path)

    assert aligned is panel


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
        settings=ExplainabilitySettings(max_rows=rows, ig_steps=2, perturb=True),
        device=torch.device("cpu"),
    )

    assert output["summary"]["warnings"]
    assert output["summary"]["attribution_scope"] == "all_tradable_nonzero_positions_gross_weighted"
    assert not output["frames"]["feature_importance_gradient"].is_empty()
    assert "top_decisions" not in output["frames"]
    assert len(output["frames"]["decision_inventory"]) == rows * symbols
    completeness = output["frames"]["explainability_completeness"].row(0, named=True)
    assert completeness["decision_inventory_rows"] == rows * symbols
    assert completeness["position_count_coverage"] == pytest.approx(1.0)
    assert completeness["gross_exposure_coverage"] == pytest.approx(1.0)
    assert completeness["gradient_feature_time_cells"] == lookback * features

    out_dir = tmp_path / "explain"
    shutil.rmtree(out_dir, ignore_errors=True)
    write_explanation_outputs(output, out_dir, metadata={"model_name": "toy"})
    assert (out_dir / "summary.json").exists()
    assert (out_dir / "report.md").exists()
    assert (out_dir / "paper_explainability_report.md").exists()
    assert (out_dir / "comprehensive_explainability_report.md").exists()
    assert (out_dir / "plot_validation.json").exists()
    assert (out_dir / "paper_explainability_summary.json").exists()
    assert (out_dir / "feature_importance_gradient.csv").exists()
    assert (out_dir / "decision_inventory.csv").exists()
    assert (out_dir / "paper_tables" / "global_feature_attribution.csv").exists()
    assert (out_dir / "paper_tables" / "feature_attribution_coverage_curve.csv").exists()
    assert (out_dir / "paper_tables" / "explainability_completeness.csv").exists()
    assert (out_dir / "paper_tables" / "trust_checks.csv").exists()
    assert (out_dir / "paper_tables" / "lookback_consistency.csv").exists()
    assert (out_dir / "paper_tables" / "method_agreement.csv").exists()
    assert (out_dir / "paper_tables" / "gross_pre_fee_risk_diagnostic.csv").exists()
    assert (out_dir / "stock_contributions.parquet").exists()
    assert (out_dir / "plots_paper" / "feature_time_gradient_grad_x_input_abs_heatmap.png").exists()
    assert (out_dir / "plots_paper" / "feature_attribution_coverage_curve.png").exists()
    assert (out_dir / "plots_paper" / "portfolio_exposure_coverage_curve.png").exists()
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["plots_generated"]
    assert summary["paper_plots"]
    assert summary["plot_validation"]["failed"] == 0
    assert list((out_dir / "plots").glob("*.png"))
    validation = json.loads((out_dir / "plot_validation.json").read_text(encoding="utf-8"))
    assert validation
    assert all(entry["status"] == "ok" for entry in validation)
    comprehensive_report = (out_dir / "comprehensive_explainability_report.md").read_text(encoding="utf-8")
    assert "完整視覺證據" in comprehensive_report
    assert "衡量內容" in comprehensive_report
    assert "解讀方式" in comprehensive_report
    assert "可疑訊號" in comprehensive_report
    assert "![" in comprehensive_report


def test_feature_correlations_zero_variance_without_runtime_warning() -> None:
    x = torch.ones(3, 2, 4, 2)
    scores = torch.ones(3, 4)
    weights = torch.full((3, 4), 0.25)
    mask = torch.ones(3, 4, dtype=torch.bool)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        frame = _feature_correlations(x, scores, weights, mask, ["constant_a", "constant_b"])

    runtime_messages = [str(item.message) for item in caught if issubclass(item.category, RuntimeWarning)]
    assert not any("invalid value encountered in divide" in message for message in runtime_messages)
    assert frame["score_corr"].to_list() == [0.0, 0.0, 0.0, 0.0]
    assert frame["weight_corr"].to_list() == [0.0, 0.0, 0.0, 0.0]


def test_streaming_global_diagnostics_match_materialized_algorithms() -> None:
    torch.manual_seed(113)
    x = torch.randn(8, 3, 7, 3)
    scores = torch.randn(8, 7)
    weights = torch.randn(8, 7)
    mask = torch.rand(8, 7) > 0.1
    feature_names = ["f0", "f1", "f2"]
    chunks = [x[:3], x[3:6], x[6:]]

    corr_full = _feature_correlations(x, scores, weights, mask, feature_names).sort(["source", "feature"])
    corr_stream = _feature_correlations_chunked(
        chunks, scores, weights, mask, feature_names
    ).sort(["source", "feature"])
    np.testing.assert_allclose(
        corr_stream.select(["score_corr", "weight_corr"]).to_numpy(),
        corr_full.select(["score_corr", "weight_corr"]).to_numpy(),
        rtol=1e-6,
        atol=1e-7,
    )

    shap_full, components_full, info_full, _ = _score_head_surrogate_shap(
        x,
        scores,
        mask,
        feature_names,
        enabled=True,
        mode="score_head_surrogate",
        progress_enabled=False,
    )
    shap_stream, components_stream, info_stream, _ = _score_head_surrogate_shap_chunked(
        chunks,
        scores,
        mask,
        feature_names,
        enabled=True,
        mode="score_head_surrogate",
        progress_enabled=False,
    )
    components_full = components_full.sort(["source", "feature"])
    components_stream = components_stream.sort(["source", "feature"])
    np.testing.assert_allclose(
        components_stream.select(["shap_abs", "surrogate_coef"]).to_numpy(),
        components_full.select(["shap_abs", "surrogate_coef"]).to_numpy(),
        rtol=1e-5,
        atol=1e-7,
    )
    assert shap_stream.height == shap_full.height == len(feature_names)
    assert info_stream["valid_rows"] == info_full["valid_rows"]
    assert info_stream["surrogate_r2"] == pytest.approx(info_full["surrogate_r2"], rel=1e-6)


def test_streaming_umap_capture_matches_global_flat_sample() -> None:
    full = torch.arange(6 * 4 * 3, dtype=torch.float32).reshape(6, 4, 3)
    max_points = 8
    captured = []
    for start, end in ((0, 2), (2, 5), (5, 6)):
        samples, _ = _streaming_aux_umap_samples(
            {"z_stock": full[start:end]},
            global_rows=6,
            row_offset=start,
            max_points=max_points,
        )
        captured.append(samples["z_stock"])
    values = torch.cat([item["values"] for item in captured], dim=0)
    flat_indices = torch.cat([item["flat_indices"] for item in captured], dim=0)
    expected_indices = _evenly_spaced_sample_indices(
        6 * 4, max_points, device=torch.device("cpu")
    )
    expected_values = full.reshape(-1, 3).index_select(0, expected_indices)
    torch.testing.assert_close(flat_indices, expected_indices)
    torch.testing.assert_close(values, expected_values)


def test_streaming_decision_inventory_matches_full_table(tmp_path: Path) -> None:
    torch.manual_seed(127)
    weights = torch.randn(5, 4)
    scores = torch.randn(5, 4)
    returns = torch.randn(5, 4)
    mask = torch.rand(5, 4) > 0.2
    selected = mask & weights.ne(0.0)
    dates = [f"2026-01-{idx + 1:02d}" for idx in range(5)]
    symbols = [f"S{idx}" for idx in range(4)]
    output = tmp_path / "decision_inventory.csv"
    written = _write_decision_inventory_streaming(
        {
            "weights": weights,
            "scores": scores,
            "returns": returns,
            "mask": mask,
            "selected": selected,
            "dates": dates,
            "symbols": symbols,
        },
        output,
        row_chunk_size=2,
    )
    streamed = pl.read_csv(output)
    assert written == streamed.height == weights.numel()
    assert streamed.get_column("date").to_list() == np.repeat(dates, len(symbols)).tolist()
    assert streamed.get_column("symbol").to_list() == np.tile(symbols, len(dates)).tolist()


def test_explainability_chunked_attribution_matches_serial_with_fewer_forwards() -> None:
    torch.manual_seed(11)
    rows, lookback, symbols, features = 3, 4, 5, 3
    batch = {
        "x": torch.randn(rows, lookback, symbols, features),
        "future_log_returns": torch.randn(rows, symbols) * 0.01,
        "tradable_mask": torch.ones(rows, symbols, dtype=torch.bool),
    }
    common = dict(
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
    diagnostics = chunked["summary"]["perturb_diagnostics"]
    assert diagnostics["elapsed_s"] >= 0.0
    assert diagnostics["perturbations_per_s"] > 0.0


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
    assert "Lookback 警告" in report
    assert "衡量內容" in report
    assert "解讀方式" in report
    assert "可疑訊號" in report

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
        fold_dir = table_dir.parent
        (fold_dir / "summary.json").write_text(
            json.dumps(
                {
                    "portfolio": {
                        "mean_gross": 1.0,
                        "mean_abs_net": 0.4 + shift,
                        "mean_long_gross": 0.7 + shift / 2,
                        "mean_short_gross": 0.3 - shift / 2,
                        "mean_turnover_proxy": 1.1 + shift,
                        "max_abs_weight_mean": 0.2 + shift,
                        "max_abs_weight_max": 0.5 + shift,
                        "mean_daily_log_return": 0.001,
                    },
                    "metadata": {
                        "date_start": f"202{fold_id}-01-01",
                        "date_end": f"202{fold_id}-12-31",
                        "sample_rows": 200,
                        "sampled_date_coverage": 1.0,
                    },
                    "shap_info": {"surrogate_r2": 0.85 - shift, "valid_rows": 1000},
                    "warnings": ["Turnover proxy is high; strategy may be relying on unstable daily flips."],
                }
            ),
            encoding="utf-8",
        )
        (fold_dir / "plot_validation.json").write_text("[]", encoding="utf-8")

    output = write_fold_stability_outputs(root)
    assert output is not None
    assert (output / "paper_tables" / "fold_feature_stability.csv").exists()
    assert (output / "plots_paper" / "fold_stability_feature_share.png").exists()
    assert (output / "paper_fold_stability_report.md").exists()
    assert (root / "comprehensive_all_folds_report.md").exists()
    assert (root / "plot_validation_all_folds.json").exists()
    report = (root / "comprehensive_all_folds_report.md").read_text(encoding="utf-8")
    assert "不是將各 Fold 報告或圖片串接" in report
    assert "完整 2 特徵" in report
    assert "Fold 1–2 漂移" in report
    assert "Fold 1–21" not in report
    assert "21 folds" not in report
    assert "fold_01_test/plots/" not in report
    assert report.count("comprehensive_explainability_report.md") == 2
    assert (root / "tables_cross_fold" / "cross_fold_portfolio_and_shap.csv").exists()
    assert (root / "plots_cross_fold" / "cross_fold_portfolio_diagnostics.png").exists()


def test_cross_fold_source_keeps_mixed_symbol_codes_as_strings(tmp_path: Path) -> None:
    source_path = tmp_path / "j_lens_stock_readout.csv"
    pl.DataFrame(
        {
            "layer": ["stock"] * 102,
            "symbol": [f"{index:06d}" for index in range(101)] + ["00637L"],
            "mean_abs": [0.1] * 102,
            "signed_mean": [0.05] * 102,
        }
    ).write_csv(source_path)

    spec = _cross_fold_figure_spec("plots/j_lens_layer_stock_score_heatmap.png")
    assert spec is not None
    frame = _read_cross_fold_source_table(source_path, spec)

    assert frame.schema["symbol"] == pl.String
    assert frame.get_column("symbol").head(1).item() == "000000"
    assert frame.get_column("symbol").tail(1).item() == "00637L"


def test_method_agreement_active_union_avoids_shared_zero_tie_inflation() -> None:
    import polars as pl

    table = pl.DataFrame(
        {
            "feature": ["a", "b", "c", "d", "e", "f"],
            "gradient_share": [0.6, 0.3, 0.1, 0.0, 0.0, 0.0],
            "integrated_gradients_share": [0.1, 0.3, 0.6, 0.0, 0.0, 0.0],
        }
    )
    agreement = _method_agreement_table(table)
    active = agreement.filter(pl.col("comparison_scope") == "active_union").row(0, named=True)
    full = agreement.filter(pl.col("comparison_scope") == "all_features_including_zero_ties").row(0, named=True)
    assert active["features_compared"] == 3
    assert full["features_compared"] == 6
    assert active["spearman_rank_correlation"] < full["spearman_rank_correlation"]


def test_aux_collapse_scope_excludes_portfolio_accounting_outputs() -> None:
    import polars as pl

    frame = pl.DataFrame(
        {
            "name": ["implicit_cash_weight", "market_tokens", "stock_embedding"],
            "zero_fraction": [1.0, 0.1, 0.2],
        }
    )
    scoped = _representation_aux_summary(frame)
    assert scoped.get_column("name").to_list() == ["market_tokens", "stock_embedding"]


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
        max_rows=2,
        ig_steps=0,
        perturb=False,
        report_style="none",
        standard_plots=False,
        shap_enabled=False,
        regime_analysis=False,
        fold_stability=False,
        umap_enabled=False,
    )

    output = run_loaded_model_explanation(
        config=config,
        panel=panel,
        fold=fold,
        model=ToyExplainableModel(features),
        checkpoint_path=tmp_path / "fold_01" / "checkpoint_best.pt",
        output_dir=tmp_path,
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
    assert "cross_asset_s" not in timing
    assert "cross_asset_summary" not in timing
    assert not (output / "abstract_cross_asset_transmission").exists()
