from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from plot_epoch_curves import _write_parquet_table_as_csv, export_report_csvs
from stockagent import explainability as explainability_module
from stockagent.explainability import (
    ExplainabilitySettings,
    _auto_explain_row_chunk_size,
    _cuda_oom_fallback_settings,
    explain_batch_row_chunked,
    parse_args,
)
from stockagent.explainability_cross_asset import CrossAssetTransmissionSettings, _auto_row_chunk_size


def test_streaming_parquet_csv_export_handles_nested_batches(tmp_path: Path) -> None:
    parquet_path = tmp_path / "holdings.parquet"
    csv_path = tmp_path / "holdings.csv"
    table = pa.table(
        {
            "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "symbol": ["A", "B", "C"],
            "nested": [[1, 2], [3], None],
        }
    )
    pq.write_table(table, parquet_path, row_group_size=1)

    _write_parquet_table_as_csv(parquet_path, csv_path, batch_size=1)

    text = csv_path.read_text(encoding="utf-8")
    assert '"date","symbol","nested"' in text
    assert '"[1, 2]"' in text
    assert '"[3]"' in text


def test_export_report_csvs_uses_same_name_outputs(tmp_path: Path) -> None:
    fold_dir = tmp_path / "fold_25"
    fold_dir.mkdir()
    pq.write_table(pa.table({"value": [1, 2, 3]}), fold_dir / "daily_weights.parquet", row_group_size=1)

    result = export_report_csvs(tmp_path, batch_size=1, quiet=True)

    assert result["candidates"] == 1
    assert result["written"] == 1
    assert (fold_dir / "daily_weights.csv").exists()


def test_cross_asset_full_universe_row_chunk_is_single_row() -> None:
    row_chunk, info = _auto_row_chunk_size(
        n_rows=32,
        n_symbols=16_808,
        settings=CrossAssetTransmissionSettings(source_chunk_size=2, max_repeated_rows=8),
    )

    assert row_chunk == 1
    assert info["reason"] == "repeated_row_budget"


def test_main_explain_full_universe_cuda_row_chunk_is_single_row(monkeypatch) -> None:
    batch = {
        "x": torch.zeros(32, 2, 16_808, 2),
        "future_log_returns": torch.zeros(32, 16_808),
        "tradable_mask": torch.ones(32, 16_808, dtype=torch.bool),
    }
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(
        explainability_module,
        "_cuda_mem_get_info",
        lambda device: (14 * 1024**3, 16 * 1024**3),
    )

    row_chunk, info = _auto_explain_row_chunk_size(
        batch,
        ExplainabilitySettings(ig_steps=8, perturb=True),
        torch.device("cuda"),
    )

    assert row_chunk == 1
    assert info["reason"] == "cuda_budget"


def test_cuda_oom_fallback_disables_high_vram_explainability_steps() -> None:
    settings = ExplainabilitySettings(
        ig_steps=8,
        perturb=True,
        perturb_max_auto_batch_size=16,
        perturb_max_input_elements=96_000_000,
        umap_enabled=True,
        umap_max_points=10000,
    )

    fallback = _cuda_oom_fallback_settings(settings)

    assert fallback is not None
    assert fallback.ig_steps == 0
    assert fallback.perturb is False
    assert fallback.perturb_max_auto_batch_size == 1
    assert fallback.perturb_max_input_elements == 8_000_000
    assert fallback.umap_enabled is False


def test_strict_no_fallback_raises_on_explainability_cuda_oom(monkeypatch) -> None:
    batch = {
        "x": torch.zeros(2, 1, 3, 2),
        "future_log_returns": torch.zeros(2, 3),
        "tradable_mask": torch.ones(2, 3, dtype=torch.bool),
    }

    def raise_cuda_oom(*args, **kwargs):
        raise RuntimeError("CUDA out of memory")

    monkeypatch.setattr(explainability_module, "explain_batch", raise_cuda_oom)

    with pytest.raises(RuntimeError, match="strict_no_fallback=true"):
        explain_batch_row_chunked(
            torch.nn.Linear(1, 1),
            batch,
            feature_names=["f0", "f1"],
            symbols=["A", "B", "C"],
            dates=["2026-01-01", "2026-01-02"],
            settings=ExplainabilitySettings(ig_steps=8, perturb=True, strict_no_fallback=True),
            device=torch.device("cpu"),
        )


def test_explain_model_cli_defaults_are_throughput_oriented() -> None:
    args = parse_args([])

    assert args.ig_steps == 0
    assert args.perturb is False
    assert args.perturb_max_auto_batch_size == 1
    assert args.perturb_max_input_elements == 8_000_000
    assert args.plots is False
    assert args.report_style == "none"
    assert args.standard_plots is False
    assert args.shap is False
    assert args.regime_analysis is False
    assert args.fold_stability is False
    assert args.umap is False
    assert args.cross_asset is False
