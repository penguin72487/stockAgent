from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from datetime import timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from plot_epoch_curves import (
    _load_curve,
    _sample_rows,
    _write_parquet_table_as_csv,
    export_report_csvs,
)
from stockagent import explainability as explainability_module
from stockagent.explainability import (
    ExplainabilitySettings,
    _adapt_dynamic_symbol_position_state,
    _auto_explain_row_chunk_size,
    _cuda_oom_fallback_settings,
    _discover_market_runs,
    explain_batch_row_chunked,
    parse_args,
    settings_from_training_config,
)
from stockagent.explainability_cross_asset import CrossAssetTransmissionSettings, _auto_row_chunk_size
from stockagent.data.walkforward import WalkForwardFold


def test_explainability_distributed_init_creates_long_timeout_gloo_barrier_group(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}
    barrier_group = object()
    monkeypatch.setenv("WORLD_SIZE", "2")
    monkeypatch.setenv("LOCAL_RANK", "0")
    monkeypatch.setenv("STOCKAGENT_EXPLAINABILITY_BARRIER_TIMEOUT_SECONDS", "4096")
    monkeypatch.setattr(torch.distributed, "is_available", lambda: True)
    monkeypatch.setattr(torch.distributed, "is_initialized", lambda: False)
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 2)
    monkeypatch.setattr(torch.cuda, "set_device", lambda rank: calls.setdefault("device", rank))
    monkeypatch.setattr(
        torch.distributed,
        "init_process_group",
        lambda **kwargs: calls.setdefault("init", kwargs),
    )
    monkeypatch.setattr(
        torch.distributed,
        "new_group",
        lambda **kwargs: calls.setdefault("new_group", kwargs) and barrier_group,
    )
    monkeypatch.setattr(explainability_module.atexit, "register", lambda fn: calls.setdefault("atexit", fn))
    monkeypatch.setattr(explainability_module, "_EXPLAINABILITY_BARRIER_GROUP", None)

    assert explainability_module._initialize_explainability_process_group() is True
    assert calls["device"] == 0
    assert calls["init"] == {"backend": "nccl", "timeout": timedelta(seconds=4096)}
    assert calls["new_group"] == {"backend": "gloo", "timeout": timedelta(seconds=4096)}
    assert explainability_module._EXPLAINABILITY_BARRIER_GROUP is barrier_group


def test_explainability_distributed_barrier_prefers_cpu_group(monkeypatch) -> None:
    barrier_group = object()
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(torch.distributed, "is_available", lambda: True)
    monkeypatch.setattr(torch.distributed, "is_initialized", lambda: True)
    monkeypatch.setattr(torch.distributed, "barrier", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        explainability_module,
        "_EXPLAINABILITY_BARRIER_GROUP",
        barrier_group,
    )

    explainability_module._distributed_barrier()

    assert calls == [{"group": barrier_group}]


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


@pytest.mark.parametrize("suffix", [".csv", ".parquet"])
def test_load_curve_replaces_float_nan_without_touching_strings(
    tmp_path: Path, suffix: str
) -> None:
    curve_path = tmp_path / f"epoch_curve{suffix}"
    table = pa.table(
        {
            "epoch": [1, 2],
            "phase": ["train", "validation"],
            "loss": [1.0, float("nan")],
        }
    )
    if suffix == ".csv":
        import pyarrow.csv as pacsv

        pacsv.write_csv(table, curve_path)
    else:
        pq.write_table(table, curve_path)

    rows = _load_curve(curve_path)

    assert [row["phase"] for row in rows] == ["train", "validation"]
    assert rows[1]["loss"] is None


def test_epoch_curve_interval_one_preserves_every_recorded_epoch() -> None:
    rows = [
        {"epoch": epoch, "train_loss": 1.0 / epoch}
        for epoch in range(1, 1001)
    ]

    assert _sample_rows(rows, interval=1) == rows


def test_explainability_reads_compacted_universe_from_parquet_weight_schema(tmp_path: Path) -> None:
    fold_dir = tmp_path / "fold_21"
    fold_dir.mkdir()
    path = fold_dir / "daily_weights.parquet"
    pq.write_table(pa.table({"date": ["2026-01-01"], "2330": [0.1], "0050": [-0.1]}), path)

    assert explainability_module._daily_weight_table_path(fold_dir) == path
    assert explainability_module._daily_weight_symbols(path) == ["2330", "0050"]


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


def test_explain_model_cli_has_no_cross_asset_execution_entrypoint() -> None:
    args = parse_args([])

    assert args.config is None
    assert args.progress is True
    assert args.market_artifacts_root == Path("artifacts/markets")
    assert args.market_config_root == Path("configs/markets")
    assert args.ig_steps == 8
    assert args.perturb is True
    assert args.perturb_max_auto_batch_size == 48
    assert args.perturb_max_input_elements == 576_000_000
    assert args.counterfactual_compile is True
    assert args.plots is True
    assert args.report_style == "paper"
    assert args.standard_plots is True
    assert args.shap is True
    assert args.regime_analysis is True
    assert args.fold_stability is True
    assert args.umap is True
    assert args.max_rows == 0
    assert not hasattr(args, "split")
    assert not hasattr(args, "first_test_year_only")
    assert args.umap_max_points == 0
    assert not hasattr(args, "top_k")
    assert not hasattr(args, "case_study_top_k")
    assert not any(name.startswith("cross_asset") for name in vars(args))
    assert args.strict_no_fallback is True

    assert parse_args(["--no-progress"]).progress is False
    with pytest.raises(SystemExit):
        parse_args(["--split", "train"])
    with pytest.raises(SystemExit):
        parse_args(["--all-test-years"])
    with pytest.raises(SystemExit):
        parse_args(["--no-first-test-year-only"])
    with pytest.raises(SystemExit):
        parse_args(["--cross-asset"])


def test_explain_model_cli_discovers_market_artifacts(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "artifacts" / "markets"
    config_root = tmp_path / "configs" / "markets"
    (artifacts_root / "tw" / "fold_25").mkdir(parents=True)
    (artifacts_root / "tw" / "fold_25" / "checkpoint_best.pt").write_bytes(b"")
    (artifacts_root / "scratch").mkdir(parents=True)
    config_root.mkdir(parents=True)
    (config_root / "tw.yaml").write_text("runner:\n  output_dir: artifacts/markets/tw\n", encoding="utf-8")

    runs = _discover_market_runs(artifacts_root, config_root)

    assert len(runs) == 1
    assert runs[0].market == "tw"
    assert runs[0].config_path == config_root / "tw.yaml"
    assert runs[0].output_dir == artifacts_root / "tw"


def test_dynamic_symbol_position_checkpoint_state_is_resized_for_explainability() -> None:
    class DynamicSymbolModel(torch.nn.Module):
        allow_dynamic_symbols = True

        def __init__(self) -> None:
            super().__init__()
            self.symbol_position = torch.nn.Parameter(torch.zeros(1, 1, 5, 3))

    model = DynamicSymbolModel()
    checkpoint_position = torch.arange(12, dtype=torch.float32).reshape(1, 1, 4, 3)

    adapted, adjustments = _adapt_dynamic_symbol_position_state(
        model,
        {"symbol_position": checkpoint_position},
        strict=False,
    )

    assert adjustments == [
        {
            "key": "symbol_position",
            "checkpoint_shape": [1, 1, 4, 3],
            "model_shape": [1, 1, 5, 3],
            "copied_symbols": 4,
        }
    ]
    assert adapted["symbol_position"].shape == (1, 1, 5, 3)
    torch.testing.assert_close(adapted["symbol_position"][:, :, :4, :], checkpoint_position)
    torch.testing.assert_close(adapted["symbol_position"][:, :, 4:, :], torch.zeros(1, 1, 1, 3))


def test_training_explainability_settings_use_throughput_defaults() -> None:
    settings = settings_from_training_config(
        SimpleNamespace(
            # These legacy TrainingConfig fields remain loadable for checkpoint
            # compatibility but must no longer create a runtime execution path.
            explain_cross_asset_enabled=True,
            explain_cross_asset_max_sources=2,
        )
    )

    assert settings.ig_steps == 0
    assert settings.ig_batch_size == 1
    assert settings.perturb is False
    assert settings.perturb_batch_size == 1
    assert settings.perturb_max_auto_batch_size == 1
    assert settings.perturb_max_input_elements == 8_000_000
    assert settings.report_style == "none"
    assert settings.standard_plots is False
    assert settings.shap_enabled is False
    assert settings.regime_analysis is False
    assert settings.fold_stability is False
    assert settings.umap_enabled is False
    assert not any(name.startswith("cross_asset") for name in settings.__slots__)


def test_training_fold_explainability_delegates_to_shared_runner(monkeypatch, tmp_path: Path) -> None:
    from stockagent.training import trainer as trainer_module

    captured: dict[str, object] = {}

    def fake_run_loaded_model_explanation(**kwargs):
        captured.update(kwargs)
        return tmp_path / "explainability" / "fold_01_test"

    monkeypatch.setattr(explainability_module, "run_loaded_model_explanation", fake_run_loaded_model_explanation)
    fold = WalkForwardFold(
        fold_id=1,
        train_indices=torch.arange(2).numpy(),
        val_indices=torch.arange(2, 3).numpy(),
        test_indices=torch.arange(3, 5).numpy(),
        train_years=[2020],
        val_years=[2021],
        test_years=[2022],
    )
    config = SimpleNamespace(
        training=SimpleNamespace(
            explain_after_each_fold=True,
            explain_write_plots=False,
            explain_fold_stability=False,
        )
    )
    model = torch.nn.Linear(1, 1)

    output = trainer_module._run_fold_explainability(
        model=model,
        panel=SimpleNamespace(),
        config=config,
        output_path=tmp_path,
        fold=fold,
        device=torch.device("cpu"),
        checkpoint_path=tmp_path / "fold_01" / "checkpoint_best.pt",
    )

    assert output == tmp_path / "explainability" / "fold_01_test"
    assert captured["model"] is model
    assert captured["fold"] is fold
    assert "split" not in captured
    assert captured["write_plots"] is False
    assert captured["timing_file_name"] == "train_explainability_timing.json"
    settings = captured["settings"]
    assert isinstance(settings, ExplainabilitySettings)
    assert settings.ig_steps == 0
    assert settings.perturb is False
