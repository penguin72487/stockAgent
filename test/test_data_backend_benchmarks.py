from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _load_benchmark_module():
    root = Path(__file__).resolve().parents[1]
    path = root / "scripts" / "benchmark_data_backends.py"
    spec = importlib.util.spec_from_file_location("benchmark_data_backends", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_symbol(path: Path, offset: float) -> None:
    rows = 8
    dates = pd.date_range("2024-01-01", periods=rows, freq="D")
    close = np.linspace(10.0 + offset, 11.0 + offset, rows)
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": close * 0.99,
            "max": close * 1.02,
            "min": close * 0.98,
            "close": close,
            "adjclose": close,
            "Trading_Volume": np.linspace(1000.0, 2000.0, rows),
        }
    )
    frame.to_parquet(path, index=False)


def _write_symbol_nan_range(path: Path, offset: float) -> None:
    rows = 8
    dates = pd.date_range("2024-01-01", periods=rows, freq="D")
    close = np.linspace(20.0 + offset, 21.0 + offset, rows)
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": close * 0.99,
            "max": [np.nan] * rows,
            "min": [np.nan] * rows,
            "close": close,
            "adjclose": close,
            "Trading_Volume": np.linspace(1000.0, 2000.0, rows),
        }
    )
    frame.to_parquet(path, index=False)


def test_scan_data_processing_hotspots_finds_panel_and_table_outputs() -> None:
    module = _load_benchmark_module()
    root = Path(__file__).resolve().parents[1]

    hotspots = module.scan_data_processing_hotspots(root)
    locations = {(item.category, item.path) for item in hotspots}

    assert ("panel_ingest", "stockagent/data/panel.py") in locations
    assert ("table_output", "stockagent/training/trainer.py") in locations


def test_feature_prep_benchmark_runs_on_synthetic_parquet(tmp_path: Path) -> None:
    module = _load_benchmark_module()
    _write_symbol(tmp_path / "AAA_features.parquet", 0.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 1.0)

    backends = ["pandas", "pyarrow"]
    if module._module_available("polars"):
        backends.append("polars")
    if module._module_available("duckdb"):
        backends.append("duckdb")

    results = module.benchmark_feature_prep(
        tmp_path,
        backends=backends,
        max_symbols=2,
        repeat=1,
    )
    successful = {result.backend: result for result in results if result.error is None}

    assert set(successful) == set(backends)
    for result in successful.values():
        assert result.available
        assert result.workload == "feature_prep"
        assert result.files == 2
        assert result.rows == 16
        assert result.elapsed_s is not None and result.elapsed_s >= 0.0
        assert result.rows_per_s is not None and result.rows_per_s > 0.0
        assert result.checksum is not None


def test_pyarrow_panel_backend_matches_pandas_on_synthetic_parquet(tmp_path: Path) -> None:
    module = _load_benchmark_module()
    from stockagent.data.panel import build_panel

    if not module._module_available("pyarrow"):
        return

    _write_symbol(tmp_path / "AAA_features.parquet", 0.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 1.0)

    pandas_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="pandas",
        panel_load_workers=1,
    )
    pyarrow_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="pyarrow",
        panel_load_workers=1,
    )

    assert pyarrow_panel.symbols == pandas_panel.symbols
    assert pyarrow_panel.feature_names == pandas_panel.feature_names
    assert np.array_equal(pyarrow_panel.dates, pandas_panel.dates)
    assert np.allclose(pyarrow_panel.features, pandas_panel.features, equal_nan=True)
    assert np.allclose(pyarrow_panel.returns_1d, pandas_panel.returns_1d, equal_nan=True)
    assert np.array_equal(pyarrow_panel.tradable_mask, pandas_panel.tradable_mask)
    assert np.array_equal(pyarrow_panel.alive_mask, pandas_panel.alive_mask)
    assert np.allclose(pyarrow_panel.benchmark_returns, pandas_panel.benchmark_returns, equal_nan=True)


def test_panel_build_benchmark_runs_duckdb_on_synthetic_parquet(tmp_path: Path) -> None:
    module = _load_benchmark_module()
    if not module._module_available("duckdb"):
        return

    _write_symbol(tmp_path / "AAA_features.parquet", 0.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 1.0)

    results = module.benchmark_panel_build(
        tmp_path,
        backends=["duckdb"],
        max_symbols=2,
        panel_load_workers=1,
        benchmark_name="universe_average_return",
    )

    assert len(results) == 1
    result = results[0]
    assert result.backend == "duckdb"
    assert result.error is None
    assert result.available
    assert result.files == 2
    assert result.rows == 16
    assert result.elapsed_s is not None and result.elapsed_s >= 0.0
    assert result.checksum is not None


def test_duckdb_panel_builder_matches_pandas_on_synthetic_parquet(tmp_path: Path) -> None:
    module = _load_benchmark_module()
    if not module._module_available("duckdb"):
        return
    from stockagent.data.panel import build_panel

    _write_symbol(tmp_path / "AAA_features.parquet", 0.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 1.0)
    _write_symbol_nan_range(tmp_path / "CCC_features.parquet", 2.0)

    pandas_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="pandas",
        panel_load_workers=1,
    )
    duckdb_panel = module._build_panel_duckdb(
        tmp_path,
        sorted(tmp_path.glob("*_features.parquet")),
        benchmark_name="universe_average_return",
        threads=1,
    )

    assert duckdb_panel.symbols == pandas_panel.symbols
    assert duckdb_panel.feature_names == pandas_panel.feature_names
    assert np.array_equal(duckdb_panel.dates, pandas_panel.dates)
    assert np.allclose(duckdb_panel.features, pandas_panel.features, equal_nan=True, atol=1e-6)
    assert np.allclose(duckdb_panel.returns_1d, pandas_panel.returns_1d, equal_nan=True, atol=1e-7)
    assert np.array_equal(duckdb_panel.tradable_mask, pandas_panel.tradable_mask)
    assert np.array_equal(duckdb_panel.alive_mask, pandas_panel.alive_mask)
    assert np.allclose(duckdb_panel.benchmark_returns, pandas_panel.benchmark_returns, equal_nan=True, atol=1e-7)


def test_runtime_duckdb_panel_backend_matches_pandas(tmp_path: Path) -> None:
    module = _load_benchmark_module()
    if not module._module_available("duckdb"):
        return
    from stockagent.data.panel import build_panel

    _write_symbol(tmp_path / "AAA_features.parquet", 0.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 1.0)
    _write_symbol_nan_range(tmp_path / "CCC_features.parquet", 2.0)

    pandas_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="pandas",
        panel_load_workers=1,
    )
    duckdb_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="duckdb",
        panel_load_workers=1,
    )

    assert duckdb_panel.symbols == pandas_panel.symbols
    assert duckdb_panel.feature_names == pandas_panel.feature_names
    assert np.array_equal(duckdb_panel.dates, pandas_panel.dates)
    assert np.allclose(duckdb_panel.features, pandas_panel.features, equal_nan=True, atol=1e-6)
    assert np.allclose(duckdb_panel.returns_1d, pandas_panel.returns_1d, equal_nan=True, atol=1e-7)
    assert np.array_equal(duckdb_panel.tradable_mask, pandas_panel.tradable_mask)


def test_wide_write_benchmark_reports_fastest_backend() -> None:
    module = _load_benchmark_module()
    backends = ["pandas", "pyarrow"]
    if module._module_available("polars"):
        backends.append("polars")
    if module._module_available("duckdb"):
        backends.append("duckdb")

    results = module.benchmark_wide_write(
        backends=backends,
        rows=8,
        symbols=6,
        repeat=1,
        seed=7,
    )
    payload = {"results": [module.asdict(result) for result in results]}
    fastest = module._fastest(results)

    assert len(results) == len(backends)
    assert fastest["wide_parquet_write"] in backends
    for result in results:
        assert result.error is None
        assert result.output_bytes is not None and result.output_bytes > 0
    assert payload["results"][0]["workload"] == "wide_parquet_write"
