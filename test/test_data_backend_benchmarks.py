from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.data.panel import build_panel


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
    dates = np.arange(np.datetime64("2024-01-01"), np.datetime64("2024-01-01") + rows)
    close = np.linspace(10.0 + offset, 11.0 + offset, rows)
    table = pa.table(
        {
            "date": pa.array(dates),
            "open": pa.array(close * 0.99),
            "max": pa.array(close * 1.02),
            "min": pa.array(close * 0.98),
            "close": pa.array(close),
            "adjclose": pa.array(close),
            "Trading_Volume": pa.array(np.linspace(1000.0, 2000.0, rows)),
        }
    )
    pq.write_table(table, path)


def _write_symbol_without_volume(path: Path, offset: float) -> None:
    rows = 4
    dates = np.arange(np.datetime64("2024-01-01"), np.datetime64("2024-01-01") + rows)
    close = np.linspace(10.0 + offset, 11.0 + offset, rows)
    table = pa.table(
        {
            "date": pa.array(dates),
            "open": pa.array(close * 0.99),
            "max": pa.array(close * 1.02),
            "min": pa.array(close * 0.98),
            "close": pa.array(close),
            "adjclose": pa.array(close),
        }
    )
    pq.write_table(table, path)


def _write_symbol_nan_range(path: Path, offset: float) -> None:
    rows = 8
    dates = np.arange(np.datetime64("2024-01-01"), np.datetime64("2024-01-01") + rows)
    close = np.linspace(20.0 + offset, 21.0 + offset, rows)
    table = pa.table(
        {
            "date": pa.array(dates),
            "open": pa.array(close * 0.99),
            "max": pa.array([np.nan] * rows),
            "min": pa.array([np.nan] * rows),
            "close": pa.array(close),
            "adjclose": pa.array(close),
            "Trading_Volume": pa.array(np.linspace(1000.0, 2000.0, rows)),
        }
    )
    pq.write_table(table, path)


def _write_tw_limit_symbol(path: Path) -> None:
    close = np.asarray([100.0, 110.0, 99.0, 100.0], dtype=np.float64)
    table = pa.table(
        {
            "date": pa.array(np.arange(np.datetime64("2024-01-01"), np.datetime64("2024-01-01") + len(close))),
            "open": pa.array(close),
            "max": pa.array(close),
            "min": pa.array(close),
            "close": pa.array(close),
            "adjclose": pa.array(close),
            "Trading_Volume": pa.array(np.full(len(close), 1000.0)),
        }
    )
    pq.write_table(table, path)


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

    backends = ["pyarrow"]
    if module._module_available("polars"):
        backends.append("polars_frame")
        backends.append("polars_lazy")
        backends.append("polars_streaming")

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


def test_panel_trading_volume_policy_required_rejects_missing_volume(tmp_path: Path) -> None:
    _write_symbol_without_volume(tmp_path / "AAA_features.parquet", 0.0)

    with pytest.raises(ValueError, match="Trading_Volume"):
        build_panel(tmp_path, panel_backend="pyarrow", trading_volume_policy="required")


def test_panel_trading_volume_policy_auto_rejects_stock_like_missing_volume(tmp_path: Path) -> None:
    root = tmp_path / "us_stocks"
    root.mkdir()
    _write_symbol_without_volume(root / "AAA_features.parquet", 0.0)

    with pytest.raises(ValueError, match="Trading_Volume"):
        build_panel(root, panel_backend="pyarrow", trading_volume_policy="auto")


def test_panel_trading_volume_policy_optional_allows_missing_volume(tmp_path: Path) -> None:
    _write_symbol_without_volume(tmp_path / "AAA_features.parquet", 0.0)

    panel = build_panel(tmp_path, panel_backend="pyarrow", trading_volume_policy="optional")

    assert panel.num_symbols == 1


def test_pyarrow_panel_backend_builds_synthetic_parquet(tmp_path: Path) -> None:
    module = _load_benchmark_module()
    from stockagent.data.panel import build_panel

    if not module._module_available("pyarrow"):
        return

    _write_symbol(tmp_path / "AAA_features.parquet", 0.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 1.0)

    pyarrow_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="pyarrow",
        panel_load_workers=1,
    )

    assert pyarrow_panel.symbols == ["AAA", "BBB"]
    assert pyarrow_panel.features.shape[:2] == (8, 2)
    assert pyarrow_panel.returns_1d.shape == (8, 2)
    assert pyarrow_panel.tradable_mask.all()


def test_polars_lazy_panel_backend_matches_pyarrow_on_synthetic_parquet(tmp_path: Path) -> None:
    module = _load_benchmark_module()
    from stockagent.data.panel import build_panel

    if not module._module_available("polars"):
        return

    _write_symbol(tmp_path / "AAA_features.parquet", 0.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 1.0)

    pyarrow_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="pyarrow",
        panel_load_workers=1,
    )
    polars_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="polars_lazy",
        panel_load_workers=1,
    )

    assert polars_panel.symbols == pyarrow_panel.symbols
    assert polars_panel.feature_names == pyarrow_panel.feature_names
    assert np.array_equal(polars_panel.dates, pyarrow_panel.dates)
    assert np.allclose(polars_panel.features, pyarrow_panel.features, equal_nan=True, atol=1e-6)
    assert np.allclose(polars_panel.returns_1d, pyarrow_panel.returns_1d, equal_nan=True, atol=1e-7)
    assert np.array_equal(polars_panel.tradable_mask, pyarrow_panel.tradable_mask)
    assert np.array_equal(polars_panel.can_buy_mask, pyarrow_panel.can_buy_mask)
    assert np.array_equal(polars_panel.can_sell_mask, pyarrow_panel.can_sell_mask)


def test_polars_streaming_panel_backend_matches_pyarrow_on_synthetic_parquet(tmp_path: Path) -> None:
    module = _load_benchmark_module()
    from stockagent.data.panel import build_panel

    if not module._module_available("polars"):
        return

    _write_symbol(tmp_path / "AAA_features.parquet", 0.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 1.0)

    pyarrow_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="pyarrow",
        panel_load_workers=1,
    )
    polars_panel = build_panel(
        tmp_path,
        use_rapids=False,
        benchmark_name="universe_average_return",
        panel_backend="polars_streaming",
        panel_load_workers=1,
    )

    assert polars_panel.symbols == pyarrow_panel.symbols
    assert polars_panel.feature_names == pyarrow_panel.feature_names
    assert np.array_equal(polars_panel.dates, pyarrow_panel.dates)
    assert np.allclose(polars_panel.features, pyarrow_panel.features, equal_nan=True, atol=1e-6)
    assert np.allclose(polars_panel.returns_1d, pyarrow_panel.returns_1d, equal_nan=True, atol=1e-7)
    assert np.array_equal(polars_panel.tradable_mask, pyarrow_panel.tradable_mask)
    assert np.array_equal(polars_panel.can_buy_mask, pyarrow_panel.can_buy_mask)
    assert np.array_equal(polars_panel.can_sell_mask, pyarrow_panel.can_sell_mask)


def test_wide_write_benchmark_reports_fastest_backend() -> None:
    module = _load_benchmark_module()
    backends = ["pyarrow"]
    if module._module_available("polars"):
        backends.append("polars_lazy")
        backends.append("polars_streaming")

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
