from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import polars as pl

from stockagent.data.panel import (
    LOG_RETURN_FEATURE_COLUMNS,
    _load_symbol_arrays_polars_lazy,
    _load_symbol_arrays_pyarrow,
    _prepare_symbol_frame,
)


def test_return_label_uses_adjclose_but_execution_price_uses_close() -> None:
    frame = pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "open": [100.0, 103.0, 104.0],
            "max": [101.0, 104.0, 105.0],
            "min": [99.0, 102.0, 103.0],
            "close": [100.0, 110.0, 120.0],
            "adjclose": [100.0, 105.0, 105.0],
            "Trading_Volume": [1000.0, 1100.0, 1200.0],
        }
    )

    prepared = _prepare_symbol_frame(frame, Path("3516_features.parquet"))

    assert np.allclose(prepared["close_raw"].to_numpy(), np.array([100.0, 110.0, 120.0], dtype=np.float32))
    rows = prepared.to_dicts()
    assert math.isclose(float(rows[0]["return_1d"]), math.log(105.0 / 100.0), rel_tol=1e-7)
    assert math.isclose(float(rows[1]["return_1d"]), 0.0, abs_tol=1e-7)
    assert math.isclose(float(rows[1]["close_logret_1d"]), math.log(110.0 / 100.0), rel_tol=1e-7)


def test_extreme_daily_price_log_returns_are_masked() -> None:
    frame = pl.DataFrame(
        {
            "date": ["2015-06-10", "2015-06-11", "2015-06-12", "2015-06-15"],
            "open": [0.09, 5500.0, 0.08, 0.081],
            "max": [0.09, 5500.0, 0.08, 0.081],
            "min": [0.09, 5500.0, 0.08, 0.081],
            "close": [0.09, 5500.0, 0.08, 0.081],
            "adjclose": [0.09, 5500.0, 0.08, 0.081],
            "Trading_Volume": [1000.0, 1000.0, 1000.0, 1000.0],
        }
    )

    prepared = _prepare_symbol_frame(frame, Path("HBE_features.parquet"))

    returns = prepared["return_1d"].to_numpy()
    close_logret = prepared["close_logret_1d"].to_numpy()
    assert np.isnan(returns[0])
    assert np.isnan(returns[1])
    assert math.isclose(float(returns[2]), math.log(0.081 / 0.08), rel_tol=1e-7)
    assert np.isnan(close_logret[1])
    assert np.isnan(close_logret[2])
    assert math.isclose(float(close_logret[3]), math.log(0.081 / 0.08), rel_tol=1e-7)


def test_symbol_array_backends_mask_extreme_daily_price_log_returns(tmp_path: Path) -> None:
    path = tmp_path / "HBE_features.parquet"
    pl.DataFrame(
        {
            "date": ["2015-06-10", "2015-06-11", "2015-06-12", "2015-06-15"],
            "open": [0.09, 5500.0, 0.08, 0.081],
            "max": [0.09, 5500.0, 0.08, 0.081],
            "min": [0.09, 5500.0, 0.08, 0.081],
            "close": [0.09, 5500.0, 0.08, 0.081],
            "adjclose": [0.09, 5500.0, 0.08, 0.081],
            "Trading_Volume": [1000.0, 1000.0, 1000.0, 1000.0],
        }
    ).write_parquet(path)

    for arrays in (_load_symbol_arrays_pyarrow(path), _load_symbol_arrays_polars_lazy(path)):
        assert np.isnan(arrays.returns_1d[0])
        assert np.isnan(arrays.returns_1d[1])
        assert math.isclose(float(arrays.returns_1d[2]), math.log(0.081 / 0.08), rel_tol=1e-7)
        close_logret_idx = LOG_RETURN_FEATURE_COLUMNS.index("close_logret_1d")
        assert np.isnan(arrays.features[1, close_logret_idx])
        assert np.isnan(arrays.features[2, close_logret_idx])
