from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import polars as pl

from stockagent.data.panel import _prepare_symbol_frame


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
