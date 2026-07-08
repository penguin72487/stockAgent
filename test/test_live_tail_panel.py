from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from stockagent.data.panel import LOG_RETURN_FEATURE_COLUMNS, build_tail_panel


def _write_symbol(path, start_price: float) -> None:
    base = datetime(2026, 1, 1, 0, 0)
    rows = 20
    close = np.asarray([start_price + i for i in range(rows)], dtype=np.float64)
    table = pa.table(
        {
            "date": [base + timedelta(minutes=15 * i) for i in range(rows)],
            "open": close - 0.1,
            "max": close + 0.2,
            "min": close - 0.3,
            "close": close,
            "adjclose": close,
            "Trading_Volume": np.asarray([1000 + i for i in range(rows)], dtype=np.float64),
        }
    )
    pq.write_table(table, path, row_group_size=5)


def test_build_tail_panel_reads_only_recent_rows(tmp_path) -> None:
    _write_symbol(tmp_path / "AAA_features.parquet", 10.0)
    _write_symbol(tmp_path / "BBB_features.parquet", 20.0)

    panel = build_tail_panel(tmp_path, tail_rows=6, panel_load_workers=2)

    assert panel.symbols == ["AAA", "BBB"]
    assert panel.features.shape == (6, 2, len(LOG_RETURN_FEATURE_COLUMNS))
    assert panel.close_prices.shape == (6, 2)
    assert str(panel.dates[0]).startswith("2026-01-01T03:30:00")
    assert str(panel.dates[-1]).startswith("2026-01-01T04:45:00")
    assert np.isfinite(panel.features[-1]).all()
