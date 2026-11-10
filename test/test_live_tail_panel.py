from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.data.panel import LOG_RETURN_FEATURE_COLUMNS, build_panel, build_tail_panel
from downloader.ohlcv_hot_tail import hot_tail_path


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


def test_live_tail_reports_progress_during_load_and_materialization(tmp_path) -> None:
    for index in range(3):
        _write_symbol(tmp_path / f"S{index}_features.parquet", 10.0 + index)
    events: list[tuple[str, int, int]] = []

    build_tail_panel(
        tmp_path,
        tail_rows=6,
        panel_load_workers=2,
        progress_callback=lambda phase, done, total: events.append((phase, done, total)),
    )

    assert ("load", 3, 3) in events
    assert ("materialize", 3, 3) in events


def test_build_tail_panel_includes_hot_tail_without_duplicate_timestamp(
    tmp_path,
) -> None:
    source = tmp_path / "AAA_features.parquet"
    _write_symbol(source, 10.0)
    tail = hot_tail_path(source)
    tail.parent.mkdir(parents=True)
    base = datetime(2026, 1, 1, 0, 0)
    pq.write_table(
        pa.table(
            {
                "date": [base + timedelta(minutes=15 * 19), base + timedelta(minutes=15 * 20)],
                "open": [28.9, 29.9],
                "max": [29.2, 30.2],
                "min": [28.7, 29.7],
                "close": [29.5, 30.0],
                "adjclose": [29.5, 30.0],
                "Trading_Volume": [1019.0, 1020.0],
            }
        ),
        tail,
    )

    panel = build_tail_panel(tmp_path, tail_rows=6, panel_load_workers=0)

    assert panel.features.shape[0] == 6
    assert str(panel.dates[-1]).startswith("2026-01-01T05:00:00")
    assert panel.close_prices[-1, 0] == 30.0


def test_build_tail_panel_filters_return_valuation_with_sparse_symbol_dates(
    tmp_path,
) -> None:
    dense_dates = [datetime(2026, 1, 1) + timedelta(days=i) for i in range(10)]
    sparse_dates = [
        datetime(2025, 12, 1) + timedelta(days=i) for i in range(4)
    ] + dense_dates[-2:]

    def write(path, dates, base_price: float) -> None:
        close = np.arange(len(dates), dtype=np.float64) + base_price
        pq.write_table(
            pa.table(
                {
                    "date": dates,
                    "open": close,
                    "max": close + 0.2,
                    "min": close - 0.2,
                    "close": close,
                    "adjclose": close,
                    "Trading_Volume": np.full(len(dates), 1000.0),
                }
            ),
            path,
        )

    write(tmp_path / "DENSE_features.parquet", dense_dates, 100.0)
    write(tmp_path / "SPARSE_features.parquet", sparse_dates, 50.0)

    panel = build_tail_panel(
        tmp_path,
        tail_rows=6,
        panel_load_workers=0,
        benchmark_name="DENSE",
    )

    sparse_idx = panel.symbols.index("SPARSE")
    assert panel.features.shape[0] == 6
    assert int(panel.tradable_mask[:, sparse_idx].sum()) == 2
    assert np.isfinite(panel.returns_1d[:, sparse_idx]).sum() <= 1


@pytest.mark.parametrize("transform", ["ordinary", "zero_fill", "shift"])
def test_tail_open_gap_matches_training_when_symbol_misses_a_session(tmp_path, transform):
    """A later real trade must not appear in the missing prior session."""
    stocks = tmp_path / "stocks"
    stocks.mkdir()
    days = [datetime(2026, 8, day) for day in (20, 21, 24, 25)]

    def write(symbol, selected, prices):
        pq.write_table(pa.table({
            "date": selected, "open": prices, "max": prices, "min": prices,
            "close": prices, "adjclose": prices, "Trading_Volume": [1000.] * len(prices),
        }), stocks / f"{symbol}_features.parquet")

    write("1111", days, [30., 31., 32., 33.])
    write("2222", [days[0], days[2], days[3]], [38., 41.1, 42.])
    gap = "next_session_open_gap_logret"
    kwargs = {"benchmark_name": "1111", "panel_load_workers": 0,
              "feature_include": ["open_raw", "close_raw", gap],
              "feature_zero_fill": [gap] if transform == "zero_fill" else [],
              "feature_shift_next_session": [gap] if transform == "shift" else []}
    full = build_panel(stocks, panel_backend="pyarrow", panel_cache_root=tmp_path / "full-cache", **kwargs)
    tail = build_tail_panel(stocks, tail_rows=16, **kwargs)
    assert full.feature_names == tail.feature_names
    np.testing.assert_array_equal(full.dates, tail.dates)
    np.testing.assert_array_equal(full.features.view(np.uint32), tail.features.view(np.uint32))
    symbol, column = tail.symbols.index("2222"), tail.feature_names.index(gap)
    # 08/21 has no source row. 08/24's opening cannot be backdated to 08/21.
    assert tail.features[0, symbol, column] == 0.
    assert tail.features[1, symbol, column] == 0.
    if transform == "ordinary":
        expected = np.float32(np.log(np.float64(np.float32(42.)) / np.float64(np.float32(41.1))))
        assert tail.features[2, symbol, column] == expected
