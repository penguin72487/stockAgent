from __future__ import annotations

from datetime import date

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from stockagent.data.panel import (
    _forward_fill_point_in_time_features,
    _load_external_feature_arrays,
    build_panel,
)


def _write_symbol(path, dates: list[date]) -> None:
    prices = [100.0 + index for index in range(len(dates))]
    pq.write_table(
        pa.table(
            {
                "date": pa.array(dates, type=pa.date32()),
                "open": prices,
                "max": prices,
                "min": prices,
                "close": prices,
                "adjclose": prices,
                "Trading_Volume": [1000.0] * len(dates),
            }
        ),
        path,
    )


def test_selected_external_projection_preserves_values_and_dates(tmp_path) -> None:
    path = tmp_path / "research.parquet"
    pq.write_table(
        pa.table(
            {
                "date": pa.array(
                    [date(2020, 1, 2), date(2020, 1, 3)] * 2,
                    type=pa.date32(),
                ),
                "symbol": ["__MARKET__", "__MARKET__", "2330", "2330"],
                "twpub_selected_raw": [1.0, 2.0, 3.0, None],
                "twpub_unused_raw": [9.0, 8.0, 7.0, 6.0],
            }
        ),
        path,
    )
    full = _load_external_feature_arrays(path, include_rules=False)
    projected = _load_external_feature_arrays(
        path,
        include_rules=False,
        selected_feature_names=("twpub_selected_raw",),
    )
    assert projected.feature_names == ["twpub_selected_raw"]
    assert np.array_equal(projected.market_dates, full.market_dates)
    assert np.array_equal(
        projected.market_values[:, 0],
        full.market_values[:, full.feature_names.index("twpub_selected_raw")],
    )
    full_dates, full_values = full.by_symbol["2330"]
    projected_dates, projected_values = projected.by_symbol["2330"]
    assert np.array_equal(projected_dates, full_dates)
    np.testing.assert_allclose(
        projected_values[:, 0],
        full_values[:, full.feature_names.index("twpub_selected_raw")],
        equal_nan=True,
    )


def test_availability_flag_moves_with_late_daily_value(tmp_path) -> None:
    _write_symbol(
        tmp_path / "2330_features.parquet",
        [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
    )
    external = tmp_path / "external.parquet"
    pq.write_table(
        pa.table(
            {
                "date": pa.array([date(2024, 1, 3)], type=pa.date32()),
                "symbol": ["2330"],
                "twpub_pe_raw": [0.0],
            }
        ),
        external,
    )
    panel = build_panel(
        tmp_path,
        benchmark_name="universe_average_return",
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external,
        feature_include=["twpub_pe_raw"],
        feature_availability_indicators=["twpub_pe_raw"],
        feature_shift_next_session=["twpub_pe_raw"],
    )
    flag = panel.feature_names.index("twpub_pe_raw__available")
    assert panel.features[:, 0, flag].tolist() == [0.0, 0.0, 1.0]


def test_tifrs_400_day_expiry_does_not_expire_m2() -> None:
    dates = np.array(["2018-01-01", "2018-06-01", "2019-03-01"], dtype="datetime64[D]")
    values = np.array([[100.0, 10.0], [np.nan, np.nan], [np.nan, np.nan]])
    _forward_fill_point_in_time_features(
        values,
        ["twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw", "twpub_cbc_m2_raw"],
        dates.astype(np.int64),
    )
    assert values[1].tolist() == [100.0, 10.0]
    assert np.isnan(values[2, 0])
    assert values[2, 1] == 10.0
