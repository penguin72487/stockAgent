from __future__ import annotations

from datetime import date

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import pytest
import stockagent.data.panel as panel_module

from stockagent.data.panel import (
    _forward_fill_point_in_time_features,
    _load_external_feature_arrays,
    build_panel,
)


@pytest.mark.parametrize("duplicates", ["none", "some", "all", "empty"])
@pytest.mark.parametrize("columns", [[], ["twpub_f32", "twpub_f64", "_twpub_rule"]])
def test_external_aggregation_preserves_every_group_value_and_dtype(duplicates, columns):
    frame = pl.DataFrame({
        "date": [date(2020, 1, 3), date(2020, 1, 2), date(2020, 1, 2)],
        "symbol": ["2330", "__MARKET__", "2330"],
        "twpub_f32": pl.Series([float("nan"), -0., 1.25], dtype=pl.Float32),
        "twpub_f64": [1.000000000123, float("inf"), None],
        "_twpub_rule": [None, 123.456789123456, -0.],
    }).with_columns(pl.col("date").cast(pl.Datetime("ns")))
    if duplicates in {"some", "all"}:
        repeated = frame.tail(1) if duplicates == "some" else frame
        correction = repeated.with_columns(
            pl.lit(None, dtype=pl.Float32).alias("twpub_f32"),
            pl.lit(2.000000000123).alias("twpub_f64"),
            pl.lit(None, dtype=pl.Float64).alias("_twpub_rule"))
        frame = pl.concat([frame, correction])
    elif duplicates == "empty":
        frame = frame.head(0)
    expected = frame.group_by(["date", "symbol"]).agg(
        [pl.col(name).drop_nulls().last().alias(name) for name in columns]
    ).sort(["symbol", "date"])
    actual = panel_module._aggregate_external_feature_rows(frame, columns)
    assert actual.schema == expected.schema
    assert actual.select(["date", "symbol"]).equals(expected.select(["date", "symbol"]))
    for name in columns:
        assert actual[name].is_null().equals(expected[name].is_null())
        assert actual[name].to_numpy().tobytes() == expected[name].to_numpy().tobytes()


def test_external_duplicates_are_normalized_before_last_non_null_resolution(tmp_path):
    path = tmp_path / "normalized-duplicates.parquet"
    pq.write_table(pa.table({
        "date": ["2020-01-02", "2020-01-02", "2020-01-03", None, "2020-01-02"],
        "symbol": [" abc ", "ABC", "__market__", "abc", " "],
        "twpub_first": [1., None, -0., 90., 80.],
        "twpub_second": [None, 2., 3., 90., 80.],
        "_twpub_price_limit_upper": [123.456789123456, None, 5., 90., 80.],
    }), path)
    actual = _load_external_feature_arrays(path)
    assert set(actual.by_symbol) == {"ABC"}
    dates, values = actual.by_symbol["ABC"]
    np.testing.assert_array_equal(dates, np.array(["2020-01-02"], dtype="datetime64[ns]"))
    np.testing.assert_array_equal(values, np.array([[1., 2.]], dtype=np.float32))
    assert actual.by_symbol_rules["ABC"][1][0, 0] == 123.456789123456
    assert np.signbit(actual.market_values[0, 0])


@pytest.mark.parametrize("dtype", [np.float32, np.float64])
@pytest.mark.parametrize("layout", ["c", "fortran", "strided", "empty"])
def test_single_mask_sanitizer_preserves_all_finite_bits_and_layout(dtype, layout):
    limits = np.finfo(dtype)
    smallest = np.nextafter(dtype(0), dtype(1))
    values = np.array([np.nan, np.inf, -np.inf, -0., 0., smallest,
                       -smallest, limits.max, -limits.max, 1., -1., 7.5], dtype=dtype).reshape(2, 2, 3)
    if layout == "fortran":
        values = np.asfortranarray(values)
    elif layout == "strided":
        values = np.repeat(values, 2, axis=1)[:, ::2]
    elif layout == "empty":
        values = values[:0]
    before = values.copy()
    expected = np.nan_to_num(before, nan=0., posinf=0., neginf=0.)
    pointer, strides = values.ctypes.data, values.strides
    actual = panel_module._zero_nonfinite_features_inplace(values)
    assert actual is values and actual.ctypes.data == pointer and actual.strides == strides
    assert actual.dtype == expected.dtype and actual.shape == expected.shape
    assert actual.tobytes() == expected.tobytes()


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


def test_native_float32_model_view_does_not_widen_rules_or_model_buffers(tmp_path, monkeypatch) -> None:
    path = tmp_path / "native-f32.parquet"
    # The rule needs Float64 precision even though the model uses Float32.
    price = 123.456789123456
    pq.write_table(pa.table({
        "date": pa.array([date(2020,1,2), date(2020,1,2), date(2020,1,3)], type=pa.date32()),
        "symbol": ["2330"]*3,
        "twcad_value": pa.array([1.25, None, -3.5], type=pa.float32()),
        "_twpub_price_limit_upper": [price, None, price+0.1],
    }), path)
    seen = []
    original = panel_module._external_frame_to_arrays

    def capture(frame, names, **kwargs):
        seen.append({n: frame.schema[n] for n in names})
        return original(frame, names, **kwargs)

    monkeypatch.setattr(panel_module, "_external_frame_to_arrays", capture)
    result = _load_external_feature_arrays(path)
    assert seen[0]["twcad_value"] == pl.Float32
    assert seen[1]["_twpub_price_limit_upper"] == pl.Float64
    _, values = result.by_symbol["2330"]
    np.testing.assert_array_equal(values[:,0], np.array([1.25,-3.5], dtype=np.float32))
    _, rules = result.by_symbol_rules["2330"]
    assert rules.dtype == np.float64
    np.testing.assert_array_equal(rules[:,0], np.array([price,price+0.1], dtype=np.float64))
