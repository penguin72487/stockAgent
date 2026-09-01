from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from stockagent.data.panel import (
    DAY_TRADE_OPEN_GAP_FEATURE,
    LOG_RETURN_FEATURE_COLUMNS,
    build_panel,
    _load_symbol_arrays_polars_lazy,
    _load_symbol_arrays_pyarrow,
    _prepare_symbol_frame,
)


def _write_symbol(
    path: Path,
    dates: list[str],
    closes: list[float],
    volumes: list[float],
) -> None:
    pl.DataFrame(
        {
            "date": dates,
            "open": closes,
            "max": closes,
            "min": closes,
            "close": closes,
            "adjclose": closes,
            "Trading_Volume": volumes,
        }
    ).write_parquet(path)


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


@pytest.mark.parametrize("panel_backend", ["pyarrow", "polars_lazy"])
def test_explicit_day_trade_open_gap_is_next_open_over_current_close(
    tmp_path: Path,
    panel_backend: str,
) -> None:
    path = tmp_path / "2330_features.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "open": [100.0, 110.0, 121.0],
            "max": [102.0, 112.0, 123.0],
            "min": [99.0, 104.0, 118.0],
            "close": [100.0, 105.0, 120.0],
            "adjclose": [100.0, 105.0, 120.0],
            # Deliberately unrelated values: volume must not enter the feature.
            "Trading_Volume": [1.0, 999999.0, 7.0],
        }
    ).write_parquet(path)

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        panel_backend=panel_backend,
        panel_load_workers=0,
        trading_volume_policy="required",
        feature_include=["close_logret_1d", DAY_TRADE_OPEN_GAP_FEATURE],
    )

    gap_idx = panel.feature_names.index(DAY_TRADE_OPEN_GAP_FEATURE)
    np.testing.assert_allclose(
        panel.features[:, 0, gap_idx],
        np.asarray([math.log(110.0 / 100.0), math.log(121.0 / 105.0), 0.0]),
        rtol=1e-6,
        atol=1e-7,
    )


def test_day_trade_open_gap_can_be_delayed_until_the_following_session(
    tmp_path: Path,
) -> None:
    path = tmp_path / "2330_features.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "open": [100.0, 110.0, 121.0],
            "max": [102.0, 112.0, 123.0],
            "min": [99.0, 104.0, 118.0],
            "close": [100.0, 105.0, 120.0],
            "adjclose": [100.0, 105.0, 120.0],
            "Trading_Volume": [1.0, 2.0, 3.0],
        }
    ).write_parquet(path)

    def load() -> object:
        return build_panel(
            tmp_path,
            benchmark_name="2330",
            panel_backend="pyarrow",
            panel_load_workers=0,
            trading_volume_policy="required",
            feature_include=["close_logret_1d", DAY_TRADE_OPEN_GAP_FEATURE],
            feature_shift_next_session=[DAY_TRADE_OPEN_GAP_FEATURE],
        )

    expected = np.asarray(
        [0.0, math.log(110.0 / 100.0), math.log(121.0 / 105.0)]
    )
    for panel in (load(), load()):
        gap_idx = panel.feature_names.index(DAY_TRADE_OPEN_GAP_FEATURE)
        np.testing.assert_allclose(
            panel.features[:, 0, gap_idx],
            expected,
            rtol=1e-6,
            atol=1e-7,
        )


def test_day_trade_open_gap_is_not_in_default_panel_schema(tmp_path: Path) -> None:
    _write_symbol(
        tmp_path / "2330_features.parquet",
        ["2024-01-02", "2024-01-03"],
        [100.0, 101.0],
        [1000.0, 1000.0],
    )

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        panel_backend="pyarrow",
        panel_load_workers=0,
        trading_volume_policy="required",
    )

    assert DAY_TRADE_OPEN_GAP_FEATURE not in panel.feature_names


def test_kbar_ratios_reject_invalid_ohlc_and_define_flat_bar(tmp_path: Path) -> None:
    frame = pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "open": [100.0, 100.0],
            "max": [100.0, 100.0],
            "min": [100.0, 100.0],
            "close": [110.0, 100.0],
            "adjclose": [110.0, 100.0],
            "Trading_Volume": [1000.0, 1000.0],
        }
    )
    path = tmp_path / "2330_features.parquet"
    frame.write_parquet(path)

    ratio_names = [
        "body_ratio",
        "signed_body_ratio",
        "clv",
        "upper_shadow",
        "lower_shadow",
    ]
    prepared_rows = _prepare_symbol_frame(frame, path).to_dicts()
    for name in ratio_names:
        assert prepared_rows[0][name] is None
    assert [float(prepared_rows[1][name]) for name in ratio_names] == [
        0.0,
        0.0,
        0.5,
        0.0,
        0.0,
    ]

    for arrays in (_load_symbol_arrays_pyarrow(path), _load_symbol_arrays_polars_lazy(path)):
        values = arrays.features[:, [LOG_RETURN_FEATURE_COLUMNS.index(name) for name in ratio_names]]
        assert np.isnan(values[0]).all()
        assert np.allclose(values[1], np.array([0.0, 0.0, 0.5, 0.0, 0.0]))


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


def test_tw_scale_discontinuity_uses_stricter_market_bound(tmp_path: Path) -> None:
    frame = pl.DataFrame(
        {
            "date": ["2008-07-07", "2008-07-08", "2008-07-09"],
            "open": [100.0, 40.0, 42.0],
            "max": [100.0, 40.0, 42.0],
            "min": [100.0, 40.0, 42.0],
            "close": [100.0, 40.0, 42.0],
            "adjclose": [100.0, 40.0, 42.0],
            "Trading_Volume": [1000.0, 1000.0, 1000.0],
        }
    )
    tw_path = tmp_path / "2537_features.parquet"
    frame.write_parquet(tw_path)

    prepared = _prepare_symbol_frame(frame, tw_path)
    assert np.isnan(prepared["return_1d"].to_numpy()[0])
    assert np.isnan(prepared["close_logret_1d"].to_numpy()[1])
    for arrays in (_load_symbol_arrays_pyarrow(tw_path), _load_symbol_arrays_polars_lazy(tw_path)):
        close_idx = LOG_RETURN_FEATURE_COLUMNS.index("close_logret_1d")
        assert np.isnan(arrays.returns_1d[0])
        assert np.isnan(arrays.features[1, close_idx])

    non_tw = _prepare_symbol_frame(frame, Path("HBE_features.parquet"))
    assert math.isclose(float(non_tw["return_1d"][0]), math.log(0.4), rel_tol=1e-7)


def test_official_tw_reference_index_allows_verified_above_two_x_return(tmp_path: Path) -> None:
    official_root = tmp_path / "data_tw_public" / "stocks"
    official_root.mkdir(parents=True)
    frame = pl.DataFrame(
        {
            "date": ["2024-10-04", "2024-10-07"],
            "open": [17.0, 18.11],
            "max": [17.95, 38.83],
            "min": [17.0, 18.11],
            "close": [17.95, 38.83],
            "adjclose": [10.0, 10.0 * 38.83 / 17.95],
            "Trading_Volume": [1000.0, 1000.0],
        }
    )
    path = official_root / "00887_features.parquet"
    frame.write_parquet(path)

    arrays = _load_symbol_arrays_pyarrow(path)

    assert math.isclose(float(arrays.returns_1d[0]), math.log(38.83 / 17.95), rel_tol=1e-6)


@pytest.mark.parametrize("panel_backend", ["pyarrow", "polars_lazy"])
def test_panel_masks_forward_return_across_symbol_session_gap(
    tmp_path: Path,
    panel_backend: str,
) -> None:
    _write_symbol(
        tmp_path / "2330_features.parquet",
        ["2024-01-02", "2024-01-03", "2024-01-04"],
        [100.0, 101.0, 102.0],
        [1000.0, 1000.0, 1000.0],
    )
    _write_symbol(
        tmp_path / "8101_features.parquet",
        ["2024-01-02", "2024-01-04"],
        [2.0, 10.0],
        [1000.0, 1000.0],
    )

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        panel_backend=panel_backend,
        panel_load_workers=0,
        trading_volume_policy="required",
    )
    symbol_idx = panel.symbols.index("8101")
    start_idx = int(np.flatnonzero(panel.dates == np.datetime64("2024-01-02"))[0])

    assert np.isnan(panel.returns_1d[start_idx, symbol_idx])


def test_panel_masks_forward_return_to_zero_volume_reference_quote(tmp_path: Path) -> None:
    _write_symbol(
        tmp_path / "2330_features.parquet",
        ["2024-01-02", "2024-01-03", "2024-01-04"],
        [100.0, 101.0, 102.0],
        [1000.0, 1000.0, 1000.0],
    )
    _write_symbol(
        tmp_path / "4950_features.parquet",
        ["2024-01-02", "2024-01-03", "2024-01-04"],
        [10.0, 25.0, 25.0],
        [1000.0, 0.0, 1000.0],
    )

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        panel_backend="pyarrow",
        panel_load_workers=0,
        trading_volume_policy="required",
    )
    symbol_idx = panel.symbols.index("4950")
    first_idx = int(np.flatnonzero(panel.dates == np.datetime64("2024-01-02"))[0])
    reference_idx = int(np.flatnonzero(panel.dates == np.datetime64("2024-01-03"))[0])

    assert np.isnan(panel.returns_1d[first_idx, symbol_idx])
    assert math.isclose(float(panel.returns_1d[reference_idx, symbol_idx]), 0.0, abs_tol=1e-7)


def test_panel_preserves_return_across_market_wide_holiday_gap(tmp_path: Path) -> None:
    dates = ["2024-02-07", "2024-02-15"]
    _write_symbol(
        tmp_path / "2330_features.parquet",
        dates,
        [100.0, 105.0],
        [1000.0, 1000.0],
    )
    _write_symbol(
        tmp_path / "2317_features.parquet",
        dates,
        [50.0, 51.0],
        [1000.0, 1000.0],
    )
    _write_symbol(
        tmp_path / "9999_features.parquet",
        ["2024-02-08"],
        [10.0],
        [1000.0],
    )

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        panel_backend="pyarrow",
        panel_load_workers=0,
        trading_volume_policy="required",
    )
    symbol_idx = panel.symbols.index("2317")

    assert np.array_equal(
        panel.dates.astype("datetime64[D]"),
        np.asarray(["2024-02-07", "2024-02-15"], dtype="datetime64[D]"),
    )
    assert not np.any(panel.dates == np.datetime64("2024-02-08"))
    assert math.isclose(
        float(panel.returns_1d[0, symbol_idx]),
        math.log(51.0 / 50.0),
        rel_tol=1e-7,
    )


def test_official_session_restores_benchmark_missing_market_day(tmp_path: Path) -> None:
    _write_symbol(
        tmp_path / "2330_features.parquet",
        ["2024-01-02", "2024-01-04"],
        [100.0, 102.0],
        [1000.0, 1000.0],
    )
    _write_symbol(
        tmp_path / "2317_features.parquet",
        ["2024-01-02", "2024-01-03", "2024-01-04"],
        [50.0, 51.0, 52.0],
        [1000.0, 1000.0, 1000.0],
    )
    external_path = tmp_path / "official.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-03"],
            # Only the receipt-verified market row is a calendar authority;
            # sparse per-stock traded markers must not manufacture sessions.
            "symbol": ["__MARKET__"],
            "_twpub_official_traded": [1.0],
        }
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        panel_backend="pyarrow",
        panel_load_workers=0,
        trading_volume_policy="required",
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
    )
    benchmark_idx = panel.symbols.index("2330")
    other_idx = panel.symbols.index("2317")

    assert np.array_equal(
        panel.dates.astype("datetime64[D]"),
        np.asarray(["2024-01-02", "2024-01-03", "2024-01-04"], dtype="datetime64[D]"),
    )
    assert np.isnan(panel.returns_1d[0, benchmark_idx])
    assert math.isclose(float(panel.returns_1d[0, other_idx]), math.log(51.0 / 50.0), rel_tol=1e-7)
