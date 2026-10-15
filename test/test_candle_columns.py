from __future__ import annotations

import random

import polars as pl
import pytest

from downloader import download_binance_perp_15m as binance
from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader.candle_columns import numeric_candle_page


def row(provider, ts=1_700_000_100_000):
    if provider is binance:
        return [ts, "100", "102", "99", "101", "12.5", ts + 59_999, "1262.5", 42, "7", "707", "ignored"]
    if provider is okx:
        return [str(ts), "100", "102", "99", "101", "12.5", "0.125", "12.625", "1"]
    return [str(ts), "100", "102", "99", "101", "12.5", "1262.5"]


def assert_exact(left, right):
    assert left.schema == right.schema
    assert left.equals(right)
    for name, dtype in left.schema.items():
        if dtype in (pl.Float64, pl.Int64):
            assert left[name].to_numpy().tobytes() == right[name].to_numpy().tobytes()


@pytest.mark.parametrize("provider", [binance, okx, bybit])
def test_native_candle_parser_matches_scalar_values_units_revisions_and_float_bits(provider):
    rng = random.Random(2837)
    values = [row(provider, 1_700_000_100_000 + index * 60_000) for index in range(500)]
    for item in values:
        for index in range(5, 8 if provider is okx else 7 if provider is bybit else 6):
            item[index] = repr(rng.uniform(0, 1e16))
    values += [row(provider, int(values[0][0])), row(provider, int(values[10][0]))]
    values[-1][4] = "101.5"
    rng.shuffle(values)
    assert_exact(provider._normalize_candles_reference(values), provider._normalize_candles(values))


@pytest.mark.parametrize("provider", [binance, okx, bybit])
@pytest.mark.parametrize("token", [None, "", "garbage", "1.5", "123e0", "1_700_000_100_000", "٤٢", -1, True, 1700000100000.5])
def test_timestamp_conversion_and_error_semantics_keep_scalar_oracle(provider, token):
    raw = row(provider)
    raw[0] = token
    try:
        expected = provider._normalize_candles_reference([raw])
    except Exception as exc:
        with pytest.raises(type(exc)) as observed:
            provider._normalize_candles([raw])
        assert str(observed.value) == str(exc)
    else:
        assert_exact(expected, provider._normalize_candles([raw]))


@pytest.mark.parametrize("provider", [binance, okx, bybit])
@pytest.mark.parametrize("field,token", [(1, None), (5, ""), (5, None), (5, "-1"),
                                         (5, "NaN"), (5, "inf"), (6, ""), (4, "1_01")])
def test_numeric_missingness_special_formats_and_volume_policies_preserved(provider, field, token):
    raw = row(provider)
    raw[field] = token
    try:
        expected = provider._normalize_candles_reference([raw])
    except Exception as exc:
        with pytest.raises(type(exc)) as observed:
            provider._normalize_candles([raw])
        assert str(observed.value) == str(exc)
    else:
        assert_exact(expected, provider._normalize_candles([raw]))


@pytest.mark.parametrize("provider", [binance, okx, bybit])
def test_native_path_does_not_call_scalar_date_renderer(provider, monkeypatch):
    def forbidden(*args):
        raise AssertionError("documented arrays should use native date formatting")
    monkeypatch.setattr(provider, "_ms_to_date_string", forbidden)
    result = provider._normalize_candles([row(provider)])
    assert result["date"].item() == "2023-11-14 22:15:00"
    assert result.height == 1


def test_polars_decimal_string_to_integer_is_not_silently_truncated():
    schema = {"ts": pl.Int64, "price": pl.Float64}
    assert numeric_candle_page([["1.5", "100"]], schema, timestamp="ts", skip_short_rows=True) is None
    assert numeric_candle_page([[None, "100"]], schema, timestamp="ts", skip_short_rows=True) is None
