from __future__ import annotations

import pytest

from downloader import download_okx_perp_daily as okx


def candle() -> list[str]:
    return ["1767225600000", "100", "102", "99", "101", "120", "12", "1212", "1"]


def test_quote_volume_never_becomes_base_volume_or_contracts():
    row = okx._normalize_candles([candle()]).row(0, named=True)
    assert row["Trading_Volume"] == row["okx_volume_quote"] == 1212
    assert row["okx_volume_base"] == 12
    assert row["okx_volume_contract"] == 120
    assert okx.VOLUME_UNIT_CONTRACT["Trading_Volume"] == "quote_currency"


@pytest.mark.parametrize("index", [5, 6, 7])
@pytest.mark.parametrize("value", ["", None, "NaN", "inf", "-1"])
def test_invalid_quantity_is_rejected_not_filled_from_another_unit(index, value):
    row = candle()
    row[index] = value
    with pytest.raises(ValueError, match="OKX candle"):
        okx._normalize_candles([row])


def test_explicit_zero_is_not_missing():
    row = candle()
    row[5:8] = ["0", "0", "0"]
    assert okx._normalize_candles([row])["Trading_Volume"].item() == 0


def test_truncated_candle_is_not_silently_dropped():
    with pytest.raises(ValueError, match="lacks documented"):
        okx._normalize_candles([candle()[:7]])
