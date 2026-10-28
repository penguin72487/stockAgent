"""Regression checks for the independently observed one-ULP clock drift."""

import numpy as np
import polars as pl
import pytest

from scripts.readback_tw_futures_prior_exchange_clocks import _entry_quote_differences


def _entry(price=9.0, status="observed_minute", volume=3.0):
    return pl.DataFrame({"date": ["2017-05-16"], "physical_contract": ["OKF:201705"],
                         "symbol": ["TAIFEX_SLOT_0698"], "entry_quote": [price],
                         "entry_volume": [volume], "source_file_sha256": ["a" * 64],
                         "status": [status]})


def test_readback_keeps_one_ulp_observation_but_rejects_larger_price_change():
    initial = 9.0
    adjacent = np.nextafter(initial, np.inf)
    two = np.nextafter(adjacent, np.inf)
    assert _entry_quote_differences(_entry(initial), _entry(initial)).height == 0
    assert _entry_quote_differences(_entry(initial), _entry(adjacent)).height == 1
    with pytest.raises(ValueError, match="more than one FP64 ULP"):
        _entry_quote_differences(_entry(initial), _entry(two))


def test_readback_never_tolerates_changed_source_or_non_observed_price():
    initial, adjacent = 9.0, np.nextafter(9.0, np.inf)
    with pytest.raises(ValueError, match="non-price facts changed"):
        _entry_quote_differences(_entry(initial), _entry(adjacent, volume=4))
    with pytest.raises(ValueError, match="non-observed"):
        _entry_quote_differences(_entry(initial, "daily_not_executable"),
                                 _entry(adjacent, "daily_not_executable"))
