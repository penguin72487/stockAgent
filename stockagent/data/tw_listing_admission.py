"""Dated, evidence-backed admission to the ordinary TWSE/TPEx stock market.

The broker's current ``market`` label is not historical listing status.  Only
independently verified transitions belong here; unknown securities retain the
existing universe policy and need their own evidence before being reclassified.
"""

from __future__ import annotations

from datetime import date
from typing import Sequence

import numpy as np


TW_LISTING_ADMISSION_CONTRACT_VERSION = 1

# TPEx listing notices:
# https://www.tpex.org.tw/storage/eb_data/10903/10900013051.html
# https://www.tpex.org.tw/storage/eb_data/10903/10900018451.html
VERIFIED_EMERGING_TO_TPEX_LISTINGS = {
    "2743": date(2020, 3, 9),
    "6716": date(2020, 3, 27),
}


def regular_market_admission_contract() -> dict[str, object]:
    """Serializable semantic identity for optimizer and report fingerprints."""

    return {
        "contract_version": TW_LISTING_ADMISSION_CONTRACT_VERSION,
        "verified_tpex_listing_dates": {
            symbol: listing.isoformat()
            for symbol, listing in sorted(VERIFIED_EMERGING_TO_TPEX_LISTINGS.items())
        },
        "prelisting_policy": "no_regular_market_features_or_execution",
    }


def regular_market_admission_mask(
    symbols: Sequence[str] | np.ndarray, trading_date: date | np.datetime64 | str
) -> np.ndarray:
    """Return dated regular-market eligibility, in source symbol order."""

    day = np.datetime64(trading_date, "D")
    if np.isnat(day):
        raise ValueError("regular-market admission requires a valid trading date")
    values = np.asarray(symbols, dtype=str)
    allowed = np.ones(values.shape, dtype=bool)
    for symbol, listing in VERIFIED_EMERGING_TO_TPEX_LISTINGS.items():
        if day < np.datetime64(listing, "D"):
            allowed &= values != symbol
    return allowed
