"""Audit-only classification of TWSE/TPEx exchange security identifiers.

This module deliberately does not define the model's tradable universe.  It
exists so source-quality audits can check quote fields for every security type
that is present in the official daily files without making warrants, bonds, or
preferred shares trainable by accident.
"""

from __future__ import annotations

from datetime import date
import re

from stockagent.data.tw_security import classify_tw_stock_or_etf


_REIT = re.compile(r"^01[0-9A-Z]*T$")
_ETN = re.compile(r"^02[0-9A-Z]+$")
_FOUR_DIGIT_EQUITY = re.compile(r"^[1-9][0-9]{3}$")
_EQUITY_SUFFIX = re.compile(r"^[1-9][0-9]{3}[A-Z](?:[0-9])?$")
_TDR = re.compile(r"^91[0-9]{4}$")
_TWSE_CONVERTIBLE_BOND = re.compile(r"^[1-9][0-9]{4}$")
_TPEX_WARRANT = re.compile(r"^7[0-9A-Z]{4,5}$")

# These pre-ETF beneficiary certificates occur in the local TWSE history.
# Their exchange quote grid followed the regular equity grid.
_LEGACY_CLOSED_END_FUNDS = frozenset({"0001", "0015", "0029"})

# Broker KBar ``market=tpex`` includes emerging listings before their first
# mainboard session. Only independently verified transitions may use the older
# emerging grid; first observed KBar dates are not listing-date evidence.
# https://www.tpex.org.tw/storage/eb_data/10903/10900013051.html
# https://www.tpex.org.tw/storage/eb_data/10903/10900018451.html
VERIFIED_EMERGING_TO_TPEX_LISTINGS = {
    "2743": date(2020, 3, 9),
    "6716": date(2020, 3, 27),
}


def classify_tw_broker_security_on_date(
    venue: str, symbol: object, trading_date: date
) -> str | None:
    """Resolve broker market labels against verified listing transitions."""

    code = str(symbol or "").strip().upper()
    listing = VERIFIED_EMERGING_TO_TPEX_LISTINGS.get(code)
    if str(venue or "").strip().lower() == "tpex" and listing and trading_date < listing:
        return "emerging_stock"
    return classify_tw_exchange_security(venue, code)


def classify_tw_exchange_security(
    venue: str, symbol: object, name: object | None = None
) -> str | None:
    """Return the quote-grid family for an official daily security row.

    ``name`` is accepted for forensic receipts and future disambiguation, but
    the current rules use the exchange identifier scheme so mojibake in old
    TPEx names cannot change the result.
    """

    del name
    market = str(venue or "").strip().lower()
    code = str(symbol or "").strip().upper()
    if market not in {"twse", "tpex"} or not code:
        return None
    if _REIT.fullmatch(code):
        return "reit"
    if _ETN.fullmatch(code):
        return "etn"
    stock_or_etf = classify_tw_stock_or_etf(code)
    if stock_or_etf is not None:
        return stock_or_etf
    if code in _LEGACY_CLOSED_END_FUNDS:
        return "stock"
    if _FOUR_DIGIT_EQUITY.fullmatch(code):
        return "stock"
    if _TDR.fullmatch(code):
        return "stock"
    if _EQUITY_SUFFIX.fullmatch(code):
        return "stock"
    if market == "twse" and _TWSE_CONVERTIBLE_BOND.fullmatch(code):
        return "convertible_bond"
    if market == "tpex" and _TPEX_WARRANT.fullmatch(code):
        return "warrant"
    return None
