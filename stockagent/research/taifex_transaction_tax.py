"""Statutory TAIFEX transaction-tax helpers for research ledgers.

The tax is assessed per contract and rounded to whole TWD.  These helpers keep
the date-versioned statutory rules separate from strategy-specific execution
logic so daily, tick, and hold-to-expiry studies can share one implementation.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import math
from typing import Final


TAIFEX_OPTION_PREMIUM_TAX_RATE: Final[float] = 0.001
TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_BEFORE_2013_04_01: Final[float] = 0.00004
TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_FROM_2013_04_01: Final[float] = 0.00002
TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_CHANGE_DATE: Final[date] = date(2013, 4, 1)
TAIFEX_TAX_SCHEDULE_FIRST_SUPPORTED_DATE: Final[date] = date(2008, 10, 6)

TAIFEX_TRANSACTION_TAX_ACT_URL: Final[str] = (
    "https://law.moj.gov.tw/LawClass/LawAll.aspx?pcode=G0340079"
)
TAIFEX_TRANSACTION_TAX_QA_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/9/tradersQAProducts"
)
TAIFEX_TRANSACTION_TAX_ROUNDING_URL: Final[str] = (
    "https://law-out.mof.gov.tw/LawContent.aspx?id=GL006626"
)
TAIFEX_STOCK_INDEX_FUTURES_2013_TAX_SOURCE_URL: Final[str] = (
    "https://www.fsc.gov.tw/fckdowndoc?file=%2F%E8%AD%89%E6%9C%9F%E8%A6%81"
    "%E8%81%9E31-4.pdf&flag=doc"
)
TAIFEX_STOCK_INDEX_FUTURES_CURRENT_TAX_SOURCE_URL: Final[str] = (
    "https://law-out.mof.gov.tw/LawContent.aspx?id=GL010556"
)
TAIFEX_ALL_FUTURES_TAX_HISTORY_SOURCE_URL: Final[str] = (
    "https://service.mof.gov.tw/public/Data/statistic/Year_Fin/111%E9%9B%BB%E5%AD%90%E6%9B%B8/htm/312090.pdf"
)
TAIFEX_PRE_2006_INTEREST_INCOME_TAX_SOURCE_URL: Final[str] = (
    "https://www.fsc.gov.tw/fckdowndoc?file=%2F97_12%E5%AF%A6%E5%8B%99%E6%96%B0%E7%9F%A5.pdf&flag=doc"
)
TAIFEX_OIL_FUTURES_TAX_SOURCE_URL: Final[str] = (
    "https://law-out.mof.gov.tw/LawContent.aspx?id=GL010460"
)


def futures_transaction_tax_rate(trading_date: date, *, tax_class: str) -> float:
    """Historical per-side transaction tax, distinct from P&L point value.

    MOF's historical table records the APPLICABLE rate, not the statutory
    min/max band. Before 2006 interest futures were subject to income tax;
    their zero transaction rate does not mean tax-exempt investment income.
    Product listing and dated TWD taxable value remain the source builder's
    responsibility. This does not backdate today's product specification.
    """
    if not isinstance(trading_date, date):
        raise TypeError("trading_date must be a datetime.date")
    if tax_class in {"stock_index", "stock", "etf"}:
        for since, rate in ((date(2013, 4, 1), .00002), (date(2008, 10, 6), .00004),
                            (date(2006, 1, 1), .0001), (date(2000, 5, 1), .00025),
                            (date(1998, 7, 10), .0005)):
            if trading_date >= since:
                return rate
        raise ValueError("equity futures transaction tax is not verified before 1998-07-10")
    if tax_class in {"commercial_paper", "government_bond"}:
        if trading_date < date(2004, 1, 2):
            raise ValueError("interest futures pre-listing date")
        if trading_date < date(2006, 1, 1):
            return 0.  # Income taxation, not a claim of tax-free total returns.
        return .000000125 if tax_class == "commercial_paper" else .00000125
    first_and_rate = {"gold": (date(2006, 3, 27), .0000025),
                      "oil": (date(2018, 7, 2), .000005)}
    if tax_class not in first_and_rate:
        raise ValueError(f"unsupported futures tax class: {tax_class}")
    first, rate = first_and_rate[tax_class]
    if trading_date < first:
        raise ValueError(f"{tax_class} futures tax is not verified before {first}")
    return rate


def futures_transaction_tax_twd(taxable_contract_value_twd: object, *,
                                trading_date: date, tax_class: str) -> float:
    """Tax the dated full taxable contract amount, never margin or P&L DV01."""
    amount = _non_negative_decimal(taxable_contract_value_twd, name="taxable_contract_value_twd")
    rate = Decimal(str(futures_transaction_tax_rate(trading_date, tax_class=tax_class)))
    return round_taifex_tax_twd(amount * rate)


def _non_negative_decimal(value: object, *, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be finite and non-negative")
    try:
        number = float(value)
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be finite and non-negative") from exc
    if not math.isfinite(number) or number < 0.0:
        raise ValueError(f"{name} must be finite and non-negative")
    return decimal_value


def round_taifex_tax_twd(value: object) -> float:
    """Round one contract's non-negative tax to whole TWD, half up."""

    decimal_value = _non_negative_decimal(value, name="tax")
    return float(decimal_value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def taifex_tax_per_contract_twd(
    price_points: object,
    *,
    multiplier_twd_per_point: object,
    tax_rate: object,
) -> float:
    """Return a per-contract TAIFEX tax after statutory whole-TWD rounding."""

    price = _non_negative_decimal(price_points, name="price_points")
    multiplier = _non_negative_decimal(
        multiplier_twd_per_point,
        name="multiplier_twd_per_point",
    )
    rate = _non_negative_decimal(tax_rate, name="tax_rate")
    return round_taifex_tax_twd(price * multiplier * rate)


def stock_index_futures_tax_rate(trading_date: date) -> float:
    """Return the historical stock-index futures tax rate for a session."""

    if not isinstance(trading_date, date):
        raise TypeError("trading_date must be a datetime.date")
    if trading_date < TAIFEX_TAX_SCHEDULE_FIRST_SUPPORTED_DATE:
        raise ValueError(
            "stock-index futures tax schedule is not verified before "
            f"{TAIFEX_TAX_SCHEDULE_FIRST_SUPPORTED_DATE.isoformat()}"
        )
    if trading_date < TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_CHANGE_DATE:
        return TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_BEFORE_2013_04_01
    return TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_FROM_2013_04_01


def option_premium_transaction_tax_twd(
    premium_points: object,
    *,
    multiplier_twd_per_point: object,
    tax_rate: object = TAIFEX_OPTION_PREMIUM_TAX_RATE,
) -> float:
    """Tax one option trade using its premium amount as the tax base."""

    return taifex_tax_per_contract_twd(
        premium_points,
        multiplier_twd_per_point=multiplier_twd_per_point,
        tax_rate=tax_rate,
    )


def option_cash_settlement_transaction_tax_twd(
    settlement_market_price_points: object,
    *,
    settlement_date: date,
    multiplier_twd_per_point: object,
) -> float:
    """Tax one exercised index option using the settlement contract amount."""

    return taifex_tax_per_contract_twd(
        settlement_market_price_points,
        multiplier_twd_per_point=multiplier_twd_per_point,
        tax_rate=stock_index_futures_tax_rate(settlement_date),
    )


__all__ = [
    "TAIFEX_OPTION_PREMIUM_TAX_RATE",
    "TAIFEX_STOCK_INDEX_FUTURES_CURRENT_TAX_SOURCE_URL",
    "TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_BEFORE_2013_04_01",
    "TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_CHANGE_DATE",
    "TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_FROM_2013_04_01",
    "TAIFEX_STOCK_INDEX_FUTURES_2013_TAX_SOURCE_URL",
    "TAIFEX_TAX_SCHEDULE_FIRST_SUPPORTED_DATE",
    "TAIFEX_TRANSACTION_TAX_ACT_URL",
    "TAIFEX_TRANSACTION_TAX_QA_URL",
    "TAIFEX_TRANSACTION_TAX_ROUNDING_URL",
    "option_cash_settlement_transaction_tax_twd",
    "option_premium_transaction_tax_twd",
    "round_taifex_tax_twd",
    "stock_index_futures_tax_rate",
    "taifex_tax_per_contract_twd",
    "futures_transaction_tax_rate",
    "futures_transaction_tax_twd",
]
