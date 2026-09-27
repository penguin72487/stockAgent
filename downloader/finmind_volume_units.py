"""Explicit stock-share units for FinMind raw partitions.

Do not infer a unit from a field named ``volume`` alone.  In particular, stock
tick/minute volume is lots for TWSE/TPEx but shares for emerging stocks, and
derivative volume is contracts rather than shares.  Only documented mappings
are eligible for a share-valued canonical field.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


SHARE_FIELDS: dict[str, tuple[str, ...]] = {
    "TaiwanStockPrice": ("Trading_Volume",),
    "TaiwanStockPriceAdj": ("Trading_Volume",),
    "TaiwanStockWeekPrice": ("trading_volume",),
    "TaiwanStockMonthPrice": ("trading_volume",),
    "TaiwanStockBlockTrade": ("volume",),
    "TaiwanStockBlockTradingDailyReport": ("buy", "sell"),
    "TaiwanStockIndustryChainMoneyFlow": ("trading_volume",),
}
COLLATERAL_PREFIXES = (
    "Margin", "SecuritiesFirmLoan", "UnrestrictedLoan",
    "SecuritiesFinanceSecuredLoan", "SettlementMargin",
)
COLLATERAL_SUFFIXES = (
    "PreviousDayBalance", "Buy", "Sell", "CashRedemption", "Replacement",
    "CurrentDayBalance", "NextDayQuota",
)
COLLATERAL_FIELDS = frozenset(
    prefix + suffix for prefix in COLLATERAL_PREFIXES for suffix in COLLATERAL_SUFFIXES
)
ORDER_BOOK_DATASET = "TaiwanStockStatisticsOfOrderBookAndTrade"
ORDER_BOOK_VOLUME_FIELDS = ("TotalBuyVolume", "TotalSellVolume", "TotalDealVolume")
ORDER_BOOK_COUNT_FIELDS = ("TotalBuyOrder", "TotalSellOrder", "TotalDealOrder")
ORDER_BOOK_CANONICAL_FIELDS = tuple(field + "_shares" for field in ORDER_BOOK_VOLUME_FIELDS) + ("TotalDealMoney_twd",)


def _order_book_number(value: Any, *, multiplier: int, whole: bool) -> int | float:
    if value is None or isinstance(value, bool):
        raise ValueError("missing or boolean order-book quantity")
    try:
        scaled = Decimal(str(value)) * multiplier
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("invalid order-book quantity") from exc
    if not scaled.is_finite() or scaled < 0:
        raise ValueError("order-book quantity must be finite and nonnegative")
    if whole:
        if scaled != scaled.to_integral_value() or scaled > 2**63 - 1:
            raise ValueError("order-book quantity must fit whole Int64 shares/counts")
        return int(scaled)
    # TWD is money, not an integer security quantity. Keep fractional TWD.
    result = float(scaled)
    if result == float("inf"):
        raise ValueError("order-book amount overflow")
    return result


def _annotate_order_book_units(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    output = []
    invalid_fields: dict[str, int] = {}
    invalid_rows = 0
    for row in rows:
        copy = dict(row)
        if set(ORDER_BOOK_CANONICAL_FIELDS) & row.keys():
            raise ValueError("provider field conflicts with canonical order-book column")
        invalid = False
        for field in (*ORDER_BOOK_VOLUME_FIELDS, "TotalDealMoney", *ORDER_BOOK_COUNT_FIELDS):
            is_volume = field in ORDER_BOOK_VOLUME_FIELDS
            is_money = field == "TotalDealMoney"
            target = field + ("_shares" if is_volume else "_twd")
            try:
                value = _order_book_number(
                    row.get(field), multiplier=1000 if is_volume else 1_000_000 if is_money else 1,
                    whole=not is_money,
                )
            except ValueError:
                value = None
                invalid = True
                invalid_fields[field] = invalid_fields.get(field, 0) + 1
            if is_volume or is_money:
                copy[target] = value
        invalid_rows += int(invalid)
        output.append(copy)
    return output, {
        "contract_version": 1,
        "stock_share_unit_status": "invalid_source_values" if invalid_rows else "documented_lots_normalized",
        "normalization_valid": invalid_rows == 0,
        "invalid_rows": invalid_rows,
        "invalid_fields": invalid_fields,
        "source_volume_units": {field: "stock_trading_lots" for field in ORDER_BOOK_VOLUME_FIELDS},
        "canonical_share_fields": {field + "_shares": "shares" for field in ORDER_BOOK_VOLUME_FIELDS},
        "source_money_units": {"TotalDealMoney": "million_TWD"},
        "canonical_money_fields": {"TotalDealMoney_twd": "TWD"},
        "source_count_units": {field: "orders" for field in ORDER_BOOK_COUNT_FIELDS},
        "multipliers": {**{field: 1000 for field in ORDER_BOOK_VOLUME_FIELDS}, "TotalDealMoney": 1_000_000},
        "market_scope": "twse_regular_trading_only",
        "excluded_mechanisms": ["odd_lot", "block_trade", "after_hours_fixed_price", "auction", "tender_offer"],
        "unit_source": "https://finmind.github.io/tutor/TaiwanMarket/Technical/",
    }


def _thousand_shares(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("boolean is not a stock share quantity")
    try:
        shares = Decimal(str(value)) * 1000
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("invalid thousand-share quantity") from exc
    if not shares.is_finite() or shares != shares.to_integral_value():
        raise ValueError("thousand-share quantity cannot resolve to whole shares")
    if shares < 0 or shares > 2**63 - 1:
        raise ValueError("thousand-share quantity must fit nonnegative Int64 shares")
    return int(shares)


def annotate_stock_share_units(
    dataset: str, rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Preserve raw provider values; add only evidence-backed share columns."""

    if dataset == ORDER_BOOK_DATASET:
        return _annotate_order_book_units(rows)
    if dataset == "TaiwanStockLoanCollateralBalance":
        output: list[dict[str, Any]] = []
        observed: set[str] = set()
        for row in rows:
            copy = dict(row)
            for field in COLLATERAL_FIELDS & row.keys():
                if field + "_shares" in copy:
                    raise ValueError("provider field conflicts with canonical share column")
                copy[field + "_shares"] = _thousand_shares(row[field])
                observed.add(field)
            output.append(copy)
        return output, {
            "stock_share_unit_status": "documented_thousand_shares_normalized",
            "source_volume_units": {field: "thousand_shares" for field in sorted(observed)},
            "canonical_share_fields": {field + "_shares": "shares" for field in sorted(observed)},
            "unit_source": "https://finmind.github.io/tutor/TaiwanMarket/Chip/",
        }
    if dataset in SHARE_FIELDS:
        present = sorted(set(SHARE_FIELDS[dataset]) & {
            field for row in rows for field in row
        })
        return rows, {
            "stock_share_unit_status": "provider_fields_already_shares",
            "source_volume_units": {field: "shares" for field in present},
            "canonical_share_fields": {field: "shares" for field in present},
            "unit_source": (
                "https://finmind.github.io/tutor/TaiwanMarket/Technical/"
                if dataset in {"TaiwanStockPrice", "TaiwanStockPriceAdj",
                               "TaiwanStockWeekPrice", "TaiwanStockMonthPrice"}
                else "https://finmind.github.io/tutor/TaiwanMarket/Chip/"
            ),
        }
    return rows, {"stock_share_unit_status": "not_mapped_do_not_assume_shares"}
