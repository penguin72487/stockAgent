"""Unit contract for FinLab whole-table quantities, without changing raw data.

The provider key names a *field*, not a universal numeric unit.  A stock lot
must not be silently treated as 1,000 shares: the trading unit can vary by
security and time.  Consumers may use a share-valued field directly only when
``canonical_unit`` is ``shares``; all other quantities need an explicit,
verified per-symbol conversion before entering a stock-share feature.
"""

from __future__ import annotations


def volume_unit_contract(key: str) -> dict | None:
    """Classify quantity keys; return None for unrelated price/count fields."""

    if ":" not in key:
        return None
    family, field = key.split(":", 1)
    if family == "security_lending" and field in {
        "前日借券餘額", "借券", "借券還券", "借券增減", "借券餘額",
    }:
        return {
            "source_unit": "shares", "canonical_unit": "shares",
            "normalization": "identity", "evidence": "provider_documentation",
            "unit_source": "https://finlab.finance/data/securities-lending",
        }
    if "股數" in field and not any(mark in field for mark in ("占比", "比重", "比例")):
        return {
            "source_unit": "shares",
            "canonical_unit": "shares",
            "normalization": "identity",
            "evidence": "provider_field_name",
        }
    if "張數" in field:
        if family == "after_market_fixed_price" and field == "成交張數":
            return {
                "source_unit": "stock_trading_lots",
                "canonical_unit": None,
                "normalization": "on_demand_verified_price_amount_share_view",
                "share_view": "scripts.finlab_fixed_price_shares.load_fixed_price_shares",
                "evidence": "same_date_symbol_trade_price_and_amount",
            }
        if family in {"cb_price", "cb_converted_status"}:
            return {
                "source_unit": "convertible_bond_lots",
                "canonical_unit": None,
                "normalization": "not_stock_shares",
                "evidence": "provider_field_name_and_instrument",
            }
        return {
            "source_unit": "stock_trading_lots",
            "canonical_unit": None,
            "normalization": "requires_verified_symbol_date_trading_unit",
            "evidence": "provider_field_name",
        }
    # These are screening/eligibility flags about unusually high turnover,
    # rather than a measured quantity of shares or lots.
    if family == "margin_trading_adjustment" and field.startswith("成交量"):
        return None
    if ("volume" in field.lower() or "成交量" in field
            or "成交總量" in field or "成交數量" in field):
        return {
            "source_unit": "contracts" if family == "futures_price" else "provider_native_unknown",
            "canonical_unit": None,
            "normalization": "not_stock_shares" if family == "futures_price" else "requires_provider_unit_proof",
            "evidence": "provider_field_name_and_instrument" if family == "futures_price" else "provider_field_name_only",
        }
    return None
