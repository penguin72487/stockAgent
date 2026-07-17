from __future__ import annotations

import re


# Four-digit common equities, including TDR-style listed equity codes.
TW_STOCK_SYMBOL_PATTERN = r"^[1-9][0-9]{3}$"

# Passive ETFs use the 0050+ 00-series. Active ETFs use the 004xxA series.
# REIT/beneficiary securities (01), ETNs (02), warrants, rights, preferreds,
# and other exchange products are intentionally outside this universe.
TW_ETF_SYMBOL_PATTERN = r"^(?:004[0-9]{2}A|00[5-9][0-9A-Z]{1,3})$"

_TW_STOCK_SYMBOL = re.compile(TW_STOCK_SYMBOL_PATTERN)
_TW_ETF_SYMBOL = re.compile(TW_ETF_SYMBOL_PATTERN)


def classify_tw_stock_or_etf(symbol: object) -> str | None:
    normalized = str(symbol or "").strip().upper()
    if _TW_STOCK_SYMBOL.fullmatch(normalized):
        return "stock"
    if _TW_ETF_SYMBOL.fullmatch(normalized):
        return "etf"
    return None


def is_tw_stock_or_etf(symbol: object) -> bool:
    return classify_tw_stock_or_etf(symbol) is not None
