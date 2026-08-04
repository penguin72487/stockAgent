from __future__ import annotations


# These datasets are not part of the canonical 13:30 close receipt. They may
# publish later even after TWSE/TPEx OHLCV is complete. A daily run records
# their missing current-session rows but may continue building close-time
# artifacts; consumers that require an execution-specific dataset must check
# that dataset independently.
DAILY_CLOSE_OPTIONAL_DATASETS = frozenset(
    {
        "twse_daily_valuation",
        "tpex_daily_valuation",
        "twse_institutional_trades",
        "tpex_institutional_trades",
        "twse_margin_balance",
        "tpex_margin_balance",
        "twse_day_trade_eligibility",
    }
)

DAILY_CLOSE_CORE_DATASETS = frozenset(
    {
        "twse_daily_ohlcv",
        "tpex_daily_ohlcv",
    }
)
