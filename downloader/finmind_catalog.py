"""Canonical Complement dataset catalog, without importing its worker.

This preserves source membership, ordering and native partition identities.
Public readers import the catalog; the collector imports the same objects.
Entitlement, release verification, requests and queue execution stay in their
existing implementations, not in this module.
"""

from __future__ import annotations

from downloader.finmind_scheduling import PRODUCT_HISTORY_STARTS
from downloader import finmind_supplemental as supplemental
from downloader import finmind_news as news


SNAPSHOTS = (
    "TaiwanStockInfo", "TaiwanSecuritiesTraderInfo", "TaiwanStockActiveETFInfo",
    "TaiwanFutOptDailyInfo", "USStockInfo", "UKStockInfo", "EuropeStockInfo",
    "JapanStockInfo",
    "TaiwanFutOptTickInfo", "taiwan_stock_tick_snapshot",
    "taiwan_futures_snapshot", "taiwan_options_snapshot",
)
GLOBAL_HISTORY = (
    "TaiwanStockTotalMarginPurchaseShortSale",
    "TaiwanStockTotalInstitutionalInvestors",
    "TaiwanStockCapitalReductionReferencePrice", "TaiwanStockDelisting",
    "TaiwanStockSplitPrice", "TaiwanStockParValueChange",
    "TaiwanFuturesDealerTradingVolumeDaily", "TaiwanOptionDealerTradingVolumeDaily",
    "TaiwanExchangeRate", "GoldPrice",
)
# Free per-ID query contracts must not become a whole-market entitlement.
PER_ID_REQUIRED = {
    "TaiwanStockCapitalReductionReferencePrice": "stock",
    "TaiwanFuturesDealerTradingVolumeDaily": "futures",
    "TaiwanOptionDealerTradingVolumeDaily": "options",
    "TaiwanExchangeRate": "currency",
}
CURRENCIES = (
    "USD", "EUR", "JPY", "GBP", "CNY", "HKD", "AUD", "CAD", "CHF", "IDR",
    "KRW", "MYR", "NZD", "PHP", "SEK", "SGD", "THB", "VND", "ZAR",
)
GLOBAL_START_YEAR = {
    "TaiwanStockTotalMarginPurchaseShortSale": 2001,
    "TaiwanStockTotalInstitutionalInvestors": 2004,
    "TaiwanStockDelisting": 2001,
    "TaiwanStockSplitPrice": 1900,  # Search floor, not verified first year.
    # Docs say 2020, but a verified local API receipt contains 2019-09-09.
    "TaiwanStockParValueChange": 1900,
    "GoldPrice": 1900,  # Search floor, not verified first year.
}
BULK_GLOBAL_HISTORY = frozenset(GLOBAL_START_YEAR)
GLOBAL_RELEASE_HOUR_TAIPEI = {
    "TaiwanStockTotalMarginPurchaseShortSale": 21,
    "TaiwanStockTotalInstitutionalInvestors": 15,
    # A daily request budget for event tables, not an actual publication proof.
    "TaiwanStockDelisting": 23,
    "TaiwanStockSplitPrice": 18,
    "TaiwanStockParValueChange": 14,
}
TW_SYMBOL_HISTORY = (
    "TaiwanStockPrice", "TaiwanStockPriceAdj", "TaiwanStockPER",
    "TaiwanStockDayTrading", "TaiwanStockPriceLimit",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockShareholding",
    "TaiwanStockSecuritiesLending", "TaiwanStockMarginShortSaleSuspension",
    "TaiwanDailyShortSaleBalances", "TaiwanStockFinancialStatements",
    "TaiwanStockBalanceSheet", "TaiwanStockCashFlowsStatement",
    "TaiwanStockDividend", "TaiwanStockDividendResult", "TaiwanStockMonthRevenue",
)
LONG_INSTITUTIONAL = "TaiwanStockInstitutionalInvestorsBuySell"
WIDE_INSTITUTIONAL = "TaiwanStockInstitutionalInvestorsBuySellWide"
INSTITUTIONAL_NAMES = (
    "Foreign_Investor", "Foreign_Dealer_Self", "Investment_Trust",
    "Dealer", "Dealer_self", "Dealer_Hedging",
)
DERIVATIVE_HISTORY = (
    "TaiwanFuturesDaily", "TaiwanOptionDaily",
    "TaiwanFuturesInstitutionalInvestors", "TaiwanOptionInstitutionalInvestors",
    *PRODUCT_HISTORY_STARTS,
)
GLOBAL_EQUITY_HISTORY = {
    "USStockPrice": "USStockInfo",
    "UKStockPrice": "UKStockInfo",
    "EuropeStockPrice": "EuropeStockInfo",
    "JapanStockPrice": "JapanStockInfo",
}
LIVE_SNAPSHOT_ENDPOINTS = frozenset({
    "taiwan_stock_tick_snapshot", "taiwan_futures_snapshot", "taiwan_options_snapshot",
})
DERIVATIVE_SNAPSHOTS = frozenset({"taiwan_futures_snapshot", "taiwan_options_snapshot"})
FIXED_ID_HISTORY = {
    "TaiwanStockTotalReturnIndex": ("TAIEX", "TPEx"),
    "InterestRate": ("FED", "ECB", "BOJ", "BOE", "RBA", "PBOC", "BOC", "RBNZ", "RBI", "CBR", "BCB", "SNB"),
    "CrudeOilPrices": ("WTI", "Brent"),
    "GovernmentBondsYield": tuple(
        f"United States {term}" for term in
        ("1-Month", "3-Month", "6-Month", "1-Year", "2-Year", "3-Year",
         "5-Year", "7-Year", "10-Year", "20-Year", "30-Year")
    ),
}
ALL_DATASETS = (
    *SNAPSHOTS[:4], *GLOBAL_HISTORY[:8], *TW_SYMBOL_HISTORY, WIDE_INSTITUTIONAL,
    *DERIVATIVE_HISTORY, "TaiwanStockTotalReturnIndex",
    *SNAPSHOTS[4:], *GLOBAL_EQUITY_HISTORY,
    *GLOBAL_HISTORY[8:], "InterestRate", "CrudeOilPrices", "GovernmentBondsYield",
    *supplemental.SOURCES, news.DATASET,
)
assert len(ALL_DATASETS) == len(set(ALL_DATASETS)) == 69
