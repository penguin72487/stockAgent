"""Shared FinMind source identity and fixed-refresh demand; no worker imports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from stockagent.live.market_status import tw_stock_day_decision

TAIPEI = ZoneInfo("Asia/Taipei")


def protected_stock_opening(now: datetime, *, day_decision=tw_stock_day_decision) -> bool:
    """Protect real/unknown stock openings without idling on a verified holiday."""
    local = now.astimezone(TAIPEI)
    if not ((local.hour == 8 and local.minute >= 20) or (local.hour == 9 and local.minute < 10)):
        return False
    decision = day_decision(local.date(), parquet_root=Path(__file__).resolve().parents[1] / "data_tw_public",
                            observed=now)
    if decision.status == "closed":
        return False
    return decision.is_session or local.weekday() < 5


@dataclass(frozen=True)
class Source:
    dataset: str
    first_date: date | None
    grain: str  # snapshot, day, two_day, month, year
    priority: int = 2
    release_hour: int = 14
    release_minute: int = 0


def _s(dataset: str, first: str | None, grain: str, priority: int = 2,
       release_hour: int = 14, release_minute: int = 0) -> Source:
    return Source(dataset, date.fromisoformat(first) if first else None,
                  grain, priority, release_hour, release_minute)


# Entire-market queries documented for Backer/Sponsor. Most accept one date
# only, including financial period anchors. Monthly/annual windows below are
# reserved for genuine range endpoints. Never fetch an open-ended market history.
SOURCES = (
    _s("TaiwanStockPrice", "1994-10-01", "day", 1, 18),
    _s("TaiwanStockPriceAdj", "1994-10-01", "day", 2, 20),
    _s("TaiwanStockDayTrading", "2014-01-01", "day", 1, 18),
    _s("TaiwanStockPriceLimit", "2000-01-01", "day", 1, 18),
    _s("TaiwanStockMarginPurchaseShortSale", "2001-01-01", "day", 1, 21),
    _s("TaiwanStockInstitutionalInvestorsBuySell", "2005-01-01", "day", 1, 18),
    _s("TaiwanStockInstitutionalInvestorsBuySellWide", "2005-01-01", "day", 2, 18),
    _s("TaiwanStockShareholding", "2004-02-01", "day", 2, 21),
    _s("TaiwanStockSecuritiesLending", "2001-05-01", "day", 2, 15),
    _s("TaiwanStockMarginShortSaleSuspension", "2015-01-01", "year"),
    _s("TaiwanDailyShortSaleBalances", "2005-07-01", "day"),
    _s("TaiwanStockFinancialStatements", "1990-03-01", "day", 1),
    _s("TaiwanStockBalanceSheet", "2011-12-01", "day", 1),
    _s("TaiwanStockCashFlowsStatement", "2008-06-01", "day", 1),
    _s("TaiwanStockDividend", "2005-05-01", "day", 1),
    _s("TaiwanStockDividendResult", "2003-05-01", "day", 1),
    _s("TaiwanStockMonthRevenue", "2002-02-01", "day", 1),
    _s("TaiwanStockCapitalReductionReferencePrice", "2011-01-01", "year"),
    _s("TaiwanFuturesDaily", "1998-07-01", "day", 1),
    _s("TaiwanOptionDaily", "2001-12-01", "day", 1),
    _s("TaiwanFuturesInstitutionalInvestors", "2018-06-05", "day", 1),
    _s("TaiwanOptionInstitutionalInvestors", "2018-06-05", "day", 1),
    _s("TaiwanFuturesDealerTradingVolumeDaily", "2021-04-01", "day"),
    _s("TaiwanOptionDealerTradingVolumeDaily", "2021-04-01", "day"),
    _s("TaiwanStock10Year", "2011-01-24", "day"),
    _s("TaiwanStockInfoWithWarrantSummary", "2011-01-03", "month"),
    _s("TaiwanStockWeekPrice", "2000-01-01", "day"),
    _s("TaiwanStockMonthPrice", "2000-01-01", "day"),
    _s("TaiwanStockEvery5SecondsIndex", "2005-01-03", "day", 4),
    _s("TaiwanStockSuspended", "2011-10-06", "year"),
    _s("TaiwanStockDayTradingSuspension", "2014-06-01", "year"),
    _s("TaiwanStockHoldingSharesPer", "2010-01-29", "day"),
    _s("TaiwanStockGovernmentBankBuySell", "2021-06-30", "day", 2, 23, 30),
    _s("TaiwanTotalExchangeMarginMaintenance", "2001-01-05", "year", 1, 21),
    _s("TaiwanStockBlockTradingDailyReport", "2026-04-28", "day", 2, 21),
    _s("TaiwanStockBlockTrade", "2005-04-04", "day"),
    _s("TaiwanStockLoanCollateralBalance", "2006-10-02", "day"),
    _s("TaiwanStockActiveETFHolding", "2025-05-05", "day"),
    _s("TaiwanStockActiveETFHoldingChange", "2025-05-05", "day"),
    _s("TaiwanStockIndustryChainMoneyFlow", "1992-01-04", "day"),
    _s("TaiwanStockMarginMaintenance", "2001-01-05", "day", 2, 22, 30),
    _s("TaiwanStockDispositionSecuritiesPeriod", "2001-01-01", "year"),
    _s("TaiwanStockMarketValue", "2004-01-01", "day"),
    _s("TaiwanStockMarketValueWeight", "2024-10-30", "day"),
    _s("TaiwanFuturesInstitutionalInvestorsAfterHours", "2021-10-12", "day"),
    _s("TaiwanOptionInstitutionalInvestorsAfterHours", "2021-10-12", "day"),
    _s("TaiwanFuturesOpenInterestLargeTraders", "1998-07-01", "day"),
    _s("TaiwanOptionOpenInterestLargeTraders", "1998-07-01", "day"),
    _s("TaiwanFuturesFinalSettlementPrice", "1998-01-01", "year"),
    _s("TaiwanOptionFinalSettlementPrice", "2001-01-01", "year"),
    _s("TaiwanOptionVix", "2026-03-01", "month", 2, 18),
    _s("TaiwanStockConvertibleBondInfo", None, "snapshot"),
    _s("TaiwanStockConvertibleBondDaily", "2011-01-01", "day"),
    _s("TaiwanStockConvertibleBondInstitutionalInvestors", "2011-01-01", "day"),
    _s("TaiwanStockConvertibleBondDailyOverview", "2011-01-01", "day"),
    _s("TaiwanStockConvertibleBondPutProvision", "2011-06-22", "year"),
    _s("TaiwanBusinessIndicator", "1982-01-01", "year"),
    _s("TaiwanStockIndustryChain", None, "snapshot"),
    _s("CnnFearGreedIndex", "2011-01-03", "year"),
)
assert len({source.dataset for source in SOURCES}) == len(SOURCES)
SPECS = {source.dataset: source for source in SOURCES}
# These provider dates are exchange sessions, unlike fundamentals, dividends,
# revenue, and issuer events, which can legitimately have non-session dates.
# Use only a receipt-verified official calendar; never infer sessions from
# weekdays (Taiwan had historical Saturday sessions).
SESSION_DAY_DATASETS = frozenset({
    "TaiwanStockPrice", "TaiwanStockPriceAdj", "TaiwanStockDayTrading",
    "TaiwanStockPriceLimit", "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockInstitutionalInvestorsBuySellWide",
    "TaiwanStockEvery5SecondsIndex", "TaiwanStockMarketValue",
    # Trade/session-accounting dates, not issuer publication dates. See the
    # Chip, Technical and ConvertibleBond provider documentation and the
    # 2026-09-27 calendar-conflict audit. Existing nonempty conflicts disable
    # pruning for the whole series; dates outside calendar proof remain due.
    "TaiwanStockShareholding", "TaiwanStockSecuritiesLending",
    "TaiwanDailyShortSaleBalances", "TaiwanStockGovernmentBankBuySell",
    "TaiwanStock10Year", "TaiwanStockBlockTrade", "TaiwanStockLoanCollateralBalance",
    "TaiwanStockIndustryChainMoneyFlow", "TaiwanStockMarginMaintenance",
    "TaiwanStockConvertibleBondDaily", "TaiwanStockConvertibleBondInstitutionalInvestors",
})


def fixed_incremental_demand(root: Path, now: datetime, *,
                             sources: tuple[Source, ...] = SOURCES,
                             session_datasets: frozenset[str] = SESSION_DAY_DATASETS,
                             day_decision=tw_stock_day_decision) -> int:
    """Lower-bound calls protected for named FinMind fixed-refresh jobs.

    Priority-0 tasks are current-period work. Look ahead one quota window so
    historical jobs cannot fill the hour immediately before a known release.
    The Free worker has two session series plus calendar/master checks.
    """

    free_checks = 4
    horizon = now + timedelta(hours=1)

    def due_priority_zero(path: Path) -> int:
        if not path.is_file():
            return 0
        try:
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0) as conn:
                return int(conn.execute(
                    "SELECT count(*) FROM tasks WHERE priority=0 AND kind!='derived' AND "
                    "((state='pending' AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?)) "
                    "OR (state IN ('complete','observed_empty','failed') "
                    "AND next_attempt_at_utc<=?))",
                    (horizon.isoformat(), horizon.isoformat()),
                ).fetchone()[0])
        except sqlite3.Error:
            # A locked/migrating queue is not evidence that demand vanished.
            return len(sources)

    local_now = now.astimezone(TAIPEI)
    local_horizon = horizon.astimezone(TAIPEI)
    upcoming = 0
    for day in {local_now.date(), local_horizon.date()}:
        stock_day = day_decision(
            day, parquet_root=Path(__file__).resolve().parents[1] / "data_tw_public",
            observed=now,
        )
        for spec in sources:
            if spec.grain not in {"day", "two_day"}:
                continue
            if spec.dataset in session_datasets and stock_day.status == "closed":
                continue
            boundary = datetime.combine(day, datetime.min.time(), tzinfo=TAIPEI) + timedelta(
                hours=spec.release_hour, minutes=spec.release_minute,
            )
            upcoming += local_now < boundary <= local_horizon
    return (
        free_checks + upcoming
        + due_priority_zero(root / "sponsor" / "queue.sqlite3")
        + due_priority_zero(root / "complement" / "queue.sqlite3")
    )
