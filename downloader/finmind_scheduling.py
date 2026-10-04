"""Shared FinMind source identity and fixed-refresh demand; no worker imports."""

from __future__ import annotations

from dataclasses import dataclass, replace
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import json
import sqlite3
from zoneinfo import ZoneInfo

from stockagent.live.market_status import tw_stock_day_decision

TAIPEI = ZoneInfo("Asia/Taipei")
RELEASE_CLOCK_VERSION = 2
# Primary tutorial pages, checked 2026-09-30. These are expected release
# boundaries, not actual publication evidence; the observation ledger owns it.
RELEASE_CLOCKS: dict[str, dict] = {}


def _clocks(topic: str, values: dict[str, tuple[int, int]], *, saturday: bool = False) -> None:
    for dataset, (hour, minute) in values.items():
        RELEASE_CLOCKS[dataset] = {
            'hour': hour, 'minute': minute, 'weekdays': list(range(6 if saturday else 5)),
            'basis': 'official_schedule_not_actual_publication', 'timezone': 'Asia/Taipei',
            'source_url': f'https://finmind.github.io/tutor/TaiwanMarket/{topic}/',
            'label': f"{'週一至六' if saturday else '週一至五'} {hour:02}:{minute:02}",
        }


_clocks('Technical', {
    'TaiwanStockTradingDate': (18, 0), 'TaiwanStockPrice': (17, 30),
    'TaiwanStockPriceAdj': (17, 30), 'TaiwanStockWeekPrice': (17, 30),
    'TaiwanStockMonthPrice': (17, 30), 'TaiwanStockPriceTick': (15, 30),
    'TaiwanStockPER': (18, 0), 'TaiwanStockPriceLimit': (18, 0),
    'TaiwanStock10Year': (20, 0), 'TaiwanStockKBar': (15, 50),
    'TaiwanStockTotalReturnIndex': (16, 50),
})
_clocks('Chip', {
    'TaiwanStockMarginPurchaseShortSale': (21, 0), 'TaiwanStockTotalMarginPurchaseShortSale': (21, 0),
    'TaiwanStockInstitutionalInvestorsBuySell': (20, 0), 'TaiwanStockInstitutionalInvestorsBuySellWide': (20, 0),
    'TaiwanStockTotalInstitutionalInvestors': (15, 0), 'TaiwanStockShareholding': (21, 0),
    'TaiwanStockSecuritiesLending': (15, 0), 'TaiwanStockMarginShortSaleSuspension': (21, 0),
    'TaiwanDailyShortSaleBalances': (21, 0), 'TaiwanStockGovernmentBankBuySell': (23, 30),
    'TaiwanTotalExchangeMarginMaintenance': (21, 0), 'TaiwanStockTradingDailyReport': (21, 0),
    'TaiwanStockTradingDailyReportSecIdAgg': (21, 0),
    # This worker uses the broker-ID endpoint, not the stock-ID endpoint.
    'TaiwanStockWarrantTradingDailyReport': (23, 0),
})
_clocks('Chip', {'TaiwanStockMarginMaintenance': (22, 30)}, saturday=True)
_clocks('Fundamental', {'TaiwanStockMarketValue': (23, 30), 'TaiwanStockDelisting': (23, 30),
                        'TaiwanStockSplitPrice': (18, 0)})
_clocks('Derivative', {
    'TaiwanFuturesDaily': (16, 30), 'TaiwanOptionDaily': (16, 30), 'TaiwanFuturesKBar': (16, 30),
    'TaiwanFuturesInstitutionalInvestors': (18, 0), 'TaiwanOptionInstitutionalInvestors': (16, 0),
    'TaiwanFuturesDealerTradingVolumeDaily': (19, 0), 'TaiwanOptionDealerTradingVolumeDaily': (18, 0),
    'TaiwanFuturesOpenInterestLargeTraders': (16, 30), 'TaiwanOptionOpenInterestLargeTraders': (16, 30),
    'TaiwanFuturesTick': (6, 0), 'TaiwanOptionTick': (6, 0),
})
_clocks('Derivative', {'TaiwanFuturesInstitutionalInvestorsAfterHours': (5, 0),
                       'TaiwanOptionInstitutionalInvestorsAfterHours': (5, 0),
                       'TaiwanOptionVix': (18, 0)}, saturday=True)
_clocks('ConvertibleBond', {'TaiwanStockConvertibleBondPutProvision': (19, 0)})
_clocks('ConvertibleBond', {'TaiwanStockConvertibleBondMonthlyAnalysis': (18, 0)}, saturday=True)


def release_details(dataset: str) -> dict:
    if dataset in RELEASE_CLOCKS:
        return {'contract_version': RELEASE_CLOCK_VERSION, **RELEASE_CLOCKS[dataset]}
    if dataset == 'TaiwanStockDayTrading':
        return {'contract_version': RELEASE_CLOCK_VERSION, 'label': '盤前名單（18:00 補查）；成交量值 21:30',
                'basis': 'official_multi_phase_schedule', 'timezone': 'Asia/Taipei',
                'phases': [{'hour': 18, 'minute': 0, 'basis': 'existing_poll_policy_not_publication'},
                           {'hour': 21, 'minute': 30, 'basis': 'official_final_field_schedule'}],
                'source_url': 'https://finmind.github.io/tutor/TaiwanMarket/Technical/'}
    spec = SPECS.get(dataset)
    return {'contract_version': RELEASE_CLOCK_VERSION,
            'label': (f'每日 {spec.release_hour:02}:{spec.release_minute:02} 檢查（發布時間未確認）' if spec else
                      '依既有追新週期（發布時間未確認）'),
            'basis': 'polling_policy_not_publication', 'timezone': 'Asia/Taipei'}


def next_release_check(dataset: str, now: datetime) -> datetime | None:
    """Next release/check phase; weekdays are NOT historical exclusions."""
    if dataset == 'TaiwanStockDayTrading':
        local = now.astimezone(TAIPEI)
        for offset in range(8):
            day = local + timedelta(days=offset)
            if day.weekday() >= 5:
                continue
            for hour, minute in ((18, 0), (21, 30)):
                candidate = day.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate > local:
                    return candidate.astimezone(UTC)
        return None
    clock = RELEASE_CLOCKS.get(dataset)
    if clock is None:
        return None
    local = now.astimezone(TAIPEI)
    candidate = local.replace(hour=clock['hour'], minute=clock['minute'], second=0, microsecond=0)
    while candidate <= local or candidate.weekday() not in clock['weekdays']:
        candidate += timedelta(days=1)
    return candidate.astimezone(UTC)


def reconcile_release_deadlines(conn: sqlite3.Connection, now: datetime) -> None:
    """Move already-seeded current jobs to the new clock, with audit evidence.

    Changing constants alone leaves old four-hour deadlines in a live queue.
    Failures/holds are untouched, and a successful pre-release sample cannot
    suppress the later documented release or day-trading final-field update.
    """
    conn.execute('CREATE TABLE IF NOT EXISTS finmind_release_clock_migrations ('
                 'version INTEGER,dataset TEXT,partition TEXT,prior_deadline TEXT,new_deadline TEXT,'
                 'changed_at_utc TEXT,PRIMARY KEY(version,dataset,partition))')
    local = now.astimezone(TAIPEI)
    rows = conn.execute("SELECT dataset,partition,state,last_attempt_at_utc,next_attempt_at_utc FROM tasks "
                        "WHERE priority=0 AND kind!='derived' AND state IN ('complete','observed_empty') "
                        "AND partition>=?", (f'{local.year}-01-01',)).fetchall()
    for dataset, partition, state, attempted, prior in rows:
        spec = SPECS.get(dataset)
        try:
            day = date.fromisoformat(partition)
        except ValueError:
            continue
        if spec is None or (spec.grain == 'day' and day != local.date()) or (
                spec.grain == 'two_day' and day + timedelta(days=2) <= local.date()) or (
                spec.grain == 'month' and (day.year, day.month) != (local.year, local.month)):
            continue
        clock = RELEASE_CLOCKS.get(dataset)
        if dataset == 'TaiwanStockDayTrading':
            clock = {'hour': 21, 'minute': 30, 'weekdays': list(range(5))}
        if not clock or conn.execute('SELECT 1 FROM finmind_release_clock_migrations '
                                     'WHERE version=? AND dataset=? AND partition=?',
                                     (RELEASE_CLOCK_VERSION, dataset, partition)).fetchone():
            continue
        stamp = _stamp(attempted)
        if stamp is None or stamp > now:
            continue
        release = local.replace(hour=clock['hour'], minute=clock['minute'], second=0, microsecond=0)
        due = _stamp(prior)
        if release.weekday() in clock['weekdays'] and stamp < release:
            due = min(due, release.astimezone(UTC)) if due else release.astimezone(UTC)
        elif state == 'complete':
            due = next_release_check(dataset, stamp)
        if due is None:
            continue
        conn.execute('INSERT INTO finmind_release_clock_migrations VALUES (?,?,?,?,?,?)',
                     (RELEASE_CLOCK_VERSION, dataset, partition, prior, due.isoformat(), now.isoformat()))
        conn.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE dataset=? AND data_id='' AND partition=?",
                     (due.isoformat(), dataset, partition))
OVERSEAS_DAILY_DATASETS = frozenset({
    "USStockPrice", "UKStockPrice", "EuropeStockPrice", "JapanStockPrice",
})


def _unspent_overseas_share(root: Path, now: datetime) -> int:
    """Protect the unspent rolling-hour share, not the same calls twice.

    This is a local scheduling guarantee, not an estimate of provider quota.
    Failed attempts also consume capacity. Missing/unreadable traffic grants
    no credit; the separate verified-account budget remains authoritative.
    """
    try:
        quota = json.loads((root / "account_status.json").read_bytes()).get("official_requests_per_hour", 600)
        if type(quota) is not int or quota <= 0:
            quota = 600
    except (OSError, ValueError, AttributeError):
        quota = 600
    share = max(1, quota // 4)
    ledger = root / "request_traffic.sqlite3"
    if not ledger.is_file():
        return share
    current = now.astimezone(UTC)
    try:
        with closing(sqlite3.connect(ledger.resolve().as_uri() + "?mode=ro", uri=True, timeout=2.0)) as conn:
            # Use the existing time index; never scan the lifetime ledger.
            served = conn.execute(
                "SELECT count(*) FROM requests WHERE started_at_utc>? AND started_at_utc<=? "
                "AND dataset IN (?,?,?,?)",
                ((current - timedelta(hours=1)).isoformat(), current.isoformat(),
                 *sorted(OVERSEAS_DAILY_DATASETS)),
            ).fetchone()[0]
    except sqlite3.Error:
        return share
    return max(0, share - served)

# No-ID settlement queries returned empty while explicit TX/TXO queries over
# the same full span returned rows. The Complement product-history worker owns
# these endpoints, not Sponsor's whole-market daily/year partition queue.
# Evidence: finmind_max_ranges_20260927T024357385865Z.json.
PRODUCT_HISTORY_STARTS = {
    "TaiwanFuturesFinalSettlementPrice": date(1998, 1, 1),
    "TaiwanOptionFinalSettlementPrice": date(2001, 1, 1),
}


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


def latest_released_stock_session(now: datetime, *, public_root: Path,
                                  day_decision=tw_stock_day_decision) -> date | None:
    """Expected session for the existing Free worker's 14:00 release boundary.

Wall-clock age alone marks long holidays stale. A missing calendar, however,
must remain unknown rather than optimistically skipping an unverified day.
"""
    local = now.astimezone(TAIPEI)
    candidate = local.date() if local.hour >= 14 else local.date() - timedelta(days=1)
    for _ in range(370):
        decision = day_decision(candidate, parquet_root=public_root, observed=now)
        if decision.status == 'unknown':
            return None
        if decision.is_session:
            return candidate
        candidate -= timedelta(days=1)
    return None


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
    _s("TaiwanStockPrice", "1994-10-01", "day", 1, 17, 30),
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
SOURCES = tuple(replace(source, release_hour=RELEASE_CLOCKS[source.dataset]['hour'],
                        release_minute=RELEASE_CLOCKS[source.dataset]['minute'])
                if source.dataset in RELEASE_CLOCKS else source for source in SOURCES)
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


def _read_metadata(path: Path) -> dict:
    try:
        with path.open('rb') as stream:
            body = stream.read(512 * 1024 + 1)
        value = json.loads(body) if len(body) <= 512 * 1024 else {}
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _stamp(value: object) -> datetime | None:
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return stamp.astimezone(UTC) if stamp.tzinfo else None
    except (ValueError, TypeError):
        return None


def active_sponsor_aliases(root: Path, now: datetime, candidates: set[str]) -> frozenset[str]:
    """The same liveness/entitlement gate for dispatch and reservation aliases."""
    from downloader.finmind_runtime import idle_heartbeat
    status = _read_metadata(root / 'sponsor' / 'status.json')
    observed = _stamp(status.get('observed_at_utc'))
    state = status.get('state')
    max_age = timedelta(minutes=35 if state in {'rate_limited', 'ip_banned'} else 15)
    if (observed is None or observed > now + timedelta(minutes=1)
            or (now - observed > max_age and not idle_heartbeat(root / 'sponsor', status, now)['alive'])
            or status.get('tier') not in {'Sponsor', 'SponsorPro'}
            or state not in {'running', 'batch_complete', 'current_queue', 'protected_opening',
                             'incremental_reserve', 'waiting_necessary_acquisition', 'rate_limited', 'ip_banned'}):
        return frozenset()
    series = status.get('series', {})
    if not isinstance(series, dict):
        return frozenset()
    return frozenset(name for name in candidates if isinstance(series.get(name), dict)
                     and series[name].get('target', 0) > 0 and not series[name].get('blocked', 0))


def calendar_next_check(root: Path, now: datetime) -> datetime:
    """Hourly freshness plus the documented weekday 18:00 publication boundary.

    A failed check does not make the calendar fresh. Its short retry is separate
    evidence, so an unavailable provider cannot create a tight retry loop.
    """
    observed = _stamp(_read_metadata(root / 'calendar.json').get('observed_at_utc'))
    if observed is None or observed > now:
        due = now
    else:
        due = observed + timedelta(hours=1)
        local = observed.astimezone(TAIPEI)
        release = local.replace(hour=18, minute=0, second=0, microsecond=0)
        if release <= local:
            release += timedelta(days=1)
        while release.weekday() >= 5:
            release += timedelta(days=1)
        due = min(due, release.astimezone(UTC))
    attempt = _read_metadata(root / 'calendar_refresh.json')
    retry = _stamp(attempt.get('retry_at_utc'))
    attempted = _stamp(attempt.get('observed_at_utc'))
    if retry and attempted and attempted <= now < retry and (observed is None or attempted > observed):
        due = max(due, retry)
    return due


def _free_refresh_events(root: Path, now: datetime) -> list[dict]:
    """Inspect a few receipt heads, never scan the historical Parquet archive."""
    events = [{'owner': 'free', 'dataset': 'TaiwanStockTradingDate',
               'due_at': calendar_next_check(root, now), 'requests': 1}]
    local = now.astimezone(TAIPEI)
    boundary = local.replace(hour=14, minute=0, second=0, microsecond=0)
    candidates = [('TaiwanStockInfoWithWarrant', local.date(), boundary)]
    calendar = _read_metadata(root / 'calendar.json')
    cutoff = local.date() if local >= boundary else local.date() - timedelta(days=1)
    raw_dates = calendar.get('dates')
    dates = [day for day in raw_dates if isinstance(day, str) and day <= str(cutoff)] if isinstance(raw_dates, list) else []
    try:
        latest = date.fromisoformat(max(dates)) if dates else None
    except ValueError:
        latest = None  # Free's calendar validation owns repair, not this budget read.
    if latest is not None:
        release = datetime.combine(latest, datetime.min.time(), TAIPEI) + timedelta(hours=14)
        candidates.extend((dataset, latest, release) for dataset in (
            'TaiwanStockStatisticsOfOrderBookAndTrade', 'TaiwanVariousIndicators5Seconds'))
    for dataset, day, release in candidates:
        receipt = _read_metadata(root / 'receipts' / dataset / f'{day}.json')
        if receipt.get('status') == 'complete':
            continue
        retry = _stamp(receipt.get('retry_at_utc'))
        events.append({'owner': 'free', 'dataset': dataset,
                       'due_at': max(release.astimezone(UTC), retry) if retry else release.astimezone(UTC),
                       'requests': 1})
    return events


def incremental_reservation(root: Path, now: datetime, *,
                             sources: tuple[Source, ...] = SOURCES,
                             session_datasets: frozenset[str] = SESSION_DAY_DATASETS,
                             day_decision=tw_stock_day_decision) -> dict:
    """Protect only this release hour, not an all-day average allocation.

    Due requests preempt background dispatch account-wide. Future requests
    reserve quota only in their Taipei wall-clock hour. Provider user_info and
    shared pacing still control actual admission; no reset-at-the-hour is
    assumed. Completed/blocked/cooling jobs never become phantom ready work.
    """
    now = now.astimezone(UTC)
    horizon = now.astimezone(TAIPEI).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    horizon = horizon.astimezone(UTC)
    events = _free_refresh_events(root, now)
    delegated = active_sponsor_aliases(root, now, set(SPECS) - set(PRODUCT_HISTORY_STARTS))
    queued = set()
    errors = []
    for owner in ('sponsor', 'complement'):
        path = root / owner / 'queue.sqlite3'
        if not path.is_file():
            continue
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=0.2)) as conn:
                rows = conn.execute(
                    "SELECT dataset,next_attempt_at_utc,count(*) FROM tasks WHERE priority=0 AND kind!='derived' AND "
                    "((state='pending' AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?)) "
                    "OR (state IN ('complete','observed_empty','failed') "
                    "AND next_attempt_at_utc<=?)) GROUP BY dataset,next_attempt_at_utc",
                    (horizon.isoformat(), horizon.isoformat()),
                ).fetchall()
            for dataset, raw_due, count in rows:
                # These are the canonical ownership contracts, not additive
                # aliases. Derivations and delegated states are excluded above.
                if owner == 'sponsor' and dataset in PRODUCT_HISTORY_STARTS:
                    continue
                if owner == 'complement' and dataset in queued | delegated:
                    continue
                due = _stamp(raw_due) or now
                if due >= horizon:
                    continue
                events.append({'owner': owner, 'dataset': dataset, 'due_at': due, 'requests': count})
            queued.update(dataset for dataset, _, _ in rows if dataset not in PRODUCT_HISTORY_STARTS)
        except sqlite3.Error:
            errors.append(owner)
    local = now.astimezone(TAIPEI)
    future_sources = [spec for spec in sources if spec.grain in {'day', 'two_day'}
                      and spec.dataset not in queued and spec.release_hour == local.hour
                      and spec.release_minute > local.minute]
    if future_sources:
        decision = day_decision(local.date(),
            parquet_root=Path(__file__).resolve().parents[1] / 'data_tw_public', observed=now)
        for spec in future_sources:
            if spec.dataset in session_datasets and decision.status == 'closed':
                continue
            events.append({'owner': 'sponsor', 'dataset': spec.dataset, 'requests': 1,
                           'due_at': local.replace(minute=spec.release_minute, second=0, microsecond=0).astimezone(UTC)})
    selected = [row for row in events if row['due_at'] < horizon]
    ready = sum(row['requests'] for row in selected if row['due_at'] <= now)
    upcoming = sum(row['requests'] for row in selected if row['due_at'] > now)
    return {'schema_version': 2, 'basis': 'due_first_current_release_hour',
            'observed_at_utc': now.isoformat(), 'window_end_at_utc': horizon.isoformat(),
            'ready_requests': ready, 'upcoming_requests': upcoming,
            'reserve_requests': ready + upcoming, 'queue_errors': errors,
            'events': [{key: value for key, value in row.items() if key != 'due_at'} |
                       {'due_at_utc': row['due_at'].isoformat()} for row in selected]}


def fixed_incremental_demand(root: Path, now: datetime, **kwargs) -> int:
    """Compatibility entry point; the structured plan is shared with monitoring."""
    return incremental_reservation(root, now, **kwargs)['reserve_requests']
