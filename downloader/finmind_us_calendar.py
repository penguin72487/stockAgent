"""A bounded, versioned official cash schedule, not inferred source coverage.

No network requests during queue seeding. The checked-in facts are auditable at
their primary source URLs. History reconciliation is reversible on proof loss,
revision or any nonempty conflicting observation, using the shared calendar ABI.
"""
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path

from downloader.finmind_history_calendar import HistoryClosures


CALENDAR_PATH = Path(__file__).resolve().parents[1] / 'configs/finmind_us_cash_calendar.json'


def published_calendar(now: datetime, *, path: Path = CALENDAR_PATH) -> dict[str, HistoryClosures]:
    try:
        raw = path.read_bytes()
        if len(raw) > 64 * 1024:
            return {}
        value = json.loads(raw)
        first, last = date.fromisoformat(value['first']), date.fromisoformat(value['last'])
        if (value['contract_version'] != 1 or value['dataset'] != 'USStockPriceMinute'
                or value['basis'] != 'official_published_us_cash_calendar'
                or value['closed_weekdays'] != [5, 6] or last < first or (last - first).days > 10 * 366
                or set(value['holidays']) != {str(year) for year in range(first.year, last.year + 1)}
                or not value['sources'] or any(not url.startswith(('https://ir.theice.com/',
                                                                  'https://www.nasdaqtrader.com/'))
                                              for url in value['sources'])):
            return {}
        holidays = {date.fromisoformat(f'{year}-{day}') for year, days in value['holidays'].items() for day in days}
        exceptions = {date.fromisoformat(day) for day in value['early_closes_still_open'] + value['explicitly_open_exceptions']}
        if any(not first <= day <= last for day in holidays | exceptions) or holidays & exceptions:
            return {}
        closed = tuple((first + timedelta(days=offset)).isoformat() for offset in range((last - first).days + 1)
                       if (first + timedelta(days=offset)).weekday() in (5, 6)
                       or first + timedelta(days=offset) in holidays)
        if any(day.isoformat() in closed for day in exceptions):
            return {}
        return {'USStockPriceMinute': HistoryClosures(closed, frozenset({'USStockPriceMinute'}),
                   hashlib.sha256(raw).hexdigest(), first.isoformat(), last.isoformat(), now.isoformat(), value['basis'])}
    except (OSError, ValueError, KeyError, TypeError):
        return {}  # Proof loss restores candidates; never silently assume closure.
