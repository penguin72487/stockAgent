"""Shared history-query repair policy; receipt validity is not market coverage."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
import json
from pathlib import Path
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from downloader.artifact_io import sha256_file
from downloader.artifact_io import atomic_write_json, atomic_write_parquet


DEFAULT_FUTURES_ACTIVITY = Path('data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet')


def futures_date_is_closed(
    day: date,
    now: datetime | None = None,
    *,
    contract: str | None = None,
) -> bool:
    local = (now or datetime.now(UTC)).astimezone(ZoneInfo('Asia/Taipei'))
    # TXFR1's trading-date day session ends before the 14:31 historical-query
    # gate. Other futures include products with a 16:15 day close, so they
    # retain the conservative 16:30 finalization clock.
    close_clock = time(14, 31) if contract == 'TXFR1' else time(16, 30)
    return day < local.date() or (day == local.date() and local.time() >= close_clock)


def latest_completed_futures_session(calendar: Path, now: datetime | None = None) -> date:
    local = (now or datetime.now(UTC)).astimezone(ZoneInfo('Asia/Taipei'))
    cutoff = local.date() if local.time() >= time(16, 30) else local.date() - timedelta(days=1)
    result = (pl.scan_parquet(calendar)
              .filter((pl.col('product') == 'TX') & (pl.col('date') <= pl.lit(cutoff)))
              .select(pl.col('date').max()).collect().item())
    if result is None:
        raise RuntimeError('no completed TAIFEX calendar; refuse a fabricated weekday target')
    return result


@lru_cache(maxsize=16384)
def _signature_sha(path: str, signature: tuple[int, ...]) -> str:
    return sha256_file(Path(path))


def verified_sha(path: Path) -> str:
    stat = path.stat()
    return _signature_sha(str(path.resolve()), (stat.st_dev, stat.st_ino, stat.st_size,
                                                stat.st_mtime_ns, stat.st_ctime_ns))


def utc_stamp(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).isoformat().replace('+00:00', 'Z')


def parsed_time(value: Any) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)
    except (TypeError, ValueError):
        return None


def recent_login_waiter(path: Path, *, now: datetime | None = None, max_age_seconds: int = 120) -> bool:
    """Only yield a history batch to a peer demonstrably waiting for its login lock."""

    try:
        payload = json.loads(path.read_bytes())
    except (OSError, ValueError):
        return False
    if not isinstance(payload, dict) or payload.get('state') != 'waiting' or payload.get('reason') != 'history_login_slot_busy':
        return False
    observed = parsed_time(payload.get('observed_at_utc'))
    if observed is None:
        return False
    age = ((now or datetime.now(UTC)).astimezone(UTC) - observed).total_seconds()
    return 0 <= age <= max_age_seconds


def checked_time(receipt: dict[str, Any] | None) -> datetime:
    receipt = receipt or {}
    return (parsed_time(receipt.get('checked_at_utc'))
            or parsed_time(receipt.get('finished_at_utc'))
            or datetime.min.replace(tzinfo=UTC))


def retry_due(receipt: dict[str, Any] | None, *, positive_activity: bool = False,
              now: datetime | None = None) -> bool:
    """Unknown empty replies expire too; legacy replies without a clock are due."""
    if not receipt:
        return True
    if receipt.get('status') == 'complete':
        return False
    now = now or datetime.now(UTC)
    next_retry = parsed_time(receipt.get('next_retry_at_utc'))
    # Newly discovered official activity must not inherit a slow unknown-empty TTL.
    if positive_activity and not receipt.get('positive_activity_expected'):
        next_retry = min(next_retry or now, checked_time(receipt) + timedelta(days=1))
    if next_retry is None:
        next_retry = checked_time(receipt) + timedelta(days=1 if positive_activity else 7)
    return now >= next_retry


def retry_metadata(previous: dict[str, Any] | None, *, empty: bool,
                   positive_activity: bool = False, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    attempts = int((previous or {}).get('empty_attempts') or 0) + 1 if empty else 0
    # A bounded exponential cooldown prevents hammering unavailable history.
    days = min(7 if positive_activity else 30,
               (1 if positive_activity else 7) * 2 ** min(max(attempts - 1, 0), 5))
    return {'checked_at_utc': utc_stamp(now), 'empty_attempts': attempts,
            'positive_activity_expected': positive_activity,
            'next_retry_at_utc': utc_stamp(now + timedelta(days=days)) if empty else None}


@dataclass
class FuturesActivity:
    continuous: dict[str, set[date]] = field(default_factory=dict)
    exact: dict[tuple[str, str], set[date]] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)

    def exact_dates(self, row: Any) -> set[date]:
        if row.security_type != 'FUT' or not row.delivery_date:
            return set()
        return {day for day in self.exact.get((row.root, row.delivery_date.isoformat()), set())
                if row.begin_date <= day <= row.end_date}


def load_futures_activity(path: Path, *, start: date, end: date) -> FuturesActivity:
    """Official positive volume selects repair queries, never supplies fake ticks.

    Exact targets match root AND final trading date. Continuous R1/R2 use the
    first two monthly series; weekly contracts must not shift those ranks.
    Missing official rows are unknown, never proof that a market did not trade.
    """
    if not path.is_file():
        raise FileNotFoundError(f'official futures activity missing: {path}')
    signature = path.stat()
    digest = sha256_file(path)
    frame = (pl.scan_parquet(path)
             .filter((pl.col('date') >= pl.lit(start)) & (pl.col('date') <= pl.lit(end)))
             .select('date', 'product', 'contract', 'resolved_last_trade_date',
                     'shioaji_roots', 'volume')
             .collect())
    current = path.stat()
    if (signature.st_ino, signature.st_size, signature.st_mtime_ns) != (
            current.st_ino, current.st_size, current.st_mtime_ns):
        raise RuntimeError('official activity changed during read; retry the plan')
    activity = FuturesActivity(provenance={'path': str(path), 'sha256': digest,
                                          'start': str(start), 'end': str(end)})
    for row in frame.filter(pl.col('volume') > 0).iter_rows(named=True):
        if row['resolved_last_trade_date'] is None:
            continue
        for root in (row['shioaji_roots'] or row['product']).split(','):
            key = root.strip(), row['resolved_last_trade_date'].isoformat()
            activity.exact.setdefault(key, set()).add(row['date'])
    monthly = (frame.filter(pl.col('contract').str.contains(r'^\d{6}$'))
               .with_columns(pl.col('contract').rank('dense').over(['date', 'product']).alias('rank'))
               .filter((pl.col('rank') <= 2) & (pl.col('volume') > 0)))
    for row in monthly.iter_rows(named=True):
        for root in (row['shioaji_roots'] or row['product']).split(','):
            code = f"{root.strip()}R{int(row['rank'])}"
            activity.continuous.setdefault(code, set()).add(row['date'])
    return activity


def prepare_query_calendar(calendar: Path, output: Path, public_root: Path) -> date:
    target = latest_completed_futures_session(calendar)
    dates = (pl.scan_parquet(calendar)
             .filter((pl.col('product') == 'TX') & (pl.col('date') <= pl.lit(target)))
             .select('date').unique().collect())
    if dates.is_empty():
        raise RuntimeError('no official completed TX calendar')
    # A stock close cannot advance all futures products into a partial session.
    dates = dates.sort('date')
    atomic_write_parquet(output, dates.with_columns(pl.lit('TX').alias('product')).select('product', 'date'))
    return target


def record_schedule(path: Path, *, reason: str, seconds: int) -> dict[str, Any]:
    now = datetime.now(UTC)
    payload = {'schema_version':1, 'state':'waiting' if seconds else 'running',
               'reason':reason, 'observed_at_utc':utc_stamp(now),
               'next_attempt_at_utc':utc_stamp(now + timedelta(seconds=seconds)),
               'wait_seconds':seconds, 'history_window':'stock weekdays 05:00:10-07:45 Asia/Taipei; other off-hours only when live logins leave capacity',
               'max_traffic_fraction':0.90}
    atomic_write_json(path, payload)
    return payload


def publication_ready(payload: dict[str, Any], *, continuous: bool) -> bool:
    if continuous:
        return (payload.get('status') == 'batch_finished'
                and payload.get('scanned_contracts') == payload.get('total_contracts')
                and payload.get('missing_dates_after_batch') == 0
                and payload.get('positive_activity_empty_dates_after_batch') == 0
                and payload.get('failed_contracts') == 0)
    return (payload.get('query_receipt_state') == 'complete'
            and payload.get('positive_activity_empty_queries') == 0
            and payload.get('official_activity_missing_kbar_dates', 0) == 0
            and payload.get('failed_contracts', 0) == 0)
