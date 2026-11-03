"""Bounded all-product access to retained official option daily source rows.

This source view is deliberately not a trading, PIT feature or account ABI.
The daily first/last prices are different legs' first/last prints, not quotes
or prices at a selected decision clock. Consumers must exhaust the iterator
and verify their own dated availability and product/contract rules before use.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import re
from typing import Iterator, Sequence

import polars as pl
import pyarrow as pa
import pyarrow.csv as arrow_csv

from downloader.artifact_io import sha256_file
from scripts.audit_taifex_option_source_universe import _csv_column_names
from stockagent.data.tw_index_futures import (
    TAIFEX_DAY_SESSION_ALIASES, iter_taifex_daily_csv_streams,
)


ALL_PRODUCT_DAILY_SOURCE_CONTRACT = 'taifex_all_option_product_daily_source_v2'
_SOURCE_COLUMNS = {
    '交易日期': 'date_raw', '契約': 'product_raw', '到期月份(週別)': 'series_raw',
    '履約價': 'strike_raw', '買賣權': 'right_raw', '開盤價': 'open_raw',
    '最高價': 'high_raw', '最低價': 'low_raw', '收盤價': 'close_raw',
    '結算價': 'settlement_raw', '成交量': 'volume_raw', '交易時段': 'session_raw',
    '最後最佳買價': 'last_bid_raw', '最後最佳賣價': 'last_ask_raw',
    # The exchange added this column to its newer daily report. Keep the raw
    # source text; older archives lack it and must remain null, not inferred.
    '契約到期日': 'contract_expiry_raw',
}
_REQUIRED = frozenset({'交易日期', '契約', '到期月份(週別)', '履約價', '買賣權',
                       '開盤價', '收盤價', '結算價', '成交量'})
_NIGHT = ('盤後', '盤後交易時段', 'after_hours', 'night', 'night_session')


@dataclass(frozen=True)
class OptionDailySourceWindow:
    """Declared source-selection bounds, not proof of a complete trading tape."""
    path: Path
    sha256: str
    start: date
    end: date


def load_option_daily_source_plan(path: str | Path, *, expected_sha256: str) -> list[OptionDailySourceWindow]:
    """Load an explicitly pinned source plan; each reader rechecks payload SHA."""
    plan_path = Path(path)
    if plan_path.is_symlink() or not re.fullmatch(r'[0-9a-f]{64}', expected_sha256):
        raise ValueError('a regular, explicitly pinned option source plan is required')
    raw = plan_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('option source plan digest mismatch')
    plan = json.loads(raw)
    if plan.get('contract') != 'tw_option_all_product_daily_source_plan_v1' or not plan.get('sources'):
        raise ValueError('unsupported or empty option source plan')
    result = []
    for item in plan['sources']:
        source_path = Path(item['path'])
        if (not source_path.is_absolute() or source_path.is_symlink()
                or not re.fullmatch(r'[0-9a-f]{64}', item['sha256'])):
            raise ValueError('invalid option source plan entry')
        result.append(OptionDailySourceWindow(source_path, item['sha256'],
                                              date.fromisoformat(item['start']), date.fromisoformat(item['end'])))
    if plan_path.read_bytes() != raw:
        raise ValueError('option source plan changed while loading')
    return result


def iter_option_product_daily_window(sources: Sequence[OptionDailySourceWindow], *,
                                     start: date, end: date,
                                     products: set[str] | None = None) -> Iterator[pl.DataFrame]:
    """Read a bounded multi-source window without filling absent observations.

    The caller must provide non-overlapping source intervals covering the
    requested dates and pinned digests. This only proves that source files
    cover a declared selection window; it never asserts a market calendar,
    complete contract universe, publication time, price clock or account ABI.
    """
    if type(start) is not date or type(end) is not date or start > end or not sources:
        raise ValueError('a nonempty, ordered date window is required')
    ordered = sorted(sources, key=lambda source: source.start)
    if any(type(source.start) is not date or type(source.end) is not date or source.start > source.end
           for source in ordered):
        raise ValueError('invalid source selection interval')
    if any(left.end >= right.start for left, right in zip(ordered, ordered[1:])):
        raise ValueError('overlapping option source selection intervals')
    relevant = [source for source in ordered if source.end >= start and source.start <= end]
    cursor = start
    for source in relevant:
        first, last = max(start, source.start), min(end, source.end)
        if first != cursor:
            raise ValueError('requested option source window has a gap')
        cursor = last + timedelta(days=1)
    if cursor != end + timedelta(days=1):
        raise ValueError('requested option source window has a gap')
    for source in relevant:
        first, last = max(start, source.start), min(end, source.end)
        days = {first + timedelta(days=offset) for offset in range((last - first).days + 1)}
        yield from iter_option_product_daily_batches(source.path, expected_sha256=source.sha256,
                                                    products=products, trading_dates=days)


def _identity(path: Path) -> tuple[int, int, int, int, int]:
    s = path.stat()
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns


def iter_option_product_daily_batches(source: str | Path, *, expected_sha256: str,
                                      products: set[str] | None = None,
                                      trading_dates: set[date] | None = None) -> Iterator[pl.DataFrame]:
    """Read source columns in bounded Arrow batches, retaining raw values.

    Duplicate records and unreported/unknown sessions remain visible. A source
    member and data-record ordinal identify every row. The iterator verifies
    the source digest again at exhaustion; no partial iteration is a complete
    source receipt and no returned date implies point-in-time availability.
    """
    path = Path(source).resolve()
    if not re.fullmatch(r'[0-9a-f]{64}', expected_sha256):
        raise ValueError('an explicit lower-case source digest is required')
    if products is not None:
        if not products or any(not isinstance(p, str) or not re.fullmatch(r'[A-Z0-9]{2,8}', p) for p in products):
            raise ValueError('products must be a nonempty set of explicit option codes')
    if trading_dates is not None:
        if not trading_dates or any(type(d) is not date for d in trading_dates):
            raise ValueError('trading_dates must be a nonempty set of dates')
    identity = _identity(path)
    members = 0
    for stream, member, actual_sha in iter_taifex_daily_csv_streams(path):
        members += 1
        if actual_sha != expected_sha256:
            raise ValueError('option daily source digest mismatch')
        names, _ = _csv_column_names(stream.buffer, member)
        if _REQUIRED - set(names):
            raise ValueError(f'{member} is missing required option source columns')
        cols = list(_SOURCE_COLUMNS)
        reader = arrow_csv.open_csv(
            stream.buffer,
            read_options=arrow_csv.ReadOptions(encoding='cp950', column_names=names, skip_rows=1,
                                               block_size=2 * 1024 * 1024),
            convert_options=arrow_csv.ConvertOptions(include_columns=cols, include_missing_columns=True,
                                                     column_types={name: pa.string() for name in cols}),
        )
        ordinal = 0
        try:
            for batch in reader:
                raw = pl.from_arrow(batch).with_row_index('source_record_ordinal', offset=ordinal + 1)
                ordinal += raw.height
                raw = raw.rename(_SOURCE_COLUMNS).with_columns(
                    pl.col('date_raw').str.strip_chars().str.strptime(pl.Date, '%Y/%m/%d', strict=False).alias('date'),
                    pl.col('product_raw').str.strip_chars().str.to_uppercase().alias('product'),
                    pl.col('series_raw').str.strip_chars().str.to_uppercase().alias('series'),
                    pl.col('session_raw').fill_null('').str.strip_chars().str.to_lowercase().alias('_session'),
                    pl.col('right_raw').fill_null('').str.strip_chars().str.to_uppercase().alias('_right'),
                )
                if raw.filter(pl.col('date').is_null() | pl.col('product').is_null() |
                              (pl.col('product') == '')).height:
                    raise ValueError(f'{member} contains an invalid date or product')
                raw = raw.with_columns(
                    pl.when(pl.col('_session') == '').then(pl.lit('unreported'))
                      .when(pl.col('_session').is_in(list(TAIFEX_DAY_SESSION_ALIASES))).then(pl.lit('day'))
                      .when(pl.col('_session').is_in(list(_NIGHT))).then(pl.lit('night'))
                      .otherwise(pl.lit('unknown')).alias('session'),
                    pl.when(pl.col('_right').is_in(['買權', 'C', 'CALL'])).then(pl.lit('C'))
                      .when(pl.col('_right').is_in(['賣權', 'P', 'PUT'])).then(pl.lit('P'))
                      .otherwise(pl.lit(None, dtype=pl.String)).alias('right'),
                    pl.when(pl.col('series').str.contains(r'^\d{6}$')).then(pl.lit('monthly_candidate'))
                      .when(pl.col('series').str.contains(r'^\d{6}[WF][1-5]$')).then(pl.lit('weekly_candidate'))
                      .otherwise(pl.lit('unclassified')).alias('series_scope'),
                    pl.lit(member).alias('source_member'), pl.lit(actual_sha).alias('source_sha256'),
                    pl.lit(ALL_PRODUCT_DAILY_SOURCE_CONTRACT).alias('source_contract'),
                ).drop('_session', '_right')
                if products is not None:
                    raw = raw.filter(pl.col('product').is_in(sorted(products)))
                if trading_dates is not None:
                    raw = raw.filter(pl.col('date').is_in(sorted(trading_dates)))
                if raw.height:
                    yield raw
        finally:
            reader.close()
    if not members or _identity(path) != identity or sha256_file(path) != expected_sha256:
        raise ValueError('option daily source changed or has no CSV members')
