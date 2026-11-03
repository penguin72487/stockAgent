"""Source-only lookup of option reference candidates across retained history.

The index stores the already-verified compact events, not quotes or fills.
The caller must supply its own dated contract eligibility and coverage proof.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import sqlite3
from zoneinfo import ZoneInfo

import polars as pl

from stockagent.data.tw_option_reference_candidates import KEYS, reference_candidates


CONTRACT = 'tw_option_observed_full_carry_index_v1'
ZONE = ZoneInfo('Asia/Taipei')


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute('CREATE TABLE IF NOT EXISTS events ('
                 'product TEXT NOT NULL,series TEXT NOT NULL,strike TEXT NOT NULL,right TEXT NOT NULL,'
                 'event_us INTEGER NOT NULL,price TEXT NOT NULL,source_sha256 TEXT NOT NULL,'
                 'source_window INTEGER NOT NULL,'
                 'PRIMARY KEY(product,series,strike,right,event_us,price,source_sha256)) WITHOUT ROWID')
    conn.execute('CREATE INDEX IF NOT EXISTS events_source_window ON events(source_window)')


def _decimal_text(value: str) -> str:
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError('invalid option decimal') from None
    if not number.is_finite() or number <= 0:
        raise ValueError('invalid option decimal')
    return format(number.normalize(), 'f')


def canonical_rows(frame: pl.DataFrame) -> list[tuple]:
    names = ('event_ts', 'product_raw', 'series_raw', 'strike_raw', 'right_raw', 'price_raw', 'source_sha256')
    if set(names) - set(frame.columns) or frame.is_empty():
        raise ValueError('nonempty compact events are required')
    frame = frame.select(*names).with_columns(pl.col('event_ts').dt.epoch('us').alias('event_us'))
    if frame.filter(pl.col('event_us').is_null() | pl.col('product_raw').is_null()
                    | pl.col('series_raw').is_null() | pl.col('strike_raw').is_null()
                    | pl.col('price_raw').is_null() | pl.col('source_sha256').is_null()
                    | ~pl.col('right_raw').is_in(['C', 'P']).fill_null(False)).height:
        raise ValueError('invalid compact event identity')
    values = []
    for product, series, strike, right, price, digest, stamp in frame.select(
            'product_raw', 'series_raw', 'strike_raw', 'right_raw', 'price_raw', 'source_sha256', 'event_us').iter_rows():
        if not product or not series or not re.fullmatch(r'[0-9a-f]{64}', digest) or stamp % 1_000_000:
            raise ValueError('invalid compact event identity or whole-second clock')
        values.append((product, series, _decimal_text(strike), right, stamp, _decimal_text(price), digest))
    if len(values) != len(set(values)):
        raise ValueError('duplicate compact event identity')
    return values


def rows_digest(rows) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(tuple(row), ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
        digest.update(b'\n')
    return digest.hexdigest()


def add_events(conn: sqlite3.Connection, frame: pl.DataFrame, *, source_window: int = 0) -> int:
    """Add one verified compact partition inside the caller's transaction."""
    if not isinstance(source_window, int) or source_window < 0:
        raise ValueError('invalid source window index')
    values = canonical_rows(frame)
    before = conn.total_changes
    conn.executemany('INSERT INTO events VALUES (?,?,?,?,?,?,?,?)',
                     [(*row, source_window) for row in values])
    return conn.total_changes - before


def _epoch_us(value: datetime) -> int:
    return int(value.timestamp()) * 1_000_000


def lookup(conn: sqlite3.Connection, keys: list[tuple[str, str, str, str]], *, day: date,
           clock: str, session_windows: dict[str, tuple[int, int]]) -> pl.DataFrame:
    """Use indexed extremes and the shared reducer for explicit contract keys.

    All source observations across earlier indexed windows may participate.
    A missing key is left missing; a source gap can still hide a newer trade.
    """
    if clock not in {'08:45', '09:00'} or not session_windows:
        raise ValueError('two-clock research policy required')
    normalized = []
    for product, series, strike, right in keys:
        if not product or not series or right not in {'C', 'P'} or product not in session_windows:
            raise ValueError('invalid requested option contract')
        start, end = session_windows[product]
        if not 0 <= start < end <= 1440:
            raise ValueError('invalid candidate session window')
        normalized.append((product, series, _decimal_text(strike), right, start, end))
    if len(normalized) != len(set(normalized)):
        raise ValueError('requested option contracts must be unique')
    conn.execute('CREATE TEMP TABLE IF NOT EXISTS requested_option_keys ('
                 'product TEXT,series TEXT,strike TEXT,right TEXT,start_min INTEGER,end_min INTEGER,'
                 'PRIMARY KEY(product,series,strike,right)) WITHOUT ROWID')
    conn.execute('DELETE FROM requested_option_keys')
    conn.executemany('INSERT INTO requested_option_keys VALUES (?,?,?,?,?,?)', normalized)
    midnight = datetime.combine(day, time.min, ZONE)
    hour, minute = map(int, clock.split(':'))
    latest_us = _epoch_us(midnight + timedelta(hours=hour, minutes=minute)) - 1_000_000
    midnight_us = _epoch_us(midnight)
    # The primary key begins with the contract and event time. Each scalar
    # subquery seeks one extreme, then the join retains every same-second tie.
    rows = conn.execute('WITH selected AS MATERIALIZED ('
        'SELECT k.*, '
        '(SELECT MIN(e.event_us) FROM events e WHERE e.product=k.product AND e.series=k.series '
        'AND e.strike=k.strike AND e.right=k.right AND e.event_us>=?+k.start_min*60000000 '
        'AND e.event_us<?+k.end_min*60000000 AND e.event_us<=?) AS first_us, '
        '(SELECT MAX(e.event_us) FROM events e WHERE e.product=k.product AND e.series=k.series '
        'AND e.strike=k.strike AND e.right=k.right AND e.event_us<=?) AS last_us '
        'FROM requested_option_keys k) '
        'SELECT e.product,e.series,e.strike,e.right,e.event_us,e.price,e.source_sha256 '
        'FROM selected k JOIN events e ON e.product=k.product AND e.series=k.series '
        'AND e.strike=k.strike AND e.right=k.right AND (e.event_us=k.first_us OR e.event_us=k.last_us)',
        (midnight_us, midnight_us, latest_us, latest_us)).fetchall()
    events = pl.DataFrame({
        'event_ts': [datetime.fromtimestamp(r[4] / 1_000_000, ZONE) for r in rows],
        'product_raw': [r[0] for r in rows], 'series_raw': [r[1] for r in rows],
        'strike_raw': [r[2] for r in rows], 'right_raw': [r[3] for r in rows],
        'price_raw': [r[5] for r in rows], 'source_sha256': [r[6] for r in rows],
    }, schema={'event_ts': pl.Datetime('us', 'Asia/Taipei'), 'product_raw': pl.String,
               'series_raw': pl.String, 'strike_raw': pl.String, 'right_raw': pl.String,
               'price_raw': pl.String, 'source_sha256': pl.String})
    return reference_candidates(events, trading_date=day, decision_time=clock, session_windows=session_windows)
