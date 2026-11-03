from datetime import date, datetime
import sqlite3
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from scripts.run_option_preclock_full_union_after import combine_clock, verify_strike_identity
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_gap_guard import mark_known_missing_days


DAY = date(2026, 10, 28)
MISSING = [date(2026, 10, 6)]


def rows(items):
    return pl.DataFrame([{'event_ts': datetime.fromisoformat(stamp).replace(tzinfo=ZoneInfo('Asia/Taipei')),
        'product_raw': 'TXO', 'series_raw': '202610', 'strike_raw': strike, 'right_raw': 'C',
        'price_raw': price, 'source_sha256': source * 64} for stamp, strike, price, source in items])


def candidate(items):
    return reference_candidates(rows(items), trading_date=DAY, decision_time='08:45',
                                session_windows={'TXO': (525, 825)})


def marked(items):
    return mark_known_missing_days(candidate(items), missing_days=MISSING,
                                   trading_date=DAY, decision_time='08:45')


def test_full_history_selection_refreshes_trade_but_preserves_missing_day_flags():
    old = [('2026-09-01T13:00:00', '20000', '4', 'a'),
           ('2026-09-02T13:00:00', '21000', '5', 'a')]
    history = [('2026-10-27T13:00:00', '20000', '6', 'b')]
    window = candidate(old).with_columns(pl.lit(DAY).alias('decision_date'),
                                          pl.lit('08:45').alias('decision_time'))
    result, counts = combine_clock(window, marked(history), day=DAY, clock='08:45', missing_days=MISSING)
    by_strike = {row['strike_decimal']: row for row in result.iter_rows(named=True)}
    assert by_strike['20000']['reference_price_decimal'] == '6'
    assert by_strike['20000']['known_missing_day_since_reference'] is False
    assert by_strike['21000']['reference_price_decimal'] == '5'
    assert by_strike['21000']['known_missing_day_since_reference'] is True
    assert counts['reference_value_or_time_changed'] == 1
    assert counts['known_gap_since_selected_trade'] == 1


def test_full_history_rejects_unrequested_contract():
    window = candidate([('2026-09-01T13:00:00', '20000', '4', 'a')]).with_columns(
        pl.lit(DAY).alias('decision_date'), pl.lit('08:45').alias('decision_time'))
    with pytest.raises(ValueError, match='unrequested'):
        combine_clock(window, marked([('2026-10-27T13:00:00', '21000', '6', 'b')]),
                      day=DAY, clock='08:45', missing_days=MISSING)


def test_full_history_rejects_float32_strike_collision():
    official = candidate([('2026-09-01T13:00:00', '20000.0001', '4', 'a')])
    with sqlite3.connect(':memory:') as conn:
        conn.execute('CREATE TABLE events (product TEXT,series TEXT,strike TEXT,right TEXT)')
        conn.execute('INSERT INTO events VALUES (?,?,?,?)', ('TXO', '202610', '20000', 'C'))
        with pytest.raises(ValueError, match='strike identity'):
            verify_strike_identity(conn, official)
