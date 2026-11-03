from datetime import date, datetime
import sqlite3
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from stockagent.data.tw_option_full_carry_index import add_events, create_schema, lookup
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_event_index import compact_decision_events


WINDOWS = {'TXO': (525, 825), 'TGO': (510, 975)}


def row(day, clock, price, *, strike='8000', product='TXO', sha='a'):
    return {'event_ts': datetime.fromisoformat(f'{day}T{clock}').replace(tzinfo=ZoneInfo('Asia/Taipei')),
            'product_raw': product, 'series_raw': '201103', 'strike_raw': strike,
            'right_raw': 'C', 'price_raw': str(price), 'source_sha256': sha * 64}


def test_lookup_matches_full_source_with_older_carry_open_ties_and_future_events():
    days = [date(2011, 2, 4), date(2011, 2, 11), date(2011, 2, 18)]
    raw = [pl.DataFrame([row('2011-02-04', '13:00:00', 10, strike='8000'),
                         row('2011-02-04', '13:01:00', 11, strike='8200')]),
           pl.DataFrame([row('2011-02-11', '13:00:00', 20, strike='8100')]),
           pl.DataFrame([row('2011-02-18', '08:44:59', 21, strike='8100'),
                         row('2011-02-18', '08:45:00', 30, strike='8100'),
                         row('2011-02-18', '08:45:00', 31, strike='8100', sha='b'),
                         row('2011-02-18', '08:55:00', 32, strike='8100'),
                         row('2011-02-18', '10:00:00', 99, strike='8000'),
                         row('2011-02-18', '08:35:00', 40, product='TGO')])]
    keys = [('TXO', '201103', '8000.0', 'C'), ('TXO', '201103', '8100', 'C'),
            ('TXO', '201103', '8200', 'C'), ('TGO', '201103', '8000', 'C')]
    conn = sqlite3.connect(':memory:')
    create_schema(conn)
    for source, day in zip(raw, days, strict=True):
        compact = compact_decision_events(source, day=day, session_windows=WINDOWS)
        assert add_events(conn, compact) == compact.height
    full = pl.concat(raw)
    for clock in ('08:45', '09:00'):
        expected = reference_candidates(full, trading_date=days[-1], decision_time=clock, session_windows=WINDOWS)
        actual = lookup(conn, keys, day=days[-1], clock=clock, session_windows=WINDOWS)
        assert actual.equals(expected)
    early = lookup(conn, keys, day=days[-1], clock='08:45', session_windows=WINDOWS)
    assert early.filter(pl.col('strike_decimal') == '8000').filter(pl.col('product_raw') == 'TXO').item(
        0, 'trade_ts').date() == days[0]
    opened = lookup(conn, keys, day=days[-1], clock='09:00', session_windows=WINDOWS)
    tied = opened.filter(pl.col('strike_decimal') == '8100')
    assert tied.item(0, 'reference_kind') == 'ambiguous_first_second'
    assert tied.item(0, 'reference_price_decimal') is None


def test_lookup_respects_explicit_scope_and_missing_contracts():
    conn = sqlite3.connect(':memory:')
    create_schema(conn)
    frame = pl.DataFrame([row('2011-01-03', '08:40:00', 5), row('2011-01-03', '08:41:00', 6, strike='8100')])
    add_events(conn, frame)
    result = lookup(conn, [('TXO', '201103', '8000', 'C'), ('TXO', '201103', '9000', 'C')],
                    day=date(2011, 1, 3), clock='08:45', session_windows=WINDOWS)
    assert result.height == 1 and result.item(0, 'strike_decimal') == '8000'
    assert lookup(conn, [], day=date(2011, 1, 3), clock='08:45', session_windows=WINDOWS).is_empty()
    with pytest.raises(ValueError, match='unique'):
        lookup(conn, [('TXO', '201103', '8000', 'C'), ('TXO', '201103', '8000.0', 'C')],
               day=date(2011, 1, 3), clock='08:45', session_windows=WINDOWS)


def test_addition_and_metadata_can_be_rolled_back_together():
    conn = sqlite3.connect(':memory:')
    create_schema(conn)
    conn.commit()
    conn.execute('BEGIN')
    add_events(conn, pl.DataFrame([row('2011-01-03', '08:40:00', 5)]))
    conn.rollback()
    assert conn.execute('SELECT count(*) FROM events').fetchone() == (0,)


def test_addition_rejects_duplicate_identity():
    conn = sqlite3.connect(':memory:')
    create_schema(conn)
    event = row('2011-01-03', '08:40:00', 5)
    with pytest.raises(ValueError, match='duplicate'):
        add_events(conn, pl.DataFrame([event, event]))
