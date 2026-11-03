from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from stockagent.data.tw_option_reference_candidates import compact_last_observations, reference_candidates
from stockagent.data.tw_option_reference_event_index import compact_decision_events


def row(day, clock, price, *, strike='8000', product='TXO', sha='a'):
    return {'event_ts': datetime.fromisoformat(f'{day}T{clock}').replace(tzinfo=ZoneInfo('Asia/Taipei')),
            'product_raw': product, 'series_raw': '201103', 'strike_raw': strike,
            'right_raw': 'C', 'price_raw': str(price), 'source_sha256': sha * 64}


def ordered(frame):
    return frame.sort('product_raw', 'series_raw', 'strike_decimal', 'right_raw')


def test_compact_index_preserves_both_references_state_and_same_second_ties():
    day = date(2011, 1, 10)
    windows = {'TXO': (525, 825), 'TGO': (510, 975)}
    prior = pl.DataFrame([row('2011-01-07', '13:00:00', 10),
                          row('2011-01-07', '13:01:00', 11, strike='8100')])
    source = pl.DataFrame([row('2011-01-10', '08:44:59', 12),
                           row('2011-01-10', '08:45:00', 15),
                           row('2011-01-10', '08:45:00', 16),
                           row('2011-01-10', '08:48:00', 17),
                           row('2011-01-10', '08:59:59', 18),
                           row('2011-01-10', '09:01:00', 19),
                           row('2011-01-10', '13:44:59', 20),
                           row('2011-01-10', '08:35:00', 21, product='TGO')])
    index = compact_decision_events(source, day=day, session_windows=windows)
    for clock in ('08:45', '09:00'):
        full = ordered(reference_candidates(pl.concat([prior, source]), trading_date=day,
                                            decision_time=clock, session_windows=windows))
        reduced = ordered(reference_candidates(pl.concat([prior, index]), trading_date=day,
                                               decision_time=clock, session_windows=windows))
        assert full.equals(reduced)
    assert compact_last_observations(pl.concat([prior, source])).equals(
        compact_last_observations(pl.concat([prior, index])))
    late = reference_candidates(pl.concat([prior, index]), trading_date=day,
                                decision_time='09:00', session_windows=windows)
    txo = late.filter((pl.col('product_raw') == 'TXO') & (pl.col('strike_decimal') == '8000'))
    assert txo.item(0, 'reference_kind') == 'ambiguous_first_second'
    assert txo.item(0, 'reference_price_decimal') is None
    assert index.height < source.height


def test_future_trade_change_does_not_change_earlier_reference():
    day = date(2011, 1, 10)
    windows = {'TXO': (525, 825)}
    earlier = [row('2011-01-10', '08:40:00', 9), row('2011-01-10', '08:46:00', 10)]
    frames = [pl.DataFrame(earlier + [row('2011-01-10', '13:00:00', p)]) for p in (12, 99)]
    refs = [reference_candidates(compact_decision_events(x, day=day, session_windows=windows),
                                 trading_date=day, decision_time='09:00', session_windows=windows)
            for x in frames]
    assert refs[0].equals(refs[1])


def test_observation_survives_more_than_one_source_window():
    windows = {'TXO': (525, 825)}
    days = [date(2011, 2, 4), date(2011, 2, 11), date(2011, 2, 18)]
    sources = [pl.DataFrame([row('2011-02-04', '13:00:00', 10, strike='8000')]),
               pl.DataFrame([row('2011-02-11', '13:00:00', 20, strike='8100')]),
               pl.DataFrame([row('2011-02-18', '08:46:00', 30, strike='8100')])]
    indexes = [compact_decision_events(s, day=d, session_windows=windows)
               for s, d in zip(sources, days, strict=True)]
    full_prior = compact_last_observations(pl.concat(sources[:2]))
    index_prior = compact_last_observations(pl.concat(indexes[:2]))
    full = ordered(reference_candidates(pl.concat([full_prior, sources[2]]),
                                        trading_date=days[2], decision_time='08:45',
                                        session_windows=windows))
    indexed = ordered(reference_candidates(pl.concat([index_prior, indexes[2]]),
                                           trading_date=days[2], decision_time='08:45',
                                           session_windows=windows))
    assert full.equals(indexed)
    old = indexed.filter(pl.col('strike_decimal') == '8000')
    assert old.item(0, 'trade_ts').date() == days[0]
    assert old.item(0, 'reference_price_decimal') == '10'
    # The result is an observed candidate; an intervening missing source day or
    # an expired contract still requires the caller's separate admission gate.


def test_reject_wrong_date_and_unknown_product():
    frame = pl.DataFrame([row('2011-01-11', '08:40:00', 9)])
    with pytest.raises(ValueError, match='source day'):
        compact_decision_events(frame, day=date(2011, 1, 10), session_windows={'TXO': (525, 825)})
    with pytest.raises(ValueError, match='source day'):
        compact_decision_events(frame, day=date(2011, 1, 11), session_windows={'TGO': (525, 825)})
