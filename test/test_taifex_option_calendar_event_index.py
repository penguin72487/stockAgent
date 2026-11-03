from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl

from scripts.build_taifex_option_calendar_event_index import append_chunk
from stockagent.data.tw_option_reference_candidates import compact_last_observations
from stockagent.data.tw_option_reference_event_index import compact_decision_events


def row(time, price):
    return {'event_ts': datetime.fromisoformat(f'2026-10-06T{time}').replace(tzinfo=ZoneInfo('Asia/Taipei')),
            'product_raw': 'TXO', 'series_raw': '202610', 'strike_raw': '20000',
            'right_raw': 'C', 'price_raw': str(price), 'source_sha256': 'a' * 64}


def test_streaming_chunks_preserve_out_of_order_same_second_alternatives():
    day, sessions = date(2026, 10, 6), {'TXO': (525, 825)}
    chunks = [pl.DataFrame([row('23:30:00', 30), row('08:45:00', 11)]),
              pl.DataFrame([row('08:44:59', 9), row('08:45:00', 12)]),
              pl.DataFrame([row('09:00:00', 20), row('08:40:00', 8)])]
    index = state = None
    for chunk in chunks:
        index, state = append_chunk(index, state, chunk, day=day, sessions=sessions)
    native = pl.concat(chunks)
    assert index.equals(compact_decision_events(native, day=day, session_windows=sessions))
    assert state.equals(compact_last_observations(native))
    ties = index.filter(pl.col('event_ts').dt.hour() == 8).filter(pl.col('event_ts').dt.minute() == 45)
    assert ties['price_raw'].to_list() == ['11', '12']
