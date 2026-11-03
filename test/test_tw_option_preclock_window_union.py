from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from scripts.build_tw_option_cross_source_references import reference_map
from scripts.build_tw_option_preclock_window_union import merge_candidates
from stockagent.data.tw_option_reference_candidates import reference_candidates


DAY = date(2026, 10, 28)
TAIPEI = ZoneInfo('Asia/Taipei')
A, B = 'a' * 64, 'b' * 64


def event(stamp, strike, price, source):
    return {'event_ts': datetime.fromisoformat(stamp).replace(tzinfo=TAIPEI),
            'product_raw': 'TXO', 'series_raw': '202610', 'strike_raw': strike,
            'right_raw': 'C', 'price_raw': price, 'source_sha256': source}


def candidate(rows, clock):
    return reference_candidates(pl.DataFrame(rows), trading_date=DAY, decision_time=clock,
        session_windows={'TXO': (525, 825)}).with_columns(
            pl.lit(DAY).alias('decision_date'), pl.lit(clock).alias('decision_time'))


@pytest.mark.parametrize('clock', ['08:45', '09:00'])
def test_window_union_matches_raw_event_reduction_and_preserves_ambiguity(clock):
    official = [event('2026-09-15T13:00:00', '20000', '7', A),
                event('2026-10-28T08:46:00', '21000', '10', A),
                event('2026-10-28T08:46:00', '22000', '12', A),
                event('2026-10-28T08:44:59', '23000', '8', A)]
    finmind = [event('2026-10-28T08:45:00', '21000', '11', B),
               event('2026-10-28T08:46:00', '22000', '13', B),
               event('2026-10-28T08:44:58', '23000', '9', B)]
    actual = merge_candidates(candidate(official, clock), candidate(finmind, clock))
    assert reference_map(actual) == reference_map(candidate(official + finmind, clock))
    if clock == '09:00':
        by_key = reference_map(actual)
        assert by_key[('TXO', '202610', '21000', 'C')][:3] == (
            'observed_session_open_candidate', '11', '20261028084500')
        assert by_key[('TXO', '202610', '22000', 'C')] == (
            'ambiguous_first_second', None, '20261028084600', (A, B))
    else:
        assert reference_map(actual)[('TXO', '202610', '23000', 'C')][:3] == (
            'last_observed_trade_candidate', '8', '20261028084459')


def test_window_union_rejects_duplicate_source_keys():
    one = candidate([event('2026-10-28T08:44:59', '23000', '8', A)], '08:45')
    with pytest.raises(ValueError, match='duplicate'):
        merge_candidates(pl.concat([one, one]), one)
