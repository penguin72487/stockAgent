from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from scripts.build_finmind_option_reference_boundary import compare, selected_windows
from scripts.build_taifex_option_reference_source_window import derive_day


def source(day, clock, price, sha):
    return {'event_ts': datetime.fromisoformat(f'{day}T{clock}').replace(tzinfo=ZoneInfo('Asia/Taipei')),
            'product_raw': 'TXO', 'series_raw': '201101', 'strike_raw': '8000',
            'right_raw': 'C', 'price_raw': str(price), 'source_sha256': sha * 64}


def test_previous_week_observation_seeds_reference_without_changing_session_open():
    prior = pl.DataFrame([source('2011-01-07', '08:50:00', 15, 'a'),
                          source('2011-01-07', '13:00:00', 16, 'a')])
    current = pl.DataFrame([source('2011-01-10', '08:45:00', 21, 'b'),
                            source('2011-01-10', '08:46:00', 22, 'b')])
    _, state = derive_day(None, prior, date(2011, 1, 7), {'TXO': (525, 825)})
    old, _ = derive_day(None, current, date(2011, 1, 10), {'TXO': (525, 825)})
    seeded, _ = derive_day(state, current, date(2011, 1, 10), {'TXO': (525, 825)})
    early = seeded.filter(pl.col('decision_time') == '08:45')
    late = seeded.filter(pl.col('decision_time') == '09:00')
    assert early.item(0, 'reference_price_decimal') == '16'
    assert early.item(0, 'trade_ts').date() == date(2011, 1, 7)
    assert late.item(0, 'reference_price_decimal') == '21'
    assert late.item(0, 'reference_kind') == 'observed_session_open_candidate'
    assert compare(old, seeded) == {'baseline_rows': 1, 'seeded_rows': 2,
                                     'new_candidate_keys': 1, 'old_keys_changed': 0,
                                     'old_session_open_keys_changed': 0}


def test_ambiguous_or_observed_open_cannot_change_silently():
    current = pl.DataFrame([source('2011-01-10', '08:45:00', 21, 'b')])
    old, _ = derive_day(None, current, date(2011, 1, 10), {'TXO': (525, 825)})
    altered = old.with_columns(pl.lit('99').alias('reference_price_decimal'))
    with pytest.raises(ValueError, match='changed an already observed'):
        compare(old, altered)
    with pytest.raises(ValueError, match='removed'):
        compare(old, old.head(0))


def test_boundary_reports_known_missing_source_day_and_never_skips_a_window():
    plan = {'contract': 'finmind_option_reference_history_plan_v1', 'training_ready': False,
            'windows': [
                {'start': '2011-02-14', 'end': '2011-02-15',
                 'source_receipt_pins': [{'date': '2011-02-14'}, {'date': '2011-02-15'}]},
                {'start': '2011-02-17', 'end': '2011-02-18',
                 'source_receipt_pins': [{'date': '2011-02-17'}, {'date': '2011-02-18'}]},
            ], 'not_planned_days': [{'date': '2011-02-16'}]}
    previous, target, missing = selected_windows(plan, date(2011, 2, 17))
    assert previous['end'] == '2011-02-15' and target['start'] == '2011-02-17'
    assert missing == ['2011-02-16']
    with pytest.raises(ValueError, match='preceding'):
        selected_windows(plan, date(2011, 2, 14))
    plan['windows'][0]['end'] = '2011-02-16'
    with pytest.raises(ValueError, match='ordered'):
        selected_windows(plan, date(2011, 2, 17))
