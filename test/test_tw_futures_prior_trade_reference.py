from datetime import date, datetime

import polars as pl
import pytest

from stockagent.data.tw_futures_decision_clock import (
    OPENING_REFERENCE_DATASET,
    opening_reference_contract,
    resolve_opening_references,
)
from stockagent.data.tw_futures_prior_trade_reference import prior_trade_events
from scripts.build_taifex_futures_prior_trade_candidates import candidate_references
from scripts.build_taifex_futures_prior_trade_window import stale_reference_counts, validate_reference_document


@pytest.mark.parametrize('version', [1, 2, 3])
def test_window_accepts_bound_supported_reference_abis(version):
    document = {'dataset': OPENING_REFERENCE_DATASET, 'status': 'complete',
                'contract': opening_reference_contract('08:45', version=version),
                'data_sha256': 'a' * 64, 'source_daily_sha256': 'b' * 64}
    assert validate_reference_document(document, data_sha='a' * 64, daily_sha='b' * 64)['version'] == version
    with pytest.raises(ValueError):
        validate_reference_document(document, data_sha='a' * 64, daily_sha='c' * 64)
    document['contract'] = opening_reference_contract('09:00', version=version)
    with pytest.raises(ValueError):
        validate_reference_document(document, data_sha='a' * 64, daily_sha='b' * 64)


def test_official_events_match_physical_generation_and_never_pick_tie():
    day = date(2026, 9, 24)
    sessions = pl.DataFrame([
        {'date': date(2026, 9, 23), 'product': 'TX', 'contract': '202610',
         'physical_contract': 'TX:202610#g0', '_start': 31500, '_end': 49500},
        {'date': day, 'product': 'TX', 'contract': '202610',
         'physical_contract': 'TX:202610#g0', '_start': 31500, '_end': 49500},
        {'date': date(2026, 9, 23), 'product': 'TX', 'contract': '202611',
         'physical_contract': 'TX:202611#g0', '_start': 31500, '_end': 49500},
        {'date': day, 'product': 'TX', 'contract': '202611',
         'physical_contract': 'TX:202611#g1', '_start': 31500, '_end': 49500},
    ])
    prints = pl.DataFrame([
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 23, 13, 45), 'price': 99.},
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 23, 22, 0), 'price': 100.},
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 24, 4, 59), 'price': 101.},
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 24, 4, 59), 'price': 102.},
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 24, 8, 45), 'price': 103.},
        {'product': 'TX', 'contract': '202611', 'event_ts': datetime(2026, 9, 23, 22, 0), 'price': 100.},
    ])
    events, stats = prior_trade_events(prints, sessions, day=day, source_sha256='a' * 64)
    assert stats['different_generation_identity_rows'] == 1
    assert stats['ambiguous_last_second_events'] == 1
    assert events.height == 2
    decisions = pl.DataFrame({'date': [day], 'physical_contract': ['TX:202610#g0']})
    result = resolve_opening_references(decisions, events, decision_time='08:45', version=2).row(0, named=True)
    assert result['reference_quote'] is None and result['reference_kind'] == 'unavailable'
    assert result['prior_trade_quote'] == 100.


def test_incomplete_minute_cannot_enter_midnight_denominator():
    day = date(2026, 9, 24)
    sessions = pl.DataFrame([{'date': d, 'product': 'TX', 'contract': '202610',
                              'physical_contract': 'TX:202610#g0', '_start': 31500, '_end': 49500}
                             for d in (date(2026, 9, 23), day)])
    prints = pl.DataFrame([{'product': 'TX', 'contract': '202610', 'event_ts': ts, 'price': price}
                           for ts, price in [(datetime(2026, 9, 23, 23, 58, 59), 100.),
                                             (datetime(2026, 9, 23, 23, 59, 59), 101.)]])
    events, _ = prior_trade_events(prints, sessions, day=day, source_sha256='a' * 64)
    decisions = pl.DataFrame({'date': [day], 'physical_contract': ['TX:202610#g0']})
    result = resolve_opening_references(decisions, events, decision_time='08:45', version=2).row(0, named=True)
    assert result['reference_quote'] == 101. and result['prior_trade_quote'] == 100.


def test_saturday_overnight_bridge_requires_identical_surrounding_generation():
    day = date(2026, 9, 14)
    sessions = pl.DataFrame([
        {'date': d, 'product': 'TX', 'contract': contract, 'physical_contract': physical,
         '_start': 31500, '_end': 49500}
        for d, contract, physical in [
            (date(2026, 9, 11), '202610', 'TX:202610#g0'),
            (day, '202610', 'TX:202610#g0'),
            (date(2026, 9, 11), '202611', 'TX:202611#g0'),
            (day, '202611', 'TX:202611#g1')]])
    prints = pl.DataFrame([
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 12, 4, 59), 'price': 101.},
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 12, 5, 0, 1), 'price': 102.},
        {'product': 'TX', 'contract': '202611', 'event_ts': datetime(2026, 9, 12, 4, 59), 'price': 103.},
    ])
    events, stats = prior_trade_events(prints, sessions, day=day, source_sha256='a' * 64)
    assert stats['missing_natural_date_identity_rows'] == 3
    assert stats['same_generation_overnight_bridge_rows'] == 1
    assert stats['unresolved_natural_date_identity_rows'] == 2
    assert events.height == 1 and events['price'][0] == 101.
    assert events['trade_date'][0] == date(2026, 9, 12)


def test_candidate_pair_preserves_known_open_and_withholds_unreconstructable_late_rows():
    day = date(2026, 9, 24)
    decisions = pl.DataFrame({'date': [day] * 3, 'physical_contract': ['A', 'B', 'C'],
                              'symbol': ['a', 'b', 'c']})
    old_events = pl.DataFrame({
        'physical_contract': ['A', 'B', 'A'], 'price': [100., 100., 110.],
        'trade_date': [date(2026, 9, 23), date(2026, 9, 23), day],
        'trade_ts': [datetime(2026, 9, 23, 13), datetime(2026, 9, 23, 13), datetime(2026, 9, 24, 8, 45)],
        'available_by': [datetime(2026, 9, 23, 13, 1), datetime(2026, 9, 23, 13, 1),
                         datetime(2026, 9, 24, 8, 46)],
        'source_sha256': ['b' * 64] * 3, 'is_session_open': [False, False, True]})
    early = resolve_opening_references(decisions, old_events, decision_time='08:45')
    late = resolve_opening_references(decisions, old_events, decision_time='09:00').with_columns(
        pl.when(pl.col('physical_contract') == 'B').then(None)
        .otherwise(pl.col('reference_trade_ts')).alias('reference_trade_ts'))
    official = pl.DataFrame({
        'physical_contract': ['A', 'B'], 'price': [105., 106.], 'trade_date': [day, day],
        'trade_ts': [datetime(2026, 9, 24, 4)] * 2,
        'available_by': [datetime(2026, 9, 24, 4, 1)] * 2,
        'source_sha256': ['a' * 64] * 2, 'is_session_open': [False] * 2,
        'opening_ambiguous': [False] * 2, 'fallback_ambiguous': [False] * 2})
    result, stats = candidate_references(early, late, official, day)
    assert stats['late_rows_withheld_and_unchanged'] == 2
    a_early = result['08:45'].filter(pl.col('physical_contract') == 'A').row(0, named=True)
    a_late = result['09:00'].filter(pl.col('physical_contract') == 'A').row(0, named=True)
    assert a_early['reference_quote'] == 105.
    assert a_late['reference_quote'] == 110. and a_late['reference_kind'] == 'observed_session_open'
    withheld = result['09:00'].filter(pl.col('physical_contract') != 'A')
    assert withheld['candidate_selection'].to_list() == ['unchanged_legacy_v1_unknown'] * 2
    assert withheld.drop('candidate_selection').equals(
        late.filter(pl.col('physical_contract') != 'A'))


def test_window_does_not_call_unavailable_or_same_day_date_only_a_stale_trade():
    refs = pl.DataFrame({'physical_contract': ['A', 'B', 'C'],
        'reference_kind': ['unavailable', 'last_observed_trade', 'last_observed_trade'],
        'reference_quote': [None, 100., 100.], 'reference_trade_ts': [None, None, None],
        'reference_trade_date': [None, date(2026, 9, 24), date(2026, 9, 23)]},
        schema_overrides={'reference_trade_ts': pl.Datetime('us')})
    events = pl.DataFrame({'physical_contract': ['A', 'B', 'C'], 'price': [101.] * 3,
        'trade_ts': [datetime(2026, 9, 24, 4)] * 3, 'fallback_ambiguous': [False] * 3})
    counts = stale_reference_counts(refs, events)
    assert counts['official_candidates_joined'] == 3
    assert counts['official_candidate_for_old_unavailable'] == 1
    assert counts['later_official_trade'] == 1
    assert counts['later_unambiguous_price_different'] == 1
