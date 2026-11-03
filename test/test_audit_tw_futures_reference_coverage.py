from datetime import date, datetime

import polars as pl
import pytest

from scripts.audit_tw_futures_reference_coverage import classify


def frames():
    day = date(2020, 1, 2)
    clock = datetime(2020, 1, 2, 8, 45)
    key = {'date': [day] * 3, 'physical_contract': ['TX:202001', 'TX:202002', 'CAF:202001']}
    ref = pl.DataFrame({**key, 'decision_ts': [clock] * 3,
                        'reference_kind': ['observed_session_open', 'unavailable', 'unavailable']})
    entries = pl.DataFrame({**key, 'status': ['observed', 'observed', 'source_gap'],
                            'entry_volume': [1., 2., 0.]})
    candidate = ref.slice(1, 1).select('date', 'physical_contract', 'decision_ts')
    return ref, entries, candidate


def test_reference_and_actual_window_counts_are_distinct():
    ref, entries, candidate = frames()
    out = classify(ref, entries, candidate, '08:45')
    states = {row['reference_state']: row for row in out['by_reference_state']}
    assert out['contract_days'] == 3 and out['candidate_rows'] == 1
    assert states['original']['positive_window_prints'] == 1
    assert states['candidate_only']['positive_window_prints'] == 1
    assert states['still_unavailable']['positive_window_prints'] == 0


def test_candidate_must_match_clock_and_unavailable_original():
    ref, entries, _ = frames()
    original = ref.slice(0, 1).select('date', 'physical_contract', 'decision_ts')
    with pytest.raises(ValueError, match='unavailable'):
        classify(ref, entries, original, '08:45')
    with pytest.raises(ValueError, match='axes'):
        classify(ref, entries.head(2), original.head(0), '08:45')


def test_candidate_duplicates_are_rejected():
    ref, entries, candidate = frames()
    with pytest.raises(ValueError, match='duplicate'):
        classify(ref, entries, pl.concat([candidate, candidate]), '08:45')
