from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_holiday_generation_paths import compare_paths


def _candidate():
    return {'product': 'ABC', 'contract': '201202', 'physical_contract': 'ABC:201202@unsettled#generation=0',
            'corporate_generation': 0, 'settlement_date': '2012-02-15'}


def _row(day, generation, *, product='ABC', contract='201202'):
    return {'product': product, 'contract': contract, 'date': date.fromisoformat(day),
            'physical_contract': f'{product}:{contract}@unsettled#generation={generation}',
            'corporate_generation': generation, 'lifetime_status': 'candidate',
            'corporate_boundary': date(2012, 2, 10), 'corporate_transfer_target': 'AB1',
            'corporate_boundary_sources': ['a' * 64], 'corporate_identity_candidate_only': True,
            'terminal_event': 'mark_only'}


def test_detects_generation_transition_without_joining_final_price():
    source = pl.DataFrame([_row('2012-01-18', 0), _row('2012-02-10', 1), _row('2012-02-15', 1)])
    result = compare_paths([_candidate()], source, date(2012, 1, 18))[0]
    assert result['status'] == 'recorded_generation_change'
    assert result['first_different_generation_date'] == '2012-02-10'
    assert result['settlement_date_generations'] == [1]
    assert not result['original_generation_present_on_settlement_date']
    assert result['pre_transfer_target_candidate'] == 'AB1'
    assert result['pre_boundary_source_sha256s'] == ['a' * 64]


def test_ignores_later_generation_after_this_maturity_and_other_contracts():
    source = pl.DataFrame([_row('2012-01-18', 0), _row('2012-02-15', 0),
                           _row('2012-02-16', 1), _row('2012-02-10', 7, contract='201203')])
    result = compare_paths([_candidate()], source, date(2012, 1, 18))[0]
    assert result['status'] == 'same_generation_recorded_not_proof_of_continuity'
    assert result['recorded_generations'] == [0]
    assert result['original_generation_present_on_settlement_date']


@pytest.mark.parametrize('mode', ['missing_initial', 'duplicate_initial', 'duplicate_candidate', 'null_generation'])
def test_rejects_missing_or_ambiguous_generation_identity(mode):
    candidate = _candidate()
    rows = [_row('2012-01-18', 0)]
    candidates = [candidate]
    if mode == 'missing_initial':
        rows = [_row('2012-01-19', 0)]
    elif mode == 'duplicate_initial':
        rows.append(dict(rows[0]))
    elif mode == 'duplicate_candidate':
        candidates.append(dict(candidate))
    else:
        rows.append({**_row('2012-01-19', 1), 'corporate_generation': None})
    with pytest.raises(ValueError):
        compare_paths(candidates, pl.DataFrame(rows), date(2012, 1, 18))
