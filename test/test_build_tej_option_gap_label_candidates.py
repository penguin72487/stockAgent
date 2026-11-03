from datetime import date

import pytest

from scripts.build_tej_option_gap_label_candidates import identity, link


def test_matches_exact_decimal_but_preserves_original_label():
    row = {'date': date(2011, 2, 16), 'product': 'CAO', 'series': '201103',
           'strike_raw': '10.00', 'right': 'C', 'session': 'unreported',
           'source_member': 'native.csv', 'source_record_ordinal': 8, 'source_sha256': 'x'}
    label = 'CAO201103C10==>南亞買權 201103 10'
    result = link([row], [label])[0]
    assert result['candidate_labels'] == [label]
    assert result['official_strike_raw'] == '10.00'
    assert result['status'] == 'unique_label_candidate'


def test_keeps_multiple_exact_decimal_aliases_ambiguous():
    row = {'date': date(2011, 2, 16), 'product': 'CAO', 'series': '201103',
           'strike_raw': '10.00', 'right': 'C', 'session': 'unreported',
           'source_member': 'native.csv', 'source_record_ordinal': 8, 'source_sha256': 'x'}
    labels = ['CAO201103C10==>南亞買權 201103 10', 'CAO201103C10.0==>南亞買權 201103 10.0']
    result = link([row], labels)[0]
    assert result['candidate_count'] == 2
    assert result['status'] == 'ambiguous_label_candidates'


@pytest.mark.parametrize('strike', ['nan', 'Infinity', '-1', 'not a price'])
def test_unknown_strike_is_rejected(strike):
    with pytest.raises(ValueError):
        identity('CAO', '201103', strike, 'C')
