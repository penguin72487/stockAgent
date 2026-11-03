from datetime import date
import hashlib
import json

import pytest

from scripts.audit_finmind_option_event_index_cross_window import prefix


def window(*dates):
    pins = [{'date': day, 'planned': True} for day in dates]
    encoded = json.dumps(pins, sort_keys=True, separators=(',', ':')).encode()
    return {'start': dates[0], 'end': dates[-1], 'source_days': len(dates),
            'source_receipt_pins': pins,
            'source_group_fingerprint': hashlib.sha256(encoded).hexdigest()}


def plan(*windows):
    return {'contract': 'finmind_option_reference_history_plan_v1',
            'training_ready': False, 'windows': list(windows)}


def test_cross_window_prefix_keeps_all_earlier_windows_even_with_source_gap():
    parts = [window('2011-02-14', '2011-02-15'), window('2011-02-17', '2011-02-18'),
             window('2011-02-21')]
    selected = prefix(plan(*parts), date(2011, 2, 17))
    assert selected == parts[:2]
    with pytest.raises(ValueError, match='own planned retained source'):
        prefix(plan(*parts), date(2011, 2, 16))


def test_cross_window_prefix_rejects_out_of_order_or_changed_source_group():
    early, late = window('2011-01-03'), window('2011-01-10')
    with pytest.raises(ValueError, match='strictly ordered'):
        prefix(plan(late, early, window('2011-01-17')), date(2011, 1, 17))
    late['source_receipt_pins'][0]['date'] = '2011-01-11'
    with pytest.raises(ValueError, match='fingerprint'):
        prefix(plan(early, late), date(2011, 1, 11))
