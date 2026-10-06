from datetime import date

import pytest

from scripts.probe_finmind_supplemental import probe_us_date_range


FIRST = date(2026, 10, 1)


def row(day, volume=1):
    return {'date': day + ' 09:30:00', 'stock_id': '^DJI', 'close': 1, 'volume': volume}


def test_range_probe_rejects_silently_ignored_end_date():
    calls = []
    def fetch(endpoint, params):
        calls.append(params.copy())
        return [row(params['start_date'])]
    result = probe_us_date_range(fetch, FIRST, 2)
    assert result['date_range_parity_verified'] is False
    assert result['days'][1]['single_day_rows'] == 1
    assert result['days'][1]['range_rows'] == 0
    assert len(calls) == 3
    assert all('end_date' not in params for params in calls[1:])


def test_range_probe_preserves_duplicate_multiplicity_and_does_not_admit_shape():
    def fetch(endpoint, params):
        if 'end_date' in params:
            return [row('2026-10-02'), row('2026-10-01'), row('2026-10-01')]
        return [row(params['start_date'])] * (2 if params['start_date'] == '2026-10-01' else 1)
    result = probe_us_date_range(fetch, FIRST, 2)
    assert result['date_range_parity_verified'] is True
    assert result['queue_writes'] == 0
    assert result['status'] == 'range_parity_verified_not_admitted'


def test_empty_references_cannot_prove_range():
    assert probe_us_date_range(lambda *args: [], FIRST, 2)['date_range_parity_verified'] is False


def test_range_probe_rejects_outside_dates_and_wrong_identity():
    for value in [row('2026-10-03'), {**row('2026-10-01'), 'stock_id': 'WRONG'}]:
        with pytest.raises(ValueError):
            probe_us_date_range(lambda *args: [value], FIRST, 2)


@pytest.mark.parametrize('days', [1, 8])
def test_range_probe_is_bounded(days):
    with pytest.raises(ValueError):
        probe_us_date_range(lambda *args: [], FIRST, days)
