from datetime import date, datetime

import polars as pl
import pytest

from scripts.audit_taifex_futures_prior_trade_references import compare_prior


def test_prior_reference_requires_same_contract_and_strictly_before_clock():
    refs = pl.DataFrame([
        {'physical_contract': 'TX:202610@unsettled#generation=0', 'reference_kind': 'last_observed_trade',
         'reference_quote': 100.0, 'reference_trade_ts': datetime(2026, 9, 23, 13, 0)},
        {'physical_contract': 'MTX:202610@unsettled#generation=0', 'reference_kind': 'unavailable',
         'reference_quote': None, 'reference_trade_ts': None},
    ])
    official = pl.DataFrame([
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 24, 4, 0), 'price': 102.0},
        {'product': 'TX', 'contract': '202610', 'event_ts': datetime(2026, 9, 24, 8, 45), 'price': 103.0},
        {'product': 'MTX', 'contract': '202611', 'event_ts': datetime(2026, 9, 24, 4, 0), 'price': 101.0},
    ])
    actual = {x['physical_contract']: x for x in compare_prior(refs, official, date(2026, 9, 24))}
    tx = actual['TX:202610@unsettled#generation=0']
    assert tx['strictly_later_official_trade'] and tx['official_last_price_if_unambiguous'] == 102.0
    assert tx['price_differs_if_unambiguous'] is True
    assert not actual['MTX:202610@unsettled#generation=0']['strictly_later_official_trade']


def test_unavailable_reference_is_reported_separately_from_stale_reference():
    refs = pl.DataFrame([{'physical_contract': 'TX:202610@unsettled#generation=0',
                          'reference_kind': 'unavailable', 'reference_quote': None,
                          'reference_trade_ts': None}])
    official = pl.DataFrame([{'product': 'TX', 'contract': '202610',
                               'event_ts': datetime(2026, 9, 24, 4), 'price': 101.0}])
    row = compare_prior(refs, official, date(2026, 9, 24))[0]
    assert row['official_trade_where_old_reference_unavailable']
    assert not row['strictly_later_official_trade'] and row['price_differs_if_unambiguous'] is None


def test_last_second_price_conflict_is_never_picked_and_duplicate_refs_fail():
    refs = pl.DataFrame([{'physical_contract': 'TX:202610@unsettled#generation=0',
                          'reference_kind': 'last_observed_trade', 'reference_quote': 100.0,
                          'reference_trade_ts': datetime(2026, 9, 23, 13, 0)}])
    official = pl.DataFrame([{'product': 'TX', 'contract': '202610',
                               'event_ts': datetime(2026, 9, 24, 4, 0), 'price': p} for p in (101.0, 102.0)])
    result = compare_prior(refs, official, date(2026, 9, 24))[0]
    assert result['strictly_later_official_trade']
    assert result['official_last_second_price_count'] == 2
    assert result['official_last_price_if_unambiguous'] is None
    assert result['price_differs_if_unambiguous'] is None
    with pytest.raises(ValueError, match='duplicate physical'):
        compare_prior(pl.concat([refs, refs]), official, date(2026, 9, 24))
