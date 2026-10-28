import polars as pl
import pytest

from scripts.audit_tw_futures_holiday_expiry_candidates import match_candidates


def test_product_month_match_retains_generation_ambiguity():
    pre = pl.DataFrame({'product': ['ABC', 'ABC'],
                        'physical_contract': ['ABC:201209@unsettled#generation=0',
                                              'ABC:201209@unsettled#generation=1']})
    final = pl.DataFrame({'product': ['ABC'], 'contract': ['201209'], 'settlement_date': ['2012-09-19']})
    result = match_candidates(pre, final)
    assert result.height == 2
    assert result['pre_generations_for_key'].to_list() == [2, 2]
    assert result['settlement_date'].to_list() == ['2012-09-19', '2012-09-19']


def test_product_month_match_rejects_ambiguous_physical_identity():
    pre = pl.DataFrame({'product': ['ABC', 'ABC'],
                        'physical_contract': ['ABC:201209@unsettled#generation=0'] * 2})
    final = pl.DataFrame({'product': ['ABC'], 'contract': ['201209']})
    with pytest.raises(ValueError, match='identity is absent or ambiguous'):
        match_candidates(pre, final)
