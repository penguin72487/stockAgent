import polars as pl
import pytest

from scripts.audit_finmind_option_tick_query_shapes import compare_frames, normalize


def _row(**changes):
    return {'date': '2016-07-15 08:49:22', 'option_id': 'RHO', 'ExercisePrice': 6.76,
            'contract_date': '201607', 'PutCall': 'C', 'price': 0.0007, 'volume': 1, **changes}


def test_source_shape_comparison_preserves_duplicates_and_price_loss():
    market = pl.DataFrame([_row(), _row()])
    product = pl.DataFrame([_row(price=0.0, contract_date='201607.0'), _row()])
    result = compare_frames(market, product)
    assert result['comparison'] == 'source_row_difference'
    assert result['price_inclusive_rows_only_in_market'] == 1
    assert result['price_inclusive_rows_only_in_product'] == 1
    assert result['nonprice_rows_only_in_market'] == result['nonprice_rows_only_in_product'] == 0
    assert result['product_nonpositive_vs_market_positive_rows_lower_bound'] == 1
    assert compare_frames(market, market)['comparison'] == 'identical_at_float32'


def test_source_shape_comparison_keeps_week_and_rejects_missing_fields():
    weekly = pl.DataFrame([_row(contract_date='201607W1')])
    assert normalize(weekly)['contract_date'][0] == '201607W1'
    assert compare_frames(weekly, pl.DataFrame([_row()]))['comparison'] == 'source_row_difference'
    with pytest.raises(ValueError, match='schema incomplete'):
        normalize(weekly.drop('volume'))
