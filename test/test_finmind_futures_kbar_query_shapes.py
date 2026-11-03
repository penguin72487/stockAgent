import polars as pl
import pytest

from scripts.audit_finmind_futures_kbar_query_shapes import compare_rows


def test_comparison_preserves_row_multiplicity_and_price_differences():
    row = {'date': '2016-11-08', 'futures_id': 'I5F', 'contract_date': '201611',
           'minute': '08:45:00', 'open': 1., 'high': 1., 'low': 1., 'close': 1., 'volume': 1}
    a = pl.DataFrame([row, row])
    b = pl.DataFrame([row, {**row, 'close': 2.}])
    assert compare_rows(a, b) == (1, 1)
    assert compare_rows(a, a) == (0, 0)
    with pytest.raises(ValueError, match='schema incomplete'):
        compare_rows(a.drop('volume'), b)
