from datetime import date
import pytest
from stockagent.research.taifex_transaction_tax import futures_transaction_tax_rate, futures_transaction_tax_twd


@pytest.mark.parametrize('day,rate', [('1998-07-21',.0005), ('2000-04-30',.0005),
    ('2000-05-01',.00025),('2005-12-31',.00025),('2006-01-01',.0001),
    ('2008-10-05',.0001),('2008-10-06',.00004),('2013-03-31',.00004),
    ('2013-04-01',.00002)])
def test_applicable_equity_rate_changes_on_effective_day(day,rate):
    assert futures_transaction_tax_rate(date.fromisoformat(day),tax_class='stock_index')==rate


def test_product_tax_bases_do_not_use_margin_or_pnl_multiplier():
    day=date(2026,7,6)
    assert futures_transaction_tax_twd(500000,tax_class='gold',trading_date=day)==1
    assert futures_transaction_tax_twd(500000,tax_class='oil',trading_date=day)==3
    assert futures_transaction_tax_twd(500000,tax_class='stock',trading_date=day)==10
    assert futures_transaction_tax_twd(100000000,tax_class='commercial_paper',trading_date=date(2010,1,4))==13
    assert futures_transaction_tax_twd(5000000,tax_class='government_bond',trading_date=date(2010,1,4))==6
    assert futures_transaction_tax_rate(date(2005,1,4),tax_class='government_bond')==0
    with pytest.raises(ValueError):
        futures_transaction_tax_rate(date(2018,7,1),tax_class='oil')
    with pytest.raises(ValueError):
        futures_transaction_tax_rate(day,tax_class='unknown')
