from datetime import date

import polars as pl

from stockagent.data.tw_futures_margin_release import select_complete_standard_stock_lives


def test_scope_drops_whole_broken_life_and_cannot_cut_corporate_inventory():
    rows, rules, blockers = [], [], []
    for product in ['OKF', 'BAD', 'AD1', 'CASH', 'NEW', 'TX']:
        for i in range(1, 4):
            d = date(2026, 1, i)
            rows.append(dict(date=d, physical_contract=product, product=product,
                asset_class='index_future' if product == 'TX' else 'stock_future',
                lifetime_status='observed_at_dataset_boundary', next_market_date=date(2026,1,i+1),
                cash_settlement=False,open=100.,settlement=100.))
            blockers.append(dict(physical_contract=product, has_blocker=product=='BAD' and i==2,
                                  is_warmup=i==1))
            if i>1:
                rules.append(dict(date=d, physical_contract=product, product=product,
                    carry_from_date=date(2026,1,i-1) if i>2 or product=='NEW' else None,
                    carry_from_physical_contract=('MISSING' if product=='NEW' else product) if i>2 or product=='NEW' else '',
                    carry_cash_twd=1. if product=='CASH' else 0., terminal_event='mark_only',
                    carry_quantity_numerator=1, carry_quantity_denominator=1,
                    contract_multiplier=2000., opening_contract_value_twd=200000.,
                    settlement_contract_value_twd=200000.))
    f,r,c=select_complete_standard_stock_lives(pl.DataFrame(rows),pl.DataFrame(rules),pl.DataFrame(blockers),
        start=date(2026,1,1),end=date(2026,1,3))
    assert set(f['product'])=={'OKF'}
    assert f.height==3 and r.height==2
    assert f.filter(pl.col('date')==date(2026,1,3))['next_market_date'].item() is None
    assert c.filter(pl.col('product')=='BAD')['selected'].item() is False
