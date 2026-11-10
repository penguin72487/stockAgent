#!/usr/bin/env python3
"""Stage retained TAIFEX and broker futures source roots on the E workspace."""
from scripts.stage_futures_aux_e_working_copy import stage_sources


SOURCES = (
    ('data_tw_futures/cpf_gap_sources', 'sources/9c3cf3f7f71273f51e7ab0d02f2964775205eb19cb6930b784b48c4e0bc072e5.json'),
    ('data_tw_futures/final_settlement_all_history_20260929', 'seed_receipts.json'),
    ('data_tw_futures/final_settlement_index_history_20260928', 'futures_final_settlement_history.parquet'),
    ('data_tw_futures/final_settlement_v1', 'futures_final_settlement_history.parquet'),
    ('data_tw_futures/shioaji_contracts', 'futures_products.md'),
    ('data_tw_futures/shioaji_gap_repair', 'repair_tick_progress.json'),
    ('data_tw_futures/stock_futures_catalog_sources', '55d0dfbea232141ad2fba386141758059b1355bbcfa29bb95a15da9a4d6b5bc2.html'),
    ('data_tw_futures/taifex_portfolio_daily', 'product_master.parquet'),
    ('data_tw_futures/taifex_portfolio_daily_v4', 'product_master.parquet'),
    ('data_tw_futures/taifex_stock_futures_minute_history_v2', 'gaps.parquet'),
    ('data_tw_futures/taifex_stock_futures_minute_v1', 'minutes.parquet'),
)


if __name__ == '__main__':
    raise SystemExit(stage_sources(SOURCES, prefix='futures-native-',
        aggregate_name='futures-native-sources-e-20261103-v1.json'))
