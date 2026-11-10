#!/usr/bin/env python3
"""Stage retained TX history and benchmark-source inputs on E."""
from scripts.stage_futures_aux_e_working_copy import stage_sources


SOURCES = (
    ('data_tw_index_futures/shioaji_history/TXFR1', 'manifest.json'),
    ('data_tw_index_futures/training_prepared/tx_contract_days_audit_projection_20261030_v1', 'manifest.json'),
    ('data_tw_index_futures/training_prepared/tx_daily_account_marks_full_history_20261030_v1', 'manifest.json'),
    ('data_tw_index_futures/training_prepared/tx_front_final_settlement_reference_20261030_v1', 'manifest.json'),
    ('data_tw_index_futures/training_prepared/tx_front_reference_full_history_20261030_v1', 'manifest.json'),
)


if __name__ == '__main__':
    raise SystemExit(stage_sources(SOURCES, prefix='futures-tx-',
        aggregate_name='futures-tx-inputs-e-20261103-v1.json'))
