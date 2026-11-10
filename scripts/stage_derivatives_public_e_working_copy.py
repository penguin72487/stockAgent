#!/usr/bin/env python3
"""Stage official derivatives and broker-history source snapshots on E."""
from scripts.stage_futures_aux_e_working_copy import stage_sources


SOURCES = (
    ('data_taifex_public_history', 'manifest.json'),
    ('data_tw_index_options_daily', 'manifest.json'),
    ('data_tw_shioaji_history', 'summary.json'),
)


if __name__ == '__main__':
    raise SystemExit(stage_sources(SOURCES, prefix='derivatives-public-',
        aggregate_name='derivatives-public-sources-e-20261103-v1.json'))
