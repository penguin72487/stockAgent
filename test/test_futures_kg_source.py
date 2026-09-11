from pathlib import Path

import polars as pl
import pytest

from stockagent.data.tw_stock_futures_transition import load_transition_bundle


def test_kg_complete_source_days_and_embedded_rights_final_value():
    path = Path('artifacts/data_preparation/futures_corporate_transitions_v5_20260910/manifest.json')
    if not path.exists():
        pytest.skip('remote verified KG source fixture unavailable')
    bundle = load_transition_bundle(path)
    coverage = bundle['coverage'].filter(pl.col('physical_contract') == 'KG1:202311')
    assert coverage.height == 6
    assert coverage['status'].unique().to_list() == ['minute_verified']
    assert coverage['tick_rows'].sum() == 700
    final = bundle['finals'].filter(pl.col('physical_contract') == 'KG1:202311')
    assert final['final_settlement_value'].to_list() == [278811.]
    assert any(r['from_physical_contract'] == 'KGF:202311' and r['new_multiplier'] == 2000 for r in bundle['records'])
