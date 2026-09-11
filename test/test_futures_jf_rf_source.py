from pathlib import Path

import polars as pl
import pytest

from stockagent.data.tw_stock_futures_transition import load_transition_bundle


def test_jf_rf_complete_continuation_and_official_full_final_values():
    path=Path('artifacts/data_preparation/futures_corporate_transitions_v4_20260910/manifest.json')
    if not path.exists():pytest.skip('remote verified source fixture unavailable')
    bundle=load_transition_bundle(path)
    coverage=bundle['coverage'].filter(pl.col('physical_contract').is_in(['JF1:202303','RF1:202303']))
    assert coverage.height==8
    assert coverage.filter(pl.col('status')=='minute_verified')['tick_rows'].sum()==25
    assert coverage.filter(pl.col('status')=='official_no_outright_trades').height==2
    finals=dict(bundle['finals'].select('physical_contract','final_settlement_value').iter_rows())
    assert finals['JF1:202303']==1593063
    assert finals['RF1:202303']==79653
    assert any(r['from_physical_contract']=='RFF:202303' and r['new_multiplier']==100 for r in bundle['records'])
