from pathlib import Path

import polars as pl
import pytest

from stockagent.data.tw_stock_futures_carry import load_carry_daily_settlements, load_carry_no_trade_evidence


def test_recent_refresh_preserves_scope_and_proves_dq_zero_volume_valuation():
    path = Path('artifacts/data_preparation/futures_carry_evidence_v3_recent_20260910/official_evidence.parquet')
    if not path.exists():
        pytest.skip('remote verified official recent source unavailable')
    old = pl.read_parquet('artifacts/data_preparation/futures_carry_evidence_v2_settlement_20260909/official_evidence.parquet')
    new = pl.read_parquet(path)
    keys = ['date', 'physical_contract']
    assert new.height == old.height == 400778
    columns = [*keys, 'no_trade_proof_eligible']
    assert new.select(columns).sort(keys).equals(old.select(columns).sort(keys))
    earlier = pl.col('date') < pl.date(2026,8,12)
    assert old.filter(earlier).sort(keys).equals(new.filter(earlier).sort(keys))
    refreshed = new.filter(~earlier)
    assert refreshed.height == 8425
    assert not refreshed.filter(pl.col('official_reason') == 'missing_official_day').height
    match = (pl.col('physical_contract') == 'DQF:202612') & (pl.col('date') == pl.date(2026,9,4))
    assert load_carry_no_trade_evidence(path).filter(match).height == 1
    assert load_carry_daily_settlements(path).filter(match)['official_daily_settlement'].to_list() == [48.6]
