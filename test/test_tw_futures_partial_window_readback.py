from datetime import date
import json

import pytest
import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file
from scripts.verify_tw_futures_partial_window_observations import independent_window, _bound_metadata
from test_tw_futures_execution_terms import specification


def test_native_tick_readback_has_strict_postdecision_boundaries_and_keeps_duplicates():
    ticks = [
        ('2016-11-07 08:45:59', 100., 2),
        ('2016-11-07 08:46:00', 101., 4),
        ('2016-11-07 08:46:00', 101., 4),
        ('2016-11-07 08:46:45', 103., 2),
        ('2016-11-07 08:47:00', 150., 2),
    ]
    volume, vwap = independent_window(ticks, day='2016-11-07',
                                      decision_time='08:45', window_minutes=1)
    assert volume == 5
    assert vwap == pytest.approx(101.4)
    assert independent_window(ticks, day='2016-11-07',
                              decision_time='09:00', window_minutes=1) is None


@pytest.mark.parametrize('price,volume', [(100.5, 2), (100., 3), (100., 0)])
def test_native_tick_readback_rejects_unverified_integer_grid_or_quantity(price, volume):
    with pytest.raises(ValueError, match='integer price or invalid'):
        independent_window([('2016-11-07 08:46:00', price, volume)],
                           day='2016-11-07', decision_time='08:45', window_minutes=1)


def test_all_product_readback_uses_dated_stock_grid_and_exact_session_end():
    ticks = [
        ('2024-06-03 08:46:00', float(np.float32(49.95)), 4),
        ('2024-06-03 13:30:00', float(np.float32(50.1)), 2),
        ('2024-06-03 13:30:01', float(np.float32(50.2)), 200),
    ]
    volume, vwap = independent_window(ticks, day='2024-06-03', decision_time='08:45',
        window_minutes=300, tick_kind='stock_future', session_start='08:45:00',
        session_end='13:30:00')
    assert volume == 3
    assert vwap == pytest.approx((49.95 * 2 + 50.1) / 3)


def test_all_product_readback_rejects_off_grid_and_uses_fixed_subcent_ticks():
    volume, vwap = independent_window([('2024-06-03 09:01:00', float(np.float32(1.2345)), 2)],
        day='2024-06-03', decision_time='09:00', window_minutes=1,
        tick_kind='fixed', tick_size=0.0001)
    assert volume == 1
    assert vwap == pytest.approx(1.2345)
    with pytest.raises(ValueError, match='unique dated legal tick'):
        independent_window([('2024-06-03 09:01:00', float(np.float32(1.23456)), 2)],
            day='2024-06-03', decision_time='09:00', window_minutes=1,
            tick_kind='fixed', tick_size=0.0001)


def test_all_product_readback_binds_physical_contract_and_unknown_final_cutoff(tmp_path):
    day = date(2026, 1, 5)
    daily = tmp_path / 'daily.parquet'
    pl.DataFrame([dict(date=day, product='ABC', contract='202603', physical_contract='abc-g0',
        asset_class='stock_future', official_expiry=None)],
        schema_overrides={'official_expiry': pl.Date}).write_parquet(daily)
    specs = tmp_path / 'specifications.parquet'
    pl.DataFrame([specification()]).write_parquet(specs)
    (tmp_path / 'manifest.json').write_text(json.dumps(dict(dataset='taifex_dated_product_specifications',
        outputs={'specifications.parquet': {'sha256': sha256_file(specs)}})))
    candidate = dict(date=day, physical_contract='abc-g0', product='ABC', contract='202603')
    diagnostic = {'sources': {'daily_sha256': sha256_file(daily), 'specifications_sha256': sha256_file(specs)}}
    bound = _bound_metadata([candidate], daily, specs, diagnostic)[('2026-01-05', 'abc-g0')]
    assert bound['session_start'] == '08:45:00'
    assert bound['session_end'] == '13:30:00'
    assert bound['tick_kind'] == 'fixed'
    with pytest.raises(ValueError, match='dated physical identity'):
        _bound_metadata([dict(candidate, contract='202604')], daily, specs, diagnostic)
