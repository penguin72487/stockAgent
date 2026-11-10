import io

import polars as pl
import pytest

from stockagent.data_sync import binance_generation_comparison as compare
from stockagent.data_sync.desync_snapshots import SnapshotError


def parquet(rows):
    out = io.BytesIO()
    pl.DataFrame(rows).write_parquet(out)
    return out.getvalue()


def candle(timestamp, price=1.0, source='old'):
    return {'market': 'spot', 'symbol': 'BTCUSDT', 'open_time': timestamp,
            'open': price, 'high': price, 'low': price, 'close': price, 'volume': 1.0,
            'close_time': timestamp+59, 'quote_volume': 1.0, 'trade_count': 1,
            'taker_buy_base_volume': 1.0, 'taker_buy_quote_volume': 1.0, 'ignore': '0',
            'archive_key': source}


def test_candle_comparison_distinguishes_missing_prices_and_provenance():
    old = parquet([candle(1), candle(2)])
    newer = parquet([candle(2, source='revised'), candle(3)])
    row = compare.compare_parquet('partitions/spot/BTCUSDT/2026-10.parquet', old, newer)
    assert (row['old_only_rows'], row['new_only_rows']) == (1, 1)
    assert row['overlap_rows_with_any_change'] == 1 and row['overlap_rows_with_price_change'] == 0
    corrected = parquet([candle(1, price=2.0), candle(2)])
    assert compare.compare_parquet('partitions/x.parquet', old, corrected)['overlap_rows_with_price_change'] == 1


def test_source_plan_and_lifecycle_use_their_own_keys():
    old = parquet([{'key': 'a', 'state': 'pending'}])
    new = parquet([{'key': 'a', 'state': 'complete'}, {'key': 'b', 'state': 'complete'}])
    row = compare.compare_parquet('archive_plan.parquet', old, new)
    assert row['category'] == 'plan' and row['new_only_rows'] == 1 and row['overlap_rows_with_any_change'] == 1


def test_duplicate_and_unregistered_grains_are_never_merged():
    rows = parquet([candle(1), candle(1)])
    with pytest.raises(SnapshotError, match='duplicate'):
        compare.compare_parquet('partitions/x.parquet', rows, rows)
    with pytest.raises(SnapshotError, match='unregistered'):
        compare.compare_parquet('other.parquet', rows, rows)
