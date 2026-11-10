"""Compare fixed Binance archive generations at their actual natural keys."""
from __future__ import annotations

import io

import polars as pl

from stockagent.data_sync.desync_snapshots import SnapshotError

CANDLE_KEY = ('market', 'symbol', 'open_time')
CANDLE_VALUES = ('open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume',
                 'trade_count', 'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore')
PLAN_KEY = ('key',)
LIFECYCLE_KEY = ('market', 'symbol')


def compare_parquet(path: str, original: bytes, newer: bytes) -> dict:
    """Count missing and conflicting observations without rewriting either file."""
    if path.startswith('partitions/') and path.endswith('.parquet'):
        key, values, category = CANDLE_KEY, CANDLE_VALUES, 'candles'
    elif path == 'archive_plan.parquet':
        key, values, category = PLAN_KEY, (), 'plan'
    elif path == 'instrument_lifecycle.parquet':
        key, values, category = LIFECYCLE_KEY, (), 'lifecycle'
    else:
        raise SnapshotError('unregistered Binance archive parquet path')
    try:
        old, new = pl.read_parquet(io.BytesIO(original)), pl.read_parquet(io.BytesIO(newer))
        if old.schema != new.schema or any(name not in old.columns for name in (*key, *values)):
            raise SnapshotError('Binance generations have incompatible parquet schemas')
        for frame in (old, new):
            if (frame.select(pl.any_horizontal([pl.col(k).is_null() for k in key]).any()).item()
                    or frame.select(list(key)).is_duplicated().any()):
                raise SnapshotError('Binance generation has null or duplicate natural keys')
        missing = old.join(new.select(list(key)), on=list(key), how='anti').height
        added = new.join(old.select(list(key)), on=list(key), how='anti').height
        joined = old.join(new, on=list(key), how='inner', suffix='__new')
        all_values = [name for name in old.columns if name not in key]
        changed = (joined.filter(pl.any_horizontal([
            ~pl.col(name).eq_missing(pl.col(name+'__new')) for name in all_values])).height
            if all_values else 0)
        price_changed = (joined.filter(pl.any_horizontal([
            ~pl.col(name).eq_missing(pl.col(name+'__new')) for name in values])).height
            if values else 0)
        return {'category': category, 'old_rows': old.height, 'new_rows': new.height,
                'old_only_rows': missing, 'new_only_rows': added,
                'overlap_rows_with_any_change': changed, 'overlap_rows_with_price_change': price_changed}
    except (OSError, pl.exceptions.PolarsError) as error:
        raise SnapshotError('Binance source parquet cannot be decoded or compared') from error
