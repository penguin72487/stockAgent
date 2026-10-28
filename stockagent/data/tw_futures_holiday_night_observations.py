"""Conservative product-specific session filter for reviewed holiday ticks.

The filter identifies candidate night observations. It does not establish a
historical exchange session, execution permission, a broker mark or inventory.
"""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl

SESSION_FILTER_CONTRACT = 'reviewed_holiday_product_night_floor_v1'
NIGHT_STARTS = {
    'TX': '15:00:00', 'BRF': '15:00:00', 'F1F': '15:00:00', 'SPF': '15:00:00',
    'SXF': '15:00:00', 'UDF': '15:00:00', 'UNF': '15:00:00', 'TGF': '17:25:00',
}


def last_reviewed_holiday_night_ticks(ticks: pl.DataFrame, *, evening: date, morning: date) -> dict:
    """Return last-timestamp candidates without mixing TGF regular trades.

    These start floors are scoped to this audit's 2018 onward sessions; the
    actual existence and dated start of each exchange session remain separate
    evidence. If a timestamp has multiple prices its order remains unknown.
    """
    if morning != evening + timedelta(days=1) or evening < date(2018, 1, 1):
        raise ValueError('unsupported reviewed holiday session dates')
    required = {'futures_id', 'contract_date', 'date', 'price', 'volume'}
    if not required <= set(ticks.columns) or ticks['futures_id'].null_count():
        raise ValueError('holiday ticks require explicit product and trade fields')
    unknown = set(ticks['futures_id'].unique().to_list()) - NIGHT_STARTS.keys()
    if unknown:
        raise ValueError(f'holiday product session floors are unreviewed: {sorted(unknown)}')
    floors = pl.DataFrame({'futures_id': list(NIGHT_STARTS),
                           '_night_start': [f'{evening} {s}' for s in NIGHT_STARTS.values()]})
    frame = (ticks.join(floors, on='futures_id', how='left', validate='m:1')
             .filter(((pl.col('date') >= pl.col('_night_start')) & (pl.col('date') <= f'{evening} 23:59:59'))
                     | ((pl.col('date') >= f'{morning} 00:00:00') & (pl.col('date') <= f'{morning} 05:00:00')))
             .filter((pl.col('price') > 0) & (pl.col('volume') > 0)
                     & pl.col('contract_date').str.contains(r'^\d{6}$')))
    if frame.is_empty():
        return {}
    latest = frame.group_by('futures_id', 'contract_date').agg(pl.col('date').max().alias('last_time'))
    last = frame.join(latest, on=['futures_id', 'contract_date'], validate='m:1').filter(
        pl.col('date') == pl.col('last_time'))
    grouped = last.group_by('futures_id', 'contract_date', 'last_time').agg(
        pl.col('price').unique().sort().alias('last_prices'), pl.len().alias('last_timestamp_rows'))
    return {(row['futures_id'], row['contract_date']): {
        'timestamp': row['last_time'], 'prices': [float(price) for price in row['last_prices']],
        'timestamp_rows': row['last_timestamp_rows'],
        'unique_price_at_last_timestamp': len(row['last_prices']) == 1,
    } for row in grouped.to_dicts()}
