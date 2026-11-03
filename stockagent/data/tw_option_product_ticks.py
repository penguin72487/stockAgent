"""Bounded access to retained all-product TAIFEX option transaction sources.

These are public, whole-second side rows, not independent fills or a dated
listing/session/accounting contract.  Decimal text and duplicate source rows
are retained.  The iterator must be exhausted for its final identity check.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import csv
import io
import re
from typing import Iterator
import zipfile

import polars as pl
import pyarrow as pa
import pyarrow.csv as arrow_csv

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_official_ticks import OPTION_SOURCE, pin_official_option_tick_inventory


OPTION_TICK_SOURCE_CONTRACT = 'taifex_all_option_product_tick_source_v1'
_COLUMNS = ('event_date_raw', 'product_raw', 'strike_raw', 'series_raw',
            'right_raw', 'event_time_raw', 'price_raw', 'side_quantity_raw', 'auction_raw')
_HEADER = ('成交日期', '商品代號', '履約價格', '到期月份(週別)', '買賣權別',
           '成交時間', '成交價格', '成交數量(B or S)', '開盤集合競價')


def _identity(path: Path) -> tuple[int, int, int, int, int]:
    s = path.stat()
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns


def iter_official_option_trade_batches(item: dict, *, products: set[str] | None = None,
                                       block_size: int = 2 * 1024 * 1024) -> Iterator[pl.DataFrame]:
    """Yield raw decimal/side rows, with physical and exchange trading dates.

    Source order and physical row numbers remain intact.  No market session,
    quote currency, matched-trade volume, observation time, fill or admission
    is inferred. The caller must exhaust the generator to verify the receipt
    and source a second time. The optional filter is exact and never implies
    that omitted products or missing rows were untradable.
    """
    if item.get('source_kind') != OPTION_SOURCE or item.get('historical_point_in_time') is not False:
        raise ValueError('an explicitly pinned official option source is required')
    if type(block_size) is not int or block_size < 1024:
        raise ValueError('invalid Arrow source block size')
    if products is not None and (not products or any(
            not isinstance(p, str) or not re.fullmatch(r'[A-Z0-9]{2,8}', p) for p in products)):
        raise ValueError('products must be a nonempty set of explicit option codes')
    archive, receipt = Path(item['path']), Path(item['receipt_path'])
    digest, receipt_digest = item['sha256'], item['receipt_sha256']
    day = date.fromisoformat(item['date'])
    pinned = pin_official_option_tick_inventory(receipt, [day])[0]
    if any(item.get(key) != value for key, value in pinned.items()):
        raise ValueError('TAIFEX option item does not match its canonical receipt')
    before = _identity(archive)
    if (before[2] != item['bytes'] or sha256_file(archive) != digest
            or sha256_file(receipt) != receipt_digest):
        raise ValueError('TAIFEX option archive or receipt SHA mismatch')
    expected_member = f"OptionsDaily_{day.strftime('%Y_%m_%d')}.csv"
    with zipfile.ZipFile(archive) as zipped:
        members = zipped.infolist()
        if (len(members) != 1 or members[0].filename != expected_member
                or members[0].is_dir() or members[0].flag_bits & 1):
            raise ValueError('unexpected TAIFEX option archive member')
        with zipped.open(members[0]) as binary:
            header = next(csv.reader([binary.readline().decode('cp950')]))
            separator = binary.readline().decode('cp950').strip()
            if tuple(cell.strip() for cell in header) != _HEADER or not separator.startswith('-'):
                raise ValueError('unexpected TAIFEX option source header')
        with zipped.open(members[0]) as binary:
            reader = arrow_csv.open_csv(
                binary,
                read_options=arrow_csv.ReadOptions(encoding='cp950', column_names=list(_COLUMNS),
                                                   skip_rows=2, block_size=block_size),
                convert_options=arrow_csv.ConvertOptions(column_types={c: pa.string() for c in _COLUMNS}),
            )
            ordinal = 0
            try:
                for batch in reader:
                    frame = pl.from_arrow(batch).with_row_index('source_row_number', offset=ordinal + 3)
                    ordinal += frame.height
                    frame = frame.with_columns(pl.all().exclude('source_row_number').fill_null('').str.strip_chars())
                    frame = frame.with_columns(
                        pl.col('event_date_raw').str.strptime(pl.Date, '%Y%m%d', strict=False).alias('event_date'),
                        pl.concat_str('event_date_raw', 'event_time_raw')
                          .str.strptime(pl.Datetime('us'), '%Y%m%d%H%M%S', strict=False)
                          .dt.replace_time_zone('Asia/Taipei').alias('event_ts'),
                    )
                    malformed = frame.filter(
                        pl.col('event_date').is_null() | pl.col('event_ts').is_null()
                        | (pl.col('event_date') > day) | (pl.col('event_date') < day - timedelta(days=10))
                        | ~pl.col('event_date_raw').str.contains(r'^\d{8}$')
                        | ~pl.col('event_time_raw').str.contains(r'^\d{6}$')
                        | ~pl.col('product_raw').str.contains(r'^[A-Z0-9]{2,8}$')
                        | ~pl.col('right_raw').is_in(['C', 'P'])
                        | (pl.col('series_raw') == '') | (pl.col('strike_raw') == '')
                        | (pl.col('price_raw') == '')
                        | (pl.col('side_quantity_raw').cast(pl.Int64, strict=False).fill_null(0) <= 0)
                        | ~pl.col('auction_raw').is_in(['', '*'])
                    )
                    if malformed.height:
                        raise ValueError(f"invalid TAIFEX option source row {malformed['source_row_number'][0]}")
                    if products is not None:
                        frame = frame.filter(pl.col('product_raw').is_in(sorted(products)))
                    if frame.height:
                        yield frame.with_columns(
                            pl.lit(day).cast(pl.Date).alias('trading_date'),
                            pl.lit(str(archive)).alias('source_file'),
                            pl.lit(expected_member).alias('source_member'),
                            pl.lit(digest).alias('source_sha256'),
                            pl.lit(OPTION_TICK_SOURCE_CONTRACT).alias('source_contract'),
                        )
            finally:
                reader.close()
    if (_identity(archive) != before or sha256_file(archive) != digest
            or sha256_file(receipt) != receipt_digest):
        raise ValueError('TAIFEX option source or receipt changed during read')
