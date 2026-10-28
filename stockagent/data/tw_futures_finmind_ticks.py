"""Receipt-bound FinMind outright prints for the canonical futures minute tape.

FinMind reports both sides of each outright match. Preserve repeated prints and
source order; divide each reported quantity by two exactly, never infer a VWAP
from OHLC or a current continuous-contract identity.
"""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import re

import polars as pl
import numpy as np

from downloader.artifact_io import sha256_file

SOURCE = 'finmind_futures_tick_contract_day_v1'
SCHEMA_VERSION = 1
BULK_SOURCE = 'finmind_whole_market_stock_prints_v1'
BULK_EQUITY_SOURCE = 'finmind_whole_market_dated_equity_prints_v2'
BULK_PRICE_POLICY = 'unique_dated_legal_quote_with_identical_float32_encoding_raw_retained_v1'
BULK_ALL_TWD_SOURCE = 'finmind_whole_market_dated_all_twd_prints_v3'
BULK_DATED_PRICE_POLICY = 'unique_source_bound_tick_float32_quote_including_subcent_ticks_v3'
BULK_INVENTORY = 'finmind_whole_market_tick_inventory_v1'
PRODUCT_FALLBACK_INVENTORY = 'finmind_product_day_tick_fallback_inventory_v1'
PRODUCT_FALLBACK_SOURCE = 'finmind_exact_product_day_tick_fallback_v2'


def pin_bulk_tick_inventory(root: Path, dates: list[date]) -> list[dict]:
    """Pin active collector objects or their exact, portable raw-source inventory.

    The portable inventory carries original receipts, never a fabricated copy
    of the collector's mutable queue. Full object readback remains in the
    shared reader on the consuming node.
    """
    import sqlite3
    root = Path(root).resolve()
    wanted = {str(d) for d in dates}
    inventory = None
    if root.is_file():
        inventory_path = root
        inventory_sha = sha256_file(inventory_path)
        document = json.loads(inventory_path.read_text())
        if document.get('contract') != BULK_INVENTORY or document.get('dataset') != 'TaiwanFuturesTick':
            raise ValueError('invalid portable bulk tick inventory')
        objects = document['objects']
        inventory = {item['date']: item for item in objects}
        if len(inventory) != len(objects):
            raise ValueError('duplicate portable bulk tick date')
        root = inventory_path.parent
        tasks = {day: ('complete', item['receipt_path']) for day, item in inventory.items() if day in wanted}
    else:
        with sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True) as db:
            tasks = {d: (state, path) for d, state, path in db.execute(
                "SELECT partition,state,receipt_path FROM tasks "
                "WHERE dataset='TaiwanFuturesTick' AND data_id='' ORDER BY partition") if d in wanted}
    if set(tasks) != wanted or any(state != 'complete' for state, _ in tasks.values()):
        raise ValueError('global tick inventory lacks a complete selected date')
    sources = []
    for day in sorted(tasks):
        receipt_path = (root / tasks[day][1]).resolve()
        if not receipt_path.is_relative_to(root):
            raise ValueError('unsafe bulk receipt path')
        payload = receipt_path.read_bytes()
        import hashlib
        receipt = json.loads(payload)
        if (receipt.get('dataset') != 'TaiwanFuturesTick' or receipt.get('data_id') != ''
                or receipt.get('partition') != day or receipt.get('status') != 'complete'):
            raise ValueError('bulk tick receipt identity differs from active inventory')
        path = (root / receipt['parquet_path']).resolve()
        if not path.is_relative_to(root):
            raise ValueError('unsafe bulk tick member path')
        item = dict(date=day, path=str(path), sha256=receipt['sha256'],
            receipt_path=str(receipt_path), receipt_sha256=hashlib.sha256(payload).hexdigest(),
            rows=receipt['rows'], bytes=receipt['parquet_size_bytes'],
            fetched_at_utc=receipt.get('fetched_at_utc'),
            historical_point_in_time=receipt.get('historical_point_in_time'),
            source_order=receipt.get('request', {}).get('source_order'))
        if inventory is not None:
            expected = inventory[day]
            for key in ('date', 'sha256', 'receipt_sha256', 'rows', 'bytes',
                        'fetched_at_utc', 'historical_point_in_time', 'source_order'):
                if item[key] != expected[key]:
                    raise ValueError('portable bulk tick receipt differs from pinned inventory')
            if path != (root / expected['path']).resolve():
                raise ValueError('portable bulk tick original path differs')
            item.update(inventory_path=str(inventory_path), inventory_sha256=inventory_sha)
        sources.append(item)
    if inventory is not None and sha256_file(inventory_path) != inventory_sha:
        raise ValueError('portable bulk tick inventory changed during pinning')
    return sources


def pin_product_fallback_inventory(root: Path, manifest_path: Path, dates: list[date]) -> list[dict]:
    """Pin each original exact-product receipt and raw member for selected days.

    The manifest is an explicit supplemental source, not a fabricated global
    response. Product coverage is checked against the official book when read.
    """
    import hashlib
    root, manifest_path = Path(root).resolve(), Path(manifest_path).resolve()
    if not root.is_dir():
        raise ValueError('product fallback requires the original collector directory')
    payload = manifest_path.read_bytes()
    manifest_sha = hashlib.sha256(payload).hexdigest()
    document = json.loads(payload)
    if document.get('contract') != PRODUCT_FALLBACK_INVENTORY or document.get('dataset') != 'TaiwanFuturesTick':
        raise ValueError('invalid product tick fallback inventory')
    wanted = {str(d) for d in dates}
    groups = {day: [] for day in wanted}
    identities = set()
    for entry in document.get('objects', []):
        day, product = entry.get('date'), entry.get('product')
        if day not in wanted:
            continue
        if not isinstance(product, str) or not re.fullmatch(r'[A-Z0-9]{2,3}', product):
            raise ValueError('invalid product fallback identity')
        if (day, product) in identities:
            raise ValueError('duplicate product fallback identity')
        identities.add((day, product))
        receipt_path = (root / entry['receipt_path']).resolve()
        if not receipt_path.is_relative_to(root):
            raise ValueError('unsafe product fallback receipt path')
        receipt_bytes = receipt_path.read_bytes()
        receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
        receipt = json.loads(receipt_bytes)
        if (receipt.get('dataset'), receipt.get('data_id'), receipt.get('partition'), receipt.get('status')) != (
                'TaiwanFuturesTick', product, day, 'complete'):
            raise ValueError('product fallback receipt identity differs')
        path = (root / receipt['parquet_path']).resolve()
        if not path.is_relative_to(root):
            raise ValueError('unsafe product fallback payload path')
        if (entry.get('sha256') != receipt.get('sha256') or entry.get('receipt_sha256') != receipt_sha
                or entry.get('rows') != receipt.get('rows')):
            raise ValueError('product fallback inventory differs from native receipt')
        groups[day].append(dict(product=product, date=day, path=str(path), sha256=receipt['sha256'],
            receipt_path=str(receipt_path), receipt_sha256=receipt_sha, rows=receipt['rows'],
            bytes=receipt['parquet_size_bytes'], fetched_at_utc=receipt.get('fetched_at_utc'),
            historical_point_in_time=receipt.get('historical_point_in_time'),
            price_representation='native_decimal'))
    if any(not products for products in groups.values()):
        raise ValueError('product fallback inventory lacks a selected date')
    if sha256_file(manifest_path) != manifest_sha:
        raise ValueError('product fallback inventory changed during pinning')
    return [dict(date=day, source_kind=PRODUCT_FALLBACK_SOURCE, path=str(manifest_path),
        sha256=manifest_sha, products=sorted(groups[day], key=lambda item: item['product']))
        for day in sorted(groups)]


def pin_mixed_tick_inventory(root: Path, dates: list[date], fallback_manifest: Path | None = None,
                             official_manifest: Path | None = None) -> list[dict]:
    """Use explicit original-source supplements on their dates and whole days elsewhere."""
    if fallback_manifest is None and official_manifest is None:
        return pin_bulk_tick_inventory(root, dates)
    import hashlib
    selected, official_selected, manifest_sha, official_sha = set(), set(), None, None
    if fallback_manifest is not None:
        manifest_bytes = Path(fallback_manifest).read_bytes()
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        document = json.loads(manifest_bytes)
        if document.get('contract') != PRODUCT_FALLBACK_INVENTORY or document.get('dataset') != 'TaiwanFuturesTick':
            raise ValueError('invalid product tick fallback inventory')
        fallback_days = {date.fromisoformat(item['date']) for item in document.get('objects', [])}
        selected = set(dates) & fallback_days
        if not selected:
            raise ValueError('product fallback inventory does not cover a selected date')
    if official_manifest is not None:
        official_bytes = Path(official_manifest).read_bytes()
        official_sha = hashlib.sha256(official_bytes).hexdigest()
        official = json.loads(official_bytes)
        official_days = {date.fromisoformat(item['trading_date']) for item in official.get('raw_downloads', [])
                         if item.get('kind') == 'futures'}
        official_selected = set(dates) & official_days
        if not official_selected:
            raise ValueError('official archive receipt does not cover a selected date')
        if selected & official_selected:
            raise ValueError('fallback sources overlap; choose one explicit source per date')
    global_days = set(dates) - selected - official_selected
    sources = (pin_bulk_tick_inventory(root, sorted(global_days)) if global_days else [])
    if selected:
        fallback = pin_product_fallback_inventory(root, fallback_manifest, sorted(selected))
        if any(item['sha256'] != manifest_sha for item in fallback):
            raise ValueError('product fallback inventory changed during date selection')
        sources += fallback
    if official_selected:
        from stockagent.data.tw_futures_official_ticks import pin_official_tick_inventory
        official_items = pin_official_tick_inventory(official_manifest, sorted(official_selected))
        if any(item['receipt_sha256'] != official_sha for item in official_items):
            raise ValueError('official archive receipt changed during date selection')
        sources += official_items
    return sorted(sources, key=lambda item: item['date'])


def decode_bulk_stock_prices(prices: np.ndarray, *, day: date, products: np.ndarray,
                             asset_classes='stock_future', tick_sizes=None) -> np.ndarray:
    """Recover a unique legal decimal quote from the provider's F32 encoding.

    This is a declared source representation conversion, not arbitrary price
    rounding. Reject any value whose legal candidate does not re-encode to the
    exact retained bits, or for which a neighboring legal tick has those bits.
    Raw objects and hashes are never rewritten. VWAPs are never rounded.
    """
    from stockagent.data.tw_price_rules import (
        taifex_futures_tick_size_numpy, price_on_taifex_futures_tick_grid_numpy,
    )
    values = np.asarray(prices, dtype=np.float64)
    raw = values.astype(np.float32)
    if not np.array_equal(values, raw.astype(np.float64)):
        raise ValueError('bulk price is not the declared Float32 source encoding')
    ticks = (taifex_futures_tick_size_numpy(values, np.datetime64(day),
        product_codes=products, asset_classes=asset_classes) if tick_sizes is None
        else np.asarray(tick_sizes,dtype=np.float64))
    if ticks.shape!=values.shape or np.any(~np.isfinite(ticks)|(ticks<=0)):
        raise ValueError('bulk price requires aligned positive dated ticks')
    with np.errstate(invalid='ignore', divide='ignore'):
        decoded = (np.rint(np.rint(values / ticks) * ticks * 100.) / 100. if tick_sizes is None
                   else np.round(np.rint(values/ticks)*ticks,decimals=12))
    valid = (price_on_taifex_futures_tick_grid_numpy(decoded, np.datetime64(day),
        product_codes=products, asset_classes=asset_classes) if tick_sizes is None
        else np.isfinite(decoded)&(decoded>0)&np.isclose(decoded/ticks,np.rint(decoded/ticks),rtol=0,atol=1e-7))
    if (not valid.all() or not np.array_equal(decoded.astype(np.float32), raw)
            or np.any((decoded + ticks).astype(np.float32) == raw)
            or np.any((decoded - ticks).astype(np.float32) == raw)):
        raise ValueError('bulk price has no unique dated legal Float32 quote')
    return decoded


def stock_print_views(prints: pl.DataFrame, *, day: date, source_sha256: str,
                      opening_evidence: pl.DataFrame | None = None):
    """One sorted, physical-identity-bound tape feeds minutes and reference events.

    The caller authenticates quantities, physical identity and dated price grid.
    Minute completion bounds are conservative historical availability; exchange
    timestamps and source order are retained without claiming receive times.
    """
    from stockagent.data.tw_stock_futures_history import MINUTE_SCHEMA
    keys = ['date', 'physical_contract']
    ordered = prints.sort('event_ts', 'source_row_index').with_columns(
        pl.lit(day).alias('date'),
        (pl.col('event_ts').dt.hour().cast(pl.Int32) * 60
         + pl.col('event_ts').dt.minute().cast(pl.Int32) + 1).alias('minute'))
    stats = ordered.group_by(keys).agg(
        pl.col('price').max().alias('tick_high'), pl.col('price').min().alias('tick_low'),
        pl.col('price').last().alias('tick_close'), pl.col('volume').sum().alias('tick_volume'),
        pl.len().alias('tick_rows'))
    bars = ordered.group_by(*keys, 'minute').agg(
        ((pl.col('price') * pl.col('volume')).sum() / pl.col('volume').sum()).alias('vwap'),
        pl.col('price').max().alias('high'), pl.col('price').min().alias('low'),
        pl.col('price').last().alias('close'), pl.col('volume').sum().cast(pl.Float64),
    ).with_columns(pl.lit(source_sha256).alias('source_file_sha256')).select(*MINUTE_SCHEMA).cast(MINUTE_SCHEMA)
    # Only three observations per contract-day are needed for the two selected
    # decisions: actual first session print, actual last prefix print and actual
    # last full-session print. No O(minutes * decisions) reference expansion.
    first = ordered.group_by(keys).first()
    # Source rows with identical timestamps do not prove within-second trade
    # order. A dated exchange opening observation can certify which actual
    # first-second print was the open; it is not itself backdated as a print.
    candidates = ordered.join(first.select(*keys, 'event_ts'), on=[*keys, 'event_ts'], how='inner')
    if opening_evidence is not None:
        if set(keys + ['observed_open']) - set(opening_evidence.columns):
            raise ValueError('opening evidence requires dated physical identity and official open')
        if opening_evidence.select(keys).is_duplicated().any():
            raise ValueError('duplicate official opening evidence')
        candidates = candidates.join(opening_evidence.select(*keys, 'observed_open'),
                                     on=keys, how='left', validate='m:1')
    else:
        candidates = candidates.with_columns(pl.lit(None, dtype=pl.Float64).alias('observed_open'))
    first_checks = candidates.group_by(keys).agg(
        pl.col('price').n_unique().alias('_first_second_prices'),
        pl.col('observed_open').first().alias('_official_open'),
        pl.when((pl.col('price') - pl.col('observed_open')).abs() <= 1e-6)
          .then(pl.col('price')).otherwise(None).drop_nulls().first().alias('_matched_open'))
    first = first.join(first_checks, on=keys, how='left', validate='1:1').with_columns(
        pl.when(pl.col('_matched_open').is_not_null()).then(pl.col('_matched_open'))
          .otherwise(pl.col('price')).alias('price'),
        pl.lit(True).alias('is_session_open'),
        pl.when(pl.col('_official_open').is_finite() & (pl.col('_official_open') > 0))
          .then(pl.col('_matched_open').is_null())
          .otherwise(pl.col('_first_second_prices') > 1).alias('opening_ambiguous')
        ).drop('_first_second_prices', '_official_open', '_matched_open')
    last = ordered.group_by(keys).last().with_columns(pl.lit(False).alias('is_session_open'),
                                                       pl.lit(False).alias('opening_ambiguous'))
    prefix = ordered.filter(pl.col('minute') <= 540).group_by(keys).last().with_columns(
        pl.lit(False).alias('is_session_open'), pl.lit(False).alias('opening_ambiguous'))
    events = pl.concat([first, last, prefix], how='vertical_relaxed').select(
        'physical_contract', 'price', pl.col('date').alias('trade_date'),
        pl.col('event_ts').cast(pl.Datetime('us')).alias('trade_ts'),
        (pl.col('date').cast(pl.Datetime('us')) + pl.duration(minutes=pl.col('minute'))).alias('available_by'),
        pl.lit(source_sha256).alias('source_sha256'), 'is_session_open', 'opening_ambiguous').unique()
    return bars, stats, events


def normalize_bulk_stock_ticks(frame: pl.DataFrame, *, day: date, expected: pl.DataFrame,
                               source_sha256: str, asset_scope='stock_only',
                               price_representation='finmind_float32',
                               unknown_final_mode='intersection'):
    """Columnar daily adapter for all selected delivery months, including zeros.

    Community native-decimal supplements must round-trip through the same
    unique dated tick-grid decoder without changing the original quote. Only
    the authenticated exchange-original adapter may use the union candidate;
    this function emits prints only after matching the full official book.
    """
    from stockagent.data.tw_price_rules import taifex_futures_day_session_minutes
    from stockagent.data.tw_futures_decision_clock import clock_asset_classes
    classes = clock_asset_classes(asset_scope)
    if (unknown_final_mode != 'intersection'
            and (unknown_final_mode != 'union_for_full_book_verification'
                 or price_representation != 'native_decimal' or asset_scope != 'all_twd')):
        raise ValueError('unknown-final union requires native all-TWD full-book verification')
    keys = ['date', 'physical_contract']
    metadata=[]
    if asset_scope=='all_twd':
        from stockagent.data.tw_futures_decision_clock import dated_entry_sessions
        metadata=['spec_tick_kind','spec_tick_size']
        if set(metadata)-set(expected.columns):
            raise ValueError('all-TWD bulk quotes require dated tick specifications')
        mapping=expected.select('product','contract','physical_contract','asset_class',*metadata).unique().join(
            dated_entry_sessions(expected, unknown_final_mode=unknown_final_mode)
            .select('physical_contract','_start','_end'),
            on='physical_contract',how='left',validate='1:1')
        # Product/date membership and tick kind are supplied by the bound
        # exchange specification. Do not substitute the legacy static product
        # registry, whose review window can end before this specification.
        if mapping.filter(pl.col('spec_tick_kind').is_null()
                | ~pl.col('spec_tick_kind').is_in(['fixed','stock_future','etf_future'])
                | ((pl.col('spec_tick_kind') != 'fixed')
                   & (pl.col('spec_tick_kind') != pl.col('asset_class')))).height:
            raise ValueError('dated bulk tick kind and asset class disagree')
    else:
        mapping = expected.select('product', 'contract', 'physical_contract', 'asset_class').unique()
    if (expected.select(keys).is_duplicated().any() or mapping.select('product', 'contract').is_duplicated().any()
            or mapping.select('product', 'asset_class').unique()['product'].is_duplicated().any()
            or expected.filter((pl.col('date') != day) | pl.col('asset_class').is_null()
                | ~pl.col('asset_class').is_in(classes)).height):
        raise ValueError('bulk reader needs unique dated physical identities within its asset scope')
    required = {'date', 'futures_id', 'contract_date', 'price', 'volume'}
    if not required <= set(frame.columns):
        raise ValueError('incomplete global tick schema')
    # Resolve dated sessions once per selected product, not once per tick.
    sessions = [] if asset_scope=='all_twd' else [dict(product=p, _start=a*60, _end=b*60)
                for p, asset in mapping.select('product', 'asset_class').unique().iter_rows()
                for a, b in [taifex_futures_day_session_minutes(day, product_code=p, asset_class=asset)]]
    delivery_pattern = r'^\d{6}$' if asset_scope == 'stock_only' else r'^\d{6}(?:W[1-5])?$'
    raw = frame.with_row_index('source_row_index').filter(
        pl.col('futures_id').is_in(mapping['product'].unique().implode())
        & pl.col('contract_date').str.contains(delivery_pattern)).rename(
            {'futures_id': 'product', 'contract_date': 'contract'}).join(mapping,
                on=['product', 'contract'], how='inner', validate='m:1').with_columns(
                    pl.col('date').str.to_datetime(time_unit='us', strict=True).alias('event_ts'))
    if raw.filter(pl.col('event_ts').is_null() | (pl.col('event_ts').dt.date() != day)).height:
        raise ValueError('selected bulk ticks lie outside receipt calendar day')
    if asset_scope!='all_twd':
        raw=raw.join(pl.DataFrame(sessions), on='product', how='left')
    raw = raw.with_columns(
        (pl.col('event_ts').dt.hour().cast(pl.Int32)*3600 + pl.col('event_ts').dt.minute().cast(pl.Int32)*60
         + pl.col('event_ts').dt.second().cast(pl.Int32)).alias('_second'))
    raw = raw.filter(pl.col('_second').is_between(pl.col('_start'), pl.col('_end')))
    if raw.filter(pl.col('price').is_null() | pl.col('volume').is_null() | (pl.col('volume') <= 0)
                  | (pl.col('volume') % 2 != 0) | (pl.col('volume') != pl.col('volume').floor())).height:
        raise ValueError('bulk outright volume must be positive even double-sided contracts')
    ticks=None
    if asset_scope=='all_twd':
        fixed=raw['spec_tick_kind'].eq('fixed').to_numpy()
        if raw.filter(~pl.col('spec_tick_kind').is_in(['fixed','stock_future','etf_future'])).height:
            raise ValueError('unsupported dated bulk tick kind')
        ticks=raw['spec_tick_size'].to_numpy().astype(np.float64,copy=True)
        if (~fixed).any():
            from stockagent.data.tw_price_rules import tick_size_numpy
            ticks[~fixed]=tick_size_numpy(raw['price'].to_numpy()[~fixed],np.datetime64(day),
                security_types=raw['spec_tick_kind'].to_numpy()[~fixed])
    native_prices = raw['price'].to_numpy()
    if price_representation == 'finmind_float32':
        encoded_prices = native_prices
    elif price_representation == 'native_decimal':
        encoded_prices = native_prices.astype(np.float32)
    else:
        raise ValueError('unknown bulk source price representation')
    decoded = decode_bulk_stock_prices(encoded_prices, day=day, products=raw['product'].to_numpy(),
                                      asset_classes=raw['asset_class'].to_numpy(),tick_sizes=ticks)
    if price_representation == 'native_decimal' and not np.allclose(
            native_prices, decoded, rtol=0., atol=1e-9):
        raise ValueError('native source price is not the identical dated legal quote')
    prints = raw.with_columns(pl.Series('price', decoded), (pl.col('volume').cast(pl.Int64)//2).alias('volume'))
    opening_evidence = (expected.select(*keys, 'observed_open')
                        if 'observed_open' in expected.columns else None)
    bars, stats, events = stock_print_views(prints, day=day, source_sha256=source_sha256,
                                            opening_evidence=opening_evidence)
    coverage = expected.select(*keys, 'product', 'contract', 'high', 'low', 'close', 'outright_volume', 'block_volume').join(
        stats, on=keys, how='left', validate='1:1').with_columns(pl.col('tick_volume').fill_null(0))
    coverage = coverage.with_columns(((pl.col('tick_volume') == pl.col('outright_volume'))
        & ((pl.col('outright_volume') == 0)
           | ((pl.col('tick_high')-pl.col('high')).abs().le(1e-6)
              & (pl.col('tick_low')-pl.col('low')).abs().le(1e-6)
              & (pl.col('tick_close')-pl.col('close')).abs().le(1e-6)))).fill_null(False).alias('verified'),
        pl.lit(source_sha256).alias('source_file_sha256'))
    verified = coverage.filter(pl.col('verified')).select(keys)
    bars = bars.join(verified, on=keys, how='semi')
    events = events.join(verified.rename({'date': 'trade_date'}), on=['trade_date', 'physical_contract'], how='semi')
    return bars, coverage, events


def read_bulk_stock_tick_day(item: dict, expected: pl.DataFrame, *, asset_scope='stock_only'):
    """Independent SHA/row/stability readback of a pinned active source object."""
    path, receipt_path = Path(item['path']), Path(item['receipt_path'])
    before = path.stat()
    if sha256_file(receipt_path) != item['receipt_sha256'] or sha256_file(path) != item['sha256']:
        raise ValueError('pinned global tick SHA mismatch')
    frame = pl.read_parquet(path)
    if frame.height != item['rows'] or before.st_size != item['bytes']:
        raise ValueError('pinned global tick row/byte count mismatch')
    result = normalize_bulk_stock_ticks(frame, day=date.fromisoformat(item['date']),
        expected=expected, source_sha256=item['sha256'], asset_scope=asset_scope)
    after = path.stat()
    if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or sha256_file(receipt_path) != item['receipt_sha256']):
        raise ValueError('pinned global tick changed during readback')
    return result


def read_product_fallback_tick_day(item: dict, expected: pl.DataFrame, *, asset_scope='all_twd'):
    """Read native product objects together while retaining each product's SHA.

    A missing product can certify only an independently evidenced zero-outight
    book; a positive official book without a receipt remains unverified. The
    concatenation retains source order within each physical contract.
    """
    if item.get('source_kind') != PRODUCT_FALLBACK_SOURCE or asset_scope != 'all_twd':
        raise ValueError('product fallback requires the all-TWD source contract')
    manifest = Path(item['path'])
    if sha256_file(manifest) != item['sha256']:
        raise ValueError('product fallback inventory SHA mismatch')
    frames, stable = [], []
    for member in item['products']:
        path, receipt_path = Path(member['path']), Path(member['receipt_path'])
        before = path.stat()
        if sha256_file(receipt_path) != member['receipt_sha256'] or sha256_file(path) != member['sha256']:
            raise ValueError('product fallback original SHA mismatch')
        raw = pl.read_parquet(path)
        if (raw.height != member['rows'] or before.st_size != member['bytes']
                or raw.schema.get('price') != pl.Float64):
            raise ValueError('product fallback native rows, bytes or decimal representation differs')
        if (set(raw['futures_id'].to_list()) != {member['product']}
                or any(not str(value).startswith(item['date']) for value in raw['date'].to_list())):
            raise ValueError('product fallback contains another product or date')
        frames.append(raw.select('date', 'futures_id', 'contract_date', 'price', 'volume'))
        stable.append((path, receipt_path, member, before))
    if not frames:
        raise ValueError('product fallback contains no native objects')
    # Each source is ordered internally. Physical contracts do not span product
    # identities, so concatenation cannot change any within-contract order.
    bars, coverage, events = normalize_bulk_stock_ticks(pl.concat(frames, how='vertical_relaxed'),
        day=date.fromisoformat(item['date']), expected=expected, source_sha256=item['sha256'],
        asset_scope=asset_scope, price_representation='native_decimal')
    keys = ['date', 'physical_contract']
    product_shas = pl.DataFrame([dict(product=x['product'], source_file_sha256=x['sha256'])
                                 for x in item['products']])
    provenance = expected.select(*keys, 'product').join(product_shas, on='product', how='left', validate='m:1')
    coverage = coverage.drop('source_file_sha256').join(provenance.select(*keys, 'source_file_sha256'),
        on=keys, how='left', validate='1:1')
    bars = bars.drop('source_file_sha256').join(provenance.select(*keys, 'source_file_sha256'),
        on=keys, how='left', validate='m:1').select(bars.columns)
    events = events.drop('source_sha256').join(provenance.select(
        pl.col('date').alias('trade_date'), 'physical_contract',
        pl.col('source_file_sha256').alias('source_sha256')),
        on=['trade_date', 'physical_contract'], how='left', validate='m:1').select(events.columns)
    for path, receipt_path, member, before in stable:
        after = path.stat()
        if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                or sha256_file(receipt_path) != member['receipt_sha256']):
            raise ValueError('product fallback native source changed during readback')
    if sha256_file(manifest) != item['sha256']:
        raise ValueError('product fallback inventory changed during readback')
    # Group-by and the provenance join do not promise output order.  A stable
    # minute order makes later FP64 window reductions reproducible when the
    # same source is rebuilt or compared with an independent product audit.
    bars = bars.sort('date', 'physical_contract', 'minute')
    coverage = coverage.sort('date', 'physical_contract')
    events = events.sort('trade_date', 'physical_contract', 'trade_ts', 'is_session_open')
    return bars, coverage, events


def read_pinned_tick_day(item: dict, expected: pl.DataFrame, *, asset_scope='stock_only'):
    if item.get('source_kind') == PRODUCT_FALLBACK_SOURCE:
        return read_product_fallback_tick_day(item, expected, asset_scope=asset_scope)
    if item.get('source_kind') == 'taifex_recent_original_all_product_futures_tick_v1':
        from stockagent.data.tw_futures_official_ticks import read_official_tick_day
        return read_official_tick_day(item, expected, asset_scope=asset_scope)
    return read_bulk_stock_tick_day(item, expected, asset_scope=asset_scope)


def source_paths(root: Path, product: str, day: date) -> tuple[Path, Path]:
    if not re.fullmatch(r'[A-Z0-9]{2,3}', product):
        raise ValueError('invalid futures product identity')
    folder = Path(root) / 'TaiwanFuturesTick' / product / str(day)
    return folder / 'data.parquet', folder / 'receipt.json'


def normalize_finmind_futures_ticks(frame: pl.DataFrame, *, day: date,
                                    product: str, physical_contract: str,
                                    source_sha256: str, asset_class: str) -> tuple[pl.DataFrame, dict]:
    """Adapt one product/calendar-day response without changing execution clocks."""
    from stockagent.data.tw_stock_futures_history import normalize_continuous_ticks
    if not re.fullmatch(re.escape(product) + r':\d{6}(?:W[1-5])?', physical_contract):
        raise ValueError('FinMind physical contract differs from requested product/month')
    required = {'date', 'futures_id', 'contract_date', 'price', 'volume'}
    if not required <= set(frame.columns) or frame.is_empty():
        raise ValueError('missing FinMind futures tick schema or empty response')
    if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in ('date','futures_id','contract_date')).any()).item():
        raise ValueError('null FinMind futures tick identity')
    raw = frame.with_row_index('source_row_index').with_columns(
        pl.col('date').str.to_datetime(time_unit='ns', strict=True).alias('event_ts'),
        pl.col('contract_date').str.strip_chars(),
    )
    if raw.filter((pl.col('futures_id') != product) | (pl.col('event_ts').dt.date() != day)).height:
        raise ValueError('FinMind response lies outside requested product/calendar day')
    # Other delivery months and spread prints remain preserved in the raw file,
    # but have no capacity in the selected physical outright contract.
    raw = raw.filter(pl.col('contract_date') == physical_contract.split(':')[1])
    if raw.is_empty():
        raise ValueError('no FinMind prints for requested physical contract')
    if raw.select((pl.col('price').is_null() | pl.col('volume').is_null()).any()).item():
        raise ValueError('null FinMind selected outright price/quantity')
    raw = raw.with_columns(pl.col('price').cast(pl.Float64), pl.col('volume').cast(pl.Float64))
    if raw.filter(~pl.col('volume').is_finite() | (pl.col('volume') <= 0)
                  | (pl.col('volume') != pl.col('volume').floor())
                  | (pl.col('volume') % 2 != 0)).height:
        raise ValueError('FinMind outright volume must be positive even double-sided contracts')
    alias = f'FinMind:{physical_contract}'
    canonical = raw.with_columns(
        pl.col('event_ts').cast(pl.Int64).alias('ts'),
        pl.col('price').alias('close'),
        (pl.col('volume') / 2).cast(pl.Int64).alias('volume'),
        pl.lit(day).alias('trading_date'), pl.lit(alias).alias('query_contract'),
    )
    bars, stats = normalize_continuous_ticks(canonical, day=day, alias=alias,
                                             physical=physical_contract, digest=source_sha256,
                                             product=product, asset_class=asset_class)
    return bars, stats


def read_finmind_contract_day(root: Path, row: dict,
                             official_fact: dict | None) -> tuple[pl.DataFrame, dict]:
    """Read and validate one receipt; failures remain explicit source evidence."""
    from stockagent.data.tw_stock_futures_history import MINUTE_SCHEMA
    from stockagent.data.tw_stock_futures_repair import _check_price_bounds
    day, product, physical = row['date'], row['product'], row['physical_contract']
    data_path, receipt_path = source_paths(Path(root), product, day)
    empty = pl.DataFrame(schema=MINUTE_SCHEMA)
    evidence = dict(date=day, physical_contract=physical, status='missing_receipt',
                    alias=f'FinMind:{physical}', source_file_sha256='', receipt_sha256='',
                    official_volume=row.get('volume'), tick_volume=None, tick_rows=None,
                    detail='', source_row_observed=bool(row.get('source_row_observed')))
    if not receipt_path.is_file():
        return empty, evidence
    try:
        receipt_sha = sha256_file(receipt_path)
        receipt = json.loads(receipt_path.read_text())
        if (receipt.get('source') != SOURCE or receipt.get('schema_version') != SCHEMA_VERSION
                or receipt.get('product') != product or receipt.get('date') != str(day)):
            raise ValueError('FinMind receipt identity mismatch')
        evidence['receipt_sha256'] = receipt_sha
        if receipt.get('status') != 'complete':
            evidence.update(status='source_empty_unresolved' if receipt.get('status') == 'source_empty'
                            else 'incomplete_receipt', detail='FinMind source receipt has no verified prints')
            return empty, evidence
        if official_fact is None or official_fact.get('outright_volume') is None:
            raise ValueError('FinMind tape requires SHA-verified dated official volume/price evidence')
        if (official_fact.get('date') != day or official_fact.get('physical_contract') != physical):
            raise ValueError('FinMind official fact identity mismatch')
        digest = sha256_file(data_path)
        if digest != receipt.get('sha256'):
            raise ValueError('FinMind tick data SHA mismatch')
        raw = pl.read_parquet(data_path)
        if raw.height != receipt.get('rows'):
            raise ValueError('FinMind tick row count mismatch')
        bars, stats = normalize_finmind_futures_ticks(raw, day=day, product=product,
                                                     physical_contract=physical, source_sha256=digest,
                                                     asset_class=row['asset_class'])
        if stats['tick_volume'] > official_fact['outright_volume']:
            raise ValueError('FinMind observed outright volume exceeds official upper bound')
        bars = _check_price_bounds(bars, official_fact, evidence)
        if sha256_file(data_path) != digest or sha256_file(receipt_path) != receipt_sha:
            raise ValueError('FinMind tick inputs changed during read')
        evidence.update(status='minute_verified', source_file_sha256=digest,
                        official_reason=official_fact.get('official_reason'),
                        outright_volume=official_fact['outright_volume'],
                        tick_volume=int(bars['volume'].sum()), tick_rows=stats['tick_rows'],
                        detail='exact_month_FinMind_outright_volume_divided_by_two_observed_prints_only',
                        repair_kind='finmind_ticks', repair_data_path=str(data_path),
                        repair_receipt_path=str(receipt_path))
        return bars, evidence
    except (OSError, ValueError, TypeError, pl.exceptions.PolarsError) as exc:
        evidence.update(status='invalid_source', detail=str(exc)[:300])
        return empty, evidence
