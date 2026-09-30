"""Independent official day evidence and exact-month minute recovery.

Official total volume includes calendar-spread legs. Only volume left after
spread-to-spread legs is evidence of an outright-book transaction. Price bars
are never reconstructed from daily prices or carried valuation rows.
"""
from __future__ import annotations

import csv
from datetime import date
import hashlib
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_index_futures import _decoded_csv_stream, _parse_trading_date, _taifex_futures_session
from stockagent.data.tw_stock_futures_kbars import normalize_futures_kbars, validate_kbar_completion

REPAIR_SOURCE = 'shioaji_ticks_and_exact_kbars_dated_physical_v2'
NO_TRADE = 'official_no_outright_trades'
NO_CAPACITY = 'official_subcontract_capacity'


def apply_block_evidence(official: pl.DataFrame, manifest_path: Path) -> tuple[pl.DataFrame, dict]:
    """Deduct only independently downloaded, hash-verified block quantities."""
    import io
    receipt = json.loads(manifest_path.read_text())
    if receipt.get('source') != 'taifex_official_block_volume_v1':
        raise ValueError('invalid official block evidence source')
    volumes = {}
    for source in receipt['sources']:
        path = Path(source['path'])
        if (source['url'] != 'https://www.taifex.com.tw/cht/3/dlProductOrderDown'
                or sha256_file(path) != source['sha256']):
            raise ValueError('official block evidence SHA/source mismatch')
        reader = csv.DictReader(io.StringIO(path.read_bytes().decode('cp950')))
        if not {'交易日期','契約','到期月份(週別)','成交數量','交易時段'} <= set(reader.fieldnames or []):
            raise ValueError('invalid block CSV schema; an HTTP response is not data')
        for row in reader:
            day = str(_parse_trading_date(row['交易日期']))
            if day != source['date']:
                raise ValueError('block CSV query date mismatch')
            if row['交易時段'].strip() != '一般':
                continue
            physical = row['契約'].strip()+':'+row['到期月份(週別)'].strip()
            qty = int(row['成交數量'])
            if qty < 0 or (day, physical) in volumes:
                raise ValueError('negative or duplicate block quantity')
            volumes[day, physical] = qty
    rows = official.to_dicts()
    for row in rows:
        block = volumes.get((str(row['date']), row['physical_contract']), 0)
        row['block_volume'] = block
        if block:
            if row['outright_volume'] is None or block > row['outright_volume']:
                raise ValueError('block volume exceeds unaccounted official volume')
            row['outright_volume'] -= block
            if row['outright_volume'] == 0:
                row['official_reason'] = 'spread_and_block_legs_only'
    return pl.DataFrame(rows, infer_schema_length=None), receipt


def _check_price_bounds(bars: pl.DataFrame, fact: dict, evidence: dict) -> pl.DataFrame:
    from stockagent.data.tw_stock_futures_minute import EVENT_MINUTES
    bad = pl.lit(False)
    for field in ('high', 'low'):
        value = fact[f'official_{field}']
        if value not in ('', '-'):
            bad = bad | ((pl.col(field) > float(value) + .011) if field == 'high'
                         else (pl.col(field) < float(value) - .011))
    rejected = bars.filter(bad)
    if rejected.height:
        bad_minutes = rejected['minute'].to_list()
        if any(m in EVENT_MINUTES for m in bad_minutes):
            raise ValueError('invalid price overlaps execution minute; cannot infer a fill')
        evidence['non_execution_quarantine'] = json.dumps(bad_minutes)
        bars = bars.filter(~bad)
        if bars.is_empty():
            raise ValueError('no valid exact observations remain')
    if fact['outright_volume'] == 0:
        raise ValueError('exact source contradicts official zero outright volume')
    return bars


def official_day_evidence(manifest_path: Path, keys: pl.DataFrame, output: Path, *,
                          allow_unreported_volume: bool = False) -> pl.DataFrame:
    needed = {(str(d), c) for d, c in keys.select('date', 'physical_contract').iter_rows()}
    wanted_dates = {d for d, _ in needed}
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    records, spreads, days = {}, {}, {}
    # Official files repeat the same date across thousands of contracts. Parse
    # each exact source token once without changing accepted date semantics.
    parsed_dates = {}
    sources = []
    for item in manifest['receipts']:
        path = Path(item['path'])
        if path.parent.name == 'annual' and path.name[:4] not in {d[:4] for d in wanted_dates}:
            continue
        digest = sha256_file(path)
        if digest != item['sha256']:
            raise ValueError(f'official raw archive SHA mismatch: {path}')
        sources.append(dict(path=str(path), sha256=digest))
        for stream, name, _ in _decoded_csv_stream(path):
            reader = csv.DictReader(stream)
            reader.fieldnames = [str(s).lstrip('\ufeff').strip() for s in reader.fieldnames]
            if not {'交易日期','契約','到期月份(週別)','成交量','開盤價','最高價','最低價','收盤價'} <= set(reader.fieldnames):
                raise ValueError(f'incomplete official daily schema: {name}')
            has_session = '交易時段' in reader.fieldnames
            for raw in reader:
                date_token = raw.get('交易日期')
                if date_token not in parsed_dates:
                    parsed_dates[date_token] = str(_parse_trading_date(date_token))
                day = parsed_dates[date_token]
                if day not in wanted_dates:
                    continue
                if not has_session and day >= '2017-05-15':
                    raise ValueError(f'official session missing after night-session launch: {name} {day}')
                session, _ = _taifex_futures_session(raw.get('交易時段'), source_has_session=has_session, source_name=name)
                if session != '一般':
                    continue
                product, month = raw['契約'].strip().upper(), raw['到期月份(週別)'].strip()
                days.setdefault(day, set()).add(digest)
                if not any((day, f'{product}:{leg}') in needed for leg in month.split('/')):
                    continue
                volume_text = raw['成交量'].strip().replace(',', '')
                if '/' in month and volume_text in ('', '-'):
                    # Unreported spreads contribute no *proven* legs. We only
                    # infer zero outright capacity when known legs already
                    # account for every reported contract lot.
                    continue
                unreported = allow_unreported_volume and volume_text in ('', '-')
                if not volume_text.isdigit() and not unreported:
                    raise ValueError(f'invalid official volume: {name} {day} {product} {month}')
                volume = None if unreported else int(volume_text)
                if '/' in month:
                    legs = month.split('/')
                    if len(legs) != 2 or any(len(v) != 6 or not v.isdigit() for v in legs):
                        continue
                    for leg in legs:
                        key = day, f'{product}:{leg}'
                        if key in needed:
                            spreads[key] = spreads.get(key, 0) + volume
                    continue
                key = day, f'{product}:{month}'
                if key not in needed:
                    continue
                if key in records:
                    raise ValueError(f'duplicate official contract-day: {key}')
                records[key] = dict(official_volume=volume, official_source_sha256=digest,
                                   official_settlement=raw.get('結算價', '').strip(),
                                   **{f'official_{field}': raw[column].strip() for field, column in
                                      [('open','開盤價'),('high','最高價'),('low','最低價'),('close','收盤價')]})
        if sha256_file(path) != digest:
            raise ValueError('official archive changed during read')
    rows = []
    for day, physical in sorted(needed):
        row = records.get((day, physical))
        spread = spreads.get((day, physical), 0)
        if row is None:
            row = dict(official_volume=None, official_source_sha256='', official_settlement='',
                       **{f'official_{s}': '' for s in ['open','high','low','close']})
            reason = 'absent_from_complete_day' if days.get(day) and not spread else 'missing_official_day'
            outright = 0 if reason == 'absent_from_complete_day' else None
        else:
            outright = None if row['official_volume'] is None else row['official_volume'] - spread
            reason = ('unreported_volume' if outright is None else
                'zero_total_volume' if row['official_volume'] == 0 else
                'spread_legs_only' if outright == 0 else 'outright_volume')
            if outright is not None and outright < 0:
                raise ValueError(f'spread legs exceed total volume: {day} {physical}')
        rows.append(dict(date=date.fromisoformat(day), physical_contract=physical, **row,
                         spread_leg_volume=spread, outright_volume=outright, official_reason=reason,
                         official_day_sources=json.dumps(sorted(days.get(day, set())))))
    frame = pl.DataFrame(rows, infer_schema_length=None)
    output.mkdir(parents=True, exist_ok=True)
    atomic_write_parquet(output/'official_evidence.parquet', frame)
    atomic_write_json(output/'official_evidence_manifest.json', dict(
        source='taifex_complete_daily_and_spread_legs_v1', rows=frame.height,
        sha256=sha256_file(output/'official_evidence.parquet'), sources=sources,
        input_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        unreported_volume_policy='preserve_unknown_never_zero' if allow_unreported_volume else 'reject',
        rule_reference='https://www.taifex.com.tw/cht/3/futDailyMarketReport'))
    return frame


class ExactMinuteRecovery:
    def __init__(self, root: Path | None, official: pl.DataFrame, *, participation: float | None = None,
                 capacity_rounding: str = "floor"):
        if capacity_rounding not in {"floor", "ceil"}:
            raise ValueError('futures minute capacity rounding must be floor or ceil')
        self.root = root
        self.participation = participation
        self.capacity_rounding = capacity_rounding
        self.official = {(r['date'], r['physical_contract']): r for r in official.to_dicts()}
        plan = json.loads((root/'repair_plan.json').read_text()) if root is not None else {"tasks": []}
        self.chunks = {}
        for task in plan['tasks']:
            self.chunks.setdefault(task['physical_contract'], []).append(task)
        tick_plan = root/'repair_tick_plan.json' if root is not None else None
        self.tick_tasks = json.loads(tick_plan.read_text())['tasks'] if tick_plan is not None and tick_plan.exists() else []

    def recover(self, row: dict, bars: pl.DataFrame, evidence: dict):
        from downloader.download_shioaji_historical_market_data import _valid_receipt
        day, physical = row['date'], row['physical_contract']
        fact = self.official.get((day, physical))
        if fact is None:
            return bars, evidence
        evidence.update(official_reason=fact['official_reason'], outright_volume=fact['outright_volume'])
        # Independent complete-day evidence can prove no possible integer fill
        # without pretending a missing provider response is a minute observation.
        if fact.get('official_day_sources') not in (None, '', '[]'):
            if fact['outright_volume'] == 0 and fact['official_reason'] in {
                'zero_total_volume', 'spread_legs_only', 'spread_and_block_legs_only', 'absent_from_complete_day',
            }:
                evidence.update(status=NO_TRADE, detail='independent_complete_official_day_zero_outright',
                                tick_volume=0, tick_rows=0)
                return pl.DataFrame(schema=bars.schema), evidence
            if (self.capacity_rounding == 'floor' and self.participation is not None
                    and fact['outright_volume'] is not None
                    and 0 < fact['outright_volume'] * self.participation < 1):
                evidence.update(status=NO_CAPACITY, detail='independent_official_volume_upper_bound_zero_integer_capacity',
                                tick_volume=0, tick_rows=0)
                return pl.DataFrame(schema=bars.schema), evidence
        for task in self.chunks.get(physical, []):
            if not task['start'] <= str(day) <= task['end']:
                continue
            folder = self.root/'contracts'/'futures'/task['code']/'kbars'/f"start={task['start']}_end={task['end']}"
            path, receipt_path = folder/'data.parquet', folder/'receipt.json'
            receipt = _valid_receipt(receipt_path, path, method='kbars', code=task['code'])
            if not receipt:
                continue
            try:
                validate_kbar_completion(
                    receipt,
                    product=row['product'] if row.get('_source_scope') == 'all_futures_intraday' else None,
                    asset_class=row.get('asset_class', 'stock_future'),
                )
                receipt_sha = sha256_file(receipt_path)
                raw = pl.read_parquet(path) if receipt['status'] == 'complete' else pl.DataFrame()
                if raw.height != receipt['rows']:
                    raise ValueError('exact KBar receipt row count mismatch')
                if raw.height:
                    raw = raw.filter(pl.col('trading_date') == day)
                if raw.height:
                    end = pl.col('ts').cast(pl.Datetime('ns'))
                    minute = end.dt.hour().cast(pl.Int32)*60+end.dt.minute().cast(pl.Int32)
                    start_minute, end_minute = 525, 825
                    if row.get('_source_scope') == 'all_futures_intraday':
                        from stockagent.data.tw_price_rules import taifex_futures_day_session_minutes
                        start_minute, end_minute = taifex_futures_day_session_minutes(
                            day, product_code=row['product'], asset_class=row['asset_class'])
                    raw = raw.filter((end.dt.date() == day) & minute.is_between(start_minute + 1,end_minute) & (pl.col('Volume') > 0))
                source_sha = receipt.get('sha256', '')
                if raw.height:
                    recovered = normalize_futures_kbars(raw, code=task['code'], physical_contract=physical,
                                                       source_sha256=source_sha, full_session=True,
                                                       product=row['product'] if row.get('_source_scope') == 'all_futures_intraday' else None,
                                                       asset_class=row.get('asset_class', 'stock_future'))
                    if (row.get('_source_scope') == 'all_futures_intraday'
                            and recovered['volume'].sum() > min(row['volume'], fact['outright_volume'] if fact['outright_volume'] is not None else row['volume'])):
                        raise ValueError('exact KBar observed volume exceeds official volume bound')
                    recovered = _check_price_bounds(recovered, fact, evidence)
                    # Exact-month identity needs no OHLC equality heuristic.
                    # Retain observed quantities just as with ticks; absent
                    # opening prints never authorize invented volume/prices.
                    evidence.update(status='minute_verified', detail='exact_month_kbars_within_official_range_observed_volume_only',
                                    tick_volume=int(recovered['volume'].sum()), tick_rows=raw.height)
                    bars = recovered
                elif fact['outright_volume'] == 0:
                    evidence.update(status=NO_TRADE, detail='official_complete_day_and_exact_kbar_no_outright_trades',
                                    tick_volume=0, tick_rows=0)
                elif (self.capacity_rounding == 'floor'
                      and self.participation is not None and fact['outright_volume'] is not None
                      and 0 < fact['outright_volume'] * self.participation < 1):
                    evidence.update(status=NO_CAPACITY, detail='daily_ordinary_volume_upper_bound_implies_zero_integer_capacity_at_every_minute',
                                    tick_volume=0, tick_rows=0)
                else:
                    evidence.update(status='source_empty_unresolved', detail='exact_kbars_empty_but_official_outright_volume_positive')
                    continue
                if (sha256_file(receipt_path) != receipt_sha
                        or (source_sha and sha256_file(path) != source_sha)):
                    raise ValueError('exact KBar source changed during read')
                evidence.update(alias=task['code'], receipt_sha256=receipt_sha, source_file_sha256=source_sha,
                                repair_receipt_path=str(receipt_path), repair_data_path=str(path), repair_kind='exact_kbars')
                return bars, evidence
            except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
                evidence.update(status='invalid_repair_source', detail=str(exc)[:300])
        # Some dates are absent from the KBar endpoint but still exist as ticks.
        # Reuse the canonical tick schema and the same physical-price checks.
        for task in self.tick_tasks:
            if task['physical_contract'] != physical or task['start'] != str(day):
                continue
            folder = self.root/'contracts'/'futures'/task['code']/'ticks'/f'trading_date={day}'
            path, receipt_path = folder/'data.parquet', folder/'receipt.json'
            receipt = _valid_receipt(receipt_path, path, method='ticks', code=task['code'])
            if not receipt or receipt['status'] != 'complete':
                continue
            try:
                from stockagent.data.tw_stock_futures_history import normalize_continuous_ticks
                validate_kbar_completion(
                    dict(receipt, start=str(day), end=str(day)),
                    product=row['product'] if row.get('_source_scope') == 'all_futures_intraday' else None,
                    asset_class=row.get('asset_class', 'stock_future'),
                )
                receipt_sha = sha256_file(receipt_path)
                raw = pl.read_parquet(path)
                if raw.height != receipt['rows']:
                    raise ValueError('exact tick receipt row count mismatch')
                recovered, stats = normalize_continuous_ticks(
                    raw, day=day, alias=task['code'], physical=physical, digest=receipt['sha256'],
                    product=(row['product'] if row.get('_source_scope') == 'all_futures_intraday'
                             or row.get('asset_class') in {'etf_future', 'index_future'} else None),
                    asset_class=row.get('asset_class', 'stock_future'),
                )
                if (row.get('_source_scope') == 'all_futures_intraday'
                        and stats['tick_volume'] > min(row['volume'], fact['outright_volume'] if fact['outright_volume'] is not None else row['volume'])):
                    raise ValueError('exact tick observed volume exceeds official volume bound')
                recovered = _check_price_bounds(recovered, fact, evidence)
                if sha256_file(path) != receipt['sha256'] or sha256_file(receipt_path) != receipt_sha:
                    raise ValueError('exact ticks changed during read')
                evidence.update(status='minute_verified', detail='exact_month_ticks_within_official_range_observed_volume_only',
                                tick_volume=int(recovered['volume'].sum()), tick_rows=stats['tick_rows'],
                                alias=task['code'], receipt_sha256=receipt_sha, source_file_sha256=receipt['sha256'],
                                repair_receipt_path=str(receipt_path), repair_data_path=str(path), repair_kind='exact_ticks')
                return recovered, evidence
            except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
                evidence.update(status='invalid_repair_source', detail=str(exc)[:300])
        return bars, evidence
