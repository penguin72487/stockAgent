"""Notice-backed physical contract transfers; no new-order universe changes."""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import re

import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file

TRANSITION_POLICY = 'notice_effective_date_transfer_signed_quantity_and_prior_mark_without_fill_or_fee; separate_new_standard_contract'
TRANSITION_CASH_POLICY = TRANSITION_POLICY + '; notice_integer_cash_adjustment_once_on_transferred_prior_inventory'


def _validate_transition_cash(record, schema_version):
    """Accept only the notice's whole-TWD old-holder adjustment, not a price."""
    amount = record.get('cash_adjustment_per_contract')
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise ValueError('invalid corporate transition cash adjustment')
    if not np.isfinite(amount) or amount < 0 or amount >= 2**24 or int(amount) != amount:
        raise ValueError('invalid corporate transition cash adjustment')
    pure = record.get('kind') == 'cash_subscription_unchanged_multiplier' and amount == 0
    mixed = (schema_version == 2
             and record.get('kind') == 'cash_dividend_and_subscription_unchanged_multiplier'
             and amount > 0)
    if not (pure or mixed):
        raise ValueError('unsupported corporate transition cash policy')


def load_transition_bundle(path):
    path = Path(path)
    manifest = json.loads(path.read_text())
    schema_version = manifest.get('schema_version')
    if schema_version not in (1, 2) or manifest.get('source') != 'taifex_corporate_contract_transition_v1':
        raise ValueError('corporate transition source contract mismatch')
    required = {'transitions.json', 'daily.parquet', 'minutes.parquet', 'coverage.parquet',
                'finals.parquet', 'official/official_evidence.parquet',
                'official/official_evidence_manifest.json'}
    for item in manifest['kbar_sources']:
        required.update((item['data_file'], item['receipt_file']))
    for item in manifest.get('tick_sources', []):
        required.update((item['data_file'], item['receipt_file']))
    if not required.issubset(manifest['files']):
        raise ValueError('corporate transition manifest omits required source hashes')
    for name, item in manifest['files'].items():
        p = path.parent / name
        if not p.resolve().is_relative_to(path.parent.resolve()) or sha256_file(p) != item['sha256']:
            raise ValueError('corporate transition input SHA mismatch')
    records = json.loads((path.parent/'transitions.json').read_text())
    seen = set()
    destinations = set()
    for r in records:
        _validate_transition_cash(r, schema_version)
        day, announced = date.fromisoformat(r['date']), date.fromisoformat(r['announcement_date'])
        old, new = r['from_physical_contract'], r['to_physical_contract']
        if (announced > day or not all(re.fullmatch(r'[A-Z0-9]{3}:[0-9]{6}', c) for c in (old,new))
                or old == new or old.split(':')[1] != new.split(':')[1]
                or r['old_multiplier'] != r['new_multiplier'] or r['new_multiplier'] not in (100.,2000.)
                or r['quantity_ratio'] != 1
                or manifest['files'].get(r['notice_file'], {}).get('sha256') != r['notice_sha256']):
            raise ValueError('unsupported or invalid corporate contract transition')
        key=(r['date'],old);dest=(r['date'],new)
        if key in seen or dest in destinations:
            raise ValueError('ambiguous corporate contract transfer')
        seen.add(key);destinations.add(dest)
    frames = {name: pl.read_parquet(path.parent/(name+'.parquet'))
              for name in ('daily','minutes','coverage','finals')}
    covered = frames['coverage']
    if covered.select('date','physical_contract').is_duplicated().any():
        raise ValueError('duplicate adjusted contract coverage')
    if covered.filter(~pl.col('status').is_in(['minute_verified','official_no_outright_trades'])).height:
        raise ValueError('unresolved adjusted contract minutes')
    from stockagent.data.tw_stock_futures_carry import load_carry_no_trade_evidence
    no_trade = covered.filter(pl.col('status') == 'official_no_outright_trades')
    proof = load_carry_no_trade_evidence(path.parent/'official/official_evidence.parquet')
    if no_trade.join(proof, on=['date','physical_contract'], how='anti').height:
        raise ValueError('adjusted zero-trade claim lacks official proof')
    bars=frames['minutes']
    if bars.select('date','physical_contract','minute').is_duplicated().any():
        raise ValueError('duplicate adjusted minute')
    verified = covered.filter(pl.col('status')=='minute_verified').select('date','physical_contract','source_file_sha256')
    if bars.select(verified.columns).unique().join(verified,on=verified.columns,how='anti').height:
        raise ValueError('adjusted minute lacks dated physical source proof')
    from stockagent.data.tw_stock_futures_minute import EVENT_MINUTES
    from stockagent.data.tw_stock_futures_kbars import normalize_futures_kbars, validate_kbar_completion
    from downloader.download_shioaji_historical_market_data import _valid_receipt
    reconstructed=[]
    source_days=[]
    for item in manifest['kbar_sources']:
        raw=path.parent/item['data_file']; receipt_path=path.parent/item['receipt_file']
        proof=_valid_receipt(receipt_path,raw,method='kbars',code=item['code'])
        if proof is None or proof['status']!='complete':
            raise ValueError('adjusted KBar source receipt invalid')
        validate_kbar_completion(proof)
        raw_frame=pl.read_parquet(raw)
        frame=normalize_futures_kbars(raw_frame,code=item['code'],
            physical_contract=item['physical_contract'],source_sha256=sha256_file(raw))
        reconstructed.append(frame)
        full=normalize_futures_kbars(raw_frame,code=item['code'],
            physical_contract=item['physical_contract'],source_sha256=sha256_file(raw),full_session=True)
        source_days.append(full.select(verified.columns).unique())
    from stockagent.data.tw_stock_futures_history import normalize_continuous_ticks
    official = pl.read_parquet(path.parent / 'official/official_evidence.parquet')
    for item in manifest.get('tick_sources', []):
        raw = path.parent / item['data_file']
        receipt_path = path.parent / item['receipt_file']
        day = date.fromisoformat(item['date'])
        physical = item['physical_contract']
        if not re.fullmatch(r'[A-Z0-9]{3}:[0-9]{6}', physical):
            raise ValueError('invalid adjusted tick physical identity')
        root, month = physical.split(':')
        if not 1 <= int(month[4:]) <= 12 or item['code'] != root + 'ABCDEFGHIJKL'[int(month[4:]) - 1] + month[3]:
            raise ValueError('adjusted tick code differs from physical delivery month')
        proof = _valid_receipt(receipt_path, raw, method='ticks', code=item['code'])
        if proof is None or proof['status'] != 'complete' or proof.get('trading_date') != str(day):
            raise ValueError('adjusted tick source receipt invalid')
        validate_kbar_completion(dict(proof, start=str(day), end=str(day)))
        raw_frame = pl.read_parquet(raw)
        if raw_frame.height != proof['rows']:
            raise ValueError('adjusted tick receipt row count mismatch')
        full, stats = normalize_continuous_ticks(raw_frame, day=day, alias=item['code'],
            physical=physical, digest=sha256_file(raw))
        facts = official.filter((pl.col('date') == day) & (pl.col('physical_contract') == physical))
        if facts.height != 1:
            raise ValueError('adjusted ticks lack exact official contract-day proof')
        fact = facts.row(0, named=True)
        if fact['outright_volume'] != stats['tick_volume'] or any(
            fact[f'official_{field}'] in ('', '-', None)
            or abs(float(fact[f'official_{field}']) - stats[f'tick_{field}']) > .011
            for field in ('open', 'high', 'low', 'close')
        ):
            raise ValueError('adjusted ticks differ from official outright volume or OHLC')
        reconstructed.append(full.filter(pl.col('minute').is_in(EVENT_MINUTES)))
        source_days.append(full.select(verified.columns).unique())
    if verified.join(pl.concat(source_days).unique(),on=verified.columns,how='anti').height:
        raise ValueError('verified continuation day absent from its exact source KBars')
    if not pl.concat(reconstructed).sort('date','physical_contract','minute').equals(bars.sort('date','physical_contract','minute')):
        raise ValueError('adjusted minute tape differs from exact source KBars')
    if bars.filter(~pl.col('minute').is_in(EVENT_MINUTES)
        | ~pl.all_horizontal(pl.col(c).is_finite() & (pl.col(c)>0) for c in ('vwap','high','low','close','volume'))
        | ~pl.col('vwap').is_between(pl.col('low')-1e-8,pl.col('high')+1e-8)
        | ~pl.col('close').is_between(pl.col('low'),pl.col('high'))).height:
        raise ValueError('invalid adjusted minute prices/capacity')
    frames.update(records=records, manifest_sha256=sha256_file(path),
                  policy=TRANSITION_CASH_POLICY if schema_version == 2 else TRANSITION_POLICY)
    return frames


def merge_transition_observations(minutes, coverage, settlements, bundle):
    """Sidecar may add new physicals; existing facts cannot be replaced silently."""
    keys=['date','physical_contract']
    for old,new,key in ((minutes,bundle['minutes'],keys+['minute']), (coverage,bundle['coverage'],keys)):
        if old.select(key).join(new.select(key),on=key,how='inner').height:
            raise ValueError('adjusted sidecar overlaps the existing minute snapshot')
    minutes=pl.concat([minutes,bundle['minutes'].select(minutes.columns)],how='vertical_relaxed')
    coverage=pl.concat([coverage.select(*keys,'status'),bundle['coverage'].select(*keys,'status')],how='vertical_relaxed')
    finals=bundle['finals']
    matched=settlements.join(finals,on=keys,how='inner',suffix='_adjusted')
    if matched.filter((pl.col('final_settlement_price')!=pl.col('final_settlement_price_adjusted'))
        | (pl.col('final_settlement_value').is_not_null()
           & (pl.col('final_settlement_value')!=pl.col('final_settlement_value_adjusted')))).height:
        raise ValueError('adjusted final settlement contradicts the pinned source')
    settlements=pl.concat([settlements.join(finals.select(keys),on=keys,how='anti'),finals],how='vertical_relaxed')
    return minutes,coverage,settlements


def append_transition_rows(rows, selected, dates, symbols, settlements, bundle):
    """Add only descendants of contracts actually reachable before the notice date."""
    starts=selected.group_by('physical_contract','underlying_symbol').agg(pl.col('date').min().alias('start'))
    frames=[]; relevant=[]
    for r in bundle['records']:
        day=date.fromisoformat(r['date']); old=r['from_physical_contract']; new=r['to_physical_contract']
        reachable=starts.filter((pl.col('physical_contract')==old)
            & (pl.col('underlying_symbol')==r['underlying_symbol']) & (pl.col('start')<day))
        if not reachable.height or np.datetime64(day) not in dates:
            continue
        final=settlements.filter(pl.col('physical_contract')==new)
        if final.height != 1:
            raise ValueError(f'reachable adjusted contract lacks official expiry: {new}')
        end=final['date'].item()
        calendar=pl.DataFrame({'date':dates,'di':np.arange(len(dates))}).with_columns(pl.col('date').cast(pl.Date))
        calendar=calendar.filter(pl.col('date').is_between(day,end)).with_columns(pl.lit(new).alias('physical_contract'))
        if calendar.join(bundle['coverage'].select('date','physical_contract'),on=['date','physical_contract'],how='anti').height:
            raise ValueError(f'reachable adjusted contract has unverified continuation days: {new}')
        facts=bundle['daily'].filter(pl.col('physical_contract')==new).select('date','physical_contract','valuation_settlement','source_row_observed')
        extra=calendar.join(facts,on=['date','physical_contract'],how='left',validate='1:1').with_columns(
            pl.lit(r['underlying_symbol']).alias('underlying_symbol'),
            pl.lit(symbols.index(r['underlying_symbol'])).alias('si'),
            pl.lit(r['new_multiplier']).alias('contract_multiplier'),
            pl.lit(day).alias('start'),pl.lit(end).alias('official_expiry'),
        )
        frames.append(extra.select(rows.columns).cast(rows.schema));relevant.append(r)
    if frames:
        rows=pl.concat([rows,*frames],how='vertical')
    if rows.select('date','physical_contract').is_duplicated().any():
        raise ValueError('adjusted contract already has another continuation owner')
    return rows,relevant
