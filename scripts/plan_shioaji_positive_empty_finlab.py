#!/usr/bin/env python3
"""Plan source-bound FinLab minute checks for official/Shioaji contradictions.

Continuous aliases select monthly rank; they are not individual contracts.
This constructs exact product/month/day candidates but does not assert that a
minute bar is an executable tick, call a provider, or change a source dataset.
"""
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json
from downloader.provider_priority import _finlab_futures_partition


ROOT = Path(__file__).resolve().parents[1]
HISTORY = ROOT / 'data_tw_futures/shioaji_history'
DAILY = ROOT / 'data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet'
OUTPUT = (ROOT / 'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'
          / 'shioaji-positive-empty-finlab-candidates-20261103-v1.json')


def signature(path):
    stat = path.lstat()
    if not path.is_file() or path.is_symlink():
        raise ValueError(f'expected a regular source file: {path}')
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def stable_bytes(path):
    before = signature(path)
    data = path.read_bytes()
    if signature(path) != before:
        raise RuntimeError(f'source changed during inspection: {path}')
    return data, hashlib.sha256(data).hexdigest()


def build(history=HISTORY, daily=DAILY):
    history, daily = Path(history), Path(daily)
    if history.is_symlink() or not history.is_dir():
        raise ValueError('the enrolled continuous history root is required')
    gaps, manifest_pins = [], []
    states = Counter()
    for directory in sorted(history.iterdir()):
        if directory.name == 'batch.lock' and directory.is_file() and not directory.is_symlink():
            continue  # Native batch coordination, not a futures alias.
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError(f'unknown history namespace entry: {directory}')
        raw, digest = stable_bytes(directory / 'manifest.json')
        manifest = json.loads(raw)
        if manifest.get('contract') != directory.name:
            raise ValueError('manifest and continuous alias differ')
        values = manifest.get('positive_activity_empty_dates', [])
        if not isinstance(values, list) or len(values) != len(set(values)):
            raise ValueError('invalid or repeated positive-activity empty dates')
        states[manifest.get('coverage_state', 'missing')] += 1
        gaps.extend({'alias': directory.name, 'date': date.fromisoformat(day)} for day in values)
        manifest_pins.append({'alias': directory.name, 'sha256': digest})
    if not gaps:
        raise ValueError('no positive-activity gaps to plan')
    gaps_frame = pl.DataFrame(gaps)
    if gaps_frame.unique().height != gaps_frame.height:
        raise ValueError('duplicate continuous alias/date')
    before = signature(daily)
    daily_sha = hashlib.sha256(daily.read_bytes()).hexdigest()
    start, end = min(x['date'] for x in gaps), max(x['date'] for x in gaps)
    # Match the canonical Shioaji activity logic: rank all monthly contracts
    # first, then require positive volume; weeklies cannot shift the rank.
    monthly = (pl.scan_parquet(daily)
        .filter((pl.col('date') >= pl.lit(start)) & (pl.col('date') <= pl.lit(end)))
        .select('date', 'product', 'contract', 'shioaji_roots', 'volume')
        .filter(pl.col('contract').str.contains(r'^\d{6}$'))
        .with_columns(pl.col('contract').rank('dense').over(['date', 'product']).alias('rank'))
        .filter((pl.col('rank') <= 2) & (pl.col('volume') > 0))
        .with_columns(pl.when(pl.col('shioaji_roots').fill_null('') == '')
            .then(pl.col('product')).otherwise(pl.col('shioaji_roots')).str.split(',').alias('root'))
        .explode('root').with_columns(pl.col('root').str.strip_chars())
        .with_columns(pl.concat_str([pl.col('root'), pl.lit('R'),
                                     pl.col('rank').cast(pl.String)]).alias('alias'))
        .select('alias', 'date', 'product', 'contract').collect())
    if signature(daily) != before:
        raise RuntimeError('official daily activity changed during inspection')
    matched = gaps_frame.join(monthly, on=['alias', 'date'], how='left')
    counts = matched.group_by(['alias', 'date']).len()
    ambiguous = set(tuple(row) for row in counts.filter(pl.col('len') > 1)
                    .select('alias', 'date').iter_rows())
    items, unresolved = {}, []
    for row in matched.iter_rows(named=True):
        identity = (row['alias'], row['date'])
        if identity in ambiguous or row['product'] is None:
            unresolved.append({'alias': row['alias'], 'date': row['date'].isoformat(),
                               'reason': 'ambiguous' if identity in ambiguous else 'no_current_official_match'})
            continue
        item = {'dataset': f"tw_futures_minute:{row['product']}_{row['contract']}",
                'partition': row['date'].isoformat()}
        _finlab_futures_partition(item)
        key = (item['dataset'], item['partition'])
        if key in items and items[key]['alias'] != row['alias']:
            raise ValueError('multiple aliases map to the same exact request; review before registration')
        items[key] = {'alias': row['alias'], **item}
    return {'contract': 'shioaji_positive_empty_finlab_candidates_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'official_daily_path': str(daily), 'official_daily_sha256': daily_sha,
            'manifest_root': str(history), 'manifest_pins': manifest_pins,
            'manifest_coverage_states': dict(states), 'source_gap_alias_dates': len(gaps),
            'candidates': [items[key] for key in sorted(items)], 'unresolved': unresolved,
            'provider_calls': 0, 'data_or_training_admission': False,
            'semantics': 'Official monthly positive volume selects source checks; minute bars do not prove exact fills.'}


def main():
    plan = build()
    atomic_write_json(OUTPUT, plan)
    print(json.dumps({'path': str(OUTPUT), 'official_daily_sha256': plan['official_daily_sha256'],
        'manifest_count': len(plan['manifest_pins']), 'source_gap_alias_dates': plan['source_gap_alias_dates'],
        'candidate_count': len(plan['candidates']), 'unresolved': plan['unresolved'],
        'provider_calls': 0}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
