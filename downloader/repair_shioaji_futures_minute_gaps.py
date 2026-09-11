"""Resume exact-month KBar repair through the canonical historical collector.

An immutable daily panel supplies physical identities and dates. Only unresolved
positive-volume candidates are queried. No current-catalog membership or R1
target is used as historical identity, and empty responses remain unresolved.
"""
from __future__ import annotations

import argparse
import fcntl
from datetime import date, timedelta
import json
import os
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.common import SharedRateLimiter
from downloader.download_shioaji_historical_market_data import (
    HistoryContract, HistoryTask, _query_task, _kbar_paths, _tick_paths, _valid_receipt,
)


def repair_tasks(daily: pl.DataFrame, gaps: pl.DataFrame, *, method: str = 'kbars') -> list[HistoryTask]:
    """Repair requested physical identities, independent of policy eligibility.

    Adjusted and older held contracts need their own observations even when
    they are never eligible for new model orders. The official inventory owns
    identity; the explicit gap table owns query dates. No gap may vanish in an
    inner join with the policy's selected front contracts.
    """
    if method not in {'kbars', 'ticks'}:
        raise ValueError('repair method must be kbars or ticks')
    inventory = daily
    if {'product', 'contract'} <= set(inventory.columns):
        identity = pl.concat_str('product', pl.lit(':'), 'contract')
        if ('physical_contract' in inventory.columns
                and inventory.filter(pl.col('physical_contract').is_null()
                                     | (pl.col('physical_contract') != identity)).height):
            raise ValueError('official inventory physical identity mismatch')
        inventory = inventory.with_columns(identity.alias('physical_contract'))
    if 'physical_contract' not in inventory.columns:
        raise ValueError('repair requires an official physical-contract inventory')
    selected = gaps.select('date', 'physical_contract').unique().sort('date', 'physical_contract')
    if selected.null_count().sum_horizontal().item():
        raise ValueError('repair gap date and physical identity must not be null')
    unknown = selected.join(inventory.select('physical_contract').unique(), on='physical_contract', how='anti')
    if unknown.height:
        raise ValueError(f'requested repair identities absent from official inventory: {unknown.head(10).to_dicts()}')
    tasks = []
    for (physical,), frame in selected.partition_by('physical_contract', as_dict=True).items():
        if not re.fullmatch(r'[A-Z0-9]{3}:\d{6}', physical):
            raise ValueError(f'invalid archived physical futures identity: {physical}')
        product, month = physical.split(':')
        if not 1 <= int(month[4:]) <= 12:
            raise ValueError(f'invalid physical delivery month: {physical}')
        dates = sorted(frame['date'].to_list())
        if any(not isinstance(d, date) or not 0 <= int(month[:4]) - d.year <= 1 for d in dates):
            raise ValueError(f'repair dates outside dated contract query range: {physical}')
        while dates:
            start = dates[0]
            included = [d for d in dates if d <= start + timedelta(days=28 if method == 'kbars' else 0)]
            end = included[-1]
            dates = [d for d in dates if d > end]
            code = product + 'ABCDEFGHIJKL'[int(month[4:]) - 1] + month[3]
            row = HistoryContract('exact_futures', 2, 'FUT', 'futures', code, product, physical,
                                  'TAIFEX', start, end, delivery_month=month)
            tasks.append(HistoryTask(2, -end.toordinal(), code, 0, method, row, start, end))
    return sorted(tasks, key=lambda t: (t.start, t.code))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--daily-data-path', type=Path, required=True)
    parser.add_argument('--gaps-path', type=Path, required=True)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('--max-tasks', type=int)
    parser.add_argument('--method', choices=['kbars','ticks'], default='kbars')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    daily_sha, gaps_sha = sha256_file(args.daily_data_path), sha256_file(args.gaps_path)
    tasks = repair_tasks(pl.read_parquet(args.daily_data_path), pl.read_parquet(args.gaps_path), method=args.method)
    inventory = [dict(physical_contract=f'{t.contract.root}:{t.contract.delivery_month}', code=t.code,
                      start=str(t.start), end=str(t.end), method=t.method) for t in tasks]
    plan = dict(source_daily_sha256=daily_sha, source_gaps_sha256=gaps_sha, tasks=inventory)
    args.output_root.mkdir(parents=True, exist_ok=True)
    # Use the same collector lock protocol; a second repair must not log in.
    with (args.output_root / 'repair.lock').open('a') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        prefix = 'repair' if args.method == 'kbars' else 'repair_tick'
        atomic_write_json(args.output_root / f'{prefix}_plan.json', plan)
        def paths(t):
            return (_kbar_paths(args.output_root, t.contract, t.start, t.end) if t.method == 'kbars'
                    else _tick_paths(args.output_root, t.contract, t.start))
        pending = [t for t in tasks if _valid_receipt(*reversed(paths(t)), method=t.method, code=t.code) is None]
        print(json.dumps(dict(tasks=len(tasks), pending=len(pending))), flush=True)
        if args.dry_run:
            return
        if not pending:
            return
        if not all(os.environ.get(k) for k in ('SHIOAJI_API_KEY', 'SHIOAJI_SECRET_KEY')):
            raise SystemExit('repair plan saved; load the existing Shioaji environment file before downloading one-minute KBars')
        import shioaji as sj
        api = sj.Shioaji(simulation=True)
        api.set_event_callback(lambda *_: None)
        api.login(api_key=os.environ['SHIOAJI_API_KEY'], secret_key=os.environ['SHIOAJI_SECRET_KEY'], subscribe_trade=False)
        limiter = SharedRateLimiter(.15, name='shioaji_quote_query')
        results = []
        try:
            for index, task in enumerate(pending[:args.max_tasks], 1):
                try:
                    receipt, _ = _query_task(api, task, output_root=args.output_root, timeout_ms=30000,
                                             retries=1, retry_backoff=2, rate_limiter=limiter,
                                             kbars_only=task.method == 'kbars', max_traffic_fraction=.9,
                                             allow_market_hours=False, allow_archived_contract=True)
                    result = dict(code=task.code, start=str(task.start), end=str(task.end), status=receipt['status'], rows=receipt['rows'])
                except Exception as exc:
                    from downloader.download_shioaji_historical_market_data import HistoricalWindowReached, TrafficBudgetReached
                    if isinstance(exc, (HistoricalWindowReached, TrafficBudgetReached)):
                        raise
                    result = dict(code=task.code, start=str(task.start), end=str(task.end), status='error', error_type=type(exc).__name__)
                results.append(result)
                atomic_write_json(args.output_root / f'{prefix}_progress.json', dict(done=index, pending_total=len(pending), results=results))
                print(json.dumps(result), flush=True)
        finally:
            api.logout()
        if sha256_file(args.daily_data_path) != daily_sha or sha256_file(args.gaps_path) != gaps_sha:
            raise ValueError('repair input changed during download')


if __name__ == '__main__':
    main()
