#!/usr/bin/env python3
"""Audit a candidate TX one-contract benchmark against retained daily inputs.

This does not establish fills, PIT publication, intraday funding or account-only
valuations. The summary is evidence for a later source-bound benchmark, not a
training or benchmark-admission receipt.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import sha256_file  # noqa: E402


def audit(reference: pl.DataFrame, daily: pl.DataFrame, rules: pl.DataFrame,
          start: str, end: str) -> dict:
    """Check identities and availability; never synthesize an execution."""
    reference = (reference.filter((pl.col('product') == 'TX') & (pl.col('tenor_rank') == 1))
                 .sort('date'))
    daily = daily.filter((pl.col('product') == 'TX') & ~pl.col('accounting_only_date'))
    rules = rules.filter(pl.col('product') == 'TX')
    for frame, keys in ((reference, ['date']), (daily, ['date', 'contract']),
                        (rules, ['date', 'contract'])):
        if frame.select(pl.any_horizontal(pl.col(key).is_null() for key in keys).any()).item():
            raise ValueError(f'null TX identity: {keys}')
        if frame.select(pl.struct(keys).is_duplicated().any()).item():
            raise ValueError(f'ambiguous TX identity: {keys}')
    ref = (reference.with_columns(pl.col('date').shift(1).alias('prior_date'),
                                   pl.col('contract').shift(1).alias('prior_contract'))
           .with_columns((pl.col('contract') != pl.col('prior_contract')).alias('roll'))
           .filter(pl.col('date').is_between(pl.lit(start).str.to_date(),
                                            pl.lit(end).str.to_date())))
    if ref.is_empty() or ref['prior_date'].null_count():
        raise ValueError('benchmark requires an observed previous source session')
    dates = daily.select('date').unique().filter(
        pl.col('date').is_between(pl.lit(start).str.to_date(), pl.lit(end).str.to_date())).sort('date')
    if not ref.select('date').equals(dates):
        raise ValueError('candidate TX market sessions differ between inputs')

    rule_fields = ['date', 'contract', 'initial', 'settlement_initial', 'margin_kind',
                   'contract_multiplier', 'known_at', 'settlement_known_at']
    rows = ref.join(rules.select(rule_fields), on=['date', 'contract'], how='left', validate='1:1')
    roll = rows.filter(pl.col('roll'))
    leg_fields = ['date', 'contract', 'official_expiry', 'terminal_event', 'final_settlement_price', 'close',
                  'observed_close', 'official_close', 'outright_volume', 'source_row_observed',
                  'official_row_observed']
    legs = daily.select(leg_fields)
    old_names = {name: 'old_' + name for name in leg_fields if name not in {'date', 'contract'}}
    new_names = {name: 'new_' + name for name in leg_fields if name not in {'date', 'contract'}}
    old = legs.rename({'date': 'prior_date', 'contract': 'prior_contract', **old_names})
    new = legs.rename({'date': 'prior_date', **new_names})
    roll = (roll.join(old, on=['prior_date', 'prior_contract'], how='left', validate='m:1')
            .join(new, on=['prior_date', 'contract'], how='left', validate='m:1'))

    def count(expr: pl.Expr, *, frame: pl.DataFrame = rows) -> int:
        return int(frame.select(expr.fill_null(False).sum()).item() or 0)

    valid_margin = ((pl.col('initial') > 0) & (pl.col('settlement_initial') > 0)
                    & (pl.col('margin_kind') == 'fixed_twd') & (pl.col('contract_multiplier') == 200))
    def leg_valid(prefix: str) -> pl.Expr:
        return ((pl.col(prefix + 'source_row_observed') == True)
                & (pl.col(prefix + 'official_row_observed') == True)
                & (pl.col(prefix + 'outright_volume') > 0)
                & (pl.col(prefix + 'observed_close') > 0))
    old_valid, new_valid = leg_valid('old_'), leg_valid('new_')
    expires = pl.col('old_official_expiry') == pl.col('prior_date')
    differs = expires & (pl.col('old_observed_close') != pl.col('old_final_settlement_price'))
    examples = (roll.filter(differs.fill_null(False)).select(
        'date', 'prior_date', 'prior_contract', 'contract', 'old_terminal_event',
        'old_official_close', 'old_observed_close', 'old_final_settlement_price',
        'new_official_close', 'new_observed_close')
        .head(5).to_dicts())
    return {
        'contract': 'tx_one_contract_daily_input_diagnostic_v1',
        'start_date': str(ref['date'][0]), 'end_date': str(ref['date'][-1]),
        'market_sessions': ref.height, 'rolls': roll.height,
        'sessions_with_positive_fixed_margin_and_200_multiplier': count(valid_margin),
        'sessions_with_margin_known_time_strings': count(pl.col('known_at').is_not_null()
                                                          & pl.col('settlement_known_at').is_not_null()),
        'rolls_with_old_prior_day_observed_close_and_positive_outright': count(old_valid, frame=roll),
        'rolls_with_new_prior_day_observed_close_and_positive_outright': count(new_valid, frame=roll),
        'rolls_with_both_prior_day_observed_closes_and_positive_outright': count(old_valid & new_valid, frame=roll),
        'rolls_where_old_expiry_is_prior_session': count(expires, frame=roll),
        'rolls_where_old_prior_terminal_is_cash_settlement': count(
            pl.col('old_terminal_event') == 'cash_settlement', frame=roll),
        'rolls_where_old_expiry_close_differs_from_final_settlement': count(differs, frame=roll),
        'expiry_close_final_difference_examples': examples,
        'training_admitted': False, 'one_contract_benchmark_admitted': False,
        'limitations': [
            'Daily positive volume and observed closes do not prove both roll legs were executable at close.',
            'A known-at string is not proof of historical publication, applicable broker rules, or PIT.',
            'Opening time, transaction costs, account-only events, funding and margin calls require separate evidence.',
            'Candidate market dates and retained sources are not proof of complete historical coverage.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', required=True, type=Path)
    parser.add_argument('--daily', required=True, type=Path)
    parser.add_argument('--rules', required=True, type=Path)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve earlier audit output; choose a new path')
    reference = pl.read_parquet(args.reference)
    daily_fields = ['date', 'product', 'contract', 'accounting_only_date', 'official_expiry',
                    'terminal_event', 'final_settlement_price', 'close', 'observed_close', 'official_close',
                    'outright_volume', 'source_row_observed', 'official_row_observed']
    rule_fields = ['date', 'product', 'contract', 'initial', 'settlement_initial',
                   'margin_kind', 'contract_multiplier', 'known_at', 'settlement_known_at']
    daily = pl.scan_parquet(args.daily).filter(pl.col('product') == 'TX').select(daily_fields).collect()
    rules = pl.scan_parquet(args.rules).filter(pl.col('product') == 'TX').select(rule_fields).collect()
    result = audit(reference, daily, rules, args.start, args.end)
    result['observed_at_utc'] = datetime.now(UTC).isoformat()
    result['input_sha256'] = {str(path): sha256_file(path) for path in
                              (args.reference, args.daily, args.rules, Path(__file__))}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n')
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
