#!/usr/bin/env python3
"""Stage a TX closing reference whose expiry mark uses the official final value.

This is a research-only gross price reference. It does not model a broker fill,
transaction costs, one-lot account funding, intraday margin or account-only
events, and must not be described as the requested benchmark account.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import sha256_file  # noqa: E402
from stockagent.data.tw_futures_benchmark import load_tx_front_rolling_benchmark  # noqa: E402


def build_reference(reference: dict[str, np.ndarray], daily: pl.DataFrame) -> pl.DataFrame:
    """Replace an expiring front's gross closing mark with its own final fixing."""
    dates = np.asarray(reference['dates'], dtype='datetime64[D]')
    months = np.asarray(reference['contract_months']).astype(str)
    covered = np.asarray(reference['source_covered'])
    previous = np.asarray(reference['prior_same_contract_close'], dtype=np.float64)
    close = np.asarray(reference['front_month_close'], dtype=np.float64)
    rolls = np.asarray(reference['front_month_roll_mask'])
    if (dates.ndim != 1 or not dates.size or np.isnat(dates).any()
            or np.any(dates[1:] <= dates[:-1]) or months.shape != dates.shape
            or any(not value for value in months)
            or covered.shape != dates.shape or covered.dtype != np.bool_ or not covered.all()
            or rolls.shape != dates.shape or rolls.dtype != np.bool_
            or previous.shape != dates.shape or close.shape != dates.shape
            or not np.isfinite(previous).all() or np.any(previous <= 0)
            or not np.isfinite(close).all() or np.any(close <= 0)):
        raise ValueError('TX source must contain covered ordered sessions and own prior marks')
    if len(dates) > 1 and not np.array_equal(rolls[1:], months[1:] != months[:-1]):
        raise ValueError('TX roll identity disagrees with monthly contracts')
    keys = ['date', 'contract']
    scoped = daily.filter((pl.col('product') == 'TX') & ~pl.col('accounting_only_date'))
    if scoped.select(pl.struct(keys).is_duplicated().any()).item():
        raise ValueError('ambiguous TX date and contract source')
    seed = pl.DataFrame({'date': dates, 'contract': months.tolist(),
                         'previous_same_contract_close': previous, 'observed_front_close': close,
                         'front_month_roll': rolls})
    joined = seed.join(scoped.select(
        'date', 'contract', 'official_expiry', 'terminal_event', 'final_settlement_price',
        'observed_close', 'source_row_observed', 'official_row_observed',
    ), on=keys, how='left', validate='1:1')
    invalid = (pl.col('source_row_observed').ne(True).fill_null(True)
               | pl.col('official_row_observed').ne(True).fill_null(True)
               | pl.col('observed_close').is_null()
               | (pl.col('observed_close') != pl.col('observed_front_close')).fill_null(True))
    if joined.filter(invalid).height:
        raise ValueError('TX front reference must match its observed own-contract daily close')
    expires = pl.col('terminal_event') == 'cash_settlement'
    invalid_final = (expires & ((pl.col('official_expiry') != pl.col('date')).fill_null(True)
                     | pl.col('final_settlement_price').is_null()
                     | ~pl.col('final_settlement_price').is_finite().fill_null(False)
                     | (pl.col('final_settlement_price') <= 0).fill_null(True)))
    if joined.filter(invalid_final.fill_null(False)).height:
        raise ValueError('TX cash settlement requires its own dated positive final fixing')
    if joined.filter(pl.col('terminal_event').is_null()).height:
        raise ValueError('TX daily source lacks a terminal event classification')
    return (joined.with_columns(expires.alias('cash_settlement_event'),
             pl.when(expires).then(pl.col('final_settlement_price'))
               .otherwise(pl.col('observed_front_close')).alias('research_pnl_mark'))
            .with_columns((pl.col('research_pnl_mark') / pl.col('previous_same_contract_close'))
                          .log().alias('gross_log_return'))
            .select('date', 'contract', 'front_month_roll', 'previous_same_contract_close',
                    'observed_front_close', 'cash_settlement_event', 'final_settlement_price',
                    'research_pnl_mark', 'gross_log_return'))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', required=True, type=Path)
    parser.add_argument('--daily', required=True, type=Path)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve earlier output; use a new dataset directory')
    frame = pl.scan_parquet(args.daily)
    classifications = (frame.select('date', 'accounting_only_date').unique()
                       .filter(pl.col('date').is_between(pl.lit(args.start).str.to_date(),
                                                         pl.lit(args.end).str.to_date())).collect())
    if (classifications['date'].is_duplicated().any()
            or classifications['accounting_only_date'].null_count()):
        raise ValueError('source calendar has ambiguous account-only classifications')
    dates = (classifications.filter(~pl.col('accounting_only_date')).sort('date')['date']
             .to_numpy().astype('datetime64[D]'))
    daily = (frame.filter(pl.col('product') == 'TX').select(
        'date', 'product', 'contract', 'accounting_only_date', 'official_expiry',
        'terminal_event', 'final_settlement_price', 'observed_close',
        'source_row_observed', 'official_row_observed').collect())
    ref = load_tx_front_rolling_benchmark(args.reference, dates)
    output = build_reference(ref, daily)
    args.output.mkdir(parents=True)
    table = args.output / 'continuous_daily.parquet'
    output.write_parquet(table)
    manifest = {
        'contract': 'tx_front_cash_settlement_mark_research_v1',
        'observed_at_utc': datetime.now(UTC).isoformat(),
        'start_date': str(dates[0]), 'end_date': str(dates[-1]), 'market_sessions': len(dates),
        'cash_settlement_events': output.filter(pl.col('cash_settlement_event')).height,
        'cash_settlement_mark_changed': output.filter(
            pl.col('cash_settlement_event') & (pl.col('research_pnl_mark') != pl.col('observed_front_close'))).height,
        'data_sha256': sha256_file(table),
        'input_sha256': {str(path): sha256_file(path) for path in (args.reference, args.daily, Path(__file__))},
        'research_only': True, 'one_contract_benchmark_admitted': False, 'training_admitted': False,
        'limitations': ['The new front contract prior close is a research proxy, not an executable fill.',
                        'Initial entry clock, fees, tax, funding and non-market account events are not modeled.',
                        'Gross price returns are not a one-contract margin account.',
                        'Input availability is not verified historical PIT.'],
    }
    (args.output / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == '__main__':
    main()
