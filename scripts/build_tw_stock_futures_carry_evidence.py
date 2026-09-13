"""Audit unobserved continuation days against original official daily archives.

Reuses the production physical calendar and zero-outright-volume proof builder.
It cannot invent minute prices or resolve positive-volume observations.
"""
import argparse
import json
from pathlib import Path

import polars as pl
import numpy as np

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.config import load_config
from stockagent.data.panel_cache import load_panel_cache_v2
from stockagent.data.tw_stock_futures_carry import carry_contract_rows, load_final_settlements
from stockagent.data.tw_stock_futures_day_trade import select_causal_front_stock_futures_candidates
from stockagent.data.tw_stock_futures_repair import official_day_evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--official-manifest', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise FileExistsError('use a fresh evidence directory; existing proofs are immutable')
    c = load_config(args.config)
    daily = Path(c.trading.tw_stock_futures_day_trade_data_path)
    manifest = json.loads(daily.with_name('manifest.json').read_text())
    if sha256_file(daily) != manifest['outputs']['continuous_daily']['sha256']:
        raise ValueError('carry daily source SHA mismatch')
    cache = load_panel_cache_v2(c.data.panel_cache_root)
    dates = cache['dates'].astype('datetime64[D]')
    dates = dates[dates >= np.datetime64(c.data.panel_start_date)]
    source = pl.read_parquet(daily).filter(pl.col('asset_class') == 'stock_future')
    selected = select_causal_front_stock_futures_candidates(source)
    rows, _ = carry_contract_rows(source, selected, dates, tuple(cache['symbols']),
        load_final_settlements(c.trading.tw_futures_portfolio_final_settlement_path),
        quarantine_contract_days=c.trading.tw_stock_futures_day_trade_quarantine_contract_days)
    coverage_path = Path(c.trading.tw_stock_futures_day_trade_minute_data_path).with_name('coverage.parquet')
    coverage = pl.read_parquet(coverage_path)
    # Valuation and executable volume are independent facts: a zero-outright
    # day can still have an official settlement. Audit every physical calendar
    # row; acceptance of extra zero-volume proof still never replaces coverage.
    gaps = rows.select('date', 'physical_contract').join(coverage.select('date', 'physical_contract'),
        on=['date', 'physical_contract'], how='anti')
    proof = official_day_evidence(args.official_manifest, rows.select('date', 'physical_contract'), args.output_dir)
    proof = (proof.join(gaps.with_columns(pl.lit(True).alias('no_trade_proof_eligible')),
                       on=['date', 'physical_contract'], how='left', validate='1:1')
             .with_columns(pl.col('no_trade_proof_eligible').fill_null(False)))
    atomic_write_parquet(args.output_dir/'official_evidence.parquet', proof)
    receipt_path = args.output_dir/'official_evidence_manifest.json'
    receipt = json.loads(receipt_path.read_text())
    receipt.update(sha256=sha256_file(args.output_dir/'official_evidence.parquet'),
                   settlement_rule='reported_same_day_same_physical_official_settlement_only_never_a_fill',
                   no_trade_proof_scope='only_absent_original_coverage_keys')
    atomic_write_json(receipt_path, receipt)
    absent_proof = proof.filter(pl.col('no_trade_proof_eligible'))
    unresolved = absent_proof.filter(pl.col('outright_volume').is_null() | (pl.col('outright_volume') != 0))
    atomic_write_parquet(args.output_dir/'unresolved.parquet', unresolved)
    summary = dict(required_contract_days=rows.height, absent_coverage_contract_days=gaps.height,
        proven_zero_outright_contract_days=absent_proof.filter(pl.col('outright_volume') == 0).height,
        unresolved_contract_days=unresolved.height, daily_sha256=sha256_file(daily),
        coverage_sha256=sha256_file(coverage_path), config=args.config,
        source_rule='all_physical_continuations_no_model_or_test_based_selection')
    atomic_write_json(args.output_dir/'build_summary.json', summary)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
