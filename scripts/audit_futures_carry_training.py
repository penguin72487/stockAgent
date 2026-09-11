"""Read-only diagnosis of saved carry curves and source gaps; never reselect a model."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.config import load_config
from stockagent.data.panel_cache import load_panel_cache_v2
from stockagent.data.tw_stock_futures_carry import (
    carry_contract_rows, load_final_settlements, load_carry_no_trade_evidence, load_carry_corporate_actions,
    load_carry_daily_settlements, apply_carry_daily_settlements,
)
from stockagent.data.tw_stock_futures_day_trade import select_causal_front_stock_futures_candidates
from stockagent.data.tw_stock_futures_minute import validate_futures_minute_data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise FileExistsError('use a fresh audit directory')
    c = load_config(args.config)
    root = args.output_dir; root.mkdir(parents=True)
    report = {'run_dir': str(args.run_dir), 'config': args.config,
              'configured_epochs': c.training.epochs, 'groups': {}, 'folds': []}
    run_manifest_path = args.run_dir/'run_manifest.json'
    run_manifest = json.loads(run_manifest_path.read_text()) if run_manifest_path.exists() else {}
    report['saved_mode_details'] = run_manifest.get('mode_details', {})
    curve_rows = []
    for path in sorted(args.run_dir.glob('train_*/epoch_curve.jsonl')):
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        invalid = [r for r in rows if r.get('train_first_settlement_default_reason', 0) in (1, 2, 3, 5, 6)]
        report['groups'][path.parent.name] = {
            'epochs_recorded': len(rows), 'data_invalid_epochs': len(invalid),
            'optimizer_steps_on_invalid_data': sum(r.get('train_optimizer_steps', 0) for r in invalid),
            'zero_gradient_epochs': sum(r.get('train_zero_grad_batches', 0) > 0 for r in rows),
            'last_train_loss': rows[-1]['train_loss'],
            'gradient_norm_range': [min(r['train_grad_norm_before_clip_mean'] for r in rows),
                                    max(r['train_grad_norm_before_clip_mean'] for r in rows)],
            'epoch_curve_sha256': sha256_file(path),
        }
        for row in rows:
            reason = row.get('train_first_settlement_default_reason', 0)
            curve_rows.append({
                'group': path.parent.name, 'epoch': row['epoch'], 'raw_train_loss': row['train_loss'],
                'val_loss': row['val_mean'], 'sampled_test_loss': row.get('test_mean'),
                'data_valid': reason not in (1, 2, 3, 5, 6), 'reason': reason,
                'optimizer_steps': row.get('train_optimizer_steps', 0),
                'gradient_norm': row.get('train_grad_norm_before_clip_mean'),
            })
    with (root/'curves.csv').open('w') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(curve_rows[0]))
        writer.writeheader(); writer.writerows(curve_rows)
    for path in sorted(args.run_dir.glob('fold_*/execution_status.json')):
        status = json.loads(path.read_text())
        a = np.load(path.with_name('test_integer_share_backtest.npz'))
        first = status['first_failure_row']; r = a['strategy_returns'].astype(np.float64)
        invalid = bool(np.isin(a['default_reason_history'], [1, 2, 3, 5, 6]).any())
        record = {'fold': path.parent.name, **status,
                  'stored_raw_cumulative_return': float(np.expm1(r.sum())),
                  'financial_full_period_return': None if invalid else float(np.expm1(r.sum()))}
        if first is not None:
            record.update(last_valid_date=str(a['dates'][first-1])[:10] if first else None,
                          observed_prefix_return=float(np.expm1(r[:first].sum())),
                          retained_equity_scale=float(a['equity_scale_history'][first]),
                          invalid_log_marker=float(r[first]))
        report['folds'].append(record)

    daily = Path(c.trading.tw_stock_futures_day_trade_data_path)
    receipt = json.loads(daily.with_name('manifest.json').read_text())
    digest = sha256_file(daily)
    if digest != receipt['outputs']['continuous_daily']['sha256']:
        raise ValueError('daily SHA mismatch')
    minutes, _ = validate_futures_minute_data(c.trading.tw_stock_futures_day_trade_minute_data_path,
        daily_sha256=digest, daily_proxy_before=c.trading.tw_stock_futures_day_trade_daily_proxy_before,
        participation=c.trading.max_volume_participation,
        capacity_rounding=c.trading.tw_stock_futures_day_trade_minute_capacity_rounding,
        quarantine_contract_days=c.trading.tw_stock_futures_day_trade_quarantine_contract_days)
    source = pl.read_parquet(daily).filter(pl.col('asset_class') == 'stock_future')
    cache = load_panel_cache_v2(c.data.panel_cache_root)
    dates = cache['dates'].astype('datetime64[D]')
    dates = dates[dates >= np.datetime64(c.data.panel_start_date)]
    rows, _ = carry_contract_rows(source, select_causal_front_stock_futures_candidates(source),
        dates, tuple(cache['symbols']), load_final_settlements(c.trading.tw_futures_portfolio_final_settlement_path),
        quarantine_contract_days=c.trading.tw_stock_futures_day_trade_quarantine_contract_days)
    coverage_path = Path(c.trading.tw_stock_futures_day_trade_minute_data_path).with_name('coverage.parquet')
    coverage = pl.read_parquet(coverage_path).select('date', 'physical_contract', 'status')
    if c.trading.tw_stock_futures_day_trade_carry_evidence_path:
        from stockagent.data.tw_stock_futures_carry import add_carry_no_trade_evidence
        coverage = add_carry_no_trade_evidence(coverage, minutes,
            load_carry_no_trade_evidence(c.trading.tw_stock_futures_day_trade_carry_evidence_path))
    evidence_path = c.trading.tw_stock_futures_day_trade_carry_evidence_path
    rows = apply_carry_daily_settlements(rows, load_carry_daily_settlements(evidence_path) if evidence_path else None)
    settlements = load_final_settlements(c.trading.tw_futures_portfolio_final_settlement_path)
    events = load_carry_corporate_actions(c.trading.tw_stock_futures_day_trade_corporate_action_path, dates)
    audit = (rows.join(coverage, on=['date', 'physical_contract'], how='left')
             .join(settlements, on=['date', 'physical_contract'], how='left')
             .join(events, on=['date', 'underlying_symbol'], how='left').with_columns(
        pl.col('status').is_in(['minute_verified', 'official_no_outright_trades']).fill_null(False).alias('minute_verified'),
        pl.when(pl.col('date') == pl.col('official_expiry'))
          .then(pl.col('final_settlement_price')).otherwise(pl.col('valuation_settlement')).alias('required_mark'),
    ).with_columns((pl.col('required_mark').is_finite() & (pl.col('required_mark') > 0)).fill_null(False).alias('mark_available')))
    gaps = audit.filter(~pl.col('minute_verified') | ~pl.col('mark_available')
                        | pl.col('unresolved_transition').fill_null(False)).sort('date', 'physical_contract')
    atomic_write_parquet(root/'possible_continuation_gaps.parquet', gaps)
    report['coverage'] = {
        'scope': 'potential_physical_continuations_not_strategy_exclusions_or_proven_reachable_inventory',
        'required_contract_days': audit.height,
        'unknown_minute_contract_days': audit.filter(~pl.col('minute_verified')).height,
        'missing_mark_contract_days': audit.filter(~pl.col('mark_available')).height,
        'union_gap_contract_days': gaps.height,
        'unsupported_corporate_transition_contract_days': audit.filter(pl.col('unresolved_transition').fill_null(False)).height,
        'source_observed_false_but_marked': audit.filter(~pl.col('source_row_observed').fill_null(False)
            & pl.col('official_daily_settlement').is_null()
            & pl.col('valuation_settlement').is_finite() & (pl.col('valuation_settlement') > 0)).height,
        'daily_sha256': digest, 'coverage_sha256': sha256_file(coverage_path),
        'official_daily_settlement_contract_days': audit.filter(pl.col('official_daily_settlement').is_not_null()).height,
        'carry_evidence_sha256': sha256_file(Path(evidence_path)) if evidence_path else None,
    }
    crossings = []
    for path in sorted(args.run_dir.glob('fold_*/execution_status.json')):
        a = np.load(path.with_name('test_integer_share_backtest.npz'))
        ds = a['dates'].astype('datetime64[D]')
        last = json.loads(path.read_text())['first_failure_row']
        last = len(ds) if last is None else last
        symbols = json.loads(path.with_name('deployment_test_symbols.json').read_text())
        index = {symbol: i for i, symbol in enumerate(symbols)}
        residuals = a['futures_residual_contract_quantities_history']
        for event in events.iter_rows(named=True):
            symbol = event['underlying_symbol']
            if symbol not in index:
                continue
            day = np.datetime64(event['date'])
            i = int(np.searchsorted(ds, day))
            if not 0 < i < last or ds[i] != day:
                continue
            qty = residuals[i-1, index[symbol]]
            if np.any(qty):
                crossings.append({'fold': path.parent.name, 'date': str(day), 'symbol': symbol,
                    'event': 'unsupported_transition' if event['unresolved_transition'] else 'pure_cash',
                    'old_signed_quantities': qty.tolist(),
                    'cash_per_share': event['cash_adjustment_per_share']})
    atomic_write_json(root/'held_corporate_events.json', crossings)
    report['observed_held_corporate_events'] = {
        key: sum(r['event'] == key for r in crossings) for key in ('pure_cash', 'unsupported_transition')}
    report['corporate_event_warning'] = (
        'Pure cash adjustments are enabled; unsupported old-position transitions remain errors. Daily mark fallback remains a research approximation.'
        if report['saved_mode_details'].get('cash_dividend_rule') else
        'Price-only prefixes also omit corporate adjustments; these are stored observations, not repaired clearing performance.')
    atomic_write_json(root/'audit.json', report)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(len(report['groups']), 2, figsize=(12, 3.8*len(report['groups'])), squeeze=False)
    for row_axes, group in zip(axes, report['groups']):
        series = [r for r in curve_rows if r['group'] == group]
        epoch = np.array([r['epoch'] for r in series]); loss = np.array([r['raw_train_loss'] for r in series])
        valid = np.array([r['data_valid'] for r in series])
        row_axes[0].plot(epoch, loss, color='0.65', lw=.8, label='Stored train loss')
        if (~valid).any():
            row_axes[0].scatter(epoch[~valid], loss[~valid], s=9, c='crimson', label='Data-invalid trajectory')
        row_axes[0].set_title(group + ': original unsmoothed evidence')
        for key, label in [('val_loss', 'Validation'), ('sampled_test_loss', 'Sampled first test year')]:
            row_axes[1].plot(epoch, [r[key] for r in series], lw=1, label=label)
        row_axes[1].set_title('Different windows; test is audit-only')
        for ax in row_axes:
            ax.set_xlabel('Epoch'); ax.set_ylabel('Annualized mean log-return loss')
            ax.grid(alpha=.2); ax.legend(fontsize=8)
    fig.tight_layout(); fig.savefig(root/'curves.png', dpi=160); plt.close(fig)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
