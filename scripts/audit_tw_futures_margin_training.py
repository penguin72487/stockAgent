#!/usr/bin/env python3
"""Read-only audit of canonical margin artifacts; never reset a stitched account."""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

import numpy as np

from scripts.audit_crypto_training import _Sources, analyze_epoch_curve
from stockagent.backtest.report import compute_metrics
from stockagent.backtest.simulator import BacktestResult
from stockagent.data.tw_futures_margin import MARGIN_AUDIT_COLUMNS
from stockagent.data.walkforward import normalize_year_boundary_mode, period_labels_from_contract
from stockagent.training.lifecycle import validate_completed_training_artifacts


def summarize_path(arrays, selection=None):
    dates = np.asarray(arrays['dates']).astype('datetime64[D]')
    if not len(dates) or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError('dates must be nonempty, ordered and unique')
    columns = arrays['futures_margin_audit_columns'].tolist()
    if columns != list(MARGIN_AUDIT_COLUMNS):
        raise ValueError('unknown margin audit column contract')
    if selection is None:
        selection = np.ones(len(dates), dtype=bool)
    dates = dates[selection]
    if not len(dates):
        raise ValueError('empty selected path')
    values = {}
    for key in ('strategy_returns', 'benchmark_returns', 'turnovers',
                'requested_weights_history', 'futures_contract_quantities_history',
                'futures_margin_audit', 'settlement_default'):
        a = np.asarray(arrays[key])
        if len(a) != len(selection) or not np.isfinite(a).all():
            raise ValueError(f'invalid margin path array: {key}')
        values[key] = a[selection]
    r = values['strategy_returns'].astype(np.float64)
    q = values['futures_contract_quantities_history']
    requested = np.abs(values['requested_weights_history']).sum(axis=1)
    audit = values['futures_margin_audit'].astype(np.float64)
    if audit.shape != (len(dates), len(columns)) or q.shape != values['requested_weights_history'].shape:
        raise ValueError('invalid quantity/audit dimensions')
    if not np.equal(q, np.round(q)).all():
        raise ValueError('fractional contracts in exact account')
    active = np.any(q != 0, axis=1)
    gross = audit[:, columns.index('gross_notional_to_equity')]
    before = audit[:, columns.index('equity_before_twd')]
    after = audit[:, columns.index('equity_after_mark_twd')]
    defaults = values['settlement_default'].astype(bool)
    alive = ~np.maximum.accumulate(defaults)
    if np.any(alive & ((before <= 0) | (after <= 0))):
        raise ValueError('nonpositive solvent NAV')
    # Source float32 log1p/marked equity may differ by rounding, not economics.
    implied = np.log(after[alive] / before[alive])
    np.testing.assert_allclose(r[alive], implied, atol=2e-6, rtol=2e-6)
    if len(before) > 1:
        np.testing.assert_allclose(before[1:], np.where(defaults[:-1], 0., after[:-1]),
                                   atol=.05, rtol=2e-6)
    metrics = compute_metrics(BacktestResult(
        strategy_returns=r, benchmark_returns=values['benchmark_returns'],
        turnovers=values['turnovers'], weights_history=np.empty((len(r), 0)),
        execution_mode='tw_stock_context_futures_portfolio',
    ))
    simple = np.expm1(r)
    std = simple.std()
    downside = np.sqrt(np.mean(np.minimum(simple, 0.) ** 2))
    occupied = np.flatnonzero(active)
    trailing_start = int(occupied[-1] + 1) if occupied.size else 0
    return {
        'start': str(dates[0]), 'end': str(dates[-1]), 'rows': len(dates),
        'canonical_metrics_log_return_ratios': metrics,
        'simple_return_sharpe_rf0_252': float(simple.mean() / std * np.sqrt(252)) if std else None,
        'simple_return_sortino_mar0_252': float(simple.mean() / downside * np.sqrt(252)) if downside else None,
        'worst_simple_day': float(simple.min()),
        'start_equity_twd': float(before[0]), 'end_equity_twd': float(after[-1]),
        'active_days': int(active.sum()), 'default_events': int(defaults.sum()),
        'requested_margin_gross_mean': float(requested.mean()),
        'requested_but_no_position_days': int(((requested > 0) & ~active).sum()),
        'trailing_flat_from': str(dates[trailing_start]) if trailing_start < len(dates) else None,
        'gross_notional_to_open_equity_mean': float(gross.mean()),
        'gross_notional_to_open_equity_max': float(gross.max()),
        'gross_notional_to_open_equity_p99': float(np.quantile(gross, .99)),
        'position_observation_note': 'Zero held quantity is not proof of learned cash or account default.',
    }


def audit_root(root: Path):
    root = root.resolve(strict=True)
    sources = _Sources(root)
    manifest, summary = sources.json('run_manifest.json'), sources.json('summary.json')
    config = manifest['configuration']
    groups = ['train_' + '-'.join(map(str, s['train_years'])) for s in summary]
    conformance = validate_completed_training_artifacts(
        root, fold_ids=manifest['selected_fold_ids'], group_names=groups)
    if not conformance.ok:
        raise ValueError(f'incomplete training artifacts: {conformance.missing}, {conformance.invalid}')

    def read_npz(relative):
        with np.load(io.BytesIO(sources.read(relative)), allow_pickle=False) as saved:
            return {key: saved[key] for key in saved.files}

    period_contract = None
    if normalize_year_boundary_mode(config['walk_forward'].get('year_boundary_mode', 'calendar')) == 'lookback_shifted':
        period_contract = sources.json('walkforward_period_boundaries.json')
        if period_contract['offset_sessions'] != config['training']['lookback']:
            raise ValueError('annual period proof differs from the configured lookback')

    def period_years(dates):
        if period_contract is not None:
            return period_labels_from_contract(dates, period_contract)
        return dates.astype('datetime64[Y]').astype(int) + 1970

    stitched = read_npz('walkforward_deployment_backtest.npz')
    years = period_years(stitched['dates'])
    full = summarize_path(stitched)
    yearly = {str(y): summarize_path(stitched, years == y) for y in np.unique(years)}
    folds = []
    for entry, group in zip(summary, groups, strict=True):
        fold_id = entry['fold_id']
        saved = read_npz(f'fold_{fold_id:02d}/test_backtest.npz')
        owned = read_npz(f'fold_{fold_id:02d}/deployment_test_backtest.npz')
        test_years = period_years(saved['dates'])
        epoch_rows = [json.loads(line) for line in sources.read(f'{group}/epoch_curve.jsonl').splitlines() if line]
        folds.append({
            'fold_id': fold_id,
            'validation': entry['val_metrics'],
            'standalone_first_test_year_reset_account': summarize_path(saved, test_years == min(entry['test_years'])),
            'carried_deployment_segment': summarize_path(owned),
            'training': analyze_epoch_curve(epoch_rows,
                epochs_cap=config['training']['epochs'],
                early_stop_ratio=config['training']['early_stopping_no_improve_ratio'],
                grad_clip_norm=config['training']['grad_clip_norm']),
        })
    sources.verify()
    return {
        'artifact_root': str(root), 'lifecycle_ok': conformance.ok,
        'selected_folds': manifest['selected_fold_ids'],
        'loss_type': config['training']['loss_type'],
        'benchmark_contract': manifest['benchmark_contract'],
        'stitched_continuous_account': full, 'stitched_by_year': yearly, 'folds': folds,
        'annual_period_contract': period_contract,
        'source_sha256': sources.digests,
        'limitations': [
            'Standalone fold tests reset capital and overlap; they are not a continuous deployment.',
            'Canonical Sharpe/Sortino use log returns; arithmetic-return companions are explicitly named.',
            'Margin legality is not a risk limit. Daily data does not prove an intraday solvency path.',
            'This audit does not establish the cause of each unfilled request or improved strategy performance.',
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(args.artifact_root.resolve()):
        parser.error('write audit outside the preserved training root')
    result = audit_root(args.artifact_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + '\n')
    print(json.dumps(result['stitched_continuous_account'], indent=2, ensure_ascii=False))
    print(f'Audit: {args.output}')


if __name__ == '__main__':
    main()
