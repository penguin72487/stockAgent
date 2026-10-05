#!/usr/bin/env python3
"""Audit saved daily returns and exact futures positions without changing a policy."""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from scripts.audit_crypto_training import _Sources, _dates, _finite_array
from scripts.audit_tw_futures_margin_training import audit_root


def dependence(strategy_log_returns, benchmark_log_returns):
    """OLS is descriptive, in simple-return units, with intercept and rf=0."""
    a = _finite_array(strategy_log_returns, 'strategy_log_returns', 1)
    b = _finite_array(benchmark_log_returns, 'benchmark_log_returns', 1)
    if a.shape != b.shape or len(a) < 3:
        raise ValueError('at least three aligned return pairs are required')
    with np.errstate(over='raise', invalid='raise'):
        s, m = np.expm1(a), np.expm1(b)

    def corr(x, y):
        if np.ptp(x) == 0 or np.ptp(y) == 0:
            return None
        xc, yc = x - x.mean(), y - y.mean()
        denominator = np.linalg.norm(xc) * np.linalg.norm(yc)
        return float(np.clip(xc @ yc / denominator, -1., 1.)) if denominator else None

    mc = m - m.mean()
    variance_sum = float(mc @ mc) if np.ptp(m) else 0.
    beta = float(mc @ (s - s.mean()) / variance_sum) if variance_sum else None
    intercept = float(s.mean() - beta * m.mean()) if beta is not None else None
    residual = s - intercept - beta * m if beta is not None else None
    correlation = corr(s, m)
    regimes = {}
    for name, mask in [('benchmark_up', m > 0), ('benchmark_down', m < 0)]:
        regimes[name] = {
            'days': int(mask.sum()),
            'mean_strategy_simple_return': float(s[mask].mean()) if mask.any() else None,
            'mean_benchmark_simple_return': float(m[mask].mean()) if mask.any() else None,
            'strategy_positive_fraction': float((s[mask] > 0).mean()) if mask.any() else None,
        }
    return {
        'rows': len(a),
        'correlation_daily_simple_returns': correlation,
        'correlation_daily_log_returns': corr(a, b),
        'ols_beta_simple_returns_rf0': beta,
        'ols_intercept_daily_simple_rf0': intercept,
        'ols_intercept_annualized_arithmetic_rf0': None if intercept is None else 252 * intercept,
        'ols_r_squared': None if correlation is None else correlation ** 2,
        'ols_residual_daily_std': None if residual is None else float(residual.std()),
        'strategy_cumulative_return': float(np.expm1(a.sum())),
        'benchmark_cumulative_return': float(np.expm1(b.sum())),
        'relative_wealth_return': float(np.expm1((a - b).sum())),
        'same_return_sign_fraction': float((np.sign(s) == np.sign(m)).mean()),
        **regimes,
    }


def aligned_comparison(dates, benchmark_log_returns, other_dates, other_log_returns):
    dates, other_dates = _dates(dates), _dates(other_dates)
    b = _finite_array(benchmark_log_returns, 'benchmark_log_returns', 1)
    a = _finite_array(other_log_returns, 'other_log_returns', 1)
    if len(dates) != len(b) or len(other_dates) != len(a):
        raise ValueError('date and return lengths differ')
    common, left, right = np.intersect1d(dates, other_dates, return_indices=True)
    result = dependence(a[right], b[left])
    return {'start': str(common[0]), 'end': str(common[-1]), **result}


def product_scope(dates, quantities, table):
    """Decode the futures action slots; stock-context symbol names are NOT actions."""
    dates = _dates(dates)
    q = _finite_array(quantities, 'quantities', 2)
    if q.shape[0] != len(dates) or not np.equal(q, np.round(q)).all():
        raise ValueError('invalid exact contract quantities')
    by_date = {str(d): i for i, d in enumerate(dates)}
    observed = np.zeros(q.shape, dtype=bool)
    available, held = set(), {}
    for row in table.to_pylist():
        day = by_date.get(str(row['date']))
        if day is None:
            continue
        slot = int(row['portfolio_slot']) - 1
        if not 0 <= slot < q.shape[1] or observed[day, slot]:
            raise ValueError('invalid or duplicate physical date/slot mapping')
        observed[day, slot] = True
        product = row['product']
        if row['executable']:
            available.add(product)
        if q[day, slot] != 0:
            facts = held.setdefault(product, {'name': row['product_name'], 'long_dates': set(), 'short_dates': set()})
            facts['long_dates' if q[day, slot] > 0 else 'short_dates'].add(str(dates[day]))
    if np.any((q != 0) & ~observed):
        raise ValueError('held physical contract has no date/slot source')
    return {
        'available_products_in_selected_dates': sorted(available),
        'held_products': {key: {'name': value['name'],
                               'long_days': len(value['long_dates']),
                               'short_days': len(value['short_dates'])}
                          for key, value in sorted(held.items())},
        'days_with_long_positions': int(np.any(q > 0, axis=1).sum()),
        'days_with_short_positions': int(np.any(q < 0, axis=1).sum()),
        'active_slot_count_over_period': int(np.any(q != 0, axis=0).sum()),
    }


def analyze(root, fold_id, comparisons):
    root = root.resolve(strict=True)
    canonical = audit_root(root)
    if fold_id not in canonical['selected_folds']:
        raise ValueError('fold is not part of this completed run')
    sources = _Sources(root)
    manifest = sources.json('run_manifest.json')
    fold = f'fold_{fold_id:02d}'
    with np.load(io.BytesIO(sources.read(f'{fold}/test_backtest.npz')), allow_pickle=False) as saved:
        a = {key: saved[key] for key in saved.files}
    dates = _dates(a['dates'])
    b = a['benchmark_returns'].astype(float)
    with np.load(io.BytesIO(sources.read(f'{fold}/futures_benchmark_audit.npz')), allow_pickle=False) as bm:
        np.testing.assert_array_equal(_dates(bm['dates']), dates)
        np.testing.assert_array_equal(bm['benchmark_log_returns'], b)
        if not bm['source_covered'].all():
            raise ValueError('benchmark contains uncovered dates')
    cfg = manifest['configuration']
    path = Path(cfg['trading']['tw_futures_portfolio_data_path']).resolve(strict=True)
    physical = _Sources(path.parent)
    table = pq.read_table(io.BytesIO(physical.read(path.name)),
                          columns=['date', 'portfolio_slot', 'product', 'product_name', 'executable'])
    scope = product_scope(dates, a['futures_contract_quantities_history'], table)
    columns = a['futures_margin_audit_columns'].tolist()
    audit = a['futures_margin_audit'].astype(float)
    field = lambda name: audit[:, columns.index(name)]
    before, opening, after = [field(k) for k in ('equity_before_twd', 'equity_at_open_twd', 'equity_after_mark_twd')]
    overnight = field('overnight_pnl_twd')
    np.testing.assert_allclose(opening - before, overnight, rtol=2e-6, atol=32.)
    total_pnl = float(after[-1] - before[0])
    comparison_results = {}
    comparison_sources = []
    for label, npz_path in comparisons:
        npz_path = npz_path.resolve(strict=True)
        source = _Sources(npz_path.parent)
        with np.load(io.BytesIO(source.read(npz_path.name)), allow_pickle=False) as other:
            comparison_results[label] = {
                'path': str(npz_path),
                **aligned_comparison(dates, b, other['dates'], other['strategy_returns']),
            }
        source.verify()
        comparison_sources.append({'root': str(source.root), 'sha256': source.digests})
    sources.verify()
    physical.verify()
    return {
        'artifact_root': str(root), 'fold_id': fold_id, 'lifecycle_ok': canonical['lifecycle_ok'],
        'start': str(dates[0]), 'end': str(dates[-1]),
        'benchmark_contract': manifest['benchmark_contract'],
        'statistics': dependence(a['strategy_returns'], b),
        'product_scope': scope,
        'pnl_decomposition': {
            'net_pnl_twd': total_pnl,
            'overnight_pnl_twd': float(overnight.sum()),
            'post_open_and_costs_net_pnl_twd': float(total_pnl - overnight.sum()),
            'overnight_fraction_of_net_pnl': float(overnight.sum() / total_pnl) if total_pnl else None,
            'mean_gross_notional_to_open_equity': float(field('gross_notional_to_equity').mean()),
        },
        'initialization_root': cfg['training']['pretrained_initialization_root'],
        'holding_policy': cfg['trading']['tw_futures_portfolio_holding_policy'],
        'loss_type': cfg['training']['loss_type'],
        'same_dates_same_tx_benchmark_comparisons': comparison_results,
        'source_sha256': sources.digests,
        'physical_source': {'root': str(physical.root), 'sha256': physical.digests},
        'comparison_sources': comparison_sources,
        'limitations': [
            'Regression uses realized simple daily returns with an intercept and zero risk-free rate; R squared describes variation, not profit attribution.',
            'The intercept and residuals are in-sample diagnostics, not evidence of tradable future alpha or a hedge backtest.',
            'Overnight plus remaining net PnL is additive TWD accounting; it does not simulate a separately funded intraday policy.',
            'Comparisons use matching observed dates and the same TX benchmark, but models/capital/execution contracts differ.',
            'The 2026 period was inspected in research and is not an untouched holdout.',
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact-root', type=Path, required=True)
    parser.add_argument('--fold', type=int, required=True)
    parser.add_argument('--compare', action='append', default=[], metavar='LABEL=NPZ')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    comparisons = []
    for spec in args.compare:
        label, sep, path = spec.partition('=')
        if not sep or not label or not path or label in {x[0] for x in comparisons}:
            parser.error('--compare requires a unique LABEL=NPZ')
        comparisons.append((label, Path(path)))
    if args.output.resolve().is_relative_to(args.artifact_root.resolve()):
        parser.error('write the audit outside the preserved training root')
    if any(args.output.resolve().is_relative_to(path.resolve().parent.parent) for _, path in comparisons):
        parser.error('write the audit outside the preserved comparison roots')
    result = analyze(args.artifact_root, args.fold, comparisons)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + '\n')
    print(json.dumps({key: result[key] for key in ('statistics', 'product_scope', 'pnl_decomposition')}, indent=2, ensure_ascii=False))
    print(f'Audit: {args.output}')


if __name__ == '__main__':
    main()
