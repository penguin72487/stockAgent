#!/usr/bin/env python3
"""Read-only exact-policy diagnostics; write evidence outside the training root.

Uses complete saved paths, without retraining, changing fills, selecting a new
checkpoint, or representing a resampled return path as an executable backtest.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from dataclasses import asdict
from pathlib import Path

import numpy as np
import torch

from scripts.audit_crypto_training import _Sources
from scripts.audit_tw_futures_margin_training import audit_root
from stockagent.training.exact_policy_step import ExactPolicyStep, PolicyEvaluation


def candidate_reasons(candidate, baseline_loss, budget):
    reasons = []
    loss, drawdown = candidate['loss'], candidate['max_drawdown']
    if loss is None or drawdown is None or not np.isfinite([loss, drawdown]).all():
        return ['nonfinite']
    if candidate['defaulted']:
        reasons.append('default')
    if not -budget <= drawdown <= 0:
        reasons.append('drawdown')
    tolerance = 8 * np.finfo(np.float32).eps * max(1., abs(baseline_loss))
    if loss > baseline_loss + tolerance:
        reasons.append('loss')
    return reasons


def interior_counterexample():
    """Exact smooth risk normal: a shorter inward ray exists but is not tested.

    Feasible set x**2+y**2 <= 1, loss=-y, initial state=(1,0).
    Adam proposes displacement (.1,1). The tangent (0,1) leaves the circle;
    the full reflected step (-.1,1) also leaves it, but its 1/8 step is feasible.
    This proves search incompleteness, not an improvement on the financial run.
    """
    x = torch.nn.Parameter(torch.tensor(1.))
    y = torch.nn.Parameter(torch.tensor(0.))
    model = torch.nn.ParameterList([x, y])
    opt = torch.optim.AdamW([
        {'params': [x], 'lr': .1}, {'params': [y], 'lr': 1.}
    ], weight_decay=0.)
    scheduler = torch.optim.lr_scheduler.LambdaLR(opt, lambda _: 1.)

    def evaluate():
        xv, yv = float(x.detach()), float(y.detach())
        return PolicyEvaluation(-yv, -.2 * (xv * xv + yv * yv), False)

    baseline = evaluate()
    guard = ExactPolicyStep(model, opt, scheduler, baseline, .2)
    x.grad, y.grad = -torch.ones_like(x), -torch.ones_like(y)
    opt.step()
    scheduler.step()
    _, receipt = guard.resolve(
        evaluate, lambda _: [torch.ones_like(x), torch.zeros_like(y)],
        risk_interior=True,
    )
    with torch.no_grad():
        x.fill_(1. - .1 / 8)
        y.fill_(1. / 8)
    missing = evaluate()
    assert missing.feasible(.2) and missing.loss < baseline.loss
    return {
        'current_search_accepted': receipt['accepted'],
        'current_trials': receipt['trials'],
        'shorter_reflected_candidate': asdict(missing),
        'shorter_reflected_fraction': .125,
        'shorter_reflected_feasible_and_improving': True,
        'observed_gap': not receipt['accepted'],
    }


def paired_block_sensitivity(strategy, benchmark, *, draws=10_000, seed=20260928):
    """Paired circular-block percentile sensitivity, not a formal significance test.

    Keep same-date strategy/benchmark pairs and within-block serial dependence.
    These intervals condition on this one selected policy and inspected period;
    they are NOT selection-adjusted, studentized, or future-win probabilities.
    """
    a, b = np.asarray(strategy, dtype=float), np.asarray(benchmark, dtype=float)
    if a.ndim != 1 or a.shape != b.shape or len(a) < 20 or draws < 2:
        raise ValueError('need matched 1-D paths of at least 20 rows and >=2 draws')
    if not np.isfinite([a, b]).all() or min(a.std(), b.std()) <= 0:
        raise ValueError('sensitivity requires finite nonconstant paths')
    rng = np.random.default_rng(seed)
    result = []
    for block in (5, 10, 20):
        starts = rng.integers(0, len(a), size=(draws, (len(a) + block - 1) // block, 1))
        ids = ((starts + np.arange(block)[None, None, :]) % len(a)).reshape(draws, -1)[:, :len(a)]
        aa, bb = a[ids], b[ids]
        if np.any(aa.std(1) == 0) or np.any(bb.std(1) == 0):
            raise ValueError('a resampled path has undefined Sharpe; do not discard it silently')
        excess = (aa - bb).sum(1)
        delta_sharpe = (aa.mean(1) / aa.std(1) - bb.mean(1) / bb.std(1)) * np.sqrt(252)
        result.append({
            'block_rows': block, 'draws': draws, 'seed': seed,
            'relative_wealth_advantage_percentile_95': np.expm1(np.quantile(excess, [.025, .975])).tolist(),
            'log_return_sharpe_delta_percentile_95': np.quantile(delta_sharpe, [.025, .975]).tolist(),
            'resampled_positive_excess_fraction': float((excess > 0).mean()),
        })
    return result


def write_csv(path, records):
    with path.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def plot_diagnostics(output, epochs, monthly, dates, strategy, benchmark, result):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 2, figsize=(13, 9), constrained_layout=True)
    x = [r['epoch'] for r in epochs]
    axes[0, 0].plot(x, [-100 * r['train_policy_max_drawdown'] for r in epochs], label='Accepted training MDD')
    axes[0, 0].axhline(100 * result['stop']['train_policy_drawdown_budget'], color='firebrick', linestyle='--', label='Training benchmark limit')
    axes[0, 0].set(title='Training reaches the observed risk boundary', xlabel='Epoch', ylabel='Maximum drawdown magnitude (%)')
    axes[0, 0].legend(fontsize=8)
    axes[0, 1].plot(x, [r['train_vs_fixed_policy_loss_gap'] for r in epochs])
    axes[0, 1].axhline(0., color='grey', linewidth=.7)
    axes[0, 1].set(title='Same-policy train / evaluation loss gap', xlabel='Epoch', ylabel='Train loss - fixed-policy loss before update')
    relative = 100 * np.expm1((strategy - benchmark).cumsum())
    axes[1, 0].plot(dates, relative, color='teal')
    axes[1, 0].axhline(0., color='grey', linewidth=.7)
    axes[1, 0].set(title='2026 inspected research path', ylabel='Strategy wealth / benchmark wealth - 1 (%)')
    axes[1, 0].tick_params(axis='x', rotation=25)
    positions = np.arange(len(monthly))
    axes[1, 1].bar(positions - .2, [100 * m['strategy_return'] for m in monthly], width=.4, label='v10 net')
    axes[1, 1].bar(positions + .2, [100 * m['benchmark_return'] for m in monthly], width=.4, label='Benchmark gross')
    axes[1, 1].set_xticks(positions, [m['month'][5:] for m in monthly])
    axes[1, 1].set(title='Every observed month (September: 4 sessions)', xlabel='2026 month', ylabel='Within-month cumulative return (%)')
    axes[1, 1].legend(fontsize=8)
    for ax in axes.flat:
        ax.grid(alpha=.2)
    fig.suptitle('Fold 14 v10 refinement audit | complete saved paths, no retraining', fontsize=14)
    fig.savefig(output / 'refinement_diagnostics.png', dpi=150)
    plt.close(fig)


def analyze(root, output, *, draws=10_000):
    root, output = root.resolve(strict=True), output.resolve()
    if output.is_relative_to(root):
        raise ValueError('output must be outside the preserved training root')
    canonical = audit_root(root)
    if len(canonical['selected_folds']) != 1:
        raise ValueError('this focused diagnostic requires one completed fold')
    sources = _Sources(root)
    summary = sources.json('summary.json')[0]
    manifest = sources.json('run_manifest.json')
    fold = f"fold_{summary['fold_id']:02d}"
    group = 'train_' + '-'.join(map(str, summary['train_years']))
    epochs = [json.loads(line) for line in sources.read(f'{group}/epoch_curve.jsonl').splitlines() if line]
    steps = [json.loads(line) for line in sources.read(f'{group}/policy_step_trials.jsonl').splitlines() if line]
    stop = sources.json(f'{group}/optimizer_stop.json')
    with np.load(io.BytesIO(sources.read(f'{fold}/test_backtest.npz')), allow_pickle=False) as saved:
        arrays = {key: saved[key] for key in saved.files}
    a, b = (arrays[k].astype(float) for k in ('strategy_returns', 'benchmark_returns'))
    dates = arrays['dates'].astype('datetime64[D]')
    audit = arrays['futures_margin_audit'].astype(float)
    cols = arrays['futures_margin_audit_columns'].tolist()
    field = lambda name: audit[:, cols.index(name)]
    before, opening, after = (field(k) for k in ('equity_before_twd', 'equity_at_open_twd', 'equity_after_mark_twd'))
    overnight = field('overnight_pnl_twd')
    np.testing.assert_allclose(opening - before, overnight, atol=16., rtol=2e-6)
    quantities, requested = arrays['futures_contract_quantities_history'], arrays['requested_weights_history']
    active_excess = a - b
    monthly = []
    months = dates.astype('datetime64[M]')
    for month in np.unique(months):
        mask = months == month
        monthly.append({'month': str(month), 'rows': int(mask.sum()),
                        'strategy_return': float(np.expm1(a[mask].sum())),
                        'benchmark_return': float(np.expm1(b[mask].sum())),
                        'log_excess': float(active_excess[mask].sum())})
    candidates = []
    for step in steps:
        for i, trial in enumerate(step['trials']):
            reasons = candidate_reasons(trial, step['train_policy_loss_before'], step['train_policy_drawdown_budget'])
            assert trial['accepted'] == (not reasons)
            candidates.append({'epoch': step['epoch'], 'candidate': i, **trial,
                               'rejection_reasons': '+'.join(reasons),
                               'loss_change': trial['loss'] - step['train_policy_loss_before'] if trial['loss'] is not None else None})
    fields = ['epoch', 'train_loss', 'val_mean', 'lr', 'train_policy_loss_before', 'train_policy_loss_after',
              'train_policy_max_drawdown', 'train_policy_step_fraction', 'train_policy_candidate_evaluations',
              'train_policy_step_s', 'epoch_wall_s_max_rank', 'train_total_s_max_rank']
    epoch_rows = [{**{k: row[k] for k in fields},
                   'train_vs_fixed_policy_loss_gap': row['train_loss'] - row['train_policy_loss_before']} for row in epochs]
    ordinary = [row for row in epoch_rows if row['epoch'] >= 3 and row['train_policy_candidate_evaluations'] == 1]
    terminal = [row for row in candidates if row['epoch'] == stop['epoch']]
    result = {
        'artifact_root': str(root), 'fold': summary['fold_id'], 'lifecycle_ok': canonical['lifecycle_ok'],
        'test_start': str(dates[0]), 'test_end': str(dates[-1]), 'test_rows': len(dates),
        'validation': summary['val_metrics'],
        'test': canonical['folds'][0]['standalone_first_test_year_reset_account'],
        'stop': stop, 'terminal_candidates': terminal,
        'training_risk_headroom_percentage_points': 100 * (stop['train_policy_drawdown_budget'] + stop['train_policy_max_drawdown']),
        'return_structure': {
            'correlation_daily_log_returns': float(np.corrcoef(a, b)[0, 1]),
            'ols_beta_daily_log_returns_with_intercept': float(np.cov(a, b, ddof=0)[0, 1] / np.var(b)),
            'daily_volatility_ratio': float(a.std() / b.std()),
            'days_with_positive_contracts': int(np.any(quantities > 0, axis=1).sum()),
            'days_with_negative_contracts': int(np.any(quantities < 0, axis=1).sum()),
            'requested_long_margin_mean': float(np.maximum(requested, 0).sum(1).mean()),
            'requested_short_margin_mean': float(-np.minimum(requested, 0).sum(1).mean()),
            'total_pnl_twd': float(after[-1] - before[0]),
            'overnight_pnl_twd': float(overnight.sum()),
            'open_to_mark_including_costs_pnl_twd': float((after - opening).sum()),
            'pnl_bridge_residual_twd': float((after[-1] - before[0]) - overnight.sum() - (after - opening).sum()),
        },
        'sensitivity': {
            'paired_block_percentiles': paired_block_sensitivity(a, b, draws=draws),
            'leave_one_paired_day_out_loses_return_lead_count': int(((active_excess.sum() - active_excess) < 0).sum()),
            'observed_log_excess': float(active_excess.sum()),
            'observed_relative_wealth_advantage': float(np.expm1(active_excess.sum())),
        },
        'timing': {
            'ordinary_epochs': [r['epoch'] for r in ordinary],
            'ordinary_median_wall_s': float(np.median([r['epoch_wall_s_max_rank'] for r in ordinary])),
            'ordinary_median_train_s': float(np.median([r['train_total_s_max_rank'] for r in ordinary])),
            'ordinary_median_policy_step_s': float(np.median([r['train_policy_step_s'] for r in ordinary])),
            'terminal_epoch_wall_s': epoch_rows[-1]['epoch_wall_s_max_rank'],
            'terminal_policy_step_s': epoch_rows[-1]['train_policy_step_s'],
        },
        'optimizer_counterexample': interior_counterexample(),
        'training_config': {k: manifest['configuration']['training'][k] for k in
                            ('batch_size_train', 'batch_size_eval', 'learning_rate', 'lr_scheduler_warmup_steps',
                             'lr_scheduler_t_max', 'lr_scheduler_eta_min', 'futures_training_max_drawdown')},
        'terminal_train_vs_fixed_policy_loss_gap': epoch_rows[-1]['train_vs_fixed_policy_loss_gap'],
        'limitations': [
            '2026 was already inspected during research; this is a selected-policy diagnosis.',
            'Block percentiles are descriptive, not selection-adjusted/studentized confidence claims or future win probabilities.',
            'Resampling and leave-one-day-out alter saved return paths only, not executable account trajectories.',
            'The smooth counterexample proves candidate search incompleteness, not improved financial performance.',
            'The cause of train versus fixed-policy evaluation loss differences has not been isolated.',
            'Observed correlation and overnight PnL are descriptive, not causal feature attribution.',
        ],
    }
    sources.verify()
    result['source_sha256'] = {**canonical['source_sha256'], **sources.digests}
    repo = Path(__file__).resolve().parents[1]
    result['analysis_code_sha256'] = {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest() for p in
        (Path(__file__).resolve(), repo / 'stockagent/training/exact_policy_step.py')}
    output.mkdir(parents=True, exist_ok=True)
    (output / 'analysis.json').write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + '\n')
    write_csv(output / 'monthly_diagnostics.csv', monthly)
    write_csv(output / 'epoch_diagnostics.csv', epoch_rows)
    write_csv(output / 'candidate_trials.csv', candidates)
    daily = [{'date': str(d), 'strategy_log_return': float(s), 'benchmark_log_return': float(m),
              'log_excess': float(s - m), 'gross_notional_to_equity': float(gross), 'overnight_pnl_twd': float(overn)}
             for d, s, m, gross, overn in zip(dates, a, b, field('gross_notional_to_equity'), overnight, strict=True)]
    write_csv(output / 'daily_diagnostics.csv', daily)
    plot_diagnostics(output, epoch_rows, monthly, dates, a, b, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact-root', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.artifact_root, args.output_dir)
    print(json.dumps({key: result[key] for key in ('training_risk_headroom_percentage_points', 'return_structure', 'timing',
                                                 'terminal_train_vs_fixed_policy_loss_gap')}, indent=2))
    print(f'Evidence: {args.output_dir / "analysis.json"}')


if __name__ == '__main__':
    main()
