#!/usr/bin/env python3
"""Read-only calendar checks using the accepted release's canonical trainer.

This does not build a panel, fit a model, allocate CUDA tensors or execute a
portfolio. Only metadata are passed to the existing annual-boundary validator.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace


def calendar_for_config(config, dates):
    """Apply the canonical entry adapter's saved horizon to metadata dates.

    Futures retain earlier cash-stock feature context on disk, while their
    entry adapter crops the model panel to the verified paired-clock horizon.
    Do not assign validation dates from that uncropped feature inventory.
    """
    import numpy as np
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_decision_clock import validate_clock_source

    values = np.asarray(dates, dtype='datetime64[D]')
    entry = getattr(config.trading, 'tw_futures_portfolio_entry_clock_path', None)
    if not entry:
        return values, {}
    daily = config.trading.tw_futures_portfolio_data_path
    rules = config.trading.tw_futures_portfolio_margin_rules_path
    _, manifest = validate_clock_source(entry, daily_path=daily, rules_path=rules,
        decision_time=config.trading.tw_futures_portfolio_decision_time)
    start, end = np.datetime64(manifest['start_date']), np.datetime64(manifest['end_date'])
    selected = values[(values >= start) & (values <= end)]
    if not selected.size or selected[0] != start or selected[-1] != end:
        raise ValueError('feature calendar does not cover the verified entry-clock horizon')
    path = Path(entry).with_name('manifest.json')
    return selected, {'entry_clock_manifest': str(path), 'sha256': sha256_file(path),
        'pair_identity': manifest['pair_identity'], 'source_context_sessions': len(values),
        'model_panel_sessions': len(selected)}


def assess_calendar(config, dates, *, build_folds, split_indices, validate_annual) -> dict:
    import numpy as np

    panel = SimpleNamespace(dates=np.asarray(dates, dtype='datetime64[D]'))
    if len(panel.dates) == 0 or np.any(np.diff(panel.dates) <= np.timedelta64(0, 'D')):
        raise ValueError('feature calendar must be nonempty, unique and increasing')
    w = config.walk_forward
    from stockagent.data.walkforward import year_boundary_offset_sessions, year_period_contract
    offset = year_boundary_offset_sessions(config)
    folds = build_folds(panel.dates, min_train_years=w.min_train_years,
                        val_years=w.val_years, require_future_test_year=w.require_future_test_year,
                        split_start_year=w.split_start_year, year_boundary_offset_sessions=offset)
    selected = [folds[-1]]
    if int(config.runner.start_fold) != selected[0].fold_id:
        raise ValueError(f'configured start_fold={config.runner.start_fold} is not the last '
                         f'canonical fold={selected[0].fold_id}')
    owned = np.sort(selected[0].train_indices)
    decisions = split_indices(panel, owned, config.training.lookback,
                              config.trading.execution_mode, w.lookback_context)
    missing = np.setdiff1d(owned, decisions)
    report = {'fold_ids': [selected[0].fold_id], 'fold_policy': 'last', 'lookback': config.training.lookback,
              'canonical_fold_count': len(folds), 'year_boundary_offset_sessions': offset,
              'period_contract': year_period_contract(panel.dates, offset),
              'train_years': selected[0].train_years, 'val_years': selected[0].val_years,
              'test_years': selected[0].test_years,
              'annual_reset': config.training.day_trade_training_annual_episodes,
              'context_sessions_before_first_target': int(owned[0]),
              'first_target': str(panel.dates[owned[0]]),
              'first_valid_target': str(panel.dates[decisions[0]]) if decisions.size else None,
              'missing_target_dates': [str(panel.dates[i]) for i in missing]}
    report['actual_intervals'] = {
        name: {'first': str(panel.dates[indices[0]]), 'last': str(panel.dates[indices[-1]]),
               'sessions': len(indices)}
        for name, indices in [('train', selected[0].train_indices),
                              ('val', selected[0].val_indices), ('test', selected[0].test_indices)]}
    try:
        report['annual_account_validation'] = validate_annual(panel, selected, config)
    except ValueError as exc:
        report.update(state='blocked_calendar_context', error=str(exc))
    else:
        # Even a non-reset comparison must not silently change target coverage.
        report['state'] = 'blocked_calendar_context' if missing.size else 'accepted_calendar_context'
        if missing.size:
            report['error'] = 'lookback omits owned training sessions; extend verified context first'
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--code-root', required=True, type=Path)
    parser.add_argument('--config-root', required=True, type=Path)
    parser.add_argument('--baseline-config', required=True, type=Path)
    parser.add_argument('--only', required=True)
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    # The script starts in its control checkout, but imports all authoritative
    # config/fold/trainer behavior from the explicitly pinned release.
    sys.path.insert(0, str(args.code_root.resolve()))
    from stockagent.config import load_config
    from stockagent.data.walkforward import build_expanding_year_folds
    from stockagent.training.trainer import (
        _split_valid_indices, _validate_annual_day_trade_training_boundaries,
    )

    baseline = load_config(args.baseline_config)
    manifest_path = Path(baseline.data.factorized_feature_manifest)
    manifest = json.loads(manifest_path.read_text())
    cases = {}
    for name in args.only.split(','):
        if not name or Path(name).name != name:
            raise ValueError('invalid selected experiment ID')
        config = load_config(args.config_root / f'{name}.yaml')
        if asdict(config.data) != asdict(baseline.data):
            raise ValueError('selected configuration changed the accepted source')
        dates, source_clock = calendar_for_config(config, manifest['dates'])
        cases[name] = assess_calendar(config, dates,
            build_folds=build_expanding_year_folds, split_indices=_split_valid_indices,
            validate_annual=_validate_annual_day_trade_training_boundaries)
        if source_clock:
            cases[name]['source_clock'] = source_clock
    blocked = [name for name, case in cases.items() if case['state'].startswith('blocked')]
    report = {'contract': 'selected_last_fold_shifted_calendar_preflight_v3',
              'state': 'blocked_calendar_context' if blocked else 'accepted_calendar_context',
              'blocked_cases': blocked, 'cases': cases,
              'old_calendar_baseline_comparable': False,
              'feature_manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
              'canonical_code_root': str(args.code_root), 'formal_training_started': False,
              'cuda_ddp_or_vram_verified': False}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'state': report['state'], 'blocked_cases': blocked,
                      'report': str(args.report), 'formal_training_started': False}, ensure_ascii=False),
          flush=True)
    return 2 if blocked else 0


if __name__ == '__main__':
    raise SystemExit(main())
