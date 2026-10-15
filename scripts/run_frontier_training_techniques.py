#!/usr/bin/env python3
"""Run a bounded optimizer screen using canonical complete-fold trials."""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.runtime_identity import verify_source_release, verify_release_bundles


def pin_screen_scheduler(baseline: dict) -> dict:
    """Shortening a diagnostic must not shorten the formal LR schedule."""
    result = deepcopy(baseline)
    training = result['training']
    if not training.get('day_trade_optimizer_step_per_trajectory'):
        raise ValueError('this screen requires the selected full-trajectory baseline')
    if int(training.get('lr_scheduler_t_max', 0)) <= 0:
        training['lr_scheduler_t_max'] = int(training['epochs'])
    training['optimizer_name'] = 'adamw'
    training['optimizer_options'] = {}
    return result


def collect_trial_result(path: Path, name: str, returncode: int, elapsed: float) -> dict:
    row = {'name': name, 'returncode': returncode, 'driver_elapsed_s': elapsed,
           'output': str(path), 'accepted': False}
    results = sorted(path.glob('full-fold/batch_*/result.json'))
    if not results:
        row['failure'] = 'complete-fold result is absent; inspect driver.log'
        return row
    result_path = results[0]
    result = json.loads(result_path.read_text())
    row.update(result=str(result_path), result_sha256=hashlib.sha256(result_path.read_bytes()).hexdigest(),
               accepted=returncode == 0 and bool(result.get('ok')) and bool(result.get('lifecycle_ok')),
               lifecycle_ok=result.get('lifecycle_ok'), reasons=result.get('reasons'),
               median_epoch_max_rank_s=result.get('median_epoch_wall_s'),
               complete_fold_wall_s=result.get('complete_fold_wall_s'), memory=result.get('memory'))
    curve = Path(result.get('epoch_curve', '/nonexistent'))
    records = [json.loads(line) for line in curve.read_text().splitlines() if line.strip()] if curve.is_file() else []
    if records:
        row.update(epoch_curve=str(curve), epoch_count=len(records),
                   last_train_loss=records[-1].get('train_loss'), last_val_loss=records[-1].get('val_mean'),
                   best_val_loss=min(float(r['val_mean']) for r in records if r.get('val_mean') is not None),
                   optimizer_steps=[r.get('train_optimizer_steps') for r in records],
                   gradient_norms=[r.get('train_grad_norm_before_clip_mean') for r in records],
                   lr=[r.get('lr') for r in records])
        steady = [r['epoch_max_rank_s'] for r in records if r.get('epoch', 0) >= 3 and 'epoch_max_rank_s' in r]
        row['observed_median_epoch_max_rank_s'] = statistics.median(steady) if steady else None
    return row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-receipt', type=Path, required=True)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('--accepted-launch', type=Path, required=True)
    parser.add_argument('--dependency-root', type=Path, required=True)
    parser.add_argument('--optimization-recipe', type=Path)
    parser.add_argument('--only', help='comma-separated declared trial names')
    args = parser.parse_args()
    # Children run from the frozen source directory, which differs from the
    # operator's cwd. Bind every operational path before changing child cwd.
    for key in ('source_receipt', 'plan', 'output_root', 'accepted_launch',
                'dependency_root', 'optimization_recipe'):
        value = getattr(args, key)
        if value is not None:
            setattr(args, key, value.resolve())
    proof = verify_source_release(args.source_receipt.resolve(), ROOT)
    verify_release_bundles(args.source_receipt.resolve())
    plan = yaml.safe_load(args.plan.read_text())
    if plan.get('contract') != 'frontier_optimizer_screen_v1' or int(plan['epochs']) < 4:
        raise ValueError('invalid optimizer screen plan')
    baseline_path = Path(plan['baseline_config'])
    if not baseline_path.is_absolute():
        baseline_path = Path('/root/stockAgent') / baseline_path
    baseline = yaml.safe_load(baseline_path.read_text())
    launch = json.loads(args.accepted_launch.read_text())
    args.output_root.mkdir(parents=True, exist_ok=False)
    preparation = args.output_root / 'input'
    preparation.mkdir()
    prepared = pin_screen_scheduler(baseline)
    prepared_path = preparation / 'baseline.yaml'
    prepared_path.write_text(yaml.safe_dump(prepared, sort_keys=False))
    atomic_write_json(preparation / 'runtime-launch.json', launch)
    recipe_path = args.optimization_recipe or ROOT / plan['optimization_recipe']
    recipe = yaml.safe_load(recipe_path.read_text())['full_fold_candidates'][0]
    runtime_path = preparation / 'runtime.yaml'
    runtime_path.write_text(yaml.safe_dump(recipe['runtime_overrides']))
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join([str(args.dependency_root.resolve()), str(ROOT), env.get('PYTHONPATH', '')])
    from scripts.run_relocated_training import renew_original_source_leases
    renew_original_source_leases(launch)
    selected = set(args.only.split(',')) if args.only else {t['name'] for t in plan['trials']}
    unknown = selected - {t['name'] for t in plan['trials']}
    if unknown:
        raise ValueError(f'unknown screen trials: {sorted(unknown)}')
    receipt = {'contract': plan['contract'], 'source_sha256': proof['source_sha256'],
               'plan_sha256': hashlib.sha256(args.plan.read_bytes()).hexdigest(),
               'optimization_recipe_sha256': hashlib.sha256(recipe_path.read_bytes()).hexdigest(),
               'baseline_config': str(baseline_path), 'baseline_config_sha256': hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
               'epochs': plan['epochs'], 'fold': plan['fold'],
               'formal_scheduler_horizon': prepared['training']['lr_scheduler_t_max'],
               'formal_warmup_steps': prepared['training']['lr_scheduler_warmup_steps'],
               'scope': 'bounded complete-fold optimizer mechanism and cost screen; convergence unresolved',
               'selection_uses_test_metrics': False, 'trials': [], 'complete': False}
    atomic_write_json(args.output_root / 'screen.json', receipt)
    for trial in plan['trials']:
        if trial['name'] not in selected:
            continue
        overrides_path = preparation / (trial['name'] + '.yaml')
        overrides_path.write_text(yaml.safe_dump(trial['overrides'], sort_keys=False))
        output = args.output_root / trial['name']
        command = [sys.executable, str(ROOT / 'scripts/run_frontier_training_trial.py'),
                   '--code-root', str(ROOT), '--source-receipt', str(args.source_receipt.resolve()),
                   '--baseline-config', str(prepared_path), '--overrides', str(overrides_path),
                   '--runtime-overrides', str(runtime_path), '--output', str(output), '--name', trial['name'],
                   '--epochs', str(plan['epochs']), '--fold', str(plan['fold']),
                   '--batch-size', str(prepared['training']['batch_size_train']), '--timeout-s', '5400']
        print(json.dumps({'phase': 'starting', 'trial': trial['name'], 'epochs': plan['epochs']}), flush=True)
        started = time.perf_counter()
        with (args.output_root / (trial['name'] + '-launcher.log')).open('w') as log:
            completed = subprocess.run(command, env=env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        row = collect_trial_result(output, trial['name'], completed.returncode, time.perf_counter() - started)
        receipt['trials'].append(row)
        atomic_write_json(args.output_root / 'screen.json', receipt)
        print(json.dumps({'phase': 'finished', **row}), flush=True)
    receipt['complete'] = True
    receipt['all_trials_accepted'] = all(t['accepted'] for t in receipt['trials'])
    atomic_write_json(args.output_root / 'screen.json', receipt)
    return 0 if receipt['all_trials_accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
