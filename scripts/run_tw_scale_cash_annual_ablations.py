#!/usr/bin/env python3
"""Select each OFAT case's last fold; delegate to the pinned canonical runner.

No trainer, model, checkpoint or accounting implementation lives in this bridge.
The original annual launch validates the baseline's source/data readiness only;
its measured speed/VRAM acceptance is NOT extended to ablation variants.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_ablation_experiments import _experiment_rows, _load_ablation_spec

SPEC = ROOT / 'configs/ablations/tw_scale_cash_annual_last_fold_selected_20261008_v5.yaml'
LAUNCH = ROOT / ('artifacts/operations/training_launches/'
                 'tw_day_trade_factorized_values_20261007_no_basis_scale_separated_cash_annual_v1/'
                 'runtime-launch.json')
RUNNER = ROOT / 'scripts/run_tw_scale_cash_annual_ablation_runner.sh'
SOURCE = ROOT / ('artifacts/operations/training_launches/'
                 'tw_scale_cash_annual_last_fold_selected_20261008_v5/source-release.json')
REPORT_SOURCE = SOURCE.parent / 'reporting-source-v1.json'
REPAIR_SPEC = ROOT / 'configs/ablations/tw_scale_cash_annual_last_fold_selected_20261008_v6.yaml'
NUMERICAL_SOURCE = ROOT / ('artifacts/operations/training_launches/'
                         'tw_scale_cash_annual_last_fold_selected_20261008_v6/numerical-source-v2.json')


def _shifted_source(launch: dict, binding_path: Path) -> tuple[dict, Path]:
    """Verify a narrow versioned amendment, never mutate the accepted release."""
    from stockagent.runtime_identity import verify_release_bundles, verify_source_release
    binding = json.loads(binding_path.read_text())
    if binding.get('contract') != 'tw_last_fold_source_v1':
        raise ValueError('unsupported shifted annual source binding')
    if binding['parent_source_sha256'] != launch['source_sha256']:
        raise ValueError('shifted annual source has a different accepted parent')
    receipt = Path(binding['source_receipt'])
    code = receipt.parent / 'build-source'
    observed = verify_source_release(receipt, code)
    verify_release_bundles(receipt)
    if observed['source_sha256'] != binding['source_sha256']:
        raise ValueError('shifted annual source identity differs from its binding')
    parent = json.loads(Path(launch['source_receipt']).read_text())['code']['files']
    child = json.loads(receipt.read_text())['code']['files']
    changed = sorted(name for name in set(parent) | set(child) if parent.get(name) != child.get(name))
    allowed = sorted(['stockagent/training/checkpoint_contract.py',
                      'stockagent/training/day_trade_carry_bridge.py',
                      'stockagent/training/trainer.py', 'scripts/run_ablation_experiments.py'])
    if changed != allowed:
        raise ValueError(f'shifted annual amendment changed unapproved source files: {changed}')
    return binding, code


def _selection(raw: str | None) -> set[str] | None:
    if raw is None:
        return None
    items = [part.strip() for part in raw.split(',')]
    if not items or any(not part for part in items):
        raise ValueError('--only requires non-empty comma-separated experiment IDs')
    return set(items)


def _reporting_source(training_source: dict, binding_path: Path) -> tuple[dict, Path]:
    """Only reporting/orchestration may differ; optimizer source remains pinned."""
    from stockagent.runtime_identity import verify_release_bundles, verify_source_release
    binding = json.loads(binding_path.read_text())
    if binding.get('contract') != 'tw_ablation_reporting_source_v1':
        raise ValueError('unsupported ablation reporting source binding')
    if binding['parent_source_sha256'] != training_source['source_sha256']:
        raise ValueError('reporting source has a different training parent')
    receipt = Path(binding['source_receipt'])
    code = receipt.parent / 'build-source'
    observed = verify_source_release(receipt, code)
    verify_release_bundles(receipt)
    if observed['source_sha256'] != binding['source_sha256']:
        raise ValueError('reporting source identity differs from its binding')
    parent = json.loads(Path(training_source['source_receipt']).read_text())['code']['files']
    child = json.loads(receipt.read_text())['code']['files']
    changed = sorted(name for name in set(parent) | set(child) if parent.get(name) != child.get(name))
    if changed != ['scripts/plot_ablation_analysis.py', 'scripts/run_ablation_experiments.py']:
        raise ValueError(f'report-only source changed optimizer/core files: {changed}')
    return binding, code


def _report_plot_specs(runtime_root: Path) -> list[dict]:
    return [{'split': split, 'prefix': prefix, 'scope_label': label,
             'comparison_mode': 'absolute', 'calendar_receipt': str(runtime_root / 'calendar-context.json')}
            for split, prefix, label in [
                ('val', 'val', 'Last-fold shifted validation'),
                ('deployment', 'test', 'Owned last-fold deployment test'),
                ('test', 'full_horizon_integer_audit', 'Diagnostic: exact full-horizon fold test')]]


def _numerical_source(training: dict, reporting: dict, binding_path: Path) -> tuple[dict, Path]:
    """Validate a new numerical ABI without rewriting the immutable v5 source."""
    from stockagent.runtime_identity import verify_release_bundles, verify_source_release
    binding = json.loads(binding_path.read_text())
    if binding.get('contract') != 'tw_ablation_numerical_source_v2':
        raise ValueError('unsupported numerical source binding')
    if (binding['parent_training_source_sha256'] != training['source_sha256']
            or binding['parent_reporting_source_sha256'] != reporting['source_sha256']):
        raise ValueError('numerical repair has a different immutable parent')
    receipt = Path(binding['source_receipt'])
    code = receipt.parent / 'build-source'
    observed = verify_source_release(receipt, code)
    verify_release_bundles(receipt)
    if observed['source_sha256'] != binding['source_sha256']:
        raise ValueError('numerical source identity differs from its binding')
    parent = json.loads(Path(reporting['source_receipt']).read_text())['code']['files']
    child = json.loads(receipt.read_text())['code']['files']
    changed = sorted(name for name in set(parent) | set(child) if parent.get(name) != child.get(name))
    allowed = ['stockagent/models/factorized_input.py', 'stockagent/models/transformer_base_portfolio.py',
               'stockagent/training/checkpoint_contract.py',
               'stockagent/training/trainer.py']
    if changed != allowed:
        raise ValueError(f'numerical amendment changed unapproved files: {changed}')
    proof = json.loads(Path(binding['acceptance_receipt']).read_text())
    if (proof.get('state') != 'accepted_narrow_head_inductor_dual_ddp_v2'
            or proof.get('source_sha256') != binding['source_sha256']
            or proof.get('world_size') != 2
            or proof.get('complete_fold_lifecycle_pass') is not True
            or proof.get('canonical_resume_pass') is not True
            or proof.get('head_compile_backend') != 'inductor'
            or proof.get('head_fullgraph_pass') is not True
            or proof.get('head_captured_adjoint_pass') is not True):
        raise ValueError('numerical repair has not passed dual-DDP lifecycle/resume acceptance')
    return binding, code


def _command(code: Path, spec: Path, selected: set[str], *, dry_run: bool) -> list[str]:
    command = [sys.executable, str(code / 'scripts/run_ablation_experiments.py'),
               '--spec', str(spec), '--runner', str(RUNNER),
               '--only', ','.join(sorted(selected)), '--max-folds', '1',
               '--multi-gpu-strategy', 'distributed_data_parallel', '--parallel-jobs', '1',
               '--cpu-threads', '8', '--torch-compile-threads', '16',
               '--no-auto-resume', '--stop-on-fail']
    if dry_run:
        command.append('--dry-run')
    return command


def _bind_contract(path: Path, payload: dict | list) -> None:
    """Never rewrite an existing experiment's identity to force resume."""
    encoded = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open('xb') as handle:
            handle.write(encoded)
    except FileExistsError:
        if path.read_bytes() != encoded:
            raise ValueError(f'incompatible existing ablation contract: {path}; use a new version/root')


def _environment(launch: dict, proof: dict, code: Path) -> dict[str, str]:
    # Match the selected launch's bounded runtime, not a historical ablation's
    # 112-thread policy. Financial ledger precision is untouched.
    env = {**os.environ,
           'STOCKAGENT_CODE_RELEASE_RECEIPT': launch['source_receipt'],
           'STOCKAGENT_TRAINING_TRANSFORM_CACHE_DIR': launch['transform_cache'],
           'STOCKAGENT_ABLATION_FROZEN_RUNNER': str(code / 'coda_runner.sh'),
           'CUDA_VISIBLE_DEVICES': '0,1', 'PYTHONNOUSERSITE': '1', 'PYTHONDONTWRITEBYTECODE': '1',
           'STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL': '1',
           'STOCKAGENT_DAY_TRADE_CARRY_CHECKPOINT_BLOCK_ROWS': '16',
           'STOCKAGENT_DAY_TRADE_CARRY_COMMIT_COMPILE': '1',
           'STOCKAGENT_DAY_TRADE_TRANSPORT_BLOCK_ROWS': str(proof['transport_block_rows']),
           'STOCKAGENT_FEATURE_VERIFY_THREADS': '4',
           'OPENBLAS_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1',
           'STOCKAGENT_CPU_THREADS': '8', 'STOCKAGENT_TORCH_COMPILE_THREADS': '16',
           'STOCKAGENT_POLARS_THREADS': '1', 'POLARS_MAX_THREADS': '1', 'RAYON_NUM_THREADS': '1',
           'STOCKAGENT_BACKTEST_COMPILE_PREP': '1', 'STOCKAGENT_STRICT_NO_FALLBACK': '1',
           'STOCKAGENT_RETRAIN_COMPLETED_FOLDS': '0',
           'PYTORCH_ALLOC_CONF': 'expandable_segments:True'}
    env.pop('PYTORCH_CUDA_ALLOC_CONF', None)
    return env


def _training_preflight(code: Path, env: dict[str, str]) -> int:
    """Operational CUDA gate; keep the accepted trainer release immutable."""
    # The canonical live environment checker reports initialization/kernel
    # failures without an NVML false positive or an uncaught properties query.
    # Then retain the exact frozen release's strict package/environment gate.
    commands = [
        [sys.executable, str(ROOT / 'scripts/check_environment.py'),
         '--require-cuda', '--minimum-cuda-devices', '2', '--strict'],
        [sys.executable, str(code / 'scripts/check_environment.py'),
         '--require-cuda', '--strict'],
    ]
    for command in commands:
        result = subprocess.run(command, cwd=code, env=env)
        if result.returncode:
            print('CUDA／環境驗收未通過；尚未啟動訓練，checkpoint 與 Inductor 設定不變。'
                  '請依環境報告修復 GPU driver／UVM 裝置後重跑原指令。', flush=True)
            return result.returncode
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--list', action='store_true', help='List choices without preparing or training.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--only', help='Run a subset of the selected queue, in catalog order.')
    group.add_argument('--run-selected', action='store_true',
                       help='Run exactly the user-selected queue, in its configured order.')
    parser.add_argument('--dry-run', action='store_true', help='Validate and generate selected configs; no training.')
    parser.add_argument('--plots-only', action='store_true', help='Refresh completed charts; no training or optimizer writes.')
    parser.add_argument('--numerical-repair', action='store_true',
                        help='Use the versioned fully Inductor small-head repair in a fresh v6 root; same experiments.')
    args = parser.parse_args(argv)
    if args.plots_only and args.dry_run:
        parser.error('--plots-only and --dry-run are mutually exclusive')
    selected = _selection(args.only)
    spec_path = REPAIR_SPEC if args.numerical_repair else SPEC
    if selected is None:
        _, all_rows = _experiment_rows(spec_path)
        selected = {row['name'] for row in all_rows}
    spec, rows = _experiment_rows(spec_path, selected)
    if args.list:
        print('last fold only per case | per-lookback shifted annual periods | foreground dual-GPU DDP')
        for row in rows:
            print(f"{row['name']:<38} {row['description']}")
        print(f'choices={len(rows)}; --list 不啟動訓練；無參數則依序執行整輪並 resume。')
        return 0
    if not LAUNCH.is_file():
        raise FileNotFoundError(f'Run on vastai1T with the accepted annual launch: {LAUNCH}')

    launch = json.loads(LAUNCH.read_text())
    # Use the remote maintained entrypoint, including its report-only source
    # amendment gate. Do not replace it with an older local control checkout.
    subprocess.run([sys.executable, str(ROOT / 'scripts/run_relocated_training.py'),
                    '--launch', str(LAUNCH), '--check-only',
                    '--start-fold', '10', '--max-folds', '1'], cwd=ROOT, check=True)
    if launch.get('contract') != 'accepted_annual_scale_cash_launch_v1' or launch.get('state') != 'ready':
        raise ValueError('accepted annual baseline required')
    proof = json.loads(Path(launch['acceptance_receipt']).read_text())
    source, code = _shifted_source(launch, SOURCE)
    report_source, report_code = _reporting_source(source, REPORT_SOURCE)
    if args.numerical_repair:
        source, code = _numerical_source(source, report_source, NUMERICAL_SOURCE)
        # The numerical child contains the exact already-accepted reporting
        # scripts; pin both roles to the real child, not a misleading old SHA.
        report_source, report_code = source, code
    for required in ('scripts/run_ablation_experiments.py', 'scripts/check_environment.py', 'coda_runner.sh'):
        if not (code / required).is_file():
            raise FileNotFoundError(code / required)

    # Generated run configurations/summary/checkpoints are training artifacts;
    # operational receipts live separately. No new data projection is built.
    output = ROOT / spec['output_root']
    if not output.resolve().is_relative_to((ROOT / 'artifacts/markets').resolve()):
        raise ValueError('ablation training outputs must be under artifacts/markets')
    contract = {
        'contract': 'annual_scale_cash_last_fold_shifted_ofat_v3',
        'catalog_sha256': hashlib.sha256(spec_path.read_bytes()).hexdigest(),
        'source_sha256': source['source_sha256'],
        'parent_source_sha256': launch['source_sha256'],
        'base_config_sha256': launch['config_sha256'],
        'input_contract_sha256': launch['source_manifest_sha256'],
        'feature_manifest_sha256': proof['feature_manifest_sha256'],
        'fold_policy': 'last_per_variant', 'max_folds': 1, 'world_size': 2,
        'year_boundary_mode': 'lookback_shifted', 'split_start_year': 2014,
        'boundary_offset': 'each_variant_training_lookback_observed_sessions',
        'old_calendar_baseline_comparable': False,
        'variant_gpu_training_verified': False,
    }
    _bind_contract(output / 'ablation-contract.json', contract)
    runtime_root = ROOT / 'artifacts/operations/training_launches' / output.name
    _bind_contract(runtime_root / 'input-contract.json', contract)
    # This is a separate report contract, not a rewritten experiment/optimizer
    # identity. The catalog, original training source and completed folds stay.
    report_contract = {'contract': 'annual_ablation_reporting_v1',
                       'training_source_sha256': source['source_sha256'],
                       'reporting_source_sha256': report_source['source_sha256'],
                       'plots': _report_plot_specs(runtime_root)}
    _bind_contract(runtime_root / 'report-contract-v1.json', report_contract)
    generated_spec = _load_ablation_spec(spec_path)
    generated_spec['base_config'] = launch['config']
    generated_spec['output_root'] = str(output)
    generated_spec['postprocess_plots'] = report_contract['plots']
    effective_spec = runtime_root / 'spec.yaml'
    # The binding above guards re-generation. Original baseline bytes untouched.
    effective_spec.write_text(yaml.safe_dump(generated_spec, allow_unicode=True, sort_keys=False))
    env = _environment({**launch, 'source_receipt': source['source_receipt']}, proof, code)
    env['STOCKAGENT_ABLATION_REPORT_SOURCE_RECEIPT'] = report_source['source_receipt']
    command = _command(report_code, effective_spec, selected, dry_run=args.dry_run)
    if args.plots_only:
        command.append('--plots-only')
    ordered = [row['name'] for row in rows]
    print(json.dumps({'state': 'selected_ablation_command', 'selected': ordered,
                      'fold_policy': 'last_per_variant', 'formal_training_started': False,
                      'training_source_sha256': source['source_sha256'],
                      'reporting_source_sha256': report_source['source_sha256'],
                      'baseline_acceptance_not_variant_performance_proof': True,
                      'command': command}, ensure_ascii=False), flush=True)
    if args.plots_only:
        # Read the existing full-queue calendar proof; do not regenerate formal
        # configurations, progress/summary or optimizer artifacts to repair PNGs.
        context_receipt = runtime_root / 'calendar-context.json'
        context_proof = json.loads(context_receipt.read_text())
        if (context_proof.get('state') != 'accepted_calendar_context'
                or context_proof['feature_manifest_sha256'] != proof['feature_manifest_sha256']):
            raise ValueError('plots-only requires the accepted full-queue calendar receipt')
        return subprocess.run(command, cwd=report_code, env=env).returncode
    # Cheap generation and canonical calendar checks precede any GPU lease/job.
    # Dry-run/config construction alone cannot prove annual lookback coverage.
    prepared = subprocess.run(_command(report_code, effective_spec, selected, dry_run=True), cwd=report_code, env=env)
    if prepared.returncode:
        return prepared.returncode
    _, full_queue = _experiment_rows(spec_path)
    selection_id = hashlib.sha256(','.join(ordered).encode()).hexdigest()[:12]
    context_name = ('calendar-context.json' if len(rows) == len(full_queue)
                    else f'calendar-context-{selection_id}.json')
    context_receipt = runtime_root / context_name
    context = subprocess.run([sys.executable, str(ROOT / 'scripts/check_tw_scale_cash_ablation_context.py'),
        '--code-root', str(code), '--config-root', str(output / 'generated_configs'),
        '--baseline-config', launch['config'], '--only', ','.join(ordered),
        '--report', str(context_receipt)], cwd=code, env=env)
    if context.returncode not in (0, 2):
        return context.returncode
    context_proof = json.loads(context_receipt.read_text())
    if context_proof['feature_manifest_sha256'] != proof['feature_manifest_sha256']:
        raise ValueError('calendar preflight read a different feature manifest')
    print(json.dumps({'state': context_proof.get('state'),
                      'fold_ids_by_case': {name: case['fold_ids'] for name, case
                                           in context_proof.get('cases', {}).items()},
                      'formal_training_started': False}, ensure_ascii=False), flush=True)
    # Old calendar-period results are not a control for shifted periods.
    # Preserve per-case reporting; paired plots wait for a matching control.
    if (output / 'baseline/summary.json').exists():
        raise ValueError('shifted queue must not inherit an unverified baseline reference')
    if context.returncode:
        print('所選實驗缺少年初因果上下文；尚未啟動訓練。詳見 ' + str(context_receipt), flush=True)
        return context.returncode
    if args.dry_run:
        return 0
    from scripts.manage_gpu_jobs import _busy_gpu_indices, _gpu_lease_command
    from scripts.run_relocated_training import renew_original_source_leases
    if _busy_gpu_indices().intersection({0, 1}):
        raise RuntimeError('GPU 0/1 occupied; do not interrupt the current training owner')
    renew_original_source_leases(launch)
    # Preflight precedes expensive jobs, and a node-global lease covers CPU
    # preparation as well as every sequential dual-GPU experiment.
    status = _training_preflight(code, env)
    if status:
        return status
    return subprocess.run(_gpu_lease_command(command, [0, 1], sharing=False), cwd=report_code, env=env).returncode


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
