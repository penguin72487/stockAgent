#!/usr/bin/env python3
"""Launch a pinned trainer: existing resume or an accepted new annual run."""
from __future__ import annotations
import ast
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.runtime_identity import verify_source_release, verify_release_bundles
from scripts.manage_gpu_jobs import _busy_gpu_indices, _gpu_lease_command


ANNUAL_LAUNCH_CONTRACT = 'accepted_annual_scale_cash_launch_v1'


def _verify_reporting_source_amendment(launch, accepted_source_sha256):
    """Permit a receipt-backed report fix, never an unmeasured trainer change."""
    amendment_path = launch.get('reporting_source_amendment')
    if amendment_path is None:
        assert launch['source_sha256'] == accepted_source_sha256, 'source_sha256'
        return
    path = Path(amendment_path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == launch['reporting_source_amendment_sha256'], 'report amendment changed'
    amendment = json.loads(path.read_text())
    assert amendment['contract'] == 'report_only_stitched_source_amendment_v1'
    assert amendment['state'] == 'verified' and not amendment['optimizer_contract_changed']
    assert amendment['parent_source_sha256'] == accepted_source_sha256
    assert amendment['source_sha256'] == launch['source_sha256']
    assert amendment['source_receipt'] == launch['source_receipt']
    assert amendment['config_sha256'] == launch['config_sha256']
    parent_receipt = Path(amendment['parent_source_receipt'])
    receipt = Path(launch['source_receipt'])
    for item, expected in ((parent_receipt, accepted_source_sha256),
                           (receipt, launch['source_sha256'])):
        assert verify_source_release(item, item.parent / 'build-source')['source_sha256'] == expected
        verify_release_bundles(item)
    parent = json.loads(parent_receipt.read_text())['code']['files']
    current = json.loads(receipt.read_text())['code']['files']
    assert parent.keys() == current.keys(), 'report fix changes source inventory'
    changed = sorted(name for name in parent if parent[name] != current[name])
    assert changed == ['stockagent/training/trainer.py'], 'report fix changes another module'
    allowed = {'_stitched_deployment_prefix_results',
               '_replay_taiwan_stitched_deployment', '_refresh_walkforward_artifacts'}
    trees = []
    for item in (parent_receipt, receipt):
        tree = ast.parse((item.parent / 'build-source/stockagent/training/trainer.py').read_text())
        tree.body = [node for node in tree.body
                     if not (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                             and node.name in allowed)]
        trees.append(ast.dump(tree, include_attributes=False))
    assert trees[0] == trees[1], 'report fix changes train, loss, model or optimizer code'


def _verify_launch_config(launch):
    """Accept only a fold-selection edit against the exact accepted bytes.

    runner.start_fold is a lifecycle selector, not a model/optimizer contract.
    Every other byte must still match the selected performance receipt.
    """
    payload = Path(launch['config']).read_bytes()
    if hashlib.sha256(payload).hexdigest() == launch['config_sha256']:
        return None
    assert launch.get('contract') == ANNUAL_LAUNCH_CONTRACT, 'config'
    pattern = rb'(?m)^(  start_fold: )([1-9][0-9]*)$'
    matches = list(re.finditer(pattern, payload))
    assert len(matches) == 1, 'config'
    normalized = re.sub(pattern, lambda match: match.group(1) + str(launch['fold_ids'][0]).encode(), payload)
    assert hashlib.sha256(normalized).hexdigest() == launch['config_sha256'], 'config'
    return int(matches[0].group(2))


def _annual_acceptance(launch):
    """Admit a new experiment only from its complete, selected DDP receipt.

    This does not import any engineering optimizer. Subsequent compatible
    resumes still belong to train.py's checkpoint contract.
    """
    if launch.get('contract') != ANNUAL_LAUNCH_CONTRACT:
        assert launch.get('contract') is None, 'unknown training launch contract'
        assert launch['fold_ids'] == [11], 'original enrolled entry owns fold 11'
        return None
    path = Path(launch['acceptance_receipt'])
    assert hashlib.sha256(path.read_bytes()).hexdigest() == launch['acceptance_sha256'], 'acceptance receipt changed'
    proof = json.loads(path.read_text())
    assert proof['state'] == 'accepted' and proof['contract'] == ANNUAL_LAUNCH_CONTRACT
    _verify_reporting_source_amendment(launch, proof['source_sha256'])
    for key in ('config_sha256', 'fold_ids'):
        assert proof[key] == launch[key], key
    assert proof['fold_ids'] == [10] and proof['train_years'] == list(range(2015, 2025))
    assert proof['context_years'] == [2014] and proof['val_years'] == [2025] and proof['test_years'] == [2026]
    feature = Path(proof['feature_manifest'])
    assert hashlib.sha256(feature.read_bytes()).hexdigest() == proof['feature_manifest_sha256'], 'accepted feature manifest changed'
    result_path = Path(proof['result'])
    assert hashlib.sha256(result_path.read_bytes()).hexdigest() == proof['result_sha256'], 'DDP receipt changed'
    result = json.loads(result_path.read_text())
    assert result['ok'] and result['return_code'] == 0 and result['lifecycle_ok'], 'complete lifecycle not accepted'
    assert result['world_size'] == 2 and result['batch_size'] == proof['batch_size']
    assert result['local_batch_size'] * 2 == proof['batch_size']
    assert result['steady_epochs'] and min(result['steady_epochs']) >= 3 and not result['failure_patterns']
    assert result['memory']['min_headroom_gib'] >= 3 and result['host_memory']['min_headroom_gib'] >= 16
    assert not proof['engineering_optimizer_donated']
    return proof


def renew_original_source_leases(launch, *, materialized=Path('/srv/stockagent-packed-materialized'),
                                 sync=Path('/srv/stockagent-packed')):
    """Keep the selected original sources pinned; never resolve moving latest."""
    from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, write_packed_pin
    from stockagent.data_sync.materialized_cache import _ready_matches
    original = json.loads(Path(launch['source_manifest']).read_text())['configuration']['data']
    previous_pins = materialized / '.training-pins/tw-daytrade-no-basis-flat-bf16-tf32-b128-v1'
    pin_root = materialized / '.training-pins' / Path(launch['output_root']).parent.name
    seen = set()
    for field in ('parquet_root', 'day_trade_minute_execution_root', 'day_trade_physical_public_feature_path'):
        relative = Path(original[field]).resolve(strict=True).relative_to(materialized)
        dataset, snapshot = relative.parts[:2]
        if (dataset, snapshot) in seen:
            continue
        seen.add((dataset, snapshot))
        resolved = resolve_packed_snapshot_id(sync, dataset, snapshot)
        prior = json.loads((previous_pins / (dataset + '.pin.json')).read_text())
        assert prior['manifest']['snapshot_id'] == snapshot and prior['manifest_sha256'] == resolved.manifest_sha256
        assert _ready_matches(materialized, resolved), 'original source READY changed'
        subprocess.run(['bash', 'scripts/run_data_cache.sh', 'use', dataset, '--snapshot-id', snapshot,
                        '--ttl-days', '7', '--timeout-seconds', '3600', '--retain-payload'],
                       cwd=ROOT, check=True)
        lease = json.loads((materialized / '.cache-state/leases' / dataset / (snapshot + '.json')).read_text())
        assert lease['state'] == 'hot' and lease['verification'] == 'full' and lease['expires_ns'] > time.time_ns()
        assert lease['manifest_sha256'] == resolved.manifest_sha256
        assert Path(lease['target']).resolve(strict=True) == materialized / dataset / snapshot
        pin_root.mkdir(parents=True, exist_ok=True)
        pin = pin_root / (dataset + '.pin.json')
        if pin.exists():
            old = json.loads(pin.read_text())
            assert old['manifest_sha256'] == resolved.manifest_sha256 and old['manifest']['snapshot_id'] == snapshot
        write_packed_pin(pin, resolved)


def _training_command(launch, *, start_fold=None, max_folds=None):
    """Select folds through train.py without modifying the pinned experiment.

    No override retains the accepted single-fold entry. An explicit start fold
    runs through the final canonical fold unless a limit is also requested.
    Acceptance still describes the measured fold, not all requested folds.
    """
    if start_fold is not None and start_fold < 1:
        raise ValueError('--start-fold must be >= 1')
    if max_folds is not None and max_folds < 1:
        raise ValueError('--max-folds must be >= 1')
    if launch.get('contract') != ANNUAL_LAUNCH_CONTRACT:
        if start_fold is not None or max_folds is not None:
            raise ValueError('fold overrides require the selected annual launch')
    fold = launch['fold_ids'][0] if start_fold is None else start_fold
    limit = 1 if start_fold is None and max_folds is None else max_folds
    command = [sys.executable, 'train.py', '--config', launch['config'],
               '--start-fold', str(fold)]
    if limit is not None:
        command.extend(['--max-folds', str(limit)])
    command.extend(['--torch-compile-threads', '16', '--resume',
                    '--no-retrain-completed-folds', '--no-profile-timing',
                    '--no-debug-timing-sync', '--no-isolate-train-folds'])
    return command


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--launch', type=Path, required=True)
    p.add_argument('--check-only', action='store_true')
    p.add_argument('--start-fold', type=int, help='Start at this fold and run through the final fold.')
    p.add_argument('--max-folds', type=int, help='Optional number of folds after --start-fold.')
    a = p.parse_args()
    launch = json.loads(a.launch.read_text())
    if launch['state'] != 'ready':
        print(json.dumps({'state':launch['state'],'pending_inputs':launch['pending_inputs']}))
        return 75
    config_start_fold = _verify_launch_config(launch)
    assert hashlib.sha256(Path(launch['source_manifest']).read_bytes()).hexdigest() == launch['source_manifest_sha256'], 'source_manifest'
    try:
        command = _training_command(launch,
            start_fold=a.start_fold if a.start_fold is not None else config_start_fold,
            max_folds=a.max_folds)
    except ValueError as exc:
        p.error(str(exc))
    annual = _annual_acceptance(launch)
    receipt = Path(launch['source_receipt']); code = receipt.parent / 'build-source'
    assert verify_source_release(receipt, code)['source_sha256'] == launch['source_sha256']
    verify_release_bundles(receipt)
    # Load only from the selected frozen trainer, in a fresh process. No model,
    # optimizer or checkpoint pickle is needed for placement acceptance.
    program = '''
import json,sys
from pathlib import Path
from stockagent.config import load_config
import stockagent.config as config_module
assert Path(config_module.__file__).resolve().parents[1] == Path.cwd(), 'wrong frozen config loader'
c=load_config(sys.argv[1])
assert str(c.runner.output_dir)==sys.argv[2]
assert c.data.factorized_feature_manifest and Path(c.data.factorized_feature_manifest).is_file(), 'full feature manifest required'
for name in ('factorized_feature_manifest','tw_public_feature_path','panel_cache_root',
             'day_trade_minute_execution_cache_dir'):
 path=Path(getattr(c.data,name))
 assert path.exists(),str(path)
 assert 'artifacts/markets/' not in str(path),str(path)
feature=json.loads(Path(c.data.factorized_feature_manifest).read_text())
assert feature['status']=='complete' and feature['model_channel_policy']=='value_only'
assert feature['logical_model_channels']==14726
assert len(feature['base_feature_names'])==6 and len(feature['individual_channels'])==14655 and len(feature['common_channels'])==65
assert feature['base_feature_names']==c.data.feature_include, 'base feature schema mismatch'
assert c.training.financial_transformer.portfolio_output_mode=='score_entmax_scale_separated_cash'
proof=json.loads(sys.argv[3])
if proof is not None:
 assert c.training.day_trade_training_annual_episodes and not c.training.day_trade_sub_lot_recovery
 assert c.walk_forward.split_start_year==2015 and c.runner.start_fold==int(sys.argv[4])
 assert c.runner.resume and c.runner.require_cuda and c.training.multi_gpu_strategy=='distributed_data_parallel'
 assert c.training.batch_size_train==proof['batch_size']
 assert c.data.factorized_host_cache_bytes==proof['host_cache_bytes']
 assert c.data.factorized_feature_manifest==proof['feature_manifest']
 assert c.training.financial_transformer.factorized_compact_projection
 assert int(getattr(c.training.financial_transformer,'factorized_compact_stream_chunk_bytes',0))==proof.get('stream_chunk_bytes',0), 'accepted stock stream budget changed'
 assert c.training.pretrained_initialization_root is None and not c.training.day_trade_sparse_events
 from stockagent.training.lifecycle import validate_completed_training_artifacts
 group='train_'+'-'.join(map(str,proof['train_years']))
 acceptance=validate_completed_training_artifacts(Path(proof['artifact_root']),fold_ids=[10],group_names=[group])
 assert acceptance.ok, ('accepted engineering evidence missing',acceptance.missing,acceptance.invalid)
print(json.dumps({'state':'accepted_local_training_layout','output_root':str(c.runner.output_dir),'feature_manifest':c.data.factorized_feature_manifest,'expected_model_input':14726}))
'''
    env = {**os.environ, 'STOCKAGENT_CODE_RELEASE_RECEIPT': str(receipt),
           'STOCKAGENT_TRAINING_TRANSFORM_CACHE_DIR':launch['transform_cache'],
           'CUDA_VISIBLE_DEVICES':'0,1', 'PYTHONNOUSERSITE':'1', 'PYTHONDONTWRITEBYTECODE':'1',
           'STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL':'1',
           'STOCKAGENT_DAY_TRADE_CARRY_CHECKPOINT_BLOCK_ROWS':'16',
           'STOCKAGENT_DAY_TRADE_CARRY_COMMIT_COMPILE':'1',
           'OPENBLAS_NUM_THREADS':'1', 'MKL_NUM_THREADS':'1',
           'STOCKAGENT_CPU_THREADS':'8', 'STOCKAGENT_TORCH_COMPILE_THREADS':'16',
           'STOCKAGENT_POLARS_THREADS':'1','POLARS_MAX_THREADS':'1','RAYON_NUM_THREADS':'1',
           'STOCKAGENT_BACKTEST_COMPILE_PREP':'1','STOCKAGENT_STRICT_NO_FALLBACK':'1',
           'PYTORCH_ALLOC_CONF':'expandable_segments:True'}
    if annual is not None:
        env['STOCKAGENT_FEATURE_VERIFY_THREADS'] = '4'
        env['STOCKAGENT_DAY_TRADE_TRANSPORT_BLOCK_ROWS'] = str(annual['transport_block_rows'])
    env.pop('PYTORCH_CUDA_ALLOC_CONF', None)
    subprocess.run([sys.executable,'-c',program,launch['config'],launch['output_root'],json.dumps(annual),
                    str(config_start_fold if config_start_fold is not None else launch['fold_ids'][0])],
                   cwd=code,env=env,check=True)
    print(json.dumps({'state': 'selected_training_command', 'config_start_fold_override': config_start_fold,
                      'command': command}), flush=True)
    if a.check_only:
        return 0
    if _busy_gpu_indices().intersection({0,1}):
        raise RuntimeError('GPU 0/1 currently owned; retain canonical training owner')
    if annual is None:
        assert (Path(launch['output_root']) / 'run_manifest.json').is_file(), 'restore the fixed original run before canonical resume'
    renew_original_source_leases(launch)
    subprocess.run([sys.executable,'scripts/check_environment.py','--require-cuda','--strict'],cwd=code,env=env,check=True)
    return subprocess.run(_gpu_lease_command(command,[0,1],sharing=False),cwd=code,env=env).returncode


if __name__=='__main__':
    raise SystemExit(main())
