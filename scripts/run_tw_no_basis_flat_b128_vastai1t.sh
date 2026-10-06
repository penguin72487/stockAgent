#!/usr/bin/env bash
# Foreground, canonical resume in the accepted new batch/precision experiment.
set -euo pipefail
task_root=/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_v1
source_root=/root/stockAgent/artifacts/markets/tw_day_trade_no_basis_batch_powers_20261006_v1/large_batch_optimization_v5
check_only=0
if [ "$#" -eq 1 ] && [ "$1" = '--check-only' ]; then
  check_only=1
elif [ "$#" -ne 0 ]; then
  echo '固定 fold11／雙卡／batch128／BF16+TF32；僅接受 --check-only。' >&2
  exit 2
fi
cd "$source_root/code"
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONNOUSERSITE=1
export STOCKAGENT_CODE_RELEASE_RECEIPT="$source_root/code-release/20261006T031336382517Z-b2c7055cac3f/release.json"
export STOCKAGENT_TRAINING_TRANSFORM_CACHE_DIR=/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_gaprepair_v4/.training_transform_cache_v1
export STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL=1
export STOCKAGENT_DAY_TRADE_CARRY_CHECKPOINT_BLOCK_ROWS=16
export STOCKAGENT_DAY_TRADE_CARRY_COMMIT_COMPILE=1
export OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export STOCKAGENT_CPU_THREADS=8 STOCKAGENT_TORCH_COMPILE_THREADS=16
export STOCKAGENT_POLARS_THREADS=1 POLARS_MAX_THREADS=1 RAYON_NUM_THREADS=1
export STOCKAGENT_BACKTEST_COMPILE_PREP=1 STOCKAGENT_STRICT_NO_FALLBACK=1
unset PYTORCH_CUDA_ALLOC_CONF
export PYTORCH_ALLOC_CONF=expandable_segments:True
run_fintech_python - <<'PY'
import hashlib
import json
from pathlib import Path
import subprocess
import time
from downloader.artifact_io import atomic_write_json
from stockagent.config import load_config
from stockagent.runtime_identity import verify_source_release, verify_release_bundles
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, write_packed_pin
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids

task_root = Path('/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_v1')
source_root = Path('/root/stockAgent/artifacts/markets/tw_day_trade_no_basis_batch_powers_20261006_v1/large_batch_optimization_v5')
proof = json.loads((task_root/'training-ready.json').read_text())
assert proof['state'] == 'accepted_complete_fold11_dual_gpu_flat_bf16_tf32_b128'
assert proof['complete_fold_lifecycle_verified'] and proof['world_size'] == 2
assert proof['formal_optimizer_reused'] is False
receipt = source_root/'code-release/20261006T031336382517Z-b2c7055cac3f/release.json'
source = verify_source_release(receipt, source_root/'code')
verify_release_bundles(receipt)
assert source['source_sha256'] == proof['source_sha256']
config_path = task_root/'config.yaml'
assert hashlib.sha256(config_path.read_bytes()).hexdigest() == proof['config_sha256']
config = load_config(config_path)
assert config.environment.amp_dtype == 'bf16' and config.environment.use_tensor_cores
assert config.environment.cpu_threads == 8 and config.runner.require_cuda
assert config.training.batch_size_train == 128 and config.training.batch_size_eval == 16
assert config.training.factorized_encoder_checkpoint and config.training.epochs == 1000
assert config.training.multi_gpu_strategy == 'distributed_data_parallel'
assert config.runner.resume and not config.runner.isolate_train_folds
assert Path(config.runner.output_dir) == task_root/'training-bf16'
assert not config.training.day_trade_sparse_events
assert not config.training.financial_transformer.temporal_basis_families
assert config.training.financial_transformer.feature_svd_components == 0
assert config.training.pretrained_initialization_root is None
feature_path = Path(config.data.factorized_feature_manifest)
assert hashlib.sha256(feature_path.read_bytes()).hexdigest() == proof['feature_manifest_sha256']
feature = json.loads(feature_path.read_text())
assert feature['logical_model_channels'] == 14726 and feature['model_channel_policy'] == 'value_only'
assert feature['research_only'] and not feature['historical_point_in_time']
materialized = Path('/srv/stockagent-packed-materialized')
pin_root = materialized/'.training-pins'/'tw-daytrade-no-basis-flat-bf16-tf32-b128-v1'
pins = []
for item in proof['source_leases']:
    subprocess.run(['bash','scripts/run_data_cache.sh','use',item['dataset'],
        '--snapshot-id',item['snapshot_id'],'--ttl-days','7','--timeout-seconds','3600',
        '--retain-payload'],check=True)
    lease = json.loads((materialized/'.cache-state/leases'/item['dataset']/(item['snapshot_id']+'.json')).read_text())
    assert lease['state'] == 'hot' and lease['verification'] == 'full' and lease['expires_ns'] > time.time_ns()
    assert lease['manifest_sha256'] == item['manifest_sha256']
    assert Path(lease['target']).resolve(strict=True) == materialized/item['dataset']/item['snapshot_id']
    resolved = resolve_packed_snapshot_id(Path('/srv/stockagent-packed'), item['dataset'], item['snapshot_id'])
    assert resolved.manifest_sha256 == item['manifest_sha256']
    pin = pin_root/(item['dataset']+'.pin.json')
    if pin.exists():
        old = json.loads(pin.read_text())
        assert old['manifest_sha256'] == item['manifest_sha256'] and old['manifest']['snapshot_id'] == item['snapshot_id']
    write_packed_pin(pin, resolved)
    pins.append({'path':str(pin),'snapshot_id':item['snapshot_id'],'manifest_sha256':item['manifest_sha256']})
assert {item['snapshot_id'] for item in proof['source_leases']} <= _pinned_snapshot_ids(materialized)
atomic_write_json(task_root/'foreground-preflight-pins.json', {
    'state':'exact_sources_7day_full_sha_leases_and_canonical_training_pins',
    'source_verification':source,'source_pins':pins,
    'foreground_entry_sha256':hashlib.sha256((task_root/'train_fold11.sh').read_bytes()).hexdigest(),
    'training_launched_by_preflight':False,'resume_enabled':True,'profile_timing':False,
})
print('[fold11] 雙卡 global128／BF16+TF32／no basis／14726 value channels。',flush=True)
print('[resume] 只續跑本新root的相容checkpoint；首次新建，不挪用舊batch32或工程optimizer。',flush=True)
PY
run_fintech_python scripts/check_environment.py --require-cuda --strict
if [ "$check_only" -eq 1 ]; then
  echo '[fold11] --check-only 通過；未啟動正式訓練。'
  exit 0
fi
run_fintech_python - <<'PY'
import subprocess
import sys
from scripts.manage_gpu_jobs import _busy_gpu_indices, _gpu_lease_command
if _busy_gpu_indices().intersection({0,1}):
    raise RuntimeError('GPU 0/1已有其他工作；不重疊啟動。')
command = [sys.executable,'train.py','--config',
    '/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_v1/config.yaml',
    '--start-fold','11','--max-folds','1','--torch-compile-threads','16',
    '--resume','--no-retrain-completed-folds','--no-profile-timing','--no-debug-timing-sync','--no-isolate-train-folds']
raise SystemExit(subprocess.run(_gpu_lease_command(command,[0,1],sharing=False)).returncode)
PY
