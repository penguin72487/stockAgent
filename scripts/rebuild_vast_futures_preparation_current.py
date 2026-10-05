#!/usr/bin/env python3
"""Reconstruct the accidentally deleted current release from its exact sources.

Uses the retained canonical builder, not modified checkout code. All output stays
private unless both original release SHAs match and the historical restoration
has already been promoted. Never train, activate models or overwrite a path.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import _ssh_base
from scripts.return_remote_legacy_archives import private_control_prefix
from stockagent.data_sync.desync_snapshots import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    original = json.loads((ROOT / 'artifacts/operations/futures_margin_prepare_20261004/remote_build_acceptance.json').read_text())
    proof = {k: original[k] for k in ('builder_sha256', 'source_manifest_sha256',
                                    'daily_sha256', 'rules_sha256', 'products', 'rows')}
    if not args.apply:
        print(json.dumps({'state': 'exact_source_rebuild_plan', **proof, 'model_activation': False}))
        return
    code = private_control_prefix() + '''
import json,hashlib,os,subprocess,sys,time
from pathlib import Path
from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.data_sync.remote_legacy_return import real,recovery_hold_references
root=Path('/root/stockAgent')
work=Path('/var/lib/stockagent-futures-recovery/20261004')
source=root/'data_tw_futures/margin_sources'
builder=root/'artifacts/runtime/tw_futures_margin/source/scripts/prepare_tw_futures_margin_training.py'
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  while b:=f.read(4*1024*1024):h.update(b)
 return h.hexdigest()
expected=EXPECTED
target=real(root/'artifacts/markets/tw_futures_v8_margin_preparation')
if not recovery_hold_references(target,root):raise RuntimeError('explicit recovery hold missing')
if sha(source/'source_manifest.json')!=expected['source_manifest_sha256'] or sha(builder)!=expected['builder_sha256']:
 raise RuntimeError('original source or original builder differs; no rebuild')
work.mkdir(parents=True,mode=0o700,exist_ok=True)
output=real(work/'current-build-restored-sources')
if output.exists():raise FileExistsError('previous rebuild exists; inspect its checkpoint')
env=dict(os.environ)
for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMBA_NUM_THREADS','POLARS_MAX_THREADS'):
 env[key]='2'
env.update(OMP_THREAD_LIMIT='2',OMP_WAIT_POLICY='PASSIVE',KMP_BLOCKTIME='0')
command=[sys.executable,str(builder),'--sources',str(source),'--output',str(output),
 '--start','2010-11-01','--end','2026-09-04','--slots','2816',
 '--integrate-rule-delta',str(root/'data_tw_futures/margin_repair_pending'),
 '--valuation-research-policy',str(root/'data_tw_futures/margin_repair_inputs/accepted_valuation/sources/valuation_research_policy.json'),
 '--omit-empty-account-prefixes','--empty-prefix-max-volume-participation','0.5']
atomic_write_json(work/'current-rebuild-intent.json',{'expected':expected,'command':command,'cold_deleted':False,'model_activation':False})
subprocess.run(command,env=env,cwd=builder.parents[1],check=True)
acceptance=json.loads((output/'build_acceptance.json').read_text())
observed={k:sha(output/'release'/rel) for k,rel in (
 ('daily_sha256','daily/continuous_daily.parquet'),('rules_sha256','rules/rules.parquet'))}
if any(observed[k]!=expected[k] for k in observed):raise RuntimeError('regenerated release differs from original SHAs; private output retained')
result={'state':'same_release_contents_regenerated_verified','output':str(output),'expected':expected,
 'observed':observed,'cold_deleted':False,'model_activation':False,'original_file_bytes_restored':True}
atomic_write_json(work/'current-rebuild-verified.json',result)
print(json.dumps(result),flush=True)
'''.replace('EXPECTED', repr(proof))
    result = subprocess.run([*_ssh_base(Path('/root/.ssh/stockagent_vastai1t_ed25519'), 40032),
                             'root@114.32.64.6',
                             'cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -'],
                            input=code, text=True, stdout=sys.stdout, stderr=sys.stderr, timeout=7200)
    if result.returncode:
        raise SystemExit(result.returncode)
    atomic_write_json(ROOT / 'artifacts/operations/futures_preparation_recovery_20261004/current-rebuild-command-completed.json',
                      {'state': 'remote_exact_source_rebuild_completed', 'expected': proof,
                       'cold_deleted': False, 'model_activation': False})


if __name__ == '__main__':
    main()
