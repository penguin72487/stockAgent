#!/usr/bin/env python3
"""Validate the recovered release through its frozen canonical financial reader.

Read-only data validation: no GPU training, download, model activation or orders.
"""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.restore_vast_futures_raw_sources import remote, RECEIPTS, WORK
from stockagent.data_sync.desync_snapshots import atomic_write_json


def main():
    child = '''
import json,sys
from pathlib import Path
from stockagent.data.tw_futures_margin import validate_margin_rule_source
from downloader.artifact_io import sha256_file
build=Path('/root/stockAgent/artifacts/markets/tw_futures_v8_margin_preparation/margin_components_current')
daily=build/'release/daily/continuous_daily.parquet'
rules=build/'release/rules/rules.parquet'
frame,manifest=validate_margin_rule_source(rules,daily)
print(json.dumps({'state':'frozen_canonical_reader_validates_recovered_source_aliases',
 'rows':frame.height,'products':len(manifest['scope']['products']),
 'daily_sha256':sha256_file(daily),'rules_sha256':sha256_file(rules),
 'source_receipts':len(manifest['sources']),'schema_version':manifest['schema_version'],
 'research_only':manifest.get('research_only'),'model_activation':False,'training_started':False}))
'''
    code = f'''
import os,sys,subprocess
from pathlib import Path
from stockagent.data_sync.desync_snapshots import atomic_write_json
source=Path('/root/stockAgent/artifacts/runtime/tw_futures_margin/source')
env=dict(os.environ,PYTHONPATH=str(source),PYTHONDONTWRITEBYTECODE='1')
for key in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMBA_NUM_THREADS','POLARS_MAX_THREADS']:env[key]='2'
process=subprocess.run([sys.executable,'-c',{child!r}],cwd=source,env=env,text=True,capture_output=True,timeout=600)
if process.returncode:
 atomic_write_json(Path({str(WORK)!r})/'canonical-reader-error.json',{{'stderr':process.stderr,'returncode':process.returncode}})
 raise RuntimeError('frozen canonical reader rejected recovered source references; private error retained')
result=json.loads(process.stdout.splitlines()[-1])
if result['rows']!=1896494 or result['products']!=697:raise RuntimeError('original selected scope changed')
atomic_write_json(Path({str(WORK)!r})/'canonical-reader-verified.json',result)
print(json.dumps(result))
'''
    result = remote(code)
    atomic_write_json(RECEIPTS / 'canonical-reader-verified.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
