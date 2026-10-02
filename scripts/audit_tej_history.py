"""Reproducible, read-only TEJ receipt audit; never drive Windows or TEJ."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
import csv
from datetime import UTC, datetime
import hashlib
import io
import json
from pathlib import Path
from statistics import median
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text
from downloader.tej_history import SOURCE_SCOPE_CONTRACT, _ready_task, expand_request, validate_download_evidence
from downloader.tej_key_layout import KEY1_CONTRACT, KEY3_CONTRACT, key_count, parquet_relative
from downloader.tej_desktop_attempts import ATTEMPT_CONTRACT, query_stage
from downloader.tej_period_keys import interpreted_payload
from stockagent.live.tej_dashboard import _read, _worker_alive


def _digest(path: Path) -> str:
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024**2),b''):
            digest.update(block)
    return digest.hexdigest()


def audit(root: Path, output: Path) -> dict:
    """Audit only completed v4 scope receipts captured in one SQL snapshot.

    A collector may commit more work after that snapshot. Those later tasks
    are explicitly outside this audit, rather than a mixed current total.
    """
    import pyarrow.parquet as pq

    if output.exists():
        raise FileExistsError('Refusing to overwrite TEJ audit evidence')
    root=root.resolve(); started=datetime.now(UTC)
    with closing(_read(root)) as con:
        con.execute('BEGIN')
        definitions={r['table_id']:dict(r) for r in con.execute('SELECT * FROM tables')}
        tasks=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE kind='download' AND state='complete' AND scope_contract=?",(SOURCE_SCOPE_CONTRACT,))]
        registry={(r['table_id'],r['name']):r['exported_non_null_cells'] or 0 for r in con.execute('SELECT table_id,name,exported_non_null_cells FROM features WHERE exported_non_null_cells>0')}
        states=[dict(r) for r in con.execute('SELECT kind,state,COUNT(*) AS tasks FROM tasks WHERE scope_contract=? GROUP BY kind,state',(SOURCE_SCOPE_CONTRACT,))]
        blocked=[dict(r) for r in con.execute("SELECT t.kind,b.name AS table_name,t.last_error_code FROM tasks t JOIN tables b USING(table_id) WHERE t.state='blocked' AND t.scope_contract=? ORDER BY b.name",(SOURCE_SCOPE_CONTRACT,))]
        snapshot_at=datetime.now(UTC).isoformat()
    results=[]; cells=Counter(); failed=0; bound_attempts=0; key3_tasks=0; snapshot_tasks=0; header_mapping_tasks=0; month_key_tasks=0
    for task in tasks:
        result={'task_id':task['task_id'],'table_name':definitions[task['table_id']]['name'],
                'completed_at_utc':task['completed_at_utc'],'rows':task['actual_rows'],
                'non_null_cells':None,'accepted':False,'error_code':None}
        try:
            receipt_path=root/'receipts'/(task['task_id']+'.json')
            if task['receipt_path'] != str(receipt_path.relative_to(root)):
                raise ValueError('receipt_identity_mismatch')
            receipt=json.loads(receipt_path.read_text())
            if (receipt.get('contract_version') != 4 or receipt.get('provider') != 'tej_smart_wizard'
                    or receipt.get('task_id') != task['task_id'] or receipt.get('table_id') != task['table_id']):
                raise ValueError('receipt_identity_mismatch')
            raw=(root/receipt['raw_path']).resolve()
            parquet=(root/receipt['parquet_path']).resolve()
            request=expand_request(json.loads(task['request_json']),definitions[task['table_id']],task['table_id'])
            if (raw.parent != root/'raw' or not raw.name.startswith(task['task_id']+'-') or raw.suffix != '.json'
                    or parquet != (root/parquet_relative(task,receipt)).resolve()
                    or raw.stat().st_size > 128*1024**2):
                raise ValueError('artifact_scope_mismatch')
            if any(_digest(path) != receipt[key] for path,key in ((raw,'raw_sha256'),(parquet,'parquet_sha256'))):
                raise ValueError('artifact_digest_mismatch')
            payload=interpreted_payload(root,task,json.loads(raw.read_text(encoding='utf-8-sig')))
            if payload.get('task_id') != task['task_id']:
                raise ValueError('source_task_mismatch')
            headers,rows,profile=validate_download_evidence(request,payload)
            if profile.get('source_period_key_contract'):
                if (receipt.get('source_period_key_contract')!=profile['source_period_key_contract']
                        or receipt.get('source_period_scope_rows')!=profile['source_period_scope_rows']
                        or receipt.get('omitted_query_grid_rows') is not None
                        or pq.read_schema(parquet).metadata.get(b'stockagent.source_period_key_contract')!=profile['source_period_key_contract'].encode()
                        or pq.read_table(parquet,columns=['_query_period'])['_query_period'].to_pylist()!=[period for _,period in profile['keys']]
                        or any(receipt.get(key)!=payload.get(key) for key in
                               ('source_period_interpretation_path','source_period_interpretation_sha256'))):
                    raise ValueError('source_period_key_mismatch')
                month_key_tasks+=1
            if profile.get('preview_header_mapping_contract') is not None:
                if (receipt.get('preview_header_mapping_contract')!=profile['preview_header_mapping_contract']
                        or receipt.get('native_source_headers')!=payload['cells'][0]
                        or receipt.get('source_key_headers')!=headers[:key_count(request)]
                        or pq.read_schema(parquet).names[:len(headers)]!=headers):
                    raise ValueError('source_header_mapping_mismatch')
                header_mapping_tasks+=1
            if key_count(request)==3:
                if (receipt.get('source_key_mode')!=3 or receipt.get('key_layout_contract')!=KEY3_CONTRACT
                        or '_native_record_key' not in pq.read_schema(parquet).names):
                    raise ValueError('source_key_layout_mismatch')
                key3_tasks+=1
            elif key_count(request)==1:
                if (receipt.get('source_key_mode')!=1 or receipt.get('key_layout_contract')!=KEY1_CONTRACT
                        or '_snapshot_observed_at_utc' not in pq.read_schema(parquet).names
                        or receipt.get('historical_values_reconstructed') is not False
                        or receipt.get('first_query_period') is not None or receipt.get('last_query_period') is not None):
                    raise ValueError('source_key_layout_mismatch')
                snapshot_tasks+=1
            elif receipt.get('key_layout_contract') is not None:
                raise ValueError('source_key_layout_mismatch')
            active=task.get('active_attempt_id')
            if active:
                prepared=root/'requests'/(active+'.json')
                stage,stage_path=query_stage(root,task,request,prepared)
                if (payload.get('query_attempt_id')!=active
                        or stage.get('preview_submission_contract')!=payload.get('preview_submission_contract')
                        or payload.get('source_outcome')!='explicit_empty_scope'
                        and payload.get('fresh_preview_transition_verified') is not True):
                    raise ValueError('desktop_attempt_mismatch')
            if receipt.get('desktop_attempt_contract') is not None:
                if (not active or receipt['desktop_attempt_contract']!=ATTEMPT_CONTRACT
                        or receipt.get('query_attempt_id')!=active
                        or receipt.get('prepared_request_path')!=str(prepared.relative_to(root))
                        or receipt.get('query_stage_path')!=str(stage_path.relative_to(root))
                        or receipt.get('prepared_request_sha256')!=_digest(prepared)
                        or receipt.get('query_stage_sha256')!=_digest(stage_path)):
                    raise ValueError('desktop_attempt_mismatch')
                bound_attempts+=1
            if not len(rows) == receipt['exported_rows'] == task['actual_rows'] == pq.read_metadata(parquet).num_rows:
                raise ValueError('row_count_mismatch')
            if (raw.stat().st_size != receipt['raw_bytes'] or parquet.stat().st_size != receipt['parquet_bytes']
                    or raw.stat().st_size+parquet.stat().st_size != task['actual_bytes']):
                raise ValueError('artifact_size_mismatch')
            counts=dict(zip(request['fields'],profile['non_null_counts']))
            if (counts != receipt['field_non_null_counts'] or sum(counts.values()) != receipt['exported_non_null_cells']
                    or task['expected_rows'] != receipt['requested_query_rows']):
                raise ValueError('source_count_mismatch')
            for name,count in counts.items():
                cells[task['table_id'],name]+=count
            result.update(accepted=True,non_null_cells=sum(counts.values()))
        except (ValueError,KeyError,TypeError,OSError) as exc:
            # Never echo source cells, account/Windows paths or exceptions in
            # a report. Detailed originals remain in the private source tree.
            safe={'receipt_identity_mismatch','artifact_scope_mismatch','artifact_digest_mismatch',
                  'source_task_mismatch','row_count_mismatch','artifact_size_mismatch','source_count_mismatch',
                  'source_key_layout_mismatch','desktop_attempt_mismatch','source_header_mapping_mismatch','source_period_key_mismatch'}
            result['error_code']=str(exc) if str(exc) in safe else 'local_receipt_or_source_validation_failed'
            failed+=1
        results.append(result)
    nonzero={key:count for key,count in cells.items() if count}
    registry_mismatches=sum(registry.get(key,0) != nonzero.get(key,0) for key in registry.keys()|nonzero.keys())
    worker=json.loads((root/'worker_status.json').read_text()) if (root/'worker_status.json').exists() else {}
    timings={}
    # Real read-only query benchmark: never call the writer's connect()/DDL.
    with closing(_read(root)) as con:
        for kind in ('discover','download'):
            samples=[]
            for _ in range(5):
                tick=time.perf_counter(); _ready_task(con,snapshot_at,kind,{'download_burst':4})
                samples.append(time.perf_counter()-tick)
            timings[kind]={'first_seconds':samples[0],'median_seconds':median(samples),'samples':len(samples)}
    report={'contract':'tej_local_receipt_integrity_audit_v4','started_at_utc':started.isoformat(),
            'sql_snapshot_at_utc':snapshot_at,'finished_at_utc':datetime.now(UTC).isoformat(),
            'provider':'tej_smart_wizard','source_scope_contract':SOURCE_SCOPE_CONTRACT,
            'completed_download_tasks_audited':len(tasks),'local_artifact_failures':failed,
            'key3_tasks_audited':key3_tasks,'receipts_with_attempt_sha_binding':bound_attempts,
            'key1_snapshot_tasks_audited':snapshot_tasks,
            'versioned_month_period_key_tasks_audited':month_key_tasks,
            'preview_header_mapping_tasks_audited':header_mapping_tasks,
            'legacy_receipts_without_attempt_sha_binding':len(tasks)-bound_attempts,
            'feature_count_mismatches':registry_mismatches,'accepted':bool(tasks) and failed==0 and registry_mismatches==0,
            'exported_rows':sum(r['rows'] or 0 for r in results if r['accepted']),
            'exported_non_null_cells':sum(r['non_null_cells'] or 0 for r in results if r['accepted']),
            'queue_states_at_snapshot':states,'blocked_tasks_at_snapshot':blocked,
            'queue_file_bytes':(root/'queue.sqlite3').stat().st_size,
            'worker':{'alive':_worker_alive(worker,datetime.now(UTC)),'state':worker.get('state'),
                      'observed_at_utc':worker.get('observed_at_utc')},
            'task_selection_benchmark':timings,'raw_values_exposed':False,'provider_queries_sent':0,
            'native_history_completeness_verified':False,'publication_timestamps_verified':False,
            'interpretation':'Local hashes, row/nonempty-cell counts and source key/schema validation only; later commits and vendor-native completeness are outside this snapshot.'}
    output.mkdir(parents=True)
    stream=io.StringIO(newline='');writer=csv.DictWriter(stream,list(results[0]) if results else ['task_id','accepted'])
    writer.writeheader();writer.writerows(results)
    atomic_write_text(output/'receipt_integrity.csv','\ufeff'+stream.getvalue())
    atomic_write_json(output/'audit.json',report)
    return report


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=ROOT/'data_tej')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    report=audit(args.root,args.output)
    print(json.dumps(report,ensure_ascii=False))
    return 0 if report['accepted'] else 1


if __name__=='__main__':
    raise SystemExit(main())
