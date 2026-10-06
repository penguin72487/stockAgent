#!/usr/bin/env python3
"""Balanced real-query TEJ benchmark through the unchanged canonical lifecycle.

Explicit bounded source requeries only. The live acquisition lock/gate protect
the ONE shared Wizard throughout; no receipt, cursor or query state is reset.
Each comparison retains its own original request, raw, Parquet and receipt.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.dataset_lock import exclusive_dataset_lock
from downloader.tej_history import (
    DesktopBridge, SOURCE_SCOPE_CONTRACT, company_code, connect, run_one,
    stable_id, task_request, validate_download_evidence,
)
from downloader.tej_startup import interactive_transport
from scripts.audit_tej_history import audit
from scripts.benchmark_tej_preview_readback import completed_source

CONTRACT = 'tej_balanced_canonical_real_query_full_flow_v1'


def read_snapshot(root: Path, case_ids: list[str], *, max_cases: int = 3) -> tuple[dict, list[dict], dict, dict]:
    if not 1 <= len(case_ids) <= max_cases or len(set(case_ids)) != len(case_ids):
        raise ValueError('Select bounded unique source-backed completed cases')
    with closing(sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True, timeout=2)) as con:
        con.row_factory = sqlite3.Row
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        row = con.execute("SELECT value FROM meta WHERE key='runtime_policy'").fetchone()
        policy = json.loads(row[0]) if row else {}
        cases = []
        for task_id in case_ids:
            row = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
            if row is None:
                raise ValueError('Exact original task required')
            task = dict(row)
            prepared, _ = completed_source(root, task_id, allow_empty=True)
            definition = dict(con.execute('SELECT * FROM tables WHERE table_id=?', (task['table_id'],)).fetchone())
            features = [dict(r) for r in con.execute('SELECT * FROM features WHERE table_id=?', (task['table_id'],))]
            request = task_request(root, task)
            if request.get('source_key_mode', 2) == 1:
                raise ValueError('Snapshot observations are not stable historical A/B cases')
            cases.append({'label': definition['name'], 'task': task, 'definition': definition,
                          'features': features, 'request': request,
                          'original_prepared_sha256': hashlib.sha256(prepared.read_bytes()).hexdigest()})
        dense = con.execute("SELECT * FROM tables WHERE name='TSE/OTC Unadjusted_Price(Daily)'").fetchone()
        if dense is None:
            raise ValueError('Registered native price table required for dense acceptance')
        dense = dict(dense)
        dense_features = [dict(r) for r in con.execute('SELECT * FROM features WHERE table_id=?', (dense['table_id'],))]
    return config, cases, {'definition': dense, 'features': dense_features}, policy


def _insert(con, table: str, record: dict) -> None:
    # Column identifiers come only from the maintained schema, not source data.
    columns = [r[1] for r in con.execute(f'PRAGMA table_info({table})') if r[1] in record]
    con.execute(f'INSERT INTO {table} ({",".join(columns)}) VALUES ({",".join("?" for _ in columns)})',
                [record[c] for c in columns])


def with_field_batch(cases: list[dict], task_id: str | None) -> list[dict]:
    """Optional adjacent field slice: same source axes, no production mutation.

    A different schema gives a distinguishable Preview transition. Repeating an
    identical query is not an acceptable shortcut around source-transition proof.
    """
    if task_id is None:
        return cases
    matches = [case for case in cases if case['task']['task_id'] == task_id]
    if len(matches) != 1:
        raise ValueError('Field-batch scope must name one selected original completed case')
    original = matches[0]
    fields = original['request'].get('fields')
    if original['task']['kind'] != 'download' or not isinstance(fields, list) or len(fields) < 2:
        raise ValueError('Field-batch comparison requires a multi-field historical download')
    request = {**original['request'], 'fields': fields[:len(fields)//2]}
    task = {**original['task'], 'task_id': stable_id(request)}
    if task['task_id'] == task_id or any(case['task']['task_id'] == task['task_id'] for case in cases):
        raise ValueError('Field-batch comparison must retain distinct attempt scopes')
    result = []
    for case in cases:
        result.append(case)
        if case is original:
            result.append({**original, 'request': request, 'task': task,
                           'label': original['label']+'_adjacent_field_batch'})
    return result


def prepare_trial(path: Path, case: dict, config: dict, policy: dict, relay: dict) -> None:
    if path.exists():
        raise FileExistsError('Retain each independent source comparison')
    with closing(connect(path)) as con, con:
        for key, value in (('config', config), ('runtime_policy', policy)):
            con.execute('INSERT INTO meta VALUES (?,?)', (key, json.dumps(value)))
        _insert(con, 'tables', case['definition'])
        for feature in case['features']:
            _insert(con, 'features', {**feature, 'exported_non_null_cells': None,
                                     'first_query_period': None, 'last_query_period': None})
        task = case['task']
        _insert(con, 'tasks', {**{k: task[k] for k in
                                ('task_id','table_id','kind','phase','priority','expected_rows','scope_contract')},
                               'request_json':json.dumps(case['request'], ensure_ascii=False), 'state':'pending'})
    atomic_write_json(path/'desktop_transport_session.json', relay)


def sample_result(path: Path, case: dict, repo: Path, task: dict, wall: float) -> dict:
    raw = path/task['output_path']
    payload = json.loads(raw.read_text(encoding='utf-8-sig'))
    sample = {'case': case['label'], 'kind': task['kind'], 'task_id': task['task_id'],
              'root': str(path.relative_to(path.parents[1])), 'wall_seconds': wall,
              'lifecycle_seconds': task['seconds'], 'raw_bytes': raw.stat().st_size,
              'bridge_sha256': hashlib.sha256((repo/'scripts/tej_smart_wizard_bridge.ps1').read_bytes()).hexdigest()}
    if task['kind'] == 'download':
        headers, rows, profile = validate_download_evidence(case['request'], payload)
        signature = {'headers': headers, 'rows': sorted(rows, key=lambda r: json.dumps(r, default=str, ensure_ascii=False))}
        sample.update(rows=len(rows), non_null_cells=sum(profile['non_null_counts']),
                      semantic_sha256=hashlib.sha256(json.dumps(signature,sort_keys=True,default=str,ensure_ascii=False).encode()).hexdigest(),
                      requested_companies=len(case['request']['company_labels']),
                      requested_dates=len(case['request']['date_labels']), fields=len(case['request']['fields']))
        accepted = audit(path, path/'acceptance'/task['task_id'], task_ids=[task['task_id']])
        if not accepted['accepted']:
            raise ValueError('Full raw/Parquet/schema/keys/counts/receipt acceptance failed')
        stage = json.loads(raw.with_suffix('.json.stage.json').read_text(encoding='utf-8-sig'))
        sample['scope_preparation_seconds'] = stage.get('scope_preparation_seconds')
        if type(stage.get('fresh_company_selection_reused')) is bool:
            sample['fresh_company_selection_reused'] = stage['fresh_company_selection_reused']
    else:
        signature = {k: payload.get(k) for k in ('type','smart_id','table','fields','company_labels','date_labels','source_key_mode')}
        sample.update(rows=0, non_null_cells=0, semantic_sha256=hashlib.sha256(json.dumps(signature,sort_keys=True,ensure_ascii=False).encode()).hexdigest())
    flow = raw.with_suffix('.json.flow.json')
    if flow.exists():
        timing = json.loads(flow.read_text(encoding='utf-8-sig'))
        if timing.get('contract') != 'tej_monotonic_windows_full_flow_v1' or timing.get('task_id') != task['task_id']:
            raise ValueError('Flow telemetry belongs to another attempt')
        sample['windows_flow'] = {k: timing[k] for k in ('phases','windows_bridge_seconds','windows_cpu_seconds')}
    return sample


def execute_case(path: Path, case: dict, repo: Path, session: dict, config: dict, policy: dict, relay: dict) -> dict:
    prepare_trial(path, case, config, policy, relay)
    with exclusive_dataset_lock(path/'.download.lock', provider='tej_full_flow_comparison', timeout_seconds=0):
        started = time.monotonic()
        state = run_one(path, DesktopBridge(repo, session), kind=case['task']['kind'])
        wall = time.monotonic()-started
    if state != 'completed_task':
        raise RuntimeError('Source comparison did not complete; exact trial evidence retained; no automatic repetition')
    with closing(connect(path)) as con:
        task = dict(con.execute('SELECT * FROM tasks WHERE task_id=?', (case['task']['task_id'],)).fetchone())
    return sample_result(path, case, repo, task, wall)


def benchmark_queue(root: Path, baseline: Path, output: Path, case_ids: list[str]) -> dict:
    """Joined actual scheduler→query→receipt chain, exact-priority ties only."""
    root,baseline,output=root.resolve(),baseline.resolve(),output.resolve()
    if output.exists() or not baseline.is_file() or baseline.stat().st_size>256*1024:
        raise ValueError('New evidence and preserved baseline required')
    with exclusive_dataset_lock(root/'.download.lock',provider='tej_full_flow_queue_comparison',timeout_seconds=180):
        config,cases,_,policy=read_snapshot(root,case_ids,max_cases=4)
        groups={case['task']['table_id'] for case in cases}
        if len(cases)!=4 or len(groups)!=2 or any(sum(c['task']['table_id']==g for c in cases)!=2 for g in groups):
            raise ValueError('Exactly two original scopes from each of two tables required')
        for case in cases:
            case['task']={**case['task'],'priority':100} # Exact priority ties in BOTH policies.
        relay=interactive_transport(root);session=json.loads((root/'desktop_session.json').read_text())
        output.mkdir(parents=True,mode=0o700);repos={}
        for mode,source in (('baseline',baseline),('candidate',ROOT/'scripts/tej_smart_wizard_bridge.ps1')):
            repo=output/mode;atomic_write_bytes(repo/'scripts/tej_smart_wizard_bridge.ps1',source.read_bytes());repos[mode]=repo
        marker=json.dumps({'contract':CONTRACT,'token':uuid.uuid4().hex,'evidence_root':str(output)})
        with closing(connect(root)) as con,con:
            if con.execute("SELECT 1 FROM meta WHERE key='desktop_interface_recovery_required'").fetchone():
                raise ValueError('Existing source barrier must be resolved first')
            con.execute("INSERT INTO meta VALUES ('desktop_interface_recovery_required',?)",(marker,))
        samples=[];chains=[];source_cases={c['task']['task_id']:c for c in cases}
        try:
            for pass_id,modes in enumerate((('baseline','candidate'),('candidate','baseline'))):
                for mode in modes:
                    trial=output/'trials'/f'{pass_id}-{mode}'
                    bounded_policy={**policy,'download_table_locality_burst':1 if mode=='baseline' else 4}
                    prepare_trial(trial,cases[0],config,bounded_policy,relay)
                    with closing(connect(trial)) as con,con:
                        for case in cases[1:]:
                            if not con.execute('SELECT 1 FROM tables WHERE table_id=?',(case['task']['table_id'],)).fetchone():
                                _insert(con,'tables',case['definition'])
                                for f in case['features']:_insert(con,'features',{**f,'exported_non_null_cells':None,'first_query_period':None,'last_query_period':None})
                            _insert(con,'tasks',{**{k:case['task'][k] for k in ('task_id','table_id','kind','phase','priority','expected_rows','scope_contract')},
                                'state':'pending','request_json':json.dumps(case['request'],ensure_ascii=False)})
                    order=[];full_started=time.monotonic();core_seconds=0.0
                    with exclusive_dataset_lock(trial/'.download.lock',provider='tej_full_flow_queue_trial',timeout_seconds=0):
                        bridge=DesktopBridge(repos[mode],session)
                        for _ in cases:
                            tick=time.monotonic();state=run_one(trial,bridge);wall=time.monotonic()-tick;core_seconds+=wall
                            if state!='completed_task':raise RuntimeError('Queue comparison stopped; retained original evidence; no automatic source repetition')
                            with closing(connect(trial)) as con:
                                task=dict(con.execute("SELECT * FROM tasks WHERE state='complete' ORDER BY completed_at_utc DESC LIMIT 1").fetchone())
                            case=source_cases[task['task_id']]
                            sample=sample_result(trial,case,repos[mode],task,wall);sample.update(mode=mode,pass_id=pass_id)
                            order.append(case['label']);samples.append(sample)
                            atomic_write_json(output/'progress.json',{'contract':CONTRACT,'samples':samples,'accepted':False,'source_values_exposed':False})
                            print(json.dumps({k:sample[k] for k in ('mode','case','wall_seconds','rows')},ensure_ascii=False),flush=True)
                    chain={'mode':mode,'pass_id':pass_id,'table_order':order,'wall_seconds':time.monotonic()-full_started,
                           'acquisition_core_seconds':core_seconds,'table_switches':sum(a!=b for a,b in zip(order,order[1:]))}
                    chains.append(chain);atomic_write_json(output/f'chain-{pass_id}-{mode}.json',chain)
            for task_id in source_cases:
                if len({s['semantic_sha256'] for s in samples if s['task_id']==task_id})!=1:
                    raise ValueError('Same original source values differ; no accepted speed gain')
            summary={}
            for mode in repos:
                own=[s for s in samples if s['mode']==mode];ch=[s for s in chains if s['mode']==mode]
                summary[mode]={'exported_rows':sum(s['rows'] for s in own),'tasks':len(own),
                    'sum_complete_wall_seconds':sum(s['wall_seconds'] for s in own),
                    'sum_chain_wall_seconds':sum(c['wall_seconds'] for c in ch),
                    'chain_wall_seconds':[c['wall_seconds'] for c in ch],
                    'table_switches':sum(c['table_switches'] for c in ch)}
            with closing(connect(root)) as con,con:
                own=con.execute("SELECT value FROM meta WHERE key='desktop_interface_recovery_required'").fetchone()
                if own is None or own[0]!=marker:raise ValueError('Benchmark gate changed; never clear another owner')
                con.execute("DELETE FROM meta WHERE key='desktop_interface_recovery_required' AND value=?",(marker,))
            report={'contract':CONTRACT,'accepted':True,'source_values_equal':True,'source_values_exposed':False,
                'production_receipts_modified':False,'data_queries_sent':len(samples),'summary':summary,'chains':chains,'samples':samples,
                'wall_speedup':summary['baseline']['sum_chain_wall_seconds']/summary['candidate']['sum_chain_wall_seconds'],
                'interpretation':'Balanced same original scopes, full actual scheduler/query/validation/Parquet/receipt chain including additional audits; bounded exact-priority table locality, not full-history coverage or a universal maximum'}
            atomic_write_json(output/'acceptance.json',report);return report
        except BaseException:
            atomic_write_json(output/'failed.json',{'contract':CONTRACT,'accepted':False,'production_gate_retained':True,'samples':samples,'source_values_exposed':False})
            raise


def benchmark(root: Path, baseline: Path, output: Path, case_ids: list[str], *, dense_start: str, dense_end: str,
              candidate: Path | None = None, field_batch_case: str | None = None) -> dict:
    root, baseline, output = root.resolve(), baseline.resolve(), output.resolve()
    if output.exists() or not baseline.is_file() or baseline.stat().st_size > 256*1024:
        raise ValueError('New evidence directory and preserved bounded baseline required')
    if not ('2020-01-01' <= dense_start <= dense_end < datetime.now(UTC).date().isoformat()):
        raise ValueError('Dense test must use closed historical dates from 2020 onward')
    candidate=(candidate or ROOT/'scripts/tej_smart_wizard_bridge.ps1').resolve()
    if not candidate.is_file() or candidate.stat().st_size>256*1024:
        raise ValueError('Reviewed bounded candidate bridge required')
    with exclusive_dataset_lock(root/'.download.lock', provider='tej_full_flow_performance', timeout_seconds=180):
        config, cases, dense, policy = read_snapshot(root, case_ids)
        cases = with_field_batch(cases, field_batch_case)
        relay = interactive_transport(root)
        session = json.loads((root/'desktop_session.json').read_text())
        output.mkdir(parents=True, mode=0o700)
        repos = {}
        for mode, source in (('baseline', baseline), ('candidate', candidate)):
            repo = output/mode
            atomic_write_bytes(repo/'scripts/tej_smart_wizard_bridge.ps1', source.read_bytes())
            repos[mode] = repo
        token = uuid.uuid4().hex
        marker = json.dumps({'contract': CONTRACT, 'token': token, 'evidence_root': str(output)})
        with closing(connect(root)) as con, con:
            if con.execute("SELECT 1 FROM meta WHERE key='desktop_interface_recovery_required'").fetchone():
                raise ValueError('Existing desktop barrier must be resolved first')
            con.execute("INSERT INTO meta VALUES ('desktop_interface_recovery_required',?)", (marker,))
        samples = []
        # A failed actual query intentionally retains the gate/trial for exact
        # recovery. Never restart the live source after an ambiguous benchmark.
        try:
            definition = dense['definition']; fields = json.loads(definition['fields_json'])
            request = {'contract_version': 4, 'action':'plan', 'type':definition['query_type'],
                       'smart_id':definition['smart_id'], 'table':definition['name'],
                       'catalog_fields':fields, 'fields':fields[:28], 'frequency':'daily',
                       'start':dense_start, 'end':dense_end, 'max_rows':10000, 'max_cells':400000}
            discover = {**dense, 'label':'dense_price_discovery', 'request':request,
                        'task':{'task_id':stable_id(request), 'table_id':definition['table_id'],
                                'kind':'discover','phase':'P1','priority':1,'expected_rows':None,
                                'scope_contract':SOURCE_SCOPE_CONTRACT}}
            discovery_sample = execute_case(output/'trials'/'axes', discover, repos['candidate'], session, config, policy, relay)
            atomic_write_json(output/'discovery_setup.json', discovery_sample)
            with closing(connect(output/'trials'/'axes')) as con:
                task = con.execute('SELECT output_path FROM tasks WHERE task_id=?',(discover['task']['task_id'],)).fetchone()
            axes = json.loads((output/'trials'/'axes'/task['output_path']).read_text(encoding='utf-8-sig'))
            wanted = {'2330','2317','2454','2303'}
            companies = [label for label in axes['company_labels'] if company_code(label) in wanted]
            if {company_code(label) for label in companies} != wanted:
                raise ValueError('All four exact price companies must be source-attested')
            dates = axes['date_labels']
            if not dates or len(dates)*len(companies) > 10000:
                raise ValueError('Dense verified date/company grid outside bounded capacity')
            req = {**request, 'action':'download', 'company_labels':companies, 'date_labels':dates, 'source_key_mode':2}
            cases += [{**dense,'label':'dense_price_download','request':req,
                       'task':{**discover['task'],'task_id':stable_id(req),'kind':'download','expected_rows':len(dates)*len(companies)}}, discover]
            # Balanced order, with identical case order in each complete chain.
            # Switching tables/discovery, launch, scope, query, FULL readback,
            # validation, normalization, Parquet and metadata remain included.
            for pass_id, modes in enumerate((('baseline','candidate'),('candidate','baseline'))):
                for mode in modes:
                    chain_started = time.monotonic()
                    for number, case in enumerate(cases):
                        trial = output/'trials'/f'{pass_id}-{mode}-{number}'
                        sample = execute_case(trial, case, repos[mode], session, config, policy, relay)
                        sample.update(mode=mode, pass_id=pass_id)
                        samples.append(sample)
                        atomic_write_json(output/'progress.json', {'contract':CONTRACT,'samples':samples,
                            'accepted':False,'source_values_exposed':False,'finished':False})
                        print(json.dumps({k:sample[k] for k in ('mode','case','kind','wall_seconds','rows')},ensure_ascii=False),flush=True)
                    atomic_write_json(output/f'chain-{pass_id}-{mode}.json',
                                      {'wall_seconds':time.monotonic()-chain_started,'mode':mode,'pass_id':pass_id})
            for case in cases:
                if len({r['semantic_sha256'] for r in samples if r['case']==case['label']}) != 1:
                    raise ValueError('Balanced comparison has different actual source values/axes; do not claim a speed gain')
            summary = {}
            for mode in repos:
                own = [s for s in samples if s['mode']==mode]
                summary[mode] = {'tasks':len(own),'exported_rows':sum(s['rows'] for s in own),
                    'sum_complete_wall_seconds':sum(s['wall_seconds'] for s in own),
                    'median_task_wall_seconds':statistics.median(s['wall_seconds'] for s in own),
                    'chain_wall_seconds':[json.loads((output/f'chain-{p}-{mode}.json').read_text())['wall_seconds'] for p in range(2)]}
            # No benchmark authority is inherited by the source scheduler.
            with closing(connect(root)) as con, con:
                row = con.execute("SELECT value FROM meta WHERE key='desktop_interface_recovery_required'").fetchone()
                if row is None or row[0] != marker:
                    raise ValueError('Desktop recovery gate changed; never clear another owner')
                con.execute("DELETE FROM meta WHERE key='desktop_interface_recovery_required' AND value=?", (marker,))
            report = {'contract':CONTRACT,'accepted':True,'observed_at_utc':datetime.now(UTC).isoformat(),
                      'source_values_equal':True,'source_values_exposed':False,'production_receipts_modified':False,
                      'data_queries_sent':sum(s['kind']=='download' for s in samples),
                      'metadata_plans':1+sum(s['kind']=='discover' for s in samples),
                      'summary':summary,'samples':samples,
                      'wall_speedup':summary['baseline']['sum_complete_wall_seconds']/summary['candidate']['sum_complete_wall_seconds'],
                      'interpretation':'Balanced repeated SAME-scope complete lifecycle on this owned desktop; not official quota, full-history coverage or universal theoretical maximum'}
            atomic_write_json(output/'acceptance.json', report)
            return report
        except BaseException:
            atomic_write_json(output/'failed.json', {'contract':CONTRACT,'accepted':False,
                'source_values_exposed':False,'production_gate_retained':True,'samples':samples,
                'next_action':'Inspect exact retained trial/attempt before resuming; no automatic source repetition'})
            raise


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,default=ROOT/'data_tej')
    p.add_argument('--baseline',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--case',action='append',required=True)
    p.add_argument('--dense-start',default='2024-01-01')
    p.add_argument('--dense-end',default='2026-09-25')
    p.add_argument('--allow-source-requeries',action='store_true',help='Explicit bounded A/B source-usage acceptance; never implied by a read-only profile')
    p.add_argument('--compare-queue-locality',action='store_true',help='Four original scopes in two exact-priority tables, actual canonical scheduler included')
    p.add_argument('--candidate',type=Path,help='Reviewed candidate snapshot; production bridge is unchanged during an experiment')
    p.add_argument('--field-batch-case',help='One selected original case; add an adjacent half-field query on its identical axes')
    a=p.parse_args(argv)
    if not a.allow_source_requeries:p.error('Real full-flow benchmark requires --allow-source-requeries')
    if a.compare_queue_locality and a.candidate:p.error('Explicit candidate is supported only for the full-flow comparison')
    if a.compare_queue_locality and a.field_batch_case:p.error('Field-batch cases require the full-flow comparison')
    r=(benchmark_queue(a.root,a.baseline,a.output,a.case) if a.compare_queue_locality else
       benchmark(a.root,a.baseline,a.output,a.case,dense_start=a.dense_start,dense_end=a.dense_end,
                 candidate=a.candidate,field_batch_case=a.field_batch_case))
    print(json.dumps({k:v for k,v in r.items() if k!='samples'},ensure_ascii=False))
    return 0


if __name__=='__main__':raise SystemExit(main())
