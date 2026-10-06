#!/usr/bin/env python3
"""Real Windows entry-gate acceptance using isolated fake scripts, never TEJ.

The syntax-error fixture is never entered. The after-entry fixture just throws;
its intentionally unknown outcome must not become a negative submission proof.
Uses the production admission, immutable release and error classifier unchanged.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from downloader.artifact_io import atomic_write_json,atomic_write_text
from downloader.tej_history import DesktopBridge,BeforeDataQueryError,connect


def verify(output:Path)->dict:
    output=output.resolve()
    if output.exists():raise FileExistsError('Retain original entry-gate acceptance evidence')
    output.mkdir(parents=True)
    results=[]
    for case,code in [('before_entry','function Broken {'),('after_entry',"throw 'Owned fake after-entry failure; no TEJ interaction'")]:
        repo=output/case;root=repo/'data';source=repo/'scripts/tej_smart_wizard_bridge.ps1'
        atomic_write_text(source,code)
        request={'action':'download','contract_version':4,'type':'fixture','smart_id':'fixture','table':'fixture',
                 'fields':['fixture'],'company_labels':['fixture'],'date_labels':['2014/01/02'],
                 'max_cells':100,'max_rows':10}
        task_id=hashlib.sha256(case.encode()).hexdigest()[:24]
        with closing(connect(root)) as con,con:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state) "
                "VALUES (?,?,'download','P1',1,?,'running')",(task_id,task_id,json.dumps(request)))
            task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone())
        bridge=DesktopBridge(repo,{'TejProcessId':1,'ExpectedWindow':2,'ExpectedTitle':'fixture',
                                  'ExpectedWorkbook':'fixture','ExpectedExcelWindow':3})
        try:
            bridge.execute(root,task)
            raise AssertionError('Fault-injection case unexpectedly succeeded')
        except BeforeDataQueryError as exc:
            assert case=='before_entry' and exc.error_code=='local_query_preparation_failed_before_preview'
        except RuntimeError:
            assert case=='after_entry'
        with closing(connect(root)) as con:
            attempt=dict(con.execute('SELECT * FROM desktop_attempts WHERE task_id=?',(task_id,)).fetchone())
        raw=root/attempt['raw_path'];outcome=raw.with_suffix('.json.outcome.json')
        assert not raw.exists() and not raw.with_suffix('.json.stage.json').exists()
        if case=='before_entry':
            proof=json.loads(outcome.read_text(encoding='utf-8-sig'))
            assert proof['bridge_invoked'] is False and proof['market_data_query_submission_possible'] is False
            assert proof['query_attempt_id']==attempt['attempt_id']
            assert attempt['state']=='proven_not_submitted'
        else:
            assert not outcome.exists() and attempt['state']=='unknown_outcome'
        results.append({'case':case,'accepted':True,'attempt_state':attempt['state'],
                        'negative_proof_present':outcome.exists(),'tej_or_excel_accessed':False})
    report={'contract':'real_windows_bridge_entry_fault_injection_v1','observed_at_utc':datetime.now(UTC).isoformat(),
            'accepted':True,'provider_queries_sent':0,'cases':results}
    atomic_write_json(output/'entry_gate_acceptance.json',report)
    return report


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    print(json.dumps(verify(args.output),ensure_ascii=False));return 0


if __name__=='__main__':raise SystemExit(main())
