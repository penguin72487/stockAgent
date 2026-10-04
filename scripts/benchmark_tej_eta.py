"""Measure full TEJ metadata projection, forbidding source values and networking."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sqlite3
import sys
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.tej_eta import quantile
from stockagent.live import tej_dashboard


def benchmark(repo: Path, output: Path, repeats: int) -> dict:
    if output.exists():
        raise FileExistsError('Do not overwrite benchmark evidence')
    if not 3 <= repeats <= 50:
        raise ValueError('Use a bounded 3–50 repetitions')
    repo=repo.resolve(); root=repo/'data_tej'; opened=[]; forbidden=[]; active=True

    def audit(event,args):
        if not active:
            return  # Python audit hooks persist; this guard is run-scoped.
        if event=='socket.connect':
            forbidden.append('network');raise RuntimeError('No network in TEJ ETA benchmark')
        if event=='open' and isinstance(args[0],(str,bytes)):
            name=str(args[0]);opened.append(name)
            if any(name.startswith(str(root/directory)+'/') for directory in ('raw','datasets','parquet')):
                forbidden.append('raw_source');raise RuntimeError('No source values in TEJ ETA benchmark')

    sys.addaudithook(audit)
    original=tej_dashboard._read; sql_reads=set()

    def read(directory):
        con=original(directory)
        def authorize(action,table,column,*_):
            if action==sqlite3.SQLITE_READ:
                sql_reads.add((table,column))
                if table=='download_plans' and column=='plan_json':
                    forbidden.append('plan_blob');return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        con.set_authorizer(authorize)
        return con

    timings=[]; fingerprints=[]
    try:
        tej_dashboard._read=read
        for _ in range(repeats):
            began=perf_counter();status=tej_dashboard.build_tej_public_status(repo)
            timings.append(perf_counter()-began)
            if status['state'] in {'not_registered','metadata_unavailable'}:
                raise RuntimeError('Benchmark requires a readable registered catalog')
            fingerprints.append(status['eta']['forecast']['input_sha256'])
    finally:
        tej_dashboard._read=original
        active=False
    forecast=status['eta']['forecast']
    result={'contract':'tej_metadata_eta_benchmark_v1','observed_at_utc':datetime.now(UTC).isoformat(),
            'scope':'full_status_metadata_sql_disk_headroom_and_all_table_scenarios_not_GUI_or_training',
            'repetitions':repeats,'first_seconds':timings[0],
            'warm_p50_seconds':quantile(timings[1:],.5),'warm_p95_seconds':quantile(timings[1:],.95),
            'timings_seconds':timings,'tables':status['catalog']['tables'],'fields':status['catalog']['fields'],
            'modeled_remaining_queries':forecast['global_scenarios']['middle']['remaining_queries'],
            'eta_contract':forecast['contract'],'input_fingerprints':sorted(set(fingerprints)),
            'stable_workload_inputs':len(set(fingerprints))==1,
            'provider_requests_sent':0,'raw_value_files_read':0,'plan_blobs_read':0,
            'forbidden_actions':forbidden,'sql_metadata_columns':sorted(sql_reads),
            'process_only_open_calls':len(opened),
            'interpretation':'Host-specific timings; not external download throughput, whole-system load or GPU evidence'}
    atomic_write_json(output,result)
    return result


def main(argv=None):
    import json
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=ROOT)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--repeats',type=int,default=10)
    args=parser.parse_args(argv)
    result=benchmark(args.root,args.output,args.repeats)
    print(json.dumps({key:value for key,value in result.items() if key not in {'sql_metadata_columns','timings_seconds'}},ensure_ascii=False))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
