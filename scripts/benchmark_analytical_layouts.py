#!/usr/bin/env python3
"""Measure real service-table queries, Parquet layout and canonical increments.

Private bounded engineering projection only. It neither produces a training
dataset nor publishes to the fleet cold store. All finite values, nulls and NaN
masks are verified, without rounding prices or changing feature eligibility.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
import shutil
import statistics
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.verify_lakehouse_engines import sha256,same_values,inventory,sql_text,connection
from stockagent.remote_build import observe_node
from stockagent.runtime_identity import runtime_identity

COLUMNS=['date','symbol','twpub_official_trading_volume_raw','twpub_official_trading_value_raw']
KEYS=(('date','ascending'),('symbol','ascending'))
SYMBOLS=['0050','2330','2317','2454']


def query(engine,path,cutoff,threads):
    from stockagent.data.columnar_lake import read_daily_projection
    return read_daily_projection(path,columns=COLUMNS,symbols=SYMBOLS,start_date=cutoff,
                                 engine=engine,duckdb_threads=threads)


def measure(engine,path,cutoff,expected,output,threads):
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    started=time.perf_counter()
    result=query(engine,path,cutoff,threads)
    result=result.select(COLUMNS).cast(expected.schema)
    result=result.take(pc.sort_indices(result,sort_keys=list(KEYS)))
    pq.write_table(result,output,compression='zstd')
    same_values(pq.read_table(output),expected,sort_keys=KEYS)
    digest=sha256(output)
    return {'engine':engine,'complete_wall_seconds':time.perf_counter()-started,
            'result_rows':result.num_rows,'result_sha256':digest,'result_bytes':output.stat().st_size}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--scratch-root',required=True,type=Path,
                        help='fresh private data-only directory outside any Git worktree')
    args=parser.parse_args()
    source=args.source.resolve(strict=True);output=args.output.absolute()
    output.mkdir(parents=True,mode=0o700,exist_ok=False)
    scratch=args.scratch_root.absolute();scratch.mkdir(parents=True,mode=0o700,exist_ok=False)
    profile=observe_node(source.parent,output)
    if profile['limits']['memory_headroom_bytes']<4*1024**3 or shutil.disk_usage(output).free<source.stat().st_size*4:
        raise ValueError('bounded full-row projection needs measured memory/scratch headroom')
    threads=min(4,profile['limits']['cpu_worker_budget'])
    os.environ['POLARS_MAX_THREADS']=str(threads)
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    pa.set_cpu_count(threads);pa.set_io_thread_count(threads)
    capture_started=time.perf_counter();source_sha=sha256(source)
    private=output/'source.parquet';shutil.copyfile(source,private);private.chmod(0o400)
    if sha256(private)!=source_sha or sha256(source)!=source_sha:
        raise ValueError('service table changed during capture')
    capture_seconds=time.perf_counter()-capture_started
    projected=pq.read_table(private,columns=COLUMNS)
    maximum=pc.max(projected['date']).as_py()
    cutoff=date(maximum.year-1,1,1)
    expected=query('arrow',private,cutoff,threads)
    expected=expected.take(pc.sort_indices(expected,sort_keys=list(KEYS)))
    receipt={'schema_version':1,'source':str(source),'source_sha256':source_sha,
             'source_rows':projected.num_rows,'columns':COLUMNS,'query_symbols':SYMBOLS,
             'query_cutoff':cutoff.isoformat(),'query_rows':expected.num_rows,
             'node_profile':profile,'threads_per_engine':threads,'runtime':runtime_identity(),
             'script_sha256':sha256(Path(__file__).resolve()),'capture_wall_seconds':capture_seconds,
             'cache_scope':'OS page cache not evicted; interleaved complete warm-cache query/write/read/verification'}
    results=[]
    for iteration in range(3):
        engines=['arrow','polars','duckdb'];engines=engines[iteration:]+engines[:iteration]
        for engine in engines:
            row=measure(engine,private,cutoff,expected,output/f'query-{engine}-{iteration}.parquet',threads)
            row['iteration']=iteration;results.append(row)
            print(json.dumps({'event':'query_measured',**row}),flush=True)
    means={engine:statistics.mean(p['complete_wall_seconds'] for p in results if p['engine']==engine)
           for engine in ('arrow','polars','duckdb')}
    steady={engine:statistics.mean(p['complete_wall_seconds'] for p in results if p['engine']==engine and p['iteration']>0)
            for engine in ('arrow','polars','duckdb')}
    receipt.update(query_measurements=results,query_mean_complete_seconds=means,
        steady_complete_seconds=steady,query_winner=min(means,key=means.get),steady_query_winner=min(steady,key=steady.get),
        columnar_owner_sha256=sha256(ROOT/'stockagent/data/columnar_lake.py'))
    atomic_write_json(output/'query-acceptance.json',receipt)
    # Materialize only four existing service fields; both layout candidates
    # contain every actual source row. This is not a panel/training ABI.
    layout_start=time.perf_counter()
    ordered=projected.take(pc.sort_indices(projected,sort_keys=list(KEYS)))
    mono=output/'monolithic.parquet';pq.write_table(ordered,mono,compression='zstd',row_group_size=131072)
    mono_build=time.perf_counter()-layout_start
    years=sorted(set(pc.year(ordered['date']).to_pylist()))
    distinct_dates=pc.unique(ordered['date']).to_pylist();distinct_dates.sort()
    delta_cutoff=distinct_dates[-min(10,len(distinct_dates))]
    base=ordered.filter(pc.less(ordered['date'],pa.scalar(delta_cutoff)))
    partitioned=output/'partitioned';partitioned.mkdir()
    build_start=time.perf_counter()
    for year in years:
        data=base.filter(pc.equal(pc.year(base['date']),year))
        if not data.num_rows:continue
        directory=partitioned/f'year={year}';directory.mkdir()
        pq.write_table(data,directory/'observations.parquet',compression='zstd',row_group_size=131072)
    base_build=time.perf_counter()-build_start
    before=inventory(partitioned)
    # Reuse the actual publication owner in a private offline trial tree.
    from stockagent.data_sync.packed_snapshots import publish_packed_snapshot,verify_packed_snapshot,fetch_packed_snapshot
    cold=scratch/'private-packed'
    first=publish_packed_snapshot(cold,'technology-layout-trial',partitioned,node_id='technology-trial',
                                  pack_buckets=8)
    unchanged=publish_packed_snapshot(cold,'technology-layout-trial',partitioned,pack_buckets=8)
    if first.manifest_sha256!=unchanged.manifest_sha256:
        raise ValueError('unchanged layout must be a semantic publication no-op')
    increment_start=time.perf_counter()
    year=maximum.year
    current=ordered.filter(pc.equal(pc.year(ordered['date']),year))
    directory=partitioned/f'year={year}';directory.mkdir(exist_ok=True)
    pq.write_table(current,directory/'observations.parquet',compression='zstd',row_group_size=131072)
    after=inventory(partitioned)
    changed=[name for name in after if before.get(name)!=after[name]]
    if changed!=[f'year={year}/observations.parquet']:
        raise ValueError('the actual ten-session delta rewrote unrelated years')
    second=publish_packed_snapshot(cold,'technology-layout-trial',partitioned,pack_buckets=8)
    cold_proof=verify_packed_snapshot(cold,second)
    restored=fetch_packed_snapshot(cold,scratch/'private-materialized',second)
    for name,row in after.items():
        if not (restored/name).is_file() or sha256(restored/name)!=row['sha256']:
            raise ValueError('canonical packed reconstruction differs')
    same_values(ds.dataset(partitioned,format='parquet',partitioning='hive').to_table(columns=COLUMNS),ordered,sort_keys=KEYS)
    same_values(ds.dataset(restored,format='parquet',partitioning='hive').to_table(columns=COLUMNS),ordered,sort_keys=KEYS)
    increment_seconds=time.perf_counter()-increment_start
    first_objects={row['sha256'] for row in first.manifest['archive']['objects']}
    second_objects={row['sha256'] for row in second.manifest['archive']['objects']}
    layout_results=[]
    for iteration in range(3):
        for layout,path in (('monolithic',mono),('year_partitioned',partitioned)):
            row=measure('arrow',path,cutoff,expected,output/f'layout-{layout}-{iteration}.parquet',threads)
            row.update(layout=layout,iteration=iteration);layout_results.append(row)
    layout_means={name:statistics.mean(p['complete_wall_seconds'] for p in layout_results if p['layout']==name)
                  for name in ('monolithic','year_partitioned')}
    receipt.update(state='accepted',source_unchanged=sha256(source)==source_sha,
        layout_measurements=layout_results,layout_query_mean_complete_seconds=layout_means,
        monolithic_build_seconds=mono_build,partitioned_base_build_seconds=base_build,
        incremental_complete_seconds=increment_seconds,incremental_actual_cutoff=delta_cutoff.isoformat(),
        changed_partition_files=changed,unchanged_partition_files=len(before)-len(changed),
        partition_count=len(after),semantic_noop_verified=True,canonical_cold_reconstruction_verified=True,
        reused_packed_objects=len(first_objects&second_objects),new_packed_objects=len(second_objects-first_objects),
        cold_proof=cold_proof,private_data_scratch_root=str(scratch),partition_file_inventory=after,
        monolithic_bytes=mono.stat().st_size,partitioned_bytes=sum(p['bytes'] for p in after.values()),
        scope='full-row four-column service projection, selected-symbol date-filter workflow and private offline publication; no training backend or fleet data migration')
    if not receipt['source_unchanged']:raise ValueError('live service input changed during trial; repeat from immutable source')
    atomic_write_json(output/'acceptance.json',receipt)
    print(json.dumps({'event':'accepted','rows':ordered.num_rows,'query_mean_complete_seconds':means,
                      'layout_query_mean_complete_seconds':layout_means}),flush=True)


if __name__=='__main__':main()
