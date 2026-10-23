#!/usr/bin/env python3
"""Same-input ABBA priority installer or read-only inventory benchmark.

Default mode reads isolated synthetic queues only. Explicit --inventory-root
reads two FinMind metadata queues in fixed read-only snapshots, never payloads.
No GUI/provider call or mutation. The baseline is retained in a Git revision.
Independent full-column readback verifies scope/state/cursor/receipt parity;
that expensive oracle is OUTSIDE the timed installer, not silently removed.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.tej_value_priority import CONTRACT, install_priorities


def _baseline(revision: str):
    identity = subprocess.check_output(
        ['git', 'rev-parse', '--verify', revision+'^{commit}'], cwd=ROOT, text=True).strip()
    source = subprocess.check_output(
        ['git', 'show', identity+':downloader/tej_value_priority.py'], cwd=ROOT)
    module = ast.parse(source)
    functions = [node for node in module.body
                 if isinstance(node, ast.FunctionDef) and node.name == 'install_priorities']
    if len(functions) != 1:
        raise ValueError('One retained baseline installer required')
    # Load the retained module's own helpers/imports too. Otherwise benchmarking
    # a newer committed baseline could accidentally use candidate dependencies
    # (or fail because its authorizer helper was omitted).
    namespace = {'__name__': 'tej_priority_retained_baseline', '__file__': str(ROOT/'downloader/tej_value_priority.py')}
    exec(compile(module, identity, 'exec'), namespace)
    return namespace['install_priorities'], identity, hashlib.sha256(source).hexdigest()


def _readback(con, *, priorities=False):
    """Exact independent oracle, including columns the old hash did not read."""
    digest = hashlib.sha256()
    for table in ('tasks', 'download_plans', 'features', 'traffic', 'desktop_attempts', 'meta',
                  'acquisition_value_priorities'):
        if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            continue
        columns = [r[1] for r in con.execute(f'PRAGMA table_info({table})')]
        if priorities:
            if table in {'tasks', 'download_plans'}:
                columns = [columns[0], 'priority']
            elif table != 'acquisition_value_priorities':
                continue
        elif table == 'acquisition_value_priorities':
            continue
        else:
            columns = [c for c in columns if c != 'priority']
        query = 'SELECT '+','.join(columns)+' FROM '+table
        if table == 'meta':
            query += " WHERE key!='acquisition_value_priority'"
        query += ' ORDER BY '+columns[0]
        digest.update(table.encode()+b'\n')
        for row in con.execute(query):
            digest.update(json.dumps(tuple(row), ensure_ascii=False, separators=(',', ':')).encode()+b'\n')
    return digest.hexdigest()


def _fixture(root: Path, channel: str, tasks: int, payload_bytes: int):
    if channel == 'smart_wizard':
        from downloader.tej_history import connect
    else:
        from downloader.tej_api import connect
    with closing(connect(root)) as con, con:
        states = ('pending', 'running', 'complete', 'blocked', 'superseded') if channel == 'smart_wizard' else (
            'pending', 'retry', 'running', 'complete', 'blocked')
        payload = json.dumps({'companies': ['FIXTURE'], 'dates': ['20140101'], 'padding': 'x'*payload_bytes})
        for index in range(tasks):
            tid, state, priority = f't{index%2}', states[index%len(states)], -2 if index%19 == 0 else 100
            if channel == 'smart_wizard':
                con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,"
                    "scope_contract,receipt_path,active_attempt_id) VALUES(?,?,'download','P1',?,?,?,?,?,?)",
                    (f'{index:024x}', tid, priority, payload, state, 'editable_source_scope_v1',
                     f'receipts/{index}.json' if state == 'complete' else None,
                     f'{index:024x}-'+('a'*32) if state in {'running', 'complete'} else None))
            else:
                con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state,cursor,query_rows,"
                    "actual_rows,page_size) VALUES(?,?,?,?,?,'paid-cursor',20000,20000,10000)",
                    (f'{index:024x}', tid, payload, priority, state))
        if channel == 'smart_wizard':
            for index in range(2):
                con.execute("INSERT INTO download_plans VALUES(?,'fixture',?,2,17,'P1',100)",
                            (f't{index}', payload))


def benchmark(output: Path, *, revision='HEAD', tasks=10000, payload_bytes=8192) -> dict:
    if output.exists():
        raise FileExistsError('Retain original benchmark evidence')
    if (type(tasks) is not int or not 10 <= tasks <= 100000
            or type(payload_bytes) is not int or not 0 <= payload_bytes <= 65536
            or tasks*payload_bytes > 512*1024**2):
        raise ValueError('Bounded isolated fixture required (at most 512 MiB payload)')
    baseline, revision, baseline_sha = _baseline(revision)
    snapshot = {'contract': CONTRACT, 'fingerprint': 'fixed-synthetic-benchmark',
        'generated_at_utc': '2026-10-22T00:00:00+00:00', 'next_refresh_at_utc': '2026-10-22T00:15:00+00:00',
        'tables': [{'channel': channel, 'table_id': f't{i}', 'priority': i*100, 'rank': i+1,
                    'value_score': 100-i, 'missing_or_gap_fields': 1, 'crosscheck_candidate_fields': 0,
                    'value_basis': 'synthetic_fixture'}
                   for channel in ('smart_wizard', 'api') for i in range(2)]}
    samples = []
    with tempfile.TemporaryDirectory(prefix='tej-priority-abba-') as temporary:
        for channel in ('smart_wizard', 'api'):
            root = Path(temporary)/channel
            _fixture(root, channel, tasks, payload_bytes)
            with closing(sqlite3.connect(root/'queue.sqlite3')) as original:
                invariant = _readback(original)
                ordered = None
                for variant in ('baseline', 'candidate', 'candidate', 'baseline'):
                    # A separate equal-input copy per run, not progressively
                    # warmed/no-op mutation of a previously ranked queue.
                    with closing(sqlite3.connect(':memory:')) as con:
                        original.backup(con)
                        con.execute('BEGIN IMMEDIATE')
                        clock = time.perf_counter()
                        result = (baseline if variant == 'baseline' else install_priorities)(con, snapshot, channel)
                        con.commit()
                        seconds = time.perf_counter()-clock
                        con.execute('PRAGMA query_only=ON')
                        if _readback(con) != invariant:
                            raise ValueError('Source/scope/state/cursor/receipt changed')
                        priority_sha = _readback(con, priorities=True)
                        if ordered is None:
                            ordered = priority_sha
                        if ordered != priority_sha:
                            raise ValueError('Priority ordering differs from baseline')
                        samples.append({'channel': channel, 'variant': variant, 'seconds': seconds,
                            'source_columns_sha256': invariant, 'priority_columns_sha256': priority_sha,
                            'source_identity_verification': result.get('source_identity_verification')})
    summary = {}
    for channel in ('smart_wizard', 'api'):
        medians = {variant: statistics.median(r['seconds'] for r in samples
                   if r['channel'] == channel and r['variant'] == variant) for variant in ('baseline', 'candidate')}
        summary[channel] = {**medians, 'installer_speedup': medians['baseline']/medians['candidate']}
    result = {'contract': 'tej_priority_install_fixed_input_abba_full_column_oracle_v1',
        'accepted': True, 'observed_at_utc': datetime.now(UTC).isoformat(),
        'baseline_revision': revision, 'baseline_source_sha256': baseline_sha,
        'candidate_source_sha256': hashlib.sha256((ROOT/'downloader/tej_value_priority.py').read_bytes()).hexdigest(),
        'fixture_tasks_per_channel': tasks, 'padding_bytes_per_task': payload_bytes,
        'fixture_is_synthetic': True, 'production_database_read': False, 'provider_queries_sent': 0,
        'source_and_priority_parity': True, 'order': ['baseline', 'candidate', 'candidate', 'baseline'],
        'timing_scope': 'installer plus commit in a fresh in-memory copy; independent full readback outside timing',
        'samples': samples, 'summary': summary,
        'interpretation': 'Installer-only same-input speed, not a production refresh or acquisition speedup'}
    atomic_write_json(output, result)
    return result


def benchmark_inventory(root: Path, output: Path, *, revision='HEAD') -> dict:
    """Compare complete bounds AND exact selected receipt paths in one snapshot.

    No FinMind schema change or downloader connection is needed. The bounded
    read transaction is closed before output, so a benchmark cannot leave a
    persistent reader pin. Hashes include metadata only, never licensed values.
    """
    from scripts.build_tej_smart_wizard_inventory import finmind_local_metadata
    if output.exists():
        raise FileExistsError('Retain original inventory benchmark evidence')
    identity = subprocess.check_output(['git','rev-parse','--verify',revision+'^{commit}'],cwd=ROOT,text=True).strip()
    source = subprocess.check_output(['git','show',identity+':scripts/build_tej_smart_wizard_inventory.py'],cwd=ROOT)
    function = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name == 'local_features')
    strings = [node.value for node in ast.walk(function)
               if isinstance(node, ast.Constant) and isinstance(node.value, str)]
    coverage_sql = next(value for value in strings if value.startswith('SELECT dataset,MIN('))
    sample_sql = next(value for value in strings if 'ROW_NUMBER() OVER(PARTITION BY dataset' in value)
    def reference(con):
        coverage = {ds:(first,last) for ds,first,last in con.execute(coverage_sql)}
        return coverage, con.execute(sample_sql).fetchall()
    samples, summaries = [], {}
    accepted = True
    for lane in ('sponsor','complement'):
        queue = root.resolve()/'data_finmind'/lane/'queue.sqlite3'
        if not queue.is_file():
            continue
        with closing(sqlite3.connect(queue.as_uri()+'?mode=ro',uri=True,timeout=.25)) as con:
            deadline = time.monotonic()+120
            con.set_progress_handler(lambda: int(time.monotonic()>deadline), 10000)
            con.execute('PRAGMA query_only=ON')
            con.execute('BEGIN')
            expected = None
            for variant in ('baseline','candidate','candidate','baseline'):
                started = time.perf_counter()
                result = (reference if variant == 'baseline' else finmind_local_metadata)(con)
                seconds = time.perf_counter()-started
                digest = hashlib.sha256(json.dumps(result,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                expected = expected or digest
                parity = digest == expected
                accepted = accepted and parity
                samples.append({'lane':lane,'variant':variant,'seconds':seconds,
                    'coverage_datasets':len(result[0]),'representative_receipts':len(result[1]),
                    'complete_bounds_and_exact_receipts_sha256':digest,'oracle_equal':parity})
        timing = {variant: statistics.mean(row['seconds'] for row in samples
                  if row['lane']==lane and row['variant']==variant) for variant in ('baseline','candidate')}
        summaries[lane] = {**timing,'metadata_query_speedup':timing['baseline']/timing['candidate']}
    if not summaries:
        raise ValueError('Existing FinMind metadata queues required')
    report = {'contract':'tej_finmind_inventory_same_read_snapshot_abba_v1','accepted':accepted,
        'observed_at_utc':datetime.now(UTC).isoformat(),'baseline_revision':identity,
        'baseline_source_sha256':hashlib.sha256(source).hexdigest(),
        'candidate_source_sha256':hashlib.sha256((ROOT/'scripts/build_tej_smart_wizard_inventory.py').read_bytes()).hexdigest(),
        'order':['baseline','candidate','candidate','baseline'],'samples':samples,'summary':summaries,
        'production_metadata_read':True,'queue_modified':False,'provider_queries_sent':0,
        'source_values_read':False,'read_snapshot_budget_seconds_per_lane':120,
        'interpretation':'Complete bounds plus representative-receipt SQL only; not whole ranking/acquisition speedup'}
    atomic_write_json(output,report)
    if not accepted:
        raise ValueError('Inventory metadata differs; failed ABBA receipt retained')
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-ref', default='HEAD')
    parser.add_argument('--tasks', type=int, default=10000)
    parser.add_argument('--payload-bytes', type=int, default=8192)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--inventory-root', type=Path,
                        help='Explicit read-only inventory SQL ABBA, not the synthetic installer benchmark')
    args = parser.parse_args(argv)
    result = (benchmark_inventory(args.inventory_root,args.output,revision=args.baseline_ref)
              if args.inventory_root else benchmark(args.output, revision=args.baseline_ref,
                                                    tasks=args.tasks, payload_bytes=args.payload_bytes))
    print(json.dumps({k: v for k, v in result.items() if k != 'samples'}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
