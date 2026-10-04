#!/usr/bin/env python3
"""Rebuild one exact TW public release twice on demand with its canonical builder.

New private outputs only. This proves derivation from the selected retained raw
release, not permission to remove originals/caches or to resume old checkpoints.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import socket
import sys
import time


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def verify(args):
    if socket.gethostname() == 'penguin':
        raise ValueError('training derivation belongs on the remote research node')
    code = args.code_root.resolve(strict=True)
    source = args.source_root.resolve(strict=True)
    output = args.output.resolve()
    if output.is_relative_to(source) or output.is_relative_to(code):
        raise ValueError('derivation outputs must be outside the immutable source and code trees')
    if source.name != args.snapshot_id or source.parent.name != 'tw-public':
        raise ValueError('source path must name the exact TW public materialization')
    if output.exists():
        raise ValueError('each derivation acceptance requires a new output directory')
    start = time.perf_counter()
    sys.path.insert(0, str(code))
    from stockagent.remote_build import observe_node, validate_selection
    from stockagent.runtime_identity import runtime_identity, verify_source_release
    profile_before = observe_node(source, output.parent)
    tuning_path = getattr(args, 'tuning_receipt', None)
    tuning = json.loads(tuning_path.read_bytes()) if tuning_path else None
    tuning_sha256 = file_sha256(tuning_path) if tuning_path else None
    if tuning:
        args.memory_budget_gib = tuning['selected']['parameters']['memory_budget_bytes'] // 1024**3
        args.polars_threads = tuning['selected']['parameters']['polars_threads']
        args.arrow_threads = tuning['selected']['parameters']['arrow_threads']
    memory_budget = args.memory_budget_gib * 1024**3
    if profile_before['limits']['memory_headroom_bytes'] < memory_budget or shutil.disk_usage(output.parent).free < 10 * 1024**3:
        raise ValueError('remote derivation needs the declared available RAM/cgroup headroom and 10 GiB scratch')
    cpu_budget = min(profile_before['limits']['cpu_thread_trial_cap'], len(os.sched_getaffinity(0)))
    arrow_threads = getattr(args, 'arrow_threads', None)
    arrow_threads = min(profile_before['limits']['cpu_worker_budget'], len(os.sched_getaffinity(0))) if arrow_threads is None else arrow_threads
    if any(type(value) is not int or not 1 <= value <= cpu_budget for value in (args.polars_threads, arrow_threads)):
        raise ValueError('requested pool budget exceeds this process CPU affinity/cgroup quota')
    code_proof = verify_source_release(args.code_receipt, code)
    native_before = runtime_identity()
    ready = json.loads((source.parent / ('.' + args.snapshot_id + '.READY.json')).read_text())
    context = {'code_receipt_sha256': file_sha256(args.code_receipt),
               'source_sha256': code_proof['source_sha256'], 'runtime_sha256': native_before['sha256'],
               'snapshot_id': args.snapshot_id, 'manifest_sha256': ready.get('manifest_sha256'),
               'end_date': args.end_date, 'builder': 'canonical_tw_public_full'}
    if tuning:
        selected = validate_selection(tuning, profile_before, context)
        args.polars_threads, args.arrow_threads = selected['polars_threads'], selected['arrow_threads']
        if selected['affinity'] is not None:
            os.sched_setaffinity(0, selected['affinity'])
    cpu_budget = min(profile_before['limits']['cpu_thread_trial_cap'], len(os.sched_getaffinity(0)))
    if any(type(value) is not int or not 1 <= value <= cpu_budget for value in (args.polars_threads, arrow_threads)):
        raise ValueError('requested pool budget exceeds this process CPU affinity/cgroup quota')
    os.environ['POLARS_MAX_THREADS'] = str(args.polars_threads)
    from downloader.artifact_io import atomic_write_json
    from stockagent.data.tw_public_features import build_tw_public_training_features
    from stockagent.data_sync.packed_snapshots import (
        resolve_packed_snapshot_id, _load_inventory, _validate_inventory, _verify_materialized,
    )
    from stockagent.runtime_identity import identity_sha256
    import pyarrow as pa
    import pyarrow.parquet as pq
    import polars as pl
    if pl.thread_pool_size() != args.polars_threads:
        raise ValueError('effective Polars thread pool differs from the requested budget')
    pa.set_cpu_count(arrow_threads)
    resolved = resolve_packed_snapshot_id(args.packed_root, 'tw-public', args.snapshot_id, require_objects=False)
    if ready.get('snapshot_id') != args.snapshot_id or ready.get('manifest_sha256') != resolved.manifest_sha256:
        raise ValueError('READY does not identify the requested immutable manifest')
    entries = _load_inventory(args.packed_root, resolved.manifest)
    _validate_inventory(resolved.manifest, entries)
    # Reuse the same per-file SHA and exact-tree gate used by canonical fetch.
    # No cold object presence, hydration, head update or publication is inferred.
    _verify_materialized(source, resolved.manifest, entries)
    output.mkdir(mode=0o700)
    print(json.dumps({'event': 'exact_materialized_source_verified', 'snapshot_id': args.snapshot_id,
                      'inventory_entries': len(entries)}), flush=True)
    results = []
    for name in ('first', 'independent')[:args.build_count]:
        path = output / (name + '.parquet')
        result = build_tw_public_training_features(
            input_dir=source, output_path=path, symbols_root=source/'stocks',
            summary_path=output/(name+'.summary.json'), end_date=date.fromisoformat(args.end_date),
        )
        schema = pq.read_schema(path)
        results.append({'name': name, 'path': str(path), 'rows': result.rows, 'stock_rows': result.stock_rows,
                        'market_rows': result.market_rows, 'features': result.feature_count,
                        'build_mode': result.build_mode, 'output_receipt': result.output_receipt,
                        'source_receipts': result.source_receipts,
                        'symbol_universe_receipt': result.symbol_universe_receipt,
                        'schema_sha256': identity_sha256([(f.name, str(f.type)) for f in schema]),
                        'stage_elapsed_seconds': result.stage_elapsed_seconds})
        atomic_write_json(output / (name + '.acceptance.json'), results[-1])
        print(json.dumps({'event': 'canonical_build_completed', 'build': name, 'rows': result.rows,
                          'features': result.feature_count, 'build_mode': result.build_mode}), flush=True)
    a = results[0]
    fields = ('rows', 'stock_rows', 'market_rows', 'features', 'schema_sha256', 'source_receipts', 'symbol_universe_receipt')
    independent_equal = None
    if len(results) == 2:
        b = results[1]
        independent_equal = all(a[k] == b[k] for k in fields) and a['output_receipt']['sha256'] == b['output_receipt']['sha256']
    if independent_equal is False or any(r['build_mode'] != 'full' for r in results):
        raise ValueError('independent full derivations differ or reused an existing derived table')
    if tuning and (a['output_receipt']['sha256'] != tuning['expected_output_sha256']
                   or file_sha256(tuning_path) != tuning_sha256):
        raise ValueError('measured selection/output changed; no acceptance')
    _verify_materialized(source, resolved.manifest, entries)
    if verify_source_release(args.code_receipt, code) != code_proof or runtime_identity() != native_before:
        raise ValueError('fixed code/runtime identity changed during derivation')
    profile_after = observe_node(source, output.parent)
    if profile_after['identity']['cgroup_limits'] != profile_before['identity']['cgroup_limits']:
        raise ValueError('node resource limits changed during derivation')
    old_table = source/'features/tw_public_stock_daily.parquet'
    previous_hash = file_sha256(old_table) if old_table.is_file() else None
    usage = resource.getrusage(resource.RUSAGE_SELF)
    if usage.ru_maxrss * 1024 > memory_budget:
        raise ValueError('complete derivation exceeded the declared peak RAM budget; no promotion receipt')
    receipt = {'schema_version': 1, 'state': 'accepted', 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
               'node': socket.gethostname(), 'snapshot_id': args.snapshot_id,
               'manifest_sha256': resolved.manifest_sha256,
               'source_inventory_sha256': resolved.manifest['archive']['inventory']['sha256'],
               'source_files_verified': sum(r['kind'] == 'file' for r in entries),
               'code_proof': code_proof, 'runtime': native_before,
               'driver_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               'end_date': args.end_date, 'builds': results,
               'requested_build_count': args.build_count, 'polars_threads': args.polars_threads,
               'process_cpu_seconds': usage.ru_utime + usage.ru_stime,
               'process_peak_rss_bytes': usage.ru_maxrss * 1024,
               'memory_budget_bytes': memory_budget, 'peak_memory_budget_met': True,
               'memory_available_before_bytes': profile_before['observations']['mem_available_bytes'],
               'effective_polars_threads': pl.thread_pool_size(),
               'effective_arrow_threads': pa.cpu_count(), 'arrow_io_threads': pa.io_thread_count(),
               'node_profile_before': profile_before, 'node_profile_after': profile_after,
               'measurement_context': context,
               'tuning_receipt_sha256': tuning_sha256,
               'process_input_blocks': usage.ru_inblock, 'process_output_blocks': usage.ru_oublock,
               'independent_full_rebuild_bytes_equal': independent_equal, 'source_and_code_unchanged': True,
               'previous_derived_table_sha256': previous_hash,
               'matches_previous_derived_table_bytes': previous_hash == a['output_receipt']['sha256'],
               'wall_seconds': time.perf_counter()-start,
               'scope': 'exact TW public raw release to requested canonical full feature tables on remote node',
               'checkpoint_resume_compatibility_verified': False, 'historical_pit_completeness_verified': False,
               'cold_object_recovery_verified': False, 'eviction_authorized': False, 'production_view_promoted': False}
    atomic_write_json(output/'acceptance.json', receipt)
    print(json.dumps({k: receipt[k] for k in ('state', 'snapshot_id', 'independent_full_rebuild_bytes_equal',
                                            'matches_previous_derived_table_bytes', 'wall_seconds')}), flush=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--code-root', type=Path, required=True)
    parser.add_argument('--code-receipt', type=Path, required=True)
    parser.add_argument('--packed-root', type=Path, required=True)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--build-count', type=int, choices=(1, 2), default=2)
    parser.add_argument('--polars-threads', type=int, default=2)
    parser.add_argument('--arrow-threads', type=int, help='CPU pool, capped by actual affinity/cgroup quota')
    parser.add_argument('--tuning-receipt', type=Path, help='Use a full-build measurement bound to this exact node/code/source/runtime')
    parser.add_argument('--memory-budget-gib', type=int, choices=(64, 128, 256), default=128,
                        help='available RAM admission and measured peak-RSS acceptance; complete build only')
    args = parser.parse_args()
    verify(args)


if __name__ == '__main__':
    main()
