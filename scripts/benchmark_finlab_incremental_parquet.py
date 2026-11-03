#!/usr/bin/env python3
"""Compare exact FinLab append storage through the complete existing cold path.

Reads receipt-bound, already acquired whole tables. Each private case writes a
real prefix and that same full table, publishes both with the common v2 writer,
fully verifies and materializes both, then compares every Arrow value/schema.
No API, production source/head/queue, deletion owner or source semantics change.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.finlab_columnar import (
    canonical_arrow, history_parquet_options, history_period_slices, write_parquet,
)
from stockagent.data.tw_public_incremental import table_content_sha256
from stockagent.data.finlab_acquisition_contract import read_receipt_bound_source, safe_stem
from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot, publish_packed_snapshot, verify_packed_snapshot,
)

CONTRACT = "finlab_full_table_incremental_parquet_storage_comparison_v1"
CHUNK_BYTES = 8 * 1024**2


def sha(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def payload_bytes(cold: Path) -> int:
    return sum(p.stat().st_size for family in ('packs', 'blobs')
               for p in (cold / 'objects' / family).rglob('*') if p.is_file())


def trial(table, scratch: Path, row_group_size: int | None, *, use_dictionary: bool = True,
          use_byte_stream_split: bool = False, verify_other_readers: bool = False,
          partition_period: str | None = None, partition_rows: int | None = None,
          native_key: str | None = None, shared_manifest: bool = False) -> dict:
    import pyarrow.parquet as pq

    source, cold, hot = (scratch / name for name in ('source', 'cold', 'hot'))
    source.mkdir(parents=True)
    expected = (table.slice(0, table.num_rows - 1), table)
    started, cpu = time.monotonic(), time.process_time()
    releases, stages = [], []
    first_payload, previous_parts = None, {}
    version_names = []
    version_manifests = []

    def same(actual, original, *, metadata=True):
        return (actual.schema.equals(original.schema, check_metadata=metadata)
                and table_content_sha256(actual) == table_content_sha256(original))

    def read_all(paths):
        return pa.concat_tables([pq.ParquetFile(p).read(use_threads=False) for p in paths])

    import pyarrow as pa
    for original in expected:
        phase = time.monotonic()
        slices = (history_period_slices(original, partition_period) if partition_period else
                  [(f'rows-{begin:020d}', begin, min(partition_rows, original.num_rows-begin))
                   for begin in range(0, original.num_rows, partition_rows)] if partition_rows else
                  [('all', 0, original.num_rows)])
        names, written_bytes, reused_parts = [], 0, 0
        options = (history_parquet_options(native_key, original) if native_key is not None else
                   dict(row_group_size=row_group_size, use_dictionary=use_dictionary,
                        use_byte_stream_split=use_byte_stream_split))
        if shared_manifest:
            from stockagent.data.partitioned_parquet import ParquetSource, write_partitioned_table
            result = write_partitioned_table(original, source / 'history.parquet',
                period='rows', date_column=None, partition_rows=partition_rows,
                source_identity={'complete_table_sha256': table_content_sha256(original)},
                parquet_options={'compression': 'zstd', **options})
            with ParquetSource(result['immutable_manifest']) as snapshot:
                paths = list(snapshot.members)
                names = [str(path.relative_to(source)) for path in paths]
                if not same(snapshot.table(), original):
                    raise ValueError('shared row manifest source parity failed')
            written_bytes, reused_parts = result['written_payload_bytes'], result['reused_partitions']
            version_manifests.append(str(Path(result['immutable_manifest']).relative_to(source)))
        for label, begin, count in ([] if shared_manifest else slices):
            name = f'history-{label}.parquet' if partition_period or partition_rows else 'history.parquet'
            names.append(name)
            part, path = original.slice(begin, count), source / name
            value_hash = table_content_sha256(part)
            if (partition_period or partition_rows) and previous_parts.get(name) == value_hash:
                reused_parts += 1
            else:
                write_parquet(part, path, **options)
                with path.open('rb') as stream:
                    os.fsync(stream.fileno())
                written_bytes += path.stat().st_size
            previous_parts[name] = value_hash
        # The source already carries the canonical writer's metadata. The
        # candidate is required to preserve it as well as every raw value.
        paths = [source / name for name in names]
        # The manifest branch already checked the complete Arrow table through
        # its canonical reader. Do not decode/hash that same table a second
        # time; Polars and DuckDB remain independent complete readbacks below.
        if not shared_manifest and not same(read_all(paths), original):
            raise ValueError('complete source Arrow value/schema/metadata parity failed')
        written = time.monotonic()
        release = publish_packed_snapshot(cold, 'finlab-history', source, node_id='fixture',
                                          file_chunk_bytes=CHUNK_BYTES)
        published = time.monotonic()
        all_names = sorted(str(p.relative_to(source)) for p in source.rglob('*') if p.is_file())
        proof = verify_packed_snapshot(cold, release, reconstruct_paths=all_names)
        if proof['independently_reconstructed_files'] != len(all_names):
            raise ValueError('complete cold original reconstruction failed')
        restored = fetch_packed_snapshot(cold, hot, release)
        restored_paths = [restored / name for name in names]
        if shared_manifest:
            with ParquetSource(restored / version_manifests[-1]) as snapshot:
                if not same(snapshot.table(), original):
                    raise ValueError('shared row manifest complete cold parity failed')
        if not shared_manifest and not same(read_all(restored_paths), original):
            raise ValueError('complete restored Arrow value/schema/metadata parity failed')
        if verify_other_readers:
            import duckdb
            import polars as pl

            polars_read = canonical_arrow(pl.concat([pl.read_parquet(p) for p in restored_paths]))
            if not same(polars_read, original, metadata=False):
                raise ValueError('complete restored Polars value/schema parity failed')
            with duckdb.connect() as connection:
                observed = connection.sql('SELECT * FROM read_parquet(?)',
                    params=[[str(p) for p in restored_paths]]).to_arrow_table()
                if not same(observed, original, metadata=False):
                    raise ValueError('complete restored DuckDB value/schema parity failed')
        releases.append(release)
        version_names.append(names)
        after = payload_bytes(cold)
        stages.append({'rows': original.num_rows, 'file_bytes': sum(p.stat().st_size for p in paths),
                       # The legacy trial labels describe requested candidates;
                       # a native-key study can override their codec policy.
                       'writer_options': {'compression': 'zstd', 'row_group_size': None,
                           'use_dictionary': True, 'use_byte_stream_split': False, **options},
                       'source_written_bytes': written_bytes, 'source_written_files': len(names)-reused_parts,
                       'source_reused_files': reused_parts,
                       'write_readback_seconds': written - phase,
                       'publication_seconds': published - written,
                       'cold_verify_fetch_readback_seconds': time.monotonic() - published,
                       'cumulative_payload_bytes': after,
                       'row_groups': sum(pq.ParquetFile(p).metadata.num_row_groups for p in paths)})
        if first_payload is None:
            first_payload = after
    # The newer release and full restoration must not invalidate the old one.
    previous = fetch_packed_snapshot(cold, hot, releases[0])
    if not shared_manifest and not same(read_all([previous / name for name in version_names[0]]), expected[0]):
        raise ValueError('old version readback changed after append')
    if shared_manifest:
        with ParquetSource(previous / version_manifests[0]) as snapshot:
            if not same(snapshot.table(), expected[0]):
                raise ValueError('shared row manifest old version changed after append')
    return {'row_group_size': row_group_size, 'use_dictionary': use_dictionary,
            'partition_period': partition_period, 'canonical_native_key': native_key,
            'partition_rows': partition_rows,
            'shared_manifest': shared_manifest,
            'compression': 'zstd',
            'use_byte_stream_split': use_byte_stream_split,
            'readers_verified': ['pyarrow', 'polars', 'duckdb'] if verify_other_readers else ['pyarrow'],
            'wall_seconds': time.monotonic() - started,
            'cpu_seconds': time.process_time() - cpu, 'stages': stages,
            'first_payload_bytes': first_payload,
            'incremental_payload_bytes': payload_bytes(cold) - first_payload,
            'two_version_payload_bytes': payload_bytes(cold),
            'complete_metadata_bytes': sum(p.stat().st_size for p in cold.rglob('*')
                if p.is_file() and 'objects/blobs' not in p.as_posix()
                and 'objects/packs' not in p.as_posix()),
            'all_raw_values_types_nulls_and_metadata_equal': True,
            'cold_versions_fully_reconstructed': 2, 'previous_version_readback_after_append': True}


def benchmark(source_root: Path, output: Path, *, keys: list[str], sizes: list[int],
              native_policy: bool = False, plain_values: bool = False,
              byte_stream_split: bool = False, partition_period: str | None = None,
              partition_rows: int | None = None, shared_manifest: bool = False) -> dict:
    import pyarrow.parquet as pq

    source_root = source_root.resolve(strict=True)
    partitioning = partition_period is not None or partition_rows is not None
    if (partition_period is not None and partition_period not in {'year', 'month'}
            or partition_rows is not None and (type(partition_rows) is not int or not 128 <= partition_rows <= 65536)
            or partition_period is not None and partition_rows is not None
            or partitioning and (native_policy or plain_values or byte_stream_split)
            or shared_manifest and partition_rows is None):
        raise ValueError('partition study compares the canonical native encoder without other candidates')
    if (output.exists() or output.is_symlink() or output.resolve() != output.absolute()
            or output.is_relative_to(source_root) or not 1 <= len(keys) <= 4
            or len(set(keys)) != len(keys) or not 1 <= len(sizes) <= 4
            or len(set(sizes)) != len(sizes)
            or any(type(n) is not int or not 128 <= n <= 4096 for n in sizes)):
        raise ValueError('use fresh external output and bounded unique real inputs/candidates')
    output.parent.mkdir(parents=True, exist_ok=True)
    implementation = ['scripts/benchmark_finlab_incremental_parquet.py', 'scripts/finlab_columnar.py',
                      'stockagent/data_sync/packed_snapshots.py', 'stockagent/data_sync/packed_metadata.py',
                      'stockagent/data/tw_public_incremental.py', 'stockagent/data/finlab_acquisition_contract.py',
                      'stockagent/data/partitioned_parquet.py']
    identities = lambda: {name: sha(ROOT / name) for name in implementation}
    code = identities()
    started = time.monotonic()
    rows = []
    for key in keys:
        receipt_path = source_root / 'receipts' / (safe_stem(key) + '.json')
        receipt_raw = receipt_path.read_bytes()
        path, receipt = read_receipt_bound_source(source_root, key, receipt_path)
        if json.loads(receipt_raw) != receipt:
            raise ValueError('source receipt changed during capture; retry without mixing generations')
        if not 0 < path.stat().st_size <= 128 * 1024**2:
            raise ValueError('whole-table fixture exceeds the bounded memory/space study')
        scratch_parent = Path('/var/tmp')
        required = 25 * 1024**3 + 32 * path.stat().st_size
        if shutil.disk_usage(scratch_parent).free < required:
            raise ValueError('private temporary study reserve unavailable')
        table = pq.read_table(path, use_threads=False)
        if table.num_rows < 2:
            raise ValueError('an observed prefix and following real row are required')
        policy = {'row_group_size': sizes[0], **({'use_dictionary': False} if plain_values else {}),
                  **({'use_byte_stream_split': True} if byte_stream_split else {})}
        if native_policy and (len(sizes) != 1 or history_parquet_options(key, table) != policy):
            raise ValueError('the selected whole real source does not match the canonical native policy')
        trials = []
        for size in sizes:
            selected = partition_period or partition_rows
            for variant in ((None, selected, selected, None) if partitioning
                            else (None, size, size, None)):
                temporary = Path(tempfile.mkdtemp(prefix='finlab-rowgroup-study-', dir=scratch_parent))
                # An interrupted/failed canonical restore remains inspectable.
                # Remove only this invocation's successful disposable fixture.
                result = trial(table, temporary, None if partitioning else variant,
                               use_dictionary=not (plain_values and variant is not None),
                               use_byte_stream_split=byte_stream_split and variant is not None,
                               verify_other_readers=bool(byte_stream_split or partitioning),
                               partition_period=variant if partition_period else None,
                               partition_rows=variant if partition_rows else None,
                               native_key=key if partitioning else None,
                               shared_manifest=shared_manifest and variant is not None)
                shutil.rmtree(temporary)
                result.update(candidate=size, key=key, trial=len(trials))
                trials.append(result)
                print(json.dumps({k: result[k] for k in ('key', 'trial', 'row_group_size',
                    'wall_seconds', 'incremental_payload_bytes', 'two_version_payload_bytes')},
                    ensure_ascii=False), flush=True)
                atomic_write_json(output.with_suffix('.progress.json'),
                    {'contract': CONTRACT, 'state': 'running_not_acceptance',
                     'completed_keys': len(rows), 'current_key': key, 'trials': trials}, durable=True)
        if sha(path) != receipt['sha256']:
            raise ValueError('captured immutable source changed during the complete comparison')
        rows.append({'key': key, 'source_sha256': receipt['sha256'], 'rows': table.num_rows,
                     'columns': table.num_columns, 'source_bytes': path.stat().st_size,
                     'source_receipt_sha256': hashlib.sha256(receipt_raw).hexdigest(),
                     'captured_source_receipt_json': receipt_raw.decode('utf-8'), 'trials': trials})
        del table
    if identities() != code:
        raise ValueError('implementation changed during matched comparison')
    result = {'contract': CONTRACT, 'state': 'accepted_complete_isolated_storage_comparison',
              'observed_at_utc': datetime.now(UTC).isoformat(), 'code_sha256': code,
              'inputs': rows, 'elapsed_seconds': time.monotonic() - started,
              'provider_calls': 0, 'production_source_writes': 0, 'production_head_changes': 0,
              'production_files_deleted': 0, 'scratch_removed_after_complete_readback': True,
              'canonical_native_storage_policy_verified': native_policy,
              'candidate_plain_value_encoding': plain_values,
              'candidate_float_byte_stream_split': byte_stream_split,
              'candidate_partition_period': partition_period,
              'candidate_partition_rows': partition_rows,
              'candidate_shared_manifest': shared_manifest,
              'official_api_throughput_claim': False, 'all_history_partition_migration_claim': False,
              'filesystem': 'private disposable fixture; production source/authority unchanged'}
    atomic_write_json(output, result, durable=True)
    atomic_write_json(output.with_suffix('.progress.json'),
        {'contract': CONTRACT, 'state': 'completed', 'acceptance': str(output),
         'completed_keys': len(rows), 'total_keys': len(keys)}, durable=True)
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, default=ROOT / 'data_finlab')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--key', action='append')
    parser.add_argument('--row-group-size', action='append', type=int)
    parser.add_argument('--native-policy', action='store_true',
                        help='require the exact canonical history policy for the whole observed table')
    parser.add_argument('--plain-values', action='store_true',
                        help='compare exact plain value encoding; leaves the reference writer unchanged')
    parser.add_argument('--byte-stream-split', action='store_true',
                        help='compare reversible floating-point byte planes and verify all three readers')
    partition_group = parser.add_mutually_exclusive_group()
    partition_group.add_argument('--partition-period', choices=('year', 'month'),
                        help='compare canonical single-file encoding with exact native period slices')
    partition_group.add_argument('--partition-rows', type=int,
                        help='compare exact coarse ordered slices without interpreting source clocks')
    parser.add_argument('--shared-manifest', action='store_true',
                        help='use the canonical immutable ordered-row manifest writer and cold reader')
    args = parser.parse_args(argv)
    result = benchmark(args.source_root, args.output.absolute(),
        keys=args.key or ['price:收盤價', 'price:成交股數'], sizes=args.row_group_size or [1024, 512],
        native_policy=args.native_policy, plain_values=args.plain_values,
        byte_stream_split=args.byte_stream_split, partition_period=args.partition_period,
        partition_rows=args.partition_rows, shared_manifest=args.shared_manifest)
    print(json.dumps({'state': result['state'], 'elapsed_seconds': result['elapsed_seconds']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
