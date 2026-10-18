"""Replay complete registration after one genuine fixed catalog capture.

Four private DuckLake tables start from one native READ_ONLY snapshot. The
original register and candidate run unchanged except their connection points
only at this private DuckDB-backed DuckLake and capture returns the genuine
fixed input. Real source mount/path/value guards remain. This is not complete
live capture, production PG performance, publication or remote recovery.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from contextlib import ExitStack
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import resource
import time
from types import ModuleType
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.benchmark_backup_catalog_identity import file_sha, validate_copy_receipt
from scripts.benchmark_lakehouse_source_wave import limits, match_native_limits
from scripts.verify_lakehouse_workflow_history import owner_identity
from stockagent.control import lakehouse
from stockagent.data_sync import backup_stream

TABLES = ('dataset_releases', 'source_objects', 'catalog_captures', 'source_metadata')
HELPERS = {'_metadata_value', '_new_metadata_descriptors', '_metadata_arrow_reader'}


def validate_scope(reference, candidate):
    def remainder(source):
        tree = ast.parse(source)
        tree.body = [node for node in tree.body if not
                     (isinstance(node, ast.FunctionDef) and node.name in HELPERS | {'register'})]
        return ast.dump(tree, include_attributes=False)
    if remainder(reference) != remainder(candidate):
        raise ValueError('unrelated source changes invalidate registration comparison')


def active_module(source, label):
    module = ModuleType('private_register_'+label)
    exec(compile(source, '<pinned-private-register-'+label+'>', 'exec'), module.__dict__)
    return module


def private_connection(c, root):
    import duckdb
    con = duckdb.connect()
    con.execute('SET threads=2')
    con.execute("SET memory_limit='512MB'")
    con.execute('SET extension_directory='+lakehouse.sql_text(c['extensions']))
    con.execute('LOAD ducklake')
    con.execute('ATTACH '+lakehouse.sql_text('ducklake:'+str(root/'registry.ducklake'))+
                ' AS lake (DATA_PATH '+lakehouse.sql_text(root/'data')+', DATA_INLINING_ROW_LIMIT 0)')
    return con


def prepare(c, root, catalog, reference):
    import pyarrow.parquet as pq
    root.mkdir(mode=0o700)
    source_sha = file_sha(catalog)
    copy_proof = validate_copy_receipt(catalog, source_sha)
    original, candidate = reference.read_text(), Path(lakehouse.__file__).read_text()
    validate_scope(original, candidate)
    atomic_write_text(root/'original_lakehouse.py', original, mode=0o600, durable=True)
    atomic_write_text(root/'candidate_lakehouse.py', candidate, mode=0o600, durable=True)
    owner = owner_identity()
    con = lakehouse.connect(c, read_only=True)
    try:
        before = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
        counts, hashes, files = {}, {}, {}
        for table in TABLES:
            path = root/(table+'.parquet')
            counts[table] = con.execute('SELECT count(*) FROM lake.'+table).fetchone()[0]
            # COPY's default large row group can itself exceed the native
            # 512MB limit for wide JSON strings. Preparation is fully bounded
            # too; no larger engineering memory allowance is hidden here.
            reader = con.execute('SELECT * FROM lake.'+table).to_arrow_reader(batch_size=16)
            with pq.ParquetWriter(path, reader.schema, compression='zstd', use_dictionary=False) as writer:
                for batch in reader:
                    writer.write_batch(batch)
            reader.close()
            hashes[table] = lakehouse._registered_table_digest(con, table)
            files[path.name] = file_sha(path)
        after = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
    finally:
        con.close()
    if owner_identity() != owner or before != after or source_sha != file_sha(catalog):
        raise ValueError('native snapshot, owner or fixed input changed during capture')
    result = {'state':'accepted_private_registration_inputs',
              'observed_at_utc':datetime.now(UTC).isoformat(), 'native_snapshot_id':before,
              'native_owner':owner, 'counts':counts, 'complete_table_row_sha256':hashes,
              'seed_parquet_sha256':files, 'catalog':str(catalog.absolute()),
              'catalog_file_sha256':source_sha, 'catalog_copy_receipt':copy_proof,
              'original_source_sha256':hashlib.sha256(original.encode()).hexdigest(),
              'candidate_source_sha256':hashlib.sha256(candidate.encode()).hexdigest(),
              'production_catalog_writes':0, 'provider_calls':0, 'service_controls':0}
    atomic_write_json(root/'inputs.json', result, durable=True)
    return result


def run_case(c, root, label, output):
    import pyarrow.parquet as pq
    manifest = json.loads((root/'inputs.json').read_bytes())
    if manifest['state'] != 'accepted_private_registration_inputs':
        raise ValueError('private initial snapshot was not verified')
    for name, sha in manifest['seed_parquet_sha256'].items():
        if file_sha(root/name) != sha:
            raise ValueError('private seed changed')
    catalog = Path(manifest['catalog'])
    if file_sha(catalog) != manifest['catalog_file_sha256']:
        raise ValueError('fixed source catalog changed')
    validate_copy_receipt(catalog, manifest['catalog_file_sha256'])
    reference = (root/'original_lakehouse.py').read_text()
    candidate = (root/'candidate_lakehouse.py').read_text()
    validate_scope(reference, candidate)
    if (hashlib.sha256(reference.encode()).hexdigest() != manifest['original_source_sha256']
            or hashlib.sha256(candidate.encode()).hexdigest() != manifest['candidate_source_sha256']
            or candidate != Path(lakehouse.__file__).read_text()):
        raise ValueError('pinned register code changed')
    owner = owner_identity()
    case = output.with_suffix('')
    case.mkdir(mode=0o700)
    state = case/'state'
    state.mkdir(mode=0o700)
    private = {**c, 'state_root':str(state)}
    con = private_connection(c, case)
    try:
        con.execute("CALL ducklake_set_option('lake', 'parquet_compression', 'zstd')")
        for table in TABLES:
            path = root/(table+'.parquet')
            query = 'CREATE TABLE lake.'+table+' AS SELECT * FROM read_parquet('+lakehouse.sql_text(path)+')'
            if table != 'source_metadata':
                con.execute(query)
            else:
                con.execute(query+' WHERE false')
                for batch in pq.ParquetFile(path).iter_batches(batch_size=16, use_threads=False):
                    con.register('seed_batch', batch)
                    con.execute('INSERT INTO lake.'+table+' SELECT * FROM seed_batch')
        initial_hashes = {table:lakehouse._registered_table_digest(con, table) for table in TABLES}
        if initial_hashes != manifest['complete_table_row_sha256']:
            raise ValueError('private initial tables differ from the complete native snapshot')
    finally:
        con.close()
    with catalog.open() as stream:
        captured = json.load(stream)
    active = active_module(reference if label == 'baseline' else candidate, label)
    connections, captures, value_reads = [], [], []
    original_read = Path.read_bytes
    def connect(config, **options):
        if config != private or options:
            raise ValueError('private comparison attempted an unexpected connection')
        connections.append('private_ducklake')
        return private_connection(c, case)
    def capture(*args, **options):
        captures.append(True)
        return captured
    def read(path):
        if path.is_relative_to(Path(c['cold_root'])):
            value_reads.append(str(path))
        return original_read(path)
    before_limits = limits()
    wall, cpu = time.perf_counter(), time.process_time()
    with ExitStack() as patches:
        patches.enter_context(patch.object(active, 'connect', connect))
        patches.enter_context(patch.object(backup_stream, 'capture_catalog', capture))
        patches.enter_context(patch.object(Path, 'read_bytes', read))
        result = active.register(private)
    elapsed, spent = time.perf_counter()-wall, time.process_time()-cpu
    required = Counter(str(Path(c['cold_root'])/row['relative']) for row in captured['files']
                       if row['role'] == 'cold_metadata' and 'captured_bytes_utf8' not in row)
    observed_reads = Counter(value_reads)
    if any(observed_reads[path] < count for path, count in required.items()):
        raise ValueError('known source values were not read and validated')
    persisted = json.loads((state/'catalog-status.json').read_bytes())
    assert persisted == result
    con = private_connection(c, case)
    try:
        hashes = {table:lakehouse._registered_table_digest(con, table) for table in TABLES}
        counts = {table:con.execute('SELECT count(*) FROM lake.'+table).fetchone()[0] for table in TABLES}
    finally:
        con.close()
    semantic = {key:value for key,value in result.items()
                if key not in {'observed_at_utc','complete_workflow_seconds'}}
    stable = (owner_identity() == owner and before_limits == limits()
              and Path(lakehouse.__file__).read_text() == candidate
              and file_sha(catalog) == manifest['catalog_file_sha256'])
    if connections != ['private_ducklake'] or captures != [True] or not stable:
        raise ValueError('complete fixed-capture registration boundary changed')
    observed = {'state':'accepted_private_fixed_capture_registration',
                'observed_at_utc':datetime.now(UTC).isoformat(), 'label':label,
                'wall_seconds':elapsed, 'cpu_seconds':spent,
                'fresh_process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                'full_status':result, 'semantic_status':semantic, 'complete_table_row_sha256':hashes,
                'counts':counts, 'resource_limits':before_limits, 'native_owner':owner,
                'stable_source_code_owner_and_limits':stable, 'metadata_value_read_calls':len(value_reads),
                'required_metadata_value_reads':sum(required.values()),
                'fixed_catalog_file_sha256':manifest['catalog_file_sha256'],
                'seed_native_snapshot_id':manifest['native_snapshot_id'],
                'production_catalog_writes':0, 'provider_calls':0, 'service_controls':0,
                'caveats':['Complete register after a fixed genuine capture; excludes live catalog capture.',
                           'Private DuckDB-backed DuckLake is not production PG performance.',
                           'Private source-signature cache must never be adopted as current source readiness.',
                           'Only two observer clock/duration fields are excluded; all table rows/hash and semantic status are retained.',
                           'Peak RSS includes input loading and private seed verification; not a native RAM acceptance.']}
    atomic_write_json(output, observed, durable=True)
    return observed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--catalog', type=Path)
    parser.add_argument('--reference', type=Path)
    parser.add_argument('--label', choices=('baseline','candidate'))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    base = Path('artifacts/benchmarks').resolve()
    if not args.root.absolute().resolve().is_relative_to(base):
        parser.error('private benchmark root required')
    if args.prepare:
        if args.root.exists() or args.catalog is None or args.reference is None:
            parser.error('preparation needs fresh root, genuine catalog and preserved reference')
    elif (args.label is None or args.output is None or args.output.exists()
          or not args.output.absolute().resolve().is_relative_to(args.root.absolute().resolve())):
        parser.error('use a fresh case receipt below its input root')
    match_native_limits()
    c = lakehouse.configuration()
    lakehouse.guard(c)
    result = (prepare(c,args.root,args.catalog,args.reference) if args.prepare else
              run_case(c,args.root,args.label,args.output))
    print(json.dumps({key:result[key] for key in
                      ('state','wall_seconds','cpu_seconds','counts') if key in result}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

