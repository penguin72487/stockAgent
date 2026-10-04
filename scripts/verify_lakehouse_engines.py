#!/usr/bin/env python3
"""Bounded real-source DuckLake/Iceberg transactions, snapshots and recovery.

Writes only a fresh trial directory and UUID-owned PostgreSQL databases. This
does not change a catalog, downloader, cold release, panel backend or service.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import unquote, urlsplit
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.runtime_identity import runtime_identity


def sha256(path: Path) -> str:
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def sql_text(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def inventory(root: Path) -> dict:
    return {str(p.relative_to(root)): {'sha256':sha256(p), 'bytes':p.stat().st_size}
            for p in sorted(root.rglob('*')) if p.is_file()}


def same_values(actual, expected, *, sort_keys=(('date','ascending'),)) -> None:
    import pyarrow.compute as pc
    import pyarrow as pa
    if actual.num_rows != expected.num_rows or set(actual.column_names) != set(expected.column_names):
        raise ValueError('column coverage differs from the real source')
    actual = actual.select(expected.column_names).cast(expected.schema).combine_chunks()
    expected = expected.combine_chunks()
    actual = actual.take(pc.sort_indices(actual, sort_keys=list(sort_keys)))
    expected = expected.take(pc.sort_indices(expected, sort_keys=list(sort_keys)))
    for field in expected.schema:
        left,right = actual[field.name],expected[field.name]
        if not pc.is_null(left).equals(pc.is_null(right)):
            raise ValueError('source null masks differ: '+field.name)
        equal=pc.equal(left,right)
        if pa.types.is_floating(field.type):
            equal=pc.or_(equal,pc.and_(pc.is_nan(left),pc.is_nan(right)))
        if not pc.all(pc.fill_null(equal,True)).as_py():
            raise ValueError('row values, dates or provenance differ: '+field.name)


def connection(extension_root: Path):
    import duckdb
    con = duckdb.connect()
    con.execute('SET threads=4')
    con.execute("SET memory_limit='1GB'")
    con.execute('SET extension_directory=' + sql_text(extension_root))
    return con


def private_database_options(env_file: Path) -> dict:
    # Same existing private env syntax, without importing downloader analytics
    # extras into the small control role or logging credentials.
    import shlex
    if not env_file.is_file() or env_file.stat().st_mode & 0o077:
        raise ValueError('the control env must be private')
    values = {}
    for line in env_file.read_text().splitlines():
        if line.startswith('CONTROL_PLANE_DSN='):
            parsed = shlex.split(line.partition('=')[2])
            if len(parsed) == 1:
                values['dsn'] = parsed[0]
    parsed = urlsplit(values.get('dsn',''))
    if parsed.scheme != 'postgresql' or parsed.hostname not in ('127.0.0.1','localhost') or unquote(parsed.username or '') != 'stockagent_control':
        raise ValueError('trial uses only the existing loopback control role')
    return {'host':parsed.hostname, 'port':parsed.port or 5432, 'user':'stockagent_control',
            'password':unquote(parsed.password or '')}


class CatalogDatabases:
    def __init__(self, output: Path, env_file: Path):
        self.output = output
        self.options = private_database_options(env_file)
        self.created = []

    def admin(self, sql: str, database: str = 'postgres', executable='psql', body=None):
        argv = ['runuser','-u','postgres','--',executable,'-p',str(self.options['port'])]
        if executable == 'psql':
            argv += ['-X','-v','ON_ERROR_STOP=1','-At','-d',database]
            body = sql.encode()
        elif executable == 'pg_dump':
            argv += ['-Fc','--no-owner','--no-privileges',database]
        else:
            argv += ['--no-owner','--no-privileges','--role','stockagent_control','-d',database]
        result = subprocess.run(argv, input=body, capture_output=True, timeout=60)
        if result.returncode:
            raise RuntimeError('owned trial catalog operation failed: '+executable)
        return result.stdout

    def create(self, label: str) -> str:
        name = 'stockagent_trial_'+label+'_'+uuid.uuid4().hex[:12]
        self.admin('CREATE DATABASE '+name+' OWNER stockagent_control;')
        self.created.append(name)
        return name

    def attach(self, con, name: str, data: Path):
        os.environ['PGPASSWORD'] = self.options['password']
        con.execute('LOAD postgres')
        con.execute('LOAD ducklake')
        path = ('ducklake:postgres:dbname='+name+' host='+self.options['host']+
                ' port='+str(self.options['port'])+' user=stockagent_control')
        con.execute('ATTACH '+sql_text(path)+' AS lake (DATA_PATH '+sql_text(data)+
                    ', OVERRIDE_DATA_PATH true, DATA_INLINING_ROW_LIMIT 0)')

    def cleanup(self):
        for name in reversed(self.created):
            self.admin('DROP DATABASE '+name+';')
        atomic_write_json(self.output/'catalog-cleanup.json', {'owned_trial_databases_removed':self.created,
                                                             'control_database_restarted':False})


def wait_for(paths: list[Path], processes=(), timeout=60):
    deadline = time.monotonic()+timeout
    while not all(p.exists() for p in paths):
        if time.monotonic() >= deadline or any(p.poll() is not None for p in processes):
            raise RuntimeError('trial barrier was not reached by both independent writers')
        time.sleep(.05)


def ducklake_writer(config_path: Path):
    import pyarrow.parquet as pq
    config = json.loads(config_path.read_bytes())
    output = Path(config['output'])
    started = time.perf_counter()
    catalog = CatalogDatabases(output, Path(config['control_env']))
    con = connection(Path(config['extensions']))
    catalog.attach(con, config['database'], Path(config['data']))
    con.register('tail', pq.read_table(config['tail']))
    con.execute('BEGIN')
    con.execute('INSERT INTO lake.taiex SELECT * FROM tail')
    (output/(config['writer']+'.ready')).write_text('ready\n')
    wait_for([output/'commit.go'])
    con.execute('COMMIT')
    con.close()
    atomic_write_json(output/(config['writer']+'.result.json'),
                      {'state':'committed', 'pid':os.getpid(), 'rows':pq.read_metadata(config['tail']).num_rows,
                       'complete_wall_seconds':time.perf_counter()-started})


def ducklake_trial(table, base, tails, output: Path, extensions: Path, control_env: Path) -> dict:
    import pyarrow.parquet as pq
    started = time.perf_counter()
    catalog = CatalogDatabases(output, control_env)
    con = connection(extensions)
    processes = []
    handles = []
    try:
        name = catalog.create('ducklake')
        data = output/'data'
        catalog.attach(con, name, data)
        con.register('base', base)
        con.execute('CREATE TABLE lake.taiex AS SELECT * FROM base')
        first = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
        start_files = inventory(data)
        for i, tail in enumerate(tails):
            writer = f'writer-{i}'
            file = output/(writer+'.parquet')
            pq.write_table(tail,file)
            config = {'output':str(output), 'database':name, 'data':str(data), 'tail':str(file),
                      'writer':writer, 'extensions':str(extensions), 'control_env':str(control_env)}
            path = output/(writer+'.json')
            atomic_write_json(path, config)
            handle = (output/(writer+'.log')).open('wb')
            handles.append(handle)
            processes.append(subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--writer',str(path)],
                                               stdout=handle, stderr=subprocess.STDOUT))
        wait_for([output/'writer-0.ready',output/'writer-1.ready'], processes)
        (output/'commit.go').write_text('commit\n')
        for process in processes:
            if process.wait(timeout=60):
                raise RuntimeError('independent DuckLake writer failed; preserve its private log')
        same_values(con.execute('SELECT * FROM lake.taiex').to_arrow_table(), table)
        same_values(con.execute(f'SELECT * FROM lake.taiex AT (VERSION => {first})').to_arrow_table(),base)
        con.execute('BEGIN'); con.execute('DELETE FROM lake.taiex'); con.execute('ROLLBACK')
        same_values(con.execute('SELECT * FROM lake.taiex').to_arrow_table(),table)
        snapshots = [{k:v.isoformat() if isinstance(v,datetime) else v for k,v in row.items()}
                     for row in con.execute("SELECT * FROM ducklake_snapshots('lake')").to_arrow_table().to_pylist()]
        files = inventory(data)
        if any(files.get(k) != v for k,v in start_files.items()):
            raise ValueError('a historical committed data file was changed by append')
        con.close(); con = None
        dump = catalog.admin('',name,executable='pg_dump')
        (output/'catalog.dump').write_bytes(dump)
        restore = catalog.create('restore')
        catalog.admin('',restore,executable='pg_restore',body=dump)
        copied = output/'restored-data'
        shutil.copytree(data,copied)
        if inventory(copied) != files:
            raise ValueError('restored Parquet bytes differ')
        con = connection(extensions)
        catalog.attach(con,restore,copied)
        same_values(con.execute('SELECT * FROM lake.taiex').to_arrow_table(),table)
        same_values(con.execute(f'SELECT * FROM lake.taiex AT (VERSION => {first})').to_arrow_table(),base)
        writers = [json.loads((output/f'writer-{i}.result.json').read_bytes()) for i in range(2)]
        result = {'state':'accepted', 'catalog':'PostgreSQL', 'rows':table.num_rows,
                  'base_snapshot':first, 'snapshots':snapshots, 'writers':writers,
                  'overlapping_transactions':True, 'exact_source_values':True,
                  'rollback_verified':True, 'historical_snapshot_verified':True,
                  'catalog_and_independent_data_copy_restored':True,
                  'catalog_dump_sha256':hashlib.sha256(dump).hexdigest(), 'data_files':files,
                  'complete_wall_seconds':time.perf_counter()-started,
                  'scope':'two independent processes on one node; remote shared object-store writers not tested'}
        atomic_write_json(output/'acceptance.json',result)
        return result
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate();process.wait(timeout=10)
        for handle in handles:
            handle.close()
        if con is not None:
            con.close()
        catalog.cleanup()


def iceberg_trial(table, base, tails, output: Path, extensions: Path) -> dict:
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table import StaticTable
    from pyiceberg.types import StringType
    started = time.perf_counter()
    # Iceberg strings use Arrow string offsets; values and clocks remain exact.
    schema = pa.schema([pa.field(f.name, pa.string() if pa.types.is_large_string(f.type) else f.type,
                                nullable=f.nullable) for f in table.schema])
    table,base = table.cast(schema),base.cast(schema)
    tails = [tail.cast(schema) for tail in tails]
    catalog = SqlCatalog('trial', uri='sqlite:///'+str(output/'catalog.sqlite'),
                         warehouse=str(output/'warehouse'))
    catalog.create_namespace('stockagent_trial')
    target = catalog.create_table('stockagent_trial.taiex',schema=schema,
                                  properties={'format-version':'2'})
    target.append(base)
    first_snapshot = target.current_snapshot().snapshot_id
    first_metadata = Path(target.metadata_location)
    first_sha = sha256(first_metadata)
    a = catalog.load_table('stockagent_trial.taiex')
    b = catalog.load_table('stockagent_trial.taiex')
    stale_start = a.metadata_location == b.metadata_location
    txa, txb = a.transaction(), b.transaction()
    txa.append(tails[0]);txb.append(tails[1])
    conflict_events=[]
    class CommitLog(logging.Handler):
        def emit(self,record):
            message=record.getMessage()
            if 'Commit failed due to a concurrent update' in message:
                conflict_events.append(message)
    logger=logging.getLogger('pyiceberg');handler=CommitLog();logger.addHandler(handler)
    conflict = False
    try:
        txa.commit_transaction()
        try:
            txb.commit_transaction()
        except CommitFailedException:
            conflict = True
            b.refresh(); b.append(tails[1])
    finally:
        logger.removeHandler(handler)
    target.refresh()
    same_values(target.scan().to_arrow(),table)
    same_values(target.scan(snapshot_id=first_snapshot).to_arrow(),base)
    if sha256(first_metadata) != first_sha:
        raise ValueError('immutable Iceberg metadata changed')
    before_evolution = str(target.metadata_location)
    with target.update_schema() as update:
        update.add_column('_trial_optional_note',StringType())
    current = target.scan().to_arrow()
    if current['_trial_optional_note'].null_count != table.num_rows:
        raise ValueError('schema evolution altered existing observations')
    same_values(current.drop(['_trial_optional_note']),table)
    con = connection(extensions)
    try:
        con.execute('LOAD iceberg')
        sql = 'SELECT * FROM iceberg_scan('+sql_text(target.metadata_location)+')'
        other_engine = con.execute(sql).to_arrow_table()
        same_values(other_engine.drop(['_trial_optional_note']),table)
        same_values(con.execute('SELECT * FROM iceberg_scan('+sql_text(first_metadata)+')').to_arrow_table(),base)
    finally:
        con.close()
    recovered = StaticTable.from_metadata(target.metadata_location)
    same_values(recovered.scan().to_arrow().drop(['_trial_optional_note']),table)
    files = inventory(output/'warehouse')
    catalog.engine.dispose()
    result = {'state':'accepted', 'rows':table.num_rows, 'base_snapshot_id':first_snapshot,
              'current_metadata':str(target.metadata_location), 'pre_evolution_metadata':before_evolution,
              'first_metadata_sha256':first_sha, 'both_writers_started_from_same_metadata':stale_start,
              'optimistic_conflict_observed':conflict or bool(conflict_events),
              'sdk_automatic_commit_retries':len(conflict_events), 'commit_conflict_events':conflict_events,
              'application_refresh_retry':conflict, 'exact_source_values':True,
              'historical_snapshot_verified':True, 'additive_schema_verified':True,
              'native_duckdb_iceberg_scan_verified':True, 'metadata_reopen_verified':True,
              'warehouse_files':files, 'catalog_sha256':sha256(output/'catalog.sqlite'),
              'complete_wall_seconds':time.perf_counter()-started,
              'scope':'PyIceberg SQLite catalog + native DuckDB read; Spark/Flink/Trino, remote catalog and HA not tested'}
    atomic_write_json(output/'acceptance.json',result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--extensions',type=Path)
    parser.add_argument('--engine',choices=('ducklake','iceberg','both'),default='both')
    parser.add_argument('--control-env',type=Path,default=Path('/etc/stockagent/control-plane.env'))
    parser.add_argument('--writer',type=Path)
    args = parser.parse_args()
    if args.writer:
        ducklake_writer(args.writer);return
    if not all((args.source,args.output,args.extensions)):
        parser.error('source, output and extensions are required')
    import pyarrow.parquet as pq
    source = args.source.resolve(strict=True)
    output = args.output.absolute()
    output.mkdir(parents=True,mode=0o700,exist_ok=False)
    source_sha = sha256(source)
    private = output/'source.parquet'
    shutil.copyfile(source,private)
    if sha256(private) != source_sha or sha256(source) != source_sha:
        raise ValueError('source changed while capturing this trial')
    private.chmod(0o400)
    table = pq.read_table(private)
    if table.num_rows < 100 or 'date' not in table.column_names:
        raise ValueError('trial requires at least 100 real dated source observations')
    import pyarrow.compute as pc
    table = table.take(pc.sort_indices(table,sort_keys=[('date','ascending')]))
    tail_rows = min(128,table.num_rows//10)
    base = table.slice(0,table.num_rows-tail_rows*2)
    tails = [table.slice(base.num_rows,tail_rows),table.slice(base.num_rows+tail_rows)]
    extensions = args.extensions.resolve(strict=True)
    con = connection(extensions)
    for extension in ['ducklake','postgres','iceberg']:
        con.execute('INSTALL '+extension)
        con.execute('LOAD '+extension)
    plugins = con.execute('SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded').fetchall()
    con.close()
    result = {'schema_version':1, 'observed_at_utc':datetime.now(timezone.utc).isoformat(),
              'source':str(source), 'source_sha256':source_sha, 'source_rows':table.num_rows,
              'runtime':runtime_identity(), 'script_sha256':sha256(Path(__file__).resolve()),
              'extensions':plugins, 'extension_files':inventory(extensions)}
    atomic_write_json(output/'inputs.json',result)
    for engine in ('ducklake','iceberg'):
        if args.engine not in (engine,'both'):
            continue
        dest = output/engine;dest.mkdir(mode=0o700)
        print(json.dumps({'event':'started','engine':engine}),flush=True)
        if engine == 'ducklake':
            proof = ducklake_trial(table,base,tails,dest,extensions,args.control_env.resolve(strict=True))
        else:
            proof = iceberg_trial(table,base,tails,dest,extensions)
        result[engine] = proof
        print(json.dumps({'event':'accepted','engine':engine,'rows':proof['rows'],
                          'complete_wall_seconds':proof['complete_wall_seconds']}),flush=True)
    if sha256(source) != source_sha:
        raise ValueError('live source changed during the bounded trial; repeat from a fixed release')
    result['state'] = 'accepted'
    result['source_unchanged'] = True
    atomic_write_json(output/'acceptance.json',result)


if __name__ == '__main__':
    main()
