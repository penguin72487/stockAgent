"""Bounded, read-only native SQL/digest probe; never an export or publisher.

Use the locked lakehouse role. Each supervised run measures one fresh-process
query strategy at the native limits. A recorded query failure is diagnostic
evidence, not performance or data acceptance.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import gc
import hashlib
import json
from pathlib import Path
import resource
import subprocess
import time

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.benchmark_lakehouse_source_wave import limits, match_native_limits
from scripts.verify_lakehouse_workflow_history import owner_identity
from stockagent.control import lakehouse


canonical_row_digest = lakehouse._canonical_row_digest
batches_as_rows = lakehouse._query_rows
paged_metadata_rows = lakehouse._metadata_rows


def engineering_reference_limits():
    """Extra memory only for the explicitly requested original semantic oracle."""
    group = Path('/proc/self/cgroup').read_text().split('0::', 1)[1].strip()
    unit = Path(group).name
    if not unit.startswith('stockagent-agent-lakehouse-source-wave-') or not unit.endswith('.service'):
        raise ValueError('semantic reference must never adjust a production cgroup')
    available = next(int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines()
                     if line.startswith('MemAvailable:'))
    if available < 8*1024**3:
        raise ValueError('semantic reference needs at least 8GiB available host headroom')
    subprocess.run(['systemctl', 'set-property', '--runtime', unit,
                    'MemoryHigh=3G', 'MemoryMax=4G'], check=True, timeout=10)
    return limits()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--order', choices=('all', 'unique_keys', 'paged_keys'), required=True)
    parser.add_argument('--memory-limit', choices=('512MB', '1GB', '2GB'), default='512MB')
    parser.add_argument('--materialized-reference', action='store_true',
                        help='explicit engineering-only original full JSON/hash oracle; not a fair speed comparator')
    parser.add_argument('--page-mib', type=int, choices=(16, 32, 64), default=16)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists() or not args.output.absolute().resolve().is_relative_to(Path('artifacts/benchmarks').resolve()):
        parser.error('a fresh private benchmark receipt is required')
    if args.materialized_reference and args.order != 'all':
        parser.error('the original reference requires the original ORDER BY ALL')
    resource_limits = match_native_limits()
    if args.materialized_reference:
        resource_limits = engineering_reference_limits()
    owner = owner_identity()
    source = Path(__file__).read_text()
    source_digest = hashlib.sha256(source.encode()).hexdigest()
    implementation = Path(lakehouse.__file__).read_text()
    atomic_write_text(args.output.with_suffix('.probe.py'), source, mode=0o600, durable=True)
    atomic_write_text(args.output.with_suffix('.lakehouse.py'), implementation, mode=0o600, durable=True)
    c = lakehouse.configuration()
    lakehouse.guard(c)
    connection = lakehouse.connect(c, read_only=True)
    import duckdb
    result = {'schema_version': 1, 'state': 'observed_readonly_row_digest',
              'observed_at_utc': datetime.now(UTC).isoformat(), 'order': args.order,
              'duckdb_version': duckdb.__version__, 'memory_limit': args.memory_limit,
              'resource_limits': resource_limits, 'materialized_reference': args.materialized_reference,
              'page_mib': args.page_mib, 'probe_source_sha256': source_digest,
              'lakehouse_source_sha256': hashlib.sha256(implementation.encode()).hexdigest(),
              'query_completed': False, 'native_owner': owner,
              'production_catalog_writes': 0, 'provider_calls': 0, 'production_service_controls': 0}
    try:
        connection.execute('SET memory_limit=' + lakehouse.sql_text(args.memory_limit))
        result['snapshot_before'] = connection.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
        count, unique = connection.execute('SELECT count(*),count(DISTINCT (relative,sha256)) FROM lake.source_metadata').fetchone()
        result.update(expected_rows=count, unique_metadata_keys=unique)
        if args.order == 'unique_keys' and count != unique:
            raise ValueError('narrow ordering requires proven unique metadata keys')
        order = 'ALL' if args.order == 'all' else 'relative,sha256,bytes'
        gc.collect()
        wall, cpu = time.perf_counter(), time.process_time()
        phase = 'execute_ordered_query'
        try:
            if args.order == 'paged_keys':
                phase = 'consume_complete_paged_row_stream'
                rows = paged_metadata_rows(connection, maximum_bytes=args.page_mib*1024**2)
            else:
                connection.execute('SELECT * FROM lake.source_metadata ORDER BY ' + order)
                phase = 'consume_complete_row_stream'
                rows = batches_as_rows(connection)
            if args.materialized_reference:
                materialized = list(rows)
                observed, digest = len(materialized), lakehouse.identity_sha256(materialized)
            else:
                observed, digest = canonical_row_digest(rows)
            if observed != count:
                raise ValueError('full registered row coverage differs')
            result.update(query_completed=True, observed_rows=observed, table_row_sha256=digest)
        except duckdb.OutOfMemoryException as error:
            result.update(error_type=type(error).__name__, error_phase=phase, error_message=str(error))
        finally:
            result.update(wall_seconds=time.perf_counter()-wall, cpu_seconds=time.process_time()-cpu,
                          process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)
        result['snapshot_after'] = connection.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
    finally:
        connection.close()
    result['native_owner_unchanged'] = owner_identity() == owner
    result['probe_source_unchanged'] = Path(__file__).read_text() == source
    result['lakehouse_source_unchanged'] = Path(lakehouse.__file__).read_text() == implementation
    result['same_registered_snapshot'] = result['snapshot_before'] == result['snapshot_after']
    result['caveats'] = ['Complete metadata row query/digest, not full capture/export/restore or all-service throughput.',
                         'No hash equivalence is claimed until independently compared at the same registered snapshot.',
                         'Any higher-memory materialized reference is a semantic oracle, never native-limit performance acceptance.']
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps({k: v for k, v in result.items() if k != 'error_message'}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
