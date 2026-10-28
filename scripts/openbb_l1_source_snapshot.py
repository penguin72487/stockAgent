"""Execute complete source SQL on a coherent, invocation-local memory backup.

The original SQL, affinities, NULL behavior and path resolver remain exact.
Copying pages in order avoids millions of scattered filesystem page fetches;
the dedicated snapshot is closed before derivative work continues. There are
no source writes, persistent covering indexes or additional writer triggers.
Insufficient physical capacity or an uncommitted owner transaction uses the
original live query; it never commits someone else's transaction to copy it.
"""
from __future__ import annotations

from contextlib import closing
from pathlib import Path
import os
import resource
import sqlite3
import time

POLICY = 'openbb_l1_complete_memory_source_snapshot_v1'


def resident_bytes():
    try:
        return int(Path('/proc/self/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
    except (OSError, ValueError, IndexError):
        return None


def release_unused_pages():
    """Return already freed libc pages to the OS; never cap live allocations."""
    import ctypes
    try:
        trim = ctypes.CDLL(None).malloc_trim
    except (AttributeError, OSError):
        return None
    trim.argtypes, trim.restype = [ctypes.c_size_t], ctypes.c_int
    return bool(trim(0))


def available_memory() -> int:
    try:
        for line in Path('/proc/meminfo').read_text().splitlines():
            if line.startswith('MemAvailable:'):
                return int(line.split()[1])*1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


def source_rows(connection, sql, parameters, *, resolver, audit):
    """Return every original SQL row; include backup and close in telemetry."""
    started, cpu = time.perf_counter(), time.process_time()
    audit.update(policy=POLICY, state='canonical_live_query')
    databases = connection.execute('PRAGMA database_list').fetchall()
    main = next((row[2] for row in databases if row[1]=='main'), '')
    page_count = connection.execute('PRAGMA page_count').fetchone()[0]
    page_size = connection.execute('PRAGMA page_size').fetchone()[0]
    size = page_count*page_size
    available = available_memory()
    audit.update(database_bytes=size, memory_available_before=available)
    # This is capacity admission for the extra complete copy, not a runtime
    # utilization ceiling. Existing work keeps its full canonical fallback.
    reason = ('owner_has_uncommitted_transaction' if connection.in_transaction
              else 'not_a_file_backed_main_database' if not main
              else 'temporary_source_tables' if connection.execute(
                  "SELECT 1 FROM sqlite_temp_schema WHERE name IN "
                  "('tasks','l1_compaction_members','l1_compaction_segments') LIMIT 1").fetchone()
              else 'insufficient_capacity_for_complete_copy' if available < size*2
              else None)
    try:
        if reason is not None:
            audit['reason'] = reason
            return connection.execute(sql, parameters).fetchall()
        audit['state'] = 'snapshot_copy_started'
        with closing(sqlite3.connect(':memory:')) as snapshot:
            connection.backup(snapshot)
            audit['backup_wall_s'] = time.perf_counter()-started
            audit['backup_cpu_s'] = time.process_time()-cpu
            snapshot.row_factory = connection.row_factory
            snapshot.execute('PRAGMA query_only=ON')
            snapshot.execute('PRAGMA temp_store=MEMORY')
            snapshot.create_function('stockagent_resolve_path', 1, resolver, deterministic=True)
            rows = snapshot.execute(sql, parameters).fetchall()
            audit.update(state='complete_snapshot_query', rows=len(rows),
                         snapshot_bytes=snapshot.execute('PRAGMA page_count').fetchone()[0]*page_size)
        return rows
    finally:
        if audit['state']=='complete_snapshot_query':
            audit['rss_before_allocator_release'] = resident_bytes()
            audit['allocator_released_pages'] = release_unused_pages()
            audit['rss_after_allocator_release'] = resident_bytes()
        audit.update(wall_s=time.perf_counter()-started, cpu_s=time.process_time()-cpu,
                     process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)
