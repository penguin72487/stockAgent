"""ABBA complete canonical publication/recovery after a real child-process exit.

Only generated, owned /var/tmp fixtures are touched. Includes the abandoned
attempt, retry, full original SHA/CRC, durable readback, incremental revision,
and independent old-version restore. Not a D/NAS or all-source speed claim.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import resource
import stat
import statistics
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.packing_checkpoint import SealedPackHints

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = 'sealed_pack_retry_complete_pipeline_abba_v1'


def identity():
    names = (Path(__file__), Path(packed.__file__), Path(migration.__file__),
             ROOT / 'stockagent/data_sync/packing_checkpoint.py',
             ROOT / 'stockagent/data_sync/packed_metadata.py')
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in names}


def digest_tree(path):
    digest = hashlib.sha256()
    for member in sorted(path.iterdir()):
        digest.update(member.name.encode() + b'\0')
        with member.open('rb') as stream:
            while block := stream.read(8 * 1024**2):
                digest.update(block)
    return digest.hexdigest()


def inputs(base, enabled):
    source, parent = base / 'source', base / 'cold'
    policy = {'private_history': {'enabled': True, 'relative_root': '.local-state/private-history',
        'shared_export': False, 'nas_export': False, 'source_retirement': False}}
    cold = migration.private_history_root(parent, policy)
    index = SealedPackHints(base / 'journal/sealed-pack-checkpoints/example', cold, source, 'example') if enabled else None
    return cold, source, index


def publish(base, enabled):
    cold, source, index = inputs(base, enabled)
    selected = packed.publish_packed_snapshot(cold, 'example', source, node_id='fixture',
        pack_buckets=1, file_chunk_bytes=1024**2, batch_directory_fsync=True,
        private_observation_cohort=True, sealed_pack_hints=index)
    return selected, index


def admit_fixture(base):
    info = base.lstat()
    if (base.resolve() != base or base.parent != Path('/var/tmp')
            or not base.name.startswith('stockagent-sealed-pack-benchmark-')
            or info.st_uid != os.geteuid() or info.st_mode & 0o777 != 0o700):
        raise ValueError('only the exact owned private benchmark fixture is permitted')


def trial(files, enabled, *, disable_pack_digest=False, reference_cohort_paths=False):
    with tempfile.TemporaryDirectory(prefix='stockagent-sealed-pack-benchmark-', dir='/var/tmp') as tmp:
        base = Path(tmp)
        source = base / 'source'
        source.mkdir()
        for number in range(files):
            raw = json.dumps({'value': -number, 'missing': None, 'unit': 'shares',
                'payload': hashlib.sha256(str(number).encode()).hexdigest() * 128}).encode()
            (source / f'{number:06}.json').write_bytes(raw)
        packed.initialize_packed_layout(base / 'cold', node_id='fixture')
        cold, _, _ = inputs(base, enabled)
        expected = digest_tree(source)
        cpu = time.process_time()
        children_cpu = resource.getrusage(resource.RUSAGE_CHILDREN)
        stamp = time.perf_counter()
        child = subprocess.run([sys.executable, '-m', 'scripts.benchmark_sealed_pack_recovery',
            '--crash-child', str(base), '--files', str(files), *(['--enabled'] if enabled else []),
            *(['--reference-cohort-paths'] if reference_cohort_paths else []),
            *(['--disable-pack-digest'] if disable_pack_digest else [])],
            capture_output=True, text=True, timeout=600)
        if child.returncode != 88 or list((cold / 'heads/example').glob('*.json')):
            raise ValueError('the fixed deliberate exit was not an uncommitted attempt')
        original_writer = packed._write_pack
        writes = []
        def observed(root, entries, target, **options):
            writes.append(len(entries))
            return original_writer(root, entries, target, **options)
        packed._write_pack = observed
        try:
            first, index = publish(base, enabled)
        finally:
            packed._write_pack = original_writer
        retry_files_written = sum(writes)
        first_proof = packed.verify_packed_snapshot(cold, first, reconstruct_all=True)
        restored = packed.fetch_packed_snapshot(cold, base / 'first-readback', first)
        if digest_tree(restored) != expected:
            raise ValueError('all original source bytes differ after retry')
        inventories = [first.manifest['archive']['inventory']['sha256']]
        if index is not None:
            retirement = migration._retire_owned_scratch(base / 'journal', 'example', {
                'sealed_pack_hint_files': list(index.receipts.values())})
            if retirement['state'] != 'retired' or list(index.root.glob('*.json')):
                raise ValueError('verified disposable checkpoint metadata was not retired')
        (source / '000000.json').write_bytes(b'revised current value,-2.5,NULL\n')
        (source / '000001.json').unlink()
        (source / 'new.json').write_bytes(b'new published observation\n')
        second, _ = publish(base, enabled)
        second_proof = packed.verify_packed_snapshot(cold, second, reconstruct_all=True)
        now = packed.fetch_packed_snapshot(cold, base / 'second-readback', second)
        if digest_tree(now) != digest_tree(source):
            raise ValueError('incremental all-file readback differs')
        old = packed.fetch_packed_snapshot(cold, base / 'old-readback', first)
        if digest_tree(old) != expected:
            raise ValueError('the old fixed original version was damaged')
        inventories.append(second.manifest['archive']['inventory']['sha256'])
        child_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        spent = time.process_time() - cpu + child_usage.ru_utime + child_usage.ru_stime - children_cpu.ru_utime - children_cpu.ru_stime
        return {'variant': 'checkpoint' if enabled else 'reference',
            'complete_wall_seconds': time.perf_counter() - stamp, 'complete_cpu_seconds': spent,
            'peak_process_rss_bytes': max(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, child_usage.ru_maxrss) * 1024,
            'source_files_per_version': files, 'retry_pack_member_writes': retry_files_written,
            'retry_full_source_hash_and_pack_recovery': True,
            'first_files_independently_reconstructed': first_proof['independently_reconstructed_files'],
            'second_files_independently_reconstructed': second_proof['independently_reconstructed_files'],
            'old_version_all_original_readback': True, 'inventory_sha256': inventories,
            'hint_statistics': index.statistics if index else None,
            'deliberate_crash_exit': child.returncode, 'production_source_payloads_deleted': 0,
            'generated_fixture_deletions': 1}


def cohort_reference_read_path(source, entry):
    """Pre-optimization constructor, only for a fixed-input benchmark oracle."""
    return entry.read_path or source.joinpath(*PurePosixPath(entry.path).parts)


def cohort_reference_guard(source, entries, root_identity, *, progress=None):
    """Pre-optimization final fence, including its unused control paths."""
    if packed._source_stat(source.lstat())[:2] != root_identity:
        raise packed.SnapshotError('observation cohort root was replaced')
    completed = 0
    for completed, entry in enumerate(entries, 1):
        path = packed._entry_read_path(source, entry)
        if entry.kind == 'file':
            if entry.atomic_json_control:
                if not entry.sha256 or not entry.storage:
                    raise packed.SnapshotError('atomic JSON observation has no sealed storage')
            else:
                packed._ensure_source_stat(path, entry)
        elif entry.kind == 'directory':
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != entry.mode:
                raise packed.SnapshotError(f'selected cohort directory changed: {path}')
        elif not path.is_symlink() or os.readlink(path) != entry.target:
            raise packed.SnapshotError(f'selected cohort symlink changed: {path}')
        if progress is not None and completed % 4096 == 0:
            progress(completed)
    if progress is not None:
        progress(completed)


@contextmanager
def _cohort_path_mode(enabled):
    if enabled:
        yield
    else:
        with patch.object(packed, '_entry_read_path', cohort_reference_read_path), \
                patch.object(packed, '_guard_observation_cohort', cohort_reference_guard):
            yield


def cohort_path_trial(files, enabled):
    """Same complete crash/retry/old-version restoration, no fence removed."""
    fences, paths = [], []
    with _cohort_path_mode(enabled):
        native_guard, native_path = packed._guard_observation_cohort, packed._entry_read_path
        def observed_guard(*args, **options):
            started, cpu = time.perf_counter(), time.process_time()
            native_guard(*args, **options)
            fences.append({'seconds': time.perf_counter() - started,
                           'cpu_seconds': time.process_time() - cpu})
        def observed_path(*args):
            paths.append(1)
            return native_path(*args)
        with patch.object(packed, '_guard_observation_cohort', observed_guard), \
                patch.object(packed, '_entry_read_path', observed_path):
            result = trial(files, True, reference_cohort_paths=not enabled)
    return {**result, 'variant': 'checkpoint' if enabled else 'reference',
        'cohort_path_workload': True, 'cohort_fence_calls': len(fences),
        'cohort_fence_seconds': sum(row['seconds'] for row in fences),
        'cohort_fence_cpu_seconds': sum(row['cpu_seconds'] for row in fences),
        'entry_read_path_calls': len(paths), 'sealed_pack_recovery_enabled_in_both_variants': True}


@contextmanager
def _pack_digest_mode(enabled):
    """Benchmark-only reference; never a production switch or archive format."""
    native = packed._SeekablePackBuffer.__init__
    def initialize(buffer, stream, **options):
        native(buffer, stream, **{**options, 'digest_writes': enabled})
    with patch.object(packed._SeekablePackBuffer, '__init__', initialize):
        yield


def pack_digest_trial(files, enabled):
    """Full real crash/retry/revision/old-version recovery, same ZIP bytes."""
    native_write, native_sha = packed._write_pack, packed.sha256_file
    writes, reads, digests = [], [], []
    def observed_write(root, entries, target, **options):
        started, cpu = time.perf_counter(), time.process_time()
        digest = native_write(root, entries, target, **options)
        writes.append({'seconds': time.perf_counter() - started,
                       'cpu_seconds': time.process_time() - cpu,
                       'bytes': target.stat().st_size, 'fused': digest is not None})
        if digest is not None:
            digests.append(digest)
        return digest
    def observed_sha(path, *args, **options):
        started, cpu = time.perf_counter(), time.process_time()
        digest = native_sha(path, *args, **options)
        if path.name.startswith('pack-') and path.suffix == '.partial':
            reads.append({'seconds': time.perf_counter() - started,
                          'cpu_seconds': time.process_time() - cpu,
                          'bytes': path.stat().st_size})
            digests.append(digest)
        return digest
    with _pack_digest_mode(enabled), patch.object(packed, '_write_pack', observed_write), \
            patch.object(packed, 'sha256_file', observed_sha):
        result = trial(files, True, disable_pack_digest=not enabled)
    if len(digests) != len(writes):
        raise ValueError('each finalized pack must have exactly one producer digest')
    return {**result, 'variant': 'checkpoint' if enabled else 'reference',
            'pack_digest_fusion_workload': True,
            'same_digest_mode_in_crashed_child': True,
            'pack_writes': writes, 'producer_pack_rehashes': reads,
            'producer_pack_rehash_bytes': sum(row['bytes'] for row in reads),
            'producer_pack_rehash_calls': len(reads),
            'pack_write_and_producer_hash_seconds': sum(row['seconds'] for row in (*writes, *reads)),
            'pack_write_and_producer_hash_cpu_seconds': sum(row['cpu_seconds'] for row in (*writes, *reads)),
            'pack_archive_sha256': digests,
            'sealed_pack_recovery_enabled_in_both_variants': True}


def _reference_inventory_writer(temporary, entries, *, progress=None):
    """Original per-row gzip plus full second read; benchmark only."""
    with temporary.open('xb') as raw:
        with gzip.GzipFile(filename='', mode='wb', compresslevel=6, fileobj=raw, mtime=0) as stream:
            completed = 0
            for completed, entry in enumerate(entries, 1):
                stream.write(packed._canonical_json_bytes(entry.inventory_row()))
                if progress is not None and completed % 4096 == 0:
                    progress(completed)
        raw.flush()
        os.fsync(raw.fileno())
    digest = packed.sha256_file(temporary)
    if progress is not None:
        progress(completed)
    return digest


def inventory_trial(files, enabled):
    """Same real crash/retry/revisions/recovery, changing only inventory I/O."""
    writer = packed._write_inventory if enabled else _reference_inventory_writer
    writes = []
    def observed(temporary, entries, **options):
        started, cpu = time.perf_counter(), time.process_time()
        digest = writer(temporary, entries, **options)
        writes.append({'seconds': time.perf_counter() - started,
                       'cpu_seconds': time.process_time() - cpu,
                       'bytes': temporary.stat().st_size, 'sha256': digest})
        return digest
    with patch.object(packed, '_write_inventory', observed):
        # Both variants use the existing sealed-pack recovery. The deliberately
        # aborted child exits before inventory serialization in either variant.
        result = trial(files, True)
    return {**result, 'variant': 'checkpoint' if enabled else 'reference',
            'inventory_buffering_workload': True, 'inventory_writes': writes,
            'inventory_write_seconds': sum(row['seconds'] for row in writes),
            'inventory_write_cpu_seconds': sum(row['cpu_seconds'] for row in writes),
            'sealed_pack_recovery_enabled_in_both_variants': True}


def object_path_trial(files, enabled):
    """Complete crash/retry/readback with only name lookup representation changed."""
    native = packed.MetadataRows.object_paths
    lookups = []
    def observed(rows, digest):
        started, cpu = time.perf_counter(), time.process_time()
        count = 0
        paths = (native(rows, digest) if enabled else
                 (rows._encode(value)['path'] for value in rows.object_rows(digest)))
        for path in paths:
            count += 1
            yield path
        lookups.append({'rows': count, 'spilled': rows.spilled,
                        'seconds': time.perf_counter() - started,
                        'cpu_seconds': time.process_time() - cpu})
    limit = min(128, files // 4)
    with patch.object(packed.MetadataRows, 'object_paths', observed), \
            patch.object(packed, 'METADATA_MEMORY_ROWS', limit):
        result = trial(files, True)
    if not lookups or any(not row['spilled'] for row in lookups):
        raise ValueError('object-name comparison did not exercise bounded spilled metadata')
    return {**result, 'variant': 'checkpoint' if enabled else 'reference',
        'object_path_index_workload': True, 'metadata_memory_rows': limit,
        'object_path_lookups': lookups,
        'object_path_rows': sum(row['rows'] for row in lookups),
        'object_path_seconds': sum(row['seconds'] for row in lookups),
        'object_path_cpu_seconds': sum(row['cpu_seconds'] for row in lookups),
        'sealed_pack_recovery_enabled_in_both_variants': True}


def metadata_update_trial(files, enabled):
    """Full spilled crash/retry/recovery, changing only metadata update work."""
    native_update, native_many = packed.MetadataRows.update, packed.MetadataRows.update_many
    groups = []
    def reference_update(rows, value, *, changed_bucket=None):
        if rows._db is None:
            return native_update(rows, value, changed_bucket=changed_bucket)
        path, kind, size, digest, bucket, payload = rows._record(value, changed_bucket)
        result = rows._db.execute('UPDATE entries SET kind=?,size=?,object_sha=?,'
            'bucket=COALESCE(?,bucket),data=? WHERE path=?', (kind, size, digest, bucket, payload, path))
        if result.rowcount != 1:
            raise ValueError('reference update lost a census member')
    def observed_many(rows, values):
        started, cpu = time.perf_counter(), time.process_time()
        count = 0
        def observed_values():
            nonlocal count
            for value in values:
                count += 1
                yield value
        if enabled:
            native_many(rows, observed_values())
        else:
            for value in observed_values():
                reference_update(rows, value)
        groups.append({'rows': count, 'spilled': rows.spilled,
                       'seconds': time.perf_counter() - started,
                       'cpu_seconds': time.process_time() - cpu})
    with patch.object(packed.MetadataRows, 'update', native_update if enabled else reference_update), \
            patch.object(packed.MetadataRows, 'update_many', observed_many), \
            patch.object(packed, 'METADATA_MEMORY_ROWS', min(128, files // 4)):
        result = trial(files, True)
    if not groups or any(not row['spilled'] for row in groups):
        raise ValueError('metadata update comparison did not spill')
    return {**result, 'variant': 'checkpoint' if enabled else 'reference',
        'metadata_update_workload': True, 'metadata_update_groups': groups,
        'metadata_update_rows': sum(row['rows'] for row in groups),
        'metadata_update_seconds': sum(row['seconds'] for row in groups),
        'metadata_update_cpu_seconds': sum(row['cpu_seconds'] for row in groups),
        'sealed_pack_recovery_enabled_in_both_variants': True}


def mutable_trial(files, enabled, *, operational_controls=False, announcement_controls=False):
    """Full v3 upgrade -> publish/retry -> cold readback -> owned cleanup.

    The writer replaces its tail with IDENTICAL bytes after sealing the pack.
    Both variants must recover the exact same values. The reference retains
    the old full control recensus and ordinary payload-stat rejection; its
    real one-second bounded retry delay remains in complete wall time.
    """
    fixture = (_owned_operational_fixture() if announcement_controls else
        tempfile.TemporaryDirectory(prefix='stockagent-sealed-pack-benchmark-', dir='/var/tmp'))
    with fixture as tmp:
        base = Path(tmp)
        source = base / ('data_finmind' if announcement_controls else
                         'data_openBB' if operational_controls else 'data_bybit')
        cold = base / 'cold'
        (base / 'configs').mkdir()
        configuration = json.loads((ROOT / 'configs/columnar_storage.json').read_text())
        configuration['source_versioning']['roots'] = {}
        atomic_write_json(base / 'configs/columnar_storage.json', configuration)
        (source / 'raw').mkdir(parents=True)
        for number in range(files):
            (source / f'raw/{number:06}.bin').write_bytes(
                json.dumps({'value': -number, 'missing': None, 'unit': 'shares'}).encode() * 64)
        mutable = source / ('announcements/repair_plan.json' if announcement_controls else
                            'logs/supervisor.log' if operational_controls else '1m/_hot_tail/EXAMPLE.parquet')
        mutable.parent.mkdir(parents=True)
        mutable.write_bytes(b'{"requests":[],"value":-7,"missing":null}\n' if announcement_controls else
                            b'fixed original raw values=-7,NULL,shares\n' * 512)
        mutable_paths = [mutable]
        if announcement_controls:
            head = source / 'announcements/head.json'
            head.write_bytes(b'{"snapshot_sha256":"original","missing":null}\n')
            mutable_paths.append(head)
        elif operational_controls:
            journal = source / '_state/monitor_history.jsonl'
            journal.parent.mkdir()
            journal.write_bytes(b'{"value":-7,"missing":null}\n' * 512)
            projection = source / 'catalog/provider_eta.parquet'
            projection.parent.mkdir()
            # A real closed Parquet projection, including raw negative/null
            # values, not a filename standing in for a parsed training table.
            import pyarrow as pa
            import pyarrow.parquet as pq
            pq.write_table(pa.table({'value': [-7., None, 2.5]}), projection, compression='zstd')
            mutable_paths.extend((journal, projection))
        entry = {'dataset': 'example', 'source': str(source), 'publish': False, 'role': 'source',
                 'note': 'owned generated fixture', 'pack_buckets': 1, 'loose_threshold_mib': 8,
                 'file_chunk_bytes': 8 * 1024**2, 'reader_contract': 'packed-v2-chunks'}
        args = SimpleNamespace(sync_root=cold, migration_state=base / 'journal', node_id='fixture',
            retry_seconds=1, maximum_passes=1, watch=False, recover_missing_base_objects=False,
            source_lock_fd=None, retire_staging=True)
        packed.initialize_packed_layout(cold, node_id='fixture')
        private = migration.private_history_root(cold, configuration['source_versioning'])
        from contextlib import ExitStack
        native_locations = migration._mutable_control_file_locations
        def old_producers(stack):
            stack.enter_context(patch.object(migration, '_mutable_published_locations', lambda *_: {}))
            if operational_controls:
                stack.enter_context(patch.object(migration, '_mutable_streamed_control_locations', lambda *_: {}))
            if announcement_controls:
                stack.enter_context(patch.object(migration, '_mutable_control_file_locations',
                    lambda root: {directory: names for directory, names in native_locations(root).items()
                                  if directory != 'announcements'}))
        with ExitStack() as old:
            old_producers(old)
            prepared = migration._private_preparation(entry, args, source, private,
                                                      configuration['source_versioning'])
        old_scope = ('recursive_mutable_controls_v14' if announcement_controls else
                     'recursive_mutable_controls_v12' if operational_controls else 'recursive_mutable_controls_v3')
        prepared['control_scope'] = old_scope
        atomic_write_json(migration._preparation_path(args, 'example'), prepared)
        intent_path = Path(prepared['controls_staging']).with_suffix('.intent.json')
        intent = json.loads(intent_path.read_text())
        atomic_write_json(intent_path, {**intent, 'contract': old_scope})

        def tree(root):
            return {p.relative_to(root).as_posix(): packed.sha256_file(p)
                    for p in sorted(root.rglob('*')) if p.is_file()}
        expected = tree(source)
        native_write, native_freeze = packed._write_pack, migration._freeze_controls
        native_census = packed._collect_entries
        writes, replaced, census_files = [], [], []
        def census(root, **options):
            result = native_census(root, **options)
            if root == source:
                census_files.append(result[1]['files'])
            return result
        def writer(root, entries, target, **options):
            result = native_write(root, entries, target, **options)
            writes.append(len(entries))
            if not replaced:
                for path in mutable_paths:
                    replacement = path.with_name('.same-observation')
                    replacement.write_bytes(path.read_bytes())
                    replacement.replace(path)
                replaced.append(True)
            return result
        def old_controls(*argv, **options):
            return native_freeze(*argv, **{**options, 'upgrade_from': None})
        started, cpu_started = time.perf_counter(), time.process_time()
        with ExitStack() as execution:
            execution.enter_context(patch.object(packed, '_write_pack', writer))
            execution.enter_context(patch.object(packed, '_collect_entries', census))
            if not enabled:
                old_producers(execution)
                if not operational_controls and not announcement_controls:
                    execution.enter_context(patch.object(migration, '_freeze_controls', old_controls))
            result = migration.migrate_catalog(base, args, [entry])
            if not enabled:
                if result['datasets']['example']['state'] != 'waiting_retry':
                    raise ValueError('reference failed to exercise the mutable-payload rejection')
                time.sleep(1.05)  # Real retry deadline; no journal edit or fake clock.
                result = migration.migrate_catalog(base, args, [entry])
        if result['state'] != 'verified' or not replaced:
            raise ValueError('complete mutable-cohort publication did not verify')
        selected = packed.resolve_latest_packed(private, 'example')
        readback = packed.fetch_packed_snapshot(private, base / 'restored', selected)
        if tree(readback) != expected or tree(source) != expected:
            raise ValueError('frozen/full-retry versions differ from original values')
        row = result['datasets']['example']
        proof = json.loads(Path(row['recovery_receipt']).read_text())
        if (proof['independently_reconstructed_files'] != files + len(mutable_paths)
                or row['owned_scratch_retirement']['state'] != 'retired'):
            raise ValueError('original-file recovery or owned cleanup was skipped')
        return {'variant': 'checkpoint' if enabled else 'reference',
            'complete_wall_seconds': time.perf_counter() - started,
            'complete_cpu_seconds': time.process_time() - cpu_started,
            'peak_process_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            'retry_pack_member_writes': sum(writes), 'attempts': row['attempts'],
            'source_census_calls': len(census_files), 'source_census_files': sum(census_files),
            'inventory_sha256': [selected.manifest['archive']['inventory']['sha256']],
            'full_original_readback': True, 'owned_scratch_retired': True,
            'reference_retry_delay_seconds': 1.05 if not enabled else 0,
            'operational_controls_workload': operational_controls,
            'announcement_controls_workload': announcement_controls,
            'original_control_files': len(mutable_paths),
            'production_source_payloads_deleted': 0, 'generated_fixture_deletions': 1}


@contextmanager
def _owned_operational_fixture(existing=None):
    """Retain failures/references; remove only this verified private fixture."""
    import shutil
    from stockagent.data_sync.materialized_cache import process_references_many
    base = (Path(existing) if existing is not None else
            Path(tempfile.mkdtemp(prefix='stockagent-sealed-pack-benchmark-', dir='/var/tmp')))
    admit_fixture(base)
    if existing is not None and process_references_many([base]):
        raise ValueError(f'operational fixture is still active: {base}')
    try:
        yield base
    except BaseException as error:
        # Independent readback or reference rejection is never permission to
        # erase its evidence. The source itself was only read.
        error.add_note(f'Owned operational fixture retained for exact retry: {base}')
        raise
    else:
        admit_fixture(base)
        if process_references_many([base]):
            raise ValueError(f'verified operational fixture remains referenced: {base}')
        shutil.rmtree(base)


def live_operational_trial(provider, *, resume_fixture=None):
    """Read only reviewed live controls; verify copies in a private SSD fixture.

    This does not own a production head/preparation, query a provider or prove
    the entire source migrated. A fixed capture remains a non-atomic cohort.
    """
    names = {'finlab': 'data_finlab', 'openbb': 'data_openBB', 'finmind': 'data_finmind'}
    source = (ROOT / names[provider]).resolve(strict=True)
    if source.name != names[provider] or not source.is_dir():
        raise ValueError('live operational source is not the canonical named root')
    excluded = ['openbb_l1.duckdb'] if provider == 'openbb' else []
    # Metadata-only admission; no raw-tree scan or multi-GB catalog DB copy.
    locations = {}
    for mapping in (migration._mutable_control_file_locations(source),
                    migration._mutable_published_locations(source),
                    migration._mutable_streamed_control_locations(source)):
        for directory, files in mapping.items():
            if files is None:
                raise ValueError('live operational fixture requires exact names')
            locations.setdefault(directory, set()).update(files)
    proposed = [source / directory / name for directory, files in locations.items()
                for name in sorted(files) if (source / directory / name).exists()
                and (Path(directory) / name).as_posix() not in excluded]
    sizes = [path.lstat().st_size for path in proposed]
    if not proposed or len(proposed) > 128 or sum(sizes) > 2 * 1024**3:
        raise ValueError('live operational fixture exceeds its file/byte admission')
    with _owned_operational_fixture(resume_fixture) as base:
        import shutil
        if shutil.disk_usage(base).free < sum(sizes) * 3 + 5 * 1024**3:
            raise ValueError('live operational readback would cross its scratch reserve')
        started, cpu = time.perf_counter(), time.process_time()
        journal = base / 'journal'
        stage = journal / 'controls' / 'live-observation'
        captures = migration._freeze_controls(source, stage, excluded,
                                              upgrade_from='recursive_mutable_controls_v12')
        expected = {name: {'sha256': row['sha256'], 'bytes': row['bytes']}
                    for name, row in captures.items()}
        if (set(expected) != {path.relative_to(source).as_posix() for path in proposed}
                or not expected or len(expected) > 128
                or sum(row['bytes'] for row in expected.values()) > 2 * 1024**3):
            raise ValueError('captured live controls exceed the bounded admission')
        parent = base / 'cold'
        packed.initialize_packed_layout(parent, node_id='fixture')
        cold = migration.private_history_root(parent, {'private_history': {
            'enabled': True, 'relative_root': '.local-state/private-history',
            'shared_export': False, 'nas_export': False, 'source_retirement': False}})
        selected = packed.publish_packed_snapshot(cold, 'operational-observations', stage,
            node_id='fixture', pack_buckets=4, file_chunk_bytes=8 * 1024**2,
            batch_directory_fsync=True, private_observation_cohort=True)
        proof = packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)
        restored = packed.fetch_packed_snapshot(cold, base / 'restored', selected)
        actual = {path.relative_to(restored).as_posix(): {
                    'bytes': path.stat().st_size, 'sha256': packed.sha256_file(path)}
                  for path in restored.rglob('*') if path.is_file()}
        if actual != expected or proof['independently_reconstructed_files'] != len(expected):
            raise ValueError('live control original bytes differ after independent recovery')
        retirement = migration._retire_owned_scratch(journal, 'operational-observations',
                                                    {'frozen_control_files': captures})
        if retirement['state'] != 'retired':
            raise ValueError('live control fixture copies remain referenced; no cleanup authorized')
        return {'state': 'accepted_live_operational_observation_readback',
            'contract': 'live_operational_controls_private_fixture_v1', 'provider': provider,
            'control_scope': migration.CONTROL_SCOPE, 'observed_at_utc': datetime.now(UTC).isoformat(),
            'resumed_owned_fixture': str(base) if resume_fixture is not None else None,
            'observations': expected, 'files': len(expected),
            'logical_bytes': sum(row['bytes'] for row in expected.values()),
            'full_original_readback': True, 'owned_capture_copies_retired': True,
            'complete_wall_seconds': time.perf_counter() - started,
            'complete_cpu_seconds': time.process_time() - cpu,
            'production_source_writes': 0, 'provider_calls': 0,
            'production_cold_writes': 0, 'production_migration_claim': False,
            'globally_atomic_source_claim': False,
            'scope': 'reviewed live controls only; isolated local SSD copies, not D/NAS/all-source acceptance'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--files', type=int, default=2048)
    parser.add_argument('--trial', choices=('reference', 'checkpoint'))
    parser.add_argument('--crash-child', type=Path)
    parser.add_argument('--enabled', action='store_true')
    parser.add_argument('--disable-pack-digest', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--reference-cohort-paths', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--native-limits', action='store_true')
    parser.add_argument('--mutable-cohort', action='store_true',
                        help='measure complete v3 control upgrade and an atomic mutable-output rewrite')
    parser.add_argument('--operational-controls', action='store_true',
                        help='measure v12 -> reviewed log/JSONL/catalog observation capture, full retry and recovery')
    parser.add_argument('--announcement-controls', action='store_true',
                        help='measure v14 -> FinMind announcement heads, avoided whole-tree retry and full recovery')
    parser.add_argument('--inventory-buffering', action='store_true',
                        help='compare exact-byte inventory buffering/digest during the full crash/revision/recovery workflow')
    parser.add_argument('--object-path-index', action='store_true',
                        help='compare names-only covering index with full JSON-row decoding in complete spilled recovery')
    parser.add_argument('--metadata-updates', action='store_true',
                        help='compare per-row routing-index rewrites with batched storage-only metadata updates')
    parser.add_argument('--pack-digest-fusion', action='store_true',
                        help='compare separate pack SHA read with bounded sequential write-time SHA; full crash/recovery')
    parser.add_argument('--cohort-paths', action='store_true',
                        help='compare repeated component parsing with single-parse lazy paths; full crash/recovery')
    parser.add_argument('--live-operational-source', choices=('finlab', 'openbb', 'finmind'),
                        help='read reviewed live controls only; full recovery of private SSD copies, never a production publication')
    parser.add_argument('--resume-operational-fixture', type=Path,
                        help='resume only an exact retained private live-control fixture; its source intent is checked')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    if sum((args.mutable_cohort, args.operational_controls, args.announcement_controls, args.inventory_buffering,
            args.object_path_index, args.metadata_updates, args.pack_digest_fusion, args.cohort_paths)) > 1:
        parser.error('choose one mutable/operational/inventory/object-name workload')
    if args.disable_pack_digest and not args.crash_child:
        parser.error('the reference digest override is restricted to the abandoned benchmark child')
    if args.reference_cohort_paths and not args.crash_child:
        parser.error('the reference path override is restricted to the abandoned benchmark child')
    if args.live_operational_source:
        if (args.mutable_cohort or args.operational_controls or args.announcement_controls
                or args.inventory_buffering or args.object_path_index
                or args.metadata_updates
                or args.pack_digest_fusion or args.disable_pack_digest or args.cohort_paths
                or args.reference_cohort_paths
                or args.trial or args.crash_child
                or args.native_limits or args.output is None or args.output.exists()
                or args.output.is_symlink() or not args.output.resolve().is_relative_to(
                    (ROOT / 'artifacts/benchmarks').resolve())):
            parser.error('live control readback requires an isolated mode and fresh private benchmark receipt')
        code = identity()
        if args.resume_operational_fixture:
            intent = args.resume_operational_fixture / 'journal/controls/live-observation.intent.json'
            admit_fixture(args.resume_operational_fixture)
            if intent.is_symlink() or not intent.is_file():
                parser.error('retained live-control fixture lacks its original intent')
            source_intent = json.loads(intent.read_bytes())
            expected_source = (ROOT / {'finlab': 'data_finlab', 'openbb': 'data_openBB', 'finmind': 'data_finmind'}[
                args.live_operational_source]).resolve(strict=True)
            if (source_intent.get('contract') != migration.CONTROL_SCOPE
                    or source_intent.get('source') != str(expected_source)):
                parser.error('retained live-control fixture belongs to another source/contract')
        proof = live_operational_trial(args.live_operational_source, resume_fixture=args.resume_operational_fixture)
        if identity() != code:
            raise ValueError('live control verifier code changed')
        proof['code_sha256'] = code
        atomic_write_json(args.output, proof, durable=True)
        print(json.dumps({key: proof[key] for key in ('state', 'provider', 'files', 'logical_bytes',
                                                     'complete_wall_seconds', 'complete_cpu_seconds')}))
        return 0
    if args.resume_operational_fixture:
        parser.error('retained operational fixtures require live source readback mode')
    if not 32 <= args.files <= 16384 or args.files % 8:
        parser.error('use a bounded multiple-of-eight fixture (32..16384 files)')
    packed.MAX_PACK_MEMBERS = args.files // 8
    if args.crash_child:
        admit_fixture(args.crash_child)
        original = packed._write_pack
        count = 0
        def crash(root, entries, target, **options):
            nonlocal count
            count += 1
            if count == 8:
                target.write_bytes(b'UNCOMMITTED partial ZIP; never a sealed hint')
                os._exit(88)  # Real process termination: bypass ExitStack/cleanup.
            return original(root, entries, target, **options)
        packed._write_pack = crash
        with _pack_digest_mode(not args.disable_pack_digest), _cohort_path_mode(not args.reference_cohort_paths):
            publish(args.crash_child, args.enabled)
        raise ValueError('deliberate mid-publication exit was not exercised')
    if args.trial:
        if args.cohort_paths:
            result = cohort_path_trial(args.files, args.trial == 'checkpoint')
        elif args.pack_digest_fusion:
            result = pack_digest_trial(args.files, args.trial == 'checkpoint')
        elif args.announcement_controls:
            result = mutable_trial(args.files, args.trial == 'checkpoint', announcement_controls=True)
        elif args.metadata_updates:
            result = metadata_update_trial(args.files, args.trial == 'checkpoint')
        elif args.object_path_index:
            result = object_path_trial(args.files, args.trial == 'checkpoint')
        elif args.inventory_buffering:
            result = inventory_trial(args.files, args.trial == 'checkpoint')
        elif args.operational_controls:
            result = mutable_trial(args.files, args.trial == 'checkpoint', operational_controls=True)
        else:
            function = mutable_trial if args.mutable_cohort else trial
            result = function(args.files, args.trial == 'checkpoint')
        print(json.dumps(result))
        return 0
    if (args.output is None or args.output.exists() or args.output.is_symlink()
            or not args.output.resolve().is_relative_to((ROOT / 'artifacts/benchmarks').resolve())):
        parser.error('a fresh private benchmark receipt is required')
    code = identity()
    limits, owner = None, None
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_wave import match_native_limits
        from scripts.verify_lakehouse_workflow_history import owner_identity
        limits, owner = match_native_limits(), owner_identity()
    results = []
    for variant in ('reference', 'checkpoint', 'checkpoint', 'reference'):
        result = subprocess.run([sys.executable, '-m', 'scripts.benchmark_sealed_pack_recovery',
            '--files', str(args.files), '--trial', variant,
            *(['--mutable-cohort'] if args.mutable_cohort else []),
            *(['--operational-controls'] if args.operational_controls else []),
            *(['--announcement-controls'] if args.announcement_controls else []),
            *(['--inventory-buffering'] if args.inventory_buffering else []),
            *(['--object-path-index'] if args.object_path_index else []),
            *(['--metadata-updates'] if args.metadata_updates else []),
            *(['--pack-digest-fusion'] if args.pack_digest_fusion else []),
            *(['--cohort-paths'] if args.cohort_paths else [])],
            check=True, capture_output=True, text=True, timeout=1200)
        row = json.loads(result.stdout)
        results.append(row)
        print(json.dumps({'trial': len(results), **row}), flush=True)
    if code != identity() or len({tuple(r['inventory_sha256']) for r in results}) != 1:
        raise ValueError('fixed code/inventory parity failed')
    if args.pack_digest_fusion and len({tuple(row['pack_archive_sha256']) for row in results}) != 1:
        raise ValueError('complete ZIP archive bytes differ between digest variants')
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_signature import limits as observed_limits
        if limits != observed_limits() or owner != owner_identity():
            raise ValueError('native owner or resource limits changed')
    metrics = ('complete_wall_seconds', 'complete_cpu_seconds', 'peak_process_rss_bytes', 'retry_pack_member_writes')
    if args.pack_digest_fusion:
        metrics += ('producer_pack_rehash_calls', 'producer_pack_rehash_bytes',
                    'pack_write_and_producer_hash_seconds', 'pack_write_and_producer_hash_cpu_seconds')
    if args.cohort_paths:
        metrics += ('cohort_fence_calls', 'cohort_fence_seconds', 'cohort_fence_cpu_seconds', 'entry_read_path_calls')
    means = {variant: {metric: statistics.mean(row[metric] for row in results if row['variant'] == variant)
        for metric in metrics}
        for variant in ('reference', 'checkpoint')}
    proof = {'contract': ('cohort_paths_complete_crash_retry_abba_v1' if args.cohort_paths else
                         'pack_digest_fusion_complete_crash_retry_abba_v1' if args.pack_digest_fusion else
                         'announcement_controls_complete_retry_abba_v1' if args.announcement_controls else
                         'metadata_update_complete_crash_retry_abba_v1' if args.metadata_updates else
                         'object_path_index_complete_crash_retry_abba_v1' if args.object_path_index else
                         'inventory_buffering_complete_crash_retry_abba_v1' if args.inventory_buffering else
                         'operational_controls_complete_retry_abba_v1' if args.operational_controls else
                         'mutable_cohort_complete_retry_abba_v1' if args.mutable_cohort else CONTRACT),
        'state': ('accepted_complete_cohort_paths_crash_retry_abba' if args.cohort_paths else
                  'accepted_complete_pack_digest_crash_retry_abba' if args.pack_digest_fusion else
                  'accepted_complete_announcement_retry_abba' if args.announcement_controls else
                  'accepted_complete_metadata_update_crash_retry_abba' if args.metadata_updates else
                  'accepted_complete_object_path_index_crash_retry_abba' if args.object_path_index else
                  'accepted_complete_inventory_crash_retry_abba' if args.inventory_buffering else
                  'accepted_complete_operational_retry_abba' if args.operational_controls else
                  'accepted_complete_mutable_retry_abba' if args.mutable_cohort else 'accepted_complete_crash_retry_abba'),
        'observed_at_utc': datetime.now(UTC).isoformat(), 'trials': results, 'means': means,
        'code_sha256': code, 'native_resource_limits': limits, 'production_migration_claim': False,
        'scope': 'owned local SSD generated fixture, not D/NAS/all providers',
        'provider_calls': 0, 'production_source_writes': 0, 'production_cold_writes': 0}
    atomic_write_json(args.output, proof, durable=True)
    print(json.dumps({'state': proof['state'], 'means': means}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
